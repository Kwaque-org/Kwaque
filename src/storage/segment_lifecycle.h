#pragma once

#include "src/base/allocation.h"
#include "src/base/error.h"
#include "src/base/invariant.h"
#include "src/base/units.h"
#include "src/codec/cooperative.h"
#include "src/codec/limits.h"
#include "src/model/position.h"
#include "src/runtime/error.h"
#include "src/runtime/file.h"
#include "src/runtime/file_error.h"
#include "src/runtime/file_position.h"
#include "src/runtime/first_failure.h"
#include "src/runtime/shard_affinity.h"
#include "src/runtime/task_scope.h"
#include "src/runtime/time.h"
#include "src/runtime/timer.h"
#include "src/storage/checkpoint.h"
#include "src/storage/device_placement.h"
#include "src/storage/format_context.h"
#include "src/storage/local_append.h"
#include "src/storage/local_discovery.h"
#include "src/storage/local_generation.h"
#include "src/storage/local_id_allocator.h"
#include "src/storage/local_metadata.h"
#include "src/storage/local_paths.h"
#include "src/storage/local_store_config.h"
#include "src/storage/page_cache.h"
#include "src/storage/retention.h"
#include "src/storage/retry_snapshot.h"
#include "src/storage/sealed_format.h"
#include "src/storage/segment_data.h"
#include "src/storage/segment_immutable.h"
#include "src/storage/segment_reader.h"
#include "src/storage/segment_run.h"
#include "src/storage/segment_scan.h"
#include "src/storage/segment_seal.h"
#include "src/storage/segment_writer.h"
#include "src/storage/segment_writer_state.h"
#include "src/storage/sparse_index.h"
#include "src/storage/sparse_index_owner.h"
#include "src/storage/workload_budget.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/noncopyable_function.hh>

#include <boost/intrusive/list.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <iterator>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <utility>
#include <variant>
#include <vector>

namespace kwaque::storage {

// The local life of one segment, as its owner on this shard keeps it. These
// are local facts. Owners above storage speak of a segment's append state,
// which has no word for a segment found again after a restart, for one that
// failed here or for one being removed, and is not widened for them.
enum class segment_life : std::uint8_t {
    // Its files are being made; nothing is appended yet.
    creating,
    // Appended to and read.
    writable,
    // Its seal runs: appended to no more, read as before.
    sealing,
    // Published sealed: read through its generation.
    sealed,
    // Found unsealed by a restart: never appended again here.
    recovering,
    // Something of it failed here. Nothing reopens it in this process.
    failed,
    // Being removed.
    deleting,
};

// What changes a segment. Appends and reads overlap one another and every
// one of these. Of these, one runs at a time for a segment: the next waits
// for the one before it, or is the same request and joins it.
enum class segment_transition : std::uint8_t {
    seal,
    index_publication,
    retry_cut,
    deletion,
    close,
};

// Whether a segment in `life` takes appends.
[[nodiscard]] constexpr bool segment_appends(segment_life life) noexcept {
    return life == segment_life::writable;
}
// Whether a segment in `life` lets a new session in. A session that is in
// reads on to the bound it took, whatever its segment becomes, until its
// owner ends it.
[[nodiscard]] constexpr bool
segment_admits_readers(segment_life life) noexcept {
    return life == segment_life::writable || life == segment_life::sealing
           || life == segment_life::sealed;
}
// Whether `change` may begin for a segment in `life`. A change that waited
// for another asks this again when its turn comes: what it waited for may
// have made it moot.
[[nodiscard]] constexpr bool
segment_admits(segment_life life, segment_transition change) noexcept {
    switch (change) {
    case segment_transition::seal:
    case segment_transition::retry_cut:
        return life == segment_life::writable;
    case segment_transition::index_publication:
        return life == segment_life::sealed;
    case segment_transition::deletion:
        // A writable segment is detached first, and one whose seal runs is
        // sealed or failed when its turn comes.
        return life == segment_life::sealed || life == segment_life::recovering
               || life == segment_life::failed;
    case segment_transition::close:
        return true;
    }
    return false;
}

// The format's and the shard's bounds on what a table may be configured
// with.
inline constexpr std::uint32_t maximum_open_sealed_segments = 256;
inline constexpr std::uint32_t maximum_concurrent_seals = 16;
inline constexpr std::uint32_t maximum_concurrent_creations = 8;
inline constexpr std::uint32_t maximum_concurrent_deletions = 4;
inline constexpr std::uint32_t maximum_deleting_segments = 256;

struct segment_table_config final {
    // Segments that are not sealed: being created, appended to, being
    // sealed, failed with their files still open, or found unsealed by a
    // restart and not yet removed. No more than the append owner attaches.
    // It is what a restart may find unsealed, so what its catalog must
    // list of them stays inside it.
    std::uint32_t maximum_writable{64};
    // Sealed segments kept open for readers: each holds its cached runs,
    // and its generation and index once a reader came. One more makes the
    // one nothing uses and nothing has used for longest close first; with
    // none unused the one more is refused.
    std::uint32_t maximum_open_sealed{64};
    // Seals in progress. The rest wait in the order they were asked for.
    std::uint32_t maximum_seals{4};
    // Segments in creation at once, which count among the writable. One
    // more is refused, and whoever asked asks again.
    std::uint32_t maximum_creations{2};
    // Removals that take files away at once. The rest wait their turn,
    // drained and under their deleting name.
    std::uint32_t maximum_deletions{1};
    // Segments being removed that the table holds an owner for: one whose
    // removal is in flight, waiting its turn or taking its files away; and
    // one the table had open before, whose removal settles or is owed. A
    // removal of a segment the table had no owner for holds a place only
    // while its attempt runs. One more is refused, and whoever asked asks
    // again.
    std::uint32_t maximum_deleting{16};
    // The stride of every index this shard feeds and opens.
    sparse_index_stride stride{};
    // What each segment's cached runs are made with.
    segment_run_limits runs{};
    byte_count feed_bytes{8_MiB};
    std::uint64_t feed_linger_nanoseconds{default_feed_linger_nanoseconds};
    // What a generation and an index are opened and read under.
    local_store_io_limits metadata{};
    segment_scan_limits scan{};
    local_reader_limits readers{};
    codec::limits policy{codec::limits::defaults()};

    [[nodiscard]] runtime::result<void> validate() const noexcept {
        if (
          maximum_writable == 0
          || maximum_writable > maximum_local_append_segments
          || maximum_open_sealed == 0
          || maximum_open_sealed > maximum_open_sealed_segments
          || maximum_seals == 0 || maximum_seals > maximum_concurrent_seals
          || maximum_creations == 0
          || maximum_creations > maximum_concurrent_creations
          || maximum_creations > maximum_writable || maximum_deletions == 0
          || maximum_deletions > maximum_concurrent_deletions
          || maximum_deleting < maximum_deletions
          || maximum_deleting > maximum_deleting_segments
          || feed_bytes.value() == 0)
            return runtime::failure(detail::path_error(errc::invalid_argument));
        if (auto valid = runs.validate(); !valid) return valid;
        if (auto valid = metadata.validate(); !valid) return valid;
        if (auto valid = scan.validate(); !valid) return valid;
        return readers.validate();
    }
    // What the budget that funds the table's writers is asked for: every
    // writable segment open and as many of them in creation as may be,
    // beside `reserved` for whatever else draws on it. A shard checks it
    // with validate_segment_handles() before it creates any owner.
    [[nodiscard]] constexpr segment_handle_demand
    handle_demand(std::uint32_t reserved = 0) const noexcept {
        return {maximum_writable, maximum_creations, reserved};
    }
};

// A segment to create: what its owner of segments supplies. Nothing of it is
// minted here.
struct segment_creation final {
    local_segment_descriptor descriptor;
    // The configured data device it is created on. With none, the shard's
    // device table chooses; a named one is taken only if that table finds
    // it eligible.
    std::optional<device_store_id> device;
    // Its retry object is reserved by the table and need not be given.
    segment_writer_config writer;
};

// What a sealed segment's owner of segments holds it to be, for a segment
// this shard has no owner open for: the generation it was published at, the
// publication, each root's context in the publication's order, the layout
// its header fixed and the sealed extent. Its generation is opened against
// these and checked as any generation is; nothing of them is taken from the
// files, and the extent is not walked.
struct sealed_segment_expectation final {
    device_store_id device;
    local_publication_generation generation;
    local_object_publication publication;
    local_segment_descriptor descriptor;
    std::vector<local_bundle_context> roots;
    segment_history_context history;
    segment_immutable_expectation extent;
};

// What a seal through the table answers: its writer's outcome and, once the
// segment is sealed, what it is opened against from here on. Whoever owns
// the segments keeps `expected` from this answer: the table lets a sealed
// owner go when it needs its place, and a reader that comes later, or a
// removal, is taken only with it. None for a segment that is not sealed,
// and when nothing could be kept of a sealed one, which a restart reads
// back.
struct segment_sealed final : segment_seal_outcome {
    seastar::lw_shared_ptr<const sealed_segment_expectation> expected;
};

// What became of a request to remove a segment.
struct segment_removal final {
    // What the authorization was found to name. Nothing is done for any
    // answer but authorized and repeated.
    authorization_verdict verdict{authorization_verdict::unknown};
    // The segment is gone: its directory's names are durably removed and
    // nothing durable on this shard names it. Whoever owns the segments
    // need not say again that it expires.
    bool gone{false};
    // Its files are removed and its directory stays, under its deleting
    // name, where a restart finds it: the durable checkpoint still carries
    // an entry for the segment or has not passed its PREPAREs. The same
    // request finishes it once a later checkpoint is durable.
    bool settling{false};
    // Files of the segment that this attempt removed.
    std::uint32_t removed{0};
    // Names in its directory that are no file of a segment: left where
    // they are, and the directory with them.
    std::uint32_t foreign{0};
    // Why the attempt ended before the segment was gone. The segment stays
    // deleting, and the same request tries again.
    runtime::first_failure failure;
    // What its owners could not close without failing, the first of it:
    // the removal went on.
    runtime::first_failure debt;
};

// A segment that whoever owns the segments should follow with another, and
// why. Each reason is told once for a segment, in the first report that has
// it; reasons that came together are in one report.
struct segment_roll_report final {
    segment_context segment;
    segment_roll_reasons reasons;
};
// Told on the table's shard from the table's own task, outside every step of
// a segment's owner and of the append owner, so it may call back into
// either. It must not throw. A report asks for nothing: no segment is sealed
// or created for it here.
using segment_roll_sink
  = seastar::noncopyable_function<void(const segment_roll_report&) noexcept>;

// What a session's caller may see, and how the session reads.
struct segment_session_request final {
    model::range_logical_end visible;
    // `visible` was already cut to what this copy holds.
    bool short_of_visibility{false};
    segment_reader_config config{};
};

// The owners of this shard's segments, each found by its segment context
// alone, and what may run together for one segment.
//
// An owner is one object for a segment's whole local life here: from when
// it is created or first opened for a reader until it is closed. It holds
// what exists for the segment in the state it is in (its writer, its
// attachment to the append owner, the index its writer feeds, its
// completed-retry cuts, its cached runs, its sessions and, once sealed, its
// generation and index) and nothing else decides for it. Appends go to the
// append owner through target(); everything else comes through here.
//
// The table is not a catalog. Which segments a store has is their owner's
// to know and to supply; a sealed segment nothing reads has no owner here,
// and one is made when a reader comes and unmade when the table needs its
// place.
//
// What runs together is one rule; see segment_transition. A segment that
// fails stays failed: it takes no append and no new session, the sessions it
// has read on below what was durable, and its files stay open until its
// owner is closed, because those sessions read through them. Nothing of a
// segment leaves its device but through remove(), for an authorization its
// owner of segments gave.
//
// Every provider outlives joined close(). close() follows the append
// owner's, or runs while that owner still takes requests.
template<
  runtime::file_system_backend Backend,
  local_directory_owner Owner,
  runtime::monotonic_clock Clock>
class segment_table final : public runtime::shard_affine {
public:
    using writer_type = segment_writer<Backend, Owner, Clock>;
    using append_type = local_append<Backend, Owner, Clock>;
    using checkpoint_type = local_checkpoint<Backend, Owner, Clock>;
    using allocator_type = local_id_allocator<Backend, Owner>;
    using retry_type = retry_snapshot<Backend, Owner, Clock>;
    using index_type = sealed_sparse_index<Backend, Owner>;

    struct providers final {
        Backend& files;
        Owner& ownership;
        // The stores a segment may be on; borrowed until joined close().
        std::span<const local_device_spec> devices;
        std::uint32_t shard;
        append_type& append;
        // The shard's checkpoint owner, when it has one.
        checkpoint_type* checkpoint;
        allocator_type& ids;
        page_cache& cache;
        sparse_index_residency& residency;
        sparse_index_rebuild_limit& rebuilds;
        // Funds writers, the indexes they feed and the table itself.
        workload_budget& writing;
        // Funds cached runs, generations and what readers use of an index.
        workload_budget& reading;
        // Funds the rebuild of an index that is owed.
        workload_budget& rebuilding;
        // The shard's device table, when it has one: it chooses the device
        // of a new segment and keeps the shard inside its share of each
        // filesystem. It outlives joined close().
        device_table* placement{nullptr};
    };

    [[nodiscard]] static runtime::result<std::unique_ptr<segment_table>>
    make(providers given, segment_table_config config) {
        if (auto valid = config.validate(); !valid)
            return runtime::failure(valid.error());
        if (given.devices.size() > maximum_local_devices)
            return runtime::failure(detail::path_error(errc::invalid_argument));
        // Segments that left the append owner are remembered for as many
        // as the table opens: see quieted().
        const auto recent = std::size_t{config.maximum_writable}
                            + config.maximum_open_sealed;
        // Beside those: the segments a restart found unsealed, which were
        // writable before it, and the removals.
        const auto capacity = recent + config.maximum_writable
                              + config.maximum_deleting;
        const auto instance = given.writing.allocation_charge(
          byte_count{sizeof(segment_table)});
        const auto slots = given.writing.allocation_charge(
          byte_count{capacity * sizeof(std::unique_ptr<owner>)});
        const auto each = given.writing.allocation_charge(
          byte_count{sizeof(owner)});
        const auto marks = given.writing.allocation_charge(
          byte_count{recent * sizeof(quiet_mark)});
        if (!instance) return runtime::failure(instance.error());
        if (!slots) return runtime::failure(slots.error());
        if (!each) return runtime::failure(each.error());
        if (!marks) return runtime::failure(marks.error());
        auto held = given.writing.try_reserve(
          byte_count{
            instance->value() + slots->value() + capacity * each->value()
            + marks->value()});
        if (!held) return runtime::failure(held.error());
        try {
            // Before the table exists: nothing unjoined is left behind.
            std::vector<std::unique_ptr<owner>> owners;
            owners.reserve(capacity);
            std::vector<quiet_mark> quiet;
            quiet.reserve(recent);
            return std::unique_ptr<segment_table>{new segment_table(
              given,
              config,
              std::move(*held),
              std::move(owners),
              std::move(quiet))};
        } catch (const std::bad_alloc&) {
            return runtime::failure(
              detail::path_error(errc::resource_exhausted));
        }
    }
    segment_table(const segment_table&) = delete;
    segment_table& operator=(const segment_table&) = delete;
    ~segment_table() {
        assert_current();
        KWAQUE_INVARIANT(
          invariant_id{"KQ-SEGMENT-TABLE-CLOSED"},
          closed_ && owners_.empty(),
          "segment table destroyed before joined close");
    }

    // Owners in the table, and those of them that are sealed.
    [[nodiscard]] std::size_t size() const noexcept {
        assert_current();
        return owners_.size();
    }
    [[nodiscard]] std::size_t open_sealed() const noexcept {
        assert_current();
        return count(segment_life::sealed);
    }
    [[nodiscard]] std::optional<segment_life>
    life(const segment_context& context) const noexcept {
        assert_current();
        const auto* found = find(context);
        return found ? std::optional{found->life} : std::nullopt;
    }
    // Where a writable segment's appends go, in the append owner.
    [[nodiscard]] std::optional<local_append_target>
    target(const segment_context& context) const noexcept {
        assert_current();
        const auto* found = find(context);
        return found && segment_appends(found->life) ? found->target
                                                     : std::nullopt;
    }
    // Sessions its owner has in its list.
    [[nodiscard]] std::size_t
    sessions(const segment_context& context) const noexcept {
        assert_current();
        const auto* found = find(context);
        return found ? found->sessions.size() : 0;
    }
    // What a sealed segment's owner here holds it to be, which its seal
    // answered with: its owner of segments keeps that answer, and supplies
    // it again when a reader comes for a segment the table has let go.
    // Nothing for a segment that is not sealed, or that the table holds no
    // owner for. A native allocation failure throws.
    [[nodiscard]] std::optional<sealed_segment_expectation>
    expectation(const segment_context& context) const {
        assert_current();
        const auto* found = find(context);
        return found && found->life == segment_life::sealed ? found->expected
                                                            : std::nullopt;
    }
    // Why the segment failed here; and what its owner could not release or
    // close without it failing the segment, the first of them.
    [[nodiscard]] runtime::first_failure
    failure(const segment_context& context) const {
        assert_current();
        const auto* found = find(context);
        return found ? found->failure : runtime::first_failure{};
    }
    [[nodiscard]] runtime::first_failure
    debt(const segment_context& context) const {
        assert_current();
        const auto* found = find(context);
        return found ? found->debt : runtime::first_failure{};
    }

    // Whether a restart of this shard would still look for a segment that
    // is appended to no more here: the durable checkpoint carries an entry
    // for it, or has not passed its PREPAREs in the WAL. While this holds,
    // whoever owns the segments lists the segment in the catalog a start is
    // given, sealed or not: a start fails on a checkpoint entry its catalog
    // does not explain. A later checkpoint ends it, and a clean stop takes
    // one. Always so for a segment that is still attached, and never on a
    // shard with no checkpoint owner.
    [[nodiscard]] bool restart_names(const segment_context& context) const {
        assert_current();
        const auto* found = find(context);
        if (found && found->target) return checkpoint_ != nullptr;
        return named_below(
          context, found && found->quiet ? found->quiet : quiet_below(context));
    }

    // Segments whose lifetime runs: each has had a group accepted and is
    // still appended to.
    [[nodiscard]] std::size_t aging() const noexcept {
        assert_current();
        return aging_.size();
    }
    // Why the lifetimes of idle segments are told no more: what the sleep on
    // their timer failed with, when it was not the table that ended it.
    [[nodiscard]] runtime::first_failure age_failure() const {
        assert_current();
        return unslept_;
    }
    // What is left of a writable segment before its next group: the bytes
    // of blocks it still takes, how many, and when its lifetime ends.
    // Whoever owns the segments reads it after an append it routed, and
    // makes the successor while there is still room.
    [[nodiscard]] runtime::result<segment_left>
    left(const segment_context& context) const {
        assert_current();
        const auto* found = find(context);
        if (!found) return failed(errc::not_found);
        if (!segment_appends(found->life)) return failed(errc::closed);
        return found->writer->left();
    }

    // Starts telling `sink` why a segment should be followed by another:
    // that a request was refused for its size, its counts or its retry
    // state, that its lifetime ended while it was idle, or that its device
    // was reported. Each reason is told once for a segment, and nothing is
    // sealed or created for it. The lifetimes are slept on `timer`, one
    // sleep at a time, to the earliest. Bound once and before the first
    // segment is created, so that nothing is missed. Both outlive joined
    // close().
    template<runtime::timer_service Timer>
    [[nodiscard]] runtime::result<void>
    report_rolls(Timer& timer, segment_roll_sink sink) {
        assert_current();
        if (closing_ || closed_) return failed(errc::closed);
        if (!sink) return failed(errc::invalid_argument);
        if (told_ || !owners_.empty()) return failed(errc::wrong_context);
        timer_ = &timer;
        sleep_ = sleep_function<Timer>;
        told_ = std::move(sink);
        runtime::result<void> spawned;
        try {
            spawned = tasks_.spawn([this] { return tell_loop(); });
        } catch (...) {
            spawned = runtime::failure(
              detail::path_error(errc::resource_exhausted));
        }
        if (!spawned) {
            told_ = segment_roll_sink{};
            timer_ = nullptr;
            sleep_ = nullptr;
        }
        return spawned;
    }
    // A device that is short of space, stalled or failed, as whoever
    // watches the devices reports it: each writable segment on it is told
    // so, once.
    void tell_device(device_store_id device) noexcept {
        assert_current();
        for (const auto& held : owners_)
            if (held->device->owner.device() == device)
                note(*held, segment_roll_reasons{segment_roll_reason::device});
    }

    // What only this shard knows of a segment, for a walk that selects
    // what may go: what its data file holds, and whether anything here
    // still needs it. A sealed segment the table has no owner for is known
    // by `sealed`, which its caller then supplies; without it such a
    // segment is unknown. Nothing is read from any device.
    [[nodiscard]] retention_local retained(
      const segment_context& context,
      const sealed_segment_expectation* sealed = nullptr) const noexcept {
        assert_current();
        const auto* found = find(context);
        if (!found) {
            if (!sealed || !names(*sealed, context)) return {};
            return {
              byte_count{sealed->extent.file_end.value()},
              retention_need::none};
        }
        // One that is being removed goes whatever a rule says of it, and
        // holds nothing that a walk should count again.
        if (found->life == segment_life::deleting)
            return {byte_count{}, retention_need::none, true};
        if (found->life != segment_life::sealed) {
            const auto* written = found->writer ? &found->writer->progress()
                                                : nullptr;
            return {
              byte_count{
                written && *written ? (*written)->written.bytes.value() : 0},
              retention_need::unsealed};
        }
        return {
          byte_count{found->extent ? found->extent->file_end.value() : 0},
          found->sessions.size() != 0 || found->inside != 0 || found->closing
            ? retention_need::read
            : retention_need::none};
    }
    // Checks an authorization to remove a segment against what this shard
    // holds the segment to be, and changes nothing. A segment that is
    // being created, appended to or sealed is refused until its owner of
    // segments has sealed or closed it; a sealed one is authorized only by
    // the extent it was published as; one that failed or was found by a
    // restart only as not sealed; one whose seal failed after it began to
    // publish its sealed pointer is not known to be either, and is
    // authorized as neither until a restart has read what its device
    // holds. One that is being removed answers the
    // authorization it was accepted with as repeated, and any other as not
    // what is held; one that is being removed with nothing accepted, which
    // a restart found under its deleting name, takes the first that names
    // it. A sealed segment the table has no owner for is known by `sealed`
    // here, which is its caller's word: remove() holds it against the
    // device before anything is done.
    [[nodiscard]] authorization_verdict check_deletion(
      const deletion_authorization& supplied,
      const sealed_segment_expectation* sealed = nullptr) const noexcept {
        assert_current();
        segment_held held{
          supplied.segment,
          segment_holding::unknown,
          std::nullopt,
          std::nullopt};
        if (const auto* found = find(supplied.segment)) {
            if (found->life == segment_life::deleting) {
                held.holding = segment_holding::deleting;
                held.accepted = found->accepted;
            } else if (
              found->life == segment_life::creating
              || found->life == segment_life::writable
              || found->life == segment_life::sealing) {
                held.holding = segment_holding::attached;
            } else if (found->life == segment_life::sealed) {
                held.holding = segment_holding::sealed;
                held.extent = found->extent;
            } else if (!maybe_sealed(*found)) {
                held.holding = segment_holding::unsealed;
            }
        } else if (sealed && names(*sealed, supplied.segment)) {
            held.holding = segment_holding::sealed;
            held.extent = sealed->extent;
        }
        return authorize_deletion(supplied, held);
    }

    // A device's state changed, as the shard's device table reports it.
    // The owner of the writable segments on it is told once for each. A
    // device that failed also fails those segments, and their owner is told
    // though it was told of the device before: they take no new session,
    // no seal and no cut, and their admission in the append owner closes,
    // so what they had accepted settles and nothing more is. What is
    // sealed there is read while reads succeed.
    void device_changed(const device_report& report) noexcept {
        assert_current();
        if (report.state == device_state::healthy) return;
        if (report.state != device_state::failed) {
            tell_device(report.device);
            return;
        }
        const auto why = runtime::make_file_error(
          errc::io_failure,
          report.cause == device_failure::read_only
            ? runtime::file_failure_detail::read_only
          : report.cause == device_failure::gone
            ? runtime::file_failure_detail::device_gone
            : runtime::file_failure_detail::device_io);
        bool noted = false;
        for (const auto& held : owners_) {
            if (
              held->device->owner.device() != report.device
              || held->life != segment_life::writable)
                continue;
            // Told first, and kept to be told: the segment is failed here
            // before its own writer has failed. Told again if the device
            // was told of while it was only short or slow.
            const segment_roll_reasons device{segment_roll_reason::device};
            held->told = held->told.without(device);
            noted = keep(*held, device) || noted;
            held->failure.observe(why);
            held->life = segment_life::failed;
            if (held->aging.is_linked())
                aging_.erase(aging_.iterator_to(*held));
            unplace(*held);
            hush(*held);
        }
        if (noted) stir();
    }
    // What this shard's segments may still come to write, for each pool of
    // its device table: a segment in creation its whole bound, a writable
    // or sealing one its bound less what its data file holds. The sampler
    // takes it off what the filesystem has before it hands out shares.
    [[nodiscard]] std::array<byte_count, maximum_local_devices>
    owed() const noexcept {
        assert_current();
        std::array<byte_count, maximum_local_devices> pools{};
        if (!placement_) return pools;
        for (const auto& held : owners_) {
            const auto pool = placement_->pool(held->device->owner.device());
            if (!pool) continue;
            const auto more = held->writer
                                  && held->life != segment_life::creating
                                ? held->writer->owed()
                              : held->life == segment_life::creating
                                ? held->bound
                                : byte_count{};
            if (const auto sum = pools[*pool].checked_add(more))
                pools[*pool] = *sum;
        }
        return pools;
    }

    // Takes in a segment a restart found unsealed. It is appended to no
    // more here and holds nothing open; its owner of segments is told once
    // that it was recovered, and removes it by an authorization that names
    // it as not sealed. It counts among the segments that are not sealed,
    // which are bounded together: one more is refused, and no segment is
    // created in its place until it is removed.
    [[nodiscard]] runtime::result<void>
    adopt_recovered(const segment_context& context, device_store_id device) {
        assert_current();
        if (closing_ || closed_) return failed(errc::closed);
        if (find(context)) return failed(errc::already_exists);
        const auto* on = store(device);
        if (!on) return failed(errc::wrong_context);
        if (unsealed() >= config_.maximum_writable)
            return failed(errc::queue_full);
        auto* made = insert(context, *on, segment_life::recovering);
        if (!made) return failed(errc::resource_exhausted);
        if (told_) {
            made->untold.add(
              segment_roll_reasons{segment_roll_reason::recovered});
            untold_.push_back(*made);
            stir();
        }
        return {};
    }

    // A writable segment failed: its writer's first failure, as the append
    // owner's failure sink reports it. The segment takes no append and no
    // new session from here on. Nothing for a segment in any other state,
    // whose own transition says what became of it.
    void fail(
      const segment_context& context,
      const runtime::first_failure& why) noexcept {
        assert_current();
        auto* found = find(context);
        if (!found || found->life != segment_life::writable) return;
        if (!found->failure.failed()) found->failure = why;
        found->life = segment_life::failed;
        retire(*found);
    }

    // Creates a segment and attaches it: its files, the index and the runs
    // its writer feeds, its place in the append owner and its cuts. The
    // descriptor, its context included, is supplied whole, and its writer
    // seals under the table's policy. Refused with already_exists for a
    // context the table has an owner for, and with queue_full when the
    // segments that are not sealed, those in creation and those a restart
    // found among them, or the creations alone are as many as may be. A
    // creation that made nothing leaves nothing: one refused before its
    // writer exists, and one whose directory was there before, which is
    // another segment's under this name and is refused with already_exists
    // too. One that fails later leaves a failed owner, since it may have
    // left files: they are not removed here. So does one whose device
    // failed while it was made.
    //
    // It seals nothing and waits for no other segment: a successor is
    // created, attached and appended to while its predecessor still takes
    // appends or seals, and which of the two a request goes to is its
    // caller's.
    [[nodiscard]] seastar::future<runtime::result<local_append_target>>
    create(segment_creation request, codec::cooperative_work& work) {
        using output = runtime::result<local_append_target>;
        assert_current();
        if (closing_) co_return output{failed(errc::closed)};
        const auto context = request.descriptor.segment;
        if (find(context)) co_return output{failed(errc::already_exists)};
        if (request.writer.policy != config_.policy)
            co_return output{failed(errc::invalid_argument)};
        if (
          unsealed() >= config_.maximum_writable
          || count(segment_life::creating) >= config_.maximum_creations)
            co_return output{failed(errc::queue_full)};
        // Its device: the one its caller named, or the one the device
        // table chooses, where its whole bound fits the shard's share.
        byte_count bound;
        if (placement_) {
            const auto whole = writer_type::disk_bound(
              request.descriptor, request.writer);
            if (!whole) co_return output{runtime::failure(whole.error())};
            bound = *whole;
            const auto chosen = placement_->choose(
              bound, Clock::now(), request.device);
            if (!chosen) co_return output{runtime::failure(chosen.error())};
            request.device = *chosen;
        }
        if (!request.device) co_return output{failed(errc::invalid_argument)};
        const auto* device = store(*request.device);
        if (!device) co_return output{failed(errc::wrong_context)};
        auto* made = insert(context, *device, segment_life::creating);
        if (!made) co_return output{failed(errc::resource_exhausted)};
        if (placement_) {
            // Taken from the share at once: the next creation sees it.
            placement_->placed(*request.device, bound);
            made->bound = bound;
            made->placed = true;
        }
        std::optional<stay> inside{std::in_place, *made};
        runtime::result<void> created = failed(errc::resource_exhausted);
        try {
            created = co_await create_owned(*made, std::move(request), work);
        } catch (const std::bad_alloc&) {
        } catch (...) {
            created = failed(errc::io_failure);
        }
        // Its device may have failed while it was made: it fails with it,
        // as the segments that were written there did.
        if (
          created && placement_
          && placement_->cause(made->device->owner.device())
               != device_failure::none)
            created = failed(errc::io_failure);
        if (created) {
            made->life = segment_life::writable;
            co_return output{*made->target};
        }
        made->failure.observe(created.error());
        made->life = segment_life::failed;
        if (made->writer && created.error().code() != errc::already_exists) {
            // It may have left files, and they are not removed here. What
            // it was attached with takes nothing.
            retire(*made);
            hush(*made);
            co_return output{runtime::failure(created.error())};
        }
        // Nothing of it was made: no owner is left, its share is given
        // back, and the same request may be made again. A close that waits
        // for this creation takes the owner out itself; a writer that made
        // nothing is closed on the way out.
        inside.reset();
        unplace(*made, true);
        if (!made->closing) {
            if (made->writer) {
                static_cast<void>(co_await close_segment(context));
            } else {
                made->feeding.reset();
                erase(*made);
            }
        }
        co_return output{runtime::failure(created.error())};
    }

    // Seals a writable segment, as one attempt its owner runs to the end
    // whoever stops waiting. In order: its cuts end; its attachment's
    // admission closes and what it accepted drains through both barriers,
    // so the writer's seal abandons nothing; it detaches, and its writer
    // seals with the facts the detach handed over, publishing the index it
    // fed in the same publication when it can; that publication covers the
    // checkpoint's entries and releases the pinned PREPAREs; its sessions
    // move to the generation and index it was published as; its runs are
    // told; its writer closes.
    //
    // No append to another segment waits for any of it, and its own readers
    // read on throughout. A request made while one runs, or after, is given
    // that attempt's outcome. A seal that fails leaves the segment failed,
    // with no evidence of a seal: when it failed after it began to publish
    // its sealed pointer the device may hold the segment sealed all the
    // same, and nothing here says which until a restart reads it. The
    // answer of a seal that succeeded carries what the sealed segment is
    // opened against, for its owner of segments to keep.
    [[nodiscard]] seastar::future<segment_sealed>
    seal(segment_context context) {
        assert_current();
        segment_sealed refused;
        auto* found = find(context);
        if (closing_ || !found || found->closing) {
            refused.failure.observe(
              detail::path_error(found ? errc::closed : errc::not_found));
            co_return refused;
        }
        if (found->sealing)
            co_return co_await found->sealing->get_shared_future();
        if (!segment_admits(found->life, segment_transition::seal)) {
            refused.failure = found->failure;
            refused.failure.observe(detail::path_error(errc::closed));
            co_return refused;
        }
        found->sealing.emplace();
        auto joined = found->sealing->get_shared_future();
        runtime::result<void> spawned;
        try {
            spawned = tasks_.spawn([this, found] { return run_seal(*found); });
        } catch (...) {
            spawned = runtime::failure(
              detail::path_error(errc::resource_exhausted));
        }
        if (!spawned) {
            // Nothing began: the next request is a new attempt.
            found->sealing.reset();
            refused.failure.observe(spawned.error());
            co_return refused;
        }
        co_return co_await std::move(joined);
    }

    // Cuts a completed-retry snapshot of a writable segment when one is
    // due, or whenever a fact is unsaved when `forced`. It is one of the
    // segment's transitions: it waits for one before it, and a seal waits
    // for it.
    [[nodiscard]] seastar::future<runtime::result<retry_snapshot_outcome>>
    cut_retries(segment_context context, bool forced) {
        using output = runtime::result<retry_snapshot_outcome>;
        assert_current();
        auto* found = find(context);
        if (!found) co_return output{failed(errc::not_found)};
        // Nobody enters an owner that takes no cut: whoever ends it need
        // not wait for them.
        if (
          closing_ || found->closing
          || !segment_admits(found->life, segment_transition::retry_cut))
            co_return output{failed(errc::closed)};
        const stay inside{*found};
        const auto turn = co_await seastar::get_units(found->turn, 1);
        if (
          closing_ || found->closing
          || !segment_admits(found->life, segment_transition::retry_cut)
          || !found->retries)
            co_return output{failed(errc::closed)};
        co_return co_await found->retries->request(forced);
    }

    // A session on a segment, whatever its state: through the pin of its
    // writer's reader handle while it is appended to, and through its
    // generation once it is sealed. `reader` is the session's budget and
    // outlives what the session returns.
    //
    // A sealed segment the table has no owner for is opened from `sealed`,
    // which its caller then supplies and which is borrowed until this
    // returns: the open needs no owner beforehand, and the owner it makes
    // goes when the table needs its place. Without it such a segment is
    // not_found. A segment that is being created, or whose seal has passed
    // the point where its writer gives a reader a pin and has not yet been
    // published, asks its caller back with queue_full; one that failed, is
    // being removed or is closing is refused with closed.
    [[nodiscard]] seastar::future<runtime::result<segment_reader>> open_session(
      segment_context context,
      segment_session_request request,
      workload_budget& reader,
      codec::cooperative_work& work,
      const sealed_segment_expectation* sealed = nullptr) {
        using output = runtime::result<segment_reader>;
        assert_current();
        if (closing_) co_return output{failed(errc::closed)};
        auto* found = find(context);
        bool cold = false;
        if (!found) {
            if (!sealed) co_return output{failed(errc::not_found)};
            const auto* device = store(sealed->device);
            if (!device || !names(*sealed, context))
                co_return output{failed(errc::wrong_context)};
            // Its own copy first: nothing of the caller's is needed after.
            std::optional<sealed_segment_expectation> expected;
            try {
                expected.emplace(*sealed);
            } catch (const std::bad_alloc&) {
                co_return output{failed(errc::resource_exhausted)};
            }
            if (!co_await make_room())
                co_return output{failed(errc::queue_full)};
            // Its place is held from here to its insert, or to its refusal.
            const auto arrived = seastar::defer(
              [this] noexcept { --arriving_; });
            if (closing_) co_return output{failed(errc::closed)};
            // Another caller may have brought it meanwhile.
            found = find(context);
            if (!found) {
                found = insert(context, *device, segment_life::sealed);
                if (!found) co_return output{failed(errc::resource_exhausted)};
                found->extent = expected->extent;
                found->expected = std::move(expected);
                found->unproved = true;
                cold = true;
            }
        }
        // Nobody enters an owner that lets no reader in: whoever ends it
        // need not wait for them.
        if (found->life == segment_life::creating)
            co_return output{failed(errc::queue_full)};
        if (found->closing || !segment_admits_readers(found->life))
            co_return output{failed(errc::closed)};
        found->used = ++uses_;
        std::optional<stay> inside{std::in_place, *found};
        auto opened = co_await open_owned(*found, request, reader, work);
        // An owner made for a reader that could not be let in is unmade.
        const bool unused = !opened && cold && found->sessions.size() == 0
                            && !found->pin;
        inside.reset();
        if (unused) static_cast<void>(co_await close_segment(context));
        co_return opened;
    }

    // Removes a segment whose removal its owner of segments authorized, as
    // one attempt the table runs to its end whoever stops waiting. The
    // authorization is checked as check_deletion() checks it, and nothing
    // is done unless it names what the shard holds.
    //
    // From the first step the segment is deleting: it takes no session,
    // and every session it has is ended where it stands. In order then:
    // whoever is inside leaves; its readers drain and what is open of it
    // closes, see drain(); its directory takes its deleting name and the
    // directory above is synced, from where the removal only goes forward
    // and a restart resumes it; that mark stands where a sealed publication
    // would, so it covers the checkpoint's entries and releases the pinned
    // PREPAREs; its files go, one call at a time, when its turn among the
    // shard's removals comes; and its directory goes last, with one more
    // sync of the directory above. Names in it that are no file of a
    // segment are left and counted, and the directory with them.
    //
    // The directory stays, and the segment is not reported gone, while the
    // durable checkpoint carries an entry for it or has not passed its
    // PREPAREs: a restart would look for it. A checkpoint that lets go of
    // the entry is asked for once; when the durable checkpoint still names
    // the segment after it, the outcome says that the removal is settling.
    //
    // A step that fails ends the attempt with its cause, and the segment
    // stays deleting. The same request made again joins the attempt in
    // flight or starts the next, which goes on where the last stopped; a
    // directory sync that failed is not tried again on its device in this
    // process.
    //
    // A segment the table has no owner for is looked for by its
    // directory's name on every data device, which decides as it does at a
    // restart. Under its deleting name its removal goes on, whatever
    // authorization names it. Under neither name it is gone, and is
    // reported so again. Whole under its own name it is removed only as
    // the sealed segment `sealed` holds it to be, which its caller then
    // supplies and which is borrowed until this returns, and only when the
    // device's own sealed root says the same; without that the shard does
    // not know it. The owner such a removal makes holds nothing, and what
    // the removal reached is on the device: it leaves the table when the
    // attempt ends, whether or not the segment is gone, and the same
    // request finds it again by its name.
    [[nodiscard]] seastar::future<segment_removal> remove(
      deletion_authorization authorization,
      const sealed_segment_expectation* sealed = nullptr) {
        assert_current();
        segment_removal refused;
        auto* found = find(authorization.segment);
        if (closing_ || (found && found->closing)) {
            refused.failure.observe(detail::path_error(errc::closed));
            co_return refused;
        }
        if (!found) {
            // Its own copy first: nothing of the caller's is needed after.
            std::optional<sealed_segment_expectation> held;
            try {
                if (sealed) held.emplace(*sealed);
            } catch (const std::bad_alloc&) {
                refused.failure.observe(
                  detail::path_error(errc::resource_exhausted));
                co_return refused;
            }
            co_return co_await remove_unowned(
              std::move(authorization), std::move(held));
        }
        // An owner made for a reader holds its caller's word until the
        // device has borne it out: asked again once it has, or once the
        // owner is unmade.
        if (found->unproved) {
            refused.failure.observe(detail::path_error(errc::queue_full));
            co_return refused;
        }
        refused.verdict = check_deletion(authorization);
        if (
          refused.verdict != authorization_verdict::authorized
          && refused.verdict != authorization_verdict::repeated)
            co_return refused;
        if (found->life != segment_life::deleting) {
            if (count(segment_life::deleting) >= config_.maximum_deleting) {
                refused.failure.observe(detail::path_error(errc::queue_full));
                co_return refused;
            }
            retire(*found);
            found->life = segment_life::deleting;
            found->surplus = false;
            found->sessions.revoke();
        }
        // Also the first authorization of a segment a restart found under
        // its deleting name.
        found->accepted = std::move(authorization);
        co_return co_await attempted(found, refused.verdict);
    }

    // Goes on with a removal that a restart found: a segment whose
    // directory is under its deleting name on `device`, or one its owner of
    // segments marks expiring and that the restart found whole there and
    // checked against `accepted`. It is deleting from here, and the attempt
    // goes on from wherever the one before the restart stopped: it marks
    // what is not marked, releases what the restart pinned for the segment,
    // takes its files away and settles as any removal does. `accepted` is
    // the authorization its owner supplied again, and nothing when its
    // owner still lists the segment as retained: the mark on the device
    // decides. For a segment the table has no owner for; the owner it makes
    // leaves when the attempt ends, as for remove().
    [[nodiscard]] seastar::future<segment_removal> resume_removal(
      segment_context segment,
      device_store_id device,
      std::optional<deletion_authorization> accepted) {
        assert_current();
        segment_removal refused;
        refused.verdict = accepted ? authorization_verdict::authorized
                                   : authorization_verdict::repeated;
        if (closing_) {
            refused.failure.observe(detail::path_error(errc::closed));
            co_return refused;
        }
        if (accepted && accepted->segment != segment) {
            refused.verdict = authorization_verdict::wrong_context;
            co_return refused;
        }
        auto* found = find(segment);
        if (found && found->closing) {
            refused.failure.observe(detail::path_error(errc::closed));
            co_return refused;
        }
        if (found && found->life != segment_life::deleting) {
            refused.failure.observe(detail::path_error(errc::already_exists));
            co_return refused;
        }
        if (!found) {
            if (count(segment_life::deleting) >= config_.maximum_deleting) {
                refused.failure.observe(detail::path_error(errc::queue_full));
                co_return refused;
            }
            const auto* on = store(device);
            if (on) found = insert(segment, *on, segment_life::deleting);
            if (!found) {
                refused.failure.observe(
                  detail::path_error(
                    on ? errc::resource_exhausted : errc::wrong_context));
                co_return refused;
            }
            found->accepted = accepted;
            found->brought = true;
        }
        co_return co_await attempted(found, refused.verdict);
    }

    // Closes one segment's owner and takes it out of the table: its
    // readers drain and what is open of it closes, see drain(). Nothing is
    // removed from any device: a removal in flight stops where it stands,
    // with what it did on the device. A transition in flight is joined
    // first, and calls made meanwhile join this one. An attachment the append
    // owner will not let go keeps the owner, failed, with its files open: the
    // call says why, and may be made again.
    [[nodiscard]] seastar::future<runtime::result<void>>
    close_segment(segment_context context) {
        assert_current();
        auto* found = find(context);
        if (!found) co_return failed(errc::not_found);
        if (found->closing)
            co_return co_await found->closing->get_shared_future();
        found->closing.emplace();
        // Taken first: an owner with nothing open is closed and gone by
        // the time the attempt has been started.
        auto joined = found->closing->get_shared_future();
        if (auto begun = start_close(*found); !begun) co_return begun;
        co_return co_await std::move(joined);
    }

    // Refuses everything new, joins every attempt in flight and closes
    // every owner. Repeated calls join the first; one that could not close
    // an owner leaves the table open and may be made again.
    [[nodiscard]] seastar::future<runtime::result<void>> close() {
        assert_current();
        if (closed_) co_return runtime::result<void>{};
        if (ending_) co_return co_await ending_->get_shared_future();
        ending_.emplace();
        closing_ = true;
        runtime::first_failure kept;
        while (!owners_.empty()) {
            const auto before = owners_.size();
            const auto context = owners_.back()->context;
            try {
                kept.observe(co_await close_segment(context));
            } catch (...) {
                kept.observe(std::current_exception());
            }
            // An owner that could not go ends the attempt: the rest stay.
            if (owners_.size() == before && find(context)) break;
        }
        if (owners_.empty()) {
            // Nothing is left to tell of: the telling ends, and its sleep.
            untelling_ = true;
            stir();
            try {
                co_await tasks_.close();
            } catch (...) {
                kept.observe(std::current_exception());
            }
            closed_ = true;
        } else {
            closing_ = false;
        }
        auto ended = std::move(*ending_);
        ending_.reset();
        ended.set_value(detail::settled(kept));
        co_return detail::settled(kept);
    }

private:
    // One segment's owner.
    struct owner final {
        owner(
          segment_context named,
          const local_device_spec& store,
          segment_life begun) noexcept
          : context(named)
          , device(&store)
          , life(begun) {}
        owner(const owner&) = delete;
        owner& operator=(const owner&) = delete;

        segment_context context;
        const local_device_spec* device;
        segment_life life;
        // One transition at a time: whoever runs one holds the unit.
        seastar::semaphore turn{1};
        // Callers that are inside across a wait: its close waits for them.
        std::uint32_t inside{0};
        seastar::condition_variable left;
        // When a session was last opened on it, among the table's uses.
        std::uint64_t used{0};
        runtime::first_failure failure, debt;

        // While it is written: the index its writer feeds. Its writer's
        // observer refers to it, so it stays where it is until that writer
        // has closed. `keeps_fed`: its seal could not publish it, and it
        // waits, frozen, for the sealed form to adopt it.
        std::unique_ptr<active_sparse_index> feeding;
        bool keeps_fed{false};
        std::unique_ptr<writer_type> writer;
        std::optional<local_append_target> target;
        std::unique_ptr<retry_type> retries;
        // Its own share of the handle its writer opened for readers, from
        // the first reader on: a session opened while the segment seals
        // reads through a share of it, up to `sealing_bound`.
        std::optional<segment_data_pin> reading;
        std::optional<runtime::file_position> sealing_bound;

        // Once it is sealed: what its generation is opened against, and
        // that generation and its index while they are open.
        std::optional<segment_immutable_expectation> extent;
        std::optional<sealed_segment_expectation> expected;
        // What it is held to be is its caller's word, which the device has
        // not yet borne out: an owner made for a reader, until its
        // generation is open.
        bool unproved{false};
        std::unique_ptr<local_generation_owner> generation;
        std::optional<local_generation_pin> pin;
        std::unique_ptr<index_type> index;

        std::unique_ptr<segment_runs> runs;
        segment_sessions sessions;
        // Its seal found no place for it among the sealed: it closes when
        // that seal is done.
        bool surplus{false};
        std::optional<seastar::shared_promise<segment_sealed>> sealing;
        std::optional<seastar::shared_promise<runtime::result<void>>> closing;

        // What was told of it, and what waits for the table's turn to
        // tell; its place among those that wait.
        segment_roll_reasons told, untold;
        boost::intrusive::list_member_hook<> telling;
        // While its lifetime runs: when it ends, and its place among the
        // shard's deadlines.
        runtime::monotonic_time deadline;
        boost::intrusive::list_member_hook<> aging;
        // While it counts among its device's writable segments in the
        // device table: the whole bound it was admitted for.
        byte_count bound;
        bool placed{false};

        // Once it left the append owner: where the WAL ended then, which is
        // above every PREPARE of it.
        std::optional<local_wal_cursor> quiet;
        // While it is removed: the authorization it was accepted with, the
        // attempt in flight, and what this process did for its removal
        // that the device does not say. `brought`: it was made for a
        // removal of a segment the table had no owner for, holds nothing,
        // and leaves when the attempt ends.
        std::optional<deletion_authorization> accepted;
        std::optional<seastar::shared_promise<segment_removal>> removing;
        bool drained{false}, resolved{false}, brought{false};
    };
    // A segment that left the append owner, and where the WAL ended then.
    struct quiet_mark final {
        segment_context segment;
        local_wal_cursor below;
    };
    // One allocation holds them at the largest table.
    static_assert(
      (std::uint64_t{maximum_local_append_segments}
       + maximum_open_sealed_segments)
        * sizeof(quiet_mark)
      <= maximum_contiguous_allocation_bytes);
    using told_list = boost::intrusive::list<
      owner,
      boost::intrusive::member_hook<
        owner,
        boost::intrusive::list_member_hook<>,
        &owner::telling>>;
    using aged_list = boost::intrusive::list<
      owner,
      boost::intrusive::member_hook<
        owner,
        boost::intrusive::list_member_hook<>,
        &owner::aging>>;
    using sleep_fn = seastar::future<runtime::result<void>> (*)(
      void*, runtime::monotonic_time, seastar::abort_source&);
    // The longest one sleep on the timer, far below what a timer cannot
    // sleep: a lifetime that ends later is slept for again.
    static constexpr runtime::monotonic_duration maximum_nap{
      std::uint64_t{3'600} * 1'000'000'000};
    template<runtime::timer_service Timer>
    static seastar::future<runtime::result<void>> sleep_function(
      void* timer,
      runtime::monotonic_time deadline,
      seastar::abort_source& abort) {
        return static_cast<Timer*>(timer)->sleep_until(deadline, abort);
    }
    // A caller inside an owner across a wait.
    class stay final {
    public:
        explicit stay(owner& entered) noexcept
          : owner_(entered) {
            ++owner_.inside;
        }
        stay(const stay&) = delete;
        stay& operator=(const stay&) = delete;
        ~stay() {
            if (--owner_.inside == 0) owner_.left.broadcast();
        }

    private:
        owner& owner_;
    };

    // Not noexcept: the task scope, made last, allocates. If it cannot,
    // nothing made before it needs closing and make() answers the failure.
    segment_table(
      providers given,
      segment_table_config config,
      workload_reservation held,
      std::vector<std::unique_ptr<owner>> owners,
      std::vector<quiet_mark> quiet)
      : held_(std::move(held))
      , files_(given.files)
      , ownership_(given.ownership)
      , devices_(given.devices)
      , shard_(given.shard)
      , append_(given.append)
      , checkpoint_(given.checkpoint)
      , ids_(given.ids)
      , cache_(given.cache)
      , residency_(given.residency)
      , rebuilds_(given.rebuilds)
      , writing_(given.writing)
      , reading_(given.reading)
      , rebuilding_(given.rebuilding)
      , placement_(given.placement)
      , config_(config)
      , owners_(std::move(owners))
      , seals_(config.maximum_seals)
      , removals_(config.maximum_deletions)
      , quiet_(std::move(quiet)) {
        // No segment of this shard is attached yet: every PREPARE there is
        // lies below where the WAL ends now.
        if (const auto end = append_.wal_end())
            quiet_floor_ = *end;
        else
            quiet_lost_ = true;
    }

    [[nodiscard]] static auto failed(errc code) noexcept {
        return runtime::failure(detail::path_error(code));
    }

    [[nodiscard]] const local_device_spec*
    store(device_store_id device) const noexcept {
        for (const auto& spec : devices_)
            if (spec.owner.device() == device && spec.stores_data())
                return &spec;
        return nullptr;
    }
    [[nodiscard]] auto position(const segment_context& context) const noexcept {
        return std::lower_bound(
          owners_.begin(),
          owners_.end(),
          context,
          [](const auto& held, const segment_context& wanted) {
              return held->context.canonical_less(wanted);
          });
    }
    [[nodiscard]] owner* find(const segment_context& context) const noexcept {
        const auto at = position(context);
        return at != owners_.end() && (*at)->context == context ? at->get()
                                                                : nullptr;
    }
    [[nodiscard]] std::size_t count(segment_life life) const noexcept {
        return static_cast<std::size_t>(std::count_if(
          owners_.begin(), owners_.end(), [life](const auto& held) {
              return held->life == life;
          }));
    }
    // Owners of segments that are not sealed, or whose writer is still
    // open: those that hold a writer's handles, and those a restart found.
    // They are bounded together.
    [[nodiscard]] std::size_t unsealed() const noexcept {
        return static_cast<std::size_t>(
          std::count_if(owners_.begin(), owners_.end(), [](const auto& held) {
              return held->life == segment_life::creating
                     || held->life == segment_life::recovering || held->writer;
          }));
    }
    // Whether a segment that failed here may be sealed on its device: its
    // seal failed at or after the publication of its sealed pointer, which
    // may have become durable though the seal reported no evidence of it.
    // Nothing is taken to be published of a seal that failed before.
    [[nodiscard]] static bool maybe_sealed(const owner& failed) noexcept {
        return failed.writer
               && failed.writer->seal_progress().pointer.disposition
                    != local_publication_disposition::untouched;
    }
    // Whether what a caller holds a sealed segment to be is of `context`,
    // as far as it says so itself. The device says the rest.
    [[nodiscard]] static bool names(
      const sealed_segment_expectation& held,
      const segment_context& context) noexcept {
        return held.publication.segment == context
               && held.descriptor.segment == context
               && held.history.segment == context
               && held.publication.state == local_object_state::sealed
               && held.roots.size() == held.publication.roots.size();
    }
    // The table was reserved for as many owners as it may hold.
    [[nodiscard]] owner* insert(
      const segment_context& context,
      const local_device_spec& device,
      segment_life life) {
        if (owners_.size() == owners_.capacity()) return nullptr;
        try {
            auto made = std::make_unique<owner>(context, device, life);
            return owners_.insert(position(context), std::move(made))->get();
        } catch (const std::bad_alloc&) {
            return nullptr;
        }
    }
    void erase(owner& gone) noexcept {
        retire(gone);
        owners_.erase(position(gone.context));
    }

    // Something to tell of a segment that is appended to. It may be noted
    // inside a step of the segment's writer or of the append owner, so it
    // is kept for the table's own turn. What was told of the segment, or
    // waits already, is not noted again.
    bool keep(owner& of, segment_roll_reasons why) noexcept {
        if (!told_ || of.life != segment_life::writable || of.closing)
            return false;
        const auto fresh = why.without(of.told).without(of.untold);
        if (fresh.empty()) return false;
        of.untold.add(fresh);
        if (!of.telling.is_linked()) untold_.push_back(of);
        return true;
    }
    void note(owner& of, segment_roll_reasons why) noexcept {
        if (keep(of, why)) stir();
    }
    // A segment's first group was accepted: it takes its place among the
    // shard's deadlines. Only a deadline earlier than every other changes
    // what is slept on.
    void aged(owner& begun, runtime::monotonic_time deadline) noexcept {
        if (
          !told_ || begun.life != segment_life::writable || begun.closing
          || begun.aging.is_linked())
            return;
        begun.deadline = deadline;
        auto after = aging_.end();
        while (after != aging_.begin() && std::prev(after)->deadline > deadline)
            --after;
        const bool earliest = after == aging_.begin();
        aging_.insert(after, begun);
        if (earliest) stir();
    }
    // A segment that is appended to no more: nothing further is told of
    // it, and its lifetime is no one's to wait for. A sleep is not
    // shortened for it: that ends at its time and finds nothing due.
    void retire(owner& gone) noexcept {
        if (gone.aging.is_linked()) aging_.erase(aging_.iterator_to(gone));
        if (gone.telling.is_linked()) untold_.erase(untold_.iterator_to(gone));
        gone.untold = {};
        unplace(gone);
    }
    // A segment that is written no more leaves its device's count. One
    // that made nothing gives its share back.
    void unplace(owner& gone, bool unused = false) noexcept {
        if (!gone.placed || !placement_) return;
        placement_->unplaced(
          gone.device->owner.device(), unused ? gone.bound : byte_count{});
        gone.placed = false;
    }
    void stir() noexcept {
        stirred_ = true;
        wake_.broadcast();
    }

    // The table's one task that tells: what was noted, and the lifetimes
    // that end. It sleeps to the earliest deadline on one sleep, and waits
    // for nothing but a note while no lifetime runs.
    seastar::future<> tell_loop() {
        std::optional<seastar::abort_source> rearm;
        std::optional<seastar::future<>> nap;
        runtime::monotonic_time until;
        for (;;) {
            stirred_ = false;
            const auto now = Clock::now();
            while (!aging_.empty() && aging_.front().deadline <= now) {
                auto& due = aging_.front();
                aging_.pop_front();
                static_cast<void>(
                  keep(due, segment_roll_reasons{segment_roll_reason::age}));
            }
            while (!untold_.empty()) {
                auto& next = untold_.front();
                untold_.pop_front();
                const segment_roll_report report{next.context, next.untold};
                next.told.add(next.untold);
                next.untold = {};
                // Its receiver may call back and close this owner: nothing
                // of it is used from here on.
                told_(report);
            }
            const bool sleeps = !untelling_ && !unslept_.failed()
                                && !aging_.empty();
            if (
              nap && (napped_ || !sleeps || aging_.front().deadline < until)) {
                rearm->request_abort();
                co_await std::move(*nap);
                nap.reset();
                rearm.reset();
                continue;
            }
            if (untelling_) break;
            if (sleeps && !nap) {
                // No further than a timer sleeps: a lifetime that ends
                // later is slept for again.
                until = std::min(
                  aging_.front().deadline,
                  now.checked_add(maximum_nap)
                    .value_or(aging_.front().deadline));
                napped_ = false;
                rearm.emplace();
                try {
                    nap.emplace(nap_until(until, *rearm));
                } catch (...) {
                    // No frame for the sleep: as a sleep that failed.
                    rearm.reset();
                    unslept_.observe(std::current_exception());
                }
            }
            while (!stirred_)
                co_await wake_.when();
        }
    }
    seastar::future<>
    nap_until(runtime::monotonic_time deadline, seastar::abort_source& rearm) {
        runtime::result<void> slept{};
        try {
            slept = co_await sleep_(timer_, deadline, rearm);
        } catch (...) {
            slept = failed(errc::io_failure);
        }
        // A sleep that ended for no reason of the table's own: its timer
        // stopped or cannot sleep that long. No lifetime is told from here
        // on, and why is kept.
        if (!slept && !rearm.abort_requested()) unslept_.observe(slept.error());
        napped_ = true;
        stir();
    }
    [[nodiscard]] segment_runs_config runs_config() const {
        return segment_runs_config{
          config_.runs,
          config_.feed_bytes,
          config_.feed_linger_nanoseconds,
          []() noexcept { return Clock::now(); }};
    }

    // Starts the close of an owner whose `closing` its caller has made: an
    // attempt the table runs to its end. It may have run to that end when
    // this returns, and the owner be gone; an attempt that could not start
    // leaves the owner as it was.
    [[nodiscard]] runtime::result<void> start_close(owner& closed) {
        runtime::result<void> spawned;
        try {
            spawned = tasks_.spawn(
              [this, &closed] { return run_close(closed); });
        } catch (...) {
            spawned = runtime::failure(
              detail::path_error(errc::resource_exhausted));
        }
        if (!spawned) closed.closing.reset();
        return spawned;
    }
    // One step of a close: what it fails with is kept, and the next step
    // runs all the same.
    template<typename Step>
    static seastar::future<> joined(runtime::first_failure& kept, Step step) {
        try {
            kept.observe(co_await step());
        } catch (...) {
            kept.observe(std::current_exception());
        }
    }

    seastar::future<runtime::result<void>> create_owned(
      owner& made, segment_creation request, codec::cooperative_work& work) {
        const auto reserved = co_await ids_.allocate_object(work);
        if (!reserved) co_return runtime::failure(reserved.error());
        request.writer.retry_object = *reserved;
        try {
            auto fed = active_sparse_index::make(
              config_.stride,
              sparse_index_capacity(
                config_.stride,
                request.descriptor.maximum_data_bytes,
                request.writer.admission.maximum_blocks),
              writing_);
            if (!fed) co_return runtime::failure(fed.error());
            made.feeding = std::make_unique<active_sparse_index>(
              std::move(*fed));
        } catch (const std::bad_alloc&) {
            co_return failed(errc::resource_exhausted);
        }
        auto writer = writer_type::make_new(
          files_,
          ownership_,
          *made.device,
          shard_,
          request.descriptor,
          writing_,
          request.writer);
        if (!writer) co_return runtime::failure(writer.error());
        made.writer = std::move(*writer);
        // Before the first group, so that the lifetime's beginning is not
        // missed.
        if (told_) {
            auto watched = made.writer->observe_roll(
              {[this, &made](runtime::monotonic_time deadline) noexcept {
                   aged(made, deadline);
               },
               [this, &made](segment_roll_reasons why) noexcept {
                   note(made, why);
               }});
            if (!watched) co_return watched;
        }
        if (auto created = co_await made.writer->create_new(work); !created)
            co_return created;
        const auto boundary = made.writer->capture();
        if (!boundary) co_return runtime::failure(boundary.error());
        auto runs = segment_runs::make(
          cache_, reading_, boundary->history(), runs_config());
        if (!runs) co_return runtime::failure(runs.error());
        made.runs = std::move(*runs);
        // Before the first group, so that no block is missed.
        if (
          auto observed = made.writer->observe_blocks(
            made.runs->observer(*made.feeding));
          !observed)
            co_return observed;
        const auto target = append_.attach(*made.writer);
        if (!target) co_return runtime::failure(target.error());
        made.target = *target;
        auto retries = retry_type::make(
          files_,
          ownership_,
          devices_,
          shard_,
          append_,
          *made.target,
          *made.writer,
          ids_,
          writing_,
          config_.metadata,
          config_.policy);
        if (!retries) co_return runtime::failure(retries.error());
        made.retries = std::move(*retries);
        co_return runtime::result<void>{};
    }

    // The attempt the segment's owner runs: bounded on the shard, then one
    // transition at a time for the segment.
    seastar::future<> run_seal(owner& sealed) {
        const stay inside{sealed};
        segment_sealed outcome;
        bool attempted = false;
        try {
            const auto bound = co_await seastar::get_units(seals_, 1);
            {
                const auto turn = co_await seastar::get_units(sealed.turn, 1);
                // What it waited for may have closed or failed the segment.
                if (
                  closing_ || sealed.closing
                  || !segment_admits(sealed.life, segment_transition::seal)) {
                    outcome.failure = sealed.failure;
                    outcome.failure.observe(detail::path_error(errc::closed));
                } else {
                    attempted = true;
                    static_cast<segment_seal_outcome&>(
                      outcome) = co_await seal_owned(sealed);
                }
            }
            // The writer goes once the segment is sealed and its sessions
            // read elsewhere. A reader that comes meanwhile opens the
            // sealed form and does not wait for this.
            if (sealed.life == segment_life::sealed && sealed.writer) {
                co_await joined(
                  sealed.debt, [&sealed] { return sealed.writer->close(); });
                sealed.writer.reset();
                drop_fed(sealed);
            }
        } catch (...) {
            outcome.failure.observe(std::current_exception());
            sealed.failure.observe(std::current_exception());
            if (sealed.life == segment_life::sealing)
                sealed.life = segment_life::failed;
        }
        // What the sealed segment is opened against goes with the answer:
        // the owner may be let go before whoever asked comes for it.
        if (sealed.life == segment_life::sealed && sealed.expected) {
            try {
                outcome.expected
                  = seastar::make_lw_shared<const sealed_segment_expectation>(
                    *sealed.expected);
            } catch (const std::bad_alloc&) {
                sealed.debt.observe(
                  detail::path_error(errc::resource_exhausted));
            }
        }
        if (attempted) {
            sealed.sealing->set_value(std::move(outcome));
        } else {
            // Nothing was tried: the next request is a new attempt.
            auto ended = std::move(*sealed.sealing);
            sealed.sealing.reset();
            ended.set_value(std::move(outcome));
        }
        // Its close waits for this attempt to leave, so the owner is here
        // until this returns.
        if (sealed.surplus && !sealed.closing) {
            sealed.closing.emplace();
            sealed.debt.observe(start_close(sealed));
        }
    }

    seastar::future<segment_seal_outcome> seal_owned(owner& sealed) {
        segment_seal_outcome outcome;
        const auto fail = [&](const runtime::operation_error& why) {
            outcome.failure.observe(why);
            sealed.failure.observe(why);
            sealed.life = segment_life::failed;
        };
        sealed.life = segment_life::sealing;
        retire(sealed);
        seastar::abort_source abort;
        codec::cooperative_work work{config_.policy, abort};
        // Its cuts end: one of them holds the attachment.
        if (sealed.retries)
            sealed.debt.observe(co_await sealed.retries->close());
        // The sequence its index bundle is published under. Without one the
        // segment is sealed all the same, and its index is owed.
        std::optional<local_object_sequence> object;
        if (auto reserved = co_await ids_.allocate_object(work))
            object = *reserved;
        // Admission closes and what was accepted drains, then it detaches.
        if (auto quiet = co_await append_.quiesce(*sealed.target); !quiet) {
            fail(quiet.error());
            co_return outcome;
        }
        auto detached = append_.detach(*sealed.target);
        if (!detached) {
            fail(detached.error());
            co_return outcome;
        }
        sealed.target.reset();
        sealed.quiet = quieted(sealed.context);
        sealed.retries.reset();
        const auto& progress = sealed.writer->progress();
        const auto completed = detached->retry.completed();
        if (!progress || completed > progress->reserved.retry_entries) {
            fail(detail::path_error(errc::wrong_context));
            co_return outcome;
        }
        // Everything accepted is durable: a session opened while the
        // writer seals reads up to here.
        sealed.sealing_bound = progress->durable.bytes;
        const auto unresolved = progress->reserved.retry_entries - completed;
        if (object)
            outcome = co_await sealed.writer->seal(
              std::move(detached->retry),
              completed,
              unresolved,
              work,
              sparse_index_seal{sealed.feeding.get(), *object});
        else
            outcome = co_await sealed.writer->seal(
              std::move(detached->retry), completed, unresolved, work);
        if (outcome.failure.failed() || !outcome.extent) {
            // Failed here whatever its device holds: a seal that failed
            // after it began to publish its sealed pointer may have left
            // the segment sealed there, see maybe_sealed().
            if (!outcome.failure.failed())
                outcome.failure.observe(detail::path_error(errc::io_failure));
            sealed.failure = outcome.failure;
            sealed.life = segment_life::failed;
            co_return outcome;
        }
        // The publication takes over from the checkpoint's entries and the
        // pinned PREPAREs, and is what the segment is opened against.
        bool known = false;
        try {
            if (const auto published = sealed.writer->published()) {
                if (checkpoint_)
                    sealed.debt.observe(
                      checkpoint_->cover(published->publication));
                // Nothing pinned is nothing to release.
                if (
                  const auto released = append_.discharge_pinned(
                    published->publication);
                  !released && released.error().code() != errc::not_found)
                    sealed.debt.observe(released.error());
                sealed.extent = *outcome.extent;
                sealed.expected.emplace(
                  sealed_segment_expectation{
                    sealed.device->owner.device(),
                    published->generation,
                    published->publication,
                    published->descriptor,
                    std::vector<local_bundle_context>(
                      published->roots.begin(), published->roots.end()),
                    sealed.runs->history(),
                    *outcome.extent});
                known = true;
            }
        } catch (const std::bad_alloc&) {
        }
        if (!known)
            sealed.debt.observe(detail::path_error(errc::resource_exhausted));
        // Its place among the sealed, and its sealed form when it has
        // readers to move.
        // From here on the segment is sealed whatever else fails: what
        // cannot be done for its readers ends them, and is not the seal's
        // failure.
        bool room = false;
        bool opened = false;
        if (known) {
            try {
                room = co_await make_room();
            } catch (...) {
                sealed.debt.observe(std::current_exception());
            }
            // Its place, kept until this owner is among the sealed itself.
            const auto arrived = seastar::defer([this, room] noexcept {
                if (room) --arriving_;
            });
            if (room && sealed.sessions.size() != 0)
                opened = (co_await open_sealed(sealed, work)).has_value();
            // One step from here to the sealed state: nothing is waited
            // for. An index the seal could not publish answers from memory,
            // frozen: for the sealed form now, or for the one a reader
            // opens later.
            const bool owed = !outcome.index && !sealed.feeding->empty();
            if (owed && !sealed.feeding->frozen()) {
                sealed.feeding->durable(outcome.extent->coverage.bytes().end());
                sealed.feeding->freeze();
            }
            sealed.keeps_fed = owed;
            std::optional<segment_data_source> through;
            if (opened) {
                adopt_fed(sealed);
                if (auto shared = sealed.pin->share())
                    through.emplace(std::move(*shared));
            }
            if (through)
                sealed.sessions.move(*through, [&sealed] {
                    return sealed_read_index(*sealed.index);
                });
            else
                sealed.sessions.revoke();
        } else {
            sealed.sessions.revoke();
        }
        sealed.reading.reset();
        sealed.sealing_bound.reset();
        sealed.life = segment_life::sealed;
        // Among the sealed it is as new as one a reader just came for.
        sealed.used = ++uses_;
        sealed.runs->seal();
        // With no place among the sealed its owner goes once its writer
        // has: nothing of it is open then but its runs.
        sealed.surplus = !room;
        co_return outcome;
    }

    // The frozen index a segment's writer fed answers for a sealed form
    // whose publication names none, so no walk of the data is owed. An
    // index that cannot be taken is owed as any other is. What is left of
    // it is empty, and stays where it is while its writer lives.
    void adopt_fed(owner& sealed) {
        if (!sealed.keeps_fed || !sealed.index) return;
        static_cast<void>(sealed.index->adopt(std::move(*sealed.feeding)));
        sealed.keeps_fed = false;
        drop_fed(sealed);
    }
    void drop_fed(owner& sealed) noexcept {
        if (!sealed.writer && !sealed.keeps_fed) sealed.feeding.reset();
    }

    // Makes room for one more sealed owner and takes the place: while they
    // are as many as may be, the one nothing uses that was used longest ago
    // is closed. Whoever is given the place counts in `arriving_` until its
    // owner is among the sealed, or it gives the place up. False when every
    // one of them is in use, or the one chosen cannot be closed now.
    seastar::future<bool> make_room() {
        for (;;) {
            if (
              count(segment_life::sealed) + arriving_
              < config_.maximum_open_sealed) {
                ++arriving_;
                co_return true;
            }
            owner* victim = nullptr;
            for (const auto& held : owners_)
                if (
                  held->life == segment_life::sealed && !held->closing
                  && held->inside == 0 && held->sessions.size() == 0
                  && (!victim || held->used < victim->used))
                    victim = held.get();
            if (!victim) co_return false;
            const auto context = victim->context;
            static_cast<void>(co_await close_segment(context));
            if (const auto* still = find(context); still && !still->closing)
                co_return false;
        }
    }

    // Opens a sealed owner's generation and index against what its owner
    // of segments, or its own seal, holds it to be.
    seastar::future<runtime::result<void>>
    open_sealed(owner& sealed, codec::cooperative_work& work) {
        if (!sealed.expected) co_return failed(errc::wrong_context);
        const auto& expected = *sealed.expected;
        const local_generation_expectation against{
          expected.generation,
          expected.publication,
          expected.descriptor,
          expected.roots};
        runtime::result<std::unique_ptr<local_generation_owner>> generation
          = failed(errc::io_failure);
        try {
            generation = co_await local_generation_owner::open(
              files_,
              ownership_,
              *sealed.device,
              shard_,
              against,
              reading_,
              config_.metadata,
              work,
              config_.readers);
        } catch (const std::bad_alloc&) {
            generation = failed(errc::resource_exhausted);
        } catch (...) {
        }
        if (!generation) co_return runtime::failure(generation.error());
        runtime::result<void> made{};
        std::optional<local_generation_pin> pin;
        std::unique_ptr<index_type> index;
        std::unique_ptr<segment_runs> runs;
        try {
            auto pinned = (*generation)->pin();
            const auto context = sparse_index_context::make(
              expected.descriptor.segment,
              expected.extent.coverage,
              expected.extent.digest,
              expected.descriptor.alignment);
            // The publication pins the extent its owner expects.
            if (!pinned) {
                made = runtime::failure(pinned.error());
            } else if (!context || !pinned_as(*pinned, expected)) {
                made = failed(errc::wrong_context);
            } else {
                std::optional<local_root_reference> named;
                for (const auto& root : expected.publication.roots)
                    if (root.kind() == local_root_kind::index) named = root;
                auto opened = index_type::make(
                  files_,
                  ownership_,
                  *sealed.device,
                  shard_,
                  expected.history,
                  *context,
                  config_.stride,
                  named,
                  pinned->index_rebuild_reason(),
                  residency_,
                  rebuilds_,
                  reading_,
                  rebuilding_,
                  config_.metadata,
                  config_.scan,
                  config_.policy,
                  &cache_);
                if (!opened) {
                    made = runtime::failure(opened.error());
                } else {
                    index = std::move(*opened);
                    pin.emplace(std::move(*pinned));
                }
            }
            if (made && !sealed.runs) {
                auto cached = segment_runs::make(
                  cache_, reading_, expected.history, runs_config());
                if (cached)
                    runs = std::move(*cached);
                else
                    made = runtime::failure(cached.error());
            }
        } catch (const std::bad_alloc&) {
            made = failed(errc::resource_exhausted);
        }
        if (!made) {
            runtime::first_failure unkept;
            pin.reset();
            if (index)
                co_await joined(unkept, [&index] { return index->close(); });
            co_await joined(
              unkept, [&generation] { return (*generation)->close(); });
            co_return made;
        }
        if (runs) sealed.runs = std::move(runs);
        sealed.generation = std::move(*generation);
        sealed.pin.emplace(std::move(*pin));
        sealed.index = std::move(index);
        sealed.unproved = false;
        co_return made;
    }

    seastar::future<runtime::result<segment_reader>> open_owned(
      owner& read,
      segment_session_request request,
      workload_budget& reader,
      codec::cooperative_work& work) {
        using output = runtime::result<segment_reader>;
        if (read.life != segment_life::sealed) {
            std::optional<runtime::operation_error> refused;
            {
                auto view = co_await read.writer->pin_data(work);
                if (read.closing || !segment_admits_readers(read.life))
                    co_return output{failed(errc::closed)};
                if (!view) refused = view.error();
                // A seal that moved its sessions while the writer was
                // asked leaves this pin unused: the session is opened on
                // the sealed form, and the writer's close is not held.
                if (view && read.life != segment_life::sealed) {
                    if (!read.reading) read.reading.emplace(view->pin.share());
                    co_return segment_reader::open(
                      *read.runs,
                      segment_data_source{std::move(view->pin)},
                      {.durable = view->durable,
                       .sealed = std::nullopt,
                       .visible = request.visible,
                       .short_of_visibility = request.short_of_visibility},
                      active_read_index(*read.feeding, view->durable),
                      reader,
                      request.config,
                      &read.sessions);
                }
            }
            if (read.life == segment_life::sealing) {
                // Its writer is sealing. With a reader's handle in hand
                // the session reads what was durable when the seal began.
                if (
                  refused && refused->code() == errc::closed && read.reading
                  && read.sealing_bound && read.feeding)
                    co_return segment_reader::open(
                      *read.runs,
                      segment_data_source{read.reading->share()},
                      {.durable = *read.sealing_bound,
                       .sealed = std::nullopt,
                       .visible = request.visible,
                       .short_of_visibility = request.short_of_visibility},
                      active_read_index(*read.feeding, *read.sealing_bound),
                      reader,
                      request.config,
                      &read.sessions);
                co_return output{failed(errc::queue_full)};
            }
            // Sealed while its writer was asked: as a sealed segment.
            if (read.life != segment_life::sealed)
                co_return output{runtime::failure(
                  refused.value_or(detail::path_error(errc::closed)))};
        }
        if (!read.pin) {
            const auto turn = co_await seastar::get_units(read.turn, 1);
            if (read.closing || read.life != segment_life::sealed)
                co_return output{failed(errc::closed)};
            if (!read.pin) {
                if (auto opened = co_await open_sealed(read, work); !opened)
                    co_return output{runtime::failure(opened.error())};
                // The index its own seal fed and could not publish.
                adopt_fed(read);
            }
        }
        auto shared = read.pin->share();
        if (!shared) co_return output{runtime::failure(shared.error())};
        co_return segment_reader::open(
          *read.runs,
          segment_data_source{std::move(*shared)},
          {.durable = read.extent->coverage.bytes().end(),
           .sealed = read.extent->coverage.logical().end(),
           .visible = request.visible,
           .short_of_visibility = request.short_of_visibility},
          sealed_read_index(*read.index),
          reader,
          request.config,
          &read.sessions);
    }

    // A segment has just left the append owner, which writes no further
    // PREPARE for it: every one of its own lies below where the WAL ends
    // now. Remembered for as many segments as the table opens; the oldest
    // gives way, and where the WAL ended for it is the floor of every
    // segment that is not remembered. Every segment of the shard is
    // attached and detached through this table, or was there before it.
    std::optional<local_wal_cursor>
    quieted(const segment_context& context) noexcept {
        const auto end = append_.wal_end();
        if (!end) {
            // Nothing says where: no segment's place is known from here on.
            quiet_lost_ = true;
            return std::nullopt;
        }
        if (quiet_.size() == quiet_.capacity()) {
            quiet_floor_ = quiet_.front().below;
            quiet_.erase(quiet_.begin());
        }
        quiet_.push_back({context, *end});
        return *end;
    }
    // Under which WAL position every PREPARE of a segment lies that is
    // attached to nothing, as far as the table knows.
    [[nodiscard]] std::optional<local_wal_cursor>
    quiet_below(const segment_context& context) const noexcept {
        if (quiet_lost_) return std::nullopt;
        for (auto at = quiet_.size(); at-- != 0;)
            if (quiet_[at].segment == context) return quiet_[at].below;
        return quiet_floor_;
    }

    // Ends a segment's readers and closes what is open of it, for its close
    // and before it is removed. Its sessions were let in no more from the
    // step before; here every one of them is ended where it stands: an idle
    // one lets go at once, one in a read when that read returns, and its
    // next read is refused as closed. What a session returned stays its
    // holder's.
    //
    // Then, in an order in which nothing waits for what closes after it:
    // its cuts end and its attachment drains and detaches; its index
    // closes, which ends a rebuild and every lookup that waits for one, so
    // the read that asked returns; its generation closes, which waits for
    // the pins those reads held; its runs close, which waits for every
    // session and for the loads that were asked for; its writer closes. So
    // the wait is bounded by the reads in flight, each one read of the
    // device. No step is skipped for the failure of one before it: what
    // failed is kept in `kept`, the first of it, and the drain goes on.
    //
    // Refused, with what is open of the segment left open, when the append
    // owner will not let its attachment go: that owner still holds its
    // writer.
    seastar::future<runtime::result<void>>
    drain(owner& closed, runtime::first_failure& kept) {
        std::optional<runtime::operation_error> held;
        try {
            closed.sessions.revoke();
            if (closed.retries)
                co_await joined(
                  kept, [&closed] { return closed.retries->close(); });
            if (closed.target) {
                // Nothing accepted is abandoned: the attachment drains,
                // then lets go. An append owner that closed first has
                // drained it already and holds nothing of it.
                if (co_await append_.quiesce(*closed.target)) {
                    if (auto gone = append_.detach(*closed.target); !gone) {
                        kept.observe(gone.error());
                        held = gone.error();
                    } else {
                        closed.quiet = quieted(closed.context);
                    }
                } else {
                    // Let go without a detach: nothing says where the WAL
                    // ended for it, nor for any segment after it.
                    quiet_lost_ = true;
                }
                if (!held) closed.target.reset();
            }
        } catch (...) {
            kept.observe(std::current_exception());
        }
        if (held) co_return runtime::failure(*held);
        closed.retries.reset();
        if (closed.index)
            co_await joined(kept, [&closed] { return closed.index->close(); });
        closed.pin.reset();
        if (closed.generation)
            co_await joined(
              kept, [&closed] { return closed.generation->close(); });
        if (closed.runs)
            co_await joined(kept, [&closed] { return closed.runs->close(); });
        closed.reading.reset();
        if (closed.writer) {
            // A failed segment's writer reports its failure again.
            runtime::first_failure again;
            co_await joined(
              closed.life == segment_life::failed ? again : kept,
              [&closed] { return closed.writer->close(); });
            closed.writer.reset();
        }
        closed.feeding.reset();
        closed.index.reset();
        closed.generation.reset();
        closed.runs.reset();
        co_return runtime::result<void>{};
    }

    // Where a segment's directory is under either name, and the directory
    // that holds it.
    struct removal_paths final {
        runtime::file_path above, live, deleting;
    };
    [[nodiscard]] runtime::result<removal_paths>
    where(const owner& removed) const {
        const auto paths = local_paths::make(removed.device->root);
        if (!paths) return runtime::failure(paths.error());
        const local_segment_name name{
          removed.context.segment(), removed.context.generation()};
        auto above = paths->segment_bucket(shard_, name.segment);
        if (!above) return runtime::failure(above.error());
        auto live = paths->segment(shard_, name);
        if (!live) return runtime::failure(live.error());
        auto deleting = paths->deleting(shard_, name);
        if (!deleting) return runtime::failure(deleting.error());
        return removal_paths{
          std::move(*above), std::move(*live), std::move(*deleting)};
    }
    // Whether its close, or the table's, asks a removal to stop where it
    // stands. What it did is on the device, and a restart goes on from it.
    [[nodiscard]] bool stopped(const owner& removed) const noexcept {
        return closing_ || removed.closing.has_value();
    }
    // Whether a restart of this shard would still look for a segment that
    // is being removed: see local_checkpoint::names(). A shard with no
    // checkpoint owner has no durable checkpoint to ask, and a removal on
    // it does not wait.
    [[nodiscard]] bool named(const owner& removed) const {
        // What was not covered or released may be in a checkpoint still.
        return checkpoint_
               && (!removed.resolved || named_below(removed.context, removed.quiet));
    }
    // The same of any segment that is attached to nothing, every PREPARE of
    // which lies under `below` in the WAL. One whose PREPAREs nothing
    // places is taken to be named.
    [[nodiscard]] bool named_below(
      const segment_context& context,
      const std::optional<local_wal_cursor>& below) const {
        if (!checkpoint_) return false;
        if (!below) return true;
        const auto still = checkpoint_->names(context, *below);
        return !still || *still;
    }
    // A directory sync of a removal on this owner's device failed in this
    // process, with what: nothing more is done to a removal's directories
    // there.
    [[nodiscard]] std::optional<runtime::operation_error>&
    unsynced(const owner& on) noexcept {
        return unsynced_[static_cast<std::size_t>(on.device - devices_.data())];
    }
    // Closes a failed segment's admission in the append owner: what it had
    // accepted settles as it would, and nothing more is accepted for it.
    // Its close or its removal detaches it.
    void hush(owner& quieted) noexcept {
        if (!quieted.target) return;
        try {
            static_cast<void>(tasks_.spawn(
              [this, target = *quieted.target] { return hushed(target); }));
        } catch (...) {
        }
    }
    seastar::future<> hushed(local_append_target target) {
        try {
            static_cast<void>(co_await append_.quiesce(target));
        } catch (...) {
        }
    }
    // Whether the sealed root a generation pinned is the extent, and the
    // history, its owner of segments holds the segment to be: what the
    // root says of itself, never a walk of the extent.
    [[nodiscard]] static bool pinned_as(
      const local_generation_pin& pinned,
      const sealed_segment_expectation& expected) {
        const auto& boundary = pinned.boundary();
        const auto* root = boundary ? std::get_if<sealed_footer>(&*boundary)
                                    : nullptr;
        return root && root->coverage() == expected.extent.coverage
               && root->extent_digest() == expected.extent.digest
               && root->encoded_extent().end() == expected.extent.file_end
               && pinned.data_end()
                    == std::optional{expected.extent.coverage.bytes().end()};
    }
    // Holds what a caller says a sealed segment is against the device: its
    // generation opens as the caller's publication, descriptor and roots
    // say, and the sealed root it pins is the caller's extent. Reads its
    // publication and its root; nothing stays open.
    seastar::future<runtime::result<void>> prove_sealed(
      const local_device_spec& device,
      const sealed_segment_expectation& expected,
      codec::cooperative_work& work) {
        const local_generation_expectation against{
          expected.generation,
          expected.publication,
          expected.descriptor,
          expected.roots};
        runtime::result<std::unique_ptr<local_generation_owner>> generation
          = failed(errc::io_failure);
        try {
            generation = co_await local_generation_owner::open(
              files_,
              ownership_,
              device,
              shard_,
              against,
              reading_,
              config_.metadata,
              work,
              config_.readers);
        } catch (const std::bad_alloc&) {
            generation = failed(errc::resource_exhausted);
        } catch (...) {
        }
        if (!generation) co_return runtime::failure(generation.error());
        runtime::result<void> proved{};
        try {
            const auto pinned = (*generation)->pin();
            if (!pinned)
                proved = runtime::failure(pinned.error());
            else if (!pinned_as(*pinned, expected))
                proved = failed(errc::wrong_context);
        } catch (const std::bad_alloc&) {
            proved = failed(errc::resource_exhausted);
        }
        runtime::first_failure unkept;
        co_await joined(
          unkept, [&generation] { return (*generation)->close(); });
        co_return proved;
    }
    // The removal of a segment the table has no owner for: see remove().
    seastar::future<segment_removal> remove_unowned(
      deletion_authorization authorization,
      std::optional<sealed_segment_expectation> held) {
        segment_removal refused;
        const auto segment = authorization.segment;
        const auto* claimed = held && names(*held, segment) ? &*held : nullptr;
        seastar::abort_source abort;
        codec::cooperative_work work{config_.policy, abort};
        runtime::result<std::optional<segment_directory>> place = failed(
          errc::io_failure);
        try {
            place = co_await find_segment_directory(
              files_, ownership_, devices_, shard_, segment, work);
        } catch (const std::bad_alloc&) {
            place = failed(errc::resource_exhausted);
        } catch (...) {
        }
        if (!place) {
            refused.failure.observe(place.error());
            co_return refused;
        }
        const local_device_spec* on = nullptr;
        auto verdict = authorization_verdict::repeated;
        if (*place && !(*place)->marked) {
            // Whole under its own name: only as the sealed segment its
            // caller holds it to be, where it is, and as the device says.
            refused.verdict = check_deletion(authorization, claimed);
            if (refused.verdict != authorization_verdict::authorized)
                co_return refused;
            on = store((*place)->device);
            if (!on || claimed->device != (*place)->device) {
                refused.verdict = authorization_verdict::wrong_identity;
                co_return refused;
            }
            if (
              const auto proved = co_await prove_sealed(*on, *claimed, work);
              !proved) {
                if (proved.error().code() == errc::wrong_context)
                    refused.verdict = authorization_verdict::wrong_identity;
                else
                    refused.failure.observe(proved.error());
                co_return refused;
            }
            verdict = authorization_verdict::authorized;
        } else if (*place) {
            on = store((*place)->device);
        } else {
            // On no device: any one of them shows that, and where the
            // removal looks is of no account.
            for (const auto& spec : devices_)
                if (!on && spec.stores_data()) on = &spec;
        }
        refused.verdict = verdict;
        // After the waits: the table may be closing, and another caller
        // may have brought the segment.
        if (closing_) {
            refused.failure.observe(detail::path_error(errc::closed));
            co_return refused;
        }
        if (find(segment))
            co_return co_await remove(std::move(authorization), claimed);
        if (!on) {
            refused.failure.observe(detail::path_error(errc::wrong_context));
            co_return refused;
        }
        if (count(segment_life::deleting) >= config_.maximum_deleting) {
            refused.failure.observe(detail::path_error(errc::queue_full));
            co_return refused;
        }
        auto* made = insert(segment, *on, segment_life::deleting);
        if (!made) {
            refused.failure.observe(
              detail::path_error(errc::resource_exhausted));
            co_return refused;
        }
        if (claimed) made->extent = claimed->extent;
        made->accepted = std::move(authorization);
        made->brought = true;
        co_return co_await attempted(made, verdict);
    }
    // Whether `name` is one a segment's directory holds of the segment: one
    // of its three files, or what an interrupted publication of one left.
    [[nodiscard]] static bool own_file(std::string_view name) {
        for (const std::string_view file :
             {"descriptor", "data", "published"}) {
            if (name == file) return true;
            const auto target = runtime::file_name::make(file);
            if (target && parse_local_temporary_name(name, *target))
                return true;
        }
        return false;
    }
    // The same of the directory of its bundles.
    [[nodiscard]] static bool own_object(std::string_view name) {
        if (parse_local_sequence_name(name, false)) return true;
        if (name.size() <= 24) return false;
        const auto target = runtime::file_name::make(
          name.substr(0, name.size() - 24));
        return target && parse_local_temporary_name(name, *target)
               && parse_local_sequence_name(target->value(), false);
    }

    // Gives the segment's directory its deleting name and makes that name
    // durable: one rename, which needs no new file, and one sync of the
    // directory above, before anything inside is touched. A crash before
    // the sync leaves the directory under one name or the other, whole.
    // Says whether a directory is there under the deleting name: with none
    // under either the segment is gone already. Asked again by every
    // attempt: what the device says is what is so, and a directory an
    // attempt before this one removed is not looked for inside.
    seastar::future<runtime::result<bool>> mark(
      const owner& removed,
      const removal_paths& at,
      codec::cooperative_work& work) {
        using output = runtime::result<bool>;
        if (
          const auto valid = co_await ownership_.validate(*removed.device);
          !valid)
            co_return output{runtime::failure(valid.error())};
        if (
          const auto inspected = co_await inspect_local_path(
            files_,
            removed.device->root,
            at.above,
            runtime::file_kind::directory,
            work,
            true);
          !inspected)
            co_return output{runtime::failure(inspected.error())};
        auto opened = co_await files_.open_directory(
          at.above, runtime::file_close_policy::checked);
        if (!opened) {
            if (opened.error().code() == errc::not_found)
                co_return output{false};
            co_return output{runtime::failure(opened.error())};
        }
        auto above = std::move(*opened);
        runtime::first_failure failed;
        bool present = true;
        try {
            const auto renamed = co_await files_.rename(
              at.live, at.deleting, runtime::file_rename_policy::no_replace);
            if (!renamed && renamed.error().code() != errc::not_found) {
                failed.observe(renamed);
            } else if (!renamed) {
                // Nothing under its own name: an attempt before this one
                // marked it, or it is gone.
                const auto status = co_await files_.stat(at.deleting);
                if (status) {
                    if (status->kind != runtime::file_kind::directory)
                        failed.observe(detail::path_error(errc::wrong_context));
                } else if (status.error().code() == errc::not_found) {
                    present = false;
                } else {
                    failed.observe(status);
                }
            }
            if (!failed.failed()) {
                const auto synced = co_await above.sync();
                if (!synced) {
                    unsynced(removed) = synced.error();
                    failed.observe(synced);
                }
            }
        } catch (...) {
            failed.observe(std::current_exception());
        }
        try {
            failed.observe(co_await above.close());
        } catch (...) {
            failed.observe(std::current_exception());
        }
        if (const auto done = detail::settled(failed); !done)
            co_return output{runtime::failure(done.error())};
        co_return output{present};
    }

    // Removes from `directory`, which is a segment's deleting directory or
    // lies in one, every file `own` says is the segment's: one call at a
    // time, in the order they are listed, with no sync between. A name
    // that is none of them is left and counted. A directory that is not
    // there holds nothing, and a file already gone counts as removed:
    // under a deleting name nothing else explains either.
    template<typename Own>
    seastar::future<runtime::result<void>> sweep(
      const owner& removed,
      const runtime::file_path& directory,
      bool whole,
      Own own,
      segment_removal& outcome,
      codec::cooperative_work& work) {
        if (
          const auto inspected = co_await inspect_local_path(
            files_,
            removed.device->root,
            directory,
            runtime::file_kind::directory,
            work,
            true);
          !inspected)
            co_return inspected;
        auto opened = co_await files_.open_directory(
          directory, runtime::file_close_policy::checked);
        if (!opened) {
            if (opened.error().code() == errc::not_found)
                co_return runtime::result<void>{};
            co_return runtime::failure(opened.error());
        }
        auto names = std::move(*opened);
        runtime::first_failure failed;
        try {
            bool end = false;
            while (!end && !failed.failed()) {
                if (stopped(removed)) {
                    failed.observe(detail::path_error(errc::closed));
                    break;
                }
                if (
                  const auto ready = co_await detail::path_checkpoint(work);
                  !ready) {
                    failed.observe(ready);
                    break;
                }
                auto page = co_await names.next(
                  {.maximum_entries = item_count{16},
                   .maximum_name_bytes = byte_count{4_KiB}});
                if (!page) {
                    failed.observe(page);
                    break;
                }
                end = page->end();
                for (const auto& entry : page->entries()) {
                    const auto& name = entry.name.value();
                    // The directory of its bundles is swept by itself.
                    if (
                      whole && entry.kind == runtime::file_kind::directory
                      && name == "objects")
                        continue;
                    if (
                      entry.kind != runtime::file_kind::regular || !own(name)) {
                        ++outcome.foreign;
                        continue;
                    }
                    const auto path = local_child_path(directory, entry.name);
                    if (!path) {
                        failed.observe(path);
                        break;
                    }
                    const auto gone = co_await files_.remove_file(*path);
                    if (!gone && gone.error().code() != errc::not_found) {
                        failed.observe(gone);
                        break;
                    }
                    ++outcome.removed;
                }
            }
        } catch (...) {
            failed.observe(std::current_exception());
        }
        try {
            failed.observe(co_await names.close());
        } catch (...) {
            failed.observe(std::current_exception());
        }
        co_return detail::settled(failed);
    }

    // Takes a deleting directory's files away: its bundles and their
    // directory first, then its own three and what interrupted
    // publications left.
    seastar::future<runtime::result<void>> empty_directory(
      const owner& removed,
      const removal_paths& at,
      segment_removal& outcome,
      codec::cooperative_work& work) {
        if (
          const auto valid = co_await ownership_.validate(*removed.device);
          !valid)
            co_return valid;
        const auto objects = local_child_path(
          at.deleting, runtime::file_name::make("objects").value());
        if (!objects) co_return runtime::failure(objects.error());
        if (
          const auto swept = co_await sweep(
            removed,
            *objects,
            false,
            [](std::string_view name) { return own_object(name); },
            outcome,
            work);
          !swept)
            co_return swept;
        // Its bundles' directory goes when nothing else is left in it.
        if (outcome.foreign == 0) {
            const auto gone = co_await files_.remove_directory(*objects);
            if (!gone && gone.error().code() != errc::not_found) co_return gone;
        }
        co_return co_await sweep(
          removed,
          at.deleting,
          true,
          [](std::string_view name) { return own_file(name); },
          outcome,
          work);
    }

    // The directory goes last, and the directory above is synced: one sync
    // for the removals it covers, after which none of the segment's names
    // comes back. A directory that still holds a name is left with it.
    seastar::future<runtime::result<void>> remove_directory(
      const owner& removed,
      const removal_paths& at,
      codec::cooperative_work& work) {
        if (
          const auto inspected = co_await inspect_local_path(
            files_,
            removed.device->root,
            at.above,
            runtime::file_kind::directory,
            work,
            true);
          !inspected)
            co_return inspected;
        auto opened = co_await files_.open_directory(
          at.above, runtime::file_close_policy::checked);
        if (!opened) {
            if (opened.error().code() == errc::not_found)
                co_return runtime::result<void>{};
            co_return runtime::failure(opened.error());
        }
        auto above = std::move(*opened);
        runtime::first_failure failed;
        try {
            const auto gone = co_await files_.remove_directory(at.deleting);
            if (!gone && gone.error().code() != errc::not_found)
                failed.observe(gone);
            if (!failed.failed()) {
                const auto synced = co_await above.sync();
                if (!synced) {
                    unsynced(removed) = synced.error();
                    failed.observe(synced);
                }
            }
        } catch (...) {
            failed.observe(std::current_exception());
        }
        try {
            failed.observe(co_await above.close());
        } catch (...) {
            failed.observe(std::current_exception());
        }
        co_return detail::settled(failed);
    }

    // The mark stands where a sealed publication would: it takes over from
    // the checkpoint's entries and the pinned PREPAREs of the segment. What
    // could not be released is owed, and asked for again by the next
    // attempt.
    void resolve(owner& removed) {
        if (removed.resolved) return;
        const local_object_publication stands{
          removed.context, local_object_state::deleting, std::nullopt, {}};
        bool done = true;
        if (checkpoint_)
            if (auto covered = checkpoint_->cover(stands); !covered) {
                removed.debt.observe(covered.error());
                done = false;
            }
        // Nothing pinned is nothing to release.
        if (
          const auto released = append_.discharge_pinned(stands);
          !released && released.error().code() != errc::not_found) {
            removed.debt.observe(released.error());
            done = false;
        }
        if (!removed.quiet) removed.quiet = quiet_below(removed.context);
        removed.resolved = done;
    }

    // One attempt at an accepted removal, from where the device says the
    // last one stopped.
    seastar::future<segment_removal> remove_owned(owner& removed) {
        segment_removal outcome;
        const auto owed = [&](const runtime::operation_error& why) {
            outcome.failure.observe(why);
            removed.debt.observe(why);
        };
        // A sync that failed proved nothing of what it covered, and one
        // that follows it on the same device proves no more: the next
        // start decides.
        if (const auto& latched = unsynced(removed)) {
            outcome.failure.observe(*latched);
            co_return outcome;
        }
        if (stopped(removed)) {
            outcome.failure.observe(detail::path_error(errc::closed));
            co_return outcome;
        }
        if (!removed.drained) {
            if (
              const auto let_go = co_await drain(removed, removed.debt);
              !let_go) {
                outcome.failure.observe(let_go.error());
                co_return outcome;
            }
            removed.drained = true;
        }
        const auto at = where(removed);
        if (!at) {
            owed(at.error());
            co_return outcome;
        }
        seastar::abort_source abort;
        codec::cooperative_work work{config_.policy, abort};
        if (stopped(removed)) {
            outcome.failure.observe(detail::path_error(errc::closed));
            co_return outcome;
        }
        const auto marked = co_await mark(removed, *at, work);
        if (!marked) {
            // A segment that is not sealed is its owner's to discard
            // whether or not its mark could be made, and a restart goes on
            // from its owner's word: the checkpoint and the WAL are not
            // held for a device that will not take a rename.
            if (removed.accepted && !removed.accepted->sealed) resolve(removed);
            owed(marked.error());
            co_return outcome;
        }
        const bool present = *marked;
        resolve(removed);
        if (present) {
            const auto turn = co_await seastar::get_units(removals_, 1);
            if (stopped(removed)) {
                outcome.failure.observe(detail::path_error(errc::closed));
                co_return outcome;
            }
            if (
              const auto emptied = co_await empty_directory(
                removed, *at, outcome, work);
              !emptied) {
                owed(emptied.error());
                co_return outcome;
            }
        }
        // The directory stays where a restart finds it while the durable
        // checkpoint may send one to look for the segment. A checkpoint is
        // asked for once, and is written for the covered entries alone
        // when the cutoff stands still. One that cannot be published, or
        // whose cutoff has not passed the segment's PREPAREs, leaves the
        // removal to be finished by a later request.
        if (named(removed)) {
            if (!stopped(removed)) {
                try {
                    static_cast<void>(co_await checkpoint_->let_go());
                } catch (...) {
                }
            }
            if (named(removed)) {
                outcome.settling = true;
                co_return outcome;
            }
        }
        if (present) {
            if (stopped(removed)) {
                outcome.failure.observe(detail::path_error(errc::closed));
                co_return outcome;
            }
            if (
              const auto gone = co_await remove_directory(removed, *at, work);
              !gone) {
                owed(gone.error());
                co_return outcome;
            }
        }
        outcome.gone = true;
        co_return outcome;
    }

    // Joins the attempt in flight for a deleting owner, or starts the
    // next. The owner may be gone when this returns.
    seastar::future<segment_removal>
    attempted(owner* removed, authorization_verdict verdict) {
        segment_removal refused;
        refused.verdict = verdict;
        if (removed->removing) {
            auto outcome = co_await removed->removing->get_shared_future();
            outcome.verdict = verdict;
            co_return outcome;
        }
        removed->removing.emplace();
        // Taken first: the attempt may have ended, and the owner be gone,
        // by the time it has been started.
        auto joined = removed->removing->get_shared_future();
        runtime::result<void> spawned;
        try {
            spawned = tasks_.spawn(
              [this, removed] { return run_removal(*removed); });
        } catch (...) {
            spawned = runtime::failure(
              detail::path_error(errc::resource_exhausted));
        }
        if (!spawned) {
            // Nothing began: the next request is a new attempt.
            removed->removing.reset();
            refused.failure.observe(spawned.error());
            co_return refused;
        }
        auto outcome = co_await std::move(joined);
        outcome.verdict = verdict;
        co_return outcome;
    }

    // The attempt the table runs for an accepted removal. A segment that is
    // gone leaves the table. One that is not stays, deleting, for the next
    // request, when the table had an owner for it before; an owner the
    // removal made leaves all the same.
    seastar::future<> run_removal(owner& removed) {
        segment_removal outcome;
        try {
            // Everyone inside leaves first: a seal's tail closes its
            // writer, and whoever came for a session finds the segment
            // deleting.
            co_await removed.left.wait(
              [&removed] { return removed.inside == 0; });
            outcome = co_await remove_owned(removed);
        } catch (...) {
            outcome.failure.observe(std::current_exception());
            outcome.gone = false;
        }
        outcome.debt = removed.debt;
        auto ended = std::move(*removed.removing);
        removed.removing.reset();
        // One made for this removal holds nothing, and what was reached is
        // on the device: it goes with the attempt. Its close waits for
        // this attempt, and takes the owner out itself.
        if ((outcome.gone || removed.brought) && !removed.closing)
            erase(removed);
        ended.set_value(std::move(outcome));
    }

    seastar::future<> run_close(owner& closed) {
        runtime::first_failure kept;
        retire(closed);
        try {
            // A removal in flight stops where it stands, and everyone
            // inside leaves: a transition in flight ends, and whoever
            // waited for one finds the owner closing.
            if (closed.removing)
                static_cast<void>(
                  co_await closed.removing->get_shared_future());
            co_await closed.left.when([&closed] { return closed.inside == 0; });
        } catch (...) {
            kept.observe(std::current_exception());
        }
        bool held = false;
        try {
            held = !(co_await drain(closed, kept)).has_value();
        } catch (...) {
            // Nothing says what is still open of it: it stays.
            kept.observe(std::current_exception());
            held = true;
        }
        auto ended = std::move(*closed.closing);
        if (held) {
            // The append owner still holds its attachment, and with it the
            // writer: the owner stays, failed or still being removed, for
            // another attempt.
            closed.failure.observe(detail::settled(kept));
            if (closed.life != segment_life::deleting)
                closed.life = segment_life::failed;
            closed.closing.reset();
        } else {
            erase(closed);
        }
        ended.set_value(detail::settled(kept));
    }

    workload_reservation held_;
    Backend& files_;
    Owner& ownership_;
    std::span<const local_device_spec> devices_;
    std::uint32_t shard_;
    append_type& append_;
    checkpoint_type* checkpoint_;
    allocator_type& ids_;
    page_cache& cache_;
    sparse_index_residency& residency_;
    sparse_index_rebuild_limit& rebuilds_;
    workload_budget& writing_;
    workload_budget& reading_;
    workload_budget& rebuilding_;
    device_table* placement_;
    segment_table_config config_;
    // In the canonical order of their contexts.
    std::vector<std::unique_ptr<owner>> owners_;
    // Who is told why a segment should be followed by another, and the
    // timer the lifetimes are slept on. None: nothing is told.
    segment_roll_sink told_;
    void* timer_{nullptr};
    sleep_fn sleep_{nullptr};
    // Owners with something to tell, in the order it was noted, and those
    // whose lifetime runs, in the order their deadlines come.
    told_list untold_;
    aged_list aging_;
    seastar::condition_variable wake_;
    bool stirred_{false}, napped_{false}, untelling_{false};
    runtime::first_failure unslept_;
    seastar::semaphore seals_;
    seastar::semaphore removals_;
    // Under which WAL position every PREPARE of a segment lies: for each
    // of the segments that left the append owner last, where the WAL ended
    // then, oldest first; and for every other segment that is attached to
    // nothing, the floor, which is where it ended when the oldest of them
    // gave way, or when the table was made. Lost when the WAL could not
    // say where it ends.
    std::vector<quiet_mark> quiet_;
    std::optional<local_wal_cursor> quiet_floor_;
    bool quiet_lost_{false};
    // For each device, by its place among `devices_`: what a directory
    // sync of a removal there failed with in this process.
    std::array<std::optional<runtime::operation_error>, maximum_local_devices>
      unsynced_{};
    std::uint64_t uses_{0};
    // Owners about to be among the sealed: each has its place already.
    std::uint32_t arriving_{0};
    bool closing_{false}, closed_{false};
    std::optional<seastar::shared_promise<runtime::result<void>>> ending_;
    // Seal and close attempts; joined last by close().
    runtime::task_scope tasks_;
};

} // namespace kwaque::storage
