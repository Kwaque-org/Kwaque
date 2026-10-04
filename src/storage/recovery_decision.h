#pragma once

#include "src/base/invariant.h"
#include "src/base/units.h"
#include "src/storage/local_id_allocator.h"
#include "src/storage/local_publication.h"
#include "src/storage/recovery_merge.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/util/defer.hh>

#include <array>
#include <exception>
#include <memory>
#include <optional>
#include <span>
#include <utility>
#include <vector>

namespace kwaque::storage {

// The durable identity of one decision record: the sequence that selects its
// fixed path on the control device, and the exact bytes it was published as.
struct recovery_decision_pin final {
    local_decision_sequence sequence;
    codec::immutable_object_digest digest;
    byte_count bytes;
    bool operator==(const recovery_decision_pin&) const = default;
};
struct recovery_decision_record final {
    recovery_decision_pin pin;
    local_recovery_decision value;
};

// A supplied resolution of one candidate. Its decision pins the exact
// PREPARE: where it begins, its exact digest, its segment and slot.
struct recovery_candidate_resolution final {
    local_wal_cursor prepare;
    segment_context segment;
    runtime::file_position target;
    // preserve, reconstruct or discard.
    local_recovery_action action;
    std::uint64_t owner_decision_id;
};
// A supplied segment-scope resolution: seal one segment at `end`, a
// complete-object boundary that may lie below its recovered boundary. One
// record resolves every candidate of the segment: those below the end are
// kept, present or reconstructed, and the rest are discarded.
struct recovery_seal_resolution final {
    segment_context segment;
    runtime::file_position end;
    std::uint64_t owner_decision_id;
};

// The WAL a segment's suffix plan is classified from: the chain the control
// pins, from the cutoff, with the segment's independent placement.
struct recovery_suffix_source final {
    // Every device that may hold the target.
    std::span<const local_device_spec> devices;
    local_shard_control control;
    std::optional<local_wal_cursor> cutoff;
    recovery_target target;
    wal_scan_limits limits{};
};

// One resolved PREPARE of the segment, in WAL order, borrowed through a
// joined visit. The visitor may take the PREPARE's child.
struct recovery_suffix_prepare final {
    local_wal_cursor begin, end;
    runtime::file_position target;
    codec::immutable_object_digest digest;
    wal_prepare* prepare;
};
struct recovery_suffix_scan final {
    // The head's classified content end.
    std::optional<local_wal_cursor> content_end;
    // The segment's placement, from its verified descriptor and header.
    wal_target target;
};

// The content identity of a segment's canonical suffix plan: every resolved
// PREPARE that names the segment in the scanned WAL, in WAL order, by its
// exact source, slot and digest, under the decided end. Those below the end
// are kept and the rest discarded. It depends only on the WAL and the end,
// so reconstructing the segment never changes it.
class recovery_suffix_identity final {
public:
    recovery_suffix_identity(
      const segment_context& segment, runtime::file_position end) noexcept;
    recovery_suffix_identity(const recovery_suffix_identity&) = delete;
    recovery_suffix_identity&
    operator=(const recovery_suffix_identity&) = delete;
    void add(const recovery_suffix_prepare& prepare) noexcept;
    [[nodiscard]] codec::immutable_object_digest finish() && noexcept;

private:
    codec::xxh3_128_hasher hash_;
    std::uint64_t count_{0};
};

namespace detail {
template<
  runtime::file_system_backend Backend,
  local_directory_owner Owner,
  typename Writer>
class recovered_reconstruction;
} // namespace detail

// A seal at `end` whose layout was proved before any decision names it:
// every slot from the segment's certified end to `end` holds a verified
// surviving object or the block of the PREPARE that names it. It carries the
// suffix plan it proved and the WAL content end that plan was classified
// from. Only the recovered seal's proving pass makes one.
class recovered_seal_proof final {
public:
    [[nodiscard]] const segment_context& segment() const noexcept {
        return segment_;
    }
    [[nodiscard]] runtime::file_position end() const noexcept { return end_; }
    [[nodiscard]] storage_alignment alignment() const noexcept {
        return alignment_;
    }
    [[nodiscard]] local_wal_cursor content_end() const noexcept {
        return content_end_;
    }
    [[nodiscard]] codec::immutable_object_digest identity() const noexcept {
        return identity_;
    }

private:
    template<
      runtime::file_system_backend Backend,
      local_directory_owner Owner,
      typename Writer>
    friend class detail::recovered_reconstruction;
    recovered_seal_proof(
      segment_context segment,
      runtime::file_position end,
      storage_alignment alignment,
      local_wal_cursor content_end,
      codec::immutable_object_digest identity) noexcept
      : segment_(segment)
      , end_(end)
      , alignment_(alignment)
      , content_end_(content_end)
      , identity_(identity) {}
    segment_context segment_;
    runtime::file_position end_;
    storage_alignment alignment_;
    local_wal_cursor content_end_;
    codec::immutable_object_digest identity_;
};

namespace detail {
// Whether a candidate decision agrees with its segment's seal at `end`: a
// candidate below the end is reconstructed, one at or after it discarded.
[[nodiscard]] bool recovery_decisions_agree(
  const local_recovery_decision& candidate,
  runtime::file_position end) noexcept;

// One pass over the WAL for one segment: the chain from the cutoff, with only
// that segment in the target catalog, every resolved PREPARE of it hashed and
// visited in WAL order. visit(const recovery_suffix_prepare&) returns
// future<result<void>>. A claim the segment cannot hold, a target out of WAL
// order or corruption of a sealed prefix stops the plan: the segment's
// layout would not be provable from this WAL.
template<
  runtime::file_system_backend Backend,
  local_directory_owner Owner,
  typename Visitor>
seastar::future<runtime::result<recovery_suffix_scan>> scan_recovery_suffix(
  Backend& files,
  Owner& ownership,
  const local_device_spec& wal_device,
  std::uint32_t shard,
  const recovery_suffix_source& source,
  workload_budget& budget,
  codec::cooperative_work& work,
  Visitor visit) {
    static_assert(sizeof(Visitor) <= 4_KiB);
    const auto& target = source.target;
    if (auto valid = validate_recovery_targets({&target, 1}); !valid)
        co_return runtime::failure(valid.error());
    if (
      target.state != local_object_state::active
      && target.state != local_object_state::recovering)
        co_return runtime::failure(path_error(errc::wrong_context));
    const auto segment = target.descriptor.segment;
    using catalog_type = local_wal_target_catalog<Backend, Owner>;
    const std::array<typename catalog_type::entry, 1> entries{
      {{target.descriptor, target.header}}};
    auto catalog = catalog_type::make(
      files,
      ownership,
      source.devices,
      shard,
      entries,
      budget,
      source.limits.metadata,
      work);
    if (!catalog) co_return runtime::failure(catalog.error());
    // Its placement first, so a segment with no PREPARE is still located.
    auto located = co_await (**catalog)(segment);
    if (!located) co_return runtime::failure(located.error());
    if (!*located)
        co_return runtime::failure(path_error(errc::invariant_violation));
    auto limits = source.limits;
    limits.digests = true;
    std::optional<runtime::file_position> last;
    auto prepares = [&](const wal_scanned_prepare& value)
      -> seastar::future<runtime::result<bool>> {
        if (value.resolution != wal_target_resolution::resolved) {
            if (value.claims.target == segment)
                co_return runtime::failure(path_error(errc::wrong_context));
            co_return true;
        }
        const auto at = value.prepare->target().position();
        if (last && at <= *last)
            co_return runtime::failure(path_error(errc::wrong_context));
        last = at;
        auto visited = co_await visit(
          recovery_suffix_prepare{
            value.begin, value.end, at, *value.digest, value.prepare});
        if (!visited) co_return runtime::failure(visited.error());
        co_return true;
    };
    auto scanned = co_await scan_local_wal(
      files,
      ownership,
      wal_device,
      shard,
      source.control,
      source.cutoff,
      budget,
      limits,
      work,
      **catalog,
      std::move(prepares),
      [](const wal_file_scan&) {
          return seastar::make_ready_future<runtime::result<void>>(
            runtime::result<void>{});
      });
    if (!scanned) co_return runtime::failure(scanned.error());
    if (scanned->verdict == wal_scan_verdict::corrupt)
        co_return runtime::failure(path_error(errc::corrupt_data));
    if (!scanned->complete)
        co_return runtime::failure(path_error(errc::invariant_violation));
    co_return recovery_suffix_scan{scanned->content_end, **located};
}

// Reads one decision back from its fixed path on the control device, under
// its pinned identity and the segment the caller independently expects.
template<runtime::file_system_backend Backend, local_directory_owner Owner>
seastar::future<runtime::result<local_recovery_decision>>
load_recovery_decision(
  Backend& files,
  Owner& ownership,
  const local_device_spec& control,
  std::uint32_t shard,
  recovery_decision_pin pin,
  const local_segment_descriptor& descriptor,
  workload_budget& budget,
  local_store_io_limits limits,
  codec::cooperative_work& work) {
    if (auto valid = co_await ownership.validate(control); !valid)
        co_return runtime::failure(valid.error());
    const auto paths = local_paths::make(control.root);
    if (!paths) co_return runtime::failure(paths.error());
    auto path = paths->sequence_file(
      shard, local_sequence_file::decision, pin.sequence.value());
    if (!path) co_return runtime::failure(path.error());
    const auto owner = control.shard_owner(shard);
    if (!owner) co_return runtime::failure(path_error(errc::wrong_context));
    auto header = local_metadata_header::make(
      local_metadata_kind::recovery_decision,
      *owner,
      local_publication_generation::make(pin.sequence.value()).value());
    if (!header) co_return runtime::failure(path_error(errc::invalid_argument));
    local_metadata_expectation expected{
      *header, control.identity.metadata_alignment};
    expected.segment = descriptor.segment;
    expected.segment_alignment = descriptor.alignment;
    expected.digest = pin.digest;
    expected.encoded_bytes = pin.bytes;
    auto record = co_await load_local_metadata_file(
      files,
      control,
      *path,
      budget,
      limits,
      work,
      local_metadata_extent::exact_file,
      [expected](
        auto& input, byte_count, codec::decode_budget memory, auto& work) {
          return decode_local_metadata(
            input, expected, memory, work, {}, codec::input_boundary::complete);
      });
    if (!record) co_return runtime::failure(record.error());
    co_return std::get<local_recovery_decision>(record->value.payload());
}
} // namespace detail

struct recovery_decision_limits final {
    local_store_io_limits metadata{};
    // Decisions held for replay and conflict checks.
    std::uint32_t decisions{1024};
    [[nodiscard]] runtime::result<void> validate() const noexcept;
};

enum class recovery_decision_status : std::uint8_t {
    // Published now.
    created,
    // The same decision is already durable; nothing was written.
    exists,
    // The segment's durable seal already resolves this candidate this way.
    resolved,
    // A durable decision for the same candidate or segment says otherwise.
    conflict,
};
struct recovery_decision_outcome final {
    recovery_decision_status status;
    // The created or stored decision; for a conflict, the one it contradicts.
    recovery_decision_record record;
};

// Persists resolutions an owner supplies, before anything they authorize
// runs. Each resolution is checked against the classified WAL first: a
// candidate decision pins the exact PREPARE it names, and a segment decision
// pins the WAL content end and the canonical suffix plan its proof proved
// (resolve_recovered_seal). Nothing is decided
// here: every action comes from the owner. A decision is one immutable
// record at decisions/<sequence>.meta on the control device, its sequence
// allocated durably through the control before the record is written. The
// same resolution again replays the stored record; one that contradicts a
// stored decision for its candidate or segment is a conflict; neither writes
// anything. An uncertain publication fences this log. Held decisions are
// bounded; after a restart their pins are adopted again. One operation at a
// time. The allocator serves this control; it and every provider outlive
// joined close.
template<runtime::file_system_backend Backend, local_directory_owner Owner>
class recovery_decision_log final : public runtime::shard_affine {
public:
    using allocator_type = local_id_allocator<Backend, Owner>;

    [[nodiscard]] static runtime::result<std::unique_ptr<recovery_decision_log>>
    make(
      Backend& files,
      Owner& ownership,
      local_device_spec control,
      std::uint32_t shard,
      allocator_type& ids,
      workload_budget& budget,
      recovery_decision_limits limits) {
        if (auto valid = limits.validate(); !valid)
            return runtime::failure(valid.error());
        if (auto valid = validate_local_device_spec(control); !valid)
            return runtime::failure(valid.error());
        if (!control.controls() || shard >= control.identity.shard_count)
            return runtime::failure(path_error(errc::invalid_argument));
        auto instance = budget.allocation_charge(
          byte_count{sizeof(recovery_decision_log)});
        auto records = budget.allocation_charge(
          byte_count{limits.decisions * sizeof(recovery_decision_record)});
        if (!instance) return runtime::failure(instance.error());
        if (!records) return runtime::failure(records.error());
        auto held = budget.try_reserve(
          byte_count{instance->value() + records->value()});
        if (!held) return runtime::failure(held.error());
        auto output = std::unique_ptr<recovery_decision_log>(
          new recovery_decision_log(
            files,
            ownership,
            std::move(control),
            shard,
            ids,
            budget,
            limits,
            std::move(*held)));
        output->records_.reserve(limits.decisions);
        return output;
    }
    recovery_decision_log(const recovery_decision_log&) = delete;
    recovery_decision_log& operator=(const recovery_decision_log&) = delete;
    ~recovery_decision_log() {
        assert_current();
        KWAQUE_INVARIANT(
          invariant_id{"KQ-RECOVERY-DECISIONS-CLOSED"},
          closed_,
          "decision log destroyed before joined close");
    }

    [[nodiscard]] std::span<const recovery_decision_record>
    decisions() const noexcept {
        assert_current();
        return records_;
    }
    [[nodiscard]] bool fenced() const noexcept {
        assert_current();
        return fenced_;
    }

    // Holds a durable decision again after a restart: read back under its
    // pin, for the segment the caller independently expects. One that
    // contradicts a held decision is not held.
    [[nodiscard]] seastar::future<runtime::result<recovery_decision_record>>
    adopt(
      recovery_decision_pin pin,
      local_segment_descriptor descriptor,
      codec::cooperative_work& work) {
        auto entered = enter(work);
        if (!entered) co_return runtime::failure(entered.error());
        auto holder = std::move(*entered);
        auto idle = seastar::defer([this] noexcept { busy_ = false; });
        for (const auto& stored : records_)
            if (stored.pin.sequence == pin.sequence) {
                if (
                  stored.pin != pin
                  || stored.value.segment != descriptor.segment)
                    co_return runtime::failure(path_error(errc::wrong_context));
                co_return stored;
            }
        if (records_.size() == limits_.decisions)
            co_return runtime::failure(path_error(errc::resource_exhausted));
        auto value = co_await detail::load_recovery_decision(
          files_,
          ownership_,
          control_,
          shard_,
          pin,
          descriptor,
          budget_,
          limits_.metadata,
          work);
        if (!value) co_return runtime::failure(value.error());
        if (
          auto held = held_by(*value);
          held && held->status != recovery_decision_status::exists)
            co_return runtime::failure(path_error(errc::wrong_context));
        records_.push_back({pin, *value});
        co_return records_.back();
    }

    // Persists one candidate's resolution. The PREPARE must begin at the
    // supplied cursor in the classified WAL and name the supplied slot.
    [[nodiscard]] seastar::future<runtime::result<recovery_decision_outcome>>
    resolve(
      recovery_candidate_resolution resolution,
      const recovery_suffix_source& source,
      codec::cooperative_work& work) {
        auto entered = enter(work);
        if (!entered) co_return runtime::failure(entered.error());
        auto holder = std::move(*entered);
        auto idle = seastar::defer([this] noexcept { busy_ = false; });
        const auto& descriptor = source.target.descriptor;
        if (
          (resolution.action != local_recovery_action::preserve
           && resolution.action != local_recovery_action::reconstruct
           && resolution.action != local_recovery_action::discard)
          || resolution.owner_decision_id == 0
          || resolution.segment != descriptor.segment)
            co_return runtime::failure(path_error(errc::invalid_argument));
        // A durable decision already answers it: no I/O.
        for (const auto& stored : records_) {
            const auto& value = stored.value;
            if (value.segment != resolution.segment) continue;
            if (value.action == local_recovery_action::seal_at) {
                const bool agrees
                  = resolution.action
                    == (resolution.target < value.target_position ? local_recovery_action::reconstruct : local_recovery_action::discard);
                co_return recovery_decision_outcome{
                  agrees ? recovery_decision_status::resolved
                         : recovery_decision_status::conflict,
                  stored};
            }
            if (value.prepare == resolution.prepare)
                co_return recovery_decision_outcome{
                  value.target_position == resolution.target
                      && value.action == resolution.action
                      && value.owner_decision_id == resolution.owner_decision_id
                    ? recovery_decision_status::exists
                    : recovery_decision_status::conflict,
                  stored};
        }
        std::optional<
          std::pair<runtime::file_position, codec::immutable_object_digest>>
          found;
        auto scanned = co_await detail::scan_recovery_suffix(
          files_,
          ownership_,
          control_,
          shard_,
          source,
          budget_,
          work,
          [&found, &resolution](const recovery_suffix_prepare& prepare) {
              if (prepare.begin == resolution.prepare)
                  found.emplace(prepare.target, prepare.digest);
              return seastar::make_ready_future<runtime::result<void>>(
                runtime::result<void>{});
          });
        if (!scanned) co_return runtime::failure(scanned.error());
        if (!found || found->first != resolution.target)
            co_return runtime::failure(path_error(errc::not_found));
        co_return co_await persist(
          {resolution.prepare,
           found->second,
           resolution.segment,
           resolution.target,
           resolution.action,
           resolution.owner_decision_id},
          descriptor.alignment,
          work);
    }

    // What the durable decisions already answer for a seal resolution, with
    // no I/O: the segment's own seal, or a held candidate decision that the
    // end contradicts. Nothing when only a new decision can answer it.
    [[nodiscard]] std::optional<recovery_decision_outcome>
    settled(const recovery_seal_resolution& resolution) const {
        assert_current();
        for (const auto& stored : records_) {
            const auto& value = stored.value;
            if (
              value.segment == resolution.segment
              && value.action == local_recovery_action::seal_at)
                return recovery_decision_outcome{
                  value.target_position == resolution.end
                      && value.owner_decision_id == resolution.owner_decision_id
                    ? recovery_decision_status::exists
                    : recovery_decision_status::conflict,
                  stored};
        }
        for (const auto& stored : records_) {
            const auto& value = stored.value;
            if (
              value.segment == resolution.segment
              && !detail::recovery_decisions_agree(value, resolution.end))
                return recovery_decision_outcome{
                  recovery_decision_status::conflict, stored};
        }
        return std::nullopt;
    }

    // Persists a segment's seal at an end whose layout `proof` proved, naming
    // the suffix plan it proved. The segment's own seal answers first; then
    // every candidate decision held for it must agree with this end.
    [[nodiscard]] seastar::future<runtime::result<recovery_decision_outcome>>
    resolve(
      recovery_seal_resolution resolution,
      const recovered_seal_proof& proof,
      codec::cooperative_work& work) {
        auto entered = enter(work);
        if (!entered) co_return runtime::failure(entered.error());
        auto holder = std::move(*entered);
        auto idle = seastar::defer([this] noexcept { busy_ = false; });
        if (
          resolution.owner_decision_id == 0
          || resolution.segment != proof.segment()
          || resolution.end != proof.end())
            co_return runtime::failure(path_error(errc::invalid_argument));
        if (auto held = settled(resolution)) co_return std::move(*held);
        co_return co_await persist(
          {proof.content_end(),
           proof.identity(),
           resolution.segment,
           resolution.end,
           local_recovery_action::seal_at,
           resolution.owner_decision_id},
          proof.alignment(),
          work);
    }

    [[nodiscard]] seastar::future<runtime::result<void>> close() {
        assert_current();
        if (closed_) co_return runtime::result<void>{};
        if (closing_) co_return runtime::failure(path_error(errc::queue_full));
        closing_ = true;
        co_await operations_.close();
        closed_ = true;
        co_return runtime::result<void>{};
    }

private:
    recovery_decision_log(
      Backend& files,
      Owner& ownership,
      local_device_spec control,
      std::uint32_t shard,
      allocator_type& ids,
      workload_budget& budget,
      recovery_decision_limits limits,
      workload_reservation held) noexcept
      : held_(std::move(held))
      , files_(files)
      , ownership_(ownership)
      , control_(std::move(control))
      , shard_(shard)
      , ids_(ids)
      , budget_(budget)
      , limits_(limits) {}

    static runtime::operation_error path_error(errc code) noexcept {
        return detail::path_error(code);
    }

    runtime::result<seastar::gate::holder>
    enter(codec::cooperative_work& work) {
        assert_current();
        if (closing_ || closed_ || fenced_)
            return runtime::failure(path_error(errc::closed));
        if (busy_) return runtime::failure(path_error(errc::queue_full));
        if (auto ready = work.poll(); !ready)
            return runtime::failure(path_error(ready.error().code()));
        busy_ = true;
        return operations_.hold();
    }

    // How a held decision answers `value`, if one does.
    std::optional<recovery_decision_outcome>
    held_by(const local_recovery_decision& value) const {
        for (const auto& stored : records_) {
            const auto& held = stored.value;
            if (held.segment != value.segment) continue;
            const bool seal = value.action == local_recovery_action::seal_at;
            const bool held_seal = held.action
                                   == local_recovery_action::seal_at;
            if (seal == held_seal && (seal || held.prepare == value.prepare))
                return recovery_decision_outcome{
                  held == value ? recovery_decision_status::exists
                                : recovery_decision_status::conflict,
                  stored};
            if (
              seal != held_seal
              && !(
                seal ? detail::recovery_decisions_agree(
                         held, value.target_position)
                     : detail::recovery_decisions_agree(
                         value, held.target_position)))
                return recovery_decision_outcome{
                  recovery_decision_status::conflict, stored};
        }
        return std::nullopt;
    }

    seastar::future<runtime::result<recovery_decision_outcome>> persist(
      local_recovery_decision value,
      storage_alignment segment_alignment,
      codec::cooperative_work& work) {
        if (records_.size() == limits_.decisions)
            co_return runtime::failure(path_error(errc::resource_exhausted));
        auto held = budget_.try_reserve(
          byte_count{
            limits_.metadata.operation_bytes.value()
            + limits_.metadata.execution_bytes.value()});
        if (!held) co_return runtime::failure(held.error());
        // The sequence is durable in the control before its record exists.
        auto sequence = co_await ids_.allocate_decision(work);
        if (!sequence) co_return runtime::failure(sequence.error());
        const auto owner = control_.shard_owner(shard_);
        if (!owner) co_return runtime::failure(path_error(errc::wrong_context));
        const auto generation = local_publication_generation::make(
          sequence->value());
        if (!generation)
            co_return runtime::failure(path_error(errc::invalid_argument));
        const auto header = local_metadata_header::make(
          local_metadata_kind::recovery_decision, *owner, *generation);
        if (!header)
            co_return runtime::failure(path_error(errc::invalid_argument));
        auto encoded = co_await encode_local_metadata(
          {*header, control_.identity.metadata_alignment, segment_alignment},
          local_metadata_payload{value},
          work,
          limits_.metadata.operation_bytes,
          limits_.metadata.charge);
        if (!encoded)
            co_return runtime::failure(path_error(encoded.error().code()));
        const recovery_decision_pin pin{
          *sequence, encoded->digest, encoded->bytes.size()};
        const auto paths = local_paths::make(control_.root);
        if (!paths) co_return runtime::failure(paths.error());
        auto path = paths->sequence_file(
          shard_, local_sequence_file::decision, sequence->value());
        if (!path) co_return runtime::failure(path.error());
        const auto split = path->value().rfind('/');
        auto parent = runtime::file_path::make(path->value().substr(0, split));
        auto name = runtime::file_name::make(path->value().substr(split + 1));
        if (!parent || !name)
            co_return runtime::failure(path_error(errc::invalid_argument));
        if (auto valid = co_await ownership_.validate(control_); !valid)
            co_return runtime::failure(valid.error());
        local_file_publisher<Backend> publisher{
          files_,
          budget_,
          {*owner,
           control_.root,
           std::move(*parent),
           std::move(*name),
           runtime::file_rename_policy::no_replace,
           {}}};
        runtime::first_failure failed;
        auto disposition = local_publication_disposition::untouched;
        bool temporary = false;
        try {
            auto published = co_await publisher.publish(
              {*owner, *generation, {}}, std::move(encoded->bytes), work);
            failed = published.failure;
            disposition = published.disposition;
            temporary = published.temporary_may_exist;
        } catch (...) {
            failed.observe(std::current_exception());
            temporary = true;
        }
        try {
            failed.observe(co_await publisher.close());
        } catch (...) {
            failed.observe(std::current_exception());
        }
        if (
          failed.failed()
          || disposition != local_publication_disposition::durable) {
            // A record that may exist is reconciled from its durable state,
            // never written again from here.
            if (
              disposition != local_publication_disposition::untouched
              || temporary)
                fenced_ = true;
            if (!failed.failed()) failed.observe(path_error(errc::io_failure));
            auto error = failed.outcome();
            co_return runtime::failure(error.error());
        }
        records_.push_back({pin, value});
        co_return recovery_decision_outcome{
          recovery_decision_status::created, records_.back()};
    }

    // Declared first: released after the records it admitted.
    workload_reservation held_;
    Backend& files_;
    Owner& ownership_;
    local_device_spec control_;
    std::uint32_t shard_;
    allocator_type& ids_;
    workload_budget& budget_;
    recovery_decision_limits limits_;
    std::vector<recovery_decision_record> records_;
    seastar::gate operations_;
    bool busy_{false}, closing_{false}, closed_{false}, fenced_{false};
};

} // namespace kwaque::storage
