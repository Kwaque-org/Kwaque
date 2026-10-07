#pragma once

#include "src/base/invariant.h"
#include "src/base/units.h"
#include "src/runtime/file_error.h"
#include "src/runtime/task_scope.h"
#include "src/runtime/timer.h"
#include "src/storage/completed_retry.h"
#include "src/storage/segment_writer.h"
#include "src/storage/wal_group_commit.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/chunked_vector.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/noncopyable_function.hh>

#include <algorithm>
#include <array>
#include <cstdint>
#include <exception>
#include <memory>
#include <optional>
#include <span>
#include <utility>
#include <vector>

namespace kwaque::storage {

inline constexpr std::uint32_t maximum_local_append_segments = 1024;
inline constexpr std::uint32_t maximum_local_append_requests = 256;

struct local_append_config final {
    // Attached writable segments; each keeps at most one forming group.
    // Every one of them holds handle credits for as long as it is open, so a
    // shard checks this count against the budget that funds its segment
    // writers with validate_segment_handles() before it starts.
    std::uint32_t maximum_segments{64};
    // Accepted requests whose outcome is not yet published.
    std::uint32_t maximum_requests{64};
    // Acceptance validation, WAL and segment execution share this policy.
    codec::limits policy{codec::limits::defaults()};
    // Coordination state and frames of one request, one group or one
    // segment's barrier loop.
    byte_count execution_bytes{4_KiB};
    [[nodiscard]] runtime::result<void> validate() const noexcept;
};

// An exact assigned child and its independently supplied context. backing
// covers the child's backing, descriptors and sharing controls.
struct local_append_request final {
    encoded_assigned_batch batch;
    workload_reservation backing;
    model::batch_decode_expectation expected;
    model::range_routing_epoch routing_epoch;
    // The physical record boundary the caller expects the batch to start at.
    std::optional<model::segment_relative_end> physical_begin;
};

// Accepted only when the segment group fits and nothing failed.
// roll_required, impossible and every failure reject with no record effect.
struct local_append_acceptance final {
    segment_capacity_decision capacity{segment_capacity_decision::impossible};
    runtime::first_failure failure;
    [[nodiscard]] bool accepted() const noexcept {
        return capacity == segment_capacity_decision::fits && !failure.failed();
    }
};

// not_written: no PREPARE was accepted and no segment byte was submitted, and
// the owner can show it. uncertain: bytes may survive and recovery classifies
// them; a later retry of the same batch is neither new nor completed.
// Neither is rollback.
enum class local_append_status : std::uint8_t {
    durable,
    not_written,
    uncertain
};

// Both barriers cover this request: its PREPARE, and its block and the group
// footer. block is the exact extent a later index needs. segment comes from
// the barrier that covered the group; one barrier can cover several groups,
// so its cut may end past this group. This is local durability, not a
// producer acknowledgement.
struct local_append_receipt final {
    assigned_batch_info batch;
    segment_block_layout block;
    wal_durable_receipt wal;
    segment_durable_receipt segment;
};
struct local_append_outcome final {
    local_append_status status{local_append_status::not_written};
    runtime::first_failure failure;
    std::optional<local_append_receipt> receipt;
};

// accepted is ready once the request holds its admission and its order in
// its segment is fixed; it will not be reordered. Requests take that order in
// call order, across segments too, so WAL order follows call order. result
// is its outcome.
// Join result also after a rejection. Dropping it detaches only the caller:
// accepted work and its buffers stay owned until both barriers settle.
struct local_append_stages final {
    seastar::future<local_append_acceptance> accepted;
    seastar::future<local_append_outcome> result;
};

// The caller's interest in one result, separate from the append. Aborting the
// source or reaching the deadline ends only this interest. Before acceptance
// the request is then rejected with no effect. After it, result fails with
// aborted or timed_out, and the append still runs and settles unobserved. A
// published outcome is not replaced by a later end of interest. The timer and
// the abort source outlive the result future.
template<runtime::timer_service Timer>
struct local_append_interest final {
    Timer& timer;
    seastar::abort_source& abort;
    std::optional<runtime::monotonic_time> deadline;
};
struct local_append_interest_stages final {
    seastar::future<local_append_acceptance> accepted;
    seastar::future<runtime::result<local_append_outcome>> result;
};

// The outcome of recording one completed-retry fact, keyed by its full batch
// identity. created stores it. exists returns the stored original for an
// identical fact. exists_with_different_* rejects a conflicting fact and names
// the first field, in fact order, that differs from the stored original.
enum class completed_retry_status : std::uint8_t {
    created,
    exists,
    exists_with_different_submitted_digest,
    exists_with_different_original_binding,
    exists_with_different_returned_span,
    exists_with_different_ack_generation,
};
struct completed_retry_ingest final {
    completed_retry_status status;
    // The fact stored for this identity.
    completed_retry stored;
};
// The first field, in fact order, where an offered fact differs from the one
// stored under its identity; exists when none does.
[[nodiscard]] inline completed_retry_status completed_retry_difference(
  const completed_retry& offered, const completed_retry& stored) noexcept {
    if (offered.submitted_digest() != stored.submitted_digest())
        return completed_retry_status::exists_with_different_submitted_digest;
    if (offered.original_binding() != stored.original_binding())
        return completed_retry_status::exists_with_different_original_binding;
    if (offered.returned_span() != stored.returned_span())
        return completed_retry_status::exists_with_different_returned_span;
    if (offered.ack_generation() != stored.ack_generation())
        return completed_retry_status::exists_with_different_ack_generation;
    return completed_retry_status::exists;
}

// The completed-retry facts of one detached attachment, in canonical batch
// identity order, with the storage its appends admitted for them, in chunks
// within the contiguous allocation bound. It is a segment seal source: read()
// returns exactly the requested slice and replays it unchanged. A reserved
// entry without a fact is unresolved, not a result.
class local_retry_snapshot final {
public:
    local_retry_snapshot(local_retry_snapshot&&) noexcept = default;
    local_retry_snapshot& operator=(local_retry_snapshot&&) noexcept = default;
    local_retry_snapshot(const local_retry_snapshot&) = delete;
    local_retry_snapshot& operator=(const local_retry_snapshot&) = delete;
    [[nodiscard]] std::uint32_t completed() const noexcept {
        return static_cast<std::uint32_t>(facts_.size());
    }
    // Entries reserved by the attachment's appends.
    [[nodiscard]] std::uint32_t reserved() const noexcept { return reserved_; }
    [[nodiscard]] const seastar::chunked_vector<completed_retry>&
    facts() const& noexcept {
        return facts_;
    }
    const seastar::chunked_vector<completed_retry>& facts() const&& = delete;
    [[nodiscard]] seastar::future<runtime::result<std::vector<completed_retry>>>
    read(std::uint32_t first, std::uint32_t count, codec::cooperative_work&)
      const {
        using output = runtime::result<std::vector<completed_retry>>;
        if (first > facts_.size() || count > facts_.size() - first)
            return seastar::make_ready_future<output>(runtime::failure(
              runtime::operation_error{
                errc::out_of_range, runtime::operation_kind::file}));
        try {
            return seastar::make_ready_future<output>(
              std::vector<completed_retry>(
                facts_.begin() + first, facts_.begin() + first + count));
        } catch (...) {
            return seastar::current_exception_as_future<output>();
        }
    }

private:
    template<
      runtime::file_system_backend Backend,
      typename Owner,
      runtime::monotonic_clock Clock>
    friend class local_append;
    local_retry_snapshot(
      std::optional<workload_reservation> held,
      seastar::chunked_vector<completed_retry> facts,
      std::uint32_t reserved) noexcept
      : held_(std::move(held))
      , facts_(std::move(facts))
      , reserved_(reserved) {}
    std::optional<workload_reservation> held_;
    seastar::chunked_vector<completed_retry> facts_;
    std::uint32_t reserved_{0};
};

// The open obligations of one WAL file toward one segment: groups whose
// PREPAREs the file holds while their segment group is not durable.
struct local_obligation final {
    model::wal_incarnation_id wal;
    segment_context segment;
    // Groups whose segment barrier has not settled.
    std::uint32_t pending{0};
    // Failed or uncertain segment groups, held until recovery resolves them.
    std::uint32_t pinned{0};
};
// Obligations recovery rebuilt from the WAL after a restart: PREPAREs of one
// WAL file toward one segment that no durable segment copy discharges, and
// where the oldest of them begins.
struct local_pinned_obligation final {
    model::wal_incarnation_id wal;
    segment_context segment;
    std::uint32_t pinned{0};
    local_wal_cursor first;
};
struct local_obligation_snapshot final {
    // The WAL's durable end.
    local_wal_cursor wal_durable;
    // Every group that begins before this cursor is segment-durable. It is
    // where the oldest open or pinned obligation begins, a group the WAL is
    // still accepting included, or the reserved WAL end when there is none.
    // It can lie past wal_durable: a group's segment barrier may settle first.
    local_wal_cursor discharged;
    // Obligations visited.
    std::uint32_t obligations{0};
};

// The newest durable group footer of one attachment: every group its segment
// barrier covered ends at or before it. Plain facts; holding one keeps nothing
// of the segment alive.
struct local_durable_boundary final {
    // The data device the segment was opened on.
    device_store_id device;
    segment_history_context history;
    // What the footer covers, and where its own bytes lie.
    storage::coverage covered;
    model::file_byte_span footer;
    bool operator==(const local_durable_boundary&) const noexcept = default;
};
// What a completed-retry snapshot of one attachment is cut from: the facts
// recorded for it so far, in canonical batch identity order, and its newest
// durable footer. The facts are borrowed until the next call into the owner.
struct local_retry_state final {
    const seastar::chunked_vector<completed_retry>* facts;
    std::optional<local_durable_boundary> boundary;
};
// What a detached attachment leaves behind.
struct local_detached final {
    // Its completed-retry facts: the seal source.
    local_retry_snapshot retry;
    // Its newest durable footer, final for this attachment. A bound
    // checkpoint owner already keeps it until the segment's own publication
    // covers it.
    std::optional<local_durable_boundary> boundary;
};

// The first storage failure of the WAL or of one segment owner.
enum class local_storage_path : std::uint8_t { wal, segment };
struct local_storage_failure final {
    local_storage_path path;
    // The failed segment, for a segment failure.
    std::optional<segment_context> segment;
    runtime::first_failure failure;
    // From the file failure; unknown when the failure carries none.
    runtime::file_failure_detail detail{runtime::file_failure_detail::unknown};
};
// Receives the first storage failure once, on the owner's shard, after the
// owner's state is consistent, so it may call back into the owner. It must not
// throw. Stopping admission, isolating or stopping is the caller's policy.
using local_failure_sink
  = seastar::noncopyable_function<void(const local_storage_failure&) noexcept>;

// A rotation the retained-WAL limit refused, after a checkpoint had its
// chance to remove a file. The requests then forming were rejected unwritten;
// accepted work, which holds its WAL extent, completes. Storage never
// resolves what holds the WAL itself: it drops no candidate, invalidates no
// pin and expires nothing. So whoever can resolve it is told at every
// refusal, for as long as the limit holds.
struct local_retention_pressure final {
    // WAL files the shard holds, and the most it may.
    std::uint32_t retained{0}, limit{0};
    // The oldest PREPARE that someone must still resolve and that keeps a
    // closed WAL file: its segment, and where it begins. Absent when nothing
    // unresolved lies below the head's file, so something else keeps the
    // files: a held cutoff, or what `reclaim` says.
    std::optional<segment_context> segment;
    std::optional<local_wal_cursor> first;
    // Why the checkpoint asked for removed nothing more, when it failed to:
    // nobody reclaims this WAL (closed), the run failed, or a file below the
    // cutoff could not be removed.
    std::optional<runtime::operation_error> reclaim;
};
// Receives every such refusal, on the owner's shard, after the owner's state
// is consistent, so it may call back into the owner. It must not throw.
using local_retention_sink = seastar::noncopyable_function<void(
  const local_retention_pressure&) noexcept>;

// Whoever checkpoints the shard and removes the WAL below the cutoff, as
// this owner needs it. All are called on the owner's shard and must not
// throw.
struct local_checkpointer final {
    // A segment is detaching: its newest durable footer, which a checkpoint
    // must pin until the segment's own publication covers it. A refusal
    // leaves the segment attached.
    seastar::noncopyable_function<runtime::result<void>(
      const local_durable_boundary&) noexcept>
      detaching;
    // A discard decision is about to release its PREPARE or, with the
    // flag, has. Refused unless the decision is held there and has released
    // nothing yet: a checkpoint that passes the PREPARE must carry the
    // decision that resolved it.
    seastar::noncopyable_function<runtime::result<void>(
      const local_recovery_decision&, bool) noexcept>
      discharging;
    // The budgets its runs draw on.
    std::array<const workload_budget*, 2> reserve{};
    // The oldest obligation moved to a later WAL file, so a closed file may
    // go. Not awaited: a run it starts must not need the caller to return.
    seastar::noncopyable_function<void() noexcept> reclaimable;
    // A rotation met the retained limit: runs a checkpoint that starts after
    // this call and resolves once it has removed what it could, each file
    // reported through wal_released() as it goes.
    seastar::noncopyable_function<
      seastar::future<runtime::result<void>>() noexcept>
      relieve;
};

// One attachment of a writable segment owner; stale after detach.
class local_append_target final {
public:
    bool operator==(const local_append_target&) const noexcept = default;

private:
    template<
      runtime::file_system_backend Backend,
      typename Owner,
      runtime::monotonic_clock Clock>
    friend class local_append;
    local_append_target(std::uint32_t slot, std::uint64_t attachment) noexcept
      : slot_(slot)
      , attachment_(attachment) {}
    std::uint32_t slot_;
    std::uint64_t attachment_;
};

namespace detail {
// A caller waiting for one result. It lives in the waiting frame, and the item
// points to it until the first terminal state: publication, or the end of the
// caller's interest.
struct local_append_waiter final {
    seastar::promise<> settled;
    // Why interest ended before publication.
    runtime::first_failure ended;
};

// One accepted request. The segment child moves into its group's freeze and
// the WAL child into its offer; the preparation keeps their shared context.
struct local_append_item final : runtime::shard_affine {
    local_append_item(
      workload_reservation held,
      workload_reservation fact,
      wal_prepared_children children,
      std::uint32_t slot,
      runtime::monotonic_time accepted);
    workload_reservation held;
    // Storage for this batch's completed-retry fact. Its segment adopts it
    // when the group installs the batch's retry entry.
    std::optional<workload_reservation> fact;
    wal_prepared_children children;
    assigned_batch_info info;
    std::uint32_t slot;
    runtime::monotonic_time accepted;
    // Its segment's branch in the formed group, and its block there.
    std::uint32_t branch{0};
    std::optional<segment_block_layout> block;
    // Installed once at publication, in storage admitted with the item.
    std::optional<local_append_outcome> outcome;
    local_append_waiter* waiter{nullptr};
};
using local_append_item_ptr = seastar::lw_shared_ptr<local_append_item>;
// Keeps the first of two failures; each holds at most one channel.
void merge_failure(
  runtime::first_failure& into, const runtime::first_failure& from) noexcept;
} // namespace detail

// Composes the WAL group commit and the segment writers into the local append
// path. A request joins its segment's forming group; a group forms when the
// WAL can take it, with no batching timer, so it grows while earlier groups
// are in flight. The WAL submission is admitted before any segment position
// freezes. Segment writes start once the WAL has accepted the group, so the
// WAL and segment barriers overlap; a request is locally durable only after
// both cover it. Each segment settles independently: one barrier loop per
// segment covers every group submitted to it, and a segment's members publish
// once its barrier and the WAL barrier cover them, whatever other segments of
// the group do. Providers outlive close(), which precedes closing them.
template<
  runtime::file_system_backend Backend,
  typename Owner,
  runtime::monotonic_clock Clock>
class local_append final : public runtime::shard_affine {
    friend class local_append_test_access;

public:
    using wal_type = wal_writer<Backend, Owner>;
    using segment_type = segment_writer<Backend, Owner, Clock>;

    [[nodiscard]] static runtime::result<std::unique_ptr<local_append>> make(
      workload_budget& budget,
      wal_group_commit& commit,
      wal_type& writer,
      local_append_config config,
      local_failure_sink sink = {},
      local_retention_sink pressure = {}) {
        if (auto valid = config.validate(); !valid)
            return runtime::failure(valid.error());
        auto instance = budget.allocation_charge(
          byte_count{sizeof(local_append)});
        auto slots = budget.allocation_charge(
          byte_count{config.maximum_segments * sizeof(segment_slot)});
        auto forming = budget.allocation_charge(
          byte_count{
            maximum_wal_group_members * sizeof(detail::local_append_item_ptr)});
        // The obligation table is fixed: a row per open WAL file and segment
        // pair, at most one per live request, plus a pinned row per segment.
        auto table = budget.allocation_charge(
          byte_count{
            obligation_rows(config) * sizeof(obligation_row)
            + config.maximum_requests * sizeof(group_state*)});
        if (!instance) return runtime::failure(instance.error());
        if (!slots) return runtime::failure(slots.error());
        if (!forming) return runtime::failure(forming.error());
        if (!table) return runtime::failure(table.error());
        // Each attachment's barrier loop runs in one execution allowance.
        auto held = budget.try_reserve(
          byte_count{
            instance->value() + slots->value() + forming->value()
            + table->value()
            + (config.maximum_segments + 1U) * config.execution_bytes.value()});
        if (!held) return runtime::failure(held.error());
        return std::unique_ptr<local_append>{new local_append(
          budget,
          commit,
          writer,
          config,
          std::move(*held),
          std::move(sink),
          std::move(pressure))};
    }
    local_append(const local_append&) = delete;
    local_append& operator=(const local_append&) = delete;
    ~local_append() {
        assert_current();
        KWAQUE_INVARIANT(
          invariant_id{"KQ-LOCAL-APPEND-CLOSED"},
          closed_ && forming_.empty() && requests_ == 0 && groups_ == 0
            && formations_ == 0,
          "local append destroyed before joined close");
    }

    // A writable owner of the WAL's cluster; attached until detach()/close().
    // Starts the segment's barrier loop.
    [[nodiscard]] runtime::result<local_append_target>
    attach(segment_type& segment) {
        assert_current();
        if (closing_ || closed_) return runtime::failure(error(errc::closed));
        auto boundary = segment.capture();
        if (!boundary) return runtime::failure(boundary.error());
        auto wal = writer_.positions();
        if (!wal) return runtime::failure(wal.error());
        if (boundary->context().cluster() != wal->owner.cluster())
            return runtime::failure(error(errc::wrong_context));
        std::optional<std::size_t> free;
        for (std::size_t i = 0; i != slots_.size(); ++i) {
            if (slots_[i].writer == &segment)
                return runtime::failure(error(errc::already_exists));
            // A detached slot is free once its loop has returned.
            if (!slots_[i].writer && !slots_[i].looping && !free) free = i;
        }
        if (!free) return runtime::failure(error(errc::queue_full));
        if (reclaiming_ && reserved_from(segment.budget()))
            return runtime::failure(error(errc::invalid_argument));
        const auto index = static_cast<std::uint32_t>(*free);
        auto& slot = slots_[index];
        slot.writer = &segment;
        slot.segment.emplace(boundary->context());
        slot.device = segment.device();
        slot.stop = false;
        slot.looping = true;
        // A native allocation failure throws, with the slot left free.
        std::optional<seastar::future<>> loop;
        try {
            loop.emplace(barrier_loop(index));
        } catch (...) {
            slot.writer = nullptr;
            slot.looping = false;
            throw;
        }
        // Once created, the loop is waiting; the stop signal ends it.
        runtime::result<void> spawned;
        try {
            spawned = tasks_.spawn(
              [work = std::move(*loop)]() mutable { return std::move(work); });
        } catch (...) {
            slot.writer = nullptr;
            stop_loop(slot);
            throw;
        }
        if (!spawned) {
            slot.writer = nullptr;
            stop_loop(slot);
            return runtime::failure(spawned.error());
        }
        slot.attachment = ++attachments_;
        return local_append_target{index, slot.attachment};
    }

    // Only an idle attachment detaches: no forming request and no group in
    // flight or waiting for its barrier. Its barrier loop then returns. The
    // completed-retry facts recorded for it move to the returned snapshot,
    // which a seal of the segment reads, and its newest durable footer is
    // returned with them. A bound checkpoint owner is handed that footer
    // here, and a detach it cannot take is refused with the segment still
    // attached. Open obligations stay in the table. The segment owner
    // itself is unchanged.
    [[nodiscard]] runtime::result<local_detached>
    detach(local_append_target target) {
        assert_current();
        auto slot = find(target);
        if (!slot) return runtime::failure(slot.error());
        auto& owned = **slot;
        if (
          owned.forming != 0 || owned.groups != 0 || owned.freezing
          || owned.head)
            return runtime::failure(error(errc::queue_full));
        // The checkpoint owner takes the final footer first: a checkpoint
        // cut after this must still pin it.
        if (checkpointer_ && owned.durable)
            if (auto kept = checkpointer_->detaching(*owned.durable); !kept)
                return runtime::failure(kept.error());
        local_detached detached{
          local_retry_snapshot{
            std::move(owned.retry_memory),
            std::move(owned.facts),
            owned.retry_capacity},
          owned.durable};
        owned.retry_memory.reset();
        owned.facts = {};
        owned.retry_capacity = 0;
        owned.writer = nullptr;
        owned.segment.reset();
        owned.durable.reset();
        owned.prepared.reset();
        stop_loop(owned);
        return detached;
    }

    // Records a completed-retry fact supplied by the completed-request owner.
    // Appending, a PREPARE, a footer or a local result never record one. The
    // fact's original binding must name the attachment's segment, and the
    // attachment holds at most one fact per retry entry its appends reserved.
    // The identity is the full batch id; nothing is evicted by time. A native
    // allocation failure throws, with no effect.
    [[nodiscard]] runtime::result<completed_retry_ingest>
    record_completed_retry(
      local_append_target target, const completed_retry& fact) {
        assert_current();
        if (closing_ || closed_) return runtime::failure(error(errc::closed));
        auto slot = find(target);
        if (!slot) return runtime::failure(slot.error());
        auto& owned = **slot;
        const auto binding = fact.original_binding();
        if (
          binding.topic() != owned.segment->topic()
          || binding.range() != owned.segment->range()
          || binding.segment() != owned.segment->segment()
          || binding.generation() != owned.segment->generation())
            return runtime::failure(error(errc::wrong_context));
        const auto at = std::lower_bound(
          owned.facts.begin(),
          owned.facts.end(),
          fact.id(),
          [](const completed_retry& stored, const model::batch_id& id) {
              return stored.id().canonical_less(id);
          });
        if (at != owned.facts.end() && at->id() == fact.id())
            return completed_retry_ingest{
              completed_retry_difference(fact, *at), *at};
        if (owned.facts.size() >= owned.retry_capacity)
            return runtime::failure(error(errc::resource_exhausted));
        const auto position = at - owned.facts.begin();
        // Growth stays within the storage the appends admitted.
        owned.facts.push_back(fact);
        std::rotate(
          owned.facts.begin() + position,
          owned.facts.end() - 1,
          owned.facts.end());
        return completed_retry_ingest{completed_retry_status::created, fact};
    }

    // The facts recorded for an attachment and its newest durable footer.
    // Nothing is copied and nothing changes: recording stays the only way a
    // fact enters, and a fact never leaves before the attachment detaches.
    [[nodiscard]] runtime::result<local_retry_state>
    completed_retries(local_append_target target) {
        assert_current();
        auto slot = find(target);
        if (!slot) return runtime::failure(slot.error());
        return local_retry_state{&(**slot).facts, (**slot).durable};
    }

    // The WAL-durable end and the discharged prefix; visit sees every open or
    // pinned obligation. All of it comes from bounded owner state: no request
    // log is kept. visit must not call into this owner.
    template<std::invocable<const local_obligation&> Visit>
    [[nodiscard]] runtime::result<local_obligation_snapshot>
    obligations(Visit&& visit) const {
        assert_current();
        const auto wal = writer_.progress();
        if (!wal) return runtime::failure(error(errc::closed));
        std::uint32_t written = 0;
        for (const auto& row : rows_) {
            if (!row.segment || (row.pending == 0 && row.pinned == 0)) continue;
            visit(
              local_obligation{
                *row.wal, *row.segment, row.pending, row.pinned});
            ++written;
        }
        // The oldest of the first open group, a group whose WAL answer is not
        // installed yet, and every pinned row. Cursor order is WAL order.
        std::optional<local_wal_cursor> oldest;
        for (const auto* group : accepted_)
            if (group->open != 0) {
                oldest.emplace(*group->wal_begin);
                break;
            }
        if (submitting_) {
            auto kept = keep_older(oldest, *submitting_, wal->owner);
            if (!kept) return runtime::failure(kept.error());
        }
        for (const auto& row : rows_) {
            if (row.pinned == 0 || !row.pinned_begin) continue;
            auto kept = keep_older(oldest, *row.pinned_begin, wal->owner);
            if (!kept) return runtime::failure(kept.error());
        }
        return local_obligation_snapshot{
          wal->durable, oldest ? *oldest : wal->reserved, written};
    }
    // Before any request: installs the obligations recovery rebuilt after a
    // restart. Each pins its WAL file exactly as a failed group's does, older
    // than every later group and in WAL order among themselves, until a
    // supplied resolution discharges it; nothing expires it. They take table
    // rows, so later groups meet that pressure rather than displace them. A
    // table without room for all of them rejects with no effect.
    [[nodiscard]] runtime::result<void>
    restore_pinned(std::span<const local_pinned_obligation> restored) {
        assert_current();
        if (closing_ || closed_) return runtime::failure(error(errc::closed));
        const auto wal = writer_.positions();
        if (!wal) return runtime::failure(wal.error());
        if (
          requests_ != 0 || groups_ != 0 || formations_ != 0
          || !forming_.empty() || sequences_ != 0)
            return runtime::failure(error(errc::invalid_argument));
        std::size_t free = 0;
        for (const auto& row : rows_)
            if (!row.segment) ++free;
        for (std::size_t i = 0; i < restored.size(); ++i) {
            const auto& value = restored[i];
            if (
              value.pinned == 0 || value.first.incarnation() != value.wal
              || value.segment.cluster() != wal->owner.cluster())
                return runtime::failure(error(errc::invalid_argument));
            for (std::size_t j = 0; j < i; ++j)
                if (
                  restored[j].wal == value.wal
                  && restored[j].segment == value.segment)
                    return runtime::failure(error(errc::invalid_argument));
        }
        if (restored.size() > free)
            return runtime::failure(error(errc::resource_exhausted));
        // Each one's place in WAL order among them: older than any group.
        const auto place =
          [&](std::size_t i) -> runtime::result<std::uint64_t> {
            std::uint64_t before = 0;
            for (std::size_t j = 0; j < restored.size(); ++j) {
                if (j == i) continue;
                auto compared = restored[j].first.compare(
                  wal->owner, restored[i].first, wal->owner);
                if (!compared)
                    return runtime::failure(error(errc::wrong_context));
                if (
                  *compared == std::strong_ordering::less
                  || (*compared == std::strong_ordering::equal && j < i))
                    ++before;
            }
            return before + 1;
        };
        for (std::size_t i = 0; i < restored.size(); ++i)
            if (auto placed = place(i); !placed)
                return runtime::failure(placed.error());
        std::size_t next = 0;
        for (std::size_t i = 0; i < restored.size(); ++i) {
            while (rows_[next].segment)
                ++next;
            auto& row = rows_[next];
            row.wal.emplace(restored[i].wal);
            row.segment.emplace(restored[i].segment);
            row.pinned = restored[i].pinned;
            row.first_pinned = place(i).value();
            row.pinned_begin.emplace(restored[i].first);
        }
        sequences_ = restored.size();
        return {};
    }
    // Releases pinned PREPAREs of one segment that a durable discard decision
    // resolves. The caller holds the decision's durable record and supplies
    // the segment's unresolved PREPAREs once it applies, from the pass that
    // validated it: `remaining` of them, the oldest beginning at `first`.
    // They replace the segment's pinned rows with one. The decision must
    // discard a PREPARE at or after the segment's oldest pinned one, and it
    // releases exactly that PREPARE: a decision names one, so every other
    // pinned PREPARE of the segment is still unresolved. A discharge that
    // would release none or more than one, a repeated one, or one for a
    // segment with nothing pinned, is rejected with no effect. So is one
    // that would move the segment's oldest pinned PREPARE while discarding a
    // later one, one for a segment with a group pinned in this process
    // (such a group counts once whatever PREPAREs it holds, so a decision
    // cannot account for it: the segment's publication or a restart resolves
    // it), and, with a checkpoint owner bound, one whose decision that owner
    // does not hold or has already seen released.
    [[nodiscard]] runtime::result<void> discharge_pinned(
      const local_recovery_decision& decision,
      std::uint32_t remaining,
      std::optional<local_wal_cursor> first) {
        assert_current();
        if (closing_ || closed_) return runtime::failure(error(errc::closed));
        const auto wal = writer_.positions();
        if (!wal) return runtime::failure(wal.error());
        if (
          decision.action != local_recovery_action::discard
          || (remaining == 0) != !first)
            return runtime::failure(error(errc::invalid_argument));
        const auto held = pinned_of(decision.segment, wal->owner);
        if (!held) return runtime::failure(held.error());
        if (held->count == 0 || !held->oldest)
            return runtime::failure(error(errc::not_found));
        const auto wrong = [] {
            return runtime::failure(error(errc::wrong_context));
        };
        const auto discarded = decision.prepare.compare(
          wal->owner, *held->oldest, wal->owner);
        if (
          !discarded || *discarded == std::strong_ordering::less
          || remaining + 1 != held->count || held->live != 0)
            return wrong();
        if (first) {
            const auto kept = first->compare(
              wal->owner, *held->oldest, wal->owner);
            if (
              !kept || *kept == std::strong_ordering::less
              || *first == decision.prepare)
                return wrong();
        }
        // A later PREPARE leaves the oldest unresolved, where it was.
        if (
          *discarded == std::strong_ordering::greater
          && (!first || *first != *held->oldest))
            return wrong();
        if (checkpointer_)
            if (
              auto known = checkpointer_->discharging(decision, false); !known)
                return known;
        auto released = replace_pinned(decision.segment, remaining, first);
        if (!released) return released;
        if (checkpointer_)
            static_cast<void>(checkpointer_->discharging(decision, true));
        offer_reclaim();
        return released;
    }
    // Releases every pinned PREPARE of one segment: its durable sealed or
    // deleting publication resolves all of them. The caller holds that
    // publication. A segment still attached or with a group in flight has no
    // such publication, and one with nothing pinned has nothing to release;
    // both are rejected with no effect.
    [[nodiscard]] runtime::result<void>
    discharge_pinned(const local_object_publication& publication) {
        assert_current();
        if (closing_ || closed_) return runtime::failure(error(errc::closed));
        const auto wal = writer_.positions();
        if (!wal) return runtime::failure(wal.error());
        if (
          publication.state != local_object_state::sealed
          && publication.state != local_object_state::deleting)
            return runtime::failure(error(errc::invalid_argument));
        const auto& segment = publication.segment;
        for (const auto& slot : slots_)
            if (slot.writer && *slot.segment == segment)
                return runtime::failure(error(errc::wrong_context));
        for (const auto& row : rows_)
            if (
              row.segment && *row.segment == segment
              && (row.reserved != 0 || row.pending != 0))
                return runtime::failure(error(errc::wrong_context));
        const auto held = pinned_of(segment, wal->owner);
        if (!held) return runtime::failure(held.error());
        if (held->count == 0) return runtime::failure(error(errc::not_found));
        auto released = replace_pinned(segment, 0, std::nullopt);
        if (released) offer_reclaim();
        return released;
    }
    // Whether a WAL file is no longer being written, which a rotation flushes
    // before leaving, and no obligation row names it. A diagnostic, not a
    // deletion rule: WAL files are deleted only as a prefix below a durable
    // checkpoint cutoff, and a segment's pinned PREPAREs in later files can be
    // counted on the row of its oldest one.
    [[nodiscard]] bool reclaimable(model::wal_incarnation_id file) const {
        assert_current();
        const auto wal = writer_.progress();
        if (!wal || wal->reserved.incarnation() == file) return false;
        return std::none_of(
          rows_.begin(), rows_.end(), [&file](const auto& row) {
              return row.wal && *row.wal == file;
          });
    }
    // Each attachment's newest durable footer, for those that have one: what
    // a checkpoint pins for the segment. visit must not call into this owner.
    template<std::invocable<const local_durable_boundary&> Visit>
    void durable_boundaries(Visit&& visit) const {
        assert_current();
        for (const auto& slot : slots_)
            if (slot.writer && slot.durable) visit(*slot.durable);
    }

    // The first storage failure of the WAL or of an attached segment, also
    // reported once through the sink. Requests keep their own outcomes.
    [[nodiscard]] const std::optional<local_storage_failure>&
    storage_failure() const& noexcept {
        assert_current();
        return storage_failure_;
    }
    const std::optional<local_storage_failure>&
    storage_failure() const&& = delete;

    // The WAL files the shard holds, and the most it may.
    [[nodiscard]] wal_retention retention() const noexcept {
        assert_current();
        return writer_.retention();
    }
    // Whether the WAL was taken over from a recovered head: the shard
    // restarted, and what recovery published covers the WAL it read.
    [[nodiscard]] bool wal_recovered() const noexcept {
        assert_current();
        return writer_.recovered();
    }
    // The newest rotation the retained limit refused, also reported through
    // the sink at every refusal. It stands while the limit holds.
    [[nodiscard]] const std::optional<local_retention_pressure>&
    retention_pressure() const& noexcept {
        assert_current();
        return pressure_;
    }
    const std::optional<local_retention_pressure>&
    retention_pressure() const&& = delete;
    // Binds the shard's checkpoint owner, one at a time. From then on a
    // detaching segment's last footer and a discharging decision go through
    // it, so no checkpoint passes a PREPARE without what covers it. It
    // unbinds before it closes, and outlives the wait of a rotation it is
    // relieving.
    [[nodiscard]] runtime::result<void> can_bind_checkpointer() const noexcept {
        assert_current();
        if (closing_ || closed_) return runtime::failure(error(errc::closed));
        if (checkpointer_) return runtime::failure(error(errc::already_exists));
        return {};
    }
    [[nodiscard]] runtime::result<void>
    bind_checkpointer(local_checkpointer checkpointer) {
        if (auto free = can_bind_checkpointer(); !free) return free;
        if (
          !checkpointer.detaching || !checkpointer.discharging
          || !checkpointer.reclaimable || !checkpointer.relieve
          || !checkpointer.reserve[0] || !checkpointer.reserve[1])
            return runtime::failure(error(errc::invalid_argument));
        checkpointer_.emplace(std::move(checkpointer));
        reclaiming_ = false;
        return {};
    }
    void unbind_checkpointer() noexcept {
        assert_current();
        checkpointer_.reset();
        reclaiming_ = false;
    }
    // Lets the bound checkpoint owner reclaim on its own: it is told when a
    // closed WAL file may go, and a rotation that meets the retained limit
    // waits for the one checkpoint it asks of it before it is refused.
    // Checkpoints then start unasked. Refused when anything the append path
    // draws on shares admission with a budget those runs draw on: the one
    // thing that frees the WAL would then wait on the appends it relieves.
    // A segment attached later is held to the same.
    [[nodiscard]] runtime::result<void> start_reclaiming() {
        assert_current();
        if (closing_ || closed_) return runtime::failure(error(errc::closed));
        if (!checkpointer_) return runtime::failure(error(errc::wrong_context));
        if (reclaiming_) return runtime::failure(error(errc::already_exists));
        if (
          reserved_from(budget_) || reserved_from(writer_.budget())
          || reserved_from(commit_.budget()))
            return runtime::failure(error(errc::invalid_argument));
        for (const auto& slot : slots_)
            if (slot.writer && reserved_from(slot.writer->budget()))
                return runtime::failure(error(errc::invalid_argument));
        reclaiming_ = true;
        reclaim_floor_.reset();
        return {};
    }
    // Their names, oldest first, as the WAL writer made each the head.
    [[nodiscard]] const storage::retained_wal& retained_wal() const& noexcept {
        assert_current();
        return writer_.retained();
    }
    const storage::retained_wal& retained_wal() const&& = delete;
    // The reclaimer removed the oldest of those files. Nothing forms here: a
    // rotation waiting for the reclaimer goes on when its run resolves, and
    // a refused one left no request forming.
    void wal_released() noexcept {
        assert_current();
        writer_.release_oldest();
        if (!writer_.retention().full()) pressure_.reset();
    }

    // Binds the environment's early stop notification. When it fires,
    // admission closes and forming requests form now; accepted work is not
    // failed and drains through close().
    [[nodiscard]] runtime::result<void>
    bind_shutdown(seastar::abort_source& abort) {
        assert_current();
        if (shutdown_bound_ || closing_ || closed_)
            return runtime::failure(error(errc::invalid_argument));
        shutdown_bound_ = true;
        if (abort.abort_requested()) {
            request_stop();
            return {};
        }
        shutdown_subscription_ = abort.subscribe(
          [this] noexcept { request_stop(); });
        if (!shutdown_subscription_ && abort.abort_requested()) request_stop();
        return {};
    }

    // Consumes the request, also on rejection. admission is borrowed until
    // accepted is ready; its cancellation can only reject before acceptance.
    [[nodiscard]] local_append_stages append(
      local_append_target target,
      local_append_request&& offered,
      codec::cooperative_work& admission) {
        assert_current();
        auto request = std::move(offered);
        seastar::promise<local_append_acceptance> accepted;
        auto accepted_future = accepted.get_future();
        try {
            auto result = accept_and_wait(
              target, std::move(request), admission, std::move(accepted));
            return {std::move(accepted_future), std::move(result)};
        } catch (...) {
            local_append_acceptance rejected;
            rejected.failure.observe(std::current_exception());
            return {
              seastar::make_ready_future<local_append_acceptance>(rejected),
              seastar::make_ready_future<local_append_outcome>(
                not_written(rejected.failure))};
        }
    }

    // The same, with a caller interest that can end before the result.
    template<runtime::timer_service Timer>
    [[nodiscard]] local_append_interest_stages append(
      local_append_target target,
      local_append_request&& offered,
      codec::cooperative_work& admission,
      local_append_interest<Timer> interest) {
        assert_current();
        auto request = std::move(offered);
        seastar::promise<local_append_acceptance> accepted;
        auto accepted_future = accepted.get_future();
        try {
            auto result = accept_and_observe(
              target,
              std::move(request),
              admission,
              std::move(accepted),
              interest);
            return {std::move(accepted_future), std::move(result)};
        } catch (...) {
            local_append_acceptance rejected;
            rejected.failure.observe(std::current_exception());
            return {
              seastar::make_ready_future<local_append_acceptance>(rejected),
              seastar::make_ready_future<runtime::result<local_append_outcome>>(
                not_written(rejected.failure))};
        }
    }

    // Stops acceptance, forms and drains every accepted request through both
    // barriers, reports those pressure leaves unformed as not_written, then
    // stops the barrier loops and joins all work. Repeated calls return the
    // first outcome.
    [[nodiscard]] seastar::future<runtime::result<void>> close() {
        assert_current();
        if (closed_) co_return close_outcome_.outcome();
        if (closing_) {
            co_await closed_event_.get_shared_future();
            co_return close_outcome_.outcome();
        }
        closing_ = true;
        try {
            // Acceptances in progress finish: rejected, or forming.
            co_await operations_.close();
            maybe_form();
            co_await drained_.wait(
              [this] { return formations_ == 0 && groups_ == 0; });
            // Pressure with nothing in flight left these unformed.
            if (!forming_.empty()) fail_forming(reason(error(errc::closed)));
        } catch (...) {
            close_outcome_.observe(std::current_exception());
        }
        for (auto& slot : slots_)
            stop_loop(slot);
        try {
            co_await tasks_.close();
        } catch (...) {
            close_outcome_.observe(std::current_exception());
        }
        shutdown_subscription_ = {};
        if (!close_outcome_.failed()) close_outcome_ = first_;
        closed_ = true;
        closed_event_.set_value();
        co_return close_outcome_.outcome();
    }

    [[nodiscard]] const runtime::first_failure& failure() const& noexcept {
        assert_current();
        return first_;
    }
    const runtime::first_failure& failure() const&& = delete;
    [[nodiscard]] std::size_t forming() const noexcept {
        assert_current();
        return forming_.size();
    }
    [[nodiscard]] std::uint32_t groups() const noexcept {
        assert_current();
        return groups_;
    }

private:
    struct group_state;
    // One segment's part of a group, with its own cooperative work.
    struct branch final {
        branch(
          group_state& owner,
          std::uint32_t index,
          codec::limits policy,
          seastar::abort_source& abort)
          : group(&owner)
          , slot(index)
          , work(policy, abort) {}
        // The group outlives its branches' settlement.
        group_state* group;
        std::uint32_t slot;
        codec::cooperative_work work;
        std::optional<segment_prepared_group> prepared;
        std::vector<detail::local_append_item_ptr> items;
        std::optional<segment_frozen_group> frozen;
        // The frozen group's cut: its blocks and footer.
        std::optional<segment_captured_boundary> boundary;
        std::optional<seastar::future<runtime::result<void>>> encoded;
        std::optional<seastar::future<segment_write_completion>> written;
        std::optional<segment_barrier_outcome> barrier;
        runtime::first_failure failure;
        // Its obligation row in the current WAL file, and whether the WAL
        // accepted its members so that an obligation is open.
        std::optional<std::uint32_t> row;
        bool obliged{false};
        // The next frozen group of the same segment, in file order. The group
        // owns its branches until each is published, which happens only
        // after its barrier loop has dequeued it.
        branch* next{nullptr};
        // offered: its members are in the WAL offer. queued: frozen and in
        // its segment's barrier queue. ready: submitted or dropped, so the
        // barrier loop may settle it. covered: settled by that loop.
        bool offered{false}, queued{false}, ready{false}, submitted{false},
          covered{false}, published{false};
    };
    using branch_ptr = seastar::lw_shared_ptr<branch>;
    struct segment_slot final {
        segment_type* writer{nullptr};
        // The attached segment's context and data device, fixed at attach.
        std::optional<segment_context> segment;
        device_store_id device;
        std::uint64_t attachment{0};
        // The footer of the newest group a barrier of this attachment covered.
        std::optional<local_durable_boundary> durable;
        // Completed-retry facts in canonical batch order, at most one per
        // retry entry the attachment's appends reserved, and the storage
        // those appends admitted for them.
        seastar::chunked_vector<completed_retry> facts;
        std::optional<workload_reservation> retry_memory;
        std::uint32_t retry_capacity{0};
        // Admission of the forming group, from the current reserved end.
        std::optional<segment_prepared_group> prepared;
        // Forming requests, and groups with unpublished members here.
        std::uint32_t forming{0}, groups{0};
        // A freeze moves the reserved end; preparations wait for it.
        bool freezing{false};
        seastar::condition_variable frozen;
        // Frozen groups in file order until their barrier settles them.
        branch* head{nullptr};
        branch* tail{nullptr};
        // The barrier loop waits here; only it waits, so a signal targets it.
        seastar::condition_variable wake;
        bool looping{false}, stop{false};
    };
    struct group_state final {
        group_state(
          workload_reservation reservation,
          std::vector<detail::local_append_item_ptr> members)
          : held(std::move(reservation))
          , items(std::move(members)) {}
        workload_reservation held;
        std::vector<detail::local_append_item_ptr> items;
        std::vector<branch_ptr> branches;
        std::optional<wal_commit_ticket> ticket;
        std::optional<seastar::future<wal_commit_result>> observed;
        // The WAL side, installed once: whether it accepted the group, and
        // then its receipt or failure. unknown: an exception left acceptance
        // unproven.
        runtime::first_failure wal_failure;
        std::optional<wal_durable_receipt> wal_receipt;
        bool wal_accepted{false}, wal_unknown{false}, wal_settled{false};
        // WAL order among accepted groups, where its PREPAREs begin, and its
        // branches whose obligation is still open.
        std::uint64_t sequence{0};
        std::optional<local_wal_cursor> wal_begin;
        std::uint32_t open{0};
        std::uint32_t unpublished{0};
        // Formation may continue; every member is published.
        seastar::promise<> submitted, published;
    };
    using group_ptr = seastar::lw_shared_ptr<group_state>;
    enum class formation : std::uint8_t { idle, formed, blocked, retry };
    // Per WAL file and segment: groups reserved by a forming group, groups
    // open until their segment group is durable, and groups pinned because it
    // failed. A row is free when all three are zero. It is keyed by segment
    // context, so a pinned row outlives its attachment.
    struct obligation_row final {
        std::optional<model::wal_incarnation_id> wal;
        std::optional<segment_context> segment;
        std::uint32_t reserved{0}, pending{0}, pinned{0};
        // How many of the pinned are groups pinned in this process, each
        // counted once whatever PREPAREs of the segment it holds.
        std::uint32_t live{0};
        // The oldest pinned group: its order among this owner's groups and
        // where it begins. A restored or replaced pin precedes every group.
        std::uint64_t first_pinned{0};
        std::optional<local_wal_cursor> pinned_begin;
    };
    static std::size_t obligation_rows(const local_append_config& config) {
        return std::size_t{config.maximum_requests} + config.maximum_segments;
    }

    local_append(
      workload_budget& budget,
      wal_group_commit& commit,
      wal_type& writer,
      local_append_config config,
      workload_reservation held,
      local_failure_sink sink,
      local_retention_sink pressure)
      : budget_(budget)
      , commit_(commit)
      , writer_(writer)
      , config_(config)
      , held_(std::move(held))
      , slots_(config.maximum_segments)
      , rows_(obligation_rows(config))
      , execution_(config.policy, abort_)
      , preparation_(config.policy, abort_)
      , sink_(std::move(sink))
      , pressure_sink_(std::move(pressure))
      , tasks_([this](std::exception_ptr failure) noexcept {
          first_.observe(std::move(failure));
      }) {
        forming_.reserve(maximum_wal_group_members);
        accepted_.reserve(config.maximum_requests);
    }

    static runtime::operation_error error(errc code) noexcept {
        return runtime::operation_error{code, runtime::operation_kind::file};
    }
    static runtime::operation_error interest_error(errc code) noexcept {
        return runtime::operation_error{code, runtime::operation_kind::timer};
    }
    static runtime::first_failure reason(runtime::operation_error failure) {
        runtime::first_failure output;
        output.observe(failure);
        return output;
    }
    static local_append_outcome not_written(runtime::first_failure failure) {
        return {local_append_status::not_written, std::move(failure), {}};
    }
    runtime::result<segment_slot*> find(local_append_target target) {
        if (
          target.slot_ >= slots_.size() || !slots_[target.slot_].writer
          || slots_[target.slot_].attachment != target.attachment_)
            return runtime::failure(error(errc::wrong_context));
        return &slots_[target.slot_];
    }

    void request_stop() noexcept {
        stopped_ = true;
        maybe_form();
    }

    // Latches the first storage failure and reports it once, after the
    // caller's state changes are complete.
    void observe_storage(
      local_storage_path path,
      const std::optional<segment_context>& segment,
      const runtime::first_failure& failure) noexcept {
        if (!failure.failed() || storage_failure_) return;
        auto detail = runtime::file_failure_detail::unknown;
        if (failure.error()) {
            if (auto found = runtime::file_detail(*failure.error()))
                detail = *found;
        }
        storage_failure_.emplace(
          local_storage_failure{path, segment, failure, detail});
        if (sink_) sink_(*storage_failure_);
    }
    // The WAL owners latch their own storage failures; this owner reports the
    // first. Every later group needs the same WAL, so this owner latches it
    // too and refuses later requests on entry.
    void observe_wal() noexcept {
        const auto& failed = commit_.storage_failure().failed()
                               ? commit_.storage_failure()
                               : writer_.failure();
        if (!failed.failed()) return;
        detail::merge_failure(first_, failed);
        observe_storage(local_storage_path::wal, std::nullopt, failed);
    }
    bool
    rotation_pressure(const runtime::operation_error& refused) const noexcept {
        return (refused.code() == errc::queue_full
                || refused.code() == errc::resource_exhausted)
               && !commit_.failure().failed() && !writer_.failure().failed();
    }
    void observe_segment(const segment_slot& slot) noexcept {
        if (slot.writer && slot.writer->failure().failed())
            observe_storage(
              local_storage_path::segment,
              slot.segment,
              slot.writer->failure());
    }

    // Reserves the obligation row of a WAL file and segment for a forming
    // group: before anything freezes, so a full table is only pressure.
    std::optional<std::uint32_t>
    reserve_row(model::wal_incarnation_id wal, const segment_context& segment) {
        std::optional<std::uint32_t> free;
        for (std::uint32_t i = 0; i != rows_.size(); ++i) {
            auto& row = rows_[i];
            if (!row.segment) {
                if (!free) free = i;
                continue;
            }
            if (*row.wal == wal && *row.segment == segment) {
                ++row.reserved;
                return i;
            }
        }
        if (free) {
            auto& row = rows_[*free];
            row.wal.emplace(wal);
            row.segment.emplace(segment);
            row.reserved = 1;
        }
        return free;
    }
    void free_row_if_unused(obligation_row& row) noexcept {
        if (row.reserved == 0 && row.pending == 0 && row.pinned == 0) {
            row.wal.reset();
            row.segment.reset();
            row.pinned_begin.reset();
        }
    }
    // Keeps the older of two WAL positions under their checked order.
    static runtime::result<void> keep_older(
      std::optional<local_wal_cursor>& oldest,
      const local_wal_cursor& begin,
      const local_store_context& owner) {
        if (oldest) {
            const auto order = begin.compare(owner, *oldest, owner);
            if (!order) return runtime::failure(error(errc::wrong_context));
            if (*order != std::strong_ordering::less) return {};
        }
        oldest.emplace(begin);
        return {};
    }
    // One segment's pinned PREPAREs across its rows, and where the oldest
    // begins.
    struct pinned_total final {
        std::uint64_t count{0}, live{0};
        std::optional<local_wal_cursor> oldest;
    };
    runtime::result<pinned_total> pinned_of(
      const segment_context& segment, const local_store_context& owner) const {
        pinned_total output;
        for (const auto& row : rows_) {
            if (!row.segment || *row.segment != segment || row.pinned == 0)
                continue;
            output.count += row.pinned;
            output.live += row.live;
            if (!row.pinned_begin) continue;
            if (
              auto kept = keep_older(output.oldest, *row.pinned_begin, owner);
              !kept)
                return runtime::failure(kept.error());
        }
        return output;
    }
    // Replaces a segment's pinned rows by one holding `remaining` PREPAREs,
    // the oldest beginning at `first`, or by none. What remains is older than
    // every group, so no later pin moves its beginning. The row is the
    // segment's in that WAL file, else one this releases, else a free one;
    // without any, nothing changes.
    runtime::result<void> replace_pinned(
      const segment_context& segment,
      std::uint32_t remaining,
      const std::optional<local_wal_cursor>& first) {
        std::optional<std::size_t> home;
        if (first) {
            std::optional<std::size_t> released, free;
            for (std::size_t i = 0; i != rows_.size() && !home; ++i) {
                const auto& row = rows_[i];
                if (!row.segment) {
                    if (!free) free = i;
                } else if (*row.segment != segment) {
                    continue;
                } else if (*row.wal == first->incarnation()) {
                    home = i;
                } else if (
                  !released && row.pinned != 0 && row.reserved == 0
                  && row.pending == 0) {
                    released = i;
                }
            }
            if (!home) home = released ? released : free;
            if (!home) return runtime::failure(error(errc::resource_exhausted));
        }
        for (auto& row : rows_) {
            if (!row.segment || *row.segment != segment || row.pinned == 0)
                continue;
            row.pinned = 0;
            row.live = 0;
            row.first_pinned = 0;
            row.pinned_begin.reset();
            free_row_if_unused(row);
        }
        if (first) {
            auto& row = rows_[*home];
            row.wal.emplace(first->incarnation());
            row.segment.emplace(segment);
            row.pinned = remaining;
            row.first_pinned = 0;
            row.pinned_begin.emplace(*first);
        }
        return {};
    }
    // The forming group did not open its obligation.
    void unreserve(branch& part) noexcept {
        if (!part.row) return;
        auto& row = rows_[*part.row];
        --row.reserved;
        free_row_if_unused(row);
        part.row.reset();
    }

    // Registers the waiter, or is ready when the outcome is already installed.
    static seastar::future<> wait_for(
      detail::local_append_item& item, detail::local_append_waiter& waiter) {
        if (item.outcome) return seastar::make_ready_future<>();
        item.waiter = &waiter;
        return waiter.settled.get_future();
    }

    // Accepts, then waits for the outcome. The gate is held only while
    // accepting; group work settles every accepted outcome.
    seastar::future<local_append_outcome> accept_and_wait(
      local_append_target target,
      local_append_request request,
      codec::cooperative_work& admission,
      seastar::promise<local_append_acceptance> accepted) {
        std::optional<detail::local_append_item_ptr> item;
        local_append_acceptance decision;
        try {
            // After close() a request rejects as closed, never with the gate.
            if (auto holder = operations_.try_hold())
                item = co_await accept(
                  target,
                  std::move(request),
                  admission,
                  decision,
                  nullptr,
                  std::nullopt);
            else
                decision.failure.observe(error(errc::closed));
        } catch (...) {
            decision.failure.observe(std::current_exception());
        }
        accepted.set_value(decision);
        if (!item) co_return not_written(decision.failure);
        auto* owner = item->get();
        detail::local_append_waiter waiter;
        auto registered = seastar::defer([owner, &waiter] noexcept {
            if (owner->waiter == &waiter) owner->waiter = nullptr;
        });
        auto published = wait_for(*owner, waiter);
        maybe_form();
        co_await std::move(published);
        co_return std::move(*owner->outcome);
    }

    // As accept_and_wait, but the first terminal state wins between the
    // publication and the end of the caller's interest. The losing timer is
    // joined and the subscription removed before this frame ends.
    template<runtime::timer_service Timer>
    seastar::future<runtime::result<local_append_outcome>> accept_and_observe(
      local_append_target target,
      local_append_request request,
      codec::cooperative_work& admission,
      seastar::promise<local_append_acceptance> accepted,
      local_append_interest<Timer> interest) {
        std::optional<detail::local_append_item_ptr> item;
        local_append_acceptance decision;
        try {
            if (auto holder = operations_.try_hold())
                item = co_await accept(
                  target,
                  std::move(request),
                  admission,
                  decision,
                  &interest.abort,
                  interest.deadline);
            else
                decision.failure.observe(error(errc::closed));
        } catch (...) {
            decision.failure.observe(std::current_exception());
        }
        accepted.set_value(decision);
        if (!item) co_return not_written(decision.failure);
        auto* owner = item->get();
        detail::local_append_waiter waiter;
        auto registered = seastar::defer([owner, &waiter] noexcept {
            if (owner->waiter == &waiter) owner->waiter = nullptr;
        });
        auto published = wait_for(*owner, waiter);
        maybe_form();
        if (!published.available()) {
            const auto end =
              [owner, &waiter](const runtime::first_failure& why) noexcept {
                  if (owner->waiter != &waiter) return;
                  owner->waiter = nullptr;
                  waiter.ended = why;
                  waiter.settled.set_value();
              };
            seastar::abort_source timer_abort;
            auto subscription = interest.abort.subscribe(
              [&end] noexcept { end(reason(interest_error(errc::aborted))); });
            if (!subscription) end(reason(interest_error(errc::aborted)));
            std::optional<seastar::future<>> timeout;
            if (interest.deadline)
                timeout.emplace(interest_deadline(
                  interest.timer, *interest.deadline, timer_abort, end));
            co_await std::move(published);
            timer_abort.request_abort();
            if (timeout) co_await std::move(*timeout);
        }
        if (waiter.ended.exception())
            std::rethrow_exception(waiter.ended.exception());
        if (waiter.ended.error())
            co_return runtime::failure(*waiter.ended.error());
        co_return std::move(*owner->outcome);
    }

    template<runtime::timer_service Timer, typename End>
    static seastar::future<> interest_deadline(
      Timer& timer,
      runtime::monotonic_time deadline,
      seastar::abort_source& timer_abort,
      const End& end) {
        runtime::first_failure why;
        try {
            const auto slept = co_await timer.sleep_until(
              deadline, timer_abort);
            if (timer_abort.abort_requested()) co_return;
            if (slept)
                why.observe(interest_error(errc::timed_out));
            else
                why.observe(slept.error());
        } catch (...) {
            if (timer_abort.abort_requested()) co_return;
            why.observe(std::current_exception());
        }
        end(why);
    }

    seastar::future<std::optional<detail::local_append_item_ptr>> accept(
      local_append_target target,
      local_append_request request,
      codec::cooperative_work& admission,
      local_append_acceptance& decision,
      const seastar::abort_source* interest,
      std::optional<runtime::monotonic_time> deadline) {
        using output = std::optional<detail::local_append_item_ptr>;
        const auto reject = [&decision](runtime::operation_error failure) {
            decision.failure.observe(failure);
            return output{};
        };
        // A caller whose interest ended is rejected while that has no effect.
        const auto ended =
          [interest, deadline]() -> std::optional<runtime::operation_error> {
            if (interest && interest->abort_requested())
                return interest_error(errc::aborted);
            if (deadline && Clock::now() >= *deadline)
                return interest_error(errc::timed_out);
            return std::nullopt;
        };
        if (closing_ || closed_ || stopped_ || first_.failed())
            co_return reject(error(errc::closed));
        if (auto attached = find(target); !attached)
            co_return reject(attached.error());
        if (admission.policy() != config_.policy)
            co_return reject(error(errc::invalid_argument));
        if (auto over = ended()) co_return reject(*over);
        if (
          requests_ >= config_.maximum_requests
          || forming_.size() >= maximum_wal_group_members)
            co_return reject(error(errc::queue_full));
        const auto cost = budget_.allocation_charge(
          byte_count{sizeof(detail::local_append_item) + 64});
        if (!cost) co_return reject(cost.error());
        auto held = budget_.try_reserve(
          byte_count{cost->value() + config_.execution_bytes.value()});
        if (!held) co_return reject(held.error());
        // Storage for the batch's completed-retry fact, with the geometric
        // spare of its segment's growing fact set.
        const auto fact_cost = budget_.allocation_charge(
          byte_count{2 * sizeof(completed_retry)});
        if (!fact_cost) co_return reject(fact_cost.error());
        auto fact = budget_.try_reserve(*fact_cost);
        if (!fact) co_return reject(fact.error());
        ++requests_;
        auto counted = seastar::defer([this] noexcept { --requests_; });
        // Requests are accepted in call order: this FIFO wait is entered
        // before append() returns and held until the request has joined or
        // been rejected. A waiting request already counts and holds its
        // charge.
        auto ordered = co_await seastar::get_units(accept_order_, 1);
        if (closing_ || closed_ || stopped_ || first_.failed())
            co_return reject(error(errc::closed));
        auto slot = find(target);
        if (!slot) co_return reject(slot.error());
        if (auto over = ended()) co_return reject(*over);
        auto child = admitted_wal_batch::make(
          std::move(request.batch),
          std::move(request.backing),
          writer_.child_limits().charge);
        if (!child) co_return reject(child.error());
        auto expected = child_expectation(**slot, request);
        if (!expected) co_return reject(expected.error());
        // Exact-child validation against the supplied identity, and the
        // segment alias, before any owner state changes.
        auto prepared = co_await prepare_wal_children(
          std::move(*child),
          *expected,
          budget_,
          writer_.child_limits(),
          admission);
        if (!prepared) co_return reject(prepared.error());
        if (auto ready = admission.poll(); !ready)
            co_return reject(error(ready.error().code()));
        for (;;) {
            if (closing_ || closed_ || stopped_ || first_.failed())
                co_return reject(error(errc::closed));
            slot = find(target);
            if (!slot) co_return reject(slot.error());
            if (!(*slot)->freezing) break;
            co_await (*slot)->frozen.when();
        }
        if (auto over = ended()) co_return reject(*over);
        if (forming_.size() >= maximum_wal_group_members)
            co_return reject(error(errc::queue_full));
        auto item = seastar::make_lw_shared<detail::local_append_item>(
          std::move(*held),
          std::move(*fact),
          std::move(*prepared),
          target.slot_,
          Clock::now());
        auto joined = join(**slot, *item, request.physical_begin, decision);
        if (!joined) co_return reject(joined.error());
        if (!decision.accepted()) co_return output{};
        counted.cancel();
        forming_.push_back(item);
        ++(*slot)->forming;
        co_return output{std::move(item)};
    }

    // The WAL expectation at the segment's current reserved end. The group's
    // freeze replaces the target with the member's final block position.
    runtime::result<wal_child_expectation>
    child_expectation(segment_slot& slot, const local_append_request& request) {
        auto current = slot.writer->capture();
        if (!current) return runtime::failure(current.error());
        const auto history = current->history();
        auto target = segment_write_context::make(
          history.segment,
          history.alignment,
          current->end().physical,
          current->end().bytes);
        if (!target) return runtime::failure(error(errc::invalid_argument));
        return wal_child_expectation{
          *target,
          history.data_start,
          request.routing_epoch,
          request.expected,
          writer_.profile(),
          history.profile};
    }

    // Adds item to its segment's forming group in one synchronous step. One
    // preparation covers every forming member of the segment, and those
    // members must fit one WAL group. A rejection restores the previous
    // preparation, which the just-released allowance funds.
    runtime::result<void> join(
      segment_slot& slot,
      const detail::local_append_item& item,
      std::optional<model::segment_relative_end> physical_begin,
      local_append_acceptance& decision) {
        std::array<const encoded_assigned_batch*, maximum_segment_group_blocks>
          children{};
        std::array<wal_admission_member, maximum_wal_group_members> members{};
        std::size_t count = 0;
        auto current = slot.writer->capture();
        if (!current) return runtime::failure(current.error());
        auto physical = current->end().physical;
        for (const auto& forming : forming_) {
            if (forming->slot != item.slot) continue;
            children[count] = &forming->children.segment.batch();
            members[count] = {
              &forming->children.wal.batch(), &forming->children.expected};
            ++count;
            const auto next = physical.checked_add(
              model::segment_record_count{
                forming->info.context.retained_count().value()});
            if (!next) return runtime::failure(error(errc::out_of_range));
            physical = *next;
        }
        if (count == maximum_segment_group_blocks)
            return runtime::failure(error(errc::queue_full));
        if (physical_begin && *physical_begin != physical)
            return runtime::failure(error(errc::wrong_context));
        children[count] = &item.children.segment.batch();
        members[count] = {&item.children.wal.batch(), &item.children.expected};
        ++count;
        auto measured = writer_.measure_submission(
          std::span{members}.first(count),
          config_.policy,
          commit_.submission_limits());
        if (!measured) return runtime::failure(measured.error());
        if (measured->decision == wal_admission_decision::too_large) {
            decision.capacity = segment_capacity_decision::impossible;
            return {};
        }
        slot.prepared.reset();
        auto next = slot.writer->prepare_group(
          std::span{children}.first(count), preparation_);
        if (next && next->decision == segment_capacity_decision::fits) {
            slot.prepared.emplace(std::move(*next->prepared));
            decision.capacity = segment_capacity_decision::fits;
            return {};
        }
        if (count > 1) {
            auto restored = slot.writer->prepare_group(
              std::span{children}.first(count - 1), preparation_);
            if (
              restored && restored->decision == segment_capacity_decision::fits)
                slot.prepared.emplace(std::move(*restored->prepared));
        }
        if (!next) return runtime::failure(next.error());
        decision.capacity = next->decision;
        return {};
    }

    // Starts one formation unless one is pending. It runs after the one in
    // progress, so the next group keeps forming meanwhile.
    void maybe_form() noexcept {
        if (form_pending_ || forming_.empty() || closed_) return;
        form_pending_ = true;
        ++formations_;
        std::optional<seastar::future<>> work;
        try {
            work.emplace(form());
        } catch (...) {
            --formations_;
            form_pending_ = false;
            runtime::first_failure failed;
            failed.observe(std::current_exception());
            fail_forming(failed);
            drained_.broadcast();
            return;
        }
        // The formation already runs and counts itself; the scope only joins
        // it. If the scope cannot take it, close still waits for its count.
        try {
            static_cast<void>(tasks_.spawn(
              [work = std::move(*work)]() mutable { return std::move(work); }));
        } catch (...) {
        }
    }

    seastar::future<> form() {
        auto finished = seastar::defer([this] noexcept {
            --formations_;
            drained_.broadcast();
        });
        std::optional<seastar::semaphore_units<>> units;
        try {
            units.emplace(co_await seastar::get_units(form_lock_, 1));
        } catch (...) {
            form_pending_ = false;
            runtime::first_failure failed;
            failed.observe(std::current_exception());
            fail_forming(failed);
            co_return;
        }
        form_pending_ = false;
        auto step = formation::idle;
        try {
            for (bool waited = false;; waited = true) {
                do {
                    step = co_await form_once();
                } while (step == formation::formed || step == formation::retry);
                if (step != formation::blocked || groups_ != 0 || waited) break;
                // With nothing in flight, a segment's pressure clears only as
                // its deferred digest releases earlier groups. Wait for that
                // once; the requests stay forming meanwhile.
                for (auto& slot : slots_)
                    if (slot.writer && slot.forming != 0)
                        static_cast<void>(
                          co_await slot.writer->digest_caught_up());
            }
        } catch (...) {
            // Formation fails before anything freezes: the forming requests
            // have no effect and the owner continues. A failed WAL owner is
            // latched by its own check.
            runtime::first_failure failed;
            failed.observe(std::current_exception());
            fail_forming(failed);
            observe_wal();
            co_return;
        }
        // Pressure that outlasts the digest, with nothing in flight, has no
        // completion to retry on.
        if (step == formation::blocked && groups_ == 0)
            fail_forming(reason(error(errc::queue_full)));
    }

    // Installs one member's outcome and releases its request bound. Its
    // waiter is woken separately, after every related state change.
    void install(
      detail::local_append_item& item, local_append_outcome value) noexcept {
        KWAQUE_INVARIANT(
          invariant_id{"KQ-LOCAL-APPEND-PUBLISH-ONCE"},
          !item.outcome,
          "local append result published twice");
        item.outcome.emplace(std::move(value));
        --requests_;
    }
    static void wake(detail::local_append_item& item) noexcept {
        if (auto* waiter = std::exchange(item.waiter, nullptr))
            waiter->settled.set_value();
    }

    void fail_forming(const runtime::first_failure& failure) noexcept {
        for (auto& slot : slots_) {
            slot.forming = 0;
            slot.prepared.reset();
        }
        // Waking only schedules the waiters, so forming_ stays unchanged
        // until it is cleared; clearing keeps its admitted capacity.
        for (auto& item : forming_)
            install(*item, not_written(failure));
        for (auto& item : forming_)
            wake(*item);
        forming_.clear();
    }
    void fail_segment(
      std::uint32_t index, const runtime::first_failure& failure) noexcept {
        slots_[index].forming = 0;
        slots_[index].prepared.reset();
        for (auto& item : forming_)
            if (item->slot == index) install(*item, not_written(failure));
        for (auto& item : forming_)
            if (item->slot == index) wake(*item);
        std::erase_if(
          forming_, [index](const auto& item) { return item->slot == index; });
    }

    // Whether `budget` shares admission with one the checkpoint owner's runs
    // draw on.
    [[nodiscard]] bool
    reserved_from(const workload_budget& budget) const noexcept {
        return std::ranges::any_of(
          checkpointer_->reserve, [&budget](const workload_budget* reserve) {
              return budget.shares_admission_with(*reserve);
          });
    }
    // Tells whoever reclaims the WAL that a closed file may go: the oldest
    // obligation lies in a later file than when it was last told, or nothing
    // is open below the head. It is asked only while the shard holds a file
    // besides the head, and once per file the obligation moves to.
    void offer_reclaim() noexcept {
        if (!reclaiming_ || writer_.retention().files < 2) return;
        const auto held = obligations([](const local_obligation&) {});
        if (!held) return;
        const auto floor = held->discharged.incarnation();
        if (reclaim_floor_ && *reclaim_floor_ == floor) return;
        reclaim_floor_.emplace(floor);
        checkpointer_->reclaimable();
    }
    // Has whoever reclaims the WAL run one checkpoint, and waits for it. The
    // error is why that run removed nothing more; with nobody to reclaim,
    // nothing can remove a file and there is nothing to wait for.
    seastar::future<std::optional<runtime::operation_error>>
    relieve_retention() {
        using output = std::optional<runtime::operation_error>;
        if (!reclaiming_) co_return output{error(errc::closed)};
        try {
            const auto relieved = co_await checkpointer_->relieve();
            if (!relieved) co_return output{relieved.error()};
        } catch (...) {
            co_return output{error(errc::io_failure)};
        }
        co_return output{};
    }
    // What keeps the shard's closed WAL files from going, as far as this
    // owner knows: the oldest pinned PREPARE below the head's file, which
    // someone must resolve; or, with none, whether a group still in flight
    // has its PREPAREs there, which settles by itself.
    struct closed_file_holder final {
        std::optional<segment_context> segment;
        std::optional<local_wal_cursor> first;
        bool settling{false};
    };
    [[nodiscard]] closed_file_holder closed_files_held() const {
        closed_file_holder held;
        const auto wal = writer_.progress();
        if (!wal) return held;
        const auto head = wal->reserved.incarnation();
        for (const auto& row : rows_) {
            if (
              row.pinned == 0 || !row.pinned_begin
              || row.pinned_begin->incarnation() == head)
                continue;
            const auto before = held.first;
            if (!keep_older(held.first, *row.pinned_begin, wal->owner))
                continue;
            if (held.first != before) held.segment = row.segment;
        }
        if (held.first) return held;
        for (const auto* group : accepted_) {
            if (group->open == 0) continue;
            held.settling = group->wal_begin
                            && !(group->wal_begin->incarnation() == head);
            break;
        }
        return held;
    }
    // The rotation stays refused: the forming requests are rejected
    // unwritten with the retained count and limit, and whoever can resolve
    // what holds the WAL is told which segment that is and where its oldest
    // unresolved PREPARE begins. Nothing is latched; the next request tries
    // again.
    void refuse_retention(
      const closed_file_holder& holder,
      std::optional<runtime::operation_error> reclaim) {
        const auto held = writer_.retention();
        auto refused = error(errc::resource_exhausted);
        static_cast<void>(refused.add_context(
          runtime::operation_context_key::limit, held.most()));
        static_cast<void>(refused.add_context(
          runtime::operation_context_key::actual, held.files));
        pressure_.emplace(
          local_retention_pressure{
            held.files,
            held.most(),
            holder.segment,
            holder.first,
            std::move(reclaim)});
        fail_forming(reason(refused));
        if (pressure_sink_) pressure_sink_(*pressure_);
    }

    // One group: segments in the order of their oldest forming member, and
    // as many whole forming sets as the current WAL file and group bounds
    // admit. A set never splits: its one preparation reserves one footer.
    // Moves the WAL to its next file. retry: it moved. blocked: the rotation
    // was refused while both WAL owners stayed healthy, as when the
    // control's turns are taken, or the retained limit holds only until a
    // group in flight settles; that is pressure, and the attempt is made
    // again. idle: it failed, and so has this owner; or the retained limit
    // held it, the forming requests were rejected and nothing failed.
    seastar::future<formation> rotate(byte_count required) {
        // A new file at the retained limit. Only a checkpoint can remove
        // one, so whoever reclaims the WAL runs one first; the rotation is
        // refused only when the limit still holds after it. A rotation the
        // WAL writer keeps pending owns its successor already.
        if (!writer_.rotation_pending() && writer_.retention().full()) {
            auto reclaim = co_await relieve_retention();
            if (forming_.empty()) co_return formation::idle;
            if (writer_.retention().full()) {
                const auto holder = closed_files_held();
                // Only a group still in flight keeps the oldest closed file.
                // Its barrier settles without anyone's decision, so the
                // forming requests wait for it as they wait for any
                // pressure a completion clears.
                if (holder.settling && groups_ != 0)
                    co_return formation::blocked;
                refuse_retention(holder, std::move(reclaim));
                co_return formation::idle;
            }
        }
        auto rotated = co_await commit_.rotate(writer_, required, execution_);
        if (rotated) {
            rotation_bytes_.reset();
            pressure_.reset();
            offer_reclaim();
            co_return formation::retry;
        }
        if (rotation_pressure(rotated.error())) {
            rotation_bytes_ = required;
            co_return formation::blocked;
        }
        first_.observe(rotated.error());
        fail_forming(reason(rotated.error()));
        observe_wal();
        co_return formation::idle;
    }

    seastar::future<formation> form_once() {
        if (forming_.empty()) co_return formation::idle;
        // A rotation the WAL writer keeps pending is finished first, with
        // the size it was first asked with: the file it is leaving takes
        // nothing more, whatever would still fit there.
        if (!writer_.rotation_pending()) {
            rotation_bytes_.reset();
        } else if (rotation_bytes_) {
            const auto resumed = co_await rotate(*rotation_bytes_);
            if (resumed != formation::retry) co_return resumed;
            if (forming_.empty()) co_return formation::idle;
        }
        std::array<wal_admission_member, maximum_wal_group_members> members{};
        std::vector<std::uint32_t> chosen;
        std::size_t count = 0;
        bool pressure = false;
        for (std::size_t i = 0; i < forming_.size(); ++i) {
            const auto index = forming_[i]->slot;
            bool seen = false;
            for (std::size_t j = 0; j < i && !seen; ++j)
                seen = forming_[j]->slot == index;
            if (seen) continue;
            auto& slot = slots_[index];
            if (!slot.prepared) {
                fail_segment(index, reason(error(errc::closed)));
                co_return formation::retry;
            }
            if (
              auto ready = slot.writer->freeze_admission(*slot.prepared);
              !ready) {
                if (ready.error().code() == errc::queue_full) {
                    pressure = true;
                    continue;
                }
                fail_segment(index, reason(ready.error()));
                co_return formation::retry;
            }
            auto trial = members;
            auto size = count;
            for (const auto& item : forming_)
                if (item->slot == index)
                    trial[size++] = {
                      &item->children.wal.batch(), &item->children.expected};
            auto measured = writer_.measure_submission(
              std::span{trial}.first(size),
              config_.policy,
              commit_.submission_limits());
            if (!measured) {
                fail_segment(index, reason(measured.error()));
                co_return formation::retry;
            }
            if (measured->decision == wal_admission_decision::fits) {
                chosen.push_back(index);
                members = trial;
                count = size;
                continue;
            }
            if (!chosen.empty()) break;
            if (measured->decision == wal_admission_decision::rotate_required)
                co_return co_await rotate(measured->encoded_bytes);
            fail_segment(index, reason(error(errc::resource_exhausted)));
            co_return formation::retry;
        }
        if (chosen.empty())
            co_return pressure ? formation::blocked : formation::idle;
        // WAL members in acceptance order across the chosen segments.
        const auto included = [&chosen](const auto& item) {
            return std::find(chosen.begin(), chosen.end(), item->slot)
                   != chosen.end();
        };
        std::vector<detail::local_append_item_ptr> items;
        items.reserve(count);
        auto origin = Clock::now();
        count = 0;
        for (const auto& item : forming_)
            if (included(item)) {
                members[count++] = {
                  &item->children.wal.batch(), &item->children.expected};
                items.push_back(item);
                origin = std::min(origin, item->accepted);
            }
        auto admitted = commit_.template admit<Clock>(
          writer_, std::span{members}.first(count), config_.policy, origin);
        if (!admitted) {
            if (admitted.error().code() == errc::queue_full)
                co_return formation::blocked;
            first_.observe(admitted.error());
            fail_forming(reason(admitted.error()));
            observe_wal();
            co_return formation::idle;
        }
        if (!admitted->admission) co_return formation::blocked;
        auto offer = wal_group::make(
          budget_, static_cast<std::uint32_t>(count));
        if (!offer) {
            if (offer.error().code() == errc::queue_full)
                co_return formation::blocked;
            fail_forming(reason(offer.error()));
            co_return formation::idle;
        }
        auto group = make_group(chosen, std::move(items));
        if (!group) {
            if (group.error().code() == errc::queue_full)
                co_return formation::blocked;
            fail_forming(reason(group.error()));
            co_return formation::idle;
        }
        std::erase_if(forming_, included);
        // The group's task exists before anything freezes, so failing to
        // start it can only report not_written.
        seastar::promise<runtime::first_failure> start;
        std::optional<seastar::future<>> task;
        try {
            task.emplace(run_group(
              *group,
              start.get_future(),
              std::move(*offer),
              std::move(*admitted->admission)));
        } catch (...) {
            runtime::first_failure failed;
            failed.observe(std::current_exception());
            unwind_group(**group, failed);
            co_return formation::idle;
        }
        // A refused spawn hands the task its reason: a typed refusal or a
        // native allocation failure, which the group unwinds with.
        runtime::first_failure refused;
        try {
            if (
              auto taken = tasks_.spawn([work = std::move(*task)]() mutable {
                  return std::move(work);
              });
              !taken)
                refused.observe(taken.error());
        } catch (...) {
            refused.observe(std::current_exception());
        }
        const bool spawned = !refused.failed();
        auto submitted = (*group)->submitted.get_future();
        start.set_value(std::move(refused));
        // Freezes and WAL submissions stay serialized under the lock.
        co_await std::move(submitted);
        co_return spawned ? formation::formed : formation::idle;
    }

    // Moves the chosen forming sets and their preparations into a group. The
    // chosen segments count as freezing until their freeze settles.
    runtime::result<group_ptr> make_group(
      const std::vector<std::uint32_t>& chosen,
      std::vector<detail::local_append_item_ptr> items) {
        const auto state = budget_.allocation_charge(
          byte_count{
            sizeof(group_state) + 64
            + items.size() * 2 * sizeof(detail::local_append_item_ptr)
            + chosen.size() * (sizeof(branch) + 64 + sizeof(branch_ptr))});
        if (!state) return runtime::failure(state.error());
        // The group's task runs in one execution allowance.
        auto held = budget_.try_reserve(
          byte_count{state->value() + config_.execution_bytes.value()});
        if (!held) return runtime::failure(held.error());
        const auto wal = writer_.positions();
        if (!wal) return runtime::failure(wal.error());
        // Every allocation first: a failure here leaves no trace.
        auto group = seastar::make_lw_shared<group_state>(
          std::move(*held), std::move(items));
        group->branches.reserve(chosen.size());
        for (const auto index : chosen) {
            auto part = seastar::make_lw_shared<branch>(
              *group, index, config_.policy, abort_);
            part->items.reserve(
              static_cast<std::size_t>(std::count_if(
                group->items.begin(),
                group->items.end(),
                [index](const auto& item) { return item->slot == index; })));
            group->branches.push_back(std::move(part));
        }
        // One obligation row per chosen segment in the current WAL file,
        // reserved before anything changes; a full table is only pressure.
        std::array<std::uint32_t, maximum_wal_group_members> rows{};
        for (std::size_t b = 0; b < chosen.size(); ++b) {
            auto row = reserve_row(
              wal->reserved.incarnation(), *slots_[chosen[b]].segment);
            if (!row) {
                for (std::size_t r = 0; r < b; ++r) {
                    --rows_[rows[r]].reserved;
                    free_row_if_unused(rows_[rows[r]]);
                }
                return runtime::failure(error(errc::queue_full));
            }
            rows[b] = *row;
        }
        // Then the transfer, which cannot fail.
        for (std::uint32_t b = 0; b < chosen.size(); ++b) {
            const auto index = chosen[b];
            auto& part = *group->branches[b];
            part.row = rows[b];
            auto& slot = slots_[index];
            if (slot.prepared) part.prepared.emplace(std::move(*slot.prepared));
            slot.prepared.reset();
            for (const auto& item : group->items)
                if (item->slot == index) {
                    item->branch = b;
                    part.items.push_back(item);
                }
            slot.forming = 0;
            slot.freezing = true;
            ++slot.groups;
        }
        group->unpublished = static_cast<std::uint32_t>(chosen.size());
        ++groups_;
        return group;
    }

    // Nothing froze: every member is not_written, and formation continues.
    void
    unwind_group(group_state& group, const runtime::first_failure& failure) {
        for (auto& part : group.branches) {
            auto& slot = slots_[part->slot];
            slot.freezing = false;
            slot.frozen.broadcast();
            part->prepared.reset();
            unreserve(*part);
            detail::merge_failure(part->failure, failure);
        }
        group.wal_settled = true;
        for (auto& part : group.branches)
            publish_branch(group, *part);
        group.submitted.set_value();
        release(group);
    }

    // The one task of a formed group. Under the formation lock it freezes the
    // segments, offers the members at their final positions and has the WAL
    // accept them. Then it submits each segment group once encoded, without
    // waiting for the WAL barrier, settles the WAL result and waits until
    // every member is published. Its segments' barrier loops settle the rest.
    seastar::future<> run_group(
      group_ptr group,
      seastar::future<runtime::first_failure> start,
      wal_group offer,
      wal_commit_admission admission) {
        runtime::first_failure refused;
        try {
            refused = co_await std::move(start);
        } catch (...) {
            refused.observe(std::current_exception());
        }
        if (refused.failed()) {
            unwind_group(*group, refused);
            co_return;
        }
        co_await submit_group(*group, std::move(offer), std::move(admission));
        group->submitted.set_value();
        for (auto& part : group->branches) {
            if (!part->queued || part->ready) continue;
            if (group->wal_accepted)
                co_await submit_branch(*part);
            else
                co_await drop_branch(*part);
        }
        if (group->wal_accepted) co_await settle_wal(*group);
        if (group->unpublished != 0) co_await group->published.get_future();
        for (auto& part : group->branches)
            co_await join_write(*part);
        release(*group);
        maybe_form();
    }

    // Freezes each segment, offers the members at their final positions and
    // has the WAL accept them. Encoding starts at freeze. Branches that did
    // not freeze are published here; the rest wait for the WAL decision.
    seastar::future<> submit_group(
      group_state& group, wal_group offer, wal_commit_admission admission) {
        runtime::first_failure refused;
        bool unknown = false;
        std::optional<wal_commit_ticket> ticket;
        try {
            for (auto& part : group.branches) {
                co_await freeze(*part);
                auto& slot = slots_[part->slot];
                slot.freezing = false;
                slot.frozen.broadcast();
            }
            for (const auto& item : group.items) {
                auto& part = *group.branches[item->branch];
                if (!part.queued) continue;
                if (
                  auto added = offer.append(
                    std::move(item->children.wal), item->children.expected);
                  !added)
                    refused.observe(added.error());
                part.offered = true;
            }
            // The sole submitter: the group's PREPAREs begin at the reserved
            // end, so every group the WAL accepts knows where.
            auto wal = writer_.positions();
            if (!wal)
                refused.observe(wal.error());
            else if (!refused.failed() && offer.size() != 0) {
                group.wal_begin.emplace(wal->reserved);
                // The WAL may hold these PREPAREs before its answer is
                // installed below; until then the snapshot counts them open.
                submitting_.emplace(wal->reserved);
                try {
                    auto accepted = co_await commit_.template submit<Clock>(
                      writer_,
                      std::move(offer),
                      std::move(admission),
                      execution_);
                    if (accepted)
                        ticket.emplace(std::move(*accepted));
                    else
                        refused.observe(accepted.error());
                } catch (...) {
                    // The WAL may have accepted before the exception.
                    refused.observe(std::current_exception());
                    unknown = true;
                }
            }
        } catch (...) {
            refused.observe(std::current_exception());
        }
        for (auto& part : group.branches) {
            auto& slot = slots_[part->slot];
            if (slot.freezing) {
                slot.freezing = false;
                slot.frozen.broadcast();
            }
        }
        if (ticket) {
            // The result observer uses the group's pre-admitted slot.
            auto observed = ticket->observe();
            if (observed) group.observed.emplace(std::move(*observed));
            group.ticket.emplace(std::move(*ticket));
            group.wal_accepted = true;
            group.sequence = ++sequences_;
            accepted_.push_back(&group);
            // The WAL file now holds every frozen segment group's PREPAREs:
            // each is an obligation until its segment group is durable.
            for (auto& part : group.branches) {
                if (!part->queued) {
                    unreserve(*part);
                    continue;
                }
                auto& row = rows_[*part->row];
                --row.reserved;
                ++row.pending;
                part->obliged = true;
                ++group.open;
            }
        } else {
            // No PREPARE was accepted; frozen groups are dropped unwritten.
            // Without that proof a PREPARE may exist, and its obligation is
            // pinned until recovery resolves it.
            detail::merge_failure(first_, refused);
            group.wal_failure = refused;
            group.wal_unknown = unknown;
            group.wal_settled = true;
            if (unknown) group.sequence = ++sequences_;
            for (auto& part : group.branches) {
                if (unknown && part->queued && part->row) {
                    auto& row = rows_[*part->row];
                    --row.reserved;
                    pin(row, group.sequence, group.wal_begin);
                    part->row.reset();
                } else {
                    unreserve(*part);
                }
            }
        }
        submitting_.reset();
        for (auto& part : group.branches)
            if (!part->queued) publish_branch(group, *part);
        // Reported once every decision above is installed.
        if (!ticket && refused.failed()) observe_wal();
        for (auto& part : group.branches)
            if (part->failure.failed()) observe_segment(slots_[part->slot]);
    }

    void
    adopt_fact(segment_slot& slot, detail::local_append_item& item) noexcept {
        if (!item.fact) return;
        if (slot.retry_memory) {
            const auto adopted = slot.retry_memory->adopt(
              std::move(*item.fact));
            KWAQUE_INVARIANT(
              invariant_id{"KQ-LOCAL-APPEND-FACT-GRANT"},
              adopted.has_value(),
              "checked fact grant was not adopted");
        } else {
            slot.retry_memory.emplace(std::move(*item.fact));
        }
        item.fact.reset();
        ++slot.retry_capacity;
    }

    void pin(
      obligation_row& row,
      std::uint64_t sequence,
      const std::optional<local_wal_cursor>& begin) noexcept {
        ++row.pinned;
        ++row.live;
        if (begin && (!row.pinned_begin || sequence < row.first_pinned)) {
            row.first_pinned = sequence;
            row.pinned_begin.emplace(*begin);
        }
    }
    // Discharged once the segment group is durable, whatever the WAL result;
    // otherwise pinned. Settles once.
    void settle_obligation(group_state& group, branch& part) noexcept {
        if (!part.obliged) return;
        auto& row = rows_[*part.row];
        --row.pending;
        if (!segment_durable(part)) pin(row, group.sequence, group.wal_begin);
        free_row_if_unused(row);
        part.obliged = false;
        --group.open;
    }
    static bool segment_durable(const branch& part) noexcept {
        return part.submitted && part.barrier && !part.barrier->failure.failed()
               && part.barrier->receipt;
    }

    // Freezes one segment's forming set at the positions its preparation
    // measured, gives each member's WAL expectation its final block target,
    // queues the group for its segment's barrier in file order and starts
    // encoding.
    seastar::future<> freeze(branch& part) {
        auto& slot = slots_[part.slot];
        std::optional<runtime::result<segment_frozen_group>> frozen;
        if (!part.prepared) {
            part.failure.observe(error(errc::closed));
            co_return;
        }
        try {
            std::vector<admitted_wal_batch> children;
            children.reserve(part.items.size());
            for (auto& item : part.items)
                children.push_back(std::move(item->children.segment));
            frozen.emplace(
              co_await slot.writer->freeze_group(
                std::move(*part.prepared), std::move(children), execution_));
        } catch (...) {
            part.failure.observe(std::current_exception());
        }
        part.prepared.reset();
        if (frozen && !*frozen) part.failure.observe(frozen->error());
        if (part.failure.failed()) co_return;
        part.frozen.emplace(std::move(**frozen));
        const auto& layout = part.frozen->layout();
        const auto history = layout.boundary().history();
        for (std::size_t i = 0; i < part.items.size(); ++i) {
            auto& item = *part.items[i];
            const auto& block = layout.blocks()[i];
            item.block.emplace(block);
            auto target = segment_write_context::make(
              history.segment,
              history.alignment,
              block.records.physical().begin(),
              block.records.bytes().begin());
            KWAQUE_INVARIANT(
              invariant_id{"KQ-LOCAL-APPEND-TARGET"},
              target.has_value(),
              "frozen block produced an invalid WAL target");
            item.children.expected.target = *target;
        }
        part.boundary.emplace(layout.boundary());
        // The group installed each batch's retry entry: its fact storage now
        // belongs to the segment's set, as the segment keeps its retry grant.
        for (auto& item : part.items)
            adopt_fact(slot, *item);
        if (slot.tail)
            slot.tail->next = &part;
        else
            slot.head = &part;
        slot.tail = &part;
        part.queued = true;
        part.encoded.emplace(
          slot.writer->encode_group(*part.frozen, part.work));
    }

    // After WAL acceptance: submit once encoded, without waiting for the WAL
    // barrier. The segment's barrier loop covers it from then on.
    seastar::future<> submit_branch(branch& part) {
        auto& slot = slots_[part.slot];
        try {
            auto encoded = co_await std::move(*part.encoded);
            part.encoded.reset();
            if (!encoded)
                part.failure.observe(encoded.error());
            else {
                auto submission = slot.writer->submit(
                  std::move(*part.frozen), part.work);
                if (!submission)
                    part.failure.observe(submission.error());
                else {
                    part.written.emplace(std::move(submission->written));
                    part.submitted = true;
                }
            }
        } catch (...) {
            part.failure.observe(std::current_exception());
        }
        part.encoded.reset();
        // An unsubmitted frozen group fences its segment when dropped.
        part.frozen.reset();
        mark_ready(part);
        if (part.failure.failed()) observe_segment(slot);
    }

    // The WAL did not accept the group: join its encoding, then drop the
    // frozen group unwritten.
    seastar::future<> drop_branch(branch& part) {
        if (part.encoded) {
            try {
                static_cast<void>(co_await std::move(*part.encoded));
            } catch (...) {
            }
            part.encoded.reset();
        }
        part.frozen.reset();
        mark_ready(part);
    }

    void mark_ready(branch& part) noexcept {
        part.ready = true;
        slots_[part.slot].wake.signal();
    }

    // Copies the WAL result, then releases the group's WAL admission: WAL
    // reclamation is tracked by obligations, not by holding a group commit
    // slot. Covered segments publish now; the rest publish when their barrier
    // settles.
    seastar::future<> settle_wal(group_state& group) {
        runtime::first_failure logged;
        std::optional<wal_commit_result> wal;
        if (!group.observed)
            logged.observe(error(errc::queue_full));
        else {
            try {
                wal.emplace(co_await std::move(*group.observed));
            } catch (...) {
                logged.observe(std::current_exception());
            }
            group.observed.reset();
        }
        if (wal) {
            detail::merge_failure(logged, wal->failure());
            if (!logged.failed()) {
                if (wal->receipt())
                    group.wal_receipt.emplace(*wal->receipt());
                else
                    logged.observe(error(errc::io_failure));
            }
        }
        group.wal_failure = logged;
        wal.reset();
        group.ticket.reset();
        group.wal_settled = true;
        for (auto& part : group.branches)
            if (part->covered) publish_branch(group, *part);
        if (group.wal_failure.failed()) observe_wal();
        maybe_form();
    }

    // Joins a submitted write's completion; its outcome already reached the
    // barrier that settled the branch.
    seastar::future<> join_write(branch& part) {
        if (!part.written) co_return;
        try {
            static_cast<void>(co_await std::move(*part.written));
        } catch (...) {
        }
        part.written.reset();
    }

    // One per attachment. It waits for a ready group, runs one barrier over
    // every group submitted so far, settles each group that barrier covers,
    // and repeats; a group submitted meanwhile waits for the next round. It
    // uses the last submitted group's cut, never the reserved end, which can
    // include frozen groups not yet submitted.
    seastar::future<> barrier_loop(std::uint32_t index) {
        auto& slot = slots_[index];
        for (;;) {
            try {
                co_await slot.wake.wait([&slot] {
                    return (slot.head && slot.head->ready)
                           || (slot.stop && !slot.head);
                });
            } catch (...) {
                first_.observe(std::current_exception());
            }
            if (!slot.head) {
                if (slot.stop) break;
                continue;
            }
            if (!slot.head->ready) continue;
            // The longest ready prefix; its last submitted group is the cut.
            branch* last = nullptr;
            for (auto* part = slot.head; part && part->ready; part = part->next)
                if (part->submitted) last = part;
            std::optional<segment_barrier_outcome> outcome;
            if (last) {
                try {
                    outcome.emplace(
                      co_await slot.writer->barrier(*last->boundary));
                } catch (...) {
                    outcome.emplace();
                    outcome->failure.observe(std::current_exception());
                }
            }
            settle_front(slot, last, outcome);
            if (outcome && outcome->failure.failed()) observe_segment(slot);
            offer_reclaim();
        }
        slot.looping = false;
    }

    // Settles the ready front of a segment's queue through last, or every
    // ready group when none of them was submitted.
    void settle_front(
      segment_slot& slot,
      const branch* last,
      const std::optional<segment_barrier_outcome>& outcome) noexcept {
        if (outcome && !outcome->failure.failed() && outcome->receipt) {
            const auto& cut = outcome->receipt->boundary();
            if (cut.footer())
                slot.durable.emplace(
                  local_durable_boundary{
                    slot.device, cut.history(), cut.covered(), *cut.footer()});
        }
        while (slot.head && slot.head->ready) {
            auto* part = std::exchange(slot.head, slot.head->next);
            part->next = nullptr;
            if (!slot.head) slot.tail = nullptr;
            part->queued = false;
            part->covered = true;
            if (part->submitted && outcome) part->barrier.emplace(*outcome);
            // The obligation follows the segment, not the WAL result.
            settle_obligation(*part->group, *part);
            if (part->group->wal_settled) publish_branch(*part->group, *part);
            if (part == last) break;
        }
    }

    void stop_loop(segment_slot& slot) noexcept {
        slot.stop = true;
        slot.wake.signal();
    }

    // The last of a branch's two settlements publishes its members: every
    // outcome and bound is installed before any waiter is woken.
    void publish_branch(group_state& group, branch& part) noexcept {
        if (part.published) return;
        part.published = true;
        settle_obligation(group, part);
        for (const auto& item : part.items)
            install(*item, outcome(*item, part, group));
        --slots_[part.slot].groups;
        --group.unpublished;
        for (const auto& item : part.items)
            wake(*item);
        if (group.unpublished == 0) group.published.set_value();
    }

    static local_append_outcome outcome(
      const detail::local_append_item& item,
      const branch& part,
      const group_state& group) {
        local_append_outcome output;
        if (!part.offered) {
            // Its segment never froze: neither a PREPARE nor a segment byte.
            output.failure = part.failure;
            if (!output.failure.failed())
                output.failure.observe(error(errc::closed));
            return output;
        }
        if (!group.wal_accepted) {
            // The frozen group was dropped unwritten. Without a proven WAL
            // rejection, a PREPARE may exist.
            output.failure = part.failure;
            detail::merge_failure(output.failure, group.wal_failure);
            if (!output.failure.failed())
                output.failure.observe(error(errc::closed));
            if (group.wal_unknown)
                output.status = local_append_status::uncertain;
            return output;
        }
        if (
          group.wal_receipt && !group.wal_failure.failed() && part.submitted
          && !part.failure.failed() && part.barrier
          && !part.barrier->failure.failed() && part.barrier->receipt) {
            output.status = local_append_status::durable;
            output.receipt.emplace(
              local_append_receipt{
                item.info,
                *item.block,
                *group.wal_receipt,
                *part.barrier->receipt});
            return output;
        }
        output.status = local_append_status::uncertain;
        output.failure = group.wal_failure;
        detail::merge_failure(output.failure, part.failure);
        if (part.barrier)
            detail::merge_failure(output.failure, part.barrier->failure);
        if (!output.failure.failed())
            output.failure.observe(error(errc::io_failure));
        return output;
    }

    void release(group_state& group) noexcept {
        std::erase(accepted_, &group);
        group.branches.clear();
        group.ticket.reset();
        --groups_;
        drained_.broadcast();
    }

    workload_budget& budget_;
    wal_group_commit& commit_;
    wal_type& writer_;
    local_append_config config_;
    workload_reservation held_;
    std::vector<segment_slot> slots_;
    std::vector<obligation_row> rows_;
    // Groups the WAL accepted and that are not yet released, in WAL order.
    std::vector<group_state*> accepted_;
    // Where the group being submitted to the WAL begins, until its answer is
    // installed. Submissions are serialized, so there is at most one.
    std::optional<local_wal_cursor> submitting_;
    // The size a rotation refused for pressure was asked with, while the WAL
    // writer keeps that rotation pending.
    std::optional<byte_count> rotation_bytes_;
    std::uint64_t sequences_{0};
    // Accepted requests not yet in a group, in acceptance order.
    std::vector<detail::local_append_item_ptr> forming_;
    // Acceptance in call order, then one formation at a time.
    seastar::semaphore accept_order_{1}, form_lock_{1};
    seastar::gate operations_;
    // Close waits here for formations and groups to drain.
    seastar::condition_variable drained_;
    // Accepted work never uses a caller's abort source.
    seastar::abort_source abort_;
    codec::cooperative_work execution_, preparation_;
    local_failure_sink sink_;
    std::optional<local_storage_failure> storage_failure_;
    local_retention_sink pressure_sink_;
    // The newest rotation the retained limit refused, while the limit holds.
    std::optional<local_retention_pressure> pressure_;
    // The shard's checkpoint owner, while bound; whether it reclaims the WAL
    // on its own; and the WAL file the oldest obligation lay in when it was
    // last told a closed file may go.
    std::optional<local_checkpointer> checkpointer_;
    bool reclaiming_{false};
    std::optional<model::wal_incarnation_id> reclaim_floor_;
    seastar::optimized_optional<seastar::abort_source::subscription>
      shutdown_subscription_;
    runtime::first_failure first_, close_outcome_;
    seastar::shared_promise<> closed_event_;
    std::uint64_t attachments_{0};
    std::uint32_t requests_{0}, groups_{0}, formations_{0};
    bool form_pending_{false}, closing_{false}, closed_{false};
    bool shutdown_bound_{false}, stopped_{false};
    // Formations, group tasks and barrier loops; joined last by close().
    runtime::task_scope tasks_;
};

} // namespace kwaque::storage
