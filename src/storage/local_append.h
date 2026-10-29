#pragma once

#include "src/base/invariant.h"
#include "src/base/units.h"
#include "src/runtime/file_error.h"
#include "src/runtime/task_scope.h"
#include "src/runtime/timer.h"
#include "src/storage/local_append_verdict.h"
#include "src/storage/placement_statistics.h"
#include "src/storage/retry_summary.h"
#include "src/storage/segment_writer.h"
#include "src/storage/table_view.h"
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

inline constexpr std::uint32_t maximum_local_append_segments = 4096;
inline constexpr std::uint32_t maximum_local_append_requests = 4096;

struct local_append_config final {
    // Attached writable segments; each keeps at most one forming group.
    // Every one of them holds handle credits for as long as it is open, so a
    // shard checks this count against the budget that funds its segment
    // writers with validate_segment_handles() before it starts.
    std::uint32_t maximum_segments{1024};
    // Accepted requests whose outcome is not yet published: one in flight
    // for each segment. Bytes, not this count, bound what waits.
    std::uint32_t maximum_requests{1024};
    // Acceptance validation, WAL and segment execution share this policy.
    codec::limits policy{codec::limits::defaults()};
    // Coordination state and frames of one request, one group or one
    // segment's barrier loop.
    byte_count execution_bytes{4_KiB};
    // A group's portion for one segment is WAL-placed while the bytes its
    // freeze would take there are fewer than this, else segment-placed:
    // zero places every portion in its segment, the maximum every one in
    // the WAL. A WAL-placed portion is reported before its segment has its
    // bytes: readers take them from its writer until they are written, and
    // a restart writes the WAL's copy into the segment before it serves.
    byte_count segment_placement_bytes{256_KiB};
    // A WAL-placed portion is durable once its WAL barrier covers it, so its
    // segment holds it, unwritten, until the run it joins reaches this many
    // bytes, a sync or a segment-placed portion needs it, or its memory is
    // wanted; then the run is one write. Zero writes each portion at once.
    // At most maximum_segment_hold_bytes.
    byte_count held_run_bytes{128_KiB};
    // Syncs this owner has its segments run at once to make WAL-placed
    // portions durable there. A barrier a segment-placed portion waits for
    // is never held back by them.
    std::uint32_t concurrent_syncs{16};
    [[nodiscard]] runtime::result<void> validate() const noexcept;
};

// An exact assigned child and its independently supplied context. backing
// covers the child's backing, descriptors and sharing controls.
struct local_append_request final {
    encoded_assigned_batch batch;
    workload_reservation backing;
    model::batch_decode_expectation expected;
    model::range_routing_epoch routing_epoch;
    // The broker time the range's assigner stamped with the batch's offsets.
    runtime::wall_time append_time;
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

// durable: inside its segment's reported prefix. It and every earlier request
// of its segment are durable in at least one place: its PREPARE under a WAL
// barrier, or its block under a barrier of its segment, which certifies
// every byte before its footer. not_written: no PREPARE was accepted and no
// segment byte was submitted, and the owner can show it. uncertain: bytes
// may survive and recovery classifies them; a later retry of the same batch
// is neither new nor completed. Neither is rollback.
enum class local_append_status : std::uint8_t {
    durable,
    not_written,
    uncertain
};

// block is the exact extent a later index needs, and boundary its group's
// cut, its blocks and footer: once the request is published the segment's
// reported prefix reaches it. wal and segment are the barriers that had
// covered it when it was published, whichever came first, or both: a
// WAL-placed request may be published by either, a segment-placed one only
// by its segment's. One barrier can cover several groups, so a cut may end
// past this group. This is local durability, not a producer
// acknowledgement.
struct local_append_receipt final {
    assigned_batch_info batch;
    segment_block_layout block;
    // What its block was stamped with: the request's append time, and the
    // end of what its segment had certified when its group formed.
    segment_block_stamps stamps;
    local_append_placement placement;
    segment_captured_boundary boundary;
    std::optional<wal_durable_receipt> wal;
    std::optional<segment_durable_receipt> segment;
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
// accepted work and its buffers stay owned until its result is decided.
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
    // still accepting included, or the reserved WAL end when there is none;
    // an open row counts from where it last opened, which can be earlier.
    // It can lie past wal_durable: a group's segment barrier may settle
    // first.
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
// Where an attachment's reported prefix ends: the end of the last group
// whose requests were published durable, its footer included when it has
// one. A held group has none: its footer comes with a later group's. Plain
// facts, as above.
struct local_reported_boundary final {
    device_store_id device;
    segment_history_context history;
    // Its records, and its bytes from the data start to that end.
    storage::coverage covered;
    bool operator==(const local_reported_boundary&) const noexcept = default;
};
// What a detached attachment leaves behind.
struct local_detached final {
    // The facts of every batch it published durable, the last R of each
    // stream: what a seal of the segment summarizes.
    retry_summary_builder retry;
    // Its newest durable footer, final for this attachment. A bound
    // checkpoint owner already keeps it until the segment's own publication
    // covers it.
    std::optional<local_durable_boundary> boundary;
};

// Where a storage failure arose: the WAL; one segment owner's writer; the
// control record, which the checkpoint owner publishes through; a segment's
// committed data, found not to be what a barrier made durable there: its
// checksums fail, or its bytes are not what they say; or the append owner's
// own work, which ended in a way no operation expects.
enum class local_storage_path : std::uint8_t {
    wal,
    segment,
    control,
    damage,
    internal,
};
struct local_storage_failure final {
    local_storage_path path;
    // The segment, for a segment failure, and for damage when its finder
    // knows which.
    std::optional<segment_context> segment;
    runtime::first_failure failure;
    // From the file failure; unknown when the failure carries none.
    runtime::file_failure_detail detail{runtime::file_failure_detail::unknown};
    // The data device the segment is on. None for the WAL and the control
    // record, which are on the control device.
    std::optional<device_store_id> device{};
    // For a segment, its reported prefix when the failure was reported. Past
    // its newest durable footer, only the WAL holds what was reported, which
    // whoever seals the failed segment must keep.
    std::optional<local_reported_boundary> reported{};
};
// Receives the WAL's first storage failure, and each segment's, once each, on
// the owner's shard, after the owner's state is consistent, so it may call
// back into the owner. It must not throw. Stopping admission, isolating or
// stopping is the caller's policy; so is what damage means.
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
      wal_prepared_children children,
      std::uint32_t slot,
      runtime::monotonic_time accepted,
      runtime::wall_time append_time);
    workload_reservation held;
    wal_prepared_children children;
    assigned_batch_info info;
    std::uint32_t slot;
    runtime::monotonic_time accepted;
    // The broker time the request was assigned at.
    runtime::wall_time append_time;
    // Its segment's branch in the formed group, and its block there.
    std::uint32_t branch{0};
    std::optional<segment_block_layout> block;
    // Its stream's facts in its segment, and whether its batch takes an
    // entry of the segment's seal: while the stream holds fewer than the
    // retry depth.
    retry_ring* ring{nullptr};
    bool entry{false};
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
// are in flight. Each segment's portion of a group is placed whole, by the
// bytes its freeze takes there: a small one in the WAL, whose submission is
// admitted before any segment position freezes and whose segment group,
// once the WAL has accepted it, is held unwritten in its segment's run
// until the run reaches held_run_bytes or is needed; a large one in its
// segment alone, written as soon as it is encoded with nothing in the WAL.
// A request is durable once one place holds it and every earlier request of
// its segment is durable, so results publish per segment in file order: a
// WAL barrier publishes its WAL-placed members, and a segment barrier
// everything before its footer. A WAL-placed group is an obligation of its
// WAL file until a barrier of its segment covers it. One barrier loop per
// segment runs the barriers wanted of it: at once for a segment-placed
// group, and for WAL-placed ones only when a sync is asked: when the WAL
// moves to a new file, at its retained limit, when the WAL fails, and when
// the attachment quiesces or the owner closes. No timer syncs anything.
// Providers outlive close(), which precedes closing them.
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
        // Each slot is an allocation of its own, so no table of them
        // outgrows one allocation at any segment count.
        auto slots = budget.allocation_charge(
          byte_count{
            config.maximum_segments * sizeof(std::unique_ptr<segment_slot>)});
        auto slot = budget.allocation_charge(byte_count{sizeof(segment_slot)});
        auto forming = budget.allocation_charge(
          byte_count{
            maximum_wal_group_members * sizeof(detail::local_append_item_ptr)});
        // The obligation table is fixed (obligation_rows), in fragments of
        // at most 128 KiB, with a stack of the rows that are free.
        static_assert(
          chunked_fragment_elements(sizeof(obligation_row))
            * sizeof(obligation_row)
          <= maximum_contiguous_allocation_bytes - 32);
        auto table = chunked_allocation_charge(
          budget, obligation_rows(config), sizeof(obligation_row));
        if (table) {
            const auto stack = budget.allocation_charge(
              byte_count{obligation_rows(config) * sizeof(std::uint32_t)});
            table = stack ? table->checked_add(*stack).value_or(byte_count{})
                          : runtime::result<byte_count>{
                              runtime::failure(stack.error())};
        }
        // Attachments waiting for a sync, each at most once.
        auto waiters = budget.allocation_charge(
          byte_count{config.maximum_segments * sizeof(std::uint32_t)});
        if (!instance) return runtime::failure(instance.error());
        if (!slots) return runtime::failure(slots.error());
        if (!slot) return runtime::failure(slot.error());
        if (!forming) return runtime::failure(forming.error());
        if (!table) return runtime::failure(table.error());
        if (!waiters) return runtime::failure(waiters.error());
        // Each attachment's barrier loop runs in one execution allowance.
        auto held = budget.try_reserve(
          byte_count{
            instance->value() + slots->value()
            + config.maximum_segments * slot->value() + forming->value()
            + table->value() + waiters->value()
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
    // What each segment this owner may attach costs it, reserved when it is
    // made: its slot, its row of obligations in two files, and its barrier
    // loop's execution.
    [[nodiscard]] static runtime::result<byte_count> segment_charge(
      const workload_budget& budget, const local_append_config& config) {
        const auto slot = budget.allocation_charge(
          byte_count{
            sizeof(segment_slot) + sizeof(std::unique_ptr<segment_slot>)});
        const auto rows = budget.allocation_charge(
          byte_count{2 * (sizeof(obligation_row) + sizeof(std::uint32_t))});
        if (!slot) return runtime::failure(slot.error());
        if (!rows) return runtime::failure(rows.error());
        return byte_count{
          slot->value() + rows->value() + config.execution_bytes.value()};
    }
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
            if (slots_[i]->writer == &segment)
                return runtime::failure(error(errc::already_exists));
            // A detached slot is free once its loop has returned.
            if (!slots_[i]->writer && !slots_[i]->looping && !free) free = i;
        }
        if (!free) return runtime::failure(error(errc::queue_full));
        if (reclaiming_ && reserved_from(segment.budget()))
            return runtime::failure(error(errc::invalid_argument));
        auto retry = retry_summary_builder::make(
          boundary->context(), segment.retry_depth(), budget_);
        if (!retry) return runtime::failure(retry.error());
        const auto index = static_cast<std::uint32_t>(*free);
        auto& slot = *slots_[index];
        slot.retry.emplace(std::move(*retry));
        slot.writer = &segment;
        slot.segment.emplace(boundary->context());
        slot.device = segment.device();
        slot.stop = false;
        slot.quiet = false;
        slot.failure_reported = false;
        slot.submitted.reset();
        slot.footed.reset();
        slot.closing.reset();
        slot.wanted.reset();
        slot.requested.reset();
        slot.attempted.reset();
        slot.sync_waiting = false;
        slot.held = 0;
        slot.receipt.reset();
        slot.failed_through.reset();
        slot.barrier_failure = {};
        slot.broken = false;
        slot.broken_failure = {};
        slot.reported.reset();
        slot.row.reset();
        slot.looping = true;
        // A native allocation failure throws, with the slot left free.
        std::optional<seastar::future<>> loop;
        try {
            loop.emplace(barrier_loop(index));
        } catch (...) {
            slot.writer = nullptr;
            slot.retry.reset();
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
            slot.retry.reset();
            stop_loop(slot);
            throw;
        }
        if (!spawned) {
            slot.writer = nullptr;
            slot.retry.reset();
            stop_loop(slot);
            return runtime::failure(spawned.error());
        }
        slot.attachment = ++attachments_;
        return local_append_target{index, slot.attachment};
    }

    // Closes one attachment's admission and resolves once it is idle, which
    // is what detach() asks for. A request for it that has not joined its
    // forming group is rejected as closed, with no effect. What was forming
    // forms now, or is rejected unwritten where pressure would reject it,
    // every group of the attachment in flight is published, and a barrier
    // of its segment has settled over everything submitted to it, a run it
    // held closed and written first: nothing accepted is failed or
    // abandoned, and what only the WAL held is durable in the segment, or
    // its failure is reported and its obligations pinned. No other attachment
    // waits or is refused anything. The attachment stays attached and takes
    // no request again; detach() follows. Calls made while one waits wait
    // with it. Refused with wrong_context for a target that is stale, also
    // when it becomes so while this waits, and with closed when the owner's
    // close ends the wait before the attachment is idle.
    [[nodiscard]] seastar::future<runtime::result<void>>
    quiesce(local_append_target target) {
        assert_current();
        if (closed_ || unquiet_)
            co_return runtime::failure(error(errc::closed));
        if (auto slot = find(target); !slot)
            co_return runtime::failure(slot.error());
        else {
            // Everything passed is now wanted durable in its segment.
            (*slot)->quiet = true;
            (*slot)->wake.signal();
        }
        ++quiescing_;
        const auto counted = seastar::defer([this] noexcept {
            --quiescing_;
            drained_.broadcast();
        });
        // Nothing more joins what is forming, so it need not wait for more.
        maybe_form();
        for (;;) {
            const auto slot = find(target);
            if (!slot) co_return runtime::failure(slot.error());
            if (idle(**slot)) co_return runtime::result<void>{};
            if (unquiet_) co_return runtime::failure(error(errc::closed));
            co_await quiet_.when();
        }
    }

    // Has one attachment's segment cover everything frozen to it so far with
    // a barrier, closing and writing a run it holds first, and resolves once
    // one settled through it: ok when it made that durable, else with the
    // segment's failure. Admission stays open; groups frozen later wait for
    // a later sync. Refused with wrong_context for a target that is stale,
    // also when it becomes so while this waits, and with closed once the
    // owner has closed.
    [[nodiscard]] seastar::future<runtime::result<void>>
    sync(local_append_target target) {
        assert_current();
        if (closed_ || unquiet_)
            co_return runtime::failure(error(errc::closed));
        auto found = find(target);
        if (!found) co_return runtime::failure(found.error());
        request_sync(**found);
        const auto through = (*found)->requested;
        if (!through) co_return runtime::result<void>{};
        ++syncing_;
        const auto counted = seastar::defer([this] noexcept {
            --syncing_;
            drained_.broadcast();
        });
        for (;;) {
            const auto slot = find(target);
            if (!slot) co_return runtime::failure(slot.error());
            const auto& owned = **slot;
            if (
              owned.receipt
              && owned.receipt->boundary().end().bytes >= *through)
                co_return runtime::result<void>{};
            if (owned.failed_through && *owned.failed_through >= *through)
                co_return owned.barrier_failure.outcome();
            if (unquiet_) co_return runtime::failure(error(errc::closed));
            co_await quiet_.when();
        }
    }

    // Only an idle attachment detaches: no forming request, no group in
    // flight or unpublished, and no barrier owed or running; after quiesce()
    // its newest durable footer covers every request reported. Its barrier
    // loop then returns. The facts of the batches it published move to the
    // returned builder, which a seal of the segment summarizes, and its
    // newest durable footer is returned with them. A bound checkpoint owner
    // is handed that footer here, and a detach it cannot take is refused
    // with the segment still attached. Open obligations stay in the table.
    // The segment owner itself is unchanged.
    [[nodiscard]] runtime::result<local_detached>
    detach(local_append_target target) {
        assert_current();
        auto slot = find(target);
        if (!slot) return runtime::failure(slot.error());
        auto& owned = **slot;
        if (!idle(owned)) return runtime::failure(error(errc::queue_full));
        // The checkpoint owner takes the final footer first: a checkpoint
        // cut after this must still pin it.
        if (checkpointer_ && owned.durable)
            if (auto kept = checkpointer_->detaching(*owned.durable); !kept)
                return runtime::failure(kept.error());
        // A request still in acceptance can no longer join: a stream it
        // opened goes.
        owned.retry->drop();
        local_detached detached{std::move(*owned.retry), owned.durable};
        owned.retry.reset();
        owned.writer = nullptr;
        owned.segment.reset();
        owned.durable.reset();
        owned.prepared.reset();
        stop_loop(owned);
        return detached;
    }

    // The facts of the batches an attachment published durable, the last R
    // of each stream, derived as each was published: a footer, a PREPARE or
    // an acceptance alone records none, and nobody supplies one. Borrowed
    // until the next call into the owner.
    [[nodiscard]] runtime::result<const retry_summary_builder*>
    completed_retries(local_append_target target) {
        assert_current();
        auto slot = find(target);
        if (!slot) return runtime::failure(slot.error());
        return &*(**slot).retry;
    }
    // Where an attachment's reported prefix ends: the cut of the last group
    // whose requests were published durable. It never moves back, and lies
    // past the newest durable footer while WAL barriers alone hold the
    // groups after it. None before the first.
    [[nodiscard]] runtime::result<std::optional<local_reported_boundary>>
    reported(local_append_target target) {
        assert_current();
        auto slot = find(target);
        if (!slot) return runtime::failure(slot.error());
        return (**slot).reported;
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
        // The oldest of every open row, a group whose WAL answer is not
        // installed yet, and every pinned row. Cursor order is WAL order. An
        // open row begins where it last opened, at or before its oldest
        // group still open.
        std::optional<local_wal_cursor> oldest;
        for (const auto& row : rows_) {
            if (row.pending == 0) continue;
            auto kept = keep_older(oldest, *row.pending_begin, wal->owner);
            if (!kept) return runtime::failure(kept.error());
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
    restore_pinned(table_view<local_pinned_obligation> restored) {
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
        for (std::size_t i = 0; i < restored.size(); ++i) {
            auto& row = rows_[take_free().value()];
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
            if (slot->writer && *slot->segment == segment)
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
    // Where the WAL ends as reserved: every PREPARE accepted so far begins
    // below it. A segment that is detached gets no further PREPARE, so each
    // of its own lies below what this says from then on.
    [[nodiscard]] runtime::result<local_wal_cursor> wal_end() const {
        assert_current();
        const auto wal = writer_.progress();
        if (!wal) return runtime::failure(error(errc::closed));
        return wal->reserved;
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
            if (slot->writer && slot->durable) visit(*slot->durable);
    }

    // The first storage failure reported through the sink, of the WAL or of
    // an attached segment. Requests keep their own outcomes.
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
            if (slot->writer && reserved_from(slot->writer->budget()))
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
    // The WAL's spares, which the reclaimer moves a file into instead of
    // removing it while they have room.
    [[nodiscard]] auto& wal_spares() noexcept {
        assert_current();
        return writer_.spares();
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

    // Stops acceptance, forms and publishes every accepted request, reports
    // those pressure leaves unformed as not_written, has every attachment's
    // segment cover all of it with a barrier, then stops the barrier loops,
    // each once it has run the barrier it owes, and joins all work. Repeated
    // calls return the first outcome.
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
            // Every attachment is synced before its loop stops: what the WAL
            // alone holds is made durable in its segment.
            for (const auto& slot : slots_) {
                if (!slot->writer) continue;
                slot->quiet = true;
                slot->wake.signal();
            }
            maybe_form();
            co_await drained_.when(
              [this] { return formations_ == 0 && groups_ == 0; });
            // Pressure with nothing in flight left these unformed.
            if (!forming_.empty()) fail_forming(reason(error(errc::closed)));
        } catch (...) {
            close_outcome_.observe(std::current_exception());
        }
        // Whoever waits for an attachment to be idle finds it so, and leaves.
        unquiet_ = true;
        try {
            quiet_.broadcast();
            co_await drained_.when(
              [this] { return quiescing_ == 0 && syncing_ == 0; });
        } catch (...) {
            close_outcome_.observe(std::current_exception());
        }
        for (const auto& slot : slots_)
            stop_loop(*slot);
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
    // What this owner placed, held and synced since it was made, and what
    // stands now: the runs its segments hold open, the bytes their reported
    // prefixes cover past their newest durable footers, and the WAL files a
    // group not yet durable in its segment keeps. From bounded owner state,
    // so nothing is counted per request.
    [[nodiscard]] local_append_statistics statistics() const noexcept {
        assert_current();
        auto counted = counted_;
        for (const auto& slot : slots_) {
            if (!slot->writer) continue;
            const auto held = slot->writer->held().value();
            counted.held_bytes += held;
            if (held != 0) ++counted.held_runs;
            if (!slot->reported) continue;
            const auto& reported = slot->reported->covered.bytes();
            const auto durable = slot->durable ? slot->durable->footer.end()
                                               : reported.begin();
            if (reported.end() > durable)
                counted.unsynced_bytes += reported.end().value()
                                          - durable.value();
        }
        if (const auto wal = writer_.progress()) {
            // Cursors of one WAL always compare.
            std::optional<local_wal_cursor> oldest;
            for (const auto& row : rows_)
                if (
                  row.pending != 0
                  && !keep_older(oldest, *row.pending_begin, wal->owner))
                    break;
            const auto& retained = writer_.retained();
            if (oldest)
                counted.unsynced_files = retained.files()
                                         - retained.below(*oldest);
        }
        return counted;
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
        local_append_placement placement{local_append_placement::wal};
        std::optional<segment_prepared_group> prepared;
        std::vector<detail::local_append_item_ptr> items;
        std::optional<segment_frozen_group> frozen;
        // The frozen group's cut: its blocks and footer.
        std::optional<segment_captured_boundary> boundary;
        // Where the segment's durable bytes ended when the group froze.
        runtime::file_position certified_end{};
        std::optional<seastar::future<runtime::result<void>>> encoded;
        // A segment-placed group's submission, started at freeze.
        std::optional<seastar::future<>> submitting;
        std::optional<seastar::future<segment_write_completion>> written;
        runtime::first_failure failure;
        // Its obligation row in the current WAL file, and whether the WAL
        // accepted its members so that an obligation is open.
        std::optional<std::uint32_t> row;
        bool obliged{false};
        // The next frozen group of the same segment, in file order. The group
        // owns its branches until each is published and passed.
        branch* next{nullptr};
        // queued: frozen and in its segment's file order; a WAL-placed one
        // has its members in the WAL offer. ready: submitted or dropped.
        // passed: ready, and so is every group before it in its segment, so
        // a barrier may cut past it.
        bool queued{false}, ready{false}, submitted{false}, passed{false},
          published{false};
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
        // The cut of the newest group published durable: the end of the
        // reported prefix.
        std::optional<local_reported_boundary> reported;
        // The facts of the batches it published, fed in file order, the last
        // R of each stream; their streams' rings are admitted at acceptance.
        std::optional<retry_summary_builder> retry;
        // Admission of the forming group, from the current reserved end.
        std::optional<segment_prepared_group> prepared;
        // Forming requests, and groups with unpublished members here.
        std::uint32_t forming{0}, groups{0};
        // A freeze moves the reserved end; preparations wait for it. A
        // request waits here too while its forming set has no room for it:
        // until a formation takes or fails the set.
        bool freezing{false};
        seastar::condition_variable frozen;
        // Frozen groups in file order, from the oldest not yet both
        // published and passed. cursor: the oldest unpublished; frontier:
        // the oldest not passed. Either is null past the last.
        branch* head{nullptr};
        branch* tail{nullptr};
        branch* cursor{nullptr};
        branch* frontier{nullptr};
        // The cut of the newest group passed submitted, and of the newest
        // passed one that ends with a footer, which a barrier may take: a
        // held group's cut has none until a later group closes its run.
        std::optional<segment_captured_boundary> submitted, footed;
        // A footer asked of the writer to close its held run for a barrier,
        // until every group before it is passed.
        std::optional<segment_captured_boundary> closing;
        // Where a barrier is wanted through: the newest passed
        // segment-placed group, whose acknowledgement waits for it, and the
        // newest frozen group when a sync was asked of the attachment.
        std::optional<runtime::file_position> wanted, requested;
        // The end of the newest cut a barrier was asked for, and whether one
        // runs, holding one of the owner's syncs when only a sync wants it.
        std::optional<runtime::file_position> attempted;
        bool barrier_running{false}, sync_waiting{false};
        // Obligations of groups passed after its newest passed footer.
        std::uint32_t held{0};
        // The newest successful barrier, and how far failed ones reached:
        // what each says of a group follows from its footer.
        std::optional<segment_durable_receipt> receipt;
        std::optional<runtime::file_position> failed_through;
        runtime::first_failure barrier_failure;
        // A group was published as a failure, and why: no later one can be
        // durable.
        bool broken{false};
        runtime::first_failure broken_failure;
        // The barrier loop waits here; only it waits, so a signal targets it.
        seastar::condition_variable wake;
        bool looping{false}, stop{false};
        // quiesce() or close() closed this attachment's admission: everything
        // passed is wanted durable in its segment.
        bool quiet{false};
        // Its writer's failure was reported.
        bool failure_reported{false};
        // Its obligation row in the newest WAL file it formed in, which its
        // next group reserves again while that file is the head.
        std::optional<std::uint32_t> row;
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
        // The WAL side, installed once: whether it answered the group's
        // submission and accepted it, and then its receipt or failure.
        // unknown: an exception left acceptance unproven.
        runtime::first_failure wal_failure;
        std::optional<wal_durable_receipt> wal_receipt;
        bool wal_decided{false}, wal_accepted{false}, wal_unknown{false},
          wal_settled{false};
        // WAL order among accepted groups, and where its PREPAREs begin.
        std::uint64_t sequence{0};
        std::optional<local_wal_cursor> wal_begin;
        // Branches not yet published and, if frozen, passed.
        std::uint32_t remaining{0};
        // Formation may continue; no branch remains.
        seastar::promise<> submitted, done;
    };
    using group_ptr = seastar::lw_shared_ptr<group_state>;
    enum class formation : std::uint8_t { idle, formed, blocked, retry };
    // Per WAL file and segment: WAL-placed groups reserved by a forming
    // group, groups open until a barrier of their segment covers them, and
    // groups pinned because one failed. Of the open, written ones are passed
    // and lie before the segment's newest passed footer, so the next barrier
    // covers them; held ones are passed after it, held unwritten until a
    // footer closes their run; covering ones are those the barrier running
    // now covers. A row is free when it counts nothing. It is keyed by
    // segment context, so a pinned row outlives its attachment.
    struct obligation_row final {
        // Its place in the table, fixed when the table is made.
        std::uint32_t index{0};
        std::optional<model::wal_incarnation_id> wal;
        std::optional<segment_context> segment;
        // The slot its segment was attached at when its groups formed.
        std::optional<std::uint32_t> slot;
        std::uint32_t reserved{0}, pending{0}, pinned{0};
        std::uint32_t written{0}, held{0}, covering{0};
        // How many of the pinned are groups pinned in this process, each
        // counted once whatever PREPAREs of the segment it holds.
        std::uint32_t live{0};
        // The oldest pinned group: its order among this owner's groups and
        // where it begins. A restored or replaced pin precedes every group.
        std::uint64_t first_pinned{0};
        std::optional<local_wal_cursor> pinned_begin;
        // The group that opened the row since it was last closed: at or
        // before its oldest group still open.
        std::uint64_t first_pending{0};
        std::optional<local_wal_cursor> pending_begin;
    };
    // A row per open WAL file and segment pair, at most one per live
    // request, plus per segment one row in a closed file whose groups wait
    // for their barrier and one pinned row.
    static std::size_t obligation_rows(const local_append_config& config) {
        return std::size_t{config.maximum_requests}
               + 2 * std::size_t{config.maximum_segments};
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
      , slots_(made_slots(config.maximum_segments))
      , rows_(made_rows(obligation_rows(config)))
      , free_rows_(made_free(obligation_rows(config)))
      , forming_(
          reserved<detail::local_append_item_ptr>(maximum_wal_group_members))
      , sync_waiters_(reserved<std::uint32_t>(config.maximum_segments))
      , syncs_(config.concurrent_syncs)
      , execution_(config.policy, abort_)
      , preparation_(config.policy, abort_)
      , sink_(std::move(sink))
      , pressure_sink_(std::move(pressure))
      , tasks_([this](std::exception_ptr failure) noexcept {
          first_.observe(failure);
          // A worker of every append ended: whoever composes the shard is
          // told now, not at close.
          if (sink_ && !internal_reported_) {
              internal_reported_ = true;
              runtime::first_failure ended;
              ended.observe(std::move(failure));
              sink_(
                local_storage_failure{
                  local_storage_path::internal, std::nullopt, ended});
          }
      }) {}

    // The obligation table, every row free and knowing its place, and the
    // stack of free rows, lowest on top.
    static seastar::chunked_vector<obligation_row>
    made_rows(std::size_t count) {
        seastar::chunked_vector<obligation_row> output;
        output.reserve(count);
        for (std::size_t i = 0; i != count; ++i)
            output.emplace_back().index = static_cast<std::uint32_t>(i);
        return output;
    }
    static std::vector<std::uint32_t> made_free(std::size_t count) {
        auto output = reserved<std::uint32_t>(count);
        for (auto i = count; i != 0; --i)
            output.push_back(static_cast<std::uint32_t>(i - 1));
        return output;
    }
    // An empty table with room for `count`. Every table is made before the
    // task scope is: a scope may not be destroyed before it is closed, so
    // nothing that can fail to allocate follows it in the constructor.
    template<typename T>
    static std::vector<T> reserved(std::size_t count) {
        std::vector<T> table;
        table.reserve(count);
        return table;
    }
    static std::vector<std::unique_ptr<segment_slot>>
    made_slots(std::uint32_t count) {
        auto slots = reserved<std::unique_ptr<segment_slot>>(count);
        for (std::uint32_t i = 0; i != count; ++i)
            slots.push_back(std::make_unique<segment_slot>());
        return slots;
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
    // Refused because the budget was short: what a release of held runs
    // clears.
    static bool refused_for_memory(const local_append_acceptance& decision) {
        const auto& refused = decision.failure.error();
        return refused && refused->code() == errc::queue_full
               && refused->operation() == runtime::operation_kind::resource;
    }
    runtime::result<segment_slot*> find(local_append_target target) {
        if (
          target.slot_ >= slots_.size() || !slots_[target.slot_]->writer
          || slots_[target.slot_]->attachment != target.attachment_)
            return runtime::failure(error(errc::wrong_context));
        return slots_[target.slot_].get();
    }

    // Where a barrier of the attachment is wanted through, if anywhere: the
    // newest passed segment-placed group, a sync asked of it, and once it is
    // quiet everything passed.
    static std::optional<runtime::file_position>
    wanted_end(const segment_slot& slot) noexcept {
        auto end = slot.wanted;
        const auto take = [&end](runtime::file_position at) {
            if (!end || at > *end) end = at;
        };
        if (slot.requested) take(*slot.requested);
        if (slot.quiet && slot.submitted) take(slot.submitted->end().bytes);
        return end;
    }
    // A barrier is wanted through a point no barrier was asked to cover.
    static bool owed(const segment_slot& slot) noexcept {
        const auto end = wanted_end(slot);
        return end && (!slot.attempted || *end > *slot.attempted);
    }
    // Nothing of the attachment is forming, in flight, unpublished, owed a
    // barrier or held behind none: a barrier settled over everything passed,
    // whether it made it durable or failed.
    static bool idle(const segment_slot& slot) noexcept {
        if (
          slot.forming != 0 || slot.groups != 0 || slot.freezing || slot.head
          || slot.barrier_running || owed(slot))
            return false;
        if (!slot.submitted) return true;
        const auto end = slot.submitted->end().bytes;
        return (slot.receipt && slot.receipt->boundary().end().bytes >= end)
               || (slot.failed_through && *slot.failed_through >= end);
    }
    // Something still in flight will complete and form again: a group, or a
    // barrier that settles obligations.
    [[nodiscard]] bool in_flight() const noexcept {
        return groups_ != 0
               || std::ranges::any_of(slots_, [](const auto& slot) {
                      return slot->barrier_running || owed(*slot);
                  });
    }
    // An attachment may have become idle or synced: whoever waits for one
    // looks again.
    void settled() noexcept {
        if (quiescing_ != 0 || syncing_ != 0) quiet_.broadcast();
    }

    void request_stop() noexcept {
        stopped_ = true;
        maybe_form();
    }

    // Reports the WAL's first storage failure, and each attachment's, once
    // each, after the caller's state changes are complete: a segment's
    // failure sets its device aside and the WAL's ends what the shard can
    // make durable, so neither may hide the other. The first of them all is
    // kept.
    void observe_storage(
      bool& reported,
      local_storage_path path,
      const std::optional<segment_context>& segment,
      const runtime::first_failure& failure,
      std::optional<device_store_id> device = std::nullopt,
      std::optional<local_reported_boundary> prefix = std::nullopt) noexcept {
        if (!failure.failed() || reported) return;
        reported = true;
        auto detail = runtime::file_failure_detail::unknown;
        if (failure.error()) {
            if (auto found = runtime::file_detail(*failure.error()))
                detail = *found;
        }
        const local_storage_failure observed{
          path, segment, failure, detail, device, prefix};
        if (!storage_failure_) storage_failure_.emplace(observed);
        if (sink_) sink_(observed);
    }
    // The WAL owners latch their own storage failures; this owner reports
    // the WAL's. Every later group needs the same WAL, so this owner latches
    // it too and refuses later requests on entry.
    // Before it is reported, every attachment is asked for a sync: a group
    // the WAL accepted can still be made durable by its segment's barrier,
    // which then stands in for the WAL's.
    void observe_wal() noexcept {
        const auto& failed = commit_.storage_failure().failed()
                               ? commit_.storage_failure()
                               : writer_.failure();
        if (!failed.failed()) return;
        detail::merge_failure(first_, failed);
        if (!wal_reported_)
            for (const auto& slot : slots_)
                if (slot->writer) request_sync(*slot);
        observe_storage(
          wal_reported_, local_storage_path::wal, std::nullopt, failed);
    }
    bool
    rotation_pressure(const runtime::operation_error& refused) const noexcept {
        return (refused.code() == errc::queue_full
                || refused.code() == errc::resource_exhausted)
               && !commit_.failure().failed() && !writer_.failure().failed();
    }
    void observe_segment(segment_slot& slot) noexcept {
        if (slot.writer && slot.writer->failure().failed())
            observe_storage(
              slot.failure_reported,
              local_storage_path::segment,
              slot.segment,
              slot.writer->failure(),
              slot.writer->device(),
              slot.reported);
    }

    // Reserves the obligation row of a WAL file and segment for a forming
    // group: before anything freezes, so a full table is only pressure.
    // An attached segment's row in a file is the one its slot made there:
    // only its own groups make one, so the slot's current row is the only
    // place to look, and a file it has not formed in yet takes a free row.
    std::optional<std::uint32_t> reserve_row(
      model::wal_incarnation_id wal,
      const segment_context& segment,
      std::uint32_t slot) {
        auto& attached = *slots_[slot];
        if (attached.row) {
            auto& row = rows_[*attached.row];
            if (row.segment && *row.wal == wal && *row.segment == segment) {
                ++row.reserved;
                row.slot = slot;
                return *attached.row;
            }
        }
        const auto free = take_free();
        if (free) {
            auto& row = rows_[*free];
            row.wal.emplace(wal);
            row.segment.emplace(segment);
            row.slot = slot;
            row.reserved = 1;
            attached.row = *free;
        }
        return free;
    }
    // A free row, off the stack of rows freed: one taken some other way
    // since is passed over; with the stack empty, the table is searched.
    std::optional<std::uint32_t> take_free() noexcept {
        while (!free_rows_.empty()) {
            const auto index = free_rows_.back();
            free_rows_.pop_back();
            if (!rows_[index].segment) return index;
        }
        for (const auto& row : rows_)
            if (!row.segment) return row.index;
        return std::nullopt;
    }
    void free_row_if_unused(obligation_row& row) noexcept {
        if (row.pending == 0) row.pending_begin.reset();
        if (row.reserved == 0 && row.pending == 0 && row.pinned == 0) {
            row.wal.reset();
            row.segment.reset();
            row.slot.reset();
            row.pinned_begin.reset();
            // Reserved for every row: a push here never allocates.
            if (free_rows_.size() < free_rows_.capacity())
                free_rows_.push_back(row.index);
        }
    }
    // A WAL-placed group the WAL accepted: open until a barrier of its
    // segment covers it.
    void open(obligation_row& row, const group_state& group) noexcept {
        --row.reserved;
        if (row.pending++ == 0) {
            row.first_pending = group.sequence;
            row.pending_begin = group.wal_begin;
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
            std::optional<std::size_t> released;
            bool free = false;
            for (std::size_t i = 0; i != rows_.size() && !home; ++i) {
                const auto& row = rows_[i];
                if (!row.segment) {
                    free = true;
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
            if (!home) home = released;
            if (!home && free) home = take_free();
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
        if (!item) {
            if (refused_for_memory(decision)) relieve_holds();
            co_return not_written(decision.failure);
        }
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
        if (!item) {
            if (refused_for_memory(decision)) relieve_holds();
            co_return not_written(decision.failure);
        }
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
        else if ((*attached)->quiet)
            co_return reject(error(errc::closed));
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
        if ((*slot)->quiet) co_return reject(error(errc::closed));
        if (auto over = ended()) co_return reject(*over);
        auto child = admitted_wal_batch::make(
          std::move(request.batch),
          std::move(request.backing),
          request.append_time,
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
        // Its stream, admitted in its segment before it joins: a segment that
        // holds as many streams as its retry depth allows rolls, with nothing
        // of the request written.
        const auto id = prepared->wal.batch().info().context.submitted().id();
        auto ring = (*slot)->retry->admit(id, request.routing_epoch);
        if (!ring) co_return reject(ring.error());
        if (!*ring) {
            decision.capacity = (*slot)->writer->refuse_streams().decision;
            co_return output{};
        }
        // A stream this request opened stays only if the request joins. A
        // detach meanwhile already dropped it.
        auto opened = seastar::defer([this, target] noexcept {
            if (auto attached = find(target)) (**attached).retry->drop();
        });
        detail::local_append_item_ptr item;
        for (;;) {
            for (;;) {
                if (closing_ || closed_ || stopped_ || first_.failed())
                    co_return reject(error(errc::closed));
                slot = find(target);
                if (!slot) co_return reject(slot.error());
                if ((*slot)->quiet) co_return reject(error(errc::closed));
                if (!(*slot)->freezing) break;
                co_await (*slot)->frozen.when();
            }
            if (auto over = ended()) co_return reject(*over);
            if (forming_.size() >= maximum_wal_group_members)
                co_return reject(error(errc::queue_full));
            if (!item) {
                item = seastar::make_lw_shared<detail::local_append_item>(
                  std::move(*held),
                  std::move(*prepared),
                  target.slot_,
                  Clock::now(),
                  request.append_time);
                item->ring = *ring;
            }
            auto joined = join(**slot, *item, request.physical_begin, decision);
            if (!joined) co_return reject(joined.error());
            if (*joined) break;
            // The segment's forming members fill one WAL group: this request
            // starts the next, once a formation takes them. Later requests
            // wait behind it, so call order holds.
            maybe_form();
            auto& full = **slot;
            co_await full.frozen.when([&full] { return full.forming == 0; });
        }
        if (!decision.accepted()) co_return output{};
        (*slot)->retry->keep();
        opened.cancel();
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
    // preparation covers every forming member of the segment, and while the
    // portion is WAL-placed those members must fit one WAL group. A
    // rejection restores the previous preparation, which the just-released
    // allowance funds. False, with nothing decided, when the members already
    // forming leave no room for it in their group: it starts the next.
    // Alone, a request no group holds is impossible. The item learns whether
    // it takes a seal entry.
    runtime::result<bool> join(
      segment_slot& slot,
      detail::local_append_item& item,
      std::optional<model::segment_relative_end> physical_begin,
      local_append_acceptance& decision) {
        std::array<const encoded_assigned_batch*, maximum_segment_group_blocks>
          children{};
        std::array<wal_admission_member, maximum_wal_group_members> members{};
        std::size_t count = 0;
        auto current = slot.writer->capture();
        if (!current) return runtime::failure(current.error());
        auto physical = current->end().physical;
        // The seal entries the forming members take, and how many of them
        // are of the item's stream, which it must follow.
        const auto sequence = item.info.context.submitted().id().sequence();
        std::uint32_t entries = 0, earlier = 0;
        for (const auto& forming : forming_) {
            if (forming->slot != item.slot) continue;
            if (forming->entry) ++entries;
            if (forming->ring == item.ring) {
                if (
                  forming->info.context.submitted().id().sequence() >= sequence)
                    return runtime::failure(error(errc::invalid_argument));
                ++earlier;
            }
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
        item.entry = slot.retry->opens_entry(*item.ring, earlier);
        slot.prepared.reset();
        auto next = slot.writer->prepare_group(
          std::span{children}.first(count),
          preparation_,
          entries + (item.entry ? 1U : 0U));
        std::optional<runtime::operation_error> refused;
        if (next && next->decision == segment_capacity_decision::fits) {
            // A portion the WAL would carry must fit one WAL group. One
            // placed in its segment never takes the WAL, and stays placed
            // there as members join: its extent only grows.
            bool fits = !wal_placed(*next->prepared);
            if (!fits) {
                auto measured = writer_.measure_submission(
                  std::span{members}.first(count),
                  config_.policy,
                  commit_.submission_limits());
                if (!measured)
                    refused = measured.error();
                else
                    fits = measured->decision
                           != wal_admission_decision::too_large;
            }
            if (fits) {
                slot.prepared.emplace(std::move(*next->prepared));
                decision.capacity = segment_capacity_decision::fits;
                return true;
            }
            next->prepared.reset();
            next->decision = segment_capacity_decision::impossible;
        }
        // Impossible beside members already forming, which fill one group of
        // their segment or the WAL, is not impossible alone: the request
        // starts the next group. A roll stays a roll: it would follow them.
        const bool full = !refused && next
                          && next->decision
                               == segment_capacity_decision::impossible
                          && count > 1;
        if (count > 1) {
            auto restored = slot.writer->prepare_group(
              std::span{children}.first(count - 1), preparation_, entries);
            if (
              restored && restored->decision == segment_capacity_decision::fits)
                slot.prepared.emplace(std::move(*restored->prepared));
        }
        if (refused) return runtime::failure(*refused);
        if (full) return false;
        if (!next) return runtime::failure(next.error());
        decision.capacity = next->decision;
        return true;
    }
    // A portion is WAL-placed while its freeze would take fewer bytes than
    // the threshold.
    [[nodiscard]] bool
    wal_placed(const segment_prepared_group& prepared) const noexcept {
        return prepared.extent().size() < config_.segment_placement_bytes;
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
                if (step != formation::blocked || in_flight() || waited) break;
                // With nothing in flight, pressure clears only as memory is
                // let go: held runs are written and the deferred digest
                // releases earlier groups. Wait for that once; the requests
                // stay forming meanwhile.
                co_await release_holds();
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
        if (step == formation::blocked && !in_flight())
            fail_forming(reason(error(errc::queue_full)));
    }

    // Writes every run a segment holds, largest first, and waits until the
    // memory of what was written is let go: memory a hold takes is given
    // back by writing, never by refusing appends for good.
    seastar::future<> release_holds() {
        for (;;) {
            segment_slot* largest = nullptr;
            for (const auto& slot : slots_)
                if (
                  slot->writer && !slot->freezing
                  && slot->writer->held().value() != 0
                  && (!largest || slot->writer->held() > largest->writer->held()))
                    largest = slot.get();
            if (!largest) break;
            // The footer that closes it covers the groups held for it, as
            // one asked for a barrier does, and a barrier may then take it.
            auto closed = largest->writer->close_run(false);
            if (!closed) {
                // A run that could not be closed failed its writer, whose
                // attachment then settles what it accepted.
                if (largest->writer->failure().failed()) largest->wake.signal();
                break;
            }
            if (closed->footer()) {
                largest->closing.emplace(std::move(*closed));
                promote_closing(*largest);
                largest->wake.signal();
            }
            if (!co_await largest->writer->materialize()) break;
            ++counted_
                .runs[static_cast<std::size_t>(segment_run_close::pressed)];
        }
        for (const auto& slot : slots_)
            if (slot->writer)
                static_cast<void>(co_await slot->writer->digest_caught_up());
    }
    // A request was refused for memory while segments hold runs: they are
    // written in the background, so the next request finds the memory. One
    // release runs at a time.
    void relieve_holds() noexcept {
        if (relieving_ || closing_ || closed_) return;
        if (std::ranges::none_of(slots_, [](const auto& slot) {
                return slot->writer && slot->writer->held().value() != 0;
            }))
            return;
        relieving_ = true;
        try {
            if (!tasks_.spawn([this] { return relieve(); })) relieving_ = false;
        } catch (...) {
            relieving_ = false;
        }
    }
    seastar::future<> relieve() {
        const auto done = seastar::defer(
          [this] noexcept { relieving_ = false; });
        co_await release_holds();
        maybe_form();
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
        for (const auto& slot : slots_) {
            slot->forming = 0;
            slot->prepared.reset();
            slot->frozen.broadcast();
        }
        settled();
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
        slots_[index]->forming = 0;
        slots_[index]->prepared.reset();
        slots_[index]->frozen.broadcast();
        settled();
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
        } catch (const std::bad_alloc&) {
            co_return output{error(errc::resource_exhausted)};
        } catch (...) {
            // Anything else a run threw ended the checkpoint owner, which
            // told it: no device failed here.
            co_return output{error(errc::invariant_violation)};
        }
        co_return output{};
    }
    // What keeps the shard's closed WAL files from going, as far as this
    // owner knows: the oldest pinned PREPARE below the head's file, which
    // someone must resolve; or, with none, whether a group whose barrier is
    // still to come has its PREPAREs there, which settles by itself.
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
        held.settling = std::ranges::any_of(rows_, [&head](const auto& row) {
            return row.pending != 0 && !(*row.wal == head);
        });
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
    // admit; a set placed in its segment takes nothing of either. A set
    // never splits: its one preparation reserves one footer.
    // Moves the WAL to its next file. retry: it moved. blocked: the rotation
    // was refused while both WAL owners stayed healthy, as when the
    // control's turns are taken, or the retained limit holds only until a
    // barrier still to come settles; that is pressure, and the attempt is
    // made again. idle: it failed, and so has this owner; or the retained
    // limit held it, the forming requests were rejected and nothing failed.
    seastar::future<formation> rotate(byte_count required) {
        // A new file at the retained limit. Only a checkpoint can remove
        // one, so whoever reclaims the WAL runs one first; the rotation is
        // refused only when the limit still holds after it. What a closed
        // file holds for a segment goes only once that segment is synced,
        // so those syncs come first and the rotation waits for them. A
        // rotation the WAL writer keeps pending owns its successor already.
        if (!writer_.rotation_pending() && writer_.retention().full()) {
            if (request_closed_syncs()) co_return formation::blocked;
            auto reclaim = co_await relieve_retention();
            if (forming_.empty()) co_return formation::idle;
            if (writer_.retention().full()) {
                const auto holder = closed_files_held();
                // Only groups whose barrier is still to come keep the oldest
                // closed file. That barrier settles without anyone's
                // decision, so the forming requests wait for it as they wait
                // for any pressure a completion clears.
                if (holder.settling) co_return formation::blocked;
                refuse_retention(holder, std::move(reclaim));
                co_return formation::idle;
            }
        }
        auto rotated = co_await commit_.rotate(writer_, required, execution_);
        if (rotated) {
            rotation_bytes_.reset();
            pressure_.reset();
            static_cast<void>(request_closed_syncs());
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
        // Each chosen set's placement, fixed here: nothing moves its
        // preparation before the group takes it.
        std::vector<local_append_placement> placements;
        std::size_t count = 0;
        bool pressure = false;
        for (std::size_t i = 0; i < forming_.size(); ++i) {
            const auto index = forming_[i]->slot;
            bool seen = false;
            for (std::size_t j = 0; j < i && !seen; ++j)
                seen = forming_[j]->slot == index;
            if (seen) continue;
            auto& slot = *slots_[index];
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
            // A set placed in its segment takes nothing of the WAL group.
            if (!wal_placed(*slot.prepared)) {
                chosen.push_back(index);
                placements.push_back(local_append_placement::segment);
                continue;
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
                placements.push_back(local_append_placement::wal);
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
        // Members in acceptance order across the chosen segments, and of
        // them the WAL-placed sets' in the WAL offer.
        const auto at = [&chosen](const auto& item) {
            return static_cast<std::size_t>(
              std::find(chosen.begin(), chosen.end(), item->slot)
              - chosen.begin());
        };
        const auto included = [&chosen, &at](const auto& item) {
            return at(item) != chosen.size();
        };
        std::vector<detail::local_append_item_ptr> items;
        items.reserve(
          static_cast<std::size_t>(
            std::count_if(forming_.begin(), forming_.end(), included)));
        auto origin = Clock::now();
        count = 0;
        for (const auto& item : forming_) {
            if (!included(item)) continue;
            items.push_back(item);
            if (placements[at(item)] != local_append_placement::wal) continue;
            members[count++] = {
              &item->children.wal.batch(), &item->children.expected};
            origin = std::min(origin, item->accepted);
        }
        // A group with nothing in the WAL takes no WAL admission or offer.
        std::optional<wal_commit_admission> admission;
        std::optional<wal_group> offer;
        if (count != 0) {
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
            auto made = wal_group::make(
              budget_, static_cast<std::uint32_t>(count));
            if (!made) {
                if (made.error().code() == errc::queue_full)
                    co_return formation::blocked;
                fail_forming(reason(made.error()));
                co_return formation::idle;
            }
            admission.emplace(std::move(*admitted->admission));
            offer.emplace(std::move(*made));
        }
        auto group = make_group(chosen, placements, std::move(items));
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
              std::move(offer),
              std::move(admission)));
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
      const std::vector<local_append_placement>& placements,
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
        // One obligation row per WAL-placed segment in the current WAL
        // file, reserved before anything changes; a full table is only
        // pressure.
        std::array<std::optional<std::uint32_t>, maximum_wal_group_members>
          rows{};
        for (std::size_t b = 0; b < chosen.size(); ++b) {
            if (placements[b] != local_append_placement::wal) continue;
            rows[b] = reserve_row(
              wal->reserved.incarnation(),
              *slots_[chosen[b]]->segment,
              chosen[b]);
            if (!rows[b]) {
                for (std::size_t r = 0; r < b; ++r) {
                    if (!rows[r]) continue;
                    --rows_[*rows[r]].reserved;
                    free_row_if_unused(rows_[*rows[r]]);
                }
                return runtime::failure(error(errc::queue_full));
            }
        }
        // Then the transfer, which cannot fail.
        for (std::uint32_t b = 0; b < chosen.size(); ++b) {
            const auto index = chosen[b];
            auto& part = *group->branches[b];
            part.placement = placements[b];
            part.row = rows[b];
            auto& slot = *slots_[index];
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
        group->remaining = static_cast<std::uint32_t>(chosen.size());
        ++groups_;
        return group;
    }

    // Nothing froze: every member is not_written, and formation continues.
    void
    unwind_group(group_state& group, const runtime::first_failure& failure) {
        settled();
        for (auto& part : group.branches) {
            auto& slot = *slots_[part->slot];
            slot.freezing = false;
            slot.frozen.broadcast();
            part->prepared.reset();
            unreserve(*part);
            detail::merge_failure(part->failure, failure);
        }
        group.wal_decided = true;
        for (auto& part : group.branches)
            publish_unfrozen(*part);
        group.submitted.set_value();
        release(group);
    }

    // The one task of a formed group. Under the formation lock it freezes the
    // segments, starts each segment-placed group's submission, offers the
    // WAL-placed members at their final positions and has the WAL accept
    // them. Then it submits each WAL-placed group once encoded, without
    // waiting for the WAL barrier, or drops it if the WAL took none, settles
    // the WAL result and waits until every branch is published and passed.
    // Its segments' barrier loops settle the rest.
    seastar::future<> run_group(
      group_ptr group,
      seastar::future<runtime::first_failure> start,
      std::optional<wal_group> offer,
      std::optional<wal_commit_admission> admission) {
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
            if (part->submitting) {
                co_await std::move(*part->submitting);
                part->submitting.reset();
                continue;
            }
            if (!part->queued || part->ready) continue;
            // A segment-placed group needs nothing of the WAL.
            if (
              part->placement == local_append_placement::segment
              || group->wal_accepted)
                co_await submit_branch(*part);
            else
                co_await drop_branch(*part);
        }
        if (group->wal_accepted) co_await settle_wal(*group);
        if (group->remaining != 0) co_await group->done.get_future();
        for (auto& part : group->branches)
            co_await join_write(*part);
        release(*group);
        maybe_form();
    }

    // Freezes each segment, starts each segment-placed group's submission,
    // offers the WAL-placed members at their final positions and has the WAL
    // accept them. Encoding starts at freeze. Branches that did not freeze
    // are published here; the rest publish in their segment's file order.
    seastar::future<> submit_group(
      group_state& group,
      std::optional<wal_group> offer,
      std::optional<wal_commit_admission> admission) {
        runtime::first_failure refused;
        bool unknown = false;
        std::optional<wal_commit_ticket> ticket;
        try {
            for (auto& part : group.branches) {
                co_await freeze(*part);
                auto& slot = *slots_[part->slot];
                slot.freezing = false;
                slot.frozen.broadcast();
                slot.wake.signal();
            }
            // A segment-placed group goes to its segment once encoded: its
            // durability waits for nothing else.
            for (auto& part : group.branches)
                if (
                  part->queued
                  && part->placement == local_append_placement::segment)
                    part->submitting.emplace(submit_branch(*part));
            for (const auto& item : group.items) {
                auto& part = *group.branches[item->branch];
                if (
                  !part.queued || part.placement != local_append_placement::wal)
                    continue;
                // Its PREPARE states the stamp its block carries.
                if (
                  auto added = offer->append(
                    std::move(item->children.wal),
                    item->children.expected,
                    part.certified_end);
                  !added)
                    refused.observe(added.error());
            }
            // The sole submitter: the group's PREPAREs begin at the reserved
            // end, so every group the WAL accepts knows where.
            if (offer && offer->size() != 0) {
                auto wal = writer_.positions();
                if (!wal)
                    refused.observe(wal.error());
                else if (!refused.failed()) {
                    group.wal_begin.emplace(wal->reserved);
                    // The WAL may hold these PREPAREs before its answer is
                    // installed below; until then the snapshot counts them
                    // open.
                    submitting_.emplace(wal->reserved);
                    try {
                        auto accepted = co_await commit_.template submit<Clock>(
                          writer_,
                          std::move(*offer),
                          std::move(*admission),
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
            }
        } catch (...) {
            refused.observe(std::current_exception());
        }
        for (auto& part : group.branches) {
            auto& slot = *slots_[part->slot];
            if (slot.freezing) {
                slot.freezing = false;
                slot.frozen.broadcast();
            }
        }
        settled();
        if (ticket) {
            // The result observer uses the group's pre-admitted slot.
            auto observed = ticket->observe();
            if (observed) group.observed.emplace(std::move(*observed));
            group.ticket.emplace(std::move(*ticket));
            group.wal_accepted = true;
            group.sequence = ++sequences_;
            // The WAL file now holds every frozen WAL-placed group's
            // PREPAREs: each is an obligation until a barrier of its segment
            // covers it.
            for (auto& part : group.branches) {
                if (!part->row) continue;
                if (!part->queued) {
                    unreserve(*part);
                    continue;
                }
                open(rows_[*part->row], group);
                part->obliged = true;
            }
        } else {
            // No PREPARE was accepted; frozen WAL-placed groups are dropped
            // unwritten. Without that proof a PREPARE may exist, and its
            // obligation is pinned until recovery resolves it.
            detail::merge_failure(first_, refused);
            group.wal_failure = refused;
            group.wal_unknown = unknown;
            if (unknown) group.sequence = ++sequences_;
            for (auto& part : group.branches) {
                if (unknown && part->queued && part->row) {
                    auto& row = rows_[*part->row];
                    --row.reserved;
                    pin(row, 1, group.sequence, group.wal_begin);
                    part->row.reset();
                } else {
                    unreserve(*part);
                }
            }
        }
        group.wal_decided = true;
        submitting_.reset();
        for (auto& part : group.branches)
            if (!part->queued) publish_unfrozen(*part);
        for (auto& part : group.branches)
            if (part->queued) publish_ready(*slots_[part->slot]);
        // Reported once every decision above is installed.
        if (!ticket && refused.failed()) observe_wal();
        for (auto& part : group.branches)
            if (part->failure.failed()) observe_segment(*slots_[part->slot]);
    }

    // count groups of the row are pinned; sequence and begin are where the
    // oldest of them lies.
    void pin(
      obligation_row& row,
      std::uint32_t count,
      std::uint64_t sequence,
      const std::optional<local_wal_cursor>& begin) noexcept {
        row.pinned += count;
        row.live += count;
        if (begin && (!row.pinned_begin || sequence < row.first_pinned)) {
            row.first_pinned = sequence;
            row.pinned_begin.emplace(*begin);
        }
    }

    // Freezes one segment's forming set at the positions its preparation
    // measured, gives each member's WAL expectation its final block target,
    // queues the group in its segment's file order and starts encoding.
    seastar::future<> freeze(branch& part) {
        auto& slot = *slots_[part.slot];
        std::optional<runtime::result<segment_frozen_group>> frozen;
        if (!part.prepared) {
            part.failure.observe(error(errc::closed));
            co_return;
        }
        // What the portion takes there, counted once it froze.
        const auto bytes = part.prepared->extent().size().value();
        try {
            std::vector<admitted_wal_batch> children;
            children.reserve(part.items.size());
            for (auto& item : part.items)
                children.push_back(std::move(item->children.segment));
            // A WAL-placed group is held in its segment's run; one placed
            // there alone is written at once.
            frozen.emplace(
              co_await slot.writer->freeze_group(
                std::move(*part.prepared),
                std::move(children),
                execution_,
                part.placement == local_append_placement::wal
                  ? config_.held_run_bytes
                  : byte_count{}));
        } catch (...) {
            part.failure.observe(std::current_exception());
        }
        part.prepared.reset();
        if (frozen && !*frozen) part.failure.observe(frozen->error());
        if (part.failure.failed()) co_return;
        part.frozen.emplace(std::move(**frozen));
        const auto placed = static_cast<std::size_t>(part.placement);
        ++counted_.portions[placed];
        counted_.portion_bytes[placed] += bytes;
        const auto closed = part.frozen->closed();
        if (closed) ++counted_.runs[static_cast<std::size_t>(*closed)];
        if (part.frozen->held() || closed == segment_run_close::filled) {
            ++counted_.held_portions;
            counted_.held_portion_bytes += bytes;
        }
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
        part.certified_end = layout.certified_end();
        // Its batches are in the segment's order now, and the group reserved
        // a seal entry for each that opens one.
        for (const auto& item : part.items)
            slot.retry->froze(
              *item->ring,
              item->info.context.submitted().id().sequence(),
              item->entry);
        if (slot.tail)
            slot.tail->next = &part;
        else
            slot.head = &part;
        slot.tail = &part;
        if (!slot.cursor) slot.cursor = &part;
        if (!slot.frontier) slot.frontier = &part;
        part.queued = true;
        part.encoded.emplace(
          slot.writer->encode_group(*part.frozen, part.work));
    }

    // Submits once encoded, without waiting for any barrier: a
    // segment-placed group at once, a WAL-placed one once the WAL accepted
    // it. Its segment's barrier loop covers it once it is passed.
    seastar::future<> submit_branch(branch& part) {
        auto& slot = *slots_[part.slot];
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

    // Passes every ready group at the front of its segment's frontier: a
    // barrier may now cut past the submitted ones that end with a footer,
    // and an accepted one dropped unwritten can never be covered, so its
    // obligation is pinned. A segment-placed one wants its barrier now.
    void mark_ready(branch& part) noexcept {
        part.ready = true;
        auto& slot = *slots_[part.slot];
        while (slot.frontier && slot.frontier->ready) {
            auto& passing = *std::exchange(slot.frontier, slot.frontier->next);
            passing.passed = true;
            if (passing.submitted) {
                const auto& cut = *passing.boundary;
                slot.submitted.emplace(cut);
                if (passing.obliged) {
                    auto& row = rows_[*passing.row];
                    if (cut.footer())
                        ++row.written;
                    else {
                        ++row.held;
                        ++slot.held;
                    }
                }
                if (cut.footer()) cover(slot, cut);
                if (passing.placement == local_append_placement::segment)
                    slot.wanted = cut.end().bytes;
            } else if (passing.obliged) {
                auto& row = rows_[*passing.row];
                --row.pending;
                pin(row, 1, passing.group->sequence, passing.group->wal_begin);
                free_row_if_unused(row);
                passing.obliged = false;
            }
        }
        promote_closing(slot);
        slot.wake.signal();
        publish_ready(slot);
    }
    // A passed footer covers every group passed before it: those held for a
    // footer to close their run are now what a barrier through it covers.
    void
    cover(segment_slot& slot, const segment_captured_boundary& cut) noexcept {
        if (slot.footed && slot.footed->end().bytes >= cut.end().bytes) return;
        slot.footed.emplace(cut);
        if (slot.held == 0) return;
        slot.held = 0;
        for (auto& row : rows_) {
            if (row.held == 0 || *row.segment != *slot.segment) continue;
            row.written += row.held;
            row.held = 0;
        }
    }
    // A footer asked of the writer to close its held run is a cut a barrier
    // may take once every group frozen before it is passed: the oldest
    // group not passed lies after it.
    void promote_closing(segment_slot& slot) noexcept {
        if (!slot.closing) return;
        if (
          slot.frontier
          && slot.frontier->boundary->end().bytes
               <= slot.closing->footer()->begin())
            return;
        const auto closing = std::move(*slot.closing);
        slot.closing.reset();
        cover(slot, closing);
    }

    // Copies the WAL result, then releases the group's WAL admission: WAL
    // reclamation is tracked by obligations, not by holding a group commit
    // slot. Each segment publishes what the result decides.
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
            if (part->queued) publish_ready(*slots_[part->slot]);
        if (group.wal_failure.failed()) observe_wal();
        maybe_form();
    }

    // Joins a submitted write's completion; its outcome already reached the
    // barrier that covered the branch.
    seastar::future<> join_write(branch& part) {
        if (!part.written) co_return;
        try {
            static_cast<void>(co_await std::move(*part.written));
        } catch (...) {
        }
        part.written.reset();
    }

    // One per attachment. It runs a barrier when one is wanted through a
    // point no barrier was asked to cover: by a segment-placed group, whose
    // acknowledgement waits for it, by a sync asked of the attachment, or,
    // once the attachment is quiet, by everything passed. A barrier cuts at
    // the newest passed footer, never at the reserved end, which can include
    // frozen groups not yet submitted; through a held run it first has the
    // writer close the run with a footer of its own, and waits for the
    // groups before that footer to pass. A barrier no acknowledgement waits
    // for takes one of the owner's concurrent syncs. Stopped, it returns
    // once nothing of the attachment is queued and no barrier is owed.
    seastar::future<> barrier_loop(std::uint32_t index) {
        auto& slot = *slots_[index];
        for (;;) {
            std::optional<segment_captured_boundary> cut;
            std::optional<seastar::semaphore_units<>> unit;
            try {
                cut = next_cut(slot);
                if (cut && !acknowledging(slot)) {
                    unit = seastar::try_get_units(syncs_, 1);
                    if (!unit) {
                        cut.reset();
                        if (!slot.sync_waiting) {
                            slot.sync_waiting = true;
                            sync_waiters_.push_back(index);
                        }
                    }
                }
            } catch (...) {
                first_.observe(std::current_exception());
                cut.reset();
            }
            if (!cut) {
                if (slot.stop && !slot.head && !owed(slot)) break;
                try {
                    co_await slot.wake.when();
                } catch (...) {
                    first_.observe(std::current_exception());
                }
                continue;
            }
            // It covers exactly the groups passed before it starts that its
            // cut reaches: each row's written ones, and when the writer
            // failed and it cuts at the newest passed group, the held ones.
            const bool whole = slot.writer->failure().failed();
            if (whole) slot.held = 0;
            for (auto& row : rows_) {
                if (!row.segment || *row.segment != *slot.segment) continue;
                if (whole) {
                    row.written += row.held;
                    row.held = 0;
                }
                row.covering = row.written;
            }
            if (unit)
                ++counted_.syncs;
            else
                ++counted_.barriers;
            slot.attempted = cut->end().bytes;
            slot.barrier_running = true;
            std::optional<segment_barrier_outcome> outcome;
            try {
                outcome.emplace(co_await slot.writer->barrier(*cut));
            } catch (...) {
                outcome.emplace();
                outcome->failure.observe(std::current_exception());
            }
            slot.barrier_running = false;
            if (unit) {
                unit.reset();
                wake_sync_waiter();
            }
            settle_barrier(slot, *cut, *outcome);
            if (outcome->failure.failed()) observe_segment(slot);
            offer_reclaim();
            maybe_form();
        }
        slot.looping = false;
    }

    // An acknowledgement waits for the barrier the attachment owes.
    static bool acknowledging(const segment_slot& slot) noexcept {
        return slot.wanted
               && (!slot.attempted || *slot.wanted > *slot.attempted);
    }
    // The cut the owed barrier takes once it can: the newest passed footer
    // through where it is wanted, or, once the writer failed, the newest
    // passed group, which the barrier refuses. Through a held run the writer
    // is first asked to close it.
    std::optional<segment_captured_boundary> next_cut(segment_slot& slot) {
        if (!owed(slot)) return std::nullopt;
        const auto target = *wanted_end(slot);
        if (slot.writer->failure().failed()) {
            // Nothing more becomes durable here: once every frozen group is
            // passed, a barrier through the newest submitted one fails and
            // settles them; with none, nothing is left to settle.
            if (slot.frontier) return std::nullopt;
            if (
              !slot.submitted
              || (slot.attempted && slot.submitted->end().bytes <= *slot.attempted)) {
                slot.attempted = target;
                return std::nullopt;
            }
            return slot.submitted;
        }
        promote_closing(slot);
        if (slot.footed && slot.footed->end().bytes >= target)
            return slot.footed;
        // Every frozen group is in the writer's reserved prefix; past its
        // newest footer lies the run it holds open.
        const auto held = slot.writer->held();
        const auto reserved = slot.writer->progress()->reserved.bytes;
        if (
          !slot.closing && !slot.freezing && held.value() != 0
          && target.value() > reserved.value() - held.value()) {
            auto closed = slot.writer->close_run(true);
            // A run that could not be closed failed its writer: what it
            // accepted settles as a failed writer's does.
            if (!closed && slot.writer->failure().failed())
                return next_cut(slot);
            if (closed && closed->footer()) {
                ++counted_
                    .runs[static_cast<std::size_t>(segment_run_close::synced)];
                slot.closing.emplace(std::move(*closed));
                promote_closing(slot);
                if (slot.footed && slot.footed->end().bytes >= target)
                    return slot.footed;
            }
        }
        return std::nullopt;
    }
    // A sync was let go: the attachment waiting longest for one tries again.
    void wake_sync_waiter() noexcept {
        while (!sync_waiters_.empty()) {
            const auto index = sync_waiters_.front();
            sync_waiters_.erase(sync_waiters_.begin());
            auto& slot = *slots_[index];
            if (!slot.sync_waiting) continue;
            slot.sync_waiting = false;
            slot.wake.signal();
            return;
        }
    }
    // Asks for a barrier of the attachment through its newest frozen group;
    // whether that asks for more than was asked already.
    static bool request_sync(segment_slot& slot) noexcept {
        std::optional<runtime::file_position> end;
        if (slot.tail)
            end = slot.tail->boundary->end().bytes;
        else if (slot.submitted)
            end = slot.submitted->end().bytes;
        if (!end) return false;
        const bool more = !slot.requested || *end > *slot.requested;
        if (more) slot.requested = end;
        slot.wake.signal();
        return more;
    }
    // Asks every attachment whose WAL-placed groups a file behind the head
    // still holds for a sync: once synced they no longer keep that file.
    // Whether any such group is still open.
    bool request_closed_syncs() noexcept {
        const auto wal = writer_.progress();
        if (!wal) return false;
        const auto head = wal->reserved.incarnation();
        bool open = false;
        std::uint64_t asked = 0;
        for (const auto& row : rows_) {
            if (row.pending == 0 || *row.wal == head) continue;
            open = true;
            if (!row.slot) continue;
            auto& slot = *slots_[*row.slot];
            if (slot.writer && *slot.segment == *row.segment)
                asked += request_sync(slot) ? 1 : 0;
        }
        if (asked != 0) {
            ++counted_.sync_rounds;
            counted_.round_syncs += asked;
        }
        return open;
    }

    // Installs a barrier's outcome: the newest durable footer, or how far
    // failure reached; the groups it covered in each row, discharged or
    // pinned; then what that decides for publication.
    void settle_barrier(
      segment_slot& slot,
      const segment_captured_boundary& cut,
      const segment_barrier_outcome& outcome) noexcept {
        const bool durable = !outcome.failure.failed() && outcome.receipt;
        if (durable) {
            slot.receipt.emplace(*outcome.receipt);
            const auto& reached = outcome.receipt->boundary();
            if (reached.footer())
                slot.durable.emplace(boundary_of(slot, reached));
        } else {
            slot.failed_through = cut.end().bytes;
            detail::merge_failure(slot.barrier_failure, outcome.failure);
            if (!slot.barrier_failure.failed())
                slot.barrier_failure.observe(error(errc::io_failure));
        }
        for (auto& row : rows_) {
            if (row.covering == 0 || *row.segment != *slot.segment) continue;
            row.pending -= row.covering;
            row.written -= row.covering;
            if (!durable)
                pin(row, row.covering, row.first_pending, row.pending_begin);
            row.covering = 0;
            free_row_if_unused(row);
        }
        publish_ready(slot);
        settled();
    }

    void stop_loop(segment_slot& slot) noexcept {
        slot.stop = true;
        slot.wake.signal();
    }

    static local_durable_boundary boundary_of(
      const segment_slot& slot, const segment_captured_boundary& cut) noexcept {
        return local_durable_boundary{
          slot.device, cut.history(), cut.covered(), *cut.footer()};
    }
    static local_reported_boundary reported_of(
      const segment_slot& slot, const segment_captured_boundary& cut) noexcept {
        const auto covered = cut.covered();
        return local_reported_boundary{
          slot.device,
          cut.history(),
          storage::coverage{
            covered.logical(),
            covered.physical(),
            model::file_byte_span::make(
              cut.history().data_start, cut.end().bytes)
              .value()}};
    }

    // The WAL side of a branch when its result is decided.
    static local_wal_state wal_state(const branch& part) noexcept {
        const auto& group = *part.group;
        if (part.placement == local_append_placement::segment || !part.queued)
            return local_wal_state::none;
        if (!group.wal_decided) return local_wal_state::deciding;
        if (!group.wal_accepted)
            return group.wal_unknown ? local_wal_state::unknown
                                     : local_wal_state::refused;
        if (!group.wal_settled) return local_wal_state::pending;
        return group.wal_receipt && !group.wal_failure.failed()
                 ? local_wal_state::durable
                 : local_wal_state::failed;
    }
    // Its segment side: what the newest successful barrier and the failed
    // ones say of its footer.
    static local_segment_state
    segment_state(const segment_slot& slot, const branch& part) noexcept {
        if (!part.queued) return local_segment_state::unfrozen;
        if (!part.ready) return local_segment_state::frozen;
        if (!part.submitted) return local_segment_state::dropped;
        const auto end = part.boundary->end().bytes;
        if (slot.receipt && end <= slot.receipt->boundary().end().bytes)
            return local_segment_state::durable;
        if (slot.failed_through && end <= *slot.failed_through)
            return local_segment_state::failed;
        return local_segment_state::submitted;
    }
    static local_append_verdict
    verdict_of(const segment_slot& slot, const branch& part) {
        return classify_local_append(
                 part.placement,
                 wal_state(part),
                 segment_state(slot, part),
                 slot.broken ? local_earlier_state::failed
                             : local_earlier_state::durable)
          .verdict;
    }

    // Publishes the front of a segment's file order while its results are
    // decided, never past one that is not; then lets go of the groups both
    // published and passed.
    void publish_ready(segment_slot& slot) noexcept {
        while (slot.cursor) {
            auto& part = *slot.cursor;
            const auto verdict = verdict_of(slot, part);
            if (verdict == local_append_verdict::wait) break;
            slot.cursor = part.next;
            publish_branch(part, verdict);
        }
        while (slot.head && slot.head->published && slot.head->passed) {
            auto* part = std::exchange(slot.head, slot.head->next);
            part->next = nullptr;
            if (!slot.head) slot.tail = nullptr;
            finish(*part->group);
        }
    }
    // A branch that never froze has no place in its segment's order.
    void publish_unfrozen(branch& part) noexcept {
        publish_branch(part, verdict_of(*slots_[part.slot], part));
        finish(*part.group);
    }

    // Installs a branch's result in each of its members, then wakes them:
    // every outcome and bound is installed before any waiter is woken.
    void publish_branch(branch& part, local_append_verdict verdict) noexcept {
        auto& slot = *slots_[part.slot];
        part.published = true;
        const auto why = failure_of(slot, part, verdict);
        for (const auto& item : part.items)
            install(*item, outcome(*item, slot, part, verdict, why));
        if (part.queued) {
            if (verdict == local_append_verdict::durable) {
                // Its batches' facts, in file order, as each is published.
                for (const auto& item : part.items)
                    slot.retry->add(
                      *item->ring,
                      item->info.context.submitted().id().sequence(),
                      item->info.fingerprint,
                      item->info.context.logical_span());
                slot.reported.emplace(reported_of(slot, *part.boundary));
                // Readers may be given it while its segment holds it.
                static_cast<void>(slot.writer->report(*part.boundary));
            } else if (!slot.broken) {
                slot.broken = true;
                slot.broken_failure = why;
            }
        }
        --slot.groups;
        settled();
        for (const auto& item : part.items)
            wake(*item);
    }

    // Why a branch is not durable: for an uncertain WAL-placed one the WAL's
    // failure first, then its own, the WAL's, its segment barrier's and the
    // earlier failure of its segment, the first kept.
    static runtime::first_failure failure_of(
      const segment_slot& slot,
      const branch& part,
      local_append_verdict verdict) {
        runtime::first_failure why;
        if (verdict == local_append_verdict::durable) return why;
        const bool logged = part.placement == local_append_placement::wal;
        if (logged && verdict == local_append_verdict::uncertain)
            detail::merge_failure(why, part.group->wal_failure);
        detail::merge_failure(why, part.failure);
        if (logged) detail::merge_failure(why, part.group->wal_failure);
        if (segment_state(slot, part) == local_segment_state::failed)
            detail::merge_failure(why, slot.barrier_failure);
        if (slot.broken) detail::merge_failure(why, slot.broken_failure);
        if (!why.failed())
            why.observe(error(
              verdict == local_append_verdict::not_written ? errc::closed
                                                           : errc::io_failure));
        return why;
    }

    static local_append_outcome outcome(
      const detail::local_append_item& item,
      const segment_slot& slot,
      const branch& part,
      local_append_verdict verdict,
      const runtime::first_failure& why) {
        local_append_outcome output;
        if (verdict != local_append_verdict::durable) {
            output.status = verdict == local_append_verdict::uncertain
                              ? local_append_status::uncertain
                              : local_append_status::not_written;
            output.failure = why;
            return output;
        }
        output.status = local_append_status::durable;
        std::optional<wal_durable_receipt> wal;
        if (wal_state(part) == local_wal_state::durable)
            wal = part.group->wal_receipt;
        std::optional<segment_durable_receipt> covered;
        if (segment_state(slot, part) == local_segment_state::durable)
            covered = slot.receipt;
        output.receipt.emplace(
          local_append_receipt{
            item.info,
            *item.block,
            {item.append_time, part.certified_end},
            part.placement,
            *part.boundary,
            std::move(wal),
            std::move(covered)});
        return output;
    }

    // A branch of the group is published and, if frozen, passed.
    static void finish(group_state& group) noexcept {
        if (--group.remaining == 0) group.done.set_value();
    }

    void release(group_state& group) noexcept {
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
    std::vector<std::unique_ptr<segment_slot>> slots_;
    seastar::chunked_vector<obligation_row> rows_;
    // Rows freed, the latest last: see take_free().
    std::vector<std::uint32_t> free_rows_;
    // Where the group being submitted to the WAL begins, until its answer is
    // installed. Submissions are serialized, so there is at most one.
    std::optional<local_wal_cursor> submitting_;
    // The size a rotation refused for pressure was asked with, while the WAL
    // writer keeps that rotation pending.
    std::optional<byte_count> rotation_bytes_;
    std::uint64_t sequences_{0};
    // Accepted requests not yet in a group, in acceptance order.
    std::vector<detail::local_append_item_ptr> forming_;
    // Attachments waiting for one of the concurrent syncs, oldest first.
    std::vector<std::uint32_t> sync_waiters_;
    seastar::semaphore syncs_;
    // Acceptance in call order, then one formation at a time.
    seastar::semaphore accept_order_{1}, form_lock_{1};
    seastar::gate operations_;
    // Close waits here for formations and groups to drain.
    seastar::condition_variable drained_;
    // quiesce() waits here for its attachment to be idle, and sync() for its
    // barrier, while any does.
    seastar::condition_variable quiet_;
    std::uint32_t quiescing_{0}, syncing_{0};
    // Close ended those waits.
    bool unquiet_{false};
    // Accepted work never uses a caller's abort source.
    seastar::abort_source abort_;
    codec::cooperative_work execution_, preparation_;
    local_failure_sink sink_;
    std::optional<local_storage_failure> storage_failure_;
    bool wal_reported_{false}, internal_reported_{false};
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
    // What was placed, held and synced: the counts statistics() returns.
    local_append_statistics counted_{};
    std::uint32_t requests_{0}, groups_{0}, formations_{0};
    bool form_pending_{false}, closing_{false}, closed_{false};
    // A release of held runs runs in the background.
    bool relieving_{false};
    bool shutdown_bound_{false}, stopped_{false};
    // Formations, group tasks and barrier loops; joined last by close().
    runtime::task_scope tasks_;
};

} // namespace kwaque::storage
