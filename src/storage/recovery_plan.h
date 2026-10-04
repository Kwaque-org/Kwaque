#pragma once

#include "src/storage/recovery_merge.h"

#include <span>
#include <vector>

namespace kwaque::storage {

// What startup may change in one scope. No action truncates, rewrites or
// deletes: a recovered WAL head is closed by its successor, and a segment's
// suffix leaves only through a durable decision's recovered seal.
enum class recovery_plan_action : std::uint8_t {
    // Proven corruption, a conflict or a misplaced claim: nothing in this
    // scope changes, and its evidence stays for a supplied decision.
    stop,
    // Nothing changes: a sealed segment, a recovering one already pinned at
    // its recovered boundary, or a WAL that was never activated.
    retain,
    // One fresh flush, then a recovering publication pinning the recovered
    // boundary, never below its pin. The segment stays roll-required.
    publish_recovering,
    // One fresh flush of the head, then a successor whose predecessor cursor
    // is the head's classified content end.
    activate_successor,
};
enum class recovery_plan_reason : std::uint8_t {
    none,
    // Damage below pinned certification.
    corruption,
    // Differing copies, a target out of WAL order or a slot that cannot be
    // placed.
    conflict,
    // A PREPARE claims a position its target cannot hold.
    misplaced,
};

struct recovery_segment_plan final {
    segment_context segment;
    recovery_plan_action action{recovery_plan_action::stop};
    recovery_plan_reason reason{recovery_plan_reason::none};
    // What a recovering publication pins: the recovered boundary, or nothing
    // for a segment with no footer.
    std::optional<local_footer_reference> boundary;
    // The logical end that boundary covers, or the segment's logical origin
    // without one. Nothing after it is certified here.
    model::range_logical_end recovered_end{};
    // Kept for a supplied resolution; never served or truncated here.
    std::uint32_t candidates{0}, suffix{0};
};
struct recovery_wal_plan final {
    recovery_plan_action action{recovery_plan_action::retain};
    recovery_plan_reason reason{recovery_plan_reason::none};
    // The successor's predecessor cursor.
    std::optional<local_wal_cursor> predecessor;
    // PREPAREs without an independent target; the WAL holding them stays.
    std::uint64_t unresolved{0};
    // Rotated files with nonzero bytes after their sealed end.
    std::uint32_t nonzero_slack{0};
};

namespace detail {
// One segment's plan from its merged report alone. A boundary below the
// segment's pin is an invariant violation: the walk starts at the pin.
[[nodiscard]] runtime::result<recovery_segment_plan>
plan_recovery_segment(const recovery_segment_report&);
// The WAL's plan from its scan and whether any of its regions was proven
// corruption.
[[nodiscard]] recovery_wal_plan
plan_recovery_wal(const wal_scan_result&, bool corrupt_region) noexcept;
} // namespace detail

// The read-only plan of one complete merge, observed item by item in delivery
// order. It reads and writes no storage and keeps one entry per target.
class recovery_planner final {
public:
    [[nodiscard]] static runtime::result<recovery_planner>
    make(std::span<const recovery_target> targets, workload_budget& budget);

    [[nodiscard]] runtime::result<void> observe(const recovery_item& item);
    // After the merge returned: it must have run to completion and every
    // target must have been reported.
    [[nodiscard]] runtime::result<void>
    finish(const recovery_merge_result& merged);

    [[nodiscard]] const recovery_wal_plan& wal() const noexcept { return wal_; }
    [[nodiscard]] std::span<const recovery_segment_plan>
    segments() const noexcept {
        return segments_;
    }
    // No scope stopped. Otherwise no mutation of the plan may run.
    [[nodiscard]] bool ready() const noexcept { return ready_; }

private:
    struct observed final {
        std::optional<recovery_segment_report> report;
    };
    recovery_planner(
      workload_reservation held,
      std::vector<recovery_segment_plan> segments,
      std::vector<observed> reports) noexcept
      : held_(std::move(held))
      , segments_(std::move(segments))
      , reports_(std::move(reports)) {}

    workload_reservation held_;
    std::vector<recovery_segment_plan> segments_;
    std::vector<observed> reports_;
    recovery_wal_plan wal_;
    bool wal_corrupt_{false}, finished_{false}, ready_{false};
};

} // namespace kwaque::storage
