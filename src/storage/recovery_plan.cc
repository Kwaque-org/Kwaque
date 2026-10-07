#include "src/storage/recovery_plan.h"

#include <algorithm>
#include <type_traits>

namespace kwaque::storage {
namespace {
using detail::path_error;
} // namespace

runtime::result<recovery_planner> recovery_planner::make(
  std::span<const recovery_target> targets, workload_budget& budget) {
    if (auto valid = detail::validate_recovery_targets(targets); !valid)
        return runtime::failure(valid.error());
    byte_count total;
    for (const auto bytes :
         {targets.size() * sizeof(recovery_segment_plan),
          targets.size() * sizeof(observed)}) {
        if (bytes == 0) continue;
        auto charged = budget.allocation_charge(byte_count{bytes});
        if (!charged) return runtime::failure(charged.error());
        auto next = total.checked_add(*charged);
        if (!next) return runtime::failure(path_error(errc::out_of_range));
        total = *next;
    }
    auto held = budget.try_reserve(total);
    if (!held) return runtime::failure(held.error());
    std::vector<recovery_segment_plan> segments;
    segments.reserve(targets.size());
    for (const auto& target : targets) {
        recovery_segment_plan plan{target.descriptor.segment};
        plan.recovered_end = target.descriptor.logical_origin;
        segments.push_back(plan);
    }
    return recovery_planner{
      std::move(*held),
      std::move(segments),
      std::vector<observed>(targets.size())};
}

runtime::result<void> recovery_planner::observe(const recovery_item& item) {
    if (finished_) return runtime::failure(path_error(errc::closed));
    return std::visit(
      [this](const auto& value) -> runtime::result<void> {
          using T = std::remove_cvref_t<decltype(value)>;
          if constexpr (std::is_same_v<T, recovery_segment_report>) {
              const auto found = std::find_if(
                segments_.begin(), segments_.end(), [&](const auto& plan) {
                    return plan.segment == value.segment;
                });
              if (found == segments_.end())
                  return runtime::failure(path_error(errc::invalid_argument));
              auto& slot
                = reports_[static_cast<std::size_t>(found - segments_.begin())];
              if (slot.report)
                  return runtime::failure(path_error(errc::invalid_argument));
              slot.report = value;
          } else if constexpr (std::is_same_v<T, recovery_region_report>) {
              if (!value.wal) return {};
              const auto classification = value.matched->classification;
              if (classification == recovery_classification::corruption)
                  wal_corrupt_ = true;
              if (
                classification == recovery_classification::slack
                && value.evidence[static_cast<std::size_t>(
                  recovery_predicate::damage)])
                  ++wal_.nonzero_slack;
          } else if constexpr (std::is_same_v<T, recovery_unresolved_report>) {
              ++wal_.unresolved;
          }
          return {};
      },
      item);
}

namespace detail {
runtime::result<recovery_segment_plan>
plan_recovery_segment(const recovery_segment_report& report) {
    recovery_segment_plan plan{report.segment};
    plan.candidates = report.candidates;
    plan.suffix = report.suffix;
    const auto stop = [&plan](recovery_plan_reason reason) {
        plan.action = recovery_plan_action::stop;
        plan.reason = reason;
        return plan;
    };
    if (report.verdict == segment_scan_verdict::corrupt)
        return stop(recovery_plan_reason::corruption);
    if (report.conflicts != 0) return stop(recovery_plan_reason::conflict);
    if (report.misplaced != 0) return stop(recovery_plan_reason::misplaced);
    if (report.state == local_object_state::sealed) {
        plan.action = recovery_plan_action::retain;
        return plan;
    }
    if (report.boundary) {
        plan.boundary = report.boundary->footer;
        plan.recovered_end = report.boundary->fields.coverage.logical().end();
    }
    // The walk starts at the pin, so its boundary is never below it; only a
    // damaged pin, already stopped, could be.
    if (
      report.pin
      && (!plan.boundary || plan.boundary->position() < report.pin->position()))
        return runtime::failure(path_error(errc::invariant_violation));
    plan.action = report.state == local_object_state::recovering
                      && plan.boundary == report.pin
                    ? recovery_plan_action::retain
                    : recovery_plan_action::publish_recovering;
    return plan;
}

recovery_wal_plan
plan_recovery_wal(const wal_scan_result& scan, bool corrupt_region) noexcept {
    recovery_wal_plan plan;
    plan.files = scan.files;
    plan.chain = scan.chain;
    plan.chain_files = scan.chain_files;
    if (scan.verdict == wal_scan_verdict::corrupt || corrupt_region) {
        plan.action = recovery_plan_action::stop;
        plan.reason = recovery_plan_reason::corruption;
    } else if (scan.content_end) {
        plan.action = recovery_plan_action::activate_successor;
        plan.predecessor = scan.content_end;
    } else {
        plan.action = recovery_plan_action::retain;
    }
    return plan;
}
} // namespace detail

runtime::result<void>
recovery_planner::finish(const recovery_merge_result& merged) {
    if (finished_ || merged.resume || !merged.wal.complete)
        return runtime::failure(path_error(errc::invalid_argument));
    for (const auto& seen : reports_)
        if (!seen.report)
            return runtime::failure(path_error(errc::invalid_argument));
    auto wal = detail::plan_recovery_wal(merged.wal, wal_corrupt_);
    wal.unresolved = wal_.unresolved;
    wal.nonzero_slack = wal_.nonzero_slack;
    bool ready = wal.action != recovery_plan_action::stop;
    for (std::size_t i = 0; i < segments_.size(); ++i) {
        auto plan = detail::plan_recovery_segment(*reports_[i].report);
        if (!plan) return runtime::failure(plan.error());
        if (plan->action == recovery_plan_action::stop) ready = false;
        // Without a boundary nothing past the logical origin is certified.
        if (!plan->boundary) plan->recovered_end = segments_[i].recovered_end;
        segments_[i] = *plan;
    }
    wal_ = wal;
    ready_ = ready;
    finished_ = true;
    return {};
}

} // namespace kwaque::storage
