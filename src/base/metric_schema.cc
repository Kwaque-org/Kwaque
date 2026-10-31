#include "src/base/metric_schema.h"

#include "src/base/names.h"

#include <array>

namespace kwaque {

namespace {

constexpr std::array descriptors{
  metric_descriptor{
    metric_id::task_active,
    "runtime_task",
    "active",
    "Currently active tasks",
    metric_value_kind::gauge,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::task_accepted_total,
    "runtime_task",
    "accepted_total",
    "Accepted tasks",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::task_completed_total,
    "runtime_task",
    "completed_total",
    "Completed tasks",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::task_failed_total,
    "runtime_task",
    "failed_total",
    "Failed tasks",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::task_abort_requests_total,
    "runtime_task",
    "abort_requests_total",
    "Task abort requests",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::queue_items,
    "bounded_queue",
    "items",
    "Currently queued items",
    metric_value_kind::gauge,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::queue_bytes,
    "bounded_queue",
    "bytes",
    "Bytes retained by queued or active work",
    metric_value_kind::gauge,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::queue_producer_waiters,
    "bounded_queue",
    "producer_waiters",
    "Waiting queue producers",
    metric_value_kind::gauge,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::queue_consumer_waiters,
    "bounded_queue",
    "consumer_waiters",
    "Waiting queue consumers",
    metric_value_kind::gauge,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::queue_active_handlers,
    "bounded_queue",
    "active_handlers",
    "Active queue handlers",
    metric_value_kind::gauge,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::queue_push_accepted_total,
    "bounded_queue",
    "push_accepted_total",
    "Accepted queue pushes",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::queue_push_rejected_total,
    "bounded_queue",
    "push_rejected_total",
    "Rejected queue pushes",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::queue_handler_failures_reported_total,
    "bounded_queue",
    "handler_failures_reported_total",
    "Reported queue handler failures",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::queue_handler_failures_suppressed_total,
    "bounded_queue",
    "handler_failures_suppressed_total",
    "Suppressed queue handler failures",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::memory_configured_bytes,
    "resource_manager",
    "memory_configured_bytes",
    "Configured workload memory bytes",
    metric_value_kind::gauge,
    metric_label_domain::workload,
    true},
  metric_descriptor{
    metric_id::memory_used_bytes,
    "resource_manager",
    "memory_used_bytes",
    "Used workload memory bytes",
    metric_value_kind::gauge,
    metric_label_domain::workload,
    true},
  metric_descriptor{
    metric_id::memory_available_bytes,
    "resource_manager",
    "memory_available_bytes",
    "Available workload memory bytes",
    metric_value_kind::gauge,
    metric_label_domain::workload,
    true},
  metric_descriptor{
    metric_id::memory_waiters,
    "resource_manager",
    "memory_waiters",
    "Workload memory waiters",
    metric_value_kind::gauge,
    metric_label_domain::workload,
    true},
  metric_descriptor{
    metric_id::timer_active,
    "runtime_timer",
    "active",
    "Currently admitted timer waits",
    metric_value_kind::gauge,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::timer_accepted_total,
    "runtime_timer",
    "accepted_total",
    "Accepted timer waits",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::timer_completed_total,
    "runtime_timer",
    "completed_total",
    "Completed timer waits",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::timer_rejected_total,
    "runtime_timer",
    "rejected_total",
    "Rejected timer waits",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::file_active,
    "runtime_file",
    "active",
    "Currently admitted file operations",
    metric_value_kind::gauge,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::file_accepted_total,
    "runtime_file",
    "accepted_total",
    "Accepted file operations",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::file_completed_total,
    "runtime_file",
    "completed_total",
    "Completed file operations",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::file_rejected_total,
    "runtime_file",
    "rejected_total",
    "Rejected file operations",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::file_completed_bytes_total,
    "runtime_file",
    "completed_bytes_total",
    "Completed file bytes",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::network_active,
    "runtime_network",
    "active",
    "Currently admitted network operations",
    metric_value_kind::gauge,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::network_accepted_total,
    "runtime_network",
    "accepted_total",
    "Accepted network operations",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::network_completed_total,
    "runtime_network",
    "completed_total",
    "Completed network operations",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::network_rejected_total,
    "runtime_network",
    "rejected_total",
    "Rejected network operations",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::network_completed_bytes_total,
    "runtime_network",
    "completed_bytes_total",
    "Completed network bytes",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::dns_active,
    "runtime_dns",
    "active",
    "Currently admitted DNS operations",
    metric_value_kind::gauge,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::dns_accepted_total,
    "runtime_dns",
    "accepted_total",
    "Accepted DNS operations",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::dns_completed_total,
    "runtime_dns",
    "completed_total",
    "Completed DNS operations",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::dns_rejected_total,
    "runtime_dns",
    "rejected_total",
    "Rejected DNS operations",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::scheduler_pending_events,
    "simulation",
    "scheduler_pending_events",
    "Pending scheduler events",
    metric_value_kind::gauge,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::scheduler_executed_events_total,
    "simulation",
    "scheduler_executed_events_total",
    "Executed scheduler events",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::trace_entries,
    "simulation",
    "trace_entries",
    "Recorded scheduler trace entries",
    metric_value_kind::gauge,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::fault_rules,
    "simulation",
    "fault_rules",
    "Configured fault rules",
    metric_value_kind::gauge,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::fault_evaluations_total,
    "simulation",
    "fault_evaluations_total",
    "Fault evaluations",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::fault_decisions_applied_total,
    "simulation",
    "fault_decisions_applied_total",
    "Applied fault decisions",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::fake_file_active,
    "simulation",
    "fake_file_active",
    "Active fake file operations",
    metric_value_kind::gauge,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::fake_network_active,
    "simulation",
    "fake_network_active",
    "Active fake network operations",
    metric_value_kind::gauge,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::fake_dns_active,
    "simulation",
    "fake_dns_active",
    "Active fake DNS operations",
    metric_value_kind::gauge,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::storage_active,
    "storage",
    "active",
    "Active admitted storage operations",
    metric_value_kind::gauge,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::storage_accepted_total,
    "storage",
    "accepted_total",
    "Accepted storage operations",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::storage_completed_total,
    "storage",
    "completed_total",
    "Terminal storage operations, including failures",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::storage_rejected_total,
    "storage",
    "rejected_total",
    "Storage admission rejections",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::storage_succeeded_total,
    "storage",
    "succeeded_total",
    "Successful storage operation results, without an implied durability "
    "barrier",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::storage_failed_total,
    "storage",
    "failed_total",
    "Failed storage operation results",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::storage_uncertain_total,
    "storage",
    "uncertain_total",
    "Uncertain storage outcomes or publication transitions",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::storage_durable_total,
    "storage",
    "durable_total",
    "Explicitly established storage durability transitions",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::storage_reserved_tasks,
    "storage",
    "reserved_tasks",
    "Storage task reservations retained by work and outputs",
    metric_value_kind::gauge,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::storage_reserved_bytes,
    "storage",
    "reserved_bytes",
    "Storage workload bytes retained by work and outputs",
    metric_value_kind::gauge,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::storage_reserved_handles,
    "storage",
    "reserved_handles",
    "Storage handle reservations retained by work and outputs",
    metric_value_kind::gauge,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::broker_process_readiness,
    "broker",
    "process_readiness",
    "Whether the broker is ready for traffic",
    metric_value_kind::gauge,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::broker_draining,
    "broker",
    "draining",
    "Whether the broker is draining before shutdown",
    metric_value_kind::gauge,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::broker_shards,
    "broker",
    "shards",
    "Configured reactor shard count",
    metric_value_kind::gauge,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::broker_startup_duration_seconds,
    "broker",
    "startup_duration_seconds",
    "Time from application start to readiness",
    metric_value_kind::gauge,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::broker_start_time_seconds,
    "broker",
    "start_time_seconds",
    "Broker process start time in seconds since the Unix epoch",
    metric_value_kind::gauge,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::storage_wal_placed_portions_total,
    "storage",
    "wal_placed_portions_total",
    "Portions of append groups placed in the WAL",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::storage_segment_placed_portions_total,
    "storage",
    "segment_placed_portions_total",
    "Portions of append groups placed in their segment alone",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::storage_wal_placed_bytes_total,
    "storage",
    "wal_placed_bytes_total",
    "Segment bytes the WAL-placed portions took",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::storage_segment_placed_bytes_total,
    "storage",
    "segment_placed_bytes_total",
    "Segment bytes the segment-placed portions took",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::storage_held_portions_total,
    "storage",
    "held_portions_total",
    "WAL-placed portions held in their segment's run instead of written at "
    "once",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::storage_held_portion_bytes_total,
    "storage",
    "held_portion_bytes_total",
    "Segment bytes of the WAL-placed portions held in a run",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::storage_runs_filled_total,
    "storage",
    "runs_filled_total",
    "Held runs written once they reached their hold",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::storage_runs_wanted_total,
    "storage",
    "runs_wanted_total",
    "Held runs written before a group that wanted its barrier at once",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::storage_runs_limited_total,
    "storage",
    "runs_limited_total",
    "Held runs written before a group that would have taken them past one "
    "write",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::storage_runs_synced_total,
    "storage",
    "runs_synced_total",
    "Held runs written for a sync of their segment",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::storage_runs_pressed_total,
    "storage",
    "runs_pressed_total",
    "Held runs written because appends wanted their memory",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::storage_segment_barriers_total,
    "storage",
    "segment_barriers_total",
    "Segment barriers a segment-placed portion's result waited for",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::storage_segment_syncs_total,
    "storage",
    "segment_syncs_total",
    "Segment barriers run for a sync",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::storage_sync_rounds_total,
    "storage",
    "sync_rounds_total",
    "Rounds that asked a sync of the segments a WAL file behind the head held "
    "groups of",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::storage_round_syncs_total,
    "storage",
    "round_syncs_total",
    "Syncs the sync rounds asked",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::storage_held_bytes,
    "storage",
    "held_bytes",
    "Bytes held unwritten in the segments' open runs",
    metric_value_kind::gauge,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::storage_held_runs,
    "storage",
    "held_runs",
    "Runs held open in segments",
    metric_value_kind::gauge,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::storage_unsynced_bytes,
    "storage",
    "unsynced_bytes",
    "Bytes reported durable that only the WAL holds durable",
    metric_value_kind::gauge,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::storage_unsynced_wal_files,
    "storage",
    "unsynced_wal_files",
    "WAL files from the oldest one holding a group not yet durable in its "
    "segment through the head",
    metric_value_kind::gauge,
    metric_label_domain::none,
    true},
  metric_descriptor{
    metric_id::observability_events_suppressed_total,
    "observability",
    "events_suppressed_total",
    "Events of rate-bounded kinds dropped by their rate bound",
    metric_value_kind::counter,
    metric_label_domain::none,
    true},
};

[[nodiscard]] consteval bool valid_name(std::string_view name) {
    return is_lowercase_name(name, 64);
}

[[nodiscard]] consteval bool descriptors_are_valid() {
    std::size_t series = 0;
    for (std::size_t index = 0; index < descriptors.size(); ++index) {
        const auto& descriptor = descriptors[index];
        if (
          static_cast<std::size_t>(descriptor.id) != index + 1U
          || !valid_name(descriptor.group) || !valid_name(descriptor.name)
          || descriptor.help.empty() || descriptor.help.size() > 160
          || (descriptor.kind != metric_value_kind::gauge && descriptor.kind != metric_value_kind::counter)
          || metric_series_count(descriptor.labels) == 0
          || !descriptor.aggregate_shard) {
            return false;
        }
        series += metric_series_count(descriptor.labels);
        for (std::size_t other = index + 1U; other < descriptors.size();
             ++other) {
            if (
              descriptor.group == descriptors[other].group
              && descriptor.name == descriptors[other].name) {
                return false;
            }
        }
    }
    return descriptors.size() == metric_inventory_size
           && series == metric_series_per_shard;
}

static_assert(descriptors_are_valid());

[[nodiscard]] consteval bool label_values_are_valid() {
    // Each value appears once and passes `valid`.
    const auto valid_set = [](auto values, auto valid) {
        for (std::size_t index = 0; index < values.size(); ++index) {
            if (!valid(values[index])) {
                return false;
            }
            for (std::size_t other = index + 1U; other < values.size();
                 ++other) {
                if (values[index] == values[other]) {
                    return false;
                }
            }
        }
        return true;
    };
    const auto digit = [](std::string_view value) {
        return value.size() == 1 && value.front() >= '0'
               && value.front() <= '9';
    };
    const auto name = [](std::string_view value) {
        return is_lowercase_name(value, 64);
    };
    return valid_set(metric_device_label_values, digit)
           && valid_set(metric_chain_step_label_values, name)
           && valid_set(metric_refusal_label_values, name);
}

static_assert(label_values_are_valid());

[[nodiscard]] consteval bool is_simulation_group(std::string_view group) {
    for (const auto simulation : metric_simulation_groups) {
        if (group == simulation) {
            return true;
        }
    }
    return false;
}

// Every group a broker shard registers has a budget its descriptors fit, and
// the budgets fit the shard's share of the scrape caps after Seastar's own
// metrics and the build information series.
[[nodiscard]] consteval bool budgets_hold() {
    std::size_t families = metric_native_families + metric_io_queue_families
                           + metric_build_info_families;
    std::size_t series = metric_native_series + metric_build_info_series;
    for (std::size_t index = 0; index < metric_group_budgets.size(); ++index) {
        const auto& budget = metric_group_budgets[index];
        families += budget.families;
        series += budget.series;
        for (std::size_t other = index + 1U;
             other < metric_group_budgets.size();
             ++other) {
            if (budget.group == metric_group_budgets[other].group) {
                return false;
            }
        }
        std::size_t used_families = 0;
        std::size_t used_series = 0;
        for (const auto& descriptor : descriptors) {
            if (descriptor.group == budget.group) {
                ++used_families;
                used_series += metric_series_count(descriptor.labels);
            }
        }
        if (used_families > budget.families || used_series > budget.series) {
            return false;
        }
    }
    for (const auto& descriptor : descriptors) {
        if (is_simulation_group(descriptor.group)) {
            continue;
        }
        bool budgeted = false;
        for (const auto& budget : metric_group_budgets) {
            budgeted = budgeted || budget.group == descriptor.group;
        }
        if (!budgeted) {
            return false;
        }
    }
    return families <= metric_shard_families_cap
           && series <= metric_shard_series_cap;
}

static_assert(budgets_hold());

} // namespace

const metric_descriptor* descriptor_for(metric_id id) noexcept {
    const auto value = static_cast<std::size_t>(id);
    return value != 0 && value <= descriptors.size() ? &descriptors[value - 1U]
                                                     : nullptr;
}

std::span<const metric_descriptor> metric_descriptors() noexcept {
    return descriptors;
}

} // namespace kwaque
