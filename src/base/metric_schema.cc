#include "src/base/metric_schema.h"

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
};

[[nodiscard]] consteval bool valid_name(std::string_view name) {
    if (
      name.empty() || name.size() > 64 || name.front() < 'a'
      || name.front() > 'z') {
        return false;
    }
    for (const char value : name) {
        if (!((value >= 'a' && value <= 'z') || (value >= '0' && value <= '9')
              || value == '_')) {
            return false;
        }
    }
    return true;
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
          || (descriptor.labels != metric_label_domain::none && descriptor.labels != metric_label_domain::workload)
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
