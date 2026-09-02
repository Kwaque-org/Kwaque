#ifndef KWAQUE_SRC_BASE_METRIC_SCHEMA_H_
#define KWAQUE_SRC_BASE_METRIC_SCHEMA_H_

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>
#include <string_view>

namespace kwaque {

enum class metric_value_kind : std::uint8_t {
    gauge = 1,
    counter = 2,
};

enum class metric_label_domain : std::uint8_t {
    none = 0,
    workload = 1,
};

enum class metric_id : std::uint16_t {
    task_active = 1,
    task_accepted_total = 2,
    task_completed_total = 3,
    task_failed_total = 4,
    task_abort_requests_total = 5,
    queue_items = 6,
    queue_bytes = 7,
    queue_producer_waiters = 8,
    queue_consumer_waiters = 9,
    queue_active_handlers = 10,
    queue_push_accepted_total = 11,
    queue_push_rejected_total = 12,
    queue_handler_failures_reported_total = 13,
    queue_handler_failures_suppressed_total = 14,
    memory_configured_bytes = 15,
    memory_used_bytes = 16,
    memory_available_bytes = 17,
    memory_waiters = 18,
    timer_active = 19,
    timer_accepted_total = 20,
    timer_completed_total = 21,
    timer_rejected_total = 22,
    file_active = 23,
    file_accepted_total = 24,
    file_completed_total = 25,
    file_rejected_total = 26,
    file_completed_bytes_total = 27,
    network_active = 28,
    network_accepted_total = 29,
    network_completed_total = 30,
    network_rejected_total = 31,
    network_completed_bytes_total = 32,
    dns_active = 33,
    dns_accepted_total = 34,
    dns_completed_total = 35,
    dns_rejected_total = 36,
    scheduler_pending_events = 37,
    scheduler_executed_events_total = 38,
    trace_entries = 39,
    fault_rules = 40,
    fault_evaluations_total = 41,
    fault_decisions_applied_total = 42,
    fake_file_active = 43,
    fake_network_active = 44,
    fake_dns_active = 45,
};

struct metric_descriptor final {
    metric_id id;
    std::string_view group;
    std::string_view name;
    std::string_view help;
    metric_value_kind kind;
    metric_label_domain labels;
    bool aggregate_shard;

    bool operator==(const metric_descriptor&) const = default;
};

inline constexpr std::size_t metric_inventory_size{45};
inline constexpr std::string_view metric_workload_label{"workload"};
inline constexpr std::array<std::string_view, 8> metric_workload_label_values{
  "foreground_protocol",
  "consensus_critical",
  "replication",
  "metadata",
  "repair",
  "compaction",
  "offload",
  "maintenance",
};
inline constexpr std::size_t metric_workload_values
  = metric_workload_label_values.size();
inline constexpr std::size_t metric_series_per_shard{73};

// Totals are plain unsigned 64-bit owner fields. Their overflow behavior is
// the language-defined modulo-2^64 arithmetic, with no update-path branch.
static_assert(std::numeric_limits<std::uint64_t>::is_modulo);

[[nodiscard]] const metric_descriptor* descriptor_for(metric_id id) noexcept;
[[nodiscard]] std::span<const metric_descriptor> metric_descriptors() noexcept;
[[nodiscard]] constexpr std::size_t
metric_series_count(metric_label_domain labels) noexcept {
    switch (labels) {
    case metric_label_domain::none:
        return 1;
    case metric_label_domain::workload:
        return metric_workload_values;
    }
    return 0;
}

} // namespace kwaque

#endif // KWAQUE_SRC_BASE_METRIC_SCHEMA_H_
