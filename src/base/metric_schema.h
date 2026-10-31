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
    // A storage device by its index in the configured list, never its path.
    device = 2,
    // A step of the append admission chain.
    chain_step = 3,
    // A definite append refusal.
    refusal = 4,
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
    storage_active = 46,
    storage_accepted_total = 47,
    storage_completed_total = 48,
    storage_rejected_total = 49,
    storage_succeeded_total = 50,
    storage_failed_total = 51,
    storage_uncertain_total = 52,
    storage_durable_total = 53,
    storage_reserved_tasks = 54,
    storage_reserved_bytes = 55,
    storage_reserved_handles = 56,
    broker_process_readiness = 57,
    broker_draining = 58,
    broker_shards = 59,
    broker_startup_duration_seconds = 60,
    broker_start_time_seconds = 61,
    storage_wal_placed_portions_total = 62,
    storage_segment_placed_portions_total = 63,
    storage_wal_placed_bytes_total = 64,
    storage_segment_placed_bytes_total = 65,
    storage_held_portions_total = 66,
    storage_held_portion_bytes_total = 67,
    storage_runs_filled_total = 68,
    storage_runs_wanted_total = 69,
    storage_runs_limited_total = 70,
    storage_runs_synced_total = 71,
    storage_runs_pressed_total = 72,
    storage_segment_barriers_total = 73,
    storage_segment_syncs_total = 74,
    storage_sync_rounds_total = 75,
    storage_round_syncs_total = 76,
    storage_held_bytes = 77,
    storage_held_runs = 78,
    storage_unsynced_bytes = 79,
    storage_unsynced_wal_files = 80,
    observability_events_suppressed_total = 81,
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

inline constexpr std::size_t metric_inventory_size{81};
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
inline constexpr std::string_view metric_device_label{"device"};
inline constexpr std::array<std::string_view, 8> metric_device_label_values{
  "0", "1", "2", "3", "4", "5", "6", "7"};
inline constexpr std::string_view metric_chain_step_label{"chain_step"};
// In chain order; the event texts for these steps use the same words.
inline constexpr std::array<std::string_view, 7> metric_chain_step_label_values{
  "envelope_size",
  "ingress_memory",
  "stream_window",
  "range_queue",
  "shard_capacity",
  "forwarded_traffic",
  "storage_pressure",
};
inline constexpr std::string_view metric_refusal_label{"refusal"};
// The definite append refusals in code order, 203 to 223.
inline constexpr std::array<std::string_view, 21> metric_refusal_label_values{
  "batch_id_conflict",
  "stale",
  "sequence_gap",
  "window_exceeded",
  "unknown_stream",
  "stream_sealed",
  "stream_failed",
  "producer_fenced",
  "invalid_ack_floor",
  "stream_limit",
  "topic_sealed",
  "not_found",
  "misrouted_key",
  "stale_route",
  "no_space",
  "too_large",
  "invalid_batch",
  "timed_out_before_acceptance",
  "aborted_before_acceptance",
  "closed",
  "unknown_producer",
};
inline constexpr std::size_t metric_series_per_shard{109};

// What one broker shard may register, as a share of the admin scrape caps:
// three quarters of them, so a scrape never nears a cap. Seastar's own
// metrics and the build information series come first; each Kwaque group has
// a budget its descriptors must fit, and the budgets together fit what is
// left. A component with no metrics yet holds an allocation it may later
// rebalance, but never exceed without moving another's.
inline constexpr std::size_t metric_shard_families_cap{192};
inline constexpr std::size_t metric_shard_series_cap{768};
// Measured on 2026-10-30: a shard running the runtime environment and the
// admin server registered 103 families and 216 enabled series, of which
// Kwaque's were 34 and 62. The broker composition test checks this reserve.
inline constexpr std::size_t metric_native_families{69};
inline constexpr std::size_t metric_native_series{154};
// Seastar registers its I/O queue's per-class metrics when a class first
// issues I/O on a device, which that measurement did not: 17 families, and
// for each class on each device 11 series and 6 more a stream. The families
// are reserved here; the series, which grow with the classes and devices in
// use, are not.
inline constexpr std::size_t metric_io_queue_families{17};
inline constexpr std::size_t metric_build_info_families{1};
inline constexpr std::size_t metric_build_info_series{1};

struct metric_group_budget final {
    std::string_view group;
    std::size_t families;
    std::size_t series;
};

inline constexpr std::array metric_group_budgets{
  metric_group_budget{"runtime_task", 5, 5},
  metric_group_budget{"bounded_queue", 9, 9},
  metric_group_budget{"resource_manager", 4, 32},
  metric_group_budget{"runtime_timer", 4, 4},
  metric_group_budget{"runtime_file", 5, 5},
  metric_group_budget{"runtime_network", 5, 5},
  metric_group_budget{"runtime_dns", 4, 4},
  metric_group_budget{"broker", 5, 5},
  metric_group_budget{"observability", 1, 1},
  // Allocations for the components that will register more: storage by
  // device, the metadata journal and its groups, and the range engine's
  // admission, streams, rolls, retention and reads. The I/O queue's
  // families came out of these, in proportion to what each had left.
  metric_group_budget{"storage", 37, 320},
  metric_group_budget{"metadata", 11, 64},
  metric_group_budget{"range", 15, 159},
};

// Groups only a simulation registers, outside a broker shard's budget.
inline constexpr std::array<std::string_view, 1> metric_simulation_groups{
  "simulation"};

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
    case metric_label_domain::device:
        return metric_device_label_values.size();
    case metric_label_domain::chain_step:
        return metric_chain_step_label_values.size();
    case metric_label_domain::refusal:
        return metric_refusal_label_values.size();
    }
    return 0;
}

} // namespace kwaque

#endif // KWAQUE_SRC_BASE_METRIC_SCHEMA_H_
