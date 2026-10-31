#pragma once

#include "src/base/units.h"

#include <array>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace kwaque::config {

// Until the first release a change to the key set or to the meaning of a key
// edits version 1 in place, and nothing reads an older file. After it, a
// change increments the schema version, and a binary accepts the inclusive
// range below so it can run with the previous configuration until that is
// upgraded. The version is read before any other key, so a binary refuses a
// file of a version it does not accept by its version, and an edited
// version-1 file by the first key it does not know.
inline constexpr std::uint32_t bootstrap_config_schema_version = 1;
inline constexpr std::uint32_t minimum_bootstrap_config_schema_version = 1;
inline constexpr std::uint32_t maximum_bootstrap_config_schema_version = 1;
inline constexpr std::size_t max_bootstrap_config_bytes = 64_KiB;
// The schema nests five collections deep (the root, kwaque, storage, its device
// list and a device); anything deeper is rejected before the parser recurses
// further, so parsing fits a small reactor thread stack.
inline constexpr std::size_t max_bootstrap_config_depth = 8;
inline constexpr std::size_t max_rendered_config_value_bytes = 256;

inline constexpr std::array<std::string_view, 11> bootstrap_config_keys{
  "schema_version",
  "data_directory",
  "admin",
  "developer_mode",
  "storage_strict_data_init",
  "crash_loop_limit",
  "diagnostic_memory_per_shard_bytes",
  "storage",
  "metadata",
  "topic_defaults",
  "range"};
inline constexpr std::array<std::string_view, 2> bootstrap_admin_keys{
  "address", "port"};
inline constexpr std::array<std::string_view, 2> bootstrap_storage_keys{
  "devices", "metadata_alignment_bytes"};
inline constexpr std::array<std::string_view, 3> bootstrap_device_keys{
  "path", "role", "mount_marker"};
inline constexpr std::size_t max_bootstrap_devices = 8;
inline constexpr std::array<std::string_view, 9> bootstrap_metadata_keys{
  "ring_size",
  "journal_file_bytes",
  "journal_retained_files",
  "snapshot_floor_bytes",
  "pending_proposals",
  "pending_proposal_bytes",
  "cohort_records",
  "cohort_bytes",
  "producer_expiration_ms"};
inline constexpr std::array<std::string_view, 9> bootstrap_topic_default_keys{
  "maximum_record_bytes",
  "maximum_batch_bytes",
  "segment_bytes",
  "segment_lifetime_ms",
  "retention_ms",
  "retention_bytes",
  "retry_window_ms",
  "retry_depth",
  "active_leaf_cap"};
inline constexpr std::array<std::string_view, 16> bootstrap_range_keys{
  "stop_deadline_ms",
  "unsealed_bytes",
  "retention_sweep_interval_ms",
  "maximum_expiring_per_range",
  "queue_requests",
  "queue_bytes",
  "roll_rate_per_range_per_minute",
  "roll_burst_per_range",
  "roll_rate_per_shard_per_second",
  "roll_burst_per_shard",
  "placement_threshold_percent",
  "placement_byte_cost_ps",
  "placement_batch_cost_ns",
  "placement_minimum_load",
  "maximum_consume_streams",
  "wheel_touches_per_task"};

// The words a device's role is written as, as storage names them.
enum class device_role : std::uint8_t { data, wal_control, all };

struct device_config final {
    std::filesystem::path path;
    device_role role{device_role::all};
    // A file whose presence shows the device is mounted, if configured.
    std::optional<std::string> mount_marker;

    bool operator==(const device_config&) const = default;
};

struct storage_config final {
    std::vector<device_config> devices;
    std::uint64_t metadata_alignment_bytes{4096};

    bool operator==(const storage_config&) const = default;
};

struct metadata_config final {
    std::uint64_t ring_size{256};
    std::uint64_t journal_file_bytes{16_MiB};
    std::uint64_t journal_retained_files{64};
    std::uint64_t snapshot_floor_bytes{4_MiB};
    std::uint64_t pending_proposals{1024};
    std::uint64_t pending_proposal_bytes{4_MiB};
    std::uint64_t cohort_records{128};
    std::uint64_t cohort_bytes{1_MiB};
    std::uint64_t producer_expiration_ms{86'400'000};

    bool operator==(const metadata_config&) const = default;
};

// Topic defaults. Each count is positive; the topic policy table's bounds are
// not checked yet.
struct topic_defaults_config final {
    std::uint64_t maximum_record_bytes{1'048'576};
    std::uint64_t maximum_batch_bytes{1'052'672};
    std::uint64_t segment_bytes{1'000'000'000};
    std::uint64_t segment_lifetime_ms{3'600'000};
    // No value means unbounded.
    std::optional<std::uint64_t> retention_ms{604'800'000};
    // No value means derived from the segment size.
    std::optional<std::uint64_t> retention_bytes;
    std::uint64_t retry_window_ms{86'400'000};
    std::uint64_t retry_depth{8};
    std::uint64_t active_leaf_cap{4096};

    bool operator==(const topic_defaults_config&) const = default;
};

struct range_config final {
    std::uint64_t stop_deadline_ms{20'000};
    std::uint64_t unsealed_bytes{10_GiB};
    std::uint64_t retention_sweep_interval_ms{300'000};
    std::uint64_t maximum_expiring_per_range{64};
    std::uint64_t queue_requests{64};
    std::uint64_t queue_bytes{4_MiB};
    std::uint64_t roll_rate_per_range_per_minute{1};
    std::uint64_t roll_burst_per_range{2};
    std::uint64_t roll_rate_per_shard_per_second{8};
    std::uint64_t roll_burst_per_shard{16};
    std::uint64_t placement_threshold_percent{1};
    std::uint64_t placement_byte_cost_ps{500};
    std::uint64_t placement_batch_cost_ns{5'000};
    std::uint64_t placement_minimum_load{100};
    // No value means derived at start from the readers' budget.
    std::optional<std::uint64_t> maximum_consume_streams;
    std::uint64_t wheel_touches_per_task{256};

    bool operator==(const range_config&) const = default;
};

struct bootstrap_config final {
    std::uint32_t schema_version{bootstrap_config_schema_version};
    std::filesystem::path data_directory{"./data"};
    std::string admin_address{"127.0.0.1"};
    std::uint16_t admin_port{9644};
    bool developer_mode{false};
    bool storage_strict_data_init{false};
    std::optional<std::uint32_t> crash_loop_limit{5};
    std::optional<std::uint64_t> diagnostic_memory_per_shard_bytes;
    storage_config storage;
    metadata_config metadata;
    topic_defaults_config topic_defaults;
    range_config range;

    bool operator==(const bootstrap_config&) const = default;
};

enum class config_errc {
    file_unavailable,
    malformed_yaml,
    input_too_large,
    missing_key,
    unknown_key,
    duplicate_key,
    invalid_type,
    invalid_data_directory,
    invalid_admin_address,
    invalid_admin_port,
    unsupported_schema_version,
    invalid_memory_budget,
    invalid_crash_loop_limit,
    // Outside the key's bounds; the message names them.
    out_of_range,
    // The right type but a broken rule, such as a power of two or a rule
    // between two keys.
    invalid_value,
    // The device list's count, control role, a path or a role.
    invalid_device_set,
};

struct config_error final {
    config_errc code;
    std::string field;
    std::string message;
};

using bootstrap_config_result = std::expected<bootstrap_config, config_error>;

enum class config_visibility { redacted, safe };

struct config_value final {
    std::string_view name;
    std::string_view value;
    config_visibility visibility{config_visibility::redacted};
};

// Parses one YAML 1.2 document of UTF-8 text. Integers are plain decimal
// without leading zeros, booleans are true or false in any of the three core
// spellings, and typed values must be unquoted and untagged.
[[nodiscard]] bootstrap_config_result
parse_bootstrap_config(std::string_view yaml);

[[nodiscard]] bootstrap_config_result
load_bootstrap_config(const std::filesystem::path& path);

[[nodiscard]] std::string render_config(std::span<const config_value> values);

[[nodiscard]] std::string render_config(const bootstrap_config& configuration);

[[nodiscard]] std::string render_config_error(const config_error& error);

} // namespace kwaque::config
