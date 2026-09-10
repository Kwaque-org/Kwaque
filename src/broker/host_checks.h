#pragma once

#include <seastar/core/abort_source.hh>
#include <seastar/core/file-types.hh>
#include <seastar/core/future.hh>
#include <seastar/util/log.hh>

#include <array>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>

namespace kwaque::broker {

enum class host_check_severity : std::uint8_t { info, warning, error };

struct host_check_result final {
    std::string_view name;
    host_check_severity severity{host_check_severity::info};
    std::string observed;
    std::string_view expected;
};

struct host_check_report final {
    std::array<host_check_result, 17> checks;
};

namespace detail {

// Missing observations remain unknown; they never imply a passing check.
struct host_snapshot final {
    std::optional<seastar::fs_type> filesystem;
    std::optional<std::uint64_t> free_disk_bytes;
    std::optional<std::uint64_t> physical_memory_bytes;
    std::optional<std::uint64_t> swap_bytes;
    std::optional<std::uint64_t> cgroup_memory_bytes;
    std::optional<std::uint64_t> cgroup_cpu_count;
    std::optional<std::uint64_t> cgroup_cpu_quota_millicores;
    std::optional<std::uint64_t> nofile_soft;
    std::optional<std::uint64_t> nofile_hard;
    std::optional<std::uint64_t> swappiness;
    std::optional<std::uint64_t> aio_max_nr;
    std::optional<std::uint64_t> data_device;
    std::string clocksource;
    std::string hugepages;
    std::string cgroup_version;
    bool io_calibration_configured{false};
    bool io_device_match{false};
    bool io_rates_finite{false};
};

[[nodiscard]] std::optional<std::uint64_t>
parse_cpu_count(std::string_view text) noexcept;
[[nodiscard]] std::optional<std::uint64_t>
parse_cpu_quota(std::string_view text) noexcept;
[[nodiscard]] host_check_report
evaluate_host_checks(const host_snapshot& snapshot);

// Reads one bounded regular kernel pseudo-file. The root exists only to permit
// controlled startup fixtures; broker startup always uses the native root.
[[nodiscard]] std::optional<std::string> read_host_file(
  const std::filesystem::path& root, std::string_view absolute_path);

} // namespace detail

// The abort source is borrowed on the caller's shard until completion. All
// checks are read-only; warnings and unsupported filesystem reports do not
// modify host settings or reject an otherwise usable directory.
[[nodiscard]] seastar::future<host_check_report> inspect_host(
  const std::filesystem::path& data_directory,
  bool io_calibration_configured,
  const seastar::abort_source& startup_abort,
  std::filesystem::path kernel_root = "/");

void log_host_checks(const host_check_report& report, seastar::logger& logger);

} // namespace kwaque::broker
