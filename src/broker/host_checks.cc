#include "src/broker/host_checks.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/io_queue.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/sysinfo.h>

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <fcntl.h>
#include <limits>
#include <system_error>
#include <unistd.h>

namespace kwaque::broker {
namespace {

constexpr std::size_t host_file_limit = 16U * 1024U;
constexpr std::size_t cgroup_ancestor_limit = 32;
constexpr std::uint64_t mib = 1024U * 1024U;

std::string_view trim(std::string_view text) noexcept {
    const auto start = text.find_first_not_of(" \t\r\n");
    if (start == std::string_view::npos) {
        return {};
    }
    return text.substr(start, text.find_last_not_of(" \t\r\n") - start + 1U);
}

std::optional<std::uint64_t> number(std::string_view text) noexcept {
    text = trim(text);
    if (text.empty()) {
        return std::nullopt;
    }
    std::uint64_t result = 0;
    const auto parsed = std::from_chars(
      text.data(), text.data() + text.size(), result);
    if (parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size()) {
        return std::nullopt;
    }
    return result;
}

std::string observed(std::optional<std::uint64_t> value) {
    return value ? std::to_string(*value) : "unobserved";
}

std::string bounded_text(std::string_view value) {
    value = trim(value);
    if (value.empty() || value.size() > 128U) {
        return "unobserved";
    }
    std::string result(value);
    for (char& byte : result) {
        if (static_cast<unsigned char>(byte) < 32U || byte == 127) {
            byte = '?';
        }
    }
    return result;
}

std::string_view filesystem_name(seastar::fs_type type) noexcept {
    switch (type) {
    case seastar::fs_type::xfs:
        return "xfs";
    case seastar::fs_type::ext2:
        return "ext-family-assumed-ext4";
    case seastar::fs_type::ext3:
        return "ext3";
    case seastar::fs_type::ext4:
        return "ext4";
    case seastar::fs_type::btrfs:
        return "btrfs";
    case seastar::fs_type::hfs:
        return "hfs";
    case seastar::fs_type::tmpfs:
        return "tmpfs";
    case seastar::fs_type::hugetlbfs:
        return "hugetlbfs";
    case seastar::fs_type::other:
        return "other";
    }
    return "unobserved";
}

std::optional<std::string>
cgroup_membership(std::string_view contents, std::string_view controller) {
    while (!contents.empty()) {
        const auto end = contents.find('\n');
        const auto line = contents.substr(0, end);
        const auto first = line.find(':');
        const auto second = first == std::string_view::npos
                              ? first
                              : line.find(':', first + 1U);
        if (second != std::string_view::npos) {
            const auto controllers = line.substr(
              first + 1U, second - first - 1U);
            bool matched = controllers.empty() && controller.empty()
                           && line.starts_with("0::");
            auto remaining = controllers;
            while (!remaining.empty()) {
                const auto comma = remaining.find(',');
                matched = matched || remaining.substr(0, comma) == controller;
                if (comma == std::string_view::npos) {
                    break;
                }
                remaining.remove_prefix(comma + 1U);
            }
            if (matched) {
                const auto path = line.substr(second + 1U);
                if (
                  path.empty() || path.front() != '/' || path.size() > 4096U
                  || path.find('\0') != std::string_view::npos) {
                    return std::nullopt;
                }
                const std::filesystem::path parsed(path);
                for (const auto& part : parsed) {
                    if (part == ".." || part == ".") {
                        return std::nullopt;
                    }
                }
                return std::string(path);
            }
        }
        if (end == std::string_view::npos) {
            break;
        }
        contents.remove_prefix(end + 1U);
    }
    return std::nullopt;
}

seastar::future<std::optional<std::string>> read_kernel_value(
  const std::filesystem::path& root,
  const std::filesystem::path& path,
  const seastar::abort_source& abort) {
    // These are bounded kernel pseudo-files, not persistent data. Yield between
    // reads; native DMA file streams cannot read procfs/sysfs in strict mode.
    co_await seastar::coroutine::maybe_yield();
    abort.check();
    auto result = detail::read_host_file(root, path.native());
    abort.check();
    co_return result;
}

seastar::future<std::optional<std::uint64_t>> cgroup_limit(
  const std::filesystem::path& root,
  std::filesystem::path current,
  const std::filesystem::path& boundary,
  std::string_view property,
  bool quota,
  const seastar::abort_source& abort,
  std::string_view period_property = {}) {
    std::optional<std::uint64_t> minimum;
    for (std::size_t depth = 0; depth < cgroup_ancestor_limit; ++depth) {
        auto value = co_await read_kernel_value(
          root, current / property, abort);
        if (!value) {
            // A stricter child or ancestor must not disappear from the
            // observation merely because its limit cannot be read.
            if (
              current != boundary || !minimum
              || (property != "memory.max" && property != "cpu.max")) {
                co_return std::nullopt;
            }
            // The actual v2 hierarchy root may omit these limit files. Only
            // genuine absence is implicit unlimited; existing unreadable or
            // oversized root files leave the effective limit unknown.
            const auto path = root / (current / property).relative_path();
            try {
                const bool exists = co_await seastar::file_exists(
                  path.native());
                abort.check();
                co_return exists ? std::nullopt : minimum;
            } catch (const std::system_error&) {
                abort.check();
                co_return std::nullopt;
            }
        }
        if (!period_property.empty()) {
            auto period = co_await read_kernel_value(
              root, current / period_property, abort);
            if (!period) {
                co_return std::nullopt;
            }
            *value = (trim(*value) == "-1" ? std::string("max")
                                           : std::string(trim(*value)))
                     + " " + *period;
        }
        auto parsed
          = quota ? detail::parse_cpu_quota(*value)
            : trim(*value) == "max"
              ? std::optional{std::numeric_limits<std::uint64_t>::max()}
              : number(*value);
        if (!parsed) {
            co_return std::nullopt;
        }
        minimum = minimum ? std::min(*minimum, *parsed) : parsed;
        if (current == boundary) {
            co_return minimum;
        }
        current = current.parent_path();
    }
    // A truncated ancestor walk cannot prove an effective resource limit.
    co_return std::nullopt;
}

} // namespace

namespace detail {

std::optional<std::uint64_t> parse_cpu_count(std::string_view text) noexcept {
    text = trim(text);
    if (text.empty()) {
        return std::nullopt;
    }
    std::uint64_t total = 0;
    std::optional<std::uint64_t> previous;
    while (!text.empty()) {
        const auto comma = text.find(',');
        const auto range = text.substr(0, comma);
        const auto dash = range.find('-');
        const auto first = number(range.substr(0, dash));
        const auto last = dash == std::string_view::npos
                            ? first
                            : number(range.substr(dash + 1U));
        if (
          !first || !last || *last < *first || *last >= 1048576U
          || (previous && *first <= *previous)) {
            return std::nullopt;
        }
        total += *last - *first + 1U;
        previous = last;
        if (comma == std::string_view::npos) {
            return total;
        }
        text.remove_prefix(comma + 1U);
    }
    return std::nullopt;
}

std::optional<std::uint64_t> parse_cpu_quota(std::string_view text) noexcept {
    text = trim(text);
    const auto space = text.find_first_of(" \t");
    if (space == std::string_view::npos) {
        return std::nullopt;
    }
    const auto period = number(text.substr(space));
    if (!period || *period == 0) {
        return std::nullopt;
    }
    if (text.substr(0, space) == "max") {
        return std::numeric_limits<std::uint64_t>::max();
    }
    const auto quota = number(text.substr(0, space));
    if (
      !quota || *quota == 0
      || *quota > std::numeric_limits<std::uint64_t>::max() / 1000U) {
        return std::nullopt;
    }
    return *quota * 1000U / *period;
}

std::optional<std::string> read_host_file(
  const std::filesystem::path& root, std::string_view absolute_path) {
    const auto path = root
                      / std::filesystem::path(absolute_path).relative_path();
    const int descriptor = ::open(
      path.c_str(), O_RDONLY | O_CLOEXEC | O_NONBLOCK);
    if (descriptor < 0) {
        return std::nullopt;
    }
    struct descriptor_owner final {
        int value;
        ~descriptor_owner() { ::close(value); }
    } owner{descriptor};
    struct stat status{};
    if (::fstat(descriptor, &status) != 0 || !S_ISREG(status.st_mode)) {
        return std::nullopt;
    }
    std::array<char, host_file_limit + 1U> bytes{};
    std::size_t used = 0;
    for (unsigned operation = 0; operation < 16U; ++operation) {
        const auto count = ::read(
          descriptor, bytes.data() + used, bytes.size() - used);
        if (count == 0) {
            return std::string(bytes.data(), used);
        }
        if (count < 0) {
            if (errno == EINTR) {
                continue;
            }
            return std::nullopt;
        }
        used += static_cast<std::size_t>(count);
        if (used > host_file_limit) {
            return std::nullopt;
        }
    }
    return std::nullopt;
}

host_check_report evaluate_host_checks(const host_snapshot& state) {
    using severity = host_check_severity;
    const auto checked = [](bool ok) {
        return ok ? severity::info : severity::warning;
    };
    const auto fs_severity = !state.filesystem ? severity::warning
                             : *state.filesystem == seastar::fs_type::xfs
                               ? severity::info
                             : *state.filesystem == seastar::fs_type::ext2
                                 || *state.filesystem == seastar::fs_type::ext4
                               ? severity::warning
                               : severity::error;
    const auto host_memory
      = state.physical_memory_bytes && state.cgroup_memory_bytes
          ? std::optional{std::min(
              *state.physical_memory_bytes, *state.cgroup_memory_bytes)}
          : std::nullopt;
    const auto memory_per_cpu
      = host_memory && state.cgroup_cpu_count && *state.cgroup_cpu_count != 0
          ? std::optional{*host_memory / mib / *state.cgroup_cpu_count}
          : std::nullopt;
#if defined(__aarch64__)
    constexpr std::string_view expected_clock = "arch_sys_counter";
#else
    constexpr std::string_view expected_clock = "tsc";
#endif
    const auto clock = bounded_text(state.clocksource);
    const bool hugepages_active = state.hugepages.find("[always]")
                                    != std::string::npos
                                  || state.hugepages.find("[madvise]")
                                       != std::string::npos;
    const bool calibrated = state.io_calibration_configured
                            && state.io_device_match && state.io_rates_finite;
    return {{{
      {"filesystem",
       fs_severity,
       state.filesystem ? std::string(filesystem_name(*state.filesystem))
                        : "unobserved",
       "xfs preferred; ext4 warning; other unsupported"},
      {"disk_free_bytes",
       checked(
         state.free_disk_bytes
         && *state.free_disk_bytes >= 10ULL * 1024U * 1024U * 1024U),
       observed(state.free_disk_bytes),
       ">=10 GiB warning only"},
      {"host_physical_memory_bytes",
       checked(state.physical_memory_bytes.has_value()),
       observed(state.physical_memory_bytes),
       "kernel observation; separate from native shard allocator capacity"},
      {"cgroup_memory_limit_bytes",
       checked(state.cgroup_memory_bytes.has_value()),
       state.cgroup_memory_bytes == std::numeric_limits<std::uint64_t>::max()
         ? "unlimited"
         : observed(state.cgroup_memory_bytes),
       "minimum observed ancestor limit"},
      {"host_memory_mib_per_cgroup_cpu",
       checked(memory_per_cpu && *memory_per_cpu >= 2048U),
       observed(memory_per_cpu),
       ">=2048 recommended; separate from shard startup floor"},
      {"cgroup_effective_cpuset_cpus",
       checked(state.cgroup_cpu_count.has_value()),
       observed(state.cgroup_cpu_count),
       "effective cpuset; CPU quota reported separately"},
      {"cgroup_cpu_quota_millicores",
       checked(state.cgroup_cpu_quota_millicores.has_value()),
       state.cgroup_cpu_quota_millicores
           == std::numeric_limits<std::uint64_t>::max()
         ? "unlimited"
         : observed(state.cgroup_cpu_quota_millicores),
       "observed hierarchy limit; no tuning"},
      {"cgroup_version",
       checked(!state.cgroup_version.empty()),
       bounded_text(state.cgroup_version),
       "bounded current membership and ancestor observations"},
      {"descriptor_limits",
       checked(state.nofile_soft && state.nofile_hard),
       "soft=" + observed(state.nofile_soft)
         + " hard=" + observed(state.nofile_hard),
       "native process limits; no automatic adjustment"},
      {"swap_bytes",
       checked(state.swap_bytes.has_value()),
       observed(state.swap_bytes),
       "informational; memory locking remains a native runtime selection"},
      {"swappiness",
       checked(state.swappiness == 1U),
       observed(state.swappiness),
       "1 recommended; no tuning"},
      {"aio_max_nr",
       checked(state.aio_max_nr && *state.aio_max_nr >= 10'000'137U),
       observed(state.aio_max_nr),
       ">=10000137 recommended; no tuning"},
      {"clocksource", checked(clock == expected_clock), clock, expected_clock},
      {"transparent_hugepages",
       checked(hugepages_active),
       bounded_text(state.hugepages),
       "active recommended; no tuning"},
      {"io_calibration_configured",
       checked(state.io_calibration_configured),
       state.io_calibration_configured ? "true" : "false",
       "explicit runtime I/O properties"},
      {"io_calibration_device",
       checked(calibrated),
       std::string("device_match=") + (state.io_device_match ? "true" : "false")
         + " rates_finite=" + (state.io_rates_finite ? "true" : "false"),
       "matching finite configured rates; throughput remains unqualified"},
      {"data_mount_device",
       checked(state.data_device.has_value()),
       observed(state.data_device),
       "observed device identity; mount intent requires configured marker"},
    }}};
}

} // namespace detail

seastar::future<host_check_report> inspect_host(
  const std::filesystem::path& data_directory,
  bool io_calibration_configured,
  const seastar::abort_source& startup_abort,
  std::filesystem::path kernel_root) {
    startup_abort.check();
    detail::host_snapshot snapshot;
    snapshot.io_calibration_configured = io_calibration_configured;
    try {
        snapshot.filesystem = co_await seastar::file_system_at(
          data_directory.native());
        startup_abort.check();
        snapshot.free_disk_bytes = co_await seastar::fs_free(
          data_directory.native());
        startup_abort.check();
        const auto stat = co_await seastar::file_stat(data_directory.native());
        snapshot.data_device = stat.device_id;
        const auto* queue = seastar::engine().try_get_io_queue(
          static_cast<dev_t>(stat.device_id));
        snapshot.io_device_match = queue != nullptr
                                   && queue->get_config().id != 0U;
        if (snapshot.io_device_match) {
            const auto& config = queue->get_config();
            const auto finite = [](std::size_t value) {
                return value != 0U
                       && value != std::numeric_limits<std::size_t>::max();
            };
            snapshot.io_rates_finite = finite(config.read_bytes_rate)
                                       && finite(config.write_bytes_rate)
                                       && finite(config.read_req_rate)
                                       && finite(config.write_req_rate);
        }
    } catch (const std::system_error&) {
        // Unavailable host observations cannot certify storage suitability.
    }
    startup_abort.check();
    struct sysinfo memory{};
    if (::sysinfo(&memory) == 0) {
        const auto unit = static_cast<std::uint64_t>(memory.mem_unit);
        const auto total = static_cast<std::uint64_t>(memory.totalram);
        const auto swap = static_cast<std::uint64_t>(memory.totalswap);
        if (
          unit != 0 && total <= std::numeric_limits<std::uint64_t>::max() / unit
          && swap <= std::numeric_limits<std::uint64_t>::max() / unit) {
            snapshot.physical_memory_bytes = total * unit;
            snapshot.swap_bytes = swap * unit;
        }
    }
    struct rlimit descriptors{};
    if (::getrlimit(RLIMIT_NOFILE, &descriptors) == 0) {
        snapshot.nofile_soft = descriptors.rlim_cur;
        snapshot.nofile_hard = descriptors.rlim_max;
    }
    const auto membership = co_await read_kernel_value(
      kernel_root, "/proc/self/cgroup", startup_abort);
    if (membership) {
        // Controllers can belong to different hierarchies on a hybrid host.
        // A v1 membership selects that controller even when v2 is also mounted.
        const auto v1_cpuset = cgroup_membership(*membership, "cpuset");
        const auto v1_memory = cgroup_membership(*membership, "memory");
        const auto v1_cpu = cgroup_membership(*membership, "cpu");
        if (auto path = cgroup_membership(*membership, "")) {
            snapshot.cgroup_version = "v2";
            const std::filesystem::path boundary = "/sys/fs/cgroup";
            const auto current = boundary
                                 / std::filesystem::path(*path).relative_path();
            if (!v1_cpuset) {
                auto cpus = co_await read_kernel_value(
                  kernel_root,
                  current / "cpuset.cpus.effective",
                  startup_abort);
                if (cpus) {
                    snapshot.cgroup_cpu_count = detail::parse_cpu_count(*cpus);
                }
            }
            if (!v1_memory) {
                snapshot.cgroup_memory_bytes = co_await cgroup_limit(
                  kernel_root,
                  current,
                  boundary,
                  "memory.max",
                  false,
                  startup_abort);
            }
            if (!v1_cpu) {
                snapshot.cgroup_cpu_quota_millicores = co_await cgroup_limit(
                  kernel_root,
                  current,
                  boundary,
                  "cpu.max",
                  true,
                  startup_abort);
            }
        }
        if (v1_cpuset) {
            snapshot.cgroup_version = "v1-or-hybrid";
            const auto current
              = std::filesystem::path("/sys/fs/cgroup/cpuset")
                / std::filesystem::path(*v1_cpuset).relative_path();
            auto cpus = co_await read_kernel_value(
              kernel_root, current / "cpuset.effective_cpus", startup_abort);
            if (cpus) {
                snapshot.cgroup_cpu_count = detail::parse_cpu_count(*cpus);
            }
        }
        if (v1_memory) {
            snapshot.cgroup_version = "v1-or-hybrid";
            const std::filesystem::path boundary = "/sys/fs/cgroup/memory";
            const auto current
              = boundary / std::filesystem::path(*v1_memory).relative_path();
            snapshot.cgroup_memory_bytes = co_await cgroup_limit(
              kernel_root,
              current,
              boundary,
              "memory.limit_in_bytes",
              false,
              startup_abort);
        }
        if (v1_cpu) {
            snapshot.cgroup_version = "v1-or-hybrid";
            const std::filesystem::path boundary = "/sys/fs/cgroup/cpu";
            const auto current
              = boundary / std::filesystem::path(*v1_cpu).relative_path();
            snapshot.cgroup_cpu_quota_millicores = co_await cgroup_limit(
              kernel_root,
              current,
              boundary,
              "cpu.cfs_quota_us",
              true,
              startup_abort,
              "cpu.cfs_period_us");
        }
    }
    const auto swappiness = co_await read_kernel_value(
      kernel_root, "/proc/sys/vm/swappiness", startup_abort);
    if (swappiness) {
        snapshot.swappiness = number(*swappiness);
    }
    const auto aio = co_await read_kernel_value(
      kernel_root, "/proc/sys/fs/aio-max-nr", startup_abort);
    if (aio) {
        snapshot.aio_max_nr = number(*aio);
    }
    const auto clock = co_await read_kernel_value(
      kernel_root,
      "/sys/devices/system/clocksource/clocksource0/current_clocksource",
      startup_abort);
    if (clock) {
        snapshot.clocksource = bounded_text(*clock);
    }
    auto hugepages = co_await read_kernel_value(
      kernel_root,
      "/sys/kernel/mm/transparent_hugepage/enabled",
      startup_abort);
    if (!hugepages) {
        hugepages = co_await read_kernel_value(
          kernel_root,
          "/sys/kernel/mm/redhat_transparent_hugepage/enabled",
          startup_abort);
    }
    if (hugepages) {
        snapshot.hugepages = bounded_text(*hugepages);
    }
    startup_abort.check();
    co_return detail::evaluate_host_checks(snapshot);
}

void log_host_checks(const host_check_report& report, seastar::logger& logger) {
    for (const auto& check : report.checks) {
        const auto level = check.severity == host_check_severity::error
                             ? seastar::log_level::error
                           : check.severity == host_check_severity::warning
                             ? seastar::log_level::warn
                             : seastar::log_level::info;
        logger.log(
          level,
          "host check {} observed={} expected={}",
          check.name,
          check.observed,
          check.expected);
    }
}

} // namespace kwaque::broker
