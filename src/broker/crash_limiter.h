#pragma once

#include "src/broker/startup_policy.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/semaphore.hh>

#include <array>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <stdexcept>

namespace kwaque::broker {

class crash_loop_limit_reached final : public std::runtime_error {
public:
    crash_loop_limit_reached();
};

namespace detail {

struct crash_loop_metadata final {
    std::uint64_t crash_count{0};
    std::array<char, 64> configuration_checksum{};
    std::int64_t last_start_milliseconds{0};
};

// An empty result refuses startup. A changed configuration or an elapsed period
// strictly greater than one hour resets the next count to one. Without a finite
// limit, the unsuccessful-start count saturates rather than wrapping.
[[nodiscard]] std::optional<std::uint64_t> next_crash_count(
  const std::optional<crash_loop_metadata>& previous,
  const configuration_identity& configuration,
  std::int64_t now_milliseconds,
  std::optional<std::uint32_t> limit) noexcept;

} // namespace detail

class crash_limiter final {
public:
    using clock_function = std::int64_t (*)() noexcept;

    explicit crash_limiter(clock_function clock = system_time_milliseconds);
    crash_limiter(const crash_limiter&) = delete;
    crash_limiter& operator=(const crash_limiter&) = delete;
    crash_limiter(crash_limiter&&) = delete;
    crash_limiter& operator=(crash_limiter&&) = delete;
    ~crash_limiter() = default;

    // The caller must hold the data directory's PID lock until crash
    // bookkeeping finishes. A limiter instance admits at most one startup
    // attempt. Developer mode bypasses startup metadata inspection and writes;
    // a qualified clean shutdown still removes an existing tracker. A null
    // limit disables finite rejection while retaining unsuccessful-start
    // counts.
    [[nodiscard]] seastar::future<> start(
      const std::filesystem::path& data_directory,
      const detail::configuration_identity& configuration,
      bool developer_mode,
      seastar::abort_source& abort,
      std::optional<std::uint32_t> limit = 5);

    // Call only after completed startup and successful mandatory cleanup.
    // Concurrent calls serialize. Failure leaves the remaining bookkeeping
    // eligible for retry, including a directory sync after completed unlink.
    [[nodiscard]] seastar::future<> record_clean_shutdown();

    [[nodiscard]] static std::int64_t system_time_milliseconds() noexcept;

private:
    friend class crash_limiter_test_access;
    [[nodiscard]] static seastar::future<>
    sync_directory(const std::filesystem::path& directory);

    clock_function clock_;
    seastar::future<> (*sync_directory_)(const std::filesystem::path&)
      = sync_directory;
    seastar::semaphore cleanup_{1};
    std::filesystem::path directory_;
    std::filesystem::path path_;
    bool attempted_{false};
    bool recorded_{false};
    bool directory_sync_pending_{false};
};

} // namespace kwaque::broker
