#pragma once

#include <atomic>
#include <cstdint>

namespace kwaque::admin {

class admin_state final {
public:
  void listener_started(unsigned shard_count) noexcept;
  void mark_ready(double startup_duration_seconds) noexcept;
  void begin_shutdown() noexcept;
  void stopped() noexcept;
  void record_request() noexcept;

  [[nodiscard]] bool live() const noexcept;
  [[nodiscard]] bool ready() const noexcept;
  [[nodiscard]] unsigned shard_count() const noexcept;
  [[nodiscard]] double startup_duration_seconds() const noexcept;
  [[nodiscard]] std::uint64_t shutdown_count() const noexcept;
  [[nodiscard]] std::uint64_t request_count() const noexcept;

private:
  enum class lifecycle : std::uint8_t { stopped, live, ready, draining };

  std::atomic<lifecycle> lifecycle_{lifecycle::stopped};
  std::atomic<unsigned> shard_count_{0};
  std::atomic<double> startup_duration_seconds_{0.0};
  std::atomic<std::uint64_t> shutdown_count_{0};
  std::atomic<std::uint64_t> request_count_{0};
};

} // namespace kwaque::admin
