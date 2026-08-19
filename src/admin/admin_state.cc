#include "src/admin/admin_state.h"

namespace kwaque::admin {

void admin_state::listener_started(unsigned shard_count) noexcept {
  shard_count_.store(shard_count, std::memory_order_relaxed);
  lifecycle_.store(lifecycle::live, std::memory_order_release);
}

void admin_state::mark_ready(double startup_duration_seconds) noexcept {
  startup_duration_seconds_.store(startup_duration_seconds,
                                  std::memory_order_relaxed);
  lifecycle expected = lifecycle::live;
  static_cast<void>(lifecycle_.compare_exchange_strong(
      expected, lifecycle::ready, std::memory_order_release,
      std::memory_order_relaxed));
}

void admin_state::begin_shutdown() noexcept {
  lifecycle current = lifecycle_.load(std::memory_order_acquire);
  while (current == lifecycle::live || current == lifecycle::ready) {
    if (lifecycle_.compare_exchange_weak(
            current, lifecycle::draining, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      shutdown_count_.fetch_add(1, std::memory_order_relaxed);
      return;
    }
  }
}

void admin_state::stopped() noexcept {
  lifecycle_.store(lifecycle::stopped, std::memory_order_release);
}

void admin_state::record_request() noexcept {
  request_count_.fetch_add(1, std::memory_order_relaxed);
}

bool admin_state::live() const noexcept {
  const lifecycle current = lifecycle_.load(std::memory_order_acquire);
  return current == lifecycle::live || current == lifecycle::ready;
}

bool admin_state::ready() const noexcept {
  return lifecycle_.load(std::memory_order_acquire) == lifecycle::ready;
}

unsigned admin_state::shard_count() const noexcept {
  return shard_count_.load(std::memory_order_relaxed);
}

double admin_state::startup_duration_seconds() const noexcept {
  return startup_duration_seconds_.load(std::memory_order_relaxed);
}

std::uint64_t admin_state::shutdown_count() const noexcept {
  return shutdown_count_.load(std::memory_order_relaxed);
}

std::uint64_t admin_state::request_count() const noexcept {
  return request_count_.load(std::memory_order_relaxed);
}

} // namespace kwaque::admin
