#include "src/runtime/production/stall_judge.h"

#include <csignal>
#include <system_error>
#include <utility>

namespace kwaque::runtime::production {

stall_judge::stall_judge(
  const stall_stamps& stamps,
  stall_judge_config config,
  ended_fn ended,
  void* context) noexcept
  : stamps_(stamps)
  , config_(config)
  , ended_(ended)
  , context_(context) {}

stall_judge::~stall_judge() { stop(); }

result<void> stall_judge::start() {
    if (
      ended_ == nullptr || config_.threshold.count() <= 0
      || config_.period.count() <= 0)
        return failure(
          operation_error{errc::invalid_argument, operation_kind::file});
    if (thread_.joinable())
        return failure(operation_error{errc::closed, operation_kind::file});
    {
        const std::scoped_lock lock{mutex_};
        stopping_ = false;
    }
    // The new thread inherits this mask, so no signal is delivered to it.
    sigset_t all;
    sigset_t before;
    sigfillset(&all);
    if (pthread_sigmask(SIG_BLOCK, &all, &before) != 0)
        return failure(
          operation_error{errc::unavailable, operation_kind::file});
    result<void> started{};
    try {
        thread_ = std::thread{[this]() noexcept { run(); }};
    } catch (const std::system_error&) {
        started = failure(
          operation_error{errc::resource_exhausted, operation_kind::file});
    }
    pthread_sigmask(SIG_SETMASK, &before, nullptr);
    return started;
}

void stall_judge::stop() noexcept {
    {
        const std::scoped_lock lock{mutex_};
        stopping_ = true;
    }
    wake_.notify_all();
    if (thread_.joinable() && thread_.get_id() != std::this_thread::get_id())
        thread_.join();
}

std::optional<stalled_operation> stall_judge::ended() const {
    const std::scoped_lock lock{mutex_};
    return found_;
}

void stall_judge::run() noexcept {
    const auto threshold = static_cast<std::uint64_t>(
      config_.threshold.count());
    std::unique_lock lock{mutex_};
    while (!stopping_) {
        wake_.wait_for(lock, config_.period, [this] { return stopping_; });
        if (stopping_) return;
        const auto now = std::chrono::duration_cast<std::chrono::nanoseconds>(
                           std::chrono::steady_clock::now().time_since_epoch())
                           .count();
        const auto found = stamps_.oldest_past(
          static_cast<std::uint64_t>(now), threshold);
        if (!found) continue;
        found_ = found;
        lock.unlock();
        ended_(context_, *found);
        return;
    }
}

} // namespace kwaque::runtime::production
