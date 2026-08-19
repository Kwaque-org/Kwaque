#include "src/broker/service_lifecycle.h"

#include <seastar/core/coroutine.hh>

#include <exception>
#include <utility>

namespace kwaque::broker {

service_lifecycle::service_lifecycle(
    seastar::abort_source &abort_source) noexcept
    : abort_source_(abort_source) {}

seastar::future<> service_lifecycle::start_step(std::string name, action start,
                                                action stop_action) {
  std::exception_ptr startup_failure;
  try {
    abort_source_.check();
    trace_.push_back("start:" + name);
    co_await start();
    started_.push_back(
        started_step{.name = std::move(name), .stop = std::move(stop_action)});
    abort_source_.check();
  } catch (...) {
    startup_failure = std::current_exception();
  }
  if (startup_failure) {
    try {
      co_await stop();
    } catch (...) {
    }
    std::rethrow_exception(startup_failure);
  }
}

seastar::future<> service_lifecycle::stop() {
  if (!shutdown_gate_.is_closed()) {
    co_await shutdown_gate_.close();
  }

  std::exception_ptr first_failure;
  while (!started_.empty()) {
    started_step step = std::move(started_.back());
    started_.pop_back();
    trace_.push_back("stop:" + step.name);
    try {
      co_await step.stop();
    } catch (...) {
      if (!first_failure) {
        first_failure = std::current_exception();
      }
    }
  }
  if (first_failure) {
    std::rethrow_exception(first_failure);
  }
}

seastar::abort_source &service_lifecycle::abort_source() noexcept {
  return abort_source_;
}

seastar::gate &service_lifecycle::gate() noexcept { return shutdown_gate_; }

const std::vector<std::string> &service_lifecycle::trace() const noexcept {
  return trace_;
}

std::size_t service_lifecycle::running_steps() const noexcept {
  return started_.size();
}

} // namespace kwaque::broker
