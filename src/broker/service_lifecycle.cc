#include "src/broker/service_lifecycle.h"

#include "src/base/invariant.h"

#include <seastar/core/coroutine.hh>

#include <exception>
#include <stdexcept>
#include <utility>

namespace kwaque::broker {

service_lifecycle::service_lifecycle(
  seastar::abort_source& abort_source) noexcept
  : abort_source_(abort_source) {}

service_lifecycle::~service_lifecycle() {
    assert_current();
    KWAQUE_INVARIANT(
      invariant_id{"KQ-SERVICE-LIFECYCLE-STOPPED"},
      state_ == service_lifecycle_state::stopped
        || (state_ == service_lifecycle_state::open && started_.empty() && !operation_active_),
      "service lifecycle destroyed with active services");
}

seastar::future<>
service_lifecycle::start_step(action start, action stop_action) {
    assert_current();
    if (state_ != service_lifecycle_state::open || operation_active_) {
        throw std::logic_error("service lifecycle is not open for startup");
    }
    if (!start || !stop_action) {
        throw std::invalid_argument(
          "service lifecycle actions must be present");
    }
    operation_active_ = true;

    std::exception_ptr startup_failure;
    try {
        abort_source_.check();
        // Register cleanup before invoking start. No allocation can occur
        // between successful start completion and durable rollback ownership.
        started_.push_back(std::move(stop_action));
        co_await start();
        abort_source_.check();
    } catch (...) {
        startup_failure = std::current_exception();
    }
    operation_active_ = false;
    if (startup_failure) {
        try {
            co_await stop();
        } catch (...) {
        }
        std::rethrow_exception(startup_failure);
    }
}

seastar::future<> service_lifecycle::stop() {
    assert_current();
    if (state_ == service_lifecycle_state::stopping) {
        return stop_done_.get_shared_future();
    }
    if (state_ == service_lifecycle_state::stopped) {
        return stop_done_.get_shared_future();
    }
    if (operation_active_) {
        return seastar::make_exception_future<>(
          std::logic_error("service lifecycle operation is in progress"));
    }

    state_ = service_lifecycle_state::stopping;
    auto completion = stop_once().then_wrapped(
      [this](seastar::future<> stopped) noexcept {
          state_ = service_lifecycle_state::stopped;
          try {
              stopped.get();
              stop_done_.set_value();
          } catch (...) {
              stop_done_.set_exception(std::current_exception());
          }
      });
    static_cast<void>(completion);
    return stop_done_.get_shared_future();
}

seastar::future<> service_lifecycle::stop_once() {
    std::exception_ptr first_failure;
    while (!started_.empty()) {
        action stop_action = std::move(started_.back());
        started_.pop_back();
        try {
            co_await stop_action();
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

service_lifecycle_state service_lifecycle::state() const {
    assert_current();
    return state_;
}

std::size_t service_lifecycle::running_steps() const {
    assert_current();
    return started_.size();
}

} // namespace kwaque::broker
