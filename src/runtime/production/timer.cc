#include "src/runtime/production/timer.h"

#include "src/base/invariant.h"
#include "src/runtime/production/clocks.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sleep.hh>

#include <chrono>
#include <limits>
#include <new>
#include <system_error>
#include <utility>

namespace kwaque::runtime::production {

namespace {

constexpr invariant_id timer_stopped_invariant{"KQ-TIMER-STOPPED"};
constexpr invariant_id timer_gate_invariant{"KQ-TIMER-GATE-OPEN"};

operation_error timer_error(errc code) noexcept {
    return operation_error{code, operation_kind::timer};
}

errc map_timer_system_error(const std::error_code& error) noexcept {
    if (error == std::errc::operation_canceled) {
        return errc::aborted;
    }
    if (error == std::errc::timed_out) {
        return errc::timed_out;
    }
    if (
      error == std::errc::not_enough_memory
      || error == std::errc::resource_unavailable_try_again) {
        return errc::resource_exhausted;
    }
    return errc::unavailable;
}

result<seastar::lowres_clock::duration>
native_duration_until(monotonic_time deadline, monotonic_time now) noexcept {
    if (deadline <= now) {
        return seastar::lowres_clock::duration::zero();
    }
    const auto delta = deadline.nanoseconds() - now.nanoseconds();
    using native_rep = seastar::lowres_clock::duration::rep;
    const auto native_maximum = static_cast<std::uint64_t>(
      std::numeric_limits<native_rep>::max());
    if (now.nanoseconds() > native_maximum) {
        return failure(timer_error(errc::out_of_range));
    }
    const auto maximum_delta = native_maximum - now.nanoseconds();
    if (delta > maximum_delta) {
        return failure(timer_error(errc::out_of_range));
    }
    return seastar::lowres_clock::duration{static_cast<native_rep>(delta)};
}

} // namespace

timer::~timer() {
    owner_.assert_current();
    KWAQUE_INVARIANT(
      timer_stopped_invariant,
      (state_ == timer_state::stopped
       || (state_ == timer_state::open && !activated_))
        && waiters_.get_count() == 0,
      "timer destroyed before stop completed");
}

seastar::future<result<void>> timer::sleep_until(
  monotonic_time deadline, seastar::abort_source& caller_abort) {
    owner_.assert_current();
    if (state_ != timer_state::open) {
        statistics_->reject();
        co_return failure(timer_error(errc::closed));
    }
    if (abort_requested_ || caller_abort.abort_requested()) {
        statistics_->reject();
        co_return failure(timer_error(errc::aborted));
    }

    activated_ = true;
    auto holder = waiters_.try_hold();
    KWAQUE_INVARIANT(
      timer_gate_invariant,
      holder.has_value(),
      "open timer rejected waiter gate entry");

    const auto duration = native_duration_until(
      deadline, monotonic_clock::now());
    if (!duration) {
        statistics_->reject();
        co_return failure(duration.error());
    }
    [[maybe_unused]] auto metric = statistics_->accept();

    seastar::abort_source sleep_abort;
    auto caller_subscription = caller_abort.subscribe(
      [&sleep_abort](const std::optional<std::exception_ptr>&) noexcept {
          sleep_abort.request_abort();
      });
    auto owner_subscription = owner_abort_.subscribe(
      [&sleep_abort](const std::optional<std::exception_ptr>&) noexcept {
          sleep_abort.request_abort();
      });
    if (!caller_subscription || !owner_subscription) {
        sleep_abort.request_abort();
    }

    try {
        co_await seastar::sleep_abortable<seastar::lowres_clock>(
          *duration, sleep_abort);
        co_return result<void>{};
    } catch (const std::bad_alloc&) {
        throw;
    } catch (const seastar::abort_requested_exception&) {
        co_return failure(timer_error(errc::aborted));
    } catch (const std::system_error& error) {
        co_return failure(timer_error(map_timer_system_error(error.code())));
    } catch (...) {
        co_return failure(timer_error(errc::unavailable));
    }
}

void timer::request_abort() noexcept {
    owner_.assert_current();
    if (abort_requested_) {
        return;
    }
    abort_requested_ = true;
    owner_abort_.request_abort();
}

seastar::future<result<void>> timer::stop() {
    owner_.assert_current();
    if (state_ == timer_state::stopping) {
        return stop_done_->get_shared_future();
    }
    if (state_ == timer_state::stopped) {
        return stop_done_ && stop_done_->available()
                 ? stop_done_->get_shared_future()
                 : seastar::make_ready_future<result<void>>(result<void>{});
    }

    try {
        stop_done_.emplace();
    } catch (...) {
        return seastar::current_exception_as_future<result<void>>();
    }
    state_ = timer_state::stopping;
    request_abort();
    auto completion = stop_once().then_wrapped(
      [this](seastar::future<result<void>> stopped) {
          state_ = timer_state::stopped;
          try {
              stop_done_->set_value(stopped.get());
          } catch (...) {
              stop_done_->set_exception(std::current_exception());
          }
      });
    static_cast<void>(completion);
    return stop_done_->get_shared_future();
}

seastar::future<result<void>> timer::stop_once() {
    co_await waiters_.close();
    co_return result<void>{};
}

timer_state timer::state() const {
    owner_.assert_current();
    return state_;
}

} // namespace kwaque::runtime::production
