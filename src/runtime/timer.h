#ifndef KWAQUE_SRC_RUNTIME_TIMER_H_
#define KWAQUE_SRC_RUNTIME_TIMER_H_

#include "src/runtime/error.h"
#include "src/runtime/time.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>

#include <concepts>
#include <utility>

namespace kwaque::runtime {

template<typename Service>
concept timer_service = requires(
  Service& service,
  monotonic_time deadline,
  seastar::abort_source& abort_source) {
    {
        service.sleep_until(deadline, abort_source)
    } -> std::same_as<seastar::future<result<void>>>;
    { service.request_abort() } noexcept -> std::same_as<void>;
    { service.stop() } -> std::same_as<seastar::future<result<void>>>;
};

// A service accepts independent concurrent waits. Each accepted wait resolves
// exactly once. stop() prevents new waits, resolves or drains every accepted
// wait, and completes before the service is destroyed.

// Relative waits share one overflow rule and delegate to the backend's single
// absolute-deadline primitive. Backends must schedule past/now deadlines rather
// than returning an inline completion.
template<monotonic_clock Clock, timer_service Service>
[[nodiscard]] seastar::future<result<void>> sleep_for(
  Service& service,
  monotonic_duration duration,
  seastar::abort_source& abort_source) {
    const auto deadline = Clock::now().checked_add(duration);
    if (!deadline) {
        result<void> outcome = failure(
          operation_error{errc::out_of_range, operation_kind::timer});
        return seastar::make_ready_future<result<void>>(std::move(outcome));
    }
    return service.sleep_until(*deadline, abort_source);
}

} // namespace kwaque::runtime

#endif // KWAQUE_SRC_RUNTIME_TIMER_H_
