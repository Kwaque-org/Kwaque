#include "src/runtime/waiting.h"

#include <seastar/core/timed_out_error.hh>

namespace kwaque::runtime {

const std::exception_ptr& deadline_exceeded() noexcept {
    static thread_local const std::exception_ptr exception
      = std::make_exception_ptr(seastar::timed_out_error{});
    return exception;
}

const std::exception_ptr& operation_aborted() noexcept {
    static thread_local const std::exception_ptr exception
      = std::make_exception_ptr(seastar::abort_requested_exception{});
    return exception;
}

operation_error ended_by(const seastar::abort_source& source) noexcept {
    return ended_by(std::optional{source.abort_requested_exception_ptr()});
}

operation_error
ended_by(const std::optional<std::exception_ptr>& reason) noexcept {
    return operation_error{
      reason && *reason == deadline_exceeded() ? errc::timed_out
                                               : errc::aborted,
      operation_kind::runtime};
}

unit_queue::~unit_queue() {
    assert_current();
    KWAQUE_INVARIANT(
      invariant_id{"KQ-UNIT-QUEUE-WAITERS"},
      waiters_.empty(),
      "a unit queue was destroyed with waits queued");
}

bool unit_queue::try_wait(std::size_t count) noexcept {
    assert_current();
    if (broken_ || !waiters_.empty() || count > available_) {
        return false;
    }
    available_ -= count;
    return true;
}

void unit_queue::signal(std::size_t count) noexcept {
    assert_current();
    if (broken_) {
        return;
    }
    available_ += count;
    grant();
}

void unit_queue::grant() noexcept {
    while (!waiters_.empty()) {
        auto& first = waiters_.front();
        if (first.count > available_) {
            return;
        }
        available_ -= first.count;
        static_cast<void>(first.tell({}));
    }
}

void unit_queue::broken(errc reason) noexcept {
    assert_current();
    broken_ = reason;
    // As a broken semaphore holds no units.
    available_ = 0;
    while (!waiters_.empty()) {
        static_cast<void>(waiters_.front().tell(
          failure(operation_error{reason, operation_kind::runtime})));
    }
}

std::size_t unit_queue::waiters() const noexcept {
    std::size_t count = 0;
    for ([[maybe_unused]] const auto& queued : waiters_) {
        ++count;
    }
    return count;
}

std::optional<unit_queue::units>
try_get_units(unit_queue& queue, std::size_t count) noexcept {
    if (!queue.try_wait(count)) {
        return std::nullopt;
    }
    return unit_queue::units{queue, count};
}

} // namespace kwaque::runtime
