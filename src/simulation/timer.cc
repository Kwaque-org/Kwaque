#include "src/simulation/timer.h"

#include "src/base/invariant.h"

#include <algorithm>
#include <exception>
#include <iterator>
#include <utility>

namespace kwaque::simulation {

namespace {

constexpr invariant_id timer_drained_invariant{"KQ-SIM-TIMER-DRAINED"};
constexpr invariant_id timer_event_invariant{"KQ-SIM-TIMER-EVENT"};

[[nodiscard]] runtime::operation_error timer_error(errc code) noexcept {
    return runtime::operation_error{code, runtime::operation_kind::timer};
}

[[nodiscard]] runtime::operation_error
timer_error(const runtime::operation_error& source) noexcept {
    auto translated = timer_error(source.code());
    for (std::size_t index = 0; index < source.context_size(); ++index) {
        const auto field = source.context_at(index);
        static_cast<void>(translated.add_context(field->key, field->value));
    }
    return translated;
}

} // namespace

timer::timer(scheduler& target) noexcept
  : scheduler_(&target) {
    assert_current();
}

timer::~timer() {
    assert_current();
    const auto failed = scheduler_->trace_failed();
    if (failed) {
        const auto* failure = scheduler_->trace_failure();
        KWAQUE_INVARIANT(
          timer_event_invariant,
          failure != nullptr,
          "failed scheduler has no trace error");
        static_cast<void>(scheduler_->discard_failed());
        discard_remaining(*failure);
    }
    KWAQUE_INVARIANT(
      timer_drained_invariant,
      waits_.empty()
        && (failed || state_ == timer_state::stopped || (state_ == timer_state::open && !activated_)),
      "simulation timer destroyed before drain");
}

seastar::future<runtime::result<void>> timer::sleep_until(
  runtime::monotonic_time deadline, seastar::abort_source& caller_abort) {
    assert_current();
    if (state_ != timer_state::open) {
        return ready_failure(timer_error(errc::closed));
    }
    if (abort_requested_ || caller_abort.abort_requested()) {
        return ready_failure(timer_error(errc::aborted));
    }

    const trace_event_descriptor terminal_descriptor{
      .kind = trace_event_kind::timer,
      .result = static_cast<std::uint32_t>(errc::aborted),
    };
    auto terminal_trace = scheduler_->reserve_trace(terminal_descriptor);
    if (!terminal_trace) {
        return ready_failure(timer_error(terminal_trace.error()));
    }
    auto terminal_id = scheduler_->reserve_event_id();
    if (!terminal_id) {
        return ready_failure(timer_error(terminal_id.error()));
    }

    try {
        waits_.emplace_back(
          std::move(*terminal_id), std::move(*terminal_trace));
    } catch (...) {
        return seastar::current_exception_as_future<runtime::result<void>>();
    }
    auto position = std::prev(waits_.end());
    auto& wait = *position;
    wait.position = position;
    auto waiting = wait.completion.get_future();

    auto subscription = caller_abort.subscribe(
      [this, state = &wait](const std::optional<std::exception_ptr>&) noexcept {
          schedule_abort(*state, errc::aborted);
      });
    if (!subscription) {
        auto completion = std::move(wait.completion);
        waits_.erase(position);
        runtime::result<void> outcome = runtime::failure(
          timer_error(errc::aborted));
        completion.set_value(std::move(outcome));
        return waiting;
    }
    wait.caller_subscription.emplace(std::move(*subscription));

    const auto effective_deadline = std::max(deadline, scheduler_->now());
    auto scheduled = scheduler_->schedule(
      effective_deadline,
      event_priority::normal(),
      [this, state = &wait] noexcept {
          if (scheduler_->discarding_failed_event()) [[unlikely]] {
              const auto* failure = scheduler_->trace_failure();
              KWAQUE_INVARIANT(
                timer_event_invariant,
                failure != nullptr,
                "discarded timer event has no trace error");
              discard_wait(*state, *failure);
          } else {
              complete_wait(*state);
          }
      },
      trace_event_descriptor{
        .kind = trace_event_kind::timer,
        .result = static_cast<std::uint32_t>(errc::success),
      },
      event_cleanup_policy::invoke);
    if (!scheduled) {
        auto completion = std::move(wait.completion);
        wait.caller_subscription.reset();
        waits_.erase(position);
        runtime::result<void> outcome = runtime::failure(
          timer_error(scheduled.error()));
        completion.set_value(std::move(outcome));
        return waiting;
    }
    wait.event = *scheduled;
    activated_ = true;
    return waiting;
}

void timer::request_abort() noexcept {
    assert_current();
    if (abort_requested_) {
        return;
    }
    abort_requested_ = true;
    for (auto& wait : waits_) {
        schedule_abort(wait, errc::aborted);
    }
}

seastar::future<runtime::result<void>> timer::stop() {
    assert_current();
    if (state_ == timer_state::stopping) {
        return stop_done_->get_shared_future();
    }
    if (state_ == timer_state::stopped) {
        return stop_done_ && stop_done_->available()
                 ? stop_done_->get_shared_future()
                 : seastar::make_ready_future<runtime::result<void>>(
                     runtime::result<void>{});
    }

    try {
        stop_done_.emplace();
    } catch (...) {
        return seastar::current_exception_as_future<runtime::result<void>>();
    }
    state_ = timer_state::stopping;
    request_abort();
    if (scheduler_->trace_failed() && scheduler_->discard_failed()) {
        discard_remaining(*scheduler_->trace_failure());
    }
    if (state_ == timer_state::stopping && waits_.empty()) {
        finish_stop();
    }
    return stop_done_->get_shared_future();
}

timer_state timer::state() const {
    assert_current();
    return state_;
}

std::size_t timer::pending_waits() const {
    assert_current();
    return waits_.size();
}

seastar::future<runtime::result<void>>
timer::ready_failure(runtime::operation_error error) {
    runtime::result<void> outcome = runtime::failure(std::move(error));
    return seastar::make_ready_future<runtime::result<void>>(
      std::move(outcome));
}

void timer::schedule_abort(wait_state& wait, errc outcome) noexcept {
    if (wait.phase != wait_phase::waiting) {
        return;
    }
    const auto canceled = scheduler_->cancel(wait.event);
    if (!canceled) {
        KWAQUE_INVARIANT(
          timer_event_invariant,
          scheduler_->trace_failed(),
          "timer cancellation failed without trace divergence");
        return;
    }
    KWAQUE_INVARIANT(
      timer_event_invariant, *canceled, "aborted timer event was not pending");
    wait.phase = wait_phase::terminal_scheduled;
    wait.outcome = outcome;
    wait.terminal_id.release();
    auto terminal = scheduler_->schedule(
      scheduler_->now(),
      event_priority::normal(),
      [this, state = &wait] noexcept {
          if (scheduler_->discarding_failed_event()) [[unlikely]] {
              const auto* failure = scheduler_->trace_failure();
              KWAQUE_INVARIANT(
                timer_event_invariant,
                failure != nullptr,
                "discarded timer event has no trace error");
              discard_wait(*state, *failure);
          } else {
              complete_wait(*state);
          }
      },
      trace_event_descriptor{
        .kind = trace_event_kind::timer,
        .result = static_cast<std::uint32_t>(outcome),
      },
      event_cleanup_policy::invoke,
      std::move(wait.terminal_trace));
    if (!terminal) {
        KWAQUE_INVARIANT(
          timer_event_invariant,
          scheduler_->trace_failed(),
          "reserved timer terminal event failed without trace divergence");
        wait.phase = wait_phase::discard_pending;
        return;
    }
    wait.event = *terminal;
}

void timer::complete_wait(wait_state& wait) noexcept {
    assert_current();
    auto completion = std::move(wait.completion);
    wait.caller_subscription.reset();
    wait.terminal_id.release();
    const auto outcome = wait.outcome;
    waits_.erase(wait.position);

    if (outcome == errc::success) {
        completion.set_value(runtime::result<void>{});
    } else {
        runtime::result<void> failure = runtime::failure(timer_error(outcome));
        completion.set_value(std::move(failure));
    }
    if (state_ == timer_state::stopping && waits_.empty()) {
        finish_stop();
    }
}

void timer::discard_wait(
  wait_state& wait, const runtime::operation_error& failure) noexcept {
    assert_current();
    auto completion = std::move(wait.completion);
    wait.caller_subscription.reset();
    wait.terminal_id.release();
    waits_.erase(wait.position);

    auto translated = timer_error(failure);
    if (state_ == timer_state::stopping && !stop_failure_) {
        stop_failure_.emplace(translated);
    }
    runtime::result<void> outcome = runtime::failure(std::move(translated));
    completion.set_value(std::move(outcome));
    if (state_ == timer_state::stopping && waits_.empty()) {
        finish_stop();
    }
}

void timer::discard_remaining(
  const runtime::operation_error& failure) noexcept {
    while (!waits_.empty()) {
        discard_wait(waits_.front(), failure);
    }
}

void timer::finish_stop() noexcept {
    KWAQUE_INVARIANT(
      timer_drained_invariant,
      state_ == timer_state::stopping && waits_.empty(),
      "simulation timer stop completed with pending waits");
    state_ = timer_state::stopped;
    if (stop_failure_) {
        runtime::result<void> outcome = runtime::failure(*stop_failure_);
        stop_done_->set_value(std::move(outcome));
    } else {
        stop_done_->set_value(runtime::result<void>{});
    }
}

} // namespace kwaque::simulation
