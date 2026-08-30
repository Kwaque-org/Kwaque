#include "src/runtime/timer.h"
#include "src/simulation/deterministic_random.h"
#include "src/simulation/event_trace.h"
#include "src/simulation/scheduler.h"
#include "src/simulation/scheduler_test_support.h"
#include "src/simulation/timer.h"
#include "src/simulation/virtual_time.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/memory.hh>
#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <exception>
#include <utility>

namespace {

using kwaque::runtime::monotonic_duration;
using kwaque::runtime::monotonic_time;
using kwaque::simulation::clock_binding;
using kwaque::simulation::event_priority;
using kwaque::simulation::scheduler;
using kwaque::simulation::scheduler_limits;
using kwaque::simulation::scheduler_test_access;
using kwaque::simulation::timer;
using kwaque::simulation::trace_digest;
using kwaque::simulation::trace_header;
using kwaque::simulation::trace_limit_values;
using kwaque::simulation::trace_limits;
using kwaque::simulation::virtual_time;
using kwaque::simulation::virtual_time_config;

class preallocated_abort_source final : public seastar::abort_source {
public:
    preallocated_abort_source()
      : exception_(
          std::make_exception_ptr(seastar::abort_requested_exception{})) {}

    std::exception_ptr get_default_exception() const noexcept final {
        return exception_;
    }

private:
    std::exception_ptr exception_;
};

seastar::future<> stop_timer(timer& service, scheduler& target) {
    auto stopping = service.stop();
    if (!stopping.available()) {
        const auto pumped = target.run_ready();
        BOOST_REQUIRE(pumped.has_value());
    }
    const auto outcome = co_await std::move(stopping);
    BOOST_REQUIRE(outcome.has_value());
}

} // namespace

SEASTAR_TEST_CASE(simulation_timer_fires_only_from_explicit_scheduler_pumps) {
    const auto limits = scheduler_limits::defaults();
    scheduler target{limits};
    const auto config = virtual_time_config::make(limits);
    BOOST_REQUIRE(config.has_value());
    virtual_time time{target, *config};
    clock_binding binding{time};
    timer service{target};
    seastar::abort_source caller_abort;

    auto immediate
      = kwaque::runtime::sleep_for<kwaque::simulation::monotonic_clock>(
        service, monotonic_duration{}, caller_abort);
    BOOST_TEST(!immediate.available());
    const auto ready = target.run_ready();
    BOOST_REQUIRE(ready.has_value());
    BOOST_TEST(*ready == 1U);
    const auto immediate_outcome = co_await std::move(immediate);
    BOOST_REQUIRE(immediate_outcome.has_value());

    auto future = service.sleep_until(monotonic_time{10}, caller_abort);
    BOOST_TEST(!future.available());
    const auto before = target.step();
    BOOST_REQUIRE(before.has_value());
    BOOST_TEST(!*before);
    const auto through_ten = target.run_until(monotonic_time{10});
    BOOST_REQUIRE(through_ten.has_value());
    BOOST_TEST(*through_ten == 1U);
    const auto future_outcome = co_await std::move(future);
    BOOST_REQUIRE(future_outcome.has_value());

    auto first = service.sleep_until(monotonic_time{20}, caller_abort);
    auto second = service.sleep_until(monotonic_time{20}, caller_abort);
    BOOST_TEST(!first.available());
    BOOST_TEST(!second.available());
    const auto through_twenty = target.run_until(monotonic_time{20});
    BOOST_REQUIRE(through_twenty.has_value());
    BOOST_TEST(*through_twenty == 2U);
    const auto first_outcome = co_await std::move(first);
    const auto second_outcome = co_await std::move(second);
    BOOST_REQUIRE(first_outcome.has_value());
    BOOST_REQUIRE(second_outcome.has_value());
    co_await stop_timer(service, target);
}

SEASTAR_TEST_CASE(simulation_timer_caller_abort_posts_a_terminal_event) {
    scheduler target{scheduler_limits::defaults()};
    timer service{target};
    preallocated_abort_source caller_abort;
    auto waiting = service.sleep_until(monotonic_time{100}, caller_abort);
    BOOST_TEST(!waiting.available());
    BOOST_TEST(service.pending_waits() == 1U);

    const auto before_abort = seastar::memory::stats().mallocs();
    caller_abort.request_abort();
    const auto after_abort = seastar::memory::stats().mallocs();
    BOOST_TEST(after_abort == before_abort);
    BOOST_TEST(!waiting.available());
    BOOST_TEST(service.pending_waits() == 1U);
    BOOST_TEST(target.pending_events() == 1U);

    const auto pumped = target.run_ready();
    BOOST_REQUIRE(pumped.has_value());
    BOOST_TEST(*pumped == 1U);
    const auto outcome = co_await std::move(waiting);
    BOOST_REQUIRE(!outcome.has_value());
    BOOST_CHECK(outcome.error().code() == kwaque::errc::aborted);
    BOOST_TEST(service.pending_waits() == 0U);
    co_await stop_timer(service, target);

    scheduler preaborted_target{scheduler_limits::defaults()};
    timer preaborted_service{preaborted_target};
    seastar::abort_source preaborted;
    preaborted.request_abort();
    auto rejected = preaborted_service.sleep_until(
      monotonic_time{}, preaborted);
    BOOST_TEST(rejected.available());
    const auto rejected_outcome = co_await std::move(rejected);
    BOOST_REQUIRE(!rejected_outcome.has_value());
    BOOST_CHECK(rejected_outcome.error().code() == kwaque::errc::aborted);
    co_await stop_timer(preaborted_service, preaborted_target);
}

SEASTAR_TEST_CASE(simulation_timer_deadline_abort_race_follows_event_order) {
    scheduler aborted_target{scheduler_limits::defaults()};
    timer aborted_service{aborted_target};
    seastar::abort_source abort_first;
    const auto abort_event = aborted_target.schedule(
      monotonic_time{10}, event_priority::normal(), [&abort_first] noexcept {
          abort_first.request_abort();
      });
    BOOST_REQUIRE(abort_event.has_value());
    auto aborted = aborted_service.sleep_until(monotonic_time{10}, abort_first);
    const auto aborted_run = aborted_target.run_until(monotonic_time{10});
    BOOST_REQUIRE(aborted_run.has_value());
    BOOST_TEST(*aborted_run == 2U);
    const auto aborted_outcome = co_await std::move(aborted);
    BOOST_REQUIRE(!aborted_outcome.has_value());
    BOOST_CHECK(aborted_outcome.error().code() == kwaque::errc::aborted);
    co_await stop_timer(aborted_service, aborted_target);

    scheduler fired_target{scheduler_limits::defaults()};
    timer fired_service{fired_target};
    seastar::abort_source abort_later;
    auto fired = fired_service.sleep_until(monotonic_time{10}, abort_later);
    const auto later_abort_event = fired_target.schedule(
      monotonic_time{10}, event_priority::normal(), [&abort_later] noexcept {
          abort_later.request_abort();
      });
    BOOST_REQUIRE(later_abort_event.has_value());
    const auto fired_run = fired_target.run_until(monotonic_time{10});
    BOOST_REQUIRE(fired_run.has_value());
    BOOST_TEST(*fired_run == 2U);
    const auto fired_outcome = co_await std::move(fired);
    BOOST_REQUIRE(fired_outcome.has_value());
    co_await stop_timer(fired_service, fired_target);
}

SEASTAR_TEST_CASE(simulation_timer_owner_abort_is_pump_driven_and_sticky) {
    scheduler target{scheduler_limits::defaults()};
    timer service{target};
    seastar::abort_source caller_abort;
    auto waiting = service.sleep_until(monotonic_time{100}, caller_abort);

    service.request_abort();
    BOOST_TEST(!waiting.available());
    const auto pumped = target.run_ready();
    BOOST_REQUIRE(pumped.has_value());
    BOOST_TEST(*pumped == 1U);
    const auto outcome = co_await std::move(waiting);
    BOOST_REQUIRE(!outcome.has_value());
    BOOST_CHECK(outcome.error().code() == kwaque::errc::aborted);

    auto rejected = service.sleep_until(monotonic_time{}, caller_abort);
    BOOST_TEST(rejected.available());
    const auto rejected_outcome = co_await std::move(rejected);
    BOOST_REQUIRE(!rejected_outcome.has_value());
    BOOST_CHECK(rejected_outcome.error().code() == kwaque::errc::aborted);
    co_await stop_timer(service, target);
}

SEASTAR_TEST_CASE(simulation_timer_stop_is_pump_driven_and_idempotent) {
    scheduler target{scheduler_limits::defaults()};
    timer service{target};
    seastar::abort_source caller_abort;
    auto first = service.sleep_until(monotonic_time{100}, caller_abort);
    auto second = service.sleep_until(monotonic_time{200}, caller_abort);

    auto first_stop = service.stop();
    auto second_stop = service.stop();
    BOOST_TEST(!first.available());
    BOOST_TEST(!second.available());
    BOOST_TEST(!first_stop.available());
    BOOST_TEST(!second_stop.available());
    BOOST_TEST(target.pending_events() == 2U);

    const auto pumped = target.run_ready();
    BOOST_REQUIRE(pumped.has_value());
    BOOST_TEST(*pumped == 2U);
    const auto first_outcome = co_await std::move(first);
    const auto second_outcome = co_await std::move(second);
    BOOST_REQUIRE(!first_outcome.has_value());
    BOOST_REQUIRE(!second_outcome.has_value());
    BOOST_CHECK(first_outcome.error().code() == kwaque::errc::aborted);
    BOOST_CHECK(second_outcome.error().code() == kwaque::errc::aborted);
    const auto first_stop_outcome = co_await std::move(first_stop);
    const auto second_stop_outcome = co_await std::move(second_stop);
    BOOST_REQUIRE(first_stop_outcome.has_value());
    BOOST_REQUIRE(second_stop_outcome.has_value());
    BOOST_CHECK(service.state() == kwaque::simulation::timer_state::stopped);

    auto closed = service.sleep_until(monotonic_time{}, caller_abort);
    BOOST_TEST(closed.available());
    const auto closed_outcome = co_await std::move(closed);
    BOOST_REQUIRE(!closed_outcome.has_value());
    BOOST_CHECK(closed_outcome.error().code() == kwaque::errc::closed);
}

SEASTAR_TEST_CASE(simulation_timer_reserves_a_terminal_event_id) {
    scheduler target{scheduler_limits::defaults()};
    scheduler_test_access::use_final_event_id(target);
    timer service{target};
    seastar::abort_source caller_abort;

    auto rejected = service.sleep_until(monotonic_time{}, caller_abort);
    BOOST_TEST(rejected.available());
    const auto outcome = co_await std::move(rejected);
    BOOST_REQUIRE(!outcome.has_value());
    BOOST_CHECK(outcome.error().code() == kwaque::errc::out_of_range);
    BOOST_CHECK(
      outcome.error().operation() == kwaque::runtime::operation_kind::timer);
    BOOST_TEST(service.pending_waits() == 0U);
    co_await stop_timer(service, target);
}

SEASTAR_TEST_CASE(
  simulation_timer_abort_reuses_the_only_pending_scheduler_slot) {
    const auto limits = scheduler_limits::make(
      kwaque::simulation::scheduler_limit_values{
        .pending_events = 1,
        .events_per_pump = 2,
        .total_events = 2,
        .maximum_deadline = monotonic_time{100},
      });
    BOOST_REQUIRE(limits.has_value());
    scheduler target{*limits};
    timer service{target};
    preallocated_abort_source caller_abort;
    auto waiting = service.sleep_until(monotonic_time{100}, caller_abort);
    BOOST_TEST(target.pending_events() == 1U);
    caller_abort.request_abort();
    BOOST_TEST(target.pending_events() == 1U);
    BOOST_TEST(!waiting.available());
    const auto pumped = target.run_ready();
    BOOST_REQUIRE(pumped.has_value());
    BOOST_TEST(*pumped == 1U);
    const auto outcome = co_await std::move(waiting);
    BOOST_REQUIRE(!outcome.has_value());
    BOOST_CHECK(outcome.error().code() == kwaque::errc::aborted);
    co_await stop_timer(service, target);
}

SEASTAR_TEST_CASE(simulation_timer_reserves_terminal_trace_capacity) {
    const auto scheduler_budget = scheduler_limits::make(
      kwaque::simulation::scheduler_limit_values{
        .pending_events = 4,
        .events_per_pump = 4,
        .total_events = 8,
        .maximum_deadline = monotonic_time{100},
      });
    BOOST_REQUIRE(scheduler_budget.has_value());
    const auto trace_budget = trace_limits::make(
      trace_limit_values{
        .entries = 4,
        .encoded_bytes = kwaque::simulation::canonical_header_encoded_size
                         + 4 * kwaque::simulation::canonical_entry_encoded_size,
        .line_bytes = 1'024,
      });
    BOOST_REQUIRE(trace_budget.has_value());
    kwaque::simulation::event_trace trace{
      trace_header::current(
        1,
        kwaque::simulation::deterministic_random_algorithm_version,
        kwaque::simulation::deterministic_random_coordinate_version,
        kwaque::simulation::trace_budget(*scheduler_budget),
        *trace_budget,
        trace_digest{},
        trace_digest{}),
      *trace_budget};
    scheduler target{*scheduler_budget, &trace};
    timer service{target};
    preallocated_abort_source caller_abort;
    auto waiting = service.sleep_until(monotonic_time{100}, caller_abort);
    BOOST_TEST(!waiting.available());

    caller_abort.request_abort();
    BOOST_TEST(!waiting.available());
    const auto pumped = target.run_ready();
    BOOST_REQUIRE(pumped.has_value());
    BOOST_TEST(*pumped == 1U);
    const auto outcome = co_await std::move(waiting);
    BOOST_REQUIRE(!outcome.has_value());
    BOOST_CHECK(outcome.error().code() == kwaque::errc::aborted);
    BOOST_TEST(trace.entries().size() == 4U);
    co_await stop_timer(service, target);
}

SEASTAR_TEST_CASE(
  simulation_timer_reports_cancel_replay_divergence_without_invariant_abort) {
    const auto scheduler_budget = scheduler_limits::make(
      kwaque::simulation::scheduler_limit_values{
        .pending_events = 4,
        .events_per_pump = 8,
        .total_events = 16,
        .maximum_deadline = monotonic_time{100},
      });
    BOOST_REQUIRE(scheduler_budget.has_value());
    const auto trace_budget = trace_limits::make(
      trace_limit_values{
        .entries = 16,
        .encoded_bytes = 8'192,
        .line_bytes = 1'024,
      });
    BOOST_REQUIRE(trace_budget.has_value());
    const auto header = trace_header::current(
      1,
      kwaque::simulation::deterministic_random_algorithm_version,
      kwaque::simulation::deterministic_random_coordinate_version,
      kwaque::simulation::trace_budget(*scheduler_budget),
      *trace_budget,
      trace_digest{},
      trace_digest{});

    kwaque::simulation::event_trace captured{header, *trace_budget};
    {
        scheduler target{*scheduler_budget, &captured};
        timer service{target};
        preallocated_abort_source caller_abort;
        auto waiting = service.sleep_until(monotonic_time{100}, caller_abort);
        caller_abort.request_abort();
        const auto pumped = target.run_ready();
        BOOST_REQUIRE(pumped.has_value());
        const auto outcome = co_await std::move(waiting);
        BOOST_REQUIRE(!outcome.has_value());
        co_await stop_timer(service, target);
    }
    const auto encoded = captured.encode();
    BOOST_REQUIRE(encoded.has_value());
    for (const bool mutate_terminal_schedule : {false, true}) {
        auto decoded = kwaque::simulation::event_trace::decode(
          *encoded, *trace_budget);
        BOOST_REQUIRE(decoded.has_value());
        auto selected = decoded->entries.end();
        std::size_t scheduled_seen = 0;
        for (auto position = decoded->entries.begin();
             position != decoded->entries.end();
             ++position) {
            if (
              !mutate_terminal_schedule
              && position->action
                   == kwaque::simulation::trace_action::canceled) {
                selected = position;
                break;
            }
            if (
              mutate_terminal_schedule
              && position->action == kwaque::simulation::trace_action::scheduled
              && ++scheduled_seen == 2U) {
                selected = position;
                break;
            }
        }
        BOOST_REQUIRE(selected != decoded->entries.end());
        ++selected->value;
        auto replay = kwaque::simulation::event_trace::replay(
          header, *trace_budget, std::move(*decoded));
        BOOST_REQUIRE(replay.has_value());

        scheduler replay_target{*scheduler_budget, replay->get()};
        timer replay_service{replay_target};
        preallocated_abort_source replay_abort;
        auto replay_waiting = replay_service.sleep_until(
          monotonic_time{100}, replay_abort);
        replay_abort.request_abort();
        BOOST_TEST(replay_target.trace_failed());
        BOOST_TEST(!replay_waiting.available());
        BOOST_TEST(
          replay_target.pending_events()
          == (mutate_terminal_schedule ? 0U : 1U));
        BOOST_REQUIRE(replay_target.discard_failed());

        if (mutate_terminal_schedule) {
            BOOST_TEST(!replay_waiting.available());
            auto stopping = replay_service.stop();
            BOOST_TEST(replay_waiting.available());
            const auto stop_outcome = co_await std::move(stopping);
            BOOST_REQUIRE(!stop_outcome.has_value());
            BOOST_CHECK(
              stop_outcome.error().code() == kwaque::errc::replay_divergence);
        } else {
            BOOST_TEST(replay_waiting.available());
        }
        const auto replay_outcome = co_await std::move(replay_waiting);
        BOOST_REQUIRE(!replay_outcome.has_value());
        BOOST_CHECK(
          replay_outcome.error().code() == kwaque::errc::replay_divergence);
        BOOST_CHECK(
          replay_outcome.error().operation()
          == kwaque::runtime::operation_kind::timer);
        if (!mutate_terminal_schedule) {
            co_await stop_timer(replay_service, replay_target);
        }
    }
}
