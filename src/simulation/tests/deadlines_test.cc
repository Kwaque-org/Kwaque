#include "src/runtime/time.h"
#include "src/runtime/waiting.h"
#include "src/simulation/alarm.h"
#include "src/simulation/environment.h"
#include "src/simulation/scheduler.h"
#include "src/simulation/scheduler_driver.h"
#include "src/simulation/virtual_time.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/when_all.hh>
#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <cstdint>
#include <utility>
#include <vector>

namespace {

namespace runtime = kwaque::runtime;
namespace simulation = kwaque::simulation;
using runtime::monotonic_time;

constexpr std::uint64_t millisecond = 1'000'000;

struct ending final {
    std::uint64_t deadline;
    std::uint64_t ended;
    bool timed_out;
};

seastar::future<> wait_until(
  runtime::unit_queue& queue,
  simulation::environment::deadlines_type& deadlines,
  monotonic_time deadline,
  std::vector<ending>& endings) {
    const auto outcome = co_await queue.wait(1, nullptr, deadlines, deadline);
    endings.push_back(
      ending{
        .deadline = deadline.nanoseconds(),
        .ended = simulation::monotonic_clock::now().nanoseconds(),
        .timed_out = !outcome.has_value()
                     && outcome.error().code() == kwaque::errc::timed_out,
      });
}

seastar::future<std::vector<ending>> run_once() {
    simulation::environment_config_values values;
    values.master_seed = 5;
    auto config = simulation::environment_config::make(std::move(values));
    BOOST_REQUIRE(config.has_value());
    auto made = simulation::environment::make(std::move(*config));
    BOOST_REQUIRE(made.has_value());
    auto& target = **made;
    auto starting = target.start();
    co_await simulation::testing::pump_until(
      target.event_scheduler(), starting);
    co_await std::move(starting);

    std::vector<ending> endings;
    {
        runtime::unit_queue queue{0};
        const auto now = simulation::monotonic_clock::now().nanoseconds();
        std::vector<seastar::future<>> waits;
        for (const std::uint64_t delay :
             {3 * millisecond, 70 * millisecond + 1, 600 * millisecond}) {
            waits.push_back(wait_until(
              queue, target.deadlines(), monotonic_time{now + delay}, endings));
        }
        auto all = seastar::when_all_succeed(std::move(waits));
        co_await simulation::testing::pump_until(target.event_scheduler(), all);
        co_await std::move(all);
    }

    auto stopping = target.stop();
    co_await simulation::testing::pump_until(
      target.event_scheduler(), stopping);
    co_await std::move(stopping);
    co_return endings;
}

} // namespace

SEASTAR_TEST_CASE(simulated_deadlines_end_waits_exactly_and_replay) {
    const auto first = co_await run_once();
    BOOST_REQUIRE_EQUAL(first.size(), 3U);
    for (const auto& one : first) {
        BOOST_CHECK(one.timed_out);
        BOOST_CHECK_GE(one.ended, one.deadline);
        BOOST_CHECK_LE(one.ended, one.deadline + millisecond);
    }
    const auto second = co_await run_once();
    BOOST_REQUIRE_EQUAL(second.size(), first.size());
    for (std::size_t index = 0; index < first.size(); ++index) {
        BOOST_CHECK_EQUAL(second[index].deadline - first[index].deadline, 0U);
        BOOST_CHECK_EQUAL(second[index].ended, first[index].ended);
    }
}

// Nothing rings after a run's last time, so an alarm for later stays unarmed
// rather than failing the run.
SEASTAR_TEST_CASE(an_alarm_past_the_runs_last_time_stays_unarmed) {
    auto limits = simulation::scheduler_limits::make(
      simulation::scheduler_limit_values{
        .pending_events = 64,
        .events_per_pump = 64,
        .total_events = 1'000,
        .maximum_deadline = monotonic_time{1'000 * millisecond},
      });
    BOOST_REQUIRE(limits.has_value());
    simulation::scheduler events{*limits};
    unsigned rang = 0;
    simulation::alarm ringing{[&rang]() noexcept { ++rang; }, events};
    ringing.arm(monotonic_time{2'000 * millisecond});
    BOOST_CHECK(!ringing.armed());
    ringing.arm(monotonic_time{500 * millisecond});
    BOOST_CHECK(ringing.armed());
    // Arming again past the last time drops the earlier ring too.
    ringing.arm(monotonic_time{1'001 * millisecond});
    BOOST_CHECK(!ringing.armed());
    BOOST_CHECK_EQUAL(events.pending_events(), 0U);
    BOOST_CHECK_EQUAL(rang, 0U);
    co_return;
}
