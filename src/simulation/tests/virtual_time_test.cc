#include "src/simulation/virtual_time.h"
#include "src/simulation/virtual_time_test_support.h"

#include <seastar/core/coroutine.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/alloc_failure_injector.hh>

#include <boost/test/unit_test.hpp>

#include <cstdint>
#include <limits>

namespace {

using kwaque::runtime::monotonic_duration;
using kwaque::runtime::monotonic_time;
using kwaque::runtime::wall_time;
using kwaque::simulation::clock_binding;
using kwaque::simulation::event_priority;
using kwaque::simulation::scheduler;
using kwaque::simulation::scheduler_limit_values;
using kwaque::simulation::scheduler_limits;
using kwaque::simulation::virtual_time;
using kwaque::simulation::virtual_time_config;
using kwaque::simulation::virtual_time_config_values;
using kwaque::simulation::virtual_time_test_access;
using kwaque::simulation::wall_offset;

scheduler_limits test_scheduler_limits(std::uint64_t maximum_deadline = 100) {
    auto limits = scheduler_limits::make(
      scheduler_limit_values{
        .pending_events = 16,
        .events_per_pump = 16,
        .total_events = 32,
        .maximum_deadline = monotonic_time{maximum_deadline},
      });
    BOOST_REQUIRE(limits.has_value());
    return *limits;
}

virtual_time_config test_time_config(
  const scheduler_limits& limits,
  wall_time epoch = wall_time{1'000},
  monotonic_duration maximum_adjustment = monotonic_duration{100}) {
    auto config = virtual_time_config::make(
      limits,
      virtual_time_config_values{
        .epoch = epoch,
        .maximum_wall_adjustment = maximum_adjustment,
      });
    BOOST_REQUIRE(config.has_value());
    return *config;
}

} // namespace

SEASTAR_TEST_CASE(virtual_time_configuration_prevents_every_wall_overflow) {
    const auto limits = test_scheduler_limits();

    const auto absolute_scheduler = scheduler_limits::make(
      scheduler_limit_values{
        .pending_events = 1,
        .events_per_pump = 1,
        .total_events = 1,
        .maximum_deadline = scheduler_limits::maximum_deadline_absolute,
      });
    BOOST_REQUIRE(absolute_scheduler.has_value());
    const auto absolute_config = virtual_time_config::make(
      *absolute_scheduler,
      virtual_time_config_values{
        .epoch = wall_time{-static_cast<std::int64_t>(
          virtual_time_config::maximum_wall_adjustment_absolute.nanoseconds())},
        .maximum_wall_adjustment
        = virtual_time_config::maximum_wall_adjustment_absolute,
      });
    BOOST_REQUIRE(absolute_config.has_value());

    auto zero_adjustment = virtual_time_config::make(
      limits,
      virtual_time_config_values{
        .epoch = wall_time{},
        .maximum_wall_adjustment = monotonic_duration{},
      });
    BOOST_REQUIRE(!zero_adjustment.has_value());
    BOOST_CHECK(
      zero_adjustment.error().code() == kwaque::errc::invalid_argument);

    auto excessive_adjustment = virtual_time_config::make(
      limits,
      virtual_time_config_values{
        .epoch = wall_time{},
        .maximum_wall_adjustment = monotonic_duration{
          virtual_time_config::maximum_wall_adjustment_absolute.nanoseconds()
          + 1},
      });
    BOOST_REQUIRE(!excessive_adjustment.has_value());
    BOOST_CHECK(
      excessive_adjustment.error().code() == kwaque::errc::out_of_range);

    auto upper_overflow = virtual_time_config::make(
      limits,
      virtual_time_config_values{
        .epoch = wall_time{std::numeric_limits<std::int64_t>::max()},
        .maximum_wall_adjustment = monotonic_duration{1},
      });
    BOOST_REQUIRE(!upper_overflow.has_value());
    BOOST_CHECK(upper_overflow.error().code() == kwaque::errc::out_of_range);

    auto lower_overflow = virtual_time_config::make(
      limits,
      virtual_time_config_values{
        .epoch = wall_time{std::numeric_limits<std::int64_t>::min()},
        .maximum_wall_adjustment = monotonic_duration{1},
      });
    BOOST_REQUIRE(!lower_overflow.has_value());
    BOOST_CHECK(lower_overflow.error().code() == kwaque::errc::out_of_range);
    co_return;
}

SEASTAR_TEST_CASE(virtual_clocks_bind_one_environment_and_isolate_epochs) {
    BOOST_TEST(!virtual_time_test_access::clock_is_bound());
    {
        const auto limits = test_scheduler_limits();
        scheduler target{limits};
        virtual_time time{target, test_time_config(limits, wall_time{1'000})};
        {
            clock_binding binding{time};
            BOOST_TEST(virtual_time_test_access::clock_is_bound());
            BOOST_TEST(
              kwaque::simulation::monotonic_clock::now().nanoseconds() == 0U);
            BOOST_TEST(
              kwaque::simulation::wall_clock::now().unix_nanoseconds()
              == 1'000);
            const auto advanced = target.run_until(monotonic_time{7});
            BOOST_REQUIRE(advanced.has_value());
            BOOST_TEST(
              kwaque::simulation::monotonic_clock::now().nanoseconds() == 7U);
            BOOST_TEST(
              kwaque::simulation::wall_clock::now().unix_nanoseconds()
              == 1'007);
            const auto maximum = target.run_until(limits.maximum_deadline());
            BOOST_REQUIRE(maximum.has_value());
            BOOST_TEST(
              kwaque::simulation::monotonic_clock::now().nanoseconds()
              == limits.maximum_deadline().nanoseconds());
            const auto beyond = target.run_until(
              monotonic_time{limits.maximum_deadline().nanoseconds() + 1});
            BOOST_REQUIRE(!beyond.has_value());
            BOOST_CHECK(beyond.error().code() == kwaque::errc::out_of_range);
        }
        BOOST_TEST(!virtual_time_test_access::clock_is_bound());
    }
    {
        const auto limits = test_scheduler_limits();
        scheduler target{limits};
        virtual_time time{target, test_time_config(limits, wall_time{-2'000})};
        clock_binding binding{time};
        BOOST_TEST(
          kwaque::simulation::monotonic_clock::now().nanoseconds() == 0U);
        BOOST_TEST(
          kwaque::simulation::wall_clock::now().unix_nanoseconds() == -2'000);
    }
    BOOST_TEST(!virtual_time_test_access::clock_is_bound());
    co_return;
}

SEASTAR_TEST_CASE(wall_adjustments_are_bounded_ordered_integer_events) {
    const auto limits = test_scheduler_limits();
    scheduler target{limits};
    virtual_time time{target, test_time_config(limits)};
    clock_binding binding{time};

    const auto rejected = time.schedule_wall_offset(
      monotonic_time{5}, wall_offset{101});
    BOOST_REQUIRE(!rejected.has_value());
    BOOST_CHECK(rejected.error().code() == kwaque::errc::out_of_range);
    BOOST_TEST(time.pending_adjustments() == 0U);
    const auto minimum_rejected = time.schedule_wall_offset(
      monotonic_time{5}, wall_offset{std::numeric_limits<std::int64_t>::min()});
    BOOST_REQUIRE(!minimum_rejected.has_value());
    BOOST_CHECK(minimum_rejected.error().code() == kwaque::errc::out_of_range);

    const auto positive = time.schedule_wall_offset(
      monotonic_time{5}, wall_offset{100});
    BOOST_REQUIRE(positive.has_value());
    std::int64_t observed_same_time = 0;
    const auto observer = target.schedule(
      monotonic_time{5},
      event_priority::normal(),
      [&observed_same_time] noexcept {
          observed_same_time
            = kwaque::simulation::wall_clock::now().unix_nanoseconds();
      });
    BOOST_REQUIRE(observer.has_value());
    BOOST_TEST(time.pending_adjustments() == 1U);
    BOOST_TEST(
      kwaque::simulation::wall_clock::now().unix_nanoseconds() == 1'000);

    const auto first = target.run_until(monotonic_time{5});
    BOOST_REQUIRE(first.has_value());
    BOOST_TEST(*first == 2U);
    BOOST_TEST(time.pending_adjustments() == 0U);
    BOOST_TEST(observed_same_time == 1'105);
    BOOST_TEST(time.offset().nanoseconds() == 100);

    const auto negative = time.schedule_wall_offset(
      monotonic_time{6}, wall_offset{-100});
    BOOST_REQUIRE(negative.has_value());
    const auto second = target.run_until(monotonic_time{6});
    BOOST_REQUIRE(second.has_value());
    BOOST_TEST(*second == 1U);
    BOOST_TEST(kwaque::simulation::monotonic_clock::now().nanoseconds() == 6U);
    BOOST_TEST(kwaque::simulation::wall_clock::now().unix_nanoseconds() == 906);
    BOOST_TEST(906 < observed_same_time);
    const auto past = time.schedule_wall_offset(
      monotonic_time{5}, wall_offset{});
    BOOST_REQUIRE(!past.has_value());
    BOOST_CHECK(past.error().code() == kwaque::errc::invalid_argument);
    BOOST_CHECK(
      past.error().operation() == kwaque::runtime::operation_kind::clock);
    BOOST_REQUIRE(time.stop().has_value());
    BOOST_CHECK_EQUAL(time.offset().nanoseconds(), -100);
    co_return;
}

SEASTAR_TEST_CASE(virtual_time_stop_cancels_pending_wall_adjustments) {
    const auto limits = test_scheduler_limits();
    scheduler target{limits};
    virtual_time time{target, test_time_config(limits)};
    clock_binding binding{time};

    BOOST_REQUIRE(time.schedule_wall_offset(monotonic_time{5}, wall_offset{17})
                    .has_value());
    BOOST_REQUIRE(time.schedule_wall_offset(monotonic_time{7}, wall_offset{-9})
                    .has_value());
    BOOST_CHECK_EQUAL(time.pending_adjustments(), 2U);
    BOOST_CHECK_EQUAL(target.pending_events(), 2U);

    BOOST_REQUIRE(time.stop().has_value());
    BOOST_REQUIRE(time.stop().has_value());
    BOOST_CHECK_EQUAL(time.pending_adjustments(), 0U);
    BOOST_CHECK_EQUAL(target.pending_events(), 0U);
    BOOST_CHECK_EQUAL(time.offset().nanoseconds(), 0);
    const auto rejected = time.schedule_wall_offset(
      monotonic_time{9}, wall_offset{1});
    BOOST_REQUIRE(!rejected.has_value());
    BOOST_CHECK(rejected.error().code() == kwaque::errc::closed);
    co_return;
}

SEASTAR_TEST_CASE(virtual_time_wall_adjustment_uses_preallocated_ownership) {
    const auto limits = test_scheduler_limits();
    scheduler target{limits};
    virtual_time time{target, test_time_config(limits)};

    std::size_t attempts = 0;
    bool scheduled = false;
    seastar::memory::with_allocation_failures([&] {
        ++attempts;
        scheduled = time
                      .schedule_wall_offset(monotonic_time{5}, wall_offset{17})
                      .has_value();
    });
    BOOST_REQUIRE(scheduled);
    BOOST_CHECK_EQUAL(attempts, 1U);
    BOOST_REQUIRE(time.stop().has_value());
    co_return;
}
