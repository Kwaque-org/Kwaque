#include "src/simulation/scheduler.h"
#include "src/simulation/scheduler_test_support.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/memory.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/later.hh>

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

namespace {

using kwaque::runtime::monotonic_time;
using kwaque::simulation::event_id;
using kwaque::simulation::event_priority;
using kwaque::simulation::scheduler;
using kwaque::simulation::scheduler_limit_values;
using kwaque::simulation::scheduler_limits;
using kwaque::simulation::scheduler_test_access;

static_assert(std::is_trivially_copyable_v<event_id>);
static_assert(std::is_trivially_copyable_v<event_priority>);
static_assert(!std::is_convertible_v<std::uint64_t, event_id>);
static_assert(!std::is_convertible_v<std::uint8_t, event_priority>);
static_assert(!std::is_copy_constructible_v<scheduler>);
static_assert(!std::is_move_constructible_v<scheduler>);

[[nodiscard]] std::optional<std::uint64_t> context_value(
  const kwaque::runtime::operation_error& error,
  kwaque::runtime::operation_context_key key) {
    for (std::size_t index = 0; index < error.context_size(); ++index) {
        const auto field = error.context_at(index);
        if (field && field->key == key) {
            return field->value;
        }
    }
    return std::nullopt;
}

[[nodiscard]] bool cancel_event(scheduler& target, event_id id) {
    const auto canceled = target.cancel(id);
    BOOST_REQUIRE(canceled.has_value());
    return *canceled;
}

class scheduled_completion final {
public:
    seastar::future<kwaque::runtime::result<void>> submit(
      scheduler& target,
      monotonic_time deadline,
      event_priority priority = event_priority::normal()) {
        auto waiting = completion_.get_future();
        auto scheduled = target.schedule(deadline, priority, [this] noexcept {
            completion_.set_value(kwaque::runtime::result<void>{});
        });
        if (!scheduled) {
            completion_.set_value(kwaque::runtime::failure(scheduled.error()));
        }
        return waiting;
    }

private:
    seastar::promise<kwaque::runtime::result<void>> completion_;
};

class same_time_rescheduler final {
public:
    explicit same_time_rescheduler(scheduler& target)
      : target_(&target) {}

    void start() noexcept { post(); }

    [[nodiscard]] std::uint64_t executions() const noexcept {
        return executions_;
    }
    [[nodiscard]] event_id pending() const noexcept { return pending_; }

private:
    void post() noexcept {
        auto scheduled = target_->schedule(
          target_->now(), event_priority::normal(), [this] noexcept {
              ++executions_;
              post();
          });
        if (scheduled) {
            pending_ = *scheduled;
        }
    }

    scheduler* target_;
    event_id pending_;
    std::uint64_t executions_{0};
};

struct order_case final {
    std::uint64_t deadline;
    std::uint8_t priority;
    std::uint64_t marker;
    event_id id;
};

[[nodiscard]] std::vector<std::uint64_t>
run_noise_scenario(std::size_t noise_size) {
    std::vector<std::uint64_t> noise(noise_size, noise_size);
    BOOST_TEST(noise.size() == noise_size);

    scheduler target{scheduler_limits::defaults()};
    std::vector<std::uint64_t> transcript;
    const std::array cases{
      order_case{7, 2, 70, {}},
      order_case{3, 9, 39, {}},
      order_case{3, 1, 31, {}},
      order_case{3, 1, 32, {}},
    };
    transcript.reserve(cases.size() * 2);
    for (const auto& item : cases) {
        auto scheduled = target.schedule(
          monotonic_time{item.deadline},
          event_priority{item.priority},
          [&transcript, marker = item.marker] noexcept {
              transcript.push_back(marker);
          });
        BOOST_REQUIRE(scheduled.has_value());
        transcript.push_back(scheduled->value());
    }
    const auto executed = target.run_until(monotonic_time{7});
    BOOST_REQUIRE(executed.has_value());
    BOOST_TEST(*executed == cases.size());
    return transcript;
}

} // namespace

SEASTAR_TEST_CASE(scheduler_limits_validate_every_local_dimension) {
    const auto defaults = scheduler_limits::make(scheduler_limit_values{});
    BOOST_REQUIRE(defaults.has_value());
    BOOST_TEST(defaults->pending_events() == 65'536U);
    BOOST_TEST(defaults->events_per_pump() == 1'024U);
    BOOST_TEST(defaults->total_events() == 100'000U);

    const auto absolute = scheduler_limits::make(
      scheduler_limit_values{
        .pending_events = scheduler_limits::pending_events_absolute,
        .events_per_pump = scheduler_limits::events_per_pump_absolute,
        .total_events = scheduler_limits::total_events_absolute,
        .maximum_deadline = scheduler_limits::maximum_deadline_absolute,
      });
    BOOST_REQUIRE(absolute.has_value());

    const auto expect_error =
      [](scheduler_limit_values values, kwaque::errc code) {
          const auto outcome = scheduler_limits::make(values);
          BOOST_REQUIRE(!outcome.has_value());
          BOOST_CHECK(outcome.error().code() == code);
      };

    auto zero_pending = scheduler_limit_values{};
    zero_pending.pending_events = 0;
    expect_error(zero_pending, kwaque::errc::invalid_argument);

    auto zero_per_pump = scheduler_limit_values{};
    zero_per_pump.events_per_pump = 0;
    expect_error(zero_per_pump, kwaque::errc::invalid_argument);

    auto zero_total = scheduler_limit_values{};
    zero_total.total_events = 0;
    expect_error(zero_total, kwaque::errc::invalid_argument);

    auto zero_deadline = scheduler_limit_values{};
    zero_deadline.maximum_deadline = monotonic_time{};
    expect_error(zero_deadline, kwaque::errc::invalid_argument);

    auto incoherent = scheduler_limit_values{};
    incoherent.events_per_pump = 11;
    incoherent.total_events = 10;
    expect_error(incoherent, kwaque::errc::invalid_argument);

    auto oversized_pending = scheduler_limit_values{};
    oversized_pending.pending_events = scheduler_limits::pending_events_absolute
                                       + 1;
    expect_error(oversized_pending, kwaque::errc::out_of_range);

    auto oversized_per_pump = scheduler_limit_values{};
    oversized_per_pump.events_per_pump
      = scheduler_limits::events_per_pump_absolute + 1;
    oversized_per_pump.total_events = oversized_per_pump.events_per_pump;
    expect_error(oversized_per_pump, kwaque::errc::out_of_range);

    auto oversized_total = scheduler_limit_values{};
    oversized_total.total_events = scheduler_limits::total_events_absolute + 1;
    expect_error(oversized_total, kwaque::errc::out_of_range);

    auto oversized_deadline = scheduler_limit_values{};
    oversized_deadline.maximum_deadline = monotonic_time{
      scheduler_limits::maximum_deadline_absolute.nanoseconds() + 1};
    expect_error(oversized_deadline, kwaque::errc::out_of_range);

    co_return;
}

SEASTAR_TEST_CASE(scheduler_orders_by_deadline_priority_and_event_id) {
    scheduler target{scheduler_limits::defaults()};
    std::vector<std::uint64_t> observed;
    observed.reserve(4);

    const auto later = target.schedule(
      monotonic_time{20}, event_priority{2}, [&observed] noexcept {
          observed.push_back(20);
      });
    const auto normal = target.schedule(
      monotonic_time{10}, event_priority{9}, [&observed] noexcept {
          observed.push_back(109);
      });
    const auto first_high = target.schedule(
      monotonic_time{10}, event_priority{1}, [&observed] noexcept {
          observed.push_back(101);
      });
    const auto second_high = target.schedule(
      monotonic_time{10}, event_priority{1}, [&observed] noexcept {
          observed.push_back(102);
      });
    BOOST_REQUIRE(later && normal && first_high && second_high);
    BOOST_TEST(later->value() == 1U);
    BOOST_TEST(normal->value() == 2U);
    BOOST_TEST(first_high->value() == 3U);
    BOOST_TEST(second_high->value() == 4U);

    const auto through_ten = target.run_until(monotonic_time{10});
    BOOST_REQUIRE(through_ten.has_value());
    BOOST_TEST(*through_ten == 3U);
    const std::vector<std::uint64_t> expected_at_ten{101, 102, 109};
    BOOST_TEST(observed == expected_at_ten, boost::test_tools::per_element());

    const auto through_twenty = target.run_until(monotonic_time{20});
    BOOST_REQUIRE(through_twenty.has_value());
    BOOST_TEST(*through_twenty == 1U);
    const std::vector<std::uint64_t> expected{101, 102, 109, 20};
    BOOST_TEST(observed == expected, boost::test_tools::per_element());
    co_return;
}

SEASTAR_TEST_CASE(scheduler_ready_batches_preserve_order_for_explicit_yields) {
    scheduler target{scheduler_limits::defaults()};
    std::vector<std::uint64_t> observed;
    for (std::uint64_t marker = 0; marker < 5; ++marker) {
        BOOST_REQUIRE(
          target
            .schedule(
              monotonic_time{},
              event_priority::normal(),
              [&observed, marker] noexcept { observed.push_back(marker); })
            .has_value());
    }

    const auto invalid = target.run_ready_batch(0);
    BOOST_REQUIRE(!invalid.has_value());
    BOOST_CHECK(invalid.error().code() == kwaque::errc::invalid_argument);
    const auto first = target.run_ready_batch(2);
    BOOST_REQUIRE(first.has_value());
    BOOST_TEST(*first == 2U);
    BOOST_TEST(target.has_ready_events());
    co_await seastar::yield();
    const auto second = target.run_ready_batch(2);
    BOOST_REQUIRE(second.has_value());
    BOOST_TEST(*second == 2U);
    BOOST_TEST(target.has_ready_events());
    co_await seastar::yield();
    const auto last = target.run_ready_batch(2);
    BOOST_REQUIRE(last.has_value());
    BOOST_TEST(*last == 1U);
    BOOST_TEST(!target.has_ready_events());
    const std::vector<std::uint64_t> expected{0, 1, 2, 3, 4};
    BOOST_TEST(observed == expected, boost::test_tools::per_element());
}

SEASTAR_TEST_CASE(scheduler_time_batches_stop_before_unexecuted_deadlines) {
    scheduler target{scheduler_limits::defaults()};
    std::vector<std::uint64_t> observed;
    for (const auto deadline : {5U, 10U, 15U}) {
        BOOST_REQUIRE(
          target
            .schedule(
              monotonic_time{deadline},
              event_priority::normal(),
              [&observed, deadline] noexcept { observed.push_back(deadline); })
            .has_value());
    }

    for (const auto expected_time : {5U, 10U}) {
        const auto batch = target.run_until_batch(monotonic_time{20}, 1);
        BOOST_REQUIRE(batch.has_value());
        BOOST_TEST(*batch == 1U);
        BOOST_TEST(target.now().nanoseconds() == expected_time);
        co_await seastar::yield();
    }
    const auto final_batch = target.run_until_batch(monotonic_time{20}, 1);
    BOOST_REQUIRE(final_batch.has_value());
    BOOST_TEST(*final_batch == 1U);
    BOOST_TEST(target.now().nanoseconds() == 20U);
    const std::vector<std::uint64_t> expected{5, 10, 15};
    BOOST_TEST(observed == expected, boost::test_tools::per_element());
}

SEASTAR_TEST_CASE(scheduler_cancellation_repairs_every_heap_position) {
    scheduler target{scheduler_limits::defaults()};
    std::vector<std::uint64_t> observed;
    observed.reserve(9);
    std::array<event_id, 9> ids{};
    for (std::uint64_t marker = 0; marker < ids.size(); ++marker) {
        auto scheduled = target.schedule(
          monotonic_time{10 + ((marker * 7) % 5)},
          event_priority{static_cast<std::uint8_t>((marker * 3) % 7)},
          [&observed, marker] noexcept { observed.push_back(marker); });
        BOOST_REQUIRE(scheduled.has_value());
        ids[marker] = *scheduled;
    }

    const auto root = scheduler_test_access::event_at(target, 0);
    const auto middle = scheduler_test_access::event_at(
      target, target.pending_events() / 2);
    const auto tail = scheduler_test_access::event_at(
      target, target.pending_events() - 1);
    BOOST_REQUIRE(root && middle && tail);
    BOOST_REQUIRE(*root != *middle);
    BOOST_REQUIRE(*root != *tail);
    BOOST_REQUIRE(*middle != *tail);
    BOOST_TEST(cancel_event(target, *root));
    BOOST_TEST(cancel_event(target, *middle));
    BOOST_TEST(cancel_event(target, *tail));
    BOOST_TEST(!cancel_event(target, *root));

    const auto drained = target.run_until(monotonic_time{20});
    BOOST_REQUIRE(drained.has_value());
    BOOST_TEST(*drained == 6U);
    BOOST_TEST(target.pending_events() == 0U);

    std::vector<order_case> expected;
    for (std::uint64_t marker = 0; marker < ids.size(); ++marker) {
        if (
          ids[marker] == *root || ids[marker] == *middle
          || ids[marker] == *tail) {
            continue;
        }
        expected.push_back(
          order_case{
            .deadline = 10 + ((marker * 7) % 5),
            .priority = static_cast<std::uint8_t>((marker * 3) % 7),
            .marker = marker,
            .id = ids[marker],
          });
    }
    std::ranges::sort(
      expected, [](const order_case& left, const order_case& right) {
          if (left.deadline != right.deadline) {
              return left.deadline < right.deadline;
          }
          if (left.priority != right.priority) {
              return left.priority < right.priority;
          }
          return left.id < right.id;
      });
    std::vector<std::uint64_t> expected_markers;
    for (const auto& item : expected) {
        expected_markers.push_back(item.marker);
    }
    BOOST_TEST(observed == expected_markers, boost::test_tools::per_element());
    co_return;
}

SEASTAR_TEST_CASE(scheduler_chunked_storage_preserves_order_across_fragments) {
    constexpr std::uint32_t event_count{8'193};
    const auto limits = scheduler_limits::make(
      scheduler_limit_values{
        .pending_events = event_count,
        .events_per_pump = event_count,
        .total_events = event_count,
        .maximum_deadline = monotonic_time{1},
      });
    BOOST_REQUIRE(limits.has_value());
    scheduler target{*limits};

    std::vector<std::uint64_t> observed;
    observed.reserve(event_count);
    std::vector<event_id> ids;
    ids.reserve(event_count);
    for (std::uint64_t marker = 0; marker < event_count; ++marker) {
        auto scheduled = target.schedule(
          monotonic_time{0},
          event_priority::normal(),
          [&observed, marker] noexcept { observed.push_back(marker); });
        BOOST_REQUIRE(scheduled.has_value());
        ids.push_back(*scheduled);
    }

    const auto root = scheduler_test_access::event_at(target, 0);
    const auto middle = scheduler_test_access::event_at(
      target, target.pending_events() / 2U);
    const auto tail = scheduler_test_access::event_at(
      target, target.pending_events() - 1U);
    BOOST_REQUIRE(root && middle && tail);
    BOOST_TEST(cancel_event(target, *root));
    BOOST_TEST(cancel_event(target, *middle));
    BOOST_TEST(cancel_event(target, *tail));

    std::uint64_t drained = 0;
    while (target.has_ready_events()) {
        const auto batch = target.run_ready_batch(256);
        BOOST_REQUIRE(batch.has_value());
        drained += *batch;
        co_await seastar::yield();
    }
    BOOST_TEST(drained == event_count - 3U);
    BOOST_TEST(target.pending_events() == 0U);

    std::size_t observed_index = 0;
    for (std::uint64_t marker = 0; marker < event_count; ++marker) {
        if (
          ids[marker] == *root || ids[marker] == *middle
          || ids[marker] == *tail) {
            continue;
        }
        BOOST_REQUIRE(observed_index < observed.size());
        BOOST_TEST(observed[observed_index] == marker);
        ++observed_index;
    }
    BOOST_TEST(observed_index == observed.size());
    co_return;
}

SEASTAR_TEST_CASE(scheduler_control_apis_have_exact_time_and_reentrancy) {
    scheduler target{scheduler_limits::defaults()};
    std::vector<std::uint64_t> observed;
    observed.reserve(3);
    std::array<kwaque::errc, 4> nested_errors{};
    event_id cancel_target;

    auto first = target.schedule(
      monotonic_time{5}, event_priority::normal(), [&] noexcept {
          observed.push_back(1);
          const auto nested_step = target.step();
          const auto nested_ready = target.run_ready();
          const auto nested_advance = target.advance_to_next();
          const auto nested_until = target.run_until(target.now());
          nested_errors[0] = nested_step ? kwaque::errc::success
                                         : nested_step.error().code();
          nested_errors[1] = nested_ready ? kwaque::errc::success
                                          : nested_ready.error().code();
          nested_errors[2] = nested_advance ? kwaque::errc::success
                                            : nested_advance.error().code();
          nested_errors[3] = nested_until ? kwaque::errc::success
                                          : nested_until.error().code();
          const auto inserted = target.schedule(
            target.now(), event_priority::highest(), [&observed] noexcept {
                observed.push_back(2);
            });
          if (!inserted) {
              nested_errors[0] = inserted.error().code();
          }
          const auto canceled = target.cancel(cancel_target);
          if (!canceled || !*canceled) {
              nested_errors[0] = kwaque::errc::invalid_argument;
          }
      });
    auto last = target.schedule(
      monotonic_time{5}, event_priority::lowest(), [&observed] noexcept {
          observed.push_back(3);
      });
    auto canceled = target.schedule(
      monotonic_time{5}, event_priority::lowest(), [&observed] noexcept {
          observed.push_back(4);
      });
    BOOST_REQUIRE(first && last && canceled);
    cancel_target = *canceled;

    const auto before_deadline = target.step();
    BOOST_REQUIRE(before_deadline.has_value());
    BOOST_TEST(!*before_deadline);
    BOOST_TEST(target.now().nanoseconds() == 0U);

    const auto advanced = target.advance_to_next();
    BOOST_REQUIRE(advanced && *advanced);
    BOOST_TEST((*advanced)->nanoseconds() == 5U);
    const auto blocked_advance = target.advance_to_next();
    BOOST_REQUIRE(!blocked_advance.has_value());
    BOOST_CHECK(blocked_advance.error().code() == kwaque::errc::unavailable);

    const auto ready = target.run_ready();
    BOOST_REQUIRE(ready.has_value());
    BOOST_TEST(*ready == 3U);
    for (const auto error : nested_errors) {
        BOOST_CHECK(error == kwaque::errc::unavailable);
    }
    const std::vector<std::uint64_t> expected{1, 2, 3};
    BOOST_TEST(observed == expected, boost::test_tools::per_element());
    BOOST_TEST(target.now().nanoseconds() == 5U);

    const auto backwards = target.run_until(monotonic_time{4});
    BOOST_REQUIRE(!backwards.has_value());
    BOOST_CHECK(backwards.error().code() == kwaque::errc::invalid_argument);
    BOOST_TEST(target.now().nanoseconds() == 5U);

    const auto to_twenty = target.run_until(monotonic_time{20});
    BOOST_REQUIRE(to_twenty.has_value());
    BOOST_TEST(*to_twenty == 0U);
    BOOST_TEST(target.now().nanoseconds() == 20U);
    const auto past = target.schedule(
      monotonic_time{19}, event_priority::normal(), [] noexcept {});
    BOOST_REQUIRE(!past.has_value());
    BOOST_CHECK(past.error().code() == kwaque::errc::invalid_argument);
    const auto empty_callback = target.schedule(
      monotonic_time{20}, event_priority::normal(), scheduler::callback{});
    BOOST_REQUIRE(!empty_callback.has_value());
    BOOST_CHECK(
      empty_callback.error().code() == kwaque::errc::invalid_argument);
    const auto empty = target.advance_to_next();
    BOOST_REQUIRE(empty.has_value());
    BOOST_TEST(!*empty);
    co_return;
}

SEASTAR_TEST_CASE(
  scheduler_enforces_pending_deadline_and_id_bounds_before_mutation) {
    const auto limits = scheduler_limits::make(
      scheduler_limit_values{
        .pending_events = 1,
        .events_per_pump = 4,
        .total_events = 4,
        .maximum_deadline = monotonic_time{10},
      });
    BOOST_REQUIRE(limits.has_value());
    scheduler target{*limits};

    const auto first = target.schedule(
      monotonic_time{10}, event_priority::normal(), [] noexcept {});
    BOOST_REQUIRE(first.has_value());
    scheduler::callback retained_callback{[] noexcept {}};
    const auto full = target.schedule(
      monotonic_time{10},
      event_priority::normal(),
      std::move(retained_callback));
    BOOST_REQUIRE(!full.has_value());
    BOOST_CHECK(full.error().code() == kwaque::errc::queue_full);
    // Rejected admission is specified to leave the rvalue callback untouched.
    // NOLINTNEXTLINE(bugprone-use-after-move)
    BOOST_TEST(static_cast<bool>(retained_callback));
    const auto full_limit = context_value(
      full.error(), kwaque::runtime::operation_context_key::limit);
    BOOST_REQUIRE(full_limit.has_value());
    BOOST_TEST(*full_limit == 1U);
    BOOST_TEST(cancel_event(target, *first));

    const auto too_late = target.schedule(
      monotonic_time{11}, event_priority::normal(), [] noexcept {});
    BOOST_REQUIRE(!too_late.has_value());
    BOOST_CHECK(too_late.error().code() == kwaque::errc::out_of_range);

    const auto second = target.schedule(
      monotonic_time{10}, event_priority::normal(), [] noexcept {});
    BOOST_REQUIRE(second.has_value());
    BOOST_TEST(second->value() == first->value() + 1);
    BOOST_TEST(cancel_event(target, *second));

    scheduler_test_access::use_final_event_id(target);
    const auto maximum = target.schedule(
      monotonic_time{10}, event_priority::normal(), [] noexcept {});
    BOOST_REQUIRE(maximum.has_value());
    BOOST_TEST(maximum->value() == std::numeric_limits<std::uint64_t>::max());
    BOOST_TEST(cancel_event(target, *maximum));
    const auto exhausted = target.schedule(
      monotonic_time{10}, event_priority::normal(), [] noexcept {});
    BOOST_REQUIRE(!exhausted.has_value());
    BOOST_CHECK(exhausted.error().code() == kwaque::errc::out_of_range);

    scheduler reserved_target{*limits};
    scheduler_test_access::use_final_event_id(reserved_target);
    auto reservation = reserved_target.reserve_event_id();
    BOOST_REQUIRE(reservation.has_value());
    const auto protected_id = reserved_target.schedule(
      monotonic_time{10}, event_priority::normal(), [] noexcept {});
    BOOST_REQUIRE(!protected_id.has_value());
    BOOST_CHECK(protected_id.error().code() == kwaque::errc::out_of_range);
    reservation->release();
    const auto final_id = reserved_target.schedule(
      monotonic_time{10}, event_priority::normal(), [] noexcept {});
    BOOST_REQUIRE(final_id.has_value());
    BOOST_TEST(final_id->value() == std::numeric_limits<std::uint64_t>::max());
    BOOST_TEST(cancel_event(reserved_target, *final_id));
    co_return;
}

SEASTAR_TEST_CASE(scheduler_stops_exactly_at_per_pump_and_lifetime_budgets) {
    const auto limits = scheduler_limits::make(
      scheduler_limit_values{
        .pending_events = 4,
        .events_per_pump = 2,
        .total_events = 3,
        .maximum_deadline = monotonic_time{100},
      });
    BOOST_REQUIRE(limits.has_value());
    scheduler target{*limits};
    same_time_rescheduler repeating{target};
    repeating.start();

    const auto first = target.run_ready();
    BOOST_REQUIRE(!first.has_value());
    BOOST_CHECK(first.error().code() == kwaque::errc::resource_exhausted);
    BOOST_TEST(repeating.executions() == 2U);
    BOOST_TEST(target.executed_events() == 2U);
    BOOST_TEST(target.pending_events() == 1U);
    const auto first_limit = context_value(
      first.error(), kwaque::runtime::operation_context_key::limit);
    const auto first_sequence = context_value(
      first.error(), kwaque::runtime::operation_context_key::sequence);
    const auto first_items = context_value(
      first.error(), kwaque::runtime::operation_context_key::items);
    const auto first_deadline = context_value(
      first.error(), kwaque::runtime::operation_context_key::deadline_ns);
    BOOST_REQUIRE(first_limit.has_value());
    BOOST_REQUIRE(first_sequence.has_value());
    BOOST_REQUIRE(first_items.has_value());
    BOOST_REQUIRE(first_deadline.has_value());
    BOOST_TEST(*first_limit == 2U);
    BOOST_TEST(*first_sequence == repeating.pending().value());
    BOOST_TEST(*first_items == 2U);
    BOOST_TEST(*first_deadline == 0U);

    const auto second = target.run_ready();
    BOOST_REQUIRE(!second.has_value());
    BOOST_CHECK(second.error().code() == kwaque::errc::resource_exhausted);
    BOOST_TEST(repeating.executions() == 3U);
    BOOST_TEST(target.executed_events() == 3U);
    BOOST_TEST(target.pending_events() == 1U);
    const auto second_limit = context_value(
      second.error(), kwaque::runtime::operation_context_key::limit);
    const auto second_sequence = context_value(
      second.error(), kwaque::runtime::operation_context_key::sequence);
    const auto second_items = context_value(
      second.error(), kwaque::runtime::operation_context_key::items);
    BOOST_REQUIRE(second_limit.has_value());
    BOOST_REQUIRE(second_sequence.has_value());
    BOOST_REQUIRE(second_items.has_value());
    BOOST_TEST(*second_limit == 3U);
    BOOST_TEST(*second_sequence == repeating.pending().value());
    BOOST_TEST(*second_items == 3U);
    BOOST_TEST(cancel_event(target, repeating.pending()));
    co_return;
}

SEASTAR_TEST_CASE(scheduler_run_until_retains_first_event_beyond_pump_budget) {
    const auto limits = scheduler_limits::make(
      scheduler_limit_values{
        .pending_events = 4,
        .events_per_pump = 1,
        .total_events = 4,
        .maximum_deadline = monotonic_time{100},
      });
    BOOST_REQUIRE(limits.has_value());
    scheduler target{*limits};
    std::uint64_t completions = 0;
    const auto first = target.schedule(
      monotonic_time{5}, event_priority::normal(), [&completions] noexcept {
          ++completions;
      });
    const auto second = target.schedule(
      monotonic_time{10}, event_priority::normal(), [&completions] noexcept {
          ++completions;
      });
    BOOST_REQUIRE(first && second);

    const auto outcome = target.run_until(monotonic_time{20});
    BOOST_REQUIRE(!outcome.has_value());
    BOOST_CHECK(outcome.error().code() == kwaque::errc::resource_exhausted);
    BOOST_TEST(completions == 1U);
    BOOST_TEST(target.now().nanoseconds() == 5U);
    BOOST_TEST(target.pending_events() == 1U);
    BOOST_TEST(cancel_event(target, *second));
    co_return;
}

SEASTAR_TEST_CASE(
  scheduler_execution_is_independent_of_allocator_and_hash_table_noise) {
    const auto expected = run_noise_scenario(1);
    for (std::size_t repetition = 1; repetition <= 100; ++repetition) {
        const auto actual = run_noise_scenario(repetition * 4097);
        BOOST_TEST(actual == expected, boost::test_tools::per_element());
    }
    co_return;
}

SEASTAR_TEST_CASE(
  scheduler_rejects_invalid_descriptors_before_callback_ownership) {
    scheduler target{scheduler_limits::defaults()};
    scheduler::callback retained{[] noexcept {}};
    const auto invalid_effect = target.schedule(
      monotonic_time{},
      event_priority::normal(),
      std::move(retained),
      kwaque::simulation::trace_event_descriptor{
        .kind = kwaque::simulation::trace_event_kind::generic,
        .effect = kwaque::simulation::trace_action::selected,
      });
    BOOST_REQUIRE(!invalid_effect.has_value());
    BOOST_CHECK(
      invalid_effect.error().code() == kwaque::errc::invalid_argument);
    // Descriptor rejection occurs before callback ownership is transferred.
    // NOLINTNEXTLINE(bugprone-use-after-move)
    BOOST_TEST(static_cast<bool>(retained));
    BOOST_TEST(target.pending_events() == 0U);

    const auto invalid_kind = target.schedule(
      monotonic_time{},
      event_priority::normal(),
      [] noexcept {},
      kwaque::simulation::trace_event_descriptor{
        .kind = kwaque::simulation::trace_event_kind::keyed_random,
      });
    BOOST_REQUIRE(!invalid_kind.has_value());
    BOOST_CHECK(invalid_kind.error().code() == kwaque::errc::invalid_argument);
    BOOST_TEST(target.pending_events() == 0U);
    co_return;
}

SEASTAR_TEST_CASE(
  scheduler_small_callback_enqueue_cancel_and_step_do_not_allocate) {
    const auto limits = scheduler_limits::make(
      scheduler_limit_values{
        .pending_events = 8,
        .events_per_pump = 8,
        .total_events = 8,
        .maximum_deadline = monotonic_time{10},
      });
    BOOST_REQUIRE(limits.has_value());
    scheduler target{*limits};

    const auto before_enqueue = seastar::memory::stats().mallocs();
    const auto canceled = target.schedule(
      monotonic_time{0}, event_priority::normal(), [] noexcept {});
    const auto after_enqueue = seastar::memory::stats().mallocs();
    BOOST_REQUIRE(canceled.has_value());
    BOOST_TEST(after_enqueue == before_enqueue);

    const auto before_cancel = seastar::memory::stats().mallocs();
    const auto canceled_outcome = target.cancel(*canceled);
    const auto after_cancel = seastar::memory::stats().mallocs();
    BOOST_REQUIRE(canceled_outcome.has_value());
    BOOST_TEST(*canceled_outcome);
    BOOST_TEST(after_cancel == before_cancel);

    bool churn_succeeded = true;
    const auto before_churn = seastar::memory::stats().mallocs();
    for (std::size_t index = 0; index < 4096; ++index) {
        const auto churned = target.schedule(
          monotonic_time{0}, event_priority::normal(), [] noexcept {});
        const auto canceled_churn = churned
                                      ? target.cancel(*churned)
                                      : kwaque::runtime::result<bool>{false};
        if (!churned || !canceled_churn || !*canceled_churn) {
            churn_succeeded = false;
            break;
        }
    }
    const auto after_churn = seastar::memory::stats().mallocs();
    BOOST_TEST(churn_succeeded);
    BOOST_TEST(after_churn == before_churn);

    const auto executed = target.schedule(
      monotonic_time{0}, event_priority::normal(), [] noexcept {});
    BOOST_REQUIRE(executed.has_value());
    const auto before_step = seastar::memory::stats().mallocs();
    const auto stepped = target.step();
    const auto after_step = seastar::memory::stats().mallocs();
    BOOST_REQUIRE(stepped.has_value());
    BOOST_TEST(*stepped);
    BOOST_TEST(after_step == before_step);
    co_return;
}

SEASTAR_TEST_CASE(scheduler_acceptance_is_asynchronous_but_rejection_is_ready) {
    scheduler accepted_scheduler{scheduler_limits::defaults()};
    scheduled_completion accepted;
    auto waiting = accepted.submit(accepted_scheduler, monotonic_time{0});
    BOOST_TEST(!waiting.available());
    const auto pumped = accepted_scheduler.run_ready();
    BOOST_REQUIRE(pumped.has_value());
    BOOST_TEST(*pumped == 1U);
    BOOST_TEST(waiting.available());
    const auto accepted_outcome = co_await std::move(waiting);
    BOOST_REQUIRE(accepted_outcome.has_value());

    const auto limits = scheduler_limits::make(
      scheduler_limit_values{
        .pending_events = 1,
        .events_per_pump = 2,
        .total_events = 2,
        .maximum_deadline = monotonic_time{10},
      });
    BOOST_REQUIRE(limits.has_value());
    scheduler rejecting_scheduler{*limits};
    const auto blocker = rejecting_scheduler.schedule(
      monotonic_time{10}, event_priority::normal(), [] noexcept {});
    BOOST_REQUIRE(blocker.has_value());

    scheduled_completion rejected;
    auto rejected_wait = rejected.submit(
      rejecting_scheduler, monotonic_time{10});
    BOOST_TEST(rejected_wait.available());
    const auto rejected_outcome = co_await std::move(rejected_wait);
    BOOST_REQUIRE(!rejected_outcome.has_value());
    BOOST_CHECK(rejected_outcome.error().code() == kwaque::errc::queue_full);
    BOOST_TEST(cancel_event(rejecting_scheduler, *blocker));
}
