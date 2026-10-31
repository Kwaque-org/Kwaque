#include "src/runtime/alarm.h"
#include "src/runtime/deadline_wheel.h"
#include "src/runtime/deadlines.h"
#include "src/runtime/time.h"
#include "src/runtime/waiting.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/util/later.hh>

#include <boost/intrusive/list.hpp>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <optional>
#include <utility>
#include <vector>

namespace {

namespace runtime = kwaque::runtime;
using kwaque::errc;
using runtime::deadline_wheel;
using runtime::monotonic_time;
using runtime::unit_queue;

constexpr std::uint64_t millisecond = 1'000'000;

monotonic_time current{};

struct manual_clock final {
    [[nodiscard]] static monotonic_time now() noexcept { return current; }
};

class manual_alarm final {
public:
    explicit manual_alarm(runtime::alarm_callback callback) noexcept
      : callback_(std::move(callback)) {}
    void arm(monotonic_time at) noexcept { armed_at = at; }
    void cancel() noexcept { armed_at.reset(); }
    // Rings every time it is armed for until `until`, as the reactor would.
    void ring_until(monotonic_time until) noexcept {
        while (armed_at && *armed_at <= until) {
            current = std::max(current, *armed_at);
            armed_at.reset();
            callback_();
        }
        current = std::max(current, until);
    }
    // Rings once, as the reactor does when the armed time comes.
    void ring() noexcept {
        armed_at.reset();
        callback_();
    }
    std::optional<monotonic_time> armed_at;

private:
    runtime::alarm_callback callback_;
};

using test_deadlines = runtime::basic_deadlines<manual_alarm, manual_clock>;

using value_waiter = runtime::waiter<int>;
using waiter_list = boost::intrusive::
  list<value_waiter, boost::intrusive::constant_time_size<false>>;

static_assert(!std::is_copy_constructible_v<value_waiter>);
static_assert(!std::is_move_constructible_v<value_waiter>);
static_assert(sizeof(value_waiter) <= 256);

TEST(WaitingTest, DeadlineAndAbortExceptionsAreMadeOncePerShard) {
    EXPECT_EQ(&runtime::deadline_exceeded(), &runtime::deadline_exceeded());
    EXPECT_NE(runtime::deadline_exceeded(), runtime::operation_aborted());
    EXPECT_THROW(
      std::rethrow_exception(runtime::deadline_exceeded()),
      seastar::timed_out_error);
}

TEST(WaitingTest, ADeadlineSourceReportsATimeoutAndAPlainAbortAnAbort) {
    runtime::deadline_abort_source expired;
    expired.expire();
    EXPECT_EQ(runtime::ended_by(expired).code(), errc::timed_out);

    runtime::deadline_abort_source aborted;
    aborted.request_abort();
    EXPECT_EQ(runtime::ended_by(aborted).code(), errc::aborted);
    EXPECT_EQ(
      aborted.abort_requested_exception_ptr(), runtime::operation_aborted());
}

enum class event : std::uint8_t { data, deadline, abort, close };

errc expected_code(event first) {
    switch (first) {
    case event::data:
        return errc::success;
    case event::deadline:
        return errc::timed_out;
    case event::abort:
        return errc::aborted;
    case event::close:
        return errc::closed;
    }
    return errc::success;
}

// Every order of the four ways a wait can end, within one task and with a
// task switch between each: exactly one outcome, the first teller's.
TEST(WaitingTest, AWaiterIsToldOnceByWhoeverTellsFirst) {
    std::array order{event::data, event::deadline, event::abort, event::close};
    std::sort(order.begin(), order.end());
    do {
        for (const bool switch_tasks : {false, true}) {
            current = monotonic_time{1000 * millisecond};
            test_deadlines deadlines{8};
            waiter_list owner;
            seastar::abort_source abort;
            value_waiter waiting;
            auto told = waiting.enlist(
              owner, &abort, &deadlines, monotonic_time{1010 * millisecond});
            ASSERT_FALSE(told.available());
            for (const auto step : order) {
                switch (step) {
                case event::data:
                    if (!owner.empty()) {
                        static_cast<void>(owner.front().tell(7));
                    }
                    break;
                case event::deadline:
                    deadlines.alarm().ring_until(
                      monotonic_time{1020 * millisecond});
                    break;
                case event::abort:
                    abort.request_abort();
                    break;
                case event::close:
                    while (!owner.empty()) {
                        static_cast<void>(owner.front().tell(
                          runtime::failure(
                            runtime::operation_error{
                              errc::closed,
                              runtime::operation_kind::runtime})));
                    }
                    break;
                }
                if (switch_tasks) {
                    seastar::yield().get();
                }
            }
            ASSERT_TRUE(told.available());
            const auto outcome = told.get();
            const auto expected = expected_code(order.front());
            if (expected == errc::success) {
                ASSERT_TRUE(outcome.has_value());
                EXPECT_EQ(*outcome, 7);
            } else {
                ASSERT_FALSE(outcome.has_value());
                EXPECT_EQ(outcome.error().code(), expected);
            }
            EXPECT_TRUE(owner.empty());
            EXPECT_EQ(deadlines.size(), 0U);
            EXPECT_FALSE(deadlines.armed());
        }
    } while (std::next_permutation(order.begin(), order.end()));
}

TEST(WaitingTest, EnlistingChecksTheAbortAndTheDeadlineBeforeLinking) {
    current = monotonic_time{50 * millisecond};
    test_deadlines deadlines{8};
    waiter_list owner;
    seastar::abort_source aborted;
    aborted.request_abort();
    value_waiter early;
    auto refused = early.enlist(
      owner, &aborted, &deadlines, monotonic_time{60 * millisecond});
    ASSERT_TRUE(refused.available());
    EXPECT_EQ(refused.get().error().code(), errc::aborted);
    EXPECT_TRUE(owner.empty());

    value_waiter late;
    auto expired = late.enlist(
      owner,
      static_cast<seastar::abort_source*>(nullptr),
      &deadlines,
      monotonic_time{40 * millisecond});
    ASSERT_TRUE(expired.available());
    EXPECT_EQ(expired.get().error().code(), errc::timed_out);
    EXPECT_TRUE(owner.empty());
    EXPECT_EQ(deadlines.size(), 0U);
}

TEST(WaitingTest, DataReachingADueWaiterBeforeItsDeadlineRunsWins) {
    current = monotonic_time{0};
    // One touch per ring: the rest stay collected between rings.
    test_deadlines deadlines{1};
    waiter_list owner;
    std::array<value_waiter, 4> waiting;
    std::vector<seastar::future<runtime::result<int>>> told;
    for (auto& one : waiting) {
        told.push_back(one.enlist(
          owner,
          static_cast<seastar::abort_source*>(nullptr),
          &deadlines,
          monotonic_time{5 * millisecond}));
    }
    current = monotonic_time{5 * millisecond};
    // One ring: one waiter ends, three are collected but not yet told, and
    // the alarm is armed for now to resume them.
    ASSERT_TRUE(deadlines.alarm().armed_at);
    deadlines.alarm().ring();
    std::size_t ended = 0;
    for (auto& future : told) {
        ended += future.available() ? 1U : 0U;
    }
    EXPECT_EQ(ended, 1U);
    EXPECT_EQ(deadlines.alarm().armed_at, current);
    // Data reaches whoever is still waiting first.
    for (auto& one : waiting) {
        if (one.waiting()) {
            EXPECT_TRUE(one.tell(9));
        }
    }
    deadlines.alarm().ring_until(monotonic_time{10 * millisecond});
    std::size_t data = 0;
    for (auto& future : told) {
        const auto outcome = future.get();
        if (outcome.has_value()) {
            EXPECT_EQ(*outcome, 9);
            ++data;
        } else {
            EXPECT_EQ(outcome.error().code(), errc::timed_out);
        }
    }
    EXPECT_EQ(data + ended, waiting.size());
    EXPECT_EQ(deadlines.size(), 0U);
}

TEST(WaitingTest, AnOperationDeadlineEndsEveryWaitThatFollowsIt) {
    current = monotonic_time{0};
    test_deadlines deadlines{8};
    seastar::abort_source shard_stop;
    runtime::operation_deadline operation;
    operation.follow(shard_stop);
    operation.arm(deadlines, monotonic_time{30 * millisecond});
    waiter_list owner;
    value_waiter first;
    value_waiter second;
    auto one = first.enlist(
      owner,
      &operation.source(),
      static_cast<test_deadlines*>(nullptr),
      std::nullopt);
    auto two = second.enlist(
      owner,
      &operation.source(),
      static_cast<test_deadlines*>(nullptr),
      std::nullopt);
    deadlines.alarm().ring_until(monotonic_time{31 * millisecond});
    EXPECT_EQ(one.get().error().code(), errc::timed_out);
    EXPECT_EQ(two.get().error().code(), errc::timed_out);
    operation.finish();

    runtime::operation_deadline stopped;
    stopped.follow(shard_stop);
    stopped.arm(deadlines, monotonic_time{90 * millisecond});
    value_waiter third;
    auto three = third.enlist(
      owner,
      &stopped.source(),
      static_cast<test_deadlines*>(nullptr),
      std::nullopt);
    shard_stop.request_abort();
    EXPECT_EQ(three.get().error().code(), errc::aborted);
    stopped.finish();
    EXPECT_EQ(deadlines.size(), 0U);
}

// Seastar's semaphore_test.cc test_semaphore_try_get_units.
TEST(UnitQueueTest, TryGetUnitsTakesAndReturnsUnits) {
    unit_queue queue{1};
    auto units = runtime::try_get_units(queue, 1);
    ASSERT_TRUE(units);
    EXPECT_FALSE(runtime::try_get_units(queue, 1));
    units.reset();
    EXPECT_EQ(queue.available_units(), 1U);
    units = runtime::try_get_units(queue, 1);
    ASSERT_TRUE(units);
    units->return_all();
    EXPECT_EQ(units->count(), 0U);
    EXPECT_EQ(queue.available_units(), 1U);
}

seastar::future<runtime::result<void>>
take(unit_queue& queue, std::size_t count, seastar::abort_source* abort) {
    co_return co_await queue.wait(count, abort);
}

// test_semaphore_units_abort.
TEST(UnitQueueTest, AQueuedWaitEndsOnAbort) {
    unit_queue queue{3};
    ASSERT_TRUE(queue.try_wait(3));
    seastar::abort_source abort;
    auto waiting = take(queue, 1, &abort);
    EXPECT_FALSE(waiting.available());
    abort.request_abort();
    EXPECT_EQ(waiting.get().error().code(), errc::aborted);
    queue.signal(3);
}

// test_semaphore_abort_after_wait.
TEST(UnitQueueTest, ASignalAfterAnAbortGrantsNothingToTheAbortedWait) {
    unit_queue queue{0};
    seastar::abort_source abort;
    auto waiting = take(queue, 1, &abort);
    abort.request_abort();
    queue.signal();
    EXPECT_EQ(waiting.get().error().code(), errc::aborted);
    EXPECT_EQ(queue.available_units(), 1U);
}

// test_semaphore_abort_before_wait.
TEST(UnitQueueTest, AWaitStartedAfterAnAbortEndsAtOnce) {
    unit_queue queue{0};
    seastar::abort_source abort;
    abort.request_abort();
    auto waiting = take(queue, 1, &abort);
    queue.signal();
    EXPECT_EQ(waiting.get().error().code(), errc::aborted);
    EXPECT_EQ(queue.available_units(), 1U);
}

TEST(UnitQueueTest, TryWaitFailsWhileAnyoneIsQueued) {
    unit_queue queue{1};
    auto large = take(queue, 2, nullptr);
    EXPECT_FALSE(large.available());
    EXPECT_FALSE(queue.try_wait(1));
    queue.signal(1);
    EXPECT_TRUE(large.get().has_value());
    EXPECT_EQ(queue.available_units(), 0U);
}

// A smaller wait that would fit stays behind the one queued before it. The
// grants are read from the queue after each signal: the continuations of two
// waits granted by one signal may run in either order, as the debug and
// sanitizer builds shuffle the reactor's task queue.
TEST(UnitQueueTest, GrantsGoInArrivalOrder) {
    unit_queue queue{0};
    auto first = take(queue, 2, nullptr);
    auto second = take(queue, 1, nullptr);
    queue.signal(1);
    EXPECT_EQ(queue.waiters(), 2U);
    EXPECT_EQ(queue.available_units(), 1U);
    queue.signal(1);
    EXPECT_EQ(queue.waiters(), 1U);
    EXPECT_EQ(queue.available_units(), 0U);
    EXPECT_TRUE(first.get().has_value());
    EXPECT_FALSE(second.available());
    queue.signal(1);
    EXPECT_EQ(queue.waiters(), 0U);
    EXPECT_TRUE(second.get().has_value());
}

// The native semaphore leaves smaller waits stranded behind one that gave
// up; this queue grants them at once.
TEST(UnitQueueTest, AWaitThatLeavesLetsTheSmallerOnesBehindItIn) {
    unit_queue queue{5};
    seastar::abort_source abort;
    auto large = take(queue, 10, &abort);
    auto small = take(queue, 1, nullptr);
    EXPECT_FALSE(small.available());
    abort.request_abort();
    EXPECT_EQ(large.get().error().code(), errc::aborted);
    EXPECT_TRUE(small.get().has_value());
    EXPECT_EQ(queue.available_units(), 4U);
}

TEST(UnitQueueTest, ADeadlineEndsAQueuedWait) {
    current = monotonic_time{0};
    test_deadlines deadlines{8};
    unit_queue queue{0};
    auto waiting = [&]() -> seastar::future<runtime::result<void>> {
        co_return co_await queue.wait(
          1, nullptr, deadlines, monotonic_time{20 * millisecond});
    }();
    EXPECT_FALSE(waiting.available());
    deadlines.alarm().ring_until(monotonic_time{25 * millisecond});
    EXPECT_EQ(waiting.get().error().code(), errc::timed_out);
    EXPECT_EQ(queue.waiters(), 0U);
}

// A broken queue holds no units, as a broken semaphore holds none.
TEST(UnitQueueTest, ABrokenQueueFailsEveryWaitAndIgnoresSignals) {
    unit_queue queue{1};
    auto waiting = take(queue, 2, nullptr);
    queue.broken(errc::closed);
    EXPECT_EQ(queue.available_units(), 0U);
    EXPECT_EQ(waiting.get().error().code(), errc::closed);
    auto later = take(queue, 1, nullptr);
    EXPECT_EQ(later.get().error().code(), errc::closed);
    queue.signal(5);
    EXPECT_EQ(queue.available_units(), 0U);
    EXPECT_FALSE(queue.try_wait(1));
}

// A queued wait costs nothing beyond its caller's frame: a call that waits
// allocates exactly what a call granted at once does.
TEST(UnitQueueTest, WaitingAllocatesNothingBeyondTheCallersFrame) {
#if !defined(SEASTAR_DEFAULT_ALLOCATOR)
    current = monotonic_time{0};
    test_deadlines deadlines{8};
    unit_queue queue{1};
    runtime::operation_deadline operation;
    operation.arm(deadlines, monotonic_time{1'000 * millisecond});
    // Warm every path once.
    take(queue, 1, &operation.source()).get();
    queue.signal(1);

    const auto granted_before = seastar::memory::stats().mallocs();
    auto granted = take(queue, 1, &operation.source());
    const auto granted_cost = seastar::memory::stats().mallocs()
                              - granted_before;
    EXPECT_TRUE(granted.get().has_value());

    const auto queued_before = seastar::memory::stats().mallocs();
    auto queued = take(queue, 1, &operation.source());
    queue.signal(1);
    const auto queued_cost = seastar::memory::stats().mallocs() - queued_before;
    EXPECT_TRUE(queued.get().has_value());
    EXPECT_EQ(queued_cost, granted_cost);

    // A deadline ending a queued wait allocates nothing either.
    const auto expiry_before = seastar::memory::stats().mallocs();
    auto expiring = take(queue, 1, &operation.source());
    const auto started = seastar::memory::stats().mallocs() - expiry_before;
    const auto ring_before = seastar::memory::stats().mallocs();
    deadlines.alarm().ring_until(monotonic_time{2'000 * millisecond});
    EXPECT_EQ(seastar::memory::stats().mallocs() - ring_before, 0U);
    EXPECT_EQ(started, granted_cost);
    EXPECT_EQ(expiring.get().error().code(), errc::timed_out);
    operation.finish();
    queue.signal(1);
#else
    GTEST_SKIP() << "the system allocator keeps no allocation count";
#endif
}

} // namespace
