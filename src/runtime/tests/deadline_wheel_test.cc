#include "src/runtime/deadline_wheel.h"
#include "src/runtime/deadlines.h"
#include "src/runtime/testing/contracts/contract_backends.h"
#include "src/runtime/time.h"

#include <seastar/core/memory.hh>

#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

namespace {

using kwaque::runtime::deadline_wheel;
using kwaque::runtime::monotonic_time;
using kwaque::runtime::wheel_entry;
using precision = deadline_wheel::precision;
using insertion = deadline_wheel::insertion;

constexpr std::uint64_t millisecond = 1'000'000;
constexpr std::uint64_t second = 1'000 * millisecond;

monotonic_time at_ms(std::uint64_t milliseconds) {
    return monotonic_time{milliseconds * millisecond};
}

// The time every test's wheel reads; expiry callbacks record it.
monotonic_time current{};

struct probe final : wheel_entry {
    probe() noexcept
      : wheel_entry(&on_expiry) {}
    ~probe() { static_cast<void>(cancel()); }

    static void on_expiry(wheel_entry& entry) noexcept {
        auto& self = static_cast<probe&>(entry);
        self.ended_at = current;
        ++self.endings;
        if (self.then) {
            self.then(self);
        }
    }

    std::optional<monotonic_time> ended_at;
    unsigned endings{0};
    void (*then)(probe&) noexcept {nullptr};
};

// Drives a wheel as its alarm would: to each next expiry in turn.
std::size_t run_until(deadline_wheel& wheel, monotonic_time until) {
    std::size_t touched = 0;
    for (;;) {
        const auto next = wheel.next_expiry(current);
        if (!next || *next > until) {
            current = std::max(current, until);
            touched += wheel.expire(current, SIZE_MAX);
            return touched;
        }
        current = std::max(current, *next);
        touched += wheel.expire(current, SIZE_MAX);
    }
}

TEST(DeadlineWheelTest, AddsFutureDeadlinesAndRefusesPastOnes) {
    current = at_ms(1000);
    deadline_wheel wheel{current};
    probe future;
    EXPECT_EQ(
      wheel.insert(future, at_ms(1020), precision::exact, current),
      insertion::linked);
    EXPECT_TRUE(future.linked());
    EXPECT_EQ(wheel.size(), 1U);

    probe past;
    EXPECT_EQ(
      wheel.insert(past, at_ms(999), precision::exact, current),
      insertion::due);
    EXPECT_FALSE(past.linked());

    // Within the clock's current tick: already due.
    probe same_tick;
    EXPECT_EQ(
      wheel.insert(
        same_tick,
        monotonic_time{1000 * millisecond},
        precision::exact,
        current),
      insertion::due);
    EXPECT_TRUE(wheel.cancel(future));
}

TEST(DeadlineWheelTest, NothingEndsBeforeItsTickAndItEndsOnIt) {
    current = at_ms(1000);
    deadline_wheel wheel{current};
    probe entry;
    ASSERT_EQ(
      wheel.insert(entry, at_ms(1010), precision::exact, current),
      insertion::linked);
    current = monotonic_time{1009 * millisecond + 999'999};
    static_cast<void>(wheel.expire(current, SIZE_MAX));
    EXPECT_FALSE(entry.ended_at);
    current = at_ms(1010);
    static_cast<void>(wheel.expire(current, SIZE_MAX));
    ASSERT_TRUE(entry.ended_at);
    EXPECT_EQ(*entry.ended_at, at_ms(1010));
}

TEST(DeadlineWheelTest, ExactDeadlinesEndWithinOneTickAfterAtEveryScale) {
    for (const std::uint64_t delay :
         {std::uint64_t{1} * millisecond,
          std::uint64_t{63} * millisecond,
          std::uint64_t{64} * millisecond + 1,
          std::uint64_t{500} * millisecond,
          std::uint64_t{60} * second,
          std::uint64_t{600} * second,
          std::uint64_t{3600} * second + 7}) {
        SCOPED_TRACE(delay);
        current = monotonic_time{123'456'789};
        deadline_wheel wheel{current};
        probe entry;
        const monotonic_time deadline{current.nanoseconds() + delay};
        ASSERT_EQ(
          wheel.insert(entry, deadline, precision::exact, current),
          insertion::linked);
        const auto touched = run_until(
          wheel, monotonic_time{deadline.nanoseconds() + 2 * millisecond});
        ASSERT_TRUE(entry.ended_at);
        EXPECT_GE(*entry.ended_at, deadline);
        EXPECT_LE(
          entry.ended_at->nanoseconds(), deadline.nanoseconds() + millisecond);
        EXPECT_EQ(entry.endings, 1U);
        // Once per level on the way down, plus once per horizon parked.
        const auto parks = delay / (262 * second);
        EXPECT_LE(touched, deadline_wheel::level_count + parks + 1);
    }
}

TEST(DeadlineWheelTest, AMaximumWaitEndsAtItsBucketUpToAnEighthEarly) {
    current = at_ms(10'000);
    deadline_wheel wheel{current};
    probe read;
    const auto deadline = at_ms(10'500);
    ASSERT_EQ(
      wheel.insert(read, deadline, precision::may_end_early, current),
      insertion::linked);
    static_cast<void>(run_until(wheel, deadline));
    ASSERT_TRUE(read.ended_at);
    EXPECT_GT(*read.ended_at, at_ms(10'492));
    EXPECT_LE(*read.ended_at, deadline);
}

// A wait shorter than the finest level's span rounds up like an exact one:
// only a coarser level's bucket may end it early.
TEST(DeadlineWheelTest, AShortMaximumWaitEndsNoMoreThanAnEighthEarly) {
    for (const std::uint64_t wait_us : {1'600U, 2'500U, 7'900U, 40'000U}) {
        current = monotonic_time{10'000 * millisecond + 900'000};
        deadline_wheel wheel{current};
        probe read;
        const monotonic_time deadline{current.nanoseconds() + wait_us * 1'000};
        ASSERT_EQ(
          wheel.insert(read, deadline, precision::may_end_early, current),
          insertion::linked);
        static_cast<void>(run_until(
          wheel, monotonic_time{deadline.nanoseconds() + millisecond}));
        ASSERT_TRUE(read.ended_at) << wait_us;
        EXPECT_GE(
          read.ended_at->nanoseconds(),
          deadline.nanoseconds() - wait_us * 1'000 / 8)
          << wait_us;
    }
}

// A driver that has fallen behind leaves the wheel's clock short of the
// time: a deadline already passed is still due, not linked.
TEST(DeadlineWheelTest, ADeadlinePassedWhileTheDriverLagsIsDue) {
    current = at_ms(1000);
    deadline_wheel wheel{current};
    probe pending;
    ASSERT_EQ(
      wheel.insert(pending, at_ms(1020), precision::exact, current),
      insertion::linked);
    current = at_ms(1100);
    probe late;
    EXPECT_EQ(
      wheel.insert(late, at_ms(1050), precision::exact, current),
      insertion::due);
    EXPECT_FALSE(late.linked());
    EXPECT_TRUE(wheel.cancel(pending));
}

TEST(DeadlineWheelTest, ACancelledEntryIsNeverTouchedAndClearsItsBucket) {
    current = at_ms(0);
    deadline_wheel wheel{current};
    probe cancelled;
    ASSERT_EQ(
      wheel.insert(cancelled, at_ms(40), precision::exact, current),
      insertion::linked);
    EXPECT_EQ(wheel.next_expiry(current), at_ms(40));
    EXPECT_TRUE(wheel.cancel(cancelled));
    EXPECT_FALSE(wheel.cancel(cancelled));
    EXPECT_FALSE(wheel.next_expiry(current));
    EXPECT_TRUE(wheel.empty());
    EXPECT_EQ(run_until(wheel, at_ms(100)), 0U);
    EXPECT_FALSE(cancelled.ended_at);
}

TEST(DeadlineWheelTest, ParksBeyondTheHorizonAndStillEndsExactly) {
    current = at_ms(0);
    deadline_wheel wheel{current};
    probe far;
    const monotonic_time deadline{3 * 262 * second + 17 * millisecond};
    ASSERT_EQ(
      wheel.insert(far, deadline, precision::exact, current),
      insertion::linked);
    // Its first bucket is the top level's farthest, within the horizon.
    const auto first = wheel.next_expiry(current);
    ASSERT_TRUE(first);
    EXPECT_LE(first->nanoseconds(), 263 * second);
    static_cast<void>(
      run_until(wheel, monotonic_time{deadline.nanoseconds() + millisecond}));
    ASSERT_TRUE(far.ended_at);
    EXPECT_GE(*far.ended_at, deadline);
    EXPECT_LE(
      far.ended_at->nanoseconds(), deadline.nanoseconds() + millisecond);
}

TEST(DeadlineWheelTest, AParkedDeadlineCollectedLateEndsAtOnce) {
    current = at_ms(0);
    deadline_wheel wheel{current};
    probe far;
    ASSERT_EQ(
      wheel.insert(
        far, monotonic_time{300 * second}, precision::exact, current),
      insertion::linked);
    // The alarm rang long after the parked bucket and the deadline.
    current = monotonic_time{400 * second};
    EXPECT_EQ(wheel.expire(current, SIZE_MAX), 1U);
    ASSERT_TRUE(far.ended_at);
    EXPECT_EQ(*far.ended_at, current);
    EXPECT_TRUE(wheel.empty());
}

// Tasks of many delays, some due together: each runs exactly once, at or
// after its delay.
TEST(DeadlineWheelTest, EveryTaskEndsOnceAtOrAfterItsDelay) {
    current = at_ms(0);
    deadline_wheel wheel{current};
    std::vector<std::unique_ptr<probe>> tasks;
    std::vector<std::uint64_t> delays;
    auto add = [&](std::uint64_t delay) {
        tasks.push_back(std::make_unique<probe>());
        delays.push_back(delay);
        const auto inserted = wheel.insert(
          *tasks.back(), at_ms(delay), precision::exact, current);
        if (inserted == insertion::due) {
            tasks.back()->ended_at = current;
            ++tasks.back()->endings;
        }
    };
    for (std::uint64_t delay = 0; delay < 5; ++delay)
        add(delay);
    for (std::uint64_t delay = 10; delay < 100; ++delay) {
        add(delay);
        add(delay);
    }
    for (std::uint64_t delay = 100; delay < 500; ++delay)
        add(delay);
    static_cast<void>(run_until(wheel, at_ms(2000)));
    for (std::size_t index = 0; index < tasks.size(); ++index) {
        SCOPED_TRACE(delays[index]);
        ASSERT_TRUE(tasks[index]->ended_at);
        EXPECT_EQ(tasks[index]->endings, 1U);
        EXPECT_GE(*tasks[index]->ended_at, at_ms(delays[index]));
        EXPECT_LE(*tasks[index]->ended_at, at_ms(delays[index] + 1));
    }
    EXPECT_TRUE(wheel.empty());
}

// What an expiry that arms another deadline arms it on, and the deadlines it
// arms.
struct follow_up_state final {
    deadline_wheel* wheel{nullptr};
    std::deque<probe> entries;
    std::vector<monotonic_time> deadlines;
};
follow_up_state* follow_ups = nullptr;

void arm_follow_up(probe&) noexcept {
    auto& entry = follow_ups->entries.emplace_back();
    const monotonic_time deadline{
      current.nanoseconds()
      + (follow_ups->entries.size() % 7U + 1U) * millisecond};
    follow_ups->deadlines.push_back(deadline);
    if (
      follow_ups->wheel->insert(entry, deadline, precision::exact, current)
      == insertion::due) {
        entry.ended_at = current;
        ++entry.endings;
    }
}

// An expiry that arms another deadline never lands it in a bucket still
// being emptied (the wheel asserts that in debug builds), and a long seeded
// history of inserts, cancels and expiries keeps every ending exact.
TEST(DeadlineWheelTest, SeededHistoriesKeepEveryEndingExact) {
    std::uint64_t state = 0x6b77617175650005ULL;
    auto next = [&state](std::uint64_t bound) {
        state = state * 6364136223846793005ULL + 1442695040888963407ULL;
        return (state >> 11U) % bound;
    };
    current = at_ms(5);
    deadline_wheel wheel{current};
    follow_up_state armed;
    armed.wheel = &wheel;
    follow_ups = &armed;
    std::deque<probe> entries;
    std::vector<monotonic_time> deadlines;
    for (std::size_t step = 0; step < 20'000; ++step) {
        const auto action = next(10);
        if (action < 6) {
            auto& entry = entries.emplace_back();
            if (action == 0) {
                entry.then = &arm_follow_up;
            }
            const monotonic_time deadline{
              current.nanoseconds() + next(300 * second) + 1};
            deadlines.push_back(deadline);
            if (
              wheel.insert(entry, deadline, precision::exact, current)
              == insertion::due) {
                entry.ended_at = current;
                ++entry.endings;
            }
        } else if (action < 8 && !entries.empty()) {
            static_cast<void>(wheel.cancel(entries[next(entries.size())]));
        } else {
            current = monotonic_time{current.nanoseconds() + next(2 * second)};
            static_cast<void>(wheel.expire(current, 1 + next(64)));
        }
    }
    static_cast<void>(
      run_until(wheel, monotonic_time{current.nanoseconds() + 400 * second}));
    for (std::size_t index = 0; index < entries.size(); ++index) {
        const auto& entry = entries[index];
        EXPECT_LE(entry.endings, 1U);
        if (entry.ended_at) {
            EXPECT_GE(*entry.ended_at, deadlines[index]);
        }
    }
    ASSERT_FALSE(armed.entries.empty());
    for (std::size_t index = 0; index < armed.entries.size(); ++index) {
        const auto& entry = armed.entries[index];
        EXPECT_EQ(entry.endings, 1U);
        ASSERT_TRUE(entry.ended_at.has_value());
        EXPECT_GE(*entry.ended_at, armed.deadlines[index]);
    }
    follow_ups = nullptr;
    if (!wheel.empty()) {
        std::size_t shown = 0;
        for (std::size_t index = 0; index < entries.size() && shown < 5;
             ++index) {
            if (entries[index].linked()) {
                ++shown;
                ADD_FAILURE()
                  << "left linked: deadline_ns="
                  << deadlines[index].nanoseconds()
                  << " current_ns=" << current.nanoseconds() << " next="
                  << (wheel.next_expiry(current)
                        ? wheel.next_expiry(current)->nanoseconds()
                        : 0)
                  << " size=" << wheel.size();
            }
        }
    }
    EXPECT_TRUE(wheel.empty());
}

// The load the plan sets: 90% idle maximum waits of 500 ms and appends
// whose 30 s deadlines are cancelled within a few milliseconds.
TEST(DeadlineWheelTest, WorkStaysProportionalToTheEntriesThatEnd) {
    current = at_ms(1);
    deadline_wheel wheel{current};
    constexpr std::size_t reads = 90'000;
    constexpr std::size_t budget = 256;
    std::vector<probe> idle(reads);
    for (std::size_t index = 0; index < reads; ++index) {
        ASSERT_EQ(
          wheel.insert(
            idle[index],
            monotonic_time{
              current.nanoseconds() + 500 * millisecond
              + (index % 1000) * millisecond},
            precision::may_end_early,
            current),
          insertion::linked);
    }
    std::deque<probe> appends;
    std::size_t touched = 0;
    std::size_t ended = 0;
    std::size_t most_in_one_call = 0;
    for (std::uint64_t ms = 2; ms <= 2000; ++ms) {
        current = at_ms(ms);
        for (std::size_t append = 0; append < 100; ++append) {
            auto& entry = appends.emplace_back();
            ASSERT_EQ(
              wheel.insert(
                entry, at_ms(ms + 30'000), precision::exact, current),
              insertion::linked);
        }
        while (appends.size() > 300) {
            EXPECT_TRUE(wheel.cancel(appends.front()));
            appends.pop_front();
        }
        const auto before = wheel.size();
        const auto now_touched = wheel.expire(current, budget);
        most_in_one_call = std::max(most_in_one_call, now_touched);
        touched += now_touched;
        ended += before - wheel.size();
        while (wheel.has_collected()) {
            const auto size_before = wheel.size();
            const auto more = wheel.expire(current, budget);
            most_in_one_call = std::max(most_in_one_call, more);
            touched += more;
            ended += size_before - wheel.size();
        }
    }
    for (auto& entry : appends)
        static_cast<void>(wheel.cancel(entry));
    EXPECT_EQ(ended, reads);
    EXPECT_LE(most_in_one_call, budget);
    // Each idle wait is touched once, when it ends; no cancelled append is.
    EXPECT_LE(static_cast<double>(touched), 1.01 * static_cast<double>(ended));
}

TEST(DeadlineWheelTest, InsertsCancelsAndExpiriesAllocateNothing) {
#if !defined(SEASTAR_DEFAULT_ALLOCATOR)
    current = at_ms(1);
    deadline_wheel wheel{current};
    std::vector<probe> entries(1024);
    const auto before = seastar::memory::stats().mallocs();
    for (std::size_t round = 0; round < 100; ++round) {
        for (std::size_t index = 0; index < entries.size(); ++index) {
            static_cast<void>(wheel.insert(
              entries[index],
              monotonic_time{
                current.nanoseconds() + (index + 1) * millisecond * 37},
              index % 3 == 0 ? precision::may_end_early : precision::exact,
              current));
        }
        for (std::size_t index = 0; index < entries.size(); index += 2) {
            static_cast<void>(wheel.cancel(entries[index]));
        }
        current = monotonic_time{current.nanoseconds() + 40 * second};
        while (!wheel.empty()) {
            current = std::max(current, *wheel.next_expiry(current));
            static_cast<void>(wheel.expire(current, 256));
        }
    }
    EXPECT_EQ(seastar::memory::stats().mallocs() - before, 0U);
#else
    GTEST_SKIP() << "the system allocator keeps no allocation count";
#endif
}

// The driver over a manual alarm and clock.
struct manual_clock final {
    [[nodiscard]] static monotonic_time now() noexcept { return current; }
};

using test_deadlines = kwaque::runtime::
  basic_deadlines<kwaque::runtime::testing::contract_alarm, manual_clock>;

TEST(DeadlinesTest, ArmsForTheNextBucketMovesEarlierAndStopsWhenEmpty) {
    current = at_ms(100);
    test_deadlines deadlines{4};
    EXPECT_FALSE(deadlines.armed());
    probe late;
    ASSERT_EQ(
      deadlines.arm(late, at_ms(150), precision::exact), insertion::linked);
    EXPECT_EQ(deadlines.armed_at(), at_ms(150));
    probe early;
    ASSERT_EQ(
      deadlines.arm(early, at_ms(120), precision::exact), insertion::linked);
    EXPECT_EQ(deadlines.armed_at(), at_ms(120));
    EXPECT_TRUE(deadlines.cancel(early));
    EXPECT_TRUE(deadlines.cancel(late));
    EXPECT_FALSE(deadlines.armed());
}

TEST(DeadlinesTest, ARingTouchesAtMostItsBudgetAndArmsForNowWhenWorkRemains) {
    current = at_ms(0);
    test_deadlines deadlines{4};
    std::vector<probe> entries(10);
    for (auto& entry : entries) {
        ASSERT_EQ(
          deadlines.arm(entry, at_ms(5), precision::exact), insertion::linked);
    }
    EXPECT_EQ(deadlines.armed_at(), at_ms(5));
    current = at_ms(5);
    std::vector<std::size_t> ended_per_ring;
    while (deadlines.alarm().armed_at()) {
        // Work left over is resumed at once: the alarm is armed for now.
        EXPECT_EQ(deadlines.alarm().armed_at(), current);
        const auto before = deadlines.size();
        deadlines.alarm().ring();
        ended_per_ring.push_back(before - deadlines.size());
    }
    EXPECT_EQ(ended_per_ring, (std::vector<std::size_t>{4, 4, 2}));
    EXPECT_EQ(deadlines.size(), 0U);
    EXPECT_FALSE(deadlines.armed());
}

TEST(DeadlinesTest, ADueDeadlineLinksNothingAndArmsNothing) {
    current = at_ms(50);
    test_deadlines deadlines{4};
    probe past;
    EXPECT_EQ(deadlines.arm(past, at_ms(50), precision::exact), insertion::due);
    EXPECT_FALSE(past.linked());
    EXPECT_FALSE(deadlines.armed());
}

} // namespace
