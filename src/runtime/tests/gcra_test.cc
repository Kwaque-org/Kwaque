#include "src/runtime/gcra.h"
#include "src/runtime/time.h"

#include <seastar/core/memory.hh>
#include <seastar/util/shared_token_bucket.hh>

#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <cstdint>
#include <ratio>

namespace {

using kwaque::runtime::gcra;
using kwaque::runtime::monotonic_duration;
using kwaque::runtime::monotonic_time;

constexpr std::uint64_t second = 1'000'000'000;

monotonic_time at(std::uint64_t nanoseconds) {
    return monotonic_time{nanoseconds};
}

TEST(GcraTest, BurstAtOnceThenOneEachInterval) {
    gcra word;
    const monotonic_duration interval{second};
    for (int grant = 0; grant < 3; ++grant) {
        EXPECT_TRUE(word.try_take(at(0), interval, 3).granted) << grant;
    }
    const auto refused = word.try_take(at(0), interval, 3);
    EXPECT_FALSE(refused.granted);
    EXPECT_EQ(refused.retry_at, at(second));
    // A refusal changes nothing.
    EXPECT_EQ(word.try_take(at(0), interval, 3), refused);
    EXPECT_FALSE(word.try_take(at(second - 1), interval, 3).granted);
    EXPECT_TRUE(word.try_take(at(second), interval, 3).granted);
    const auto next = word.try_take(at(second + second / 2), interval, 3);
    EXPECT_FALSE(next.granted);
    EXPECT_EQ(next.retry_at, at(2 * second));
}

TEST(GcraTest, IdleTimeBanksNoMoreThanTheBurst) {
    gcra word;
    const monotonic_duration interval{second};
    EXPECT_TRUE(word.try_take(at(0), interval, 2).granted);
    const auto later = 3'600 * second;
    EXPECT_TRUE(word.try_take(at(later), interval, 2).granted);
    EXPECT_TRUE(word.try_take(at(later), interval, 2).granted);
    const auto refused = word.try_take(at(later), interval, 2);
    EXPECT_FALSE(refused.granted);
    EXPECT_EQ(refused.retry_at, at(later + second));
}

TEST(GcraTest, ARefundRestoresAGrant) {
    gcra word;
    const monotonic_duration interval{second};
    EXPECT_TRUE(word.try_take(at(0), interval, 1).granted);
    EXPECT_FALSE(word.try_take(at(0), interval, 1).granted);
    word.refund(interval);
    EXPECT_TRUE(word.try_take(at(0), interval, 1).granted);
    // A refund on a fresh word stays at the start of time.
    gcra fresh;
    fresh.refund(interval);
    EXPECT_EQ(fresh.theoretical_arrival(), at(0));
}

// A grant refunded when it was not used, or refunded twice by mistake, never
// lets more than the burst through at once.
TEST(GcraTest, RefundsNeverBankMoreThanTheBurst) {
    gcra word;
    const monotonic_duration interval{second};
    const auto later = 100 * second;
    EXPECT_TRUE(word.try_take(at(0), interval, 2).granted);
    for (int refund = 0; refund < 5; ++refund) {
        word.refund(interval);
    }
    EXPECT_TRUE(word.try_take(at(later), interval, 2).granted);
    EXPECT_TRUE(word.try_take(at(later), interval, 2).granted);
    const auto refused = word.try_take(at(later), interval, 2);
    EXPECT_FALSE(refused.granted);
    EXPECT_EQ(refused.retry_at, at(later + second));
}

// A burst of zero is a burst of one.
TEST(GcraTest, ABurstOfZeroIsABurstOfOne) {
    gcra word;
    const monotonic_duration interval{second};
    EXPECT_TRUE(word.try_take(at(0), interval, 0).granted);
    const auto refused = word.try_take(at(0), interval, 0);
    EXPECT_FALSE(refused.granted);
    EXPECT_EQ(refused.retry_at, at(second));
}

// Refunds at the moment of the takes they return, more of them than were
// taken, restore the burst and never more.
TEST(GcraTest, RefundsAtTheTakeNeverBankMoreThanTheBurst) {
    gcra word;
    const monotonic_duration interval{second};
    EXPECT_TRUE(word.try_take(at(5 * second), interval, 2).granted);
    EXPECT_TRUE(word.try_take(at(5 * second), interval, 2).granted);
    EXPECT_FALSE(word.try_take(at(5 * second), interval, 2).granted);
    word.refund(interval);
    word.refund(interval);
    word.refund(interval);
    EXPECT_TRUE(word.try_take(at(5 * second), interval, 2).granted);
    EXPECT_TRUE(word.try_take(at(5 * second), interval, 2).granted);
    EXPECT_FALSE(word.try_take(at(5 * second), interval, 2).granted);
}

// A zero interval spends no time per grant; configured rates are never zero.
TEST(GcraTest, AZeroIntervalGrantsEveryRequest) {
    gcra word;
    for (int request = 0; request < 1'000; ++request) {
        EXPECT_TRUE(word.try_take(at(5), monotonic_duration{0}, 1).granted);
    }
}

TEST(GcraTest, OneAMinuteIsExpressible) {
    gcra word;
    const monotonic_duration minute{60 * second};
    EXPECT_TRUE(word.try_take(at(0), minute, 1).granted);
    const auto refused = word.try_take(at(60 * second - 1), minute, 1);
    EXPECT_FALSE(refused.granted);
    EXPECT_EQ(refused.retry_at, at(60 * second));
    EXPECT_TRUE(word.try_take(at(60 * second), minute, 1).granted);
}

TEST(GcraTest, ArithmeticSaturatesInsteadOfWrapping) {
    gcra word;
    const monotonic_duration forever{UINT64_MAX};
    EXPECT_TRUE(word.try_take(at(UINT64_MAX - 1), forever, 1).granted);
    EXPECT_EQ(word.theoretical_arrival(), at(UINT64_MAX));
    EXPECT_FALSE(word.try_take(at(UINT64_MAX - 1), forever, 1).granted);
    // A tolerance too large to represent saturates and grants every request:
    // the precondition the header states.
    gcra wide;
    EXPECT_TRUE(wide.try_take(at(0), forever, 3).granted);
    EXPECT_TRUE(wide.try_take(at(0), forever, 3).granted);
}

// The reference: a shard's token bucket, driven at times that are whole
// multiples of the interval, where its rounding to whole tokens is exact.
struct reference_clock final {
    using duration = std::chrono::nanoseconds;
    using rep = duration::rep;
    using period = duration::period;
    using time_point = std::chrono::time_point<reference_clock, duration>;
    static time_point now() noexcept { return time_point{}; }
};

using reference_bucket = seastar::internal::shared_token_bucket<
  std::uint64_t,
  std::ratio<1>,
  seastar::internal::capped_release::no,
  reference_clock>;

// The bucket's decision for one token at `now`, made as the I/O scheduler's
// bandwidth throttle makes it: replenish, grab, and on a shortfall wait the
// time the shortfall takes to replenish, counted from now. A refused grab is
// returned, so it changes nothing.
gcra::decision reference_take(reference_bucket& bucket, std::uint64_t now) {
    const reference_clock::time_point point{std::chrono::nanoseconds{now}};
    bucket.replenish(point);
    const auto from = bucket.grab(1);
    const auto shortfall = bucket.deficiency(from);
    if (shortfall == 0) {
        return gcra::decision{.granted = true, .retry_at = at(now)};
    }
    bucket.refund(1);
    const auto wait = std::chrono::duration_cast<std::chrono::nanoseconds>(
      bucket.duration_for(shortfall));
    return gcra::decision{
      .granted = false,
      .retry_at = at(now + static_cast<std::uint64_t>(wait.count()))};
}

// A fixed-seed stream for the request times.
struct splitmix final {
    std::uint64_t next() noexcept {
        state += UINT64_C(0x9e3779b97f4a7c15);
        auto value = state;
        value = (value ^ (value >> 30U)) * UINT64_C(0xbf58476d1ce4e5b9);
        value = (value ^ (value >> 27U)) * UINT64_C(0x94d049bb133111eb);
        return value ^ (value >> 31U);
    }
    std::uint64_t state;
};

TEST(GcraTest, MatchesTheTokenBucketAtWholeIntervals) {
    splitmix draws{.state = 0x6763726100000001};
    for (const std::uint64_t rate : {1U, 2U, 5U, 10U, 1'000U}) {
        for (const std::uint32_t burst : {1U, 2U, 4U, 16U}) {
            const auto step = second / rate;
            gcra word;
            reference_bucket bucket{rate, burst, 1};
            std::uint64_t now = 0;
            for (int request = 0; request < 10'000; ++request) {
                // Mostly bursts at one time, sometimes a gap of whole
                // intervals, now and then a long idle.
                const auto draw = draws.next() % 16U;
                now += draw < 10U   ? 0U
                       : draw < 15U ? (draw - 9U) * step
                                    : 40U * step;
                const auto expected = reference_take(bucket, now);
                const auto actual = word.try_take(
                  at(now), monotonic_duration{step}, burst);
                ASSERT_EQ(actual, expected)
                  << "rate=" << rate << " burst=" << burst
                  << " request=" << request << " now=" << now;
            }
        }
    }
}

// The algorithm's other standard form, the continuous-state leaky bucket:
// the bucket holds X nanoseconds of work as of the last conforming request,
// drains one nanosecond a nanosecond, and takes a request whose arrival finds
// at most the tolerance in it, adding one interval. Exact at every time, so
// it checks the times the token bucket cannot.
class leaky_bucket final {
public:
    gcra::decision
    take(std::uint64_t now, std::uint64_t interval, std::uint64_t burst) {
        const auto tolerance = (burst - 1) * interval;
        const auto drained = now - last_conforming_;
        const auto level = content_ > drained ? content_ - drained : 0;
        if (level > tolerance) {
            return gcra::decision{
              .granted = false, .retry_at = at(now + (level - tolerance))};
        }
        content_ = level + interval;
        last_conforming_ = now;
        return gcra::decision{.granted = true, .retry_at = at(now)};
    }

private:
    std::uint64_t content_{0};
    std::uint64_t last_conforming_{0};
};

TEST(GcraTest, MatchesTheLeakyBucketFormAtEveryTime) {
    splitmix draws{.state = 0x6763726100000002};
    // Intervals that divide a second and ones that do not: seven a minute,
    // a prime number of nanoseconds, a single nanosecond.
    constexpr std::array<std::uint64_t, 5> intervals{
      second, second / 8, 60 * second / 7, 999'999'937, 1};
    for (const auto interval : intervals) {
        for (const std::uint32_t burst : {1U, 2U, 3U, 16U}) {
            gcra word;
            leaky_bucket reference;
            std::uint64_t now = 0;
            for (int request = 0; request < 10'000; ++request) {
                const auto draw = draws.next();
                // Bursts at one time, gaps anywhere inside or past an
                // interval, and now and then a long idle.
                switch (draw % 4U) {
                case 0:
                    break;
                case 1:
                case 2:
                    now += (draw >> 8U) % (2 * interval + 1);
                    break;
                default:
                    now += (draw >> 8U) % (40 * interval + 1);
                    break;
                }
                const auto expected = reference.take(now, interval, burst);
                const auto actual = word.try_take(
                  at(now), monotonic_duration{interval}, burst);
                ASSERT_EQ(actual, expected)
                  << "interval=" << interval << " burst=" << burst
                  << " request=" << request << " now=" << now;
            }
        }
    }
}

// The token bucket tests of a replication throttle and of the I/O
// scheduler, with an explicit clock. Both of those buckets start with
// tokens a timer or a poll adds; the word starts with its whole burst.
TEST(GcraTest, TokenBucketCasesHoldWithAnExplicitClock) {
    constexpr std::uint32_t rate = 25;
    const monotonic_duration interval{second / rate};
    {
        // Taking the whole rate at once leaves nothing; the next one is due
        // one interval later.
        gcra word;
        for (std::uint32_t grant = 0; grant < rate; ++grant) {
            ASSERT_TRUE(word.try_take(at(0), interval, rate).granted) << grant;
        }
        const auto refused = word.try_take(at(0), interval, rate);
        EXPECT_FALSE(refused.granted);
        EXPECT_EQ(refused.retry_at, at(second / rate));
        // A caller that waits until then is granted, and the one after it is
        // refused again.
        EXPECT_TRUE(word.try_take(refused.retry_at, interval, rate).granted);
        EXPECT_FALSE(word.try_take(refused.retry_at, interval, rate).granted);
    }
    {
        // A burst of three seconds' worth: after four idle seconds exactly
        // the burst is taken, not four seconds' worth.
        gcra word;
        const std::uint32_t burst = 3 * rate;
        for (std::uint32_t grant = 0; grant < burst; ++grant) {
            ASSERT_TRUE(word.try_take(at(0), interval, burst).granted);
        }
        EXPECT_FALSE(word.try_take(at(0), interval, burst).granted);
        const auto later = 4 * second;
        for (std::uint32_t grant = 0; grant < burst; ++grant) {
            ASSERT_TRUE(word.try_take(at(later), interval, burst).granted)
              << grant;
        }
        EXPECT_FALSE(word.try_take(at(later), interval, burst).granted);
    }
    {
        // One a second, burst one: each grant is the only one until the
        // clock moves a whole second.
        gcra word;
        const monotonic_duration one_second{second};
        std::uint64_t now = 0;
        EXPECT_TRUE(word.try_take(at(now), one_second, 1).granted);
        for (int loop = 0; loop < 2; ++loop) {
            EXPECT_FALSE(word.try_take(at(now), one_second, 1).granted);
            now += second;
            EXPECT_TRUE(word.try_take(at(now), one_second, 1).granted);
        }
    }
}

TEST(GcraTest, TakingAndRefusingAllocateNothing) {
#if !defined(SEASTAR_DEFAULT_ALLOCATOR)
    gcra word;
    const monotonic_duration interval{second};
    std::uint64_t granted = 0;
    const auto before = seastar::memory::stats().mallocs();
    for (std::uint64_t request = 0; request < 1'000'000; ++request) {
        const auto decision = word.try_take(at(request * 1'000), interval, 2);
        granted += decision.granted ? 1U : 0U;
        if (request % 7 == 0) {
            word.refund(interval);
        }
    }
    const auto after = seastar::memory::stats().mallocs();
    EXPECT_EQ(after - before, 0U);
    EXPECT_GT(granted, 0U);
#else
    GTEST_SKIP() << "the system allocator keeps no allocation count";
#endif
}

} // namespace
