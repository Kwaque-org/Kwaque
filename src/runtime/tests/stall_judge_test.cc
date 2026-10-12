#include "src/runtime/production/stall_judge.h"
#include "src/runtime/stall_watch.h"

#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <optional>

namespace {
using namespace kwaque::runtime;
using production::stall_judge;

// What the judge called, for the test's thread to wait on.
struct verdict final {
    std::mutex mutex;
    std::condition_variable told;
    std::optional<stalled_operation> found;
    int calls{0};
    static void ended(void* context, const stalled_operation& what) noexcept {
        auto& self = *static_cast<verdict*>(context);
        {
            const std::scoped_lock lock{self.mutex};
            self.found = what;
            ++self.calls;
        }
        self.told.notify_all();
    }
};

// An operation in flight past the threshold ends the judging, once, from a
// thread that is no shard's; one that is not past it does not.
TEST(StallJudgeTest, AnOperationPastTheThresholdEndsTheJudgingOnce) {
    stall_stamps stamps{2};
    verdict seen;
    stall_judge judge{
      stamps,
      {std::chrono::milliseconds{1}, std::chrono::milliseconds{5}},
      verdict::ended,
      &seen};
    ASSERT_TRUE(judge.start().has_value());
    EXPECT_FALSE(judge.start().has_value());
    // Nothing in flight: nothing is found.
    {
        std::unique_lock lock{seen.mutex};
        EXPECT_FALSE(
          seen.told.wait_for(lock, std::chrono::milliseconds{50}, [&] {
              return seen.calls != 0;
          }));
    }
    EXPECT_FALSE(judge.ended().has_value());
    // Started at the host clock's first nanosecond: older than any
    // threshold by now.
    stamps.at(1, 3)->store(1);
    {
        std::unique_lock lock{seen.mutex};
        ASSERT_TRUE(seen.told.wait_for(
          lock, std::chrono::seconds{30}, [&] { return seen.calls != 0; }));
        ASSERT_TRUE(seen.found.has_value());
        EXPECT_EQ(seen.found->shard, 1U);
        EXPECT_EQ(seen.found->device, 3U);
        EXPECT_GT(seen.found->age_nanoseconds, 1'000'000U);
    }
    judge.stop();
    judge.stop();
    EXPECT_EQ(seen.calls, 1);
    ASSERT_TRUE(judge.ended().has_value());
    EXPECT_EQ(judge.ended()->shard, 1U);
}

// Stopped, it judges no more; and a threshold that is not positive is
// refused.
TEST(StallJudgeTest, AStoppedJudgeFindsNothing) {
    stall_stamps stamps{1};
    verdict seen;
    {
        stall_judge judge{
          stamps,
          {std::chrono::milliseconds{1}, std::chrono::milliseconds{5}},
          verdict::ended,
          &seen};
        ASSERT_TRUE(judge.start().has_value());
        judge.stop();
        stamps.at(0, 0)->store(1);
    }
    EXPECT_EQ(seen.calls, 0);
    stall_judge unset{
      stamps, {std::chrono::nanoseconds{0}, {}}, verdict::ended, &seen};
    EXPECT_FALSE(unset.start().has_value());
    stall_judge mute{stamps, {}, nullptr, nullptr};
    EXPECT_FALSE(mute.start().has_value());
}
} // namespace
