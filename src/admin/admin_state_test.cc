#include "src/admin/admin_state.h"

#include <gtest/gtest.h>

namespace {

TEST(AdminStateTest, ReadinessTracksListenerAndDrainLifecycle) {
    kwaque::admin::admin_state state;

    EXPECT_FALSE(state.live());
    EXPECT_FALSE(state.ready());

    state.listener_started(3);
    EXPECT_TRUE(state.live());
    EXPECT_FALSE(state.ready());
    EXPECT_EQ(state.shard_count(), 3U);

    state.mark_ready(1.25);
    EXPECT_TRUE(state.live());
    EXPECT_TRUE(state.ready());
    EXPECT_DOUBLE_EQ(state.startup_duration_seconds(), 1.25);

    state.begin_shutdown();
    state.begin_shutdown();
    EXPECT_FALSE(state.live());
    EXPECT_FALSE(state.ready());
    EXPECT_EQ(state.shutdown_count(), 1U);

    state.stopped();
    EXPECT_FALSE(state.live());
    EXPECT_FALSE(state.ready());
}

TEST(AdminStateTest, RequestCounterIsMonotonic) {
    kwaque::admin::admin_state state;
    state.record_request();
    state.record_request();
    EXPECT_EQ(state.request_count(), 2U);
}

} // namespace
