#include "src/admin/admin_state.h"

#include <seastar/core/metrics.hh>
#include <seastar/core/metrics_api.hh>
#include <seastar/core/metrics_registration.hh>

#include <gtest/gtest.h>

#include <optional>
#include <stdexcept>

namespace {

bool metric_registered(const char* full_name) {
    return seastar::metrics::impl::get_value_map().contains(
      seastar::sstring{full_name});
}

TEST(AdminStateTest, ReadinessTracksListenerAndDrainLifecycle) {
    kwaque::admin::admin_state state;
    state.register_metrics();

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

    state.stop().get();
    EXPECT_FALSE(state.live());
    EXPECT_FALSE(state.ready());
}

TEST(AdminStateTest, RequestCounterIsMonotonic) {
    kwaque::admin::admin_state state;
    state.register_metrics();
    state.record_request();
    state.record_request();
    EXPECT_EQ(state.request_count(), 2U);
    state.stop().get();
}

TEST(AdminStateTest, MetricRegistrationIsUniqueAndRestartableAfterStop) {
    kwaque::admin::admin_state state;
    state.register_metrics();
    EXPECT_THROW(state.register_metrics(), std::logic_error);
    state.stop().get();
    EXPECT_NO_THROW(state.register_metrics());
    state.stop().get();
}

TEST(AdminStateTest, PartialMetricRegistrationRollsBackNativeGroup) {
    namespace metrics = seastar::metrics;
    std::optional<metrics::metric_groups> blocker;
    blocker.emplace();
    blocker->add_group(
      "broker",
      {metrics::make_gauge(
        "shard_count",
        [] { return 1U; },
        metrics::description("Registration rollback blocker"))});
    ASSERT_TRUE(metric_registered("broker_shard_count"));

    kwaque::admin::admin_state state;
    EXPECT_THROW(state.register_metrics(), metrics::double_registration);
    EXPECT_FALSE(metric_registered("broker_process_readiness"));
    EXPECT_TRUE(metric_registered("broker_shard_count"));

    blocker.reset();
    EXPECT_FALSE(metric_registered("broker_shard_count"));
    EXPECT_NO_THROW(state.register_metrics());
    EXPECT_TRUE(metric_registered("broker_process_readiness"));
    state.stop().get();
    EXPECT_FALSE(metric_registered("broker_process_readiness"));
}

} // namespace
