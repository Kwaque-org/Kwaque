#include "src/admin/admin_state.h"
#include "src/base/build_info.h"

#include <seastar/core/metrics.hh>
#include <seastar/core/metrics_api.hh>
#include <seastar/core/metrics_registration.hh>

#include <gtest/gtest.h>

#include <optional>
#include <stdexcept>
#include <string>

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
    EXPECT_FALSE(state.draining());
    EXPECT_DOUBLE_EQ(state.startup_duration_seconds(), 1.25);

    // A draining broker is leaving service but must not be restarted.
    state.begin_shutdown();
    state.begin_shutdown();
    EXPECT_TRUE(state.live());
    EXPECT_FALSE(state.ready());
    EXPECT_TRUE(state.draining());

    state.stop().get();
    EXPECT_FALSE(state.live());
    EXPECT_FALSE(state.ready());
    EXPECT_FALSE(state.draining());
}

TEST(AdminStateTest, ShardZeroOwnsTheProcessMetrics) {
    kwaque::admin::admin_state state;
    state.register_metrics();
    for (const char* name :
         {"broker_process_readiness",
          "broker_draining",
          "broker_shards",
          "broker_startup_duration_seconds",
          "broker_start_time_seconds",
          "build_info"}) {
        EXPECT_TRUE(metric_registered(name)) << name;
    }
    EXPECT_FALSE(metric_registered("broker_shutdown_total"));
    EXPECT_FALSE(metric_registered("broker_http_requests_total"));
    EXPECT_GT(kwaque::admin::process_start_time_seconds(), 0.0);
    state.stop().get();
    EXPECT_FALSE(metric_registered("broker_process_readiness"));
    EXPECT_FALSE(metric_registered("build_info"));
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
    // Only an identical label set is a duplicate registration.
    blocker->add_group(
      "build",
      {metrics::make_gauge(
        "info",
        [] { return 1U; },
        metrics::description("Registration rollback blocker"),
        {metrics::label("version")(std::string{kwaque::build_info::version()}),
         metrics::label("revision")(
           std::string{kwaque::build_info::git_revision()}),
         metrics::label("build_mode")(
           std::string{kwaque::build_info::build_mode()}),
         metrics::label("dirty")(
           kwaque::build_info::git_dirty() ? "true" : "false")})});
    ASSERT_TRUE(metric_registered("build_info"));

    // The broker group registers first; the blocked build group must remove
    // it again.
    kwaque::admin::admin_state state;
    EXPECT_THROW(state.register_metrics(), metrics::double_registration);
    EXPECT_FALSE(metric_registered("broker_process_readiness"));
    EXPECT_TRUE(metric_registered("build_info"));

    blocker.reset();
    EXPECT_FALSE(metric_registered("build_info"));
    EXPECT_NO_THROW(state.register_metrics());
    EXPECT_TRUE(metric_registered("broker_process_readiness"));
    state.stop().get();
    EXPECT_FALSE(metric_registered("broker_process_readiness"));
}

} // namespace
