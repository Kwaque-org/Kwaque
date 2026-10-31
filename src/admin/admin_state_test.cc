#include "src/admin/admin_state.h"
#include "src/base/build_info.h"

#include <seastar/core/metrics.hh>
#include <seastar/core/metrics_api.hh>
#include <seastar/core/metrics_registration.hh>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

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

    state.mark_started(1.25);
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

using kwaque::readiness_bit;
using kwaque::readiness_reason;
using kwaque::readiness_reasons;
using kwaque::admin::readiness_owner;

constexpr auto wal = readiness_bit(readiness_reason::wal_ceiling);
constexpr auto space = readiness_bit(readiness_reason::no_space);

// A started state whose broker-wide reasons are the owner's, as the
// broadcast delivers them.
struct started_shard final {
    started_shard() {
        state.listener_started(2);
        state.mark_started(0.0);
    }
    ~started_shard() { state.stop().get(); }
    void deliver(const std::optional<readiness_owner::broadcast>& update) {
        if (update) {
            state.apply_reasons(update->reasons, update->version);
        }
    }
    kwaque::admin::admin_state state;
};

TEST(AdminStateTest, AReasonHeldBeforeTheStartKeepsTheBrokerUnready) {
    kwaque::admin::admin_state state;
    readiness_owner owner{1};
    state.listener_started(1);
    const auto update = owner.apply(0, space, 1);
    ASSERT_TRUE(update);
    state.apply_reasons(update->reasons, update->version);
    state.mark_started(0.5);
    EXPECT_TRUE(state.started());
    EXPECT_FALSE(state.ready());
    EXPECT_EQ(state.reasons(), space);
    state.apply_reasons(0, owner.apply(0, 0, 2)->version);
    EXPECT_TRUE(state.ready());
    state.stop().get();
}

TEST(AdminStateTest, RestoringOneReasonKeepsAnotherHeld) {
    started_shard shard;
    readiness_owner owner{1};
    shard.deliver(owner.apply(0, wal, 1));
    shard.deliver(owner.apply(0, wal | space, 2));
    shard.deliver(owner.apply(0, space, 3));
    EXPECT_FALSE(shard.state.ready());
    EXPECT_EQ(shard.state.reasons(), space);
    shard.deliver(owner.apply(0, 0, 4));
    EXPECT_TRUE(shard.state.ready());
}

TEST(AdminStateTest, OneShardsRestoreKeepsAnotherShardsReason) {
    started_shard shard;
    readiness_owner owner{2};
    shard.deliver(owner.apply(0, wal, 1));
    shard.deliver(owner.apply(1, wal, 1));
    // Shard 0 restores; shard 1 still holds the same reason.
    EXPECT_FALSE(owner.apply(0, 0, 2));
    EXPECT_FALSE(shard.state.ready());
    shard.deliver(owner.apply(1, 0, 2));
    EXPECT_TRUE(shard.state.ready());
}

TEST(AdminStateTest, DrainingIsFinalWhateverIsRestored) {
    started_shard shard;
    readiness_owner owner{1};
    shard.deliver(owner.apply(0, space, 1));
    shard.state.begin_shutdown();
    shard.deliver(owner.apply(0, 0, 2));
    EXPECT_TRUE(shard.state.draining());
    EXPECT_FALSE(shard.state.ready());
    EXPECT_TRUE(shard.state.live());
}

TEST(AdminStateTest, AnOlderBroadcastChangesNothing) {
    started_shard shard;
    shard.state.apply_reasons(space, 2);
    shard.state.apply_reasons(0, 1);
    shard.state.apply_reasons(0, 2);
    EXPECT_EQ(shard.state.reasons(), space);
    EXPECT_EQ(shard.state.reasons_version(), 2U);
    EXPECT_FALSE(shard.state.ready());
}

// Two shards each hold and release both reasons; their statements reach the
// owner duplicated and in any order, and the owner's broadcasts reach a shard
// in any order. After every statement the owner holds exactly the newest
// statement of each shard it has seen, and after every broadcast the shard
// holds the set of the newest version it has seen, never an older one.
TEST(
  AdminStateTest, DuplicatesAndReorderingNeverAnswerReadyWhileAReasonIsHeld) {
    struct statement final {
        unsigned shard;
        readiness_reasons reasons;
        std::uint64_t sequence;
    };
    // A fixed-seed stream for the orders.
    std::uint64_t seed = 0x7265616479000001;
    const auto next = [&seed] {
        seed += UINT64_C(0x9e3779b97f4a7c15);
        auto value = seed;
        value = (value ^ (value >> 30U)) * UINT64_C(0xbf58476d1ce4e5b9);
        value = (value ^ (value >> 27U)) * UINT64_C(0x94d049bb133111eb);
        return value ^ (value >> 31U);
    };
    for (int round = 0; round < 2'000; ++round) {
        std::vector<statement> statements;
        std::array<readiness_reasons, 2> latest{};
        std::array<std::uint64_t, 2> sequence{};
        for (int step = 0; step < 8; ++step) {
            const auto shard = static_cast<unsigned>(next() % 2U);
            latest[shard] = static_cast<readiness_reasons>(next() % 4U);
            statements.push_back(
              statement{shard, latest[shard], ++sequence[shard]});
            if (next() % 3U == 0) {
                statements.push_back(statements.back());
            }
        }
        for (std::size_t index = statements.size(); index > 1; --index) {
            std::swap(statements[index - 1U], statements[next() % index]);
        }
        started_shard shard;
        readiness_owner owner{2};
        std::vector<readiness_owner::broadcast> broadcasts;
        std::array<readiness_reasons, 2> applied{};
        std::array<std::uint64_t, 2> applied_sequence{};
        std::map<std::uint64_t, readiness_reasons> sent{{0, 0}};
        for (const auto& value : statements) {
            const auto update = owner.apply(
              value.shard, value.reasons, value.sequence);
            if (value.sequence > applied_sequence[value.shard]) {
                applied_sequence[value.shard] = value.sequence;
                applied[value.shard] = value.reasons;
            }
            EXPECT_EQ(
              owner.reasons(),
              static_cast<readiness_reasons>(applied[0] | applied[1]))
              << round;
            if (update) {
                sent[update->version] = update->reasons;
                broadcasts.push_back(*update);
            }
        }
        for (std::size_t index = broadcasts.size(); index > 1; --index) {
            std::swap(broadcasts[index - 1U], broadcasts[next() % index]);
        }
        std::uint64_t newest = 0;
        for (const auto& update : broadcasts) {
            shard.state.apply_reasons(update.reasons, update.version);
            EXPECT_GE(shard.state.reasons_version(), newest) << round;
            newest = shard.state.reasons_version();
            EXPECT_EQ(shard.state.reasons(), sent.at(newest)) << round;
            EXPECT_EQ(shard.state.ready(), shard.state.reasons() == 0) << round;
        }
        // Every statement has arrived, in whatever order: the owner holds
        // exactly the latest of each shard, and so does the shard after the
        // broadcasts, in whatever order they came.
        const auto held = static_cast<readiness_reasons>(latest[0] | latest[1]);
        EXPECT_EQ(owner.reasons(), held) << round;
        EXPECT_EQ(shard.state.reasons(), held) << round;
        EXPECT_EQ(shard.state.ready(), held == 0) << round;
    }
}

TEST(AdminStateTest, TheObserverSeesEachChangeOfReadinessOrReasonsOnce) {
    kwaque::admin::admin_state state;
    std::vector<std::pair<bool, readiness_reasons>> seen;
    state.listener_started(1);
    state.observe_readiness([&seen](bool ready, readiness_reasons reasons) {
        seen.emplace_back(ready, reasons);
    });
    state.mark_started(0.0);
    state.apply_reasons(wal, 1);
    state.apply_reasons(wal, 1);
    state.apply_reasons(wal | space, 2);
    state.apply_reasons(0, 3);
    state.begin_shutdown();
    state.begin_shutdown();
    const std::vector<std::pair<bool, readiness_reasons>> expected{
      {true, 0},
      {false, wal},
      {false, static_cast<readiness_reasons>(wal | space)},
      {true, 0},
      {false, 0},
    };
    EXPECT_EQ(seen, expected);
    state.stop().get();
}

TEST(AdminStateTest, TheGaugeFollowsReadiness) {
    kwaque::admin::admin_state state;
    state.register_metrics();
    state.listener_started(1);
    state.mark_started(0.0);
    const auto gauge = [] {
        for (const auto& [name, metric] :
             seastar::metrics::impl::get_value_map()) {
            if (name == "broker_process_readiness") {
                for (const auto& [labels, registered] : metric) {
                    return (*registered)().d();
                }
            }
        }
        return -1.0;
    };
    EXPECT_EQ(gauge(), 1.0);
    state.apply_reasons(space, 1);
    EXPECT_EQ(gauge(), 0.0);
    state.apply_reasons(0, 2);
    EXPECT_EQ(gauge(), 1.0);
    state.stop().get();
}

} // namespace
