#include "src/simulation/bandwidth.h"
#include "src/simulation/tests/network_oracle.h"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

namespace {

using kwaque::simulation::bandwidth_capacity;
using kwaque::simulation::bandwidth_constraint;
using kwaque::simulation::bandwidth_flow;
using kwaque::simulation::bandwidth_planner;
using kwaque::simulation::bandwidth_resource_key;
using kwaque::simulation::testing::dense_network_oracle;
using kwaque::simulation::testing::oracle_capacity;
using kwaque::simulation::testing::oracle_constraint;
using kwaque::simulation::testing::oracle_flow;
using kwaque::simulation::testing::oracle_script;
using kwaque::simulation::testing::oracle_step;
using kwaque::simulation::testing::oracle_step_kind;
using kwaque::simulation::testing::solve_bandwidth_oracle;

std::uint64_t random_word(std::uint64_t& state) {
    state ^= state << 13U;
    state ^= state >> 7U;
    state ^= state << 17U;
    return state;
}

bandwidth_capacity actual_capacity(oracle_capacity value) {
    return value.is_unlimited() ? bandwidth_capacity::unlimited()
                                : bandwidth_capacity::finite(value.value());
}

void reconcile(std::span<const oracle_flow> flows) {
    auto expected = solve_bandwidth_oracle(flows);
    ASSERT_TRUE(expected.has_value());
    auto made = bandwidth_planner::make(
      static_cast<std::uint32_t>(flows.size()));
    ASSERT_TRUE(made.has_value());
    auto planner = std::move(*made);
    for (const auto& flow : flows) {
        bandwidth_flow actual{.id = flow.id};
        for (std::size_t index = 0; index < flow.constraint_count; ++index) {
            actual.constraints[actual.constraint_count++]
              = bandwidth_constraint{
                .resource = bandwidth_resource_key::numeric(
                  1, flow.constraints[index].resource),
                .capacity = actual_capacity(flow.constraints[index].capacity),
            };
        }
        ASSERT_TRUE(planner->add_flow(std::move(actual)).has_value());
    }
    ASSERT_TRUE(planner->solve().has_value());
    ASSERT_EQ(planner->allocation_count(), expected->allocations.size());
    ASSERT_EQ(planner->resource_count(), expected->resources);
    ASSERT_EQ(planner->membership_count(), expected->memberships);
    EXPECT_EQ(planner->allocation_digest().words, expected->digest.words);
    for (std::size_t index = 0; index < expected->allocations.size(); ++index) {
        const auto& actual = planner->allocation_at(index);
        const auto& oracle = expected->allocations[index];
        ASSERT_EQ(actual.flow, oracle.flow);
        ASSERT_EQ(actual.rate.is_unlimited(), oracle.rate.unlimited);
        if (oracle.rate.unlimited) {
            continue;
        }
        ASSERT_LE(
          oracle.rate.finite.numerator(),
          kwaque::simulation::testing::oracle_fraction::integer{
            std::numeric_limits<std::uint64_t>::max()});
        ASSERT_LE(
          oracle.rate.finite.denominator(),
          kwaque::simulation::testing::oracle_fraction::integer{
            std::numeric_limits<std::uint64_t>::max()});
        EXPECT_TRUE(actual.rate.finite_value().equals(
          oracle.rate.finite.numerator().convert_to<std::uint64_t>(),
          oracle.rate.finite.denominator().convert_to<std::uint64_t>()));
    }
}

} // namespace

TEST(NetworkOracleTest, ProgressiveFillReconcilesHundredsOfSeededScenarios) {
    for (std::uint64_t seed = 1; seed <= 300; ++seed) {
        std::uint64_t random = seed;
        const auto flow_count = 2U + random_word(random) % 7U;
        std::array<oracle_capacity, 4> capacities{
          oracle_capacity::finite(96U * (1U + random_word(random) % 20U)),
          oracle_capacity::finite(96U * (1U + random_word(random) % 20U)),
          oracle_capacity::finite(96U * (1U + random_word(random) % 20U)),
          random_word(random) % 5U == 0
            ? oracle_capacity::unlimited()
            : oracle_capacity::finite(96U * (1U + random_word(random) % 20U)),
        };
        if (seed % 29U == 0) {
            capacities[2] = oracle_capacity::finite(0);
        }
        std::vector<oracle_flow> flows;
        flows.reserve(flow_count);
        for (std::uint64_t index = 0; index < flow_count; ++index) {
            oracle_flow flow{
              .id = 100U + index,
              .bytes = 1U + random_word(random) % 4'096U,
            };
            flow.constraints[flow.constraint_count++] = oracle_constraint{
              .resource = 1, .capacity = capacities[0]};
            const auto target = 1U + index % 2U;
            flow.constraints[flow.constraint_count++] = oracle_constraint{
              .resource = 1U + target, .capacity = capacities[target]};
            if (index % 3U == 0) {
                flow.constraints[flow.constraint_count++] = oracle_constraint{
                  .resource = 4, .capacity = capacities[3]};
            }
            flows.push_back(std::move(flow));
        }
        SCOPED_TRACE(seed);
        reconcile(flows);
    }
}

TEST(NetworkOracleTest, ConservesResourcesAtThirtyTwoAndNinetySixFlows) {
    for (const bool one_to_many : {true, false}) {
        for (const std::size_t count : {std::size_t{32}, std::size_t{96}}) {
            std::vector<oracle_flow> flows;
            flows.reserve(count);
            for (std::size_t index = 0; index < count; ++index) {
                oracle_flow flow{
                  .id = index + 1U,
                  .bytes = 8'192,
                  .constraint_count = 1,
                };
                flow.constraints[0] = oracle_constraint{
                  .resource = one_to_many ? 1U : 2U,
                  .capacity = oracle_capacity::finite(9'600),
                };
                flows.push_back(std::move(flow));
            }
            reconcile(flows);
            auto solved = solve_bandwidth_oracle(flows);
            ASSERT_TRUE(solved.has_value());
            for (const auto& allocation : solved->allocations) {
                ASSERT_FALSE(allocation.rate.unlimited);
                EXPECT_TRUE(allocation.rate.finite.equals(9'600, count));
                EXPECT_FALSE(allocation.rate.finite.zero());
            }
        }
    }
}

TEST(NetworkOracleTest, GeneratorIsCanonicalBoundedAndCoversEverySurface) {
    auto first = oracle_script::generate(0x1234, 256);
    auto second = oracle_script::generate(0x1234, 256);
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(first->render(), second->render());
    EXPECT_LT(first->render().size(), 128U * 1024U);

    std::array<bool, kwaque::simulation::testing::oracle_step_kind_count>
      seen{};
    std::array<bool, 14> actions{};
    for (const auto& step : first->steps()) {
        seen[static_cast<std::size_t>(step.kind)] = true;
        actions[static_cast<std::size_t>(step.action)] = true;
    }
    EXPECT_TRUE(std::ranges::all_of(seen, [](bool value) { return value; }));
    for (const auto action : {
           kwaque::runtime::fault_action::error,
           kwaque::runtime::fault_action::delay,
           kwaque::runtime::fault_action::short_operation,
           kwaque::runtime::fault_action::drop,
           kwaque::runtime::fault_action::duplicate,
           kwaque::runtime::fault_action::reorder,
           kwaque::runtime::fault_action::disconnect,
           kwaque::runtime::fault_action::corrupt,
           kwaque::runtime::fault_action::drop_completion,
         }) {
        EXPECT_TRUE(actions[static_cast<std::size_t>(action)]);
    }
}

TEST(NetworkOracleTest, DenseModelReconcilesHistoriesAndDetectsMutation) {
    dense_network_oracle two_endpoints{2};
    ASSERT_TRUE(
      two_endpoints
        .apply(
          oracle_step{
            .kind = oracle_step_kind::bind_exact, .source = 1, .port = 7})
        .has_value());
    ASSERT_TRUE(
      two_endpoints
        .apply(
          oracle_step{.kind = oracle_step_kind::connect_implicit, .target = 1})
        .has_value());
    ASSERT_TRUE(
      two_endpoints
        .apply(
          oracle_step{.kind = oracle_step_kind::write, .target = 1, .value = 4})
        .has_value());
    EXPECT_EQ(two_endpoints.snapshot().visible[1], "aaaa");

    for (std::uint64_t seed = 1; seed <= 200; ++seed) {
        auto script = oracle_script::generate(seed, 128);
        ASSERT_TRUE(script.has_value());
        dense_network_oracle first;
        dense_network_oracle second;
        for (const auto& step : script->steps()) {
            const auto left = first.apply(step);
            const auto right = second.apply(step);
            ASSERT_EQ(left.has_value(), right.has_value()) << script->render();
            if (!left) {
                ASSERT_EQ(left.error(), right.error()) << script->render();
            }
            ASSERT_EQ(first.snapshot(), second.snapshot()) << script->render();
        }
    }

    dense_network_oracle expected;
    dense_network_oracle mutated;
    const std::array setup{
      oracle_step{.kind = oracle_step_kind::bind_exact, .source = 1, .port = 7},
      oracle_step{.kind = oracle_step_kind::connect_implicit, .target = 1},
      oracle_step{
        .kind = oracle_step_kind::write,
        .target = 1,
        .value = 8,
      },
    };
    for (const auto& step : setup) {
        static_cast<void>(expected.apply(step));
        auto changed = step;
        if (changed.kind == oracle_step_kind::write) {
            changed.action = kwaque::runtime::fault_action::drop;
        }
        static_cast<void>(mutated.apply(changed));
    }
    EXPECT_NE(expected.snapshot(), mutated.snapshot());
}

TEST(NetworkOracleTest, DirectedSwizzleClogFiltersAtActualDelivery) {
    dense_network_oracle oracle;
    ASSERT_TRUE(
      oracle
        .apply(
          oracle_step{
            .kind = oracle_step_kind::bind_exact, .source = 1, .port = 9})
        .has_value());
    ASSERT_TRUE(
      oracle
        .apply(
          oracle_step{.kind = oracle_step_kind::connect_implicit, .target = 1})
        .has_value());
    ASSERT_TRUE(
      oracle.apply(oracle_step{.kind = oracle_step_kind::clog, .target = 1})
        .has_value());
    ASSERT_TRUE(
      oracle
        .apply(
          oracle_step{.kind = oracle_step_kind::write, .target = 1, .value = 3})
        .has_value());
    ASSERT_TRUE(
      oracle
        .apply(oracle_step{.kind = oracle_step_kind::partition, .target = 1})
        .has_value());
    ASSERT_TRUE(
      oracle.apply(oracle_step{.kind = oracle_step_kind::unclog, .target = 1})
        .has_value());
    EXPECT_TRUE(oracle.snapshot().visible[1].empty());

    ASSERT_TRUE(
      oracle.apply(oracle_step{.kind = oracle_step_kind::heal, .target = 1})
        .has_value());
    ASSERT_TRUE(
      oracle.apply(oracle_step{.kind = oracle_step_kind::clog, .target = 1})
        .has_value());
    ASSERT_TRUE(
      oracle
        .apply(
          oracle_step{.kind = oracle_step_kind::write, .target = 1, .value = 3})
        .has_value());
    ASSERT_TRUE(
      oracle.apply(oracle_step{.kind = oracle_step_kind::unclog, .target = 1})
        .has_value());
    EXPECT_EQ(oracle.snapshot().visible[1], "aaa");
}

TEST(NetworkOracleTest, DenseModelCoversDnsAndReleasesPacketPressure) {
    dense_network_oracle oracle{2};
    ASSERT_TRUE(
      oracle
        .apply(oracle_step{.kind = oracle_step_kind::dns_record, .value = 2})
        .has_value());
    EXPECT_TRUE(oracle.apply(oracle_step{.kind = oracle_step_kind::dns_resolve})
                  .has_value());
    const auto missing = oracle.apply(
      oracle_step{.kind = oracle_step_kind::dns_resolve, .source = 1});
    ASSERT_FALSE(missing.has_value());
    EXPECT_EQ(missing.error().code(), kwaque::errc::dns_failure);
    EXPECT_EQ(oracle.snapshot().dns_answers, 2U);

    ASSERT_TRUE(
      oracle
        .apply(
          oracle_step{
            .kind = oracle_step_kind::bind_exact, .source = 1, .port = 7})
        .has_value());
    ASSERT_TRUE(
      oracle
        .apply(
          oracle_step{.kind = oracle_step_kind::connect_implicit, .target = 1})
        .has_value());
    const oracle_step write{
      .kind = oracle_step_kind::write, .target = 1, .value = 1};
    for (std::size_t index = 0;
         index < kwaque::simulation::testing::oracle_maximum_packets;
         ++index) {
        ASSERT_TRUE(oracle.apply(write).has_value());
    }
    const auto saturated = oracle.apply(write);
    ASSERT_FALSE(saturated.has_value());
    EXPECT_EQ(saturated.error().code(), kwaque::errc::queue_full);
    EXPECT_EQ(
      oracle.snapshot().live_packets,
      kwaque::simulation::testing::oracle_maximum_packets);
    ASSERT_TRUE(
      oracle.apply(oracle_step{.kind = oracle_step_kind::read, .source = 1})
        .has_value());
    EXPECT_EQ(oracle.snapshot().live_packets, 0U);
    EXPECT_TRUE(oracle.apply(write).has_value());
}
