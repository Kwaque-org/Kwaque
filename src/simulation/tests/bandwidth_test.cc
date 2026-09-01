#include "src/simulation/bandwidth.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <initializer_list>
#include <limits>
#include <memory>
#include <optional>
#include <utility>

namespace {

using kwaque::simulation::bandwidth_allocation;
using kwaque::simulation::bandwidth_capacity;
using kwaque::simulation::bandwidth_constraint;
using kwaque::simulation::bandwidth_flow;
using kwaque::simulation::bandwidth_fraction;
using kwaque::simulation::bandwidth_planner;
using kwaque::simulation::bandwidth_resource_key;

bandwidth_constraint
limited(std::uint8_t domain, std::uint64_t resource, std::uint64_t capacity) {
    return bandwidth_constraint{
      .resource = bandwidth_resource_key::numeric(domain, resource),
      .capacity = bandwidth_capacity::finite(capacity),
    };
}

bandwidth_flow flow(
  std::uint64_t id, std::initializer_list<bandwidth_constraint> constraints) {
    bandwidth_flow result{.id = id};
    for (const auto& constraint : constraints) {
        result.constraints[result.constraint_count++] = constraint;
    }
    return result;
}

const bandwidth_allocation&
allocation(const bandwidth_planner& planner, std::uint64_t id) {
    for (std::size_t index = 0; index < planner.allocation_count(); ++index) {
        if (planner.allocation_at(index).flow == id) {
            return planner.allocation_at(index);
        }
    }
    ADD_FAILURE() << "missing bandwidth allocation";
    return planner.allocation_at(0);
}

TEST(bandwidth_test, validates_workspace_and_flow_shape) {
    EXPECT_FALSE(bandwidth_planner::make(0).has_value());
    EXPECT_FALSE(
      bandwidth_planner::make(kwaque::simulation::maximum_bandwidth_flows + 1U)
        .has_value());

    auto made = bandwidth_planner::make(2);
    ASSERT_TRUE(made.has_value());
    auto planner = std::move(*made);
    EXPECT_FALSE(planner->add_flow({}).has_value());
    ASSERT_TRUE(planner->add_flow(flow(1, {limited(1, 1, 100)})).has_value());
    EXPECT_FALSE(planner->add_flow(flow(1, {limited(1, 2, 100)})).has_value());
    auto too_many = flow(2, {});
    too_many.constraint_count = static_cast<std::uint8_t>(
      kwaque::simulation::maximum_bandwidth_constraints_per_flow + 1U);
    EXPECT_FALSE(planner->add_flow(too_many).has_value());
    ASSERT_TRUE(planner->add_flow(flow(2, {limited(1, 2, 100)})).has_value());
    EXPECT_FALSE(planner->add_flow(flow(3, {})).has_value());
}

TEST(bandwidth_test, progressively_splits_one_shared_resource) {
    auto made = bandwidth_planner::make(2);
    ASSERT_TRUE(made.has_value());
    auto planner = std::move(*made);
    ASSERT_TRUE(
      planner->add_flow(flow(1, {limited(1, 10, 1'000)})).has_value());
    ASSERT_TRUE(
      planner->add_flow(flow(2, {limited(1, 10, 1'000)})).has_value());
    ASSERT_TRUE(planner->solve().has_value());

    ASSERT_FALSE(allocation(*planner, 1).rate.is_unlimited());
    EXPECT_TRUE(allocation(*planner, 1).rate.finite_value().equals(500, 1));
    EXPECT_TRUE(allocation(*planner, 2).rate.finite_value().equals(500, 1));
    EXPECT_EQ(planner->resource_count(), 1U);
    EXPECT_EQ(planner->membership_count(), 2U);
    EXPECT_EQ(
      planner->allocation_digest().words,
      (std::array<std::uint64_t, 4>{
        UINT64_C(0xc39f9e3b6b5adeb5),
        UINT64_C(0x44a0107ee01fa771),
        UINT64_C(0x74853e9eb083f7d1),
        UINT64_C(0x78dcf32ff80c1ab1)}));

    auto reversed_made = bandwidth_planner::make(2);
    ASSERT_TRUE(reversed_made.has_value());
    auto reversed = std::move(*reversed_made);
    ASSERT_TRUE(
      reversed->add_flow(flow(2, {limited(1, 10, 1'000)})).has_value());
    ASSERT_TRUE(
      reversed->add_flow(flow(1, {limited(1, 10, 1'000)})).has_value());
    ASSERT_TRUE(reversed->solve().has_value());
    EXPECT_EQ(reversed->allocation_at(0).flow, 1U);
    EXPECT_EQ(reversed->allocation_at(1).flow, 2U);
    EXPECT_EQ(reversed->allocation_digest(), planner->allocation_digest());
}

TEST(bandwidth_test, progressively_splits_shared_ingress) {
    auto made = bandwidth_planner::make(2);
    ASSERT_TRUE(made.has_value());
    auto planner = std::move(*made);
    ASSERT_TRUE(
      planner->add_flow(flow(1, {limited(3, 20, 2'000)})).has_value());
    ASSERT_TRUE(
      planner->add_flow(flow(2, {limited(3, 20, 2'000)})).has_value());
    ASSERT_TRUE(planner->solve().has_value());
    EXPECT_TRUE(allocation(*planner, 1).rate.finite_value().equals(1'000, 1));
    EXPECT_TRUE(allocation(*planner, 2).rate.finite_value().equals(1'000, 1));
}

TEST(bandwidth_test, rejects_incoherent_shared_resource_capacity) {
    auto made = bandwidth_planner::make(2);
    ASSERT_TRUE(made.has_value());
    auto planner = std::move(*made);
    ASSERT_TRUE(planner->add_flow(flow(1, {limited(1, 10, 100)})).has_value());
    ASSERT_TRUE(planner->add_flow(flow(2, {limited(1, 10, 200)})).has_value());
    const auto solved = planner->solve();
    ASSERT_FALSE(solved.has_value());
    EXPECT_EQ(solved.error().code(), kwaque::errc::invalid_argument);
}

TEST(bandwidth_test, freezes_multi_resource_bottlenecks_in_order) {
    auto made = bandwidth_planner::make(2);
    ASSERT_TRUE(made.has_value());
    auto planner = std::move(*made);
    ASSERT_TRUE(planner
                  ->add_flow(flow(
                    1,
                    {limited(1, 1, 50'000),
                     limited(2, 11, 100'000),
                     limited(3, 21, 1'000)}))
                  .has_value());
    ASSERT_TRUE(planner
                  ->add_flow(flow(
                    2,
                    {limited(1, 1, 50'000),
                     limited(2, 12, 100'000),
                     limited(3, 22, 500'000)}))
                  .has_value());
    ASSERT_TRUE(planner->solve().has_value());

    EXPECT_TRUE(allocation(*planner, 1).rate.finite_value().equals(1'000, 1));
    EXPECT_TRUE(allocation(*planner, 2).rate.finite_value().equals(49'000, 1));
}

TEST(bandwidth_test, solves_absolute_flow_and_membership_bound) {
    auto made = bandwidth_planner::make(
      kwaque::simulation::maximum_bandwidth_flows);
    ASSERT_TRUE(made.has_value());
    auto planner = std::move(*made);
    constexpr auto capacity = std::numeric_limits<std::uint64_t>::max();
    for (std::uint64_t id = 1;
         id <= kwaque::simulation::maximum_bandwidth_flows;
         ++id) {
        ASSERT_TRUE(planner
                      ->add_flow(flow(
                        id,
                        {limited(1, 1, capacity),
                         limited(2, id, capacity),
                         limited(3, id, capacity)}))
                      .has_value());
    }
    ASSERT_TRUE(planner->solve().has_value());
    EXPECT_EQ(
      planner->allocation_count(), kwaque::simulation::maximum_bandwidth_flows);
    EXPECT_EQ(
      planner->resource_count(),
      1U + 2U * kwaque::simulation::maximum_bandwidth_flows);
    EXPECT_EQ(
      planner->membership_count(),
      kwaque::simulation::maximum_bandwidth_resources);
    for (std::size_t index = 0; index < planner->allocation_count(); ++index) {
        EXPECT_TRUE(planner->allocation_at(index).rate.finite_value().equals(
          capacity, kwaque::simulation::maximum_bandwidth_flows));
    }
}

TEST(bandwidth_test, preserves_fractional_progress_and_ceil_duration) {
    auto half = bandwidth_fraction::ratio(1, 2);
    ASSERT_TRUE(half.has_value());
    const auto rate = kwaque::simulation::bandwidth_rate::finite(*half);
    const auto initial = bandwidth_fraction::whole(1);
    const auto partial = kwaque::simulation::bandwidth_transfer(
      rate, kwaque::runtime::monotonic_duration{500'000'000}, initial);
    EXPECT_TRUE(partial.equals(3, 4));

    const auto duration = kwaque::simulation::bandwidth_duration(rate, partial);
    ASSERT_TRUE(duration.has_value());
    ASSERT_TRUE(duration->has_value());
    EXPECT_EQ((*duration)->nanoseconds(), 1'500'000'000U);
}

TEST(bandwidth_test, distinguishes_zero_and_unlimited_rates) {
    auto made = bandwidth_planner::make(2);
    ASSERT_TRUE(made.has_value());
    auto planner = std::move(*made);
    ASSERT_TRUE(planner->add_flow(flow(1, {})).has_value());
    ASSERT_TRUE(planner->add_flow(flow(2, {limited(1, 2, 0)})).has_value());
    ASSERT_TRUE(planner->solve().has_value());
    EXPECT_TRUE(allocation(*planner, 1).rate.is_unlimited());
    EXPECT_FALSE(allocation(*planner, 2).rate.is_unlimited());
    EXPECT_TRUE(allocation(*planner, 2).rate.finite_value().zero());
    EXPECT_EQ(
      planner->allocation_digest().words,
      (std::array<std::uint64_t, 4>{
        UINT64_C(0x1a50659551a81525),
        UINT64_C(0xf969017377f64676),
        UINT64_C(0x9ec6f377f86c03b0),
        UINT64_C(0xbe66b86dd5323361)}));

    const auto parked = kwaque::simulation::bandwidth_duration(
      allocation(*planner, 2).rate, bandwidth_fraction::whole(1));
    ASSERT_TRUE(parked.has_value());
    EXPECT_FALSE(parked->has_value());
}

TEST(bandwidth_test, matches_representable_bit_rate_vectors) {
    const std::array cases{
      std::pair{1'000'000'000ULL, 512ULL},
      std::pair{8'000'000'000ULL, 64ULL},
      std::pair{1'000'000ULL, 512'000ULL},
    };
    for (const auto [bits_per_second, expected_ns] : cases) {
        const auto duration
          = kwaque::simulation::bit_rate_transmission_duration(
            kwaque::byte_count{64}, bits_per_second);
        ASSERT_TRUE(duration.has_value());
        ASSERT_TRUE(duration->has_value());
        EXPECT_EQ((*duration)->nanoseconds(), expected_ns);
    }

    const auto arbitrary = kwaque::simulation::bytes_per_second_from_bits(10);
    ASSERT_FALSE(arbitrary.is_unlimited());
    EXPECT_TRUE(arbitrary.finite_value().equals(5, 4));
}

} // namespace
