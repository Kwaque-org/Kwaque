#include "src/base/error.h"
#include "src/base/units.h"
#include "src/resource/resource_config.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <concepts>
#include <cstdint>
#include <limits>

namespace kwaque::resource {

namespace {

void expect_fully_accounted(std::uint64_t total) {
    const auto config = resource_config::from_total_memory(byte_count{total});
    ASSERT_TRUE(config.has_value());

    auto accounted = config->reactor_headroom();
    for (const auto classification : all_workload_classes) {
        const auto next = accounted.checked_add(config->budget(classification));
        ASSERT_TRUE(next.has_value());
        accounted = *next;
        EXPECT_GT(config->budget(classification).value(), 0U);
    }
    EXPECT_LE(accounted, config->total_memory());
    EXPECT_EQ(accounted.value(), total);
}

void expect_fully_accounted(std::uint64_t total, std::uint64_t headroom) {
    const auto config = resource_config::from_total_memory(
      byte_count{total}, byte_count{headroom});
    ASSERT_TRUE(config.has_value());
    EXPECT_EQ(config->reactor_headroom(), byte_count{headroom});

    auto accounted = config->reactor_headroom();
    for (const auto classification : all_workload_classes) {
        const auto next = accounted.checked_add(config->budget(classification));
        ASSERT_TRUE(next.has_value());
        accounted = *next;
        EXPECT_GT(config->budget(classification).value(), 0U);
    }
    EXPECT_EQ(accounted.value(), total);
}

static_assert(!std::convertible_to<std::uint64_t, byte_count>);
static_assert(!std::convertible_to<byte_count, std::uint64_t>);

} // namespace

TEST(resource_config_test, rejects_memory_below_the_viable_minimum) {
    const auto minimum = resource_config::minimum_total_memory().value();
    const auto rejected = resource_config::from_total_memory(
      byte_count{minimum - 1});
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error(), make_error_code(errc::resource_exhausted));
    expect_fully_accounted(minimum);
    EXPECT_EQ(
      resource_config::from_total_memory(byte_count{minimum})
        ->reactor_headroom(),
      resource_config::default_reactor_headroom());
    EXPECT_FALSE(
      resource_config::from_total_memory(byte_count{minimum}, byte_count{})
        .has_value());
    EXPECT_FALSE(
      resource_config::from_total_memory(
        byte_count{minimum}, byte_count{minimum})
        .has_value());
    EXPECT_FALSE(
      resource_config::from_total_memory(
        byte_count{minimum}, byte_count{minimum - 14})
        .has_value());
    expect_fully_accounted(minimum, 8ULL * 1024ULL * 1024ULL);
}

TEST(resource_config_test, checked_partition_never_exceeds_supplied_memory) {
    constexpr std::uint64_t mebibyte = 1024ULL * 1024ULL;
    const std::array<std::uint64_t, 8> boundary_values{
      64ULL * mebibyte,
      64ULL * mebibyte + 1,
      128ULL * mebibyte,
      192ULL * mebibyte,
      1024ULL * mebibyte,
      std::numeric_limits<std::uint64_t>::max() / 2,
      std::numeric_limits<std::uint64_t>::max() - 1,
      std::numeric_limits<std::uint64_t>::max(),
    };
    for (const auto total : boundary_values) {
        expect_fully_accounted(total);
    }

    const auto minimum = resource_config::minimum_total_memory().value();
    for (std::uint64_t sample = 0; sample < 2048; ++sample) {
        expect_fully_accounted(minimum + sample * 104729ULL);
    }
}

TEST(resource_config_test, production_floor_includes_explicit_reservations) {
    constexpr std::uint64_t mebibyte = 1024ULL * 1024ULL;
    const memory_reservations reservations{
      .reactor_headroom = resource_config::default_reactor_headroom(),
      .admin_memory = byte_count{2ULL * mebibyte}};
    const auto minimum = resource_config::production_baseline_memory().value()
                         + reservations.admin_memory.value();
    const auto rejected = resource_config::from_production_memory(
      byte_count{minimum - 1}, reservations);
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error(), make_error_code(errc::resource_exhausted));

    const auto accepted = resource_config::from_production_memory(
      byte_count{minimum}, reservations);
    ASSERT_TRUE(accepted.has_value());
    EXPECT_EQ(accepted->admin_memory_reservation(), reservations.admin_memory);
    byte_count shares;
    for (const auto budget : accepted->budgets()) {
        const auto next = shares.checked_add(budget);
        ASSERT_TRUE(next.has_value());
        shares = *next;
    }
    // The suitability baseline is included in the available memory. Only
    // disjoint reservations and uncharged headroom reduce workload shares.
    EXPECT_EQ(shares.value(), 112ULL * mebibyte);
    EXPECT_EQ(
      shares.value() + accepted->reactor_headroom().value()
        + accepted->admin_memory_reservation().value(),
      minimum);
    EXPECT_EQ(
      resource_config::recommended_total_memory().value(), 1024ULL * mebibyte);
}

TEST(
  resource_config_test, explicit_development_capacity_keeps_its_small_floor) {
    const memory_reservations reservations{
      .reactor_headroom = resource_config::default_reactor_headroom(),
      .admin_memory = byte_count{2ULL * 1024ULL * 1024ULL}};
    const auto minimum = resource_config::minimum_total_memory();
    const auto development = resource_config::from_total_memory(
      minimum, reservations);
    ASSERT_TRUE(development.has_value());
    EXPECT_EQ(development->total_memory(), minimum);
    EXPECT_FALSE(
      resource_config::from_production_memory(minimum, reservations)
        .has_value());
}

TEST(
  resource_config_test,
  disjoint_reservations_never_overflow_or_overlap_shares) {
    constexpr std::uint64_t mebibyte = 1024ULL * 1024ULL;
    constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();
    const auto excessive_floor = resource_config::from_production_memory(
      byte_count{maximum},
      memory_reservations{
        .reactor_headroom = byte_count{1},
        .admin_memory = byte_count{maximum}});
    ASSERT_FALSE(excessive_floor.has_value());
    EXPECT_EQ(excessive_floor.error(), make_error_code(errc::out_of_range));

    const auto overflowing_reserves = resource_config::from_total_memory(
      byte_count{maximum},
      memory_reservations{
        .reactor_headroom = byte_count{2},
        .admin_memory = byte_count{maximum}});
    ASSERT_FALSE(overflowing_reserves.has_value());
    EXPECT_EQ(
      overflowing_reserves.error(), make_error_code(errc::out_of_range));

    const std::array<std::uint64_t, 3> totals{
      130ULL * mebibyte, 256ULL * mebibyte, maximum};
    for (const auto total : totals) {
        for (const auto admin : {0ULL, 1ULL, 2ULL * mebibyte}) {
            const memory_reservations reservations{
              .reactor_headroom = resource_config::default_reactor_headroom(),
              .admin_memory = byte_count{admin}};
            const auto configured = resource_config::from_production_memory(
              byte_count{total}, reservations);
            ASSERT_TRUE(configured.has_value());
            auto accounted = reservations.reactor_headroom.checked_add(
              reservations.admin_memory);
            ASSERT_TRUE(accounted.has_value());
            for (const auto budget : configured->budgets()) {
                accounted = accounted->checked_add(budget);
                ASSERT_TRUE(accounted.has_value());
                EXPECT_GT(budget.value(), 0U);
            }
            EXPECT_EQ(accounted->value(), total);
        }
    }

    const auto exhausted = resource_config::from_total_memory(
      byte_count{130ULL * mebibyte},
      memory_reservations{
        .reactor_headroom = byte_count{128ULL * mebibyte},
        .admin_memory = byte_count{2ULL * mebibyte}});
    ASSERT_FALSE(exhausted.has_value());
    EXPECT_EQ(exhausted.error(), make_error_code(errc::resource_exhausted));
}

TEST(
  resource_config_test, shared_partition_fits_every_unequal_native_capacity) {
    constexpr std::uint64_t mebibyte = 1024ULL * 1024ULL;
    const std::array capacities{
      byte_count{256ULL * mebibyte},
      byte_count{130ULL * mebibyte},
      byte_count{192ULL * mebibyte},
      byte_count{512ULL * mebibyte}};
    const auto minimum = *std::min_element(
      capacities.begin(), capacities.end());
    const auto configured = resource_config::from_production_memory(
      minimum,
      memory_reservations{
        .reactor_headroom = resource_config::default_reactor_headroom(),
        .admin_memory = byte_count{2ULL * mebibyte}});
    ASSERT_TRUE(configured.has_value());
    auto accounted = configured->reactor_headroom().checked_add(
      configured->admin_memory_reservation());
    ASSERT_TRUE(accounted.has_value());
    for (const auto budget : configured->budgets()) {
        accounted = accounted->checked_add(budget);
        ASSERT_TRUE(accounted.has_value());
    }
    EXPECT_EQ(*accounted, minimum);
    for (const auto capacity : capacities) {
        EXPECT_LE(*accounted, capacity);
    }
}

} // namespace kwaque::resource
