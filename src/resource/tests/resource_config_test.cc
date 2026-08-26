#include "src/base/error.h"
#include "src/base/units.h"
#include "src/resource/resource_config.h"

#include <gtest/gtest.h>

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

} // namespace kwaque::resource
