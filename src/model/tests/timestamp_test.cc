#include "src/model/position.h"
#include "src/runtime/time.h"

#include <gtest/gtest.h>

#include <array>
#include <concepts>
#include <cstdint>
#include <limits>
#include <optional>

namespace {

using kwaque::model::checked_timestamp_delta;
using kwaque::model::checked_timestamp_from_delta;
using kwaque::runtime::monotonic_duration;
using kwaque::runtime::monotonic_time;
using kwaque::runtime::wall_time;

constexpr auto minimum = std::numeric_limits<std::int64_t>::min();
constexpr auto maximum = std::numeric_limits<std::int64_t>::max();

template<typename Base, typename Value>
concept timestamp_difference_expression = requires(Base base, Value value) {
    checked_timestamp_delta(base, value);
};

template<typename Base, typename Delta>
concept timestamp_reconstruction_expression = requires(Base base, Delta delta) {
    checked_timestamp_from_delta(base, delta);
};

static_assert(timestamp_difference_expression<wall_time, wall_time>);
static_assert(!timestamp_difference_expression<monotonic_time, wall_time>);
static_assert(!timestamp_difference_expression<wall_time, monotonic_time>);
static_assert(!timestamp_difference_expression<std::int64_t, wall_time>);
static_assert(!timestamp_difference_expression<wall_time, std::int64_t>);
static_assert(timestamp_reconstruction_expression<wall_time, std::int64_t>);
static_assert(
  !timestamp_reconstruction_expression<monotonic_time, std::int64_t>);
static_assert(
  !timestamp_reconstruction_expression<wall_time, monotonic_duration>);
static_assert(std::same_as<
              decltype(checked_timestamp_delta(wall_time{}, wall_time{})),
              std::optional<std::int64_t>>);
static_assert(std::same_as<
              decltype(checked_timestamp_from_delta(wall_time{}, 0)),
              std::optional<wall_time>>);
static_assert(noexcept(checked_timestamp_delta(wall_time{}, wall_time{})));
static_assert(noexcept(checked_timestamp_from_delta(wall_time{}, 0)));

constexpr auto minimum_delta = checked_timestamp_delta(
  wall_time{0}, wall_time{minimum});
static_assert(minimum_delta.has_value() && *minimum_delta == minimum);
static_assert(
  !checked_timestamp_delta(wall_time{minimum}, wall_time{0}).has_value());
constexpr auto minimum_reconstructed = checked_timestamp_from_delta(
  wall_time{0}, minimum);
static_assert(
  minimum_reconstructed.has_value()
  && minimum_reconstructed->unix_nanoseconds() == minimum);
static_assert(
  !checked_timestamp_from_delta(wall_time{minimum}, -1).has_value());

struct timestamp_case {
    std::int64_t base;
    std::int64_t argument;
    std::optional<std::int64_t> expected;
};

constexpr std::array difference_cases{
  timestamp_case{0, 0, 0},
  timestamp_case{-1, 0, 1},
  timestamp_case{0, -1, -1},
  timestamp_case{7, 3, -4},
  timestamp_case{3, 7, 4},
  timestamp_case{minimum, minimum, 0},
  timestamp_case{maximum, maximum, 0},
  timestamp_case{minimum, minimum + 1, 1},
  timestamp_case{maximum, maximum - 1, -1},
  timestamp_case{0, minimum, minimum},
  timestamp_case{minimum, 0, std::nullopt},
  timestamp_case{maximum, -1, minimum},
  timestamp_case{-1, maximum, std::nullopt},
  timestamp_case{minimum, -1, maximum},
  timestamp_case{maximum, 0, -maximum},
  timestamp_case{0, maximum, maximum},
  timestamp_case{-1, minimum, minimum + 1},
  timestamp_case{minimum, maximum, std::nullopt},
  timestamp_case{maximum, minimum, std::nullopt},
  timestamp_case{minimum + 1, 0, maximum},
  timestamp_case{-2, maximum - 1, std::nullopt},
  timestamp_case{maximum - 1, -2, minimum},
  timestamp_case{1, minimum, std::nullopt},
  timestamp_case{minimum, 1, std::nullopt}};

constexpr std::array reconstruction_cases{
  timestamp_case{0, 0, 0},
  timestamp_case{-1, 1, 0},
  timestamp_case{0, -1, -1},
  timestamp_case{7, -4, 3},
  timestamp_case{minimum, 0, minimum},
  timestamp_case{maximum, 0, maximum},
  timestamp_case{0, minimum, minimum},
  timestamp_case{maximum, minimum, -1},
  timestamp_case{-1, maximum, maximum - 1},
  timestamp_case{minimum, maximum, -1},
  timestamp_case{minimum, 1, minimum + 1},
  timestamp_case{minimum, -1, std::nullopt},
  timestamp_case{maximum, 1, std::nullopt},
  timestamp_case{maximum, -1, maximum - 1},
  timestamp_case{minimum, minimum, std::nullopt},
  timestamp_case{maximum, maximum, std::nullopt},
  timestamp_case{1, minimum, minimum + 1},
  timestamp_case{-1, minimum, std::nullopt},
  timestamp_case{0, maximum, maximum},
  timestamp_case{1, maximum, std::nullopt},
  timestamp_case{minimum + 1, maximum, 0},
  timestamp_case{maximum - 1, minimum, -2}};

TEST(TimestampTest, CheckedDifferenceMatchesLiteralSignedBoundaryCases) {
    for (const auto& input : difference_cases) {
        const wall_time base{input.base};
        const wall_time value{input.argument};
        const auto delta = checked_timestamp_delta(base, value);
        ASSERT_EQ(delta.has_value(), input.expected.has_value())
          << input.base << ':' << input.argument;
        if (input.expected) {
            EXPECT_EQ(*delta, *input.expected);
            const auto restored = checked_timestamp_from_delta(base, *delta);
            ASSERT_TRUE(restored.has_value());
            EXPECT_EQ(*restored, value);
        }
        EXPECT_EQ(base.unix_nanoseconds(), input.base);
        EXPECT_EQ(value.unix_nanoseconds(), input.argument);
    }
}

TEST(TimestampTest, CheckedReconstructionMatchesLiteralSignedBoundaryCases) {
    for (const auto& input : reconstruction_cases) {
        const wall_time base{input.base};
        const auto value = checked_timestamp_from_delta(base, input.argument);
        ASSERT_EQ(value.has_value(), input.expected.has_value())
          << input.base << ':' << input.argument;
        if (input.expected) {
            EXPECT_EQ(value->unix_nanoseconds(), *input.expected);
            const auto restored_delta = checked_timestamp_delta(base, *value);
            ASSERT_TRUE(restored_delta.has_value());
            EXPECT_EQ(*restored_delta, input.argument);
        }
        EXPECT_EQ(base.unix_nanoseconds(), input.base);
    }
}

} // namespace
