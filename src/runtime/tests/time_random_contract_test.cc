#include "src/runtime/random.h"
#include "src/runtime/time.h"

#include <gtest/gtest.h>

#include <array>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>
#include <type_traits>

namespace {

using kwaque::runtime::monotonic_duration;
using kwaque::runtime::monotonic_time;
using kwaque::runtime::wall_time;

struct test_monotonic_clock final {
    static monotonic_time now() noexcept { return monotonic_time{17}; }
};

struct test_wall_clock final {
    static wall_time now() noexcept { return wall_time{-23}; }
};

struct test_clock_backend final {
    using monotonic_clock = test_monotonic_clock;
    using wall_clock = test_wall_clock;
};

struct invalid_monotonic_clock final {
    static wall_time now() noexcept { return wall_time{}; }
};

struct sequence_source final {
    std::array<std::uint64_t, 4> words{};
    std::size_t next{0};

    std::uint64_t next_u64() noexcept { return words[next++]; }
};

static_assert(kwaque::runtime::monotonic_clock<test_monotonic_clock>);
static_assert(kwaque::runtime::wall_clock<test_wall_clock>);
static_assert(kwaque::runtime::clock_backend<test_clock_backend>);
static_assert(!kwaque::runtime::monotonic_clock<invalid_monotonic_clock>);
static_assert(kwaque::runtime::random_source<sequence_source>);
static_assert(std::is_trivially_copyable_v<monotonic_duration>);
static_assert(std::is_trivially_copyable_v<monotonic_time>);
static_assert(std::is_trivially_copyable_v<wall_time>);
static_assert(sizeof(monotonic_duration) == sizeof(std::uint64_t));
static_assert(sizeof(monotonic_time) == sizeof(std::uint64_t));
static_assert(sizeof(wall_time) == sizeof(std::int64_t));
static_assert(!std::convertible_to<std::uint64_t, monotonic_duration>);
static_assert(!std::convertible_to<std::uint64_t, monotonic_time>);
static_assert(!std::convertible_to<std::int64_t, wall_time>);
static_assert(!std::convertible_to<monotonic_time, wall_time>);
static_assert(!std::convertible_to<wall_time, monotonic_time>);

TEST(TimeContractTest, ChecksMonotonicArithmetic) {
    constexpr monotonic_time start{100};
    constexpr monotonic_duration elapsed{25};
    constexpr auto end = start.checked_add(elapsed);
    static_assert(end.has_value() && end->nanoseconds() == 125);
    static_assert(
      end->checked_elapsed_since(start)->nanoseconds()
      == elapsed.nanoseconds());

    EXPECT_FALSE(start.checked_sub(monotonic_duration{101}).has_value());
    EXPECT_FALSE(start.checked_elapsed_since(monotonic_time{101}).has_value());
    EXPECT_FALSE(
      monotonic_time::maximum().checked_add(monotonic_duration{1}).has_value());
    EXPECT_FALSE(
      monotonic_duration::maximum()
        .checked_add(monotonic_duration{1})
        .has_value());
}

TEST(TimeContractTest, ChecksSignedWallTimeArithmeticWithoutClockMixing) {
    constexpr wall_time before_epoch{-1};
    constexpr auto epoch = before_epoch.checked_add(monotonic_duration{1});
    static_assert(epoch.has_value() && epoch->unix_nanoseconds() == 0);

    const wall_time minimum{std::numeric_limits<std::int64_t>::min()};
    const wall_time maximum{std::numeric_limits<std::int64_t>::max()};
    EXPECT_FALSE(minimum.checked_sub(monotonic_duration{1}).has_value());
    EXPECT_FALSE(maximum.checked_add(monotonic_duration{1}).has_value());
    EXPECT_EQ(
      maximum.checked_sub(monotonic_duration::maximum())->unix_nanoseconds(),
      std::numeric_limits<std::int64_t>::min());
}

TEST(RandomContractTest, ValidatesBoundsAndUsesRejectionSampling) {
    sequence_source source{{0, std::numeric_limits<std::uint64_t>::max()}};

    const auto invalid = kwaque::runtime::uniform_u64(source, 0);
    ASSERT_FALSE(invalid.has_value());
    EXPECT_EQ(invalid.error().code(), kwaque::errc::invalid_argument);
    EXPECT_EQ(source.next, 0U);

    const auto singleton = kwaque::runtime::uniform_u64(source, 1);
    ASSERT_TRUE(singleton.has_value());
    EXPECT_EQ(*singleton, 0U);
    EXPECT_EQ(source.next, 0U);

    const auto selected = kwaque::runtime::uniform_u64(source, 3);
    ASSERT_TRUE(selected.has_value());
    EXPECT_EQ(*selected, 2U);
    EXPECT_EQ(source.next, 2U);

    sequence_source maximum_source{{std::numeric_limits<std::uint64_t>::max()}};
    const auto below_maximum = kwaque::runtime::uniform_u64(
      maximum_source, std::numeric_limits<std::uint64_t>::max());
    ASSERT_TRUE(below_maximum.has_value());
    EXPECT_EQ(*below_maximum, std::numeric_limits<std::uint64_t>::max() - 1);
}

TEST(RandomContractTest, ValidatesRationalChanceWithoutUnneededDraws) {
    EXPECT_FALSE(kwaque::runtime::probability_ratio::make(1, 0).has_value());
    EXPECT_FALSE(kwaque::runtime::probability_ratio::make(3, 2).has_value());

    const auto never = kwaque::runtime::probability_ratio::make(0, 7);
    const auto always = kwaque::runtime::probability_ratio::make(7, 7);
    const auto half = kwaque::runtime::probability_ratio::make(1, 2);
    ASSERT_TRUE(never.has_value());
    ASSERT_TRUE(always.has_value());
    ASSERT_TRUE(half.has_value());

    sequence_source source{{0, std::numeric_limits<std::uint64_t>::max()}};
    EXPECT_FALSE(kwaque::runtime::chance(source, *never));
    EXPECT_TRUE(kwaque::runtime::chance(source, *always));
    EXPECT_EQ(source.next, 0U);
    EXPECT_TRUE(kwaque::runtime::chance(source, *half));
    EXPECT_FALSE(kwaque::runtime::chance(source, *half));
    EXPECT_EQ(source.next, 2U);
}

TEST(RandomContractTest, FillsCanonicalLittleEndianWordsAndTail) {
    sequence_source source{
      {UINT64_C(0x0807060504030201), UINT64_C(0x11100f0e0d0c0b0a)}};
    std::array<std::byte, 11> output{};
    kwaque::runtime::fill_bytes(source, std::span<std::byte>{output}.first(0));
    EXPECT_EQ(source.next, 0U);
    kwaque::runtime::fill_bytes(source, std::span<std::byte>{output});

    const std::array expected{
      std::byte{0x01},
      std::byte{0x02},
      std::byte{0x03},
      std::byte{0x04},
      std::byte{0x05},
      std::byte{0x06},
      std::byte{0x07},
      std::byte{0x08},
      std::byte{0x0a},
      std::byte{0x0b},
      std::byte{0x0c},
    };
    EXPECT_TRUE(output == expected);
    EXPECT_EQ(source.next, 2U);
}

} // namespace
