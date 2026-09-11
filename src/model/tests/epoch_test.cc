#include "src/base/error.h"
#include "src/model/epoch.h"

#include <absl/hash/hash.h>
#include <absl/hash/hash_testing.h>
#include <gtest/gtest.h>

#include <array>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <type_traits>
#include <utility>

namespace {

using kwaque::model::lease_epoch;
using kwaque::model::producer_epoch;
using kwaque::model::range_manifest_generation;
using kwaque::model::range_routing_epoch;
using kwaque::model::segment_generation;

constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();

template<typename Epoch>
class EpochTest : public ::testing::Test {
    static_assert(sizeof(Epoch) == sizeof(std::uint64_t));
    static_assert(std::is_standard_layout_v<Epoch>);
    static_assert(std::is_trivially_copyable_v<Epoch>);
    static_assert(std::is_nothrow_default_constructible_v<Epoch>);
    static_assert(std::is_nothrow_copy_constructible_v<Epoch>);
    static_assert(std::is_nothrow_copy_assignable_v<Epoch>);
    static_assert(std::is_nothrow_move_constructible_v<Epoch>);
    static_assert(std::is_nothrow_move_assignable_v<Epoch>);
    static_assert(std::is_nothrow_destructible_v<Epoch>);
    static_assert(!std::constructible_from<Epoch, std::uint64_t>);
    static_assert(!std::convertible_to<Epoch, std::uint64_t>);
    static_assert(!std::convertible_to<std::uint64_t, Epoch>);
    static_assert(std::same_as<
                  decltype(std::declval<const Epoch&>().value()),
                  std::uint64_t>);
    static_assert(Epoch{}.value() == 0U);
    static_assert(!Epoch{}.is_valid());
    static_assert(noexcept(Epoch::make(1)));
    static_assert(noexcept(std::declval<const Epoch&>().checked_successor()));
};

using epoch_types = ::testing::Types<
  segment_generation,
  range_routing_epoch,
  lease_epoch,
  producer_epoch,
  range_manifest_generation>;
TYPED_TEST_SUITE(EpochTest, epoch_types);

TYPED_TEST(EpochTest, DistinguishesUninitializedFromPublishedValues) {
    const TypeParam staging;
    EXPECT_FALSE(staging.is_valid());
    EXPECT_EQ(staging.value(), 0U);

    const auto zero = TypeParam::make(0);
    ASSERT_FALSE(zero.has_value());
    EXPECT_EQ(zero.error(), kwaque::errc::invalid_argument);
    const auto uninitialized_successor = staging.checked_successor();
    ASSERT_FALSE(uninitialized_successor.has_value());
    EXPECT_EQ(uninitialized_successor.error(), kwaque::errc::invalid_argument);
    EXPECT_EQ(staging.value(), 0U);

    for (const auto raw : {std::uint64_t{1}, maximum - 1U, maximum}) {
        const auto value = TypeParam::make(raw);
        ASSERT_TRUE(value.has_value()) << raw;
        EXPECT_TRUE(value->is_valid());
        EXPECT_EQ(value->value(), raw);
    }
}

TYPED_TEST(EpochTest, SuccessorPreservesItsInputAndExhaustsOnlyAtMaximum) {
    for (const auto raw : {std::uint64_t{1}, maximum - 1U}) {
        const auto value = TypeParam::make(raw);
        ASSERT_TRUE(value.has_value());
        const auto successor = value->checked_successor();
        ASSERT_TRUE(successor.has_value()) << raw;
        EXPECT_EQ(successor->value(), raw + 1U);
        EXPECT_GT(*successor, *value);
        EXPECT_EQ(value->value(), raw);
    }

    const auto last = TypeParam::make(maximum);
    ASSERT_TRUE(last.has_value());
    const auto exhausted = last->checked_successor();
    ASSERT_FALSE(exhausted.has_value());
    EXPECT_EQ(exhausted.error(), kwaque::errc::out_of_range);
    EXPECT_TRUE(last->is_valid());
    EXPECT_EQ(last->value(), maximum);
}

TYPED_TEST(EpochTest, ComparesUnsignedValuesAndHashesEveryBit) {
    const auto small = TypeParam::make(1);
    const auto next_byte = TypeParam::make(256);
    const auto high_bit = TypeParam::make(std::uint64_t{1} << 63U);
    const auto below_high_bit = TypeParam::make((std::uint64_t{1} << 63U) - 1U);
    const auto last = TypeParam::make(maximum);
    ASSERT_TRUE(small.has_value());
    ASSERT_TRUE(next_byte.has_value());
    ASSERT_TRUE(high_bit.has_value());
    ASSERT_TRUE(below_high_bit.has_value());
    ASSERT_TRUE(last.has_value());
    EXPECT_LT(*small, *next_byte);
    EXPECT_LT(*below_high_bit, *high_bit);
    EXPECT_LT(*high_bit, *last);

    std::array<TypeParam, 68> cases{};
    cases[1] = *small;
    cases[2] = *small;
    for (std::size_t bit = 0; bit < 64; ++bit) {
        const auto value = TypeParam::make(std::uint64_t{1} << bit);
        ASSERT_TRUE(value.has_value());
        cases[bit + 3] = *value;
    }
    cases.back() = *last;
    EXPECT_TRUE(absl::VerifyTypeImplementsAbslHashCorrectly(cases));
    EXPECT_EQ(
      absl::Hash<TypeParam>{}(cases[1]), absl::Hash<TypeParam>{}(cases[2]));
}

} // namespace
