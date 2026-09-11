#include "src/model/transport_identity.h"

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

using kwaque::model::correlation_id;
using kwaque::model::frame_sequence;
using kwaque::model::transport_stream_id;

constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();

template<typename Value>
concept successor_expression = requires(const Value& value) {
    value.checked_successor();
};

template<typename Value>
class TransportIdentityTest : public ::testing::Test {
    static_assert(sizeof(Value) == sizeof(std::uint64_t));
    static_assert(std::is_standard_layout_v<Value>);
    static_assert(std::is_trivially_copyable_v<Value>);
    static_assert(std::is_nothrow_default_constructible_v<Value>);
    static_assert(std::is_nothrow_copy_constructible_v<Value>);
    static_assert(std::is_nothrow_copy_assignable_v<Value>);
    static_assert(std::is_nothrow_move_constructible_v<Value>);
    static_assert(std::is_nothrow_move_assignable_v<Value>);
    static_assert(std::is_nothrow_destructible_v<Value>);
    static_assert(std::is_nothrow_constructible_v<Value, std::uint64_t>);
    static_assert(!std::convertible_to<Value, std::uint64_t>);
    static_assert(!std::convertible_to<std::uint64_t, Value>);
    static_assert(!successor_expression<Value>);
    static_assert(std::same_as<
                  decltype(std::declval<const Value&>().value()),
                  std::uint64_t>);
    static_assert(Value{}.value() == 0U);
    static_assert(Value{maximum}.value() == maximum);
};

using transport_types
  = ::testing::Types<transport_stream_id, correlation_id, frame_sequence>;
TYPED_TEST_SUITE(TransportIdentityTest, transport_types);

TYPED_TEST(TransportIdentityTest, PreservesZeroAndAllUnsignedBoundaryValues) {
    for (const auto raw :
         {std::uint64_t{0}, std::uint64_t{1}, maximum - 1U, maximum}) {
        const TypeParam value{raw};
        const auto copy = value;
        EXPECT_EQ(value.value(), raw);
        EXPECT_EQ(value, copy);
    }
    EXPECT_LT(TypeParam{0}, TypeParam{1});
    EXPECT_LT(TypeParam{1}, TypeParam{256});
    EXPECT_LT(
      TypeParam{(std::uint64_t{1} << 63U) - 1U},
      TypeParam{std::uint64_t{1} << 63U});
    EXPECT_LT(TypeParam{maximum - 1U}, TypeParam{maximum});
}

TYPED_TEST(TransportIdentityTest, HashExpansionCoversEveryValueBit) {
    std::array<TypeParam, 68> cases{};
    cases[1] = TypeParam{1};
    cases[2] = TypeParam{1};
    for (std::size_t bit = 0; bit < 64; ++bit) {
        cases[bit + 3] = TypeParam{std::uint64_t{1} << bit};
    }
    cases.back() = TypeParam{maximum};
    EXPECT_TRUE(absl::VerifyTypeImplementsAbslHashCorrectly(cases));
    EXPECT_EQ(
      absl::Hash<TypeParam>{}(cases[1]), absl::Hash<TypeParam>{}(cases[2]));
}

} // namespace
