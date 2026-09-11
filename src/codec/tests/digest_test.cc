#include "src/codec/digest.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <concepts>
#include <cstddef>
#include <optional>
#include <span>
#include <tuple>
#include <type_traits>
#include <utility>

namespace {

namespace codec = kwaque::codec;
using codec::sha256_digest;

template<typename Left, typename Right>
concept equal_expression = requires(Left left, Right right) { left == right; };

template<typename Left, typename Right>
concept order_expression = requires(Left left, Right right) {
    left < right;
} || requires(Left left, Right right) { left <=> right; };

template<typename Value, typename... Peers>
consteval bool digest_contract(std::tuple<Peers...>*) {
    constexpr auto occurrences
      = (std::size_t{std::same_as<Value, Peers>} + ...);
    return occurrences == 1U && sizeof(Value) == 32
           && std::is_standard_layout_v<Value>
           && std::is_trivially_copyable_v<Value>
           && std::is_nothrow_copy_constructible_v<Value>
           && std::is_nothrow_move_constructible_v<Value>
           && std::is_nothrow_destructible_v<Value>
           && !std::default_initializable<Value> && !std::is_aggregate_v<Value>
           && std::constructible_from<Value, sha256_digest>
           && !std::convertible_to<sha256_digest, Value>
           && !std::convertible_to<Value, sha256_digest>
           && !equal_expression<Value, sha256_digest>
           && !std::constructible_from<Value, std::span<const unsigned char>>
           && std::same_as<
             decltype(std::declval<const Value&>().bytes()),
             sha256_digest>
           && std::
             same_as<decltype(std::declval<Value&&>().bytes()), sha256_digest>
           && noexcept(std::declval<const Value&>().bytes())
           && ((equal_expression<Value, Peers> == std::same_as<Value, Peers>) && ...)
           && (!order_expression<Value, Peers> && ...)
           && ((!std::constructible_from<Value, Peers> || std::same_as<Value, Peers>) && ...);
}

using digest_types = std::tuple<
  codec::semantic_batch_digest,
  codec::checkpoint_digest,
  codec::immutable_object_digest,
  codec::extent_digest>;

template<typename... Values>
consteval bool all_contracts(std::tuple<Values...>* types) {
    return (digest_contract<Values>(types) && ...);
}

static_assert(codec::sha256_digest_bytes == 32);
static_assert(std::same_as<sha256_digest, std::array<unsigned char, 32>>);
static_assert(all_contracts(static_cast<digest_types*>(nullptr)));
static_assert(
  codec::semantic_batch_digest{sha256_digest{}}.bytes() == sha256_digest{});
static_assert(codec::semantic_batch_domain.size() == 11);
static_assert(codec::checkpoint_domain.size() == 16);

template<typename Value>
class DigestValueTest : public ::testing::Test {};

using tested_digests = ::testing::Types<
  codec::semantic_batch_digest,
  codec::checkpoint_digest,
  codec::immutable_object_digest,
  codec::extent_digest>;
TYPED_TEST_SUITE(DigestValueTest, tested_digests);

TYPED_TEST(DigestValueTest, ZeroIsAValueAndAbsenceIsSeparate) {
    const TypeParam zero{sha256_digest{}};
    EXPECT_EQ(zero.bytes(), sha256_digest{});
    std::optional<TypeParam> absent;
    EXPECT_FALSE(absent.has_value());
    absent.emplace(sha256_digest{});
    ASSERT_TRUE(absent.has_value());
    EXPECT_EQ(*absent, zero);
}

TYPED_TEST(
  DigestValueTest, SnapshotsOwnAllOctetsAcrossSourceAndValueLifetimes) {
    sha256_digest bytes{};
    for (std::size_t index = 0; index < bytes.size(); ++index) {
        bytes[index] = static_cast<unsigned char>(index);
    }
    const auto expected = bytes;
    const TypeParam original{bytes};
    bytes.fill(0xff);
    EXPECT_EQ(original.bytes(), expected);
    auto snapshot = TypeParam{expected}.bytes();
    EXPECT_EQ(snapshot, expected);
    snapshot.fill(0xaa);
    EXPECT_EQ(original.bytes(), expected);
    EXPECT_NE(snapshot, original.bytes());
}

TYPED_TEST(DigestValueTest, EqualityUsesEveryOctet) {
    const TypeParam zero{sha256_digest{}};
    EXPECT_EQ(zero, TypeParam{sha256_digest{}});
    for (std::size_t index = 0; index < 32; ++index) {
        for (const auto octet : {0x01U, 0x80U, 0xffU}) {
            sha256_digest changed{};
            changed[index] = static_cast<unsigned char>(octet);
            const TypeParam other{changed};
            EXPECT_NE(other, zero) << index;
            EXPECT_NE(zero, other) << index;
            EXPECT_EQ(other.bytes(), changed);
        }
    }
}

TEST(DigestDomainTest, SemanticPrefixesHaveExactBytesAndOneTerminator) {
    constexpr std::array<char, 11> batch{
      0x4b, 0x51, 0x2f, 0x42, 0x41, 0x54, 0x43, 0x48, 0x2f, 0x31, 0x00};
    constexpr std::array<char, 16> checkpoint{
      0x4b,
      0x51,
      0x2f,
      0x43,
      0x48,
      0x45,
      0x43,
      0x4b,
      0x50,
      0x4f,
      0x49,
      0x4e,
      0x54,
      0x2f,
      0x31,
      0x00};
    EXPECT_EQ(codec::semantic_batch_domain, batch);
    EXPECT_EQ(codec::checkpoint_domain, checkpoint);
    EXPECT_EQ(std::ranges::count(codec::semantic_batch_domain, '\0'), 1);
    EXPECT_EQ(std::ranges::count(codec::checkpoint_domain, '\0'), 1);
}

} // namespace
