#include "src/base/error.h"
#include "src/model/identity.h"

#include <absl/hash/hash.h>
#include <absl/hash/hash_testing.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>
#include <tuple>
#include <type_traits>
#include <utility>

namespace {

using kwaque::model::broker_id;
using kwaque::model::cluster_id;
using kwaque::model::control_transaction_id;
using kwaque::model::manifest_id;
using kwaque::model::producer_id;
using kwaque::model::range_id;
using kwaque::model::segment_id;
using kwaque::model::tenant_id;
using kwaque::model::topic_id;
using kwaque::model::vnode_index;
using kwaque::model::wal_incarnation_id;

using identity_types = std::tuple<
  cluster_id,
  broker_id,
  tenant_id,
  topic_id,
  range_id,
  segment_id,
  producer_id,
  control_transaction_id,
  manifest_id,
  wal_incarnation_id>;

using identity_bytes = std::array<std::uint8_t, 16>;

constexpr identity_bytes sample_bytes{
  0x01,
  0x23,
  0x45,
  0x67,
  0x89,
  0xab,
  0xcd,
  0xef,
  0xfe,
  0xdc,
  0xba,
  0x98,
  0x76,
  0x54,
  0x32,
  0x10};

template<typename Left, typename Right>
concept equality_expression = requires(const Left& lhs, const Right& rhs) {
    lhs == rhs;
};

template<typename Left, typename Right>
concept inequality_expression = requires(const Left& lhs, const Right& rhs) {
    lhs != rhs;
};

template<typename Left, typename Right>
concept ordering_expression
  = requires(const Left& lhs, const Right& rhs) { lhs < rhs; }
    || requires(const Left& lhs, const Right& rhs) { lhs <= rhs; }
    || requires(const Left& lhs, const Right& rhs) { lhs > rhs; }
    || requires(const Left& lhs, const Right& rhs) { lhs >= rhs; }
    || requires(const Left& lhs, const Right& rhs) { lhs <=> rhs; };

template<typename Left, typename Right>
concept canonical_order_expression = requires(
  const Left& lhs, const Right& rhs) { lhs.canonical_less(rhs); };

template<typename Left, typename Right>
concept arithmetic_expression = requires(Left lhs, const Right& rhs) {
    lhs + rhs;
} || requires(Left lhs, const Right& rhs) {
    lhs - rhs;
} || requires(Left lhs, const Right& rhs) {
    lhs += rhs;
} || requires(Left lhs, const Right& rhs) { lhs -= rhs; };

template<typename Value>
concept increment_expression = requires(Value value) { ++value; }
                               || requires(Value value) { value++; }
                               || requires(Value value) { --value; }
                               || requires(Value value) { value--; };

template<typename Value>
concept borrowed_bytes_expression = requires(Value&& value) {
    std::forward<Value>(value).bytes();
};

template<typename Value>
concept writable_bytes_expression = requires(Value& value) {
    value.bytes()[0] = std::uint8_t{1};
};

template<typename Left, typename Right>
consteval bool identity_pair_contract() {
    if constexpr (std::same_as<Left, Right>) {
        return equality_expression<Left, Right>
               && inequality_expression<Left, Right>
               && canonical_order_expression<Left, Right>
               && !arithmetic_expression<Left, Right>
               && !ordering_expression<Left, Right>;
    } else {
        return !equality_expression<Left, Right>
               && !arithmetic_expression<Left, Right>
               && !inequality_expression<Left, Right>
               && !ordering_expression<Left, Right>
               && !canonical_order_expression<Left, Right>
               && !std::constructible_from<Left, Right>
               && !std::constructible_from<Left, const Right&>
               && !std::convertible_to<Left, Right>;
    }
}

template<typename Left, typename... Right>
consteval bool identity_peer_contract(std::tuple<Right...>*) {
    constexpr auto occurrences = (std::size_t{std::same_as<Left, Right>} + ...);
    return occurrences == 1U && (identity_pair_contract<Left, Right>() && ...);
}

template<typename... Id>
consteval bool identity_type_contract(std::tuple<Id...>* types) {
    return (identity_peer_contract<Id>(types) && ...);
}

static_assert(identity_type_contract(static_cast<identity_types*>(nullptr)));

template<typename Id>
class ObjectIdentityTest : public ::testing::Test {
    static_assert(Id::width == 16);
    static_assert(sizeof(Id) == 16);
    static_assert(std::is_standard_layout_v<Id>);
    static_assert(std::is_trivially_copyable_v<Id>);
    static_assert(std::is_nothrow_default_constructible_v<Id>);
    static_assert(std::is_nothrow_copy_constructible_v<Id>);
    static_assert(std::is_nothrow_copy_assignable_v<Id>);
    static_assert(std::is_nothrow_move_constructible_v<Id>);
    static_assert(std::is_nothrow_move_assignable_v<Id>);
    static_assert(std::is_nothrow_destructible_v<Id>);
    static_assert(!std::constructible_from<Id, std::uint64_t>);
    static_assert(!std::constructible_from<Id, identity_bytes>);
    static_assert(!std::constructible_from<Id, std::span<const std::uint8_t>>);
    static_assert(!std::convertible_to<Id, std::uint64_t>);
    static_assert(!std::convertible_to<Id, bool>);
    static_assert(!equality_expression<Id, std::uint64_t>);
    static_assert(!equality_expression<std::uint64_t, Id>);
    static_assert(!ordering_expression<Id, std::uint64_t>);
    static_assert(!ordering_expression<std::uint64_t, Id>);
    static_assert(!arithmetic_expression<Id, std::uint64_t>);
    static_assert(!arithmetic_expression<std::uint64_t, Id>);
    static_assert(!increment_expression<Id>);
    static_assert(borrowed_bytes_expression<Id&>);
    static_assert(borrowed_bytes_expression<const Id&>);
    static_assert(!borrowed_bytes_expression<Id>);
    static_assert(!borrowed_bytes_expression<const Id>);
    static_assert(!writable_bytes_expression<Id>);
    static_assert(std::same_as<
                  decltype(std::declval<const Id&>().bytes()),
                  std::span<const std::uint8_t, 16>>);
    static_assert(noexcept(std::declval<const Id&>().bytes()));
    static_assert(noexcept(std::declval<const Id&>().is_nil()));
    static_assert(
      noexcept(std::declval<const Id&>() == std::declval<const Id&>()));
    static_assert(noexcept(
      std::declval<const Id&>().canonical_less(std::declval<const Id&>())));
    static_assert(noexcept(Id::make(std::span<const std::uint8_t>{})));
};

using tested_identities = ::testing::Types<
  cluster_id,
  broker_id,
  tenant_id,
  topic_id,
  range_id,
  segment_id,
  producer_id,
  control_transaction_id,
  manifest_id,
  wal_incarnation_id>;
TYPED_TEST_SUITE(ObjectIdentityTest, tested_identities);

TYPED_TEST(ObjectIdentityTest, ChecksWidthAndRejectsNilPublication) {
    const TypeParam staging;
    EXPECT_TRUE(staging.is_nil());
    EXPECT_EQ(staging, TypeParam{});
    EXPECT_TRUE(std::ranges::all_of(staging.bytes(), [](std::uint8_t byte) {
        return byte == 0;
    }));

    const auto nil = TypeParam::make(identity_bytes{});
    ASSERT_FALSE(nil.has_value());
    EXPECT_EQ(nil.error(), kwaque::errc::invalid_argument);

    std::array<std::uint8_t, 17> input{};
    input.fill(0xff);
    const std::span<const std::uint8_t> source{input};
    for (const auto width :
         {std::size_t{0}, std::size_t{15}, std::size_t{17}}) {
        const auto invalid = TypeParam::make(source.first(width));
        ASSERT_FALSE(invalid.has_value()) << width;
        EXPECT_EQ(invalid.error(), kwaque::errc::invalid_argument) << width;
    }

    const auto maximum_bits = TypeParam::make(source.first(16));
    ASSERT_TRUE(maximum_bits.has_value());
    EXPECT_FALSE(maximum_bits->is_nil());
    EXPECT_TRUE(std::ranges::equal(maximum_bits->bytes(), source.first(16)));

    const auto ordinary = TypeParam::make(sample_bytes);
    ASSERT_TRUE(ordinary.has_value());
    EXPECT_TRUE(std::ranges::equal(ordinary->bytes(), sample_bytes));
}

TYPED_TEST(ObjectIdentityTest, OwnsBytesAfterInputMutationAndDestruction) {
    const auto owned = [] {
        auto input = sample_bytes;
        auto value = TypeParam::make(input);
        input.fill(0);
        return value;
    }();
    ASSERT_TRUE(owned.has_value());
    EXPECT_TRUE(std::ranges::equal(owned->bytes(), sample_bytes));

    TypeParam copied;
    {
        auto input = sample_bytes;
        const auto original = TypeParam::make(input);
        ASSERT_TRUE(original.has_value());
        copied = *original;
        input.fill(0xff);
        EXPECT_EQ(copied, *original);
    }
    EXPECT_TRUE(std::ranges::equal(copied.bytes(), sample_bytes));
    EXPECT_EQ(copied, *owned);
}

TYPED_TEST(ObjectIdentityTest, CanonicalOrderingUsesUnsignedOctets) {
    identity_bytes low_bytes{};
    low_bytes[0] = 0x7f;
    low_bytes[15] = 0xff;
    identity_bytes high_bytes{};
    high_bytes[0] = 0x80;
    const auto low = TypeParam::make(low_bytes);
    const auto high = TypeParam::make(high_bytes);
    ASSERT_TRUE(low.has_value());
    ASSERT_TRUE(high.has_value());
    EXPECT_TRUE(low->canonical_less(*high));
    EXPECT_FALSE(high->canonical_less(*low));
    EXPECT_FALSE(low->canonical_less(*low));
    EXPECT_NE(*low, *high);

    high_bytes = low_bytes;
    high_bytes[15] = 0xfe;
    const auto lower_tail = TypeParam::make(high_bytes);
    ASSERT_TRUE(lower_tail.has_value());
    EXPECT_TRUE(lower_tail->canonical_less(*low));
    EXPECT_FALSE(low->canonical_less(*lower_tail));
}

TYPED_TEST(ObjectIdentityTest, EqualityAndHashExpansionCoverEveryOctet) {
    const auto original = TypeParam::make(sample_bytes);
    ASSERT_TRUE(original.has_value());
    std::array<TypeParam, 19> cases{};
    cases[0] = *original;
    cases[1] = *original;
    // cases[2] remains nil staging, distinct from every published fixture.
    for (std::size_t index = 0; index < sample_bytes.size(); ++index) {
        auto changed_bytes = sample_bytes;
        changed_bytes[index] ^= 0xff;
        const auto changed = TypeParam::make(changed_bytes);
        ASSERT_TRUE(changed.has_value());
        EXPECT_NE(*original, *changed) << index;
        cases[index + 3] = *changed;
    }
    EXPECT_TRUE(absl::VerifyTypeImplementsAbslHashCorrectly(cases));
    EXPECT_EQ(
      absl::Hash<TypeParam>{}(cases[0]), absl::Hash<TypeParam>{}(cases[1]));
}

static_assert(sizeof(vnode_index) == sizeof(std::uint32_t));
static_assert(std::is_standard_layout_v<vnode_index>);
static_assert(std::is_trivially_copyable_v<vnode_index>);
static_assert(std::is_nothrow_copy_constructible_v<vnode_index>);
static_assert(std::is_nothrow_copy_assignable_v<vnode_index>);
static_assert(std::is_nothrow_move_constructible_v<vnode_index>);
static_assert(std::is_nothrow_move_assignable_v<vnode_index>);
static_assert(std::is_nothrow_destructible_v<vnode_index>);
static_assert(!std::default_initializable<vnode_index>);
static_assert(!std::constructible_from<vnode_index, std::uint32_t>);
static_assert(!std::constructible_from<vnode_index, std::uint64_t>);
static_assert(!std::convertible_to<vnode_index, std::uint32_t>);
static_assert(!equality_expression<vnode_index, std::uint32_t>);
static_assert(std::same_as<
              decltype(std::declval<const vnode_index&>().value()),
              std::uint32_t>);
static_assert(noexcept(vnode_index::make(cluster_id{}, 1, 0)));

TEST(VnodeIndexTest, RequiresValidClusterAndRepresentablePowerOfTwoRing) {
    const auto invalid_cluster = vnode_index::make(cluster_id{}, 256, 0);
    ASSERT_FALSE(invalid_cluster.has_value());
    EXPECT_EQ(invalid_cluster.error(), kwaque::errc::invalid_argument);

    const auto cluster = cluster_id::make(sample_bytes);
    ASSERT_TRUE(cluster.has_value());
    const std::array invalid_rings{
      std::uint64_t{0},
      std::uint64_t{3},
      std::uint64_t{255},
      std::uint64_t{257},
      std::uint64_t{1} << 32U,
      (std::uint64_t{1} << 32U) + 256U,
      std::numeric_limits<std::uint64_t>::max()};
    for (const auto ring : invalid_rings) {
        const auto invalid = vnode_index::make(*cluster, ring, 0);
        ASSERT_FALSE(invalid.has_value()) << ring;
        EXPECT_EQ(invalid.error(), kwaque::errc::invalid_argument) << ring;
    }
}

TEST(VnodeIndexTest, AcceptsZeroAndLastIndexWithoutNarrowingOrModulo) {
    const auto cluster = cluster_id::make(sample_bytes);
    ASSERT_TRUE(cluster.has_value());
    for (const auto ring :
         {std::uint64_t{1}, std::uint64_t{256}, std::uint64_t{1} << 31U}) {
        const auto first = vnode_index::make(*cluster, ring, 0);
        const auto last = vnode_index::make(*cluster, ring, ring - 1U);
        ASSERT_TRUE(first.has_value()) << ring;
        ASSERT_TRUE(last.has_value()) << ring;
        EXPECT_EQ(first->value(), 0U);
        EXPECT_EQ(last->value(), ring - 1U);
        EXPECT_EQ(*first == *last, ring == 1U);

        for (const auto index :
             {ring,
              (std::uint64_t{1} << 32U) + ring - 1U,
              std::numeric_limits<std::uint64_t>::max()}) {
            const auto invalid = vnode_index::make(*cluster, ring, index);
            ASSERT_FALSE(invalid.has_value()) << ring << ':' << index;
            EXPECT_EQ(invalid.error(), kwaque::errc::out_of_range)
              << ring << ':' << index;
        }
    }
}

} // namespace
