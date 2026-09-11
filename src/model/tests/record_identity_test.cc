#include "src/base/error.h"
#include "src/model/epoch.h"
#include "src/model/identity.h"
#include "src/model/position.h"

#include <absl/hash/hash.h>
#include <absl/hash/hash_testing.h>
#include <gtest/gtest.h>

#include <array>
#include <compare>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <type_traits>
#include <utility>

namespace {

using kwaque::model::physical_address;
using kwaque::model::range_id;
using kwaque::model::range_logical_end;
using kwaque::model::range_logical_offset;
using kwaque::model::range_routing_epoch;
using kwaque::model::record_id;
using kwaque::model::segment_generation;
using kwaque::model::segment_id;
using kwaque::model::segment_relative_end;
using kwaque::model::segment_relative_offset;
using kwaque::model::topic_id;

constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();
constexpr std::array<std::uint8_t, 16> object_bytes{
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

template<typename Value>
concept ordering_expression
  = requires(const Value& lhs, const Value& rhs) { lhs < rhs; }
    || requires(const Value& lhs, const Value& rhs) { lhs <= rhs; }
    || requires(const Value& lhs, const Value& rhs) { lhs > rhs; }
    || requires(const Value& lhs, const Value& rhs) { lhs >= rhs; }
    || requires(const Value& lhs, const Value& rhs) { lhs <=> rhs; };

template<typename Offset>
concept record_construction_expression = requires(Offset offset) {
    record_id::make(topic_id{}, range_id{}, offset);
};

template<typename Offset>
concept address_construction_expression = requires(Offset offset) {
    physical_address::make(segment_id{}, offset);
};

template<typename Generation>
concept layout_comparison_expression = requires(
  const physical_address& address, Generation generation) {
    address.compare_in_layout(address, generation, generation);
};

template<typename Value>
consteval bool owned_record_components() {
    return std::same_as<decltype(std::declval<Value>().topic()), topic_id>
           && std::same_as<decltype(std::declval<Value>().range()), range_id>
           && std::same_as<
             decltype(std::declval<Value>().offset()),
             range_logical_offset>;
}

template<typename Value>
consteval bool owned_address_components() {
    return std::same_as<decltype(std::declval<Value>().segment()), segment_id>
           && std::same_as<
             decltype(std::declval<Value>().offset()),
             segment_relative_offset>;
}

template<typename Value>
consteval bool identity_value_contract(std::size_t bytes) {
    return sizeof(Value) == bytes && std::is_standard_layout_v<Value>
           && std::is_trivially_copyable_v<Value>
           && std::is_nothrow_copy_constructible_v<Value>
           && std::is_nothrow_copy_assignable_v<Value>
           && std::is_nothrow_move_constructible_v<Value>
           && std::is_nothrow_move_assignable_v<Value>
           && std::is_nothrow_destructible_v<Value>
           && !std::default_initializable<Value> && !ordering_expression<Value>;
}

static_assert(identity_value_contract<record_id>(40));
static_assert(identity_value_contract<physical_address>(24));
static_assert(
  !std::
    constructible_from<record_id, topic_id, range_id, range_logical_offset>);
static_assert(
  !std::
    constructible_from<physical_address, segment_id, segment_relative_offset>);
static_assert(!std::constructible_from<record_id, physical_address>);
static_assert(!std::constructible_from<physical_address, record_id>);
static_assert(record_construction_expression<range_logical_offset>);
static_assert(!record_construction_expression<range_logical_end>);
static_assert(!record_construction_expression<segment_relative_offset>);
static_assert(!record_construction_expression<std::uint64_t>);
static_assert(address_construction_expression<segment_relative_offset>);
static_assert(!address_construction_expression<segment_relative_end>);
static_assert(!address_construction_expression<range_logical_offset>);
static_assert(!address_construction_expression<std::uint64_t>);
static_assert(layout_comparison_expression<segment_generation>);
static_assert(!layout_comparison_expression<range_routing_epoch>);
static_assert(!layout_comparison_expression<std::uint64_t>);
static_assert(owned_record_components<record_id&>());
static_assert(owned_record_components<const record_id&>());
static_assert(owned_record_components<record_id>());
static_assert(owned_record_components<const record_id>());
static_assert(owned_address_components<physical_address&>());
static_assert(owned_address_components<const physical_address&>());
static_assert(owned_address_components<physical_address>());
static_assert(owned_address_components<const physical_address>());
static_assert(
  noexcept(record_id::make(topic_id{}, range_id{}, range_logical_offset{})));
static_assert(
  noexcept(physical_address::make(segment_id{}, segment_relative_offset{})));
static_assert(noexcept(std::declval<const record_id&>().compare_in_range(
  std::declval<const record_id&>())));
static_assert(
  noexcept(std::declval<const physical_address&>().compare_in_layout(
    std::declval<const physical_address&>(),
    segment_generation{},
    segment_generation{})));

class RecordIdentityTest : public ::testing::Test {
protected:
    void SetUp() override {
        const auto topic = topic_id::make(object_bytes);
        const auto range = range_id::make(object_bytes);
        const auto segment = segment_id::make(object_bytes);
        const auto generation = segment_generation::make(1);
        ASSERT_TRUE(topic.has_value());
        ASSERT_TRUE(range.has_value());
        ASSERT_TRUE(segment.has_value());
        ASSERT_TRUE(generation.has_value());
        topic_ = *topic;
        range_ = *range;
        segment_ = *segment;
        generation_ = *generation;
    }

    auto make_record(range_logical_offset offset = {}) const noexcept {
        return record_id::make(topic_, range_, offset);
    }

    auto make_address(segment_relative_offset offset = {}) const noexcept {
        return physical_address::make(segment_, offset);
    }

    topic_id topic_;
    range_id range_;
    segment_id segment_;
    segment_generation generation_;
};

TEST_F(RecordIdentityTest, RejectsNilContextAndPreservesActualRecordEndpoints) {
    const auto missing_topic = record_id::make(
      topic_id{}, range_, range_logical_offset{});
    const auto missing_range = record_id::make(
      topic_, range_id{}, range_logical_offset{});
    const auto missing_segment = physical_address::make(
      segment_id{}, segment_relative_offset{});
    ASSERT_FALSE(missing_topic.has_value());
    ASSERT_FALSE(missing_range.has_value());
    ASSERT_FALSE(missing_segment.has_value());
    EXPECT_EQ(missing_topic.error(), kwaque::errc::invalid_argument);
    EXPECT_EQ(missing_range.error(), kwaque::errc::invalid_argument);
    EXPECT_EQ(missing_segment.error(), kwaque::errc::invalid_argument);

    const auto last_logical = range_logical_offset::make(maximum - 1U);
    const auto last_physical = segment_relative_offset::make(maximum - 1U);
    ASSERT_TRUE(last_logical.has_value());
    ASSERT_TRUE(last_physical.has_value());
    const auto record = make_record(*last_logical);
    const auto address = make_address(*last_physical);
    ASSERT_TRUE(record.has_value());
    ASSERT_TRUE(address.has_value());
    EXPECT_EQ(record->topic(), topic_);
    EXPECT_EQ(record->range(), range_);
    EXPECT_EQ(record->offset(), *last_logical);
    EXPECT_EQ(address->segment(), segment_);
    EXPECT_EQ(address->offset(), *last_physical);
}

TEST_F(RecordIdentityTest, AppendComparisonRequiresBothTopicAndRangeToMatch) {
    const auto later_offset = range_logical_offset::make(256);
    ASSERT_TRUE(later_offset.has_value());
    const auto earlier = make_record();
    const auto later = make_record(*later_offset);
    ASSERT_TRUE(earlier.has_value());
    ASSERT_TRUE(later.has_value());
    const auto less = earlier->compare_in_range(*later);
    const auto greater = later->compare_in_range(*earlier);
    const auto equal = earlier->compare_in_range(*earlier);
    ASSERT_TRUE(less.has_value());
    ASSERT_TRUE(greater.has_value());
    ASSERT_TRUE(equal.has_value());
    EXPECT_EQ(*less, std::strong_ordering::less);
    EXPECT_EQ(*greater, std::strong_ordering::greater);
    EXPECT_EQ(*equal, std::strong_ordering::equal);

    auto other_bytes = object_bytes;
    other_bytes.back() ^= 0xff;
    const auto other_topic = topic_id::make(other_bytes);
    const auto other_range = range_id::make(other_bytes);
    ASSERT_TRUE(other_topic.has_value());
    ASSERT_TRUE(other_range.has_value());
    const std::array wrong_contexts{
      record_id::make(*other_topic, range_, range_logical_offset{}),
      record_id::make(topic_, *other_range, range_logical_offset{})};
    for (const auto& wrong : wrong_contexts) {
        ASSERT_TRUE(wrong.has_value());
        const auto mismatch = earlier->compare_in_range(*wrong);
        const auto reverse_mismatch = wrong->compare_in_range(*earlier);
        ASSERT_FALSE(mismatch.has_value());
        ASSERT_FALSE(reverse_mismatch.has_value());
        EXPECT_EQ(mismatch.error(), kwaque::errc::invalid_argument);
        EXPECT_EQ(reverse_mismatch.error(), kwaque::errc::invalid_argument);
    }
}

TEST_F(RecordIdentityTest, CanonicalRecordOrderIsUnsignedAndSeparateFromScope) {
    auto low_bytes = object_bytes;
    auto high_bytes = object_bytes;
    low_bytes[0] = 0x7f;
    high_bytes[0] = 0x80;
    const auto low_topic = topic_id::make(low_bytes);
    const auto high_topic = topic_id::make(high_bytes);
    const auto low_range = range_id::make(low_bytes);
    const auto high_range = range_id::make(high_bytes);
    const auto one = range_logical_offset::make(1);
    const auto byte256 = range_logical_offset::make(256);
    const auto last = range_logical_offset::make(maximum - 1U);
    ASSERT_TRUE(low_topic.has_value());
    ASSERT_TRUE(high_topic.has_value());
    ASSERT_TRUE(low_range.has_value());
    ASSERT_TRUE(high_range.has_value());
    ASSERT_TRUE(one.has_value());
    ASSERT_TRUE(byte256.has_value());
    ASSERT_TRUE(last.has_value());
    const std::array ordered_pairs{
      std::pair{
        record_id::make(*low_topic, *high_range, *last),
        record_id::make(*high_topic, *low_range, range_logical_offset{})},
      std::pair{
        record_id::make(topic_, *low_range, *last),
        record_id::make(topic_, *high_range, range_logical_offset{})},
      std::pair{make_record(*one), make_record(*byte256)}};
    for (const auto& [earlier, later] : ordered_pairs) {
        ASSERT_TRUE(earlier.has_value());
        ASSERT_TRUE(later.has_value());
        EXPECT_TRUE(earlier->canonical_less(*later));
        EXPECT_FALSE(later->canonical_less(*earlier));
        EXPECT_FALSE(earlier->canonical_less(*earlier));
        EXPECT_NE(*earlier, *later);
    }
}

TEST_F(RecordIdentityTest, RecordEqualityAndHashExpansionCoverEveryField) {
    auto changed_bytes = object_bytes;
    changed_bytes.back() ^= 0xff;
    const auto other_topic = topic_id::make(changed_bytes);
    const auto other_range = range_id::make(changed_bytes);
    const auto one = range_logical_offset::make(1);
    const auto high_bit = range_logical_offset::make(std::uint64_t{1} << 63U);
    const auto original = make_record();
    ASSERT_TRUE(other_topic.has_value());
    ASSERT_TRUE(other_range.has_value());
    ASSERT_TRUE(one.has_value());
    ASSERT_TRUE(high_bit.has_value());
    ASSERT_TRUE(original.has_value());
    const std::array variants{
      record_id::make(*other_topic, range_, range_logical_offset{}),
      record_id::make(topic_, *other_range, range_logical_offset{}),
      make_record(*one),
      make_record(*high_bit)};
    for (const auto& variant : variants) {
        ASSERT_TRUE(variant.has_value());
        EXPECT_NE(*original, *variant);
    }
    const std::array cases{
      *original,
      *original,
      *variants[0],
      *variants[1],
      *variants[2],
      *variants[3]};
    EXPECT_TRUE(absl::VerifyTypeImplementsAbslHashCorrectly(cases));
    EXPECT_EQ(
      absl::Hash<record_id>{}(cases[0]), absl::Hash<record_id>{}(cases[1]));
}

TEST_F(RecordIdentityTest, PhysicalComparisonRequiresTheSameValidLayout) {
    const auto later_offset = segment_relative_offset::make(256);
    ASSERT_TRUE(later_offset.has_value());
    const auto earlier = make_address();
    const auto later = make_address(*later_offset);
    ASSERT_TRUE(earlier.has_value());
    ASSERT_TRUE(later.has_value());
    const auto less = earlier->compare_in_layout(
      *later, generation_, generation_);
    const auto greater = later->compare_in_layout(
      *earlier, generation_, generation_);
    const auto equal = earlier->compare_in_layout(
      *earlier, generation_, generation_);
    ASSERT_TRUE(less.has_value());
    ASSERT_TRUE(greater.has_value());
    ASSERT_TRUE(equal.has_value());
    EXPECT_EQ(*less, std::strong_ordering::less);
    EXPECT_EQ(*greater, std::strong_ordering::greater);
    EXPECT_EQ(*equal, std::strong_ordering::equal);

    const auto other_generation = segment_generation::make(2);
    const auto last_generation = segment_generation::make(maximum);
    ASSERT_TRUE(other_generation.has_value());
    ASSERT_TRUE(last_generation.has_value());
    const std::array generation_pairs{
      std::pair{segment_generation{}, generation_},
      std::pair{generation_, segment_generation{}},
      std::pair{segment_generation{}, segment_generation{}},
      std::pair{generation_, *other_generation},
      std::pair{*other_generation, generation_}};
    for (const auto& [left, right] : generation_pairs) {
        const auto invalid = earlier->compare_in_layout(*earlier, left, right);
        ASSERT_FALSE(invalid.has_value());
        EXPECT_EQ(invalid.error(), kwaque::errc::invalid_argument);
    }
    const auto maximum_layout = earlier->compare_in_layout(
      *later, *last_generation, *last_generation);
    ASSERT_TRUE(maximum_layout.has_value());
    EXPECT_EQ(*maximum_layout, std::strong_ordering::less);

    auto changed_bytes = object_bytes;
    changed_bytes.back() ^= 0xff;
    const auto other_segment = segment_id::make(changed_bytes);
    ASSERT_TRUE(other_segment.has_value());
    const auto elsewhere = physical_address::make(
      *other_segment, segment_relative_offset{});
    ASSERT_TRUE(elsewhere.has_value());
    const auto mismatch = earlier->compare_in_layout(
      *elsewhere, generation_, generation_);
    ASSERT_FALSE(mismatch.has_value());
    EXPECT_EQ(mismatch.error(), kwaque::errc::invalid_argument);
}

TEST_F(
  RecordIdentityTest, PhysicalCanonicalOrderAndHashCoverSegmentAndOrdinal) {
    auto low_bytes = object_bytes;
    auto high_bytes = object_bytes;
    low_bytes[0] = 0x7f;
    high_bytes[0] = 0x80;
    const auto low_segment = segment_id::make(low_bytes);
    const auto high_segment = segment_id::make(high_bytes);
    const auto one = segment_relative_offset::make(1);
    const auto byte256 = segment_relative_offset::make(256);
    const auto last = segment_relative_offset::make(maximum - 1U);
    const auto high_bit = segment_relative_offset::make(
      std::uint64_t{1} << 63U);
    ASSERT_TRUE(low_segment.has_value());
    ASSERT_TRUE(high_segment.has_value());
    ASSERT_TRUE(one.has_value());
    ASSERT_TRUE(byte256.has_value());
    ASSERT_TRUE(last.has_value());
    ASSERT_TRUE(high_bit.has_value());
    const std::array ordered_pairs{
      std::pair{
        physical_address::make(*low_segment, *last),
        physical_address::make(*high_segment, segment_relative_offset{})},
      std::pair{make_address(*one), make_address(*byte256)}};
    for (const auto& [earlier, later] : ordered_pairs) {
        ASSERT_TRUE(earlier.has_value());
        ASSERT_TRUE(later.has_value());
        EXPECT_TRUE(earlier->canonical_less(*later));
        EXPECT_FALSE(later->canonical_less(*earlier));
        EXPECT_FALSE(earlier->canonical_less(*earlier));
    }
    const auto original = make_address();
    const std::array variants{
      physical_address::make(*high_segment, segment_relative_offset{}),
      make_address(*one),
      make_address(*high_bit)};
    ASSERT_TRUE(original.has_value());
    for (const auto& variant : variants) {
        ASSERT_TRUE(variant.has_value());
        EXPECT_NE(*original, *variant);
    }
    const std::array cases{
      *original, *original, *variants[0], *variants[1], *variants[2]};
    EXPECT_TRUE(absl::VerifyTypeImplementsAbslHashCorrectly(cases));
    EXPECT_EQ(
      absl::Hash<physical_address>{}(cases[0]),
      absl::Hash<physical_address>{}(cases[1]));
}

TEST_F(RecordIdentityTest, ComponentsOutliveTemporaryValuesAndResults) {
    const auto record = make_record();
    const auto address = make_address();
    ASSERT_TRUE(record.has_value());
    ASSERT_TRUE(address.has_value());
    auto topic = record_id{*record}.topic();
    const auto range = record_id{*record}.range();
    const auto logical_offset = record_id{*record}.offset();
    auto segment = physical_address{*address}.segment();
    const auto physical_offset = physical_address{*address}.offset();
    const auto result_topic = decltype(record){*record}->topic();
    const auto result_segment = decltype(address){*address}->segment();
    EXPECT_EQ(topic, topic_);
    EXPECT_EQ(range, range_);
    EXPECT_EQ(logical_offset, range_logical_offset{});
    EXPECT_EQ(segment, segment_);
    EXPECT_EQ(physical_offset, segment_relative_offset{});
    EXPECT_EQ(result_topic, topic_);
    EXPECT_EQ(result_segment, segment_);
    topic = topic_id{};
    segment = segment_id{};
    EXPECT_TRUE(topic.is_nil());
    EXPECT_TRUE(segment.is_nil());
    EXPECT_EQ(record->topic(), topic_);
    EXPECT_EQ(address->segment(), segment_);
}

TEST_F(RecordIdentityTest, LogicalIdentityDoesNotContainPhysicalPlacement) {
    const auto logical_offset = range_logical_offset::make(123);
    const auto old_offset = segment_relative_offset::make(17);
    const auto new_offset = segment_relative_offset::make(2);
    auto replacement_bytes = object_bytes;
    replacement_bytes.back() ^= 0xff;
    const auto replacement = segment_id::make(replacement_bytes);
    ASSERT_TRUE(logical_offset.has_value());
    ASSERT_TRUE(old_offset.has_value());
    ASSERT_TRUE(new_offset.has_value());
    ASSERT_TRUE(replacement.has_value());
    const auto logical = make_record(*logical_offset);
    const auto old_address = make_address(*old_offset);
    const auto new_address = physical_address::make(*replacement, *new_offset);
    ASSERT_TRUE(logical.has_value());
    ASSERT_TRUE(old_address.has_value());
    ASSERT_TRUE(new_address.has_value());
    const std::pair before{*logical, *old_address};
    const std::pair after{before.first, *new_address};
    EXPECT_EQ(before.first, after.first);
    EXPECT_NE(before.second, after.second);
    EXPECT_EQ(after.first.offset(), *logical_offset);
    EXPECT_EQ(before.second.offset(), *old_offset);
    EXPECT_EQ(after.second.offset(), *new_offset);
    const auto same_record = before.first.compare_in_range(after.first);
    ASSERT_TRUE(same_record.has_value());
    EXPECT_EQ(*same_record, std::strong_ordering::equal);
}

} // namespace
