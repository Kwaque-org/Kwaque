#include "src/base/error.h"
#include "src/model/batch_identity.h"
#include "src/model/epoch.h"
#include "src/model/identity.h"

#include <gtest/gtest.h>

#include <array>
#include <concepts>
#include <cstdint>
#include <limits>
#include <type_traits>
#include <utility>

namespace {

using kwaque::model::producer_stream_binding;
using kwaque::model::range_id;
using kwaque::model::range_routing_epoch;
using kwaque::model::segment_generation;
using kwaque::model::segment_id;
using kwaque::model::topic_id;

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

template<typename Binding>
consteval bool owned_binding_components() {
    return std::same_as<decltype(std::declval<Binding>().topic()), topic_id>
           && std::same_as<decltype(std::declval<Binding>().range()), range_id>
           && std::same_as<
             decltype(std::declval<Binding>().routing_epoch()),
             range_routing_epoch>
           && std::
             same_as<decltype(std::declval<Binding>().segment()), segment_id>
           && std::same_as<
             decltype(std::declval<Binding>().generation()),
             segment_generation>;
}

static_assert(sizeof(producer_stream_binding) == 64);
static_assert(std::is_standard_layout_v<producer_stream_binding>);
static_assert(std::is_trivially_copyable_v<producer_stream_binding>);
static_assert(std::is_nothrow_copy_constructible_v<producer_stream_binding>);
static_assert(std::is_nothrow_copy_assignable_v<producer_stream_binding>);
static_assert(std::is_nothrow_move_constructible_v<producer_stream_binding>);
static_assert(std::is_nothrow_move_assignable_v<producer_stream_binding>);
static_assert(std::is_nothrow_destructible_v<producer_stream_binding>);
static_assert(!std::default_initializable<producer_stream_binding>);
static_assert(!std::constructible_from<
              producer_stream_binding,
              topic_id,
              range_id,
              range_routing_epoch,
              segment_id,
              segment_generation>);
static_assert(!ordering_expression<producer_stream_binding>);
static_assert(owned_binding_components<producer_stream_binding&>());
static_assert(owned_binding_components<const producer_stream_binding&>());
static_assert(owned_binding_components<producer_stream_binding>());
static_assert(owned_binding_components<const producer_stream_binding>());
static_assert(noexcept(producer_stream_binding::make(
  topic_id{},
  range_id{},
  range_routing_epoch{},
  segment_id{},
  segment_generation{})));
static_assert(
  noexcept(std::declval<const producer_stream_binding&>().validate_expected(
    std::declval<const producer_stream_binding&>())));

class ProducerStreamBindingTest : public ::testing::Test {
protected:
    void SetUp() override {
        const auto topic = topic_id::make(object_bytes);
        const auto range = range_id::make(object_bytes);
        const auto routing_epoch = range_routing_epoch::make(1);
        const auto segment = segment_id::make(object_bytes);
        const auto generation = segment_generation::make(1);
        ASSERT_TRUE(topic.has_value());
        ASSERT_TRUE(range.has_value());
        ASSERT_TRUE(routing_epoch.has_value());
        ASSERT_TRUE(segment.has_value());
        ASSERT_TRUE(generation.has_value());
        topic_ = *topic;
        range_ = *range;
        routing_epoch_ = *routing_epoch;
        segment_ = *segment;
        generation_ = *generation;
    }

    auto make_binding() const noexcept {
        return producer_stream_binding::make(
          topic_, range_, routing_epoch_, segment_, generation_);
    }

    topic_id topic_;
    range_id range_;
    range_routing_epoch routing_epoch_;
    segment_id segment_;
    segment_generation generation_;
};

TEST_F(ProducerStreamBindingTest, RejectsEachNilIdentityAndUninitializedEpoch) {
    const std::array invalid_cases{
      producer_stream_binding::make(
        topic_id{}, range_, routing_epoch_, segment_, generation_),
      producer_stream_binding::make(
        topic_, range_id{}, routing_epoch_, segment_, generation_),
      producer_stream_binding::make(
        topic_, range_, range_routing_epoch{}, segment_, generation_),
      producer_stream_binding::make(
        topic_, range_, routing_epoch_, segment_id{}, generation_),
      producer_stream_binding::make(
        topic_, range_, routing_epoch_, segment_, segment_generation{})};
    for (const auto& invalid : invalid_cases) {
        ASSERT_FALSE(invalid.has_value());
        EXPECT_EQ(invalid.error(), kwaque::errc::invalid_argument);
    }
}

TEST_F(ProducerStreamBindingTest, PreservesCompleteContextAndMaximumEpochs) {
    const auto binding = make_binding();
    ASSERT_TRUE(binding.has_value());
    EXPECT_EQ(binding->topic(), topic_);
    EXPECT_EQ(binding->range(), range_);
    EXPECT_EQ(binding->routing_epoch(), routing_epoch_);
    EXPECT_EQ(binding->segment(), segment_);
    EXPECT_EQ(binding->generation(), generation_);
    EXPECT_TRUE(binding->validate_expected(*binding).has_value());

    constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();
    const auto last_routing_epoch = range_routing_epoch::make(maximum);
    const auto last_generation = segment_generation::make(maximum);
    ASSERT_TRUE(last_routing_epoch.has_value());
    ASSERT_TRUE(last_generation.has_value());
    const auto last = producer_stream_binding::make(
      topic_, range_, *last_routing_epoch, segment_, *last_generation);
    ASSERT_TRUE(last.has_value());
    EXPECT_EQ(last->routing_epoch().value(), maximum);
    EXPECT_EQ(last->generation().value(), maximum);
    EXPECT_TRUE(last->validate_expected(*last).has_value());
}

TEST_F(ProducerStreamBindingTest, ExpectedContextChecksAllFiveFields) {
    auto changed_bytes = object_bytes;
    changed_bytes.back() ^= 0xff;
    const auto other_topic = topic_id::make(changed_bytes);
    const auto other_range = range_id::make(changed_bytes);
    const auto other_routing_epoch = range_routing_epoch::make(2);
    const auto other_segment = segment_id::make(changed_bytes);
    const auto other_generation = segment_generation::make(2);
    ASSERT_TRUE(other_topic.has_value());
    ASSERT_TRUE(other_range.has_value());
    ASSERT_TRUE(other_routing_epoch.has_value());
    ASSERT_TRUE(other_segment.has_value());
    ASSERT_TRUE(other_generation.has_value());
    const auto original = make_binding();
    ASSERT_TRUE(original.has_value());
    const auto saved = *original;
    const std::array variants{
      producer_stream_binding::make(
        *other_topic, range_, routing_epoch_, segment_, generation_),
      producer_stream_binding::make(
        topic_, *other_range, routing_epoch_, segment_, generation_),
      producer_stream_binding::make(
        topic_, range_, *other_routing_epoch, segment_, generation_),
      producer_stream_binding::make(
        topic_, range_, routing_epoch_, *other_segment, generation_),
      producer_stream_binding::make(
        topic_, range_, routing_epoch_, segment_, *other_generation)};
    for (const auto& variant : variants) {
        ASSERT_TRUE(variant.has_value());
        EXPECT_NE(*original, *variant);
        const auto mismatch = variant->validate_expected(*original);
        const auto reverse_mismatch = original->validate_expected(*variant);
        ASSERT_FALSE(mismatch.has_value());
        ASSERT_FALSE(reverse_mismatch.has_value());
        EXPECT_EQ(mismatch.error(), kwaque::errc::invalid_argument);
        EXPECT_EQ(reverse_mismatch.error(), kwaque::errc::invalid_argument);
        EXPECT_EQ(*original, saved);
    }
    EXPECT_TRUE(original->validate_expected(saved).has_value());
}

TEST_F(
  ProducerStreamBindingTest, ComponentsOutliveTemporaryBindingsAndResults) {
    const auto original = make_binding();
    ASSERT_TRUE(original.has_value());
    const auto copy = *original;
    auto topic = producer_stream_binding{copy}.topic();
    const auto range = producer_stream_binding{copy}.range();
    const auto routing_epoch = producer_stream_binding{copy}.routing_epoch();
    const auto segment = producer_stream_binding{copy}.segment();
    const auto generation = producer_stream_binding{copy}.generation();
    const auto result_topic = decltype(original){copy}->topic();
    EXPECT_EQ(topic, topic_);
    EXPECT_EQ(range, range_);
    EXPECT_EQ(routing_epoch, routing_epoch_);
    EXPECT_EQ(segment, segment_);
    EXPECT_EQ(generation, generation_);
    EXPECT_EQ(result_topic, topic_);
    topic = topic_id{};
    EXPECT_TRUE(topic.is_nil());
    EXPECT_EQ(original->topic(), topic_);
    EXPECT_EQ(*original, copy);
}

} // namespace
