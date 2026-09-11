#include "src/base/error.h"
#include "src/base/result.h"
#include "src/base/units.h"
#include "src/model/topic_policy.h"
#include "src/runtime/time.h"

#include <gtest/gtest.h>

#include <array>
#include <concepts>
#include <cstdint>
#include <limits>
#include <type_traits>
#include <utility>

namespace {

using kwaque::byte_count;
using kwaque::item_count;
using kwaque::model::topic_policy;
using kwaque::runtime::monotonic_duration;
using kwaque::runtime::monotonic_time;
using kwaque::runtime::wall_time;

constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();

template<typename RecordBytes, typename SegmentBytes, typename Lifetime>
concept policy_construction_expression = requires(
  RecordBytes record, SegmentBytes segment, Lifetime lifetime) {
    topic_policy::make(record, segment, lifetime);
};

template<typename Policy>
consteval bool owned_policy_components() {
    return std::same_as<
             decltype(std::declval<Policy>().maximum_record_bytes()),
             byte_count>
           && std::same_as<
             decltype(std::declval<Policy>().segment_bytes()),
             byte_count>
           && std::same_as<
             decltype(std::declval<Policy>().segment_lifetime()),
             monotonic_duration>;
}

static_assert(
  sizeof(topic_policy) == 2 * sizeof(byte_count) + sizeof(monotonic_duration));
static_assert(std::is_standard_layout_v<topic_policy>);
static_assert(std::is_trivially_copyable_v<topic_policy>);
static_assert(std::is_nothrow_copy_constructible_v<topic_policy>);
static_assert(std::is_nothrow_copy_assignable_v<topic_policy>);
static_assert(std::is_nothrow_move_constructible_v<topic_policy>);
static_assert(std::is_nothrow_move_assignable_v<topic_policy>);
static_assert(std::is_nothrow_destructible_v<topic_policy>);
static_assert(!std::is_aggregate_v<topic_policy>);
static_assert(!std::default_initializable<topic_policy>);
static_assert(!std::constructible_from<
              topic_policy,
              byte_count,
              byte_count,
              monotonic_duration>);
static_assert(
  policy_construction_expression<byte_count, byte_count, monotonic_duration>);
static_assert(!policy_construction_expression<
              std::uint64_t,
              byte_count,
              monotonic_duration>);
static_assert(!policy_construction_expression<
              byte_count,
              std::uint64_t,
              monotonic_duration>);
static_assert(
  !policy_construction_expression<byte_count, byte_count, std::uint64_t>);
static_assert(
  !policy_construction_expression<item_count, byte_count, monotonic_duration>);
static_assert(
  !policy_construction_expression<byte_count, item_count, monotonic_duration>);
static_assert(
  !policy_construction_expression<byte_count, byte_count, monotonic_time>);
static_assert(
  !policy_construction_expression<byte_count, byte_count, wall_time>);
static_assert(owned_policy_components<topic_policy&>());
static_assert(owned_policy_components<const topic_policy&>());
static_assert(owned_policy_components<topic_policy>());
static_assert(owned_policy_components<const topic_policy>());
static_assert(std::same_as<
              decltype(std::declval<const topic_policy&>()
                         .effective_record_limit(byte_count{1})),
              kwaque::result<byte_count>>);
static_assert(noexcept(topic_policy::defaults()));
static_assert(noexcept(
  topic_policy::make(byte_count{1}, byte_count{1}, monotonic_duration{1})));
static_assert(noexcept(
  std::declval<const topic_policy&>().effective_record_limit(byte_count{1})));

TEST(TopicPolicyTest, DefaultsUseExplicitRecordSegmentAndDurationUnits) {
    constexpr auto defaults = topic_policy::defaults();
    static_assert(defaults.maximum_record_bytes().value() == 1'048'576);
    static_assert(defaults.segment_bytes().value() == 1'000'000'000);
    static_assert(
      defaults.segment_lifetime().nanoseconds() == 3'600'000'000'000);

    const auto explicit_defaults = topic_policy::make(
      byte_count{1'048'576},
      byte_count{1'000'000'000},
      monotonic_duration{3'600'000'000'000});
    ASSERT_TRUE(explicit_defaults.has_value());
    EXPECT_EQ(*explicit_defaults, defaults);
    EXPECT_NE(defaults.segment_bytes(), byte_count{1'073'741'824});
}

TEST(TopicPolicyTest, PreservesConfiguredDecimalMegabytesAndThirtyMinutes) {
    const auto configured = topic_policy::make(
      byte_count{1'048'576},
      byte_count{512'000'000},
      monotonic_duration{1'800'000'000'000});
    ASSERT_TRUE(configured.has_value());
    EXPECT_EQ(configured->maximum_record_bytes(), byte_count{1'048'576});
    EXPECT_EQ(configured->segment_bytes(), byte_count{512'000'000});
    EXPECT_EQ(
      configured->segment_lifetime(), monotonic_duration{1'800'000'000'000});
    EXPECT_NE(configured->segment_bytes(), byte_count{536'870'912});
    EXPECT_NE(*configured, topic_policy::defaults());
}

TEST(TopicPolicyTest, RejectsEachZeroFieldWithoutUnlimitedSentinels) {
    const std::array invalid_cases{
      topic_policy::make(byte_count{}, byte_count{1}, monotonic_duration{1}),
      topic_policy::make(byte_count{1}, byte_count{}, monotonic_duration{1}),
      topic_policy::make(byte_count{1}, byte_count{1}, monotonic_duration{})};
    for (const auto& invalid : invalid_cases) {
        ASSERT_FALSE(invalid.has_value());
        EXPECT_EQ(invalid.error(), kwaque::errc::invalid_argument);
    }
}

TEST(TopicPolicyTest, PreservesPositiveExtremesAndIndependentSegmentBounds) {
    const auto largest = topic_policy::make(
      byte_count{maximum}, byte_count{maximum}, monotonic_duration{maximum});
    ASSERT_TRUE(largest.has_value());
    EXPECT_EQ(largest->maximum_record_bytes(), byte_count{maximum});
    EXPECT_EQ(largest->segment_bytes(), byte_count{maximum});
    EXPECT_EQ(largest->segment_lifetime(), monotonic_duration{maximum});

    const auto smallest = topic_policy::make(
      byte_count{1}, byte_count{1}, monotonic_duration{1});
    ASSERT_TRUE(smallest.has_value());
    EXPECT_EQ(smallest->maximum_record_bytes(), byte_count{1});
    EXPECT_EQ(smallest->segment_bytes(), byte_count{1});
    EXPECT_EQ(smallest->segment_lifetime(), monotonic_duration{1});

    const auto independent = topic_policy::make(
      byte_count{maximum}, byte_count{1}, monotonic_duration{1});
    ASSERT_TRUE(independent.has_value());
    EXPECT_EQ(independent->maximum_record_bytes(), byte_count{maximum});
    EXPECT_EQ(independent->segment_bytes(), byte_count{1});
}

TEST(TopicPolicyTest, EffectiveRecordLimitIntersectsWithoutMutatingPolicy) {
    struct limit_case {
        std::uint64_t topic;
        std::uint64_t codec;
        std::uint64_t expected;
    };
    const std::array cases{
      limit_case{1, 1, 1},
      limit_case{1'048'576, 524'288, 524'288},
      limit_case{524'288, 1'048'576, 524'288},
      limit_case{1'048'576, 1'048'576, 1'048'576},
      limit_case{2'097'152, 1'048'576, 1'048'576},
      limit_case{1'048'576, 2'097'152, 1'048'576},
      limit_case{maximum, 1'048'576, 1'048'576},
      limit_case{1'048'576, maximum, 1'048'576},
      limit_case{maximum, maximum, maximum}};
    for (const auto& input : cases) {
        const auto policy = topic_policy::make(
          byte_count{input.topic}, byte_count{1}, monotonic_duration{1});
        ASSERT_TRUE(policy.has_value());
        const auto original = *policy;
        const auto effective = policy->effective_record_limit(
          byte_count{input.codec});
        ASSERT_TRUE(effective.has_value());
        EXPECT_EQ(*effective, byte_count{input.expected});
        EXPECT_EQ(policy->maximum_record_bytes(), byte_count{input.topic});
        EXPECT_EQ(*policy, original);
    }

    const auto defaults = topic_policy::defaults();
    const auto invalid = defaults.effective_record_limit(byte_count{});
    ASSERT_FALSE(invalid.has_value());
    EXPECT_EQ(invalid.error(), kwaque::errc::invalid_argument);
    EXPECT_EQ(defaults, topic_policy::defaults());
}

TEST(TopicPolicyTest, EqualityIncludesEveryFieldAndGettersOwnTheirValues) {
    constexpr auto defaults = topic_policy::defaults();
    const std::array variants{
      topic_policy::make(
        byte_count{1'048'577},
        defaults.segment_bytes(),
        defaults.segment_lifetime()),
      topic_policy::make(
        defaults.maximum_record_bytes(),
        byte_count{1'000'000'001},
        defaults.segment_lifetime()),
      topic_policy::make(
        defaults.maximum_record_bytes(),
        defaults.segment_bytes(),
        monotonic_duration{3'600'000'000'001})};
    for (const auto& variant : variants) {
        ASSERT_TRUE(variant.has_value());
        EXPECT_NE(*variant, defaults);
    }

    auto record_bytes = topic_policy::defaults().maximum_record_bytes();
    const auto segment_bytes = topic_policy::defaults().segment_bytes();
    const auto lifetime = topic_policy::defaults().segment_lifetime();
    EXPECT_EQ(record_bytes, byte_count{1'048'576});
    EXPECT_EQ(segment_bytes, byte_count{1'000'000'000});
    EXPECT_EQ(lifetime, monotonic_duration{3'600'000'000'000});
    record_bytes = byte_count{};
    EXPECT_EQ(record_bytes, byte_count{});
    EXPECT_EQ(defaults.maximum_record_bytes(), byte_count{1'048'576});
}

} // namespace
