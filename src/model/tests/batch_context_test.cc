#include "src/base/error.h"
#include "src/base/units.h"
#include "src/codec/limits.h"
#include "src/model/batch_context.h"
#include "src/model/batch_identity.h"
#include "src/model/epoch.h"
#include "src/model/identity.h"
#include "src/model/position.h"
#include "src/runtime/file_position.h"
#include "src/runtime/time.h"

#include <gtest/gtest.h>

#include <array>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <type_traits>
#include <utility>

namespace {

using kwaque::byte_count;
using kwaque::item_count;
using kwaque::model::assigned_batch_context;
using kwaque::model::batch_id;
using kwaque::model::batch_sequence;
using kwaque::model::producer_epoch;
using kwaque::model::producer_id;
using kwaque::model::producer_stream_binding;
using kwaque::model::producer_stream_id;
using kwaque::model::range_id;
using kwaque::model::range_logical_count;
using kwaque::model::range_logical_end;
using kwaque::model::range_logical_offset;
using kwaque::model::range_logical_span;
using kwaque::model::range_routing_epoch;
using kwaque::model::segment_generation;
using kwaque::model::segment_id;
using kwaque::model::segment_record_count;
using kwaque::model::segment_relative_end;
using kwaque::model::submitted_batch_context;
using kwaque::model::topic_id;
using kwaque::runtime::file_position;
using kwaque::runtime::monotonic_time;
using kwaque::runtime::wall_time;

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

template<typename Count, typename Time>
concept submission_expression = requires(
  batch_id id, producer_stream_binding binding, Count count, Time timestamp) {
    submitted_batch_context::make(id, binding, count, timestamp);
};

template<typename Base>
concept assignment_expression = requires(
  submitted_batch_context submitted,
  Base base,
  producer_stream_binding binding) {
    assigned_batch_context::assign(submitted, base, binding);
};

template<typename Count>
concept retained_count_expression = requires(
  const assigned_batch_context& assigned, Count count) {
    assigned.with_retained_count(count);
};

template<typename Value>
concept assigned_position_expression = requires(const Value& value) {
    value.logical_span();
};

template<typename Context>
consteval bool owned_submission_members() {
    return std::same_as<decltype(std::declval<Context>().id()), batch_id>
           && std::same_as<
             decltype(std::declval<Context>().binding()),
             producer_stream_binding>
           && std::same_as<
             decltype(std::declval<Context>().original_count()),
             range_logical_count>
           && std::same_as<
             decltype(std::declval<Context>().original_timestamp_base()),
             wall_time>;
}

template<typename Context>
consteval bool owned_assignment_members() {
    return std::same_as<
             decltype(std::declval<Context>().submitted()),
             submitted_batch_context>
           && std::same_as<
             decltype(std::declval<Context>().logical_span()),
             range_logical_span>
           && std::same_as<
             decltype(std::declval<Context>().retained_count()),
             item_count>;
}

template<typename Context>
consteval bool context_value_contract(std::size_t bytes) {
    return sizeof(Context) == bytes && std::is_standard_layout_v<Context>
           && std::is_trivially_copyable_v<Context>
           && std::is_nothrow_copy_constructible_v<Context>
           && std::is_nothrow_copy_assignable_v<Context>
           && std::is_nothrow_move_constructible_v<Context>
           && std::is_nothrow_move_assignable_v<Context>
           && std::is_nothrow_destructible_v<Context>
           && !std::default_initializable<Context>
           && !std::is_aggregate_v<Context>;
}

static_assert(context_value_contract<submitted_batch_context>(
  sizeof(batch_id) + sizeof(producer_stream_binding)
  + sizeof(range_logical_count) + sizeof(wall_time)));
static_assert(context_value_contract<assigned_batch_context>(
  sizeof(submitted_batch_context) + sizeof(range_logical_span)
  + sizeof(item_count)));
static_assert(!std::constructible_from<
              submitted_batch_context,
              batch_id,
              producer_stream_binding,
              range_logical_count,
              wall_time>);
static_assert(!std::constructible_from<
              assigned_batch_context,
              submitted_batch_context,
              range_logical_span,
              item_count>);
static_assert(
  !std::convertible_to<submitted_batch_context, assigned_batch_context>);
static_assert(
  !std::convertible_to<assigned_batch_context, submitted_batch_context>);
static_assert(submission_expression<range_logical_count, wall_time>);
static_assert(!submission_expression<item_count, wall_time>);
static_assert(!submission_expression<byte_count, wall_time>);
static_assert(!submission_expression<std::uint64_t, wall_time>);
static_assert(!submission_expression<range_logical_count, monotonic_time>);
static_assert(!submission_expression<range_logical_count, std::int64_t>);
static_assert(!assigned_position_expression<submitted_batch_context>);
static_assert(assignment_expression<range_logical_end>);
static_assert(!assignment_expression<range_logical_offset>);
static_assert(!assignment_expression<segment_relative_end>);
static_assert(!assignment_expression<file_position>);
static_assert(!assignment_expression<std::uint64_t>);
static_assert(retained_count_expression<item_count>);
static_assert(!retained_count_expression<range_logical_count>);
static_assert(!retained_count_expression<segment_record_count>);
static_assert(!retained_count_expression<byte_count>);
static_assert(!retained_count_expression<std::uint64_t>);
static_assert(owned_submission_members<submitted_batch_context&>());
static_assert(owned_submission_members<const submitted_batch_context&>());
static_assert(owned_submission_members<submitted_batch_context>());
static_assert(owned_submission_members<const submitted_batch_context>());
static_assert(owned_assignment_members<assigned_batch_context&>());
static_assert(owned_assignment_members<const assigned_batch_context&>());
static_assert(owned_assignment_members<assigned_batch_context>());
static_assert(owned_assignment_members<const assigned_batch_context>());
static_assert(noexcept(submitted_batch_context::make(
  std::declval<batch_id>(),
  std::declval<producer_stream_binding>(),
  range_logical_count{1},
  wall_time{})));
static_assert(noexcept(assigned_batch_context::assign(
  std::declval<submitted_batch_context>(),
  range_logical_end{},
  std::declval<const producer_stream_binding&>())));
static_assert(noexcept(std::declval<const assigned_batch_context&>()
                         .with_retained_count(item_count{1})));

class BatchContextTest : public ::testing::Test {
protected:
    void SetUp() override {
        const auto producer = producer_id::make(object_bytes);
        const auto epoch = producer_epoch::make(7);
        const auto stream = producer_stream_id::make(11);
        const auto topic = topic_id::make(object_bytes);
        const auto range = range_id::make(object_bytes);
        const auto routing_epoch = range_routing_epoch::make(13);
        const auto segment = segment_id::make(object_bytes);
        const auto generation = segment_generation::make(17);
        ASSERT_TRUE(producer.has_value());
        ASSERT_TRUE(epoch.has_value());
        ASSERT_TRUE(stream.has_value());
        ASSERT_TRUE(topic.has_value());
        ASSERT_TRUE(range.has_value());
        ASSERT_TRUE(routing_epoch.has_value());
        ASSERT_TRUE(segment.has_value());
        ASSERT_TRUE(generation.has_value());
        const auto id = batch_id::make(
          *producer, *epoch, *stream, batch_sequence{19});
        const auto binding = producer_stream_binding::make(
          *topic, *range, *routing_epoch, *segment, *generation);
        ASSERT_TRUE(id.has_value());
        ASSERT_TRUE(binding.has_value());
        id_ = *id;
        binding_ = *binding;
    }

    auto make_submitted(
      range_logical_count count = range_logical_count{5},
      wall_time timestamp = wall_time{-5}) const noexcept {
        return submitted_batch_context::make(*id_, *binding_, count, timestamp);
    }

    std::optional<batch_id> id_;
    std::optional<producer_stream_binding> binding_;
};

TEST_F(BatchContextTest, SubmissionRequiresNonemptyBoundedOriginalCount) {
    const auto empty = make_submitted(range_logical_count{});
    ASSERT_FALSE(empty.has_value());
    EXPECT_EQ(empty.error(), kwaque::errc::invalid_argument);
    for (const auto count : {std::uint64_t{4097}, maximum}) {
        const auto too_many = make_submitted(range_logical_count{count});
        ASSERT_FALSE(too_many.has_value());
        EXPECT_EQ(too_many.error(), kwaque::errc::resource_exhausted);
    }
    for (const auto count : {std::uint64_t{1}, std::uint64_t{4096}}) {
        const auto submitted = make_submitted(range_logical_count{count});
        ASSERT_TRUE(submitted.has_value());
        EXPECT_EQ(submitted->id(), *id_);
        EXPECT_EQ(submitted->binding(), *binding_);
        EXPECT_EQ(submitted->original_count(), range_logical_count{count});
        EXPECT_EQ(submitted->original_timestamp_base(), wall_time{-5});
    }
}

TEST_F(BatchContextTest, AssignmentPreservesSubmissionAndChecksExclusiveEnd) {
    struct assignment_case {
        std::uint64_t count;
        std::uint64_t base;
        std::uint64_t end;
    };
    const std::array cases{
      assignment_case{1, 0, 1},
      assignment_case{5, 100, 105},
      assignment_case{1, maximum - 1U, maximum},
      assignment_case{4096, maximum - 4096U, maximum}};
    for (const auto& input : cases) {
        const auto submitted = make_submitted(range_logical_count{input.count});
        ASSERT_TRUE(submitted.has_value());
        const auto original = *submitted;
        const range_logical_end base{input.base};
        const auto assigned = assigned_batch_context::assign(
          *submitted, base, *binding_);
        ASSERT_TRUE(assigned.has_value());
        EXPECT_EQ(assigned->submitted(), original);
        EXPECT_EQ(assigned->logical_span().begin(), base);
        EXPECT_EQ(assigned->logical_span().end(), range_logical_end{input.end});
        EXPECT_EQ(
          assigned->logical_span().count(), range_logical_count{input.count});
        EXPECT_EQ(assigned->retained_count(), item_count{input.count});
        EXPECT_EQ(*submitted, original);
        EXPECT_EQ(base.value(), input.base);
    }

    for (const auto& [count, base] :
         {std::pair{std::uint64_t{1}, maximum},
          std::pair{std::uint64_t{2}, maximum - 1U},
          std::pair{std::uint64_t{4096}, maximum - 4095U}}) {
        const auto submitted = make_submitted(range_logical_count{count});
        ASSERT_TRUE(submitted.has_value());
        const auto original = *submitted;
        const auto overflow = assigned_batch_context::assign(
          *submitted, range_logical_end{base}, *binding_);
        ASSERT_FALSE(overflow.has_value());
        EXPECT_EQ(overflow.error(), kwaque::errc::out_of_range);
        EXPECT_EQ(*submitted, original);
    }
}

TEST_F(
  BatchContextTest, AssignmentRejectsEachIndependentExpectedBindingMismatch) {
    auto changed_bytes = object_bytes;
    changed_bytes.back() ^= 0xff;
    const auto other_topic = topic_id::make(changed_bytes);
    const auto other_range = range_id::make(changed_bytes);
    const auto other_routing_epoch = range_routing_epoch::make(14);
    const auto other_segment = segment_id::make(changed_bytes);
    const auto other_generation = segment_generation::make(18);
    ASSERT_TRUE(other_topic.has_value());
    ASSERT_TRUE(other_range.has_value());
    ASSERT_TRUE(other_routing_epoch.has_value());
    ASSERT_TRUE(other_segment.has_value());
    ASSERT_TRUE(other_generation.has_value());
    const std::array mismatches{
      producer_stream_binding::make(
        *other_topic,
        binding_->range(),
        binding_->routing_epoch(),
        binding_->segment(),
        binding_->generation()),
      producer_stream_binding::make(
        binding_->topic(),
        *other_range,
        binding_->routing_epoch(),
        binding_->segment(),
        binding_->generation()),
      producer_stream_binding::make(
        binding_->topic(),
        binding_->range(),
        *other_routing_epoch,
        binding_->segment(),
        binding_->generation()),
      producer_stream_binding::make(
        binding_->topic(),
        binding_->range(),
        binding_->routing_epoch(),
        *other_segment,
        binding_->generation()),
      producer_stream_binding::make(
        binding_->topic(),
        binding_->range(),
        binding_->routing_epoch(),
        binding_->segment(),
        *other_generation)};
    const auto submitted = make_submitted();
    ASSERT_TRUE(submitted.has_value());
    const auto original = *submitted;
    for (const auto& expected : mismatches) {
        ASSERT_TRUE(expected.has_value());
        const auto assigned = assigned_batch_context::assign(
          *submitted, range_logical_end{}, *expected);
        ASSERT_FALSE(assigned.has_value());
        EXPECT_EQ(assigned.error(), kwaque::errc::invalid_argument);
        EXPECT_EQ(*submitted, original);
    }
}

TEST_F(
  BatchContextTest, RepeatedSparseRewriteCannotGrowTheCurrentSurvivorCount) {
    const auto submitted = make_submitted(range_logical_count{11});
    ASSERT_TRUE(submitted.has_value());
    const auto dense = assigned_batch_context::assign(
      *submitted, range_logical_end{100}, *binding_);
    ASSERT_TRUE(dense.has_value());
    const auto unchanged = dense->with_retained_count(item_count{11});
    ASSERT_TRUE(unchanged.has_value());
    EXPECT_EQ(*unchanged, *dense);

    const auto five = dense->with_retained_count(item_count{5});
    ASSERT_TRUE(five.has_value());
    const auto two = five->with_retained_count(item_count{2});
    ASSERT_TRUE(two.has_value());
    const auto one = two->with_retained_count(item_count{1});
    ASSERT_TRUE(one.has_value());
    for (const auto& current : std::array{*dense, *five, *two, *one}) {
        const auto saved = current;
        const auto same_count = current.with_retained_count(
          current.retained_count());
        ASSERT_TRUE(same_count.has_value());
        EXPECT_EQ(*same_count, saved);
        EXPECT_EQ(current.submitted(), *submitted);
        EXPECT_EQ(current.logical_span(), dense->logical_span());
        EXPECT_EQ(current.logical_span().begin(), range_logical_end{100});
        EXPECT_EQ(current.logical_span().end(), range_logical_end{111});
        const auto empty = current.with_retained_count(item_count{});
        const auto growth = current.with_retained_count(
          item_count{current.retained_count().value() + 1U});
        const auto enormous_growth = current.with_retained_count(
          item_count{maximum});
        ASSERT_FALSE(empty.has_value());
        ASSERT_FALSE(growth.has_value());
        ASSERT_FALSE(enormous_growth.has_value());
        EXPECT_EQ(empty.error(), kwaque::errc::invalid_argument);
        EXPECT_EQ(growth.error(), kwaque::errc::invalid_argument);
        EXPECT_EQ(enormous_growth.error(), kwaque::errc::invalid_argument);
        EXPECT_EQ(current, saved);
    }
    EXPECT_EQ(dense->retained_count(), item_count{11});
    EXPECT_EQ(five->retained_count(), item_count{5});
    EXPECT_EQ(two->retained_count(), item_count{2});
    EXPECT_EQ(one->retained_count(), item_count{1});
}

TEST_F(BatchContextTest, ASingleSurvivorDoesNotCollapseTheOriginalSpan) {
    for (const auto count : {std::uint64_t{11}, std::uint64_t{501}}) {
        const auto submitted = make_submitted(range_logical_count{count});
        ASSERT_TRUE(submitted.has_value());
        const auto assigned = assigned_batch_context::assign(
          *submitted, range_logical_end{}, *binding_);
        ASSERT_TRUE(assigned.has_value());
        const auto sparse = assigned->with_retained_count(item_count{1});
        ASSERT_TRUE(sparse.has_value());
        EXPECT_EQ(sparse->retained_count(), item_count{1});
        EXPECT_EQ(
          sparse->submitted().original_count(), range_logical_count{count});
        EXPECT_EQ(sparse->logical_span().begin(), range_logical_end{});
        EXPECT_EQ(sparse->logical_span().end(), range_logical_end{count});
        EXPECT_EQ(sparse->logical_span().count(), range_logical_count{count});
    }
}

TEST_F(BatchContextTest, NarrowerLimitsCountOriginalSlotsAfterSparseRewrite) {
    kwaque::codec::limits_config config;
    config.max_original_records = item_count{64};
    const auto limits = kwaque::codec::limits::make(config);
    ASSERT_TRUE(limits.has_value());

    for (const auto& [original_count, accepted] :
         {std::pair{std::uint64_t{501}, false},
          std::pair{std::uint64_t{11}, true}}) {
        const auto submitted = make_submitted(
          range_logical_count{original_count});
        ASSERT_TRUE(submitted.has_value());
        const auto assigned = assigned_batch_context::assign(
          *submitted, range_logical_end{}, *binding_);
        ASSERT_TRUE(assigned.has_value());
        const auto sparse = assigned->with_retained_count(item_count{1});
        ASSERT_TRUE(sparse.has_value());

        const auto valid = limits->validate_batch_counts(
          item_count{sparse->submitted().original_count().value()},
          sparse->retained_count(),
          item_count{});
        ASSERT_EQ(valid.has_value(), accepted) << original_count;
        if (!accepted) {
            EXPECT_EQ(valid.error(), kwaque::errc::resource_exhausted);
        }
        EXPECT_EQ(
          sparse->submitted().original_count(),
          range_logical_count{original_count});
        EXPECT_EQ(sparse->retained_count(), item_count{1});
    }
}

TEST_F(
  BatchContextTest, AssignmentAndSparseRewritePreserveSignedTimestampExtremes) {
    for (const auto timestamp :
         {std::numeric_limits<std::int64_t>::min(),
          std::int64_t{-1},
          std::int64_t{0},
          std::numeric_limits<std::int64_t>::max()}) {
        const auto submitted = make_submitted(
          range_logical_count{5}, wall_time{timestamp});
        ASSERT_TRUE(submitted.has_value());
        const auto assigned = assigned_batch_context::assign(
          *submitted, range_logical_end{42}, *binding_);
        ASSERT_TRUE(assigned.has_value());
        const auto sparse = assigned->with_retained_count(item_count{1});
        ASSERT_TRUE(sparse.has_value());
        EXPECT_EQ(submitted->original_timestamp_base(), wall_time{timestamp});
        EXPECT_EQ(assigned->submitted(), *submitted);
        EXPECT_EQ(sparse->submitted(), *submitted);
        EXPECT_EQ(
          sparse->submitted().original_timestamp_base(), wall_time{timestamp});
    }
}

TEST_F(BatchContextTest, EqualityIncludesOriginalMetadataSpanAndRetainedCount) {
    const auto submitted = make_submitted();
    ASSERT_TRUE(submitted.has_value());
    const auto other_id = batch_id::make(
      id_->producer(), id_->epoch(), id_->stream(), batch_sequence{20});
    const auto next_epoch = range_routing_epoch::make(14);
    ASSERT_TRUE(other_id.has_value());
    ASSERT_TRUE(next_epoch.has_value());
    const auto other_binding = producer_stream_binding::make(
      binding_->topic(),
      binding_->range(),
      *next_epoch,
      binding_->segment(),
      binding_->generation());
    ASSERT_TRUE(other_binding.has_value());
    const std::array different_submissions{
      submitted_batch_context::make(
        *other_id, *binding_, range_logical_count{5}, wall_time{-5}),
      submitted_batch_context::make(
        *id_, *other_binding, range_logical_count{5}, wall_time{-5}),
      make_submitted(range_logical_count{6}),
      make_submitted(range_logical_count{5}, wall_time{-6})};
    for (const auto& different : different_submissions) {
        ASSERT_TRUE(different.has_value());
        EXPECT_NE(*different, *submitted);
    }

    const auto original = assigned_batch_context::assign(
      *submitted, range_logical_end{100}, *binding_);
    const auto other_span = assigned_batch_context::assign(
      *submitted, range_logical_end{101}, *binding_);
    const auto other_metadata = assigned_batch_context::assign(
      *different_submissions.back(), range_logical_end{100}, *binding_);
    ASSERT_TRUE(original.has_value());
    ASSERT_TRUE(other_span.has_value());
    ASSERT_TRUE(other_metadata.has_value());
    const auto fewer_records = original->with_retained_count(item_count{4});
    ASSERT_TRUE(fewer_records.has_value());
    EXPECT_NE(*original, *other_span);
    EXPECT_NE(*original, *other_metadata);
    EXPECT_NE(*original, *fewer_records);
    EXPECT_EQ(*original, assigned_batch_context{*original});
}

TEST_F(BatchContextTest, GettersOwnValuesAcrossTemporaryContextsAndResults) {
    const auto submitted = make_submitted();
    ASSERT_TRUE(submitted.has_value());
    const auto id = submitted_batch_context{*submitted}.id();
    const auto binding = submitted_batch_context{*submitted}.binding();
    auto original_count = submitted_batch_context{*submitted}.original_count();
    const auto timestamp
      = decltype(submitted){*submitted}->original_timestamp_base();
    EXPECT_EQ(id, *id_);
    EXPECT_EQ(binding, *binding_);
    EXPECT_EQ(original_count, range_logical_count{5});
    EXPECT_EQ(timestamp, wall_time{-5});

    const auto assigned = assigned_batch_context::assign(
      *submitted, range_logical_end{100}, *binding_);
    ASSERT_TRUE(assigned.has_value());
    const auto original = assigned_batch_context{*assigned}.submitted();
    const auto span = decltype(assigned){*assigned}->logical_span();
    auto retained = assigned_batch_context{*assigned}.retained_count();
    EXPECT_EQ(original, *submitted);
    EXPECT_EQ(span.begin(), range_logical_end{100});
    EXPECT_EQ(span.end(), range_logical_end{105});
    EXPECT_EQ(retained, item_count{5});
    original_count = range_logical_count{};
    retained = item_count{};
    EXPECT_EQ(original_count, range_logical_count{});
    EXPECT_EQ(retained, item_count{});
    EXPECT_EQ(submitted->original_count(), range_logical_count{5});
    EXPECT_EQ(assigned->retained_count(), item_count{5});
}

} // namespace
