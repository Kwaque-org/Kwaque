#include "src/base/error.h"
#include "src/base/units.h"
#include "src/model/batch_context.h"
#include "src/model/batch_identity.h"
#include "src/model/epoch.h"
#include "src/model/identity.h"
#include "src/model/keyspace.h"
#include "src/model/position.h"
#include "src/runtime/time.h"

#include <gtest/gtest.h>

#include <array>
#include <compare>
#include <cstddef>
#include <cstdint>

namespace {

using kwaque::item_count;
using kwaque::model::assigned_batch_context;
using kwaque::model::batch_id;
using kwaque::model::batch_sequence;
using kwaque::model::keyspace_interval;
using kwaque::model::physical_address;
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
using kwaque::model::record_id;
using kwaque::model::segment_generation;
using kwaque::model::segment_id;
using kwaque::model::segment_relative_offset;
using kwaque::model::submitted_batch_context;
using kwaque::model::topic_id;
using kwaque::model::validate_keyspace_coverage;
using kwaque::runtime::wall_time;

template<typename Id>
auto fixed_id(std::uint8_t marker) {
    std::array<std::uint8_t, 16> bytes{};
    bytes.back() = marker;
    return Id::make(bytes);
}

TEST(RangeContextTest, SplitAndMergeKeepNewIdentitiesAndSiblingPositions) {
    const auto topic = fixed_id<topic_id>(1);
    // Explicit fixture identities, deliberately not in keyspace order.
    const std::array ids{
      fixed_id<range_id>(0x40),
      fixed_id<range_id>(0xe0),
      fixed_id<range_id>(0x10),
      fixed_id<range_id>(0x20)};
    ASSERT_TRUE(topic.has_value());
    for (const auto& id : ids) {
        ASSERT_TRUE(id.has_value());
    }
    const auto left = keyspace_interval::make(0, 1);
    const auto right = keyspace_interval::make(0x8000'0000'0000'0000ULL, 1);
    ASSERT_TRUE(left.has_value());
    ASSERT_TRUE(right.has_value());
    struct range_snapshot final {
        range_id id;
        keyspace_interval interval;
    };
    // Value snapshots only: no allocator or lineage transition is exercised.
    const range_snapshot original{*ids[0], keyspace_interval::root()};
    const std::array children{
      range_snapshot{*ids[1], *left}, range_snapshot{*ids[2], *right}};
    const range_snapshot merged{*ids[3], keyspace_interval::root()};
    const std::array child_intervals{
      children[0].interval, children[1].interval};
    EXPECT_TRUE(validate_keyspace_coverage(child_intervals, original.interval));
    EXPECT_TRUE(validate_keyspace_coverage(child_intervals, merged.interval));
    EXPECT_TRUE(children[0].interval.is_buddy_of(children[1].interval));
    EXPECT_EQ(merged.interval, original.interval);
    EXPECT_NE(merged.id, original.id);

    // Offset zero is local to each range, including a new merged range whose
    // interval equals the historical root. Geometry cannot alias RecordIDs.
    const std::array first_records{
      record_id::make(*topic, original.id, range_logical_offset{}),
      record_id::make(*topic, children[0].id, range_logical_offset{}),
      record_id::make(*topic, children[1].id, range_logical_offset{}),
      record_id::make(*topic, merged.id, range_logical_offset{})};
    for (const auto& record : first_records) {
        ASSERT_TRUE(record.has_value());
        EXPECT_EQ(record->offset().value(), 0U);
    }
    for (std::size_t lhs = 0; lhs < first_records.size(); ++lhs) {
        for (std::size_t rhs = 0; rhs < first_records.size(); ++rhs) {
            SCOPED_TRACE(::testing::Message() << lhs << ':' << rhs);
            const auto comparison = first_records[lhs]->compare_in_range(
              *first_records[rhs]);
            if (lhs == rhs) {
                ASSERT_TRUE(comparison.has_value());
                EXPECT_EQ(*comparison, std::strong_ordering::equal);
            } else {
                EXPECT_NE(*first_records[lhs], *first_records[rhs]);
                ASSERT_FALSE(comparison.has_value());
                EXPECT_EQ(comparison.error(), kwaque::errc::invalid_argument);
            }
        }
    }

    // The two children advance by independent counts; the merged range does
    // not inherit their sum or the predecessor's logical position.
    const auto left_append = range_logical_span::from_count(
      first_records[1]->offset().as_end(), range_logical_count{1});
    const auto right_append = range_logical_span::from_count(
      first_records[2]->offset().as_end(), range_logical_count{2});
    ASSERT_TRUE(left_append.has_value());
    ASSERT_TRUE(right_append.has_value());
    EXPECT_EQ(left_append->begin().value(), 0U);
    EXPECT_EQ(right_append->begin().value(), 0U);
    EXPECT_EQ(left_append->end().value(), 1U);
    EXPECT_EQ(right_append->end().value(), 2U);
    EXPECT_EQ(first_records[3]->offset().value(), 0U);
}

TEST(RangeContextTest, SparseRewriteKeepsOriginalBindingAcrossPhysicalLayouts) {
    const auto topic = fixed_id<topic_id>(1);
    const auto range = fixed_id<range_id>(2);
    const auto producer = fixed_id<producer_id>(3);
    const auto original_segment = fixed_id<segment_id>(4);
    const auto replacement_segment = fixed_id<segment_id>(5);
    const auto epoch = producer_epoch::make(7);
    const auto stream = producer_stream_id::make(11);
    const auto routing = range_routing_epoch::make(13);
    const auto original_generation = segment_generation::make(17);
    const auto replacement_generation = segment_generation::make(19);
    ASSERT_TRUE(topic.has_value());
    ASSERT_TRUE(range.has_value());
    ASSERT_TRUE(producer.has_value());
    ASSERT_TRUE(original_segment.has_value());
    ASSERT_TRUE(replacement_segment.has_value());
    ASSERT_TRUE(epoch.has_value());
    ASSERT_TRUE(stream.has_value());
    ASSERT_TRUE(routing.has_value());
    ASSERT_TRUE(original_generation.has_value());
    ASSERT_TRUE(replacement_generation.has_value());
    const auto id = batch_id::make(
      *producer, *epoch, *stream, batch_sequence{23});
    const auto binding = producer_stream_binding::make(
      *topic, *range, *routing, *original_segment, *original_generation);
    const auto replacement_binding = producer_stream_binding::make(
      *topic, *range, *routing, *replacement_segment, *replacement_generation);
    ASSERT_TRUE(id.has_value());
    ASSERT_TRUE(binding.has_value());
    ASSERT_TRUE(replacement_binding.has_value());
    const auto submitted = submitted_batch_context::make(
      *id, *binding, range_logical_count{11}, wall_time{-5000});
    ASSERT_TRUE(submitted.has_value());
    const auto dense = assigned_batch_context::assign(
      *submitted, range_logical_end{1000}, *binding);
    ASSERT_TRUE(dense.has_value());
    const auto sparse = dense->with_retained_count(item_count{1});
    ASSERT_TRUE(sparse.has_value());

    // One selected survivor at original delta 10. The metadata fixture does
    // not claim to verify record bytes or survivor membership.
    const auto logical = range_logical_offset::make(1010);
    const auto old_ordinal = segment_relative_offset::make(10);
    ASSERT_TRUE(logical.has_value());
    ASSERT_TRUE(old_ordinal.has_value());
    const auto record = record_id::make(*topic, *range, *logical);
    const auto original_address = physical_address::make(
      *original_segment, *old_ordinal);
    const auto replacement_address = physical_address::make(
      *replacement_segment, segment_relative_offset{});
    ASSERT_TRUE(record.has_value());
    ASSERT_TRUE(original_address.has_value());
    ASSERT_TRUE(replacement_address.has_value());
    struct record_snapshot final {
        record_id record;
        assigned_batch_context batch;
        physical_address address;
        segment_generation generation;
    };
    const record_snapshot before{
      *record, *dense, *original_address, *original_generation};
    const record_snapshot after{
      before.record, *sparse, *replacement_address, *replacement_generation};
    EXPECT_EQ(after.record, before.record);
    EXPECT_EQ(after.record.offset().value(), 1010U);
    EXPECT_EQ(after.batch.submitted(), before.batch.submitted());
    EXPECT_EQ(after.batch.submitted().id(), *id);
    EXPECT_EQ(after.batch.submitted().binding(), *binding);
    EXPECT_EQ(
      after.batch.submitted().original_timestamp_base(), wall_time{-5000});
    EXPECT_EQ(
      after.batch.submitted().original_count(), range_logical_count{11});
    EXPECT_EQ(after.batch.logical_span(), before.batch.logical_span());
    EXPECT_EQ(after.batch.logical_span().begin().value(), 1000U);
    EXPECT_EQ(after.batch.logical_span().end().value(), 1011U);
    EXPECT_TRUE(after.batch.logical_span().contains(after.record.offset()));
    EXPECT_EQ(before.batch.retained_count(), item_count{11});
    EXPECT_EQ(after.batch.retained_count(), item_count{1});
    EXPECT_NE(after.address.segment(), before.address.segment());
    EXPECT_EQ(before.address.offset().value(), 10U);
    EXPECT_EQ(after.address.offset().value(), 0U);
    EXPECT_NE(after.generation, before.generation);

    const auto different_layout = before.address.compare_in_layout(
      after.address, before.generation, after.generation);
    ASSERT_FALSE(different_layout.has_value());
    EXPECT_EQ(different_layout.error(), kwaque::errc::invalid_argument);
    const auto wrong_retry_context = assigned_batch_context::assign(
      after.batch.submitted(),
      after.batch.logical_span().begin(),
      *replacement_binding);
    ASSERT_FALSE(wrong_retry_context.has_value());
    EXPECT_EQ(wrong_retry_context.error(), kwaque::errc::invalid_argument);
    const auto original_retry_context = assigned_batch_context::assign(
      after.batch.submitted(), after.batch.logical_span().begin(), *binding);
    ASSERT_TRUE(original_retry_context.has_value());
    const auto restored_sparse = original_retry_context->with_retained_count(
      item_count{1});
    ASSERT_TRUE(restored_sparse.has_value());
    EXPECT_EQ(*restored_sparse, after.batch);
}

} // namespace
