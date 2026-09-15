#include "src/model/batch_rewrite.h"
#include "src/model/record_scan.h"
#include "src/storage/tests/segment_test_support.h"

#include <gtest/gtest.h>

namespace kwaque::storage {
namespace {
using namespace testing;
using bytes::fragmented_buffer_parser;

TEST(SegmentRelocationTest, SparseAndDenseRelocationPreserveOriginalIdentity) {
    for (const bool sparse : {false, true}) {
        for (const bool compressed : {false, true}) {
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            auto child = checked_child(work, compressed, 40, sparse);
            const auto bytes = flat(child.bytes());
            const auto original = child.info();
            const auto relocated = block_expected(0x80, 2, 4096, 17);
            const auto initial = validate_initial_append(
              child, relocated.location);
            ASSERT_FALSE(initial.has_value());
            auto encoded = encode_segment_block(
                             std::move(child),
                             relocated,
                             work,
                             budget().operation_remaining,
                             charge)
                             .get();
            ASSERT_TRUE(encoded.has_value());
            EXPECT_EQ(flat(encoded->bytes()), block_wire(bytes, relocated));
            const auto descriptor = encoded->descriptor();
            EXPECT_EQ(
              descriptor.context().segment(), id<model::segment_id>(0x80));
            EXPECT_EQ(descriptor.context().generation().value(), 2U);
            EXPECT_EQ(descriptor.coverage().physical().begin().value(), 17U);
            EXPECT_EQ(
              descriptor.coverage().physical().count().value(),
              sparse ? 2U : 1U);
            EXPECT_EQ(descriptor.coverage().bytes().begin().value(), 4096U);
            EXPECT_EQ(descriptor.batch(), original);
            fragmented_buffer_parser input{std::move(*encoded).release_bytes()};
            auto decoded = decode_segment_block(
                             input, relocated, reserve(input, work), work)
                             .get();
            ASSERT_TRUE(decoded.has_value());
            EXPECT_TRUE(input.at_end());
            EXPECT_EQ(decoded->value.descriptor(), descriptor);
            EXPECT_EQ(
              decoded->value.descriptor()
                .batch()
                .context.submitted()
                .binding()
                .segment(),
              id<model::segment_id>(0x30));
            EXPECT_EQ(
              decoded->value.descriptor()
                .batch()
                .context.submitted()
                .binding()
                .generation()
                .value(),
              1U);
            EXPECT_EQ(
              flat(decoded->value.bytes()).substr(32 + 120, bytes.size()),
              bytes);
        }
    }
}

TEST(
  SegmentRelocationTest,
  SparseRecordDeltasAndTimestampsKeepTheirOriginalSlots) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto fixed = assigned_wire(false, 32, true).substr(32, 184);
    const auto original_records = hex(
      "0600000001000006000101010000060014020100000600030301000006000204010000");
    put(fixed, 148, 5, 4);
    put(fixed, 160, original_records.size(), 4);
    put(fixed, 164, original_records.size(), 4);
    fragmented_buffer_parser input{buffer(frame(fixed + original_records, 2))};
    auto decoded = model::decode_assigned_batch(
                     input, batch_expected(), reserve(input, work), work)
                     .get()
                     .value();
    const auto context = decoded.value.context();
    const auto fingerprint = decoded.value.fingerprint();
    const std::array selected{
      model::range_logical_count{1}, model::range_logical_count{3}};
    auto sparse = model::rewrite_assigned_batch(
                    std::move(decoded.value), selected, budget(), work)
                    .get()
                    .value();
    EXPECT_EQ(context.logical_span().begin().value(), 100U);
    EXPECT_EQ(context.logical_span().end().value(), 105U);
    EXPECT_EQ(flat(sparse.records()), hex("0600010101000006000303010000"));
    EXPECT_EQ(
      context.submitted().original_timestamp_base().unix_nanoseconds(), 0);
    auto current = block_expected(0x80, 2, 8192, 23);
    auto scan = model::record_region_scanner::make(
                  buffer(flat(sparse.records())),
                  {context.submitted().original_timestamp_base(),
                   context.submitted().original_count(),
                   item_count{2},
                   {},
                   model::record_region_kind::sparse},
                  budget(),
                  work)
                  .get()
                  .value();
    std::array<model::record_id, 2> record_ids{
      model::record_id::make(
        batch_expected().topic,
        batch_expected().range,
        model::range_logical_offset::make(101).value())
        .value(),
      model::record_id::make(
        batch_expected().topic,
        batch_expected().range,
        model::range_logical_offset::make(103).value())
        .value()};
    for (std::size_t i = 0; i < selected.size(); ++i) {
        ASSERT_TRUE(scan.next(work).get().value());
        ASSERT_NE(scan.current(), nullptr);
        EXPECT_EQ(scan.current()->fields.logical_delta, selected[i]);
        EXPECT_EQ(scan.current()->fields.timestamp_delta, i == 0 ? -1 : -2);
        const auto offset = model::range_logical_offset::make(
                              context.logical_span().begin().value()
                              + scan.current()->fields.logical_delta.value())
                              .value();
        EXPECT_EQ(
          model::record_id::make(
            context.submitted().binding().topic(),
            context.submitted().binding().range(),
            offset)
            .value(),
          record_ids[i]);
    }
    EXPECT_TRUE(scan.complete());
    scan.close(work).get();
    auto child = make_encoded_assigned_batch(
                   std::move(sparse),
                   compression::codec_id::lz4,
                   work,
                   budget().operation_remaining,
                   charge)
                   .get()
                   .value();
    const auto original = child.info();
    EXPECT_EQ(original.fingerprint, fingerprint);
    auto block
      = encode_segment_block(
          std::move(child), current, work, budget().operation_remaining, charge)
          .get()
          .value();
    EXPECT_EQ(block.descriptor().batch(), original);
    EXPECT_EQ(block.descriptor().coverage().logical(), context.logical_span());
    EXPECT_EQ(block.descriptor().coverage().physical().count().value(), 2U);
    const auto relocated = block.descriptor().batch().context;
    for (std::size_t i = 0; i < selected.size(); ++i) {
        const auto offset = model::range_logical_offset::make(
                              relocated.logical_span().begin().value()
                              + selected[i].value())
                              .value();
        EXPECT_EQ(
          model::record_id::make(
            relocated.submitted().binding().topic(),
            relocated.submitted().binding().range(),
            offset)
            .value(),
          record_ids[i]);
    }
}

TEST(
  SegmentRelocationTest,
  InitialAppendRejectsSparseDataEvenAtOriginalPlacement) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto child = checked_child(work, false, 32, true);
    const auto checked = validate_initial_append(
      child, block_expected().location);
    ASSERT_FALSE(checked.has_value());
    EXPECT_EQ(checked.error(), errc::invalid_argument);
    auto encoded = encode_segment_block(
                     std::move(child),
                     block_expected(),
                     work,
                     budget().operation_remaining,
                     charge)
                     .get();
    ASSERT_TRUE(encoded.has_value());
    EXPECT_EQ(encoded->descriptor().coverage().logical().count().value(), 5U);
    EXPECT_EQ(encoded->descriptor().coverage().physical().count().value(), 2U);
}

TEST(SegmentRelocationTest, RemovingAllSurvivorsCannotCreateAnEmptyDataBlock) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto wire = assigned_wire(false, 32, true);
    fragmented_buffer_parser original{buffer(wire)};
    auto decoded = model::decode_assigned_batch(
                     original, batch_expected(), reserve(original, work), work)
                     .get()
                     .value();
    const auto context = decoded.value.context();
    const auto digest = decoded.value.fingerprint();
    const auto removed
      = model::remove_all_records(std::move(decoded.value), work).get().value();
    EXPECT_EQ(removed.submitted(), context.submitted());
    EXPECT_EQ(removed.logical_span(), context.logical_span());
    EXPECT_EQ(removed.fingerprint(), digest);
    put(wire, 32 + 148, 0, 4);
    put(wire, 32 + 160, 0, 4);
    put(wire, 32 + 164, 0, 4);
    wire.resize(32 + 184);
    put(wire, 12, 184, 4);
    repair(wire);
    auto input = buffer(wire);
    const auto memory = reserve(input, work);
    const auto result = validate_encoded_assigned_batch(
                          std::move(input), batch_expected(), memory, work)
                          .get();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), errc::malformed_data);
}
} // namespace
} // namespace kwaque::storage
