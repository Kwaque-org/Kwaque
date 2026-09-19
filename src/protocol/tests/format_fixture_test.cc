#include "src/codec/tests/format_fixture.h"
#include "src/codec/tests/format_test_support.h"
#include "src/protocol/batch_frame_codec.h"
#include "src/protocol/control_frame_codec.h"
#include "src/protocol/tests/control_fuzz_oracle.h"
#include "src/protocol/tests/control_test_support.h"
#include "src/protocol/tests/frame_test_support.h"

#include <gtest/gtest.h>

#include <array>
#include <string>

namespace {
using namespace kwaque;
namespace fixtures = codec::testing::format_fixture;
namespace control = protocol::testing::control_fixture;
namespace frame = protocol::testing::frame_fixture;
using bytes::fragmented_buffer_parser;

template<typename T>
void check_header(const T& value, protocol::frame_kind kind) {
    EXPECT_EQ(value.header.metadata.kind, kind);
    EXPECT_EQ(value.header.header_bytes.value(), 48U);
    EXPECT_EQ(value.header.metadata.correlation.value(), 0x0102030405060708ULL);
    EXPECT_EQ(value.header.metadata.sequence.value(), 0x1112131415161718ULL);
    EXPECT_EQ(
      value.header.metadata.stream.value(),
      static_cast<unsigned>(kind) < 5 ? 0U : 7U);
}

TEST(
  ProtocolFormatFixtureTest,
  FourControlsPreserveValuesPresenceAndFrameBoundaries) {
    const std::array names{"request", "response", "redirect", "error"};
    for (std::size_t i = 0; i < names.size(); ++i) {
        const auto kind = static_cast<protocol::frame_kind>(i + 1);
        const auto payload = fixtures::read(std::string{"control_"} + names[i]);
        auto expected = protocol::testing::probe_control(payload, kind);
        ASSERT_NE(expected, nullptr);
        auto value = control::read(payload, kind);
        EXPECT_TRUE(
          protocol::testing::matches_control(*expected, value.data()));
        const auto wire = fixtures::read(std::string{"frame_"} + names[i]);
        for (const std::size_t width : {1U, 7U, 67U}) {
            fragmented_buffer_parser input{
              control::fragmented(wire + wire, width)};
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            auto decoded = protocol::decode_control_frame(
                             input,
                             kind,
                             {},
                             frame::bounds,
                             control::reserve(input, work),
                             work)
                             .get();
            ASSERT_TRUE(decoded.has_value());
            auto* result = std::get_if<protocol::decoded_control_frame>(
              &*decoded);
            ASSERT_NE(result, nullptr);
            check_header(*result, kind);
            EXPECT_EQ(result->header.payload_bytes.value(), payload.size());
            EXPECT_EQ(input.bytes_consumed().value(), wire.size());
            EXPECT_EQ(input.bytes_remaining().value(), wire.size());
            EXPECT_TRUE(
              protocol::testing::matches_control(
                *expected, result->value.data()));
            auto encoded = protocol::encode_control(
                             result->value,
                             work,
                             {},
                             result->remaining.operation_remaining,
                             control::charge)
                             .get();
            ASSERT_TRUE(encoded.has_value());
            auto normalized = protocol::testing::probe_control(
              control::flatten(*encoded), kind);
            ASSERT_NE(normalized, nullptr);
            EXPECT_TRUE(
              protocol::testing::matches_control(
                *normalized, result->value.data()));
            EXPECT_TRUE(protocol::testing::control_unknowns_empty(*normalized));
        }
    }
}

TEST(
  ProtocolFormatFixtureTest,
  RawKindsUseBatchDecodersAndIndependentTopicContext) {
    std::array<std::uint8_t, 16> topic{}, range{};
    topic.fill(0x10);
    range.fill(0x20);
    const model::batch_decode_expectation expected{
      model::topic_id::make(topic).value(),
      model::range_id::make(range).value()};
    for (const bool assigned : {false, true}) {
        const auto wire = fixtures::read(
          assigned ? "frame_assigned" : "frame_submitted");
        for (const std::size_t width : {1U, 7U, 67U}) {
            fragmented_buffer_parser input{control::fragmented(wire, width)};
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            if (assigned) {
                auto decoded = protocol::decode_assigned_frame(
                                 input,
                                 expected,
                                 frame::bounds,
                                 control::reserve(input, work),
                                 work)
                                 .get();
                ASSERT_TRUE(decoded.has_value());
                auto* value = std::get_if<protocol::decoded_assigned_frame>(
                  &*decoded);
                ASSERT_NE(value, nullptr);
                check_header(*value, protocol::frame_kind::assigned_batch);
                EXPECT_EQ(
                  value->batch.fingerprint().bytes(),
                  fixtures::digest("batch_digest"));
                EXPECT_EQ(
                  value->batch.context().logical_span().begin().value(), 100U);
                EXPECT_TRUE(value->batch.records().content_equals(
                  fixtures::read("record")));
            } else {
                auto decoded = protocol::decode_submitted_frame(
                                 input,
                                 expected,
                                 frame::bounds,
                                 control::reserve(input, work),
                                 work)
                                 .get();
                ASSERT_TRUE(decoded.has_value());
                auto* value = std::get_if<protocol::decoded_submitted_frame>(
                  &*decoded);
                ASSERT_NE(value, nullptr);
                check_header(*value, protocol::frame_kind::submitted_batch);
                EXPECT_EQ(
                  value->batch.fingerprint().bytes(),
                  fixtures::digest("batch_digest"));
                EXPECT_EQ(value->batch.context().original_count().value(), 1U);
                EXPECT_TRUE(value->batch.records().content_equals(
                  fixtures::read("record")));
            }
            EXPECT_TRUE(input.at_end());
        }
    }
}

TEST(
  ProtocolFormatFixtureTest, RepairedFrameAndBatchRetainInnerErrorCoordinates) {
    namespace envelope = codec::testing::envelope_fixture;
    const auto original = fixtures::read("frame_assigned");
    const auto child = fixtures::read("assigned_dense");
    std::array<std::uint8_t, 16> topic{}, range{};
    topic.fill(0x10);
    range.fill(0x20);
    const model::batch_decode_expectation expected{
      model::topic_id::make(topic).value(),
      model::range_id::make(range).value(),
      {},
      {},
      codec::semantic_batch_digest{fixtures::digest("batch_digest")}};
    for (const auto& mutation : fixtures::record_mutations) {
        for (const std::size_t width : {7U, 67U}) {
            SCOPED_TRACE(
              ::testing::Message{} << mutation.offset << " width=" << width);
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            auto batch = child;
            batch.at(216 + mutation.offset) = mutation.replacement;
            envelope::put_u32(
              batch, 24, envelope::crc32c(std::string_view{batch}.substr(32)));
            envelope::repair_header_crc(batch);
            auto wire = original;
            wire.replace(48, child.size(), batch);
            frame::put_u32(wire, 44, frame::crc32c(batch));
            frame::repair(wire);
            fragmented_buffer_parser input{
              control::fragmented("prefix" + wire + original, width)};
            input.skip(byte_count{6}).value();
            input.push_checkpoint().value();
            const auto decoded = protocol::decode_assigned_frame(
                                   input,
                                   expected,
                                   frame::bounds,
                                   control::reserve(input, work),
                                   work,
                                   frame::context)
                                   .get();
            fixtures::expect_rejection(
              decoded,
              codec::error{
                mutation.code,
                2,
                mutation.field,
                77 + 48 + 216 + mutation.error_offset},
              input,
              byte_count{6},
              1);
        }
    }
}
} // namespace
