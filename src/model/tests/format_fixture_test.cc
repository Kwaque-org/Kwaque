#include "src/bytes/test_allocation_profile.h"
#include "src/codec/sha256.h"
#include "src/codec/tests/envelope_decode_test_support.h"
#include "src/codec/tests/format_fixture.h"
#include "src/codec/tests/format_test_support.h"
#include "src/model/batch_codec.h"
#include "src/model/checkpoint_codec.h"
#include "src/model/record_codec.h"

#include <seastar/core/abort_source.hh>

#include <gtest/gtest.h>

#include <array>
#include <string>
#include <string_view>
#include <utility>

namespace {
using namespace kwaque;
namespace fixture = codec::testing::format_fixture;
using bytes::fragmented_buffer_parser;
using codec::testing::envelope_fixture::fragmented;

codec::immutable_object_digest
exact_digest(const bytes::fragmented_buffer& value) {
    codec::sha256_hasher hash;
    for (const auto fragment : value)
        hash.update(fragment.data(), fragment.size());
    return codec::immutable_object_digest{std::move(hash).final()};
}

template<typename Id>
Id id(std::uint8_t byte) {
    std::array<std::uint8_t, 16> raw{};
    raw.fill(byte);
    return Id::make(raw).value();
}
auto reserve(
  const fragmented_buffer_parser& input, codec::cooperative_work& work) {
    // The unclaimed operation half covers bounded fixtures and native frames.
    return codec::reserve_decode_input(
             input,
             work.policy(),
             {byte_count{32U << 20U},
              byte_count{1U << 20U},
              bytes::testing::charge})
      .value();
}

TEST(ModelFormatFixtureTest, RawRecordsPreserveNullableAndOrderedHeaderFields) {
    for (const bool rich : {false, true}) {
        const auto wire = fixture::read(rich ? "record_rich" : "record");
        for (const std::size_t width : {1U, 7U, 67U}) {
            fragmented_buffer_parser input{fragmented(wire, width)};
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            auto decoded = model::decode_record(
                             input,
                             {{}, model::range_logical_count{1}, item_count{2}},
                             reserve(input, work),
                             work,
                             {},
                             codec::input_boundary::complete)
                             .get();
            ASSERT_TRUE(decoded.has_value());
            EXPECT_TRUE(input.at_end());
            EXPECT_EQ(decoded->value.fields(), model::record_fields{});
            ASSERT_EQ(decoded->value.has_key(), rich);
            ASSERT_EQ(decoded->value.has_value(), !rich);
            ASSERT_EQ(decoded->value.headers().size(), rich ? 2U : 0U);
            if (rich) {
                EXPECT_TRUE(decoded->value.key()->content_equals("k"));
                EXPECT_TRUE(decoded->value.headers()[0].name().empty());
                EXPECT_FALSE(decoded->value.headers()[0].value());
                EXPECT_TRUE(decoded->value.headers()[1].name().content_equals(
                  std::string(1, '\xff')));
                ASSERT_TRUE(decoded->value.headers()[1].value());
                EXPECT_TRUE(decoded->value.headers()[1].value()->empty());
            } else {
                EXPECT_TRUE(decoded->value.value()->empty());
            }
            auto encoded = model::encode_record(
                             decoded->value,
                             work,
                             decoded->remaining.operation_remaining,
                             bytes::testing::charge)
                             .get();
            ASSERT_TRUE(encoded.has_value());
            EXPECT_TRUE(encoded->content_equals(wire));
        }
    }
}

TEST(
  ModelFormatFixtureTest, BatchArtifactsPinOriginalIdentityAndSparseCoverage) {
    const model::batch_decode_expectation expected{
      id<model::topic_id>(0x10), id<model::range_id>(0x20)};
    for (const std::string name :
         {"submitted", "assigned_dense", "assigned_sparse"}) {
        const auto wire = fixture::read(name);
        for (const std::size_t width : {1U, 7U, 67U}) {
            fragmented_buffer_parser input{fragmented(wire, width)};
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            const auto check =
              [&](const model::submitted_batch_context& original) {
                  EXPECT_EQ(
                    original.id().producer(), id<model::producer_id>(0x40));
                  EXPECT_EQ(original.id().epoch().value(), 1U);
                  EXPECT_EQ(original.id().stream().value(), 1U);
                  EXPECT_EQ(original.id().sequence().value(), 0U);
                  EXPECT_EQ(original.binding().topic(), expected.topic);
                  EXPECT_EQ(original.binding().range(), expected.range);
                  EXPECT_EQ(
                    original.binding().segment(), id<model::segment_id>(0x30));
                  EXPECT_EQ(original.binding().generation().value(), 1U);
                  EXPECT_EQ(original.binding().routing_epoch().value(), 1U);
                  EXPECT_EQ(
                    original.original_timestamp_base().unix_nanoseconds(), 0);
                  EXPECT_EQ(
                    original.original_count().value(),
                    name == "assigned_sparse" ? 5U : 1U);
              };
            if (name == "submitted") {
                auto decoded = model::decode_submitted_batch(
                                 input, expected, reserve(input, work), work)
                                 .get();
                ASSERT_TRUE(decoded.has_value());
                check(decoded->value.context());
                EXPECT_EQ(
                  decoded->value.fingerprint().bytes(),
                  fixture::digest("batch_digest"));
                EXPECT_TRUE(decoded->value.records().content_equals(
                  fixture::read("record")));
            } else {
                auto decoded = model::decode_assigned_batch(
                                 input, expected, reserve(input, work), work)
                                 .get();
                ASSERT_TRUE(decoded.has_value());
                check(decoded->value.context().submitted());
                EXPECT_EQ(
                  decoded->value.context().logical_span().begin().value(),
                  100U);
                EXPECT_EQ(
                  decoded->value.context().logical_span().end().value(),
                  name == "assigned_sparse" ? 105U : 101U);
                EXPECT_EQ(
                  decoded->value.context().retained_count().value(),
                  name == "assigned_sparse" ? 2U : 1U);
                EXPECT_EQ(
                  decoded->fingerprint_verification,
                  name == "assigned_sparse"
                    ? model::batch_fingerprint_verification::carried
                    : model::batch_fingerprint_verification::recomputed);
                if (name == "assigned_dense")
                    EXPECT_EQ(
                      decoded->value.fingerprint().bytes(),
                      fixture::digest("batch_digest"));
                EXPECT_TRUE(decoded->value.records().content_equals(
                  std::string_view{wire}.substr(216)));
            }
            EXPECT_TRUE(input.at_end());
        }
    }
}

TEST(
  ModelFormatFixtureTest, CheckpointExtensionsDoNotChangeSemanticProjection) {
    std::array<std::uint8_t, 16> topic{};
    for (std::size_t i = 0; i < topic.size(); ++i)
        topic[i] = static_cast<std::uint8_t>(i + 1);
    const auto plain = fragmented(fixture::read("checkpoint"), 67);
    const auto plain_digest = exact_digest(plain);
    for (const auto* name : {"checkpoint", "checkpoint_extended"}) {
        const auto wire = fixture::read(name);
        for (const std::size_t width : {1U, 7U, 67U}) {
            auto wire_buffer = fragmented(wire, width);
            if (std::string_view{name} == "checkpoint")
                EXPECT_EQ(exact_digest(wire_buffer), plain_digest);
            else
                EXPECT_NE(exact_digest(wire_buffer), plain_digest);
            fragmented_buffer_parser input{std::move(wire_buffer)};
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            auto decoded = model::decode_read_checkpoint(
                             input,
                             model::topic_id::make(topic).value(),
                             reserve(input, work),
                             work)
                             .get();
            ASSERT_TRUE(decoded.has_value());
            EXPECT_TRUE(input.at_end());
            EXPECT_EQ(
              decoded->fingerprint.bytes(),
              fixture::digest("checkpoint_digest"));
            ASSERT_EQ(decoded->value.cursors().size(), 2U);
            for (std::size_t i = 0; i < 2; ++i) {
                std::array<std::uint8_t, 16> range{};
                range.back() = static_cast<std::uint8_t>(i + 1);
                EXPECT_EQ(
                  decoded->value.cursors()[i].range(),
                  model::range_id::make(range).value());
                EXPECT_EQ(decoded->value.cursors()[i].next().value(), i);
            }
            auto encoded = model::encode_read_checkpoint(
                             decoded->value,
                             work,
                             decoded->remaining.operation_remaining,
                             bytes::testing::charge)
                             .get();
            ASSERT_TRUE(encoded.has_value());
            EXPECT_TRUE(
              encoded->bytes.content_equals(fixture::read("checkpoint")));
            EXPECT_EQ(exact_digest(encoded->bytes), plain_digest);
        }
    }
}

TEST(
  ModelFormatFixtureTest, RepairedBatchIntegrityStillChecksExactRecordGrammar) {
    namespace envelope = codec::testing::envelope_fixture;
    const auto original = fixture::read("assigned_dense");
    const auto raw = fixture::read("record");
    const model::batch_decode_expectation expected{
      id<model::topic_id>(0x10),
      id<model::range_id>(0x20),
      {},
      {},
      codec::semantic_batch_digest{fixture::digest("batch_digest")}};
    constexpr codec::field_context context{
      .origin = 71, .family = 91, .field = 92};
    for (const auto& mutation : fixture::record_mutations) {
        for (const std::size_t width : {7U, 67U}) {
            SCOPED_TRACE(
              ::testing::Message{} << mutation.offset << " width=" << width);
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            auto record = raw;
            record.at(mutation.offset) = mutation.replacement;
            fragmented_buffer_parser record_input{
              fragmented("prefix" + record + raw, width)};
            record_input.skip(byte_count{6}).value();
            record_input.push_checkpoint().value();
            const auto decoded
              = model::decode_record(
                  record_input,
                  {{}, model::range_logical_count{1}, item_count{2}},
                  reserve(record_input, work),
                  work,
                  context,
                  codec::input_boundary::complete)
                  .get();
            fixture::expect_rejection(
              decoded,
              codec::error{
                mutation.code, 91, mutation.field, 77 + mutation.error_offset},
              record_input,
              byte_count{6},
              1);
            auto batch = original;
            batch.replace(216, raw.size(), record);
            envelope::put_u32(
              batch, 24, envelope::crc32c(std::string_view{batch}.substr(32)));
            envelope::repair_header_crc(batch);
            fragmented_buffer_parser input{
              fragmented("prefix" + batch + original, width)};
            input.skip(byte_count{6}).value();
            input.push_checkpoint().value();
            const auto result
              = model::decode_assigned_batch(
                  input, expected, reserve(input, work), work, context)
                  .get();
            fixture::expect_rejection(
              result,
              codec::error{
                mutation.code,
                2,
                mutation.field,
                77 + 216 + mutation.error_offset},
              input,
              byte_count{6},
              1);
        }
    }
}

TEST(
  ModelFormatFixtureTest,
  RecordShortageUsesTheCallerBoundaryAndAbsoluteOrigin) {
    const auto wire = fixture::read("record");
    constexpr codec::field_context context{
      .origin = 71, .family = 91, .field = 92};
    for (const auto boundary :
         {codec::input_boundary::open, codec::input_boundary::complete}) {
        for (std::size_t cut = 0; cut < wire.size(); ++cut) {
            SCOPED_TRACE(cut);
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            fragmented_buffer_parser input{
              fragmented("prefix" + wire.substr(0, cut), 1)};
            input.skip(byte_count{6}).value();
            input.push_checkpoint().value();
            const auto decoded
              = model::decode_record(
                  input,
                  {{}, model::range_logical_count{1}, item_count{}},
                  reserve(input, work),
                  work,
                  context,
                  boundary)
                  .get();
            fixture::expect_rejection(
              decoded,
              codec::error{
                boundary == codec::input_boundary::open ? errc::truncated_data
                                                        : errc::malformed_data,
                91,
                64,
                77 + cut},
              input,
              byte_count{6},
              1);
        }
    }
}
} // namespace
