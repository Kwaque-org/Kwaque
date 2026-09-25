#include "src/storage/tests/segment_test_support.h"

#include <seastar/core/preempt.hh>
#include <seastar/util/alloc_failure_injector.hh>

#include <gtest/gtest.h>

#include <chrono>
#include <concepts>
#include <optional>
#include <type_traits>

namespace kwaque::storage {
namespace {
using namespace testing;
using bytes::fragmented_buffer;
using bytes::fragmented_buffer_parser;
static_assert(!std::default_initializable<encoded_assigned_batch>);
static_assert(!std::is_aggregate_v<encoded_assigned_batch>);
static_assert(!std::is_copy_constructible_v<encoded_assigned_batch>);
static_assert(!std::default_initializable<complete_block_descriptor>);
static_assert(!std::is_aggregate_v<complete_block_descriptor>);
static_assert(!std::is_copy_constructible_v<segment_block>);
static_assert(!std::is_aggregate_v<segment_block>);
static_assert(std::is_nothrow_move_constructible_v<segment_block>);

// Bounded slow oracle for the accelerated fixture checksum. Large fixtures
// use the library implementation; this deliberately visits only short inputs.
std::uint32_t bitwise_crc(std::string_view bytes) {
    std::uint32_t value = 0xffffffffU;
    for (char byte : bytes) {
        value ^= static_cast<unsigned char>(byte);
        for (unsigned bit = 0; bit < 8; ++bit)
            value = (value >> 1U) ^ ((value & 1U) ? 0x82f63b78U : 0U);
    }
    return value ^ 0xffffffffU;
}

TEST(SegmentFormatTest, FixtureChecksumMatchesBoundedBitwiseOracle) {
    std::array<char, 257> bytes{};
    for (std::size_t i = 0; i < bytes.size(); ++i)
        bytes[i] = std::bit_cast<char>(static_cast<std::uint8_t>(i));
    for (std::size_t size = 0; size <= 256; ++size) {
        const std::string_view input{bytes.data() + 1, size};
        EXPECT_EQ(crc(input), bitwise_crc(input)) << size;
        seastar::thread::maybe_yield();
    }
}

TEST(SegmentFormatTest, IndependentCrcAndInterconnectedGoldenBytes) {
    EXPECT_EQ(crc(""), 0U);
    EXPECT_EQ(crc("123456789"), 0xe3069283U);
    EXPECT_EQ(crc(std::string(32, '\0')), 0x8a9136aaU);
    EXPECT_EQ(crc(std::string(32, '\xff')), 0x62a8ab43U);
    EXPECT_EQ(
      assigned_wire().substr(0, 32),
      hex("4b5142460200010001002000bf0000000000000000000000e0c8efd69dcfba5b"));
    EXPECT_EQ(
      header_wire().substr(0, 32),
      hex("4b5142460300010001002000e001000000000000000000003d7d385d496f3b42"));
    EXPECT_EQ(
      block_wire().substr(0, 32),
      hex("4b5142460400010001002000e001000000000000000000007bfe4b76db35d52f"));
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto encoded_header
      = encode_segment_header(
          header(), work, budget().operation_remaining, charge)
          .get();
    ASSERT_TRUE(encoded_header.has_value());
    EXPECT_EQ(flat(*encoded_header), header_wire());
    auto child = checked_child(work);
    EXPECT_TRUE(validate_initial_append(child, block_expected().location));
    auto encoded_block = encode_segment_block(
                           std::move(child),
                           block_expected(),
                           work,
                           budget().operation_remaining,
                           charge)
                           .get();
    ASSERT_TRUE(encoded_block.has_value());
    EXPECT_EQ(flat(encoded_block->bytes()), block_wire());
    const auto coverage = encoded_block->descriptor().coverage();
    EXPECT_EQ(coverage.bytes().begin().value(), 512U);
    EXPECT_EQ(coverage.bytes().end().value(), 1024U);
    EXPECT_EQ(coverage.physical().begin().value(), 0U);
    EXPECT_EQ(coverage.physical().end().value(), 1U);
    EXPECT_EQ(coverage.logical().begin().value(), 100U);
    EXPECT_EQ(coverage.logical().end().value(), 101U);
}

TEST(SegmentFormatTest, HeaderAllFieldsAlignmentExtensionsAndDataStart) {
    const auto context
      = segment_context::make(
          id<model::cluster_id>(0x61),
          id<model::topic_id>(0x72),
          id<model::range_id>(0x83),
          id<model::segment_id>(0x94),
          model::segment_generation::make(0x1020304050607080).value())
          .value();
    for (std::uint64_t a = 512; a <= 65536; a *= 2U) {
        const auto expected = segment_header::make(
                                context,
                                model::range_logical_end{0x1122334455667788},
                                alignment(a))
                                .value();
        for (const std::size_t h : {32U, 40U, 4096U}) {
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            const auto literal = header_wire(expected, h);
            fragmented_buffer_parser input{buffer("p" + literal + "next", 67)};
            input.skip(byte_count{1}).value();
            input.push_checkpoint().value();
            const auto decoded
              = decode_segment_header(
                  input, expected, {}, reserve(input, work), work)
                  .get();
            ASSERT_TRUE(decoded.has_value());
            EXPECT_EQ(decoded->value.context().cluster(), context.cluster());
            EXPECT_EQ(decoded->value.context().topic(), context.topic());
            EXPECT_EQ(decoded->value.context().range(), context.range());
            EXPECT_EQ(decoded->value.context().segment(), context.segment());
            EXPECT_EQ(
              decoded->value.context().generation().value(),
              0x1020304050607080U);
            EXPECT_EQ(
              decoded->value.logical_origin().value(), 0x1122334455667788U);
            EXPECT_EQ(decoded->value.profile(), storage_profile::v1);
            EXPECT_EQ(decoded->value.alignment().bytes().value(), a);
            EXPECT_EQ(decoded->bytes.begin().value(), 0U);
            EXPECT_EQ(decoded->bytes.end().value(), literal.size());
            EXPECT_EQ(input.bytes_consumed().value(), 1U + literal.size());
            EXPECT_EQ(input.checkpoint_depth(), 1U);
            if (h == 32) {
                const auto encoded
                  = encode_segment_header(
                      expected, work, budget().operation_remaining, charge)
                      .get();
                ASSERT_TRUE(encoded.has_value());
                EXPECT_EQ(flat(*encoded), literal);
            }
        }
    }
}

TEST(SegmentFormatTest, HeaderInvalidFieldsAndExpectedIdentityAfterIntegrity) {
    struct mutation {
        std::size_t offset, width;
        std::uint64_t value;
        errc error;
    };
    constexpr mutation cases[]{
      mutation{0, 1, 0x51, errc::wrong_context},
      {16, 1, 0x11, errc::wrong_context},
      {32, 1, 0x21, errc::wrong_context},
      {48, 1, 0x31, errc::wrong_context},
      {64, 8, 2, errc::wrong_context},
      {64, 8, 0, errc::malformed_data},
      {72, 8, 101, errc::wrong_context},
      {80, 2, 0, errc::malformed_data},
      {80, 2, 2, errc::unsupported_format},
      {82, 2, 0, errc::malformed_data},
      {82, 2, 2, errc::unsupported_format},
      {84, 4, 513, errc::malformed_data},
      {84, 4, 1024, errc::wrong_context},
      {88, 4, 383, errc::malformed_data},
      {92, 4, 1, errc::malformed_data},
      {96, 1, 1, errc::malformed_data}};
    for (const auto& change : cases) {
        SCOPED_TRACE(change.offset);
        auto wire = header_wire();
        put(wire, 32 + change.offset, change.value, change.width);
        for (const bool repair_checksum : {false, true}) {
            if (repair_checksum) repair(wire);
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            fragmented_buffer_parser input{buffer(wire, 7)};
            input.push_checkpoint().value();
            const auto decoded
              = decode_segment_header(
                  input, header(), {}, reserve(input, work), work)
                  .get();
            ASSERT_FALSE(decoded.has_value());
            EXPECT_EQ(
              decoded.error().code(),
              repair_checksum ? change.error : errc::corrupt_data);
            EXPECT_EQ(input.bytes_consumed(), byte_count{});
            EXPECT_EQ(input.checkpoint_depth(), 1U);
        }
    }
    for (const std::size_t offset : {0U, 16U, 32U, 48U}) {
        auto wire = header_wire();
        wire.replace(32 + offset, 16, 16, '\0');
        repair(wire);
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        fragmented_buffer_parser input{buffer(wire)};
        const auto result = decode_segment_header(
                              input, header(), {}, reserve(input, work), work)
                              .get();
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().code(), errc::malformed_data);
    }
}

TEST(SegmentFormatTest, HeaderEveryTruncationAndFilePosition) {
    const auto wire = header_wire();
    for (std::size_t size = 0; size < wire.size(); ++size) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        fragmented_buffer_parser input{
          buffer(std::string_view{wire}.substr(0, size))};
        auto memory = reserve(input, work);
        auto result
          = decode_segment_header(input, header(), {}, memory, work).get();
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().code(), errc::truncated_data);
        result = decode_segment_header(
                   input,
                   header(),
                   {},
                   memory,
                   work,
                   {},
                   codec::input_boundary::complete)
                   .get();
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().code(), errc::malformed_data);
        EXPECT_EQ(input.bytes_consumed(), byte_count{});
        seastar::thread::maybe_yield();
    }
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    fragmented_buffer_parser input{buffer(wire)};
    const auto result = decode_segment_header(
                          input,
                          header(),
                          runtime::file_position{512},
                          reserve(input, work),
                          work)
                          .get();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), errc::wrong_context);
}

TEST(SegmentFormatTest, ExactChildBytesAndOuterExtensionsSurviveAllOwners) {
    for (const bool compressed : {false, true}) {
        for (const std::size_t inner_header : {32U, 40U, 4096U}) {
            const auto child_wire = assigned_wire(compressed, inner_header);
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            auto child = checked_child(work, compressed, inner_header);
            auto encoded = encode_segment_block(
                             std::move(child),
                             block_expected(),
                             work,
                             budget().operation_remaining,
                             charge)
                             .get();
            ASSERT_TRUE(encoded.has_value());
            EXPECT_EQ(flat(encoded->bytes()), block_wire(child_wire));
            for (const std::size_t outer_header : {32U, 40U, 4096U}) {
                const auto literal = block_wire(
                  child_wire, block_expected(), outer_header);
                const codec::field_context context{
                  .origin = UINT64_MAX - literal.size() - 2U};
                auto result = [&] {
                    fragmented_buffer_parser input{
                      buffer("p" + literal + "s", 67)};
                    input.skip(byte_count{1}).value();
                    input.push_checkpoint().value();
                    const auto memory = reserve(input, work, context);
                    auto decoded
                      = decode_segment_block(
                          input, block_expected(), memory, work, context)
                          .get()
                          .value();
                    EXPECT_EQ(
                      input.bytes_consumed().value(), literal.size() + 1U);
                    EXPECT_EQ(input.checkpoint_depth(), 1U);
                    const auto cost
                      = decoded.value.bytes().allocation_cost(charge).value();
                    EXPECT_EQ(
                      memory.operation_remaining.value()
                        - decoded.remaining.operation_remaining.value(),
                      cost.descriptors.value());
                    EXPECT_EQ(
                      memory.metadata_remaining.value()
                        - decoded.remaining.metadata_remaining.value(),
                      cost.descriptors.value());
                    return decoded;
                }();
                const auto complete = flat(result.value.bytes());
                EXPECT_EQ(complete, literal);
                EXPECT_EQ(
                  complete.substr(outer_header + 120, child_wire.size()),
                  child_wire);
                EXPECT_EQ(
                  result.value.descriptor()
                    .batch()
                    .context.logical_span()
                    .begin()
                    .value(),
                  100U);
                EXPECT_EQ(
                  result.value.descriptor().batch().verification,
                  model::batch_fingerprint_verification::recomputed);
            }
        }
    }
}

TEST(SegmentFormatTest, BlockFixedFieldsAndInnerFailureRollback) {
    struct mutation {
        std::size_t offset, width;
        std::uint64_t value;
        errc error;
    };
    constexpr mutation cases[]{
      mutation{0, 1, 0x51, errc::wrong_context},
      {16, 1, 0x11, errc::wrong_context},
      {32, 1, 0x21, errc::wrong_context},
      {48, 1, 0x31, errc::wrong_context},
      {64, 8, 2, errc::wrong_context},
      {72, 8, 1024, errc::wrong_context},
      {80, 8, 1, errc::wrong_context},
      {88, 8, 2, errc::malformed_data},
      {96, 8, 99, errc::malformed_data},
      {104, 8, 102, errc::malformed_data},
      {112, 4, 0, errc::malformed_data},
      {112, 4, 222, errc::malformed_data},
      {112, 4, 0xffffffffU, errc::resource_exhausted},
      {116, 4, 136, errc::malformed_data},
      {120, 1, 0, errc::malformed_data},
      {343, 1, 1, errc::malformed_data}};
    for (const auto& change : cases) {
        SCOPED_TRACE(change.offset);
        auto wire = block_wire();
        put(wire, 32 + change.offset, change.value, change.width);
        repair(wire);
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        fragmented_buffer_parser input{buffer("p" + wire, 7)};
        input.skip(byte_count{1}).value();
        input.push_checkpoint().value();
        const auto result
          = decode_segment_block(
              input, block_expected(), reserve(input, work), work)
              .get();
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().code(), change.error);
        EXPECT_EQ(input.bytes_consumed(), byte_count{1});
        EXPECT_EQ(input.checkpoint_depth(), 1U);
    }
    auto child = assigned_wire();
    put(child, 32 + 148, 0, 4);
    repair(child);
    const auto wire = block_wire(child);
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    fragmented_buffer_parser input{buffer(wire)};
    const auto result = decode_segment_block(
                          input, block_expected(), reserve(input, work), work)
                          .get();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), errc::malformed_data);
    EXPECT_EQ(input.bytes_consumed(), byte_count{});
}

TEST(SegmentFormatTest, BlockDistinctMultibyteFieldsHaveIndependentOffsets) {
    constexpr std::uint64_t generation = 0x1122334455667788;
    constexpr std::uint64_t position = 0x0102030405060800;
    constexpr std::uint64_t physical = 0x2233445566778800;
    constexpr std::uint64_t logical = 0x3344556677889900;
    auto child_wire = assigned_wire(false, 4096, true);
    put(child_wire, 4096 + 168, logical, 8);
    put(child_wire, 4096 + 176, logical + 5U, 8);
    repair(child_wire);
    const auto expected = block_expected(0x80, generation, position, physical);
    const auto literal = block_wire(child_wire, expected, 40);
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    fragmented_buffer_parser input{buffer(literal)};
    auto decoded
      = decode_segment_block(input, expected, reserve(input, work), work).get();
    ASSERT_TRUE(decoded.has_value());
    const auto descriptor = decoded->value.descriptor();
    EXPECT_EQ(descriptor.context().cluster(), sc().cluster());
    EXPECT_EQ(descriptor.context().topic(), sc().topic());
    EXPECT_EQ(descriptor.context().range(), sc().range());
    EXPECT_EQ(descriptor.context().segment(), id<model::segment_id>(0x80));
    EXPECT_EQ(descriptor.context().generation().value(), generation);
    EXPECT_EQ(descriptor.coverage().bytes().begin().value(), position);
    EXPECT_EQ(
      descriptor.coverage().bytes().end().value(), position + literal.size());
    EXPECT_EQ(descriptor.coverage().physical().begin().value(), physical);
    EXPECT_EQ(descriptor.coverage().physical().end().value(), physical + 2U);
    EXPECT_EQ(descriptor.coverage().logical().begin().value(), logical);
    EXPECT_EQ(descriptor.coverage().logical().end().value(), logical + 5U);
    EXPECT_EQ(flat(decoded->value.bytes()), literal);
    auto child_bytes = buffer(child_wire);
    const auto memory = reserve(child_bytes, work);
    auto child = validate_encoded_assigned_batch(
                   std::move(child_bytes), batch_expected(), memory, work)
                   .get()
                   .value();
    auto encoded = encode_segment_block(
                     std::move(child),
                     expected,
                     work,
                     budget().operation_remaining,
                     charge)
                     .get();
    ASSERT_TRUE(encoded.has_value());
    EXPECT_EQ(flat(encoded->bytes()), block_wire(child_wire, expected));
}

TEST(
  SegmentFormatTest,
  MovedCurrentAndOriginalContextsStillNeedIndependentExpectation) {
    auto child = assigned_wire();
    child[32 + 40] = '\x11';
    repair(child);
    auto wire = block_wire(child);
    wire[32 + 16] = '\x11';
    repair(wire);
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    fragmented_buffer_parser input{buffer(wire)};
    const auto result = decode_segment_block(
                          input, block_expected(), reserve(input, work), work)
                          .get();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), errc::wrong_context);
    EXPECT_EQ(
      result.error().field(), static_cast<std::uint16_t>(segment_field::topic));
}

TEST(SegmentFormatTest, ExactFragmentLimitsAndPositionOverflow) {
    const auto separate
      = charge(byte_count{32}).checked_add(charge(byte_count{120})).value();
    const std::uint64_t fragments
      = assigned_wire().size() + (charge(byte_count{152}) <= separate ? 1U : 2U)
        + 1U;
    for (const std::uint64_t limit : {fragments - 1U, fragments}) {
        seastar::abort_source abort;
        codec::cooperative_work prepare{codec::limits::defaults(), abort};
        auto bytes = buffer(assigned_wire(), 1);
        auto child = validate_encoded_assigned_batch(
                       std::move(bytes), batch_expected(), budget(), prepare)
                       .get()
                       .value();
        auto config = codec::limits_config{};
        config.max_buffer_fragments = item_count{limit};
        codec::cooperative_work work{
          codec::limits::make(config).value(), abort};
        const auto result = encode_segment_block(
                              std::move(child),
                              block_expected(),
                              work,
                              budget().operation_remaining,
                              charge)
                              .get();
        if (limit != fragments) {
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(result.error().code(), errc::resource_exhausted);
        } else {
            ASSERT_TRUE(result.has_value());
            EXPECT_EQ(result->bytes().fragment_count(), fragments);
        }
    }
    for (const bool physical : {false, true}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto child = checked_child(work);
        const auto expected = physical
                                ? block_expected(0x30, 1, 512, UINT64_MAX)
                                : block_expected(0x30, 1, UINT64_MAX - 511U);
        const auto result = encode_segment_block(
                              std::move(child),
                              expected,
                              work,
                              budget().operation_remaining,
                              charge)
                              .get();
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().code(), errc::out_of_range);
    }
}

TEST(SegmentFormatTest, BlockTruncationsBudgetsAndCheckpointCapacity) {
    const auto wire = block_wire();
    for (const std::size_t length : {0U, 31U, 32U, 151U, 152U, 374U, 511U}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        fragmented_buffer_parser input{
          buffer(std::string_view{wire}.substr(0, length))};
        const auto result
          = decode_segment_block(
              input, block_expected(), reserve(input, work), work)
              .get();
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().code(), errc::truncated_data);
        EXPECT_EQ(input.bytes_consumed(), byte_count{});
        EXPECT_EQ(input.checkpoint_depth(), 0U);
    }
    for (int mode = 0; mode < 3; ++mode) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        fragmented_buffer_parser input{buffer(wire)};
        auto memory = reserve(input, work);
        if (mode == 0) memory.operation_remaining = {};
        if (mode == 1) memory.metadata_remaining = {};
        if (mode == 2)
            for (std::size_t i = 0; i < 7; ++i)
                input.push_checkpoint().value();
        const auto result
          = decode_segment_block(input, block_expected(), memory, work).get();
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().code(), errc::resource_exhausted);
        EXPECT_EQ(input.bytes_consumed(), byte_count{});
        EXPECT_EQ(input.checkpoint_depth(), mode == 2 ? 7U : 0U);
    }
}

TEST(SegmentFormatTest, AllocationFailureRestoresCursorAndMarks) {
#ifndef SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION
    GTEST_SKIP() << "allocation failure injection is unavailable";
#else
    for (const bool block : {false, true}) {
        bool completed = false;
        std::size_t failures = 0;
        const auto wire = block ? block_wire(assigned_wire(true))
                                : header_wire();
        for (std::uint64_t ordinal = 0; ordinal < 384 && !completed;
             ++ordinal) {
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            fragmented_buffer_parser input{buffer("p" + wire, 67)};
            input.skip(byte_count{1}).value();
            input.push_checkpoint().value();
            const auto memory = reserve(input, work);
            auto& injector = seastar::memory::local_failure_injector();
            bool threw = false, succeeded = false;
            injector.fail_after(ordinal);
            try {
                if (block)
                    succeeded = decode_segment_block(
                                  input, block_expected(), memory, work)
                                  .get()
                                  .has_value();
                else
                    succeeded = decode_segment_header(
                                  input, header(), {}, memory, work)
                                  .get()
                                  .has_value();
            } catch (const std::bad_alloc&) {
                threw = true;
            } catch (const std::runtime_error&) {
                if (!injector.failed()) {
                    injector.cancel();
                    throw;
                }
                threw = true;
            } catch (...) {
                injector.cancel();
                throw;
            }
            const bool reached = injector.failed();
            injector.cancel();
            if (threw) {
                EXPECT_TRUE(reached);
                EXPECT_FALSE(succeeded);
                EXPECT_EQ(input.bytes_consumed(), byte_count{1});
                ++failures;
            } else {
                ASSERT_TRUE(succeeded);
                EXPECT_EQ(input.bytes_consumed().value(), 1U + wire.size());
                completed = !reached;
            }
            EXPECT_EQ(input.checkpoint_depth(), 1U);
        }
        EXPECT_TRUE(completed);
        EXPECT_GT(failures, 0U);
    }
#endif
}

TEST(SegmentFormatTest, EncodingRechecksNarrowPolicyAndWireCoordinates) {
    seastar::abort_source abort;
    codec::cooperative_work prepare{codec::limits::defaults(), abort};
    auto child = checked_child(prepare, true, 40);
    auto config = codec::limits_config{};
    config.max_record_bytes = byte_count{6};
    codec::cooperative_work narrow{codec::limits::make(config).value(), abort};
    const auto rejected = encode_segment_block(
                            std::move(child),
                            block_expected(),
                            narrow,
                            budget().operation_remaining,
                            charge,
                            {.origin = 5000})
                            .get();
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error().code(), errc::resource_exhausted);
    EXPECT_GE(rejected.error().byte_offset(), 5000U);
    EXPECT_LT(rejected.error().byte_offset(), 5512U);
    const auto first = encode_segment_header(
                         header(),
                         prepare,
                         budget().operation_remaining,
                         charge,
                         {.origin = UINT64_MAX - 512U})
                         .get();
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(flat(*first), header_wire());
    const auto overflow = encode_segment_header(
                            header(),
                            prepare,
                            budget().operation_remaining,
                            charge,
                            {.origin = UINT64_MAX - 511U})
                            .get();
    ASSERT_FALSE(overflow.has_value());
    EXPECT_EQ(overflow.error().code(), errc::invalid_argument);
}

TEST(SegmentFormatTest, EncodingAllocationFailuresReleasePartialStaging) {
#ifndef SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION
    GTEST_SKIP() << "allocation failure injection is unavailable";
#else
    for (const bool block : {false, true}) {
        bool completed = false;
        std::size_t failures = 0;
        for (std::uint64_t ordinal = 0; ordinal < 384 && !completed;
             ++ordinal) {
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            std::optional<encoded_assigned_batch> child;
            if (block) child.emplace(checked_child(work, true, 40));
            auto& injector = seastar::memory::local_failure_injector();
            bool threw = false, succeeded = false;
            injector.fail_after(ordinal);
            try {
                if (block)
                    succeeded = encode_segment_block(
                                  std::move(*child),
                                  block_expected(),
                                  work,
                                  budget().operation_remaining,
                                  charge)
                                  .get()
                                  .has_value();
                else
                    succeeded
                      = encode_segment_header(
                          header(), work, budget().operation_remaining, charge)
                          .get()
                          .has_value();
            } catch (const std::bad_alloc&) {
                threw = true;
            } catch (...) {
                injector.cancel();
                throw;
            }
            const bool reached = injector.failed();
            injector.cancel();
            if (threw) {
                EXPECT_TRUE(reached);
                EXPECT_FALSE(succeeded);
                ++failures;
            } else {
                ASSERT_TRUE(succeeded);
                completed = !reached;
            }
        }
        EXPECT_TRUE(completed);
        EXPECT_GT(failures, 0U);
    }
#endif
}

TEST(
  SegmentFormatTest, CancellationDuringSuspensionRollsBackWithoutPublication) {
    for (const bool block : {false, true}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        fragmented_buffer_parser input{
          buffer(block ? block_wire() : header_wire(), 1)};
        input.push_checkpoint().value();
        const auto memory = reserve(input, work);
        const auto deadline = std::chrono::steady_clock::now()
                              + std::chrono::seconds{2};
        while (!seastar::need_preempt()
               && std::chrono::steady_clock::now() < deadline) {
        }
        ASSERT_TRUE(seastar::need_preempt());
        if (block) {
            auto pending = decode_segment_block(
              input, block_expected(), memory, work);
            const bool suspended = !pending.available();
            abort.request_abort();
            const auto result = pending.get();
            EXPECT_TRUE(suspended);
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(result.error().code(), errc::aborted);
        } else {
            auto pending = decode_segment_header(
              input, header(), {}, memory, work);
            const bool suspended = !pending.available();
            abort.request_abort();
            const auto result = pending.get();
            EXPECT_TRUE(suspended);
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(result.error().code(), errc::aborted);
        }
        EXPECT_EQ(input.bytes_consumed(), byte_count{});
        EXPECT_EQ(input.checkpoint_depth(), 1U);
    }
}
} // namespace
} // namespace kwaque::storage
