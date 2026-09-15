#include "src/storage/tests/wal_test_support.h"

#include <seastar/core/preempt.hh>
#include <seastar/util/alloc_failure_injector.hh>
#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

#include <chrono>
#include <concepts>
#include <exception>
#include <optional>
#include <type_traits>

namespace kwaque::storage {
namespace {
using namespace testing;
using bytes::fragmented_buffer;
using bytes::fragmented_buffer_parser;

static_assert(!std::default_initializable<wal_prepare>);
static_assert(!std::is_aggregate_v<wal_prepare>);
static_assert(!std::is_copy_constructible_v<wal_prepare>);
static_assert(std::is_nothrow_move_constructible_v<wal_prepare>);
static_assert(std::is_nothrow_destructible_v<wal_prepare>);
static_assert(!std::convertible_to<wal_write_context, segment_write_context>);

TEST(WalFormatTest, IndependentGoldenAndCompletePayload) {
    const auto literal = wal_wire();
    ASSERT_EQ(literal.size(), 512U);
    EXPECT_EQ(
      literal.substr(0, 32),
      hex("4b5142460500010001002000e001000000000000000000005f808b121ccb119f"));
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto child = checked_child(work);
    const auto info = child.info();
    auto encoded = encode_wal_prepare(
                     std::move(child),
                     wal_expected(),
                     work,
                     budget().operation_remaining,
                     charge)
                     .get();
    ASSERT_TRUE(encoded.has_value());
    EXPECT_EQ(flat(*encoded), literal);
    fragmented_buffer_parser input{std::move(*encoded)};
    auto decoded = decode_wal_prepare(
                     input, wal_expected(), reserve(input, work), work)
                     .get();
    ASSERT_TRUE(decoded.has_value());
    EXPECT_TRUE(input.at_end());
    EXPECT_EQ(
      decoded->value.wal().incarnation(), id<model::wal_incarnation_id>(0x70));
    EXPECT_EQ(decoded->value.wal().position().value(), 0U);
    EXPECT_EQ(decoded->value.wal().alignment().bytes().value(), 512U);
    EXPECT_EQ(decoded->value.target(), wal_expected().target);
    EXPECT_EQ(decoded->value.routing_epoch().value(), 1U);
    EXPECT_EQ(decoded->value.profile(), replay_profile::v1);
    EXPECT_EQ(decoded->value.target_profile(), storage_profile::v1);
    EXPECT_EQ(decoded->value.wal_extent().begin().value(), 0U);
    EXPECT_EQ(decoded->value.wal_extent().end().value(), 512U);
    EXPECT_EQ(decoded->value.batch().info(), info);
    EXPECT_EQ(flat(decoded->value.batch().bytes()), assigned_wire());
}

TEST(WalFormatTest, EveryTruncationAndFragmentCutNeedsTheWholeEnvelope) {
    const auto literal = wal_wire();
    for (std::size_t size = 0; size < literal.size(); ++size) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        fragmented_buffer_parser input{
          buffer(std::string_view{literal}.substr(0, size))};
        input.push_checkpoint().value();
        const auto memory = reserve(input, work);
        auto result
          = decode_wal_prepare(input, wal_expected(), memory, work).get();
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().code(), errc::truncated_data);
        result = decode_wal_prepare(
                   input,
                   wal_expected(),
                   memory,
                   work,
                   {},
                   codec::input_boundary::complete)
                   .get();
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().code(), errc::malformed_data);
        EXPECT_EQ(input.bytes_consumed(), byte_count{});
        EXPECT_EQ(input.checkpoint_depth(), 1U);
        seastar::thread::maybe_yield();
    }
    for (std::size_t cut = 1; cut < literal.size(); ++cut) {
        std::array pieces{
          seastar::temporary_buffer<char>{literal.data(), cut},
          seastar::temporary_buffer<char>{
            literal.data() + cut, literal.size() - cut}};
        fragmented_buffer_parser input{
          fragmented_buffer::copy_from_fragments(pieces).value()};
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        const auto result = decode_wal_prepare(
                              input, wal_expected(), reserve(input, work), work)
                              .get();
        ASSERT_TRUE(result.has_value());
        EXPECT_TRUE(input.at_end());
        EXPECT_EQ(flat(result->value.batch().bytes()), assigned_wire());
        seastar::thread::maybe_yield();
    }
}

TEST(WalFormatTest, IndependentAlignmentsAndFutureHeadersPreserveExactChild) {
    for (const std::uint64_t a : {512U, 4096U, 65536U}) {
        const auto context = wal_expected(a, a == 512 ? 4096 : 512);
        for (const bool compressed : {false, true}) {
            for (const std::size_t h : {32U, 40U, 4096U}) {
                const auto child_wire = assigned_wire(compressed, 40, true);
                const auto literal = wal_wire(child_wire, context, h);
                const codec::field_context diagnostic{
                  .origin = UINT64_MAX - literal.size() - 2U};
                seastar::abort_source abort;
                codec::cooperative_work work{codec::limits::defaults(), abort};
                auto decoded = [&] {
                    fragmented_buffer_parser input{
                      buffer("p" + literal + "s", 67)};
                    input.skip(byte_count{1}).value();
                    input.push_checkpoint().value();
                    const auto memory = reserve(input, work, diagnostic);
                    auto result = decode_wal_prepare(
                                    input, context, memory, work, diagnostic)
                                    .get()
                                    .value();
                    const auto cost = result.value.batch()
                                        .bytes()
                                        .allocation_cost(charge)
                                        .value();
                    EXPECT_EQ(
                      memory.operation_remaining.value()
                        - result.remaining.operation_remaining.value(),
                      cost.descriptors.value());
                    EXPECT_EQ(
                      memory.metadata_remaining.value()
                        - result.remaining.metadata_remaining.value(),
                      cost.descriptors.value());
                    EXPECT_EQ(
                      input.bytes_consumed().value(), 1U + literal.size());
                    EXPECT_EQ(input.checkpoint_depth(), 1U);
                    return result;
                }();
                EXPECT_EQ(flat(decoded.value.batch().bytes()), child_wire);
                EXPECT_EQ(
                  decoded.value.wal().alignment(), context.wal.alignment());
                EXPECT_EQ(
                  decoded.value.target().alignment(),
                  context.target.alignment());
                EXPECT_EQ(
                  decoded.value.wal_extent().end().value(), literal.size());
                EXPECT_EQ(
                  decoded.value.batch().info().verification,
                  model::batch_fingerprint_verification::carried);
                auto child = std::move(decoded.value).release_batch();
                const auto encoded = encode_wal_prepare(
                                       std::move(child),
                                       context,
                                       work,
                                       budget().operation_remaining,
                                       charge)
                                       .get();
                ASSERT_TRUE(encoded.has_value());
                EXPECT_EQ(flat(*encoded), wal_wire(child_wire, context));
            }
        }
    }
}

TEST(WalFormatTest, ContextAndPaddingErrorsAreCheckedAfterIntegrity) {
    struct mutation {
        std::size_t offset, width;
        std::uint64_t value;
        errc error;
    };
    constexpr mutation cases[]{
      {0, 1, 0x71, errc::wrong_context},
      {15, 1, 0x71, errc::wrong_context},
      {16, 8, 512, errc::wrong_context},
      {24, 1, 0x51, errc::wrong_context},
      {40, 1, 0x11, errc::wrong_context},
      {56, 1, 0x21, errc::wrong_context},
      {72, 1, 0x31, errc::wrong_context},
      {88, 8, 2, errc::wrong_context},
      {88, 8, 0, errc::malformed_data},
      {96, 8, 2, errc::wrong_context},
      {96, 8, 0, errc::malformed_data},
      {104, 8, 1, errc::wrong_context},
      {112, 8, 1024, errc::wrong_context},
      {120, 4, 513, errc::malformed_data},
      {120, 4, 1024, errc::wrong_context},
      {124, 2, 0, errc::malformed_data},
      {124, 2, 2, errc::unsupported_format},
      {126, 2, 1, errc::malformed_data},
      {128, 4, 0, errc::malformed_data},
      {128, 4, 222, errc::malformed_data},
      {128, 4, 0xffffffffU, errc::resource_exhausted},
      {132, 4, 120, errc::malformed_data},
      {136, 1, 0, errc::malformed_data},
      {359, 1, 1, errc::malformed_data},
      {479, 1, 1, errc::malformed_data}};
    for (const auto& change : cases) {
        SCOPED_TRACE(change.offset);
        auto wire = wal_wire();
        put(wire, 32 + change.offset, change.value, change.width);
        for (const bool repaired : {false, true}) {
            if (repaired) repair(wire);
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            fragmented_buffer_parser input{buffer("p" + wire, 7)};
            input.skip(byte_count{1}).value();
            input.push_checkpoint().value();
            const auto result
              = decode_wal_prepare(
                  input, wal_expected(), reserve(input, work), work)
                  .get();
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(
              result.error().code(),
              repaired ? change.error : errc::corrupt_data);
            EXPECT_EQ(input.bytes_consumed(), byte_count{1});
            EXPECT_EQ(input.checkpoint_depth(), 1U);
        }
    }
    for (const std::size_t offset : {0U, 24U, 40U, 56U, 72U}) {
        auto wire = wal_wire();
        wire.replace(32 + offset, 16, 16, '\0');
        repair(wire);
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        fragmented_buffer_parser input{buffer(wire)};
        const auto result = decode_wal_prepare(
                              input, wal_expected(), reserve(input, work), work)
                              .get();
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().code(), errc::malformed_data);
    }
}

TEST(WalFormatTest, DistinctMultibyteReplayFieldsHaveIndependentOffsets) {
    constexpr std::uint64_t wal_position = 0x2233445566778000;
    constexpr std::uint64_t target_position = 0x0102030405060800;
    constexpr std::uint64_t physical = 0x3344556677889900;
    constexpr std::uint64_t generation = 0x1122334455667788;
    constexpr std::uint64_t routing = 0x445566778899aabb;
    auto context = wal_expected(4096, 512);
    context.wal = wal_write_context::make(
                    id<model::wal_incarnation_id>(0x91),
                    alignment(4096),
                    runtime::file_position{wal_position})
                    .value();
    context.target
      = block_expected(0x82, generation, target_position, physical).location;
    context.routing_epoch = model::range_routing_epoch::make(routing).value();
    auto child_wire = assigned_wire(false, 4096, true);
    put(child_wire, 4096 + 72, routing, 8);
    child_wire.replace(
      4096 + 104,
      32,
      hex("5baaf58ba633d6a2903fb34ddef1796b6e9450fe9f6cb1acc2826050b51731c2"));
    repair(child_wire);
    const auto literal = wal_wire(child_wire, context, 40);
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    fragmented_buffer_parser input{buffer(literal, 67)};
    auto decoded
      = decode_wal_prepare(input, context, reserve(input, work), work).get();
    ASSERT_TRUE(decoded.has_value());
    EXPECT_TRUE(input.at_end());
    EXPECT_EQ(
      decoded->value.wal().incarnation(), id<model::wal_incarnation_id>(0x91));
    EXPECT_EQ(decoded->value.wal().position().value(), wal_position);
    EXPECT_EQ(
      decoded->value.wal_extent().end().value(), wal_position + literal.size());
    EXPECT_EQ(decoded->value.target().segment().cluster(), sc().cluster());
    EXPECT_EQ(decoded->value.target().segment().topic(), sc().topic());
    EXPECT_EQ(decoded->value.target().segment().range(), sc().range());
    EXPECT_EQ(
      decoded->value.target().segment().segment(), id<model::segment_id>(0x82));
    EXPECT_EQ(
      decoded->value.target().segment().generation().value(), generation);
    EXPECT_EQ(decoded->value.target().physical_begin().value(), physical);
    EXPECT_EQ(decoded->value.target().position().value(), target_position);
    EXPECT_EQ(decoded->value.routing_epoch().value(), routing);
    EXPECT_EQ(decoded->value.wal().alignment().bytes().value(), 4096U);
    EXPECT_EQ(decoded->value.target().alignment().bytes().value(), 512U);
    EXPECT_EQ(flat(decoded->value.batch().bytes()), child_wire);
    auto child = std::move(decoded->value).release_batch();
    const auto encoded
      = encode_wal_prepare(
          std::move(child), context, work, budget().operation_remaining, charge)
          .get();
    ASSERT_TRUE(encoded.has_value());
    EXPECT_EQ(flat(*encoded), wal_wire(child_wire, context));
}

TEST(
  WalFormatTest, NestedContextAgreementDoesNotReplaceIndependentExpectations) {
    auto child = assigned_wire();
    child[32 + 40] = '\x11';
    repair(child);
    auto wire = wal_wire(child);
    wire[32 + 40] = '\x11';
    repair(wire);
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    fragmented_buffer_parser input{buffer(wire)};
    auto result = decode_wal_prepare(
                    input, wal_expected(), reserve(input, work), work)
                    .get();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), errc::wrong_context);
    EXPECT_EQ(
      result.error().field(), static_cast<std::uint16_t>(wal_field::topic));
    auto context = wal_expected();
    context.routing_epoch = model::range_routing_epoch::make(2).value();
    input = fragmented_buffer_parser{
      buffer(wal_wire(assigned_wire(), context))};
    result
      = decode_wal_prepare(input, context, reserve(input, work), work).get();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), errc::malformed_data);
    EXPECT_EQ(
      result.error().field(),
      static_cast<std::uint16_t>(wal_field::routing_epoch));
    EXPECT_EQ(input.bytes_consumed(), byte_count{});
}

TEST(WalFormatTest, RelocatedPrepareFeedsSegmentWriterWithoutReencodingChild) {
    for (const bool compressed : {false, true}) {
        auto context = wal_expected(4096, 512);
        context.target = block_expected(0x80, 2, 8192, 17).location;
        const auto child_wire = assigned_wire(compressed, 4096, true);
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto result = [&] {
            fragmented_buffer_parser input{
              buffer(wal_wire(child_wire, context), 67)};
            return decode_wal_prepare(
                     input, context, reserve(input, work), work)
              .get()
              .value();
        }();
        EXPECT_EQ(
          result.value.target().segment().segment(),
          id<model::segment_id>(0x80));
        EXPECT_EQ(
          result.value.batch().info().context.submitted().binding().segment(),
          id<model::segment_id>(0x30));
        const auto original = result.value.batch().info();
        auto child = std::move(result.value).release_batch();
        const auto block = encode_segment_block(
                             std::move(child),
                             block_expected(0x80, 2, 8192, 17),
                             work,
                             budget().operation_remaining,
                             charge)
                             .get();
        ASSERT_TRUE(block.has_value());
        EXPECT_EQ(block->descriptor().batch(), original);
        EXPECT_EQ(
          flat(block->bytes()).substr(32 + 120, child_wire.size()), child_wire);
        EXPECT_EQ(block->descriptor().coverage().logical().count().value(), 5U);
        EXPECT_EQ(
          block->descriptor().coverage().physical().count().value(), 2U);
    }
}

TEST(WalFormatTest, OnlyOneAssignedEnvelopeAndExpectedFamilyAreAccepted) {
    auto child = assigned_wire();
    for (int mode = 0; mode < 3; ++mode) {
        auto wire = wal_wire(mode == 0 ? child + child : child);
        if (mode == 1) {
            put(wire, 4, 4, 2);
            repair(wire);
        }
        if (mode == 2) {
            auto empty_child = child;
            put(empty_child, 32 + 148, 0, 4);
            repair(empty_child);
            wire = wal_wire(empty_child);
        }
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        fragmented_buffer_parser input{buffer(wire)};
        const auto result = decode_wal_prepare(
                              input, wal_expected(), reserve(input, work), work)
                              .get();
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(
          result.error().code(),
          mode == 1 ? errc::wrong_context : errc::malformed_data);
        EXPECT_EQ(input.bytes_consumed(), byte_count{});
    }
}

TEST(WalFormatTest, AdjacentPreparesShareTheParentRemainder) {
    auto second_context = wal_expected();
    second_context.wal = wal_write_context::make(
                           second_context.wal.incarnation(),
                           alignment(),
                           runtime::file_position{512})
                           .value();
    second_context.target = block_expected(0x30, 1, 1024, 1).location;
    const auto one = wal_wire();
    const auto two = wal_wire(assigned_wire(), second_context);
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    fragmented_buffer_parser input{buffer(one + two, 67)};
    const auto memory = reserve(input, work);
    auto first
      = decode_wal_prepare(input, wal_expected(), memory, work).get().value();
    const auto boundary = input.bytes_consumed();
    const auto wrong
      = decode_wal_prepare(input, wal_expected(), first.remaining, work).get();
    ASSERT_FALSE(wrong.has_value());
    EXPECT_EQ(wrong.error().code(), errc::wrong_context);
    EXPECT_EQ(input.bytes_consumed(), boundary);
    auto second = decode_wal_prepare(
                    input, second_context, first.remaining, work)
                    .get()
                    .value();
    EXPECT_TRUE(input.at_end());
    const auto cost
      = second.value.batch().bytes().allocation_cost(charge).value();
    EXPECT_EQ(
      first.remaining.operation_remaining.value()
        - second.remaining.operation_remaining.value(),
      cost.descriptors.value());
    EXPECT_EQ(flat(first.value.batch().bytes()), assigned_wire());
    EXPECT_EQ(flat(second.value.batch().bytes()), assigned_wire());
}

TEST(WalFormatTest, InvalidCallerContextAndExhaustedBudgetLeaveInputUnchanged) {
    for (int mode = 0; mode < 8; ++mode) {
        auto context = wal_expected();
        if (mode == 0) context.routing_epoch = {};
        if (mode == 1) context.target_data_start = {};
        if (mode == 2) context.target_data_start = runtime::file_position{513};
        if (mode == 3) context.target_data_start = runtime::file_position{1024};
        if (mode == 4) context.batch.topic = {};
        if (mode == 5) context.target_profile = static_cast<storage_profile>(0);
        auto memory = budget();
        if (mode == 6) memory.operation_remaining = {};
        if (mode == 7) memory.metadata_remaining = {};
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        fragmented_buffer_parser input{buffer(wal_wire())};
        input.push_checkpoint().value();
        const auto result
          = decode_wal_prepare(input, context, memory, work).get();
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(
          result.error().code(),
          mode >= 6 ? errc::resource_exhausted : errc::invalid_argument);
        EXPECT_EQ(input.bytes_consumed(), byte_count{});
        EXPECT_EQ(input.checkpoint_depth(), 1U);
    }
}

TEST(WalFormatTest, StricterPolicyAndPositionOverflowRejectBeforePublication) {
    for (int mode = 0; mode < 4; ++mode) {
        seastar::abort_source abort;
        codec::cooperative_work prepare{codec::limits::defaults(), abort};
        auto child = checked_child(prepare, true, 40);
        auto context = wal_expected();
        auto config = codec::limits_config{};
        if (mode == 0) config.max_record_bytes = byte_count{6};
        if (mode == 1)
            context.wal = wal_write_context::make(
                            context.wal.incarnation(),
                            alignment(),
                            runtime::file_position{UINT64_MAX - 511U})
                            .value();
        if (mode == 2)
            context.target = block_expected(0x30, 1, 512, UINT64_MAX).location;
        codec::cooperative_work work{
          codec::limits::make(config).value(), abort};
        const codec::field_context diagnostic{
          .origin = mode == 3 ? UINT64_MAX - 511U : 1234};
        const auto result = encode_wal_prepare(
                              std::move(child),
                              context,
                              work,
                              budget().operation_remaining,
                              charge,
                              diagnostic)
                              .get();
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(
          result.error().code(),
          mode == 0   ? errc::resource_exhausted
          : mode == 3 ? errc::invalid_argument
                      : errc::out_of_range);
    }
}

TEST(WalFormatTest, AllocationFailuresJoinOwnersAndRestoreExistingMarks) {
#ifndef SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION
    GTEST_SKIP() << "allocation failure injection is unavailable";
#else
    for (const bool encode : {false, true}) {
        bool completed = false;
        std::size_t failures = 0;
        const auto literal = wal_wire(assigned_wire(true));
        for (std::uint64_t ordinal = 0; ordinal < 384 && !completed;
             ++ordinal) {
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            fragmented_buffer_parser input{buffer("p" + literal, 67)};
            input.skip(byte_count{1}).value();
            input.push_checkpoint().value();
            const auto memory = reserve(input, work);
            std::optional<encoded_assigned_batch> child;
            if (encode) child.emplace(checked_child(work, true));
            auto& injector = seastar::memory::local_failure_injector();
            bool threw = false, succeeded = false;
            injector.fail_after(ordinal);
            try {
                if (encode)
                    succeeded = encode_wal_prepare(
                                  std::move(*child),
                                  wal_expected(),
                                  work,
                                  budget().operation_remaining,
                                  charge)
                                  .get()
                                  .has_value();
                else
                    succeeded = decode_wal_prepare(
                                  input, wal_expected(), memory, work)
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
                ++failures;
            } else {
                ASSERT_TRUE(succeeded);
                completed = !reached;
            }
            EXPECT_EQ(
              input.bytes_consumed().value(),
              !encode && succeeded ? 1U + literal.size() : 1U);
            EXPECT_EQ(input.checkpoint_depth(), 1U);
        }
        EXPECT_TRUE(completed);
        EXPECT_GT(failures, 0U);
    }
#endif
}

TEST(WalFormatTest, FinalBodyAndFragmentLimitsIncludeHeadersAndPadding) {
    for (const std::uint64_t cap : {479U, 480U}) {
        seastar::abort_source abort;
        codec::cooperative_work prepare{codec::limits::defaults(), abort};
        auto child = checked_child(prepare);
        auto config = codec::limits_config{};
        config.max_encoded_body_bytes = byte_count{cap};
        codec::cooperative_work work{
          codec::limits::make(config).value(), abort};
        const auto output = encode_wal_prepare(
                              std::move(child),
                              wal_expected(),
                              work,
                              budget().operation_remaining,
                              charge)
                              .get();
        if (cap == 479) {
            ASSERT_FALSE(output.has_value());
            EXPECT_EQ(output.error().code(), errc::resource_exhausted);
        } else {
            ASSERT_TRUE(output.has_value());
            EXPECT_EQ(flat(*output), wal_wire());
        }
    }
    for (const std::uint64_t cap : {225U, 226U}) {
        seastar::abort_source abort;
        codec::cooperative_work prepare{codec::limits::defaults(), abort};
        auto source = buffer(assigned_wire(), 1);
        const auto memory = reserve(source, prepare);
        auto child = validate_encoded_assigned_batch(
                       std::move(source), batch_expected(), memory, prepare)
                       .get()
                       .value();
        auto config = codec::limits_config{};
        config.max_buffer_fragments = item_count{cap};
        codec::cooperative_work work{
          codec::limits::make(config).value(), abort};
        const auto output = encode_wal_prepare(
                              std::move(child),
                              wal_expected(),
                              work,
                              budget().operation_remaining,
                              charge)
                              .get();
        if (cap == 225) {
            ASSERT_FALSE(output.has_value());
            EXPECT_EQ(output.error().code(), errc::resource_exhausted);
        } else {
            ASSERT_TRUE(output.has_value());
            EXPECT_EQ(output->fragment_count(), 226U);
        }
    }
}

TEST(
  WalFormatTest, CancellationDuringSuspensionConsumesDonorOrRollsBackCursor) {
    for (const bool encode : {false, true}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto child = checked_child(work, true);
        fragmented_buffer_parser input{
          buffer(wal_wire(assigned_wire(true)), 1)};
        input.push_checkpoint().value();
        const auto memory = reserve(input, work);
        const auto deadline = std::chrono::steady_clock::now()
                              + std::chrono::seconds{2};
        while (!seastar::need_preempt()
               && std::chrono::steady_clock::now() < deadline) {
        }
        ASSERT_TRUE(seastar::need_preempt());
        if (encode) {
            auto pending = encode_wal_prepare(
              std::move(child),
              wal_expected(),
              work,
              budget().operation_remaining,
              charge);
            const bool suspended = !pending.available();
            abort.request_abort();
            const auto result = pending.get();
            EXPECT_TRUE(suspended);
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(result.error().code(), errc::aborted);
            // Consuming entry empties the donor before suspension.
            // NOLINTNEXTLINE(bugprone-use-after-move)
            EXPECT_TRUE(child.bytes().empty());
        } else {
            auto pending = decode_wal_prepare(
              input, wal_expected(), memory, work);
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
TEST(WalFormatTest, UsesOneParentCheckpointAndPreservesExistingMarks) {
    for (const std::size_t depth : {7U, 8U}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        fragmented_buffer_parser input{buffer(wal_wire())};
        for (std::size_t i = 0; i < depth; ++i)
            input.push_checkpoint().value();
        const auto result = decode_wal_prepare(
                              input, wal_expected(), reserve(input, work), work)
                              .get();
        if (depth == 7) {
            ASSERT_TRUE(result.has_value());
            EXPECT_TRUE(input.at_end());
        } else {
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(result.error().code(), errc::resource_exhausted);
            EXPECT_EQ(input.bytes_consumed(), byte_count{});
        }
        EXPECT_EQ(input.checkpoint_depth(), depth);
    }
}

// Prebuild the abort exception so even the cancellation probe keeps the
// allocator-profile callback nonallocating. There are no abort subscriptions.
class prepared_abort_source final : public seastar::abort_source {
    std::exception_ptr stopped_{
      std::make_exception_ptr(seastar::abort_requested_exception{})};
    std::exception_ptr get_default_exception() const noexcept override {
        return stopped_;
    }
};
thread_local seastar::abort_source* accounting_abort = nullptr;
thread_local std::uint64_t accounting_calls = 0, cancel_at = 0;
byte_count observed_charge(byte_count request) noexcept {
    if (++accounting_calls == cancel_at && accounting_abort != nullptr)
        accounting_abort->request_abort();
    return charge(request);
}

TEST(WalFormatTest, CancellationAfterChildAndPaddingValidationPreventsCommit) {
    std::uint64_t last = 0;
    for (const bool cancel : {false, true}) {
        prepared_abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        fragmented_buffer_parser input{
          buffer("p" + wal_wire(assigned_wire(true)), 67)};
        input.skip(byte_count{1}).value();
        input.push_checkpoint().value();
        auto memory = budget();
        memory.charge = observed_charge;
        memory
          = codec::reserve_decode_input(input, work.policy(), memory).value();
        accounting_calls = 0;
        cancel_at = cancel ? last : 0;
        accounting_abort = &abort;
        const auto reset = seastar::defer([] noexcept {
            accounting_abort = nullptr;
            cancel_at = 0;
        });
        const auto result
          = decode_wal_prepare(input, wal_expected(), memory, work).get();
        if (!cancel) {
            ASSERT_TRUE(result.has_value());
            // The final charge query measures the retained child only after
            // complete model validation and every padding byte have passed.
            last = accounting_calls;
            ASSERT_GT(last, 0U);
            EXPECT_TRUE(input.at_end());
        } else {
            EXPECT_EQ(accounting_calls, last);
            EXPECT_TRUE(abort.abort_requested());
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(result.error().code(), errc::aborted);
            EXPECT_EQ(input.bytes_consumed(), byte_count{1});
        }
        EXPECT_EQ(input.checkpoint_depth(), 1U);
    }
}
} // namespace
} // namespace kwaque::storage
