#include "src/storage/tests/footer_test_support.h"

#include <seastar/util/alloc_failure_injector.hh>

#include <gtest/gtest.h>

#include <concepts>
#include <string>
#include <type_traits>
#include <vector>

namespace kwaque::storage {
namespace {
using namespace testing;
using bytes::fragmented_buffer_parser;
static_assert(!std::default_initializable<verified_extent>);
static_assert(!std::is_aggregate_v<verified_extent>);
static_assert(!std::default_initializable<durable_footer>);
static_assert(!std::is_aggregate_v<durable_footer>);
static_assert(!std::default_initializable<encoded_durable_footer>);
static_assert(!std::is_aggregate_v<encoded_durable_footer>);
static_assert(!std::copy_constructible<encoded_durable_footer>);
static_assert(!std::constructible_from<
              verified_extent,
              segment_history_context,
              boundary_fields>);

// The block lies at [512, 911) and its footer follows it in the same write,
// padding from 911 to the boundary at 1536.
constexpr std::uint64_t block_end = 911;

TEST(FooterFormatTest, IndependentGoldenAndEvidenceBackedEncoding) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    ASSERT_EQ(512U + data_block().size(), block_end);
    const auto evidence = single_evidence(work);
    const footer_expectation expected{
      history(), runtime::file_position{block_end}};
    const auto literal = footer_wire(
      {scope(100, 101, 0, 1, 512, block_end),
       1,
       scope(100, 101, 0, 1, 512, block_end),
       {runtime::wall_time{1}, runtime::wall_time{1}, runtime::wall_time{0}},
       crc(data_block())},
      expected);
    EXPECT_EQ(
      literal.substr(0, 32),
      hex("4b514246060001000100200051020000000000000000000091de2e46033d214b"));
    EXPECT_EQ(literal.size(), 1536U - block_end);
    const auto output
      = encode_durable_footer(
          evidence, expected, work, budget().operation_remaining, charge)
          .get();
    ASSERT_TRUE(output.has_value());
    EXPECT_EQ(flat(output->bytes()), literal);
    // Packed into one owner only when the prefix and padding staging covers
    // the whole envelope.
    if (bytes::testing::native_charge_profile)
        EXPECT_EQ(
          output->bytes().fragment_count(),
          charge(byte_count{literal.size()})
              <= *charge(byte_count{248})
                    .checked_add(charge(byte_count{literal.size() - 248U}))
            ? 1U
            : 2U);
    fragmented_buffer_parser input{buffer("p" + literal + "s", 7)};
    input.skip(byte_count{1}).value();
    input.push_checkpoint().value();
    const auto decoded = decode_durable_footer(
                           input, expected, reserve(input, work), work)
                           .get();
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(output->descriptor().boundary(), decoded->boundary());
    EXPECT_EQ(output->descriptor().encoded_extent(), decoded->encoded_extent());
    EXPECT_TRUE(validate_durable_footer(*decoded, evidence));
    EXPECT_EQ(decoded->location().history, history());
    EXPECT_EQ(decoded->location().position.value(), block_end);
    EXPECT_EQ(decoded->encoded_extent().end().value(), 1536U);
    EXPECT_EQ(decoded->boundary(), evidence.boundary());
    EXPECT_EQ(input.bytes_consumed().value(), 1U + literal.size());
    EXPECT_EQ(input.checkpoint_depth(), 1U);
}

TEST(FooterFormatTest, EmptyTerminalBoundaryAndFooterOnlyHistory) {
    auto h = history();
    h.logical_origin = model::range_logical_end{UINT64_MAX};
    h.physical_origin = model::segment_relative_end{UINT64_MAX};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto empty = scope(
      UINT64_MAX, UINT64_MAX, UINT64_MAX, UINT64_MAX, 512, 512);
    auto verifier = extent_verifier::make(h, empty, work.policy()).value();
    const auto evidence = verifier.finish(work).value();
    const auto literal = footer_wire(
      evidence.boundary(), {h, runtime::file_position{512}});
    const auto encoded = encode_durable_footer(
                           evidence,
                           {h, runtime::file_position{512}},
                           work,
                           budget().operation_remaining,
                           charge)
                           .get();
    ASSERT_TRUE(encoded.has_value());
    EXPECT_EQ(flat(encoded->bytes()), literal);
    EXPECT_EQ(evidence.boundary().data_crc32c, 0U);
    EXPECT_FALSE(evidence.boundary().last_block.has_value());
    auto only = extent_verifier::make(
                  h,
                  scope(
                    UINT64_MAX, UINT64_MAX, UINT64_MAX, UINT64_MAX, 512, 1024),
                  work.policy())
                  .value();
    ASSERT_TRUE(feed_footer(only, literal, work, evidence));
    const auto all = only.finish(work).value();
    EXPECT_EQ(all.boundary().block_count, 0U);
    EXPECT_EQ(all.boundary().data_crc32c, crc(literal));
    EXPECT_FALSE(all.boundary().last_block.has_value());
    EXPECT_TRUE(encode_durable_footer(
                  all,
                  {h, runtime::file_position{1024}},
                  work,
                  budget().operation_remaining,
                  charge)
                  .get());
}

TEST(FooterFormatTest, HeaderExtensionsChangePaddingAndNotNamedHistory) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto evidence = single_evidence(work);
    for (const std::size_t h : {32U, 40U, 4096U}) {
        const footer_expectation expected{
          history(), runtime::file_position{1536}};
        const auto literal = footer_wire(evidence.boundary(), expected, h);
        const codec::field_context coordinates{
          .origin = UINT64_MAX - literal.size()};
        fragmented_buffer_parser input{buffer(literal, 67)};
        const auto decoded = decode_durable_footer(
                               input,
                               expected,
                               reserve(input, work, coordinates),
                               work,
                               coordinates)
                               .get();
        ASSERT_TRUE(decoded.has_value());
        EXPECT_TRUE(validate_durable_footer(*decoded, evidence));
        EXPECT_EQ(decoded->encoded_extent().size().value(), literal.size());
        EXPECT_EQ(
          decoded->boundary().coverage.bytes().end().value(), block_end);
    }
}

TEST(FooterFormatTest, EveryTruncationAndRepairedFieldCorruptionRejects) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto evidence = single_evidence(work);
    const footer_expectation expected{
      history(), runtime::file_position{block_end}};
    const auto literal = footer_wire(evidence.boundary(), expected);
    for (std::size_t size = 0; size < literal.size(); ++size) {
        fragmented_buffer_parser input{
          buffer(std::string_view{literal}.substr(0, size))};
        const auto memory = reserve(input, work);
        auto result
          = decode_durable_footer(input, expected, memory, work).get();
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().code(), errc::truncated_data);
        result = decode_durable_footer(
                   input,
                   expected,
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
    struct mutation {
        std::size_t offset, width;
        std::uint64_t value;
        errc error;
    };
    constexpr mutation cases[]{
      {0, 1, 0x51, errc::wrong_context},
      {16, 1, 0x11, errc::wrong_context},
      {32, 1, 0x21, errc::wrong_context},
      {48, 1, 0x31, errc::wrong_context},
      {64, 8, 2, errc::wrong_context},
      {64, 8, 0, errc::malformed_data},
      {72, 8, 1536, errc::wrong_context},
      {80, 8, 102, errc::malformed_data},
      {96, 8, 2, errc::malformed_data},
      {112, 8, 0, errc::malformed_data},
      {120, 8, 1536, errc::malformed_data},
      {128, 4, 0, errc::malformed_data},
      {128, 4, 2, errc::malformed_data},
      {132, 1, 2, errc::malformed_data},
      {133, 1, 1, errc::malformed_data},
      {134, 1, 1, errc::malformed_data},
      {135, 1, 1, errc::malformed_data},
      {136, 8, 99, errc::malformed_data},
      {160, 8, 0, errc::malformed_data},
      {176, 8, 1536, errc::malformed_data},
      // A first append time below 1 ns, a first
      // after the latest, a latest before the
      // first.
      {184, 8, 0, errc::malformed_data},
      {184, 8, 2, errc::malformed_data},
      {192, 8, 0, errc::malformed_data},
      // The padding takes the footer from its own
      // position to the next boundary.
      {212, 4, 376, errc::malformed_data},
      {212, 4, 0, errc::malformed_data},
      {216, 1, 1, errc::malformed_data},
      {592, 1, 1, errc::malformed_data}};
    for (const auto& change : cases) {
        SCOPED_TRACE(change.offset);
        auto wire = literal;
        put(wire, 32 + change.offset, change.value, change.width);
        for (const bool repaired : {false, true}) {
            if (repaired) repair(wire);
            fragmented_buffer_parser input{buffer("p" + wire, 7)};
            input.skip(byte_count{1}).value();
            input.push_checkpoint().value();
            const auto result = decode_durable_footer(
                                  input, expected, reserve(input, work), work)
                                  .get();
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(
              result.error().code(),
              repaired ? change.error : errc::corrupt_data);
            EXPECT_EQ(input.bytes_consumed(), byte_count{1});
            EXPECT_EQ(input.checkpoint_depth(), 1U);
        }
    }
}

TEST(FooterFormatTest, IsolatedParsingCannotProveCrcOrInteriorBlockCount) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto second = data_block(
      101, 1, block_end, false, 0x30, 1, false, 512, 3);
    const auto end = block_end + second.size();
    auto verifier = extent_verifier::make(
                      history(), scope(100, 102, 0, 2, 512, end), work.policy())
                      .value();
    ASSERT_TRUE(feed_block(verifier, data_block(), work));
    ASSERT_TRUE(feed_block(verifier, second, work));
    const auto evidence = verifier.finish(work).value();
    EXPECT_EQ(
      evidence.boundary().times,
      (boundary_times{
        runtime::wall_time{1}, runtime::wall_time{3}, runtime::wall_time{0}}));
    const footer_expectation expected{history(), runtime::file_position{end}};
    // A footer's times, like its count and checksum, are what its evidence
    // gives; parsing it alone cannot tell.
    for (int mode = 0; mode < 5; ++mode) {
        auto fields = evidence.boundary();
        if (mode == 0) fields.data_crc32c ^= 1U;
        if (mode == 1) fields.block_count = 1;
        if (mode == 2) fields.times.first_append = runtime::wall_time{2};
        if (mode == 3) fields.times.max_append = runtime::wall_time{4};
        if (mode == 4) fields.times.max_record = runtime::wall_time{-9};
        fragmented_buffer_parser input{buffer(footer_wire(fields, expected))};
        const auto parsed = decode_durable_footer(
                              input, expected, reserve(input, work), work)
                              .get();
        ASSERT_TRUE(parsed.has_value());
        const auto checked = validate_durable_footer(*parsed, evidence);
        ASSERT_FALSE(checked.has_value());
        EXPECT_EQ(
          checked.error().code(),
          mode == 0 ? errc::corrupt_data : errc::malformed_data);
    }
}

TEST(
  FooterFormatTest, PlacementContextAndFullyRemovedRepresentationStayDistinct) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto evidence = single_evidence(work);
    for (const auto position : {512U, 768U}) {
        const auto result = encode_durable_footer(
                              evidence,
                              {history(), runtime::file_position{position}},
                              work,
                              budget().operation_remaining,
                              charge)
                              .get();
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().code(), errc::invalid_argument);
    }
    const auto wrong = encode_durable_footer(
                         evidence,
                         {history(0x30, 2), runtime::file_position{1024}},
                         work,
                         budget().operation_remaining,
                         charge)
                         .get();
    ASSERT_FALSE(wrong.has_value());
    EXPECT_EQ(wrong.error().code(), errc::wrong_context);
    auto removed = extent_verifier::make(
                     history(),
                     scope(100, 105, 0, 0, 512, 512),
                     work.policy(),
                     extent_layout_kind::rewrite)
                     .value();
    const auto proof = removed.finish(work).value();
    EXPECT_EQ(proof.boundary().coverage.logical().count().value(), 5U);
    const auto invalid = encode_durable_footer(
                           proof,
                           {history(), runtime::file_position{512}},
                           work,
                           budget().operation_remaining,
                           charge)
                           .get();
    ASSERT_FALSE(invalid.has_value());
    EXPECT_EQ(invalid.error().code(), errc::invalid_argument);
}
TEST(FooterFormatTest, DistinctWideFieldsAndTerminalRecordEnds) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto h = history(0x80, 0x4142434445464748);
    constexpr std::uint64_t l = 0x1112131415161700, p = 0x2122232425262700,
                            b = 0x3132333435363800;
    const boundary_fields fields{
      scope(l, l + 12, p, p + 8, b, b + 4093),
      3,
      scope(l + 7, l + 12, p + 6, p + 8, b + 1029, b + 4093),
      {runtime::wall_time{0x0102030405060708},
       runtime::wall_time{0x0102030405060799},
       runtime::wall_time{-0x5152535455565758}},
      0x8192a3b4};
    const footer_expectation location{h, runtime::file_position{b + 4608}};
    fragmented_buffer_parser input{buffer(footer_wire(fields, location, 4096))};
    const auto decoded = decode_durable_footer(
                           input, location, reserve(input, work), work)
                           .get();
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->boundary(), fields);
    EXPECT_EQ(
      decoded->location().history.segment.generation().value(),
      0x4142434445464748U);
    EXPECT_EQ(decoded->encoded_extent().begin().value(), b + 4608);
    const auto terminal = data_block(UINT64_MAX - 1, UINT64_MAX - 1);
    const auto end = 512 + terminal.size();
    auto verifier
      = extent_verifier::make(
          history(),
          scope(
            UINT64_MAX - 1, UINT64_MAX, UINT64_MAX - 1, UINT64_MAX, 512, end),
          work.policy())
          .value();
    ASSERT_TRUE(feed_block(verifier, terminal, work));
    const auto evidence = verifier.finish(work).value();
    const auto encoded = encode_durable_footer(
                           evidence,
                           {history(), runtime::file_position{end}},
                           work,
                           budget().operation_remaining,
                           charge)
                           .get();
    ASSERT_TRUE(encoded.has_value());
    EXPECT_EQ(
      flat(encoded->bytes()),
      footer_wire(
        evidence.boundary(), {history(), runtime::file_position{end}}));
}

TEST(
  FooterFormatTest,
  EmptyPaddingAndAbsentDescriptorAreCanonicalAtEveryAlignment) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    for (std::uint64_t a = 512; a <= 65536; a *= 2) {
        const auto h = history(0x30, 1, a);
        auto verifier = extent_verifier::make(
                          h, scope(100, 100, 0, 0, a, a), work.policy())
                          .value();
        const auto evidence = verifier.finish(work).value();
        const footer_expectation location{h, runtime::file_position{a}};
        const auto output
          = encode_durable_footer(
              evidence, location, work, budget().operation_remaining, charge)
              .get();
        ASSERT_TRUE(output.has_value());
        EXPECT_EQ(
          flat(output->bytes()), footer_wire(evidence.boundary(), location));
    }
    const footer_expectation location{history(), runtime::file_position{512}};
    const boundary_fields empty{
      scope(100, 100, 0, 0, 512, 512), 0, std::nullopt, {}, 0};
    for (int mode = 0; mode < 8; ++mode) {
        auto wire = footer_wire(empty, location);
        if (mode == 0) put(wire, 32 + 132, 1, 1);
        if (mode == 1) put(wire, 32 + 136, 1, 1);
        if (mode == 2) put(wire, 32 + 104, 1, 8);
        if (mode == 3) put(wire, 32 + 88, 101, 8);
        // No block: every time is zero, and no byte is checksummed.
        if (mode == 4) put(wire, 32 + 184, 1, 8);
        if (mode == 5) put(wire, 32 + 192, 1, 8);
        if (mode == 6) put(wire, 32 + 200, 1, 8);
        if (mode == 7) put(wire, 32 + 208, 1, 4);
        repair(wire);
        fragmented_buffer_parser input{buffer(wire)};
        const auto result = decode_durable_footer(
                              input, location, reserve(input, work), work)
                              .get();
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().code(), errc::malformed_data);
    }
}

// A write's blocks lie back to back from its start; its footer follows the
// last of them and pads to the next boundary. A write may also hold a footer
// alone.
TEST(FooterFormatTest, FooterEndsAWriteOfPackedBlocks) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    for (const std::uint64_t count : {0U, 1U, 2U, 7U, 64U}) {
        SCOPED_TRACE(count);
        std::uint64_t position = 512;
        std::vector<std::string> blocks;
        for (std::uint64_t i = 0; i < count; ++i) {
            blocks.push_back(data_block(
              100 + i,
              i,
              position,
              false,
              0x30,
              1,
              false,
              512,
              static_cast<std::int64_t>(1 + i)));
            position += blocks.back().size();
        }
        auto verifier = extent_verifier::make(
                          history(),
                          scope(100, 100 + count, 0, count, 512, position),
                          work.policy())
                          .value();
        for (const auto& block : blocks)
            ASSERT_TRUE(feed_block(verifier, block, work));
        const auto evidence = verifier.finish(work).value();
        const auto expected_times
          = count == 0 ? boundary_times{}
                       : boundary_times{
                           runtime::wall_time{1},
                           runtime::wall_time{static_cast<std::int64_t>(count)},
                           runtime::wall_time{0}};
        EXPECT_EQ(evidence.boundary().times, expected_times);
        const footer_expectation at{
          history(), runtime::file_position{position}};
        const auto encoded
          = encode_durable_footer(
              evidence, at, work, budget().operation_remaining, charge)
              .get();
        ASSERT_TRUE(encoded.has_value());
        const auto literal = footer_wire(evidence.boundary(), at);
        EXPECT_EQ(flat(encoded->bytes()), literal);
        EXPECT_EQ((position + literal.size()) % 512U, 0U);
        EXPECT_LT(literal.size(), 248U + 512U);
        fragmented_buffer_parser input{buffer(literal)};
        const auto decoded
          = decode_durable_footer(input, at, reserve(input, work), work).get();
        ASSERT_TRUE(decoded.has_value());
        EXPECT_TRUE(validate_durable_footer(*decoded, evidence));
    }
}

TEST(FooterFormatTest, AllocationFailuresRestoreInputOrDiscardPartialOutput) {
#ifndef SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION
    GTEST_SKIP() << "allocation failure injection is unavailable";
#else
    seastar::abort_source setup_abort;
    codec::cooperative_work setup{codec::limits::defaults(), setup_abort};
    const auto evidence = single_evidence(setup);
    const footer_expectation location{
      history(), runtime::file_position{block_end}};
    const auto literal = footer_wire(evidence.boundary(), location);
    for (const bool encode : {false, true}) {
        bool completed = false;
        std::size_t failures = 0;
        for (std::uint64_t ordinal = 0; ordinal < 256 && !completed;
             ++ordinal) {
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            fragmented_buffer_parser input{buffer("p" + literal, 67)};
            input.skip(byte_count{1}).value();
            input.push_checkpoint().value();
            const auto memory = reserve(input, work);
            auto& injector = seastar::memory::local_failure_injector();
            bool threw = false, success = false;
            injector.fail_after(ordinal);
            try {
                if (encode)
                    success = encode_durable_footer(
                                evidence,
                                location,
                                work,
                                budget().operation_remaining,
                                charge)
                                .get()
                                .has_value();
                else
                    success = decode_durable_footer(
                                input, location, memory, work)
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
                EXPECT_FALSE(success);
                ++failures;
            } else {
                ASSERT_TRUE(success);
                completed = !reached;
            }
            EXPECT_EQ(
              input.bytes_consumed().value(),
              !encode && success ? 1U + literal.size() : 1U);
            EXPECT_EQ(input.checkpoint_depth(), 1U);
        }
        EXPECT_TRUE(completed);
        EXPECT_GT(failures, 0U);
    }
#endif
}
} // namespace
} // namespace kwaque::storage
