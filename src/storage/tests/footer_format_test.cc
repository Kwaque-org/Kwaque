#include "src/storage/tests/footer_test_support.h"

#include <seastar/util/alloc_failure_injector.hh>

#include <gtest/gtest.h>

#include <concepts>
#include <type_traits>

namespace kwaque::storage {
namespace {
using namespace testing;
using bytes::fragmented_buffer_parser;
static_assert(!std::default_initializable<verified_extent>);
static_assert(!std::is_aggregate_v<verified_extent>);
static_assert(!std::default_initializable<durable_footer>);
static_assert(!std::is_aggregate_v<durable_footer>);
static_assert(!std::constructible_from<
              verified_extent,
              segment_history_context,
              boundary_fields>);

TEST(FooterFormatTest, IndependentGoldenAndEvidenceBackedEncoding) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto evidence = single_evidence(work);
    const footer_expectation expected{history(), runtime::file_position{1024}};
    const auto literal = footer_wire(
      {scope(100, 101, 0, 1, 512, 1024),
       1,
       scope(100, 101, 0, 1, 512, 1024),
       crc(data_block())},
      expected);
    EXPECT_EQ(
      literal.substr(0, 32),
      hex("4b5142460600010001002000e0010000000000000000000049be8a18fff66e8b"));
    const auto output
      = encode_durable_footer(
          evidence, expected, work, budget().operation_remaining, charge)
          .get();
    ASSERT_TRUE(output.has_value());
    EXPECT_EQ(flat(*output), literal);
    fragmented_buffer_parser input{buffer("p" + literal + "s", 7)};
    input.skip(byte_count{1}).value();
    input.push_checkpoint().value();
    const auto decoded = decode_durable_footer(
                           input, expected, reserve(input, work), work)
                           .get();
    ASSERT_TRUE(decoded.has_value());
    EXPECT_TRUE(validate_durable_footer(*decoded, evidence));
    EXPECT_EQ(decoded->location().history, history());
    EXPECT_EQ(decoded->location().position.value(), 1024U);
    EXPECT_EQ(decoded->encoded_extent().end().value(), 1536U);
    EXPECT_EQ(decoded->boundary(), evidence.boundary());
    EXPECT_EQ(input.bytes_consumed().value(), 513U);
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
    EXPECT_EQ(flat(*encoded), literal);
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
        EXPECT_EQ(decoded->boundary().coverage.bytes().end().value(), 1024U);
    }
}

TEST(FooterFormatTest, EveryTruncationAndRepairedFieldCorruptionRejects) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto evidence = single_evidence(work);
    const footer_expectation expected{history(), runtime::file_position{1024}};
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
    constexpr mutation cases[]{{0, 1, 0x51, errc::wrong_context},
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
                               {188, 4, 287, errc::malformed_data},
                               {192, 1, 1, errc::malformed_data},
                               {479, 1, 1, errc::malformed_data}};
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
    auto verifier = extent_verifier::make(
                      history(),
                      scope(100, 102, 0, 2, 512, 1536),
                      work.policy())
                      .value();
    ASSERT_TRUE(feed_block(verifier, data_block(), work));
    ASSERT_TRUE(feed_block(verifier, data_block(101, 1, 1024), work));
    const auto evidence = verifier.finish(work).value();
    const footer_expectation expected{history(), runtime::file_position{1536}};
    for (const bool crc_error : {false, true}) {
        auto fields = evidence.boundary();
        if (crc_error)
            fields.data_crc32c ^= 1U;
        else
            fields.block_count = 1;
        fragmented_buffer_parser input{buffer(footer_wire(fields, expected))};
        const auto parsed = decode_durable_footer(
                              input, expected, reserve(input, work), work)
                              .get();
        ASSERT_TRUE(parsed.has_value());
        const auto checked = validate_durable_footer(*parsed, evidence);
        ASSERT_FALSE(checked.has_value());
        EXPECT_EQ(
          checked.error().code(),
          crc_error ? errc::corrupt_data : errc::malformed_data);
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
      scope(l, l + 12, p, p + 8, b, b + 4096),
      3,
      scope(l + 7, l + 12, p + 6, p + 8, b + 1024, b + 3584),
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
    auto verifier
      = extent_verifier::make(
          history(),
          scope(
            UINT64_MAX - 1, UINT64_MAX, UINT64_MAX - 1, UINT64_MAX, 512, 1024),
          work.policy())
          .value();
    ASSERT_TRUE(
      feed_block(verifier, data_block(UINT64_MAX - 1, UINT64_MAX - 1), work));
    const auto evidence = verifier.finish(work).value();
    const auto encoded = encode_durable_footer(
                           evidence,
                           {history(), runtime::file_position{1024}},
                           work,
                           budget().operation_remaining,
                           charge)
                           .get();
    ASSERT_TRUE(encoded.has_value());
    EXPECT_EQ(
      flat(*encoded),
      footer_wire(
        evidence.boundary(), {history(), runtime::file_position{1024}}));
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
        EXPECT_EQ(flat(*output), footer_wire(evidence.boundary(), location));
    }
    const footer_expectation location{history(), runtime::file_position{512}};
    const boundary_fields empty{
      scope(100, 100, 0, 0, 512, 512), 0, std::nullopt, 0};
    for (int mode = 0; mode < 5; ++mode) {
        auto wire = footer_wire(empty, location);
        if (mode == 0) put(wire, 32 + 132, 1, 1);
        if (mode == 1) put(wire, 32 + 136, 1, 1);
        if (mode == 2) put(wire, 32 + 104, 1, 8);
        if (mode == 3) put(wire, 32 + 88, 101, 8);
        if (mode == 4) put(wire, 32 + 184, 1, 4);
        repair(wire);
        fragmented_buffer_parser input{buffer(wire)};
        const auto result = decode_durable_footer(
                              input, location, reserve(input, work), work)
                              .get();
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().code(), errc::malformed_data);
    }
}

TEST(FooterFormatTest, AllocationFailuresRestoreInputOrDiscardPartialOutput) {
#ifndef SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION
    GTEST_SKIP() << "allocation failure injection is unavailable";
#else
    seastar::abort_source setup_abort;
    codec::cooperative_work setup{codec::limits::defaults(), setup_abort};
    const auto evidence = single_evidence(setup);
    const footer_expectation location{history(), runtime::file_position{1024}};
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
              input.bytes_consumed().value(), !encode && success ? 513U : 1U);
            EXPECT_EQ(input.checkpoint_depth(), 1U);
        }
        EXPECT_TRUE(completed);
        EXPECT_GT(failures, 0U);
    }
#endif
}
} // namespace
} // namespace kwaque::storage
