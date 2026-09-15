#include "src/storage/tests/retry_test_support.h"

#include <seastar/util/alloc_failure_injector.hh>

#include <gtest/gtest.h>

#include <malloc.h>

namespace kwaque::storage {
namespace {
using namespace testing;
using bytes::fragmented_buffer_parser;

TEST(SealedFormatTest, IndependentGoldenAndExternalRootPin) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const std::array entries{retry()};
    const auto page = retry_page_wire(entries);
    const std::array refs{reference(page)};
    const auto wire = one_root(refs);
    EXPECT_EQ(
      wire.substr(0, 32),
      hex("4b5142460700010001002000e0010000000000000000000088e209138d5f87e6"));
    const auto evidence = hashed_evidence(work);
    const auto encoded = encode_sealed_footer(
                           evidence,
                           root_location(),
                           1,
                           refs,
                           work,
                           budget().operation_remaining,
                           charge)
                           .get();
    ASSERT_TRUE(encoded.has_value());
    EXPECT_EQ(flat(encoded->bytes), wire);
    EXPECT_EQ(encoded->digest.bytes(), exact_sha(wire));
    fragmented_buffer_parser input{buffer("prefix" + wire + "suffix", 7)};
    input.skip(byte_count{6}).value();
    input.push_checkpoint().value();
    const auto decoded = decode_sealed_footer(
                           input,
                           root_location(),
                           codec::immutable_object_digest{exact_sha(wire)},
                           reserve(input, work),
                           work)
                           .get();
    ASSERT_TRUE(decoded.has_value());
    EXPECT_TRUE(validate_sealed_footer(decoded->value, evidence));
    EXPECT_EQ(decoded->value.location().history, root_location().history);
    EXPECT_EQ(decoded->value.location().position.value(), 1536U);
    EXPECT_EQ(decoded->value.coverage(), evidence.boundary().coverage);
    EXPECT_EQ(decoded->value.block_count(), 1U);
    EXPECT_EQ(decoded->value.last_block(), evidence.boundary().last_block);
    EXPECT_EQ(decoded->value.extent_digest(), *evidence.digest());
    EXPECT_EQ(decoded->value.retry_count(), 1U);
    ASSERT_EQ(decoded->value.pages().size(), 1U);
    EXPECT_EQ(decoded->value.pages()[0], refs[0]);
    EXPECT_EQ(decoded->value.encoded_extent().end().value(), 2048U);
    EXPECT_EQ(input.bytes_consumed().value(), 518U);
    EXPECT_EQ(input.checkpoint_depth(), 1U);
    EXPECT_FALSE(validate_sealed_footer(decoded->value, single_evidence(work)));
}

TEST(SealedFormatTest, ShaCoversInterleavedFootersOnceAndNoBytesOutsideExtent) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto verifier = extent_verifier::make(
                      history(),
                      scope(100, 102, 0, 2, 512, 2048),
                      work.policy(),
                      extent_layout_kind::initial_append,
                      {},
                      extent_integrity::crc32c_and_sha256)
                      .value();
    const auto first = data_block();
    ASSERT_TRUE(feed_block(verifier, first, work));
    const auto prefix = verifier.checkpoint(work).value();
    EXPECT_FALSE(prefix.digest());
    const auto footer = footer_wire(
      prefix.boundary(), {history(), runtime::file_position{1024}});
    ASSERT_TRUE(feed_footer(verifier, footer, work));
    auto moved = std::move(verifier);
    // The documented move closes the source and preserves the stable native
    // owner. NOLINTNEXTLINE(bugprone-use-after-move)
    EXPECT_TRUE(verifier.closed());
    const auto second = data_block(101, 1, 1536);
    ASSERT_TRUE(feed_block(moved, second, work));
    const auto evidence = moved.finish(work).value();
    ASSERT_TRUE(evidence.digest());
    EXPECT_EQ(evidence.digest()->bytes(), exact_sha(first + footer + second));
    EXPECT_NE(evidence.digest()->bytes(), exact_sha(first + second));
    EXPECT_EQ(evidence.boundary().data_crc32c, crc(first + footer + second));
    const footer_expectation location{history(), runtime::file_position{2560}};
    const auto encoded
      = encode_sealed_footer(
          evidence, location, 0, {}, work, budget().operation_remaining, charge)
          .get();
    ASSERT_TRUE(encoded.has_value());
    const auto root = pin_root(flat(encoded->bytes), work, location);
    EXPECT_TRUE(validate_sealed_footer(root, evidence));
    EXPECT_EQ(root.coverage().bytes().end().value(), 2048U);
    EXPECT_NE(
      evidence.digest()->bytes(),
      exact_sha(first + footer + second + flat(encoded->bytes)));
}

TEST(SealedFormatTest, RemovedDataPreservesOriginalCoverageAndCompletedResult) {
    for (const bool all_removed : {false, true}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        const auto h = history(0x80, 2);
        const auto extent = all_removed ? scope(100, 110, 0, 0, 512, 512)
                                        : scope(100, 110, 0, 2, 512, 1024);
        auto verifier = extent_verifier::make(
                          h,
                          extent,
                          work.policy(),
                          extent_layout_kind::rewrite,
                          {},
                          extent_integrity::crc32c_and_sha256)
                          .value();
        std::string supplied;
        if (!all_removed) {
            supplied = data_block(102, 0, 512, true, 0x80, 2);
            ASSERT_TRUE(feed_block(verifier, supplied, work));
        }
        const auto evidence = verifier.finish(work).value();
        EXPECT_EQ(evidence.digest()->bytes(), exact_sha(supplied));
        const auto location = root_location(0x80, 2);
        const std::array entries{retry()};
        const auto page = encode_retry_page(
                            entries,
                            location,
                            page_ordinal::make(0).value(),
                            0,
                            work,
                            budget().operation_remaining,
                            charge)
                            .get();
        ASSERT_TRUE(page.has_value());
        const std::array refs{page->reference};
        const auto encoded = encode_sealed_footer(
                               evidence,
                               location,
                               1,
                               refs,
                               work,
                               budget().operation_remaining,
                               charge)
                               .get();
        ASSERT_TRUE(encoded.has_value());
        const auto root = pin_root(flat(encoded->bytes), work, location);
        EXPECT_TRUE(validate_sealed_footer(root, evidence));
        EXPECT_EQ(root.coverage().logical(), extent.logical());
        EXPECT_EQ(root.block_count(), all_removed ? 0U : 1U);
        EXPECT_EQ(root.last_block().has_value(), !all_removed);
        if (!all_removed)
            EXPECT_EQ(root.last_block()->logical().end().value(), 107U);
        retry_summary_verifier summary{root, work.policy()};
        fragmented_buffer_parser parser{buffer(flat(page->bytes))};
        const auto decoded
          = summary.next(parser, reserve_page(parser, root, work), work).get();
        ASSERT_TRUE(decoded.has_value());
        EXPECT_EQ(decoded->value.entries()[0], entries[0]);
        EXPECT_EQ(decoded->value.entries()[0].ack_generation().value(), 1U);
        EXPECT_EQ(root.location().history.segment.generation().value(), 2U);
        EXPECT_TRUE(summary.finish(work));
    }
}

TEST(SealedFormatTest, EmptyAndFooterOnlyHashWithoutCreatingRecords) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto verifier = extent_verifier::make(
                      history(),
                      scope(100, 100, 0, 0, 512, 512),
                      work.policy(),
                      extent_layout_kind::initial_append,
                      {},
                      extent_integrity::crc32c_and_sha256)
                      .value();
    const auto empty = verifier.finish(work).value();
    EXPECT_EQ(empty.digest()->bytes(), exact_sha(""));
    const auto prior = footer_wire(
      empty.boundary(), {history(), runtime::file_position{512}});
    auto only = extent_verifier::make(
                  history(),
                  scope(100, 110, 0, 0, 512, 1024),
                  work.policy(),
                  extent_layout_kind::rewrite,
                  {},
                  extent_integrity::crc32c_and_sha256)
                  .value();
    ASSERT_TRUE(feed_footer(only, prior, work, empty));
    const auto evidence = only.finish(work).value();
    EXPECT_EQ(evidence.boundary().block_count, 0U);
    EXPECT_EQ(evidence.digest()->bytes(), exact_sha(prior));
    for (const auto& proof : {empty, evidence}) {
        const auto output = encode_sealed_footer(
                              proof,
                              root_location(),
                              0,
                              {},
                              work,
                              budget().operation_remaining,
                              charge)
                              .get();
        ASSERT_TRUE(output.has_value());
        const auto root = pin_root(flat(output->bytes), work);
        EXPECT_TRUE(validate_sealed_footer(root, proof));
        retry_summary_verifier summary{root, work.policy()};
        const auto complete = summary.finish(work);
        ASSERT_TRUE(complete.has_value());
        EXPECT_EQ(complete->entry_count(), 0U);
    }
}

TEST(SealedFormatTest, ParsedDigestAndHistoryClaimsNeedIndependentEvidence) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto evidence = hashed_evidence(work);
    auto wire = one_root();
    wire[32 + 188] ^= 1;
    repair(wire);
    const auto altered = pin_root(wire, work);
    const auto result = validate_sealed_footer(altered, evidence);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), errc::corrupt_data);
    EXPECT_FALSE(encode_sealed_footer(
                   single_evidence(work),
                   root_location(),
                   0,
                   {},
                   work,
                   budget().operation_remaining,
                   charge)
                   .get());
    fragmented_buffer_parser input{buffer(wire)};
    const auto rejected = decode_sealed_footer(
                            input,
                            root_location(),
                            codec::immutable_object_digest{
                              exact_sha(one_root())},
                            reserve(input, work),
                            work)
                            .get();
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error().code(), errc::corrupt_data);
    EXPECT_EQ(input.bytes_consumed().value(), 0U);
}

TEST(
  SealedFormatTest, TruncationsMalformedSubkindsAndRepairedStructuralFields) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto wire = one_root();
    for (std::size_t n = 0; n < wire.size(); ++n) {
        fragmented_buffer_parser input{
          buffer(std::string_view{wire}.substr(0, n))};
        const auto memory = reserve(input, work);
        for (const auto boundary :
             {codec::input_boundary::open, codec::input_boundary::complete}) {
            const auto result = decode_sealed_footer(
                                  input,
                                  root_location(),
                                  codec::immutable_object_digest{
                                    exact_sha(wire)},
                                  memory,
                                  work,
                                  {},
                                  boundary)
                                  .get();
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(
              result.error().code(),
              boundary == codec::input_boundary::open ? errc::truncated_data
                                                      : errc::malformed_data);
            EXPECT_EQ(input.bytes_consumed().value(), 0U);
        }
        seastar::thread::maybe_yield();
    }
    struct mutation {
        std::size_t offset, width;
        std::uint64_t value;
    };
    constexpr mutation cases[]{
      {0, 2, 0},     {0, 2, 2},   {0, 2, 3},      {2, 2, 1},
      {4, 1, 0x51},  {68, 8, 0},  {68, 8, 2},     {76, 8, 1024},
      {84, 8, 102},  {100, 8, 2}, {116, 8, 0},    {124, 8, 2048},
      {132, 4, 0},   {132, 4, 2}, {136, 1, 2},    {137, 1, 1},
      {140, 8, 99},  {156, 8, 2}, {180, 8, 1536}, {220, 4, 1},
      {224, 4, 257}, {228, 4, 0}};
    for (const auto change : cases) {
        auto wrong = wire;
        put(wrong, 32 + change.offset, change.value, change.width);
        repair(wrong);
        fragmented_buffer_parser input{buffer(wrong)};
        EXPECT_FALSE(decode_sealed_footer(
                       input,
                       root_location(),
                       codec::immutable_object_digest{exact_sha(wrong)},
                       reserve(input, work),
                       work)
                       .get());
        EXPECT_EQ(input.bytes_consumed().value(), 0U);
    }
}

TEST(SealedFormatTest, ReferenceCountsLengthsAndIndicesAreCheckedAtTheRoot) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const std::array entry{retry()};
    const auto page = retry_page_wire(entry);
    const std::array refs{
      reference(page),
      reference(retry_page_wire(entry, root_location(), 1, 1), 1, 1)};
    const auto wire = one_root(refs);
    for (const auto offset :
         {220U, 224U, 232U, 236U, 240U, 244U, 280U, 284U, 288U, 292U}) {
        auto wrong = wire;
        put(wrong, 32 + offset, get(wrong, 32 + offset, 4) + 1U, 4);
        repair(wrong);
        fragmented_buffer_parser input{buffer(wrong)};
        EXPECT_FALSE(decode_sealed_footer(
                       input,
                       root_location(),
                       codec::immutable_object_digest{exact_sha(wrong)},
                       reserve(input, work),
                       work)
                       .get());
        EXPECT_EQ(input.bytes_consumed().value(), 0U);
    }
    const auto evidence = hashed_evidence(work);
    auto reversed = refs;
    std::swap(reversed[0], reversed[1]);
    EXPECT_FALSE(encode_sealed_footer(
                   evidence,
                   root_location(),
                   2,
                   reversed,
                   work,
                   budget().operation_remaining,
                   charge)
                   .get());
    EXPECT_FALSE(encode_sealed_footer(
                   evidence,
                   root_location(),
                   1,
                   refs,
                   work,
                   budget().operation_remaining,
                   charge)
                   .get());
}

TEST(SealedFormatTest, ExtensionsMaximumPageRefsAndDiagnosticEnd) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    std::vector<page_ref> refs;
    refs.reserve(256);
    for (std::uint32_t i = 0; i < 256; ++i)
        refs.push_back(
          page_ref::make(
            page_ordinal::make(i).value(),
            i,
            1,
            byte_count{512},
            codec::immutable_object_digest{{}})
            .value());
    for (const std::size_t h : {32U, 4096U}) {
        const auto wire = one_root(refs, root_location(), h);
        ASSERT_LE(wire.size(), 65536U);
        const codec::field_context c{
          .origin = UINT64_MAX - wire.size(), .family = 7};
        fragmented_buffer_parser input{buffer(wire, 67)};
        const auto memory = reserve(input, work, c);
        const auto decoded = decode_sealed_footer(
                               input,
                               root_location(),
                               codec::immutable_object_digest{exact_sha(wire)},
                               memory,
                               work,
                               c)
                               .get();
        ASSERT_TRUE(decoded.has_value());
        EXPECT_EQ(decoded->value.pages().size(), 256U);
        EXPECT_EQ(decoded->value.page_capacity(), 256U);
        EXPECT_EQ(decoded->value.retry_count(), 256U);
        const auto served = charge(
          byte_count{decoded->value.page_capacity() * sizeof(page_ref)});
        EXPECT_LE(served.value(), 131072U);
        EXPECT_GE(
          served.value(),
          ::malloc_usable_size(
            const_cast<page_ref*>(decoded->value.pages().data())));
        EXPECT_EQ(
          memory.metadata_remaining.value()
            - decoded->remaining.metadata_remaining.value(),
          served.value());
        EXPECT_EQ(
          memory.operation_remaining.value()
            - decoded->remaining.operation_remaining.value(),
          served.value());
        retry_summary_verifier missing{decoded->value, work.policy()};
        EXPECT_FALSE(missing.finish(work));
    }
}

TEST(
  SealedFormatTest, DistinctWideSpansAndNarrowMetadataAreCheckedIndependently) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const footer_expectation location{
      history(0x80, 0x1122334455667788ULL),
      runtime::file_position{0x556677880000ULL}};
    const auto extent = scope(
      0x223344550000ULL,
      0x223344550007ULL,
      0x334455660000ULL,
      0x334455660003ULL,
      0x445566770000ULL,
      0x445566771000ULL);
    const auto last = scope(
      0x223344550002ULL,
      0x223344550006ULL,
      0x334455660001ULL,
      0x334455660003ULL,
      0x445566770800ULL,
      0x445566771000ULL);
    const std::array entries{retry()};
    const auto page = retry_page_wire(entries, location);
    const std::array refs{reference(page)};
    const auto wire = sealed_wire(
      {extent, 2, last, 0},
      exact_sha("supplied extent digest"),
      refs,
      location);
    const auto root = pin_root(wire, work, location);
    EXPECT_EQ(
      root.location().history.segment.generation().value(),
      0x1122334455667788ULL);
    EXPECT_EQ(root.location().position, location.position);
    EXPECT_EQ(root.coverage().logical(), extent.logical());
    EXPECT_EQ(root.coverage().physical(), extent.physical());
    EXPECT_EQ(root.coverage().bytes(), extent.bytes());
    EXPECT_EQ(root.last_block(), last);
    EXPECT_EQ(root.block_count(), 2U);
    EXPECT_EQ(
      root.extent_digest().bytes(), exact_sha("supplied extent digest"));
    fragmented_buffer_parser input{buffer(wire)};
    auto memory = reserve(input, work);
    memory.metadata_remaining = {};
    const auto rejected = decode_sealed_footer(
                            input,
                            location,
                            codec::immutable_object_digest{exact_sha(wire)},
                            memory,
                            work)
                            .get();
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error().code(), errc::resource_exhausted);
    EXPECT_EQ(input.bytes_consumed().value(), 0U);
}

TEST(SealedFormatTest, EmptyRetryRootReturnsTheOriginalMetadataAllowance) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    for (const std::size_t header : {32U, 4096U}) {
        const auto wire = one_root({}, root_location(), header);
        fragmented_buffer_parser input{buffer("p" + wire, 7)};
        input.skip(byte_count{1}).value();
        input.push_checkpoint().value();
        const auto memory = reserve(input, work);
        const auto result = decode_sealed_footer(
                              input,
                              root_location(),
                              codec::immutable_object_digest{exact_sha(wire)},
                              memory,
                              work)
                              .get();
        ASSERT_TRUE(result.has_value());
        EXPECT_TRUE(result->value.pages().empty());
        EXPECT_EQ(result->remaining, memory);
        EXPECT_EQ(input.checkpoint_depth(), 1U);
        EXPECT_TRUE(input.at_end());
    }
}

TEST(SealedFormatTest, AllocationFailuresRestoreRootOrDiscardWriterStaging) {
#ifndef SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION
    GTEST_SKIP() << "allocation failure injection is unavailable";
#else
    seastar::abort_source warm_abort;
    codec::cooperative_work warm_work{codec::limits::defaults(), warm_abort};
    const auto evidence = hashed_evidence(warm_work);
    const std::array entry{retry()};
    const auto page = retry_page_wire(entry);
    const std::array refs{reference(page)};
    const auto wire = one_root(refs);
    for (const bool encode : {false, true}) {
        bool completed = false;
        std::size_t failures = 0;
        for (std::uint64_t ordinal = 0; ordinal < 256 && !completed;
             ++ordinal) {
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            fragmented_buffer_parser input{buffer("p" + wire)};
            input.skip(byte_count{1}).value();
            input.push_checkpoint().value();
            const auto memory = reserve(input, work);
            const auto digest = codec::immutable_object_digest{exact_sha(wire)};
            auto& injector = seastar::memory::local_failure_injector();
            bool succeeded = false;
            injector.fail_after(ordinal);
            try {
                if (encode)
                    succeeded = encode_sealed_footer(
                                  evidence,
                                  root_location(),
                                  1,
                                  refs,
                                  work,
                                  budget().operation_remaining,
                                  charge)
                                  .get()
                                  .has_value();
                else
                    succeeded = decode_sealed_footer(
                                  input, root_location(), digest, memory, work)
                                  .get()
                                  .has_value();
            } catch (const std::bad_alloc&) {
            } catch (const std::runtime_error&) {
                if (!injector.failed()) {
                    injector.cancel();
                    throw;
                }
            } catch (...) {
                injector.cancel();
                throw;
            }
            const auto reached = injector.failed();
            injector.cancel();
            if (!succeeded) {
                EXPECT_TRUE(reached);
                ++failures;
                EXPECT_EQ(input.bytes_consumed().value(), 1U);
            } else
                completed = !reached;
            EXPECT_EQ(input.checkpoint_depth(), 1U);
        }
        EXPECT_TRUE(completed);
        EXPECT_GT(failures, 0U);
    }
#endif
}
} // namespace
} // namespace kwaque::storage
