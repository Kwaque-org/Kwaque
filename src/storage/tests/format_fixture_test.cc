#include "src/codec/tests/format_fixture.h"
#include "src/codec/tests/format_test_support.h"
#include "src/storage/tests/range_manifest_test_support.h"
#include "src/storage/tests/retry_test_support.h"
#include "src/storage/tests/sparse_index_test_support.h"
#include "src/storage/tests/wal_test_support.h"

#include <gtest/gtest.h>

namespace kwaque::storage {
namespace {
using namespace testing;
namespace fixture = codec::testing::format_fixture;
using bytes::fragmented_buffer_parser;

verified_extent fixture_extent(codec::cooperative_work& work) {
    auto verifier = extent_verifier::make(
                      history(),
                      scope(100, 101, 0, 1, 512, 1024),
                      work.policy(),
                      extent_layout_kind::initial_append,
                      {},
                      extent_integrity::crc32c_and_sha256)
                      .value();
    feed_block(verifier, fixture::read("segment_block"), work).value();
    return verifier.finish(work).value();
}

TEST(
  StorageFormatFixtureTest,
  HeaderBlockAndWalRetainIndependentContextsAndExactChild) {
    const auto child = fixture::read("assigned_dense");
    for (const std::size_t width : {1U, 7U, 67U}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        {
            const auto wire = fixture::read("segment_header");
            fragmented_buffer_parser input{buffer(wire, width)};
            const auto decoded
              = decode_segment_header(
                  input, header(), {}, reserve(input, work), work)
                  .get();
            ASSERT_TRUE(decoded.has_value());
            EXPECT_EQ(decoded->value, header());
            EXPECT_EQ(decoded->bytes.end().value(), 512U);
            EXPECT_TRUE(input.at_end());
            auto encoded
              = encode_segment_header(
                  decoded->value, work, budget().operation_remaining, charge)
                  .get();
            ASSERT_TRUE(encoded.has_value());
            EXPECT_TRUE(encoded->content_equals(wire));
        }
        {
            const auto wire = fixture::read("segment_block");
            fragmented_buffer_parser input{buffer(wire, width)};
            const auto decoded
              = decode_segment_block(
                  input, block_expected(), reserve(input, work), work)
                  .get();
            ASSERT_TRUE(decoded.has_value());
            EXPECT_TRUE(input.at_end());
            EXPECT_TRUE(decoded->value.bytes().content_equals(wire));
            EXPECT_EQ(decoded->value.descriptor().context(), sc());
            EXPECT_EQ(
              decoded->value.descriptor().coverage(),
              scope(100, 101, 0, 1, 512, 1024));
            EXPECT_EQ(
              decoded->value.descriptor().batch().fingerprint.bytes(),
              fixture::digest("batch_digest"));
            EXPECT_EQ(wire.substr(152, child.size()), child);
        }
        {
            const auto wire = fixture::read("wal_prepare");
            fragmented_buffer_parser input{buffer(wire, width)};
            const auto decoded
              = decode_wal_prepare(
                  input, wal_expected(), reserve(input, work), work)
                  .get();
            ASSERT_TRUE(decoded.has_value());
            EXPECT_TRUE(input.at_end());
            EXPECT_EQ(decoded->value.wal(), wal_expected().wal);
            EXPECT_EQ(decoded->value.target(), wal_expected().target);
            EXPECT_EQ(decoded->value.routing_epoch().value(), 1U);
            EXPECT_EQ(decoded->value.wal_extent().end().value(), 512U);
            EXPECT_TRUE(decoded->value.batch().bytes().content_equals(child));
        }
    }
}

TEST(
  StorageFormatFixtureTest,
  FootersAndRetryPageRequireMatchingSuppliedEvidence) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto evidence = fixture_extent(work);
    ASSERT_TRUE(evidence.digest());
    EXPECT_EQ(evidence.digest()->bytes(), fixture::digest("extent_digest"));
    {
        const auto wire = fixture::read("durable_footer");
        fragmented_buffer_parser input{buffer(wire, 7)};
        const auto decoded
          = decode_durable_footer(
              input, root_location(), reserve(input, work), work)
              .get();
        ASSERT_TRUE(decoded.has_value());
        EXPECT_TRUE(input.at_end());
        EXPECT_TRUE(validate_durable_footer(*decoded, evidence));
        auto encoded = encode_durable_footer(
                         evidence,
                         root_location(),
                         work,
                         budget().operation_remaining,
                         charge)
                         .get();
        ASSERT_TRUE(encoded.has_value());
        EXPECT_TRUE(encoded->content_equals(wire));
    }
    const auto wire = fixture::read("sealed_footer");
    auto root = pin_root(wire, work);
    EXPECT_TRUE(validate_sealed_footer(root, evidence));
    EXPECT_EQ(root.retry_count(), 1U);
    ASSERT_EQ(root.pages().size(), 1U);
    retry_summary_verifier walk{root, work.policy()};
    {
        fragmented_buffer_parser input{buffer(fixture::read("retry_page"), 7)};
        auto page
          = walk.next(input, reserve_page(input, root, work), work).get();
        ASSERT_TRUE(page.has_value());
        EXPECT_TRUE(input.at_end());
        ASSERT_EQ(page->value.entries().size(), 1U);
        EXPECT_EQ(page->value.entries()[0], retry());
        EXPECT_EQ(page->value.reference(), root.pages()[0]);
        auto encoded = encode_retry_page(
                         page->value.entries(),
                         root_location(),
                         page_ordinal::make(0).value(),
                         0,
                         work,
                         page->remaining.operation_remaining,
                         charge)
                         .get();
        ASSERT_TRUE(encoded.has_value());
        EXPECT_TRUE(encoded->bytes.content_equals(fixture::read("retry_page")));
    }
    EXPECT_TRUE(walk.finish(work));
    auto encoded = encode_sealed_footer(
                     evidence,
                     root_location(),
                     1,
                     root.pages(),
                     work,
                     budget().operation_remaining,
                     charge)
                     .get();
    ASSERT_TRUE(encoded.has_value());
    EXPECT_TRUE(encoded->bytes.content_equals(wire));
}

TEST(
  StorageFormatFixtureTest, IndexAndManifestArtifactsBindPagesAndActualData) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto evidence = fixture_extent(work);
    const auto context
      = sparse_index_context::make(
          sc(), evidence.boundary().coverage, *evidence.digest(), alignment())
          .value();
    {
        auto root = index::pin(fixture::read("index_root"), context, work);
        ASSERT_TRUE(validate_sparse_index_extent(root, evidence));
        sparse_index_verifier walk{root, work.policy()};
        {
            fragmented_buffer_parser input{
              buffer(fixture::read("index_page"), 7)};
            auto page
              = walk.next(input, index::page_memory(input, root, work), work)
                  .get();
            ASSERT_TRUE(page.has_value());
            EXPECT_TRUE(input.at_end());
            ASSERT_EQ(page->value.entries().size(), 1U);
            EXPECT_EQ(page->value.entries()[0], index::entry());
            fragmented_buffer_parser block{
              buffer(fixture::read("segment_block"))};
            auto decoded
              = decode_segment_block(
                  block, block_expected(), reserve(block, work), work)
                  .get();
            ASSERT_TRUE(decoded.has_value());
            EXPECT_TRUE(validate_sparse_index_anchor(
              context, page->value.entries()[0], decoded->value.descriptor()));
            auto encoded = encode_sparse_index_page(
                             page->value.entries(),
                             context,
                             page_ordinal::make(0).value(),
                             0,
                             work,
                             page->remaining.operation_remaining,
                             charge)
                             .get();
            ASSERT_TRUE(encoded.has_value());
            EXPECT_TRUE(
              encoded->bytes.content_equals(fixture::read("index_page")));
        }
        EXPECT_TRUE(walk.finish(work));
        auto encoded = encode_sparse_index_root(
                         context,
                         1,
                         root.pages(),
                         work,
                         budget().operation_remaining,
                         charge)
                         .get();
        ASSERT_TRUE(encoded.has_value());
        EXPECT_TRUE(encoded->bytes.content_equals(fixture::read("index_root")));
    }
    {
        const auto header = manifest::root_header(
          1, 1, manifest::mc(), 100, 101);
        auto root = manifest::pin(fixture::read("manifest_root"), header, work);
        range_manifest_verifier walk{root, work.policy()};
        {
            fragmented_buffer_parser input{
              buffer(fixture::read("manifest_page"), 7)};
            auto page
              = walk.next(input, manifest::page_memory(input, root, work), work)
                  .get();
            ASSERT_TRUE(page.has_value());
            EXPECT_TRUE(input.at_end());
            ASSERT_EQ(page->value.entries().size(), 1U);
            const auto& entry = page->value.entries()[0];
            EXPECT_EQ(entry.segment(), sc().segment());
            EXPECT_EQ(entry.generation(), sc().generation());
            EXPECT_EQ(entry.coverage(), evidence.boundary().coverage);
            EXPECT_EQ(entry.digest(), *evidence.digest());
            EXPECT_TRUE(validate_range_manifest_extent(
              header.context(), sc().cluster(), entry, evidence));
            auto encoded = encode_range_manifest_page(
                             page->value.header(),
                             page->value.entries(),
                             alignment(),
                             work,
                             page->remaining.operation_remaining,
                             charge)
                             .get();
            ASSERT_TRUE(encoded.has_value());
            EXPECT_TRUE(
              encoded->bytes.content_equals(fixture::read("manifest_page")));
        }
        EXPECT_TRUE(walk.finish(work));
        auto encoded = encode_range_manifest_root(
                         header,
                         root.pages(),
                         alignment(),
                         work,
                         budget().operation_remaining,
                         charge)
                         .get();
        ASSERT_TRUE(encoded.has_value());
        EXPECT_TRUE(
          encoded->bytes.content_equals(fixture::read("manifest_root")));
    }
}

TEST(
  StorageFormatFixtureTest,
  RepairedPhysicalWrappersRetainInnerErrorCoordinates) {
    const auto child = fixture::read("assigned_dense");
    constexpr codec::field_context context{
      .origin = 71, .family = 91, .field = 92};
    for (const bool wal : {false, true}) {
        const auto original = fixture::read(
          wal ? "wal_prepare" : "segment_block");
        const std::size_t child_at = wal ? 168 : 152;
        for (const auto& mutation : fixture::record_mutations) {
            for (const std::size_t width : {7U, 67U}) {
                SCOPED_TRACE(
                  ::testing::Message{} << "wal=" << wal
                                       << " offset=" << mutation.offset
                                       << " width=" << width);
                seastar::abort_source abort;
                codec::cooperative_work work{codec::limits::defaults(), abort};
                auto batch = child;
                batch.at(216 + mutation.offset) = mutation.replacement;
                repair(batch);
                auto wire = original;
                wire.replace(child_at, child.size(), batch);
                repair(wire);
                fragmented_buffer_parser input{
                  buffer("prefix" + wire + original, width)};
                input.skip(byte_count{6}).value();
                input.push_checkpoint().value();
                const codec::error error{
                  mutation.code,
                  2,
                  mutation.field,
                  77 + child_at + 216 + mutation.error_offset};
                if (wal) {
                    auto expected = wal_expected();
                    expected.batch.fingerprint = codec::semantic_batch_digest{
                      fixture::digest("batch_digest")};
                    const auto decoded
                      = decode_wal_prepare(
                          input, expected, reserve(input, work), work, context)
                          .get();
                    fixture::expect_rejection(
                      decoded, error, input, byte_count{6}, 1);
                } else {
                    auto expected = block_expected();
                    expected.batch.fingerprint = codec::semantic_batch_digest{
                      fixture::digest("batch_digest")};
                    const auto decoded
                      = decode_segment_block(
                          input, expected, reserve(input, work), work, context)
                          .get();
                    fixture::expect_rejection(
                      decoded, error, input, byte_count{6}, 1);
                }
            }
        }
    }
}

TEST(
  StorageFormatFixtureTest, HeaderExtensionsAndPaddingBelongToTheExactDigest) {
    const auto child = fixture::read("assigned_dense");
    const auto plain = fixture::read("segment_block");
    const auto extended = block_wire(child, block_expected(), 41);
    ASSERT_EQ(plain.size(), extended.size());
    EXPECT_NE(exact_sha(plain), exact_sha(extended));
    for (const auto& wire : {plain, extended}) {
        for (const std::size_t width : {1U, 67U}) {
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            auto verifier = extent_verifier::make(
                              history(),
                              scope(100, 101, 0, 1, 512, 1024),
                              work.policy(),
                              extent_layout_kind::initial_append,
                              {},
                              extent_integrity::crc32c_and_sha256)
                              .value();
            auto stored = buffer(wire, width);
            const auto cost = stored.allocation_cost(charge).value();
            const auto memory
              = codec::detail::consume_decode_budget(
                  work.policy(),
                  budget(),
                  cost.backing,
                  cost.descriptors.checked_add(cost.share_controls).value(),
                  {},
                  0)
                  .value();
            auto expected = batch_expected();
            expected.fingerprint = codec::semantic_batch_digest{
              fixture::digest("batch_digest")};
            ASSERT_TRUE(
              verifier.add_block(std::move(stored), expected, memory, work)
                .get());
            const auto evidence = verifier.finish(work).value();
            EXPECT_EQ(evidence.digest()->bytes(), exact_sha(wire));
            EXPECT_EQ(evidence.boundary().block_count, 1U);
            EXPECT_EQ(
              evidence.boundary().coverage, scope(100, 101, 0, 1, 512, 1024));
        }
    }
}
} // namespace
} // namespace kwaque::storage
