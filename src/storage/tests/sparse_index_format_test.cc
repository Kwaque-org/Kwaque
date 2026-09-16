#include "src/storage/sparse_index_format.h"
#include "src/storage/tests/footer_test_support.h"
#include "src/storage/tests/sparse_index_test_support.h"

#include <seastar/core/preempt.hh>
#include <seastar/util/alloc_failure_injector.hh>

#include <gtest/gtest.h>

#include <chrono>
#include <concepts>
#include <malloc.h>
#include <type_traits>

namespace kwaque::storage {
namespace {
using namespace testing;
using namespace testing::index;
using bytes::fragmented_buffer_parser;
static_assert(!std::default_initializable<sparse_index_context>);
static_assert(!std::is_aggregate_v<sparse_index_context>);
static_assert(!std::is_copy_constructible_v<sparse_index_root>);
static_assert(!std::is_copy_constructible_v<sparse_index_page>);
static_assert(!std::is_aggregate_v<verified_sparse_index_pages>);
static_assert(!std::is_constructible_v<
              sparse_index_verifier,
              sparse_index_root&&,
              codec::limits>);
static_assert(sizeof(sparse_index_entry) == 16);
static_assert(!std::is_constructible_v<
              sparse_index_entry,
              model::segment_relative_offset,
              runtime::file_position>);
static_assert(sizeof(sparse_index_verifier) < 2048);
static_assert(256U * sizeof(page_ref) <= 131072);

template<typename T>
void error(const codec::result<T>& value, errc code) {
    ASSERT_FALSE(value.has_value());
    EXPECT_EQ(value.error().code(), code);
}
codec::result<decoded_sparse_index_root> read_root(
  fragmented_buffer_parser& input,
  std::string_view wire,
  sparse_index_context context,
  codec::cooperative_work& work,
  codec::field_context c = {},
  codec::input_boundary boundary = codec::input_boundary::open) {
    return decode_sparse_index_root(
             input,
             context,
             codec::immutable_object_digest{sha(wire)},
             reserve(input, work, c),
             work,
             c,
             boundary)
      .get();
}

TEST(
  SparseIndexFormatTest,
  ContextPreservesIndependentSpansAndValidatesEmptyBytes) {
    const auto empty = target(0);
    EXPECT_TRUE(empty.coverage().logical().empty());
    EXPECT_TRUE(empty.coverage().physical().empty());
    EXPECT_EQ(empty.digest().bytes(), sha(""));
    EXPECT_TRUE(
      sparse_index_context::make(
        sc(),
        scope(100, 200, 0, 0, 512, 512),
        codec::extent_digest{sha("")},
        alignment()));
    EXPECT_TRUE(
      sparse_index_context::make(
        sc(),
        scope(UINT64_MAX, UINT64_MAX, UINT64_MAX, UINT64_MAX, 512, 1024),
        codec::extent_digest{sha("footer")},
        alignment()));
    EXPECT_FALSE(
      sparse_index_context::make(
        sc(),
        scope(100, 101, 0, 2, 512, 1024),
        codec::extent_digest{sha("x")},
        alignment()));
    EXPECT_FALSE(
      sparse_index_context::make(
        sc(),
        scope(100, 101, 0, 1, 512, 512),
        codec::extent_digest{sha("")},
        alignment()));
    EXPECT_FALSE(
      sparse_index_context::make(
        sc(),
        scope(100, 100, 0, 0, 512, 512),
        codec::extent_digest{sha("x")},
        alignment()));
    EXPECT_FALSE(
      sparse_index_context::make(
        sc(),
        scope(100, 101, 0, 1, 513, 1024),
        codec::extent_digest{sha("x")},
        alignment()));
    EXPECT_FALSE(
      sparse_index_context::make(
        sc(),
        scope(100, 101, 0, 1, 512, 1025),
        codec::extent_digest{sha("x")},
        alignment()));
    EXPECT_FALSE(
      sparse_index_context::make(
        sc(),
        target().coverage(),
        target().digest(),
        alignment(),
        static_cast<storage_profile>(0)));
    EXPECT_FALSE(
      sparse_index_context::make(
        sc(),
        target().coverage(),
        target().digest(),
        alignment(),
        static_cast<storage_profile>(2)));
}

TEST(SparseIndexFormatTest, IndependentRootAndPageBytesKeepEveryField) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto context = target();
    const std::array entries{entry(100, 512), entry(102, 1536)};
    const auto page = page_wire(entries, context);
    const std::array refs{page_reference(page, 2)};
    const auto wire = root_wire(refs, context);
    EXPECT_EQ(
      page.substr(0, 32),
      hex("4b5142460800010001002000e0010000000000000000000066de9383930fb69a"));
    EXPECT_EQ(
      wire.substr(0, 32),
      hex("4b5142460800010001002000e00100000000000000000000aa083091116fb5e3"));
    EXPECT_TRUE(
      std::ranges::equal(
        std::bit_cast<std::array<char, 32>>(sha(page)),
        hex(
          "d193ec3713f5c1a9610caf9a461c84c0b4cfc140f76d030939969e8db6ebfd46")));
    EXPECT_TRUE(
      std::ranges::equal(
        std::bit_cast<std::array<char, 32>>(sha(wire)),
        hex(
          "ff1ea42cf2fdaa9de71045ed07cfe2e01f1781893e1af0cf41a38dfa9f775aac")));
    const auto encoded_page = encode_sparse_index_page(
                                entries,
                                context,
                                page_ordinal::make(0).value(),
                                0,
                                work,
                                budget().operation_remaining,
                                charge)
                                .get();
    ASSERT_TRUE(encoded_page.has_value());
    EXPECT_EQ(flat(encoded_page->bytes), page);
    EXPECT_EQ(encoded_page->reference, refs[0]);
    const auto encoded_root
      = encode_sparse_index_root(
          context, 2, refs, work, budget().operation_remaining, charge)
          .get();
    ASSERT_TRUE(encoded_root.has_value());
    EXPECT_EQ(flat(encoded_root->bytes), wire);
    EXPECT_EQ(encoded_root->digest.bytes(), sha(wire));
    for (const std::size_t width : {1U, 7U, 67U, 512U}) {
        fragmented_buffer_parser input{buffer("p" + wire + "suffix", width)};
        input.skip(byte_count{1}).value();
        input.push_checkpoint().value();
        const codec::field_context c{.origin = 17};
        const auto decoded = read_root(input, wire, context, work, c);
        ASSERT_TRUE(decoded.has_value());
        const auto& root = decoded->value;
        EXPECT_EQ(root.context().segment(), context.segment());
        EXPECT_EQ(
          root.context().coverage().logical(), context.coverage().logical());
        EXPECT_EQ(
          root.context().coverage().physical(), context.coverage().physical());
        EXPECT_EQ(
          root.context().coverage().bytes(), context.coverage().bytes());
        EXPECT_EQ(root.context().digest(), context.digest());
        EXPECT_EQ(root.context().alignment(), context.alignment());
        EXPECT_EQ(root.context().profile(), context.profile());
        EXPECT_EQ(root.entry_count(), 2U);
        EXPECT_EQ(root.encoded_bytes().value(), wire.size());
        ASSERT_EQ(root.pages().size(), 1U);
        EXPECT_EQ(root.pages()[0], refs[0]);
        EXPECT_EQ(input.bytes_consumed().value(), wire.size() + 1U);
        EXPECT_EQ(input.checkpoint_depth(), 1U);
        fragmented_buffer_parser child{buffer("p" + page + "suffix", width)};
        child.skip(byte_count{1}).value();
        child.push_checkpoint().value();
        const auto memory = page_memory(child, root, work);
        const auto result
          = decode_sparse_index_page(
              child, root, page_ordinal::make(0).value(), memory, work, c)
              .get();
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(result->value.reference(), refs[0]);
        EXPECT_TRUE(std::ranges::equal(result->value.entries(), entries));
        EXPECT_EQ(child.bytes_consumed().value(), page.size() + 1U);
        EXPECT_EQ(child.checkpoint_depth(), 1U);
        const auto retained = charge(
          byte_count{
            result->value.entry_capacity() * sizeof(sparse_index_entry)});
        EXPECT_EQ(
          memory.metadata_remaining.value()
            - result->remaining.metadata_remaining.value(),
          retained.value());
        EXPECT_EQ(
          memory.operation_remaining.value()
            - result->remaining.operation_remaining.value(),
          retained.value());
    }
}

TEST(
  SparseIndexFormatTest,
  ExtensionsAndBothAlignmentEndpointsUseActualHeaderSize) {
    for (const std::uint64_t a : {512U, 65536U}) {
        for (const std::size_t h : {32U, 40U, 4096U}) {
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            const auto context = target(3, a);
            const std::array entries{entry(101, 2U * a)};
            const auto page = page_wire(entries, context, 0, 0, h);
            const std::array refs{page_reference(page)};
            const auto wire = root_wire(refs, context, h);
            const auto root = pin(wire, context, work);
            EXPECT_EQ(root.encoded_bytes().value(), wire.size());
            EXPECT_EQ(wire.size() % a, 0U);
            EXPECT_EQ(page.size() % a, 0U);
            fragmented_buffer_parser input{buffer(page, 1024)};
            const auto decoded = decode_sparse_index_page(
                                   input,
                                   root,
                                   page_ordinal::make(0).value(),
                                   page_memory(input, root, work),
                                   work)
                                   .get();
            ASSERT_TRUE(decoded.has_value());
            EXPECT_TRUE(input.at_end());
            EXPECT_EQ(decoded->value.entries().front(), entries[0]);
            auto bad_padding = page;
            put(bad_padding, h + 168, get(page, h + 168, 4) + 1U, 4);
            repair(bad_padding);
            const std::array bad_refs{page_reference(bad_padding)};
            const auto bad_root = pin(
              root_wire(bad_refs, context, h), context, work);
            fragmented_buffer_parser bad{buffer(bad_padding, 1024)};
            error(
              decode_sparse_index_page(
                bad,
                bad_root,
                page_ordinal::make(0).value(),
                page_memory(bad, bad_root, work),
                work)
                .get(),
              errc::malformed_data);
            EXPECT_EQ(bad.bytes_consumed().value(), 0U);
        }
    }
}

TEST(SparseIndexFormatTest, AbsoluteColumnsAndGenerationDoNotNarrowAt32Bits) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto base = std::uint64_t{1} << 48U;
    const auto context = sparse_index_context::make(
                           sc(0x81, UINT64_MAX),
                           scope(
                             UINT64_MAX - 10,
                             UINT64_MAX,
                             UINT64_MAX - 3,
                             UINT64_MAX,
                             base,
                             base + 1536),
                           codec::extent_digest{sha("wide")},
                           alignment())
                           .value();
    const std::array entries{
      entry(UINT64_MAX - 9, base), entry(UINT64_MAX - 1, base + 1024)};
    const auto page = page_wire(entries, context);
    const std::array refs{page_reference(page, 2)};
    const auto encoded = encode_sparse_index_page(
                           entries,
                           context,
                           page_ordinal::make(0).value(),
                           0,
                           work,
                           budget().operation_remaining,
                           charge)
                           .get();
    ASSERT_TRUE(encoded.has_value());
    EXPECT_EQ(flat(encoded->bytes), page);
    const auto root = pin(root_wire(refs, context), context, work);
    EXPECT_EQ(root.context(), context);
    fragmented_buffer_parser input{buffer(page)};
    const auto got = decode_sparse_index_page(
                       input,
                       root,
                       page_ordinal::make(0).value(),
                       page_memory(input, root, work),
                       work)
                       .get();
    ASSERT_TRUE(got.has_value());
    EXPECT_TRUE(std::ranges::equal(got->value.entries(), entries));
}

TEST(SparseIndexFormatTest, EmptyAndRemovedExtentsNeedNoSyntheticAnchor) {
    for (const auto& context :
         {target(0),
          sparse_index_context::make(
            sc(),
            scope(100, 200, 0, 0, 512, 512),
            codec::extent_digest{sha("")},
            alignment())
            .value(),
          sparse_index_context::make(
            sc(),
            scope(100, 200, 0, 0, 512, 1024),
            codec::extent_digest{sha("footer")},
            alignment())
            .value(),
          sparse_index_context::make(
            sc(),
            scope(UINT64_MAX, UINT64_MAX, UINT64_MAX, UINT64_MAX, 512, 512),
            codec::extent_digest{sha("")},
            alignment())
            .value()}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        const auto encoded
          = encode_sparse_index_root(
              context, 0, {}, work, budget().operation_remaining, charge)
              .get();
        ASSERT_TRUE(encoded.has_value());
        const auto wire = root_wire({}, context);
        EXPECT_EQ(flat(encoded->bytes), wire);
        fragmented_buffer_parser input{buffer(wire)};
        const auto memory = reserve(input, work);
        const auto result = decode_sparse_index_root(
                              input, context, encoded->digest, memory, work)
                              .get();
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(result->remaining, memory);
        EXPECT_TRUE(result->value.pages().empty());
        sparse_index_verifier verifier{result->value, work.policy()};
        const auto finished = verifier.finish(work);
        ASSERT_TRUE(finished.has_value());
        EXPECT_EQ(finished->entry_count(), 0U);
        EXPECT_TRUE(verifier.closed());
        const std::array entries{entry()};
        error(
          encode_sparse_index_page(
            entries,
            context,
            page_ordinal::make(0).value(),
            0,
            work,
            budget().operation_remaining,
            charge)
            .get(),
          errc::invalid_argument);
    }
}

TEST(
  SparseIndexFormatTest,
  SparseRelocatedAnchorUsesDeclaredBaseAndCompleteStart) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto location = block_expected(0x80, 2, 1024, 5);
    const auto block = block_wire(
      assigned_wire(false, 32, true), location, 4096);
    fragmented_buffer_parser input{buffer(block)};
    const auto decoded
      = decode_segment_block(input, location, reserve(input, work), work).get();
    ASSERT_TRUE(decoded.has_value());
    const auto descriptor = decoded->value.descriptor();
    const auto context = sparse_index_context::make(
                           location.location.segment(),
                           scope(
                             90, 110, 5, 7, 512, 1024U + block.size() + 512U),
                           codec::extent_digest{sha(block)},
                           alignment())
                           .value();
    EXPECT_TRUE(
      validate_sparse_index_anchor(context, entry(100, 1024), descriptor));
    error(
      validate_sparse_index_anchor(context, entry(101, 1024), descriptor),
      errc::malformed_data);
    error(
      validate_sparse_index_anchor(context, entry(100, 1536), descriptor),
      errc::malformed_data);
    error(
      validate_sparse_index_anchor(context, entry(100, 1056), descriptor),
      errc::malformed_data);
    const auto wrong
      = sparse_index_context::make(
          sc(0x80, 3), context.coverage(), context.digest(), alignment())
          .value();
    error(
      validate_sparse_index_anchor(wrong, entry(100, 1024), descriptor),
      errc::wrong_context);
    for (const auto& clipped :
         {scope(100, 104, 5, 7, 512, 1024U + block.size()),
          scope(100, 105, 5, 6, 512, 1024U + block.size()),
          scope(100, 105, 5, 7, 1024, 1536)}) {
        const auto short_context
          = sparse_index_context::make(
              context.segment(), clipped, context.digest(), alignment())
              .value();
        error(
          validate_sparse_index_anchor(
            short_context, entry(100, 1024), descriptor),
          errc::malformed_data);
    }
}

TEST(SparseIndexFormatTest, ExtentEvidenceChecksBytesSeparatelyFromMetadata) {
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
    ASSERT_TRUE(feed_block(verifier, data_block(), work));
    const auto evidence = verifier.finish(work).value();
    const auto context
      = sparse_index_context::make(
          sc(), evidence.boundary().coverage, *evidence.digest(), alignment())
          .value();
    const std::array entries{entry()};
    const std::array refs{page_reference(page_wire(entries, context))};
    const auto root = pin(root_wire(refs, context), context, work);
    EXPECT_TRUE(validate_sparse_index_extent(root, evidence));
    error(
      validate_sparse_index_extent(root, single_evidence(work)),
      errc::invalid_argument);
    auto digest = context.digest().bytes();
    digest[0] ^= 1U;
    const auto altered
      = sparse_index_context::make(
          sc(), context.coverage(), codec::extent_digest{digest}, alignment())
          .value();
    const auto incorrect = pin(root_wire(refs, altered), altered, work);
    error(
      validate_sparse_index_extent(incorrect, evidence), errc::corrupt_data);
    const auto other_generation
      = sparse_index_context::make(
          sc(0x30, 2), context.coverage(), context.digest(), alignment())
          .value();
    const auto stale = pin(
      root_wire(refs, other_generation), other_generation, work);
    error(validate_sparse_index_extent(stale, evidence), errc::wrong_context);
}

TEST(
  SparseIndexFormatTest,
  SuppliedBlocksMustFitTheExpectedAlignmentAndBlockCount) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto wire = data_block(100, 0, 1024);
    fragmented_buffer_parser input{buffer(wire)};
    const auto block
      = decode_segment_block(
          input, block_expected(0x30, 1, 1024), reserve(input, work), work)
          .get();
    ASSERT_TRUE(block.has_value());
    const auto context = sparse_index_context::make(
                           sc(),
                           scope(100, 101, 0, 1, 1024, 2048),
                           codec::extent_digest{sha(wire)},
                           alignment(1024))
                           .value();
    error(
      validate_sparse_index_anchor(
        context, entry(100, 1024), block->value.descriptor()),
      errc::malformed_data);

    const auto sparse = data_block(100, 0, 512, true);
    const auto extent = scope(100, 105, 0, 2, 512, 1024);
    auto walk = extent_verifier::make(
                  history(),
                  extent,
                  work.policy(),
                  extent_layout_kind::rewrite,
                  {},
                  extent_integrity::crc32c_and_sha256)
                  .value();
    ASSERT_TRUE(feed_block(walk, sparse, work));
    const auto evidence = walk.finish(work).value();
    EXPECT_EQ(evidence.boundary().block_count, 1U);
    const auto indexed = sparse_index_context::make(
                           sc(), extent, *evidence.digest(), alignment())
                           .value();
    const std::array refs{
      page_ref::make(
        page_ordinal::make(0).value(),
        0,
        2,
        byte_count{512},
        codec::immutable_object_digest{sha("unverified page")})
        .value()};
    const auto root = pin(root_wire(refs, indexed), indexed, work);
    error(validate_sparse_index_extent(root, evidence), errc::malformed_data);
}

TEST(SparseIndexFormatTest, DiagnosticEndIsNotAnIndexFilePosition) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const std::array entries{entry()};
    const std::array refs{page_reference(page_wire(entries))};
    const auto wire = root_wire(refs);
    const codec::field_context exact{.origin = UINT64_MAX - wire.size()};
    fragmented_buffer_parser input{buffer(wire)};
    const auto root = read_root(input, wire, target(), work, exact);
    ASSERT_TRUE(root.has_value());
    EXPECT_TRUE(input.at_end());
    EXPECT_EQ(root->value.encoded_bytes().value(), wire.size());
    const auto encoded
      = encode_sparse_index_root(
          target(), 1, refs, work, budget().operation_remaining, charge, exact)
          .get();
    ASSERT_TRUE(encoded.has_value());
    EXPECT_EQ(flat(encoded->bytes), wire);
    const codec::field_context overflow{.origin = exact.origin + 1U};
    error(
      encode_sparse_index_root(
        target(), 1, refs, work, budget().operation_remaining, charge, overflow)
        .get(),
      errc::out_of_range);
}

TEST(
  SparseIndexFormatTest,
  OrderedEntryValidationRejectsBothColumnsAndExtentEdges) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    for (const auto& entries :
         {std::array{entry(100, 512), entry(100, 1024)},
          std::array{entry(101, 512), entry(100, 1024)},
          std::array{entry(100, 512), entry(101, 512)},
          std::array{entry(100, 1024), entry(101, 512)},
          std::array{entry(99, 512), entry(101, 1024)},
          std::array{entry(100, 512), entry(103, 1024)},
          std::array{entry(100, 0), entry(101, 1024)},
          std::array{entry(100, 512), entry(101, 2048)},
          std::array{entry(100, 512), entry(101, 1025)}}) {
        const auto wire = page_wire(entries);
        const std::array refs{page_reference(wire, 2)};
        const auto root = pin(root_wire(refs), target(), work);
        fragmented_buffer_parser input{buffer("p" + wire)};
        input.skip(byte_count{1}).value();
        input.push_checkpoint().value();
        error(
          decode_sparse_index_page(
            input,
            root,
            page_ordinal::make(0).value(),
            page_memory(input, root, work),
            work)
            .get(),
          errc::malformed_data);
        EXPECT_EQ(input.bytes_consumed().value(), 1U);
        EXPECT_EQ(input.checkpoint_depth(), 1U);
        error(
          encode_sparse_index_page(
            entries,
            target(),
            page_ordinal::make(0).value(),
            0,
            work,
            budget().operation_remaining,
            charge)
            .get(),
          errc::invalid_argument);
    }
    const std::array entries{entry()};
    auto wire = page_wire(entries);
    put(wire, 32 + 172, UINT64_MAX, 8);
    repair(wire);
    const std::array refs{page_reference(wire)};
    const auto root = pin(root_wire(refs), target(), work);
    fragmented_buffer_parser input{buffer(wire)};
    error(
      decode_sparse_index_page(
        input,
        root,
        page_ordinal::make(0).value(),
        page_memory(input, root, work),
        work)
        .get(),
      errc::malformed_data);
}
TEST(SparseIndexFormatTest, EveryRootAndPageTruncationRestoresCallerMarks) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const std::array entries{entry()};
    const auto page = page_wire(entries);
    const std::array refs{page_reference(page)};
    const auto wire = root_wire(refs);
    const auto root = pin(wire, target(), work);
    for (const bool is_root : {false, true}) {
        const auto& bytes = is_root ? wire : page;
        for (const auto boundary :
             {codec::input_boundary::open, codec::input_boundary::complete}) {
            for (std::size_t length = 0; length < bytes.size(); ++length) {
                fragmented_buffer_parser input{
                  buffer("p" + bytes.substr(0, length))};
                input.skip(byte_count{1}).value();
                input.push_checkpoint().value();
                const auto expected = boundary == codec::input_boundary::open
                                        ? errc::truncated_data
                                        : errc::malformed_data;
                if (is_root)
                    error(
                      read_root(input, wire, target(), work, {}, boundary),
                      expected);
                else
                    error(
                      decode_sparse_index_page(
                        input,
                        root,
                        page_ordinal::make(0).value(),
                        page_memory(input, root, work),
                        work,
                        {},
                        boundary)
                        .get(),
                      expected);
                EXPECT_EQ(input.bytes_consumed().value(), 1U);
                EXPECT_EQ(input.checkpoint_depth(), 1U);
            }
        }
    }
}

TEST(
  SparseIndexFormatTest, RootReferencesRejectInvalidTopologyBeforePublication) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const std::array entries{entry()};
    const auto page = page_wire(entries);
    const std::array refs{page_reference(page), page_reference(page, 1, 1, 1)};
    const auto wire = root_wire(refs);
    struct change {
        std::size_t offset;
        std::uint64_t value;
        errc error;
    };
    for (const auto test : std::array{
           change{156, 0, errc::malformed_data},
           change{156, 4, errc::malformed_data},
           change{156, 65537, errc::resource_exhausted},
           change{160, 257, errc::resource_exhausted},
           change{168, 1, errc::malformed_data},
           change{172, 1, errc::malformed_data},
           change{176, 0, errc::malformed_data},
           change{180, 511, errc::malformed_data},
           change{168 + 48, 0, errc::malformed_data},
           change{172 + 48, 0, errc::malformed_data}}) {
        SCOPED_TRACE(test.offset);
        auto bad = wire;
        put(bad, 32 + test.offset, test.value, 4);
        repair(bad);
        fragmented_buffer_parser input{buffer("p" + bad)};
        input.skip(byte_count{1}).value();
        input.push_checkpoint().value();
        error(read_root(input, bad, target(), work), test.error);
        EXPECT_EQ(input.bytes_consumed().value(), 1U);
        EXPECT_EQ(input.checkpoint_depth(), 1U);
    }
    error(
      encode_sparse_index_root(
        target(), 0, {}, work, budget().operation_remaining, charge)
        .get(),
      errc::malformed_data);
    error(
      encode_sparse_index_root(
        target(), 3, refs, work, budget().operation_remaining, charge)
        .get(),
      errc::invalid_argument);
}

TEST(
  SparseIndexFormatTest,
  PinnedDigestsExactLengthsAndIndependentContextAreChecked) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const std::array entries{entry()};
    const auto page = page_wire(entries);
    const std::array refs{page_reference(page)};
    const auto wire = root_wire(refs);
    const auto root = pin(wire, target(), work);
    auto changed = page;
    put(changed, 32 + 172, 101, 8);
    repair(changed);
    fragmented_buffer_parser altered{buffer(changed)};
    error(
      decode_sparse_index_page(
        altered,
        root,
        page_ordinal::make(0).value(),
        page_memory(altered, root, work),
        work)
        .get(),
      errc::corrupt_data);
    EXPECT_EQ(altered.bytes_consumed().value(), 0U);
    auto wrong_digest = sha(wire);
    wrong_digest[0] ^= 1U;
    fragmented_buffer_parser input{buffer(wire)};
    error(
      decode_sparse_index_root(
        input,
        target(),
        codec::immutable_object_digest{wrong_digest},
        reserve(input, work),
        work)
        .get(),
      errc::corrupt_data);
    EXPECT_EQ(input.bytes_consumed().value(), 0U);
    const std::array larger{
      page_ref::make(
        page_ordinal::make(0).value(), 0, 1, byte_count{1024}, refs[0].digest())
        .value()};
    const auto larger_root = pin(root_wire(larger), target(), work);
    fragmented_buffer_parser wrong_size{buffer(page)};
    error(
      decode_sparse_index_page(
        wrong_size,
        larger_root,
        page_ordinal::make(0).value(),
        page_memory(wrong_size, larger_root, work),
        work)
        .get(),
      errc::malformed_data);
    for (const std::size_t offset :
         {4U, 20U, 36U, 52U, 68U, 76U, 92U, 108U, 124U}) {
        auto bad = wire;
        if (offset == 68)
            put(bad, 32 + offset, 2, 8);
        else
            bad[32 + offset] ^= 1;
        repair(bad);
        fragmented_buffer_parser wrong{buffer(bad)};
        error(read_root(wrong, bad, target(), work), errc::wrong_context);
        EXPECT_EQ(wrong.bytes_consumed().value(), 0U);
    }
    for (const std::size_t offset : {4U, 76U, 124U, 156U, 160U, 164U}) {
        auto bad = page;
        bad[32 + offset] ^= 1;
        repair(bad);
        fragmented_buffer_parser wrong{buffer(bad)};
        error(
          decode_sparse_index_page(
            wrong,
            root,
            page_ordinal::make(0).value(),
            page_memory(wrong, root, work),
            work)
            .get(),
          errc::wrong_context);
    }
}

TEST(SparseIndexFormatTest, FamilySubkindReservedBytesAndPaddingReject) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const std::array entries{entry()};
    const auto page = page_wire(entries);
    const std::array refs{page_reference(page)};
    const auto wire = root_wire(refs);
    struct change {
        std::size_t offset;
        std::uint64_t value;
        errc error;
    };
    for (const bool is_root : {false, true}) {
        for (const auto test : std::array{
               change{4, 7, errc::wrong_context},
               change{4, 9, errc::wrong_context},
               change{32, 0, errc::malformed_data},
               change{32, 3, errc::unsupported_format},
               change{32, is_root ? 2U : 1U, errc::wrong_context},
               change{34, 1, errc::malformed_data},
               change{510, 1, errc::malformed_data}}) {
            SCOPED_TRACE(::testing::Message() << is_root << ':' << test.offset);
            auto bad = is_root ? wire : page;
            put(bad, test.offset, test.value, 2);
            repair(bad);
            if (is_root) {
                fragmented_buffer_parser input{buffer(bad)};
                error(read_root(input, bad, target(), work), test.error);
                EXPECT_EQ(input.bytes_consumed().value(), 0U);
            } else {
                const std::array bad_refs{page_reference(bad)};
                const auto pinned = pin(root_wire(bad_refs), target(), work);
                fragmented_buffer_parser input{buffer(bad)};
                error(
                  decode_sparse_index_page(
                    input,
                    pinned,
                    page_ordinal::make(0).value(),
                    page_memory(input, pinned, work),
                    work)
                    .get(),
                  test.error);
                EXPECT_EQ(input.bytes_consumed().value(), 0U);
            }
        }
    }
}

TEST(SparseIndexFormatTest, SequentialPagesCheckBothColumnEdgesAndCompletion) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto context = target(8);
    const std::array a{entry(100, 512), entry(102, 1536)};
    const std::array b{entry(104, 2560), entry(106, 3584)};
    const auto first = page_wire(a, context);
    const auto second = page_wire(b, context, 1, 2);
    const std::array refs{
      page_reference(first, 2), page_reference(second, 2, 1, 2)};
    const auto root = pin(root_wire(refs, context), context, work);
    sparse_index_verifier verifier{root, work.policy()};
    for (const auto& wire : {first, second}) {
        fragmented_buffer_parser input{buffer("p" + wire + "suffix")};
        input.skip(byte_count{1}).value();
        input.push_checkpoint().value();
        const auto page
          = verifier.next(input, page_memory(input, root, work), work).get();
        ASSERT_TRUE(page.has_value());
        EXPECT_EQ(page->value.entries().size(), 2U);
        EXPECT_EQ(input.bytes_consumed().value(), 513U);
        EXPECT_EQ(input.checkpoint_depth(), 1U);
    }
    const auto complete = verifier.finish(work);
    ASSERT_TRUE(complete.has_value());
    EXPECT_EQ(complete->entry_count(), 4U);
    EXPECT_EQ(complete->root_digest(), root.digest());
    error(verifier.finish(work), errc::closed);
    sparse_index_verifier missing{root, work.policy()};
    fragmented_buffer_parser one{buffer(first)};
    ASSERT_TRUE(missing.next(one, page_memory(one, root, work), work).get());
    error(missing.finish(work), errc::malformed_data);
    EXPECT_TRUE(missing.closed());
    for (const auto& invalid :
         {std::array{entry(101, 2560), entry(106, 3584)},
          std::array{entry(104, 1024), entry(106, 3584)}}) {
        const auto bad_second = page_wire(invalid, context, 1, 2);
        const std::array bad_refs{
          page_reference(first, 2), page_reference(bad_second, 2, 1, 2)};
        const auto bad_root = pin(root_wire(bad_refs, context), context, work);
        // Each page is valid alone; the edge is the rejected condition.
        fragmented_buffer_parser standalone{buffer(bad_second)};
        ASSERT_TRUE(decode_sparse_index_page(
                      standalone,
                      bad_root,
                      page_ordinal::make(1).value(),
                      page_memory(standalone, bad_root, work),
                      work)
                      .get());
        sparse_index_verifier walk{bad_root, work.policy()};
        fragmented_buffer_parser earlier{buffer(first)};
        ASSERT_TRUE(
          walk.next(earlier, page_memory(earlier, bad_root, work), work).get());
        fragmented_buffer_parser later{buffer("p" + bad_second)};
        later.skip(byte_count{1}).value();
        later.push_checkpoint().value();
        error(
          walk.next(later, page_memory(later, bad_root, work), work).get(),
          errc::malformed_data);
        EXPECT_TRUE(walk.closed());
        EXPECT_EQ(later.bytes_consumed().value(), 1U);
        EXPECT_EQ(later.checkpoint_depth(), 1U);
    }
    sparse_index_verifier repeated{root, work.policy()};
    fragmented_buffer_parser earlier{buffer(first)}, again{buffer(first)};
    ASSERT_TRUE(
      repeated.next(earlier, page_memory(earlier, root, work), work).get());
    error(
      repeated.next(again, page_memory(again, root, work), work).get(),
      errc::wrong_context);
    EXPECT_TRUE(repeated.closed());
    EXPECT_EQ(again.bytes_consumed().value(), 0U);
}

TEST(SparseIndexFormatTest, PageCapacityAndServedMetadataUseActualHeaderSize) {
    for (const std::size_t h : {32U, 4096U}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        const auto cap = sparse_index_page_capacity(
                           byte_count{h}, alignment(), work.policy())
                           .value();
        EXPECT_EQ(cap, h == 32 ? 4083U : 3829U);
        const auto context = target(cap);
        std::vector<sparse_index_entry> entries;
        entries.reserve(cap);
        for (std::uint32_t i = 0; i < cap; ++i)
            entries.push_back(entry(100U + i, 512U * (std::uint64_t{i} + 1U)));
        const auto wire = page_wire(entries, context, 0, 0, h);
        EXPECT_LE(wire.size(), 65536U);
        const std::array refs{page_reference(wire, cap)};
        const auto root = pin(root_wire(refs, context, h), context, work);
        fragmented_buffer_parser input{buffer(wire, 1024)};
        auto memory = page_memory(input, root, work);
        const auto fixture = charge(
          byte_count{entries.capacity() * sizeof(sparse_index_entry)});
        memory.operation_remaining
          = memory.operation_remaining.checked_sub(fixture).value();
        memory.metadata_remaining
          = memory.metadata_remaining.checked_sub(fixture).value();
        const auto page
          = decode_sparse_index_page(
              input, root, page_ordinal::make(0).value(), memory, work)
              .get();
        ASSERT_TRUE(page.has_value());
        EXPECT_TRUE(std::ranges::equal(page->value.entries(), entries));
        const auto retained = charge(
          byte_count{
            page->value.entry_capacity() * sizeof(sparse_index_entry)});
        EXPECT_LE(retained.value(), 131072U);
        EXPECT_LE(
          malloc_usable_size(
            const_cast<sparse_index_entry*>(page->value.entries().data())),
          retained.value());
        EXPECT_EQ(
          memory.metadata_remaining.value()
            - page->remaining.metadata_remaining.value(),
          retained.value());
        EXPECT_EQ(
          memory.operation_remaining.value()
            - page->remaining.operation_remaining.value(),
          retained.value());
    }
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    std::vector<sparse_index_entry> too_many(4084, entry());
    error(
      encode_sparse_index_page(
        too_many,
        target(4084),
        page_ordinal::make(0).value(),
        0,
        work,
        budget().operation_remaining,
        charge)
        .get(),
      errc::resource_exhausted);
}

TEST(SparseIndexFormatTest, PageOrdinalAndFirstEntryMustAdmitAPossiblePrefix) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    struct position {
        std::uint32_t ordinal, first;
    };
    const auto context = target(65536);
    for (const auto& at :
         {position{0, 1}, position{1, 0}, position{2, 1}, position{255, 254}}) {
        const std::array entries{
          entry(100U + at.first, 512U * (std::uint64_t{at.first} + 1U))};
        error(
          encode_sparse_index_page(
            entries,
            context,
            page_ordinal::make(at.ordinal).value(),
            at.first,
            work,
            budget().operation_remaining,
            charge)
            .get(),
          errc::invalid_argument);
    }
    for (const auto& at :
         {position{0, 0}, position{1, 2}, position{255, 65535}}) {
        const std::array entries{
          entry(100U + at.first, 512U * (std::uint64_t{at.first} + 1U))};
        const auto encoded = encode_sparse_index_page(
                               entries,
                               context,
                               page_ordinal::make(at.ordinal).value(),
                               at.first,
                               work,
                               budget().operation_remaining,
                               charge)
                               .get();
        ASSERT_TRUE(encoded.has_value());
        EXPECT_EQ(encoded->reference.ordinal().value(), at.ordinal);
        EXPECT_EQ(encoded->reference.first_entry(), at.first);
        EXPECT_EQ(
          flat(encoded->bytes),
          page_wire(entries, context, at.ordinal, at.first));
    }
}

TEST(SparseIndexFormatTest, RootFitsMaximumPageCountAndEmptyPageIsRejected) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto context = target(256);
    std::vector<page_ref> refs;
    refs.reserve(256);
    for (std::uint32_t i = 0; i < 256; ++i)
        refs.push_back(
          page_ref::make(
            page_ordinal::make(i).value(),
            i,
            1,
            byte_count{512},
            codec::immutable_object_digest{sha("page")})
            .value());
    const auto encoded
      = encode_sparse_index_root(
          context, 256, refs, work, budget().operation_remaining, charge)
          .get();
    ASSERT_TRUE(encoded.has_value());
    const auto root = pin(root_wire(refs, context), context, work);
    EXPECT_EQ(root.pages().size(), 256U);
    EXPECT_EQ(root.entry_count(), 256U);
    EXPECT_LE(root.encoded_bytes().value(), 65536U);
    error(
      encode_sparse_index_page(
        {},
        context,
        page_ordinal::make(0).value(),
        0,
        work,
        budget().operation_remaining,
        charge)
        .get(),
      errc::invalid_argument);
    auto config = work.policy().config();
    config.max_object_pages = item_count{255};
    codec::cooperative_work limited{codec::limits::make(config).value(), abort};
    sparse_index_verifier walk{root, limited.policy()};
    error(walk.finish(limited), errc::resource_exhausted);
    EXPECT_TRUE(walk.closed());
}

TEST(SparseIndexFormatTest, LimitsAndParserMarksRejectWithoutAdvancing) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const std::array entries{entry()};
    const auto page = page_wire(entries);
    const std::array refs{page_reference(page)};
    const auto wire = root_wire(refs);
    const auto root = pin(wire, target(), work);
    for (const bool metadata : {false, true}) {
        fragmented_buffer_parser input{buffer(page)};
        auto memory = page_memory(input, root, work);
        if (metadata)
            memory.metadata_remaining = {};
        else
            memory.operation_remaining = {};
        error(
          decode_sparse_index_page(
            input, root, page_ordinal::make(0).value(), memory, work)
            .get(),
          errc::resource_exhausted);
        EXPECT_EQ(input.bytes_consumed().value(), 0U);
    }
    for (const std::size_t depth : {6U, 7U, 8U}) {
        fragmented_buffer_parser input{buffer(page)};
        for (std::size_t i = 0; i < depth; ++i)
            input.push_checkpoint().value();
        sparse_index_verifier walk{root, work.policy()};
        error(
          walk.next(input, page_memory(input, root, work), work).get(),
          errc::resource_exhausted);
        EXPECT_EQ(input.checkpoint_depth(), depth);
        EXPECT_EQ(input.bytes_consumed().value(), 0U);
        EXPECT_TRUE(walk.closed());
    }
    auto config = work.policy().config();
    config.max_page_bytes = byte_count{511};
    codec::cooperative_work narrow{codec::limits::make(config).value(), abort};
    fragmented_buffer_parser input{buffer(page)};
    error(
      decode_sparse_index_page(
        input,
        root,
        page_ordinal::make(0).value(),
        page_memory(input, root, narrow),
        narrow)
        .get(),
      errc::resource_exhausted);
    sparse_index_verifier mismatched{root, work.policy()};
    error(mismatched.finish(narrow), errc::invalid_argument);
    EXPECT_TRUE(mismatched.closed());
}

TEST(SparseIndexFormatTest, RealSuspensionCancellationDrainsAndRestoresCursor) {
    seastar::abort_source setup_abort;
    codec::cooperative_work setup{codec::limits::defaults(), setup_abort};
    const std::array entries{entry(100, 512), entry(102, 1536)};
    const auto page = page_wire(entries);
    const std::array refs{page_reference(page, 2)};
    const auto root = pin(root_wire(refs), target(), setup);
    for (const unsigned operation : {0U, 1U, 2U}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        fragmented_buffer_parser input{buffer("p" + page)};
        input.skip(byte_count{1}).value();
        input.push_checkpoint().value();
        const auto memory = page_memory(input, root, work);
        sparse_index_verifier walk{root, work.policy()};
        const auto deadline = std::chrono::steady_clock::now()
                              + std::chrono::seconds{2};
        while (!seastar::need_preempt()
               && std::chrono::steady_clock::now() < deadline) {
        }
        ASSERT_TRUE(seastar::need_preempt());
        if (operation == 0) {
            auto pending = walk.next(input, memory, work);
            const bool suspended = !pending.available();
            abort.request_abort();
            error(pending.get(), errc::aborted);
            EXPECT_TRUE(suspended);
            EXPECT_TRUE(walk.closed());
        } else if (operation == 1) {
            auto pending = encode_sparse_index_page(
              entries,
              target(),
              page_ordinal::make(0).value(),
              0,
              work,
              budget().operation_remaining,
              charge);
            const bool suspended = !pending.available();
            abort.request_abort();
            error(pending.get(), errc::aborted);
            EXPECT_TRUE(suspended);
        } else {
            auto pending = encode_sparse_index_root(
              target(), 2, refs, work, budget().operation_remaining, charge);
            const bool suspended = !pending.available();
            abort.request_abort();
            error(pending.get(), errc::aborted);
            EXPECT_TRUE(suspended);
        }
        EXPECT_EQ(input.bytes_consumed().value(), 1U);
        EXPECT_EQ(input.checkpoint_depth(), 1U);
    }
}

TEST(
  SparseIndexFormatTest, AllocationFailurePreservesInputsAndClosesEnteredWalk) {
#ifndef SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION
    GTEST_SKIP() << "allocation failure injection is unavailable";
#else
    seastar::abort_source setup_abort;
    codec::cooperative_work setup{codec::limits::defaults(), setup_abort};
    const std::array entries{entry(100, 512), entry(102, 1536)};
    const auto page = page_wire(entries);
    const std::array refs{page_reference(page, 2)};
    const auto wire = root_wire(refs);
    const auto context = target();
    const codec::immutable_object_digest digest{sha(wire)};
    const auto root = pin(wire, context, setup);
    for (const unsigned operation : {0U, 1U, 2U, 3U}) {
        std::size_t failures = 0;
        bool completed = false;
        for (std::uint64_t ordinal = 0; ordinal < 256 && !completed;
             ++ordinal) {
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            fragmented_buffer_parser input{
              buffer("p" + (operation == 1 ? wire : page), 7)};
            input.skip(byte_count{1}).value();
            input.push_checkpoint().value();
            const auto memory = page_memory(input, root, work);
            sparse_index_verifier walk{root, work.policy()};
            auto& injector = seastar::memory::local_failure_injector();
            bool succeeded = false, entered = false;
            injector.fail_after(ordinal);
            try {
                if (operation == 0) {
                    auto pending = walk.next(input, memory, work);
                    entered = true;
                    succeeded = pending.get().has_value();
                } else if (operation == 1) {
                    succeeded = decode_sparse_index_root(
                                  input, context, digest, memory, work)
                                  .get()
                                  .has_value();
                } else if (operation == 2) {
                    succeeded = encode_sparse_index_page(
                                  entries,
                                  context,
                                  page_ordinal::make(0).value(),
                                  0,
                                  work,
                                  budget().operation_remaining,
                                  charge)
                                  .get()
                                  .has_value();
                } else {
                    succeeded = encode_sparse_index_root(
                                  context,
                                  2,
                                  refs,
                                  work,
                                  budget().operation_remaining,
                                  charge)
                                  .get()
                                  .has_value();
                }
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
            const bool reached = injector.failed();
            injector.cancel();
            if (!succeeded) {
                EXPECT_TRUE(reached);
                ++failures;
                EXPECT_EQ(input.bytes_consumed().value(), 1U);
                if (operation == 0 && entered) EXPECT_TRUE(walk.closed());
            } else {
                if (operation == 0) EXPECT_TRUE(walk.finish(work));
                completed = !reached;
            }
            EXPECT_EQ(input.checkpoint_depth(), 1U);
        }
        EXPECT_TRUE(completed);
        EXPECT_GT(failures, 0U);
    }
#endif
}
} // namespace
} // namespace kwaque::storage
