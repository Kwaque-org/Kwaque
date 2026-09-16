#include "src/storage/tests/range_manifest_test_support.h"
#include "src/storage/tests/sparse_index_test_support.h"

#include <seastar/core/preempt.hh>
#include <seastar/util/alloc_failure_injector.hh>
#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

#include <chrono>
#include <concepts>
#include <type_traits>

namespace kwaque::storage {
namespace {
using namespace testing;
using namespace testing::manifest;
using bytes::fragmented_buffer_parser;
static_assert(!std::copy_constructible<range_manifest_root>);
static_assert(!std::copy_constructible<range_manifest_page>);
static_assert(!std::default_initializable<verified_range_manifest_pages>);
static_assert(!std::is_aggregate_v<verified_range_manifest_pages>);
static_assert(!std::is_constructible_v<
              range_manifest_verifier,
              range_manifest_root&&,
              codec::limits>);
static_assert(sizeof(range_manifest_verifier) < 2048);

template<typename T>
void error(const codec::result<T>& value, errc code) {
    ASSERT_FALSE(value.has_value());
    EXPECT_EQ(value.error().code(), code);
}
codec::result<decoded_range_manifest_root> read_root(
  fragmented_buffer_parser& input,
  std::string_view wire,
  codec::cooperative_work& work,
  range_manifest_root_header header = root_header(),
  storage_alignment a = alignment(),
  codec::field_context c = {},
  codec::input_boundary boundary = codec::input_boundary::open) {
    const auto memory = codec::reserve_decode_input(
      input, work.policy(), budget(), c, boundary);
    if (!memory) return codec::failure(memory.error());
    return decode_range_manifest_root(
             input,
             header.context(),
             header.logical_span(),
             a,
             codec::immutable_object_digest{manifest_sha(wire)},
             *memory,
             work,
             c,
             boundary)
      .get();
}

TEST(
  RangeManifestDecodeTest,
  IndependentFramesRoundtripAllHeaderAndAlignmentEndpoints) {
    for (const std::size_t h : {32U, 4096U}) {
        for (const std::uint64_t a : {512U, 65536U}) {
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            const auto entries = items();
            const auto page = expected_page(page_header(), entries, a, h);
            const std::array refs{manifest_page_reference(page, page_header())};
            const auto wire = expected_root(root_header(), refs, a, h);
            if (h == 4096) {
                const auto page_sha = a == 512
                                        ? "1964992b58213a7c142787e67b23313daf08"
                                          "b57f35c7ec9731e6b32f1677e859"
                                        : "5aea561dd1ab8b3d0d5fb2a0663945e845ba"
                                          "0457b054fbc1306e0e0a30920d8b";
                const auto root_sha = a == 512
                                        ? "02edfaac9cc818f1c157dea288fd9198a92d"
                                          "fd63ae384c79f3b825c027d103eb"
                                        : "f1970901b082fa2323e40fe970febfd87ac3"
                                          "2c1be9a1a02694904c5331bc122f";
                EXPECT_TRUE(
                  std::ranges::equal(
                    std::bit_cast<std::array<char, 32>>(manifest_sha(page)),
                    hex(page_sha)));
                EXPECT_TRUE(
                  std::ranges::equal(
                    std::bit_cast<std::array<char, 32>>(manifest_sha(wire)),
                    hex(root_sha)));
            }
            for (const std::size_t width : {67U, 1024U}) {
                fragmented_buffer_parser input{buffer("p" + wire + "s", width)};
                input.skip(byte_count{1}).value();
                input.push_checkpoint().value();
                const auto original = reserve(input, work);
                auto decoded = read_root(
                  input, wire, work, root_header(), alignment(a));
                ASSERT_TRUE(decoded.has_value());
                EXPECT_EQ(decoded->value.header(), root_header());
                EXPECT_EQ(decoded->value.alignment(), alignment(a));
                EXPECT_TRUE(std::ranges::equal(decoded->value.pages(), refs));
                EXPECT_EQ(input.bytes_consumed().value(), wire.size() + 1U);
                EXPECT_EQ(input.bytes_remaining().value(), 1U);
                EXPECT_EQ(input.checkpoint_depth(), 1U);
                const auto root_cost = charge(
                  byte_count{
                    decoded->value.page_capacity() * sizeof(page_ref)});
                EXPECT_EQ(
                  original.operation_remaining.value()
                    - decoded->remaining.operation_remaining.value(),
                  root_cost.value());
                EXPECT_EQ(
                  original.metadata_remaining.value()
                    - decoded->remaining.metadata_remaining.value(),
                  root_cost.value());
                range_manifest_verifier walk{decoded->value, work.policy()};
                fragmented_buffer_parser page_input{
                  buffer("p" + page + "s", width)};
                page_input.skip(byte_count{1}).value();
                page_input.push_checkpoint().value();
                const auto memory = page_memory(
                  page_input, decoded->value, work);
                const auto output = walk.next(page_input, memory, work).get();
                ASSERT_TRUE(output.has_value());
                EXPECT_EQ(output->value.header(), page_header());
                EXPECT_EQ(output->value.reference(), refs[0]);
                EXPECT_TRUE(
                  std::ranges::equal(output->value.entries(), entries));
                const auto retained = charge(
                  byte_count{
                    output->value.entry_capacity()
                    * sizeof(range_manifest_entry)});
                EXPECT_EQ(
                  memory.operation_remaining.value()
                    - output->remaining.operation_remaining.value(),
                  retained.value());
                EXPECT_EQ(
                  memory.metadata_remaining.value()
                    - output->remaining.metadata_remaining.value(),
                  retained.value());
                EXPECT_EQ(
                  page_input.bytes_consumed().value(), page.size() + 1U);
                EXPECT_EQ(page_input.bytes_remaining().value(), 1U);
                EXPECT_EQ(page_input.checkpoint_depth(), 1U);
                const auto proof = walk.finish(work);
                ASSERT_TRUE(proof.has_value());
                EXPECT_EQ(proof->root_digest(), decoded->value.digest());
                EXPECT_EQ(proof->entry_count(), 2U);
                error(walk.finish(work), errc::closed);
            }
        }
    }
}

TEST(
  RangeManifestDecodeTest, EmptyAndMaximumLogicalEndpointsStayRepresentable) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    for (const std::uint64_t end :
         {std::uint64_t{0}, std::uint64_t{100}, UINT64_MAX}) {
        const auto header = root_header(0, 0, mc(), end, end);
        const auto root = pin(expected_root(header, {}), header, work);
        range_manifest_verifier walk{root, work.policy()};
        EXPECT_TRUE(walk.finish(work));
        EXPECT_TRUE(root.pages().empty());
    }
    const auto context = mc(0x60, UINT64_MAX);
    const auto header = page_header(1, context, UINT64_MAX - 5U, UINT64_MAX);
    const std::array entries{
      range_manifest_entry::make(
        id<model::segment_id>(0x80),
        model::segment_generation::make(UINT64_MAX).value(),
        extent_scope(
          UINT64_MAX - 5U,
          UINT64_MAX,
          UINT64_MAX - 2U,
          UINT64_MAX,
          UINT64_MAX - 1024U,
          UINT64_MAX),
        codec::extent_digest{manifest_sha("wide")})
        .value()};
    const auto bytes = expected_page(header, entries);
    const std::array refs{manifest_page_reference(bytes, header)};
    const auto rh = root_header(1, 1, context, UINT64_MAX - 5U, UINT64_MAX);
    const auto wire = expected_root(rh, refs);
    const auto root = pin(wire, rh, work);
    fragmented_buffer_parser input{buffer(bytes, 7)};
    const auto page = decode_range_manifest_page(
                        input,
                        root,
                        page_ordinal::make(0).value(),
                        page_memory(input, root, work),
                        work)
                        .get();
    ASSERT_TRUE(page.has_value());
    EXPECT_EQ(page->value.entries().front(), entries[0]);
    const codec::field_context c{.origin = UINT64_MAX - wire.size()};
    fragmented_buffer_parser at_end{buffer(wire)};
    EXPECT_TRUE(read_root(at_end, wire, work, rh, alignment(), c));
    fragmented_buffer_parser overflow{buffer(wire)};
    auto over = c;
    ++over.origin;
    error(
      read_root(overflow, wire, work, rh, alignment(), over),
      errc::invalid_argument);
    EXPECT_EQ(overflow.bytes_consumed().value(), 0U);
}

TEST(RangeManifestDecodeTest, EveryTruncationRestoresPrefixAndCallerMarks) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto page = expected_page(page_header(), items());
    const std::array refs{manifest_page_reference(page, page_header())};
    const auto wire = expected_root(root_header(), refs);
    const auto root = pin(wire, root_header(), work);
    for (const bool is_root : {false, true}) {
        const auto& bytes = is_root ? wire : page;
        for (const auto boundary :
             {codec::input_boundary::open, codec::input_boundary::complete}) {
            for (std::size_t length = 0; length < bytes.size(); ++length) {
                fragmented_buffer_parser input{
                  buffer("p" + bytes.substr(0, length), 7)};
                input.skip(byte_count{1}).value();
                input.push_checkpoint().value();
                const auto expected = boundary == codec::input_boundary::open
                                        ? errc::truncated_data
                                        : errc::malformed_data;
                if (is_root)
                    error(
                      read_root(
                        input,
                        wire,
                        work,
                        root_header(),
                        alignment(),
                        {},
                        boundary),
                      expected);
                else
                    error(
                      decode_range_manifest_page(
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
  RangeManifestDecodeTest,
  ContextDigestAndExactPageLengthAreIndependentChecks) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto page = expected_page(page_header(), items());
    const std::array refs{manifest_page_reference(page, page_header())};
    const auto wire = expected_root(root_header(), refs);
    for (const bool is_root : {false, true}) {
        for (const std::size_t at : {4U, 20U, 36U, 52U}) {
            auto bytes = is_root ? wire : page;
            bytes[32 + at] ^= 1;
            repair(bytes);
            fragmented_buffer_parser input{buffer(bytes)};
            if (is_root)
                error(read_root(input, bytes, work), errc::wrong_context);
            else {
                const std::array changed{
                  manifest_page_reference(bytes, page_header())};
                const auto root = pin(
                  expected_root(root_header(), changed), root_header(), work);
                error(
                  decode_range_manifest_page(
                    input,
                    root,
                    page_ordinal::make(0).value(),
                    page_memory(input, root, work),
                    work)
                    .get(),
                  errc::wrong_context);
            }
            EXPECT_EQ(input.bytes_consumed().value(), 0U);
        }
    }
    fragmented_buffer_parser wrong_root{buffer(wire)};
    error(
      decode_range_manifest_root(
        wrong_root,
        mc(),
        logical(100, 105),
        alignment(),
        codec::immutable_object_digest{manifest_sha("different root")},
        reserve(wrong_root, work),
        work)
        .get(),
      errc::corrupt_data);
    fragmented_buffer_parser wrong_span{buffer(wire)};
    error(
      read_root(wrong_span, wire, work, root_header(2, 1, mc(), 99, 105)),
      errc::wrong_context);
    for (const bool wrong_length : {false, true}) {
        const std::array changed{
          page_ref::make(
            refs[0].ordinal(),
            0,
            2,
            byte_count{wrong_length ? 1024U : 512U},
            wrong_length
              ? refs[0].digest()
              : codec::immutable_object_digest{manifest_sha("different page")})
            .value()};
        const auto root = pin(
          expected_root(root_header(), changed), root_header(), work);
        fragmented_buffer_parser input{buffer(page)};
        error(
          decode_range_manifest_page(
            input,
            root,
            refs[0].ordinal(),
            page_memory(input, root, work),
            work)
            .get(),
          wrong_length ? errc::malformed_data : errc::corrupt_data);
        EXPECT_EQ(input.bytes_consumed().value(), 0U);
    }
}

TEST(
  RangeManifestDecodeTest, RepairedFramesRejectMalformedFixedFieldsAndEntries) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto page = expected_page(page_header(), items());
    const std::array refs{manifest_page_reference(page, page_header())};
    const auto wire = expected_root(root_header(), refs);
    struct mutation {
        std::size_t offset;
        std::size_t width;
        std::uint64_t value;
    };
    for (const bool is_root : {false, true}) {
        const std::array common{
          mutation{2, 2, 1}, mutation{52, 8, 0}, mutation{60, 8, 106}};
        for (const auto& change : common) {
            auto bytes = is_root ? wire : page;
            put(bytes, 32 + change.offset, change.value, change.width);
            repair(bytes);
            fragmented_buffer_parser input{buffer(bytes)};
            if (is_root)
                error(read_root(input, bytes, work), errc::malformed_data);
            else {
                const std::array changed{
                  manifest_page_reference(bytes, page_header())};
                const auto root = pin(
                  expected_root(root_header(), changed), root_header(), work);
                error(
                  decode_range_manifest_page(
                    input,
                    root,
                    refs[0].ordinal(),
                    page_memory(input, root, work),
                    work)
                    .get(),
                  errc::malformed_data);
            }
            EXPECT_EQ(input.bytes_consumed().value(), 0U);
        }
    }
    // Repaired CRC and independently repinned SHA cannot hide invalid geometry.
    const std::array bad_entries{
      mutation{92, 8, 99},
      mutation{92, 8, 102},
      mutation{92 + 8, 8, 100},
      mutation{92 + 32, 8, 0},
      mutation{92 + 48, 8, 3},
      mutation{92 + 64, 8, 512},
      mutation{92 + 104, 8, 101},
      mutation{92 + 104, 8, 103},
      mutation{92 + 104 + 8, 8, 106},
      mutation{92 + 104 + 48, 8, 9},
      mutation{92 + 104 + 72, 1, 0},
      mutation{88, 4, 0}};
    for (const auto& change : bad_entries) {
        auto bytes = page;
        put(bytes, 32 + change.offset, change.value, change.width);
        repair(bytes);
        const std::array changed{manifest_page_reference(bytes, page_header())};
        const auto root = pin(
          expected_root(root_header(), changed), root_header(), work);
        fragmented_buffer_parser input{buffer(bytes)};
        error(
          decode_range_manifest_page(
            input,
            root,
            refs[0].ordinal(),
            page_memory(input, root, work),
            work)
            .get(),
          errc::malformed_data);
        EXPECT_EQ(input.bytes_consumed().value(), 0U);
    }
}

TEST(
  RangeManifestDecodeTest,
  FamiliesSubkindsAndRootTopologyRejectBeforePublication) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto page = expected_page(page_header(), items());
    const std::array refs{manifest_page_reference(page, page_header())};
    const auto wire = expected_root(root_header(), refs);
    for (const bool is_root : {false, true}) {
        for (const bool family : {false, true}) {
            for (const std::uint16_t value :
                 {std::uint16_t{7}, std::uint16_t{8}}) {
                auto bytes = is_root ? wire : page;
                put(bytes, family ? 4U : 32U, value, 2);
                repair(bytes);
                fragmented_buffer_parser input{buffer(bytes)};
                if (is_root)
                    EXPECT_FALSE(read_root(input, bytes, work));
                else {
                    const std::array changed{
                      manifest_page_reference(bytes, page_header())};
                    const auto root = pin(
                      expected_root(root_header(), changed),
                      root_header(),
                      work);
                    EXPECT_FALSE(decode_range_manifest_page(
                                   input,
                                   root,
                                   refs[0].ordinal(),
                                   page_memory(input, root, work),
                                   work)
                                   .get());
                }
                EXPECT_EQ(input.bytes_consumed().value(), 0U);
            }
        }
    }
    struct mutation {
        std::size_t offset;
        std::uint64_t value;
    };
    for (const auto& change :
         {mutation{76, 0},
          mutation{76, 65537},
          mutation{80, 0},
          mutation{80, 257},
          mutation{88, 1},
          mutation{92, 1},
          mutation{96, 0},
          mutation{96, 65537},
          mutation{100, 511},
          mutation{84, 0}}) {
        auto bytes = wire;
        put(bytes, 32 + change.offset, change.value, 4);
        repair(bytes);
        fragmented_buffer_parser input{buffer(bytes)};
        EXPECT_FALSE(read_root(input, bytes, work));
        EXPECT_EQ(input.bytes_consumed().value(), 0U);
    }
}

TEST(
  RangeManifestDecodeTest,
  SequentialPartitionChecksFirstMiddleAndFinalCoverage) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    for (const unsigned mode : {0U, 1U, 2U, 3U, 4U, 5U}) {
        const auto first_begin = mode == 1 ? 101U : 100U;
        const auto second_begin = mode == 2 ? 101U : mode == 3 ? 103U : 102U;
        const auto end = mode == 4 ? 104U : 105U;
        const auto h0 = page_header(1, mc(), first_begin, 102, 0, 0);
        const auto h1 = page_header(
          1, mode == 5 ? mc(0x61, 4) : mc(), second_begin, end, 1, 1);
        const std::array e0{item(first_begin, 102)},
          e1{item(second_begin, end, true)};
        const std::array bytes{expected_page(h0, e0), expected_page(h1, e1)};
        const std::array refs{
          manifest_page_reference(bytes[0], h0),
          manifest_page_reference(bytes[1], h1)};
        const auto rh = root_header(2, 2);
        const auto root = pin(expected_root(rh, refs), rh, work);
        range_manifest_verifier walk{root, work.policy()};
        for (std::size_t i = 0; i < 2; ++i) {
            fragmented_buffer_parser input{buffer("p" + bytes[i] + "s")};
            input.skip(byte_count{1}).value();
            input.push_checkpoint().value();
            const auto out
              = walk.next(input, page_memory(input, root, work), work).get();
            const bool fails
              = (mode == 1 && i == 0)
                || ((mode == 2 || mode == 3 || mode == 5) && i == 1);
            if (fails) {
                error(
                  out, mode == 5 ? errc::wrong_context : errc::malformed_data);
                EXPECT_EQ(input.bytes_consumed().value(), 1U);
                EXPECT_TRUE(walk.closed());
                error(walk.finish(work), errc::closed);
                error(
                  walk.next(input, page_memory(input, root, work), work).get(),
                  errc::closed);
                break;
            }
            ASSERT_TRUE(out.has_value());
            EXPECT_EQ(input.bytes_consumed().value(), bytes[i].size() + 1U);
            EXPECT_EQ(input.checkpoint_depth(), 1U);
        }
        if (mode == 0) EXPECT_TRUE(walk.finish(work));
        if (mode == 4) error(walk.finish(work), errc::malformed_data);
    }
}

TEST(RangeManifestDecodeTest, MissingRepeatedAndSwappedPagesCannotComplete) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto h0 = page_header(1, mc(), 100, 102, 0, 0),
               h1 = page_header(1, mc(), 102, 105, 1, 1);
    const std::array e0{item()}, e1{item(102, 105, true)};
    const std::array bytes{expected_page(h0, e0), expected_page(h1, e1)};
    const std::array refs{
      manifest_page_reference(bytes[0], h0),
      manifest_page_reference(bytes[1], h1)};
    const auto rh = root_header(2, 2);
    const auto root = pin(expected_root(rh, refs), rh, work);
    for (const unsigned mode : {0U, 1U, 2U}) {
        range_manifest_verifier walk{root, work.policy()};
        fragmented_buffer_parser first{buffer(bytes[mode == 2 ? 1U : 0U])};
        const auto result
          = walk.next(first, page_memory(first, root, work), work).get();
        if (mode == 2) {
            EXPECT_FALSE(result);
            EXPECT_TRUE(walk.closed());
            continue;
        }
        ASSERT_TRUE(result.has_value());
        if (mode == 0)
            error(walk.finish(work), errc::malformed_data);
        else {
            fragmented_buffer_parser repeated{buffer(bytes[0])};
            EXPECT_FALSE(
              walk.next(repeated, page_memory(repeated, root, work), work)
                .get());
            EXPECT_EQ(repeated.bytes_consumed().value(), 0U);
            EXPECT_TRUE(walk.closed());
        }
    }
}

TEST(RangeManifestDecodeTest, LimitsMarksAndPreCanceledCallsLeaveInputIntact) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto page = expected_page(page_header(), items());
    const std::array refs{manifest_page_reference(page, page_header())};
    const auto wire = expected_root(root_header(), refs);
    const auto root = pin(wire, root_header(), work);
    for (const bool is_root : {false, true}) {
        for (const bool metadata : {false, true}) {
            fragmented_buffer_parser input{buffer(is_root ? wire : page)};
            auto memory = page_memory(input, root, work);
            if (metadata)
                memory.metadata_remaining = {};
            else
                memory.operation_remaining = {};
            if (is_root)
                error(
                  decode_range_manifest_root(
                    input,
                    mc(),
                    logical(100, 105),
                    alignment(),
                    root.digest(),
                    memory,
                    work)
                    .get(),
                  errc::resource_exhausted);
            else
                error(
                  decode_range_manifest_page(
                    input, root, refs[0].ordinal(), memory, work)
                    .get(),
                  errc::resource_exhausted);
            EXPECT_EQ(input.bytes_consumed().value(), 0U);
        }
    }
    for (const std::size_t depth : {6U, 7U, 8U}) {
        fragmented_buffer_parser input{buffer(page)};
        for (std::size_t i = 0; i < depth; ++i)
            input.push_checkpoint().value();
        range_manifest_verifier walk{root, work.policy()};
        error(
          walk.next(input, page_memory(input, root, work), work).get(),
          errc::resource_exhausted);
        EXPECT_EQ(input.checkpoint_depth(), depth);
        EXPECT_EQ(input.bytes_consumed().value(), 0U);
        EXPECT_TRUE(walk.closed());
    }
    auto config = work.policy().config();
    config.max_object_entries = item_count{1};
    codec::cooperative_work narrow{codec::limits::make(config).value(), abort};
    fragmented_buffer_parser input{buffer(wire)};
    error(read_root(input, wire, narrow), errc::resource_exhausted);
    range_manifest_verifier mismatched{root, work.policy()};
    error(mismatched.finish(narrow), errc::invalid_argument);
    abort.request_abort();
    range_manifest_verifier canceled{root, work.policy()};
    fragmented_buffer_parser canceled_input{buffer(page)};
    error(
      canceled
        .next(canceled_input, page_memory(canceled_input, root, work), work)
        .get(),
      errc::aborted);
    EXPECT_EQ(canceled_input.bytes_consumed().value(), 0U);
    EXPECT_TRUE(canceled.closed());
}

TEST(
  RangeManifestDecodeTest,
  SuspensionCancellationAndConcurrentEntryDoNotPublish) {
    seastar::abort_source setup_abort;
    codec::cooperative_work setup{codec::limits::defaults(), setup_abort};
    const auto page = expected_page(page_header(), items());
    const std::array refs{manifest_page_reference(page, page_header())};
    const auto wire = expected_root(root_header(), refs);
    const auto root = pin(wire, root_header(), setup);
    for (const bool is_root : {false, true}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        fragmented_buffer_parser input{
          buffer("p" + (is_root ? wire : page), 7)};
        input.skip(byte_count{1}).value();
        input.push_checkpoint().value();
        const auto memory = page_memory(input, root, work);
        range_manifest_verifier walk{root, work.policy()};
        const auto deadline = std::chrono::steady_clock::now()
                              + std::chrono::seconds{2};
        while (!seastar::need_preempt()
               && std::chrono::steady_clock::now() < deadline) {
        }
        ASSERT_TRUE(seastar::need_preempt());
        if (is_root) {
            auto pending = decode_range_manifest_root(
              input,
              mc(),
              logical(100, 105),
              alignment(),
              root.digest(),
              memory,
              work);
            const bool suspended = !pending.available();
            abort.request_abort();
            error(pending.get(), errc::aborted);
            EXPECT_TRUE(suspended);
        } else {
            auto pending = walk.next(input, memory, work);
            const bool suspended = !pending.available();
            // No second operation enters while the first owns its transaction.
            error(walk.finish(work), errc::closed);
            abort.request_abort();
            error(pending.get(), errc::aborted);
            EXPECT_TRUE(suspended);
            EXPECT_TRUE(walk.closed());
        }
        EXPECT_EQ(input.bytes_consumed().value(), 1U);
        EXPECT_EQ(input.checkpoint_depth(), 1U);
    }
}

TEST(
  RangeManifestDecodeTest, AllocationFailuresCloseEnteredWalkAndRestoreMarks) {
#ifndef SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION
    GTEST_SKIP() << "allocation failure injection is unavailable";
#else
    seastar::abort_source setup_abort;
    codec::cooperative_work setup{codec::limits::defaults(), setup_abort};
    const auto page = expected_page(page_header(), items());
    const std::array refs{manifest_page_reference(page, page_header())};
    const auto wire = expected_root(root_header(), refs);
    const auto root = pin(wire, root_header(), setup);
    for (const bool is_root : {false, true}) {
        std::size_t failures = 0;
        bool completed = false;
        for (std::uint64_t ordinal = 0; ordinal < 256 && !completed;
             ++ordinal) {
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            fragmented_buffer_parser input{
              buffer("p" + (is_root ? wire : page), 7)};
            input.skip(byte_count{1}).value();
            input.push_checkpoint().value();
            const auto memory = page_memory(input, root, work);
            range_manifest_verifier walk{root, work.policy()};
            auto& injector = seastar::memory::local_failure_injector();
            bool succeeded = false, entered = false;
            injector.fail_after(ordinal);
            try {
                if (is_root)
                    succeeded = decode_range_manifest_root(
                                  input,
                                  mc(),
                                  logical(100, 105),
                                  alignment(),
                                  root.digest(),
                                  memory,
                                  work)
                                  .get()
                                  .has_value();
                else {
                    auto pending = walk.next(input, memory, work);
                    entered = true;
                    succeeded = pending.get().has_value();
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
                if (!is_root && entered) EXPECT_TRUE(walk.closed());
            } else {
                if (!is_root) EXPECT_TRUE(walk.finish(work));
                completed = !reached;
            }
            EXPECT_EQ(input.checkpoint_depth(), 1U);
        }
        EXPECT_TRUE(completed);
        EXPECT_GT(failures, 0U);
    }
#endif
}
thread_local seastar::abort_source* metadata_abort = nullptr;
thread_local byte_count metadata_abort_request;
thread_local bool metadata_admitted = false;
byte_count cancel_metadata_charge(byte_count request) noexcept {
    const auto served = charge(request);
    if (metadata_abort != nullptr && request == metadata_abort_request) {
        metadata_admitted = true;
        metadata_abort->request_abort();
        metadata_abort = nullptr;
    }
    return served;
}
TEST(
  RangeManifestDecodeTest,
  CancellationAfterMetadataAdmissionDrainsTheStagedVector) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto bytes = expected_page(page_header(), items());
    const std::array refs{manifest_page_reference(bytes, page_header())};
    const auto root = pin(
      expected_root(root_header(), refs), root_header(), work);
    fragmented_buffer_parser input{buffer("p" + bytes, 67)};
    input.skip(byte_count{1}).value();
    input.push_checkpoint().value();
    auto memory = page_memory(input, root, work);
    memory.charge = cancel_metadata_charge;
    range_manifest_verifier walk{root, work.policy()};
    metadata_abort_request = byte_count{2U * sizeof(range_manifest_entry)};
    metadata_admitted = false;
    metadata_abort = &abort;
    // The scalar charge callback aborts at vector admission. Allocation and
    // joined cleanup still run; no partially decoded page escapes.
    const auto cleanup = seastar::defer([] { metadata_abort = nullptr; });
    const auto result = walk.next(input, memory, work).get();
    EXPECT_TRUE(metadata_admitted);
    error(result, errc::aborted);
    EXPECT_TRUE(walk.closed());
    EXPECT_EQ(input.bytes_consumed().value(), 1U);
    EXPECT_EQ(input.checkpoint_depth(), 1U);
}

// Fixture construction supplies exact stored bytes through the public extent
// verifier. Callers retain only the returned scalar proof, never a history.
verified_extent supplied_extent(unsigned kind, codec::cooperative_work& work) {
    if (kind == 0 || kind == 1) {
        const bool sparse = kind == 1;
        const std::uint8_t segment = sparse ? 0x80 : 0x30;
        const std::uint64_t generation = sparse ? 9U : 1U;
        const auto bytes
          = sparse ? data_block(101, 17, 4096, true, segment, generation)
                   : data_block(100, 0, 512, false, segment, generation);
        const auto coverage = sparse ? scope(101, 106, 17, 19, 4096, 4608)
                                     : scope(100, 101, 0, 1, 512, 1024);
        auto verifier = extent_verifier::make(
                          history(segment, generation),
                          coverage,
                          work.policy(),
                          sparse ? extent_layout_kind::rewrite
                                 : extent_layout_kind::initial_append,
                          {},
                          extent_integrity::crc32c_and_sha256)
                          .value();
        feed_block(verifier, bytes, work).value();
        return verifier.finish(work).value();
    }
    if (kind == 2) {
        auto verifier = extent_verifier::make(
                          history(0x90, 11),
                          scope(106, 109, 23, 23, 8192, 8192),
                          work.policy(),
                          extent_layout_kind::rewrite,
                          {},
                          extent_integrity::crc32c_and_sha256)
                          .value();
        return verifier.finish(work).value();
    }
    const auto previous = single_evidence(work);
    const auto footer = footer_wire(
      previous.boundary(), {history(), runtime::file_position{1024}});
    auto verifier = extent_verifier::make(
                      history(),
                      scope(109, 112, 1, 1, 1024, 1536),
                      work.policy(),
                      extent_layout_kind::rewrite,
                      {},
                      extent_integrity::crc32c_and_sha256)
                      .value();
    feed_footer(verifier, footer, work, previous).value();
    return verifier.finish(work).value();
}
range_manifest_entry extent_entry(const verified_extent& proof) {
    return range_manifest_entry::make(
             proof.context().segment.segment(),
             proof.context().segment.generation(),
             proof.boundary().coverage,
             *proof.digest())
      .value();
}

TEST(
  RangeManifestDecodeTest,
  DenseSparseEmptyAndFooterOnlyExtentsKeepTheirOriginalSpans) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    for (unsigned kind = 0; kind < 4; ++kind) {
        SCOPED_TRACE(kind);
        const auto evidence = supplied_extent(kind, work);
        const auto value = extent_entry(evidence);
        EXPECT_TRUE(validate_range_manifest_extent(
          mc(), id<model::cluster_id>(0x50), value, evidence));
        const auto context = sparse_index_context::make(
                               evidence.context().segment,
                               value.coverage(),
                               value.digest(),
                               evidence.context().alignment)
                               .value();
        std::vector<sparse_index_entry> anchors;
        if (kind < 2)
            anchors.push_back(
              testing::index::entry(
                value.coverage().logical().begin().value(),
                value.coverage().bytes().begin().value()));
        std::vector<page_ref> refs;
        if (!anchors.empty()) {
            const auto page = testing::index::page_wire(anchors, context);
            refs.push_back(testing::index::page_reference(page));
        }
        const auto root = testing::index::pin(
          testing::index::root_wire(refs, context), context, work);
        EXPECT_TRUE(validate_sparse_index_extent(root, evidence));
        sparse_index_verifier walk{root, work.policy()};
        if (!anchors.empty()) {
            const auto page = testing::index::page_wire(anchors, context);
            fragmented_buffer_parser input{buffer(page)};
            const auto decoded
              = walk
                  .next(
                    input, testing::index::page_memory(input, root, work), work)
                  .get();
            ASSERT_TRUE(decoded.has_value());
            EXPECT_EQ(decoded->value.entries().front(), anchors.front());
            const bool sparse = kind == 1;
            const auto block = sparse ? data_block(101, 17, 4096, true, 0x80, 9)
                                      : data_block(100, 0, 512, false, 0x30, 1);
            fragmented_buffer_parser data{buffer(block)};
            const auto at = sparse ? block_expected(0x80, 9, 4096, 17)
                                   : block_expected(0x30, 1);
            const auto supplied
              = decode_segment_block(data, at, reserve(data, work), work).get();
            ASSERT_TRUE(supplied.has_value());
            EXPECT_TRUE(validate_sparse_index_anchor(
              context, anchors.front(), supplied->value.descriptor()));
            if (sparse) {
                EXPECT_EQ(
                  supplied->value.descriptor().batch().context.logical_span(),
                  logical(101, 106));
                EXPECT_EQ(
                  supplied->value.descriptor()
                    .coverage()
                    .physical()
                    .count()
                    .value(),
                  2U);
                // The first survivor is original base + 1, not the index key.
                error(
                  validate_sparse_index_anchor(
                    context,
                    testing::index::entry(102, 4096),
                    supplied->value.descriptor()),
                  errc::malformed_data);
                error(
                  validate_sparse_index_anchor(
                    context,
                    testing::index::entry(17, 4096),
                    supplied->value.descriptor()),
                  errc::malformed_data);
                error(
                  validate_sparse_index_anchor(
                    context,
                    testing::index::entry(101, 4608),
                    supplied->value.descriptor()),
                  errc::malformed_data);
            }
        } else {
            EXPECT_TRUE(value.coverage().physical().empty());
            EXPECT_FALSE(value.coverage().logical().empty());
            EXPECT_EQ(value.coverage().bytes().empty(), kind == 2);
        }
        EXPECT_TRUE(walk.finish(work));
    }
}

TEST(
  RangeManifestDecodeTest,
  ReplacementManifestReusesIndependentSegmentGenerations) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    std::vector<range_manifest_entry> entries;
    entries.reserve(4);
    for (unsigned kind = 0; kind < 4; ++kind)
        entries.push_back(extent_entry(supplied_extent(kind, work)));
    EXPECT_NE(entries[0].generation(), entries[1].generation());
    for (const auto& context : {mc(), mc(0x61, 4)}) {
        const auto ph = page_header(4, context, 100, 112);
        const auto page = expected_page(ph, entries, 65536);
        const std::array refs{manifest_page_reference(page, ph)};
        const auto rh = root_header(4, 1, context, 100, 112);
        const auto root = pin(
          expected_root(rh, refs, 65536), rh, work, alignment(65536));
        range_manifest_verifier walk{root, work.policy()};
        fragmented_buffer_parser input{buffer(page, 1024)};
        const auto decoded
          = walk.next(input, page_memory(input, root, work), work).get();
        ASSERT_TRUE(decoded.has_value());
        ASSERT_EQ(decoded->value.entries().size(), 4U);
        for (unsigned kind = 0; kind < 4; ++kind) {
            const auto proof = supplied_extent(kind, work);
            EXPECT_EQ(decoded->value.entries()[kind], entries[kind]);
            EXPECT_TRUE(validate_range_manifest_extent(
              context,
              id<model::cluster_id>(0x50),
              decoded->value.entries()[kind],
              proof));
        }
        EXPECT_TRUE(walk.finish(work));
    }
}

TEST(
  RangeManifestDecodeTest,
  RepinnedMetadataDoesNotProveReferencedDataAndEveryExtentFieldMatters) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto evidence = supplied_extent(0, work);
    const auto value = extent_entry(evidence);
    auto digest = value.digest().bytes();
    digest[0] ^= 1U;
    const auto changed = range_manifest_entry::make(
                           value.segment(),
                           value.generation(),
                           value.coverage(),
                           codec::extent_digest{digest})
                           .value();
    const std::array entries{changed};
    const auto ph = page_header(1, mc(), 100, 101);
    const auto page = expected_page(ph, entries);
    const std::array refs{manifest_page_reference(page, ph)};
    const auto rh = root_header(1, 1, mc(), 100, 101);
    const auto root = pin(expected_root(rh, refs), rh, work);
    range_manifest_verifier walk{root, work.policy()};
    fragmented_buffer_parser input{buffer(page)};
    const auto decoded
      = walk.next(input, page_memory(input, root, work), work).get();
    ASSERT_TRUE(decoded.has_value());
    EXPECT_TRUE(walk.finish(work));
    error(
      validate_range_manifest_extent(
        mc(),
        id<model::cluster_id>(0x50),
        decoded->value.entries()[0],
        evidence),
      errc::corrupt_data);
    error(
      validate_range_manifest_extent(
        mc(), id<model::cluster_id>(0x51), value, evidence),
      errc::wrong_context);
    error(
      validate_range_manifest_extent(mc(), {}, value, evidence),
      errc::invalid_argument);
    for (const bool topic : {false, true}) {
        const auto context = manifest_context::make(
                               topic ? id<model::topic_id>(0x11) : mc().topic(),
                               topic ? mc().range() : id<model::range_id>(0x21),
                               mc().manifest(),
                               mc().generation())
                               .value();
        error(
          validate_range_manifest_extent(
            context, id<model::cluster_id>(0x50), value, evidence),
          errc::wrong_context);
    }
    for (const bool generation : {false, true}) {
        const auto entry
          = range_manifest_entry::make(
              generation ? value.segment() : id<model::segment_id>(0x31),
              model::segment_generation::make(generation ? 8U : 1U).value(),
              value.coverage(),
              value.digest())
              .value();
        error(
          validate_range_manifest_extent(
            mc(), id<model::cluster_id>(0x50), entry, evidence),
          errc::wrong_context);
    }
    for (const auto& coverage :
         {scope(99, 101, 0, 1, 512, 1024),
          scope(100, 102, 0, 1, 512, 1024),
          scope(100, 101, 1, 2, 512, 1024),
          scope(100, 101, 0, 1, 0, 1024),
          scope(100, 101, 0, 1, 512, 1536)}) {
        const auto entry
          = range_manifest_entry::make(
              value.segment(), value.generation(), coverage, value.digest())
              .value();
        error(
          validate_range_manifest_extent(
            mc(), id<model::cluster_id>(0x50), entry, evidence),
          errc::malformed_data);
    }
    auto crc_only = extent_verifier::make(
                      history(0x30, 1), value.coverage(), work.policy())
                      .value();
    ASSERT_TRUE(
      feed_block(crc_only, data_block(100, 0, 512, false, 0x30, 1), work));
    error(
      validate_range_manifest_extent(
        mc(),
        id<model::cluster_id>(0x50),
        value,
        crc_only.finish(work).value()),
      errc::invalid_argument);
    // A valid alternative encoding at identical coordinates has different
    // exact bytes. Metadata/coordinate equality cannot authenticate that swap.
    auto replacement = extent_verifier::make(
                         history(0x30, 1),
                         value.coverage(),
                         work.policy(),
                         extent_layout_kind::initial_append,
                         {},
                         extent_integrity::crc32c_and_sha256)
                         .value();
    ASSERT_TRUE(feed_block(
      replacement, data_block(100, 0, 512, false, 0x30, 1, true), work));
    error(
      validate_range_manifest_extent(
        mc(),
        id<model::cluster_id>(0x50),
        value,
        replacement.finish(work).value()),
      errc::corrupt_data);
}
} // namespace
} // namespace kwaque::storage
