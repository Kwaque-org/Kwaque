#include "src/storage/tests/retry_test_support.h"

#include <seastar/util/alloc_failure_injector.hh>

#include <gtest/gtest.h>

#include <concepts>
#include <malloc.h>
#include <type_traits>

namespace kwaque::storage {
namespace {
using namespace testing;
using bytes::fragmented_buffer_parser;
static_assert(!std::default_initializable<completed_retry>);
static_assert(!std::is_aggregate_v<completed_retry>);
static_assert(!std::same_as<page_ordinal, page_count>);
static_assert(!std::is_copy_constructible_v<retry_page>);
static_assert(!std::is_aggregate_v<verified_retry_summary>);
static_assert(sizeof(retry_summary_verifier) < 2048);
static_assert(408U * sizeof(completed_retry) <= 131072);
static_assert(256U * sizeof(page_ref) <= 131072);

TEST(RetryFormatTest, CompletedResultKeepsEveryOriginalField) {
    const auto original = retry();
    auto bytes = original.submitted_digest().bytes();
    bytes[0] ^= 1U;
    const auto different = completed_retry::make(
                             original.id(),
                             codec::semantic_batch_digest{bytes},
                             original.original_binding(),
                             original.returned_span(),
                             original.ack_generation())
                             .value();
    EXPECT_EQ(original.id(), different.id());
    EXPECT_NE(original, different);
    for (const auto& altered :
         {retry(1), retry(0, 2), retry(0, 1, 2), retry(0, 1, 1, 0x41)}) {
        EXPECT_NE(original.id(), altered.id());
        EXPECT_TRUE(original.id().canonical_less(altered.id()));
    }
    const auto binding = original.original_binding();
    EXPECT_FALSE(
      completed_retry::make(
        original.id(),
        original.submitted_digest(),
        binding,
        model::range_logical_span::make(
          model::range_logical_end{100}, model::range_logical_end{100})
          .value(),
        original.ack_generation()));
    EXPECT_FALSE(
      completed_retry::make(
        original.id(),
        original.submitted_digest(),
        binding,
        original.returned_span(),
        model::segment_generation::make(2).value()));
    EXPECT_FALSE(
      completed_retry::make(
        original.id(),
        original.submitted_digest(),
        binding,
        model::range_logical_span::make(
          model::range_logical_end{0}, model::range_logical_end{4097})
          .value(),
        original.ack_generation()));
    EXPECT_TRUE(
      completed_retry::make(
        original.id(),
        original.submitted_digest(),
        binding,
        model::range_logical_span::make(
          model::range_logical_end{UINT64_MAX - 4096},
          model::range_logical_end{UINT64_MAX})
          .value(),
        original.ack_generation()));
}

TEST(RetryFormatTest, IndependentGoldenPreservesExactCompletedResult) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const std::array entries{retry()};
    const auto wire = retry_page_wire(entries);
    EXPECT_EQ(
      wire.substr(0, 32),
      hex("4b5142460700010001002000e00100000000000000000000a16841c2a71b070b"));
    const auto encoded = encode_retry_page(
                           entries,
                           root_location(),
                           page_ordinal::make(0).value(),
                           0,
                           work,
                           budget().operation_remaining,
                           charge)
                           .get();
    ASSERT_TRUE(encoded.has_value());
    EXPECT_EQ(flat(encoded->bytes), wire);
    EXPECT_EQ(encoded->reference, reference(wire));
    const std::array refs{reference(wire)};
    const auto root = pin_root(one_root(refs), work);
    for (const std::size_t width : {1U, 7U, 67U, 512U}) {
        fragmented_buffer_parser input{buffer("p" + wire + "suffix", width)};
        input.skip(byte_count{1}).value();
        input.push_checkpoint().value();
        const auto decoded = decode_retry_page(
                               input,
                               root,
                               page_ordinal::make(0).value(),
                               reserve_page(input, root, work),
                               work)
                               .get();
        ASSERT_TRUE(decoded.has_value());
        ASSERT_EQ(decoded->value.entries().size(), 1U);
        const auto got = decoded->value.entries().front();
        EXPECT_EQ(got.id().producer(), entries[0].id().producer());
        EXPECT_EQ(got.id().epoch(), entries[0].id().epoch());
        EXPECT_EQ(got.id().stream(), entries[0].id().stream());
        EXPECT_EQ(got.id().sequence(), entries[0].id().sequence());
        EXPECT_EQ(got.submitted_digest(), entries[0].submitted_digest());
        EXPECT_EQ(got.original_binding(), entries[0].original_binding());
        EXPECT_EQ(got.returned_span(), entries[0].returned_span());
        EXPECT_EQ(got.ack_generation(), entries[0].ack_generation());
        EXPECT_EQ(input.bytes_consumed().value(), 513U);
        EXPECT_EQ(input.checkpoint_depth(), 1U);
        EXPECT_EQ(decoded->value.entry_capacity(), 1U);
    }
}

TEST(RetryFormatTest, WideDistinctFieldsKeepOriginalGenerationAfterRelocation) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto binding
      = model::producer_stream_binding::make(
          id<model::topic_id>(0x10),
          id<model::range_id>(0x20),
          model::range_routing_epoch::make(0x1122334455667788ULL).value(),
          id<model::segment_id>(0x91),
          model::segment_generation::make(0x2233445566778899ULL).value())
          .value();
    const auto bid
      = model::batch_id::make(
          id<model::producer_id>(0x82),
          model::producer_epoch::make(0x33445566778899aaULL).value(),
          model::producer_stream_id::make(0x445566778899aabbULL).value(),
          model::batch_sequence{UINT64_MAX})
          .value();
    const auto span = model::range_logical_span::make(
                        model::range_logical_end{UINT64_MAX - 7},
                        model::range_logical_end{UINT64_MAX})
                        .value();
    const std::array entries{
      completed_retry::make(
        bid, retry().submitted_digest(), binding, span, binding.generation())
        .value()};
    const auto location = root_location(0x80, 2);
    const auto wire = retry_page_wire(entries, location);
    const std::array refs{reference(wire)};
    const auto root = pin_root(one_root(refs, location), work, location);
    fragmented_buffer_parser input{buffer(wire, 7)};
    const auto decoded = decode_retry_page(
                           input,
                           root,
                           page_ordinal::make(0).value(),
                           reserve_page(input, root, work),
                           work)
                           .get();
    ASSERT_TRUE(decoded.has_value());
    const auto value = decoded->value.entries()[0];
    EXPECT_EQ(value.id().producer(), bid.producer());
    EXPECT_EQ(value.id().epoch().value(), 0x33445566778899aaULL);
    EXPECT_EQ(value.id().stream().value(), 0x445566778899aabbULL);
    EXPECT_EQ(value.id().sequence().value(), UINT64_MAX);
    EXPECT_EQ(value.original_binding().topic(), binding.topic());
    EXPECT_EQ(value.original_binding().range(), binding.range());
    EXPECT_EQ(
      value.original_binding().routing_epoch().value(), 0x1122334455667788ULL);
    EXPECT_EQ(value.original_binding().segment(), binding.segment());
    EXPECT_EQ(
      value.original_binding().generation().value(), 0x2233445566778899ULL);
    EXPECT_EQ(value.ack_generation(), binding.generation());
    EXPECT_EQ(value.returned_span(), span);
    EXPECT_EQ(value.submitted_digest(), entries[0].submitted_digest());
    const auto encoded = encode_retry_page(
                           entries,
                           location,
                           page_ordinal::make(0).value(),
                           0,
                           work,
                           budget().operation_remaining,
                           charge)
                           .get();
    ASSERT_TRUE(encoded.has_value());
    EXPECT_EQ(flat(encoded->bytes), wire);
}

TEST(RetryFormatTest, UnsignedFullKeyOrderIsNotLogicalOrder) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const std::array entries{
      retry(UINT64_MAX), retry(0, 1, 2), retry(0, 2), retry(0, 1, 1, 0xff)};
    const auto encoded = encode_retry_page(
                           entries,
                           root_location(),
                           page_ordinal::make(0).value(),
                           0,
                           work,
                           budget().operation_remaining,
                           charge)
                           .get();
    ASSERT_TRUE(encoded.has_value());
    const std::array refs{encoded->reference};
    const auto root = pin_root(one_root(refs), work);
    fragmented_buffer_parser input{buffer(flat(encoded->bytes))};
    const auto decoded = decode_retry_page(
                           input,
                           root,
                           page_ordinal::make(0).value(),
                           reserve_page(input, root, work),
                           work)
                           .get();
    ASSERT_TRUE(decoded.has_value());
    EXPECT_TRUE(std::ranges::equal(decoded->value.entries(), entries));
    for (const auto& bad :
         {std::array{retry(1), retry(0)}, std::array{retry(0), retry(0)}}) {
        EXPECT_FALSE(encode_retry_page(
                       bad,
                       root_location(),
                       page_ordinal::make(0).value(),
                       0,
                       work,
                       budget().operation_remaining,
                       charge)
                       .get());
        const auto wire = retry_page_wire(bad);
        const std::array bad_refs{reference(wire, 0, 0, 2)};
        const auto bad_root = pin_root(one_root(bad_refs), work);
        fragmented_buffer_parser parser{buffer(wire)};
        const auto rejected = decode_retry_page(
                                parser,
                                bad_root,
                                page_ordinal::make(0).value(),
                                reserve_page(parser, bad_root, work),
                                work)
                                .get();
        ASSERT_FALSE(rejected.has_value());
        EXPECT_EQ(rejected.error().code(), errc::malformed_data);
        EXPECT_EQ(parser.bytes_consumed().value(), 0U);
    }
}

TEST(RetryFormatTest, SummaryRequiresEveryPageAndCrossPageKeysAscend) {
    for (const bool duplicate : {false, true}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        const std::array first{retry(1)}, second{retry(duplicate ? 1U : 2U)};
        const auto a = retry_page_wire(first),
                   b = retry_page_wire(second, root_location(), 1, 1);
        const std::array refs{reference(a), reference(b, 1, 1)};
        const auto root = pin_root(one_root(refs), work);
        retry_summary_verifier summary{root, work.policy()};
        {
            fragmented_buffer_parser input{buffer(a)};
            const auto page
              = summary.next(input, reserve_page(input, root, work), work)
                  .get();
            ASSERT_TRUE(page.has_value());
        }
        fragmented_buffer_parser input{buffer("p" + b)};
        input.skip(byte_count{1}).value();
        input.push_checkpoint().value();
        const auto page
          = summary.next(input, reserve_page(input, root, work), work).get();
        if (duplicate) {
            ASSERT_FALSE(page.has_value());
            EXPECT_EQ(page.error().code(), errc::malformed_data);
            EXPECT_EQ(input.bytes_consumed().value(), 1U);
            EXPECT_TRUE(summary.closed());
            EXPECT_FALSE(summary.finish(work));
        } else {
            ASSERT_TRUE(page.has_value());
            const auto proof = summary.finish(work);
            ASSERT_TRUE(proof.has_value());
            EXPECT_EQ(proof->entry_count(), 2U);
            EXPECT_EQ(proof->root_digest(), root.digest());
            EXPECT_FALSE(summary.finish(work));
        }
        EXPECT_EQ(root.retry_count(), 2U);
        EXPECT_EQ(root.pages()[1], refs[1]);
        EXPECT_EQ(input.checkpoint_depth(), 1U);
        retry_summary_verifier missing{root, work.policy()};
        EXPECT_FALSE(missing.finish(work));
        EXPECT_TRUE(missing.closed());
    }
}

TEST(RetryFormatTest, GapsDuplicatesWrongRootAndWrongDigestRollback) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const std::array entries{retry()};
    const auto wire = retry_page_wire(entries);
    const std::array refs{
      reference(wire),
      reference(retry_page_wire(entries, root_location(), 1, 1), 1, 1)};
    const auto root = pin_root(one_root(refs), work);
    for (const auto offset : {4U, 76U, 84U, 88U, 92U}) {
        auto wrong = wire;
        wrong[32 + offset] ^= 1;
        repair(wrong);
        fragmented_buffer_parser input{buffer(wrong)};
        retry_summary_verifier summary{root, work.policy()};
        EXPECT_FALSE(
          summary.next(input, reserve_page(input, root, work), work).get());
        EXPECT_TRUE(summary.closed());
        EXPECT_EQ(input.bytes_consumed().value(), 0U);
    }
    auto different = wire;
    different[32 + 100 + 40] ^= 1;
    repair(different);
    fragmented_buffer_parser input{buffer(different)};
    const auto result = decode_retry_page(
                          input,
                          root,
                          page_ordinal::make(0).value(),
                          reserve_page(input, root, work),
                          work)
                          .get();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), errc::corrupt_data);
    EXPECT_EQ(input.bytes_consumed().value(), 0U);
    retry_summary_verifier repeated{root, work.policy()};
    fragmented_buffer_parser one{buffer(wire)};
    ASSERT_TRUE(repeated.next(one, reserve_page(one, root, work), work).get());
    fragmented_buffer_parser two{buffer(wire)};
    EXPECT_FALSE(repeated.next(two, reserve_page(two, root, work), work).get());
    EXPECT_TRUE(repeated.closed());
}

TEST(RetryFormatTest, EveryTruncationAndRepairedEntryCorruption) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const std::array entries{retry()};
    const auto wire = retry_page_wire(entries);
    const std::array refs{reference(wire)};
    const auto root = pin_root(one_root(refs), work);
    for (std::size_t length = 0; length < wire.size(); ++length) {
        fragmented_buffer_parser input{
          buffer(std::string_view{wire}.substr(0, length))};
        const auto result = decode_retry_page(
                              input,
                              root,
                              page_ordinal::make(0).value(),
                              reserve_page(input, root, work),
                              work)
                              .get();
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().code(), errc::truncated_data);
        EXPECT_EQ(input.bytes_consumed().value(), 0U);
    }
    for (const auto offset :
         {0U, 16U, 24U, 72U, 88U, 104U, 112U, 128U, 136U, 144U, 152U}) {
        auto wrong = wire;
        const auto length = (offset == 0 || offset == 72 || offset == 88
                             || offset == 112)
                              ? 16U
                              : 8U;
        wrong.replace(32 + 100 + offset, length, length, '\0');
        if (offset == 136) put(wrong, 32 + 100 + offset, 102, 8);
        repair(wrong);
        const std::array bad_refs{reference(wrong)};
        const auto bad_root = pin_root(one_root(bad_refs), work);
        fragmented_buffer_parser input{buffer(wrong)};
        EXPECT_FALSE(decode_retry_page(
                       input,
                       bad_root,
                       page_ordinal::make(0).value(),
                       reserve_page(input, bad_root, work),
                       work)
                       .get());
        EXPECT_EQ(input.bytes_consumed().value(), 0U);
    }
    auto wrong_ack = wire;
    put(wrong_ack, 32 + 100 + 152, 2, 8);
    repair(wrong_ack);
    const std::array bad_refs{reference(wrong_ack)};
    const auto bad_root = pin_root(one_root(bad_refs), work);
    fragmented_buffer_parser input{buffer(wrong_ack)};
    EXPECT_FALSE(decode_retry_page(
                   input,
                   bad_root,
                   page_ordinal::make(0).value(),
                   reserve_page(input, bad_root, work),
                   work)
                   .get());
}

TEST(RetryFormatTest, PageCapacityUsesHeaderAlignmentAndServedMetadata) {
    for (const std::uint64_t a : {512U, 4096U, 65536U}) {
        for (const std::size_t h : {32U, 4096U}) {
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            const auto count = retry_page_capacity(
                                 byte_count{h}, alignment(a), work.policy())
                                 .value();
            EXPECT_EQ(count, h == 32 ? 408U : 383U);
            std::vector<completed_retry> entries;
            entries.reserve(count);
            for (std::uint32_t i = 0; i < count; ++i)
                entries.push_back(retry(i));
            const auto location = root_location(0x30, 1, a);
            const auto wire = retry_page_wire(entries, location, 0, 0, h);
            ASSERT_LE(wire.size(), 65536U);
            const std::array refs{reference(wire, 0, 0, count)};
            const auto root = pin_root(
              one_root(refs, location), work, location);
            fragmented_buffer_parser input{buffer(wire, 4096)};
            const auto memory = reserve_page(input, root, work);
            const auto decoded
              = decode_retry_page(
                  input, root, page_ordinal::make(0).value(), memory, work)
                  .get();
            ASSERT_TRUE(decoded.has_value());
            EXPECT_EQ(decoded->value.entry_capacity(), count);
            const auto served = charge(
              byte_count{
                decoded->value.entry_capacity() * sizeof(completed_retry)});
            EXPECT_LE(served, work.policy().config().max_allocation_bytes);
            EXPECT_GE(
              served.value(),
              ::malloc_usable_size(
                const_cast<completed_retry*>(decoded->value.entries().data())));
            EXPECT_EQ(
              memory.metadata_remaining.value()
                - decoded->remaining.metadata_remaining.value(),
              served.value());
            EXPECT_EQ(
              memory.operation_remaining.value()
                - decoded->remaining.operation_remaining.value(),
              served.value());
            EXPECT_TRUE(std::ranges::equal(decoded->value.entries(), entries));
            auto bad = wire;
            put(bad, h + 92, count + 1U, 4);
            repair(bad);
            // Count cannot be increased without changing the independently
            // pinned root.
            fragmented_buffer_parser short_input{buffer(bad, 4096)};
            EXPECT_FALSE(decode_retry_page(
                           short_input,
                           root,
                           page_ordinal::make(0).value(),
                           reserve_page(short_input, root, work),
                           work)
                           .get());
        }
    }
    EXPECT_FALSE(page_ordinal::make(256));
    EXPECT_TRUE(page_count::make(256));
    EXPECT_FALSE(page_count::make(257));
    EXPECT_FALSE(
      page_ref::make(
        page_ordinal::make(0).value(),
        65536,
        1,
        byte_count{512},
        codec::immutable_object_digest{{}}));
}

TEST(RetryFormatTest, ZeroBudgetAndCancellationNeverPublishAPage) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const std::array entries{retry()};
    const auto wire = retry_page_wire(entries);
    const std::array refs{reference(wire)};
    const auto root = pin_root(one_root(refs), work);
    fragmented_buffer_parser input{buffer(wire)};
    auto memory = reserve_page(input, root, work);
    memory.metadata_remaining = {};
    EXPECT_FALSE(decode_retry_page(
                   input, root, page_ordinal::make(0).value(), memory, work)
                   .get());
    EXPECT_EQ(input.bytes_consumed().value(), 0U);
    EXPECT_FALSE(encode_retry_page(
                   entries,
                   root_location(),
                   page_ordinal::make(0).value(),
                   0,
                   work,
                   byte_count{},
                   charge)
                   .get());
    retry_summary_verifier summary{root, work.policy()};
    memory = reserve_page(input, root, work);
    abort.request_abort();
    const auto result = summary.next(input, memory, work).get();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), errc::aborted);
    EXPECT_TRUE(summary.closed());
    EXPECT_EQ(input.bytes_consumed().value(), 0U);
}
} // namespace
} // namespace kwaque::storage
