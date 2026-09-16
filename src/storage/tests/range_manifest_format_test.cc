#include "src/codec/sha256.h"
#include "src/storage/range_manifest_format.h"
#include "src/storage/tests/range_manifest_test_support.h"
#include "src/storage/tests/segment_test_support.h"

#include <seastar/core/preempt.hh>
#include <seastar/util/alloc_failure_injector.hh>

#include <gtest/gtest.h>

#include <chrono>
#include <concepts>
#include <optional>
#include <type_traits>
#include <vector>

namespace kwaque::storage {
namespace {
using namespace testing;
using namespace testing::manifest;
static_assert(!std::default_initializable<manifest_context>);
static_assert(!std::is_aggregate_v<manifest_context>);
static_assert(!std::default_initializable<range_manifest_entry>);
static_assert(!std::is_aggregate_v<range_manifest_entry>);
static_assert(!std::default_initializable<range_manifest_root_header>);
static_assert(!std::default_initializable<range_manifest_page_header>);
static_assert(
  !std::same_as<model::range_manifest_generation, model::segment_generation>);
static_assert(628U * sizeof(range_manifest_entry) <= 131072);

template<typename T>
void error(const codec::result<T>& value, errc code) {
    ASSERT_FALSE(value.has_value());
    EXPECT_EQ(value.error().code(), code);
}

TEST(
  RangeManifestFormatTest,
  ContextRequiresEveryIdentityAndIndependentGeneration) {
    const auto context = mc();
    EXPECT_EQ(context.topic(), id<model::topic_id>(0x10));
    EXPECT_EQ(context.range(), id<model::range_id>(0x20));
    EXPECT_EQ(context.manifest(), id<model::manifest_id>(0x60));
    EXPECT_EQ(context.generation().value(), 3U);
    EXPECT_TRUE(context.validate_expected(mc()));
    EXPECT_FALSE(context.validate_expected(mc(0x61)));
    EXPECT_FALSE(context.validate_expected(mc(0x60, 4)));
    EXPECT_FALSE(
      manifest_context::make(
        {}, context.range(), context.manifest(), context.generation()));
    EXPECT_FALSE(
      manifest_context::make(
        context.topic(), {}, context.manifest(), context.generation()));
    EXPECT_FALSE(
      manifest_context::make(
        context.topic(), context.range(), {}, context.generation()));
    EXPECT_FALSE(
      manifest_context::make(
        context.topic(), context.range(), context.manifest(), {}));
    EXPECT_EQ(mc(0x60, UINT64_MAX).generation().value(), UINT64_MAX);
    EXPECT_FALSE(mc(0x60, UINT64_MAX).generation().checked_successor());
}

TEST(RangeManifestFormatTest, EntriesKeepNonAffineAndEmptyPhysicalExtents) {
    const auto data = item();
    const auto empty = item(102, 105, true, 0x80, 9);
    EXPECT_EQ(data.segment(), id<model::segment_id>(0x30));
    EXPECT_EQ(data.generation().value(), 7U);
    EXPECT_EQ(empty.generation().value(), 9U);
    EXPECT_EQ(empty.coverage().logical().count().value(), 3U);
    EXPECT_TRUE(empty.coverage().physical().empty());
    EXPECT_TRUE(empty.coverage().bytes().empty());
    EXPECT_EQ(empty.digest().bytes(), manifest_sha(""));
    EXPECT_TRUE(
      range_manifest_entry::make(
        data.segment(),
        data.generation(),
        data.coverage(),
        codec::extent_digest{codec::sha256_digest{}}));
    EXPECT_TRUE(
      range_manifest_entry::make(
        data.segment(),
        data.generation(),
        extent_scope(100, 105, 0, 2, 512, 1024),
        data.digest()));
    EXPECT_TRUE(
      range_manifest_entry::make(
        data.segment(),
        data.generation(),
        extent_scope(100, 105, 0, 0, 512, 1024),
        codec::extent_digest{manifest_sha("footer")}));
    EXPECT_FALSE(
      range_manifest_entry::make(
        {}, data.generation(), data.coverage(), data.digest()));
    EXPECT_FALSE(
      range_manifest_entry::make(
        data.segment(), {}, data.coverage(), data.digest()));
    EXPECT_FALSE(
      range_manifest_entry::make(
        data.segment(),
        data.generation(),
        extent_scope(100, 100, 0, 0, 512, 512),
        codec::extent_digest{manifest_sha("")}));
    EXPECT_FALSE(
      range_manifest_entry::make(
        data.segment(),
        data.generation(),
        extent_scope(100, 101, 0, 2, 512, 1024),
        data.digest()));
    EXPECT_FALSE(
      range_manifest_entry::make(
        data.segment(),
        data.generation(),
        extent_scope(100, 101, 0, 1, 512, 512),
        codec::extent_digest{manifest_sha("")}));
    EXPECT_FALSE(
      range_manifest_entry::make(
        data.segment(), data.generation(), empty.coverage(), data.digest()));
}

TEST(RangeManifestFormatTest, HeadersValidateEmptyRootAndNonemptyPageCounts) {
    const auto context = mc();
    EXPECT_TRUE(
      range_manifest_root_header::make(
        context,
        logical(UINT64_MAX, UINT64_MAX),
        0,
        page_count::make(0).value()));
    EXPECT_FALSE(
      range_manifest_root_header::make(
        context, logical(100, 101), 0, page_count::make(0).value()));
    EXPECT_FALSE(
      range_manifest_root_header::make(
        context, logical(100, 101), 1, page_count::make(0).value()));
    EXPECT_FALSE(
      range_manifest_root_header::make(
        context, logical(100, 100), 1, page_count::make(1).value()));
    EXPECT_FALSE(
      range_manifest_root_header::make(
        context, logical(100, 105), 1, page_count::make(2).value()));
    EXPECT_FALSE(
      range_manifest_root_header::make(
        context, logical(100, 101), 2, page_count::make(1).value()));
    EXPECT_TRUE(
      range_manifest_root_header::make(
        context, logical(0, 65536), 65536, page_count::make(256).value()));
    EXPECT_FALSE(
      range_manifest_root_header::make(
        context, logical(0, 65537), 65537, page_count::make(256).value()));
    const auto ordinal = page_ordinal::make(0).value();
    EXPECT_FALSE(
      range_manifest_page_header::make(
        context, logical(100, 105), ordinal, 0, 0));
    EXPECT_FALSE(
      range_manifest_page_header::make(
        context, logical(100, 100), ordinal, 0, 1));
    EXPECT_FALSE(
      range_manifest_page_header::make(
        context, logical(100, 101), ordinal, 0, 2));
    EXPECT_FALSE(
      range_manifest_page_header::make(
        context, logical(100, 105), ordinal, 65536, 1));
    EXPECT_FALSE(
      range_manifest_page_header::make(
        context, logical(100, 105), ordinal, UINT32_MAX, 1));
    EXPECT_FALSE(
      range_manifest_page_header::make(
        context, logical(100, 105), ordinal, 1, 1));
    EXPECT_FALSE(
      range_manifest_page_header::make(
        context, logical(100, 105), page_ordinal::make(2).value(), 1, 1));
    EXPECT_TRUE(
      range_manifest_page_header::make(
        context,
        logical(UINT64_MAX - 1, UINT64_MAX),
        page_ordinal::make(255).value(),
        65535,
        1));
}

TEST(RangeManifestFormatTest, IndependentGoldenRootAndPagePreserveEveryField) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto entries = items();
    const auto header = page_header();
    const auto page = encode_range_manifest_page(
                        header,
                        entries,
                        alignment(),
                        work,
                        budget().operation_remaining,
                        charge)
                        .get();
    ASSERT_TRUE(page.has_value());
    const auto wire = expected_page(header, entries);
    EXPECT_EQ(
      wire.substr(0, 32),
      hex("4b5142460900010001002000e00100000000000000000000a977229837a46df4"));
    EXPECT_TRUE(
      std::ranges::equal(
        std::bit_cast<std::array<char, 32>>(manifest_sha(wire)),
        hex(
          "7544c47d396d88f4c8b85cbca97026cebfd5c927ff99eda4eaf87bfffe428264")));
    EXPECT_EQ(flat(page->bytes), wire);
    EXPECT_EQ(page->reference, manifest_page_reference(wire, header));
    EXPECT_EQ(header.context(), mc());
    EXPECT_EQ(header.logical_span(), logical(100, 105));
    EXPECT_EQ(header.ordinal().value(), 0U);
    EXPECT_EQ(header.first_entry(), 0U);
    EXPECT_EQ(header.entry_count(), 2U);
    const std::array refs{page->reference};
    const auto root = encode_range_manifest_root(
                        root_header(),
                        refs,
                        alignment(),
                        work,
                        budget().operation_remaining,
                        charge)
                        .get();
    ASSERT_TRUE(root.has_value());
    const auto root_wire = expected_root(root_header(), refs);
    EXPECT_EQ(
      root_wire.substr(0, 32),
      hex("4b5142460900010001002000e00100000000000000000000f698e1853f3c9287"));
    EXPECT_TRUE(
      std::ranges::equal(
        std::bit_cast<std::array<char, 32>>(manifest_sha(root_wire)),
        hex(
          "258a17860ce5238753a07825729bc9504ee41e747a369395da7c0ca0dfe4f416")));
    EXPECT_EQ(flat(root->bytes), root_wire);
    EXPECT_EQ(root->digest.bytes(), manifest_sha(root_wire));
}

TEST(RangeManifestFormatTest, MetadataAlignmentDoesNotRewriteExtentLocations) {
    const auto entries = items();
    const auto header = page_header();
    for (const std::uint64_t a : {512U, 65536U}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        const auto page = encode_range_manifest_page(
                            header,
                            entries,
                            alignment(a),
                            work,
                            budget().operation_remaining,
                            charge)
                            .get();
        ASSERT_TRUE(page.has_value());
        const auto wire = flat(page->bytes);
        EXPECT_EQ(wire, expected_page(header, entries, a));
        EXPECT_EQ(wire.size() % a, 0U);
        EXPECT_EQ(get(wire, 32 + 92 + 56, 8), 512U);
        EXPECT_EQ(get(wire, 32 + 92 + 64, 8), 1536U);
        const std::array refs{page->reference};
        const auto root = encode_range_manifest_root(
                            root_header(),
                            refs,
                            alignment(a),
                            work,
                            budget().operation_remaining,
                            charge)
                            .get();
        ASSERT_TRUE(root.has_value());
        EXPECT_EQ(flat(root->bytes), expected_root(root_header(), refs, a));
        EXPECT_EQ(root->bytes.size().value() % a, 0U);
    }
}

TEST(
  RangeManifestFormatTest, EmptyRootsAndMaximumBoundariesEncodeWithoutEntries) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    for (const std::uint64_t end :
         {std::uint64_t{0}, std::uint64_t{100}, std::uint64_t{UINT64_MAX}}) {
        const auto header = root_header(0, 0, mc(), end, end);
        const auto result = encode_range_manifest_root(
                              header,
                              {},
                              alignment(),
                              work,
                              budget().operation_remaining,
                              charge)
                              .get();
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(flat(result->bytes), expected_root(header, {}));
    }
    const auto wide = range_manifest_entry::make(
                        id<model::segment_id>(0x81),
                        model::segment_generation::make(UINT64_MAX).value(),
                        extent_scope(
                          UINT64_MAX - 4,
                          UINT64_MAX,
                          UINT64_MAX - 2,
                          UINT64_MAX,
                          (std::uint64_t{1} << 48U),
                          (std::uint64_t{1} << 48U) + 512),
                        codec::extent_digest{manifest_sha("wide")})
                        .value();
    const std::array entries{wide};
    const auto header = page_header(
      1, mc(0x80, UINT64_MAX), UINT64_MAX - 4, UINT64_MAX, 255, 65535);
    const auto result = encode_range_manifest_page(
                          header,
                          entries,
                          alignment(),
                          work,
                          budget().operation_remaining,
                          charge)
                          .get();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(flat(result->bytes), expected_page(header, entries));
    EXPECT_EQ(result->reference.ordinal().value(), 255U);
    EXPECT_EQ(result->reference.first_entry(), 65535U);
}

TEST(
  RangeManifestFormatTest, ChangedManifestKeepsIndependentSegmentGenerations) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto entries = items();
    const auto first = encode_range_manifest_page(
                         page_header(),
                         entries,
                         alignment(),
                         work,
                         budget().operation_remaining,
                         charge)
                         .get();
    const auto second = encode_range_manifest_page(
                          page_header(2, mc(0x61, 4)),
                          entries,
                          alignment(),
                          work,
                          budget().operation_remaining,
                          charge)
                          .get();
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());
    EXPECT_NE(first->reference.digest(), second->reference.digest());
    EXPECT_EQ(
      flat(first->bytes).substr(32 + 92, 208),
      flat(second->bytes).substr(32 + 92, 208));
    EXPECT_EQ(get(flat(first->bytes), 32 + 92 + 32, 8), 7U);
    EXPECT_EQ(get(flat(first->bytes), 32 + 92 + 104 + 32, 8), 9U);
}

TEST(
  RangeManifestFormatTest,
  OrderedEntriesRejectDuplicatesOverlapsGapsAndReordering) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    for (const auto& entries :
         {std::array{item(100, 102), item(100, 105)},
          std::array{item(100, 103), item(102, 105)},
          std::array{item(100, 102), item(103, 105)},
          std::array{item(102, 105), item(100, 102)},
          std::array{item(101, 102), item(102, 105)},
          std::array{item(100, 102), item(102, 104)},
          std::array{item(100, 102), item(102, 106)}}) {
        error(
          encode_range_manifest_page(
            page_header(),
            entries,
            alignment(),
            work,
            budget().operation_remaining,
            charge)
            .get(),
          errc::invalid_argument);
    }
    const auto entries = items();
    error(
      encode_range_manifest_page(
        page_header(),
        std::span{entries}.first(1),
        alignment(),
        work,
        budget().operation_remaining,
        charge)
        .get(),
      errc::invalid_argument);
    error(
      encode_range_manifest_page(
        page_header(),
        {},
        alignment(),
        work,
        budget().operation_remaining,
        charge)
        .get(),
      errc::invalid_argument);
    auto reversed = entries;
    std::reverse(reversed.begin(), reversed.end());
    std::sort(
      reversed.begin(), reversed.end(), [](const auto& a, const auto& b) {
          return a.coverage().logical().begin()
                 < b.coverage().logical().begin();
      });
    const auto a = encode_range_manifest_page(
                     page_header(),
                     entries,
                     alignment(),
                     work,
                     budget().operation_remaining,
                     charge)
                     .get();
    const auto b = encode_range_manifest_page(
                     page_header(),
                     reversed,
                     alignment(),
                     work,
                     budget().operation_remaining,
                     charge)
                     .get();
    ASSERT_TRUE(a.has_value());
    ASSERT_TRUE(b.has_value());
    EXPECT_EQ(flat(a->bytes), flat(b->bytes));
}

TEST(RangeManifestFormatTest, RootRequiresExactReferenceTopologyAndTotals) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto digest = codec::immutable_object_digest{manifest_sha("page")};
    const auto first
      = page_ref::make(
          page_ordinal::make(0).value(), 0, 1, byte_count{512}, digest)
          .value();
    const auto second
      = page_ref::make(
          page_ordinal::make(1).value(), 1, 1, byte_count{512}, digest)
          .value();
    const auto header = root_header(2, 2);
    for (const auto& refs :
         {std::array{second, first},
          std::array{first, first},
          std::array{
            first,
            page_ref::make(
              page_ordinal::make(1).value(), 2, 1, byte_count{512}, digest)
              .value()},
          std::array{
            first,
            page_ref::make(
              page_ordinal::make(1).value(), 1, 1, byte_count{511}, digest)
              .value()}}) {
        error(
          encode_range_manifest_root(
            header,
            refs,
            alignment(),
            work,
            budget().operation_remaining,
            charge)
            .get(),
          errc::malformed_data);
    }
    const std::array refs{first, second};
    error(
      encode_range_manifest_root(
        root_header(3, 2),
        refs,
        alignment(),
        work,
        budget().operation_remaining,
        charge)
        .get(),
      errc::invalid_argument);
    error(
      encode_range_manifest_root(
        header,
        std::span{refs}.first(1),
        alignment(),
        work,
        budget().operation_remaining,
        charge)
        .get(),
      errc::invalid_argument);
    const std::array oversized{
      page_ref::make(
        page_ordinal::make(0).value(), 0, 628, byte_count{512}, digest)
        .value()};
    error(
      encode_range_manifest_root(
        root_header(628, 1, mc(), 0, 628),
        oversized,
        alignment(),
        work,
        budget().operation_remaining,
        charge)
        .get(),
      errc::malformed_data);
}

TEST(RangeManifestFormatTest, PageCapacityAndNarrowerPoliciesBoundOutput) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    EXPECT_EQ(
      range_manifest_page_capacity(byte_count{32}, alignment(), work.policy())
        .value(),
      628U);
    EXPECT_EQ(
      range_manifest_page_capacity(
        byte_count{4096}, alignment(65536), work.policy())
        .value(),
      589U);
    std::vector<range_manifest_entry> entries;
    entries.reserve(629);
    for (std::uint32_t i = 0; i < 629; ++i)
        entries.push_back(item(100U + i, 101U + i, true));
    for (const std::uint64_t a : {512U, 65536U}) {
        const auto header = page_header(628, mc(), 100, 728);
        const auto page = encode_range_manifest_page(
                            header,
                            std::span{entries}.first(628),
                            alignment(a),
                            work,
                            budget().operation_remaining,
                            charge)
                            .get();
        ASSERT_TRUE(page.has_value());
        EXPECT_LE(page->bytes.size().value(), 65536U);
        EXPECT_EQ(
          flat(page->bytes),
          expected_page(header, std::span{entries}.first(628), a));
    }
    error(
      encode_range_manifest_page(
        page_header(629, mc(), 100, 729),
        entries,
        alignment(),
        work,
        budget().operation_remaining,
        charge)
        .get(),
      errc::resource_exhausted);
    auto config = work.policy().config();
    config.max_object_entries = item_count{1};
    codec::cooperative_work narrow{codec::limits::make(config).value(), abort};
    const auto pair = items();
    error(
      encode_range_manifest_page(
        page_header(),
        pair,
        alignment(),
        narrow,
        budget().operation_remaining,
        charge)
        .get(),
      errc::resource_exhausted);
    error(
      encode_range_manifest_page(
        page_header(1, mc(), 100, 101, 1, 1),
        std::span{entries}.first(1),
        alignment(),
        narrow,
        budget().operation_remaining,
        charge)
        .get(),
      errc::resource_exhausted);
}

TEST(RangeManifestFormatTest, MaximumRootPageCountAndDiagnosticEnd) {
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
            codec::immutable_object_digest{manifest_sha("page")})
            .value());
    const auto header = root_header(256, 256, mc(), 100, 356);
    const auto wire = expected_root(header, refs);
    const codec::field_context c{.origin = UINT64_MAX - wire.size()};
    const auto root = encode_range_manifest_root(
                        header,
                        refs,
                        alignment(),
                        work,
                        budget().operation_remaining,
                        charge,
                        c)
                        .get();
    ASSERT_TRUE(root.has_value());
    EXPECT_EQ(flat(root->bytes), wire);
    error(
      encode_range_manifest_root(
        header,
        refs,
        alignment(),
        work,
        budget().operation_remaining,
        charge,
        {.origin = c.origin + 1U})
        .get(),
      errc::out_of_range);
    auto config = work.policy().config();
    config.max_object_pages = item_count{255};
    codec::cooperative_work narrow{codec::limits::make(config).value(), abort};
    error(
      encode_range_manifest_root(
        header, refs, alignment(), narrow, budget().operation_remaining, charge)
        .get(),
      errc::resource_exhausted);
}

TEST(RangeManifestFormatTest, OutputOwnsBytesAfterBorrowedEntriesAreDestroyed) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    std::optional<encoded_range_manifest_page> output;
    {
        const auto entries = items();
        auto encoded = encode_range_manifest_page(
                         page_header(),
                         entries,
                         alignment(),
                         work,
                         budget().operation_remaining,
                         charge)
                         .get();
        ASSERT_TRUE(encoded.has_value());
        output.emplace(std::move(*encoded));
    }
    EXPECT_EQ(flat(output->bytes), expected_page(page_header(), items()));
}

TEST(RangeManifestFormatTest, BudgetAndKnownCancellationReturnNoOutput) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto entries = items();
    const std::array refs{manifest_page_reference(
      expected_page(page_header(), entries), page_header())};
    error(
      encode_range_manifest_page(
        page_header(), entries, alignment(), work, {}, charge)
        .get(),
      errc::resource_exhausted);
    error(
      encode_range_manifest_root(
        root_header(), refs, alignment(), work, {}, charge)
        .get(),
      errc::resource_exhausted);
    error(
      encode_range_manifest_page(
        page_header(),
        entries,
        alignment(),
        work,
        budget().operation_remaining,
        nullptr)
        .get(),
      errc::invalid_argument);
    error(
      encode_range_manifest_root(
        root_header(),
        refs,
        alignment(),
        work,
        budget().operation_remaining,
        nullptr)
        .get(),
      errc::invalid_argument);
    abort.request_abort();
    error(
      encode_range_manifest_page(
        page_header(),
        entries,
        alignment(),
        work,
        budget().operation_remaining,
        charge)
        .get(),
      errc::aborted);
    error(
      encode_range_manifest_root(
        root_header(),
        refs,
        alignment(),
        work,
        budget().operation_remaining,
        charge)
        .get(),
      errc::aborted);
    EXPECT_EQ(entries, items());
}

TEST(RangeManifestFormatTest, SuspendedWritersObserveCancellation) {
    const auto entries = items();
    const auto page = page_header();
    const auto root = root_header();
    const std::array refs{
      manifest_page_reference(expected_page(page, entries), page)};
    for (const bool is_root : {false, true}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        const auto deadline = std::chrono::steady_clock::now()
                              + std::chrono::seconds{2};
        while (!seastar::need_preempt()
               && std::chrono::steady_clock::now() < deadline) {
        }
        ASSERT_TRUE(seastar::need_preempt());
        if (is_root) {
            auto pending = encode_range_manifest_root(
              root,
              refs,
              alignment(),
              work,
              budget().operation_remaining,
              charge);
            const bool suspended = !pending.available();
            abort.request_abort();
            error(pending.get(), errc::aborted);
            EXPECT_TRUE(suspended);
        } else {
            auto pending = encode_range_manifest_page(
              page,
              entries,
              alignment(),
              work,
              budget().operation_remaining,
              charge);
            const bool suspended = !pending.available();
            abort.request_abort();
            error(pending.get(), errc::aborted);
            EXPECT_TRUE(suspended);
        }
    }
}

TEST(
  RangeManifestFormatTest,
  AllocationFailureDrainsPrivateOutputAndPreservesEntries) {
#ifndef SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION
    GTEST_SKIP() << "allocation failure injection is unavailable";
#else
    const auto entries = items();
    const auto page = page_header();
    const auto root = root_header();
    const std::array refs{
      manifest_page_reference(expected_page(page, entries), page)};
    for (const bool is_root : {false, true}) {
        std::size_t failures = 0;
        bool complete = false;
        for (std::uint64_t ordinal = 0; ordinal < 256 && !complete; ++ordinal) {
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            auto& injector = seastar::memory::local_failure_injector();
            bool succeeded = false;
            injector.fail_after(ordinal);
            try {
                if (is_root)
                    succeeded = encode_range_manifest_root(
                                  root,
                                  refs,
                                  alignment(),
                                  work,
                                  budget().operation_remaining,
                                  charge)
                                  .get()
                                  .has_value();
                else
                    succeeded = encode_range_manifest_page(
                                  page,
                                  entries,
                                  alignment(),
                                  work,
                                  budget().operation_remaining,
                                  charge)
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
            const bool reached = injector.failed();
            injector.cancel();
            if (!succeeded) {
                EXPECT_TRUE(reached);
                ++failures;
            } else
                complete = !reached;
            EXPECT_EQ(entries, items());
        }
        EXPECT_TRUE(complete);
        EXPECT_GT(failures, 0U);
    }
#endif
}
} // namespace
} // namespace kwaque::storage
