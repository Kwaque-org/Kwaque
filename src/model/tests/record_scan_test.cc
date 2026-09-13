#include "src/model/record_scan.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/util/alloc_failure_injector.hh>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <bit>
#include <limits>
#include <new>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

namespace {
using namespace std::literals;
namespace model = kwaque::model;
namespace codec = kwaque::codec;
using kwaque::byte_count;
using kwaque::errc;
using kwaque::item_count;
using kwaque::bytes::fragmented_buffer;
using kwaque::bytes::fragmented_buffer_parser;

constexpr auto null_record = "\x06\x00\x00\x00\x01\x01\x00"sv;
constexpr auto full_record
  = "\x0c\x00\x00\x00\x02\x6b\x01\x02\x00\x01\x01\xff\x00"sv;
constexpr codec::field_context origin{.origin = 1000, .family = 1};
static_assert(
  std::is_nothrow_move_constructible_v<model::record_region_scanner>);
static_assert(!std::is_copy_constructible_v<model::record_region_scanner>);
static_assert(sizeof(model::record_region_scanner) < 8192);
static_assert(sizeof(model::record_layout) < 4096);
static_assert(sizeof(model::record_header_range) * 64 < 4096);

byte_count charge(byte_count request) noexcept {
    if (request.value() == 0) return {};
    if (request.value() > (std::uint64_t{1} << 62U))
        return byte_count{UINT64_MAX};
    return byte_count{
      2U * std::bit_ceil(std::max(request.value(), std::uint64_t{16}))};
}
codec::decode_budget memory() {
    // Other live fixture owners and native/frame reservations occupy the
    // unclaimed half; this is not a measurement of whole-operation memory.
    return {byte_count{32U << 20U}, byte_count{1U << 20U}, charge};
}
fragmented_buffer bytes(std::string_view raw, std::size_t width = 65536) {
    std::vector<seastar::temporary_buffer<char>> parts;
    for (std::size_t at = 0; at < raw.size(); at += width) {
        const auto piece = raw.substr(at, width);
        seastar::temporary_buffer<char> part{piece.size()};
        std::copy(piece.begin(), piece.end(), part.get_write());
        parts.push_back(std::move(part));
    }
    return fragmented_buffer::copy_from_fragments(parts).value();
}
model::record_region_context
expected(std::uint64_t n = 1, std::uint64_t headers = 0) {
    return {
      kwaque::runtime::wall_time{100},
      model::range_logical_count{n},
      item_count{n},
      item_count{headers}};
}
std::string range_bytes(
  const model::record_region_scanner& scanner, model::record_byte_range range) {
    // Test-only independent range projection; no owning field API is implicit.
    std::string all;
    for (const auto fragment : scanner.bytes())
        all.append(fragment.data(), fragment.size());
    return all.substr(range.offset.value(), range.length.value());
}
template<typename T>
void error(const codec::result<T>& result, errc code) {
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), code);
}

TEST(RecordScanTest, OwnedRegionSurvivesInputDestructionAndProjectsEveryField) {
    for (std::size_t width = 1; width <= full_record.size(); ++width) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto scanner = [&] {
            auto source = bytes(full_record, width);
            auto made
              = model::record_region_scanner::make(
                  std::move(source), expected(1, 2), memory(), work, origin)
                  .get();
            // NOLINTNEXTLINE(bugprone-use-after-move) -- empty donor contract.
            EXPECT_TRUE(source.empty());
            return std::move(made).value();
        }();
        EXPECT_FALSE(scanner.complete());
        ASSERT_TRUE(scanner.next(work).get().value());
        ASSERT_NE(scanner.current(), nullptr);
        const auto& layout = *scanner.current();
        EXPECT_EQ(layout.encoded.offset, byte_count{});
        EXPECT_EQ(layout.encoded.length, byte_count{13});
        EXPECT_EQ(layout.fields.timestamp_delta, 0);
        ASSERT_TRUE(layout.key);
        EXPECT_EQ(range_bytes(scanner, *layout.key), "k");
        EXPECT_FALSE(layout.value);
        ASSERT_EQ(layout.headers().size(), 2U);
        EXPECT_EQ(range_bytes(scanner, layout.headers()[0].name), "");
        EXPECT_FALSE(layout.headers()[0].value);
        EXPECT_EQ(range_bytes(scanner, layout.headers()[1].name), "\xff"sv);
        ASSERT_TRUE(layout.headers()[1].value);
        EXPECT_EQ(range_bytes(scanner, *layout.headers()[1].value), "");
        EXPECT_TRUE(scanner.complete());
        EXPECT_FALSE(scanner.next(work).get().value());
        EXPECT_EQ(scanner.current(), nullptr);
        scanner.close(work).get();
        EXPECT_TRUE(scanner.bytes().empty());
    }
}

TEST(RecordScanTest, ExplicitMaterializationRetainsFieldsAfterScannerCloses) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto scanner = model::record_region_scanner::make(
                     bytes(full_record), expected(1, 2), memory(), work)
                     .get()
                     .value();
    ASSERT_TRUE(scanner.next(work).get().value());
    auto first
      = scanner.materialize_current(scanner.remaining(), work).get().value();
    EXPECT_LT(
      first.remaining.metadata_remaining,
      scanner.remaining().metadata_remaining);
    auto second
      = scanner.materialize_current(first.remaining, work).get().value();
    EXPECT_LT(
      second.remaining.metadata_remaining, first.remaining.metadata_remaining);
    EXPECT_TRUE(scanner.complete());
    auto empty = scanner.remaining();
    empty.metadata_remaining = byte_count{};
    error(
      scanner.materialize_current(empty, work).get(), errc::resource_exhausted);
    scanner.close(work).get();
    for (const auto* record : {&first.value, &second.value}) {
        EXPECT_TRUE(record->key()->content_equals("k"sv));
        EXPECT_FALSE(record->value());
        ASSERT_EQ(record->headers().size(), 2U);
        EXPECT_TRUE(record->headers()[1].name().content_equals("\xff"sv));
        ASSERT_TRUE(record->headers()[1].value());
        EXPECT_TRUE(record->headers()[1].value()->empty());
    }
}

TEST(RecordScanTest, NullMaterializationRefundsAllTemporaryDescriptors) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto scanner = model::record_region_scanner::make(
                     bytes(null_record), expected(), memory(), work)
                     .get()
                     .value();
    ASSERT_TRUE(scanner.next(work).get().value());
    auto owned
      = scanner.materialize_current(scanner.remaining(), work).get().value();
    EXPECT_EQ(owned.remaining, scanner.remaining());
    EXPECT_FALSE(owned.value.key());
    EXPECT_FALSE(owned.value.value());
}

TEST(RecordScanTest, EverySmallFragmentSplitKeepsRegionRelativeCoordinates) {
    std::string wire{null_record};
    auto second = std::string{full_record};
    second[2] = 1; // timestamp -1
    second[3] = 1; // dense ordinal one
    wire += second;
    for (std::size_t cut = 0; cut <= wire.size(); ++cut) {
        std::vector<seastar::temporary_buffer<char>> parts;
        for (auto view :
             {std::string_view{wire}.substr(0, cut),
              std::string_view{wire}.substr(cut)}) {
            seastar::temporary_buffer<char> part{view.size()};
            std::copy(view.begin(), view.end(), part.get_write());
            parts.push_back(std::move(part));
        }
        auto source = fragmented_buffer::copy_from_fragments(parts).value();
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto scanner
          = model::record_region_scanner::make(
              std::move(source), expected(2, 2), memory(), work, origin)
              .get()
              .value();
        EXPECT_TRUE(scanner.next(work).get().value());
        EXPECT_FALSE(scanner.complete());
        EXPECT_TRUE(scanner.next(work).get().value());
        EXPECT_EQ(scanner.current()->encoded.offset, byte_count{7});
        EXPECT_EQ(scanner.current()->key->offset, byte_count{12});
        EXPECT_EQ(scanner.current()->fields.timestamp_delta, -1);
        EXPECT_TRUE(scanner.complete());
    }
}

TEST(RecordScanTest, EarlyStopNeverCertifiesBadSuffixOrIncorrectCounts) {
    for (int mode = 0; mode != 5; ++mode) {
        auto second = std::string{null_record};
        second[3] = 1;
        std::string wire = std::string{null_record} + second;
        auto target = expected(2);
        if (mode == 0) wire.back() = 1; // final header framing missing
        if (mode == 1) {
            wire += null_record;
        } // extra record
        if (mode == 2) target.header_count = item_count{1};
        if (mode == 3) wire[10] = 0; // duplicate dense slot
        if (mode == 4) wire[10] = 2; // outside original slots
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto scanner = model::record_region_scanner::make(
                         bytes(wire), target, memory(), work, origin)
                         .get()
                         .value();
        ASSERT_TRUE(scanner.next(work).get().value());
        EXPECT_FALSE(scanner.complete());
        error(
          scanner.next(work).get(),
          mode == 0 ? errc::resource_exhausted : errc::malformed_data);
        EXPECT_FALSE(scanner.complete());
        EXPECT_EQ(scanner.current(), nullptr);
        error(scanner.next(work).get(), errc::closed);
    }
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    error(
      model::record_region_scanner::make(
        bytes(null_record), expected(2), memory(), work)
        .get(),
      errc::malformed_data);
}

TEST(RecordScanTest, EveryTruncatedRecordIsMalformedInTheCompleteRegion) {
    for (std::size_t cut = 0; cut < full_record.size(); ++cut) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto made = model::record_region_scanner::make(
                      bytes(full_record.substr(0, cut)),
                      expected(1, 2),
                      memory(),
                      work,
                      origin)
                      .get();
        if (cut < 7)
            error(made, errc::malformed_data);
        else {
            ASSERT_TRUE(made.has_value());
            error(made->next(work).get(), errc::malformed_data);
            EXPECT_FALSE(made->complete());
            EXPECT_EQ(made->current(), nullptr);
        }
    }
}

TEST(RecordScanTest, DenseAndSparseTimestampAndOrderRulesAreSeparate) {
    for (int mode = 0; mode != 5; ++mode) {
        auto first = std::string{null_record};
        auto second = first;
        first[2] = 1;
        first[3] = 1;
        second[2] = 3;
        second[3] = 3;
        auto target = expected(5);
        target.retained_count = item_count{2};
        target.kind = model::record_region_kind::sparse;
        if (mode == 1) second[3] = 1;
        if (mode == 2) second[3] = 0;
        if (mode == 3) second[3] = 5;
        if (mode == 4)
            target.timestamp_base = kwaque::runtime::wall_time{INT64_MIN};
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto scanner = model::record_region_scanner::make(
                         bytes(first + second), target, memory(), work)
                         .get()
                         .value();
        if (mode == 4) {
            error(scanner.next(work).get(), errc::malformed_data);
            continue;
        }
        ASSERT_TRUE(scanner.next(work).get().value());
        EXPECT_EQ(scanner.current()->fields.logical_delta.value(), 1U);
        auto next = scanner.next(work).get();
        if (mode == 0) {
            ASSERT_TRUE(next.has_value());
            EXPECT_EQ(scanner.current()->fields.logical_delta.value(), 3U);
            EXPECT_TRUE(scanner.complete());
        } else
            error(next, errc::malformed_data);
    }
    auto nonzero_first = std::string{null_record};
    nonzero_first[2] = 2;
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto dense = model::record_region_scanner::make(
                   bytes(nonzero_first), expected(), memory(), work)
                   .get()
                   .value();
    error(dense.next(work).get(), errc::malformed_data);
}

TEST(RecordScanTest, ScannerAndMaterializerReportTheSameMalformedField) {
    constexpr std::array<unsigned char, 3> mutations{0x7f, 0x80, 0xff};
    for (std::size_t index = 0; index < full_record.size(); ++index) {
        for (const auto mutation : mutations) {
            auto wire = std::string{full_record};
            wire[index] = static_cast<char>(mutation);
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            auto scanner
              = model::record_region_scanner::make(
                  bytes(wire), expected(1, 2), memory(), work, origin)
                  .get()
                  .value();
            const auto scanned = scanner.next(work).get();
            fragmented_buffer_parser parser{bytes(wire)};
            const auto budget = codec::reserve_decode_input(
                                  parser, work.policy(), memory(), origin)
                                  .value();
            const auto owned = model::decode_record(
                                 parser,
                                 {kwaque::runtime::wall_time{100},
                                  model::range_logical_count{1},
                                  item_count{2}},
                                 budget,
                                 work,
                                 origin,
                                 codec::input_boundary::complete)
                                 .get();
            if (!owned) {
                ASSERT_FALSE(scanned.has_value());
                EXPECT_EQ(scanned.error(), owned.error());
            }
        }
    }
}

TEST(
  RecordScanTest, MetadataAllowanceDoesNotAccumulateWithRecordOrHeaderCount) {
    std::string wire;
    for (std::uint32_t i = 0; i < 4096; ++i) {
        // Null fields, one empty-name/null-value header, unsigned slot delta.
        std::string logical;
        auto n = i;
        do {
            auto b = n & 127U;
            n >>= 7U;
            logical.push_back(static_cast<char>(b | (n != 0 ? 128U : 0U)));
        } while (n != 0);
        wire.push_back(static_cast<char>(7 + logical.size()));
        wire += "\x00\x00"sv;
        wire += logical;
        wire += "\x01\x01\x01\x00\x01"sv;
    }
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto budget = memory();
    budget.metadata_remaining = byte_count{65536};
    auto scanner = model::record_region_scanner::make(
                     bytes(wire, 4096), expected(4096, 4096), budget, work)
                     .get()
                     .value();
    const auto residual = scanner.remaining();
    for (std::size_t i = 0; i < 4096; ++i) {
        ASSERT_TRUE(scanner.next(work).get().value());
        EXPECT_EQ(scanner.current()->fields.logical_delta.value(), i);
        EXPECT_EQ(scanner.current()->headers().size(), 1U);
        EXPECT_EQ(scanner.remaining(), residual);
    }
    EXPECT_TRUE(scanner.complete());
    const auto actual = scanner.bytes().allocation_cost(charge).value();
    EXPECT_LT(
      actual.descriptors.value() + actual.share_controls.value()
        + sizeof(scanner),
      65536U);
    scanner.close(work).get();
}

TEST(RecordScanTest, OnlyTemporaryBodyMetadataIsNeededForManyHeaders) {
    std::string wire{"\x86\x01\x00\x00\x00\x01\x01\x40"sv};
    for (int i = 0; i < 64; ++i)
        wire += "\x00\x01"sv;
    auto source = bytes(wire);
    fragmented_buffer_parser cost_parser{source.share()};
    auto admitted = codec::reserve_decode_input(
                      cost_parser, codec::limits::defaults(), memory())
                      .value();
    const auto alias = cost_parser
                         .next_buffer_allocation_cost(
                           cost_parser.total_bytes(), charge)
                         .value()
                         .descriptors;
    cost_parser.skip(byte_count{2}).value();
    const auto child = cost_parser
                         .next_buffer_allocation_cost(byte_count{134}, charge)
                         .value()
                         .descriptors;
    auto budget = memory();
    budget.metadata_remaining = byte_count{
      memory().metadata_remaining.value() - admitted.metadata_remaining.value()
      + alias.value() + child.value()};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto scanner = model::record_region_scanner::make(
                     std::move(source), expected(1, 64), budget, work)
                     .get()
                     .value();
    EXPECT_TRUE(scanner.next(work).get().value());
    EXPECT_EQ(scanner.current()->headers().size(), 64U);
    EXPECT_TRUE(scanner.complete());
}

TEST(RecordScanTest, InvalidInputContextsBudgetsAndOriginsFailBeforeIteration) {
    for (int mode = 0; mode != 8; ++mode) {
        auto target = expected();
        auto budget = memory();
        auto context = origin;
        codec::limits_config config;
        if (mode == 0) target.original_count = model::range_logical_count{};
        if (mode == 1) target.retained_count = item_count{};
        if (mode == 2) target.kind = static_cast<model::record_region_kind>(99);
        if (mode == 3) budget.charge = nullptr;
        if (mode == 4) budget.metadata_remaining = byte_count{};
        if (mode == 5)
            config.max_work_bytes = byte_count{
              4U * sizeof(model::record_layout) - 1U};
        if (mode == 6) context.origin = UINT64_MAX - 6U;
        if (mode == 7) target.header_count = item_count{4097};
        seastar::abort_source abort;
        codec::cooperative_work work{
          codec::limits::make(config).value(), abort};
        auto source = bytes(null_record);
        error(
          model::record_region_scanner::make(
            std::move(source), target, budget, work, context)
            .get(),
          mode == 4 || mode == 5 || mode == 7 ? errc::resource_exhausted
                                              : errc::invalid_argument);
        // NOLINTNEXTLINE(bugprone-use-after-move) -- empty donor contract.
        EXPECT_TRUE(source.empty());
    }
}

TEST(RecordScanTest, AbortAndMoveInvalidateFurtherIterationWithoutLosingBytes) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto scanner = model::record_region_scanner::make(
                     bytes(full_record), expected(1, 2), memory(), work)
                     .get()
                     .value();
    auto moved = std::move(scanner);
    // NOLINTNEXTLINE(bugprone-use-after-move) -- moved scanner is terminal.
    error(scanner.next(work).get(), errc::closed);
    abort.request_abort();
    error(moved.next(work).get(), errc::aborted);
    EXPECT_FALSE(moved.complete());
    EXPECT_TRUE(moved.bytes().content_equals(full_record));
    moved.close(work).get();
    EXPECT_TRUE(moved.bytes().empty());
}

TEST(RecordScanTest, ReachedAllocationFailuresLeaveNoReusablePartialScan) {
#if !defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    GTEST_SKIP() << "ordinary allocation injection is disabled";
#else
    static_cast<void>(kwaque::error_category());
    bool failed_once = false, succeeded = false;
    for (std::uint64_t ordinal = 0; ordinal < 64 && !succeeded; ++ordinal) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto scanner = model::record_region_scanner::make(
                         bytes(full_record, 1), expected(1, 2), memory(), work)
                         .get()
                         .value();
        auto& injector = seastar::memory::local_failure_injector();
        injector.fail_after(ordinal);
        bool threw = false, reached = false;
        try {
            auto result = scanner.next(work).get();
            reached = injector.failed();
            injector.cancel();
            ASSERT_TRUE(result.has_value());
            succeeded = true;
        } catch (const std::bad_alloc&) {
            reached = injector.failed();
            threw = true;
        } catch (...) {
            injector.cancel();
            throw;
        }
        injector.cancel();
        if (threw) {
            EXPECT_TRUE(reached);
            failed_once = true;
            EXPECT_FALSE(scanner.complete());
            // Outer frame allocation failure precedes entry; close works in
            // either state and joins all retained ownership.
            EXPECT_EQ(scanner.current(), nullptr);
        } else
            EXPECT_FALSE(reached);
        scanner.close(work).get();
    }
    EXPECT_TRUE(failed_once);
    EXPECT_TRUE(succeeded);
#endif
}
} // namespace
