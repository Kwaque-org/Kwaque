#include "src/base/error.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/bytes/fragmented_buffer_builder.h"
#include "src/bytes/fragmented_buffer_parser.h"
#include "src/codec/error.h"
#include "src/codec/header_extensions.h"
#include "src/codec/integer.h"
#include "src/codec/limits.h"
#include "src/codec/tests/header_extensions_test_support.h"
#include "src/codec/transaction.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/util/alloc_failure_injector.hh>
#include <seastar/util/later.hh>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <limits>
#include <new>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

using namespace std::literals;
namespace codec = kwaque::codec;
using codec::header_extension_field;
using kwaque::byte_count;
using kwaque::errc;
using kwaque::item_count;
using kwaque::bytes::fragmented_buffer;
using kwaque::bytes::fragmented_buffer_builder;
using kwaque::bytes::fragmented_buffer_parser;

constexpr codec::field_context context{.origin = 100, .family = 7};
constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();

void append_little(std::string& bytes, std::uint64_t value, unsigned width) {
    for (unsigned octet = 0; octet < width; ++octet) {
        bytes.push_back(static_cast<char>((value >> (octet * 8U)) & 0xffU));
    }
}

// Literal wire construction is independent from the fixture writer and reader.
std::string extension_prefix(
  std::uint16_t tag, std::uint16_t flags = 0, std::uint32_t length = 0) {
    std::string encoded;
    append_little(encoded, tag, 2);
    append_little(encoded, flags, 2);
    append_little(encoded, length, 4);
    return encoded;
}

fragmented_buffer split_bytes(std::string_view bytes, std::size_t chunk = 0) {
    if (chunk == 0 || bytes.empty()) {
        return fragmented_buffer::copy_of(bytes).value();
    }
    std::vector<seastar::temporary_buffer<char>> fragments;
    for (std::size_t offset = 0; offset < bytes.size(); offset += chunk) {
        const auto current = bytes.substr(offset, chunk);
        seastar::temporary_buffer<char> fragment{current.size()};
        std::ranges::copy(current, fragment.get_write());
        fragments.push_back(std::move(fragment));
    }
    return fragmented_buffer::copy_from_fragments(fragments).value();
}

std::string unread(const fragmented_buffer_parser& input) {
    std::string encoded(input.bytes_remaining().value(), '\0');
    input.peek_to(std::span<char>{encoded}).value();
    return encoded;
}

codec::result<item_count> scan_owned(
  fragmented_buffer_parser& input,
  byte_count bytes,
  codec::cooperative_work& work,
  byte_count fixed_prefix = byte_count{32},
  codec::field_context coordinates = context) {
    const auto depth = input.checkpoint_depth();
    input.push_checkpoint().value();
    codec::detail::parser_transaction_guard transaction{input, depth};
    auto scanned = codec::scan_header_extensions_in_transaction(
                     input, bytes, fixed_prefix, work, coordinates)
                     .get();
    if (!scanned) {
        return scanned;
    }
    if (auto ready = work.poll(); !ready) {
        return codec::failure(ready.error());
    }
    transaction.commit();
    return scanned;
}

void expect_error(
  const codec::result<item_count>& value,
  errc reason,
  header_extension_field field,
  std::uint64_t offset) {
    ASSERT_FALSE(value.has_value());
    EXPECT_EQ(
      value.error(),
      (codec::error{
        reason, context.family, static_cast<std::uint16_t>(field), offset}));
}

TEST(HeaderExtensionsTest, EmptyRegionConsumesPositiveWorkAndNoFollowingBytes) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    fragmented_buffer_parser input{split_bytes("body"sv)};
    const auto scanned = scan_owned(input, byte_count{}, work);
    ASSERT_TRUE(scanned.has_value());
    EXPECT_EQ(*scanned, item_count{});
    EXPECT_EQ(input.bytes_consumed(), byte_count{});
    EXPECT_EQ(input.checkpoint_depth(), 0U);
    EXPECT_EQ(unread(input), "body");
    EXPECT_EQ(work.items_remaining(), item_count{255});
}

TEST(HeaderExtensionsTest, MissingCallerMarkRejectsWithoutAdvancement) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    fragmented_buffer_parser input{split_bytes(extension_prefix(1))};
    const auto scanned = codec::scan_header_extensions_in_transaction(
                           input, byte_count{8}, byte_count{32}, work, context)
                           .get();
    expect_error(
      scanned, errc::invalid_argument, header_extension_field::region, 100);
    EXPECT_EQ(input.bytes_consumed(), byte_count{});
    EXPECT_EQ(input.checkpoint_depth(), 0U);
}

TEST(HeaderExtensionsTest, OptionalValuesSkipExactlyAcrossEverySmallSplit) {
    const auto region = extension_prefix(2, 0, 3) + "abc"
                        + extension_prefix(5, 0, 2) + "de";
    for (std::size_t chunk = 1; chunk <= region.size(); ++chunk) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        fragmented_buffer_parser input{
          split_bytes("p" + region + "body", chunk)};
        input.skip(byte_count{1}).value();
        const auto scanned = scan_owned(input, byte_count{region.size()}, work);
        ASSERT_TRUE(scanned.has_value());
        EXPECT_EQ(*scanned, item_count{2});
        EXPECT_EQ(input.bytes_consumed(), byte_count{1 + region.size()});
        EXPECT_EQ(unread(input), "body");
    }
}

TEST(HeaderExtensionsTest, EmptyValuesAndMaximumTagDoNotWrapOrStall) {
    const auto region = extension_prefix(1) + extension_prefix(0xfffe)
                        + extension_prefix(0xffff);
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    fragmented_buffer_parser input{split_bytes(region, 1)};
    const auto scanned = scan_owned(input, byte_count{region.size()}, work);
    ASSERT_TRUE(scanned.has_value());
    EXPECT_EQ(*scanned, item_count{3});
    EXPECT_TRUE(input.at_end());
}

TEST(HeaderExtensionsTest, ErrorPrecedencePreservesTheOwningTransaction) {
    struct bad_case final {
        std::string bytes;
        errc reason;
        header_extension_field field;
        std::uint64_t offset;
    };
    const std::array cases{
      bad_case{
        extension_prefix(0, 3, 0xffffffff),
        errc::malformed_data,
        header_extension_field::tag,
        101},
      bad_case{
        extension_prefix(1, 2, 0xffffffff),
        errc::unsupported_format,
        header_extension_field::flags,
        103},
      bad_case{
        extension_prefix(1, 1, 0xffffffff),
        errc::malformed_data,
        header_extension_field::value,
        109},
      bad_case{
        extension_prefix(1, 1),
        errc::unsupported_format,
        header_extension_field::tag,
        101},
      bad_case{
        extension_prefix(5) + extension_prefix(5, 3, 0xffffffff),
        errc::malformed_data,
        header_extension_field::tag,
        109},
      bad_case{
        extension_prefix(5) + extension_prefix(2, 3, 0xffffffff),
        errc::malformed_data,
        header_extension_field::tag,
        109},
      bad_case{
        extension_prefix(1, 0, 2) + "x",
        errc::malformed_data,
        header_extension_field::value,
        110}};
    for (const auto& value : cases) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        fragmented_buffer_parser input{
          split_bytes("p" + value.bytes + "body", 1)};
        input.skip(byte_count{1}).value();
        const auto scanned = scan_owned(
          input, byte_count{value.bytes.size()}, work);
        expect_error(scanned, value.reason, value.field, value.offset);
        EXPECT_EQ(input.bytes_consumed(), byte_count{1});
        EXPECT_EQ(input.checkpoint_depth(), 0U);
        EXPECT_EQ(unread(input), value.bytes + "body");
    }
}

TEST(HeaderExtensionsTest, EveryIncompletePrefixStopsAtItsDeclaredHeaderEnd) {
    const auto prefix = extension_prefix(1);
    for (std::size_t cut = 1; cut < prefix.size(); ++cut) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        const auto partial = prefix.substr(0, cut);
        fragmented_buffer_parser input{split_bytes(partial + "body", 1)};
        const auto scanned = scan_owned(input, byte_count{cut}, work);
        expect_error(
          scanned,
          errc::malformed_data,
          header_extension_field::tag,
          100 + cut);
        EXPECT_EQ(input.bytes_consumed(), byte_count{});
        EXPECT_EQ(unread(input), partial + "body");
    }
}

TEST(HeaderExtensionsTest, CompleteRegionAvailabilityPrecedesItsContents) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    fragmented_buffer_parser input{split_bytes(extension_prefix(0))};
    const auto scanned = scan_owned(input, byte_count{9}, work);
    expect_error(
      scanned, errc::malformed_data, header_extension_field::region, 108);
    EXPECT_EQ(input.bytes_consumed(), byte_count{});
}

TEST(HeaderExtensionsTest, SixtyFourEntriesFitAndSixtyFifthRejects) {
    std::string region;
    for (std::uint16_t tag = 1; tag <= 65; ++tag) {
        region += extension_prefix(tag);
    }
    for (const std::uint64_t count : {64U, 65U}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        fragmented_buffer_parser input{split_bytes(region, 1)};
        const auto scanned = scan_owned(input, byte_count{count * 8U}, work);
        if (count == 64) {
            ASSERT_TRUE(scanned.has_value());
            EXPECT_EQ(*scanned, item_count{64});
            EXPECT_EQ(input.bytes_remaining(), byte_count{8});
        } else {
            expect_error(
              scanned,
              errc::resource_exhausted,
              header_extension_field::count,
              100 + 64 * 8);
            EXPECT_EQ(input.bytes_consumed(), byte_count{});
        }
    }
}

TEST(HeaderExtensionsTest, TruncatedNextPrefixPrecedesTheCountCeiling) {
    auto config = codec::limits_config{};
    config.max_extensions = item_count{1};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::make(config).value(), abort};
    const auto region = extension_prefix(1) + "x";
    fragmented_buffer_parser input{split_bytes(region)};
    expect_error(
      scan_owned(input, byte_count{region.size()}, work),
      errc::malformed_data,
      header_extension_field::tag,
      109);
    EXPECT_EQ(input.bytes_consumed(), byte_count{});
}

TEST(HeaderExtensionsTest, HeaderCapIncludesTheIndependentFixedPrefix) {
    for (const std::uint64_t fixed : {32U, 48U}) {
        const std::uint64_t extent = 4096 - fixed;
        const auto region = extension_prefix(
                              1, 0, static_cast<std::uint32_t>(extent - 8))
                            + std::string(extent - 8, 'x');
        for (const bool oversized : {false, true}) {
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            fragmented_buffer_parser input{split_bytes(region + "b", 8)};
            const auto scanned = scan_owned(
              input,
              byte_count{extent + (oversized ? 1U : 0U)},
              work,
              byte_count{fixed});
            if (oversized) {
                expect_error(
                  scanned,
                  errc::resource_exhausted,
                  header_extension_field::region,
                  100);
                EXPECT_EQ(input.bytes_consumed(), byte_count{});
            } else {
                ASSERT_TRUE(scanned.has_value());
                EXPECT_EQ(*scanned, item_count{1});
                EXPECT_EQ(unread(input), "b");
            }
        }
    }
}

TEST(HeaderExtensionsTest, CapFailureWinsMissingHeaderAvailability) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    fragmented_buffer_parser input;
    expect_error(
      scan_owned(input, byte_count{4065}, work),
      errc::resource_exhausted,
      header_extension_field::region,
      100);
}

TEST(
  HeaderExtensionsTest, InvalidFixedAndOverflowedCoordinatesRejectBeforeRead) {
    for (const byte_count fixed : {byte_count{31}, byte_count{maximum}}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        fragmented_buffer_parser input{split_bytes(extension_prefix(1))};
        expect_error(
          scan_owned(input, byte_count{8}, work, fixed),
          errc::invalid_argument,
          header_extension_field::region,
          100);
        EXPECT_EQ(input.bytes_consumed(), byte_count{});
    }
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    fragmented_buffer_parser input{split_bytes(extension_prefix(1))};
    const auto scanned = scan_owned(
      input,
      byte_count{8},
      work,
      byte_count{32},
      codec::field_context{.origin = maximum - 7, .family = context.family});
    expect_error(
      scanned,
      errc::invalid_argument,
      header_extension_field::region,
      maximum - 7);
    EXPECT_EQ(input.bytes_consumed(), byte_count{});
}

TEST(HeaderExtensionsTest, EighthOwnedMarkWorksAndAllExistingMarksSurvive) {
    for (const bool malformed : {false, true}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        const auto region = extension_prefix(malformed ? 0 : 1);
        fragmented_buffer_parser input{split_bytes("p" + region)};
        for (std::size_t depth = 0; depth < 7; ++depth) {
            input.push_checkpoint().value();
        }
        input.skip(byte_count{1}).value();
        const auto scanned = scan_owned(input, byte_count{region.size()}, work);
        EXPECT_EQ(scanned.has_value(), !malformed);
        EXPECT_EQ(input.checkpoint_depth(), 7U);
        EXPECT_EQ(input.bytes_consumed(), byte_count{malformed ? 1U : 9U});
        for (std::size_t depth = 7; depth != 0; --depth) {
            input.rollback().value();
            EXPECT_EQ(input.checkpoint_depth(), depth - 1);
            EXPECT_EQ(input.bytes_consumed(), byte_count{});
        }
    }
}

TEST(HeaderExtensionsTest, SpeculativeFailureLeavesTheCallersMarkUntouched) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    fragmented_buffer_parser input{
      split_bytes(extension_prefix(1) + extension_prefix(0))};
    input.push_checkpoint().value();
    const auto scanned = codec::scan_header_extensions_in_transaction(
                           input, byte_count{16}, byte_count{32}, work, context)
                           .get();
    ASSERT_FALSE(scanned.has_value());
    EXPECT_EQ(input.checkpoint_depth(), 1U);
    EXPECT_EQ(input.bytes_consumed(), byte_count{16});
    input.rollback().value();
    EXPECT_EQ(input.bytes_consumed(), byte_count{});
}

TEST(HeaderExtensionsTest, NarrowedWorkSplitsValuesWithoutEnlargingTheQuantum) {
    auto config = codec::limits_config{};
    config.max_work_bytes = byte_count{32};
    config.max_work_items = item_count{16};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::make(config).value(), abort};
    const auto region = extension_prefix(1, 0, 4000) + std::string(4000, 'x');
    fragmented_buffer_parser input{split_bytes(region, 5)};
    const auto scanned = scan_owned(input, byte_count{region.size()}, work);
    ASSERT_TRUE(scanned.has_value());
    EXPECT_EQ(*scanned, item_count{1});
    EXPECT_TRUE(input.at_end());
    EXPECT_EQ(work.byte_quantum(), byte_count{32});
    EXPECT_EQ(work.item_quantum(), item_count{16});
}

TEST(HeaderExtensionsTest, ImpossiblePrefixLeafRejectsWithoutReadingItsFields) {
    for (const bool small_bytes : {false, true}) {
        auto config = codec::limits_config{};
        config.max_work_bytes = byte_count{small_bytes ? 31U : 32U};
        config.max_work_items = item_count{small_bytes ? 16U : 15U};
        seastar::abort_source abort;
        codec::cooperative_work work{
          codec::limits::make(config).value(), abort};
        fragmented_buffer_parser input{split_bytes(extension_prefix(0))};
        expect_error(
          scan_owned(input, byte_count{8}, work),
          errc::resource_exhausted,
          header_extension_field::tag,
          100);
        EXPECT_EQ(input.bytes_consumed(), byte_count{});
    }
}

TEST(HeaderExtensionsTest, InitialAbortDoesNotReadOrAdvance) {
    seastar::abort_source abort;
    abort.request_abort();
    codec::cooperative_work work{codec::limits::defaults(), abort};
    fragmented_buffer_parser input{split_bytes(extension_prefix(0))};
    expect_error(
      scan_owned(input, byte_count{8}, work),
      errc::aborted,
      header_extension_field::region,
      100);
    EXPECT_EQ(input.bytes_consumed(), byte_count{});
}

TEST(HeaderExtensionsTest, QueuedAbortDuringSharedWorkRollsBackTheOwner) {
    bool observed_during_scan = false;
    const auto deadline = std::chrono::steady_clock::now()
                          + std::chrono::seconds{2};
    for (unsigned attempt = 0; attempt < 64 && !observed_during_scan
                               && std::chrono::steady_clock::now() < deadline;
         ++attempt) {
        auto config = codec::limits_config{};
        config.max_work_bytes = byte_count{32};
        config.max_work_items = item_count{16};
        seastar::abort_source abort;
        codec::cooperative_work work{
          codec::limits::make(config).value(), abort};
        const auto region = extension_prefix(1, 0, 4000)
                            + std::string(4000, 'x');
        fragmented_buffer_parser input{split_bytes(region, 5)};
        bool active = true;
        auto observer = seastar::yield().then([&] {
            observed_during_scan = active;
            abort.request_abort();
        });
        std::optional<codec::result<item_count>> scanned;
        std::exception_ptr failure;
        try {
            scanned.emplace(scan_owned(input, byte_count{region.size()}, work));
        } catch (...) {
            failure = std::current_exception();
        }
        active = false;
        observer.get();
        if (failure) {
            std::rethrow_exception(failure);
        }
        ASSERT_TRUE(scanned.has_value());
        if (observed_during_scan) {
            ASSERT_FALSE(scanned->has_value());
            EXPECT_EQ(scanned->error().code(), errc::aborted);
            EXPECT_EQ(input.bytes_consumed(), byte_count{});
            EXPECT_EQ(input.checkpoint_depth(), 0U);
        }
    }
    EXPECT_TRUE(observed_during_scan);
}

TEST(HeaderExtensionsTest, ConstructedScannerNeedsNoOrdinaryAllocation) {
#if !defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    GTEST_SKIP() << "allocation failure injection is not enabled";
#else
    // Native critical coroutine frames are outside ordinary allocation
    // injection. This checks subsequent buffer/cursor work, not those frames.
    for (const std::size_t fragment_bytes : {0U, 5U}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        const auto region = extension_prefix(1, 0, 4000)
                            + std::string(4000, 'x');
        fragmented_buffer_parser input{split_bytes(region, fragment_bytes)};
        std::optional<codec::result<item_count>> scanned;
        bool failed = false;
        auto& injector = seastar::memory::local_failure_injector();
        injector.fail_after(0);
        try {
            scanned.emplace(scan_owned(input, byte_count{region.size()}, work));
        } catch (const std::bad_alloc&) {
            failed = true;
        } catch (...) {
            injector.cancel();
            throw;
        }
        const auto injected = injector.failed();
        injector.cancel();
        EXPECT_FALSE(failed);
        EXPECT_FALSE(injected);
        EXPECT_EQ(input.checkpoint_depth(), 0U);
        ASSERT_TRUE(scanned.has_value());
        ASSERT_TRUE(scanned->has_value());
        EXPECT_EQ(**scanned, item_count{1});
        EXPECT_TRUE(input.at_end());
    }
#endif
}

TEST(HeaderExtensionsTest, FixtureWriterMatchesIndependentOrderedBytes) {
    const std::array extensions{
      codec::testing::header_extension{2, 0, std::span<const char>{"abc"sv}},
      codec::testing::header_extension{5, 0, std::span<const char>{"de"sv}}};
    fragmented_buffer_builder output;
    ASSERT_TRUE(
      codec::testing::append_header_extensions(
        output, extensions, byte_count{32}, codec::limits::defaults(), context)
        .has_value());
    auto encoded = output.finish();
    ASSERT_TRUE(encoded.has_value());
    EXPECT_TRUE(encoded->content_equals(
      "\x02\x00\x00\x00\x03\x00\x00\x00"
      "abc"
      "\x05\x00\x00\x00\x02\x00\x00\x00"
      "de"sv));
}

TEST(HeaderExtensionsTest, FixtureWriterEmptyListAppendsNothing) {
    fragmented_buffer_builder output;
    ASSERT_TRUE(
      codec::testing::append_header_extensions(
        output, {}, byte_count{32}, codec::limits::defaults(), context)
        .has_value());
    EXPECT_EQ(output.size(), byte_count{});
}

TEST(HeaderExtensionsTest, FixtureWriterRejectsDuplicateDescendingAndZeroTags) {
    for (const auto next : std::array<std::uint16_t, 3>{0, 2, 5}) {
        const std::array extensions{
          codec::testing::header_extension{5, 0, {}},
          codec::testing::header_extension{next, 0, {}}};
        fragmented_buffer_builder output;
        const auto written = codec::testing::append_header_extensions(
          output,
          extensions,
          byte_count{32},
          codec::limits::defaults(),
          context);
        ASSERT_FALSE(written.has_value());
        EXPECT_EQ(written.error().code(), errc::malformed_data);
        EXPECT_EQ(output.size(), byte_count{8});
    }
}

TEST(HeaderExtensionsTest, FixtureWriterBoundsFlagsLengthsAndHeaderCapacity) {
    const std::string payload(4057, 'x');
    const std::array extensions{
      codec::testing::header_extension{1, 0, std::span<const char>{payload}}};
    fragmented_buffer_builder output;
    const auto written = codec::testing::append_header_extensions(
      output, extensions, byte_count{32}, codec::limits::defaults(), context);
    ASSERT_FALSE(written.has_value());
    EXPECT_EQ(written.error().code(), errc::resource_exhausted);
    EXPECT_EQ(output.size(), byte_count{});

    const std::array reserved{codec::testing::header_extension{1, 2, {}}};
    const auto flags = codec::testing::append_header_extensions(
      output, reserved, byte_count{32}, codec::limits::defaults(), context);
    ASSERT_FALSE(flags.has_value());
    EXPECT_EQ(
      flags.error(),
      (codec::error{
        errc::unsupported_format,
        context.family,
        static_cast<std::uint16_t>(header_extension_field::flags),
        102}));
    EXPECT_EQ(output.size(), byte_count{});
}

TEST(HeaderExtensionsTest, FixtureWriterChecksCoordinatesBeforeFieldOffsets) {
    const std::array extensions{codec::testing::header_extension{1, 2, {}}};
    fragmented_buffer_builder output;
    const auto written = codec::testing::append_header_extensions(
      output,
      extensions,
      byte_count{32},
      codec::limits::defaults(),
      codec::field_context{.origin = maximum - 7, .family = context.family});
    ASSERT_FALSE(written.has_value());
    EXPECT_EQ(
      written.error(),
      (codec::error{
        errc::invalid_argument,
        context.family,
        static_cast<std::uint16_t>(header_extension_field::region),
        maximum - 7}));
    EXPECT_EQ(output.size(), byte_count{});

    const std::array value{
      codec::testing::header_extension{1, 0, std::span<const char>{"abc"sv}}};
    const auto overflowed_value = codec::testing::append_header_extensions(
      output,
      value,
      byte_count{32},
      codec::limits::defaults(),
      codec::field_context{.origin = maximum - 8, .family = context.family});
    ASSERT_FALSE(overflowed_value.has_value());
    EXPECT_EQ(
      overflowed_value.error(),
      (codec::error{
        errc::invalid_argument,
        context.family,
        static_cast<std::uint16_t>(header_extension_field::value_length),
        maximum - 4}));
    EXPECT_EQ(output.size(), byte_count{});
}

TEST(HeaderExtensionsTest, FixtureWriterPropagatesReachedAllocationExceptions) {
#if !defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    GTEST_SKIP() << "allocation failure injection is not enabled";
#else
    const std::array extensions{
      codec::testing::header_extension{1, 0, std::span<const char>{"abc"sv}}};
    bool observed_failure = false;
    bool observed_success = false;
    for (std::uint64_t ordinal = 0; ordinal < 32 && !observed_success;
         ++ordinal) {
        fragmented_buffer_builder output;
        std::optional<codec::result<void>> written;
        bool failed = false;
        auto& injector = seastar::memory::local_failure_injector();
        injector.fail_after(ordinal);
        try {
            written.emplace(
              codec::testing::append_header_extensions(
                output,
                extensions,
                byte_count{32},
                codec::limits::defaults(),
                context));
        } catch (const std::bad_alloc&) {
            failed = true;
        } catch (...) {
            injector.cancel();
            throw;
        }
        const bool injected = injector.failed();
        injector.cancel();
        if (failed) {
            EXPECT_TRUE(injected);
            observed_failure = true;
        } else {
            ASSERT_TRUE(written.has_value());
            ASSERT_TRUE(written->has_value());
            EXPECT_EQ(output.size(), byte_count{11});
            observed_success = true;
        }
    }
    EXPECT_TRUE(observed_failure);
    EXPECT_TRUE(observed_success);
#endif
}

} // namespace
