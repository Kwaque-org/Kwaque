#include "src/codec/crc32c.h"
#include "src/protocol/tests/frame_test_support.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/util/alloc_failure_injector.hh>
#include <seastar/util/later.hh>

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <new>
#include <optional>

namespace {
using namespace kwaque;
namespace protocol = kwaque::protocol;
namespace fixture = protocol::testing::frame_fixture;
using bytes::fragmented_buffer_parser;

void expect_error(const auto& result, errc code) {
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), code);
}

TEST(FrameHeaderTest, EveryHeaderSplitRestoresCallerMarksAndFollowingBytes) {
    const auto header = fixture::wire("", fixture::extension());
    const auto combined = "pre" + header + "following";
    for (std::size_t split = 0; split <= combined.size(); ++split) {
        for (const auto depth : {0U, 7U, 8U}) {
            fragmented_buffer_parser input{fixture::split_at(combined, split)};
            for (unsigned mark = 0; mark < depth; ++mark)
                ASSERT_TRUE(input.push_checkpoint().has_value());
            ASSERT_TRUE(input.skip(byte_count{3}).has_value());
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            const auto result = fixture::inspect(input, work);
            if (depth == 8) {
                expect_error(result, errc::resource_exhausted);
            } else {
                ASSERT_TRUE(result.has_value());
                EXPECT_EQ(result->metadata, fixture::metadata);
                EXPECT_EQ(result->header_bytes, byte_count{57});
                EXPECT_EQ(result->payload_bytes, byte_count{});
                EXPECT_EQ(result->payload_crc32c, 0U);
            }
            EXPECT_EQ(input.bytes_consumed(), byte_count{3});
            EXPECT_EQ(input.checkpoint_depth(), depth);
            std::string actual(combined.size() - 3, '\0');
            ASSERT_TRUE(input.peek_to(actual).has_value());
            EXPECT_EQ(actual, combined.substr(3));
        }
    }
}

TEST(FrameHeaderTest, EveryShortHeaderHasAnExactBoundaryError) {
    const auto header = fixture::wire("abc", fixture::extension());
    for (std::size_t size = 0; size < 57; ++size) {
        for (const auto boundary :
             {codec::input_boundary::open, codec::input_boundary::complete}) {
            fragmented_buffer_parser input{
              fixture::fragmented(header.substr(0, size), 1)};
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            const auto result = fixture::inspect(
              input, work, fixture::bounds, fixture::context, boundary);
            expect_error(
              result,
              boundary == codec::input_boundary::open ? errc::truncated_data
                                                      : errc::malformed_data);
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(
              result.error().byte_offset(), fixture::context.origin + size);
            EXPECT_EQ(input.bytes_consumed(), byte_count{});
        }
    }
}

TEST(FrameHeaderTest, HeaderInspectionDoesNotRequireOrValidateThePayload) {
    auto wire = fixture::wire();
    fixture::put_u32(wire, 12, 16'777'216);
    fixture::put_u32(wire, 44, 0xdeadbeef);
    fixture::repair(wire);
    wire.resize(48);
    fragmented_buffer_parser input{fixture::fragmented(wire, 1)};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto result = fixture::inspect(input, work);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->payload_bytes, byte_count{16'777'216});
    EXPECT_EQ(result->payload_crc32c, 0xdeadbeefU);
    EXPECT_EQ(input.bytes_consumed(), byte_count{});
}

TEST(FrameHeaderTest, RawHeaderCrcCoversEveryOctetIncludingItsStoredSlot) {
    const auto wire = fixture::wire("", fixture::extension());
    for (std::size_t byte = 0; byte < wire.size(); ++byte) {
        auto changed = wire;
        changed[byte] ^= 1;
        auto buffer = fixture::fragmented(changed, 1);
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        const auto result = protocol::verify_frame_header_crc(
                              buffer, work, fixture::context)
                              .get();
        expect_error(result, errc::corrupt_data);
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().byte_offset(), fixture::context.origin + 40);
    }
}

TEST(FrameHeaderTest, RepairedCrcReachesEachKindFlagAndStreamRule) {
    struct mutation {
        std::size_t offset;
        std::uint16_t value;
        errc code;
    };
    const std::array cases{
      mutation{4, 0, errc::unsupported_format},
      mutation{4, 2, errc::unsupported_format},
      mutation{6, 0, errc::malformed_data},
      mutation{6, 5, errc::unsupported_format},
      mutation{10, 1, errc::unsupported_format},
      mutation{6, 1, errc::malformed_data}};
    for (const auto test : cases) {
        auto wire = fixture::wire();
        fixture::put_u16(wire, test.offset, test.value);
        fragmented_buffer_parser corrupt{fixture::fragmented(wire, 1)};
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        expect_error(fixture::inspect(corrupt, work), errc::corrupt_data);
        fixture::repair(wire);
        fragmented_buffer_parser repaired{fixture::fragmented(wire, 1)};
        const auto result = fixture::inspect(repaired, work);
        expect_error(result, test.code);
        EXPECT_EQ(repaired.bytes_consumed(), byte_count{});
    }
    for (unsigned bit = 0; bit < 16; ++bit) {
        auto wire = fixture::wire();
        fixture::put_u16(wire, 10, static_cast<std::uint16_t>(1U << bit));
        fixture::repair(wire);
        fragmented_buffer_parser input{fixture::fragmented(wire, 7)};
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        expect_error(fixture::inspect(input, work), errc::unsupported_format);
    }
    constexpr std::array<std::uint16_t, 6> kinds{1, 2, 3, 4, 16, 17};
    for (auto kind : kinds) {
        auto wire = fixture::wire();
        fixture::put_u16(wire, 6, kind);
        fixture::put_u64(wire, 16, kind < 16 ? 0 : UINT64_MAX);
        fixture::repair(wire);
        fragmented_buffer_parser input{fixture::fragmented(wire, 3)};
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        ASSERT_TRUE(fixture::inspect(input, work).has_value());
    }
}

TEST(
  FrameHeaderTest, ControlCapRejectsBeforeWaitingForPayloadButAfterHeaderCrc) {
    auto wire = fixture::wire();
    fixture::put_u16(wire, 6, 1);
    fixture::put_u64(wire, 16, 0);
    fixture::put_u32(wire, 12, 65537);
    wire.resize(48);
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    fragmented_buffer_parser corrupt{fixture::fragmented(wire, 1)};
    expect_error(fixture::inspect(corrupt, work), errc::corrupt_data);
    fixture::repair(wire);
    fragmented_buffer_parser over{fixture::fragmented(wire, 1)};
    expect_error(fixture::inspect(over, work), errc::resource_exhausted);
    fixture::put_u32(wire, 12, 65536);
    fixture::repair(wire);
    fragmented_buffer_parser exact{fixture::fragmented(wire, 1)};
    ASSERT_TRUE(fixture::inspect(exact, work).has_value());
}

TEST(FrameHeaderTest, SharedExtensionGrammarAndBoundsApplyAtTheFrameOffset) {
    struct test_case {
        std::string extensions;
        errc code;
    };
    const std::array cases{
      test_case{fixture::extension(), errc::success},
      test_case{fixture::extension(0), errc::malformed_data},
      test_case{fixture::extension(7, 1), errc::unsupported_format},
      test_case{fixture::extension(7, 2), errc::unsupported_format},
      test_case{
        fixture::extension(7) + fixture::extension(7), errc::malformed_data},
      test_case{
        fixture::extension(7) + fixture::extension(6), errc::malformed_data},
      test_case{fixture::extension().substr(0, 8), errc::malformed_data},
      test_case{std::string(7, '\0'), errc::malformed_data}};
    for (const auto& test : cases) {
        auto wire = fixture::wire("payload-follows", test.extensions);
        fragmented_buffer_parser input{fixture::fragmented(wire, 1)};
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        const auto result = fixture::inspect(input, work);
        if (test.code == errc::success)
            ASSERT_TRUE(result.has_value());
        else
            expect_error(result, test.code);
        EXPECT_EQ(input.bytes_consumed(), byte_count{});
    }
    for (const unsigned count : {64U, 65U}) {
        std::string extensions;
        for (unsigned i = 1; i <= count; ++i)
            extensions += fixture::extension(
              static_cast<std::uint16_t>(i), 0, "");
        fragmented_buffer_parser input{
          fixture::fragmented(fixture::wire("", extensions), 1)};
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        const auto result = fixture::inspect(input, work);
        if (count == 64)
            ASSERT_TRUE(result.has_value());
        else
            expect_error(result, errc::resource_exhausted);
    }
    fragmented_buffer_parser maximum{fixture::fragmented(
      fixture::wire("", fixture::extension(7, 0, std::string(4040, 'x'))), 7)};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto result = fixture::inspect(maximum, work);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->header_bytes, byte_count{4096});
}

TEST(FrameHeaderTest, AliasBudgetAndCancellationPreserveTheParent) {
    fragmented_buffer_parser input{fixture::fragmented(fixture::wire(), 1)};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto memory
      = codec::reserve_decode_input(
          input,
          work.policy(),
          {fixture::parent_budget, byte_count{1'048'576}, fixture::charge})
          .value();
    auto exhausted = memory;
    exhausted.metadata_remaining = byte_count{};
    expect_error(
      protocol::inspect_frame_header(input, fixture::bounds, exhausted, work)
        .get(),
      errc::resource_exhausted);
    exhausted = memory;
    exhausted.operation_remaining = byte_count{};
    expect_error(
      protocol::inspect_frame_header(input, fixture::bounds, exhausted, work)
        .get(),
      errc::resource_exhausted);
    abort.request_abort();
    expect_error(
      protocol::inspect_frame_header(input, fixture::bounds, memory, work)
        .get(),
      errc::aborted);
    EXPECT_EQ(input.bytes_consumed(), byte_count{});
    EXPECT_EQ(input.checkpoint_depth(), 0U);
}

TEST(FrameHeaderTest, NativeAllocationFailuresRestorePositionAndMarks) {
#if !defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    GTEST_SKIP() << "allocation failure injection is disabled";
#else
    static_cast<void>(kwaque::error_category());
    codec::crc32c warm;
    const std::array<char, 65> warm_bytes{};
    warm.extend(warm_bytes);
    bool completed = false;
    unsigned failures = 0;
    for (std::uint64_t ordinal = 0; ordinal < 128; ++ordinal) {
        fragmented_buffer_parser input{fixture::fragmented(
          "pre" + fixture::wire("", fixture::extension()), 1)};
        ASSERT_TRUE(input.push_checkpoint().has_value());
        ASSERT_TRUE(input.skip(byte_count{3}).has_value());
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto memory
          = codec::reserve_decode_input(
              input,
              work.policy(),
              {fixture::parent_budget, byte_count{1'048'576}, fixture::charge})
              .value();
        std::optional<codec::result<protocol::frame_header>> result;
        bool allocation_failed = false;
        auto& injector = seastar::memory::local_failure_injector();
        injector.fail_after(ordinal);
        try {
            result.emplace(
              protocol::inspect_frame_header(
                input, fixture::bounds, memory, work)
                .get());
        } catch (const std::bad_alloc&) {
            allocation_failed = true;
        } catch (...) {
            injector.cancel();
            throw;
        }
        const bool injected = injector.failed();
        injector.cancel();
        EXPECT_EQ(input.bytes_consumed(), byte_count{3});
        EXPECT_EQ(input.checkpoint_depth(), 1U);
        if (injected) {
            ++failures;
            EXPECT_TRUE(allocation_failed);
            EXPECT_FALSE(result.has_value());
        } else {
            ASSERT_TRUE(result.has_value());
            ASSERT_TRUE(result->has_value());
            completed = true;
            break;
        }
    }
    EXPECT_GT(failures, 0U);
    EXPECT_TRUE(completed);
#endif
}

} // namespace
