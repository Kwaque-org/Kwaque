#include "src/codec/crc32c.h"
#include "src/protocol/tests/frame_test_support.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/preempt.hh>
#include <seastar/util/alloc_failure_injector.hh>
#include <seastar/util/later.hh>

#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <exception>
#include <new>
#include <optional>
#include <variant>

namespace {
using namespace kwaque;
namespace protocol = kwaque::protocol;
namespace fixture = protocol::testing::frame_fixture;
using bytes::fragmented_buffer;
using bytes::fragmented_buffer_parser;

codec::decode_budget reserve(
  const fragmented_buffer_parser& input,
  const codec::cooperative_work& work,
  codec::field_context context = fixture::context) {
    return codec::reserve_decode_input(
             input,
             work.policy(),
             {fixture::parent_budget,
              work.policy().config().max_metadata_bytes,
              fixture::charge},
             context)
      .value();
}
auto decode(
  fragmented_buffer_parser& input,
  codec::cooperative_work& work,
  codec::input_boundary boundary = codec::input_boundary::open) {
    return protocol::decode_frame(
             input,
             fixture::bounds,
             reserve(input, work),
             work,
             fixture::context,
             boundary)
      .get();
}
void expect_error(const auto& result, errc code) {
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), code);
}

TEST(FrameDecodeTest, EverySplitCommitsOneFrameAndThePayloadOutlivesItsInput) {
    const auto wire = fixture::wire("abc", fixture::extension());
    const auto combined = "pre" + wire + wire;
    for (std::size_t cut = 0; cut <= combined.size(); ++cut) {
        std::optional<protocol::framed_payload> owned;
        {
            fragmented_buffer_parser input{fixture::split_at(combined, cut)};
            ASSERT_TRUE(input.skip(byte_count{3}).has_value());
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            auto result = decode(input, work);
            ASSERT_TRUE(result.has_value());
            ASSERT_TRUE(
              std::holds_alternative<protocol::framed_payload>(*result));
            owned.emplace(
              std::get<protocol::framed_payload>(std::move(*result)));
            EXPECT_EQ(input.bytes_consumed(), byte_count{3 + wire.size()});
            EXPECT_EQ(input.checkpoint_depth(), 0U);
            std::string following(wire.size(), '\0');
            ASSERT_TRUE(input.peek_to(following).has_value());
            EXPECT_EQ(following, wire);
        }
        ASSERT_TRUE(owned.has_value());
        EXPECT_TRUE(owned->payload.content_equals("abc"));
        EXPECT_EQ(owned->header.metadata, fixture::metadata);
    }
}

TEST(FrameDecodeTest, EveryShortSnapshotReportsItsNextCheckBoundary) {
    const auto wire = fixture::wire("abcdef", fixture::extension());
    for (std::size_t cut = 0; cut < wire.size(); ++cut) {
        for (auto boundary :
             {codec::input_boundary::open, codec::input_boundary::complete}) {
            fragmented_buffer_parser input{
              fixture::fragmented("pre" + wire.substr(0, cut), 1)};
            ASSERT_TRUE(input.skip(byte_count{3}).has_value());
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            const auto result = decode(input, work, boundary);
            if (boundary == codec::input_boundary::complete)
                expect_error(result, errc::malformed_data);
            else {
                ASSERT_TRUE(result.has_value());
                ASSERT_TRUE(
                  std::holds_alternative<protocol::need_more>(*result));
                const std::size_t target = cut < 48   ? 48
                                           : cut < 57 ? 57
                                                      : wire.size();
                EXPECT_EQ(
                  std::get<protocol::need_more>(*result).additional_bytes,
                  byte_count{target - cut});
            }
            EXPECT_EQ(input.bytes_consumed(), byte_count{3});
            EXPECT_EQ(input.checkpoint_depth(), 0U);
        }
    }
}

TEST(FrameDecodeTest, NoPartialPayloadChecksumOrStaleHeaderStateIsRetained) {
    auto wire = fixture::wire(std::string(200, 'x'));
    wire[48] ^= 1;
    fragmented_buffer_parser partial{
      fixture::fragmented(wire.substr(0, 100), 7)};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto memory = reserve(partial, work);
    for (unsigned retry = 0; retry < 3; ++retry) {
        const auto result
          = protocol::decode_frame(
              partial, fixture::bounds, memory, work, fixture::context)
              .get();
        ASSERT_TRUE(result.has_value());
        ASSERT_TRUE(std::holds_alternative<protocol::need_more>(*result));
        EXPECT_EQ(
          std::get<protocol::need_more>(*result).additional_bytes,
          byte_count{148});
        EXPECT_EQ(partial.bytes_consumed(), byte_count{});
    }
    fragmented_buffer_parser complete{fixture::fragmented(wire, 7)};
    expect_error(decode(complete, work), errc::corrupt_data);
    auto changed = fixture::wire("abc");
    fixture::put_u16(changed, 4, 2);
    fixture::repair(changed);
    fragmented_buffer_parser replacement{fixture::fragmented(changed, 7)};
    expect_error(decode(replacement, work), errc::unsupported_format);
}

TEST(
  FrameDecodeTest, EmptyPayloadIsFramingOnlyAndAdjacentFramesStayIndependent) {
    const auto first = fixture::wire("");
    auto second = fixture::wire("x");
    second.back() ^= 1;
    fragmented_buffer_parser input{fixture::fragmented(first + second, 1)};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto memory = reserve(input, work);
    const auto result
      = protocol::decode_frame(
          input, fixture::bounds, memory, work, fixture::context)
          .get();
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(std::holds_alternative<protocol::framed_payload>(*result));
    EXPECT_TRUE(std::get<protocol::framed_payload>(*result).payload.empty());
    EXPECT_EQ(input.bytes_consumed(), byte_count{48});
    expect_error(
      protocol::decode_frame(
        input, fixture::bounds, memory, work, fixture::context)
        .get(),
      errc::corrupt_data);
    EXPECT_EQ(input.bytes_consumed(), byte_count{48});
}

TEST(FrameDecodeTest, SevenMarksAllowOneFrameButEightRejectWithoutMutation) {
    for (const auto depth : {7U, 8U}) {
        fragmented_buffer_parser input{
          fixture::fragmented("pre" + fixture::wire(), 1)};
        for (unsigned i = 0; i < depth; ++i)
            ASSERT_TRUE(input.push_checkpoint().has_value());
        ASSERT_TRUE(input.skip(byte_count{3}).has_value());
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        const auto result = decode(input, work);
        if (depth == 8)
            expect_error(result, errc::resource_exhausted);
        else {
            ASSERT_TRUE(result.has_value());
            ASSERT_TRUE(
              std::holds_alternative<protocol::framed_payload>(*result));
        }
        EXPECT_EQ(input.checkpoint_depth(), depth);
        EXPECT_EQ(input.bytes_consumed(), byte_count{depth == 7 ? 54U : 3U});
        ASSERT_TRUE(input.rollback().has_value());
        EXPECT_EQ(input.bytes_consumed(), byte_count{});
    }
}

TEST(FrameDecodeTest, ResultBudgetExcludesOnlyPersistentPayloadDescriptors) {
    fragmented_buffer_parser input{fixture::fragmented(fixture::wire(), 1)};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto memory = reserve(input, work);
    auto result = protocol::decode_frame(
                    input, fixture::bounds, memory, work, fixture::context)
                    .get();
    ASSERT_TRUE(result.has_value());
    auto* ready = std::get_if<protocol::framed_payload>(&*result);
    ASSERT_NE(ready, nullptr);
    const auto cost = ready->payload.allocation_cost(fixture::charge);
    ASSERT_TRUE(cost.has_value());
    EXPECT_EQ(
      memory.operation_remaining.value()
        - ready->remaining.operation_remaining.value(),
      cost->descriptors.value());
    EXPECT_EQ(
      memory.metadata_remaining.value()
        - ready->remaining.metadata_remaining.value(),
      cost->descriptors.value());
    EXPECT_EQ(ready->remaining.charge, memory.charge);
}

TEST(FrameDecodeTest, HeaderAndPayloadErrorsKeepTheirPriorityAndOffsets) {
    auto wire = fixture::wire();
    wire[4] ^= 1;
    wire.back() ^= 1;
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    fragmented_buffer_parser input{fixture::fragmented("pre" + wire, 1)};
    ASSERT_TRUE(input.skip(byte_count{3}).has_value());
    const auto corrupt_header = decode(input, work);
    expect_error(corrupt_header, errc::corrupt_data);
    ASSERT_FALSE(corrupt_header.has_value());
    EXPECT_EQ(
      corrupt_header.error().byte_offset(), fixture::context.origin + 3 + 40);
    wire = fixture::wire();
    wire.back() ^= 1;
    fragmented_buffer_parser payload{fixture::fragmented("pre" + wire, 1)};
    ASSERT_TRUE(payload.skip(byte_count{3}).has_value());
    const auto corrupt_payload = decode(payload, work);
    expect_error(corrupt_payload, errc::corrupt_data);
    ASSERT_FALSE(corrupt_payload.has_value());
    EXPECT_EQ(
      corrupt_payload.error().byte_offset(), fixture::context.origin + 3 + 44);
    EXPECT_EQ(payload.bytes_consumed(), byte_count{3});
}

TEST(FrameDecodeTest, QueuedCancellationDrainsBeforeRestoringTheParent) {
    fragmented_buffer_parser input{
      fixture::fragmented(fixture::wire(std::string(32768, 'x')), 67)};
    auto config = codec::limits_config{};
    config.max_work_bytes = protocol::frame_prefix_work_bytes;
    config.max_work_items = protocol::frame_prefix_work_items;
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::make(config).value(), abort};
    const auto memory = reserve(input, work);
    const auto deadline = std::chrono::steady_clock::now()
                          + std::chrono::seconds{2};
    while (!seastar::need_preempt()
           && std::chrono::steady_clock::now() < deadline) {
    }
    ASSERT_TRUE(seastar::need_preempt());
    bool observed = false;
    auto observer = seastar::yield().then([&] {
        observed = true;
        abort.request_abort();
    });
    std::optional<protocol::frame_read_result<protocol::framed_payload>>
      outcome;
    std::exception_ptr exception;
    bool pending = false;
    try {
        auto future = protocol::decode_frame(
          input, fixture::bounds, memory, work, fixture::context);
        pending = !future.available();
        outcome.emplace(future.get());
    } catch (...) {
        exception = std::current_exception();
    }
    const bool during_work = observed;
    observer.get();
    if (exception) std::rethrow_exception(exception);
    EXPECT_TRUE(pending);
    EXPECT_TRUE(during_work);
    ASSERT_TRUE(outcome.has_value());
    expect_error(*outcome, errc::aborted);
    EXPECT_EQ(input.bytes_consumed(), byte_count{});
    EXPECT_EQ(input.checkpoint_depth(), 0U);
}

TEST(FrameDecodeTest, AllocationFailureAtEveryAliasLeavesTheWholeFrameUnread) {
#if !defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    GTEST_SKIP() << "allocation failure injection is disabled";
#else
    static_cast<void>(kwaque::error_category());
    codec::crc32c warm;
    const std::array<char, 65> warm_bytes{};
    warm.extend(warm_bytes);
    unsigned failures = 0;
    bool completed = false;
    for (std::uint64_t ordinal = 0; ordinal < 256; ++ordinal) {
        fragmented_buffer_parser input{
          fixture::fragmented("pre" + fixture::wire("xyz"), 1)};
        ASSERT_TRUE(input.skip(byte_count{3}).has_value());
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        const auto memory = reserve(input, work);
        std::optional<protocol::frame_read_result<protocol::framed_payload>>
          result;
        bool allocation_failed = false;
        auto& injector = seastar::memory::local_failure_injector();
        injector.fail_after(ordinal);
        try {
            result.emplace(
              protocol::decode_frame(
                input, fixture::bounds, memory, work, fixture::context)
                .get());
        } catch (const std::bad_alloc&) {
            allocation_failed = true;
        } catch (...) {
            injector.cancel();
            throw;
        }
        const bool injected = injector.failed();
        injector.cancel();
        EXPECT_EQ(input.checkpoint_depth(), 0U);
        if (injected) {
            ++failures;
            EXPECT_TRUE(allocation_failed);
            EXPECT_FALSE(result.has_value());
            EXPECT_EQ(input.bytes_consumed(), byte_count{3});
        } else {
            ASSERT_TRUE(result.has_value());
            ASSERT_TRUE(result->has_value());
            ASSERT_TRUE(
              std::holds_alternative<protocol::framed_payload>(**result));
            EXPECT_EQ(input.bytes_consumed(), byte_count{54});
            completed = true;
            break;
        }
    }
    EXPECT_GT(failures, 0U);
    EXPECT_TRUE(completed);
#endif
}

TEST(FrameDecodeTest, TerminalCoordinatesDoNotDoubleCountHeaderOrChildOffsets) {
    const auto wire = fixture::wire();
    fragmented_buffer_parser input{fixture::fragmented(wire, 1)};
    auto context = fixture::context;
    context.origin = UINT64_MAX - wire.size();
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto result
      = protocol::decode_frame(
          input, fixture::bounds, reserve(input, work, context), work, context)
          .get();
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(std::holds_alternative<protocol::framed_payload>(*result));
    EXPECT_TRUE(input.at_end());
    fragmented_buffer_parser empty;
    context.origin = UINT64_MAX;
    auto incomplete
      = protocol::decode_frame(
          empty, fixture::bounds, reserve(empty, work, context), work, context)
          .get();
    ASSERT_TRUE(incomplete.has_value());
    ASSERT_TRUE(std::holds_alternative<protocol::need_more>(*incomplete));
    EXPECT_EQ(
      std::get<protocol::need_more>(*incomplete).additional_bytes,
      byte_count{48});
}

TEST(FrameDecodeTest, ExhaustedAliasesAndInitialAbortLeaveNoPartialFrame) {
    fragmented_buffer_parser input{fixture::fragmented(fixture::wire(), 1)};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto memory = reserve(input, work);
    auto denied = memory;
    denied.metadata_remaining = byte_count{};
    expect_error(
      protocol::decode_frame(input, fixture::bounds, denied, work).get(),
      errc::resource_exhausted);
    denied = memory;
    denied.operation_remaining = byte_count{};
    expect_error(
      protocol::decode_frame(input, fixture::bounds, denied, work).get(),
      errc::resource_exhausted);
    denied = memory;
    denied.charge = nullptr;
    expect_error(
      protocol::decode_frame(input, fixture::bounds, denied, work).get(),
      errc::invalid_argument);
    abort.request_abort();
    expect_error(
      protocol::decode_frame(input, fixture::bounds, memory, work).get(),
      errc::aborted);
    EXPECT_EQ(input.bytes_consumed(), byte_count{});
    EXPECT_EQ(input.checkpoint_depth(), 0U);
}

} // namespace
