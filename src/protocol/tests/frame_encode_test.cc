#include "src/bytes/fragmented_buffer_test_support.h"
#include "src/codec/crc32c.h"
#include "src/protocol/tests/frame_test_support.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/deleter.hh>
#include <seastar/core/preempt.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/util/alloc_failure_injector.hh>
#include <seastar/util/later.hh>

#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <exception>
#include <memory>
#include <new>
#include <optional>

namespace {
using namespace kwaque;
namespace protocol = kwaque::protocol;
namespace fixture = protocol::testing::frame_fixture;
using bytes::fragmented_buffer;
using bytes::fragmented_buffer_parser;

void expect_error(const auto& result, errc code) {
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), code);
}

void expect_consumed(const fragmented_buffer& body) {
    EXPECT_TRUE(body.empty());
    EXPECT_EQ(body.retained_bytes(), byte_count{});
    EXPECT_EQ(body.fragment_count(), 0U);
}

TEST(FrameEncodeTest, IndependentLiteralPinsEveryWireOffsetAndCrc) {
    using namespace std::literals;
    constexpr auto golden
      = "\x4b\x51\x57\x46\x01\x00\x10\x00\x30\x00\x00\x00\x03\x00\x00\x00"
        "\x01\x02\x03\x04\x05\x06\x07\x08\x11\x12\x13\x14\x15\x16\x17\x18"
        "\x21\x22\x23\x24\x25\x26\x27\x28\xbe\xdc\xee\x3b\xb7\x3f\x4b\x36"
        "\x61\x62\x63"sv;
    static_assert(golden.size() == 51);
    ASSERT_EQ(fixture::wire(), golden);
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto result = fixture::encode(fixture::fragmented("abc", 1), work);
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->content_equals(golden));
    const auto prefix = protocol::encode_frame_prefix(
      {fixture::metadata, byte_count{3}, 1005509822U, 910901175U},
      codec::limits::defaults(),
      fixture::bounds);
    ASSERT_TRUE(prefix.has_value());
    EXPECT_EQ(
      (std::string_view{prefix->data(), prefix->size()}), golden.substr(0, 48));
}

TEST(FrameEncodeTest, WritesAllSixKindsWithExactIndependentBytes) {
    constexpr std::array<std::uint16_t, 6> kinds{1, 2, 3, 4, 16, 17};
    for (auto kind : kinds) {
        auto metadata = fixture::metadata;
        metadata.kind = static_cast<protocol::frame_kind>(kind);
        if (kind < 16) metadata.stream = model::transport_stream_id{};
        auto expected = fixture::wire();
        fixture::put_u16(expected, 6, kind);
        fixture::put_u64(expected, 16, metadata.stream.value());
        fixture::repair(expected);
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto body = fixture::fragmented("abc", 1);
        const auto encoded = fixture::encode(std::move(body), work, metadata);
        ASSERT_TRUE(encoded.has_value());
        EXPECT_TRUE(encoded->content_equals(expected));
        // The consuming writer explicitly leaves an empty donor.
        // NOLINTNEXTLINE(bugprone-use-after-move)
        expect_consumed(body);
    }
}

TEST(FrameEncodeTest, EmptyAndFragmentedPayloadsKeepExactFraming) {
    for (const auto& text :
         {std::string{}, std::string{"abc"}, std::string(32768, 'x')}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        const auto encoded = fixture::encode(
          fixture::fragmented(text, 67), work);
        ASSERT_TRUE(encoded.has_value());
        EXPECT_TRUE(encoded->content_equals(fixture::wire(text)));
        EXPECT_EQ(encoded->size(), byte_count{48 + text.size()});
    }
}

TEST(FrameEncodeTest, PayloadBackingIsTransferredWithoutCopyingOrEarlyRelease) {
    bool released = false;
    auto storage = std::make_unique<char[]>(1);
    storage[0] = 'x';
    auto* raw = storage.get();
    auto deleter = seastar::make_deleter(
      [storage = std::move(storage), &released] noexcept {
          static_cast<void>(storage);
          released = true;
      });
    auto native = seastar::temporary_buffer<char>::maybe_unsafe_from_deleter(
      raw, 1, std::move(deleter));
    auto body = bytes::fragmented_buffer_test_access::adopt_fragment(
                  std::move(native), byte_count{1})
                  .value();
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto encoded = fixture::encode(std::move(body), work);
    ASSERT_TRUE(encoded.has_value());
    bool same_backing = false;
    for (const auto fragment : *encoded)
        same_backing |= fragment.data() == raw;
    EXPECT_TRUE(same_backing);
    EXPECT_FALSE(released);
    *encoded = fragmented_buffer{};
    EXPECT_TRUE(released);
}

TEST(
  FrameEncodeTest, InitialAbortAndExhaustedBudgetsConsumeWithoutPublication) {
    for (bool cancel : {false, true}) {
        seastar::abort_source abort;
        if (cancel) abort.request_abort();
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto body = fixture::fragmented("abc", 1);
        const auto result = fixture::encode(
          std::move(body), work, fixture::metadata, byte_count{});
        expect_error(result, cancel ? errc::aborted : errc::resource_exhausted);
        // NOLINTNEXTLINE(bugprone-use-after-move)
        expect_consumed(body);
    }
}

TEST(FrameEncodeTest, HeaderAdditionRespectsTheFragmentCeiling) {
    for (std::size_t count : {1023U, 1024U}) {
        auto body = fixture::fragmented(std::string(count, 'x'), 1);
        ASSERT_EQ(body.fragment_count(), count);
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        const auto result = fixture::encode(std::move(body), work);
        if (count == 1023) {
            ASSERT_TRUE(result.has_value());
            EXPECT_EQ(result->fragment_count(), 1024U);
        } else {
            expect_error(result, errc::resource_exhausted);
        }
    }
}

TEST(FrameEncodeTest, SmallVisiblePayloadDoesNotHideLargeRetainedBacking) {
    auto body = fixture::fragmented(std::string(32768, 'x'), 32768);
    ASSERT_TRUE(body.trim_front(byte_count{32767}).has_value());
    auto config = codec::limits_config{};
    config.max_retained_bytes = byte_count{4096};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::make(config).value(), abort};
    expect_error(
      fixture::encode(std::move(body), work), errc::resource_exhausted);
}

TEST(FrameEncodeTest, QueuedAbortJoinsWorkAndDrainsTheTransferredDonor) {
    auto body = fixture::fragmented(std::string(32768, 'x'), 67);
    auto config = codec::limits_config{};
    config.max_work_bytes = protocol::frame_prefix_work_bytes;
    config.max_work_items = protocol::frame_prefix_work_items;
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::make(config).value(), abort};
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
    std::optional<codec::result<fragmented_buffer>> outcome;
    std::exception_ptr exception;
    bool pending = false;
    try {
        auto future = protocol::encode_frame(
          std::move(body),
          fixture::metadata,
          work,
          fixture::bounds,
          {},
          fixture::parent_budget,
          fixture::charge);
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
    // NOLINTNEXTLINE(bugprone-use-after-move)
    expect_consumed(body);
}

TEST(
  FrameEncodeTest,
  AllocationFailuresReachTheWriterAndNeverPublishPartialOutput) {
#if !defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    GTEST_SKIP() << "allocation failure injection is disabled";
#else
    static_cast<void>(kwaque::error_category());
    codec::crc32c warm;
    const std::array<char, 65> warm_bytes{};
    warm.extend(warm_bytes);
    bool completed = false;
    unsigned failures = 0;
    for (std::uint64_t ordinal = 0; ordinal < 256; ++ordinal) {
        auto body = fixture::fragmented("abc", 1);
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        std::optional<codec::result<fragmented_buffer>> produced;
        bool bad_alloc = false;
        auto& injector = seastar::memory::local_failure_injector();
        injector.fail_after(ordinal);
        try {
            produced.emplace(fixture::encode(std::move(body), work));
        } catch (const std::bad_alloc&) {
            bad_alloc = true;
        } catch (...) {
            injector.cancel();
            throw;
        }
        const bool injected = injector.failed();
        injector.cancel();
        // NOLINTNEXTLINE(bugprone-use-after-move)
        expect_consumed(body);
        if (injected) {
            ++failures;
            EXPECT_TRUE(bad_alloc);
            EXPECT_FALSE(produced.has_value());
        } else {
            ASSERT_TRUE(produced.has_value());
            ASSERT_TRUE(produced->has_value());
            EXPECT_TRUE((**produced).content_equals(fixture::wire()));
            completed = true;
            break;
        }
    }
    EXPECT_GT(failures, 0U);
    EXPECT_TRUE(completed);
#endif
}

} // namespace
