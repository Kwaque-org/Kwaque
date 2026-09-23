#include "src/base/error.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/bytes/fragmented_buffer_parser.h"
#include "src/bytes/fragmented_buffer_test_support.h"
#include "src/codec/cooperative.h"
#include "src/codec/crc32c.h"
#include "src/codec/envelope.h"
#include "src/codec/envelope_encode.h"
#include "src/codec/error.h"
#include "src/codec/format_registry.h"
#include "src/codec/integer.h"
#include "src/codec/limits.h"
#include "src/codec/tests/envelope_decode_test_support.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/deleter.hh>
#include <seastar/core/future.hh>
#include <seastar/core/preempt.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/alloc_failure_injector.hh>
#include <seastar/util/later.hh>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <bit>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

namespace codec = kwaque::codec;
namespace fixture = codec::testing::envelope_fixture;
using kwaque::byte_count;
using kwaque::errc;
using kwaque::item_count;
using kwaque::bytes::fragmented_buffer;
using kwaque::bytes::fragmented_buffer_parser;
using native_fragment = seastar::temporary_buffer<char>;
using namespace std::literals;

constexpr codec::field_context context{.origin = 71, .family = 7, .field = 91};
constexpr byte_count parent_budget{32U * 1024U * 1024U};
constexpr codec::envelope_extent_limits owner_limits{
  .max_body_bytes = byte_count{16U * 1024U * 1024U},
  .max_encoded_bytes = byte_count{32U * 1024U * 1024U}};

// The other half of the operation allowance remains unavailable to these
// fixtures for native CRC/frame/opaque owners. These tests inspect byte-owner
// accounting, not compiler-frame size, allocator page use or total shard RSS.
byte_count charge(byte_count request) noexcept {
    if (request.value() > (std::uint64_t{1} << 62U)) {
        return byte_count{std::numeric_limits<std::uint64_t>::max()};
    }
    return byte_count{
      2U * std::bit_ceil(std::max(request.value(), std::uint64_t{16}))};
}

fragmented_buffer text(std::string_view value) {
    return fragmented_buffer::copy_of(value).value();
}

codec::result<fragmented_buffer> encode(
  fragmented_buffer&& body,
  codec::cooperative_work& work,
  codec::envelope_extent_limits bounds = owner_limits,
  byte_count remaining = parent_budget,
  codec::operation_usage other_live = {},
  kwaque::bytes::allocation_charge_fn accounting = charge,
  codec::format_family family = codec::format_family::submitted_batch,
  codec::field_context origin = context) {
    return codec::encode_envelope(
             std::move(body),
             family,
             work,
             bounds,
             other_live,
             remaining,
             accounting,
             origin)
      .get();
}

void expect_error(const auto& value, errc code) {
    ASSERT_FALSE(value.has_value());
    EXPECT_EQ(value.error().code(), code);
    EXPECT_EQ(value.error().family(), context.family);
}

void expect_consumed(const fragmented_buffer& input) {
    EXPECT_TRUE(input.empty());
    EXPECT_EQ(input.size(), byte_count{});
    EXPECT_EQ(input.retained_bytes(), byte_count{});
    EXPECT_EQ(input.fragment_count(), 0U);
}

std::uint32_t read_u32(std::span<const char> input, std::size_t offset) {
    std::uint32_t value = 0;
    for (unsigned index = 0; index < 4; ++index) {
        value |= static_cast<std::uint32_t>(
                   static_cast<unsigned char>(input[offset + index]))
                 << (8U * index);
    }
    return value;
}

void expect_header(
  const fragmented_buffer& output,
  std::string_view body,
  std::uint16_t family = 1) {
    const auto expected = fixture::make_envelope(body, {}, family);
    EXPECT_TRUE(output.content_equals(expected));
    EXPECT_EQ(output.size(), byte_count{32U + body.size()});
}

TEST(EnvelopeEncodeTest, EmitsTheIndependentLiteralEnvelope) {
    ASSERT_EQ(fixture::crc32c(fixture::fixed_body), 0xdd0949a2U);
    std::string literal{fixture::fixed_prefix};
    literal.append(fixture::fixed_body);
    ASSERT_EQ(fixture::make_envelope(), literal);
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto body = text(fixture::fixed_body);
    const auto encoded = encode(std::move(body), work);
    ASSERT_TRUE(encoded.has_value());
    EXPECT_TRUE(encoded->content_equals(literal));
    // NOLINTNEXTLINE(bugprone-use-after-move)
    expect_consumed(body);
}

TEST(EnvelopeEncodeTest, EveryBodySplitHasIdenticalFinalBytes) {
    for (std::size_t cut = 0; cut <= fixture::fixed_body.size(); ++cut) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto body = fixture::split_at(fixture::fixed_body, cut);
        const auto encoded = encode(std::move(body), work);
        ASSERT_TRUE(encoded.has_value()) << cut;
        expect_header(*encoded, fixture::fixed_body);
        // NOLINTNEXTLINE(bugprone-use-after-move)
        expect_consumed(body);
    }
}

TEST(EnvelopeEncodeTest, BodyChecksumIsFinalBeforeHeaderChecksum) {
    for (std::size_t changed = 0; changed < fixture::fixed_body.size();
         ++changed) {
        std::string body_bytes{fixture::fixed_body};
        body_bytes[changed] ^= 1;
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto encoded = encode(text(body_bytes), work);
        ASSERT_TRUE(encoded.has_value()) << changed;
        expect_header(*encoded, body_bytes);
        fragmented_buffer_parser input{std::move(*encoded)};
        std::array<char, 32> prefix{};
        ASSERT_TRUE(input.peek_to(prefix).has_value());
        EXPECT_EQ(read_u32(prefix, 24), fixture::crc32c(body_bytes));
        const auto stored_header = read_u32(prefix, 28);
        std::fill(prefix.begin() + 28, prefix.end(), '\0');
        EXPECT_EQ(
          stored_header,
          fixture::crc32c(std::string_view{prefix.data(), prefix.size()}));
    }
}

TEST(EnvelopeEncodeTest, EveryRegisteredFamilyUsesOnlyTheCurrentWriterProfile) {
    for (std::uint16_t raw = 1; raw <= 11; ++raw) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        const auto encoded = encode(
          text(fixture::fixed_body),
          work,
          owner_limits,
          parent_budget,
          {},
          charge,
          static_cast<codec::format_family>(raw));
        ASSERT_TRUE(encoded.has_value()) << raw;
        expect_header(*encoded, fixture::fixed_body, raw);
    }
}

TEST(EnvelopeEncodeTest, EmptyBodyStillProducesACompleteChecksummedPrefix) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto encoded = encode(
      fragmented_buffer{},
      work,
      {.max_body_bytes = byte_count{}, .max_encoded_bytes = byte_count{32}});
    ASSERT_TRUE(encoded.has_value());
    expect_header(*encoded, ""sv);
    EXPECT_EQ(encoded->size(), byte_count{32});
    auto invalid = encode(
      fragmented_buffer{},
      work,
      {.max_body_bytes = byte_count{}, .max_encoded_bytes = byte_count{31}});
    expect_error(invalid, errc::resource_exhausted);
}

TEST(EnvelopeEncodeTest, BodyAndWholeEnvelopeCapsIntersectAtExactBoundaries) {
    for (const auto extra : {0U, 1U}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        const auto accepted = encode(
          text(fixture::fixed_body),
          work,
          {.max_body_bytes = byte_count{36U + extra},
           .max_encoded_bytes = byte_count{68U + extra}});
        ASSERT_TRUE(accepted.has_value());
        expect_header(*accepted, fixture::fixed_body);
    }
    for (const auto bounds : std::array{
           codec::envelope_extent_limits{byte_count{35}, byte_count{68}},
           codec::envelope_extent_limits{byte_count{36}, byte_count{67}},
           codec::envelope_extent_limits{byte_count{}, byte_count{68}},
           codec::envelope_extent_limits{byte_count{36}, byte_count{}}}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto body = text(fixture::fixed_body);
        expect_error(
          encode(std::move(body), work, bounds), errc::resource_exhausted);
        // NOLINTNEXTLINE(bugprone-use-after-move)
        expect_consumed(body);
    }
}

TEST(EnvelopeEncodeTest, NarrowPolicyCannotBeWidenedByOwnerBounds) {
    for (const unsigned selection : {0U, 1U, 2U, 3U}) {
        codec::limits_config config;
        switch (selection) {
        case 0:
            config.max_encoded_body_bytes = byte_count{35};
            break;
        case 1:
            config.max_header_bytes = byte_count{31};
            break;
        case 2:
            config.max_work_bytes = byte_count{127};
            break;
        case 3:
            config.max_work_items = item_count{63};
            break;
        default:
            FAIL() << "unexpected fixture selector";
        }
        seastar::abort_source abort;
        codec::cooperative_work work{
          codec::limits::make(config).value(), abort};
        auto body = text(fixture::fixed_body);
        expect_error(encode(std::move(body), work), errc::resource_exhausted);
        // NOLINTNEXTLINE(bugprone-use-after-move)
        expect_consumed(body);
    }
}

TEST(EnvelopeEncodeTest, InvalidFamilyChargeAndOriginConsumeTheDonor) {
    for (const auto raw :
         {std::uint16_t{0},
          std::uint16_t{12},
          std::numeric_limits<std::uint16_t>::max()}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto body = text(fixture::fixed_body);
        expect_error(
          encode(
            std::move(body),
            work,
            owner_limits,
            parent_budget,
            {},
            charge,
            static_cast<codec::format_family>(raw)),
          errc::invalid_argument);
        // NOLINTNEXTLINE(bugprone-use-after-move)
        expect_consumed(body);
    }
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto without_charge = text(fixture::fixed_body);
    expect_error(
      encode(
        std::move(without_charge),
        work,
        owner_limits,
        parent_budget,
        {},
        nullptr),
      errc::invalid_argument);
    // NOLINTNEXTLINE(bugprone-use-after-move)
    expect_consumed(without_charge);
    constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();
    auto impossible = text(fixture::fixed_body);
    expect_error(
      encode(
        std::move(impossible),
        work,
        owner_limits,
        parent_budget,
        {},
        charge,
        codec::format_family::submitted_batch,
        {.origin = maximum - 67U,
         .family = context.family,
         .field = context.field}),
      errc::invalid_argument);
    // NOLINTNEXTLINE(bugprone-use-after-move)
    expect_consumed(impossible);
    const auto exact = encode(
      text(fixture::fixed_body),
      work,
      owner_limits,
      parent_budget,
      {},
      charge,
      codec::format_family::submitted_batch,
      {.origin = maximum - 68U,
       .family = context.family,
       .field = context.field});
    ASSERT_TRUE(exact.has_value());
    expect_header(*exact, fixture::fixed_body);
}

TEST(EnvelopeEncodeTest, ExistingUsageAndParentRemainderStayCharged) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    for (const auto remaining : {byte_count{}, byte_count{1}}) {
        auto body = text(fixture::fixed_body);
        expect_error(
          encode(std::move(body), work, owner_limits, remaining),
          errc::resource_exhausted);
        // NOLINTNEXTLINE(bugprone-use-after-move)
        expect_consumed(body);
    }
    const codec::operation_usage exhausted{.retained_input = parent_budget};
    const auto before = exhausted;
    expect_error(
      encode(
        text(fixture::fixed_body),
        work,
        owner_limits,
        parent_budget,
        exhausted),
      errc::resource_exhausted);
    EXPECT_EQ(exhausted, before);
    expect_error(
      encode(
        text(fixture::fixed_body),
        work,
        owner_limits,
        parent_budget,
        {.decoded_metadata = byte_count{1024U * 1024U + 1U}}),
      errc::resource_exhausted);
    expect_error(
      encode(
        text(fixture::fixed_body),
        work,
        owner_limits,
        parent_budget,
        {.scratch = byte_count{1024U * 1024U + 1U}}),
      errc::resource_exhausted);
}

TEST(EnvelopeEncodeTest, SmallVisibleBodyDoesNotHideRetainedBacking) {
    std::string storage(32768, 'x');
    auto body = text(storage);
    ASSERT_TRUE(body.trim_front(byte_count{32767}).has_value());
    ASSERT_EQ(body.size(), byte_count{1});
    ASSERT_EQ(body.retained_bytes(), byte_count{32768});
    codec::limits_config config;
    config.max_retained_bytes = byte_count{4096};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::make(config).value(), abort};
    expect_error(encode(std::move(body), work), errc::resource_exhausted);
    // NOLINTNEXTLINE(bugprone-use-after-move)
    expect_consumed(body);
}

fragmented_buffer large_body() {
    std::vector<native_fragment> fragments;
    for (const char value : {'A', 'B'}) {
        native_fragment fragment{65536};
        std::fill_n(fragment.get_write(), fragment.size(), value);
        fragments.push_back(std::move(fragment));
    }
    return fragmented_buffer::copy_from_fragments(fragments).value();
}

TEST(EnvelopeEncodeTest, LargePayloadBackingSurvivesPrefixAssembly) {
    constexpr auto expected_prefix
      = "\x4b\x51\x42\x46\x01\x00\x01\x00\x01\x00\x20\x00\x00\x00\x02\x00"
        "\x00\x00\x00\x00\x00\x00\x00\x00\xfc\xe2\x76\x4c\xd7\xad\x22\x8e"sv;
    static_assert(expected_prefix.size() == 32);
    auto body = large_body();
    const auto* first = body.fragment_at(0)->data();
    const auto* second = body.fragment_at(1)->data();
    codec::limits_config config;
    config.max_work_bytes = byte_count{128};
    config.max_work_items = item_count{64};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::make(config).value(), abort};
    auto encoded = encode(std::move(body), work);
    ASSERT_TRUE(encoded.has_value());
    EXPECT_EQ(encoded->size(), byte_count{32U + 131072U});
    bool retained_first = false;
    bool retained_second = false;
    std::size_t position = 0;
    for (const auto fragment : *encoded) {
        retained_first |= fragment.data() == first && fragment.size() == 65536;
        retained_second |= fragment.data() == second
                           && fragment.size() == 65536;
        bool matches = true;
        for (const char value : fragment.bytes()) {
            const char expected = position < 32 ? expected_prefix[position]
                                  : position < 32U + 65536U ? 'A'
                                                            : 'B';
            matches = matches && value == expected;
            ++position;
        }
        EXPECT_TRUE(matches);
        seastar::thread::maybe_yield();
    }
    EXPECT_TRUE(retained_first);
    EXPECT_TRUE(retained_second);
    EXPECT_EQ(position, 32U + 131072U);
    // NOLINTNEXTLINE(bugprone-use-after-move)
    expect_consumed(body);
}

TEST(EnvelopeEncodeTest, NewAllocationCapDoesNotRejectPreexistingLargeBacking) {
    constexpr auto expected_prefix
      = "\x4b\x51\x42\x46\x01\x00\x01\x00\x01\x00\x20\x00\x00\x00\x01\x00"
        "\x00\x00\x00\x00\x00\x00\x00\x00\x32\xb7\x7a\xfc\x70\xb0\x12\x43"sv;
    static_assert(expected_prefix.size() == 32);
    native_fragment storage{65536};
    std::fill_n(storage.get_write(), storage.size(), 'A');
    auto body = fragmented_buffer::copy_from_fragment(storage).value();
    storage = native_fragment{};
    const auto* original = body.fragment_at(0)->data();
    ASSERT_EQ(body.retained_bytes(), byte_count{65536});
    ASSERT_EQ(charge(body.retained_bytes()), byte_count{131072});

    codec::limits_config config;
    config.max_allocation_bytes = byte_count{4096};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::make(config).value(), abort};
    const auto encoded = encode(std::move(body), work);
    ASSERT_TRUE(encoded.has_value());
    EXPECT_EQ(encoded->size(), byte_count{32U + 65536U});
    bool retained_original = false;
    std::size_t position = 0;
    for (const auto fragment : *encoded) {
        retained_original |= fragment.data() == original
                             && fragment.size() == 65536;
        bool matches = true;
        for (const char value : fragment.bytes()) {
            const char expected = position < 32 ? expected_prefix[position]
                                                : 'A';
            matches = matches && value == expected;
            ++position;
        }
        EXPECT_TRUE(matches);
        seastar::thread::maybe_yield();
    }
    EXPECT_TRUE(retained_original);
    EXPECT_EQ(position, 32U + 65536U);
    // NOLINTNEXTLINE(bugprone-use-after-move)
    expect_consumed(body);
}

TEST(EnvelopeEncodeTest, InitialAbortConsumesInputAndPublishesNothing) {
    seastar::abort_source abort;
    abort.request_abort_ex(std::make_exception_ptr(std::runtime_error("stop")));
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto body = text(fixture::fixed_body);
    expect_error(encode(std::move(body), work), errc::aborted);
    // NOLINTNEXTLINE(bugprone-use-after-move)
    expect_consumed(body);
}

TEST(
  EnvelopeEncodeTest,
  TransferredDonorRemainsAliveUntilEncodedOutputIsReleased) {
    seastar::abort_source abort;
    bool released = false;
    auto storage = std::make_unique<char[]>(1);
    storage[0] = 'x';
    auto* raw = storage.get();
    auto deleter = seastar::make_deleter(
      [storage = std::move(storage), &abort, &released] noexcept {
          static_cast<void>(storage);
          released = true;
          abort.request_abort();
      });
    auto native = native_fragment::maybe_unsafe_from_deleter(
      raw, 1, std::move(deleter));
    auto body = kwaque::bytes::fragmented_buffer_test_access::adopt_fragment(
                  std::move(native), byte_count{1})
                  .value();
    codec::cooperative_work work{codec::limits::defaults(), abort};
    ASSERT_FALSE(released);
    auto encoded = encode(std::move(body), work);
    ASSERT_TRUE(encoded.has_value());
    EXPECT_FALSE(released);
    EXPECT_FALSE(abort.abort_requested());
    *encoded = fragmented_buffer{};
    EXPECT_TRUE(released);
    EXPECT_TRUE(abort.abort_requested());
    // NOLINTNEXTLINE(bugprone-use-after-move)
    expect_consumed(body);
}

TEST(EnvelopeEncodeTest, QueuedAbortInterruptsPendingWorkAndDrainsTheDonor) {
    auto body = large_body();
    codec::limits_config config;
    config.max_work_bytes = byte_count{128};
    config.max_work_items = item_count{64};
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
        auto waiting = codec::encode_envelope(
          std::move(body),
          codec::format_family::submitted_batch,
          work,
          owner_limits,
          {},
          parent_budget,
          charge,
          context);
        pending = !waiting.available();
        outcome.emplace(waiting.get());
    } catch (...) {
        exception = std::current_exception();
    }
    const bool during_work = observed;
    observer.get();
    if (exception) {
        std::rethrow_exception(exception);
    }
    EXPECT_TRUE(pending);
    EXPECT_TRUE(during_work);
    ASSERT_TRUE(outcome.has_value());
    expect_error(*outcome, errc::aborted);
    // NOLINTNEXTLINE(bugprone-use-after-move)
    expect_consumed(body);
}

TEST(EnvelopeEncodeTest, ObservedAllocationFailuresNeverPublishPartialOutput) {
#if !defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    GTEST_SKIP() << "allocation failure injection is disabled";
#else
    static_cast<void>(kwaque::error_category());
    // Native CRC initialization is qualified separately in isolated processes;
    // this sweep reaches writer-owned allocations after that initialization.
    // Coroutine critical allocations are not counted by this injector.
    codec::crc32c warm;
    const std::array<char, 65> warm_bytes{};
    warm.extend(std::span<const char>{warm_bytes});
    bool completed = false;
    std::size_t failures = 0;
    for (std::uint64_t ordinal = 0; ordinal < 256; ++ordinal) {
        auto body = fixture::fragmented(fixture::fixed_body, 5);
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        std::optional<codec::result<fragmented_buffer>> produced;
        bool bad_alloc = false;
        auto& injector = seastar::memory::local_failure_injector();
        injector.fail_after(ordinal);
        try {
            produced.emplace(encode(std::move(body), work));
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
            expect_header(**produced, fixture::fixed_body);
            completed = true;
            break;
        }
    }
    EXPECT_GT(failures, 0U);
    EXPECT_TRUE(completed);
#endif
}

} // namespace
