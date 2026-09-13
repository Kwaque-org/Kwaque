#include "src/base/error.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/codec/cooperative.h"
#include "src/codec/crc32c.h"
#include "src/codec/envelope.h"
#include "src/codec/envelope_integrity.h"
#include "src/codec/error.h"
#include "src/codec/integer.h"
#include "src/codec/limits.h"
#include "src/codec/tests/envelope_decode_test_support.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/util/alloc_failure_injector.hh>

#include <gtest/gtest.h>

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>

namespace {

using namespace std::literals;
namespace codec = kwaque::codec;
namespace fixture = codec::testing::envelope_fixture;
using kwaque::byte_count;
using kwaque::errc;
using kwaque::item_count;
using kwaque::bytes::fragmented_buffer;

constexpr codec::field_context context{.origin = 100, .family = 7};
constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();

template<typename Header>
concept checksum_header_argument = requires(
  Header&& header, codec::cooperative_work& work) {
    {
        codec::verify_envelope_header_crc(std::forward<Header>(header), work)
    } -> std::same_as<seastar::future<codec::result<void>>>;
};

static_assert(checksum_header_argument<fragmented_buffer&>);
static_assert(checksum_header_argument<const fragmented_buffer&>);
static_assert(!checksum_header_argument<fragmented_buffer>);
static_assert(!checksum_header_argument<const fragmented_buffer>);

void expect_error(
  const codec::result<void>& outcome,
  errc reason,
  codec::envelope_field field,
  std::uint64_t offset) {
    ASSERT_FALSE(outcome.has_value());
    EXPECT_EQ(
      outcome.error(),
      (codec::error{
        reason, context.family, static_cast<std::uint16_t>(field), offset}));
}

std::string header_bytes(std::string_view extensions = {}) {
    auto encoded = fixture::make_envelope(fixture::fixed_body, extensions);
    encoded.resize(fixture::header_bytes(encoded));
    return encoded;
}

codec::result<void> verify(
  const fragmented_buffer& header,
  codec::limits policy = codec::limits::defaults(),
  codec::field_context coordinates = context) {
    seastar::abort_source abort;
    codec::cooperative_work work{policy, abort};
    return codec::verify_envelope_header_crc(header, work, coordinates).get();
}

TEST(EnvelopeIntegrityTest, LiteralHeaderVerifiesWithoutAnyBodyBytes) {
    auto header = fragmented_buffer::copy_of(fixture::fixed_prefix).value();
    ASSERT_TRUE(verify(header).has_value());
    EXPECT_TRUE(header.content_equals(fixture::fixed_prefix));
}

TEST(
  EnvelopeIntegrityTest, HeaderChecksumUsesFourZerosRatherThanOmittingItsSlot) {
    auto encoded = header_bytes("raw extension bytes");
    auto omitted = encoded.substr(0, 28) + encoded.substr(32);
    const auto omitted_crc = fixture::crc32c(omitted);
    auto zeroed = encoded;
    fixture::put_u32(zeroed, 28, 0);
    ASSERT_NE(omitted_crc, fixture::crc32c(zeroed));
    fixture::put_u32(encoded, 28, omitted_crc);
    auto header = fragmented_buffer::copy_of(encoded).value();
    expect_error(
      verify(header),
      errc::corrupt_data,
      codec::envelope_field::header_crc32c,
      128);
}

TEST(EnvelopeIntegrityTest, EveryRawHeaderByteAndStoredCrcOctetIsProtected) {
    const auto original = header_bytes("unknown optional value");
    for (std::size_t offset = 0; offset < original.size(); ++offset) {
        SCOPED_TRACE(offset);
        auto mutated = original;
        mutated[offset] = static_cast<char>(
          static_cast<unsigned char>(mutated[offset]) ^ 0x80U);
        auto header = fixture::split_at(mutated, offset);
        expect_error(
          verify(header),
          errc::corrupt_data,
          codec::envelope_field::header_crc32c,
          128);
        EXPECT_TRUE(header.content_equals(mutated));
    }
}

TEST(EnvelopeIntegrityTest, EveryFragmentSplitPreservesTheRawHeaderChecksum) {
    const auto encoded = header_bytes("\x7f\0\x80\xff"sv);
    for (std::size_t cut = 0; cut <= encoded.size(); ++cut) {
        auto header = fixture::split_at(encoded, cut);
        ASSERT_TRUE(verify(header).has_value());
        EXPECT_TRUE(header.content_equals(encoded));
    }
    auto bytes = fixture::fragmented(encoded, 1);
    ASSERT_TRUE(verify(bytes).has_value());
}

TEST(EnvelopeIntegrityTest, BodyCrcAndUninterpretedExtensionsRemainProtected) {
    auto encoded = header_bytes("unassigned mandatory representation");
    for (std::size_t offset :
         {std::size_t{24}, std::size_t{27}, std::size_t{32}}) {
        auto mutated = encoded;
        mutated[offset] = static_cast<char>(
          static_cast<unsigned char>(mutated[offset]) ^ 1U);
        auto header = fragmented_buffer::copy_of(mutated).value();
        expect_error(
          verify(header),
          errc::corrupt_data,
          codec::envelope_field::header_crc32c,
          128);
    }
}

TEST(
  EnvelopeIntegrityTest,
  SuccessDoesNotInterpretVersionsFamilyFeaturesOrTlvBytes) {
    auto encoded = fixture::make_envelope(
      fixture::fixed_body, "not a valid TLV", 65535, 0, 99, maximum);
    encoded.resize(fixture::header_bytes(encoded));
    auto header = fragmented_buffer::copy_of(encoded).value();
    ASSERT_TRUE(verify(header).has_value());
}

TEST(
  EnvelopeIntegrityTest, EveryShortCompleteAliasRejectsAtItsFirstMissingByte) {
    for (std::size_t size = 0; size < codec::envelope_prefix_bytes; ++size) {
        auto header = fragmented_buffer::copy_of(
                        fixture::fixed_prefix.substr(0, size))
                        .value();
        expect_error(
          verify(header),
          errc::malformed_data,
          codec::envelope_field::header_bytes,
          100 + size);
    }
}

TEST(EnvelopeIntegrityTest, MaximumHeaderTraversesNarrowSharedQuanta) {
    const auto encoded = header_bytes(std::string(4064, 'x'));
    ASSERT_EQ(encoded.size(), 4096U);
    auto header = fixture::fragmented(encoded, 4);
    auto config = codec::limits_config{};
    config.max_work_bytes = byte_count{3};
    config.max_work_items = item_count{2};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::make(config).value(), abort};
    ASSERT_TRUE(
      codec::verify_envelope_header_crc(header, work, context)
        .get()
        .has_value());
    EXPECT_EQ(work.byte_quantum(), byte_count{3});
    EXPECT_EQ(work.item_quantum(), item_count{2});
    EXPECT_TRUE(header.content_equals(encoded));
}

TEST(EnvelopeIntegrityTest, HeaderByteAndFragmentLimitsRejectBeforeCrcWork) {
    auto oversized = fragmented_buffer::copy_of(
                       header_bytes(std::string(4065, 'x')))
                       .value();
    expect_error(
      verify(oversized),
      errc::resource_exhausted,
      codec::envelope_field::header_bytes,
      110);

    auto config = codec::limits_config{};
    config.max_buffer_fragments = item_count{31};
    auto fragmented = fixture::fragmented(fixture::fixed_prefix, 1);
    expect_error(
      verify(fragmented, codec::limits::make(config).value()),
      errc::resource_exhausted,
      codec::envelope_field::header_bytes,
      110);

    config = codec::limits_config{};
    config.max_header_bytes = byte_count{31};
    auto header = fragmented_buffer::copy_of(fixture::fixed_prefix).value();
    expect_error(
      verify(header, codec::limits::make(config).value()),
      errc::resource_exhausted,
      codec::envelope_field::header_bytes,
      110);
}

TEST(EnvelopeIntegrityTest, CheckedOriginAllowsAnExactMaximumEnd) {
    auto header = fragmented_buffer::copy_of(fixture::fixed_prefix).value();
    ASSERT_TRUE(
      verify(
        header,
        codec::limits::defaults(),
        codec::field_context{.origin = maximum - 32, .family = context.family})
        .has_value());
    expect_error(
      verify(
        header,
        codec::limits::defaults(),
        codec::field_context{.origin = maximum - 31, .family = context.family}),
      errc::invalid_argument,
      codec::envelope_field::header_bytes,
      maximum - 31);
}

TEST(EnvelopeIntegrityTest, InitialAbortPrecedesIntegrityAndPreservesTheOwner) {
    auto encoded = std::string(fixture::fixed_prefix);
    encoded[28] = static_cast<char>(
      static_cast<unsigned char>(encoded[28]) ^ 1U);
    auto header = fragmented_buffer::copy_of(encoded).value();
    seastar::abort_source abort;
    abort.request_abort();
    codec::cooperative_work work{codec::limits::defaults(), abort};
    expect_error(
      codec::verify_envelope_header_crc(header, work, context).get(),
      errc::aborted,
      codec::envelope_field::header_crc32c,
      100);
    EXPECT_TRUE(header.content_equals(encoded));
}

TEST(EnvelopeIntegrityTest, ImpossibleChecksumSlotQuantumRejectsTyped) {
    for (const bool narrow_bytes : {false, true}) {
        auto config = codec::limits_config{};
        config.max_work_bytes = byte_count{narrow_bytes ? 1U : 2U};
        config.max_work_items = item_count{narrow_bytes ? 2U : 1U};
        auto header = fragmented_buffer::copy_of(fixture::fixed_prefix).value();
        expect_error(
          verify(header, codec::limits::make(config).value()),
          errc::resource_exhausted,
          codec::envelope_field::header_crc32c,
          128);
        EXPECT_TRUE(header.content_equals(fixture::fixed_prefix));
    }
}

TEST(
  EnvelopeIntegrityTest, WarmTraversalMakesNoOrdinaryAllocationOrHeaderShare) {
#if !defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    GTEST_SKIP() << "allocation failure injection is not enabled";
#else
    const auto encoded = header_bytes(std::string(4064, 'x'));
    auto header = fixture::fragmented(encoded, 4);
    // Warm the actual native CRC dispatch with a full-size span. Critical
    // coroutine frames are outside ordinary allocation injection; no frame or
    // cold-native allocation guarantee is inferred by this test.
    codec::crc32c warm;
    warm.extend(std::span<const char>{encoded});
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    std::optional<codec::result<void>> outcome;
    std::exception_ptr failure;
    auto& injector = seastar::memory::local_failure_injector();
    injector.fail_after(0);
    try {
        outcome.emplace(
          codec::verify_envelope_header_crc(header, work, context).get());
    } catch (...) {
        failure = std::current_exception();
    }
    const bool injected = injector.failed();
    injector.cancel();
    if (failure) {
        std::rethrow_exception(failure);
    }
    EXPECT_FALSE(injected);
    ASSERT_TRUE(outcome.has_value());
    ASSERT_TRUE(outcome->has_value());
    EXPECT_TRUE(header.content_equals(encoded));
#endif
}

} // namespace
