#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/codec/crc32c.h"

#include <seastar/core/temporary_buffer.hh>

#include <crc32c/crc32c.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <bit>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace {

using namespace std::literals;
using checksum = kwaque::codec::crc32c;
using kwaque::byte_count;
using kwaque::bytes::fragmented_buffer;

template<typename T>
concept accepts_bytes = requires(checksum& crc, T&& bytes) {
    { crc.extend(std::forward<T>(bytes)) } -> std::same_as<void>;
};

static_assert(sizeof(checksum) == sizeof(std::uint32_t));
static_assert(std::is_trivially_copyable_v<checksum>);
static_assert(!std::is_convertible_v<std::uint32_t, checksum>);
static_assert(checksum{}.value() == 0);
static_assert(checksum{0x12345678U}.value() == 0x12345678U);
static_assert(noexcept(checksum{}));
static_assert(noexcept(std::declval<const checksum&>().value()));
static_assert(
  !noexcept(std::declval<checksum&>().extend(std::span<const char>{})));
static_assert(accepts_bytes<std::span<const char>>);
static_assert(accepts_bytes<std::string_view>);
static_assert(accepts_bytes<std::array<char, 4>&>);
static_assert(accepts_bytes<const std::array<char, 4>&>);
static_assert(!accepts_bytes<const char (&)[4]>);
static_assert(!accepts_bytes<char (&)[4]>);
static_assert(!accepts_bytes<std::uint32_t>);
static_assert(!accepts_bytes<std::uint64_t>);
static_assert(!accepts_bytes<bool>);

constexpr std::array seeds{0U, 0x12345678U, 0xffffffffU};

std::uint32_t
comparison_extend(std::uint32_t seed, std::span<const char> data) {
    if (data.empty()) {
        return seed;
    }
    return ::crc32c::Extend(
      seed, reinterpret_cast<const std::uint8_t*>(data.data()), data.size());
}

std::uint32_t checksum_of(std::span<const char> data, std::uint32_t seed = 0) {
    checksum crc{seed};
    crc.extend(data);
    return crc.value();
}

template<std::size_t N>
std::array<char, N> patterned_bytes() {
    std::array<char, N> data{};
    for (std::size_t index = 0; index < data.size(); ++index) {
        data[index] = std::bit_cast<char>(
          static_cast<std::uint8_t>((index * 31U + 7U) & 0xffU));
    }
    return data;
}

void expect_known(std::span<const char> data, std::uint32_t expected) {
    EXPECT_EQ(checksum_of(data), expected);
    EXPECT_EQ(comparison_extend(0, data), expected);
}

TEST(Crc32cTest, FourBinaryKnownAnswersPinPolynomialAndFinalization) {
    std::array<char, 32> zeros{};
    expect_known(zeros, 0x8a9136aaU);

    std::array<char, 32> ones{};
    ones.fill(std::bit_cast<char>(std::uint8_t{0xff}));
    expect_known(ones, 0x62a8ab43U);

    std::array<char, 32> increasing{};
    std::array<char, 32> decreasing{};
    for (std::size_t index = 0; index < increasing.size(); ++index) {
        increasing[index] = std::bit_cast<char>(
          static_cast<std::uint8_t>(index));
        decreasing[index] = std::bit_cast<char>(
          static_cast<std::uint8_t>(31U - index));
    }
    expect_known(increasing, 0x46dd794eU);
    expect_known(decreasing, 0x113fdb5cU);
}

TEST(Crc32cTest, TextKnownAnswersAndExplicitLittleEndianValue) {
    expect_known(""sv, 0U);
    expect_known("a"sv, 0xc1d04330U);
    expect_known("foo"sv, 0xcfc4ae1dU);
    expect_known("hello world"sv, 0xc99465aaU);
    expect_known("123456789"sv, 0xe3069283U);

    const auto value = checksum_of("123456789"sv);
    const std::array<std::uint8_t, 4> encoded{
      static_cast<std::uint8_t>(value & 0xffU),
      static_cast<std::uint8_t>((value >> 8U) & 0xffU),
      static_cast<std::uint8_t>((value >> 16U) & 0xffU),
      static_cast<std::uint8_t>((value >> 24U) & 0xffU)};
    constexpr std::array<std::uint8_t, 4> expected{0x83, 0x92, 0x06, 0xe3};
    EXPECT_EQ(encoded, expected);
}

TEST(Crc32cTest, EmptyInputsPreserveEveryFinalizedSeed) {
    const std::array<char, 1> storage{'x'};
    for (const auto seed : seeds) {
        checksum crc{seed};
        crc.extend(std::span<const char>{});
        EXPECT_EQ(crc.value(), seed);
        crc.extend(std::string_view{});
        EXPECT_EQ(crc.value(), seed);
        crc.extend(std::span<const char>{storage}.first(0));
        EXPECT_EQ(crc.value(), seed);
        crc.extend("abc"sv);
        EXPECT_EQ(crc.value(), comparison_extend(seed, "abc"sv));
        const auto after = crc.value();
        crc.extend(std::span<const char>{});
        EXPECT_EQ(crc.value(), after);
    }
}

TEST(Crc32cTest, EveryHeaderSplitCanResumeFromTheFinalizedValue) {
    const auto data = patterned_bytes<48>();
    for (const std::size_t length : {std::size_t{32}, std::size_t{48}}) {
        const auto bytes = std::span<const char>{data}.first(length);
        for (const auto seed : seeds) {
            const auto expected = comparison_extend(seed, bytes);
            EXPECT_EQ(checksum_of(bytes, seed), expected);
            for (std::size_t split = 0; split <= length; ++split) {
                SCOPED_TRACE(
                  ::testing::Message()
                  << length << ':' << seed << ':' << split);
                checksum first{seed};
                first.extend(bytes.first(split));
                const auto partial = comparison_extend(
                  seed, bytes.first(split));
                EXPECT_EQ(first.value(), partial);
                checksum resumed{first.value()};
                resumed.extend(bytes.subspan(split));
                first.extend(std::span<const char>{});
                first.extend(bytes.subspan(split));
                EXPECT_EQ(first.value(), expected);
                EXPECT_EQ(resumed.value(), expected);
                EXPECT_EQ(
                  comparison_extend(partial, bytes.subspan(split)), expected);
            }
        }
    }
}

constexpr std::array<std::size_t, 34> boundaries{
  0,    1,    7,    8,    31,   32,   33,   47,   48,   49,   63,   64,
  65,   127,  128,  129,  255,  256,  257,  1007, 1008, 1009, 2047, 2048,
  2049, 4032, 4079, 4080, 4081, 4095, 4096, 4097, 8191, 8192};

TEST(Crc32cTest, SeededIncrementalUpdatesCrossSizeBoundaries) {
    const auto data = patterned_bytes<8192>();
    const std::span<const char> bytes{data};
    for (const auto seed : seeds) {
        const auto expected = comparison_extend(seed, bytes);
        EXPECT_EQ(checksum_of(bytes, seed), expected);
        for (const auto split : boundaries) {
            SCOPED_TRACE(::testing::Message() << seed << ':' << split);
            checksum crc{seed};
            crc.extend(bytes.first(split));
            const auto partial = comparison_extend(seed, bytes.first(split));
            EXPECT_EQ(crc.value(), partial);
            crc.extend(bytes.subspan(split));
            EXPECT_EQ(crc.value(), expected);
            EXPECT_EQ(
              comparison_extend(partial, bytes.subspan(split)), expected);
        }
    }
}

TEST(Crc32cTest, UnalignedSpansAgreeAtEveryBoundaryLength) {
    const auto data = patterned_bytes<8199>();
    for (std::size_t offset = 0; offset < 8; ++offset) {
        for (const auto length : boundaries) {
            const auto bytes = std::span<const char>{data}.subspan(
              offset, length);
            for (const auto seed : seeds) {
                SCOPED_TRACE(
                  ::testing::Message()
                  << offset << ':' << length << ':' << seed);
                EXPECT_EQ(
                  checksum_of(bytes, seed), comparison_extend(seed, bytes));
            }
        }
    }
}

TEST(Crc32cTest, ByteAtATimeMatchesContiguousInput) {
    constexpr auto data = "the quick brown fox jumps over the dog"sv;
    for (const auto seed : seeds) {
        checksum crc{seed};
        auto comparison = seed;
        for (const char byte : data) {
            const std::span<const char> one{&byte, 1};
            crc.extend(one);
            comparison = comparison_extend(comparison, one);
            EXPECT_EQ(crc.value(), comparison);
        }
        EXPECT_EQ(crc.value(), checksum_of(data, seed));
        EXPECT_EQ(crc.value(), comparison_extend(seed, data));
    }
}

fragmented_buffer
fragment_at_size(std::span<const char> data, std::size_t size) {
    std::vector<seastar::temporary_buffer<char>> fragments;
    fragments.reserve(1U + (data.size() - 1U) / size);
    for (std::size_t offset = 0; offset < data.size(); offset += size) {
        const auto bytes = data.subspan(
          offset, std::min(size, data.size() - offset));
        seastar::temporary_buffer<char> fragment{bytes.size()};
        std::ranges::copy(bytes, fragment.get_write());
        fragments.push_back(std::move(fragment));
    }
    return fragmented_buffer::copy_from_fragments(fragments).value();
}

TEST(Crc32cTest, ExactLegalFragmentLayoutsPreserveChecksum) {
    const auto data = patterned_bytes<8192>();
    for (const std::size_t fragment_size :
         {std::size_t{1},
          std::size_t{7},
          std::size_t{64},
          std::size_t{512},
          std::size_t{4096},
          std::size_t{8192}}) {
        // Tiny layouts retain exact cuts while respecting the buffer's ceiling.
        const auto length = std::min(
          data.size(), kwaque::bytes::max_buffer_fragments * fragment_size);
        const auto bytes = std::span<const char>{data}.first(length);
        const auto buffer = fragment_at_size(bytes, fragment_size);
        ASSERT_EQ(buffer.fragment_count(), 1U + (length - 1U) / fragment_size);
        ASSERT_LE(buffer.fragment_count(), kwaque::bytes::max_buffer_fragments);
        for (const auto seed : seeds) {
            SCOPED_TRACE(::testing::Message() << fragment_size << ':' << seed);
            checksum crc{seed};
            auto comparison = seed;
            for (const auto fragment : buffer) {
                const std::span<const char> part{
                  fragment.data(), fragment.size()};
                crc.extend(part);
                comparison = comparison_extend(comparison, part);
            }
            EXPECT_EQ(crc.value(), comparison);
            EXPECT_EQ(crc.value(), comparison_extend(seed, bytes));
            EXPECT_EQ(crc.value(), checksum_of(bytes, seed));
        }
    }
}

TEST(Crc32cTest, SharedSlicesHashOnlyTheirPresentedBytes) {
    const auto data = patterned_bytes<97>();
    auto source = fragment_at_size(data, 7);
    auto sliced = source.share(byte_count{3}, byte_count{80});
    ASSERT_TRUE(sliced.has_value());
    ASSERT_TRUE(sliced->trim_front(byte_count{2}).has_value());
    ASSERT_TRUE(sliced->trim_back(byte_count{3}).has_value());
    const auto expected_bytes = std::span<const char>{data}.subspan(5, 75);
    ASSERT_TRUE(sliced->content_equals(
      std::string_view{expected_bytes.data(), expected_bytes.size()}));
    for (const auto seed : seeds) {
        checksum crc{seed};
        for (const auto fragment : *sliced) {
            crc.extend(std::span<const char>{fragment.data(), fragment.size()});
        }
        EXPECT_EQ(crc.value(), comparison_extend(seed, expected_bytes));
    }
}

} // namespace
