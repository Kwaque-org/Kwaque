#include "src/codec/digest.h"
#include "src/codec/xxh3.h"

#include <gtest/gtest.h>

#include <array>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <new>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace {

using namespace std::literals;
namespace codec = kwaque::codec;
using codec::content_digest;
using codec::xxh3_128_hasher;

template<typename T>
concept can_finalize = requires(T&& hasher) {
    { std::forward<T>(hasher).final() } -> std::same_as<content_digest>;
};

static_assert(codec::content_digest_bytes == 16);
static_assert(std::same_as<content_digest, std::array<unsigned char, 16>>);
static_assert(!std::is_copy_constructible_v<xxh3_128_hasher>);
static_assert(!std::is_copy_assignable_v<xxh3_128_hasher>);
static_assert(!std::is_move_constructible_v<xxh3_128_hasher>);
static_assert(!std::is_move_assignable_v<xxh3_128_hasher>);
static_assert(std::is_nothrow_default_constructible_v<xxh3_128_hasher>);
static_assert(std::is_nothrow_destructible_v<xxh3_128_hasher>);
// Owners, including coroutine frames, need no extended alignment.
static_assert(alignof(xxh3_128_hasher) == 1);
static_assert(can_finalize<xxh3_128_hasher>);
static_assert(!can_finalize<xxh3_128_hasher&>);
static_assert(!can_finalize<const xxh3_128_hasher&>);
static_assert(noexcept(std::declval<xxh3_128_hasher&>().update(nullptr, 0)));
static_assert(noexcept(std::declval<xxh3_128_hasher&&>().final()));

std::string hex(const content_digest& digest) {
    constexpr std::string_view digits{"0123456789ABCDEF"};
    std::string encoded;
    for (const auto octet : digest) {
        encoded.push_back(digits[octet >> 4U]);
        encoded.push_back(digits[octet & 0x0fU]);
    }
    return encoded;
}

// The sanity buffer xxHash's own checks hash.
std::vector<unsigned char> sanity_buffer(std::size_t size) {
    std::vector<unsigned char> buffer(size);
    std::uint64_t generator = 2654435761U;
    for (auto& octet : buffer) {
        octet = static_cast<unsigned char>(generator >> 56U);
        generator *= 11400714785074694797ULL;
    }
    return buffer;
}

constexpr std::array<unsigned char, 7> binary_payload{
  0x00, 0x01, 0x7f, 0x80, 0xff, 0x00, 0x42};

content_digest streamed(
  const std::vector<unsigned char>& input,
  std::size_t size,
  std::size_t split) {
    xxh3_128_hasher hasher;
    EXPECT_EQ(&hasher.update(nullptr, 0), &hasher);
    hasher.update(input.data(), split);
    hasher.update(nullptr, 0);
    hasher.update(input.data() + split, size - split);
    return std::move(hasher).final();
}

TEST(CodecXxh3Test, OfficialVectorsAgreeOneShotAndStreamed) {
    // xxHash's seed-0 XXH128 sanity vectors, written high 64 bits then low.
    struct vector_case {
        std::size_t size;
        std::string_view expected;
    };
    constexpr std::array cases{
      vector_case{0, "99AA06D3014798D86001C324468D497F"sv},
      vector_case{1, "A6CD5E9392000F6AC44BDFF4074EECDB"sv},
      vector_case{6, "082AFE0B8162D12A3E7039BDDA43CFC6"sv},
      vector_case{12, "6E3EFD8FC7802B18061A192713F69AD9"sv},
      vector_case{24, "0CE966E4678D37611E7044D28B1B901D"sv},
      vector_case{48, "A002AC4E5478227EF942219AED80F67B"sv},
      vector_case{81, "4952F58181AB00425E8BAFB9F95FB803"sv},
      vector_case{222, "337E09641B948717F1AEBD597CEC6B3A"sv},
      vector_case{403, "1B6DE21E332DD73DCDEB804D65C6DEA4"sv},
      vector_case{512, "18D2D110DCC9BCA1617E49599013CB6B"sv},
      vector_case{2048, "F736557FD47073A5DD59E2C3A5F038E0"sv},
      vector_case{2240, "CCB134FBFA7CE49D6E73A90539CF2948"sv},
      vector_case{2367, "E89C0F6FF369B427CB37AEB9E5D361ED"sv}};
    const auto input = sanity_buffer(2367);
    for (const auto& value : cases) {
        SCOPED_TRACE(value.size);
        EXPECT_EQ(
          hex(codec::xxh3_128(input.data(), value.size)), value.expected);
        for (std::size_t split = 0; split <= value.size;
             split += value.size > 256 ? 61 : 1)
            EXPECT_EQ(hex(streamed(input, value.size, split)), value.expected);
        EXPECT_EQ(hex(streamed(input, value.size, value.size)), value.expected);
    }
}

TEST(CodecXxh3Test, SmallFragmentsMatchOneShotAcrossBlockBoundaries) {
    const auto input = sanity_buffer(70000);
    for (const std::size_t size :
         {239UL, 240UL, 241UL, 1023UL, 1024UL, 1025UL, 65536UL, 70000UL}) {
        SCOPED_TRACE(size);
        xxh3_128_hasher hasher;
        for (std::size_t at = 0, piece = 1; at < size;) {
            const auto count = std::min(piece, size - at);
            hasher.update(input.data() + at, count);
            at += count;
            piece = piece * 3 % 257 + 1;
        }
        EXPECT_EQ(
          std::move(hasher).final(), codec::xxh3_128(input.data(), size));
    }
}

// A compact value schema: for every prefix length below 430 of a repeated
// word, the sum of both halves modulo 61 selects one character. It crosses
// every XXH3 size path, including the first long-input stripes.
std::string value_schema(std::string_view repeat, std::size_t limit) {
    constexpr std::string_view encode{
      "abcdefghijklmnopqrstuvwxyz123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"};
    std::string input;
    while (input.size() < limit)
        input.append(repeat);
    std::string result;
    for (std::size_t size = 0; size < limit; ++size) {
        const auto digest = codec::xxh3_128(input.data(), size);
        std::uint64_t high = 0, low = 0;
        for (std::size_t i = 0; i < 8; ++i) {
            high = high << 8U | digest[i];
            low = low << 8U | digest[8 + i];
        }
        result.push_back(encode[(high + low) % 61]);
    }
    return result;
}

TEST(CodecXxh3Test, ValueSchemaCoversEverySizePath) {
    EXPECT_EQ(
      value_schema("foo", 430),
      "bUMA3As8n9I4vNGhThXlEevxZlyMcbb6TYAlIKJ2f5ponsv99q962rYclQ7u3gfnRdCDQ5JI"
      "2LrGUaCycbXrvLFe4SjgRb9RQwCfrnmNQ7VSEwSKMnkGCK3bDbXSrnIh5qLXdtvIZklbJpGH"
      "Dqr93BlqF9ubTnOSYkSdx89XvQqflMIW8bjfQp9BPjQejWOeEQspnN1D3sfgVdFhpaQdHYA5"
      "pI2XcPlCMFPxvrFuRr7joaDvjNe9IUZaunLPMewuXmC3EL95h52Ju3D7y9RNKhgYxMTrA84B"
      "yJrMvyjdm3vlBxet4EN7v2GEyjbGuaZW9UL6lrX6PghJDg7ACfLGdxNbH3qXM4zaiG2RKnL5"
      "S3WXKR78RBB5fRFQ8KDIEQjHFvSNsc3GrAEi6W8P2lv8JMTzjBODO2uN4wadVQFT9wpGfV");
}

TEST(CodecXxh3Test, EmptyInputConstantMatchesTheHash) {
    EXPECT_EQ(codec::xxh3_128(nullptr, 0), codec::xxh3_128_empty);
    EXPECT_EQ(xxh3_128_hasher{}.final(), codec::xxh3_128_empty);
}

TEST(CodecXxh3Test, InlineStateHashesAtEveryPlacementOffset) {
    const auto input = sanity_buffer(4096);
    const auto expected = codec::xxh3_128(input.data(), input.size());
    alignas(64) std::array<std::byte, 64 + sizeof(xxh3_128_hasher)> storage{};
    for (std::size_t offset = 0; offset < 64; ++offset) {
        SCOPED_TRACE(offset);
        auto* hasher = ::new (storage.data() + offset) xxh3_128_hasher;
        hasher->update(input.data(), 1000);
        hasher->update(input.data() + 1000, input.size() - 1000);
        EXPECT_EQ(std::move(*hasher).final(), expected);
        hasher->~xxh3_128_hasher();
    }
}

TEST(CodecXxh3Test, NamedPrefixesHashExactlyOneTerminatorBeforePayload) {
    xxh3_128_hasher batch;
    batch.update(
      codec::semantic_batch_domain.data(), codec::semantic_batch_domain.size());
    batch.update(binary_payload.data(), binary_payload.size());
    EXPECT_EQ(
      hex(std::move(batch).final()), "D469E1CFE293EACBDBB95DC2AA132292");

    xxh3_128_hasher checkpoint;
    checkpoint.update(
      codec::checkpoint_domain.data(), codec::checkpoint_domain.size());
    checkpoint.update(binary_payload.data(), binary_payload.size());
    EXPECT_EQ(
      hex(std::move(checkpoint).final()), "9D4AA1EFCDC988F7D991699BC7C5959E");
}

TEST(CodecXxh3Test, ObjectAndExtentTagsPreserveTheUnprefixedDigestBytes) {
    const auto raw = codec::xxh3_128(
      binary_payload.data(), binary_payload.size());
    const codec::immutable_object_digest object{raw};
    const codec::extent_digest extent{raw};
    EXPECT_EQ(hex(raw), "A3E16817866AEACF047E1E4199919B4A");
    EXPECT_EQ(object.bytes(), raw);
    EXPECT_EQ(extent.bytes(), raw);
}

} // namespace
