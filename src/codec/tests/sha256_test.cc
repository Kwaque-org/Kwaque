#include "src/codec/digest.h"
#include "src/codec/sha256.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <concepts>
#include <cstddef>
#include <string_view>
#include <type_traits>
#include <utility>

namespace {

using namespace std::literals;
namespace codec = kwaque::codec;
using codec::sha256_digest;
using codec::sha256_hasher;

template<typename T>
concept can_finalize = requires(T&& hasher) {
    { std::forward<T>(hasher).final() } -> std::same_as<sha256_digest>;
};

static_assert(codec::sha256_digest_bytes == 32);
static_assert(std::same_as<sha256_digest, std::array<unsigned char, 32>>);
static_assert(sizeof(sha256_digest) == 32);
static_assert(!std::is_copy_constructible_v<sha256_hasher>);
static_assert(!std::is_copy_assignable_v<sha256_hasher>);
static_assert(!std::is_move_constructible_v<sha256_hasher>);
static_assert(!std::is_move_assignable_v<sha256_hasher>);
static_assert(std::is_nothrow_destructible_v<sha256_hasher>);
static_assert(can_finalize<sha256_hasher>);
static_assert(!can_finalize<sha256_hasher&>);
static_assert(!can_finalize<const sha256_hasher>);
static_assert(!can_finalize<const sha256_hasher&>);
static_assert(std::same_as<
              decltype(std::declval<sha256_hasher&>().update(nullptr, 0)),
              sha256_hasher&>);
static_assert(!noexcept(std::declval<sha256_hasher&>().update(nullptr, 0)));
static_assert(!noexcept(std::declval<sha256_hasher&&>().final()));

void expect_digest(const sha256_digest& digest, std::string_view expected) {
    constexpr std::string_view digits{"0123456789abcdef"};
    std::array<char, 64> encoded{};
    for (std::size_t index = 0; index < digest.size(); ++index) {
        encoded[2 * index] = digits[digest[index] >> 4U];
        encoded[2 * index + 1] = digits[digest[index] & 0x0fU];
    }
    EXPECT_EQ((std::string_view{encoded.data(), encoded.size()}), expected);
}

constexpr std::array<unsigned char, 7> binary_payload{
  0x00, 0x01, 0x7f, 0x80, 0xff, 0x00, 0x42};

TEST(CodecSha256Test, KnownMessagesAgreeAtEverySplitIncludingEmptyUpdates) {
    struct vector_case {
        std::string_view message;
        std::string_view expected;
    };
    constexpr std::array cases{
      vector_case{
        ""sv,
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"sv},
      vector_case{
        "abc"sv,
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"sv},
      vector_case{
        "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"sv,
        "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"sv}};
    for (const auto& value : cases) {
        SCOPED_TRACE(value.message.size());
        for (std::size_t split = 0; split <= value.message.size(); ++split) {
            SCOPED_TRACE(split);
            sha256_hasher hasher;
            EXPECT_EQ(&hasher.update(nullptr, 0), &hasher);
            hasher.update(value.message.data(), split);
            hasher.update(nullptr, 0);
            hasher.update(
              value.message.data() + split, value.message.size() - split);
            hasher.update(nullptr, 0);
            expect_digest(std::move(hasher).final(), value.expected);
        }
    }
}

TEST(CodecSha256Test, BinaryBlockBoundariesMatchIndependentDigests) {
    struct vector_case {
        std::size_t length;
        std::string_view expected;
    };
    constexpr std::array cases{
      vector_case{
        63,
        "29af2686fd53374a36b0846694cc342177e428d1647515f078784d69cdb9e488"sv},
      vector_case{
        64,
        "fdeab9acf3710362bd2658cdc9a29e8f9c757fcf9811603a8c447cd1d9151108"sv},
      vector_case{
        65,
        "4bfd2c8b6f1eec7a2afeb48b934ee4b2694182027e6d0fc075074f2fabb31781"sv},
      vector_case{
        127,
        "92ca0fa6651ee2f97b884b7246a562fa71250fedefe5ebf270d31c546bfea976"sv},
      vector_case{
        128,
        "471fb943aa23c511f6f72f8d1652d9c880cfa392ad80503120547703e56a2be5"sv},
      vector_case{
        129,
        "5099c6a56203f9687f7d33f4bfdf576d31dc91f6b695ecea38b2770c87631135"sv}};
    std::array<unsigned char, 129> message{};
    for (std::size_t index = 0; index < message.size(); ++index) {
        message[index] = static_cast<unsigned char>(index % 251U);
    }
    for (const auto& value : cases) {
        SCOPED_TRACE(value.length);
        for (const std::size_t chunk : {1U, 7U, 63U, 64U, 65U}) {
            SCOPED_TRACE(chunk);
            sha256_hasher hasher;
            for (std::size_t offset = 0; offset < value.length;) {
                const auto size = std::min(chunk, value.length - offset);
                hasher.update(message.data() + offset, size);
                offset += size;
            }
            expect_digest(std::move(hasher).final(), value.expected);
        }
    }
}

TEST(CodecSha256Test, MillionByteVectorUsesBoundedRepeatedUpdates) {
    std::array<char, 1024> chunk{};
    chunk.fill('a');
    sha256_hasher hasher;
    constexpr std::size_t length = 1'000'000;
    for (std::size_t offset = 0; offset < length;) {
        const auto size = std::min(chunk.size(), length - offset);
        hasher.update(chunk.data(), size);
        offset += size;
    }
    expect_digest(
      std::move(hasher).final(),
      "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0"sv);
}

TEST(CodecSha256Test, NamedPrefixesHashExactlyOneTerminatorBeforePayload) {
    sha256_hasher batch;
    batch.update(
      codec::semantic_batch_domain.data(), codec::semantic_batch_domain.size());
    batch.update(binary_payload.data(), binary_payload.size());
    expect_digest(
      std::move(batch).final(),
      "916187034d64dba2815da3fe7948f9d2fe5d94cdf8d6aa2a8d92e9317da11f47"sv);

    sha256_hasher checkpoint;
    checkpoint.update(
      codec::checkpoint_domain.data(), codec::checkpoint_domain.size());
    checkpoint.update(binary_payload.data(), binary_payload.size());
    expect_digest(
      std::move(checkpoint).final(),
      "8a50bad747737456ec59ae4cf336351aa7849338016f8df9a2f7595bdc13ac4e"sv);
}

TEST(CodecSha256Test, ObjectAndExtentTagsPreserveTheUnprefixedDigestBytes) {
    sha256_hasher hasher;
    hasher.update(binary_payload.data(), binary_payload.size());
    const auto raw = std::move(hasher).final();
    const codec::immutable_object_digest object{raw};
    const codec::extent_digest extent{raw};
    constexpr std::string_view expected{
      "8184569834ae09c86f52f84662be23139718c4ac9b489954e4ac6f7e183782fa"};
    expect_digest(raw, expected);
    expect_digest(object.bytes(), expected);
    expect_digest(extent.bytes(), expected);
    EXPECT_EQ(object.bytes(), raw);
    EXPECT_EQ(extent.bytes(), raw);
}

} // namespace
