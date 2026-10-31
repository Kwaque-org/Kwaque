#include "src/model/identity.h"
#include "src/model/identity_mint.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <span>

namespace {

namespace model = kwaque::model;

// Supplies the octets it is given, then the next pattern.
class scripted_entropy final {
public:
    explicit scripted_entropy(std::array<std::uint8_t, 16> octets) noexcept
      : octets_(octets) {}

    void fill(std::span<std::uint8_t, 16> out) noexcept {
        std::copy(octets_.begin(), octets_.end(), out.begin());
    }

private:
    std::array<std::uint8_t, 16> octets_;
};

// A seeded split-mix sequence; the same octets every run.
class mixed_entropy final {
public:
    void fill(std::span<std::uint8_t, 16> out) noexcept {
        for (std::size_t word = 0; word < 2; ++word) {
            state_ += 0x9e3779b97f4a7c15ULL;
            auto mixed = state_;
            mixed = (mixed ^ (mixed >> 30U)) * 0xbf58476d1ce4e5b9ULL;
            mixed = (mixed ^ (mixed >> 27U)) * 0x94d049bb133111ebULL;
            mixed ^= mixed >> 31U;
            for (std::size_t octet = 0; octet < 8; ++octet)
                out[word * 8 + octet] = static_cast<std::uint8_t>(
                  mixed >> (octet * 8U));
        }
    }

private:
    std::uint64_t state_{0x6b77617175650003ULL};
};

static_assert(model::object_id_entropy<scripted_entropy>);
static_assert(model::object_id_entropy<mixed_entropy>);

template<typename Id>
std::array<std::uint8_t, 16> octets_of(const Id& id) {
    std::array<std::uint8_t, 16> out{};
    const auto bytes = id.bytes();
    std::copy(bytes.begin(), bytes.end(), out.begin());
    return out;
}

bool is_fixed_bit(std::size_t bit) noexcept {
    // Octet 6's high nibble and octet 8's top two bits, most significant bit
    // of each octet first.
    const auto octet = bit / 8;
    const auto position = 7 - bit % 8;
    return (octet == 6 && position >= 4) || (octet == 8 && position >= 6);
}

TEST(ModelIdentityMintTest, SetsTheVersionFourLayoutOverTheDrawnOctets) {
    std::array<std::uint8_t, 16> counting{};
    for (std::size_t at = 0; at < counting.size(); ++at)
        counting[at] = static_cast<std::uint8_t>(at);
    scripted_entropy source{counting};
    const auto minted = octets_of(
      model::mint_object_id<model::topic_id>(source));
    const std::array<std::uint8_t, 16> expected{
      0x00,
      0x01,
      0x02,
      0x03,
      0x04,
      0x05,
      0x46,
      0x07,
      0x88,
      0x09,
      0x0a,
      0x0b,
      0x0c,
      0x0d,
      0x0e,
      0x0f};
    EXPECT_EQ(minted, expected);
}

TEST(ModelIdentityMintTest, AllZeroAndAllOneDrawsAreNeitherNilNorMax) {
    scripted_entropy zeros{std::array<std::uint8_t, 16>{}};
    const auto low = model::mint_object_id<model::range_id>(zeros);
    EXPECT_FALSE(low.is_nil());
    const std::array<std::uint8_t, 16> low_expected{
      0, 0, 0, 0, 0, 0, 0x40, 0, 0x80, 0, 0, 0, 0, 0, 0, 0};
    EXPECT_TRUE(std::ranges::equal(low.bytes(), low_expected));

    std::array<std::uint8_t, 16> all_ones{};
    all_ones.fill(0xff);
    scripted_entropy ones{all_ones};
    const auto high = model::mint_object_id<model::segment_id>(ones);
    auto high_expected = all_ones;
    high_expected[6] = 0x4f;
    high_expected[8] = 0xbf;
    EXPECT_TRUE(std::ranges::equal(high.bytes(), high_expected));
}

TEST(ModelIdentityMintTest, EveryFreeBitComesFromItsDrawnBit) {
    std::size_t free_bits = 0;
    for (std::size_t bit = 0; bit < 128; ++bit) {
        SCOPED_TRACE(bit);
        std::array<std::uint8_t, 16> single{};
        single[bit / 8] = static_cast<std::uint8_t>(0x80U >> (bit % 8));
        scripted_entropy source{single};
        auto minted = octets_of(
          model::mint_object_id<model::producer_id>(source));
        EXPECT_EQ(minted[6] & 0xf0U, 0x40U);
        EXPECT_EQ(minted[8] & 0xc0U, 0x80U);
        if (is_fixed_bit(bit)) continue;
        ++free_bits;
        minted[6] = static_cast<std::uint8_t>(minted[6] & 0x0fU);
        minted[8] = static_cast<std::uint8_t>(minted[8] & 0x3fU);
        EXPECT_EQ(minted, single);
    }
    EXPECT_EQ(free_bits, 122U);
}

TEST(ModelIdentityMintTest, FixedBitsNeverVaryAndFreeBitsAreUnbiased) {
    constexpr std::size_t mints = std::size_t{1} << 20U;
    mixed_entropy source;
    std::array<std::uint64_t, 128> set{};
    for (std::size_t round = 0; round < mints; ++round) {
        const auto minted = octets_of(
          model::mint_object_id<model::topic_id>(source));
        ASSERT_EQ(minted[6] >> 4U, 0x4U);
        ASSERT_EQ(minted[8] >> 6U, 0x2U);
        for (std::size_t bit = 0; bit < 128; ++bit)
            set[bit] += (minted[bit / 8] >> (7 - bit % 8)) & 1U;
    }
    // Five standard deviations of a fair coin over this many draws.
    const auto tolerance = 5.0 * std::sqrt(static_cast<double>(mints) * 0.25);
    for (std::size_t bit = 0; bit < 128; ++bit) {
        if (is_fixed_bit(bit)) continue;
        SCOPED_TRACE(bit);
        EXPECT_LE(
          std::abs(
            static_cast<double>(set[bit]) - static_cast<double>(mints) / 2),
          tolerance);
    }
}

} // namespace
