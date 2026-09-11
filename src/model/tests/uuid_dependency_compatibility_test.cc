#include "src/model/identity.h"

#include <boost/uuid/string_generator.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <span>

namespace {

using kwaque::model::producer_id;

TEST(UuidDependencyCompatibilityTest, PreservesExactBytesAndCanonicalText) {
    constexpr char text[] = "00112233-4455-6677-8899-aabbccddeeff";
    constexpr std::array<std::uint8_t, 16> bytes{
      0x00,
      0x11,
      0x22,
      0x33,
      0x44,
      0x55,
      0x66,
      0x77,
      0x88,
      0x99,
      0xaa,
      0xbb,
      0xcc,
      0xdd,
      0xee,
      0xff};
    const auto native = boost::uuids::string_generator{}(text);
    const auto model = producer_id::make(bytes);
    ASSERT_TRUE(model.has_value());
    EXPECT_TRUE(std::ranges::equal(model->bytes(), native));
    EXPECT_EQ(boost::uuids::to_string(native), text);

    boost::uuids::uuid copied{};
    std::ranges::copy(model->bytes(), copied.begin());
    EXPECT_EQ(copied, native);
    EXPECT_EQ(boost::uuids::to_string(copied), text);
    const auto roundtrip = producer_id::make(
      std::span<const std::uint8_t>{copied.begin(), copied.size()});
    ASSERT_TRUE(roundtrip.has_value());
    EXPECT_EQ(*roundtrip, *model);
}

TEST(UuidDependencyCompatibilityTest, PreservesNativeUnsignedOctetOrdering) {
    const auto native_low = boost::uuids::string_generator{}(
      "7fffffff-ffff-ffff-ffff-ffffffffffff");
    const auto native_high = boost::uuids::string_generator{}(
      "80000000-0000-0000-0000-000000000000");
    const auto low = producer_id::make(
      std::span<const std::uint8_t>{native_low.begin(), native_low.size()});
    const auto high = producer_id::make(
      std::span<const std::uint8_t>{native_high.begin(), native_high.size()});
    ASSERT_TRUE(low.has_value());
    ASSERT_TRUE(high.has_value());
    EXPECT_TRUE(native_low < native_high);
    EXPECT_TRUE(low->canonical_less(*high));
    EXPECT_EQ(low->canonical_less(*high), native_low < native_high);
    EXPECT_EQ(high->canonical_less(*low), native_high < native_low);
    EXPECT_EQ(*low == *high, native_low == native_high);
}

} // namespace
