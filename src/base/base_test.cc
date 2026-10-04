#include "src/base/error.h"
#include "src/base/result.h"
#include "src/base/units.h"

#include <gtest/gtest.h>

#include <concepts>
#include <cstdint>
#include <limits>
#include <system_error>

namespace {

template<typename Left, typename Right>
concept addable = requires(Left lhs, Right rhs) { lhs + rhs; };

static_assert(!addable<kwaque::byte_count, kwaque::byte_count>);
static_assert(!addable<kwaque::item_count, kwaque::item_count>);
static_assert(!addable<kwaque::byte_count, kwaque::item_count>);
static_assert(!std::convertible_to<std::uint64_t, kwaque::byte_count>);

kwaque::result<int> validate_nonnegative(int value) {
    if (value < 0) [[unlikely]] {
        return kwaque::failure(kwaque::errc::out_of_range);
    }
    return value;
}

kwaque::result<int> increment_nonnegative(int value) {
    auto validated = validate_nonnegative(value);
    if (!validated) {
        return kwaque::failure(validated.error());
    }
    return *validated + 1;
}

TEST(UnitsTest, ChecksStrongTypeArithmetic) {
    constexpr kwaque::byte_count first{1024};
    constexpr kwaque::byte_count second{512};
    constexpr auto sum = first.checked_add(second);
    constexpr auto difference = first.checked_sub(second);
    static_assert(sum.has_value() && sum->value() == 1536);
    static_assert(difference.has_value() && difference->value() == 512);

    constexpr kwaque::byte_count maximum{
      std::numeric_limits<kwaque::byte_count::value_type>::max()};
    static_assert(!maximum.checked_add(kwaque::byte_count{1}).has_value());
    static_assert(
      !kwaque::byte_count{0}.checked_sub(kwaque::byte_count{1}).has_value());

    ASSERT_TRUE(sum.has_value());
    EXPECT_EQ(sum->value(), 1536U);
}

TEST(UnitsTest, ChecksMultiplication) {
    constexpr kwaque::byte_count block{4096};
    static_assert(block.checked_mul(3)->value() == 12288U);
    static_assert(!kwaque::byte_count{std::uint64_t{1} << 32U}
                     .checked_mul(std::uint64_t{1} << 32U)
                     .has_value());
    EXPECT_EQ(block.checked_mul(0)->value(), 0U);
}

TEST(UnitsTest, SizeLiteralsUseSixtyFourBitArithmetic) {
    using namespace kwaque::literals;
    static_assert(1_KiB == 1024U);
    static_assert(64_MiB == std::uint64_t{64} * 1024U * 1024U);
    // A 32-bit product of these factors would wrap to zero.
    static_assert(4_GiB == std::uint64_t{4} << 30U);
    static_assert(std::same_as<decltype(1_GiB), std::uint64_t>);
    static_assert(1_TiB == std::uint64_t{1} << 40U);
    static_assert(16'777'215_TiB == std::uint64_t{16'777'215} << 40U);
    // A default member initializer must not make its class throwing.
    static_assert(noexcept(1_KiB) && noexcept(1_TiB));
    EXPECT_EQ(2_GiB, 2147483648U);
}

TEST(ErrorTest, IntegratesWithStandardErrorCodes) {
    const std::error_code error = kwaque::errc::malformed_data;
    EXPECT_EQ(error.category().name(), std::string_view("kwaque"));
    EXPECT_EQ(error.message(), "malformed data");
    EXPECT_EQ(error, std::errc::invalid_argument);
    EXPECT_NE(error, std::errc::result_out_of_range);
}

TEST(ResultTest, HoldsAndPropagatesValuesAndErrors) {
    const auto success = increment_nonnegative(41);
    ASSERT_TRUE(success.has_value());
    EXPECT_EQ(*success, 42);

    const auto failure = increment_nonnegative(-1);
    ASSERT_FALSE(failure.has_value());
    EXPECT_EQ(failure.error(), kwaque::errc::out_of_range);

    const kwaque::result<void> empty_success;
    EXPECT_TRUE(empty_success.has_value());
}

} // namespace
