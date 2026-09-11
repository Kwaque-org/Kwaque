#include "src/base/error.h"
#include "src/codec/error.h"
#include "src/codec/limits.h"

#include <gtest/gtest.h>

#include <array>
#include <concepts>
#include <cstdint>
#include <expected>
#include <limits>
#include <type_traits>
#include <utility>

namespace {

using kwaque::errc;
using kwaque::codec::error;
using kwaque::codec::result;

static_assert(
  std::same_as<
    decltype(kwaque::codec::limits::make(kwaque::codec::limits_config{})),
    kwaque::result<kwaque::codec::limits>>);
static_assert(std::same_as<
              decltype(kwaque::codec::limits::defaults().validate_allocation(
                kwaque::byte_count{})),
              kwaque::result<void>>);

static_assert(sizeof(error) == 16);
static_assert(std::is_trivially_copyable_v<error>);
static_assert(std::is_standard_layout_v<error>);
static_assert(std::is_nothrow_copy_constructible_v<error>);
static_assert(std::is_nothrow_move_constructible_v<error>);
static_assert(std::is_nothrow_destructible_v<error>);
static_assert(!std::default_initializable<error>);
static_assert(!std::is_aggregate_v<error>);
static_assert(!std::is_convertible_v<errc, error>);
static_assert(std::same_as<result<int>, std::expected<int, error>>);
static_assert(std::same_as<result<void>, std::expected<void, error>>);
static_assert(std::same_as<
              decltype(kwaque::codec::failure(error{errc::malformed_data})),
              std::unexpected<error>>);
static_assert(noexcept(error{errc::truncated_data, 1, 2, 3}));
static_assert(noexcept(kwaque::codec::failure(error{errc::truncated_data})));
static_assert(noexcept(std::declval<const error&>().code()));
static_assert(noexcept(std::declval<const error&>().family()));
static_assert(noexcept(std::declval<const error&>().field()));
static_assert(noexcept(std::declval<const error&>().byte_offset()));
static_assert(
  noexcept(std::declval<const error&>() == std::declval<const error&>()));
static_assert(
  std::same_as<decltype(std::declval<const error&&>().code()), errc>);
static_assert(std::same_as<
              decltype(std::declval<const error&&>().family()),
              std::uint16_t>);
static_assert(
  std::same_as<decltype(std::declval<const error&&>().field()), std::uint16_t>);
static_assert(std::same_as<
              decltype(std::declval<const error&&>().byte_offset()),
              std::uint64_t>);

constexpr error compile_time_error{errc::corrupt_data, 2, 7, 321};
static_assert(compile_time_error.code() == errc::corrupt_data);
static_assert(compile_time_error.family() == 2);
static_assert(compile_time_error.field() == 7);
static_assert(compile_time_error.byte_offset() == 321);
static_assert(compile_time_error == error{errc::corrupt_data, 2, 7, 321});
constexpr result<int> compile_time_failure = kwaque::codec::failure(
  compile_time_error);
static_assert(!compile_time_failure.has_value());
static_assert(compile_time_failure.error() == compile_time_error);

TEST(CodecErrorTest, DefaultDiagnosticsKeepZeroAsAValidOffset) {
    const error value{errc::truncated_data};
    EXPECT_EQ(value.code(), errc::truncated_data);
    EXPECT_EQ(value.family(), 0U);
    EXPECT_EQ(value.field(), 0U);
    EXPECT_EQ(value.byte_offset(), 0U);
}

TEST(CodecErrorTest, OwnsEveryDiagnosticAndAllowsTemporaryGetters) {
    auto code = errc::wrong_context;
    std::uint16_t family = 2;
    std::uint16_t field = 17;
    std::uint64_t offset = 256;
    const error saved{code, family, field, offset};
    code = errc::corrupt_data;
    family = 3;
    field = 18;
    offset = 257;
    EXPECT_EQ(saved, (error{errc::wrong_context, 2, 17, 256}));
    EXPECT_NE(saved, (error{code, family, field, offset}));

    const auto copied = [] {
        const error local{errc::malformed_data, 4, 9, 1234};
        return local;
    }();
    EXPECT_EQ(copied, (error{errc::malformed_data, 4, 9, 1234}));
    EXPECT_EQ(error{saved}.code(), errc::wrong_context);
    EXPECT_EQ(error{saved}.family(), 2U);
    EXPECT_EQ(error{saved}.field(), 17U);
    EXPECT_EQ(error{saved}.byte_offset(), 256U);
}

TEST(CodecErrorTest, PreservesFullWidthDiagnostics) {
    constexpr auto max_field = std::numeric_limits<std::uint16_t>::max();
    constexpr auto max_offset = std::numeric_limits<std::uint64_t>::max();
    const error value{
      errc::unsupported_format, max_field, max_field, max_offset};
    EXPECT_EQ(value.family(), max_field);
    EXPECT_EQ(value.field(), max_field);
    EXPECT_EQ(value.byte_offset(), max_offset);
}

TEST(CodecErrorTest, EqualityIncludesEveryComponent) {
    const error original{errc::malformed_data, 2, 3, 4};
    const std::array changed{
      error{errc::truncated_data, 2, 3, 4},
      error{errc::malformed_data, 1, 3, 4},
      error{errc::malformed_data, 2, 1, 4},
      error{errc::malformed_data, 2, 3, 1}};
    EXPECT_EQ(original, error{original});
    for (const auto& value : changed) {
        EXPECT_NE(original, value);
        EXPECT_NE(value, original);
    }
}

TEST(CodecErrorTest, ResultCarriesTheCompleteFailureByValue) {
    error source{errc::resource_exhausted, 6, 11, 4096};
    const result<int> failed = kwaque::codec::failure(source);
    source = error{errc::aborted};
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error(), (error{errc::resource_exhausted, 6, 11, 4096}));
    EXPECT_EQ(source.code(), errc::aborted);

    const result<void> failed_void = kwaque::codec::failure(
      error{errc::corrupt_data, 8, 13, 8192});
    ASSERT_FALSE(failed_void.has_value());
    EXPECT_EQ(failed_void.error(), (error{errc::corrupt_data, 8, 13, 8192}));

    const result<int> succeeded{37};
    ASSERT_TRUE(succeeded.has_value());
    EXPECT_EQ(*succeeded, 37);
    const result<void> succeeded_void;
    EXPECT_TRUE(succeeded_void.has_value());
}

} // namespace
