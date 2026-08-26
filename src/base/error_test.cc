#include "src/base/error.h"

#include <gtest/gtest.h>

#include <array>
#include <string>
#include <string_view>
#include <system_error>

namespace {

static_assert(static_cast<int>(kwaque::errc::success) == 0);
static_assert(static_cast<int>(kwaque::errc::invalid_argument) == 1);
static_assert(static_cast<int>(kwaque::errc::out_of_range) == 2);
static_assert(static_cast<int>(kwaque::errc::malformed_data) == 3);
static_assert(static_cast<int>(kwaque::errc::unavailable) == 4);
static_assert(static_cast<int>(kwaque::errc::truncated_data) == 17);

struct error_case final {
    kwaque::errc code;
    int value;
    std::string_view message;
};

constexpr std::array cases{
  error_case{kwaque::errc::success, 0, "success"},
  error_case{kwaque::errc::invalid_argument, 1, "invalid argument"},
  error_case{kwaque::errc::out_of_range, 2, "value out of range"},
  error_case{kwaque::errc::malformed_data, 3, "malformed data"},
  error_case{kwaque::errc::unavailable, 4, "service unavailable"},
  error_case{kwaque::errc::aborted, 5, "operation aborted"},
  error_case{kwaque::errc::closed, 6, "resource closed"},
  error_case{kwaque::errc::timed_out, 7, "operation timed out"},
  error_case{kwaque::errc::resource_exhausted, 8, "resource exhausted"},
  error_case{kwaque::errc::queue_full, 9, "queue full"},
  error_case{kwaque::errc::wrong_shard, 10, "wrong shard"},
  error_case{kwaque::errc::io_failure, 11, "I/O failure"},
  error_case{kwaque::errc::network_failure, 12, "network failure"},
  error_case{kwaque::errc::dns_failure, 13, "DNS failure"},
  error_case{kwaque::errc::fault_injected, 14, "fault injected"},
  error_case{kwaque::errc::replay_divergence, 15, "replay divergence"},
  error_case{kwaque::errc::invariant_violation, 16, "invariant violation"},
  error_case{kwaque::errc::truncated_data, 17, "truncated data"},
};

TEST(ErrorTest, PreservesStableValuesAndRoundTripsEveryCode) {
    for (const auto& test_case : cases) {
        const std::error_code error = kwaque::make_error_code(test_case.code);
        EXPECT_EQ(error.category().name(), std::string_view{"kwaque"});
        EXPECT_EQ(error.value(), test_case.value);
        EXPECT_EQ(error.message(), test_case.message);
        EXPECT_EQ(static_cast<kwaque::errc>(error.value()), test_case.code);
        EXPECT_EQ(error, test_case.code);
        EXPECT_EQ(error.message().find('\n'), std::string::npos);
        EXPECT_EQ(error.message().find('\r'), std::string::npos);
    }
}

TEST(ErrorTest, MapsOnlyPortableConditionsToStandardEquivalents) {
    EXPECT_EQ(
      kwaque::make_error_code(kwaque::errc::aborted),
      std::errc::operation_canceled);
    EXPECT_EQ(
      kwaque::make_error_code(kwaque::errc::closed), std::errc::broken_pipe);
    EXPECT_EQ(
      kwaque::make_error_code(kwaque::errc::timed_out), std::errc::timed_out);
    EXPECT_EQ(
      kwaque::make_error_code(kwaque::errc::resource_exhausted),
      std::errc::resource_unavailable_try_again);
    EXPECT_EQ(
      kwaque::make_error_code(kwaque::errc::queue_full),
      std::errc::no_buffer_space);
    EXPECT_EQ(
      kwaque::make_error_code(kwaque::errc::io_failure), std::errc::io_error);
    EXPECT_EQ(
      kwaque::make_error_code(kwaque::errc::network_failure),
      std::errc::network_unreachable);
    EXPECT_EQ(
      kwaque::make_error_code(kwaque::errc::dns_failure),
      std::errc::host_unreachable);
    EXPECT_NE(
      kwaque::make_error_code(kwaque::errc::truncated_data),
      std::errc::result_out_of_range);
    EXPECT_NE(
      kwaque::make_error_code(kwaque::errc::wrong_shard),
      std::errc::invalid_argument);
}

} // namespace
