#include "src/runtime/operation_statistics.h"
#include "src/runtime/operation_statistics_test_support.h"

#include <gtest/gtest.h>

#include <limits>
#include <type_traits>
#include <utility>

namespace {

static_assert(
  std::is_trivially_destructible_v<kwaque::runtime::operation_statistics>);
static_assert(
  sizeof(kwaque::runtime::operation_statistics)
  == sizeof(kwaque::runtime::operation_statistics_snapshot));

TEST(OperationStatisticsTest, AdmissionReservationOwnsOneTerminalTransition) {
    kwaque::runtime::operation_statistics statistics;
    {
        auto first = statistics.accept();
        EXPECT_EQ(
          statistics.snapshot(),
          (kwaque::runtime::operation_statistics_snapshot{
            .active = 1,
            .accepted = 1,
          }));
        auto moved = std::move(first);
        moved.add_completed_bytes(17);
    }
    statistics.reject();
    statistics.reject();
    EXPECT_EQ(
      statistics.snapshot(),
      (kwaque::runtime::operation_statistics_snapshot{
        .active = 0,
        .accepted = 1,
        .completed = 1,
        .rejected = 2,
        .completed_bytes = 17,
      }));
}

TEST(OperationStatisticsTest, LifetimeTotalsUseUnsignedModuloArithmetic) {
    constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();
    kwaque::runtime::operation_statistics statistics;
    kwaque::runtime::operation_statistics_test_access::seed(
      statistics,
      kwaque::runtime::operation_statistics_snapshot{
        .active = 0,
        .accepted = maximum,
        .completed = maximum,
        .rejected = maximum,
        .completed_bytes = maximum,
      });
    {
        auto reservation = statistics.accept();
        reservation.add_completed_bytes(1);
    }
    statistics.reject();
    EXPECT_EQ(
      statistics.snapshot(),
      (kwaque::runtime::operation_statistics_snapshot{}));
}

} // namespace
