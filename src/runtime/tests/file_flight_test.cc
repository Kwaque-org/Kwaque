#include "src/base/error.h"
#include "src/runtime/file_flight.h"
#include "src/runtime/operation_statistics.h"
#include "src/runtime/stall_watch.h"
#include "src/runtime/time.h"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>

namespace {
using namespace kwaque::runtime;

// The clock the flights read, set by each case.
std::uint64_t ticks = 0;
monotonic_time tick() noexcept { return monotonic_time{ticks}; }

// Operations are kept in the order they started. The oldest one's start is
// a read, and the stamp is written only when the oldest one changes.
TEST(FileFlightTest, TheOldestOperationInFlightIsARead) {
    std::atomic<std::uint64_t> stamp{7};
    file_flight flight{{tick, &stamp}};
    EXPECT_EQ(stamp.load(), no_file_operation_in_flight);
    EXPECT_FALSE(flight.oldest().has_value());
    ticks = 100;
    std::optional<file_flight::entry> first{std::in_place, &flight};
    EXPECT_EQ(flight.oldest(), monotonic_time{100});
    EXPECT_EQ(stamp.load(), 100U);
    ticks = 200;
    std::optional<file_flight::entry> second{std::in_place, &flight};
    ticks = 300;
    std::optional<file_flight::entry> third{std::in_place, &flight};
    EXPECT_EQ(flight.oldest(), monotonic_time{100});
    // One that is not the oldest ends: nothing is written.
    stamp.store(1);
    second.reset();
    EXPECT_EQ(stamp.load(), 1U);
    EXPECT_EQ(flight.oldest(), monotonic_time{100});
    // The oldest ends: the next oldest is.
    first.reset();
    EXPECT_EQ(flight.oldest(), monotonic_time{300});
    EXPECT_EQ(stamp.load(), 300U);
    third.reset();
    EXPECT_FALSE(flight.oldest().has_value());
    EXPECT_EQ(stamp.load(), no_file_operation_in_flight);
}

// A token moves with its operation and keeps its place.
TEST(FileFlightTest, AMovedTokenKeepsItsPlace) {
    std::atomic<std::uint64_t> stamp{0};
    file_flight flight{{tick, &stamp}};
    operation_statistics statistics;
    ticks = 10;
    std::optional<file_operation> first{std::in_place, statistics, &flight};
    ticks = 20;
    std::optional<file_operation> second{std::in_place, statistics, &flight};
    {
        file_operation moved{std::move(*first)};
        first.reset();
        EXPECT_EQ(flight.oldest(), monotonic_time{10});
        EXPECT_EQ(statistics.snapshot().active, 2U);
        file_operation again{std::move(moved)};
        EXPECT_EQ(flight.oldest(), monotonic_time{10});
    }
    EXPECT_EQ(flight.oldest(), monotonic_time{20});
    EXPECT_EQ(stamp.load(), 20U);
    {
        file_operation last{std::move(*second)};
        second.reset();
        EXPECT_EQ(flight.oldest(), monotonic_time{20});
    }
    EXPECT_FALSE(flight.oldest().has_value());
    EXPECT_EQ(statistics.snapshot().active, 0U);
    EXPECT_EQ(statistics.snapshot().completed, 2U);
    // With no order to be in, an operation is only counted.
    {
        const file_operation plain{statistics, nullptr};
    }
    EXPECT_EQ(statistics.snapshot().completed, 3U);
}

// Each watched device's operations are kept apart, by the root its files
// are under.
TEST(FileFlightTest, DevicesAreKeptApartByTheirRoots) {
    file_flights flights;
    ASSERT_TRUE(flights.watch("/data/a", {tick, nullptr}).has_value());
    ASSERT_TRUE(flights.watch("/data/ab", {tick, nullptr}).has_value());
    const auto twice = flights.watch("/data/a", {tick, nullptr});
    ASSERT_FALSE(twice.has_value());
    EXPECT_EQ(twice.error().code(), kwaque::errc::already_exists);
    EXPECT_FALSE(flights.watch("/data/a/inner", {tick, nullptr}).has_value());
    EXPECT_FALSE(flights.watch("/data", {tick, nullptr}).has_value());
    EXPECT_FALSE(flights.watch("/data/c", {nullptr, nullptr}).has_value());
    EXPECT_FALSE(flights.watch("/data/c/", {tick, nullptr}).has_value());
    EXPECT_NE(flights.under("/data/a/shards/0/x").get(), nullptr);
    EXPECT_NE(flights.under("/data/a").get(), nullptr);
    EXPECT_NE(
      flights.under("/data/a/x").get(), flights.under("/data/ab/x").get());
    EXPECT_EQ(flights.under("/data/abc/x").get(), nullptr);
    EXPECT_EQ(flights.under("/other").get(), nullptr);
    ticks = 5;
    {
        const file_flight::entry on_a{flights.under("/data/a/x").get()};
        EXPECT_EQ(flights.oldest("/data/a"), monotonic_time{5});
        EXPECT_FALSE(flights.oldest("/data/ab").has_value());
    }
    EXPECT_FALSE(flights.oldest("/data/a").has_value());
    // No more devices than the runtime schedules apart.
    file_flights full;
    for (char name = 'a'; name != 'a' + 8; ++name)
        ASSERT_TRUE(
          full.watch(std::string{"/d/"} + name, {tick, nullptr}).has_value());
    const auto ninth = full.watch("/d/z", {tick, nullptr});
    ASSERT_FALSE(ninth.has_value());
    EXPECT_EQ(ninth.error().code(), kwaque::errc::resource_exhausted);
}

// The stamps of every shard are read by one scan, which finds the
// operation that has been in flight for longest past a threshold.
TEST(StallWatchTest, TheOldestStampPastTheThresholdIsFound) {
    stall_stamps stamps{3};
    EXPECT_EQ(stamps.shards(), 3U);
    EXPECT_EQ(stamps.at(3, 0), nullptr);
    EXPECT_EQ(stamps.at(0, stall_stamps::devices), nullptr);
    EXPECT_FALSE(stamps.oldest_past(1'000, 10).has_value());
    stamps.at(1, 2)->store(900);
    stamps.at(2, 7)->store(400);
    // Exactly the threshold is not past it.
    EXPECT_FALSE(stamps.oldest_past(1'000, 600).has_value());
    const auto found = stamps.oldest_past(1'000, 599);
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(*found, (stalled_operation{2, 7, 600}));
    EXPECT_EQ(stamps.oldest_past(1'000, 50), (stalled_operation{2, 7, 600}));
    // A stamp from after the reading is no age at all.
    stamps.at(2, 7)->store(2'000);
    EXPECT_EQ(stamps.oldest_past(1'000, 50), (stalled_operation{1, 2, 100}));
    // A flight writes its stamp, and clears it when it has none left.
    {
        file_flight flight{{tick, stamps.at(0, 0)}};
        ticks = 100;
        const file_flight::entry held{&flight};
        EXPECT_EQ(
          stamps.oldest_past(1'000, 850), (stalled_operation{0, 0, 900}));
    }
    EXPECT_FALSE(stamps.oldest_past(1'000, 850).has_value());
}
} // namespace
