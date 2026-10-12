#include "src/base/error.h"
#include "src/base/units.h"
#include "src/runtime/file.h"
#include "src/runtime/file_error.h"
#include "src/runtime/time.h"
#include "src/storage/device_placement.h"
#include "src/storage/local_metadata.h"
#include "src/storage/local_store_config.h"
#include "src/storage/local_types.h"
#include "src/storage/space_sampler.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

namespace kwaque::storage {
namespace {
using runtime::file_failure_detail;
using runtime::monotonic_time;

template<typename Id>
Id id(std::uint8_t value) {
    std::array<std::uint8_t, 16> bytes{};
    bytes.fill(value);
    return Id::make(bytes).value();
}
device_store_id device(std::uint8_t value) {
    return id<device_store_id>(value);
}
local_device_spec spec(
  const char* path,
  std::uint8_t named,
  local_device_role role,
  std::uint64_t filesystem) {
    return {
      runtime::file_path::make(path).value(),
      local_store_context::make(
        id<model::cluster_id>(1),
        id<model::broker_id>(2),
        device(named),
        local_store_shard)
        .value(),
      {1, storage_alignment::make(byte_count{4096}).value(), 2, role},
      {filesystem, named}};
}
// A control device and three data devices, each on a filesystem of its own.
std::vector<local_device_spec> three() {
    return {
      spec("/c", 9, local_device_role::wal_control, 90),
      spec("/b", 2, local_device_role::data, 20),
      spec("/a", 1, local_device_role::data, 10),
      spec("/d", 3, local_device_role::data, 30)};
}
// Late enough that a threshold can be taken off it. Every time below is
// made with checked arithmetic: one that wrapped would read as a start far
// after the reading, which is no age at all.
constexpr monotonic_time at{10'000'000'000};
monotonic_time after(monotonic_time from, std::uint64_t nanoseconds) {
    return from.checked_add(runtime::monotonic_duration{nanoseconds}).value();
}
monotonic_time before(monotonic_time from, std::uint64_t nanoseconds) {
    return from.checked_sub(runtime::monotonic_duration{nanoseconds}).value();
}
void hand(device_table& table, byte_count share, monotonic_time now = at) {
    std::array<device_pool_share, device_table::maximum_devices> shares{};
    for (auto& pool : shares)
        pool = {true, false, share};
    table.sampled(shares, now);
}

// The device with the fewest of the shard's writable segments is chosen,
// and after any run of creations the counts differ by at most one.
TEST(DevicePlacementTest, FewestWritableFirstKeepsCountsWithinOne) {
    const auto devices = three();
    auto table = device_table::make(devices, {}).value();
    ASSERT_EQ(table->size(), 3U);
    ASSERT_EQ(table->pools(), 3U);
    hand(*table, byte_count{1_GiB});
    for (unsigned placed = 0; placed != 20; ++placed) {
        const auto chosen = table->choose(byte_count{1_MiB}, at);
        ASSERT_TRUE(chosen.has_value());
        table->placed(*chosen, byte_count{1_MiB});
        std::array<std::uint32_t, 3> counts{
          table->writable(device(1)),
          table->writable(device(2)),
          table->writable(device(3))};
        const auto [least, most] = std::minmax_element(
          counts.begin(), counts.end());
        EXPECT_LE(*most, *least + 1);
        EXPECT_EQ(counts[0] + counts[1] + counts[2], placed + 1U);
    }
}

// The count comes before the share: a device with more room and one writable
// segment is not chosen over one with less room and none.
TEST(DevicePlacementTest, FewerWritableComesBeforeTheLargerShare) {
    const auto devices = three();
    auto table = device_table::make(devices, {}).value();
    std::array<device_pool_share, device_table::maximum_devices> shares{};
    shares[0] = {true, false, byte_count{8_GiB}};
    shares[1] = {true, false, byte_count{2_GiB}};
    shares[2] = {true, false, byte_count{1_GiB}};
    table->sampled(shares, at);
    // While the counts are equal the device with the most room is chosen.
    const auto roomiest = table->choose(byte_count{1_MiB}, at);
    ASSERT_TRUE(roomiest.has_value());
    for (const auto other : {std::uint8_t{1}, std::uint8_t{2}, std::uint8_t{3}})
        EXPECT_GE(
          table->share(*roomiest).value(), table->share(device(other)).value());
    table->placed(*roomiest, byte_count{1_MiB});
    // It still has the most room, and one writable segment more.
    const auto next = table->choose(byte_count{1_MiB}, at);
    ASSERT_TRUE(next.has_value());
    EXPECT_NE(*next, *roomiest);
    EXPECT_EQ(table->writable(*next), 0U);
    EXPECT_GT(table->share(*roomiest).value(), table->share(*next).value());
}

// Equal counts: the larger share, then the lower identity. The same table
// chooses the same device.
TEST(DevicePlacementTest, TiesGoToTheLargerShareThenTheLowerIdentity) {
    const auto devices = three();
    auto table = device_table::make(devices, {}).value();
    hand(*table, byte_count{1_GiB});
    EXPECT_EQ(table->choose(byte_count{1_MiB}, at).value(), device(1));
    EXPECT_EQ(table->choose(byte_count{1_MiB}, at).value(), device(1));
    // The first device's pool has less left: the next of equals is taken.
    table->placed(device(1), byte_count{1_MiB});
    table->unplaced(device(1));
    EXPECT_EQ(table->writable(device(1)), 0U);
    EXPECT_EQ(table->choose(byte_count{1_MiB}, at).value(), device(2));
    // A named device is taken when it is eligible, whatever the order.
    EXPECT_EQ(
      table->choose(byte_count{1_MiB}, at, device(3)).value(), device(3));
    const auto unknown = table->choose(byte_count{1_MiB}, at, device(7));
    ASSERT_FALSE(unknown.has_value());
    EXPECT_EQ(unknown.error().code(), errc::wrong_context);
    // The control device holds no data.
    EXPECT_FALSE(table->choose(byte_count{1_MiB}, at, device(9)).has_value());
}

// A device is eligible only while its share covers the segment's whole
// bound, and a placed segment's bound is taken from the share at once.
TEST(DevicePlacementTest, TheShareCoversTheWholeBoundAndIsTakenAtOnce) {
    const auto devices = three();
    auto table = device_table::make(devices, {}).value();
    hand(*table, byte_count{10_MiB});
    table->placed(device(1), byte_count{8_MiB});
    EXPECT_EQ(table->share(device(1)).value(), 2_MiB);
    // Fewest first would be the second device; a bound only the others
    // hold excludes the first.
    EXPECT_EQ(table->choose(byte_count{4_MiB}, at).value(), device(2));
    const auto named = table->choose(byte_count{4_MiB}, at, device(1));
    ASSERT_FALSE(named.has_value());
    EXPECT_EQ(named.error().code(), errc::resource_exhausted);
    const auto none = table->choose(byte_count{11_MiB}, at);
    ASSERT_FALSE(none.has_value());
    EXPECT_EQ(none.error().code(), errc::resource_exhausted);
    // A creation that made nothing gives its bound back.
    table->unplaced(device(1), byte_count{8_MiB});
    EXPECT_EQ(table->share(device(1)).value(), 10_MiB);
    EXPECT_EQ(table->writable(device(1)), 0U);
}

// Devices on one filesystem are one pool: one share, taken from by each.
TEST(DevicePlacementTest, DevicesOnOneFilesystemShareOnePool) {
    const std::vector<local_device_spec> devices{
      spec("/c", 9, local_device_role::all, 10),
      spec("/a", 1, local_device_role::data, 10),
      spec("/b", 2, local_device_role::data, 20)};
    auto table = device_table::make(devices, {}).value();
    ASSERT_EQ(table->size(), 3U);
    ASSERT_EQ(table->pools(), 2U);
    EXPECT_EQ(table->pool(device(1)), table->pool(device(9)));
    EXPECT_NE(table->pool(device(1)), table->pool(device(2)));
    hand(*table, byte_count{10_MiB});
    table->placed(device(1), byte_count{6_MiB});
    EXPECT_EQ(table->share(device(9)).value(), 4_MiB);
    EXPECT_EQ(table->share(device(2)).value(), 10_MiB);
}

// With no room a pool is low and takes no new segment; it returns when
// the bound it was last asked for fits again, not before.
TEST(DevicePlacementTest, ALowPoolReturnsWhenWhatWasAskedFitsAgain) {
    const std::vector<local_device_spec> devices{
      spec("/c", 9, local_device_role::all, 10)};
    std::vector<device_report> told;
    told.reserve(8);
    auto table
      = device_table::make(devices, {}, [&](const auto& report) noexcept {
            if (told.size() != told.capacity()) told.push_back(report);
        }).value();
    hand(*table, byte_count{2_MiB});
    EXPECT_EQ(table->state(device(9), at), device_state::healthy);
    const auto refused = table->choose(byte_count{8_MiB}, at);
    ASSERT_FALSE(refused.has_value());
    EXPECT_EQ(refused.error().code(), errc::resource_exhausted);
    // The next sample still has no room for it: the pool is low, and its
    // segments' owner is told once.
    hand(*table, byte_count{4_MiB});
    hand(*table, byte_count{4_MiB});
    EXPECT_EQ(table->state(device(9), at), device_state::low);
    ASSERT_EQ(told.size(), 1U);
    EXPECT_EQ(told[0].state, device_state::low);
    EXPECT_FALSE(table->choose(byte_count{1_MiB}, at).has_value());
    hand(*table, byte_count{8_MiB});
    EXPECT_EQ(table->state(device(9), at), device_state::healthy);
    EXPECT_TRUE(table->choose(byte_count{8_MiB}, at).has_value());
    // No room at all is low too, with nothing asked.
    hand(*table, byte_count{});
    EXPECT_EQ(table->state(device(9), at), device_state::low);
    hand(*table, byte_count{1});
    EXPECT_EQ(table->state(device(9), at), device_state::healthy);
}

// A sample that failed, or is too old, gives no share: the last good
// number is not used.
TEST(DevicePlacementTest, AFailedOrOldSampleGivesNoShare) {
    const std::vector<local_device_spec> devices{
      spec("/c", 9, local_device_role::all, 10)};
    auto table = device_table::make(devices, {}).value();
    // Never sampled.
    EXPECT_EQ(table->state(device(9), at), device_state::stale);
    const auto unsampled = table->choose(byte_count{1}, at);
    ASSERT_FALSE(unsampled.has_value());
    EXPECT_EQ(unsampled.error().code(), errc::unavailable);
    hand(*table, byte_count{1_GiB});
    EXPECT_TRUE(table->choose(byte_count{1_MiB}, at).has_value());
    // Three seconds old is trusted; older is not.
    const auto edge = after(at, 3'000'000'000);
    const auto late = after(edge, 1);
    EXPECT_EQ(table->state(device(9), edge), device_state::healthy);
    EXPECT_EQ(table->state(device(9), late), device_state::stale);
    EXPECT_FALSE(table->choose(byte_count{1_MiB}, late).has_value());
    // A sample that could not be taken.
    hand(*table, byte_count{1_GiB}, late);
    std::array<device_pool_share, device_table::maximum_devices> failed{};
    table->sampled(failed, late);
    EXPECT_EQ(table->state(device(9), late), device_state::stale);
    EXPECT_EQ(table->share(device(9)).value(), 0U);
    // Asked what it owes, a shard promises nothing until its shares come.
    hand(*table, byte_count{1_GiB}, late);
    table->await_shares();
    EXPECT_EQ(table->share(device(9)).value(), 0U);
    EXPECT_FALSE(table->choose(byte_count{1}, late).has_value());
    hand(*table, byte_count{1_GiB}, late);
    EXPECT_TRUE(table->choose(byte_count{1}, late).has_value());
}

// What a failure does to its device: space returns, an I/O error, a
// read-only filesystem and a device that is gone do not, and what is no
// fault of the device changes nothing.
TEST(DevicePlacementTest, FailuresAreClassifiedAndToldOnce) {
    static_assert(
      device_verdict_of(file_failure_detail::no_space).state
      == device_state::full);
    static_assert(
      device_verdict_of(file_failure_detail::quota).state
      == device_state::full);
    static_assert(
      device_verdict_of(file_failure_detail::device_io).cause
      == device_failure::io_error);
    static_assert(
      device_verdict_of(file_failure_detail::read_only).cause
      == device_failure::read_only);
    static_assert(
      device_verdict_of(file_failure_detail::device_gone).cause
      == device_failure::gone);
    static_assert(
      device_verdict_of(file_failure_detail::permission).state
      == device_state::healthy);
    static_assert(
      device_verdict_of(file_failure_detail::unknown).state
      == device_state::healthy);
    const auto devices = three();
    std::vector<device_report> told;
    told.reserve(16);
    auto table
      = device_table::make(devices, {}, [&](const auto& report) noexcept {
            if (told.size() != told.capacity()) told.push_back(report);
        }).value();
    hand(*table, byte_count{1_GiB});
    // Full: no new segment until a sample shows room.
    table->failed(device(1), file_failure_detail::no_space);
    table->failed(device(1), file_failure_detail::no_space);
    EXPECT_EQ(table->state(device(1), at), device_state::full);
    EXPECT_EQ(table->choose(byte_count{1_MiB}, at).value(), device(2));
    hand(*table, byte_count{1_GiB});
    EXPECT_EQ(table->state(device(1), at), device_state::healthy);
    // No fault of the device.
    table->failed(device(2), file_failure_detail::permission);
    table->failed(device(2), file_failure_detail::descriptor_limit);
    EXPECT_EQ(table->state(device(2), at), device_state::healthy);
    // Failed lasts, with its first cause.
    table->failed(device(2), file_failure_detail::device_io);
    table->failed(device(2), file_failure_detail::device_gone);
    hand(*table, byte_count{1_GiB});
    EXPECT_EQ(table->state(device(2), at), device_state::failed);
    EXPECT_EQ(table->cause(device(2)), device_failure::io_error);
    const auto named = table->choose(byte_count{1_MiB}, at, device(2));
    ASSERT_FALSE(named.has_value());
    EXPECT_EQ(named.error().code(), errc::unavailable);
    // A filesystem that turned read-only fails its devices.
    std::array<device_pool_share, device_table::maximum_devices> shares{};
    for (auto& pool : shares)
        pool = {true, false, byte_count{1_GiB}};
    shares[*table->pool(device(3))].read_only = true;
    table->sampled(shares, at);
    EXPECT_EQ(table->state(device(3), at), device_state::failed);
    EXPECT_EQ(table->cause(device(3)), device_failure::read_only);
    EXPECT_EQ(table->choose(byte_count{1_MiB}, at).value(), device(1));
    ASSERT_EQ(told.size(), 3U);
    EXPECT_EQ(told[0].device, device(1));
    EXPECT_EQ(told[0].state, device_state::full);
    EXPECT_EQ(told[1].device, device(2));
    EXPECT_EQ(told[1].cause, device_failure::io_error);
    EXPECT_EQ(told[2].device, device(3));
    EXPECT_EQ(told[2].cause, device_failure::read_only);
}

// A device whose oldest operation has been in flight past the first
// threshold is stalled, reported with the age, and returns when that
// operation has ended.
TEST(DevicePlacementTest, AnOldOperationStallsItsDeviceUntilItEnds) {
    const auto devices = three();
    std::vector<device_report> told;
    told.reserve(8);
    auto table
      = device_table::make(devices, {}, [&](const auto& report) noexcept {
            if (told.size() != told.capacity()) told.push_back(report);
        }).value();
    hand(*table, byte_count{1_GiB});
    // In flight for exactly the threshold is not past it.
    const auto began = before(at, 5'000'000'000);
    table->aged(device(1), began, at);
    EXPECT_EQ(table->state(device(1), at), device_state::healthy);
    EXPECT_TRUE(told.empty());
    const auto later = after(at, 1);
    table->aged(device(1), began, later);
    table->aged(device(1), began, later);
    EXPECT_EQ(table->state(device(1), later), device_state::stalled);
    ASSERT_EQ(told.size(), 1U);
    EXPECT_EQ(told[0].device, device(1));
    EXPECT_EQ(told[0].state, device_state::stalled);
    EXPECT_EQ(told[0].age_nanoseconds, 5'000'000'001U);
    EXPECT_EQ(table->choose(byte_count{1_MiB}, later).value(), device(2));
    table->aged(device(1), std::nullopt, later);
    EXPECT_EQ(table->state(device(1), later), device_state::healthy);
    EXPECT_EQ(told.size(), 1U);
    // A start that is not before the reading is no age: nothing stalls.
    table->aged(device(1), after(later, 1), later);
    table->aged(device(2), later, later);
    EXPECT_EQ(table->state(device(1), later), device_state::healthy);
    EXPECT_EQ(table->state(device(2), later), device_state::healthy);
    EXPECT_EQ(told.size(), 1U);
}

// What is left after the floor and what is owed is divided evenly, and
// nothing is handed out when that is nothing.
TEST(SpaceSamplerTest, HeadroomIsWhatIsLeftAfterTheFloorAndWhatIsOwed) {
    const space_sampler_config config;
    // One per cent of 1 TiB is more than the least floor.
    const auto wide = divide_space(
      byte_count{1024_GiB},
      byte_count{500_GiB},
      byte_count{100_GiB},
      4,
      config);
    EXPECT_EQ(wide.floor.value(), 1024_GiB / 100);
    EXPECT_EQ(wide.headroom.value(), 400_GiB - 1024_GiB / 100);
    EXPECT_EQ(wide.share.value(), wide.headroom.value() / 4);
    // A small filesystem keeps the least floor.
    const auto small = divide_space(
      byte_count{10_GiB}, byte_count{5_GiB}, byte_count{1_GiB}, 2, config);
    EXPECT_EQ(small.floor.value(), 1_GiB);
    EXPECT_EQ(small.headroom.value(), 3_GiB);
    EXPECT_EQ(small.share.value(), 3_GiB / 2);
    // What is owed and the floor leave nothing: no share, never a negative.
    const auto none = divide_space(
      byte_count{10_GiB}, byte_count{2_GiB}, byte_count{1_GiB}, 2, config);
    EXPECT_EQ(none, (space_division{byte_count{1_GiB}, {}, {}}));
    const auto over = divide_space(
      byte_count{10_GiB}, byte_count{1_GiB}, byte_count{64_GiB}, 2, config);
    EXPECT_EQ(over.share.value(), 0U);
}

} // namespace
} // namespace kwaque::storage
