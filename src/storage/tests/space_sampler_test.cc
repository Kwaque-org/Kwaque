#include "src/base/error.h"
#include "src/base/units.h"
#include "src/runtime/error.h"
#include "src/runtime/file.h"
#include "src/runtime/production/clocks.h"
#include "src/runtime/production/timer.h"
#include "src/runtime/time.h"
#include "src/storage/device_placement.h"
#include "src/storage/local_metadata.h"
#include "src/storage/local_store_config.h"
#include "src/storage/local_types.h"
#include "src/storage/space_sampler.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <array>
#include <cstdint>
#include <exception>
#include <memory>
#include <optional>
#include <vector>

namespace {
using namespace kwaque;
using namespace kwaque::storage;
using clock_type = runtime::production::monotonic_clock;

template<typename Id>
Id id(std::uint8_t value) {
    std::array<std::uint8_t, 16> bytes{};
    bytes.fill(value);
    return Id::make(bytes).value();
}
local_device_spec whole_store() {
    return {
      runtime::file_path::make("/c").value(),
      local_store_context::make(
        id<model::cluster_id>(1),
        id<model::broker_id>(2),
        id<device_store_id>(9),
        local_store_shard)
        .value(),
      {1,
       storage_alignment::make(byte_count{4096}).value(),
       3,
       local_device_role::all},
      {10, 9}};
}

// Three shards' device tables and what each owes, on one thread, with a
// filesystem whose numbers the test sets.
struct shards final {
    shards() {
        for (auto& table : tables)
            table = device_table::make(devices, {}).value();
    }
    [[nodiscard]] std::unique_ptr<space_sampler>
    sampler(space_sampler_config config = {}) {
        return space_sampler::make(
                 {pools,
                  3,
                  [this](const runtime::file_path&) {
                      ++reads;
                      using output
                        = runtime::result<runtime::file_system_space>;
                      if (!readable)
                          return seastar::make_ready_future<output>(
                            runtime::failure(
                              storage::detail::path_error(errc::io_failure)));
                      return seastar::make_ready_future<output>(
                        runtime::file_system_space::make(
                          capacity, available, available, read_only)
                          .value());
                  },
                  [this](std::uint32_t shard) {
                      using output = runtime::result<space_sampler::owed_bytes>;
                      if (shard == deaf)
                          return seastar::make_ready_future<output>(
                            runtime::failure(
                              storage::detail::path_error(errc::unavailable)));
                      tables[shard]->await_shares();
                      space_sampler::owed_bytes owed{};
                      owed[0] = owes[shard];
                      return seastar::make_ready_future<output>(owed);
                  },
                  [this](
                    std::uint32_t shard, space_sampler::pool_shares given) {
                      tables[shard]->sampled(given, clock_type::now());
                      return seastar::make_ready_future<>();
                  }},
                 config)
          .value();
    }
    [[nodiscard]] byte_count share(std::uint32_t shard) const {
        return tables[shard]->share(device);
    }
    [[nodiscard]] device_state state(std::uint32_t shard) const {
        return *tables[shard]->state(device, clock_type::now());
    }

    std::vector<local_device_spec> devices{whole_store()};
    std::array<runtime::file_path, 1> pools{
      runtime::file_path::make("/c").value()};
    device_store_id device{id<device_store_id>(9)};
    std::array<std::unique_ptr<device_table>, 3> tables;
    std::array<byte_count, 3> owes{};
    byte_count capacity{100_GiB};
    byte_count available{50_GiB};
    bool readable{true}, read_only{false};
    std::optional<std::uint32_t> deaf;
    std::uint32_t reads{0};
};
} // namespace

// Each shard is handed an even part of what the filesystem has after its
// floor and after what every shard still owes. A shard promises only inside
// its part, so what the shards promise together never passes what is there.
SEASTAR_TEST_CASE(space_sampler_shares_under_several_shards) {
    shards store;
    store.owes = {byte_count{4_GiB}, byte_count{2_GiB}, byte_count{}};
    auto sampler = store.sampler();
    co_await sampler->sample();
    // 50 available, 1 the floor, 6 owed: 43 to divide by three.
    const auto first = sampler->last(0);
    BOOST_REQUIRE(first.sampled);
    BOOST_CHECK_EQUAL(first.owed.value(), 6_GiB);
    BOOST_CHECK_EQUAL(first.divided.floor.value(), 1_GiB);
    BOOST_CHECK_EQUAL(first.divided.headroom.value(), 43_GiB);
    for (std::uint32_t shard = 0; shard != 3; ++shard) {
        BOOST_CHECK_EQUAL(store.share(shard).value(), 43_GiB / 3);
        BOOST_CHECK(store.state(shard) == device_state::healthy);
    }
    // One shard promises a segment: it comes off that shard's part alone,
    // and off everyone's at the next sample.
    BOOST_REQUIRE(store.tables[0]
                    ->choose(byte_count{10_GiB}, clock_type::now())
                    .has_value());
    store.tables[0]->placed(store.device, byte_count{10_GiB});
    store.owes[0] = byte_count{14_GiB};
    BOOST_CHECK_EQUAL(store.share(0).value(), 43_GiB / 3 - 10_GiB);
    BOOST_CHECK_EQUAL(store.share(1).value(), 43_GiB / 3);
    BOOST_CHECK(!store.tables[0]
                   ->choose(byte_count{10_GiB}, clock_type::now())
                   .has_value());
    co_await sampler->sample();
    for (std::uint32_t shard = 0; shard != 3; ++shard)
        BOOST_CHECK_EQUAL(store.share(shard).value(), 33_GiB / 3);
    // What the shards may promise and what they owe stay inside what the
    // filesystem has after its floor.
    BOOST_CHECK_LE(3 * (33_GiB / 3) + 16_GiB, 50_GiB - 1_GiB);

    // Another program takes the space: it shows at the next sample, and
    // nothing new is promised.
    store.available = byte_count{12_GiB};
    co_await sampler->sample();
    for (std::uint32_t shard = 0; shard != 3; ++shard) {
        BOOST_CHECK_EQUAL(store.share(shard).value(), 0U);
        BOOST_CHECK(store.state(shard) == device_state::low);
    }
    // It returns when there is room again.
    store.available = byte_count{50_GiB};
    co_await sampler->sample();
    for (std::uint32_t shard = 0; shard != 3; ++shard)
        BOOST_CHECK(store.state(shard) == device_state::healthy);
    BOOST_CHECK_EQUAL(sampler->samples(), 4U);
}

// A filesystem that cannot be read, or a shard that cannot be asked what it
// owes, leaves every shard with no share: the last good number is not used,
// and the sampling goes on.
SEASTAR_TEST_CASE(space_sampler_failed_sample_gives_no_shares) {
    shards store;
    auto sampler = store.sampler();
    co_await sampler->sample();
    BOOST_REQUIRE(store.share(0).value() != 0);
    store.readable = false;
    co_await sampler->sample();
    BOOST_CHECK(!sampler->last(0).sampled);
    for (std::uint32_t shard = 0; shard != 3; ++shard) {
        BOOST_CHECK_EQUAL(store.share(shard).value(), 0U);
        BOOST_CHECK(store.state(shard) == device_state::stale);
    }
    store.readable = true;
    co_await sampler->sample();
    BOOST_CHECK(store.state(1) == device_state::healthy);
    // One shard does not answer: what it owes is unknown, so nothing is
    // promised by anyone.
    store.deaf = 1;
    co_await sampler->sample();
    BOOST_CHECK(!sampler->last(0).sampled);
    BOOST_CHECK_EQUAL(store.share(0).value(), 0U);
    BOOST_CHECK(store.state(0) == device_state::stale);
    BOOST_CHECK(store.state(2) == device_state::stale);
    store.deaf.reset();
    co_await sampler->sample();
    BOOST_CHECK(store.state(0) == device_state::healthy);
    // A filesystem that turned read-only fails its devices on every shard.
    store.read_only = true;
    co_await sampler->sample();
    for (std::uint32_t shard = 0; shard != 3; ++shard)
        BOOST_CHECK(store.state(shard) == device_state::failed);
    BOOST_CHECK(!sampler->failure().failed());
}

// Started, it samples every period until it is stopped.
SEASTAR_TEST_CASE(space_sampler_samples_every_period) {
    shards store;
    space_sampler_config config;
    config.period_nanoseconds = 10'000'000;
    auto sampler = store.sampler(config);
    runtime::production::timer timer;
    std::exception_ptr failed;
    try {
        BOOST_REQUIRE(sampler->template start<clock_type>(timer).has_value());
        BOOST_CHECK(!sampler->template start<clock_type>(timer).has_value());
        seastar::abort_source never;
        for (unsigned waits = 0; waits != 500 && sampler->samples() < 3;
             ++waits)
            static_cast<void>(co_await timer.sleep_until(
              clock_type::now()
                .checked_add(runtime::monotonic_duration{10'000'000})
                .value(),
              never));
        BOOST_CHECK_GE(sampler->samples(), 3U);
        BOOST_CHECK(store.state(0) == device_state::healthy);
    } catch (...) {
        failed = std::current_exception();
    }
    co_await sampler->stop();
    const auto after = sampler->samples();
    BOOST_CHECK(!sampler->failure().failed());
    static_cast<void>(co_await timer.stop());
    BOOST_CHECK_EQUAL(sampler->samples(), after);
    if (failed) std::rethrow_exception(failed);
}
