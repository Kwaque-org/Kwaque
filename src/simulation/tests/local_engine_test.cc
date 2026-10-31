#include "src/simulation/fake_file_test_support.h"
#include "src/simulation/tests/local_store_fixture.h"
#include "src/simulation/virtual_time.h"
#include "src/storage/tests/local_engine_contract.h"
#include "src/storage/tests/local_store_contract.h"

#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <optional>

namespace {
using namespace kwaque;
using namespace kwaque::simulation;
using namespace kwaque::storage;
using namespace kwaque::storage::testing::store_contract;
using namespace kwaque::simulation::testing::local_store_fixture;
} // namespace

SEASTAR_TEST_CASE(local_engine_fake_started_and_restarted) {
    co_await with_reserved_store([](auto&&... shared) {
        return storage::testing::local_engine_contract::started_and_restarted<
          simulation::monotonic_clock>(shared...);
    });
}

SEASTAR_TEST_CASE(local_engine_fake_prepared_restarted) {
    co_await with_reserved_store([](auto&&... shared) {
        return storage::testing::local_engine_contract::prepared_restarted<
          simulation::monotonic_clock>(shared...);
    });
}

SEASTAR_TEST_CASE(local_engine_fake_derived_segments) {
    co_await with_reserved_store([](auto&&... shared) {
        return storage::testing::local_engine_contract::derived_segments<
          simulation::monotonic_clock>(shared...);
    });
}

SEASTAR_TEST_CASE(local_engine_fake_placed_by_default) {
    co_await with_reserved_store([](auto&&... shared) {
        return storage::testing::local_engine_contract::placed_by_default<
          simulation::monotonic_clock>(shared...);
    });
}

// The device crashes and is down while the shard stops, so nothing of the
// shard changes it after the crash.
SEASTAR_TEST_CASE(local_engine_fake_damaged_head) {
    using damage = storage::testing::local_engine_contract::head_damage;
    for (const auto kind :
         {damage::zeroed,
          damage::flipped,
          damage::junk,
          damage::torn_header,
          damage::torn_body})
        co_await with_reserved_store(
          [kind](
            auto& files,
            auto& owner,
            auto& spec,
            auto& budget,
            auto& reserve,
            auto drive,
            auto& timer) -> seastar::future<> {
              co_await storage::testing::local_engine_contract::damaged_head<
                simulation::monotonic_clock>(
                files,
                owner,
                spec,
                budget,
                reserve,
                drive,
                timer,
                kind,
                [&files, drive](auto& engine) -> seastar::future<> {
                    take(co_await drive.lifecycle(files.crash()));
                    fake_file_test_access::set_down(files, true);
                    static_cast<void>(co_await drive.lifecycle(engine.stop()));
                    fake_file_test_access::set_down(files, false);
                });
          });
}

SEASTAR_TEST_CASE(local_engine_fake_damaged_certified_block) {
    using damage = storage::testing::local_engine_contract::block_damage;
    for (const auto kind :
         {damage::none, damage::wal_placed, damage::segment_placed})
        co_await with_reserved_store(
          [kind](
            auto& files,
            auto& owner,
            auto& spec,
            auto& budget,
            auto& reserve,
            auto drive,
            auto& timer) -> seastar::future<> {
              co_await storage::testing::local_engine_contract::
                damaged_certified_block<simulation::monotonic_clock>(
                  files,
                  owner,
                  spec,
                  budget,
                  reserve,
                  drive,
                  timer,
                  kind,
                  [&files, drive](auto& engine) -> seastar::future<> {
                      take(co_await drive.lifecycle(files.crash()));
                      fake_file_test_access::set_down(files, true);
                      static_cast<void>(
                        co_await drive.lifecycle(engine.stop()));
                      fake_file_test_access::set_down(files, false);
                  });
          });
}

SEASTAR_TEST_CASE(local_engine_fake_stale_control_record_refused) {
    co_await with_reserved_store([](auto&&... shared) {
        return storage::testing::local_engine_contract::
          stale_control_record_refused<simulation::monotonic_clock>(shared...);
    });
}

SEASTAR_TEST_CASE(local_engine_fake_stale_control_check_passes_spares) {
    co_await with_reserved_store([](auto&&... shared) {
        return storage::testing::local_engine_contract::
          stale_control_check_passes_spares<simulation::monotonic_clock>(
            shared...);
    });
}

// A removal cut after every number of scheduler events, once with unsynced
// names reverting at the crash and once with all of them surviving. Each
// cut is its own store, started three times.
SEASTAR_TEST_CASE(local_engine_fake_removal_crash_at_every_cut) {
    for (const auto survive : {false, true}) {
        const auto crash = survive ? std::optional{fake_crash_policy{
                                       .namespace_percent = 100}}
                                   : std::nullopt;
        // Every cut where names revert, the ordinary crash; every third
        // one where they all survive.
        const std::uint32_t stride = survive ? 3 : 1;
        bool finished = false;
        std::uint32_t cut = 0;
        for (; !finished && cut < 2048; cut += stride)
            co_await with_reserved_store(
              [cut, &finished](
                auto& files,
                auto& owner,
                auto& spec,
                auto& budget,
                auto& reserve,
                auto drive,
                auto& timer) -> seastar::future<> {
                  finished = co_await storage::testing::local_engine_contract::
                    removal_crash<simulation::monotonic_clock>(
                      files,
                      owner,
                      spec,
                      budget,
                      reserve,
                      drive,
                      timer,
                      cut,
                      one_event{&drive.events});
              },
              crash);
        BOOST_REQUIRE(finished);
        // Several operations lie between the first and the last effect.
        BOOST_CHECK(cut > 8);
    }
}
