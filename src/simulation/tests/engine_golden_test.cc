#include "src/simulation/fake_file.h"
#include "src/simulation/tests/engine_history_fixture.h"
#include "src/simulation/tests/local_store_fixture.h"
#include "src/storage/tests/engine_fixture_contract.h"
#include "src/storage/tests/engine_history.h"
#include "src/storage/tests/engine_history_contract.h"
#include "src/storage/tests/engine_ledger.h"
#include "src/storage/tests/local_store_contract.h"

#include <seastar/core/future.hh>
#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <array>
#include <cstdint>
#include <ios>
#include <sstream>
#include <utility>
#include <vector>

// What the engine stores, as goldens on the simulated file system: the store
// a fixed history leaves, as one number, and the engine started on a store
// image it never wrote. Every build and processor runs these.
namespace {
using namespace kwaque;
using namespace kwaque::simulation;
using namespace kwaque::storage;
using namespace kwaque::storage::testing::store_contract;
using namespace kwaque::simulation::testing::local_store_fixture;
using simulation::testing::engine_history_fixture::image;
using simulation::testing::engine_history_fixture::stored_file;
using simulation::testing::engine_history_fixture::stored_files;
using verdict = storage::testing::engine_ledger::verdict;

// The checked-in store image on three devices of its own beside the
// fixture's store, and the engine started on it.
seastar::future<> on_the_image(bool sealed) {
    namespace image = storage::testing::engine_fixture_contract;
    co_await with_reserved_store(
      [sealed](
        auto& files,
        auto&,
        auto&,
        auto& budget,
        auto& reserve,
        auto drive,
        auto& timer) -> seastar::future<> {
          const std::array roots{
            take(runtime::file_path::make("/kwaque/w")),
            take(runtime::file_path::make("/kwaque/a")),
            take(runtime::file_path::make("/kwaque/b"))};
          for (const auto& root : roots)
              take(co_await drive.lifecycle(files.create_directories(root)));
          const auto devices = image::devices(
            roots, {{{1, 2}, {1, 3}, {1, 4}}});
          ownership_input owner{devices};
          co_await image::started_on_the_image<simulation::monotonic_clock>(
            files, owner, devices, budget, reserve, drive, timer, sealed);
      });
}
// The same image after a crash lost segment A's blocks, and the engine
// started on it twice.
seastar::future<> materialized_on_the_image() {
    namespace image = storage::testing::engine_fixture_contract;
    co_await with_reserved_store(
      [](
        auto& files,
        auto&,
        auto&,
        auto& budget,
        auto& reserve,
        auto drive,
        auto& timer) -> seastar::future<> {
          const std::array roots{
            take(runtime::file_path::make("/kwaque/w")),
            take(runtime::file_path::make("/kwaque/a")),
            take(runtime::file_path::make("/kwaque/b"))};
          for (const auto& root : roots)
              take(co_await drive.lifecycle(files.create_directories(root)));
          const auto devices = image::devices(
            roots, {{{1, 2}, {1, 3}, {1, 4}}});
          ownership_input owner{devices};
          co_await image::materialized_on_the_image<
            simulation::monotonic_clock>(
            files, owner, devices, budget, reserve, drive, timer);
      });
}
} // namespace

// The store that history leaves is the same store on every build: every
// name and every byte of it, as one number written down here. A debug and
// an optimized build, either allocator, a shuffled task order and another
// processor all reach it. A change to anything the engine stores changes
// it, and is then a change made on purpose with this number beside it. No
// compressor runs in the history, so none of these bytes is one's output.
// When the number differs, the files, sizes and page digests that follow it
// are set beside the same listing from a build that holds the number, which
// it prints at Boost's message level (BOOST_TEST_LOG_LEVEL=message).
SEASTAR_TEST_CASE(engine_history_fake_the_store_is_the_same_on_every_build) {
    std::uint64_t left = 0;
    std::vector<stored_file> stored;
    simulation::testing::engine_history_fixture::around looked;
    looked.over = [&left, &stored](fake_file_system& files) {
        left = image(files);
        stored = stored_files(files);
    };
    const auto report
      = co_await simulation::testing::engine_history_fixture::run(
        storage::testing::whole_life_history(), {}, std::move(looked));
    BOOST_REQUIRE_MESSAGE(
      report.verdict == verdict::holds,
      "the uncut history: verdict " << static_cast<unsigned>(report.verdict)
                                    << " about " << report.about);
    std::ostringstream listing;
    for (const auto& file : stored) {
        listing << '\n' << file.path << ' ' << file.size << std::hex;
        for (const auto page : file.page_digests)
            listing << ' ' << page;
        listing << std::dec;
    }
    BOOST_TEST_MESSAGE("the store's files:" << listing.str());
    BOOST_CHECK_MESSAGE(
      left == UINT64_C(0x388d5782d14418a5),
      "the store's digest is 0x" << std::hex << left << std::dec
                                 << "; its files:" << listing.str());
}

// The engine starts on a store image it never wrote: every record, the WAL
// and both segments are the checked-in files, on three devices.
SEASTAR_TEST_CASE(engine_fixture_fake_started_on_the_image) {
    co_await on_the_image(false);
}

// The same image with its first segment sealed: a reader is given the two
// requests the image holds, byte for byte.
SEASTAR_TEST_CASE(engine_fixture_fake_sealed_segment_read_from_the_image) {
    co_await on_the_image(true);
}

// The same image after a crash lost segment A's blocks while the WAL kept
// their PREPAREs: the start writes them back before it publishes A.
SEASTAR_TEST_CASE(engine_fixture_fake_lost_blocks_written_from_the_wal) {
    co_await materialized_on_the_image();
}
