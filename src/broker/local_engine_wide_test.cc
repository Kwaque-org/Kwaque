#include "src/base/units.h"
#include "src/broker/native_store_test_support.h"
#include "src/runtime/testing/test_directory.h"
#include "src/storage/tests/local_engine_contract.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/tmp_file.hh>

#include <boost/test/unit_test.hpp>

#include <cstdint>

// A shard at its default width on the production file system, in a binary of
// its own: the engine keeps a writable segment for every quarter of its
// appends' budget its idle owners take, about 128 KiB a segment, so 1,024 of
// them need more than a 1 GiB reactor's foreground lease. The simulated file
// system would need a trace entry allocated up front for every event of the
// run, about a million.
//
// The leases come from the reactor's memory as Seastar reports it. With the
// system allocator it reports a fixed 1 GiB whatever the reactor is given,
// too little for this width, so the case runs with the native allocator
// only.
#ifndef SEASTAR_DEFAULT_ALLOCATOR
namespace {
using namespace kwaque;
using clock_type = broker::testing::native_clock;

// Each writable segment takes twelve tasks of the appends' budget and 16
// handles; the appends take the whole foreground lease.
constexpr std::uint32_t width = 1024;
constexpr broker::testing::native_budgets wide{
  .appends
  = {.tasks = 1024 + 12 * width, .bytes = byte_count{4_GiB}, .handles = 32 + 16 * width},
  .reserve = {.tasks = 256, .bytes = byte_count{64_MiB}, .handles = 64}};
} // namespace
#endif

SEASTAR_TEST_CASE(local_engine_native_wide_shard) {
#if defined(SEASTAR_DEFAULT_ALLOCATOR)
    BOOST_TEST_MESSAGE(
      "a shard's width needs the native allocator's memory statistics");
    co_return;
#else
    co_await seastar::tmp_dir::do_with(
      runtime::testing::test_directory_template(),
      seastar::coroutine::lambda(
        [](seastar::tmp_dir& directory) -> seastar::future<> {
            co_await broker::testing::with_native_store(
              directory.get_path() / "store",
              [](auto&&... shared) {
                  return storage::testing::local_engine_contract::wide_shard<
                    clock_type>(shared...);
              },
              wide);
        }));
#endif
}
