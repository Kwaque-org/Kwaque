#include "src/base/units.h"
#include "src/broker/storage_directories.h"
#include "src/resource/resource_registry.h"
#include "src/runtime/production/clocks.h"
#include "src/runtime/production/file.h"
#include "src/runtime/production/timer.h"
#include "src/runtime/testing/test_directory.h"
#include "src/storage/tests/local_append_contract.h"
#include "src/storage/tests/recovery_successor_contract.h"

#include <seastar/core/memory.hh>
#include <seastar/core/seastar.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/tmp_file.hh>

#include <boost/test/unit_test.hpp>

namespace {
using namespace kwaque;
using storage::testing::store_contract::take;
namespace contract = storage::testing::local_append_contract;
using clock_type = runtime::production::monotonic_clock;
struct native_driver final {
    template<typename T>
    seastar::future<T> lifecycle(seastar::future<T> operation) const {
        return operation;
    }
};
template<typename Func>
seastar::future<> with_local_store(Func function) {
    auto config = resource::resource_config::from_total_memory(
                    byte_count{seastar::memory::stats().total_memory()})
                    .value();
    resource::resource_registry registry;
    co_await registry.start(config);
    resource::resource_manager manager{registry.handles()};
    std::exception_ptr first;
    try {
        co_await manager.start();
        co_await seastar::tmp_dir::do_with(
          runtime::testing::test_directory_template(),
          seastar::coroutine::lambda(
            [&manager,
             &function](seastar::tmp_dir& directory) -> seastar::future<> {
                const auto root = directory.get_path() / "store";
                co_await seastar::recursive_touch_directory(root.string());
                const auto status = co_await seastar::file_stat(
                  root.string(), seastar::follow_symlink::no);
                const auto spec
                  = storage::testing::store_contract::specification(
                    take(runtime::file_path::make(root.string())),
                    {status.device_id, status.inode_number},
                    68);
                const std::array specs{spec};
                auto ownership = take(
                  co_await broker::storage_directories::acquire(specs));
                runtime::production::file_system files;
                storage::workload_budget budget{
                  manager.acquire_workload(
                    resource::workload_class::foreground_protocol),
                  {.tasks = 128, .bytes = byte_count{48_MiB}, .handles = 32},
                  bytes::testing::charge};
                runtime::production::timer timer;
                std::exception_ptr failure;
                try {
                    co_await function(
                      files, *ownership, spec, budget, native_driver{}, timer);
                } catch (...) {
                    failure = std::current_exception();
                }
                take(co_await timer.stop());
                if (failure) std::rethrow_exception(failure);
                BOOST_CHECK_EQUAL(budget.snapshot().tasks, 0U);
                BOOST_CHECK_EQUAL(budget.snapshot().bytes, 0U);
                BOOST_CHECK_EQUAL(budget.snapshot().handles, 0U);
            }));
    } catch (...) {
        first = std::current_exception();
    }
    co_await manager.stop();
    co_await registry.stop();
    if (first) std::rethrow_exception(first);
}
} // namespace

SEASTAR_TEST_CASE(local_append_native_durable_append) {
    co_await with_local_store([](auto&&... shared) {
        return contract::durable_append<clock_type>(shared...);
    });
}

SEASTAR_TEST_CASE(local_append_native_independent_alignments) {
    co_await with_local_store([](auto&&... shared) {
        return contract::durable_append<clock_type>(shared..., 4096);
    });
}

SEASTAR_TEST_CASE(local_append_native_occupancy_batching) {
    co_await with_local_store([](auto&&... shared) {
        return contract::occupancy_batching<clock_type>(shared..., false);
    });
}

SEASTAR_TEST_CASE(local_append_native_rejections_have_no_effect) {
    co_await with_local_store([](auto&&... shared) {
        return contract::rejections_have_no_effect<clock_type>(shared...);
    });
}

SEASTAR_TEST_CASE(local_append_native_rotation_before_freeze) {
    co_await with_local_store([](auto&&... shared) {
        return contract::rotation_before_freeze<clock_type>(shared...);
    });
}

// Native file systems give no flush count; the fake case checks it.
struct no_flush_count final {
    std::optional<std::uint64_t> operator()(const runtime::file_path&) const {
        return std::nullopt;
    }
};
SEASTAR_TEST_CASE(local_recovered_successor_native) {
    co_await with_local_store([](auto&&... shared) {
        return storage::testing::recovery_successor_contract::
          recovered_successor<clock_type>(shared..., no_flush_count{});
    });
}

SEASTAR_TEST_CASE(local_recovery_repeated_restarts_native) {
    co_await with_local_store([](auto&&... shared) {
        return storage::testing::recovery_successor_contract::repeated_restarts<
          clock_type>(shared...);
    });
}

SEASTAR_TEST_CASE(local_append_native_close_drains_accepted) {
    co_await with_local_store([](auto&&... shared) {
        return contract::close_drains_accepted<clock_type>(shared...);
    });
}

SEASTAR_TEST_CASE(local_append_native_interest_ends_without_cancelling) {
    co_await with_local_store([](auto&&... shared) {
        return contract::interest_ends_without_cancelling<clock_type>(
          shared..., false);
    });
}

SEASTAR_TEST_CASE(local_append_native_completed_retry_facts) {
    co_await with_local_store([](auto&&... shared) {
        return contract::completed_retry_facts<clock_type>(shared...);
    });
}

SEASTAR_TEST_CASE(local_append_native_shutdown_stops_admission) {
    co_await with_local_store([](auto&&... shared) {
        return contract::shutdown_stops_admission<clock_type>(shared...);
    });
}

SEASTAR_TEST_CASE(local_append_native_concurrent_pipeline) {
    co_await with_local_store([](auto&&... shared) {
        return contract::concurrent_pipeline<clock_type>(shared...);
    });
}

#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION) && !defined(SEASTAR_DEBUG)
SEASTAR_TEST_CASE(local_append_native_acceptance_allocation_cuts) {
    std::size_t reached = 0;
    for (std::size_t at = 0; at != 256; ++at) {
        bool injected = false;
        co_await with_local_store(
          [at, &injected](auto&&... shared) -> seastar::future<> {
              injected
                = co_await contract::acceptance_allocation_cut<clock_type>(
                  shared..., at);
          });
        if (!injected) break;
        ++reached;
    }
    BOOST_REQUIRE_GT(reached, 0U);
    BOOST_REQUIRE_LT(reached, 256U);
}
#endif

#if !defined(SEASTAR_DEBUG)
SEASTAR_TEST_CASE(local_append_native_close_before_group_runs) {
    co_await with_local_store([](auto&&... shared) {
        return contract::close_before_group_runs<clock_type>(shared...);
    });
}
#endif

SEASTAR_TEST_CASE(local_append_native_identity_and_rejections) {
    co_await with_local_store([](auto&&... shared) {
        return contract::identity_and_rejections<clock_type>(shared...);
    });
}
