#include "src/broker/storage_directories.h"
#include "src/resource/resource_registry.h"
#include "src/runtime/production/file.h"
#include "src/runtime/testing/test_directory.h"
#include "src/storage/tests/local_installation_contract.h"

#include <seastar/core/memory.hh>
#include <seastar/core/preempt.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/later.hh>
#include <seastar/util/tmp_file.hh>

#include <boost/test/unit_test.hpp>

namespace {
using namespace kwaque;
using storage::testing::store_contract::take;
struct native_driver final {
    template<typename T>
    seastar::future<T> lifecycle(seastar::future<T> operation) const {
        return operation;
    }
};
template<typename Func>
seastar::future<> with_installation(std::uint8_t device, Func function) {
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
            [&manager, &function, device](
              seastar::tmp_dir& directory) -> seastar::future<> {
                const auto root = directory.get_path() / "store";
                co_await seastar::recursive_touch_directory(root.string());
                const auto status = co_await seastar::file_stat(
                  root.string(), seastar::follow_symlink::no);
                const auto spec
                  = storage::testing::store_contract::specification(
                    take(runtime::file_path::make(root.string())),
                    {status.device_id, status.inode_number},
                    device);
                const std::array specs{spec};
                auto ownership = take(
                  co_await broker::storage_directories::acquire(specs));
                runtime::production::file_system files;
                storage::workload_budget budget{
                  manager.acquire_workload(resource::workload_class::metadata),
                  {.tasks = 16,
                   .bytes = byte_count{8U * 1024U * 1024U},
                   .handles = 32},
                  bytes::testing::charge};
                co_await function(
                  files, *ownership, spec, budget, native_driver{});
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

SEASTAR_TEST_CASE(local_fixture_buffer_yields_from_coroutine) {
    co_await seastar::yield();
    BOOST_REQUIRE(!seastar::thread::running_in_thread());
    auto pending = [] {
        seastar::internal::preemption_monitor requested{};
        requested.head.store(1, std::memory_order_relaxed);
        const auto* previous = seastar::internal::get_need_preempt_var();
        auto restore = seastar::defer([previous] noexcept {
            seastar::internal::set_need_preempt_var(previous);
        });
        seastar::internal::set_need_preempt_var(&requested);
        // Restore the real monitor before letting the reactor run the child.
        return storage::testing::installation_contract::buffer_async(
          std::string(8192, 'x'), 67);
    }();
    const bool suspended = !pending.available();
    auto value = co_await std::move(pending);
    BOOST_CHECK(suspended);
    BOOST_CHECK(value.content_equals(std::string(8192, 'x')));
    BOOST_CHECK_EQUAL(value.fragment_count(), (8192U + 66U) / 67U);
}

SEASTAR_TEST_CASE(local_installation_native_allocation) {
    co_await with_installation(
      51,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::installation_contract::allocation(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(local_installation_native_descriptors) {
    co_await with_installation(
      68,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::installation_contract::descriptors(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(local_installation_native_checkpoint_bundle) {
    co_await with_installation(
      51,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::installation_contract::checkpoint_bundle(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(local_installation_native_segment_bundles) {
    co_await with_installation(
      68,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::installation_contract::segment_bundles(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(local_installation_native_maximum_bundle) {
    co_await with_installation(
      68,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::installation_contract::maximum_bundle(
            files, owner, spec, budget, drive);
      });
}
