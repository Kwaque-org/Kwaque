#include "src/broker/storage_directories.h"
#include "src/resource/resource_registry.h"
#include "src/runtime/production/file.h"
#include "src/runtime/testing/test_directory.h"
#include "src/storage/tests/local_store_contract.h"

#include <seastar/core/memory.hh>
#include <seastar/core/seastar.hh>
#include <seastar/testing/test_case.hh>
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
} // namespace
SEASTAR_TEST_CASE(local_store_native_bootstrap_and_control) {
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
            [&manager](seastar::tmp_dir& directory) -> seastar::future<> {
                const auto root = directory.get_path() / "store";
                co_await seastar::recursive_touch_directory(root.string());
                const auto status = co_await seastar::file_stat(
                  root.string(), seastar::follow_symlink::no);
                const auto spec
                  = storage::testing::store_contract::specification(
                    take(runtime::file_path::make(root.string())),
                    {status.device_id, status.inode_number});
                const std::array specs{spec};
                broker::pid_file primary{root / "kwaque.pid"};
                const std::array<const broker::pid_file*, 1> borrowed{&primary};
                auto ownership = take(
                  co_await broker::storage_directories::acquire(
                    specs, borrowed));
                bool contended = false;
                try {
                    static_cast<void>(
                      co_await broker::storage_directories::acquire(specs));
                } catch (const broker::pid_file_locked&) {
                    contended = true;
                }
                BOOST_REQUIRE(contended);
                runtime::production::file_system files;
                storage::workload_budget budget{
                  manager.acquire_workload(resource::workload_class::metadata),
                  {.tasks = 16,
                   .bytes = byte_count{8U * 1024U * 1024U},
                   .handles = 32},
                  bytes::testing::charge};
                co_await storage::testing::store_contract::exercise(
                  files, *ownership, spec, budget, native_driver{});
                BOOST_CHECK_EQUAL(budget.snapshot().tasks, 0U);
                BOOST_CHECK_EQUAL(budget.snapshot().bytes, 0U);
                BOOST_CHECK_EQUAL(budget.snapshot().handles, 0U);
                const auto moved = directory.get_path() / "moved";
                std::filesystem::rename(root, moved);
                std::filesystem::create_directory(root);
                const auto invalid = co_await ownership->validate(spec);
                BOOST_REQUIRE(!invalid);
                BOOST_CHECK(invalid.error().code() == errc::wrong_context);
            }));
    } catch (...) {
        first = std::current_exception();
    }
    co_await manager.stop();
    co_await registry.stop();
    if (first) std::rethrow_exception(first);
}

SEASTAR_TEST_CASE(
  local_store_native_rejects_aliased_and_wrong_devices_before_locking) {
    co_await seastar::tmp_dir::do_with(
      runtime::testing::test_directory_template(),
      [](seastar::tmp_dir& directory) -> seastar::future<> {
          const auto a = directory.get_path() / "a";
          const auto b = directory.get_path() / "b";
          co_await seastar::recursive_touch_directory(a.string());
          std::filesystem::create_directory_symlink(a, b);
          auto status = co_await seastar::file_stat(a.string());
          auto primary = storage::testing::store_contract::specification(
            take(runtime::file_path::make(a.string())),
            {status.device_id, status.inode_number});
          auto alias = storage::testing::store_contract::specification(
            take(runtime::file_path::make(b.string())),
            {status.device_id, status.inode_number},
            0x44,
            storage::local_device_role::data);
          const std::array duplicate{primary, alias};
          auto rejected = co_await broker::storage_directories::acquire(
            duplicate);
          BOOST_REQUIRE(!rejected);
          BOOST_CHECK(rejected.error().code() == errc::wrong_context);
          BOOST_CHECK(!std::filesystem::exists(a / "kwaque.pid"));
          primary.directory.inode ^= 1;
          const std::array wrong{primary};
          rejected = co_await broker::storage_directories::acquire(wrong);
          BOOST_REQUIRE(!rejected);
          BOOST_CHECK(rejected.error().code() == errc::wrong_context);
          BOOST_CHECK(!std::filesystem::exists(a / "kwaque.pid"));
      });
}
