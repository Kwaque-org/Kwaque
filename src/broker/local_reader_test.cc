#include "src/broker/storage_directories.h"
#include "src/resource/resource_registry.h"
#include "src/runtime/production/file.h"
#include "src/runtime/testing/test_directory.h"
#include "src/storage/tests/local_reader_contract.h"

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
template<typename Func>
seastar::future<> with_readers(std::uint8_t device, Func function) {
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

SEASTAR_TEST_CASE(local_readers_native_root_lifetime) {
    co_await with_readers(
      51,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::reader_contract::root_lifetime(
            files, owner, spec, budget, drive);
      });
}
SEASTAR_TEST_CASE(local_readers_native_root_errors) {
    co_await with_readers(
      51,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::reader_contract::root_errors(
            files, owner, spec, budget, drive);
      });
}
SEASTAR_TEST_CASE(local_readers_native_generation_lifetime) {
    co_await with_readers(
      68,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::reader_contract::generation_lifetime(
            files, owner, spec, budget, drive);
      });
}
SEASTAR_TEST_CASE(local_readers_native_generation_errors) {
    co_await with_readers(
      68,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::reader_contract::generation_errors(
            files, owner, spec, budget, drive);
      });
}
SEASTAR_TEST_CASE(local_readers_native_wal_chain) {
    co_await with_readers(
      51,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::reader_contract::wal_chain(
            files, owner, spec, budget, drive);
      });
}
SEASTAR_TEST_CASE(local_readers_native_wal_alignment) {
    co_await with_readers(
      51,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::reader_contract::wal_alignment(
            files, owner, spec, budget, drive);
      });
}
SEASTAR_TEST_CASE(local_readers_native_discovery_cleanup) {
    co_await with_readers(
      68,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::reader_contract::discovery_cleanup(
            files, owner, spec, budget, drive);
      });
}
SEASTAR_TEST_CASE(local_readers_native_empty_retry) {
    co_await with_readers(
      68,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::reader_contract::empty_retry(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(local_readers_native_preserve_unexpected_symlinks) {
    co_await with_readers(
      0x33,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive)
        -> seastar::future<> {
          using namespace storage::testing::reader_contract;
          seastar::abort_source abort;
          codec::cooperative_work work{codec::limits::defaults(), abort};
          co_await seed_checkpoint(files, owner, spec, budget, work, drive);
          const auto path = take(
            take(storage::local_paths::make(spec.root))
              .sequence_file(0, storage::local_sequence_file::checkpoint, 70));
          const auto saved = spec.root.value() + "/preserved";
          std::filesystem::rename(path.value(), saved);
          std::filesystem::create_symlink(saved, path.value());
          auto rejected = co_await storage::local_root_owner::open(
            files,
            owner,
            spec,
            0,
            storage::testing::installation_contract::checkpoint_reference(),
            storage::testing::installation_contract::checkpoint_expectation(
              spec),
            budget,
            limits(),
            work);
          if (rejected) {
              take(co_await (*rejected)->close());
              rejected->reset();
          }
          require(
            !rejected && rejected.error().code() == errc::wrong_context,
            "root followed a symlink");
          bool observed = false;
          auto visit = [&](const storage::local_namespace_entry& entry) {
              if (entry.path == path) {
                  require(
                    entry.unexpected_kind,
                    "symlink was treated as regular metadata");
                  observed = true;
              }
              return seastar::make_ready_future<runtime::result<bool>>(true);
          };
          auto inventory = take(
            co_await storage::walk_local_namespace(
              files, owner, spec, budget, limits(), work, visit));
          require(
            inventory.complete && observed
              && std::filesystem::is_symlink(path.value()),
            "inventory removed or followed the link");
      });
}
SEASTAR_TEST_CASE(
  local_readers_native_cold_resolution_rejects_duplicate_devices) {
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
                const auto a = directory.get_path() / "a",
                           b = directory.get_path() / "b";
                co_await seastar::recursive_touch_directory(a.string());
                co_await seastar::recursive_touch_directory(b.string());
                const auto sa = co_await seastar::file_stat(a.string()),
                           sb = co_await seastar::file_stat(b.string());
                const std::array specs{
                  storage::testing::store_contract::specification(
                    take(runtime::file_path::make(a.string())),
                    {sa.device_id, sa.inode_number},
                    0x44),
                  storage::testing::store_contract::specification(
                    take(runtime::file_path::make(b.string())),
                    {sb.device_id, sb.inode_number},
                    0x55,
                    storage::local_device_role::data)};
                auto owner = take(
                  co_await broker::storage_directories::acquire(specs));
                runtime::production::file_system files;
                storage::workload_budget budget{
                  manager.acquire_workload(resource::workload_class::metadata),
                  {.tasks = 16,
                   .bytes = byte_count{8U * 1024U * 1024U},
                   .handles = 32},
                  bytes::testing::charge};
                co_await storage::testing::reader_contract::cold_resolution(
                  files, *owner, specs, budget, native_driver{});
                BOOST_CHECK_EQUAL(budget.snapshot().tasks, 0U);
            }));
    } catch (...) {
        first = std::current_exception();
    }
    co_await manager.stop();
    co_await registry.stop();
    if (first) std::rethrow_exception(first);
}
