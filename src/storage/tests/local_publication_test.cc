#include "src/bytes/test_allocation_profile.h"
#include "src/resource/resource_registry.h"
#include "src/runtime/production/file.h"
#include "src/runtime/testing/test_directory.h"
#include "src/storage/tests/local_publication_contract.h"

#include <seastar/core/memory.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/tmp_file.hh>

#include <boost/test/unit_test.hpp>

#include <filesystem>

namespace {
using namespace kwaque;
using namespace storage::testing::publication_contract;
struct driver {
    template<typename T>
    seastar::future<T> lifecycle(seastar::future<T> operation) const {
        return operation;
    }
};
} // namespace

SEASTAR_TEST_CASE(local_publication_and_bounded_discovery_use_real_files) {
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
                runtime::production::file_system files;
                storage::workload_budget budget{
                  manager.acquire_workload(resource::workload_class::metadata),
                  {.tasks = 4,
                   .bytes = byte_count{2U * 1024U * 1024U},
                   .handles = 4},
                  bytes::testing::charge};
                const auto root = take(
                  runtime::file_path::make(
                    (directory.get_path() / "store").string()));
                co_await kwaque::storage::testing::publication_contract::run(
                  files, budget, root, driver{});
                co_await walk(files, root, driver{});
                auto link = directory.get_path() / "link";
                std::filesystem::create_directory_symlink(
                  directory.get_path() / "store", link);
                seastar::abort_source abort;
                codec::cooperative_work work{codec::limits::defaults(), abort};
                auto linked = take(
                  runtime::file_path::make((link / "control").string()));
                auto rejected = co_await storage::inspect_local_path(
                  files,
                  take(runtime::file_path::make(directory.get_path().string())),
                  linked,
                  runtime::file_kind::regular,
                  work);
                BOOST_REQUIRE(!rejected);
                BOOST_CHECK(rejected.error().code() == errc::wrong_context);
                std::filesystem::create_symlink(
                  directory.get_path() / "store" / "control",
                  directory.get_path() / "file-link");
                rejected = co_await storage::inspect_local_path(
                  files,
                  take(runtime::file_path::make(directory.get_path().string())),
                  take(
                    runtime::file_path::make(
                      (directory.get_path() / "file-link").string())),
                  runtime::file_kind::regular,
                  work);
                BOOST_REQUIRE(!rejected);
                BOOST_CHECK(rejected.error().code() == errc::wrong_context);
            }));
    } catch (...) {
        first = std::current_exception();
    }
    co_await manager.stop();
    co_await registry.stop();
    if (first) std::rethrow_exception(first);
}
