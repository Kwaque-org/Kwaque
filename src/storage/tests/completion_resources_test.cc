#include "src/base/allocation.h"
#include "src/resource/resource_registry.h"
#include "src/runtime/cross_shard.h"
#include "src/runtime/production/file.h"
#include "src/runtime/testing/test_directory.h"
#include "src/storage/tests/completion_contract.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/memory.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/alloc_failure_injector.hh>
#include <seastar/util/later.hh>
#include <seastar/util/tmp_file.hh>

#include <boost/test/unit_test.hpp>

#include <array>
#include <cstdint>
#include <exception>
#include <limits>
#include <malloc.h>
#include <optional>
#include <stdexcept>

namespace {
using namespace kwaque;
using namespace kwaque::storage;
using kwaque::storage::testing::take;
constexpr auto workload = resource::workload_class::metadata;
static_assert(!runtime::cross_shard_value<completion_resources>);
static_assert(!runtime::cross_shard_value<runtime::file::metadata_reservation>);

template<typename Func>
seastar::future<> with_file(Func body) {
    auto config = resource::resource_config::from_total_memory(
      byte_count{seastar::memory::stats().total_memory()});
    if (!config) throw std::system_error(config.error());
    resource::resource_registry registry;
    co_await registry.start(*config);
    resource::resource_manager manager{registry.handles()};
    std::exception_ptr first;
    try {
        co_await manager.start();
        co_await seastar::tmp_dir::do_with(
          runtime::testing::test_directory_template(),
          seastar::coroutine::lambda(
            [&body,
             &manager](seastar::tmp_dir& directory) -> seastar::future<> {
                runtime::production::file_system files;
                auto file = take(
                  co_await files.open(
                    take(
                      runtime::file_path::make(
                        (directory.get_path() / "completion").string())),
                    {.access = runtime::file_access::read_write,
                     .create = true,
                     .exclusive = true,
                     .close_policy = runtime::file_close_policy::checked}));
                std::exception_ptr failure;
                try {
                    co_await body(manager, file);
                } catch (...) {
                    failure = std::current_exception();
                }
                try {
                    take(co_await file.close());
                } catch (...) {
                    if (!failure) failure = std::current_exception();
                }
                if (failure) std::rethrow_exception(failure);
            }));
    } catch (...) {
        first = std::current_exception();
    }
    co_await manager.stop();
    co_await registry.stop();
    if (first) std::rethrow_exception(first);
}
} // namespace

SEASTAR_TEST_CASE(
  completion_reserve_keeps_native_capacity_after_admission_closes) {
    co_await with_file([](auto& manager, auto& file) -> seastar::future<> {
        workload_budget budget{
          manager.acquire_workload(workload),
          {.tasks = 1, .bytes = byte_count{256U * 1024U}, .handles = 1},
          bytes::testing::charge};
        auto reserved = take(completion_resources::make(budget, file));
        seastar::chunked_vector<runtime::file::metadata_reservation> pressure;
        while (auto slot = file.try_reserve_metadata())
            pressure.push_back(std::move(*slot));
        auto lease = manager.acquire_workload(workload);
        auto memory = seastar::try_get_units(
          lease.memory_admission(), lease.memory_admission().current());
        budget.close_admission();
        const auto rejected = budget.try_reserve(byte_count{1});
        const auto ordinary = co_await file.flush();
        reserved.scratch().front() = 'x';
        const auto flushed = co_await file.flush(reserved.metadata());
        const auto snapshot = budget.snapshot();
        reserved.release_metadata();
        pressure.clear();
        BOOST_CHECK(!rejected && rejected.error().code() == errc::closed);
        BOOST_CHECK(!ordinary && ordinary.error().code() == errc::queue_full);
        BOOST_CHECK(flushed.has_value());
        BOOST_CHECK_EQUAL(reserved.scratch().front(), 'x');
        BOOST_CHECK_EQUAL(snapshot.tasks, 1U);
        BOOST_CHECK_EQUAL(snapshot.handles, 1U);
        BOOST_CHECK_EQUAL(snapshot.bytes, reserved.charged_bytes().value());
        BOOST_CHECK_EQUAL(file.pending_metadata_operations(), 0U);
    });
}

SEASTAR_TEST_CASE(
  completion_reserve_rolls_back_allocation_and_admission_failures) {
    co_await with_file([](auto& manager, auto& file) -> seastar::future<> {
        workload_budget budget{
          manager.acquire_workload(workload),
          {.tasks = 8, .bytes = byte_count{1024U * 1024U}, .handles = 2},
          bytes::testing::charge};
        auto lease = manager.acquire_workload(workload);
        const auto available = lease.memory_admission().current();
        {
            auto occupied = seastar::try_get_units(
              lease.memory_admission(), available);
            const auto blocked = completion_resources::make(budget, file);
            BOOST_CHECK(!blocked && blocked.error().code() == errc::queue_full);
            BOOST_CHECK_EQUAL(budget.snapshot().tasks, 0U);
            BOOST_CHECK_EQUAL(budget.snapshot().bytes, 0U);
        }
        {
            auto handles = take(budget.try_reserve(byte_count{512}));
            take(handles.try_acquire_handles(2));
            const auto before = budget.snapshot();
            const auto blocked = completion_resources::make(budget, file);
            BOOST_CHECK(!blocked && blocked.error().code() == errc::queue_full);
            BOOST_CHECK_EQUAL(budget.snapshot().tasks, before.tasks);
            BOOST_CHECK_EQUAL(budget.snapshot().bytes, before.bytes);
            BOOST_CHECK_EQUAL(budget.snapshot().handles, before.handles);
            BOOST_CHECK_EQUAL(file.pending_metadata_operations(), 0U);
        }
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
        bool success = false, failed = false;
        for (unsigned allocation = 0; allocation < 64 && !success;
             ++allocation) {
            auto& injector = seastar::memory::local_failure_injector();
            injector.fail_after(allocation);
            std::optional<completion_resources> reserved;
            try {
                auto made = completion_resources::make(budget, file);
                injector.cancel();
                reserved.emplace(take(std::move(made)));
                success = true;
            } catch (const std::bad_alloc&) {
                failed = true;
                injector.cancel();
            }
            reserved.reset();
            BOOST_CHECK_EQUAL(budget.snapshot().tasks, 0U);
            BOOST_CHECK_EQUAL(budget.snapshot().bytes, 0U);
            BOOST_CHECK_EQUAL(budget.snapshot().handles, 0U);
            BOOST_CHECK_EQUAL(file.pending_metadata_operations(), 0U);
            BOOST_CHECK_EQUAL(lease.memory_admission().current(), available);
        }
        BOOST_CHECK(success && failed);
#endif
        co_return;
    });
}

SEASTAR_TEST_CASE(
  completion_reserve_qualifies_served_backing_maxima_and_churn) {
    co_await with_file([](auto& manager, auto& file) -> seastar::future<> {
        workload_budget budget{
          manager.acquire_workload(workload),
          {.tasks = 65536,
           .bytes = byte_count{1024U * 1024U},
           .handles = 65536},
          bytes::testing::charge};
        const std::array invalid{
          completion_resource_limits{byte_count{}, byte_count{16384}},
          completion_resource_limits{byte_count{4096}, byte_count{}},
          completion_resource_limits{byte_count{UINT64_MAX}, byte_count{16384}},
          completion_resource_limits{byte_count{4096}, byte_count{UINT64_MAX}}};
        for (const auto bounds : invalid) {
            BOOST_CHECK(!completion_resources::make(budget, file, bounds));
            BOOST_CHECK_EQUAL(budget.snapshot().tasks, 0U);
        }
        for (const std::uint64_t scratch_size : {1U, 4096U, 65536U, 131072U}) {
            auto made = completion_resources::make(
              budget, file, {.scratch_bytes = byte_count{scratch_size}});
            if (!made) {
                BOOST_CHECK(
                  bytes::testing::charge(byte_count{scratch_size}).value()
                  > maximum_contiguous_allocation_bytes);
                continue;
            }
            const auto served = ::malloc_usable_size(made->scratch().data());
            BOOST_CHECK_GE(served, made->scratch().size());
            BOOST_CHECK_LE(served, maximum_contiguous_allocation_bytes);
            BOOST_CHECK_LE(
              served,
              bytes::testing::charge(byte_count{made->scratch().size()})
                .value());
            BOOST_CHECK_LE(
              served + sizeof(completion_resources),
              made->charged_bytes().value());
            made->release_metadata();
        }
        for (unsigned iteration = 0; iteration < 512; ++iteration) {
            {
                auto reserved = take(completion_resources::make(budget, file));
                reserved.scratch().front() = static_cast<char>(iteration);
                auto moved = std::move(reserved);
                BOOST_CHECK_EQUAL(
                  moved.scratch().front(), static_cast<char>(iteration));
                moved.release_metadata();
            }
            BOOST_CHECK_EQUAL(budget.snapshot().tasks, 0U);
            BOOST_CHECK_EQUAL(budget.snapshot().bytes, 0U);
            BOOST_CHECK_EQUAL(budget.snapshot().handles, 0U);
            BOOST_CHECK_EQUAL(file.pending_metadata_operations(), 0U);
            if (iteration % 32U == 0) co_await seastar::yield();
        }
    });
}
