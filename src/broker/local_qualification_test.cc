#include "src/base/allocation.h"
#include "src/broker/storage_directories.h"
#include "src/resource/resource_registry.h"
#include "src/runtime/production/file.h"
#include "src/runtime/testing/test_directory.h"
#include "src/storage/tests/local_qualification_contract.h"

#include <seastar/core/memory.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/util/tmp_file.hh>

#include <gtest/gtest.h>

#include <bit>

namespace {
using namespace kwaque;
using storage::testing::store_contract::require;
using storage::testing::store_contract::take;
// The native allocator counts oversized allocations even when their callers
// are inside shared libraries. Its threshold is inclusive; the storage limit
// allows exactly 128 KiB, so reject starting at the next byte.
static_assert(std::has_single_bit(maximum_contiguous_allocation_bytes));
constexpr std::size_t allocation_warning_threshold
  = maximum_contiguous_allocation_bytes + 1;

void check_allocation_bound(
  const seastar::memory::statistics& before,
  const seastar::memory::statistics& after) {
#if !defined(SEASTAR_DEFAULT_ALLOCATOR)
    EXPECT_GT(after.mallocs(), before.mallocs());
    EXPECT_EQ(after.large_allocations(), before.large_allocations());
    EXPECT_EQ(after.failed_allocations(), before.failed_allocations());
    EXPECT_EQ(after.foreign_mallocs(), before.foreign_mallocs());
    EXPECT_EQ(after.fallback_allocations(), before.fallback_allocations());
    EXPECT_EQ(
      seastar::memory::get_large_allocation_warning_threshold(),
      allocation_warning_threshold);
#else
    static_cast<void>(before);
    static_cast<void>(after);
#endif
}

struct native_driver final {
    template<typename T>
    seastar::future<T> lifecycle(seastar::future<T> operation) const {
        return operation;
    }
};
template<typename Func>
seastar::future<> with_qualification(std::uint8_t device, Func function) {
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
                EXPECT_EQ(budget.snapshot().tasks, 0U);
                EXPECT_EQ(budget.snapshot().bytes, 0U);
                EXPECT_EQ(budget.snapshot().handles, 0U);
            }));
    } catch (...) {
        first = std::current_exception();
    }
    co_await manager.stop();
    co_await registry.stop();
    if (first) std::rethrow_exception(first);
}

seastar::future<> local_metadata_native_publication_limits() {
    co_await with_qualification(
      51,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::qualification_contract::publication_limits(
            files, owner, spec, budget, drive);
      });
}
seastar::future<> local_metadata_native_root_pressure_and_cancellation() {
    co_await with_qualification(
      51,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::qualification_contract::
            root_pressure_and_cancellation(files, owner, spec, budget, drive);
      });
}
seastar::future<> local_metadata_native_failed_generation_open() {
    co_await with_qualification(
      68,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::qualification_contract::
            failed_generation_open(files, owner, spec, budget, drive);
      });
}
seastar::future<> local_metadata_native_discovery_cancel_and_unsupported() {
    co_await with_qualification(
      51,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::qualification_contract::
            discovery_cancel_and_unsupported(files, owner, spec, budget, drive);
      });
}
seastar::future<> local_metadata_native_allocation_rollback() {
    co_await with_qualification(
      51,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::qualification_contract::allocation_rollback(
            files, owner, spec, budget, drive);
      });
}

seastar::future<> local_metadata_native_served_allocation_bound() {
    co_await with_qualification(
      68,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive)
        -> seastar::future<> {
          std::exception_ptr failure;
          const auto before = seastar::memory::stats();
          seastar::memory::scoped_large_allocation_warning_threshold threshold{
            allocation_warning_threshold};
          try {
              co_await storage::testing::installation_contract::maximum_bundle(
                files, owner, spec, budget, drive);
          } catch (...) {
              failure = std::current_exception();
          }
          const auto after = seastar::memory::stats();
          if (failure) std::rethrow_exception(failure);
          check_allocation_bound(before, after);
      });
}

seastar::future<> local_metadata_native_failed_replacement_keeps_old_pin() {
    co_await with_qualification(
      68,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::qualification_contract::
            failed_replacement_keeps_old_pin(files, owner, spec, budget, drive);
      });
}

seastar::future<> local_metadata_native_reader_allocation_bound() {
    co_await with_qualification(
      68,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive)
        -> seastar::future<> {
          using namespace storage::testing::reader_contract;
          seastar::abort_source abort;
          codec::cooperative_work work{codec::limits::defaults(), abort};
          co_await seed_segment(files, owner, spec, budget, work, drive);
          const auto path = take(
            take(storage::local_paths::make(spec.root))
              .segment_file(
                0,
                {segment().segment(), segment().generation()},
                storage::local_segment_file::published));
          co_await write_bytes(
            files,
            path,
            storage::testing::local_fixture::read("publication_all_roots"),
            drive);
          const auto root_contexts = contexts(spec);
          const storage::local_generation_expectation expected{
            storage::testing::publication_contract::generation(3),
            sealed_publication(),
            descriptor(),
            root_contexts};
          const std::array expected_pages{
            storage::testing::local_fixture::read("index_page"),
            storage::testing::local_fixture::read("sealed_retry_page"),
            storage::testing::local_fixture::read("snapshot_page")};
          std::unique_ptr<storage::local_generation_owner> generation;
          runtime::first_failure failed;
          const auto before = seastar::memory::stats();
          seastar::memory::scoped_large_allocation_warning_threshold threshold{
            allocation_warning_threshold};
          try {
              generation = take(
                co_await storage::local_generation_owner::open(
                  files, owner, spec, 0, expected, budget, limits(), work));
              auto pin = take(generation->pin());
              std::size_t i = 0;
              for (const auto kind :
                   {storage::local_root_kind::index,
                    storage::local_root_kind::sealed_retry,
                    storage::local_root_kind::completed_retry_snapshot}) {
                  auto root = take(pin.root(kind));
                  auto page = take(
                    co_await root.read(
                      storage::page_ordinal::make(0).value(), work));
                  require(
                    page.bytes.content_equals(expected_pages[i++]),
                    "measured root read differs from fixture");
              }
          } catch (...) {
              failed.observe(std::current_exception());
          }
          if (generation) {
              try {
                  failed.observe(co_await generation->close());
              } catch (...) {
                  failed.observe(std::current_exception());
              }
              generation.reset();
          }
          const auto after = seastar::memory::stats();
          take(failed.outcome());
          check_allocation_bound(before, after);
      });
}

seastar::future<> local_metadata_native_maximum_metadata_allocation_bound() {
    co_await with_qualification(
      51,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive)
        -> seastar::future<> {
          std::exception_ptr failure;
          const auto before = seastar::memory::stats();
          seastar::memory::scoped_large_allocation_warning_threshold threshold{
            allocation_warning_threshold};
          try {
              co_await storage::testing::qualification_contract::
                publication_limits(files, owner, spec, budget, drive);
          } catch (...) {
              failure = std::current_exception();
          }
          const auto after = seastar::memory::stats();
          if (failure) std::rethrow_exception(failure);
          check_allocation_bound(before, after);
      });
}

seastar::future<> local_metadata_native_read_limits() {
    co_await with_qualification(
      51,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::qualification_contract::metadata_read_limits(
            files, owner, spec, budget, drive);
      });
}

class LocalQualificationTest : public ::testing::Test {
protected:
    void SetUp() override { seastar::set_abort_on_internal_error(true); }
    void TearDown() override {
        EXPECT_EQ(seastar::engine().abandoned_failed_futures(), 0U);
    }
};

TEST_F(LocalQualificationTest, MetadataReadLimits) {
    local_metadata_native_read_limits().get();
}

TEST_F(LocalQualificationTest, PublicationLimits) {
    local_metadata_native_publication_limits().get();
}

TEST_F(LocalQualificationTest, RootPressureAndCancellation) {
    local_metadata_native_root_pressure_and_cancellation().get();
}

TEST_F(LocalQualificationTest, FailedGenerationOpen) {
    local_metadata_native_failed_generation_open().get();
}

TEST_F(LocalQualificationTest, DiscoveryCancelAndUnsupported) {
    local_metadata_native_discovery_cancel_and_unsupported().get();
}

TEST_F(LocalQualificationTest, AllocationRollback) {
    local_metadata_native_allocation_rollback().get();
}

TEST_F(LocalQualificationTest, ServedAllocationBound) {
    local_metadata_native_served_allocation_bound().get();
}

TEST_F(LocalQualificationTest, FailedReplacementKeepsOldPin) {
    local_metadata_native_failed_replacement_keeps_old_pin().get();
}

TEST_F(LocalQualificationTest, ReaderAllocationBound) {
    local_metadata_native_reader_allocation_bound().get();
}

TEST_F(LocalQualificationTest, MaximumMetadataAllocationBound) {
    local_metadata_native_maximum_metadata_allocation_bound().get();
}

} // namespace
