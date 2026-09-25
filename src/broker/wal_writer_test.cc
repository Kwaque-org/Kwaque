#include "src/broker/storage_directories.h"
#include "src/observability/event_identity.h"
#include "src/resource/resource_registry.h"
#include "src/runtime/production/clocks.h"
#include "src/runtime/production/environment.h"
#include "src/runtime/production/file.h"
#include "src/runtime/production/timer.h"
#include "src/runtime/testing/test_directory.h"
#include "src/storage/tests/wal_append_contract.h"
#include "src/storage/tests/wal_commit_lifecycle_contract.h"
#include "src/storage/tests/wal_commit_qualification_contract.h"
#include "src/storage/tests/wal_durability_contract.h"
#include "src/storage/tests/wal_group_commit_contract.h"
#include "src/storage/tests/wal_lifecycle_contract.h"
#include "src/storage/tests/wal_qualification_contract.h"
#include "src/storage/tests/wal_rotation_contract.h"
#include "src/storage/tests/wal_writer_contract.h"

#include <seastar/core/memory.hh>
#include <seastar/core/preempt.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/later.hh>
#include <seastar/util/tmp_file.hh>

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <cstdint>

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
seastar::future<>
with_wal(std::uint8_t device, Func function, std::uint32_t tasks = 32) {
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
            [&, device, tasks](
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
                  {.tasks = tasks,
                   .bytes = byte_count{16U * 1024U * 1024U},
                   .handles = 32},
                  bytes::testing::charge};
                if constexpr (requires {
                                  function(
                                    files,
                                    *ownership,
                                    spec,
                                    budget,
                                    native_driver{},
                                    manager);
                              })
                    co_await function(
                      files,
                      *ownership,
                      spec,
                      budget,
                      native_driver{},
                      manager);
                else
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

SEASTAR_TEST_CASE(wal_writer_native_bootstrap_and_exact_child_preparation) {
    co_await with_wal(
      0x33,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::wal_writer_contract::exercise(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(wal_writer_native_group_write_and_captured_barrier) {
    co_await with_wal(
      0x33,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::wal_append_contract::exercise(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(wal_writer_native_allocator_rounded_staging) {
    co_await with_wal(
      0x33,
      [](
        auto& files,
        auto& owner,
        const auto& spec,
        auto&,
        auto drive,
        auto& manager) -> seastar::future<> {
          const auto charge = +[](byte_count request) noexcept {
              if (request.value() > UINT64_MAX / 2)
                  return byte_count{UINT64_MAX};
              return std::max(
                bytes::testing::charge(request),
                byte_count{2 * request.value()});
          };
          storage::workload_budget budget{
            manager.acquire_workload(resource::workload_class::metadata),
            {.tasks = 32,
             .bytes = byte_count{16U * 1024U * 1024U},
             .handles = 32},
            charge};
          BOOST_CHECK(!budget.allocation_charge(byte_count{131072}));
          BOOST_CHECK(budget.allocation_charge(byte_count{65536}).has_value());
          co_await storage::testing::wal_append_contract::exercise(
            files, owner, spec, budget, drive);
          BOOST_CHECK_EQUAL(budget.snapshot().bytes, 0U);
          BOOST_CHECK_EQUAL(budget.snapshot().tasks, 0U);
          BOOST_CHECK_EQUAL(budget.snapshot().handles, 0U);
      });
}

SEASTAR_TEST_CASE(wal_writer_native_construction_failure_joins_providers) {
    co_await with_wal(
      0x33,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::wal_writer_contract::
            construction_failure_cleanup(files, owner, spec, budget, drive);
      });
}
SEASTAR_TEST_CASE(wal_writer_native_whole_group_capacity) {
    co_await with_wal(
      0x33,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::wal_append_contract::capacity(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(wal_writer_native_rotation_and_extent) {
    co_await with_wal(
      0x33,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::wal_rotation_contract::exercise(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(wal_writer_native_rotation_exact_capacity) {
    co_await with_wal(
      0x33,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::wal_rotation_contract::capacity(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(wal_writer_native_rotation_publication_pressure) {
    co_await with_wal(
      0x33,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::wal_rotation_contract::pressure(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(wal_writer_native_close_retains_unreferenced_successor) {
    co_await with_wal(
      0x33,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::wal_rotation_contract::pressure(
            files, owner, spec, budget, drive, true);
      });
}

SEASTAR_TEST_CASE(wal_writer_native_recovery_inventory) {
    co_await with_wal(
      0x33,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::wal_lifecycle_contract::inventory(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(wal_writer_native_exceptional_rotation_close) {
    co_await with_wal(
      0x33,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::wal_lifecycle_contract::
            exceptional_rotation_close(files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(wal_writer_native_early_stop_under_workload_pressure) {
    co_await with_wal(
      0x33,
      [](
        auto& files,
        auto& owner,
        const auto& spec,
        auto& budget,
        auto drive,
        auto& manager) -> seastar::future<> {
          auto observer = manager.acquire_workload(
            resource::workload_class::metadata);
          seastar::abort_source stopped;
          co_await storage::testing::wal_lifecycle_contract::shutdown(
            files,
            owner,
            spec,
            budget,
            drive,
            stopped,
            [&] { stopped.request_abort(); },
            observer.memory_admission());
      });
}

SEASTAR_TEST_CASE(
  wal_writer_native_completion_reentry_reuses_slot_and_joins_close) {
    co_await with_wal(
      0x33,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::wal_lifecycle_contract::completion_reentry(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(wal_writer_native_pinned_headers) {
    co_await with_wal(
      0x33,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::wal_qualification_contract::pinned_headers(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(wal_writer_native_bounded_admission) {
    co_await with_wal(
      0x33,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::wal_qualification_contract::
            bounded_admission(files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(wal_writer_native_submission_allocation_cuts_join_owners) {
    for (std::size_t at = 0; at != 16; ++at)
        co_await with_wal(
          0x33,
          [at](
            auto& files,
            auto& owner,
            const auto& spec,
            auto& budget,
            auto drive) {
              return storage::testing::wal_qualification_contract::
                submission_allocation_cut(
                  files, owner, spec, budget, drive, at);
          });
}

SEASTAR_TEST_CASE(wal_writer_native_accepted_encoding_oom_joins_owners) {
    co_await with_wal(
      0x33,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::wal_qualification_contract::
            accepted_encoding_allocation_cut(files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(wal_group_commit_native_formation_and_retained_capture) {
    co_await with_wal(
      0x33,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::wal_group_commit_contract::formation<
            runtime::production::monotonic_clock>(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(wal_group_commit_native_capacity_seals_forming_batch) {
    co_await with_wal(
      0x33,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::wal_group_commit_contract::
            capacity_seals_forming_batch<runtime::production::monotonic_clock>(
              files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(wal_group_commit_native_count_byte_and_deadline_boundaries) {
    co_await with_wal(
      0x33,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::wal_group_commit_contract::boundaries<
            runtime::production::monotonic_clock>(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(wal_group_commit_native_timer_and_forced_drain) {
    co_await with_wal(
      0x33,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive)
        -> seastar::future<> {
          runtime::production::timer timer;
          std::exception_ptr failure;
          try {
              co_await storage::testing::wal_group_commit_contract::timers<
                runtime::production::monotonic_clock>(
                files, owner, spec, budget, drive, timer);
          } catch (...) {
              failure = std::current_exception();
          }
          take(co_await timer.stop());
          if (failure) std::rethrow_exception(failure);
      });
}

SEASTAR_TEST_CASE(wal_group_commit_native_preacceptance_allocation_cuts) {
    co_await with_wal(
      0x33,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::wal_group_commit_contract::allocation_cuts<
            runtime::production::monotonic_clock>(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(
  wal_group_commit_native_maximum_members_crosses_two_writer_groups) {
    co_await with_wal(
      0x33,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::wal_group_commit_contract::maximum_members<
            runtime::production::monotonic_clock>(
            files, owner, spec, budget, drive);
      },
      256);
}

SEASTAR_TEST_CASE(
  wal_group_commit_native_accepted_encoding_exception_is_joined) {
    co_await with_wal(
      0x33,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::wal_group_commit_contract::
            accepted_encoding_failure<runtime::production::monotonic_clock>(
              files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(wal_group_commit_native_durable_completion) {
    co_await with_wal(
      0x33,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::wal_durability_contract::completion<
            runtime::production::monotonic_clock>(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(wal_group_commit_native_durable_retained_pressure) {
    co_await with_wal(
      0x33,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::wal_durability_contract::retained_pressure<
            runtime::production::monotonic_clock>(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(wal_group_commit_native_durable_observer_admission) {
    co_await with_wal(
      0x33,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::wal_durability_contract::observer_admission<
            runtime::production::monotonic_clock>(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(wal_group_commit_native_durable_result_allocation_cuts) {
    co_await with_wal(
      0x33,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::wal_durability_contract::
            result_allocation_cuts<runtime::production::monotonic_clock>(
              files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(wal_group_commit_native_durable_close_during_flush) {
    co_await with_wal(
      0x33,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::wal_durability_contract::close_during_flush<
            runtime::production::monotonic_clock>(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(wal_group_commit_native_durable_stale_capture) {
    co_await with_wal(
      0x33,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::wal_durability_contract::stale_capture<
            runtime::production::monotonic_clock>(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(wal_group_commit_native_durable_observer_chunk_boundaries) {
    co_await with_wal(
      0x33,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::wal_durability_contract::
            observer_chunk_boundaries<runtime::production::monotonic_clock>(
              files, owner, spec, budget, drive);
      });
}

namespace {
template<typename Func>
seastar::future<> with_commit_timer(Func body) {
    co_await with_wal(
      0x33,
      [&](auto& files, auto& owner, const auto& spec, auto& budget, auto drive)
        -> seastar::future<> {
          runtime::production::timer timer;
          std::exception_ptr failure;
          try {
              co_await body(files, owner, spec, budget, drive, timer);
          } catch (...) {
              failure = std::current_exception();
          }
          take(co_await timer.stop());
          if (failure) std::rethrow_exception(failure);
      });
}
} // namespace

SEASTAR_TEST_CASE(wal_group_commit_native_lifecycle_cancellation) {
    co_await with_commit_timer([](
                                 auto& files,
                                 auto& owner,
                                 const auto& spec,
                                 auto& budget,
                                 auto drive,
                                 auto& timer) {
        return storage::testing::wal_commit_lifecycle_contract::cancellation<
          runtime::production::monotonic_clock>(
          files, owner, spec, budget, drive, timer);
    });
}

SEASTAR_TEST_CASE(wal_group_commit_native_lifecycle_deadlines) {
    co_await with_commit_timer([](
                                 auto& files,
                                 auto& owner,
                                 const auto& spec,
                                 auto& budget,
                                 auto drive,
                                 auto& timer) {
        return storage::testing::wal_commit_lifecycle_contract::deadlines<
          runtime::production::monotonic_clock>(
          files, owner, spec, budget, drive, timer);
    });
}

SEASTAR_TEST_CASE(wal_group_commit_native_lifecycle_rotation) {
    co_await with_commit_timer([](
                                 auto& files,
                                 auto& owner,
                                 const auto& spec,
                                 auto& budget,
                                 auto drive,
                                 auto& timer) {
        return storage::testing::wal_commit_lifecycle_contract::rotation<
          runtime::production::monotonic_clock>(
          files, owner, spec, budget, drive, timer);
    });
}

SEASTAR_TEST_CASE(wal_group_commit_native_lifecycle_timer_failure) {
    co_await with_wal(
      0x33,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::wal_commit_lifecycle_contract::timer_failure<
            runtime::production::monotonic_clock>(
            files, owner, spec, budget, drive);
      });
}
SEASTAR_TEST_CASE(wal_group_commit_native_lifecycle_shutdown) {
    co_await with_commit_timer(
      [](
        auto& files,
        auto& owner,
        const auto& spec,
        auto& budget,
        auto drive,
        auto& timer) -> seastar::future<> {
          seastar::abort_source source;
          co_await storage::testing::wal_commit_lifecycle_contract::shutdown<
            runtime::production::monotonic_clock>(
            files, owner, spec, budget, drive, timer, source, [&] {
                source.request_abort();
                timer.request_abort();
            });
      });
}

SEASTAR_TEST_CASE(wal_group_commit_native_lifecycle_service) {
    co_await with_commit_timer([](
                                 auto& files,
                                 auto& owner,
                                 const auto& spec,
                                 auto& budget,
                                 auto drive,
                                 auto& timer) {
        return storage::testing::wal_commit_lifecycle_contract::service<
          runtime::production::monotonic_clock>(
          files, owner, spec, budget, drive, timer);
    });
}

SEASTAR_TEST_CASE(wal_group_commit_native_lifecycle_environment_abort) {
    auto config = resource::resource_config::from_total_memory(
                    byte_count{seastar::memory::stats().total_memory()})
                    .value();
    resource::resource_registry registry;
    co_await registry.start(config);
    seastar::logger logger{"wal_environment_test"};
    runtime::first_failure failed;
    {
        runtime::production::environment env{
          runtime::production::environment_dependencies{
            registry.handles(),
            logger,
            observability::event_sink_identity{
              .epoch = take(observability::event_sink_epoch::make(1)),
              .configuration_digest = {}}}};
        try {
            co_await env.start();
            co_await seastar::tmp_dir::do_with(
              runtime::testing::test_directory_template(),
              seastar::coroutine::lambda(
                [&](seastar::tmp_dir& directory) -> seastar::future<> {
                    const auto root = directory.get_path() / "store";
                    co_await seastar::recursive_touch_directory(root.string());
                    const auto status = co_await seastar::file_stat(
                      root.string(), seastar::follow_symlink::no);
                    const auto spec
                      = storage::testing::store_contract::specification(
                        take(runtime::file_path::make(root.string())),
                        {status.device_id, status.inode_number},
                        0x33);
                    const std::array specs{spec};
                    auto ownership = take(
                      co_await broker::storage_directories::acquire(specs));
                    storage::workload_budget budget{
                      env.resource_manager().acquire_workload(
                        resource::workload_class::metadata),
                      {.tasks = 32,
                       .bytes = byte_count{16U * 1024U * 1024U},
                       .handles = 32},
                      bytes::testing::charge};
                    auto& files = env.file_system();
                    auto& timer = env.timer();
                    auto& source = env.tasks().abort_source();
                    co_await storage::testing::wal_commit_lifecycle_contract::
                      shutdown<runtime::production::monotonic_clock>(
                        files,
                        *ownership,
                        spec,
                        budget,
                        native_driver{},
                        timer,
                        source,
                        [&] {
                            env.request_abort();
                            storage::testing::store_contract::require(
                              !env.lifetime().acquire(),
                              "shutdown acquired a new runtime lease");
                        });
                    BOOST_CHECK_EQUAL(budget.snapshot().tasks, 0U);
                    BOOST_CHECK_EQUAL(budget.snapshot().bytes, 0U);
                    BOOST_CHECK_EQUAL(budget.snapshot().handles, 0U);
                }));
        } catch (...) {
            failed.observe(std::current_exception());
        }
        try {
            co_await env.stop();
        } catch (...) {
            failed.observe(std::current_exception());
        }
    }
    try {
        co_await registry.stop();
    } catch (...) {
        failed.observe(std::current_exception());
    }
    take(failed.outcome());
}

SEASTAR_TEST_CASE(wal_group_commit_native_lifecycle_observer_failures) {
    co_await with_commit_timer([](
                                 auto& files,
                                 auto& owner,
                                 const auto& spec,
                                 auto& budget,
                                 auto drive,
                                 auto& timer) {
        return storage::testing::wal_commit_lifecycle_contract::
          observer_failures<runtime::production::monotonic_clock>(
            files, owner, spec, budget, drive, timer);
    });
}

SEASTAR_TEST_CASE(wal_group_commit_native_qualification_delivery_permutations) {
    co_await with_wal(
      0x33,
      [](auto& files, auto& owner, const auto& spec, auto& budget, auto drive) {
          return storage::testing::wal_commit_qualification_contract::ordering<
            runtime::production::monotonic_clock>(
            files, owner, spec, budget, drive);
      });
}
