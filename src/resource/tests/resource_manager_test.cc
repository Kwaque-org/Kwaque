#include "src/base/error.h"
#include "src/base/units.h"
#include "src/resource/resource_manager.h"
#include "src/resource/resource_registry.h"
#include "src/resource/workload_class.h"
#include "src/runtime/sharded_service.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/smp.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/later.hh>
#include <seastar/util/tmp_file.hh>

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace kwaque::resource {

namespace {

resource_config test_config() {
    auto config = resource_config::from_total_memory(
      byte_count{
        static_cast<std::uint64_t>(seastar::memory::stats().total_memory())});
    if (!config) {
        throw std::runtime_error("test resource configuration was rejected");
    }
    return *config;
}

seastar::future<> verify_local_manager(resource_manager& manager) {
    if (!manager.ready() || !manager.owner().is_current()) {
        throw std::runtime_error("resource manager has the wrong owner");
    }
    for (const auto classification : all_workload_classes) {
        const auto expected = manager.scheduling_group(classification);
        co_await manager.with_workload_class(classification, [expected] {
            if (seastar::current_scheduling_group() != expected) {
                throw std::runtime_error(
                  "work ran in the wrong scheduling group");
            }
        });
    }
}

struct remote_saturation_state final {
    seastar::shared_promise<> release;
    unsigned active{0};
    unsigned maximum{0};
};

thread_local std::unique_ptr<remote_saturation_state> remote_saturation;

void reset_remote_saturation() {
    remote_saturation = std::make_unique<remote_saturation_state>();
}

seastar::future<> hold_remote_request() {
    auto& state = *remote_saturation;
    ++state.active;
    state.maximum = std::max(state.maximum, state.active);
    co_await state.release.get_shared_future();
    --state.active;
}

unsigned remote_active_requests() { return remote_saturation->active; }

unsigned remote_maximum_requests() { return remote_saturation->maximum; }

void release_remote_requests() { remote_saturation->release.set_value(); }

seastar::future<> verify_dma_scheduling_group(
  resource_manager& manager,
  bool& observed_group,
  seastar::tmp_file& temporary) {
    auto& file = temporary.get_file();
    const auto transfer_size = file.memory_dma_alignment();
    auto buffer = seastar::temporary_buffer<char>::aligned(
      transfer_size, transfer_size);
    std::memset(buffer.get_write(), 0x5a, buffer.size());
    const auto written = co_await manager.with_workload_class(
      workload_class::offload,
      [&manager, &file, &observed_group, buffer = std::move(buffer)] mutable {
          observed_group = seastar::current_scheduling_group()
                           == manager.scheduling_group(workload_class::offload);
          return file.dma_write(0, buffer.get(), buffer.size());
      });
    BOOST_CHECK_EQUAL(written, transfer_size);
}

} // namespace

SEASTAR_TEST_CASE(resource_manager_preserves_context_results_and_failures) {
    resource_registry registry;
    co_await registry.start(test_config());
    resource_manager manager{registry.handles()};

    BOOST_CHECK_THROW(
      static_cast<void>(manager.hard_budget(workload_class::metadata)),
      std::logic_error);
    bool prestart_work_rejected = false;
    try {
        co_await manager.with_workload_class(workload_class::repair, [] {});
    } catch (const std::logic_error&) {
        prestart_work_rejected = true;
    }
    BOOST_CHECK(prestart_work_rejected);
    co_await manager.start();
    BOOST_CHECK_EQUAL(manager.counters(workload_class::repair).rejected, 1U);

    for (const auto classification : all_workload_classes) {
        const auto expected = manager.scheduling_group(classification);
        const auto result = co_await manager.with_workload_class(
          classification, [expected] {
              BOOST_CHECK(seastar::current_scheduling_group() == expected);
              return 41;
          });
        BOOST_CHECK_EQUAL(result, 41);
    }

    const auto original = seastar::current_scheduling_group();
    const auto foreground = manager.scheduling_group(
      workload_class::foreground_protocol);
    const auto compaction = manager.scheduling_group(
      workload_class::compaction);
    co_await manager.with_workload_class(
      workload_class::foreground_protocol,
      [&manager, foreground, compaction] -> seastar::future<> {
          BOOST_CHECK(seastar::current_scheduling_group() == foreground);
          co_await manager.with_workload_class(
            workload_class::compaction, [compaction] {
                BOOST_CHECK(seastar::current_scheduling_group() == compaction);
            });
          BOOST_CHECK(seastar::current_scheduling_group() == foreground);
      });
    BOOST_CHECK(seastar::current_scheduling_group() == original);

    bool observed_failure = false;
    try {
        co_await manager.with_workload_class(
          workload_class::maintenance,
          [] -> int { throw std::runtime_error("synthetic failure"); });
    } catch (const std::runtime_error&) {
        observed_failure = true;
    }
    BOOST_CHECK(observed_failure);
    const auto maintenance = manager.counters(workload_class::maintenance);
    BOOST_CHECK_EQUAL(maintenance.completed, 1U);
    BOOST_CHECK_EQUAL(maintenance.failed, 1U);
    BOOST_CHECK_EQUAL(maintenance.queued, 0U);
    BOOST_CHECK_EQUAL(maintenance.executing, 0U);

    bool dma_submission_observed_group = false;
    co_await seastar::tmp_file::do_with(
      [&manager, &dma_submission_observed_group](seastar::tmp_file& temporary) {
          return verify_dma_scheduling_group(
            manager, dma_submission_observed_group, temporary);
      });
    BOOST_CHECK(dma_submission_observed_group);

    const auto budget = manager.hard_budget(workload_class::metadata);
    BOOST_REQUIRE(
      manager.reserve_bytes(workload_class::metadata, budget).has_value());
    const auto rejected = manager.reserve_bytes(
      workload_class::metadata, byte_count{1});
    BOOST_REQUIRE(!rejected.has_value());
    BOOST_CHECK_EQUAL(
      rejected.error(), make_error_code(errc::resource_exhausted));
    manager.record_reclaim_attempt(workload_class::metadata);
    auto metadata = manager.counters(workload_class::metadata);
    BOOST_CHECK_EQUAL(metadata.bytes_reserved, budget.value());
    BOOST_CHECK_EQUAL(metadata.reclaim_attempts, 1U);
    BOOST_CHECK_EQUAL(metadata.rejected, 1U);
    manager.release_bytes(workload_class::metadata, budget);
    metadata = manager.counters(workload_class::metadata);
    BOOST_CHECK_EQUAL(metadata.bytes_reserved, 0U);

    resource_manager competing{registry.handles()};
    bool competing_rejected = false;
    try {
        co_await competing.start();
    } catch (const std::logic_error&) {
        competing_rejected = true;
    }
    BOOST_CHECK(competing_rejected);
    co_await competing.stop();

    co_await manager.stop();
    BOOST_CHECK_THROW(
      static_cast<void>(manager.scheduling_group(workload_class::metadata)),
      std::logic_error);
    co_await registry.stop();
}

SEASTAR_TEST_CASE(resource_manager_shutdown_rejects_and_drains_pending_work) {
    resource_registry registry;
    co_await registry.start(test_config());
    resource_manager manager{registry.handles()};
    co_await manager.start();

    seastar::promise<> release;
    seastar::promise<> started;
    auto started_wait = started.get_future();
    auto pending = manager.with_workload_class(
      workload_class::replication,
      [waiting = release.get_future(), &started] mutable -> seastar::future<> {
          started.set_value();
          co_await std::move(waiting);
      });
    auto replication = manager.counters(workload_class::replication);
    BOOST_CHECK_EQUAL(replication.queued, 1U);
    BOOST_CHECK_EQUAL(replication.executing, 0U);
    co_await std::move(started_wait);
    replication = manager.counters(workload_class::replication);
    BOOST_CHECK_EQUAL(replication.queued, 0U);
    BOOST_CHECK_EQUAL(replication.executing, 1U);

    auto stopping = manager.stop();
    co_await seastar::yield();
    BOOST_CHECK(!stopping.available());
    BOOST_CHECK(manager.state() == resource_manager_state::stopping);

    bool rejected = false;
    try {
        co_await manager.with_workload_class(
          workload_class::replication, [] {});
    } catch (const std::logic_error&) {
        rejected = true;
    }
    BOOST_CHECK(rejected);

    release.set_value();
    co_await std::move(pending);
    co_await std::move(stopping);
    co_await manager.stop();
    co_await registry.stop();
}

SEASTAR_TEST_CASE(resource_managers_observe_one_local_owner_per_shard) {
    for (unsigned cycle = 0; cycle < 2; ++cycle) {
        resource_registry registry;
        co_await registry.start(test_config());
        const auto handles = registry.handles();
        runtime::sharded_service<resource_manager> managers{
          handles.smp_service_group(workload_class::metadata)};

        co_await managers.start(handles);
        co_await managers.invoke_on_all(&verify_local_manager);
        co_await managers.stop();
        co_await registry.stop();
    }
}

SEASTAR_TEST_CASE(workload_smp_group_bounds_nonlocal_execution) {
    BOOST_REQUIRE_GE(seastar::this_smp_shard_count(), 2U);
    resource_registry registry;
    co_await registry.start(test_config());
    resource_manager manager{registry.handles()};
    co_await manager.start();
    const auto& descriptor = descriptor_for(workload_class::maintenance);
    const auto admission_limit = manager.smp_admission_limit(
      workload_class::maintenance);
    BOOST_CHECK_EQUAL(admission_limit, descriptor.max_nonlocal_requests);

    co_await seastar::smp::submit_to(1, &reset_remote_saturation);
    const unsigned request_count = admission_limit + 8;
    std::vector<seastar::future<runtime::result<void>>> admitted_requests;
    admitted_requests.reserve(admission_limit);
    unsigned rejected_requests = 0;
    for (unsigned request = 0; request < request_count; ++request) {
        auto submission = manager.try_with_smp_service_group(
          workload_class::maintenance,
          [](seastar::smp_service_group service_group) {
              return seastar::smp::submit_to(
                1,
                seastar::smp_submit_to_options{service_group},
                &hold_remote_request);
          });
        if (submission.available()) {
            auto outcome = submission.get();
            BOOST_REQUIRE(!outcome.has_value());
            BOOST_CHECK(
              outcome.error().code() == kwaque::errc::resource_exhausted);
            ++rejected_requests;
        } else {
            admitted_requests.push_back(std::move(submission));
        }
    }
    BOOST_CHECK_EQUAL(admitted_requests.size(), admission_limit);
    BOOST_CHECK_EQUAL(rejected_requests, request_count - admission_limit);

    unsigned active = 0;
    for (unsigned attempt = 0; attempt < 256; ++attempt) {
        active = co_await seastar::smp::submit_to(1, &remote_active_requests);
        if (active == admission_limit) {
            break;
        }
        co_await seastar::yield();
    }
    BOOST_CHECK_EQUAL(active, admission_limit);
    BOOST_CHECK_LT(active, request_count);

    co_await seastar::smp::submit_to(1, &release_remote_requests);
    for (auto& request : admitted_requests) {
        auto outcome = co_await std::move(request);
        BOOST_REQUIRE(outcome.has_value());
    }
    const auto maximum = co_await seastar::smp::submit_to(
      1, &remote_maximum_requests);
    BOOST_CHECK_LE(maximum, admission_limit);
    BOOST_CHECK_EQUAL(
      manager.counters(workload_class::maintenance).rejected,
      rejected_requests);
    co_await manager.stop();
    co_await registry.stop();
}

SEASTAR_TEST_CASE(foreground_progresses_while_background_remains_runnable) {
    resource_registry registry;
    co_await registry.start(test_config());
    resource_manager manager{registry.handles()};
    co_await manager.start();

    constexpr std::size_t iteration_limit = 10000;
    bool foreground_completed = false;
    std::size_t background_iterations = 0;
    auto background = manager.with_workload_class(
      workload_class::compaction,
      [&foreground_completed, &background_iterations] -> seastar::future<> {
          while (!foreground_completed
                 && background_iterations < iteration_limit) {
              ++background_iterations;
              co_await seastar::yield();
          }
      });

    co_await seastar::yield();
    co_await manager.with_workload_class(
      workload_class::foreground_protocol,
      [&foreground_completed] { foreground_completed = true; });
    co_await std::move(background);

    BOOST_CHECK(foreground_completed);
    BOOST_CHECK_GT(background_iterations, 0U);
    BOOST_CHECK_LT(background_iterations, iteration_limit);
    co_await manager.stop();
    co_await registry.stop();
}

} // namespace kwaque::resource
