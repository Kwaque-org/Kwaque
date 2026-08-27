#include "src/base/units.h"
#include "src/resource/resource_manager.h"
#include "src/resource/resource_registry.h"
#include "src/resource/workload_class.h"
#include "src/runtime/cross_shard.h"
#include "src/runtime/sharded_service.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/later.hh>
#include <seastar/util/tmp_file.hh>

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
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
        auto workload = manager.acquire_workload(classification);
        const auto group = workload.scheduling_group();
        co_await seastar::with_scheduling_group(group, [group] {
            if (seastar::current_scheduling_group() != group) {
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

struct remote_staged_state final {
    seastar::gate work;
    seastar::shared_promise<> release;
    unsigned dispatched{0};
    unsigned completed{0};
};

thread_local std::unique_ptr<remote_staged_state> remote_staged;

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

void reset_remote_staged() {
    remote_staged = std::make_unique<remote_staged_state>();
}

seastar::future<> complete_remote_stage(remote_staged_state& state) {
    co_await state.release.get_shared_future();
    ++state.completed;
}

void dispatch_remote_stage() {
    auto& state = *remote_staged;
    ++state.dispatched;
    auto background = seastar::with_gate(
      state.work, [&state] { return complete_remote_stage(state); });
    static_cast<void>(background.handle_exception([](std::exception_ptr) {}));
}

std::array<unsigned, 2> remote_stage_counts() {
    return {remote_staged->dispatched, remote_staged->completed};
}

seastar::future<unsigned> release_and_wait_remote_stages() {
    remote_staged->release.set_value();
    co_await remote_staged->work.close();
    co_return remote_staged->completed;
}

seastar::future<> verify_dma_scheduling_group(
  resource_manager& manager,
  bool& observed_group,
  seastar::tmp_file& temporary) {
    auto& file = temporary.get_file();
    const auto transfer_size = file.memory_dma_alignment();
    auto buffer = seastar::temporary_buffer<char>::aligned(
      transfer_size, transfer_size);
    std::memset(buffer.get_write(), 0x5a, buffer.size());
    auto workload = manager.acquire_workload(workload_class::offload);
    const auto group = workload.scheduling_group();
    const auto written = co_await seastar::with_scheduling_group(
      group,
      [&file, &observed_group, group, buffer = std::move(buffer)] mutable {
          observed_group = seastar::current_scheduling_group() == group;
          return file.dma_write(0, buffer.get(), buffer.size());
      });
    BOOST_CHECK_EQUAL(written, transfer_size);
}

seastar::future<> verify_nested_scheduling_groups(
  seastar::scheduling_group foreground, seastar::scheduling_group compaction) {
    BOOST_CHECK(seastar::current_scheduling_group() == foreground);
    co_await seastar::with_scheduling_group(foreground, [foreground] {
        BOOST_CHECK(seastar::current_scheduling_group() == foreground);
    });
    co_await seastar::with_scheduling_group(compaction, [compaction] {
        BOOST_CHECK(seastar::current_scheduling_group() == compaction);
    });
    BOOST_CHECK(seastar::current_scheduling_group() == foreground);
}

seastar::future<> run_background_until_signal(
  bool& signaled,
  std::size_t& background_iterations,
  seastar::promise<>& started) {
    started.set_value();
    while (!signaled) {
        ++background_iterations;
        co_await seastar::yield();
    }
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
        static_cast<void>(manager.acquire_workload(workload_class::repair));
    } catch (const std::logic_error&) {
        prestart_work_rejected = true;
    }
    BOOST_CHECK(prestart_work_rejected);
    co_await manager.start();

    const auto local_owner = runtime::owner_shard{};
    {
        auto metadata = manager.acquire_workload(workload_class::metadata);
        const auto local_submission = co_await runtime::invoke_on_owner(
          local_owner, metadata.smp_service_group(), [] {
              return runtime::owner_shard{};
          });
        BOOST_CHECK(local_submission == local_owner);
    }

    for (const auto classification : all_workload_classes) {
        auto workload = manager.acquire_workload(classification);
        const auto group = workload.scheduling_group();
        const auto result = co_await seastar::with_scheduling_group(
          group, [group] {
              BOOST_CHECK(seastar::current_scheduling_group() == group);
              return 41;
          });
        BOOST_CHECK_EQUAL(result, 41);
    }

    const auto original = seastar::current_scheduling_group();
    {
        auto foreground = manager.acquire_workload(
          workload_class::foreground_protocol);
        auto compaction = manager.acquire_workload(workload_class::compaction);
        const auto foreground_group = foreground.scheduling_group();
        const auto compaction_group = compaction.scheduling_group();
        co_await seastar::with_scheduling_group(
          foreground_group, [foreground_group, compaction_group] {
              return verify_nested_scheduling_groups(
                foreground_group, compaction_group);
          });
    }
    BOOST_CHECK(seastar::current_scheduling_group() == original);

    bool observed_failure = false;
    try {
        auto maintenance = manager.acquire_workload(
          workload_class::maintenance);
        const auto maintenance_group = maintenance.scheduling_group();
        co_await seastar::with_scheduling_group(maintenance_group, [] -> int {
            throw std::runtime_error("synthetic failure");
        });
    } catch (const std::runtime_error&) {
        observed_failure = true;
    }
    BOOST_CHECK(observed_failure);

    bool observed_asynchronous_failure = false;
    try {
        auto maintenance = manager.acquire_workload(
          workload_class::maintenance);
        const auto maintenance_group = maintenance.scheduling_group();
        co_await seastar::with_scheduling_group(maintenance_group, [] {
            return seastar::yield().then(
              [] { throw std::runtime_error("asynchronous failure"); });
        });
    } catch (const std::runtime_error&) {
        observed_asynchronous_failure = true;
    }
    BOOST_CHECK(observed_asynchronous_failure);

    bool dma_submission_observed_group = false;
    co_await seastar::tmp_file::do_with(
      [&manager, &dma_submission_observed_group](seastar::tmp_file& temporary) {
          return verify_dma_scheduling_group(
            manager, dma_submission_observed_group, temporary);
      });
    BOOST_CHECK(dma_submission_observed_group);

    std::optional<workload_handle> memory_workload{
      manager.acquire_workload(workload_class::metadata)};
    const auto budget = manager.hard_budget(workload_class::metadata);
    auto reservation = seastar::try_get_units(
      memory_workload->memory_admission(), budget.value());
    BOOST_REQUIRE(reservation.has_value());
    auto split_reservation = reservation->split(budget.value());
    BOOST_CHECK_EQUAL(reservation->count(), 0U);
    const auto rejected = seastar::try_get_units(
      memory_workload->memory_admission(), 1);
    BOOST_REQUIRE(!rejected.has_value());
    BOOST_CHECK_EQUAL(
      manager.memory_used(workload_class::metadata).value(), budget.value());
    BOOST_CHECK_EQUAL(
      manager.memory_available(workload_class::metadata).value(), 0U);
    split_reservation.return_all();
    BOOST_CHECK_EQUAL(
      manager.memory_used(workload_class::metadata).value(), 0U);
    memory_workload.reset();

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
      static_cast<void>(manager.acquire_workload(workload_class::metadata)),
      std::logic_error);
    co_await registry.stop();
}

SEASTAR_TEST_CASE(
  resource_manager_shutdown_rejects_new_and_drains_live_leases) {
    resource_registry registry;
    co_await registry.start(test_config());
    resource_manager manager{registry.handles()};
    co_await manager.start();

    std::optional<workload_handle> workload{
      manager.acquire_workload(workload_class::replication)};
    auto reservation = seastar::try_get_units(workload->memory_admission(), 1);
    BOOST_REQUIRE(reservation.has_value());

    auto stopping = manager.stop();
    co_await seastar::yield();
    BOOST_CHECK(!stopping.available());
    BOOST_CHECK(manager.state() == resource_manager_state::stopping);

    bool rejected = false;
    try {
        static_cast<void>(
          manager.acquire_workload(workload_class::replication));
    } catch (const std::logic_error&) {
        rejected = true;
    }
    BOOST_CHECK(rejected);

    reservation->return_all();
    co_await seastar::yield();
    BOOST_CHECK(!stopping.available());
    workload.reset();
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
          seastar::default_smp_service_group()};

        co_await managers.start(handles);
        co_await managers.invoke_on_all(&verify_local_manager);
        co_await managers.stop();
        co_await registry.stop();
    }
}

SEASTAR_TEST_CASE(workload_smp_group_backpressures_nonlocal_execution) {
    BOOST_REQUIRE_GE(seastar::this_smp_shard_count(), 2U);
    resource_registry registry;
    co_await registry.start(test_config());
    resource_manager manager{registry.handles()};
    co_await manager.start();
    const auto& descriptor = descriptor_for(workload_class::maintenance);
    const auto admission_limit = std::max(
      descriptor.max_nonlocal_requests, seastar::this_smp_shard_count() - 1);
    const auto per_link_limit = admission_limit
                                / (seastar::this_smp_shard_count() - 1);
    BOOST_REQUIRE_GT(per_link_limit, 0U);

    co_await seastar::smp::submit_to(1, &reset_remote_saturation);
    const auto target = co_await seastar::smp::submit_to(
      1, [] { return runtime::owner_shard{}; });
    const unsigned request_count = per_link_limit + 8;
    {
        auto workload = manager.acquire_workload(workload_class::maintenance);
        const auto smp_group = workload.smp_service_group();
        std::vector<seastar::future<>> requests;
        requests.reserve(request_count);
        for (unsigned request = 0; request < request_count; ++request) {
            requests.push_back(
              runtime::invoke_on_owner(
                target, smp_group, &hold_remote_request));
        }
        BOOST_CHECK_EQUAL(requests.size(), request_count);

        unsigned active = 0;
        for (unsigned attempt = 0; attempt < 256; ++attempt) {
            active = co_await seastar::smp::submit_to(
              1, &remote_active_requests);
            if (active == per_link_limit) {
                break;
            }
            co_await seastar::yield();
        }
        BOOST_CHECK_EQUAL(active, per_link_limit);
        BOOST_CHECK_LT(active, request_count);

        co_await seastar::smp::submit_to(1, &release_remote_requests);
        for (auto& request : requests) {
            co_await std::move(request);
        }
        const auto maximum = co_await seastar::smp::submit_to(
          1, &remote_maximum_requests);
        BOOST_CHECK_LE(maximum, per_link_limit);
    }
    co_await manager.stop();
    co_await registry.stop();
}

SEASTAR_TEST_CASE(workload_smp_group_releases_slots_after_staged_dispatch) {
    BOOST_REQUIRE_GE(seastar::this_smp_shard_count(), 2U);
    resource_registry registry;
    co_await registry.start(test_config());
    resource_manager manager{registry.handles()};
    co_await manager.start();

    const auto& descriptor = descriptor_for(workload_class::maintenance);
    const auto total_limit = std::max(
      descriptor.max_nonlocal_requests, seastar::this_smp_shard_count() - 1);
    const auto per_link_limit = total_limit
                                / (seastar::this_smp_shard_count() - 1);
    const unsigned request_count = per_link_limit + 8;
    co_await seastar::smp::submit_to(1, &reset_remote_staged);
    const auto target = co_await seastar::smp::submit_to(
      1, [] { return runtime::owner_shard{}; });

    {
        auto workload = manager.acquire_workload(workload_class::maintenance);
        std::vector<seastar::future<>> dispatches;
        dispatches.reserve(request_count);
        for (unsigned request = 0; request < request_count; ++request) {
            dispatches.push_back(
              runtime::invoke_on_owner(
                target, workload.smp_service_group(), &dispatch_remote_stage));
        }
        for (auto& dispatch : dispatches) {
            co_await std::move(dispatch);
        }

        const auto counts = co_await seastar::smp::submit_to(
          1, &remote_stage_counts);
        BOOST_CHECK_EQUAL(counts[0], request_count);
        BOOST_CHECK_EQUAL(counts[1], 0U);
    }

    const auto completed = co_await seastar::smp::submit_to(
      1, &release_and_wait_remote_stages);
    BOOST_CHECK_EQUAL(completed, request_count);
    co_await manager.stop();
    co_await registry.stop();
}

SEASTAR_TEST_CASE(consensus_critical_progresses_during_replication_load) {
    resource_registry registry;
    co_await registry.start(test_config());
    resource_manager manager{registry.handles()};
    co_await manager.start();

    bool critical_completed = false;
    std::size_t replication_iterations = 0;
    seastar::promise<> replication_started;
    auto replication_started_wait = replication_started.get_future();
    {
        auto replication = manager.acquire_workload(
          workload_class::replication);
        auto consensus = manager.acquire_workload(
          workload_class::consensus_critical);
        auto background = seastar::with_scheduling_group(
          replication.scheduling_group(),
          [&critical_completed, &replication_iterations, &replication_started] {
              return run_background_until_signal(
                critical_completed,
                replication_iterations,
                replication_started);
          });

        co_await std::move(replication_started_wait);
        co_await seastar::with_scheduling_group(
          consensus.scheduling_group(),
          [&critical_completed] { critical_completed = true; });
        co_await std::move(background);
    }

    BOOST_CHECK(critical_completed);
    BOOST_CHECK_GT(replication_iterations, 0U);
    co_await manager.stop();
    co_await registry.stop();
}

SEASTAR_TEST_CASE(foreground_progresses_while_background_remains_runnable) {
    resource_registry registry;
    co_await registry.start(test_config());
    resource_manager manager{registry.handles()};
    co_await manager.start();

    bool foreground_completed = false;
    std::size_t background_iterations = 0;
    seastar::promise<> background_started;
    auto background_started_wait = background_started.get_future();
    {
        auto background_work = manager.acquire_workload(
          workload_class::compaction);
        auto foreground_work = manager.acquire_workload(
          workload_class::foreground_protocol);
        const auto background_group = background_work.scheduling_group();
        const auto foreground_group = foreground_work.scheduling_group();
        auto background = seastar::with_scheduling_group(
          background_group,
          [&foreground_completed, &background_iterations, &background_started] {
              return run_background_until_signal(
                foreground_completed,
                background_iterations,
                background_started);
          });

        co_await std::move(background_started_wait);
        co_await seastar::with_scheduling_group(
          foreground_group,
          [&foreground_completed] { foreground_completed = true; });
        co_await std::move(background);
    }

    BOOST_CHECK(foreground_completed);
    BOOST_CHECK_GT(background_iterations, 0U);
    co_await manager.stop();
    co_await registry.stop();
}

} // namespace kwaque::resource
