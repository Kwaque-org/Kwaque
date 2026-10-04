#include "src/base/units.h"
#include "src/runtime/testing/reactor_tasks.h"
#include "src/simulation/environment.h"
#include "src/simulation/fake_file_test_support.h"
#include "src/simulation/scheduler_driver.h"
#include "src/simulation/virtual_time.h"
#include "src/storage/tests/local_append_contract.h"
#include "src/storage/tests/local_store_contract.h"
#include "src/storage/tests/recovery_successor_contract.h"

#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <optional>
#include <type_traits>
#include <utility>

namespace {
using namespace kwaque;
using namespace kwaque::simulation;
using namespace kwaque::storage;
using namespace kwaque::storage::testing::store_contract;
namespace contract = kwaque::storage::testing::local_append_contract;

environment_config config(std::vector<fault_rule> faults = {}) {
    environment_config_values values;
    values.resource_total_memory = byte_count{256_MiB};
    values.scheduler.pending_events = 256;
    values.scheduler.events_per_pump = 64;
    values.scheduler.total_events = 100000;
    values.trace.entries = 32768;
    values.trace.encoded_bytes = 8_MiB;
    values.event_log.entries = 32;
    values.event_log.encoded_bytes = 32_KiB;
    values.file.maximum_objects = 256;
    values.file.maximum_open_handles = 16;
    values.file.native_max_length = static_cast<std::uint32_t>(
      maximum_contiguous_allocation_bytes);
    // The WAL and segment writers run their physical windows together.
    values.file.maximum_pending_operations = 32;
    values.file.maximum_pending_reads = 8;
    values.file.maximum_pending_writes = 16;
    values.file.memory_dma_alignment = 4096;
    values.network.maximum_listeners = 2;
    values.network.maximum_connection_pairs = 2;
    values.network.maximum_pending_connects = 2;
    values.network.maximum_backlog_entries = 4;
    values.network.maximum_operations = 8;
    values.network.maximum_parked_operations = 4;
    values.network.maximum_packets = 16;
    values.network.maximum_direction_packets = 8;
    values.network.maximum_links = 8;
    values.network.maximum_address_entries = 8;
    values.network.maximum_active_flows = 4;
    values.network.maximum_controls = 8;
    values.network.stop_batch = 8;
    values.dns.maximum_records = 16;
    values.dns.maximum_answers = 32;
    values.dns.maximum_name_bytes = byte_count{8_KiB};
    values.dns.stop_batch = 8;
    values.dns.query_limits.maximum_waiters = 8;
    values.maximum_fault_rules = 16;
    for (auto& fault : faults)
        values.fault_rules.push_back(std::move(fault));
    return take(environment_config::make(std::move(values)));
}

// Drain native continuations before returning or advancing virtual time, so
// acceptance work never loses a race against simulated I/O completion. A
// future that is ready on entry still drains first: native preemption is
// timed, so an acceptance may complete inside the call, before the group
// formation it started.
struct ordered_driver final {
    scheduler& events;
    template<typename T>
    seastar::future<T> lifecycle(seastar::future<T> waiting) const {
        for (unsigned steps = 0;; ++steps) {
            co_await runtime::testing::drain_reactor_tasks();
            if (waiting.available()) break;
            if (steps == 100000 || events.pending_events() == 0)
                throw simulation::testing::scheduler_liveness_error{};
            simulation::testing::scheduler_driver_detail::run_next_batch(
              events, 1);
        }
        if constexpr (std::is_void_v<T>) {
            co_await std::move(waiting);
            co_return;
        } else {
            co_return co_await std::move(waiting);
        }
    }
};

template<typename Func>
seastar::future<>
with_local_store(Func function, std::vector<fault_rule> faults = {}) {
    auto target = take(environment::make(config(std::move(faults))));
    ordered_driver drive{target->event_scheduler()};
    co_await target->start();
    std::exception_ptr first;
    try {
        workload_budget budget{
          target->resource_manager().acquire_workload(
            resource::workload_class::foreground_protocol),
          {.tasks = 128, .bytes = byte_count{32_MiB}, .handles = 32},
          bytes::testing::charge};
        auto& files = target->file_system();
        const auto root = take(runtime::file_path::make("/kwaque/store"));
        take(co_await drive.lifecycle(files.create_directories(root)));
        take(
          fake_file_test_access::sync_directory(
            files, take(fake_file_test_access::resolve(files, "/kwaque"))));
        const auto spec = specification(root, {1, 1}, 68);
        const std::array specs{spec};
        ownership_input owner{specs};
        co_await function(files, owner, spec, budget, drive, target->timer());
        BOOST_CHECK_EQUAL(
          fake_file_test_access::open_handles(target->file_system()), 0U);
        BOOST_CHECK_EQUAL(target->file_system().pending_operations(), 0U);
        BOOST_CHECK_EQUAL(budget.snapshot().tasks, 0U);
        BOOST_CHECK_EQUAL(budget.snapshot().bytes, 0U);
    } catch (...) {
        first = std::current_exception();
    }
    co_await drive.lifecycle(target->stop());
    if (first) std::rethrow_exception(first);
}

// A file of the fixture and the flushes it has had once the fixture is
// attached. Creation order is deterministic, so a second environment with
// the same segments reproduces both.
struct flush_target final {
    std::uint64_t object{0};
    std::uint64_t flushes{0};
};
struct flush_targets final {
    flush_target wal;
    std::array<flush_target, 2> segments;
};
seastar::future<flush_targets> calibrate(std::uint32_t segments) {
    flush_targets found;
    co_await with_local_store(
      [&found, segments](
        auto& files,
        auto& owner,
        const auto& spec,
        auto& budget,
        auto drive,
        auto& timer) -> seastar::future<> {
          co_await contract::with_local_append<simulation::monotonic_clock>(
            files,
            owner,
            spec,
            budget,
            drive,
            timer,
            {.outstanding = 1, .segments = segments},
            [&](auto&, auto& writer, auto&, auto&, auto&) -> seastar::future<> {
                const auto target = [&](const runtime::file_path& file) {
                    const auto object = take(
                      fake_file_test_access::lookup(
                        files,
                        take(
                          fake_file_test_access::resolve(
                            files, file.value()))));
                    return flush_target{
                      object.value(),
                      take(
                        fake_file_test_access::occurrences(
                          files,
                          object,
                          runtime::builtin_fault_point::file_flush))};
                };
                found.wal = target(
                  contract::wal_path(
                    spec, writer.prepared_head()->incarnation));
                for (std::uint32_t i = 0; i != segments; ++i)
                    found.segments[i] = target(
                      contract::segment_path(spec, i + 1U));
                co_return;
            });
      });
    co_return found;
}

// The flush'th next flush of a calibrated file, with one decision.
fault_rule flush_fault(
  std::uint64_t id,
  flush_target target,
  std::uint64_t flush,
  runtime::fault_decision decision) {
    const auto occurrence = take(
      runtime::fault_occurrence::make(target.flushes + flush));
    return take(
      fault_rule::make(
        take(fault_rule_id::make(id)),
        runtime::builtin_fault_point::file_flush,
        runtime::fault_object_key::from_u64(target.object),
        occurrence,
        occurrence,
        fault_selector::once(),
        decision));
}
runtime::fault_decision failed_flush(runtime::fault_action when) {
    return take(
      runtime::fault_decision::make_file_failure(
        when, runtime::file_failure_detail::device_io));
}
runtime::fault_decision slow_flush() {
    return runtime::fault_decision::make_delay(
      runtime::monotonic_duration{1'000'000'000});
}

// The flushes the fake counted for one file.
struct flush_count final {
    fake_file_system* files;
    std::optional<std::uint64_t>
    operator()(const runtime::file_path& path) const {
        const auto object = take(
          fake_file_test_access::lookup(
            *files,
            take(fake_file_test_access::resolve(*files, path.value()))));
        return take(
          fake_file_test_access::occurrences(
            *files, object, runtime::builtin_fault_point::file_flush));
    }
};

} // namespace

SEASTAR_TEST_CASE(local_append_fake_durable_append) {
    co_await with_local_store([](auto&&... shared) {
        return contract::durable_append<simulation::monotonic_clock>(shared...);
    });
}

SEASTAR_TEST_CASE(local_append_fake_independent_alignments) {
    co_await with_local_store([](auto&&... shared) {
        return contract::durable_append<simulation::monotonic_clock>(
          shared..., 4096);
    });
}

SEASTAR_TEST_CASE(local_append_fake_occupancy_batching) {
    co_await with_local_store([](auto&&... shared) {
        return contract::occupancy_batching<simulation::monotonic_clock>(
          shared..., true);
    });
}

SEASTAR_TEST_CASE(local_append_fake_multi_segment_group) {
    co_await with_local_store([](auto&&... shared) {
        return contract::multi_segment_group<simulation::monotonic_clock>(
          shared...);
    });
}

SEASTAR_TEST_CASE(local_append_fake_rejections_have_no_effect) {
    co_await with_local_store([](auto&&... shared) {
        return contract::rejections_have_no_effect<simulation::monotonic_clock>(
          shared...);
    });
}

SEASTAR_TEST_CASE(local_append_fake_pressure_before_freeze) {
    co_await with_local_store([](auto&&... shared) {
        return contract::pressure_before_freeze<simulation::monotonic_clock>(
          shared...);
    });
}

SEASTAR_TEST_CASE(local_append_fake_rotation_before_freeze) {
    co_await with_local_store([](auto&&... shared) {
        return contract::rotation_before_freeze<simulation::monotonic_clock>(
          shared...);
    });
}

SEASTAR_TEST_CASE(local_recovered_successor_fake) {
    co_await with_local_store([](auto& files, auto&&... shared) {
        return storage::testing::recovery_successor_contract::
          recovered_successor<simulation::monotonic_clock>(
            files, shared..., flush_count{&files});
    });
}

SEASTAR_TEST_CASE(local_recovery_repeated_restarts_fake) {
    co_await with_local_store([](auto&&... shared) {
        return storage::testing::recovery_successor_contract::repeated_restarts<
          simulation::monotonic_clock>(shared...);
    });
}

SEASTAR_TEST_CASE(local_append_fake_close_drains_accepted) {
    co_await with_local_store([](auto&&... shared) {
        return contract::close_drains_accepted<simulation::monotonic_clock>(
          shared...);
    });
}

SEASTAR_TEST_CASE(local_append_fake_barrier_coalescing) {
    co_await with_local_store([](auto&&... shared) {
        return contract::barrier_coalescing<simulation::monotonic_clock>(
          shared...);
    });
}

SEASTAR_TEST_CASE(local_append_fake_independent_segment_outcomes) {
    const auto targets = co_await calibrate(2);
    BOOST_REQUIRE_NE(targets.segments[1].object, 0U);
    co_await with_local_store(
      [](auto&&... shared) {
          return contract::independent_segment_outcomes<
            simulation::monotonic_clock>(shared...);
      },
      {flush_fault(
        1,
        targets.segments[1],
        1,
        failed_flush(runtime::fault_action::file_failure_before_effect))});
}

SEASTAR_TEST_CASE(local_append_fake_interest_ends_without_cancelling) {
    co_await with_local_store([](auto&&... shared) {
        return contract::interest_ends_without_cancelling<
          simulation::monotonic_clock>(shared..., true);
    });
}

SEASTAR_TEST_CASE(local_append_fake_deadline_detaches_waiter) {
    co_await with_local_store([](auto&&... shared) {
        return contract::deadline_detaches_waiter<simulation::monotonic_clock>(
          shared...);
    });
}

SEASTAR_TEST_CASE(local_append_fake_completed_retry_facts) {
    co_await with_local_store([](auto&&... shared) {
        return contract::completed_retry_facts<simulation::monotonic_clock>(
          shared...);
    });
}

SEASTAR_TEST_CASE(local_append_fake_obligations_track_segments) {
    co_await with_local_store([](auto&&... shared) {
        return contract::obligations_track_segments<
          simulation::monotonic_clock>(shared...);
    });
}

SEASTAR_TEST_CASE(local_append_fake_shutdown_stops_admission) {
    co_await with_local_store([](auto&&... shared) {
        return contract::shutdown_stops_admission<simulation::monotonic_clock>(
          shared...);
    });
}

SEASTAR_TEST_CASE(local_append_fake_barrier_order) {
    const auto targets = co_await calibrate(1);
    for (const bool wal_slow : {true, false})
        co_await with_local_store(
          [wal_slow](auto&&... shared) {
              return contract::barrier_order<simulation::monotonic_clock>(
                shared..., wal_slow);
          },
          {flush_fault(
            1, wal_slow ? targets.wal : targets.segments[0], 1, slow_flush())});
}

SEASTAR_TEST_CASE(local_append_fake_reordered_completions) {
    const auto targets = co_await calibrate(2);
    co_await with_local_store(
      [](auto&&... shared) {
          return contract::reordered_completions<simulation::monotonic_clock>(
            shared...);
      },
      {flush_fault(1, targets.segments[0], 1, slow_flush())});
}

SEASTAR_TEST_CASE(local_append_fake_wal_failure_after_segment) {
    const auto targets = co_await calibrate(1);
    co_await with_local_store(
      [](auto&&... shared) {
          return contract::wal_failure_after_segment<
            simulation::monotonic_clock>(shared...);
      },
      {flush_fault(
        1,
        targets.wal,
        1,
        failed_flush(runtime::fault_action::file_failure_after_effect))});
}

SEASTAR_TEST_CASE(local_append_fake_crash_histories) {
    using history = contract::survival_history;
    const auto targets = co_await calibrate(1);
    for (const auto chosen :
         {history::both,
          history::wal_only,
          history::footer_only,
          history::neither,
          history::lost_wal_completion}) {
        std::vector<fault_rule> faults;
        if (chosen == history::wal_only || chosen == history::neither)
            faults.push_back(
              flush_fault(1, targets.segments[0], 1, slow_flush()));
        if (chosen == history::footer_only || chosen == history::neither)
            faults.push_back(flush_fault(2, targets.wal, 1, slow_flush()));
        if (chosen == history::lost_wal_completion)
            faults.push_back(flush_fault(
              2,
              targets.wal,
              1,
              runtime::fault_decision::make_drop_completion()));
        co_await with_local_store(
          [chosen](auto&&... shared) {
              return contract::crash_history<simulation::monotonic_clock>(
                shared..., chosen);
          },
          std::move(faults));
    }
}

SEASTAR_TEST_CASE(local_append_fake_concurrent_pipeline) {
    co_await with_local_store([](auto&&... shared) {
        return contract::concurrent_pipeline<simulation::monotonic_clock>(
          shared...);
    });
}

SEASTAR_TEST_CASE(local_append_fake_close_during_slow_flush) {
    const auto targets = co_await calibrate(1);
    co_await with_local_store(
      [](auto&&... shared) {
          return contract::close_during_slow_flush<simulation::monotonic_clock>(
            shared...);
      },
      {flush_fault(1, targets.wal, 1, slow_flush())});
}

#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION) && !defined(SEASTAR_DEBUG)
SEASTAR_TEST_CASE(local_append_fake_acceptance_allocation_cuts) {
    std::size_t reached = 0;
    for (std::size_t at = 0; at != 256; ++at) {
        bool injected = false;
        co_await with_local_store(
          [at, &injected](auto&&... shared) -> seastar::future<> {
              injected = co_await contract::acceptance_allocation_cut<
                simulation::monotonic_clock>(shared..., at);
          });
        if (!injected) break;
        ++reached;
    }
    BOOST_REQUIRE_GT(reached, 0U);
    BOOST_REQUIRE_LT(reached, 256U);
}
#endif

SEASTAR_TEST_CASE(local_append_fake_wal_stops_before_acceptance) {
    co_await with_local_store([](auto&&... shared) {
        return contract::wal_stops_before_acceptance<
          simulation::monotonic_clock>(shared...);
    });
}

#if !defined(SEASTAR_DEBUG)
SEASTAR_TEST_CASE(local_append_fake_close_before_group_runs) {
    co_await with_local_store([](auto&&... shared) {
        return contract::close_before_group_runs<simulation::monotonic_clock>(
          shared...);
    });
}
#endif

SEASTAR_TEST_CASE(local_append_fake_identity_and_rejections) {
    co_await with_local_store([](auto&&... shared) {
        return contract::identity_and_rejections<simulation::monotonic_clock>(
          shared...);
    });
}
