#include "src/simulation/environment.h"
#include "src/simulation/fake_file_test_support.h"
#include "src/simulation/scheduler_driver.h"
#include "src/storage/tests/completion_contract.h"

#include <seastar/core/coroutine.hh>
#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <optional>
#include <string>

namespace {
using namespace kwaque;
using namespace kwaque::simulation;
using kwaque::storage::testing::take;
using consumer = storage::testing::completion_contract<environment>;

environment_config config(
  std::optional<runtime::fault_action> flush_failure = std::nullopt,
  std::uint32_t memory_alignment = 4096) {
    environment_config_values values;
    values.scheduler.pending_events = 256;
    values.scheduler.events_per_pump = 64;
    values.scheduler.total_events = 10000;
    values.trace.entries = 2048;
    values.trace.encoded_bytes = 512U * 1024U;
    values.event_log.entries = 32;
    values.event_log.encoded_bytes = 32U * 1024U;
    values.file.maximum_objects = 64;
    values.file.maximum_open_handles = 8;
    values.file.maximum_pending_operations = 8;
    values.file.maximum_pending_reads = 8;
    values.file.maximum_pending_writes = 8;
    values.file.memory_dma_alignment = memory_alignment;
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
    values.dns.maximum_name_bytes = byte_count{8U * 1024U};
    values.dns.stop_batch = 8;
    values.dns.query_limits.maximum_waiters = 8;
    values.maximum_fault_rules = 16;
    if (flush_failure) {
        const auto decision
          = *flush_failure == runtime::fault_action::drop_completion
              ? runtime::fault_decision::make_drop_completion()
              : take(
                  runtime::fault_decision::make_file_failure(
                    *flush_failure, runtime::file_failure_detail::device_io));
        values.fault_rules.push_back(take(
          fault_rule::make(
            take(fault_rule_id::make(1)),
            runtime::builtin_fault_point::file_flush,
            std::nullopt,
            take(runtime::fault_occurrence::make(1)),
            take(runtime::fault_occurrence::make(1)),
            fault_selector::once(),
            decision)));
    }
    return take(environment_config::make(std::move(values)));
}

seastar::future<> run_contract(
  std::optional<std::size_t> fail_at,
  std::optional<runtime::fault_action> flush_failure = std::nullopt,
  std::uint32_t memory_alignment = 4096) {
    auto target = take(
      environment::make(config(flush_failure, memory_alignment)));
    simulation::testing::scheduler_driver drive{target->event_scheduler()};
    co_await target->start();
    std::unique_ptr<consumer> storage;
    std::exception_ptr first;
    bool startup_failed = false;
    try {
        storage = std::make_unique<consumer>(
          *target,
          take(runtime::file_path::make("/kwaque/storage-completion")));
        try {
            co_await storage->start(*target, drive, fail_at);
        } catch (const std::runtime_error& error) {
            if (
              std::string_view{error.what()}
              != "injected storage startup failure")
                throw;
            startup_failed = true;
        }
        target->request_abort();
        if (!fail_at) storage->check_early_abort(*target);
        const auto stopped = co_await drive.lifecycle(storage->stop());
        const auto repeated = co_await drive.lifecycle(storage->stop());
        if (flush_failure) {
            BOOST_CHECK(!stopped && !repeated);
            if (!stopped && !repeated) {
                BOOST_CHECK(stopped.error() == repeated.error());
                BOOST_CHECK(
                  runtime::file_detail(stopped.error())
                  == runtime::file_failure_detail::device_io);
            }
        } else {
            take(stopped);
            take(repeated);
        }
        storage->check_stopped(!fail_at, !flush_failure);
        if (!fail_at && !flush_failure)
            co_await storage->verify_contents(drive);
        BOOST_CHECK_EQUAL(startup_failed, fail_at.has_value());
    } catch (...) {
        first = std::current_exception();
    }
    if (storage) {
        try {
            static_cast<void>(co_await drive.lifecycle(storage->stop()));
        } catch (...) {
            if (!first) first = std::current_exception();
        }
        storage.reset();
    }
    const auto snapshot = fake_file_test_access::snapshot(
      target->file_system());
    BOOST_CHECK(snapshot.has_value());
    if (snapshot) {
        BOOST_CHECK_EQUAL(snapshot->open_handles, 0U);
        BOOST_CHECK_EQUAL(snapshot->pending_operations, 0U);
    }
    co_await drive.lifecycle(target->stop());
    if (first) std::rethrow_exception(first);
}
} // namespace

SEASTAR_TEST_CASE(storage_completion_survives_environment_abort_and_pressure) {
    co_await run_contract(std::nullopt);
}

SEASTAR_TEST_CASE(storage_completion_accepts_sub_pointer_dma_alignment) {
    for (std::uint32_t alignment = 1; alignment < sizeof(void*); alignment *= 2)
        co_await run_contract(std::nullopt, std::nullopt, alignment);
}

SEASTAR_TEST_CASE(storage_completion_rolls_back_each_partial_start_boundary) {
    for (std::size_t point = 0; point < consumer::start_boundaries; ++point)
        co_await run_contract(point);
}

SEASTAR_TEST_CASE(
  storage_completion_preserves_failed_barrier_and_releases_owners) {
    for (const auto action :
         {runtime::fault_action::file_failure_before_effect,
          runtime::fault_action::file_failure_after_effect})
        co_await run_contract(std::nullopt, action);
}

SEASTAR_TEST_CASE(
  storage_completion_retains_resources_until_a_lost_barrier_is_joined) {
    auto target = take(
      environment::make(config(runtime::fault_action::drop_completion)));
    simulation::testing::scheduler_driver drive{target->event_scheduler()};
    co_await target->start();
    auto storage = std::make_unique<consumer>(
      *target, take(runtime::file_path::make("/kwaque/lost-completion")));
    std::exception_ptr first;
    try {
        co_await storage->start(*target, drive);
        auto parked = fake_file_test_access::wait_parked(
          target->file_system(), fake_submission_kind::flush, 1);
        target->request_abort();
        storage->check_early_abort(*target);
        auto stopping = storage->stop();
        co_await drive.lifecycle(std::move(parked));
        const auto held = take(
          fake_file_test_access::snapshot(target->file_system()));
        const bool pending = !stopping.available();
        const auto memory = target->resource_manager().memory_used(
          resource::workload_class::metadata);
        take(co_await drive.lifecycle(target->file_system().stop()));
        const auto stopped = co_await drive.lifecycle(std::move(stopping));
        storage->check_stopped(false);
        BOOST_CHECK(pending);
        BOOST_CHECK_EQUAL(held.open_handles, 1U);
        BOOST_CHECK_EQUAL(held.pending_operations, 1U);
        BOOST_CHECK(memory.value() > 0);
        BOOST_CHECK(!stopped && stopped.error().code() == errc::aborted);
    } catch (...) {
        first = std::current_exception();
    }
    // Stop the fake device before joining any parked native work on failure.
    try {
        static_cast<void>(
          co_await drive.lifecycle(target->file_system().stop()));
    } catch (...) {
        if (!first) first = std::current_exception();
    }
    try {
        static_cast<void>(co_await drive.lifecycle(storage->stop()));
    } catch (...) {
        if (!first) first = std::current_exception();
    }
    storage.reset();
    co_await drive.lifecycle(target->stop());
    if (first) std::rethrow_exception(first);
}

SEASTAR_TEST_CASE(
  storage_completion_trace_exhaustion_reports_failure_without_leaking_owners) {
    auto target = take(environment::make(config()));
    simulation::testing::scheduler_driver drive{target->event_scheduler()};
    co_await target->start();
    auto storage = std::make_unique<consumer>(
      *target, take(runtime::file_path::make("/kwaque/trace-capacity")));
    std::exception_ptr first;
    try {
        co_await storage->start(*target, drive);
        bool exhausted = false;
        const auto root = take(runtime::file_path::make("/kwaque"));
        for (unsigned operation = 0; operation < 4096 && !exhausted;
             ++operation) {
            const auto sampled = co_await drive.operation(
              target->file_system().space(root));
            exhausted = !sampled
                        && sampled.error().code() == errc::resource_exhausted;
            if (!sampled && !exhausted)
                throw std::runtime_error("unexpected trace-pressure failure");
        }
        target->request_abort();
        const auto stopped = co_await drive.lifecycle(storage->stop());
        const auto repeated = co_await drive.lifecycle(storage->stop());
        storage->check_stopped(false);
        const auto state = take(
          fake_file_test_access::snapshot(target->file_system()));
        BOOST_CHECK(exhausted);
        BOOST_CHECK(
          !stopped && stopped.error().code() == errc::resource_exhausted);
        BOOST_CHECK(
          !repeated && !stopped && repeated.error() == stopped.error());
        BOOST_CHECK_EQUAL(state.open_handles, 0U);
        BOOST_CHECK_EQUAL(state.pending_operations, 0U);
    } catch (...) {
        first = std::current_exception();
    }
    try {
        static_cast<void>(co_await drive.lifecycle(storage->stop()));
    } catch (...) {
        if (!first) first = std::current_exception();
    }
    storage.reset();
    co_await drive.lifecycle(target->stop());
    if (first) std::rethrow_exception(first);
}
