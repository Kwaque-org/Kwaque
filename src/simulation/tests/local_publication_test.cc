#include "src/bytes/test_allocation_profile.h"
#include "src/simulation/environment.h"
#include "src/simulation/fake_file_test_support.h"
#include "src/simulation/scheduler_driver.h"
#include "src/storage/tests/local_publication_contract.h"

#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

namespace {
using namespace kwaque;
using namespace kwaque::simulation;
using namespace kwaque::storage;
using namespace kwaque::storage::testing::publication_contract;
environment_config config(
  std::optional<runtime::builtin_fault_point> point = std::nullopt,
  runtime::fault_action action
  = runtime::fault_action::file_failure_before_effect) {
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
    values.dns.maximum_name_bytes = byte_count{8U * 1024U};
    values.dns.stop_batch = 8;
    values.dns.query_limits.maximum_waiters = 8;
    values.maximum_fault_rules = 16;
    if (point) {
        const auto decision
          = action == runtime::fault_action::drop_completion
              ? runtime::fault_decision::make_drop_completion()
            : action == runtime::fault_action::error
              ? runtime::fault_decision::make_error()
              : take(
                  runtime::fault_decision::make_file_failure(
                    action, runtime::file_failure_detail::device_io));
        values.fault_rules.push_back(take(
          fault_rule::make(
            take(fault_rule_id::make(1)),
            *point,
            std::nullopt,
            take(runtime::fault_occurrence::make(1)),
            take(runtime::fault_occurrence::make(1)),
            fault_selector::once(),
            decision)));
    }
    return take(environment_config::make(std::move(values)));
}

// Establish the external parent-chain precondition without consuming the
// fault occurrence reserved for the publication operation being tested.
void stabilize_root(fake_file_system& files) {
    take(
      fake_file_test_access::sync_directory(
        files, take(fake_file_test_access::resolve(files, "/kwaque"))));
}

template<typename Func>
seastar::future<>
with_environment(environment_config configuration, Func function) {
    auto target = take(environment::make(std::move(configuration)));
    simulation::testing::scheduler_driver drive{target->event_scheduler()};
    co_await target->start();
    std::exception_ptr first;
    try {
        workload_budget budget{
          target->resource_manager().acquire_workload(
            resource::workload_class::metadata),
          {.tasks = 4, .bytes = byte_count{2U * 1024U * 1024U}, .handles = 4},
          bytes::testing::charge};
        co_await function(*target, budget, drive);
        const auto snapshot = take(
          fake_file_test_access::snapshot(target->file_system()));
        BOOST_CHECK_EQUAL(snapshot.open_handles, 0U);
        BOOST_CHECK_EQUAL(snapshot.pending_operations, 0U);
        BOOST_CHECK_EQUAL(budget.snapshot().bytes, 0U);
    } catch (...) {
        first = std::current_exception();
    }
    co_await drive.lifecycle(target->stop());
    if (first) std::rethrow_exception(first);
}
} // namespace

SEASTAR_TEST_CASE(local_publication_and_bounded_discovery_use_fake_files) {
    co_await with_environment(
      config(),
      [](auto& environment, auto& budget, auto drive) -> seastar::future<> {
          auto root = take(runtime::file_path::make("/kwaque/store"));
          co_await kwaque::storage::testing::publication_contract::run(
            environment.file_system(), budget, root, drive);
          co_await walk(environment.file_system(), root, drive);
          for (unsigned restart = 0; restart < 2; ++restart) {
              take(co_await drive.lifecycle(environment.file_system().crash()));
              co_await check_contents(
                environment.file_system(),
                take(local_child_path(
                  root, take(runtime::file_name::make("control")))),
                drive,
                'b');
          }
      });
}

SEASTAR_TEST_CASE(
  local_publication_preserves_first_error_and_confirmed_barriers) {
    for (auto point :
         {runtime::builtin_fault_point::file_flush,
          runtime::builtin_fault_point::file_rename,
          runtime::builtin_fault_point::directory_sync}) {
        for (auto action :
             {runtime::fault_action::file_failure_before_effect,
              runtime::fault_action::file_failure_after_effect}) {
            co_await with_environment(
              config(point, action),
              [point](auto& environment, auto& budget, auto drive)
                -> seastar::future<> {
                  auto& files = environment.file_system();
                  auto root = take(
                    runtime::file_path::make("/kwaque/failed-publication"));
                  take(
                    co_await drive.lifecycle(files.create_directories(root)));
                  stabilize_root(files);
                  local_file_publisher<fake_file_system> publisher{
                    files,
                    budget,
                    {owner(),
                     root,
                     root,
                     take(runtime::file_name::make("control")),
                     runtime::file_rename_policy::no_replace,
                     {}}};
                  runtime::first_failure first;
                  try {
                      seastar::abort_source abort;
                      codec::cooperative_work work{
                        codec::limits::defaults(), abort};
                      auto result = co_await drive.lifecycle(publisher.publish(
                        {owner(), generation(1), {}}, payload('q'), work));
                      BOOST_REQUIRE(result.failure.error());
                      BOOST_CHECK(
                        runtime::file_detail(*result.failure.error())
                        == runtime::file_failure_detail::device_io);
                      BOOST_CHECK(publisher.fenced());
                      BOOST_CHECK(result.temporary_may_exist);
                      BOOST_CHECK(
                        result.disposition
                        == (point == runtime::builtin_fault_point::file_flush ? local_publication_disposition::untouched : local_publication_disposition::uncertain));
                      BOOST_CHECK(result.stage==(point==runtime::builtin_fault_point::file_flush
                      ? local_publication_stage::written : point==runtime::builtin_fault_point::file_rename
                      ? local_publication_stage::file_closed : local_publication_stage::renamed));
                  } catch (...) {
                      first.observe(std::current_exception());
                  }
                  take(co_await drive.lifecycle(publisher.close()));
                  take(first.outcome());
              });
        }
    }
}

SEASTAR_TEST_CASE(
  local_publication_parent_close_error_preserves_durable_outcome) {
    co_await with_environment(
      config(
        runtime::builtin_fault_point::directory_cursor_close,
        runtime::fault_action::error),
      [](auto& environment, auto& budget, auto drive) -> seastar::future<> {
          auto& files = environment.file_system();
          auto root = take(runtime::file_path::make("/kwaque/close-failure"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize_root(files);
          local_file_publisher<fake_file_system> publisher{
            files,
            budget,
            {owner(),
             root,
             root,
             take(runtime::file_name::make("control")),
             runtime::file_rename_policy::no_replace,
             {}}};
          runtime::first_failure first;
          try {
              seastar::abort_source abort;
              codec::cooperative_work work{codec::limits::defaults(), abort};
              auto result = co_await drive.lifecycle(publisher.publish(
                {owner(), generation(1), {}}, payload('d'), work));
              BOOST_REQUIRE(result.failure.error());
              BOOST_CHECK(
                result.failure.error()->code() == errc::fault_injected);
              BOOST_CHECK(
                result.disposition == local_publication_disposition::durable);
              BOOST_CHECK(
                result.stage == local_publication_stage::directory_synced);
              BOOST_CHECK(publisher.fenced());
          } catch (...) {
              first.observe(std::current_exception());
          }
          take(co_await drive.lifecycle(publisher.close()));
          take(first.outcome());
          take(co_await drive.lifecycle(files.crash()));
          co_await check_contents(
            files,
            take(local_child_path(
              root, take(runtime::file_name::make("control")))),
            drive,
            'd');
      });
}

SEASTAR_TEST_CASE(local_publication_joins_accepted_barrier_after_abort) {
    co_await with_environment(
      config(
        runtime::builtin_fault_point::file_flush,
        runtime::fault_action::drop_completion),
      [](auto& environment, auto& budget, auto drive) -> seastar::future<> {
          auto& files = environment.file_system();
          auto root = take(
            runtime::file_path::make("/kwaque/pending-publication"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize_root(files);
          local_file_publisher<fake_file_system> publisher{
            files,
            budget,
            {owner(),
             root,
             root,
             take(runtime::file_name::make("control")),
             runtime::file_rename_policy::no_replace,
             {}}};
          seastar::abort_source abort;
          codec::cooperative_work work{codec::limits::defaults(), abort};
          auto pending = publisher.publish(
            {owner(), generation(1), {}}, payload('p'), work);
          bool consumed = false;
          std::optional<seastar::future<runtime::result<void>>> closing;
          runtime::first_failure first;
          try {
              co_await drive.lifecycle(
                fake_file_test_access::wait_parked(
                  files, fake_submission_kind::flush, 1));
              abort.request_abort();
              environment.request_abort();
              closing.emplace(publisher.close());
              BOOST_CHECK(!pending.available());
              BOOST_CHECK(!closing->available());
              BOOST_CHECK_EQUAL(budget.snapshot().handles, 2U);
              take(co_await drive.lifecycle(files.stop()));
              consumed = true;
              auto result = co_await drive.lifecycle(std::move(pending));
              auto stopped = co_await drive.lifecycle(std::move(*closing));
              closing.reset();
              take(stopped);
              BOOST_CHECK(result.failure.failed());
              BOOST_CHECK(result.stage == local_publication_stage::written);
              BOOST_CHECK(
                result.disposition == local_publication_disposition::untouched);
          } catch (...) {
              first.observe(std::current_exception());
          }
          static_cast<void>(co_await drive.lifecycle(files.stop()));
          // The earlier path marks consumed before transferring the future.
          if (!consumed)
              // NOLINTNEXTLINE(bugprone-use-after-move)
              static_cast<void>(co_await drive.lifecycle(std::move(pending)));
          if (closing)
              static_cast<void>(co_await drive.lifecycle(std::move(*closing)));
          static_cast<void>(co_await drive.lifecycle(publisher.close()));
          take(first.outcome());
      });
}
