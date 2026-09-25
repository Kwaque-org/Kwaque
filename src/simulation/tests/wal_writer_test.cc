#include "src/codec/sha256.h"
#include "src/runtime/testing/reactor_tasks.h"
#include "src/simulation/environment.h"
#include "src/simulation/fake_file_test_support.h"
#include "src/simulation/scheduler_driver.h"
#include "src/simulation/virtual_time.h"
#include "src/storage/tests/local_installation_contract.h"
#include "src/storage/tests/wal_append_contract.h"
#include "src/storage/tests/wal_commit_lifecycle_contract.h"
#include "src/storage/tests/wal_commit_qualification_contract.h"
#include "src/storage/tests/wal_durability_contract.h"
#include "src/storage/tests/wal_group_commit_contract.h"
#include "src/storage/tests/wal_lifecycle_contract.h"
#include "src/storage/tests/wal_qualification_contract.h"
#include "src/storage/tests/wal_rotation_contract.h"
#include "src/storage/tests/wal_survival_oracle.h"
#include "src/storage/tests/wal_writer_contract.h"

#include <seastar/testing/test_case.hh>
#include <seastar/util/later.hh>

#include <boost/test/unit_test.hpp>

#include <bit>
#include <optional>
#include <utility>

namespace {
using namespace kwaque;
using namespace kwaque::simulation;
using namespace kwaque::storage;
using namespace kwaque::storage::testing::store_contract;
environment_config config(
  std::optional<runtime::builtin_fault_point> point = std::nullopt,
  runtime::fault_action action
  = runtime::fault_action::file_failure_before_effect,
  std::uint64_t occurrence = 1,
  std::optional<runtime::fault_object_key> object = std::nullopt,
  std::optional<fake_crash_policy> crash_policy = std::nullopt,
  std::optional<fault_rule> additional = std::nullopt,
  std::uint32_t memory_alignment = 4096,
  std::uint32_t native_max_length = 131072,
  std::uint32_t overwrite_alignment = 4096) {
    environment_config_values values;
    values.resource_total_memory = byte_count{256U * 1024U * 1024U};
    values.scheduler.pending_events = 256;
    values.scheduler.events_per_pump = 64;
    values.scheduler.total_events = 100000;
    values.trace.entries = 32768;
    values.trace.encoded_bytes = 8U * 1024U * 1024U;
    values.event_log.entries = 32;
    values.event_log.encoded_bytes = 32U * 1024U;
    values.file.crash_policy = crash_policy;
    values.file.maximum_objects = 256;
    values.file.maximum_open_handles = 16;
    values.file.maximum_pending_operations = 8;
    values.file.maximum_pending_reads = 8;
    values.file.maximum_pending_writes = 8;
    values.file.memory_dma_alignment = memory_alignment;
    values.file.native_max_length = native_max_length;
    values.file.disk_overwrite_dma_alignment = overwrite_alignment;
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
                    action,
                    runtime::file_failure_detail::device_io,
                    byte_count{
                      action == runtime::fault_action::file_failure_after_prefix
                        ? 512U
                        : 0U}));
        values.fault_rules.push_back(take(
          fault_rule::make(
            take(fault_rule_id::make(1)),
            *point,
            object,
            take(runtime::fault_occurrence::make(occurrence)),
            take(runtime::fault_occurrence::make(occurrence)),
            fault_selector::once(),
            decision)));
    }
    if (additional) values.fault_rules.push_back(std::move(*additional));
    return take(environment_config::make(std::move(values)));
}

template<typename Func>
seastar::future<> with_wal_environment(
  environment_config configuration, Func function, std::uint32_t tasks = 32) {
    auto target = take(environment::make(std::move(configuration)));
    simulation::testing::scheduler_driver drive{target->event_scheduler()};
    co_await target->start();
    std::exception_ptr first;
    try {
        workload_budget budget{
          target->resource_manager().acquire_workload(
            resource::workload_class::metadata),
          {.tasks = tasks,
           .bytes = byte_count{16U * 1024U * 1024U},
           .handles = 32},
          bytes::testing::charge};
        co_await function(*target, budget, drive);
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

} // namespace

SEASTAR_TEST_CASE(wal_writer_fake_bootstrap_and_exact_child_preparation) {
    co_await with_wal_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto spec = specification(
            take(runtime::file_path::make("/kwaque/store")), {1, 1});
          const std::array specs{spec};
          ownership_input owner{specs};
          take(co_await drive.lifecycle(files.create_directories(spec.root)));
          co_await storage::testing::wal_writer_contract::exercise(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(wal_writer_fake_construction_failure_joins_providers) {
    co_await with_wal_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto spec = specification(
            take(runtime::file_path::make("/kwaque/store")), {1, 1});
          const std::array specs{spec};
          ownership_input owner{specs};
          take(co_await drive.lifecycle(files.create_directories(spec.root)));
          co_await storage::testing::wal_writer_contract::
            construction_failure_cleanup(files, owner, spec, budget, drive);
      });
}

namespace {
struct fault_target final {
    std::uint64_t object{0}, occurrence{1};
};
struct bootstrap_targets final {
    fault_target header, head, header_directory_sync, header_directory_close,
      head_directory_sync, head_directory_close;
};

seastar::future<bootstrap_targets> bootstrap_history(
  environment& env,
  workload_budget& budget,
  simulation::testing::scheduler_driver drive,
  bool expect_failure,
  bool durable_header_error = false,
  bool durable_head_error = false,
  bool detach = false,
  bool close_while_pending = false,
  bool collision = false,
  bool geometry_failure = false) {
    auto& files = env.file_system();
    const auto spec = specification(
      take(runtime::file_path::make("/kwaque/store")), {1, 1});
    const std::array specs{spec};
    ownership_input owner{specs};
    take(co_await drive.lifecycle(files.create_directories(spec.root)));
    take(
      fake_file_test_access::sync_directory(
        files, take(fake_file_test_access::resolve(files, "/kwaque"))));
    seastar::abort_source admission_abort;
    codec::cooperative_work work{codec::limits::defaults(), admission_abort};
    co_await storage::testing::installation_contract::bootstrap(
      files, owner, spec, budget, work, drive);
    using writer_type = wal_writer<fake_file_system, ownership_input>;
    std::unique_ptr<writer_type::control_type> control;
    std::unique_ptr<writer_type::allocator_type> ids;
    std::unique_ptr<writer_type> writer;
    runtime::first_failure failed;
    bootstrap_targets targets;
    bool expected_close_error = expect_failure || close_while_pending
                                || collision;
    try {
        control = take(
          co_await drive.lifecycle(
            writer_type::control_type::open(
              files, owner, spec, 0, false, budget, limits(), work)));
        ids = take(writer_type::allocator_type::make(*control, budget, 4));
        auto writer_config
          = storage::testing::wal_writer_contract::configuration();
        if (geometry_failure)
            writer_config.alignment = storage::testing::alignment(4096);
        writer = take(
          writer_type::make(
            *control,
            *ids,
            budget,
            writer_config,
            wal_start_intent::known_unactivated));
        std::optional<runtime::file_path> collision_path;
        if (collision) {
            collision_path = take(
              take(local_paths::make(spec.root))
                .wal(
                  0,
                  local_wal_high{}.checked_advance(1)->incarnation().value()));
            auto parent = take(
              runtime::file_path::make(collision_path->value().substr(
                0, collision_path->value().rfind('/'))));
            take(co_await drive.lifecycle(files.create_directories(parent)));
            co_await storage::testing::store_contract::write_bytes(
              files, *collision_path, std::string(4096, 'x'), drive);
        }
        auto pending = writer->bootstrap(work);
        BOOST_REQUIRE(!pending.available());
        BOOST_CHECK(!writer->positions());
        if (detach) admission_abort.request_abort();
        std::optional<seastar::future<runtime::result<void>>> closing;
        if (close_while_pending) closing.emplace(writer->close());
        auto result = co_await drive.lifecycle(std::move(pending));
        if (closing) {
            auto done = co_await drive.lifecycle(std::move(*closing));
            BOOST_CHECK_EQUAL(done.has_value(), !expect_failure && !collision);
            if (!expect_failure && !collision)
                BOOST_CHECK(!writer->failure().failed());
        }
        BOOST_CHECK_EQUAL(result.has_value(), !expected_close_error);
        if (geometry_failure) {
            BOOST_REQUIRE(!result);
            BOOST_CHECK(result.error().code() == errc::invalid_argument);
            BOOST_CHECK(!take(control->snapshot()).fields.wal_head);
            BOOST_CHECK(
              writer->header_publication().disposition
              == local_publication_disposition::durable);
        }
        BOOST_CHECK_EQUAL(
          writer->positions().has_value(), !expected_close_error);
        if (collision) {
            BOOST_CHECK(!take(control->snapshot()).fields.wal_head);
            BOOST_CHECK(
              (co_await storage::testing::store_contract::read_bytes(
                files, *collision_path, drive))
              == std::string(4096, 'x'));
            BOOST_CHECK(
              take(control->snapshot()).fields.wal_high
              == local_wal_high{}.checked_advance(4).value());
        }
        if (durable_header_error) {
            BOOST_CHECK(
              writer->header_publication().disposition
              == local_publication_disposition::durable);
            BOOST_CHECK(writer->header_publication().failure.failed());
            BOOST_CHECK(!take(control->snapshot()).fields.wal_head);
        }
        if (durable_head_error) {
            BOOST_CHECK(
              writer->head_publication().disposition
              == local_publication_disposition::durable);
            BOOST_CHECK(writer->head_publication().failure.failed());
            const auto path = take(
              take(local_paths::make(spec.root)).control(0));
            auto loaded = take(
              co_await drive.lifecycle(load_local_control(
                files, spec, 0, path, budget, limits(), work)));
            BOOST_CHECK(
              std::get<local_shard_control>(loaded.value.payload()).wal_head
              == writer->prepared_head());
        }
        if (!expected_close_error) {
            const auto paths = take(local_paths::make(spec.root));
            const auto wal = take(
              paths.wal(0, writer->prepared_head()->incarnation));
            const auto head = take(paths.control(0));
            const auto snapshot = take(fake_file_test_access::snapshot(files));
            const auto lookup = [&](const runtime::file_path& path) {
                return take(
                         fake_file_test_access::lookup(
                           files,
                           take(
                             fake_file_test_access::resolve(
                               files, path.value()))))
                  .value();
            };
            const auto directory_target = [&](
                                            const runtime::file_path& path,
                                            runtime::builtin_fault_point
                                              point) {
                const auto parent = take(
                  runtime::file_path::make(
                    path.value().substr(0, path.value().rfind('/'))));
                const auto id = lookup(parent);
                for (const auto& object : snapshot.objects)
                    if (object.id == id)
                        return fault_target{
                          id,
                          object.occurrences[static_cast<std::size_t>(point)]};
                throw std::runtime_error("missing publication directory");
            };
            targets.header = {lookup(wal), 1};
            targets.head = {lookup(head), 1};
            targets.header_directory_sync = directory_target(
              wal, runtime::builtin_fault_point::directory_sync);
            targets.header_directory_close = directory_target(
              wal, runtime::builtin_fault_point::directory_cursor_close);
            targets.head_directory_sync = directory_target(
              head, runtime::builtin_fault_point::directory_sync);
            targets.head_directory_close = directory_target(
              head, runtime::builtin_fault_point::directory_cursor_close);
        }
    } catch (...) {
        failed.observe(std::current_exception());
    }
    if (writer) {
        try {
            auto closed = co_await drive.lifecycle(writer->close());
            BOOST_CHECK_EQUAL(
              closed.has_value(), !expect_failure && !collision);
        } catch (...) {
            failed.observe(std::current_exception());
        }
        writer.reset();
    }
    if (ids) {
        failed.observe(co_await drive.lifecycle(ids->close()));
        ids.reset();
    }
    if (control) {
        failed.observe(co_await drive.lifecycle(control->close()));
        control.reset();
    }
    take(failed.outcome());
    co_return targets;
}
} // namespace

SEASTAR_TEST_CASE(wal_bootstrap_joins_detached_and_closing_callers) {
    co_await with_wal_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          static_cast<void>(co_await bootstrap_history(
            env, budget, drive, false, false, false, true));
      });
    co_await with_wal_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          static_cast<void>(co_await bootstrap_history(
            env, budget, drive, false, false, false, false, true));
      });
}

SEASTAR_TEST_CASE(wal_bootstrap_preserves_an_existing_final_file) {
    co_await with_wal_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          static_cast<void>(co_await bootstrap_history(
            env, budget, drive, false, false, false, false, false, true));
      });
}

SEASTAR_TEST_CASE(wal_bootstrap_publication_failures_never_activate) {
    bootstrap_targets targets;
    co_await with_wal_environment(
      config(), [&](auto& env, auto& budget, auto drive) -> seastar::future<> {
          targets = co_await bootstrap_history(env, budget, drive, false);
      });
    using point = runtime::builtin_fault_point;
    using action = runtime::fault_action;
    struct fault_case {
        point at;
        fault_target target;
        action effect;
        bool header_close{false}, head_close{false};
    };
    const std::array cases{
      fault_case{
        point::file_write, targets.header, action::file_failure_before_effect},
      fault_case{
        point::file_write, targets.header, action::file_failure_after_prefix},
      fault_case{
        point::file_flush, targets.header, action::file_failure_before_effect},
      fault_case{
        point::file_size, targets.header, action::file_failure_before_effect},
      fault_case{
        point::file_read, targets.header, action::file_failure_before_effect},
      fault_case{
        point::file_close, targets.header, action::file_failure_before_effect},
      fault_case{
        point::file_rename, targets.header, action::file_failure_after_effect},
      fault_case{
        point::directory_sync,
        targets.header_directory_sync,
        action::file_failure_after_effect},
      fault_case{
        point::directory_cursor_close,
        targets.header_directory_close,
        action::file_failure_before_effect,
        true},
      fault_case{
        point::file_write, targets.head, action::file_failure_before_effect},
      fault_case{
        point::file_flush, targets.head, action::file_failure_before_effect},
      fault_case{
        point::file_close, targets.head, action::file_failure_before_effect},
      fault_case{
        point::file_rename, targets.head, action::file_failure_after_effect},
      fault_case{
        point::directory_sync,
        targets.head_directory_sync,
        action::file_failure_after_effect},
      fault_case{
        point::directory_cursor_close,
        targets.head_directory_close,
        action::file_failure_before_effect,
        false,
        true}};
    for (const auto& fault : cases) {
        BOOST_REQUIRE(fault.target.object != 0 && fault.target.occurrence != 0);
        co_await with_wal_environment(
          config(
            fault.at,
            fault.effect,
            fault.target.occurrence,
            runtime::fault_object_key::from_u64(fault.target.object)),
          [&fault](auto& env, auto& budget, auto drive) -> seastar::future<> {
              static_cast<void>(co_await bootstrap_history(
                env,
                budget,
                drive,
                true,
                fault.header_close,
                fault.head_close));
          });
    }
}

SEASTAR_TEST_CASE(wal_writer_fake_group_write_and_captured_barrier) {
    co_await with_wal_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto spec = specification(
            take(runtime::file_path::make("/kwaque/store")), {1, 1});
          const std::array specs{spec};
          ownership_input owner{specs};
          take(co_await drive.lifecycle(files.create_directories(spec.root)));
          co_await storage::testing::wal_append_contract::exercise(
            files, owner, spec, budget, drive);
      });
}
SEASTAR_TEST_CASE(wal_writer_fake_whole_group_capacity) {
    co_await with_wal_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto spec = specification(
            take(runtime::file_path::make("/kwaque/store")), {1, 1});
          const std::array specs{spec};
          ownership_input owner{specs};
          take(co_await drive.lifecycle(files.create_directories(spec.root)));
          co_await storage::testing::wal_append_contract::capacity(
            files, owner, spec, budget, drive);
      });
}

namespace {
namespace append_contract = kwaque::storage::testing::wal_append_contract;
std::uint64_t wal_object(
  fake_file_system& files, const local_device_spec& spec, local_wal_head head) {
    const auto path = take(
      take(local_paths::make(spec.root)).wal(0, head.incarnation));
    return take(
             fake_file_test_access::lookup(
               files,
               take(fake_file_test_access::resolve(files, path.value()))))
      .value();
}
std::uint64_t wal_occurrences(
  fake_file_system& files,
  std::uint64_t object,
  runtime::builtin_fault_point point) {
    auto state = take(fake_file_test_access::snapshot(files));
    for (const auto& inode : state.objects)
        if (inode.id == object)
            return inode.occurrences[static_cast<std::size_t>(point)];
    throw std::runtime_error("missing WAL inode");
}
using runtime::testing::drain_reactor_tasks;

template<typename Predicate>
seastar::future<> run_cpu_until(Predicate ready) {
    if (!ready()) co_await drain_reactor_tasks();
    require(ready(), "drained reactor work did not reach its checkpoint");
}
} // namespace

SEASTAR_TEST_CASE(
  wal_writer_gathers_queued_groups_and_flushes_only_its_capture_under_pressure) {
    co_await with_wal_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto spec = specification(
            take(runtime::file_path::make("/kwaque/store")), {1, 1});
          const std::array specs{spec};
          ownership_input owner{specs};
          take(co_await drive.lifecycle(files.create_directories(spec.root)));
          co_await append_contract::with_writer(
            files,
            owner,
            spec,
            budget,
            drive,
            storage::testing::wal_writer_contract::configuration(),
            [&](auto& writer, auto& work) -> seastar::future<> {
                const auto inode = wal_object(
                  files, spec, *writer.prepared_head());
                const auto flushes = wal_occurrences(
                  files, inode, runtime::builtin_fault_point::file_flush);
                const std::array records{
                  storage::testing::assigned_wire(true, 48)};
                auto input
                  = co_await storage::testing::wal_writer_contract::offer(
                    budget, records[0], work);
                auto preserved = take(
                  co_await writer.prepare(
                    std::move(input),
                    storage::testing::wal_writer_contract::child_context(
                      spec.owner.cluster()),
                    work));
                auto first = take(wal_group::make(budget, 1));
                take(
                  first.append(std::move(preserved.wal), preserved.expected));
                auto second = co_await append_contract::offer(
                  budget, spec.owner.cluster(), work, records);
                auto third = co_await append_contract::offer(
                  budget, spec.owner.cluster(), work, records);
                seastar::abort_source caller_abort;
                codec::cooperative_work caller_work{
                  codec::limits::defaults(), caller_abort};
                auto one = take(
                  co_await writer.submit(std::move(first), caller_work));
                caller_abort.request_abort();
                co_await run_cpu_until([&] {
                    return writer.statistics().write_calls == 1
                           || writer.failure().failed();
                });
                require(
                  !writer.failure().failed() && !one.written.available(),
                  "first write was not parked at backend completion");
                require(
                  preserved.segment.batch().bytes().content_equals(records[0]),
                  "submitted exact child changed while native completion was "
                  "pending");
                auto two = take(
                  co_await writer.submit(std::move(second), work));
                auto three = take(
                  co_await writer.submit(std::move(third), work));
                co_await run_cpu_until([&] {
                    return writer.statistics().encoded_groups == 3
                           || writer.failure().failed();
                });
                require(
                  !writer.failure().failed()
                    && writer.statistics().write_calls == 1,
                  "another runtime write crossed the active write");
                auto barrier = writer.barrier(two.boundary);
                auto overlap = co_await writer.barrier(three.boundary);
                require(
                  overlap.failure.error()
                    && overlap.failure.error()->code() == errc::queue_full,
                  "overlapping barrier queued without admission");
                std::array<std::optional<workload_reservation>, 32> pressure;
                for (auto& slot : pressure) {
                    auto held = budget.try_reserve(byte_count{4096});
                    if (!held) break;
                    slot.emplace(std::move(*held));
                }
                require(
                  !budget.try_reserve(byte_count{1}),
                  "ordinary task budget did not saturate");
                auto done_one = co_await drive.lifecycle(
                  std::move(one.written));
                auto done_two = co_await drive.lifecycle(
                  std::move(two.written));
                auto done_three = co_await drive.lifecycle(
                  std::move(three.written));
                take(done_one.failure.outcome());
                take(done_two.failure.outcome());
                take(done_three.failure.outcome());
                auto flushed = co_await drive.lifecycle(std::move(barrier));
                take(flushed.failure.outcome());
                require(
                  flushed.receipt
                    && flushed.receipt->boundary().cursor()
                         == two.boundary.cursor(),
                  "barrier changed its captured membership");
                require(
                  writer.progress()->write_complete == three.boundary.cursor()
                    && writer.progress()->durable == two.boundary.cursor(),
                  "later completed write enlarged earlier certification");
                require(
                  writer.statistics().write_calls == 2
                    && writer.statistics().gathered_groups == 1,
                  "adjacent ready groups did not share one logical write");
                require(
                  wal_occurrences(
                    files, inode, runtime::builtin_fault_point::file_flush)
                    == flushes + 1,
                  "captured barrier did not perform exactly one native WAL "
                  "flush");
                auto reused = co_await drive.lifecycle(
                  writer.barrier(two.boundary));
                take(reused.failure.outcome());
                require(
                  wal_occurrences(
                    files, inode, runtime::builtin_fault_point::file_flush)
                    == flushes + 1,
                  "successful capture started another native flush");
                auto last = co_await drive.lifecycle(
                  writer.barrier(three.boundary));
                take(last.failure.outcome());
                require(
                  writer.progress()->durable == three.boundary.cursor()
                    && wal_occurrences(
                         files, inode, runtime::builtin_fault_point::file_flush)
                         == flushes + 2,
                  "later capture lacked its own successful barrier");
            });
      });
}

namespace {
struct wal_fault_targets final {
    fault_target write, flush, close;
};
seastar::future<wal_fault_targets> append_targets(
  std::uint32_t memory_alignment = 4096,
  std::uint32_t native_max_length = 131072) {
    wal_fault_targets targets;
    co_await with_wal_environment(
      config(
        std::nullopt,
        runtime::fault_action::file_failure_before_effect,
        1,
        {},
        {},
        {},
        memory_alignment,
        native_max_length),
      [&](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto spec = specification(
            take(runtime::file_path::make("/kwaque/store")), {1, 1});
          const std::array specs{spec};
          ownership_input owner{specs};
          take(co_await drive.lifecycle(files.create_directories(spec.root)));
          co_await append_contract::with_writer(
            files,
            owner,
            spec,
            budget,
            drive,
            storage::testing::wal_writer_contract::configuration(),
            [&](auto& writer, auto&) -> seastar::future<> {
                const auto inode = wal_object(
                  files, spec, *writer.prepared_head());
                targets.write = {
                  inode,
                  wal_occurrences(
                    files, inode, runtime::builtin_fault_point::file_write)
                    + 1};
                targets.flush = {
                  inode,
                  wal_occurrences(
                    files, inode, runtime::builtin_fault_point::file_flush)
                    + 1};
                targets.close = {
                  inode,
                  wal_occurrences(
                    files, inode, runtime::builtin_fault_point::file_close)
                    + 1};
                co_return;
            });
      });
    co_return targets;
}
} // namespace

SEASTAR_TEST_CASE(
  wal_writer_failure_settles_dependents_without_reusing_accepted_coordinates) {
    const auto targets = co_await append_targets();
    using point = runtime::builtin_fault_point;
    using action = runtime::fault_action;
    const std::array cases{
      std::pair{point::file_write, action::file_failure_before_effect},
      std::pair{point::file_write, action::file_failure_after_prefix},
      std::pair{point::file_flush, action::file_failure_before_effect},
      std::pair{point::file_flush, action::file_failure_after_effect}};
    for (const auto [at, effect] : cases) {
        const auto selected = at == point::file_write ? targets.write
                                                      : targets.flush;
        co_await with_wal_environment(
          config(
            at,
            effect,
            selected.occurrence,
            runtime::fault_object_key::from_u64(selected.object)),
          [at](auto& env, auto& budget, auto drive) -> seastar::future<> {
              auto& files = env.file_system();
              const auto spec = specification(
                take(runtime::file_path::make("/kwaque/store")), {1, 1});
              const std::array specs{spec};
              ownership_input owner{specs};
              take(
                co_await drive.lifecycle(files.create_directories(spec.root)));
              co_await append_contract::with_writer(
                files,
                owner,
                spec,
                budget,
                drive,
                storage::testing::wal_writer_contract::configuration(),
                [&](auto& writer, auto& work) -> seastar::future<> {
                    const auto initial = *writer.progress();
                    const std::array records{storage::testing::assigned_wire()};
                    auto first = co_await append_contract::offer(
                      budget, spec.owner.cluster(), work, records);
                    auto second = co_await append_contract::offer(
                      budget, spec.owner.cluster(), work, records);
                    auto one = take(
                      co_await writer.submit(std::move(first), work));
                    co_await run_cpu_until([&] {
                        return writer.statistics().write_calls == 1
                               || writer.failure().failed();
                    });
                    require(
                      !writer.failure().failed(),
                      "write failed before scheduled backend effect");
                    auto two = take(
                      co_await writer.submit(std::move(second), work));
                    auto cut = two.boundary;
                    auto barrier = writer.barrier(cut);
                    auto written_one = co_await drive.lifecycle(
                      std::move(one.written));
                    auto written_two = co_await drive.lifecycle(
                      std::move(two.written));
                    auto outcome = co_await drive.lifecycle(std::move(barrier));
                    require(
                      outcome.failure.failed() && !outcome.receipt,
                      "failed barrier published durability");
                    require(
                      writer.progress()->reserved == cut.cursor()
                        && writer.progress()->durable == initial.durable,
                      "failure reused coordinates or fabricated a barrier");
                    if (at == point::file_write) {
                        require(
                          written_one.failure.failed()
                            && written_two.failure.failed()
                            && writer.progress()->write_complete
                                 == initial.write_complete
                            && writer.statistics().write_calls == 1,
                          "write error skipped failure or advanced across a "
                          "hole");
                    } else {
                        take(written_one.failure.outcome());
                        take(written_two.failure.outcome());
                        require(
                          writer.progress()->write_complete == cut.cursor(),
                          "flush failure erased written evidence");
                    }
                    auto again = co_await append_contract::offer(
                      budget, spec.owner.cluster(), work, records);
                    auto rejected = co_await writer.submit(
                      std::move(again), work);
                    require(
                      !rejected && writer.progress()->reserved == cut.cursor(),
                      "failed owner admitted more data");
                },
                true);
          });
    }
}

SEASTAR_TEST_CASE(wal_writer_fake_rotation_and_extent) {
    co_await with_wal_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto spec = specification(
            take(runtime::file_path::make("/kwaque/store")), {1, 1});
          const std::array specs{spec};
          ownership_input owner{specs};
          take(co_await drive.lifecycle(files.create_directories(spec.root)));
          co_await storage::testing::wal_rotation_contract::exercise(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(wal_writer_fake_rotation_exact_capacity) {
    co_await with_wal_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto spec = specification(
            take(runtime::file_path::make("/kwaque/store")), {1, 1});
          const std::array specs{spec};
          ownership_input owner{specs};
          take(co_await drive.lifecycle(files.create_directories(spec.root)));
          co_await storage::testing::wal_rotation_contract::capacity(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(wal_writer_fake_rotation_publication_pressure) {
    co_await with_wal_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto spec = specification(
            take(runtime::file_path::make("/kwaque/store")), {1, 1});
          const std::array specs{spec};
          ownership_input owner{specs};
          take(co_await drive.lifecycle(files.create_directories(spec.root)));
          co_await storage::testing::wal_rotation_contract::pressure(
            files, owner, spec, budget, drive);
      });
}

namespace {
namespace rotation_contract = kwaque::storage::testing::wal_rotation_contract;
struct rotation_fault_targets final {
    fault_target old_flush, old_close, header, header_sync, head, head_sync,
      head_close, verification_read, verification_close;
};
seastar::future<rotation_fault_targets> rotation_history(
  environment& env,
  workload_budget& budget,
  simulation::testing::scheduler_driver drive,
  bool expect_failure,
  bool durable_head_error = false,
  bool verification_no_space = false) {
    rotation_fault_targets targets;
    auto& files = env.file_system();
    const auto spec = specification(
      take(runtime::file_path::make("/kwaque/store")), {1, 1});
    const std::array specs{spec};
    ownership_input owner{specs};
    take(co_await drive.lifecycle(files.create_directories(spec.root)));
    co_await append_contract::with_writer(
      files,
      owner,
      spec,
      budget,
      drive,
      storage::testing::wal_writer_contract::configuration(),
      [&](auto& writer, auto& control, auto&, auto& work) -> seastar::future<> {
          const auto old_head = *writer.prepared_head();
          const auto inode = wal_object(files, spec, old_head);
          targets.old_flush = {
            inode,
            wal_occurrences(
              files, inode, runtime::builtin_fault_point::file_flush)
              + 1};
          targets.old_close = {
            inode,
            wal_occurrences(
              files, inode, runtime::builtin_fault_point::file_close)
              + 1};
          auto accepted = co_await rotation_contract::submit_one(
            writer, budget, spec.owner.cluster(), work);
          const auto cut = accepted.boundary;
          co_await run_cpu_until([&] {
              return writer.statistics().write_calls == 1
                     || writer.failure().failed();
          });
          auto transition = writer.rotate(cut, byte_count{8192}, work);
          runtime::first_failure assertions;
          try {
              require(
                writer.rotation_pending() && !accepted.written.available(),
                "rotation did not freeze while the accepted native write was "
                "pending");
              auto overlap = co_await writer.rotate(
                cut, byte_count{8192}, work);
              require(
                !overlap && overlap.error().code() == errc::queue_full,
                "overlapping rotation acquired another successor");
              auto barrier = co_await writer.barrier(cut);
              require(
                barrier.failure.error()
                  && barrier.failure.error()->code() == errc::queue_full
                  && !barrier.receipt,
                "external barrier overlapped the rotation barrier");
              const std::array records{storage::testing::assigned_wire()};
              auto group = co_await append_contract::offer(
                budget, spec.owner.cluster(), work, records);
              auto blocked = co_await writer.submit(std::move(group), work);
              require(
                !blocked && writer.progress()->reserved == cut.cursor(),
                "frozen predecessor accepted another group");
          } catch (...) {
              assertions.observe(std::current_exception());
          }
          auto rotated = co_await drive.lifecycle(std::move(transition));
          auto done = co_await drive.lifecycle(std::move(accepted.written));
          take(assertions.outcome());
          if (expect_failure) {
              require(
                !rotated && writer.failure().failed()
                  && writer.statistics().rotations == 0
                  && writer.progress()->reserved == cut.cursor()
                  && writer.prepared_head() == old_head,
                "failed transition activated successor or reused old "
                "coordinates");
              if (verification_no_space) {
                  const auto& publication = writer.rotation_publication();
                  require(
                    !publication.admission_rejected
                      && publication.stage == local_publication_stage::none
                      && publication.failure.error()
                      && runtime::file_detail(*publication.failure.error())
                           == runtime::file_failure_detail::no_space
                      && writer.failure().error()
                           == publication.failure.error(),
                    "verification/cleanup failure became admission pressure or "
                    "lost its first cause");
              }
              if (durable_head_error) {
                  auto loaded = take(
                    co_await drive.lifecycle(load_local_control(
                      files,
                      spec,
                      0,
                      take(take(local_paths::make(spec.root)).control(0)),
                      budget,
                      limits(),
                      work)));
                  require(
                    writer.rotation_publication().disposition
                        == local_publication_disposition::durable
                      && control.fenced()
                      && std::get<local_shard_control>(loaded.value.payload())
                             .wal_head
                           == writer.successor_head(),
                    "durable head cleanup error lost confirmed namespace "
                    "state");
              }
              auto retry = co_await writer.rotate(cut, byte_count{8192}, work);
              require(
                !retry && writer.statistics().rotations == 0,
                "native failure became a retryable transition");
              co_return;
          }
          take(rotated);
          take(done.failure.outcome());
          const auto paths = take(local_paths::make(spec.root));
          const auto new_path = take(
            paths.wal(0, writer.prepared_head()->incarnation));
          const auto control_path = take(paths.control(0));
          const auto lookup = [&](const runtime::file_path& path) {
              return take(
                       fake_file_test_access::lookup(
                         files,
                         take(
                           fake_file_test_access::resolve(
                             files, path.value()))))
                .value();
          };
          const auto directory = [&](
                                   const runtime::file_path& path,
                                   runtime::builtin_fault_point point) {
              const auto parent = take(
                runtime::file_path::make(
                  path.value().substr(0, path.value().rfind('/'))));
              const auto id = lookup(parent);
              return fault_target{id, wal_occurrences(files, id, point)};
          };
          targets.header = {lookup(new_path), 1};
          targets.head = {lookup(control_path), 1};
          const auto reads = wal_occurrences(
            files,
            targets.header.object,
            runtime::builtin_fault_point::file_read);
          require(
            reads >= 3,
            "successor corroboration did not read its header and prefix/body");
          targets.verification_read = {targets.header.object, reads - 1};
          targets.verification_close = {
            targets.header.object,
            wal_occurrences(
              files,
              targets.header.object,
              runtime::builtin_fault_point::file_close)};
          targets.header_sync = directory(
            new_path, runtime::builtin_fault_point::directory_sync);
          targets.head_sync = directory(
            control_path, runtime::builtin_fault_point::directory_sync);
          targets.head_close = directory(
            control_path, runtime::builtin_fault_point::directory_cursor_close);
          require(
            writer.statistics().flush_calls == 1,
            "rotation did not flush exactly its old cut");
          const auto snapshot = take(fake_file_test_access::snapshot(files));
          for (const auto& object : snapshot.objects) {
              if (object.id == inode) {
                  require(
                    object.durable_size == cut.cursor().position().value()
                      && object.occurrences[static_cast<std::size_t>(
                           runtime::builtin_fault_point::file_truncate)]
                           == 0,
                    "rotation truncated capacity or failed to persist the old "
                    "complete end");
              }
              if (object.id == targets.header.object) {
                  require(
                    object.durable_size == 8192,
                    "successor capacity became persisted content");
              }
          }
      },
      expect_failure);
    co_return targets;
}
} // namespace

SEASTAR_TEST_CASE(
  wal_writer_rotation_fences_native_and_durable_cleanup_failures) {
    rotation_fault_targets targets;
    co_await with_wal_environment(
      config(), [&](auto& env, auto& budget, auto drive) -> seastar::future<> {
          targets = co_await rotation_history(env, budget, drive, false);
      });
    using point = runtime::builtin_fault_point;
    using action = runtime::fault_action;
    struct fault_case final {
        point at;
        action effect;
        fault_target target;
        bool durable_head{false};
    };
    const std::array cases{
      fault_case{
        point::file_flush,
        action::file_failure_before_effect,
        targets.old_flush},
      fault_case{
        point::file_close,
        action::file_failure_before_effect,
        targets.old_close},
      fault_case{
        point::file_write, action::file_failure_after_prefix, targets.header},
      fault_case{
        point::file_flush, action::file_failure_after_effect, targets.header},
      fault_case{
        point::directory_sync,
        action::file_failure_after_effect,
        targets.header_sync},
      fault_case{
        point::file_flush, action::file_failure_before_effect, targets.head},
      fault_case{
        point::directory_sync,
        action::file_failure_after_effect,
        targets.head_sync},
      fault_case{
        point::directory_cursor_close,
        action::file_failure_before_effect,
        targets.head_close,
        true}};
    for (const auto& fault : cases) {
        co_await with_wal_environment(
          config(
            fault.at,
            fault.effect,
            fault.target.occurrence,
            runtime::fault_object_key::from_u64(fault.target.object)),
          [&fault](auto& env, auto& budget, auto drive) -> seastar::future<> {
              static_cast<void>(co_await rotation_history(
                env, budget, drive, true, fault.durable_head));
          });
    }
}

namespace {
seastar::future<fault_target> no_space_history(
  environment& env,
  workload_budget& budget,
  simulation::testing::scheduler_driver drive,
  bool fail) {
    fault_target next;
    auto& files = env.file_system();
    const auto spec = specification(
      take(runtime::file_path::make("/kwaque/store")), {1, 1});
    const std::array specs{spec};
    ownership_input owner{specs};
    take(co_await drive.lifecycle(files.create_directories(spec.root)));
    co_await append_contract::with_writer(
      files,
      owner,
      spec,
      budget,
      drive,
      storage::testing::wal_writer_contract::configuration(),
      [&](auto& writer, auto& work) -> seastar::future<> {
          auto first = co_await rotation_contract::submit_one(
            writer, budget, spec.owner.cluster(), work);
          auto written = co_await drive.lifecycle(std::move(first.written));
          take(written.failure.outcome());
          auto durable = co_await drive.lifecycle(
            writer.barrier(first.boundary));
          take(durable.failure.outcome());
          const auto inode = wal_object(files, spec, *writer.prepared_head());
          next = {
            inode,
            wal_occurrences(
              files, inode, runtime::builtin_fault_point::file_write)
              + 1};
          if (!fail) co_return;
          const auto path = take(
            take(local_paths::make(spec.root))
              .wal(0, writer.prepared_head()->incarnation));
          const auto prefix = co_await read_bytes(files, path, drive);
          auto second = co_await rotation_contract::submit_one(
            writer, budget, spec.owner.cluster(), work);
          auto failed = co_await drive.lifecycle(std::move(second.written));
          require(
            failed.failure.error()
              && runtime::file_detail(*failed.failure.error())
                   == runtime::file_failure_detail::no_space
              && writer.progress()->write_complete == first.boundary.cursor()
              && writer.progress()->durable == first.boundary.cursor()
              && writer.progress()->reserved == second.boundary.cursor(),
            "no-space failure replaced the certified prefix with attempted "
            "bytes");
          auto refused = co_await writer.barrier(second.boundary);
          require(
            refused.failure.failed() && !refused.receipt,
            "partial tail received a durable receipt");
          const auto snapshot = take(fake_file_test_access::snapshot(files));
          bool checked = false;
          for (const auto& object : snapshot.objects)
              if (object.id == inode) {
                  checked = true;
                  require(
                    object.visible_size > prefix.size()
                      && object.visible_size
                           <= second.boundary.cursor().position().value()
                      && object.durable_size == prefix.size()
                      && object.occurrences[static_cast<std::size_t>(
                           runtime::builtin_fault_point::file_truncate)]
                           == 0,
                    "failed append converted EOF/capacity into content "
                    "certification");
                  require(
                    std::equal(
                      prefix.begin(),
                      prefix.end(),
                      object.visible_bytes.begin(),
                      [](char a, std::byte b) {
                          return std::bit_cast<std::byte>(a) == b;
                      }),
                    "no-space failure modified the preceding certified bytes");
              }
          require(checked, "failed WAL inode disappeared");
      },
      fail);
    co_return next;
}
} // namespace

SEASTAR_TEST_CASE(
  wal_writer_no_space_keeps_certified_prefix_and_untrusted_tail) {
    fault_target next;
    co_await with_wal_environment(
      config(), [&](auto& env, auto& budget, auto drive) -> seastar::future<> {
          next = co_await no_space_history(env, budget, drive, false);
      });
    const auto rule = take(
      fault_rule::make(
        take(fault_rule_id::make(2)),
        runtime::builtin_fault_point::file_write,
        runtime::fault_object_key::from_u64(next.object),
        take(runtime::fault_occurrence::make(next.occurrence)),
        take(runtime::fault_occurrence::make(next.occurrence)),
        fault_selector::once(),
        take(
          runtime::fault_decision::make_file_failure(
            runtime::fault_action::file_failure_after_prefix,
            runtime::file_failure_detail::no_space,
            byte_count{512}))));
    co_await with_wal_environment(
      config(
        std::nullopt,
        runtime::fault_action::file_failure_before_effect,
        1,
        std::nullopt,
        std::nullopt,
        rule),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          static_cast<void>(
            co_await no_space_history(env, budget, drive, true));
      });
}

SEASTAR_TEST_CASE(
  wal_rotation_preserves_no_space_over_verification_close_failure) {
    rotation_fault_targets targets;
    co_await with_wal_environment(
      config(), [&](auto& env, auto& budget, auto drive) -> seastar::future<> {
          targets = co_await rotation_history(env, budget, drive, false);
      });
    const auto read = targets.verification_read;
    const auto closed = targets.verification_close;
    const auto rule = take(
      fault_rule::make(
        take(fault_rule_id::make(2)),
        runtime::builtin_fault_point::file_read,
        runtime::fault_object_key::from_u64(read.object),
        take(runtime::fault_occurrence::make(read.occurrence)),
        take(runtime::fault_occurrence::make(read.occurrence)),
        fault_selector::once(),
        take(
          runtime::fault_decision::make_file_failure(
            runtime::fault_action::file_failure_before_effect,
            runtime::file_failure_detail::no_space,
            byte_count{}))));
    co_await with_wal_environment(
      config(
        runtime::builtin_fault_point::file_close,
        runtime::fault_action::file_failure_before_effect,
        closed.occurrence,
        runtime::fault_object_key::from_u64(closed.object),
        std::nullopt,
        rule),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          static_cast<void>(
            co_await rotation_history(env, budget, drive, true, false, true));
      });
}

SEASTAR_TEST_CASE(wal_writer_fake_close_retains_unreferenced_successor) {
    co_await with_wal_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto spec = specification(
            take(runtime::file_path::make("/kwaque/store")), {1, 1});
          const std::array specs{spec};
          ownership_input owner{specs};
          take(co_await drive.lifecycle(files.create_directories(spec.root)));
          co_await storage::testing::wal_rotation_contract::pressure(
            files, owner, spec, budget, drive, true);
      });
}

SEASTAR_TEST_CASE(wal_writer_fake_recovery_inventory) {
    co_await with_wal_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto spec = specification(
            take(runtime::file_path::make("/kwaque/store")), {1, 1});
          const std::array specs{spec};
          ownership_input owner{specs};
          take(co_await drive.lifecycle(files.create_directories(spec.root)));
          co_await storage::testing::wal_lifecycle_contract::inventory(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(wal_writer_fake_exceptional_rotation_close) {
    co_await with_wal_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto spec = specification(
            take(runtime::file_path::make("/kwaque/store")), {1, 1});
          const std::array specs{spec};
          ownership_input owner{specs};
          take(co_await drive.lifecycle(files.create_directories(spec.root)));
          co_await storage::testing::wal_lifecycle_contract::
            exceptional_rotation_close(files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(
  wal_writer_fake_early_environment_stop_under_workload_pressure) {
    co_await with_wal_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto spec = specification(
            take(runtime::file_path::make("/kwaque/store")), {1, 1});
          const std::array specs{spec};
          ownership_input owner{specs};
          take(co_await drive.lifecycle(files.create_directories(spec.root)));
          auto observer = env.resource_manager().acquire_workload(
            resource::workload_class::metadata);
          co_await storage::testing::wal_lifecycle_contract::shutdown(
            files,
            owner,
            spec,
            budget,
            drive,
            env.tasks().abort_source(),
            [&] {
                env.request_abort();
                require(
                  !env.lifetime().acquire(),
                  "stop admitted a new runtime lease");
            },
            observer.memory_admission());
      });
}

namespace {
seastar::future<> parked_close_history(
  environment& env,
  workload_budget& budget,
  simulation::testing::scheduler_driver drive,
  bool fail,
  bool rotate = false,
  std::uint32_t native_max_length = 131072) {
    auto& files = env.file_system();
    const auto spec = specification(
      take(runtime::file_path::make("/kwaque/store")), {1, 1});
    const std::array specs{spec};
    ownership_input owner{specs};
    take(co_await drive.lifecycle(files.create_directories(spec.root)));
    co_await append_contract::with_writer(
      files,
      owner,
      spec,
      budget,
      drive,
      storage::testing::wal_writer_contract::configuration(),
      [&](auto& writer, auto& control, auto&, auto& work) -> seastar::future<> {
          const auto initial = *writer.progress();
          const auto head = *writer.prepared_head();
          const auto inode = wal_object(files, spec, head);
          const auto initial_writes = wal_occurrences(
            files, inode, runtime::builtin_fault_point::file_write);
          const auto first_bytes = storage::testing::assigned_wire(true, 48);
          auto input = co_await storage::testing::wal_writer_contract::offer(
            budget, first_bytes, work);
          auto aliases = take(
            co_await writer.prepare(
              std::move(input),
              storage::testing::wal_writer_contract::child_context(
                spec.owner.cluster()),
              work));
          auto first = take(wal_group::make(budget, 1));
          take(first.append(std::move(aliases.wal), aliases.expected));
          const std::array second_bytes{
            storage::testing::assigned_wire(false, 48)};
          const std::array third_bytes{
            storage::testing::assigned_wire(true, 32)};
          auto second = co_await append_contract::offer(
            budget, spec.owner.cluster(), work, second_bytes);
          auto third = co_await append_contract::offer(
            budget, spec.owner.cluster(), work, third_bytes);
          seastar::abort_source caller_abort;
          codec::cooperative_work caller{work.policy(), caller_abort};
          auto one = take(co_await writer.submit(std::move(first), caller));
          caller_abort.request_abort();
          co_await run_cpu_until([&] {
              return writer.statistics().write_calls == 1
                     || writer.failure().failed();
          });
          auto two = take(co_await writer.submit(std::move(second), work));
          {
              auto detached = take(
                co_await writer.submit(std::move(third), work));
              static_cast<void>(detached);
          }
          const auto cut = take(writer.capture());
          co_await run_cpu_until([&] {
              return writer.statistics().encoded_groups == 3
                     || writer.failure().failed();
          });
          runtime::first_failure assertions;
          try {
              require(
                !writer.failure().failed() && !one.written.available()
                  && writer.statistics().write_calls == 1,
                "queued groups bypassed the parked native write");
              require(
                take(fake_file_test_access::verify_pending_write_buffers(files))
                    == 1 + (8192 - 1) / native_max_length
                  && aliases.segment.batch().bytes().content_equals(
                    first_bytes),
                "queued append mutated a submitted DMA extent or exact child "
                "alias");
          } catch (...) {
              assertions.observe(std::current_exception());
          }
          std::optional<seastar::future<runtime::result<void>>> rotating;
          std::optional<seastar::future<wal_barrier_outcome>> barrier;
          if (rotate)
              rotating.emplace(writer.rotate(cut, byte_count{8192}, work));
          else
              barrier.emplace(writer.barrier(one.boundary));
          if (rotate) budget.close_admission();
          auto closing = writer.close();
          const auto overlap = co_await writer.close();
          const auto closed = co_await drive.lifecycle(std::move(closing));
          const auto first_done = co_await drive.lifecycle(
            std::move(one.written));
          const auto second_done = co_await drive.lifecycle(
            std::move(two.written));
          if (rotating) {
              const auto stopped = co_await drive.lifecycle(
                std::move(*rotating));
              require(
                !stopped && stopped.error().code() == errc::closed,
                "stopping a pending rotation did not settle its observer");
          }
          std::optional<wal_barrier_outcome> flushed;
          if (barrier)
              flushed.emplace(co_await drive.lifecycle(std::move(*barrier)));
          const auto closes = wal_occurrences(
            files, inode, runtime::builtin_fault_point::file_close);
          const auto again = co_await drive.lifecycle(writer.close());
          take(assertions.outcome());
          require(
            !overlap && overlap.error().code() == errc::queue_full,
            "parked close accepted another cleanup owner");
          require(
            closed.has_value() == !fail && again.has_value() == !fail
              && wal_occurrences(
                   files, inode, runtime::builtin_fault_point::file_close)
                   == closes,
            "close lost its terminal result or retried native close");
          if (fail) {
              require(
                first_done.failure.failed() && second_done.failure.failed()
                  && first_done.failure.error()
                  && first_done.failure.error()->code() == errc::io_failure
                  && second_done.failure.error() == first_done.failure.error()
                  && flushed && flushed->failure.failed() && !flushed->receipt
                  && writer.progress()->reserved == cut.cursor()
                  && writer.progress()->write_complete == initial.write_complete
                  && writer.progress()->durable == initial.durable
                  && closed.error() == again.error(),
                "failed short write skipped dependents, reused coordinates or "
                "manufactured durability");
          } else {
              take(first_done.failure.outcome());
              take(second_done.failure.outcome());
              if (flushed) {
                  take(flushed->failure.outcome());
                  require(
                    flushed->receipt
                      && flushed->receipt->boundary().cursor()
                           == one.boundary.cursor(),
                    "later writes enlarged the captured barrier");
              }
              require(
                writer.progress()->durable == cut.cursor()
                  && writer.progress()->write_complete == cut.cursor()
                  && !writer.failure().failed(),
                "graceful close failed to cover accepted work");
              if (rotate)
                  require(
                    take(control.snapshot()).fields.wal_head == head
                      && writer.statistics().rotations == 0,
                    "shutdown activated a successor or poisoned the old "
                    "barrier");
              const auto writes = wal_occurrences(
                                    files,
                                    inode,
                                    runtime::builtin_fault_point::file_write)
                                  - initial_writes;
              require(
                writes >= (native_max_length == 4096 ? 6U : (rotate ? 2U : 3U)),
                "record-size versus physical-chunk coverage did not execute");
              auto expected
                = storage::testing::wal_writer_contract::header_bytes(
                  spec, 0, head.incarnation);
              expected += append_contract::prepare_bytes(
                first_bytes, head, spec.owner.cluster(), 8192);
              expected += append_contract::prepare_bytes(
                second_bytes[0], head, spec.owner.cluster(), 16384);
              expected += append_contract::prepare_bytes(
                third_bytes[0], head, spec.owner.cluster(), 24576);
              require(
                (co_await read_bytes(
                  files,
                  take(take(local_paths::make(spec.root))
                         .wal(0, head.incarnation)),
                  drive))
                  == expected,
                "short-write recovery or queued gather changed independent "
                "bytes");
          }
      },
      fail);
}
} // namespace

SEASTAR_TEST_CASE(wal_writer_close_drains_parked_writes_and_abandons_rotation) {
    co_await with_wal_environment(
      config(), [](auto& env, auto& budget, auto drive) {
          return parked_close_history(env, budget, drive, false, true);
      });
}

SEASTAR_TEST_CASE(
  wal_writer_short_and_zero_native_writes_through_joined_close) {
    for (const auto memory : {4096U, 8192U}) {
        for (const auto maximum : {4096U, 131072U}) {
            const auto targets = co_await append_targets(memory, maximum);
            for (const auto short_bytes : {0U, 1024U, 4096U}) {
                const auto selected = targets.write;
                const auto rule = take(
                  fault_rule::make(
                    take(fault_rule_id::make(2)),
                    runtime::builtin_fault_point::file_write,
                    runtime::fault_object_key::from_u64(selected.object),
                    take(runtime::fault_occurrence::make(selected.occurrence)),
                    take(runtime::fault_occurrence::make(selected.occurrence)),
                    fault_selector::once(),
                    runtime::fault_decision::make_short_operation(
                      byte_count{short_bytes})));
                co_await with_wal_environment(
                  config(
                    std::nullopt,
                    runtime::fault_action::file_failure_before_effect,
                    1,
                    {},
                    {},
                    rule,
                    memory,
                    maximum),
                  [short_bytes, maximum](auto& env, auto& budget, auto drive) {
                      return parked_close_history(
                        env,
                        budget,
                        drive,
                        short_bytes != 4096,
                        false,
                        maximum);
                  });
            }
        }
    }
}

SEASTAR_TEST_CASE(
  wal_writer_rejects_incompatible_reopened_geometry_before_head_activation) {
    co_await with_wal_environment(
      config(
        std::nullopt,
        runtime::fault_action::file_failure_before_effect,
        1,
        {},
        {},
        {},
        4096,
        131072,
        8192),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          static_cast<void>(co_await bootstrap_history(
            env, budget, drive, true, false, false, false, false, false, true));
      });
}

SEASTAR_TEST_CASE(
  wal_writer_fake_completion_reentry_reuses_slot_and_joins_close) {
    co_await with_wal_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto spec = specification(
            take(runtime::file_path::make("/kwaque/store")), {1, 1});
          const std::array specs{spec};
          ownership_input owner{specs};
          take(co_await drive.lifecycle(files.create_directories(spec.root)));
          co_await storage::testing::wal_lifecycle_contract::completion_reentry(
            files, owner, spec, budget, drive);
      });
}

namespace {
namespace writer_contract = storage::testing::wal_writer_contract;
using storage::testing::wal_survival_oracle;

enum class wal_history_boundary { stepped, completed, parked };
template<typename T>
seastar::future<wal_history_boundary>
wal_history_step(scheduler& events, seastar::future<T>& pending) {
    if (pending.available()) co_return wal_history_boundary::completed;
    co_await drain_reactor_tasks();
    if (pending.available()) co_return wal_history_boundary::completed;
    if (events.pending_events()) {
        if (!events.has_ready_events()) take(events.advance_to_next());
        require(take(events.step()), "WAL history failed to step");
        co_return wal_history_boundary::stepped;
    }
    co_return wal_history_boundary::parked;
}

local_wal_head
independent_head(model::wal_incarnation_id id, std::string_view bytes) {
    codec::sha256_hasher hash;
    hash.update(bytes.data(), bytes.size());
    return {id, codec::immutable_object_digest{std::move(hash).final()}};
}
std::string control_image(
  std::uint64_t generation,
  std::uint64_t high,
  std::optional<local_wal_head> head) {
    auto bytes = storage::testing::local_fixture::read("control_empty");
    storage::testing::put(bytes, 88, generation, 8);
    const auto high_water = high == 0
                              ? local_wal_high{}
                              : local_wal_high{}.checked_advance(high).value();
    const auto mark = high_water.bytes();
    for (std::size_t i = 0; i < mark.size(); ++i)
        bytes[104 + i] = std::bit_cast<char>(mark[i]);
    if (head) {
        storage::testing::put(bytes, 96, 96, 4);
        storage::testing::put(bytes, 100, 4096 - 32 - 72 - 96, 4);
        bytes[148] = '\1';
        const auto id = head->incarnation.bytes();
        for (std::size_t i = 0; i < id.size(); ++i)
            bytes[152 + i] = std::bit_cast<char>(id[i]);
        const auto hash = head->header_digest.bytes();
        std::copy(hash.begin(), hash.end(), bytes.begin() + 168);
    }
    storage::testing::repair(bytes);
    return bytes;
}

// Crash the real owner at every scheduled boundary, including the interval
// after native persistence and before the caller consumes its result.
seastar::future<std::size_t> wal_namespace_history(
  environment& env,
  workload_budget& budget,
  simulation::testing::scheduler_driver drive,
  bool rotation,
  std::optional<std::size_t> cut,
  bool expect_failure = false,
  bool expect_parked = false) {
    auto& files = env.file_system();
    const auto spec = specification(
      take(runtime::file_path::make("/kwaque/store")), {1, 1});
    const std::array specs{spec};
    ownership_input owner{specs};
    take(co_await drive.lifecycle(files.create_directories(spec.root)));
    take(
      fake_file_test_access::sync_directory(
        files, take(fake_file_test_access::resolve(files, "/kwaque"))));
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await storage::testing::installation_contract::bootstrap(
      files, owner, spec, budget, work, drive);
    using writer_type = wal_writer<fake_file_system, ownership_input>;
    std::unique_ptr<writer_type::control_type> control;
    std::unique_ptr<writer_type::allocator_type> ids;
    std::unique_ptr<writer_type> writer;
    const auto first_id
      = local_wal_high{}.checked_advance(1)->incarnation().value();
    const auto next_id
      = local_wal_high{}.checked_advance(5)->incarnation().value();
    const auto first_header = writer_contract::header_bytes(spec, 0, first_id);
    const auto first_head = independent_head(first_id, first_header);
    const auto old_end
      = local_wal_cursor::make(first_id, runtime::file_position{16384}).value();
    const auto next_header = writer_contract::header_bytes(
      spec, 0, next_id, old_end);
    const auto next_head = independent_head(next_id, next_header);
    std::optional<seastar::future<runtime::result<void>>> pending;
    std::size_t steps = 0;
    bool succeeded = false;
    runtime::first_failure failed;
    try {
        control = take(
          co_await drive.lifecycle(
            writer_type::control_type::open(
              files, owner, spec, 0, false, budget, limits(), work)));
        ids = take(writer_type::allocator_type::make(*control, budget, 4));
        writer = take(
          writer_type::make(
            *control,
            *ids,
            budget,
            writer_contract::configuration(),
            wal_start_intent::known_unactivated));
        if (rotation) {
            take(co_await drive.lifecycle(writer->bootstrap(work)));
            auto group = co_await rotation_contract::submit_one(
              *writer, budget, spec.owner.cluster(), work);
            auto done = co_await drive.lifecycle(std::move(group.written));
            take(done.failure.outcome());
            auto durable = co_await drive.lifecycle(
              writer->barrier(group.boundary));
            take(durable.failure.outcome());
            require(
              durable.receipt.has_value(), "old WAL lacks observed barrier");
            // Exhaust this block without creating files. The successor must
            // refill and link over three legitimately unused incarnations.
            for (unsigned i = 0; i != 3; ++i)
                static_cast<void>(
                  take(co_await drive.lifecycle(ids->allocate_wal(work))));
            pending.emplace(
              writer->rotate(group.boundary, byte_count{8192}, work));
        } else {
            pending.emplace(writer->bootstrap(work));
        }
        while ((!cut || steps < *cut) && steps < 512) {
            const auto boundary = co_await wal_history_step(
              env.event_scheduler(), *pending);
            if (boundary == wal_history_boundary::completed) break;
            if (boundary == wal_history_boundary::parked) {
                require(
                  expect_parked,
                  "namespace history parked without a selected fault");
                break;
            }
            ++steps;
        }
        require(steps < 512, "namespace history exceeded event cap");
        if (!cut && !expect_parked) {
            require(pending->available(), "namespace history did not complete");
            auto result = co_await std::move(*pending);
            pending.reset();
            succeeded = result.has_value();
            require(
              succeeded != expect_failure,
              "namespace fault did not produce its expected result");
        } else if (expect_parked) {
            require(
              !pending->available(), "lost namespace notification completed");
        }
    } catch (...) {
        failed.observe(std::current_exception());
    }
    if (writer) writer->request_stop();
    take(co_await drive.lifecycle(files.crash()));
    if (pending) {
        try {
            auto result = co_await drive.lifecycle(std::move(*pending));
            succeeded = result.has_value();
            if (expect_parked)
                require(
                  !succeeded, "lost notification became success after crash");
        } catch (...) {
            failed.observe(std::current_exception());
        }
        pending.reset();
    }
    // Stale native handles are joined; their expected crash/close errors are
    // not durability evidence and cannot trigger a repair or a fresh writer.
    if (writer) {
        try {
            static_cast<void>(co_await drive.lifecycle(writer->close()));
        } catch (...) {
            failed.observe(std::current_exception());
        }
        writer.reset();
    }
    if (ids) {
        failed.observe(co_await drive.lifecycle(ids->close()));
        ids.reset();
    }
    if (control) {
        failed.observe(co_await drive.lifecycle(control->close()));
        control.reset();
    }
    take(failed.outcome());
    const auto paths = take(local_paths::make(spec.root));
    const auto control_path = take(paths.control(0));
    const auto recovered = co_await read_bytes(files, control_path, drive);
    const auto final = rotation ? control_image(5, 8, next_head)
                                : control_image(3, 4, first_head);
    const bool allowed = rotation
                           ? recovered == control_image(3, 4, first_head)
                               || recovered == control_image(4, 8, first_head)
                               || recovered == final
                           : recovered == control_image(1, 0, {})
                               || recovered == control_image(2, 4, {})
                               || recovered == final;
    require(
      allowed && (!succeeded || recovered == final),
      "crash selected an impossible control image");
    const auto old_file = first_header + (rotation
      ? append_contract::prepare_bytes(storage::testing::assigned_wire(true, 48), first_head, spec.owner.cluster(), 8192)
      : std::string{});
    for (unsigned restart = 0; restart != 2; ++restart) {
        auto selected = take(
          co_await drive.lifecycle(load_local_control(
            files, spec, 0, control_path, budget, limits(), work)));
        const auto fields = std::get<local_shard_control>(
          selected.value.payload());
        const wal_inventory_snapshot snapshot{
          take(spec.shard_owner(0)),
          {selected.value.header().generation(), fields}};
        unsigned chain = 0;
        auto visit = [&](const local_wal_chain_entry& entry)
          -> seastar::future<runtime::result<bool>> {
            const auto& descriptor = std::get<local_wal_descriptor>(
              entry.record.value.payload());
            const bool newest = descriptor.incarnation == next_id;
            require(
              descriptor.incarnation == first_id || (rotation && newest),
              "inventory elected an unselected incarnation");
            require(
              descriptor.predecessor
                == (newest ? std::optional{old_end} : std::nullopt),
              "inventory changed predecessor membership");
            const auto wire = co_await read_bytes(
              files, take(paths.wal(0, descriptor.incarnation)), drive);
            require(
              wire == (newest ? next_header : old_file),
              "selected WAL lost independently expected bytes");
            ++chain;
            co_return true;
        };
        auto names =
          [](const local_discovered_record&, local_cleanup_observation) {
              return seastar::make_ready_future<runtime::result<bool>>(true);
          };
        const auto intent = fields.wal_head
                              ? wal_inventory_intent::require_head
                              : wal_inventory_intent::known_unactivated;
        auto inventory = take(
          co_await drive.lifecycle(inspect_wal_inventory(
            files,
            owner,
            spec,
            snapshot,
            {},
            intent,
            budget,
            limits(),
            work,
            visit,
            names)));
        require(
          inventory.chain.complete && inventory.names.complete
            && chain
                 == (fields.wal_head ? (fields.wal_head == next_head ? 2U : 1U) : 0U),
          "reopened inventory lost the selected chain");
        if (restart == 0) take(co_await drive.lifecycle(files.crash()));
        require(
          (co_await read_bytes(files, control_path, drive)) == recovered,
          "second crash changed selected control bytes");
    }
    require(
      budget.snapshot().tasks == 0 && budget.snapshot().bytes == 0,
      "namespace history retained operation admission");
    co_return steps;
}
} // namespace

SEASTAR_TEST_CASE(wal_writer_crashes_at_every_creation_and_rotation_boundary) {
    for (unsigned mask = 0; mask != 8; ++mask) {
        const fake_crash_policy policy{
          .data_percent = static_cast<std::uint8_t>((mask & 1U) ? 100 : 0),
          .namespace_percent = static_cast<std::uint8_t>((mask & 2U) ? 100 : 0),
          .eof_percent = static_cast<std::uint8_t>((mask & 4U) ? 100 : 0)};
        for (bool rotation : {false, true}) {
            std::size_t count = 0;
            co_await with_wal_environment(
              config({}, runtime::fault_action::error, 1, {}, policy),
              [&](auto& env, auto& budget, auto drive) -> seastar::future<> {
                  count = co_await wal_namespace_history(
                    env, budget, drive, rotation, {});
              });
            require(
              count != 0 && count < 512, "namespace history lacked boundaries");
            for (std::size_t cut = 0; cut <= count; ++cut) {
                BOOST_TEST_CONTEXT(
                  "survival=" << mask << " rotation=" << rotation
                              << " cut=" << cut) {
                    co_await with_wal_environment(
                      config({}, runtime::fault_action::error, 1, {}, policy),
                      [=](auto& env, auto& budget, auto drive)
                        -> seastar::future<> {
                          static_cast<void>(co_await wal_namespace_history(
                            env, budget, drive, rotation, cut));
                      });
                }
            }
        }
    }
}

SEASTAR_TEST_CASE(
  wal_survival_observer_keeps_later_attempts_after_earlier_sync) {
    wal_survival_oracle oracle{"header"};
    oracle.attempted("first");
    oracle.written(11);
    oracle.attempted("second");
    oracle.synced(11);
    BOOST_CHECK_EQUAL(oracle.minimum(), 11U);
    BOOST_CHECK_EQUAL(oracle.possible(), 17U);
    BOOST_CHECK_EQUAL(oracle.written(), 11U);
    BOOST_CHECK(oracle.allows("headerfirst"));
    BOOST_CHECK(oracle.allows("headerfirstsecond"));
    BOOST_CHECK(
      oracle.allows(std::string{"headerfirstsec"} + std::string(3, '\0')));
    BOOST_CHECK(!oracle.allows("headerfirs"));
    BOOST_CHECK(!oracle.allows("headerFirst"));
    BOOST_CHECK(!oracle.allows("headerfirstjunk"));
    BOOST_CHECK(!oracle.allows("headerfirstsecondextra"));
    co_return;
}

namespace {
enum class survival_history {
    captured_sync,
    partial_gather,
    failed_sync,
    lost_write,
    close_after_sync
};
seastar::future<> wal_data_history(
  environment& env,
  workload_budget& budget,
  simulation::testing::scheduler_driver drive,
  survival_history mode) {
    auto& files = env.file_system();
    const auto spec = specification(
      take(runtime::file_path::make("/kwaque/store")), {1, 1});
    const std::array specs{spec};
    ownership_input owner{specs};
    take(co_await drive.lifecycle(files.create_directories(spec.root)));
    take(
      fake_file_test_access::sync_directory(
        files, take(fake_file_test_access::resolve(files, "/kwaque"))));
    co_await append_contract::with_writer(
      files,
      owner,
      spec,
      budget,
      drive,
      writer_contract::configuration(),
      [&](auto& writer, auto& work) -> seastar::future<> {
          const auto head = *writer.prepared_head();
          const auto path = take(
            take(local_paths::make(spec.root)).wal(0, head.incarnation));
          wal_survival_oracle oracle{
            writer_contract::header_bytes(spec, 0, head.incarnation)};
          const auto record = storage::testing::assigned_wire(true, 48);
          auto first = co_await rotation_contract::submit_one(
            writer, budget, spec.owner.cluster(), work);
          oracle.attempted(
            append_contract::prepare_bytes(
              record, head, spec.owner.cluster(), 8192));
          auto first_done = co_await drive.lifecycle(std::move(first.written));
          take(first_done.failure.outcome());
          require(
            first_done.written == byte_count{8192},
            "observed write changed whole-group bytes");
          oracle.written(16384);
          auto first_sync = co_await drive.lifecycle(
            writer.barrier(first.boundary));
          take(first_sync.failure.outcome());
          require(
            first_sync.receipt
              && first_sync.receipt->boundary().cursor()
                   == first.boundary.cursor(),
            "first barrier changed capture");
          oracle.synced(16384);
          const auto possible = oracle.possible();
          auto invalid = take(wal_group::make(budget, 1));
          auto rejected = co_await writer.submit(std::move(invalid), work);
          require(
            !rejected && oracle.possible() == possible
              && writer.progress()->reserved == first.boundary.cursor(),
            "preacceptance rejection raised possible survival");
          const std::array two_records{record, record};
          auto offered = co_await append_contract::offer(
            budget,
            spec.owner.cluster(),
            work,
            std::span<const std::string>{two_records}.first(
              mode == survival_history::captured_sync ? 1 : 2));
          auto second = take(co_await writer.submit(std::move(offered), work));
          oracle.attempted(append_contract::prepare_bytes(record, head, spec.owner.cluster(), 16384)
          + (mode == survival_history::captured_sync ? std::string{}
             : append_contract::prepare_bytes(record, head, spec.owner.cluster(), 24576)));
          std::optional<wal_submission> third;
          if (mode == survival_history::captured_sync) {
              auto more = co_await rotation_contract::submit_one(
                writer, budget, spec.owner.cluster(), work);
              oracle.attempted(
                append_contract::prepare_bytes(
                  record, head, spec.owner.cluster(), 24576));
              third.emplace(std::move(more));
          }
          std::optional<seastar::future<wal_barrier_outcome>> barrier;
          if (
            mode != survival_history::partial_gather
            && mode != survival_history::lost_write)
              barrier.emplace(writer.barrier(second.boundary));
          if (mode == survival_history::lost_write) {
              wal_history_boundary boundary = wal_history_boundary::stepped;
              unsigned steps = 0;
              while (boundary == wal_history_boundary::stepped && steps++ < 64)
                  boundary = co_await wal_history_step(
                    env.event_scheduler(), second.written);
              require(
                boundary == wal_history_boundary::parked,
                "lost write delivered a result");
              writer.request_stop();
              take(co_await drive.lifecycle(files.crash()));
          }
          auto second_done = co_await drive.lifecycle(
            std::move(second.written));
          if (
            mode == survival_history::partial_gather
            || mode == survival_history::lost_write) {
              require(
                second_done.failure.failed() && oracle.written() == 16384,
                "partial gather became a whole-write observation");
              require(
                writer.progress()->write_complete == first.boundary.cursor(),
                "failed gather advanced written prefix");
              if (mode == survival_history::partial_gather)
                  require(
                    second_done.failure.error()
                      && runtime::file_detail(*second_done.failure.error())
                           == runtime::file_failure_detail::no_space,
                    "partial gather lost no-space detail");
          } else {
              take(second_done.failure.outcome());
              oracle.written(
                mode == survival_history::captured_sync ? 24576 : 32768);
          }
          if (third) {
              auto done = co_await drive.lifecycle(std::move(third->written));
              take(done.failure.outcome());
              oracle.written(32768);
          }
          if (barrier) {
              auto result = co_await drive.lifecycle(std::move(*barrier));
              if (mode == survival_history::failed_sync) {
                  require(
                    result.failure.failed() && !result.receipt,
                    "failed sync certified bytes");
              } else {
                  take(result.failure.outcome());
                  require(
                    result.receipt
                      && result.receipt->boundary().cursor()
                           == second.boundary.cursor(),
                    "sync changed captured membership");
                  oracle.synced(
                    mode == survival_history::captured_sync ? 24576 : 32768);
              }
          }
          if (mode == survival_history::captured_sync)
              require(
                oracle.minimum() == 24576 && oracle.possible() == 32768
                  && writer.progress()->durable == second.boundary.cursor(),
                "earlier barrier erased later attempted history");
          if (mode == survival_history::close_after_sync) {
              auto closed = co_await drive.lifecycle(writer.close());
              require(
                !closed && closed.error().code() == errc::io_failure
                  && oracle.minimum() == 32768,
                "close error lost the prior successful sync");
          }
          writer.request_stop();
          if (mode != survival_history::lost_write)
              take(co_await drive.lifecycle(files.crash()));
          static_cast<void>(co_await drive.lifecycle(writer.close()));
          const auto recovered = co_await read_bytes(files, path, drive);
          require(
            oracle.allows(recovered),
            "crash image exceeded independent survival bounds");
          if (
            mode == survival_history::partial_gather
            && recovered.size() >= 24576)
              require(
                recovered.substr(0, 24576) == oracle.bytes().substr(0, 24576)
                  || recovered.substr(16384)
                       == std::string(recovered.size() - 16384, '\0'),
                "partial gather altered a completed member");
          take(co_await drive.lifecycle(files.crash()));
          require(
            (co_await read_bytes(files, path, drive)) == recovered,
            "second crash changed the recovered WAL image");
      },
      true);
}
} // namespace

SEASTAR_TEST_CASE(
  wal_writer_crash_survival_separates_write_sync_and_notification) {
    // At 4-KiB native chunks an 8-KiB complete envelope spans two writes.
    const auto targets = co_await append_targets(4096, 4096);
    for (unsigned mask = 0; mask != 8; ++mask) {
        const fake_crash_policy policy{
          .data_percent = static_cast<std::uint8_t>((mask & 1U) ? 100 : 0),
          .namespace_percent = static_cast<std::uint8_t>((mask & 2U) ? 100 : 0),
          .eof_percent = static_cast<std::uint8_t>((mask & 4U) ? 100 : 0)};
        for (auto mode :
             {survival_history::captured_sync,
              survival_history::partial_gather,
              survival_history::failed_sync,
              survival_history::lost_write,
              survival_history::close_after_sync}) {
            std::optional<fault_rule> rule;
            using point = runtime::builtin_fault_point;
            using action = runtime::fault_action;
            if (mode != survival_history::captured_sync) {
                const auto at = mode == survival_history::failed_sync
                                  ? point::file_flush
                                : mode == survival_history::close_after_sync
                                  ? point::file_close
                                  : point::file_write;
                const auto occurrence
                  = mode == survival_history::failed_sync
                      ? targets.flush.occurrence + 1
                    : mode == survival_history::close_after_sync
                      ? targets.close.occurrence
                      : targets.write.occurrence
                          + (mode == survival_history::partial_gather ? 4U : 2U);
                const auto decision
                  = mode == survival_history::lost_write
                      ? runtime::fault_decision::make_drop_completion()
                      : take(
                          runtime::fault_decision::make_file_failure(
                            mode == survival_history::partial_gather
                              ? action::file_failure_after_prefix
                            : mode == survival_history::failed_sync
                              ? action::file_failure_after_effect
                              : action::file_failure_before_effect,
                            mode == survival_history::partial_gather
                              ? runtime::file_failure_detail::no_space
                              : runtime::file_failure_detail::device_io,
                            byte_count{
                              mode == survival_history::partial_gather ? 512U
                                                                       : 0U}));
                rule.emplace(take(
                  fault_rule::make(
                    take(fault_rule_id::make(12)),
                    at,
                    runtime::fault_object_key::from_u64(targets.write.object),
                    take(runtime::fault_occurrence::make(occurrence)),
                    take(runtime::fault_occurrence::make(occurrence)),
                    fault_selector::once(),
                    decision)));
            }
            BOOST_TEST_CONTEXT(
              "survival=" << mask
                          << " history=" << static_cast<unsigned>(mode)) {
                co_await with_wal_environment(
                  config({}, action::error, 1, {}, policy, rule, 4096, 4096),
                  [mode](auto& env, auto& budget, auto drive) {
                      return wal_data_history(env, budget, drive, mode);
                  });
            }
        }
    }
}

SEASTAR_TEST_CASE(wal_writer_fake_pinned_headers) {
    co_await with_wal_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto spec = specification(
            take(runtime::file_path::make("/kwaque/store")), {1, 1});
          const std::array specs{spec};
          ownership_input owner{specs};
          take(co_await drive.lifecycle(files.create_directories(spec.root)));
          co_await storage::testing::wal_qualification_contract::pinned_headers(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(wal_writer_fake_bounded_admission) {
    co_await with_wal_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto spec = specification(
            take(runtime::file_path::make("/kwaque/store")), {1, 1});
          const std::array specs{spec};
          ownership_input owner{specs};
          take(co_await drive.lifecycle(files.create_directories(spec.root)));
          co_await storage::testing::wal_qualification_contract::
            bounded_admission(files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(
  wal_writer_creation_faults_reopen_without_fabricating_success) {
    bootstrap_targets targets;
    co_await with_wal_environment(
      config(), [&](auto& env, auto& budget, auto drive) -> seastar::future<> {
          targets = co_await bootstrap_history(env, budget, drive, false);
      });
    using point = runtime::builtin_fault_point;
    using action = runtime::fault_action;
    struct scenario {
        point at;
        fault_target target;
        action effect;
        runtime::file_failure_detail cause;
    };
    const std::array cases{
      scenario{
        point::file_write,
        targets.header,
        action::file_failure_before_effect,
        runtime::file_failure_detail::no_space},
      scenario{
        point::file_rename,
        targets.head,
        action::drop_completion,
        runtime::file_failure_detail::device_io},
      scenario{
        point::directory_cursor_close,
        targets.header_directory_close,
        action::file_failure_before_effect,
        runtime::file_failure_detail::device_io},
      scenario{
        point::directory_cursor_close,
        targets.head_directory_close,
        action::file_failure_before_effect,
        runtime::file_failure_detail::device_io}};
    for (unsigned mask = 0; mask != 8; ++mask) {
        const fake_crash_policy policy{
          .data_percent = static_cast<std::uint8_t>((mask & 1U) ? 100 : 0),
          .namespace_percent = static_cast<std::uint8_t>((mask & 2U) ? 100 : 0),
          .eof_percent = static_cast<std::uint8_t>((mask & 4U) ? 100 : 0)};
        for (const auto& fault : cases) {
            const auto decision
              = fault.effect == action::drop_completion
                  ? runtime::fault_decision::make_drop_completion()
                  : take(
                      runtime::fault_decision::make_file_failure(
                        fault.effect, fault.cause));
            const auto rule = take(
              fault_rule::make(
                take(fault_rule_id::make(13)),
                fault.at,
                runtime::fault_object_key::from_u64(fault.target.object),
                take(runtime::fault_occurrence::make(fault.target.occurrence)),
                take(runtime::fault_occurrence::make(fault.target.occurrence)),
                fault_selector::once(),
                decision));
            BOOST_TEST_CONTEXT(
              "survival=" << mask
                          << " point=" << static_cast<unsigned>(fault.at)) {
                co_await with_wal_environment(
                  config({}, action::error, 1, {}, policy, rule),
                  [&fault](
                    auto& env, auto& budget, auto drive) -> seastar::future<> {
                      static_cast<void>(co_await wal_namespace_history(
                        env,
                        budget,
                        drive,
                        false,
                        {},
                        true,
                        fault.effect == action::drop_completion));
                  });
            }
        }
    }
}

SEASTAR_TEST_CASE(wal_writer_fake_submission_allocation_cuts_join_owners) {
    for (std::size_t at = 0; at != 16; ++at)
        co_await with_wal_environment(
          config(),
          [at](auto& env, auto& budget, auto drive) -> seastar::future<> {
              auto& files = env.file_system();
              const auto spec = specification(
                take(runtime::file_path::make("/kwaque/store")), {1, 1});
              const std::array specs{spec};
              ownership_input owner{specs};
              take(
                co_await drive.lifecycle(files.create_directories(spec.root)));
              co_await storage::testing::wal_qualification_contract::
                submission_allocation_cut(
                  files, owner, spec, budget, drive, at);
          });
}

SEASTAR_TEST_CASE(wal_writer_fake_accepted_encoding_oom_joins_owners) {
    co_await with_wal_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto spec = specification(
            take(runtime::file_path::make("/kwaque/store")), {1, 1});
          const std::array specs{spec};
          ownership_input owner{specs};
          take(co_await drive.lifecycle(files.create_directories(spec.root)));
          co_await storage::testing::wal_qualification_contract::
            accepted_encoding_allocation_cut(files, owner, spec, budget, drive);
      });
}

namespace {
template<typename Func>
seastar::future<> with_cohort_environment(Func body, std::uint32_t tasks = 32) {
    co_await with_wal_environment(
      config(),
      [&](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto spec = specification(
            take(runtime::file_path::make("/kwaque/store")), {1, 1});
          const std::array specs{spec};
          ownership_input owner{specs};
          take(co_await drive.lifecycle(files.create_directories(spec.root)));
          co_await body(files, owner, spec, budget, drive, env.timer());
      },
      tasks);
}
} // namespace

SEASTAR_TEST_CASE(wal_group_commit_fake_formation_and_retained_capture) {
    co_await with_cohort_environment([](
                                       auto& files,
                                       auto& owner,
                                       const auto& spec,
                                       auto& budget,
                                       auto drive,
                                       auto&) {
        return storage::testing::wal_group_commit_contract::formation<
          simulation::monotonic_clock>(files, owner, spec, budget, drive);
    });
}

SEASTAR_TEST_CASE(wal_group_commit_fake_capacity_seals_forming_batch) {
    co_await with_cohort_environment([](
                                       auto& files,
                                       auto& owner,
                                       const auto& spec,
                                       auto& budget,
                                       auto drive,
                                       auto&) {
        return storage::testing::wal_group_commit_contract::
          capacity_seals_forming_batch<simulation::monotonic_clock>(
            files, owner, spec, budget, drive);
    });
}

SEASTAR_TEST_CASE(wal_group_commit_fake_count_byte_and_deadline_boundaries) {
    co_await with_cohort_environment([](
                                       auto& files,
                                       auto& owner,
                                       const auto& spec,
                                       auto& budget,
                                       auto drive,
                                       auto&) {
        return storage::testing::wal_group_commit_contract::boundaries<
          simulation::monotonic_clock>(files, owner, spec, budget, drive);
    });
}

SEASTAR_TEST_CASE(wal_group_commit_fake_timer_and_forced_drain) {
    co_await with_cohort_environment([](
                                       auto& files,
                                       auto& owner,
                                       const auto& spec,
                                       auto& budget,
                                       auto drive,
                                       auto& timer) {
        return storage::testing::wal_group_commit_contract::timers<
          simulation::monotonic_clock>(
          files, owner, spec, budget, drive, timer);
    });
}

SEASTAR_TEST_CASE(wal_group_commit_fake_preacceptance_allocation_cuts) {
    co_await with_cohort_environment([](
                                       auto& files,
                                       auto& owner,
                                       const auto& spec,
                                       auto& budget,
                                       auto drive,
                                       auto&) {
        return storage::testing::wal_group_commit_contract::allocation_cuts<
          simulation::monotonic_clock>(files, owner, spec, budget, drive);
    });
}

SEASTAR_TEST_CASE(
  wal_group_commit_fake_maximum_members_crosses_two_writer_groups) {
    co_await with_cohort_environment(
      [](
        auto& files,
        auto& owner,
        const auto& spec,
        auto& budget,
        auto drive,
        auto&) {
          return storage::testing::wal_group_commit_contract::maximum_members<
            simulation::monotonic_clock>(files, owner, spec, budget, drive);
      },
      256);
}

SEASTAR_TEST_CASE(wal_group_commit_failed_lower_write_joins_every_successor) {
    const auto targets = co_await append_targets();
    co_await with_wal_environment(
      config(
        runtime::builtin_fault_point::file_write,
        runtime::fault_action::file_failure_after_prefix,
        targets.write.occurrence,
        runtime::fault_object_key::from_u64(targets.write.object)),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto spec = specification(
            take(runtime::file_path::make("/kwaque/store")), {1, 1});
          const std::array specs{spec};
          ownership_input owner{specs};
          take(co_await drive.lifecycle(files.create_directories(spec.root)));
          co_await append_contract::with_writer(
            files,
            owner,
            spec,
            budget,
            drive,
            storage::testing::wal_writer_contract::configuration(),
            [&](auto& writer, auto& work) -> seastar::future<> {
                auto group_config
                  = storage::testing::wal_group_commit_contract::slow_batch();
                group_config.target_members = 1;
                co_await storage::testing::wal_group_commit_contract::
                  with_groups(
                    writer,
                    budget,
                    drive,
                    group_config,
                    [&](auto& groups) -> seastar::future<> {
                        const std::array records{
                          storage::testing::assigned_wire()};
                        auto one = co_await append_contract::offer(
                          budget, spec.owner.cluster(), work, records);
                        auto two = co_await append_contract::offer(
                          budget, spec.owner.cluster(), work, records);
                        auto three = co_await append_contract::offer(
                          budget, spec.owner.cluster(), work, records);
                        auto first = take(
                          co_await groups
                            .template submit<simulation::monotonic_clock>(
                              writer, std::move(one), work));
                        auto second = take(
                          co_await groups
                            .template submit<simulation::monotonic_clock>(
                              writer, std::move(two), work));
                        auto third = take(
                          co_await groups
                            .template submit<simulation::monotonic_clock>(
                              writer, std::move(three), work));
                        const auto accepted = writer.progress()->reserved;
                        runtime::first_failure original;
                        for (unsigned i = 0; i != 3; ++i) {
                            auto capture = groups.capture(
                              simulation::monotonic_clock::now());
                            require(
                              capture && capture->groups() == 1,
                              "failure chain lost a queued cohort");
                            auto failed = co_await drive.lifecycle(
                              groups.join_written(*capture));
                            require(
                              failed.error().has_value(),
                              "failed lower write manufactured success");
                            if (i == 0)
                                original = failed;
                            else
                                require(
                                  failed.error()->code()
                                    == original.error()->code(),
                                  "successor lost the first typed failure");
                            take(groups.retire_written(*capture));
                        }
                        require(
                          groups.queued_groups() == 0
                            && writer.progress()->reserved == accepted
                            && writer.statistics().flush_calls == 0,
                          "failed cohort left an observer, reused coordinates "
                          "or flushed");
                    },
                    true);
            },
            true);
      });
}

SEASTAR_TEST_CASE(wal_group_commit_fake_accepted_encoding_exception_is_joined) {
    co_await with_cohort_environment([](
                                       auto& files,
                                       auto& owner,
                                       const auto& spec,
                                       auto& budget,
                                       auto drive,
                                       auto&) {
        return storage::testing::wal_group_commit_contract::
          accepted_encoding_failure<simulation::monotonic_clock>(
            files, owner, spec, budget, drive);
    });
}

SEASTAR_TEST_CASE(wal_group_commit_fake_durable_completion) {
    co_await with_cohort_environment([](
                                       auto& files,
                                       auto& owner,
                                       const auto& spec,
                                       auto& budget,
                                       auto drive,
                                       auto&) {
        return storage::testing::wal_durability_contract::completion<
          simulation::monotonic_clock>(files, owner, spec, budget, drive);
    });
}

SEASTAR_TEST_CASE(wal_group_commit_fake_durable_retained_pressure) {
    co_await with_cohort_environment([](
                                       auto& files,
                                       auto& owner,
                                       const auto& spec,
                                       auto& budget,
                                       auto drive,
                                       auto&) {
        return storage::testing::wal_durability_contract::retained_pressure<
          simulation::monotonic_clock>(files, owner, spec, budget, drive);
    });
}

SEASTAR_TEST_CASE(wal_group_commit_fake_durable_observer_admission) {
    co_await with_cohort_environment([](
                                       auto& files,
                                       auto& owner,
                                       const auto& spec,
                                       auto& budget,
                                       auto drive,
                                       auto&) {
        return storage::testing::wal_durability_contract::observer_admission<
          simulation::monotonic_clock>(files, owner, spec, budget, drive);
    });
}

SEASTAR_TEST_CASE(wal_group_commit_fake_durable_result_allocation_cuts) {
    co_await with_cohort_environment([](
                                       auto& files,
                                       auto& owner,
                                       const auto& spec,
                                       auto& budget,
                                       auto drive,
                                       auto&) {
        return storage::testing::wal_durability_contract::
          result_allocation_cuts<simulation::monotonic_clock>(
            files, owner, spec, budget, drive);
    });
}

SEASTAR_TEST_CASE(wal_group_commit_fake_durable_close_during_flush) {
    co_await with_cohort_environment([](
                                       auto& files,
                                       auto& owner,
                                       const auto& spec,
                                       auto& budget,
                                       auto drive,
                                       auto&) {
        return storage::testing::wal_durability_contract::close_during_flush<
          simulation::monotonic_clock>(files, owner, spec, budget, drive);
    });
}

SEASTAR_TEST_CASE(wal_group_commit_later_write_progress_during_captured_flush) {
    const auto targets = co_await append_targets();
    const auto delay = take(
      fault_rule::make(
        take(fault_rule_id::make(2)),
        runtime::builtin_fault_point::file_flush,
        runtime::fault_object_key::from_u64(targets.flush.object),
        take(runtime::fault_occurrence::make(targets.flush.occurrence)),
        take(runtime::fault_occurrence::make(targets.flush.occurrence)),
        fault_selector::once(),
        runtime::fault_decision::make_delay(
          runtime::monotonic_duration{1'000'000})));
    co_await with_wal_environment(
      config({}, runtime::fault_action::error, 1, {}, {}, delay),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto spec = specification(
            take(runtime::file_path::make("/kwaque/store")), {1, 1});
          const std::array specs{spec};
          ownership_input owner{specs};
          take(co_await drive.lifecycle(files.create_directories(spec.root)));
          co_await append_contract::with_writer(
            files,
            owner,
            spec,
            budget,
            drive,
            storage::testing::wal_writer_contract::configuration(),
            [&](auto& writer, auto& work) -> seastar::future<> {
                auto cfg
                  = storage::testing::wal_group_commit_contract::slow_batch();
                cfg.target_members = 1;
                co_await storage::testing::wal_group_commit_contract::
                  with_groups(
                    writer,
                    budget,
                    drive,
                    cfg,
                    [&](auto& groups) -> seastar::future<> {
                        const std::array records{
                          storage::testing::assigned_wire()};
                        auto one = co_await append_contract::offer(
                          budget, spec.owner.cluster(), work, records);
                        auto two = co_await append_contract::offer(
                          budget, spec.owner.cluster(), work, records);
                        const auto durable_before = writer.progress()->durable;
                        const auto submitted = fake_file_test_access::submitted(
                          files, fake_submission_kind::flush);
                        auto first = take(
                          co_await groups
                            .template submit<simulation::monotonic_clock>(
                              writer, std::move(one), work));
                        auto waiting = take(first.observe());
                        auto capture = groups.capture(
                          simulation::monotonic_clock::now());
                        require(capture.has_value(), "missing first capture");
                        take(groups.flush(writer, *capture));
                        take(groups.flush(writer, *capture));
                        co_await drain_reactor_tasks();
                        require(
                          !waiting.available()
                            && writer.statistics().flush_calls == 0,
                          "flush overtook a lower incomplete write");
                        // Advance only bounded write-completion ticks. A
                        // generic pump-until waiter could jump to the delayed
                        // flush while a native notification continuation is
                        // still queued.
                        for (unsigned step = 0;
                             step != 64
                             && fake_file_test_access::submitted(
                                  files, fake_submission_kind::flush)
                                  != submitted + 1;
                             ++step) {
                            co_await drain_reactor_tasks();
                            if (
                              fake_file_test_access::submitted(
                                files, fake_submission_kind::flush)
                              == submitted + 1)
                                break;
                            take(env.event_scheduler().run_until(
                              env.event_scheduler()
                                .now()
                                .checked_add(runtime::monotonic_duration{1})
                                .value()));
                        }
                        require(
                          fake_file_test_access::submitted(
                            files, fake_submission_kind::flush)
                              == submitted + 1
                            && !waiting.available(),
                          "delayed flush was not observed before its scheduled "
                          "effect");
                        auto second = take(
                          co_await groups
                            .template submit<simulation::monotonic_clock>(
                              writer, std::move(two), work));
                        auto later = take(second.observe());
                        for (unsigned step = 0;
                             step != 64
                             && writer.progress()->write_complete
                                  != second.boundary().cursor();
                             ++step) {
                            co_await drain_reactor_tasks();
                            take(env.event_scheduler().run_until(
                              env.event_scheduler()
                                .now()
                                .checked_add(runtime::monotonic_duration{1})
                                .value()));
                        }
                        co_await drain_reactor_tasks();
                        require(
                          writer.progress()->write_complete
                              == second.boundary().cursor()
                            && writer.progress()->durable == durable_before
                            && !waiting.available() && !later.available(),
                          "later writes stalled on an earlier flush or gained "
                          "incidental certification");
                        auto done = co_await drive.lifecycle(
                          std::move(waiting));
                        take(done.failure().outcome());
                        require(
                          done.receipt()
                            && done.receipt()->boundary() == first.boundary()
                            && writer.progress()->durable
                                 == first.boundary().cursor()
                            && !later.available(),
                          "first receipt expanded to the later write");
                        take(groups.flush(writer, *capture));
                        auto next = groups.capture(
                          simulation::monotonic_clock::now());
                        require(next.has_value(), "later capture lost");
                        take(groups.flush(writer, *next));
                        auto second_done = co_await drive.lifecycle(
                          std::move(later));
                        take(second_done.failure().outcome());
                        require(
                          second_done.receipt()
                            && second_done.receipt()->boundary()
                                 == second.boundary()
                            && writer.statistics().flush_calls == 2,
                          "separate captures did not require exactly two "
                          "physical barriers");
                    });
            });
      });
}

SEASTAR_TEST_CASE(
  wal_group_commit_failed_barrier_settles_every_captured_waiter) {
    const auto targets = co_await append_targets();
    using point = runtime::builtin_fault_point;
    using action = runtime::fault_action;
    for (const auto [at, effect] : std::array{
           std::pair{point::file_write, action::file_failure_after_prefix},
           std::pair{point::file_flush, action::file_failure_before_effect},
           std::pair{point::file_flush, action::file_failure_after_effect}}) {
        const auto selected = at == point::file_write ? targets.write
                                                      : targets.flush;
        const bool earlier_success = effect
                                     == action::file_failure_after_effect;
        co_await with_wal_environment(
          config(
            at,
            effect,
            selected.occurrence + static_cast<std::uint64_t>(earlier_success),
            runtime::fault_object_key::from_u64(selected.object)),
          [at, earlier_success](
            auto& env, auto& budget, auto drive) -> seastar::future<> {
              auto& files = env.file_system();
              const auto spec = specification(
                take(runtime::file_path::make("/kwaque/store")), {1, 1});
              const std::array specs{spec};
              ownership_input owner{specs};
              take(
                co_await drive.lifecycle(files.create_directories(spec.root)));
              co_await append_contract::with_writer(
                files,
                owner,
                spec,
                budget,
                drive,
                storage::testing::wal_writer_contract::configuration(),
                [&](auto& writer, auto& work) -> seastar::future<> {
                    auto cfg = storage::testing::wal_group_commit_contract::
                      slow_batch();
                    cfg.target_members = 1;
                    co_await storage::testing::wal_group_commit_contract::
                      with_groups(
                        writer,
                        budget,
                        drive,
                        cfg,
                        [&](auto& groups) -> seastar::future<> {
                            const std::array records{
                              storage::testing::assigned_wire()};
                            auto one = co_await append_contract::offer(
                              budget, spec.owner.cluster(), work, records);
                            auto two = co_await append_contract::offer(
                              budget, spec.owner.cluster(), work, records);
                            auto three = co_await append_contract::offer(
                              budget, spec.owner.cluster(), work, records);
                            const auto durable = writer.progress()->durable;
                            auto first = take(
                              co_await groups
                                .template submit<simulation::monotonic_clock>(
                                  writer, std::move(one), work));
                            auto second = take(
                              co_await groups
                                .template submit<simulation::monotonic_clock>(
                                  writer, std::move(two), work));
                            auto third = take(
                              co_await groups
                                .template submit<simulation::monotonic_clock>(
                                  writer, std::move(three), work));
                            std::array<seastar::future<wal_commit_result>, 3>
                              waiters{
                                take(first.observe()),
                                take(second.observe()),
                                take(third.observe())};
                            auto duplicate = take(first.observe());
                            std::optional<wal_commit_result> preserved;
                            unsigned index = 0;
                            for (auto& waiter : waiters) {
                                auto captured = groups.capture(
                                  simulation::monotonic_clock::now());
                                require(
                                  captured.has_value(),
                                  "failed cohort lost membership");
                                take(groups.flush(writer, *captured));
                                auto result = co_await drive.lifecycle(
                                  std::move(waiter));
                                if (earlier_success && index++ == 0) {
                                    take(result.failure().outcome());
                                    require(
                                      result.receipt().has_value(),
                                      "initial successful barrier lacked "
                                      "a receipt");
                                    preserved.emplace(std::move(result));
                                    continue;
                                }
                                require(
                                  result.failure().error().has_value()
                                    && !result.receipt()
                                    && writer.failure().error()
                                    && result.failure().error()->code()
                                         == writer.failure().error()->code(),
                                  "failed predecessor or sync "
                                  "manufactured a "
                                  "successful result");
                            }
                            auto repeated = co_await drive.lifecycle(
                              std::move(duplicate));
                            if (earlier_success) {
                                require(
                                  preserved && !preserved->failure().failed()
                                    && preserved->receipt()
                                    && !repeated.failure().failed()
                                    && repeated.receipt()
                                    && repeated.receipt()->boundary()
                                         == first.boundary(),
                                  "later failure revoked an earlier receipt");
                            } else {
                                require(
                                  repeated.failure().failed()
                                    && !repeated.receipt(),
                                  "failed cohort certified duplicate interest");
                            }
                            require(
                              groups.queued_groups() == 0,
                              "failed cohort stranded a waiter");
                            const auto expected_durable
                              = earlier_success ? first.boundary().cursor()
                                                : durable;
                            require(
                              writer.progress()->durable == expected_durable,
                              "failed cohort changed the durable boundary");
                            const auto expected_flushes
                              = at == point::file_flush
                                  ? (earlier_success ? 2U : 1U)
                                  : 0U;
                            require(
                              writer.statistics().flush_calls
                                == expected_flushes,
                              "failed sync was retried");
                        },
                        true);
                },
                true);
          });
    }
}

SEASTAR_TEST_CASE(wal_group_commit_fake_durable_stale_capture) {
    co_await with_cohort_environment([](
                                       auto& files,
                                       auto& owner,
                                       const auto& spec,
                                       auto& budget,
                                       auto drive,
                                       auto&) {
        return storage::testing::wal_durability_contract::stale_capture<
          simulation::monotonic_clock>(files, owner, spec, budget, drive);
    });
}

SEASTAR_TEST_CASE(wal_group_commit_fake_durable_observer_chunk_boundaries) {
    co_await with_cohort_environment([](
                                       auto& files,
                                       auto& owner,
                                       const auto& spec,
                                       auto& budget,
                                       auto drive,
                                       auto&) {
        return storage::testing::wal_durability_contract::
          observer_chunk_boundaries<simulation::monotonic_clock>(
            files, owner, spec, budget, drive);
    });
}

namespace {
// Drain native continuations before advancing virtual time. An observer timer
// must not win merely because its notification is several native tasks deep.
struct commit_lifecycle_driver final {
    scheduler& events;
    template<typename T>
    seastar::future<T> lifecycle(seastar::future<T> waiting) const {
        for (unsigned steps = 0; !waiting.available(); ++steps) {
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
seastar::future<> with_commit_environment(Func body) {
    co_await with_wal_environment(
      config(), [&](auto& env, auto& budget, auto) -> seastar::future<> {
          commit_lifecycle_driver drive{env.event_scheduler()};
          auto& files = env.file_system();
          const auto spec = specification(
            take(runtime::file_path::make("/kwaque/store")), {1, 1});
          const std::array specs{spec};
          ownership_input owner{specs};
          take(co_await drive.lifecycle(files.create_directories(spec.root)));
          co_await body(files, owner, spec, budget, drive, env.timer());
      });
}
} // namespace

SEASTAR_TEST_CASE(wal_group_commit_fake_lifecycle_cancellation) {
    co_await with_commit_environment([](
                                       auto& files,
                                       auto& owner,
                                       const auto& spec,
                                       auto& budget,
                                       auto drive,
                                       auto& timer) {
        return storage::testing::wal_commit_lifecycle_contract::cancellation<
          simulation::monotonic_clock>(
          files, owner, spec, budget, drive, timer);
    });
}

SEASTAR_TEST_CASE(wal_group_commit_fake_lifecycle_deadlines) {
    co_await with_commit_environment([](
                                       auto& files,
                                       auto& owner,
                                       const auto& spec,
                                       auto& budget,
                                       auto drive,
                                       auto& timer) {
        return storage::testing::wal_commit_lifecycle_contract::deadlines<
          simulation::monotonic_clock>(
          files, owner, spec, budget, drive, timer);
    });
}

SEASTAR_TEST_CASE(wal_group_commit_fake_lifecycle_rotation) {
    co_await with_commit_environment([](
                                       auto& files,
                                       auto& owner,
                                       const auto& spec,
                                       auto& budget,
                                       auto drive,
                                       auto& timer) {
        return storage::testing::wal_commit_lifecycle_contract::rotation<
          simulation::monotonic_clock>(
          files, owner, spec, budget, drive, timer);
    });
}

SEASTAR_TEST_CASE(wal_group_commit_fake_lifecycle_timer_failure) {
    co_await with_commit_environment([](
                                       auto& files,
                                       auto& owner,
                                       const auto& spec,
                                       auto& budget,
                                       auto drive,
                                       auto&) {
        return storage::testing::wal_commit_lifecycle_contract::timer_failure<
          simulation::monotonic_clock>(files, owner, spec, budget, drive);
    });
}
SEASTAR_TEST_CASE(wal_group_commit_fake_lifecycle_environment_abort) {
    co_await with_wal_environment(
      config(), [](auto& env, auto& budget, auto) -> seastar::future<> {
          commit_lifecycle_driver drive{env.event_scheduler()};
          auto& files = env.file_system();
          const auto spec = specification(
            take(runtime::file_path::make("/kwaque/store")), {1, 1});
          const std::array specs{spec};
          ownership_input owner{specs};
          take(co_await drive.lifecycle(files.create_directories(spec.root)));
          auto& timer = env.timer();
          auto& source = env.tasks().abort_source();
          co_await storage::testing::wal_commit_lifecycle_contract::shutdown<
            simulation::monotonic_clock>(
            files, owner, spec, budget, drive, timer, source, [&] {
                env.request_abort();
                require(
                  !env.lifetime().acquire(),
                  "shutdown acquired a new runtime lease");
            });
      });
}

SEASTAR_TEST_CASE(wal_group_commit_fake_lifecycle_service) {
    co_await with_commit_environment([](
                                       auto& files,
                                       auto& owner,
                                       const auto& spec,
                                       auto& budget,
                                       auto drive,
                                       auto& timer) {
        return storage::testing::wal_commit_lifecycle_contract::service<
          simulation::monotonic_clock>(
          files, owner, spec, budget, drive, timer);
    });
}

SEASTAR_TEST_CASE(wal_group_commit_preserves_coordinator_and_storage_failures) {
    const auto targets = co_await append_targets();
    co_await with_wal_environment(
      config(
        runtime::builtin_fault_point::file_flush,
        runtime::fault_action::file_failure_after_effect,
        targets.flush.occurrence,
        runtime::fault_object_key::from_u64(targets.flush.object)),
      [](auto& env, auto& budget, auto) -> seastar::future<> {
          commit_lifecycle_driver drive{env.event_scheduler()};
          auto& files = env.file_system();
          const auto spec = specification(
            take(runtime::file_path::make("/kwaque/store")), {1, 1});
          const std::array specs{spec};
          ownership_input owner{specs};
          take(co_await drive.lifecycle(files.create_directories(spec.root)));
          co_await append_contract::with_writer(
            files,
            owner,
            spec,
            budget,
            drive,
            storage::testing::wal_writer_contract::configuration(),
            [&](auto& writer, auto& work) -> seastar::future<> {
                storage::testing::wal_commit_lifecycle_contract::failed_timer
                  timer;
                co_await storage::testing::wal_group_commit_contract::
                  with_groups(
                    writer,
                    budget,
                    drive,
                    storage::testing::wal_group_commit_contract::slow_batch(),
                    [&](auto& groups) -> seastar::future<> {
                        const std::array records{
                          storage::testing::assigned_wire()};
                        auto input = co_await append_contract::offer(
                          budget, spec.owner.cluster(), work, records);
                        auto ticket = take(
                          co_await groups
                            .template submit<simulation::monotonic_clock>(
                              writer, std::move(input), work));
                        const auto before = writer.progress()->durable;
                        auto observed = take(ticket.observe());
                        take(groups.template start<simulation::monotonic_clock>(
                          timer));
                        auto result = co_await drive.lifecycle(
                          std::move(observed));
                        require(
                          !result.receipt() && result.failure().error()
                            && result.failure().error()->code()
                                 == errc::resource_exhausted
                            && groups.coordinator_failure().error()
                            && groups.storage_failure().error()
                            && writer.failure().error()
                            && groups.storage_failure().error()->code()
                                 == writer.failure().error()->code()
                            && writer.progress()->durable == before
                            && writer.statistics().flush_calls == 1,
                          "timer cause masked a later failed sync or advanced "
                          "uncertified durability");
                    },
                    true);
            },
            true);
      });
}

SEASTAR_TEST_CASE(wal_group_commit_late_file_close_failure_preserves_receipt) {
    const auto targets = co_await append_targets();
    co_await with_wal_environment(
      config(
        runtime::builtin_fault_point::file_close,
        runtime::fault_action::file_failure_before_effect,
        targets.close.occurrence,
        runtime::fault_object_key::from_u64(targets.close.object)),
      [](auto& env, auto& budget, auto) -> seastar::future<> {
          commit_lifecycle_driver drive{env.event_scheduler()};
          auto& files = env.file_system();
          const auto spec = specification(
            take(runtime::file_path::make("/kwaque/store")), {1, 1});
          const std::array specs{spec};
          ownership_input owner{specs};
          take(co_await drive.lifecycle(files.create_directories(spec.root)));
          co_await append_contract::with_writer(
            files,
            owner,
            spec,
            budget,
            drive,
            storage::testing::wal_writer_contract::configuration(),
            [&](auto& writer, auto& work) -> seastar::future<> {
                co_await storage::testing::wal_group_commit_contract::
                  with_groups(
                    writer,
                    budget,
                    drive,
                    storage::testing::wal_group_commit_contract::slow_batch(),
                    [&](auto& groups) -> seastar::future<> {
                        const std::array records{
                          storage::testing::assigned_wire()};
                        auto input = co_await append_contract::offer(
                          budget, spec.owner.cluster(), work, records);
                        auto ticket = take(
                          co_await groups
                            .template submit<simulation::monotonic_clock>(
                              writer, std::move(input), work));
                        auto observed = take(ticket.observe());
                        take(co_await drive.lifecycle(groups.close()));
                        auto result = co_await drive.lifecycle(
                          std::move(observed));
                        auto closed = co_await drive.lifecycle(writer.close());
                        require(
                          !closed && writer.failure().failed()
                            && !result.failure().failed() && result.receipt()
                            && !groups.failure().failed(),
                          "later checked-close failure revoked an earned WAL "
                          "receipt");
                        auto cached = co_await take(ticket.observe());
                        require(
                          cached.receipt()
                            && cached.receipt()->boundary()
                                 == result.receipt()->boundary(),
                          "terminal receipt changed after checked-close "
                          "failure");
                    });
            },
            true);
      });
}

SEASTAR_TEST_CASE(wal_group_commit_fake_lifecycle_observer_failures) {
    co_await with_commit_environment([](
                                       auto& files,
                                       auto& owner,
                                       const auto& spec,
                                       auto& budget,
                                       auto drive,
                                       auto& timer) {
        return storage::testing::wal_commit_lifecycle_contract::
          observer_failures<simulation::monotonic_clock>(
            files, owner, spec, budget, drive, timer);
    });
}

SEASTAR_TEST_CASE(wal_group_commit_fake_qualification_delivery_permutations) {
    co_await with_commit_environment([](
                                       auto& files,
                                       auto& owner,
                                       const auto& spec,
                                       auto& budget,
                                       auto drive,
                                       auto&) {
        return storage::testing::wal_commit_qualification_contract::ordering<
          simulation::monotonic_clock>(files, owner, spec, budget, drive);
    });
}

SEASTAR_TEST_CASE(
  wal_group_commit_qualification_failed_successors_join_reordered_io) {
    const auto targets = co_await append_targets(4096, 4096);
    for (std::uint64_t delayed = 0; delayed != 3; ++delayed) {
        auto delay = take(
          fault_rule::make(
            take(fault_rule_id::make(2)),
            runtime::builtin_fault_point::file_write,
            runtime::fault_object_key::from_u64(targets.write.object),
            take(
              runtime::fault_occurrence::make(
                targets.write.occurrence + delayed)),
            take(
              runtime::fault_occurrence::make(
                targets.write.occurrence + delayed)),
            fault_selector::once(),
            runtime::fault_decision::make_delay(
              runtime::monotonic_duration{1'000'000})));
        BOOST_TEST_CONTEXT("delayed_chunk=" << delayed) {
            co_await with_wal_environment(
              config(
                runtime::builtin_fault_point::file_write,
                runtime::fault_action::file_failure_before_effect,
                targets.write.occurrence + 3,
                runtime::fault_object_key::from_u64(targets.write.object),
                {},
                delay,
                4096,
                4096),
              [](auto& env, auto& budget, auto) -> seastar::future<> {
                  commit_lifecycle_driver drive{env.event_scheduler()};
                  auto& files = env.file_system();
                  const auto spec = specification(
                    take(runtime::file_path::make("/kwaque/store")), {1, 1});
                  const std::array specs{spec};
                  ownership_input owner{specs};
                  take(
                    co_await drive.lifecycle(
                      files.create_directories(spec.root)));
                  co_await append_contract::with_writer(
                    files,
                    owner,
                    spec,
                    budget,
                    drive,
                    writer_contract::configuration(),
                    [&](auto& writer, auto& work) -> seastar::future<> {
                        auto cfg = storage::testing::wal_group_commit_contract::
                          slow_batch();
                        cfg.target_members = 1;
                        co_await storage::testing::wal_group_commit_contract::
                          with_groups(
                            writer,
                            budget,
                            drive,
                            cfg,
                            [&](auto& groups) -> seastar::future<> {
                                const auto inode = wal_object(
                                  files, spec, *writer.prepared_head());
                                const auto before_calls = wal_occurrences(
                                  files,
                                  inode,
                                  runtime::builtin_fault_point::file_write);
                                const auto record
                                  = storage::testing::assigned_wire();
                                const std::array records{
                                  record, record, record};
                                std::array<std::optional<wal_group>, 4> offers;
                                for (unsigned i = 0; i != offers.size(); ++i)
                                    offers[i].emplace(
                                      co_await append_contract::offer(
                                        budget,
                                        spec.owner.cluster(),
                                        work,
                                        std::span<const std::string>{records}
                                          .first(i == 0 ? 3 : 1)));
                                std::array<std::optional<wal_commit_ticket>, 4>
                                  tickets;
                                std::array<
                                  std::optional<
                                    seastar::future<wal_commit_result>>,
                                  4>
                                  waiters;
                                for (unsigned i = 0; i != tickets.size(); ++i) {
                                    tickets[i].emplace(take(
                                      co_await groups.template submit<
                                        simulation::monotonic_clock>(
                                        writer, std::move(*offers[i]), work)));
                                    offers[i].reset();
                                    waiters[i].emplace(
                                      take(tickets[i]->observe()));
                                }
                                const auto before_written
                                  = writer.progress()->write_complete;
                                const auto reserved
                                  = tickets.back()->boundary().cursor();
                                take(groups.template start<
                                     simulation::monotonic_clock>(env.timer()));
                                // Later physical operations return before the
                                // delayed lower one. The first logical write
                                // must remain owned.
                                const auto before = env.event_scheduler().now();
                                for (unsigned step = 0; step != 64; ++step) {
                                    co_await drain_reactor_tasks();
                                    if (
                                      files.pending_writes() == 1
                                      && wal_occurrences(
                                           files,
                                           inode,
                                           runtime::builtin_fault_point::
                                             file_write)
                                           >= before_calls + 4)
                                        break;
                                    take(env.event_scheduler().run_until(
                                      before
                                        .checked_add(
                                          runtime::monotonic_duration{
                                            step + 1U})
                                        .value()));
                                }
                                require(
                                  files.pending_writes() == 1
                                    && take(
                                         fake_file_test_access::
                                           verify_pending_write_buffers(files))
                                         == 1
                                    && !waiters[0]->available(),
                                  "delayed lower I/O was not retained after "
                                  "later completion");
                                for (auto& waiter : waiters) {
                                    auto result = co_await drive.lifecycle(
                                      std::move(*waiter));
                                    waiter.reset();
                                    require(
                                      result.failure().error()
                                        && writer.failure().error()
                                        && result.failure().error()->code()
                                             == writer.failure().error()->code()
                                        && !result.receipt(),
                                      "failed predecessor certified a "
                                      "successor");
                                }
                                require(
                                  groups.queued_groups() == 0
                                    && writer.statistics().flush_calls == 0
                                    && writer.progress()->reserved == reserved
                                    && files.pending_writes() == 0
                                    && writer.progress()->write_complete
                                         == before_written
                                    && writer.progress()->durable
                                         == before_written
                                    && writer.failure().error()
                                    && writer.failure().error()->code()
                                         == errc::io_failure
                                    && runtime::file_detail(
                                         *writer.failure().error())
                                         == runtime::file_failure_detail::
                                           device_io,
                                  "failed reordered write leaked work or "
                                  "reused accepted coordinates");
                            },
                            true);
                    },
                    true);
              },
              64);
        }
    }
}

namespace {
enum class commit_crash_cut {
    freeze,
    written,
    effect,
    receipt,
    delivery,
    detached,
    failed_sync,
    rotation
};

fake_inode_snapshot
commit_inode_snapshot(fake_file_system& files, std::uint64_t inode) {
    auto snapshot = take(fake_file_test_access::snapshot(files));
    for (auto& object : snapshot.objects)
        if (object.id == inode) return std::move(object);
    throw std::runtime_error("WAL inode missing from crash observation");
}
bool commit_has_prefix(
  const std::vector<std::byte>& bytes, std::string_view expected) {
    return bytes.size() >= expected.size()
           && std::equal(
             expected.begin(),
             expected.end(),
             bytes.begin(),
             [](char left, std::byte right) {
                 return std::bit_cast<std::byte>(left) == right;
             });
}

seastar::future<> commit_crash_history(
  environment& env, workload_budget& budget, commit_crash_cut at) {
    commit_lifecycle_driver drive{env.event_scheduler()};
    auto& files = env.file_system();
    const auto spec = specification(
      take(runtime::file_path::make("/kwaque/store")), {1, 1});
    const std::array specs{spec};
    ownership_input owner{specs};
    take(co_await drive.lifecycle(files.create_directories(spec.root)));
    take(
      fake_file_test_access::sync_directory(
        files, take(fake_file_test_access::resolve(files, "/kwaque"))));
    co_await append_contract::with_writer(
      files,
      owner,
      spec,
      budget,
      drive,
      writer_contract::configuration(),
      [&](auto& writer, auto& work) -> seastar::future<> {
          const auto head = *writer.prepared_head();
          const auto path = take(
            take(local_paths::make(spec.root)).wal(0, head.incarnation));
          const auto inode = wal_object(files, spec, head);
          wal_survival_oracle oracle{
            writer_contract::header_bytes(spec, 0, head.incarnation)};
          std::optional<wal_survival_oracle> successor_oracle;
          std::optional<runtime::file_path> successor_path;
          auto cfg = storage::testing::wal_group_commit_contract::slow_batch();
          cfg.target_members = 2;
          co_await storage::testing::wal_group_commit_contract::with_groups(
            writer,
            budget,
            drive,
            cfg,
            [&](auto& groups) -> seastar::future<> {
                const auto record = storage::testing::assigned_wire(true, 48);
                const std::array records{record};
                std::array<std::optional<wal_commit_ticket>, 3> tickets;
                for (unsigned i = 0; i != 2; ++i) {
                    auto input = co_await append_contract::offer(
                      budget, spec.owner.cluster(), work, records);
                    tickets[i].emplace(take(
                      co_await groups
                        .template submit<simulation::monotonic_clock>(
                          writer, std::move(input), work)));
                    oracle.attempted(
                      append_contract::prepare_bytes(
                        record, head, spec.owner.cluster(), 8192U * (i + 1U)));
                }
                auto captured = groups.capture(
                  simulation::monotonic_clock::now());
                const auto cohort = oracle.freeze(0, 1);
                require(
                  captured && captured->groups() == 2
                    && captured->boundary().cursor().position().value()
                         == oracle.cut(cohort),
                  "capture disagreed with independently frozen membership");
                auto next = co_await append_contract::offer(
                  budget, spec.owner.cluster(), work, records);
                tickets[2].emplace(take(
                  co_await groups.template submit<simulation::monotonic_clock>(
                    writer, std::move(next), work)));
                oracle.attempted(
                  append_contract::prepare_bytes(
                    record, head, spec.owner.cluster(), 24576));
                const auto interested = oracle.interest(0);
                const auto later_interest = oracle.interest(2);
                seastar::abort_source caller;
                std::optional<
                  seastar::future<runtime::result<wal_commit_result>>>
                  client{
                    tickets[0]->template observe<simulation::monotonic_clock>(
                      env.timer(), caller)};
                std::optional<seastar::future<wal_commit_result>> audit{
                  take(tickets[1]->observe())};
                std::optional<wal_commit_result> returned;
                bool written = false, effect = false;
                if (at == commit_crash_cut::detached) {
                    caller.request_abort();
                    auto outcome = co_await drive.lifecycle(std::move(*client));
                    client.reset();
                    require(
                      !outcome && outcome.error().code() == errc::aborted,
                      "cancelled crash observer succeeded");
                    oracle.detached(interested);
                }
                if (
                  at != commit_crash_cut::freeze
                  && at != commit_crash_cut::written)
                    take(groups.flush(writer, *captured));
                if (at != commit_crash_cut::freeze) {
                    bool reached = false;
                    for (unsigned step = 0; step != 256; ++step) {
                        // Inspect immediately after an event, before runnable
                        // native continuations can turn persistence into a
                        // receipt.
                        auto state = commit_inode_snapshot(files, inode);
                        const auto expected
                          = std::string_view{oracle.bytes()}.substr(
                            0, oracle.cut(cohort));
                        if (
                          !written
                          && commit_has_prefix(state.visible_bytes, expected)) {
                            oracle.written(oracle.cut(cohort));
                            written = true;
                        }
                        if (
                          !effect
                          && commit_has_prefix(state.durable_bytes, expected)) {
                            oracle.flush_effect(cohort);
                            effect = true;
                        }
                        if (
                          (at == commit_crash_cut::written && written)
                          || (at == commit_crash_cut::effect && effect)) {
                            reached = true;
                            break;
                        }
                        if (audit->available()) {
                            returned.emplace(audit->get());
                            audit.reset();
                            const bool success = !returned->failure().failed();
                            require(
                              success == (at != commit_crash_cut::failed_sync)
                                && bool(returned->receipt()) == success,
                              "flush outcome changed its certification "
                              "meaning");
                            if (success)
                                require(
                                  returned->receipt()->boundary()
                                    == captured->boundary(),
                                  "receipt expanded to later arrival");
                            oracle.returned(cohort, success);
                            reached = true;
                            break;
                        }
                        co_await drain_reactor_tasks();
                        if (audit->available()) continue;
                        require(
                          env.event_scheduler().pending_events() != 0,
                          "crash history parked before its cut");
                        if (!env.event_scheduler().has_ready_events())
                            take(env.event_scheduler().advance_to_next());
                        require(
                          take(env.event_scheduler().step()),
                          "crash history failed to step");
                    }
                    require(
                      reached, "cohort crash cut exceeded its event bound");
                }
                if (at == commit_crash_cut::effect)
                    require(
                      effect && audit && !audit->available(),
                      "effect-only cut also returned a receipt");
                if (at == commit_crash_cut::delivery) {
                    auto result = take(
                      co_await drive.lifecycle(std::move(*client)));
                    client.reset();
                    require(
                      result.receipt().has_value(),
                      "delivered success lacks receipt");
                    oracle.delivered(interested, cohort, true);
                } else if (at != commit_crash_cut::detached) {
                    oracle.notification_lost(interested);
                }
                require(
                  !oracle.permits_delivery(later_interest, cohort, true),
                  "incidental survival certified a later group");
                if (at == commit_crash_cut::rotation) {
                    auto tail = take(tickets[2]->observe());
                    const auto tail_cohort = oracle.freeze(2, 2);
                    take(
                      co_await drive.lifecycle(
                        groups.rotate(writer, byte_count{8192}, work)));
                    auto tail_result = co_await drive.lifecycle(
                      std::move(tail));
                    take(tail_result.failure().outcome());
                    require(
                      tail_result.receipt()
                        && tail_result.receipt()
                               ->boundary()
                               .cursor()
                               .position()
                               .value()
                             == 32768,
                      "rotation lost the complete old-file result");
                    oracle.written(32768);
                    oracle.flush_effect(tail_cohort);
                    oracle.returned(tail_cohort, true);
                    const auto successor = *writer.prepared_head();
                    const auto predecessor = take(
                      local_wal_cursor::make(
                        head.incarnation, runtime::file_position{32768}));
                    successor_oracle.emplace(
                      writer_contract::header_bytes(
                        spec, 0, successor.incarnation, predecessor));
                    successor_path.emplace(
                      take(take(local_paths::make(spec.root))
                             .wal(0, successor.incarnation)));
                    auto input = co_await append_contract::offer(
                      budget, spec.owner.cluster(), work, records);
                    auto new_ticket = take(
                      co_await groups
                        .template submit<simulation::monotonic_clock>(
                          writer, std::move(input), work));
                    successor_oracle->attempted(
                      append_contract::prepare_bytes(
                        record, successor, spec.owner.cluster(), 8192));
                    require(
                      new_ticket.boundary().cursor().incarnation()
                          != head.incarnation
                        && returned->receipt()->boundary()
                             == captured->boundary(),
                      "rotation revoked a retained old-file receipt");
                }
                groups.request_stop();
                writer.request_stop();
                take(co_await drive.lifecycle(files.crash()));
                // Cleanup consumes native futures without pretending the lost
                // application notification was delivered before the crash.
                static_cast<void>(co_await drive.lifecycle(groups.close()));
                if (audit)
                    static_cast<void>(
                      co_await drive.lifecycle(std::move(*audit)));
                if (client)
                    static_cast<void>(
                      co_await drive.lifecycle(std::move(*client)));
                static_cast<void>(co_await drive.lifecycle(writer.close()));
                const auto recovered = co_await read_bytes(files, path, drive);
                require(
                  oracle.allows(recovered),
                  "cohort crash image violated independent byte bounds");
                require(
                  oracle.delivered_successes()
                    == (at == commit_crash_cut::delivery ? 1U : 0U),
                  "surviving or merely returned bytes became delivered "
                  "success");
                std::optional<std::string> successor_bytes;
                if (successor_path) {
                    successor_bytes.emplace(
                      co_await read_bytes(files, *successor_path, drive));
                    require(
                      successor_oracle->allows(*successor_bytes),
                      "successor crash image violated independent bounds");
                }
                take(co_await drive.lifecycle(files.crash()));
                require(
                  (co_await read_bytes(files, path, drive)) == recovered,
                  "second crash changed old WAL image");
                if (successor_path)
                    require(
                      (co_await read_bytes(files, *successor_path, drive))
                        == *successor_bytes,
                      "second crash changed successor WAL image");
            },
            true);
      },
      true);
}
} // namespace

SEASTAR_TEST_CASE(
  wal_group_commit_qualification_crash_cuts_and_selective_survival) {
    const auto targets = co_await append_targets();
    for (unsigned mask = 0; mask != 9; ++mask) {
        const fake_crash_policy policy{
          .data_percent = static_cast<std::uint8_t>(
            mask == 8     ? 50
            : (mask & 1U) ? 100
                          : 0),
          .namespace_percent = static_cast<std::uint8_t>(
            mask == 8     ? 50
            : (mask & 2U) ? 100
                          : 0),
          .eof_percent = static_cast<std::uint8_t>(
            mask == 8     ? 50
            : (mask & 4U) ? 100
                          : 0)};
        for (auto cut :
             {commit_crash_cut::freeze,
              commit_crash_cut::written,
              commit_crash_cut::effect,
              commit_crash_cut::receipt,
              commit_crash_cut::delivery,
              commit_crash_cut::detached,
              commit_crash_cut::failed_sync,
              commit_crash_cut::rotation}) {
            const bool lost = cut == commit_crash_cut::effect;
            const bool failed = cut == commit_crash_cut::failed_sync;
            BOOST_TEST_CONTEXT(
              "survival=" << mask
                          << " cohort_cut=" << static_cast<unsigned>(cut)) {
                co_await with_wal_environment(
                  config(
                    lost || failed
                      ? std::optional{runtime::builtin_fault_point::file_flush}
                      : std::nullopt,
                    lost ? runtime::fault_action::drop_completion
                         : runtime::fault_action::file_failure_after_effect,
                    targets.flush.occurrence,
                    runtime::fault_object_key::from_u64(targets.flush.object),
                    policy),
                  [cut](auto& env, auto& budget, auto) {
                      return commit_crash_history(env, budget, cut);
                  });
            }
        }
    }
}

SEASTAR_TEST_CASE(
  wal_group_commit_oracle_rejects_unearned_or_duplicate_delivery) {
    wal_survival_oracle oracle{"header"};
    oracle.attempted("first");
    const auto cohort = oracle.freeze(0, 0);
    const auto caller = oracle.interest(0);
    const auto cancelled = oracle.interest(0);
    oracle.detached(cancelled);
    oracle.attempted("second");
    const auto later = oracle.interest(1);
    BOOST_CHECK(!oracle.permits_delivery(caller, cohort, true));
    BOOST_CHECK_THROW(oracle.returned(cohort, true), std::runtime_error);
    oracle.written(11);
    oracle.flush_effect(cohort);
    BOOST_CHECK(!oracle.permits_delivery(caller, cohort, true));
    oracle.returned(cohort, true);
    BOOST_CHECK_THROW(oracle.returned(cohort, true), std::runtime_error);
    BOOST_CHECK(!oracle.permits_delivery(cancelled, cohort, true));
    BOOST_CHECK(!oracle.permits_delivery(later, cohort, true));
    oracle.delivered(caller, cohort, true);
    BOOST_CHECK(!oracle.permits_delivery(caller, cohort, true));
    BOOST_CHECK_EQUAL(oracle.delivered_successes(), 1U);
    BOOST_CHECK(oracle.allows("headerfirstsecond"));
    co_return;
}

SEASTAR_TEST_CASE(wal_group_commit_qualification_deadline_threshold_ties) {
    for (bool bytes : {false, true}) {
        for (bool timer_first : {false, true}) {
            BOOST_TEST_CONTEXT(
              "bytes=" << bytes << " timer_first=" << timer_first) {
                co_await with_wal_environment(
                  config(),
                  [=](auto& env, auto& budget, auto) -> seastar::future<> {
                      commit_lifecycle_driver drive{env.event_scheduler()};
                      auto& files = env.file_system();
                      const auto spec = specification(
                        take(runtime::file_path::make("/kwaque/store")),
                        {1, 1});
                      const std::array specs{spec};
                      ownership_input owner{specs};
                      take(
                        co_await drive.lifecycle(
                          files.create_directories(spec.root)));
                      co_await append_contract::with_writer(
                        files,
                        owner,
                        spec,
                        budget,
                        drive,
                        writer_contract::configuration(),
                        [&](auto& writer, auto& work) -> seastar::future<> {
                            auto cfg = storage::testing::
                              wal_group_commit_contract::slow_batch();
                            cfg.maximum_wait = runtime::monotonic_duration{
                              1'000'000};
                            cfg.target_members = bytes ? 128U : 2U;
                            cfg.target_bytes = byte_count{
                              bytes ? 16384U : 4U * 1024U * 1024U};
                            co_await storage::testing::
                              wal_group_commit_contract::with_groups(
                                writer,
                                budget,
                                drive,
                                cfg,
                                [&](auto& groups) -> seastar::future<> {
                                    const std::array records{
                                      storage::testing::assigned_wire()};
                                    auto one = co_await append_contract::offer(
                                      budget,
                                      spec.owner.cluster(),
                                      work,
                                      records);
                                    auto two = co_await append_contract::offer(
                                      budget,
                                      spec.owner.cluster(),
                                      work,
                                      records);
                                    const auto origin
                                      = simulation::monotonic_clock::now();
                                    const auto deadline
                                      = origin.checked_add(cfg.maximum_wait)
                                          .value();
                                    auto first = take(
                                      co_await groups.template submit<
                                        simulation::monotonic_clock>(
                                        writer, std::move(one), work, origin));
                                    auto waiting = groups.template wait_capture<
                                      simulation::monotonic_clock>(env.timer());
                                    for (unsigned step = 0;
                                         writer.progress()->write_complete
                                           != first.boundary().cursor()
                                         && step != 64;
                                         ++step) {
                                        co_await drain_reactor_tasks();
                                        if (
                                          writer.progress()->write_complete
                                          == first.boundary().cursor())
                                            break;
                                        require(
                                          take(env.event_scheduler()
                                                 .advance_to_next())
                                            .has_value(),
                                          "write lacked a completion event");
                                        require(
                                          env.event_scheduler().now()
                                            < deadline,
                                          "write preparation consumed batch "
                                          "deadline");
                                        require(
                                          take(env.event_scheduler().step()),
                                          "write completion was not stepped");
                                    }
                                    co_await drain_reactor_tasks();
                                    require(
                                      take(
                                        env.event_scheduler().advance_to_next())
                                        && env.event_scheduler().now()
                                             == deadline,
                                      "tie did not reach the original batch "
                                      "deadline");
                                    std::optional<wal_flush_capture> selected;
                                    std::optional<wal_commit_ticket> second;
                                    if (timer_first) {
                                        require(
                                          take(env.event_scheduler().step()),
                                          "deadline event missing");
                                        selected = take(
                                          co_await drive.lifecycle(
                                            std::move(waiting)));
                                        second.emplace(take(
                                          co_await groups.template submit<
                                            simulation::monotonic_clock>(
                                            writer, std::move(two), work)));
                                    } else {
                                        second.emplace(take(
                                          co_await groups.template submit<
                                            simulation::monotonic_clock>(
                                            writer, std::move(two), work)));
                                        selected = take(
                                          co_await drive.lifecycle(
                                            std::move(waiting)));
                                    }
                                    require(
                                      selected && selected->groups() == 1
                                        && selected->deadline() == deadline
                                        && selected->boundary()
                                             == first.boundary(),
                                      "equality restarted the oldest deadline "
                                      "or enlarged the expired cohort");
                                    auto first_result = take(first.observe());
                                    auto second_result = take(
                                      second->observe());
                                    take(groups.flush(writer, *selected));
                                    auto done = co_await drive.lifecycle(
                                      std::move(first_result));
                                    require(
                                      done.receipt()
                                        && !second_result.available(),
                                      "deadline tie certified an uncovered "
                                      "group");
                                    take(
                                      co_await drive.lifecycle(groups.close()));
                                    auto tail = co_await drive.lifecycle(
                                      std::move(second_result));
                                    require(
                                      tail.receipt()
                                        && writer.statistics().flush_calls == 2,
                                      "deadline tie lost its later cohort or "
                                      "repeated a flush");
                                });
                        });
                  });
            }
        }
    }
}
