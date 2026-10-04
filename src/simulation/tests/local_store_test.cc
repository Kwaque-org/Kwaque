#include "src/base/units.h"
#include "src/runtime/testing/reactor_tasks.h"
#include "src/simulation/environment.h"
#include "src/simulation/fake_file_test_support.h"
#include "src/simulation/scheduler_driver.h"
#include "src/simulation/virtual_time.h"
#include "src/storage/tests/local_installation_contract.h"
#include "src/storage/tests/local_qualification_contract.h"
#include "src/storage/tests/local_reader_contract.h"
#include "src/storage/tests/local_store_contract.h"
#include "src/storage/tests/recovery_contract.h"
#include "src/storage/tests/segment_qualification_contract.h"
#include "src/storage/tests/segment_scan_contract.h"
#include "src/storage/tests/segment_writer_contract.h"
#include "src/storage/tests/wal_scan_contract.h"

#include <seastar/testing/test_case.hh>
#include <seastar/util/later.hh>

#include <boost/test/unit_test.hpp>

#include <bit>
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
  std::uint32_t native_max_length
  = static_cast<std::uint32_t>(maximum_contiguous_allocation_bytes),
  std::uint32_t maximum_open_handles = 16) {
    environment_config_values values;
    values.scheduler.pending_events = 256;
    values.scheduler.events_per_pump = 64;
    values.scheduler.total_events = 100000;
    values.trace.entries = 32768;
    values.trace.encoded_bytes = 8_MiB;
    values.event_log.entries = 32;
    values.event_log.encoded_bytes = 32_KiB;
    values.file.crash_policy = crash_policy;
    values.file.maximum_objects = 256;
    values.file.maximum_open_handles = maximum_open_handles;
    values.file.native_max_length = native_max_length;
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
    values.dns.maximum_name_bytes = byte_count{8_KiB};
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

void stabilize(fake_file_system& files, const runtime::file_path& path) {
    take(
      fake_file_test_access::sync_directory(
        files, take(fake_file_test_access::resolve(files, path.value()))));
}
void seed_bytes(
  fake_file_system& files,
  const runtime::file_path& path,
  std::string_view bytes) {
    const auto selected = take(
      fake_file_test_access::resolve(files, path.value()));
    take(fake_file_test_access::create_file(files, selected));
    take(
      fake_file_test_access::write(
        files,
        selected,
        0,
        std::as_bytes(std::span{bytes.data(), bytes.size()})));
    take(fake_file_test_access::flush(files, selected));
    stabilize(
      files,
      take(
        runtime::file_path::make(
          path.value().substr(0, path.value().rfind('/')))));
}

template<typename Func>
seastar::future<> with_store_environment(
  environment_config configuration,
  Func function,
  byte_count budget_bytes = byte_count{8_MiB},
  resource::workload_class classification = resource::workload_class::metadata,
  std::uint32_t budget_tasks = 16) {
    auto target = take(environment::make(std::move(configuration)));
    simulation::testing::scheduler_driver drive{target->event_scheduler()};
    co_await target->start();
    std::exception_ptr first;
    try {
        workload_budget budget{
          target->resource_manager().acquire_workload(classification),
          {.tasks = budget_tasks, .bytes = budget_bytes, .handles = 32},
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

seastar::future<> seed_initial(
  fake_file_system& files,
  const local_device_spec& spec,
  simulation::testing::scheduler_driver drive) {
    const auto paths = take(local_paths::make(spec.root));
    take(co_await drive.lifecycle(files.create_directories(spec.root)));
    stabilize(files, take(runtime::file_path::make("/kwaque")));
    seed_bytes(
      files,
      take(paths.store()),
      storage::testing::local_fixture::read("store"));
    auto shards = take(
      local_child_path(spec.root, take(runtime::file_name::make("shards"))));
    take(co_await drive.lifecycle(files.create_directories(shards)));
    stabilize(files, spec.root);
    for (std::uint32_t shard = 0; shard < spec.identity.shard_count; ++shard) {
        auto control = take(paths.control(shard));
        auto parent = take(
          runtime::file_path::make(
            control.value().substr(0, control.value().rfind('/'))));
        take(co_await drive.lifecycle(files.create_directories(parent)));
        stabilize(files, shards);
        for (const auto name :
             {"wal", "segments", "checkpoints", "decisions", "deletions"})
            take(
              co_await drive.lifecycle(
                files.create_directories(take(local_child_path(
                  parent, take(runtime::file_name::make(name)))))));
        auto checkpoints = take(local_child_path(
          parent, take(runtime::file_name::make("checkpoints"))));
        take(
          co_await drive.lifecycle(
            files.create_directories(take(local_child_path(
              checkpoints, take(runtime::file_name::make("evidence")))))));
        stabilize(files, parent);
        stabilize(files, checkpoints);
        auto bytes = storage::testing::local_fixture::read("control_empty");
        storage::testing::put(bytes, 84, shard, 4);
        storage::testing::repair(bytes);
        seed_bytes(files, control, bytes);
    }
}
} // namespace

SEASTAR_TEST_CASE(local_store_fake_bootstrap_and_control) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(
            co_await drive.lifecycle(
              env.file_system().create_directories(root)));
          stabilize(
            env.file_system(), take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1});
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::store_contract::exercise(
            env.file_system(), owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(local_store_checks_all_devices_before_mutating_any) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto a = specification(
            take(runtime::file_path::make("/kwaque/a")), {1, 1});
          const auto b = specification(
            take(runtime::file_path::make("/kwaque/b")),
            {1, 2},
            0x44,
            local_device_role::data);
          const std::array specs{a, b};
          ownership_input owner{specs};
          for (const auto& spec : specs)
              take(
                co_await drive.lifecycle(files.create_directories(spec.root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          auto unknown = take(local_child_path(
            b.root, take(runtime::file_name::make("unexpected"))));
          seed_bytes(files, unknown, "keep");
          seastar::abort_source abort;
          codec::cooperative_work work{codec::limits::defaults(), abort};
          {
              auto result = co_await drive.lifecycle(initialize_local_stores(
                files,
                owner,
                specs,
                {local_store_intent::create_or_resume, false},
                budget,
                limits(),
                work));
              BOOST_CHECK(result.failure.failed());
              BOOST_CHECK(!result.mutation_attempted);
              auto exists = take(
                co_await drive.lifecycle(files.exists(
                  take(local_paths::make(a.root)).store().value())));
              BOOST_CHECK(!exists);
          }
          take(co_await drive.lifecycle(files.remove_file(unknown)));
          {
              auto result = co_await drive.lifecycle(initialize_local_stores(
                files,
                owner,
                specs,
                {local_store_intent::create_or_resume, false},
                budget,
                limits(),
                work));
              take(result.failure.outcome());
          }
          const auto data_paths = take(local_paths::make(b.root));
          auto data_control = take(
            co_await drive.lifecycle(
              files.exists(take(data_paths.control(0)))));
          BOOST_CHECK(!data_control);
          take(co_await drive.lifecycle(files.crash()));
          {
              auto result = co_await drive.lifecycle(initialize_local_stores(
                files,
                owner,
                specs,
                {local_store_intent::must_exist, false},
                budget,
                limits(),
                work));
              take(result.failure.outcome());
              BOOST_CHECK(!result.mutation_attempted);
          }
          owner.active = false;
          auto denied = co_await drive.lifecycle(initialize_local_stores(
            files,
            owner,
            specs,
            {local_store_intent::create_or_resume, false},
            budget,
            limits(),
            work));
          BOOST_CHECK(denied.failure.failed());
          BOOST_CHECK(!denied.mutation_attempted);
      });
}

SEASTAR_TEST_CASE(local_store_access_failure_is_not_pristine) {
    co_await with_store_environment(
      config(
        runtime::builtin_fault_point::file_stat, runtime::fault_action::error),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1});
          const std::array specs{spec};
          ownership_input owner{specs};
          seastar::abort_source abort;
          codec::cooperative_work work{codec::limits::defaults(), abort};
          auto result = co_await drive.lifecycle(initialize_local_stores(
            files,
            owner,
            specs,
            {local_store_intent::create_or_resume, false},
            budget,
            limits(),
            work));
          BOOST_REQUIRE(result.failure.error());
          BOOST_CHECK(result.failure.error()->code() == errc::fault_injected);
          BOOST_CHECK(!result.mutation_attempted);
      });
}

SEASTAR_TEST_CASE(local_control_uncertain_publication_fences_further_updates) {
    for (const auto point :
         {runtime::builtin_fault_point::file_rename,
          runtime::builtin_fault_point::directory_sync}) {
        for (const auto action :
             {runtime::fault_action::file_failure_before_effect,
              runtime::fault_action::file_failure_after_effect}) {
            co_await with_store_environment(
              config(
                point,
                action,
                point == runtime::builtin_fault_point::directory_sync ? 2 : 1),
              [](auto& env, auto& budget, auto drive) -> seastar::future<> {
                  auto& files = env.file_system();
                  const auto spec = specification(
                    take(runtime::file_path::make("/kwaque/store")), {1, 1});
                  const std::array specs{spec};
                  ownership_input owner{specs};
                  co_await seed_initial(files, spec, drive);
                  seastar::abort_source abort;
                  codec::cooperative_work work{
                    codec::limits::defaults(), abort};
                  using control_type
                    = local_control_owner<fake_file_system, ownership_input>;
                  auto control = take(
                    co_await drive.lifecycle(
                      control_type::open(
                        files, owner, spec, 0, false, budget, limits(), work)));
                  runtime::first_failure failed;
                  try {
                      auto outcome = co_await drive.lifecycle(control->update(
                        [](local_shard_control& next) -> runtime::result<void> {
                            next.object_high = local_object_high{16};
                            return {};
                        },
                        work));
                      BOOST_REQUIRE(outcome.failure.error());
                      BOOST_CHECK(control->fenced());
                      BOOST_CHECK(
                        outcome.disposition
                        == local_publication_disposition::uncertain);
                      bool edited = false;
                      auto denied = co_await drive.lifecycle(control->update(
                        [&edited](
                          local_shard_control&) -> runtime::result<void> {
                            edited = true;
                            return {};
                        },
                        work));
                      BOOST_CHECK(denied.failure.failed());
                      BOOST_CHECK(!edited);
                  } catch (...) {
                      failed.observe(std::current_exception());
                  }
                  take(co_await drive.lifecycle(control->close()));
                  control.reset();
                  take(failed.outcome());
              });
        }
    }
}

SEASTAR_TEST_CASE(
  local_control_pins_explicit_head_and_preserves_all_control_fields) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto spec = specification(
            take(runtime::file_path::make("/kwaque/store")), {1, 1});
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await seed_initial(files, spec, drive);
          const auto paths = take(local_paths::make(spec.root));
          seastar::abort_source abort;
          codec::cooperative_work work{codec::limits::defaults(), abort};
          using control_type
            = local_control_owner<fake_file_system, ownership_input>;
          auto control = take(
            co_await drive.lifecycle(
              control_type::open(
                files, owner, spec, 0, false, budget, limits(), work)));
          runtime::first_failure failed;
          const auto first_id
            = local_wal_high{}.checked_advance(1)->incarnation().value();
          try {
              {
                  auto reserved = co_await drive.lifecycle(control->update(
                    [](local_shard_control& next) -> runtime::result<void> {
                        next.wal_high
                          = local_wal_high{}.checked_advance(16).value();
                        next.object_high = local_object_high{128};
                        return {};
                    },
                    work));
                  take(reserved.failure.outcome());
              }
              for (const auto number : {1U, 9U}) {
                  const auto id = local_wal_high{}
                                    .checked_advance(number)
                                    ->incarnation()
                                    .value();
                  const auto path = take(paths.wal(0, id));
                  const auto parent = take(
                    runtime::file_path::make(
                      path.value().substr(0, path.value().rfind('/'))));
                  take(
                    co_await drive.lifecycle(files.create_directories(parent)));
                  stabilize(
                    files, take(paths.buckets(0, local_bucket_kind::wal)));
                  auto bytes = storage::testing::local_fixture::read(
                    number == 1 ? "wal_header" : "wal_successor_gap");
                  if (number == 1)
                      for (auto name :
                           {"prepare_a1", "prepare_b1", "prepare_a2"})
                          bytes += storage::testing::local_fixture::read(name);
                  seed_bytes(files, path, bytes);
              }
              const auto head = local_wal_head{
                first_id,
                codec::immutable_object_digest{storage::testing::exact_digest(
                  storage::testing::local_fixture::read("wal_header"))}};
              auto install =
                [head](local_shard_control& next) -> runtime::result<void> {
                  next.wal_head = head;
                  return {};
              };
              {
                  auto denied = co_await drive.lifecycle(
                    control->update(install, work));
                  BOOST_REQUIRE(denied.failure.error());
                  BOOST_CHECK(
                    denied.failure.error()->code() == errc::invalid_argument);
                  BOOST_CHECK(!take(control->snapshot()).fields.wal_head);
              }
              // Supplied producer readiness/pin lifetime is the input here. WAL
              // execution and checkpoint proof discharge are separate owners.
              auto ready = [](
                             const local_control_snapshot&,
                             const local_shard_control&,
                             codec::cooperative_work&) {
                  return seastar::make_ready_future<runtime::result<void>>(
                    runtime::result<void>{});
              };
              {
                  auto done = co_await drive.lifecycle(
                    control->update(install, ready, work));
                  take(done.failure.outcome());
              }
              const auto root_bytes = storage::testing::local_fixture::read(
                "checkpoint_root");
              const auto checkpoint_path = take(
                paths.sequence_file(0, local_sequence_file::checkpoint, 70));
              seed_bytes(
                files,
                checkpoint_path,
                root_bytes
                  + storage::testing::local_fixture::read("checkpoint_page"));
              const auto reference = local_root_reference::make(
                                       local_root_kind::checkpoint,
                                       local_object_sequence::make(70).value(),
                                       runtime::file_position{},
                                       byte_count{root_bytes.size()},
                                       page_count::make(1).value(),
                                       codec::immutable_object_digest{
                                         storage::testing::exact_digest(
                                           root_bytes)})
                                       .value();
              {
                  auto done = co_await drive.lifecycle(control->update(
                    [reference](
                      local_shard_control& next) -> runtime::result<void> {
                        next.checkpoint = reference;
                        return {};
                    },
                    ready,
                    work));
                  take(done.failure.outcome());
              }
              const auto snapshot = take(control->snapshot());
              BOOST_CHECK(snapshot.fields.wal_head == head);
              BOOST_CHECK(snapshot.fields.checkpoint == reference);
              BOOST_CHECK_EQUAL(snapshot.fields.object_high.value(), 128U);
              BOOST_CHECK(
                snapshot.fields.wal_high
                == local_wal_high{}.checked_advance(16).value());
              {
                  auto clear = co_await drive.lifecycle(control->update(
                    [](local_shard_control& next) -> runtime::result<void> {
                        next.wal_head.reset();
                        return {};
                    },
                    ready,
                    work));
                  BOOST_CHECK(clear.failure.failed());
                  BOOST_CHECK(!control->fenced());
              }
          } catch (...) {
              failed.observe(std::current_exception());
          }
          take(co_await drive.lifecycle(control->close()));
          control.reset();
          take(failed.outcome());
          control = take(
            co_await drive.lifecycle(
              control_type::open(
                files, owner, spec, 0, true, budget, limits(), work)));
          auto reopened = control->snapshot();
          take(co_await drive.lifecycle(control->close()));
          control.reset();
          BOOST_REQUIRE(reopened);
          BOOST_REQUIRE(reopened->fields.wal_head);
          BOOST_CHECK(reopened->fields.wal_head->incarnation == first_id);
          BOOST_REQUIRE(reopened->fields.checkpoint);
          BOOST_CHECK_EQUAL(
            reopened->fields.checkpoint->sequence().value(), 70U);
          const auto selected = take(paths.wal(0, first_id));
          take(co_await drive.lifecycle(files.remove_file(selected)));
          auto missing = co_await drive.lifecycle(
            control_type::open(
              files, owner, spec, 0, true, budget, limits(), work));
          if (missing) {
              take(co_await drive.lifecycle((*missing)->close()));
              missing->reset();
          }
          BOOST_REQUIRE(!missing);
          BOOST_CHECK(missing.error().code() == errc::not_found);
      });
}

SEASTAR_TEST_CASE(local_store_rejects_persisted_identity_profile_mismatches) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto original = specification(
            take(runtime::file_path::make("/kwaque/store")), {1, 1});
          take(
            co_await drive.lifecycle(files.create_directories(original.root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto marker = take(
            take(local_paths::make(original.root)).store());
          seed_bytes(
            files, marker, storage::testing::local_fixture::read("store"));
          seastar::abort_source abort;
          codec::cooperative_work work{codec::limits::defaults(), abort};
          for (unsigned changed = 0; changed < 6; ++changed) {
              auto expected = original;
              expected.owner
                = local_store_context::make(
                    identity<model::cluster_id>(changed == 0 ? 0x77 : 0x11),
                    identity<model::broker_id>(changed == 1 ? 0x88 : 0x22),
                    identity<device_store_id>(changed == 2 ? 0x99 : 0x33),
                    local_store_shard)
                    .value();
              if (changed == 3) expected.identity.shard_count = 3;
              if (changed == 4)
                  expected.identity.role = local_device_role::wal_control;
              if (changed == 5)
                  expected.identity.metadata_alignment
                    = storage_alignment::make(byte_count{8192}).value();
              const std::array specs{expected};
              ownership_input owner{specs};
              auto inspected = take(
                co_await drive.lifecycle(inspect_local_store(
                  files,
                  owner,
                  expected,
                  {local_store_intent::must_exist, false},
                  budget,
                  limits(),
                  work)));
              BOOST_CHECK(inspected.state == local_store_state::corrupt);
              auto refused = co_await drive.lifecycle(initialize_local_stores(
                files,
                owner,
                specs,
                {local_store_intent::create_or_resume, false},
                budget,
                limits(),
                work));
              BOOST_CHECK(refused.failure.failed());
              BOOST_CHECK(!refused.mutation_attempted);
          }
          auto after = co_await read_bytes(files, marker, drive);
          BOOST_CHECK(after == storage::testing::local_fixture::read("store"));
      });
}

SEASTAR_TEST_CASE(local_installation_fake_allocation) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 51);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::installation_contract::allocation(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(local_installation_fake_descriptors) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 68);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::installation_contract::descriptors(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(local_installation_fake_checkpoint_bundle) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 51);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::installation_contract::checkpoint_bundle(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(local_installation_fake_segment_bundles) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 68);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::installation_contract::segment_bundles(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(local_installation_fake_maximum_bundle) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 68);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::installation_contract::maximum_bundle(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(
  local_allocator_failed_reservation_never_serves_or_reuses_ids) {
    // Probe the identical deterministic prefix, then target only its next
    // temporary inode. A wildcard "once" applies separately to every inode.
    std::uint64_t first_temporary = 0;
    co_await with_store_environment(
      config(),
      [&first_temporary](auto& env, auto&, auto drive) -> seastar::future<> {
          const auto spec = specification(
            take(runtime::file_path::make("/kwaque/store")), {1, 1});
          co_await seed_initial(env.file_system(), spec, drive);
          first_temporary
            = take(fake_file_test_access::snapshot(env.file_system()))
                .next_object_id;
      });
    for (auto action :
         {runtime::fault_action::file_failure_before_effect,
          runtime::fault_action::file_failure_after_effect}) {
        co_await with_store_environment(
          config(
            runtime::builtin_fault_point::file_rename,
            action,
            1,
            runtime::fault_object_key::from_u64(first_temporary)),
          [first_temporary](
            auto& env, auto& budget, auto drive) -> seastar::future<> {
              auto& files = env.file_system();
              const auto spec = specification(
                take(runtime::file_path::make("/kwaque/store")), {1, 1});
              const std::array specs{spec};
              ownership_input owner{specs};
              co_await seed_initial(files, spec, drive);
              require(
                take(fake_file_test_access::snapshot(files)).next_object_id
                  == first_temporary,
                "fault target differs from the deterministic prefix");
              seastar::abort_source abort;
              codec::cooperative_work work{codec::limits::defaults(), abort};
              using control_type
                = local_control_owner<fake_file_system, ownership_input>;
              using allocator_type
                = local_id_allocator<fake_file_system, ownership_input>;
              auto control = take(
                co_await drive.lifecycle(
                  control_type::open(
                    files, owner, spec, 0, false, budget, limits(), work)));
              auto allocator = take(allocator_type::make(*control, budget, 4));
              runtime::first_failure failed;
              try {
                  auto result = co_await drive.lifecycle(
                    allocator->allocate_object(work));
                  require(
                    !result && result.error().code() == errc::io_failure,
                    "reservation did not encounter the selected I/O fault");
                  result = co_await drive.lifecycle(
                    allocator->allocate_object(work));
                  require(
                    !result && result.error().code() == errc::closed,
                    "fenced allocator served IDs");
              } catch (...) {
                  failed.observe(std::current_exception());
              }
              take(co_await drive.lifecycle(allocator->close()));
              allocator.reset();
              take(co_await drive.lifecycle(control->close()));
              control.reset();
              take(failed.outcome());
              control = take(
                co_await drive.lifecycle(
                  control_type::open(
                    files, owner, spec, 0, false, budget, limits(), work)));
              const auto high
                = take(control->snapshot()).fields.object_high.value();
              allocator = take(allocator_type::make(*control, budget, 4));
              try {
                  auto result = take(
                    co_await drive.lifecycle(allocator->allocate_object(work)));
                  require(
                    result.value() == high + 1,
                    "reopen reused a selected reservation");
              } catch (...) {
                  failed.observe(std::current_exception());
              }
              take(co_await drive.lifecycle(allocator->close()));
              allocator.reset();
              take(co_await drive.lifecycle(control->close()));
              control.reset();
              take(failed.outcome());
          });
    }
}

SEASTAR_TEST_CASE(local_allocator_drains_refill_and_burns_block_after_crash) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto spec = specification(
            take(runtime::file_path::make("/kwaque/store")), {1, 1});
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await seed_initial(files, spec, drive);
          seastar::abort_source abort;
          codec::cooperative_work work{codec::limits::defaults(), abort};
          using control_type
            = local_control_owner<fake_file_system, ownership_input>;
          using allocator_type
            = local_id_allocator<fake_file_system, ownership_input>;
          auto control = take(
            co_await drive.lifecycle(
              control_type::open(
                files, owner, spec, 0, false, budget, limits(), work)));
          auto allocator = take(allocator_type::make(*control, budget, 4));
          auto pending = allocator->allocate_object(work);
          auto overlap = co_await allocator->allocate_object(work);
          auto closing = allocator->close();
          runtime::first_failure failed;
          try {
              require(
                !overlap && overlap.error().code() == errc::queue_full,
                "overlapping refill queued");
              require(
                take(control->snapshot()).fields.object_high.value() == 0,
                "reservation installed before I/O");
          } catch (...) {
              failed.observe(std::current_exception());
          }
          auto first = co_await drive.lifecycle(std::move(pending));
          take(co_await drive.lifecycle(std::move(closing)));
          allocator.reset();
          take(co_await drive.lifecycle(control->close()));
          control.reset();
          take(failed.outcome());
          require(
            first && first->value() == 1,
            "close abandoned accepted allocation");
          take(co_await drive.lifecycle(files.crash()));
          control = take(
            co_await drive.lifecycle(
              control_type::open(
                files, owner, spec, 0, false, budget, limits(), work)));
          allocator = take(allocator_type::make(*control, budget, 4));
          auto next = co_await drive.lifecycle(
            allocator->allocate_object(work));
          take(co_await drive.lifecycle(allocator->close()));
          allocator.reset();
          take(co_await drive.lifecycle(control->close()));
          control.reset();
          require(
            next && next->value() == 5,
            "crash reused the unused durable range");
      });
}

SEASTAR_TEST_CASE(local_bundle_failure_cuts_never_release_a_root_reference) {
    using namespace storage::testing::installation_contract;
    using fault_point = runtime::builtin_fault_point;
    using runtime::fault_action;
    const auto before = fault_action::file_failure_before_effect;
    const auto after = fault_action::file_failure_after_effect;
    const std::array faults{
      std::pair{fault_point::file_write, before},
      std::pair{fault_point::file_write, after},
      std::pair{fault_point::file_flush, before},
      std::pair{fault_point::file_flush, after},
      std::pair{fault_point::file_size, before},
      std::pair{fault_point::file_close, before},
      std::pair{fault_point::file_rename, before},
      std::pair{fault_point::file_rename, after},
      std::pair{fault_point::directory_sync, before},
      std::pair{fault_point::directory_sync, after},
      std::pair{fault_point::directory_cursor_close, fault_action::error}};
    for (const auto& [point, action] : faults) {
        co_await with_store_environment(
          config(point, action),
          [point,
           action](auto& env, auto& budget, auto drive) -> seastar::future<> {
              auto& files = env.file_system();
              const auto spec = specification(
                take(runtime::file_path::make("/kwaque/store")), {1, 1});
              const std::array specs{spec};
              ownership_input owner{specs};
              co_await seed_initial(files, spec, drive);
              seastar::abort_source abort;
              codec::cooperative_work work{codec::limits::defaults(), abort};
              auto bundle = take(
                co_await local_bundle::make(
                  checkpoint_reference(),
                  checkpoint_expectation(spec),
                  co_await storage::testing::installation_contract::
                    buffer_async(
                      storage::testing::local_fixture::read("checkpoint_root")),
                  budget,
                  limits(),
                  work));
              const std::array<std::string_view, 1> pages{"checkpoint_page"};
              auto outcome = co_await drive.lifecycle(publish_local_bundle(
                files,
                owner,
                spec,
                0,
                std::move(bundle),
                fixture_pages{pages},
                ready_dependencies{},
                budget,
                work));
              require(
                outcome.publication.failure.failed() && !outcome.reference,
                "failed bundle exposed reference");
              require(
                outcome.publication.failure.error()
                  && outcome.publication.failure.error()->code()
                       == (action == fault_action::error ? errc::fault_injected : errc::io_failure),
                "publication did not encounter the selected fault");
              if (point == runtime::builtin_fault_point::directory_cursor_close)
                  require(
                    outcome.publication.disposition
                      == local_publication_disposition::durable,
                    "parent close failure erased confirmed durability");
              else if (
                point == runtime::builtin_fault_point::file_rename
                || point == runtime::builtin_fault_point::directory_sync)
                  require(
                    outcome.publication.disposition
                      == local_publication_disposition::uncertain,
                    "ambiguous publication lost disposition");
              else
                  require(
                    outcome.publication.disposition
                      == local_publication_disposition::untouched,
                    "pre-rename failure changed the final publication");
          });
    }
}

SEASTAR_TEST_CASE(local_readers_fake_root_lifetime) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 51);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::reader_contract::root_lifetime(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(local_readers_fake_root_errors) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 51);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::reader_contract::root_errors(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(local_readers_fake_generation_lifetime) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 68);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::reader_contract::generation_lifetime(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(local_readers_fake_generation_errors) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 68);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::reader_contract::generation_errors(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(local_readers_fake_wal_chain) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 51);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::reader_contract::wal_chain(
            files, owner, spec, budget, drive);
      });
}
SEASTAR_TEST_CASE(local_readers_fake_wal_alignment) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 51);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::reader_contract::wal_alignment(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(local_readers_fake_discovery_cleanup) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 68);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::reader_contract::discovery_cleanup(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(local_readers_fake_empty_retry) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 68);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::reader_contract::empty_retry(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(
  local_readers_fake_cold_resolution_rejects_duplicate_devices) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const std::array specs{
            specification(
              take(runtime::file_path::make("/kwaque/a")), {1, 1}, 0x44),
            specification(
              take(runtime::file_path::make("/kwaque/b")),
              {1, 2},
              0x55,
              local_device_role::data)};
          ownership_input owner{specs};
          for (const auto& spec : specs)
              take(
                co_await drive.lifecycle(files.create_directories(spec.root)));
          co_await storage::testing::reader_contract::cold_resolution(
            files, owner, specs, budget, drive);
      });
}

SEASTAR_TEST_CASE(
  local_root_failed_open_and_close_keep_ownership_and_first_error) {
    for (const auto point :
         {runtime::builtin_fault_point::file_read,
          runtime::builtin_fault_point::file_close}) {
        co_await with_store_environment(
          config(point, runtime::fault_action::file_failure_before_effect),
          [point](auto& env, auto& budget, auto drive) -> seastar::future<> {
              auto& files = env.file_system();
              const auto spec = specification(
                take(runtime::file_path::make("/kwaque/store")), {1, 1});
              const std::array specs{spec};
              ownership_input owner{specs};
              co_await seed_initial(files, spec, drive);
              const auto path = take(
                take(local_paths::make(spec.root))
                  .sequence_file(0, local_sequence_file::checkpoint, 70));
              seed_bytes(
                files,
                path,
                storage::testing::local_fixture::read("checkpoint_root")
                  + storage::testing::local_fixture::read("checkpoint_page"));
              seastar::abort_source abort;
              codec::cooperative_work work{codec::limits::defaults(), abort};
              auto opened = co_await drive.lifecycle(
                local_root_owner::open(
                  files,
                  owner,
                  spec,
                  0,
                  storage::testing::installation_contract::
                    checkpoint_reference(),
                  storage::testing::installation_contract::
                    checkpoint_expectation(spec),
                  budget,
                  limits(),
                  work));
              if (point == runtime::builtin_fault_point::file_read) {
                  if (opened) {
                      take(co_await drive.lifecycle((*opened)->close()));
                      opened->reset();
                  }
                  require(
                    !opened && opened.error().code() == errc::io_failure,
                    "root did not encounter the selected read fault");
              } else {
                  auto root = take(std::move(opened));
                  auto first = co_await drive.lifecycle(root->close());
                  auto again = co_await drive.lifecycle(root->close());
                  root.reset();
                  require(
                    !first && !again && first.error() == again.error(),
                    "checked close lost first error or repeated the "
                    "syscall");
              }
          });
    }
}
SEASTAR_TEST_CASE(
  local_namespace_failed_cursor_is_not_a_complete_empty_inventory) {
    for (const auto point :
         {runtime::builtin_fault_point::directory_cursor_open,
          runtime::builtin_fault_point::directory_cursor_next,
          runtime::builtin_fault_point::directory_cursor_close}) {
        co_await with_store_environment(
          config(point, runtime::fault_action::error),
          [](auto& env, auto& budget, auto drive) -> seastar::future<> {
              auto& files = env.file_system();
              const auto spec = specification(
                take(runtime::file_path::make("/kwaque/store")), {1, 1});
              const std::array specs{spec};
              ownership_input owner{specs};
              co_await seed_initial(files, spec, drive);
              seastar::abort_source abort;
              codec::cooperative_work work{codec::limits::defaults(), abort};
              auto visit = [](const local_namespace_entry&) {
                  return seastar::make_ready_future<runtime::result<bool>>(
                    true);
              };
              auto result = co_await drive.lifecycle(walk_local_namespace(
                files, owner, spec, budget, limits(), work, visit));
              require(
                !result,
                "failed directory operation became successful inventory");
          });
    }
}

SEASTAR_TEST_CASE(local_root_partial_open_refunds_admission_and_closes_file) {
    co_await with_store_environment(
      config(), [](auto& env, auto&, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto spec = specification(
            take(runtime::file_path::make("/kwaque/store")), {1, 1});
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await seed_initial(files, spec, drive);
          const auto path = take(
            take(local_paths::make(spec.root))
              .sequence_file(0, local_sequence_file::checkpoint, 70));
          seed_bytes(
            files,
            path,
            storage::testing::local_fixture::read("checkpoint_root")
              + storage::testing::local_fixture::read("checkpoint_page"));
          workload_budget pressure{
            env.resource_manager().acquire_workload(
              resource::workload_class::metadata),
            {.tasks = 1, .bytes = byte_count{2_MiB}, .handles = 2},
            bytes::testing::charge};
          seastar::abort_source abort;
          codec::cooperative_work work{codec::limits::defaults(), abort};
          auto rejected = co_await drive.lifecycle(
            local_root_owner::open(
              files,
              owner,
              spec,
              0,
              storage::testing::installation_contract::checkpoint_reference(),
              storage::testing::installation_contract::checkpoint_expectation(
                spec),
              pressure,
              limits(),
              work));
          if (rejected) {
              take(co_await drive.lifecycle((*rejected)->close()));
              rejected->reset();
          }
          require(!rejected, "root metadata bypassed task admission");
          require(
            pressure.snapshot().tasks == 0 && pressure.snapshot().handles == 0,
            "failed construction leaked admission");
          require(
            fake_file_test_access::open_handles(files) == 0,
            "failed construction leaked checked file");
      });
}

SEASTAR_TEST_CASE(local_metadata_fake_publication_limits) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto spec = specification(
            take(runtime::file_path::make("/kwaque/store")), {1, 1}, 51);
          take(co_await drive.lifecycle(files.create_directories(spec.root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::qualification_contract::publication_limits(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(local_metadata_fake_root_pressure_and_cancellation) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto spec = specification(
            take(runtime::file_path::make("/kwaque/store")), {1, 1}, 51);
          take(co_await drive.lifecycle(files.create_directories(spec.root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::qualification_contract::
            root_pressure_and_cancellation(files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(local_metadata_fake_failed_generation_open) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto spec = specification(
            take(runtime::file_path::make("/kwaque/store")), {1, 1}, 68);
          take(co_await drive.lifecycle(files.create_directories(spec.root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::qualification_contract::
            failed_generation_open(files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(local_metadata_fake_discovery_cancel_and_unsupported) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto spec = specification(
            take(runtime::file_path::make("/kwaque/store")), {1, 1}, 51);
          take(co_await drive.lifecycle(files.create_directories(spec.root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::qualification_contract::
            discovery_cancel_and_unsupported(files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(local_metadata_fake_allocation_rollback) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto spec = specification(
            take(runtime::file_path::make("/kwaque/store")), {1, 1}, 51);
          take(co_await drive.lifecycle(files.create_directories(spec.root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::qualification_contract::
            allocation_rollback(files, owner, spec, budget, drive);
      });
}

namespace {
// Drain runnable work in every scheduling group before advancing one backend
// event. Only reactor quiescence without an event is a parked completion.
enum class history_boundary { stepped, completed, parked };
template<typename T>
seastar::future<history_boundary>
history_step(scheduler& events, seastar::future<T>& operation) {
    if (operation.available()) co_return history_boundary::completed;
    co_await runtime::testing::drain_reactor_tasks();
    if (operation.available()) co_return history_boundary::completed;
    if (events.pending_events()) {
        if (!events.has_ready_events()) take(events.advance_to_next());
        require(take(events.step()), "history failed to advance its event");
        co_return history_boundary::stepped;
    }
    co_return history_boundary::parked;
}

std::string reserved_control() {
    auto wire = storage::testing::local_fixture::read("control_empty");
    storage::testing::put(wire, 88, 2, 8);
    storage::testing::put(wire, 120, 4, 8);
    storage::testing::repair(wire);
    return wire;
}

// Expected bytes and accepted outcome sets come from the driver's input,
// never from publication stages, decoded counters or an engine watermark.
bool publication_image_allowed(
  std::string_view actual,
  std::string_view old,
  std::string_view candidate,
  bool must_be_new) {
    return actual == candidate || (!must_be_new && actual == old);
}

struct publication_fault_objects final {
    std::uint64_t root{0}, parent{0}, temporary{0};
    runtime::fault_object_key select(runtime::builtin_fault_point point) const {
        using p = runtime::builtin_fault_point;
        if (point == p::file_open) return runtime::fault_object_key::none();
        if (point == p::file_stat)
            return runtime::fault_object_key::from_u64(root);
        if (
          point == p::directory_cursor_open || point == p::directory_sync
          || point == p::directory_cursor_close)
            return runtime::fault_object_key::from_u64(parent);
        return runtime::fault_object_key::from_u64(temporary);
    }
};
seastar::future<publication_fault_objects> publication_targets() {
    publication_fault_objects objects;
    co_await with_store_environment(
      config(), [&](auto& env, auto&, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto spec = specification(
            take(runtime::file_path::make("/kwaque/store")), {1, 1});
          co_await seed_initial(files, spec, drive);
          const auto path = take(take(local_paths::make(spec.root)).control(0));
          auto lookup = [&](std::string_view path) {
              return take(
                       fake_file_test_access::lookup(
                         files,
                         take(fake_file_test_access::resolve(files, path))))
                .value();
          };
          objects.root = lookup(spec.root.value());
          objects.parent = lookup(
            path.value().substr(0, path.value().rfind('/')));
          objects.temporary
            = take(fake_file_test_access::snapshot(files)).next_object_id;
      });
    co_return objects;
}

seastar::future<std::size_t> publication_history(
  environment& env,
  workload_budget& budget,
  simulation::testing::scheduler_driver drive,
  std::optional<std::size_t> cut,
  bool expect_parked = false,
  bool require_new = false,
  bool injected_failure = false,
  std::optional<bool> selected_candidate = std::nullopt) {
    auto& files = env.file_system();
    const auto spec = specification(
      take(runtime::file_path::make("/kwaque/store")), {1, 1});
    co_await seed_initial(files, spec, drive);
    const auto paths = take(local_paths::make(spec.root));
    const auto path = take(paths.control(0));
    const auto parent = take(
      runtime::file_path::make(
        path.value().substr(0, path.value().rfind('/'))));
    const auto old = storage::testing::local_fixture::read("control_empty");
    const auto candidate = reserved_control();
    const auto owner = take(spec.shard_owner(0));
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    local_file_publisher<fake_file_system> publisher{
      files,
      budget,
      {owner,
       spec.root,
       parent,
       take(runtime::file_name::make("control")),
       runtime::file_rename_policy::replace,
       storage::testing::publication_contract::generation(1)}};
    auto pending = publisher.publish(
      {owner,
       storage::testing::publication_contract::generation(2),
       storage::testing::publication_contract::generation(1)},
      take(bytes::fragmented_buffer::copy_of(candidate)),
      work);
    std::size_t steps = 0;
    history_boundary boundary = history_boundary::stepped;
    runtime::first_failure failed;
    bool consumed = false;
    try {
        while ((!cut || steps < *cut) && steps < 512) {
            boundary = co_await history_step(env.event_scheduler(), pending);
            if (boundary != history_boundary::stepped) break;
            ++steps;
        }
        require(steps < 512, "publication history exceeded its event bound");
        if (expect_parked)
            require(
              boundary == history_boundary::parked && !pending.available(),
              "lost completion did not retain the accepted operation");
        if (!cut && !expect_parked) {
            require(
              pending.available(), "publication stopped without a result");
            consumed = true;
            auto result = co_await std::move(pending);
            if (injected_failure)
                require(
                  result.failure.error()
                    && result.failure.error()->code() == errc::io_failure,
                  "history did not encounter its selected fault");
            require_new = require_new || !result.failure.failed();
        }
        // At a cut the caller has not received the local result. Accepted
        // backend work is settled by the crash and joined before reopen.
        take(co_await drive.lifecycle(files.crash()));
        if (!consumed) {
            consumed = true;
            // The earlier await marks consumed before moving pending.
            // NOLINTNEXTLINE(bugprone-use-after-move)
            auto result = co_await drive.lifecycle(std::move(pending));
            if (expect_parked)
                require(
                  result.failure.failed(),
                  "lost notification reported success after crash");
            require_new = require_new || !result.failure.failed();
        }
    } catch (...) {
        failed.observe(std::current_exception());
    }
    if (!consumed) {
        static_cast<void>(co_await drive.lifecycle(files.crash()));
        // Both earlier paths mark consumed before transferring the future.
        // NOLINTNEXTLINE(bugprone-use-after-move)
        static_cast<void>(co_await drive.lifecycle(std::move(pending)));
    }
    failed.observe(co_await drive.lifecycle(publisher.close()));
    take(failed.outcome());
    storage::testing::qualification_contract::released(budget);
    // A crashed process issues nothing more, but this stale publisher kept
    // running: resumed after the crash, it can rename without its directory
    // sync. Crash again so restart sees only what survived.
    take(co_await drive.lifecycle(files.crash()));
    const auto recovered = co_await read_bytes(files, path, drive);
    require(
      publication_image_allowed(recovered, old, candidate, require_new),
      "crash lost or fabricated the selected control bytes");
    if (selected_candidate)
        require(
          recovered == (*selected_candidate ? candidate : old),
          "fault history produced an impossible publication image");
    take(co_await drive.lifecycle(files.crash()));
    require(
      (co_await read_bytes(files, path, drive)) == recovered,
      "second crash changed the recovered control image");
    // Discovery must find both shard controls after every interrupted update.
    const std::array specs{spec};
    ownership_input ownership{specs};
    unsigned controls = 0;
    auto visit = [&](const local_namespace_entry& entry) {
        if (entry.kind == local_entry_kind::control) ++controls;
        return seastar::make_ready_future<runtime::result<bool>>(true);
    };
    auto inventory = take(
      co_await drive.lifecycle(walk_local_namespace(
        files, ownership, spec, budget, limits(), work, visit)));
    require(
      inventory.complete && controls == 2,
      "crash discovery fabricated an empty store");
    co_return steps;
}
} // namespace

SEASTAR_TEST_CASE(local_publication_oracle_rejects_lost_and_mixed_images) {
    const auto old = storage::testing::local_fixture::read("control_empty");
    const auto candidate = reserved_control();
    BOOST_CHECK(publication_image_allowed(old, old, candidate, false));
    BOOST_CHECK(publication_image_allowed(candidate, old, candidate, true));
    BOOST_CHECK(!publication_image_allowed(old, old, candidate, true));
    BOOST_CHECK(!publication_image_allowed({}, old, candidate, false));
    auto mixed = candidate;
    mixed[120] = old[120];
    BOOST_CHECK(!publication_image_allowed(mixed, old, candidate, false));
    co_return;
}

SEASTAR_TEST_CASE(local_publication_crashes_at_each_scheduled_boundary) {
    // Data, namespace and EOF are deliberately independent crash choices.
    for (unsigned mask = 0; mask != 8; ++mask) {
        const fake_crash_policy policy{
          .data_percent = static_cast<std::uint8_t>((mask & 1U) ? 100 : 0),
          .namespace_percent = static_cast<std::uint8_t>((mask & 2U) ? 100 : 0),
          .eof_percent = static_cast<std::uint8_t>((mask & 4U) ? 100 : 0)};
        std::size_t count = 0;
        co_await with_store_environment(
          config({}, runtime::fault_action::error, 1, {}, policy),
          [&](auto& env, auto& budget, auto drive) -> seastar::future<> {
              count = co_await publication_history(env, budget, drive, {});
          });
        require(count != 0 && count < 512, "history had no effect boundaries");
        for (std::size_t cut = 0; cut <= count; ++cut) {
            BOOST_TEST_CONTEXT("survival=" << mask << " cut=" << cut) {
                co_await with_store_environment(
                  config({}, runtime::fault_action::error, 1, {}, policy),
                  [cut](
                    auto& env, auto& budget, auto drive) -> seastar::future<> {
                      static_cast<void>(
                        co_await publication_history(env, budget, drive, cut));
                  });
            }
        }
    }
}

SEASTAR_TEST_CASE(
  local_publication_failed_effects_and_lost_notifications_survive_crash) {
    using point = runtime::builtin_fault_point;
    using action = runtime::fault_action;
    const auto before = action::file_failure_before_effect;
    const auto after = action::file_failure_after_effect;
    const auto lost = action::drop_completion;
    const auto objects = co_await publication_targets();
    const std::array cases{
      std::pair{point::file_stat, before},
      std::pair{point::file_stat, lost},
      std::pair{point::directory_cursor_open, before},
      std::pair{point::directory_cursor_open, lost},
      std::pair{point::file_open, before},
      std::pair{point::file_open, lost},
      std::pair{point::file_write, before},
      std::pair{point::file_write, after},
      std::pair{point::file_write, action::file_failure_after_prefix},
      std::pair{point::file_write, lost},
      std::pair{point::file_flush, before},
      std::pair{point::file_flush, after},
      std::pair{point::file_flush, lost},
      std::pair{point::file_close, before},
      std::pair{point::file_close, lost},
      std::pair{point::file_rename, before},
      std::pair{point::file_rename, after},
      std::pair{point::file_rename, lost},
      std::pair{point::directory_sync, before},
      std::pair{point::directory_sync, after},
      std::pair{point::directory_sync, lost},
      std::pair{point::directory_cursor_close, before},
      std::pair{point::directory_cursor_close, lost}};
    for (unsigned mask = 0; mask != 8; ++mask) {
        const fake_crash_policy policy{
          .data_percent = static_cast<std::uint8_t>((mask & 1U) ? 100 : 0),
          .namespace_percent = static_cast<std::uint8_t>((mask & 2U) ? 100 : 0),
          .eof_percent = static_cast<std::uint8_t>((mask & 4U) ? 100 : 0)};
        for (const auto [fault, effect] : cases) {
            BOOST_TEST_CONTEXT(
              "survival=" << mask << " point=" << static_cast<unsigned>(fault)
                          << " effect=" << static_cast<unsigned>(effect)) {
                co_await with_store_environment(
                  config(fault, effect, 1, objects.select(fault), policy),
                  [fault, effect, mask](
                    auto& env, auto& budget, auto drive) -> seastar::future<> {
                      const bool synced
                        = fault == point::directory_cursor_close
                          || (fault == point::directory_sync && effect != action::file_failure_before_effect);
                      static_cast<void>(co_await publication_history(
                        env,
                        budget,
                        drive,
                        {},
                        effect == action::drop_completion,
                        synced,
                        true,
                        synced
                          || ((mask & 2U) && (fault == point::directory_sync || (fault == point::file_rename && effect != action::file_failure_before_effect)))));
                  });
            }
        }
    }
}

SEASTAR_TEST_CASE(
  local_publication_cleanup_error_keeps_original_cause_and_old_file) {
    const auto objects = co_await publication_targets();
    const auto cleanup = take(
      fault_rule::make(
        take(fault_rule_id::make(2)),
        runtime::builtin_fault_point::file_remove,
        runtime::fault_object_key::from_u64(objects.temporary),
        runtime::fault_occurrence::first(),
        runtime::fault_occurrence::first(),
        fault_selector::once(),
        runtime::fault_decision::make_error()));
    co_await with_store_environment(
      config(
        runtime::builtin_fault_point::file_write,
        runtime::fault_action::file_failure_before_effect,
        1,
        objects.select(runtime::builtin_fault_point::file_write),
        fake_crash_policy{.namespace_percent = 100},
        cleanup),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          static_cast<void>(co_await publication_history(
            env, budget, drive, {}, false, false, true, false));
      });
}

SEASTAR_TEST_CASE(local_parent_creation_requires_its_own_namespace_barrier) {
    using point = runtime::builtin_fault_point;
    using action = runtime::fault_action;
    const std::array cases{
      std::pair{point::directory_create, action::file_failure_before_effect},
      std::pair{point::directory_create, action::drop_completion},
      std::pair{point::directory_sync, action::file_failure_before_effect},
      std::pair{point::directory_sync, action::file_failure_after_effect},
      std::pair{point::directory_sync, action::drop_completion},
      std::pair{
        point::directory_cursor_close, action::file_failure_before_effect},
      std::pair{point::directory_cursor_close, action::drop_completion}};
    for (const auto survival : {std::uint8_t{0}, std::uint8_t{100}}) {
        for (const auto [fault, effect] : cases) {
            co_await with_store_environment(
              config(
                fault,
                effect,
                1,
                {},
                fake_crash_policy{.namespace_percent = survival}),
              [fault, effect, survival](
                auto& env, auto&, auto drive) -> seastar::future<> {
                  auto& files = env.file_system();
                  const auto spec = specification(
                    take(runtime::file_path::make("/kwaque/store")), {1, 1});
                  take(
                    fake_file_test_access::create_directory(
                      files,
                      take(
                        fake_file_test_access::resolve(
                          files, spec.root.value()))));
                  stabilize(files, take(runtime::file_path::make("/kwaque")));
                  seastar::abort_source abort;
                  codec::cooperative_work work{
                    codec::limits::defaults(), abort};
                  const auto name = take(runtime::file_name::make("shards"));
                  const auto path = take(local_child_path(spec.root, name));
                  auto pending = storage::detail::ensure_local_directory(
                    files, spec, spec.root, name, work);
                  history_boundary boundary = history_boundary::stepped;
                  for (unsigned steps = 0; steps != 128; ++steps) {
                      boundary = co_await history_step(
                        env.event_scheduler(), pending);
                      if (boundary != history_boundary::stepped) break;
                  }
                  const bool parked = boundary == history_boundary::parked;
                  // Join first, then assert, including a lost close completion.
                  take(co_await drive.lifecycle(files.crash()));
                  const auto result = co_await drive.lifecycle(
                    std::move(pending));
                  require(
                    !result, "selected parent barrier failure was ignored");
                  require(
                    parked == (effect == action::drop_completion),
                    "parent notification cut did not park");
                  const bool created = !(
                    fault == point::directory_create
                    && effect == action::file_failure_before_effect);
                  const bool barrier
                    = fault == point::directory_cursor_close
                      || (fault == point::directory_sync && effect != action::file_failure_before_effect);
                  const bool expected = created && (barrier || survival == 100);
                  for (unsigned restart = 0; restart != 2; ++restart) {
                      require(
                        take(co_await drive.lifecycle(files.exists(path)))
                          == expected,
                        "parent directory durability was inferred from child "
                        "creation");
                      if (restart == 0)
                          take(co_await drive.lifecycle(files.crash()));
                  }
              });
        }
    }
}

SEASTAR_TEST_CASE(
  local_control_uncertainty_is_reconciled_only_after_crash_and_reopen) {
    const auto objects = co_await publication_targets();
    for (const auto survival : {std::uint8_t{0}, std::uint8_t{100}}) {
        for (const auto effect :
             {runtime::fault_action::file_failure_before_effect,
              runtime::fault_action::file_failure_after_effect}) {
            co_await with_store_environment(
              config(
                runtime::builtin_fault_point::file_rename,
                effect,
                1,
                objects.select(runtime::builtin_fault_point::file_rename),
                fake_crash_policy{.namespace_percent = survival}),
              [effect, survival](
                auto& env, auto& budget, auto drive) -> seastar::future<> {
                  auto& files = env.file_system();
                  const auto spec = specification(
                    take(runtime::file_path::make("/kwaque/store")), {1, 1});
                  const std::array specs{spec};
                  ownership_input owner{specs};
                  co_await seed_initial(files, spec, drive);
                  seastar::abort_source abort;
                  codec::cooperative_work work{
                    codec::limits::defaults(), abort};
                  using control_type
                    = local_control_owner<fake_file_system, ownership_input>;
                  auto control = take(
                    co_await drive.lifecycle(
                      control_type::open(
                        files, owner, spec, 0, false, budget, limits(), work)));
                  runtime::first_failure failed;
                  try {
                      auto result = co_await drive.lifecycle(control->update(
                        [](local_shard_control& next) -> runtime::result<void> {
                            next.object_high = local_object_high{4};
                            return {};
                        },
                        work));
                      require(
                        result.failure.error()
                          && result.failure.error()->code() == errc::io_failure,
                        "control publication missed selected rename fault");
                      require(
                        control->fenced() && !control->snapshot(),
                        "uncertain owner exposed a current control");
                      bool edited = false;
                      auto denied = co_await drive.lifecycle(control->update(
                        [&](local_shard_control&) -> runtime::result<void> {
                            edited = true;
                            return {};
                        },
                        work));
                      require(
                        denied.failure.failed() && !edited,
                        "uncertain owner accepted another update");
                  } catch (...) {
                      failed.observe(std::current_exception());
                  }
                  failed.observe(co_await drive.lifecycle(control->close()));
                  control.reset();
                  take(failed.outcome());
                  const bool new_image
                    = effect == runtime::fault_action::file_failure_after_effect
                      && survival == 100;
                  const auto expected
                    = new_image ? reserved_control()
                                : storage::testing::local_fixture::read(
                                    "control_empty");
                  const auto path = take(
                    take(local_paths::make(spec.root)).control(0));
                  for (unsigned restart = 0; restart != 2; ++restart) {
                      take(co_await drive.lifecycle(files.crash()));
                      require(
                        (co_await read_bytes(files, path, drive)) == expected,
                        "control crash image violated the independent rename "
                        "history");
                      control = take(
                        co_await drive.lifecycle(
                          control_type::open(
                            files,
                            owner,
                            spec,
                            0,
                            false,
                            budget,
                            limits(),
                            work)));
                      try {
                          const auto snapshot = take(control->snapshot());
                          require(
                            snapshot.generation.value() == (new_image ? 2U : 1U)
                              && snapshot.fields.object_high.value()
                                   == (new_image ? 4U : 0U)
                              && !snapshot.fields.wal_head,
                            "reopen fabricated activation or allocation state");
                      } catch (...) {
                          failed.observe(std::current_exception());
                      }
                      failed.observe(
                        co_await drive.lifecycle(control->close()));
                      control.reset();
                      take(failed.outcome());
                  }
              });
        }
    }
}

SEASTAR_TEST_CASE(local_metadata_fake_failed_replacement_keeps_old_pin) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto spec = specification(
            take(runtime::file_path::make("/kwaque/store")), {1, 1}, 68);
          take(co_await drive.lifecycle(files.create_directories(spec.root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::qualification_contract::
            failed_replacement_keeps_old_pin(files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(local_immutable_collision_survives_repeated_crash) {
    for (const auto survival : {std::uint8_t{0}, std::uint8_t{100}}) {
        co_await with_store_environment(
          config(
            {},
            runtime::fault_action::error,
            1,
            {},
            fake_crash_policy{.namespace_percent = survival}),
          [](auto& env, auto& budget, auto drive) -> seastar::future<> {
              auto& files = env.file_system();
              const auto spec = specification(
                take(runtime::file_path::make("/kwaque/store")), {1, 1});
              co_await seed_initial(files, spec, drive);
              const auto path = take(
                take(local_paths::make(spec.root)).store());
              const auto name = take(
                runtime::file_name::make(
                  path.value().substr(path.value().rfind('/') + 1)));
              local_file_publisher<fake_file_system> publisher{
                files,
                budget,
                {spec.owner,
                 spec.root,
                 spec.root,
                 name,
                 runtime::file_rename_policy::no_replace,
                 {}}};
              seastar::abort_source abort;
              codec::cooperative_work work{codec::limits::defaults(), abort};
              runtime::first_failure failed;
              try {
                  auto result = co_await drive.lifecycle(publisher.publish(
                    {spec.owner,
                     storage::testing::publication_contract::generation(1),
                     {}},
                    take(
                      bytes::fragmented_buffer::copy_of(
                        storage::testing::local_fixture::read(
                          "foreign_device"))),
                    work));
                  require(
                    result.failure.error()
                      && result.failure.error()->code() == errc::already_exists,
                    "immutable final collision did not reject");
                  require(
                    result.disposition
                      == local_publication_disposition::untouched,
                    "immutable collision changed disposition");
              } catch (...) {
                  failed.observe(std::current_exception());
              }
              failed.observe(co_await drive.lifecycle(publisher.close()));
              take(failed.outcome());
              for (unsigned restart = 0; restart != 2; ++restart) {
                  take(co_await drive.lifecycle(files.crash()));
                  require(
                    (co_await read_bytes(files, path, drive))
                      == storage::testing::local_fixture::read("store"),
                    "crash resurrected a colliding immutable replacement");
              }
          });
    }
}

SEASTAR_TEST_CASE(local_discovery_crash_is_an_error_then_reopens_complete) {
    const auto objects = co_await publication_targets();
    co_await with_store_environment(
      config(
        runtime::builtin_fault_point::directory_cursor_next,
        runtime::fault_action::drop_completion,
        1,
        runtime::fault_object_key::from_u64(objects.root),
        fake_crash_policy{}),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto spec = specification(
            take(runtime::file_path::make("/kwaque/store")), {1, 1});
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await seed_initial(files, spec, drive);
          seastar::abort_source abort;
          codec::cooperative_work work{codec::limits::defaults(), abort};
          unsigned controls = 0;
          auto visit = [&](const local_namespace_entry& entry) {
              if (entry.kind == local_entry_kind::control) ++controls;
              return seastar::make_ready_future<runtime::result<bool>>(true);
          };
          auto pending = walk_local_namespace(
            files, owner, spec, budget, limits(), work, visit);
          history_boundary boundary = history_boundary::stepped;
          for (unsigned steps = 0; steps != 128; ++steps) {
              boundary = co_await history_step(env.event_scheduler(), pending);
              if (boundary != history_boundary::stepped) break;
          }
          take(co_await drive.lifecycle(files.crash()));
          const auto interrupted = co_await drive.lifecycle(std::move(pending));
          require(
            boundary == history_boundary::parked && !interrupted,
            "crashed directory cursor fabricated successful inventory");
          storage::testing::qualification_contract::released(budget);
          for (unsigned restart = 0; restart != 2; ++restart) {
              controls = 0;
              const auto inventory = take(
                co_await drive.lifecycle(walk_local_namespace(
                  files, owner, spec, budget, limits(), work, visit)));
              require(
                inventory.complete && controls == 2,
                "reopened discovery lost durable controls");
              if (restart == 0) take(co_await drive.lifecycle(files.crash()));
          }
      });
}

SEASTAR_TEST_CASE(local_metadata_fake_read_limits) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto spec = specification(
            take(runtime::file_path::make("/kwaque/store")), {1, 1});
          take(co_await drive.lifecycle(files.create_directories(spec.root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::qualification_contract::
            metadata_read_limits(files, owner, spec, budget, drive);
          const auto path = take(take(local_paths::make(spec.root)).store());
          seastar::abort_source abort;
          codec::cooperative_work work{codec::limits::defaults(), abort};
          for (const std::uint64_t cap : {31U, 1024U}) {
              auto bounded = limits();
              bounded.operation_bytes = byte_count{cap};
              bounded.metadata_bytes = byte_count{cap};
              const auto before = fake_file_test_access::submitted(
                files, fake_submission_kind::read);
              auto result = co_await drive.lifecycle(read_local_metadata_file(
                files, spec.root, path, budget, bounded, work));
              require(
                !result && result.error().code() == errc::resource_exhausted,
                "read admission did not reject the configured allowance");
              require(
                fake_file_test_access::submitted(
                  files, fake_submission_kind::read)
                    - before
                  == (cap < codec::envelope_prefix_bytes ? 0U : 1U),
                "metadata admission dispatched an unbudgeted body read");
          }
      });
}

SEASTAR_TEST_CASE(segment_writer_fake_creation) {
    co_await with_store_environment(
      config(),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 68);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::segment_writer_contract::creation<
            simulation::monotonic_clock>(files, owner, spec, budget, drive);
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol);
}

SEASTAR_TEST_CASE(segment_writer_fake_admission) {
    co_await with_store_environment(
      config(),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 68);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::segment_writer_contract::admission<
            simulation::monotonic_clock>(files, owner, spec, budget, drive);
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol);
}

SEASTAR_TEST_CASE(segment_writer_fake_reserved_publication) {
    co_await with_store_environment(
      config(),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 68);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::segment_writer_contract::
            reserved_publication(files, owner, spec, budget, drive);
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol);
}

SEASTAR_TEST_CASE(segment_writer_fake_rejects_incompatible_data_geometry) {
    co_await with_store_environment(
      config(),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          namespace contract = storage::testing::segment_writer_contract;
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 68);
          const std::array specs{spec};
          ownership_input owner{specs};
          seastar::abort_source abort;
          codec::cooperative_work work{codec::limits::defaults(), abort};
          co_await storage::testing::installation_contract::bootstrap(
            files, owner, spec, budget, work, drive);
          auto description = contract::descriptor();
          description.alignment = storage::testing::alignment(512);
          using writer_type = storage::segment_writer<
            fake_file_system,
            ownership_input,
            simulation::monotonic_clock>;
          auto writer = take(
            writer_type::make_new(
              files,
              owner,
              spec,
              0,
              description,
              budget,
              contract::configuration()));
          runtime::first_failure failed;
          try {
              auto created = co_await drive.lifecycle(writer->create_new(work));
              require(
                !created && created.error().code() == errc::invalid_argument
                  && writer->append_state() == model::append_state::creating
                  && !writer->capture(),
                "unsupported DMA geometry opened segment admission");
          } catch (...) {
              failed.observe(std::current_exception());
          }
          try {
              static_cast<void>(co_await drive.lifecycle(writer->close()));
          } catch (...) {
              failed.observe(std::current_exception());
          }
          writer.reset();
          take(failed.outcome());
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol);
}

SEASTAR_TEST_CASE(segment_writer_fake_execution) {
    co_await with_store_environment(
      config(),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 68);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::segment_writer_contract::execution<
            simulation::monotonic_clock>(files, owner, spec, budget, drive);
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_barriers) {
    co_await with_store_environment(
      config(),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 68);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::segment_writer_contract::execution<
            simulation::monotonic_clock>(
            files, owner, spec, budget, drive, true);
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_grouped_execution) {
    co_await with_store_environment(
      config(),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 68);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::segment_writer_contract::grouped_execution<
            simulation::monotonic_clock>(files, owner, spec, budget, drive);
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_preallocated_execution) {
    co_await with_store_environment(
      config(),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 68);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::segment_writer_contract::
            preallocated_execution<simulation::monotonic_clock>(
              files, owner, spec, budget, drive);
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_concurrent_execution) {
    co_await with_store_environment(
      config(),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 68);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::segment_writer_contract::
            concurrent_execution<simulation::monotonic_clock>(
              files, owner, spec, budget, drive);
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_extended_execution) {
    co_await with_store_environment(
      config(),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 68);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::segment_writer_contract::
            extended_execution<simulation::monotonic_clock>(
              files, owner, spec, budget, drive);
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_abandoned_group) {
    co_await with_store_environment(
      config(),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 68);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::segment_writer_contract::abandoned_group<
            simulation::monotonic_clock>(files, owner, spec, budget, drive);
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_close_preserves_borrowed_blocks) {
    co_await with_store_environment(
      config(),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 68);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::segment_writer_contract::abandoned_group<
            simulation::monotonic_clock>(
            files, owner, spec, budget, drive, true);
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

namespace {
struct segment_write_fault_target final {
    std::uint64_t object{}, occurrence{};
};
seastar::future<segment_write_fault_target> segment_write_history(
  environment& env,
  workload_budget& resources,
  simulation::testing::scheduler_driver drive,
  bool expect_failure,
  bool detach) {
    namespace contract = storage::testing::segment_writer_contract;
    auto& files = env.file_system();
    const auto root = take(runtime::file_path::make("/kwaque/store"));
    take(co_await drive.lifecycle(files.create_directories(root)));
    stabilize(files, take(runtime::file_path::make("/kwaque")));
    const auto spec = specification(root, {1, 1}, 68);
    const std::array specs{spec};
    ownership_input owner{specs};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await storage::testing::installation_contract::bootstrap(
      files, owner, spec, resources, work, drive);
    auto writer = take(
      segment_writer<
        fake_file_system,
        ownership_input,
        simulation::monotonic_clock>::
        make_new(
          files,
          owner,
          spec,
          0,
          contract::descriptor(),
          resources,
          contract::configuration()));
    runtime::first_failure failed;
    segment_write_fault_target target;
    try {
        take(co_await drive.lifecycle(writer->create_new(work)));
        const auto initial = writer->progress()->written;
        const auto selected = take(local_paths::make(root));
        const auto path = take(selected.segment_file(
          0,
          {contract::descriptor().segment.segment(),
           contract::descriptor().segment.generation()},
          local_segment_file::data));
        target.object
          = take(
              fake_file_test_access::lookup(
                files,
                take(fake_file_test_access::resolve(files, path.value()))))
              .value();
        std::uint64_t flushes = 0;
        for (const auto& inode :
             take(fake_file_test_access::snapshot(files)).objects) {
            if (inode.id != target.object) continue;
            target.occurrence = inode.occurrences[static_cast<std::size_t>(
                                  runtime::builtin_fault_point::file_write)]
                                + 1;
            flushes = inode.occurrences[static_cast<std::size_t>(
              runtime::builtin_fault_point::file_flush)];
        }
        require(target.occurrence != 0, "missing segment fault target");
        auto group = co_await contract::freeze_child(
          *writer, co_await contract::child(work), resources, work);
        const auto cut = group.layout().boundary();
        take(co_await writer->encode_group(group, work));
        const auto block_bytes = storage::testing::flat(
          group.blocks()[0].bytes());
        const auto footer = storage::testing::footer_wire(
          {cut.covered(),
           cut.end().blocks,
           group.layout().blocks().back().records,
           storage::testing::crc(block_bytes)},
          {cut.history(), cut.footer()->begin()});
        const auto expected_bytes = block_bytes + footer;
        const auto before = fake_file_test_access::submitted(
          files, fake_submission_kind::write);
        auto submission = take(writer->submit(std::move(group), work));
        co_await runtime::testing::drain_reactor_tasks();
        require(
          fake_file_test_access::submitted(files, fake_submission_kind::write)
              > before
            && !submission.written.available()
            && writer->progress()->written == initial,
          "parked device write released or advanced its segment group");
        std::optional<seastar::future<segment_write_completion>> waiting{
          std::move(submission.written)};
        if (detach) waiting.reset();
        abort.request_abort();
        auto closing = writer->close();
        const bool close_was_pending = !closing.available();
        // Join before checking outcomes: a failed expectation must not discard
        // an active close and turn its original error into a teardown abort.
        const auto closed = co_await drive.lifecycle(std::move(closing));
        require(close_was_pending, "close did not join parked segment bytes");
        if (waiting) {
            const auto done = co_await drive.lifecycle(std::move(*waiting));
            require(
              done.failure.failed() == expect_failure,
              "segment lost its write outcome");
            if (!expect_failure)
                require(
                  done.written.value() == expected_bytes.size(),
                  "short write was not completed exactly");
            if (expect_failure)
                require(
                  done.failure.error() == writer->failure().error(),
                  "segment replaced its first write failure");
        }
        require(
          closed.has_value() != expect_failure
            && writer->progress()->written
                 == (expect_failure ? initial : cut.end())
            && writer->progress()->durable == initial
            && writer->progress()->reserved == cut.end(),
          "close changed reserved coordinates or fabricated completion");
        for (const auto& inode :
             take(fake_file_test_access::snapshot(files)).objects) {
            if (inode.id != target.object) continue;
            require(
              inode.durable_size == initial.bytes.value()
                && inode.occurrences[static_cast<std::size_t>(
                     runtime::builtin_fault_point::file_flush)]
                     == flushes,
              "segment write or close issued an unsolicited barrier");
        }
        if (!expect_failure) {
            const auto actual
              = co_await storage::testing::store_contract::read_bytes(
                files, path, drive);
            require(
              actual.substr(initial.bytes.value()) == expected_bytes,
              "parked write changed its immutable payload");
        }
    } catch (...) {
        failed.observe(std::current_exception());
    }
    try {
        static_cast<void>(co_await drive.lifecycle(writer->close()));
    } catch (...) {
        failed.observe(std::current_exception());
    }
    writer.reset();
    take(failed.outcome());
    co_return target;
}
} // namespace

SEASTAR_TEST_CASE(
  segment_writer_fake_joins_parked_short_failed_and_detached_writes) {
    segment_write_fault_target target;
    co_await with_store_environment(
      config(),
      [&](auto& env, auto& budget, auto drive) -> seastar::future<> {
          target = co_await segment_write_history(
            env, budget, drive, false, false);
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
    const std::array decisions{
      // Only the first short completion preserves the native disk alignment.
      runtime::fault_decision::make_short_operation(byte_count{4_KiB}),
      runtime::fault_decision::make_short_operation(byte_count{512}),
      runtime::fault_decision::make_short_operation(byte_count{}),
      take(
        runtime::fault_decision::make_file_failure(
          runtime::fault_action::file_failure_after_prefix,
          runtime::file_failure_detail::no_space,
          byte_count{512})),
      take(
        runtime::fault_decision::make_file_failure(
          runtime::fault_action::file_failure_before_effect,
          runtime::file_failure_detail::device_io,
          byte_count{}))};
    for (std::size_t i = 0; i < decisions.size(); ++i) {
        const auto rule = take(
          fault_rule::make(
            take(fault_rule_id::make(2)),
            runtime::builtin_fault_point::file_write,
            runtime::fault_object_key::from_u64(target.object),
            take(runtime::fault_occurrence::make(target.occurrence)),
            take(runtime::fault_occurrence::make(target.occurrence)),
            fault_selector::once(),
            decisions[i]));
        co_await with_store_environment(
          config(
            std::nullopt,
            runtime::fault_action::file_failure_before_effect,
            1,
            std::nullopt,
            std::nullopt,
            rule),
          [i](auto& env, auto& budget, auto drive) -> seastar::future<> {
              static_cast<void>(co_await segment_write_history(
                env, budget, drive, i != 0, false));
          },
          byte_count{18_MiB},
          resource::workload_class::foreground_protocol,
          64);
    }
    co_await with_store_environment(
      config(),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          static_cast<void>(
            co_await segment_write_history(env, budget, drive, false, true));
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

namespace {
struct segment_barrier_targets final {
    std::uint64_t object{}, write{}, flush{};
};
seastar::future<segment_barrier_targets> segment_barrier_history(
  environment& env,
  workload_budget& resources,
  simulation::testing::scheduler_driver drive,
  bool write_failure = false,
  bool flush_failure = false,
  bool close_pending = false) {
    namespace contract = storage::testing::segment_writer_contract;
    auto& files = env.file_system();
    const auto root = take(runtime::file_path::make("/kwaque/store"));
    take(co_await drive.lifecycle(files.create_directories(root)));
    stabilize(files, take(runtime::file_path::make("/kwaque")));
    const auto spec = specification(root, {1, 1}, 68);
    const std::array specs{spec};
    ownership_input owner{specs};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await storage::testing::installation_contract::bootstrap(
      files, owner, spec, resources, work, drive);
    auto options = contract::configuration();
    options.admission.working_bytes = byte_count{1_MiB};
    auto writer = take(
      segment_writer<
        fake_file_system,
        ownership_input,
        simulation::monotonic_clock>::
        make_new(
          files, owner, spec, 0, contract::descriptor(), resources, options));
    runtime::first_failure failed;
    segment_barrier_targets targets;
    try {
        take(co_await drive.lifecycle(writer->create_new(work)));
        const auto initial = writer->progress()->durable;
        const auto empty = co_await drive.lifecycle(
          writer->barrier(take(writer->capture())));
        require(
          !empty.receipt && empty.failure.failed(),
          "empty header manufactured a segment receipt");
        const auto paths = take(local_paths::make(root));
        const auto path = take(paths.segment_file(
          0,
          {contract::descriptor().segment.segment(),
           contract::descriptor().segment.generation()},
          local_segment_file::data));
        targets.object
          = take(
              fake_file_test_access::lookup(
                files,
                take(fake_file_test_access::resolve(files, path.value()))))
              .value();
        auto inode = [&] {
            auto snapshot = take(fake_file_test_access::snapshot(files));
            for (auto& value : snapshot.objects)
                if (value.id == targets.object) return std::move(value);
            throw std::runtime_error("missing segment barrier inode");
        };
        const auto before = inode();
        constexpr auto write_index = static_cast<std::size_t>(
          runtime::builtin_fault_point::file_write);
        constexpr auto flush_index = static_cast<std::size_t>(
          runtime::builtin_fault_point::file_flush);
        targets.write = before.occurrences[write_index] + 1;
        targets.flush = before.occurrences[flush_index] + 1;
        auto first = co_await contract::freeze_child(
          *writer, co_await contract::child(work), resources, work);
        auto second = co_await contract::freeze_child(
          *writer,
          co_await contract::execution_child(101, work),
          resources,
          work);
        take(co_await writer->encode_group(first, work));
        take(co_await writer->encode_group(second, work));
        const auto first_cut = first.layout().boundary();
        const auto second_cut = second.layout().boundary();
        auto one = take(writer->submit(std::move(first), work));
        auto barrier = writer->barrier(first_cut);
        const auto busy = co_await drive.lifecycle(writer->barrier(first_cut));
        co_await runtime::testing::drain_reactor_tasks();
        const bool ordered = inode().occurrences[flush_index]
                               == before.occurrences[flush_index]
                             && !barrier.available() && !one.written.available()
                             && writer->progress()->durable == initial;
        auto two = take(writer->submit(std::move(second), work));
        const auto admission = resources.snapshot();
        resources.close_admission();
        abort.request_abort(); // Detached caller interest cannot cancel
                               // accepted work.
        std::optional<seastar::future<runtime::result<void>>> closing;
        if (close_pending) closing.emplace(writer->close());
        const auto result = co_await drive.lifecycle(std::move(barrier));
        const auto written_one = co_await drive.lifecycle(
          std::move(one.written));
        const auto written_two = co_await drive.lifecycle(
          std::move(two.written));
        if (closing) {
            const auto closed = co_await drive.lifecycle(std::move(*closing));
            closing.reset();
            take(closed);
        }
        require(
          ordered, "flush ran before captured block/footer writes completed");
        require(
          !busy.receipt && busy.failure.error()
            && busy.failure.error()->code() == errc::queue_full,
          "overlapping segment barriers were not bounded");
        const auto after = inode();
        if (write_failure || flush_failure) {
            require(
              result.failure.failed() && !result.receipt
                && writer->progress()->durable == initial,
              "failed write/flush manufactured a durable cut");
            if (write_failure)
                require(
                  written_one.failure.failed()
                    && after.occurrences[flush_index]
                         == before.occurrences[flush_index],
                  "failed lower write did not suppress the captured flush");
            else
                require(
                  after.occurrences[flush_index] == targets.flush,
                  "flush failure did not hit the selected captured cut");
            const auto retry = co_await drive.lifecycle(
              writer->barrier(first_cut));
            require(
              retry.failure.failed() && !retry.receipt
                && inode().occurrences[flush_index]
                     == after.occurrences[flush_index],
              "a later barrier repaired an uncertain earlier operation");
        } else {
            take(written_one.failure.outcome());
            take(written_two.failure.outcome());
            take(result.failure.outcome());
            require(
              result.receipt && result.receipt->boundary() == first_cut
                && writer->progress()->written == second_cut.end()
                && writer->progress()->durable == first_cut.end()
                && after.occurrences[flush_index] == targets.flush,
              "captured barrier promoted a higher write or omitted its flush");
            require(
              after.durable_size >= first_cut.end().bytes.value(),
              "successful receipt exceeded the persisted byte prefix");
            if (!close_pending) {
                const auto repeated = co_await drive.lifecycle(
                  writer->barrier(first_cut));
                take(repeated.failure.outcome());
                require(
                  repeated.receipt && repeated.receipt->boundary() == first_cut
                    && inode().occurrences[flush_index] == targets.flush,
                  "same captured success issued another physical flush");
                // The first flush covered every ordinary write completed
                // before it began, and the second group's write may or may
                // not have been among them. The later cut needs one more
                // flush exactly when it was not.
                const auto [completed, flushed]
                  = segment_writer_test_access::plain_writes(*writer);
                const auto own_flush = flushed < completed ? 1U : 0U;
                const auto next = co_await drive.lifecycle(
                  writer->barrier(second_cut));
                take(next.failure.outcome());
                require(
                  next.receipt && next.receipt->boundary() == second_cut
                    && writer->progress()->durable == second_cut.end()
                    && inode().occurrences[flush_index]
                         == targets.flush + own_flush
                    && inode().durable_size >= second_cut.end().bytes.value(),
                  "later captured cut did not receive its own successful "
                  "barrier");
            }
        }
        require(
          resources.snapshot().accepted == admission.accepted
            && resources.snapshot().rejected == admission.rejected,
          "accepted evidence/barrier work reacquired ordinary admission");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    try {
        static_cast<void>(co_await drive.lifecycle(writer->close()));
    } catch (...) {
        failed.observe(std::current_exception());
    }
    writer.reset();
    take(failed.outcome());
    co_return targets;
}
} // namespace

SEASTAR_TEST_CASE(
  segment_writer_fake_captured_barrier_order_failure_reuse_and_close) {
    segment_barrier_targets targets;
    co_await with_store_environment(
      config(),
      [&](auto& env, auto& budget, auto drive) -> seastar::future<> {
          targets = co_await segment_barrier_history(env, budget, drive);
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
    for (const auto action :
         {runtime::fault_action::file_failure_before_effect,
          runtime::fault_action::file_failure_after_effect}) {
        co_await with_store_environment(
          config(
            runtime::builtin_fault_point::file_flush,
            action,
            targets.flush,
            runtime::fault_object_key::from_u64(targets.object)),
          [](auto& env, auto& budget, auto drive) -> seastar::future<> {
              static_cast<void>(co_await segment_barrier_history(
                env, budget, drive, false, true));
          },
          byte_count{18_MiB},
          resource::workload_class::foreground_protocol,
          64);
    }
    co_await with_store_environment(
      config(
        runtime::builtin_fault_point::file_write,
        runtime::fault_action::file_failure_after_prefix,
        targets.write,
        runtime::fault_object_key::from_u64(targets.object)),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          static_cast<void>(
            co_await segment_barrier_history(env, budget, drive, true));
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
    co_await with_store_environment(
      config(),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          static_cast<void>(co_await segment_barrier_history(
            env, budget, drive, false, false, true));
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_seal) {
    co_await with_store_environment(
      config(),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 68);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::segment_writer_contract::seal_lifecycle<
            simulation::monotonic_clock>(files, owner, spec, budget, drive);
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_empty_seal) {
    co_await with_store_environment(
      config(),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 68);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::segment_writer_contract::seal_lifecycle<
            simulation::monotonic_clock>(
            files, owner, spec, budget, drive, true);
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_reserved_seal) {
    co_await with_store_environment(
      config(),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 68);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::segment_writer_contract::seal_lifecycle<
            simulation::monotonic_clock>(
            files, owner, spec, budget, drive, false, true);
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_changed_seal_source) {
    co_await with_store_environment(
      config(),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 68);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::segment_writer_contract::seal_lifecycle<
            simulation::monotonic_clock>(
            files, owner, spec, budget, drive, false, false, true);
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_immutable_empty_initial) {
    co_await with_store_environment(
      config(),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 68);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::segment_writer_contract::immutable_import<
            simulation::monotonic_clock>(
            files,
            owner,
            spec,
            budget,
            drive,
            storage::testing::segment_writer_contract::immutable_import_kind::
              empty_initial);
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_immutable_sparse_rewrite) {
    co_await with_store_environment(
      config(),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 68);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::segment_writer_contract::immutable_import<
            simulation::monotonic_clock>(
            files,
            owner,
            spec,
            budget,
            drive,
            storage::testing::segment_writer_contract::immutable_import_kind::
              sparse_rewrite);
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_immutable_dense_relocation) {
    co_await with_store_environment(
      config(),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 68);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::segment_writer_contract::immutable_import<
            simulation::monotonic_clock>(
            files,
            owner,
            spec,
            budget,
            drive,
            storage::testing::segment_writer_contract::immutable_import_kind::
              dense_relocation);
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_immutable_removed_terminal) {
    co_await with_store_environment(
      config(),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 68);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::segment_writer_contract::immutable_import<
            simulation::monotonic_clock>(
            files,
            owner,
            spec,
            budget,
            drive,
            storage::testing::segment_writer_contract::immutable_import_kind::
              removed_terminal);
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_immutable_empty_terminal) {
    co_await with_store_environment(
      config(),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 68);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::segment_writer_contract::immutable_import<
            simulation::monotonic_clock>(
            files,
            owner,
            spec,
            budget,
            drive,
            storage::testing::segment_writer_contract::immutable_import_kind::
              empty_terminal);
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

namespace {
struct segment_seal_fault final {
    runtime::builtin_fault_point point;
    std::uint64_t object, occurrence;
};
seastar::future<std::vector<segment_seal_fault>> segment_seal_failure_history(
  environment& env,
  workload_budget& resources,
  simulation::testing::scheduler_driver drive,
  bool expect_failure = false,
  bool close_pending = false) {
    namespace contract = storage::testing::segment_writer_contract;
    auto& files = env.file_system();
    const auto root = take(runtime::file_path::make("/kwaque/store"));
    take(co_await drive.lifecycle(files.create_directories(root)));
    stabilize(files, take(runtime::file_path::make("/kwaque")));
    const auto spec = specification(root, {1, 1}, 68);
    const std::array specs{spec};
    ownership_input owner{specs};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await storage::testing::installation_contract::bootstrap(
      files, owner, spec, resources, work, drive);
    auto options = contract::configuration();
    options.admission.working_bytes = byte_count{1_MiB};
    auto writer = take(
      segment_writer<
        fake_file_system,
        ownership_input,
        simulation::monotonic_clock>::
        make_new(
          files, owner, spec, 0, contract::descriptor(), resources, options));
    const auto paths = local_paths::make(root).value();
    const local_segment_name name{
      contract::descriptor().segment.segment(),
      contract::descriptor().segment.generation()};
    const auto data_path = take(
      paths.segment_file(0, name, local_segment_file::data));
    const auto pointer_path = take(
      paths.segment_file(0, name, local_segment_file::published));
    const auto pages_path = take(paths.object(0, name, *options.retry_object));
    auto lookup = [&](const runtime::file_path& path) {
        return take(
                 fake_file_test_access::lookup(
                   files,
                   take(fake_file_test_access::resolve(files, path.value()))))
          .value();
    };
    auto occurrence =
      [&](std::uint64_t object, runtime::builtin_fault_point point) {
          for (const auto& inode :
               take(fake_file_test_access::snapshot(files)).objects)
              if (inode.id == object)
                  return inode.occurrences[static_cast<std::size_t>(point)];
          throw std::runtime_error("seal fault inode missing");
      };
    auto parent = [](const runtime::file_path& path) {
        return runtime::file_path::make(
                 path.value().substr(0, path.value().rfind('/')))
          .value();
    };
    std::vector<segment_seal_fault> targets;
    runtime::first_failure failed;
    std::string old_pointer, expected_data, expected_pages;
    std::optional<segment_seal_outcome> result;
    try {
        take(co_await drive.lifecycle(writer->create_new(work)));
        auto group = co_await contract::freeze_child(
          *writer, co_await contract::child(work), resources, work);
        take(co_await writer->encode_group(group, work));
        const auto cut = group.layout().boundary();
        const auto block = storage::testing::flat(group.blocks()[0].bytes());
        const auto last = group.layout().blocks()[0].records;
        const auto footer = storage::testing::footer_wire(
          {cut.covered(), cut.end().blocks, last, storage::testing::crc(block)},
          {cut.history(), cut.footer()->begin()});
        const auto extent = block + footer;
        const footer_expectation root_location{cut.history(), cut.end().bytes};
        const std::array entries{storage::testing::retry()};
        expected_pages = storage::testing::retry_page_wire(
          entries, root_location);
        const std::array refs{storage::testing::reference(expected_pages)};
        const auto coverage = storage::coverage{
          cut.covered().logical(),
          cut.covered().physical(),
          model::file_byte_span::make(cut.history().data_start, cut.end().bytes)
            .value()};
        expected_data = storage::testing::header_wire(
                          segment_header::make(
                            cut.context(),
                            cut.history().logical_origin,
                            cut.history().alignment)
                            .value())
                        + extent
                        + storage::testing::sealed_wire(
                          {coverage, cut.end().blocks, last, 0},
                          storage::testing::exact_digest(extent),
                          refs,
                          root_location);
        auto submitted = take(writer->submit(std::move(group), work));
        const auto written = co_await drive.lifecycle(
          std::move(submitted.written));
        take(written.failure.outcome());
        old_pointer = co_await read_bytes(files, pointer_path, drive);
        using p = runtime::builtin_fault_point;
        const auto data = lookup(data_path);
        const auto data_flush = occurrence(data, p::file_flush);
        const auto data_write = occurrence(data, p::file_write);
        const auto data_close = occurrence(data, p::file_close);
        const auto pages_parent = lookup(parent(pages_path));
        const auto pointer_parent = lookup(parent(pointer_path));
        const auto page_sync = occurrence(pages_parent, p::directory_sync);
        const auto pointer_sync = occurrence(pointer_parent, p::directory_sync);
        std::uint32_t calls = 0;
        std::vector<completed_retry> facts{storage::testing::retry()};
        auto held = take(resources.try_reserve(byte_count{4_KiB}));
        const auto before = resources.snapshot();
        resources.close_admission();
        auto sealing = writer->seal(
          contract::completed_source{std::move(held), std::move(facts), &calls},
          1,
          0,
          work);
        auto joined = writer->seal(
          contract::completed_source{{}, {}, &calls}, 0, 0, work);
        abort.request_abort();
        std::array<std::optional<seastar::future<runtime::result<void>>>, 8>
          closing;
        bool bounded = true;
        if (close_pending) {
            for (auto& interest : closing)
                interest.emplace(writer->close());
            const auto excess = co_await drive.lifecycle(writer->close());
            bounded = !excess && excess.error().code() == errc::queue_full;
        }
        result = co_await drive.lifecycle(std::move(sealing));
        const auto second = co_await drive.lifecycle(std::move(joined));
        for (auto& interest : closing) {
            if (!interest) continue;
            const auto closed = co_await drive.lifecycle(std::move(*interest));
            interest.reset();
            bounded = bounded && (closed.has_value() == !expect_failure);
        }
        require(bounded, "close did not bound/join the same seal result");
        require(
          result->failure.failed() == expect_failure
            && second.failure.error() == result->failure.error(),
          "seal failure cut or joined interest changed outcome");
        require(
          resources.snapshot().accepted == before.accepted
            && resources.snapshot().rejected == before.rejected,
          "seal acquired ordinary resources after admission stopped");
        if (expect_failure) {
            require(
              !result->extent && !result->boundary && !result->retry
                && writer->failure().failed() && !writer->capture(),
              "uncertain seal manufactured evidence or reopened append");
            const auto after = take(fake_file_test_access::state_digest(files));
            const auto repeated = co_await drive.lifecycle(writer->seal(
              contract::completed_source{{}, {}, &calls}, 0, 0, work));
            require(
              repeated.failure.error() == result->failure.error()
                && take(fake_file_test_access::state_digest(files)) == after,
              "failed seal retried an uncertain effect");
        } else {
            take(result->failure.outcome());
            const auto page_file = lookup(pages_path);
            const auto pointer = lookup(pointer_path);
            targets = {
              {p::file_flush, data, data_flush + 1},
              {p::file_write, data, data_write + 1},
              {p::file_flush, data, data_flush + 2},
              {p::file_close, data, data_close + 1},
              {p::directory_sync, pages_parent, page_sync + 1},
              {p::directory_sync, pointer_parent, pointer_sync + 1}};
            for (const auto object : {page_file, pointer})
                for (const auto point :
                     {p::file_write,
                      p::file_flush,
                      p::file_close,
                      p::file_rename})
                    targets.push_back({point, object, 1});
        }
    } catch (...) {
        failed.observe(std::current_exception());
    }
    try {
        static_cast<void>(co_await drive.lifecycle(writer->close()));
    } catch (...) {
        failed.observe(std::current_exception());
    }
    writer.reset();
    take(failed.outcome());
    require(
      fake_file_test_access::open_handles(files) == 0,
      "seal failure skipped an owned checked close");
    const auto visible = co_await read_bytes(files, pointer_path, drive);
    take(co_await drive.lifecycle(files.crash()));
    const auto survived = co_await read_bytes(files, pointer_path, drive);
    require(
      survived == old_pointer || survived == visible,
      "seal crash produced a mixed publication candidate");
    if (!expect_failure)
        require(
          survived == visible && result->extent,
          "successful seal lost its durable publication");
    if (survived != old_pointer) {
        // A published replacement can survive a lost notification, but its
        // dependencies must already be durable. A second crash cannot invent
        // a receipt or change those bytes.
        const auto data = co_await read_bytes(files, data_path, drive);
        const auto pages = co_await read_bytes(files, pages_path, drive);
        require(
          data == expected_data && pages == expected_pages,
          "sealed pointer survived without exact durable dependencies");
        take(co_await drive.lifecycle(files.crash()));
        require(
          (co_await read_bytes(files, data_path, drive)) == data
            && (co_await read_bytes(files, pages_path, drive)) == pages,
          "sealed dependency changed on the second crash");
    } else
        take(co_await drive.lifecycle(files.crash()));
    require(
      (co_await read_bytes(files, pointer_path, drive)) == survived,
      "second crash changed the selected seal publication");
    co_return targets;
}
} // namespace

SEASTAR_TEST_CASE(segment_writer_fake_seal_failure_cuts_and_joined_close) {
    std::vector<segment_seal_fault> targets;
    co_await with_store_environment(
      config(),
      [&](auto& env, auto& budget, auto drive) -> seastar::future<> {
          targets = co_await segment_seal_failure_history(env, budget, drive);
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
    for (const auto& target : targets) {
        for (const auto effect :
             {runtime::fault_action::file_failure_before_effect,
              runtime::fault_action::file_failure_after_effect,
              runtime::fault_action::file_failure_after_prefix}) {
            if (
              target.point == runtime::builtin_fault_point::file_close
              && effect == runtime::fault_action::file_failure_after_effect)
                continue;
            if (
              effect == runtime::fault_action::file_failure_after_prefix
              && target.point != runtime::builtin_fault_point::file_write)
                continue;
            co_await with_store_environment(
              config(
                target.point,
                effect,
                target.occurrence,
                runtime::fault_object_key::from_u64(target.object)),
              [](auto& env, auto& budget, auto drive) -> seastar::future<> {
                  static_cast<void>(co_await segment_seal_failure_history(
                    env, budget, drive, true));
              },
              byte_count{18_MiB},
              resource::workload_class::foreground_protocol,
              64);
        }
    }
    co_await with_store_environment(
      config(),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          static_cast<void>(co_await segment_seal_failure_history(
            env, budget, drive, false, true));
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_unresolved_seal) {
    co_await with_store_environment(
      config(),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 68);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::segment_writer_contract::seal_lifecycle<
            simulation::monotonic_clock>(
            files, owner, spec, budget, drive, false, false, false, true);
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_idle_reader_serialization_and_close) {
    co_await with_store_environment(
      config(),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          namespace contract = storage::testing::segment_writer_contract;
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 68);
          const std::array specs{spec};
          ownership_input owner{specs};
          seastar::abort_source abort;
          codec::cooperative_work work{codec::limits::defaults(), abort};
          co_await storage::testing::installation_contract::bootstrap(
            files, owner, spec, budget, work, drive);
          auto options = contract::configuration();
          options.admission.working_bytes = byte_count{1_MiB};
          auto writer = take(
            segment_writer<
              fake_file_system,
              ownership_input,
              simulation::monotonic_clock>::
              make_new(
                files,
                owner,
                spec,
                0,
                contract::descriptor(),
                budget,
                options));
          runtime::first_failure failed;
          try {
              take(co_await drive.lifecycle(writer->create_new(work)));
              std::uint32_t calls = 0;
              const auto seal = co_await drive.lifecycle(writer->seal(
                contract::completed_source{{}, {}, &calls}, 0, 0, work));
              take(seal.failure.outcome());
              const auto position = seal.boundary->position();
              const auto length = seal.boundary->bytes();
              {
                  const auto read = take(
                    co_await drive.lifecycle(
                      writer->read_immutable(position, length, work)));
                  require(
                    read.bytes.size() == length,
                    "first immutable read was short");
              }
              auto eviction = writer->evict_read_handle();
              const auto rejected = co_await drive.lifecycle(
                writer->read_immutable(position, length, work));
              take(co_await drive.lifecycle(std::move(eviction)));
              require(
                !rejected && rejected.error().code() == errc::queue_full,
                "read opened a replacement before idle close completed");
              auto reopening = writer->read_immutable(position, length, work);
              const auto overlap = co_await drive.lifecycle(
                writer->read_immutable(position, length, work));
              {
                  const auto read = take(
                    co_await drive.lifecycle(std::move(reopening)));
                  require(read.bytes.size() == length, "idle reopen failed");
              }
              require(
                !overlap && overlap.error().code() == errc::queue_full,
                "idle reopen admitted duplicate descriptors");
              codec::cooperative_work first_work{
                codec::limits::defaults(), abort};
              codec::cooperative_work second_work{
                codec::limits::defaults(), abort};
              auto first = writer->read_immutable(position, length, first_work);
              auto second = writer->read_immutable(
                position, length, second_work);
              const auto busy = co_await drive.lifecycle(
                writer->evict_read_handle());
              auto close = writer->close();
              const bool pending = !close.available();
              const auto a = co_await drive.lifecycle(std::move(first));
              const auto b = co_await drive.lifecycle(std::move(second));
              const auto closed = co_await drive.lifecycle(std::move(close));
              take(closed);
              require(
                a && b && pending && !busy
                  && busy.error().code() == errc::queue_full,
                "idle eviction raced reads or read interest was unnecessarily "
                "serialized");
              require(
                a->bytes.size() == length && b->bytes.size() == length
                  && !writer->capture(),
                "joined close changed returned bytes or restored append "
                "authority");
          } catch (...) {
              failed.observe(std::current_exception());
          }
          try {
              static_cast<void>(co_await drive.lifecycle(writer->close()));
          } catch (...) {
              failed.observe(std::current_exception());
          }
          writer.reset();
          take(failed.outcome());
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_age) {
    co_await with_store_environment(
      config(),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 68);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::segment_qualification_contract::
            age_boundaries(files, owner, spec, budget, drive, false);
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_age_overflow_restart) {
    co_await with_store_environment(
      config(),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 68);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::segment_qualification_contract::
            age_boundaries(files, owner, spec, budget, drive, true);
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_completion_pressure) {
    co_await with_store_environment(
      config(),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 68);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::segment_qualification_contract::
            reserved_completion<simulation::monotonic_clock>(
              files, owner, spec, budget, drive);
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

namespace {
seastar::future<segment_write_fault_target> segment_ordering_history(
  environment& env,
  workload_budget& resources,
  simulation::testing::scheduler_driver drive,
  bool delayed = false,
  bool failing = false,
  bool abort_environment = false) {
    namespace contract = storage::testing::segment_writer_contract;
    auto& files = env.file_system();
    const auto root = take(runtime::file_path::make("/kwaque/store"));
    take(co_await drive.lifecycle(files.create_directories(root)));
    stabilize(files, take(runtime::file_path::make("/kwaque")));
    const auto spec = specification(root, {1, 1}, 68);
    const std::array specs{spec};
    ownership_input owner{specs};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await storage::testing::installation_contract::bootstrap(
      files, owner, spec, resources, work, drive);
    auto options = contract::configuration();
    options.admission.working_bytes = byte_count{1_MiB};
    auto description = contract::descriptor();
    description.alignment = storage::testing::alignment(4096);
    auto writer = take(
      segment_writer<
        fake_file_system,
        ownership_input,
        simulation::monotonic_clock>::
        make_new(files, owner, spec, 0, description, resources, options));
    const auto paths = local_paths::make(root).value();
    const auto sc = contract::descriptor().segment;
    const auto path = take(paths.segment_file(
      0, {sc.segment(), sc.generation()}, local_segment_file::data));
    runtime::first_failure failed;
    segment_write_fault_target target;
    std::string expected;
    bool reentrant = false, held_prefix = true, intact_dma = true,
         later_visible = true;
    std::array<std::optional<seastar::future<segment_write_completion>>, 4>
      interests;
    std::optional<seastar::future<segment_barrier_outcome>> pending_barrier;
    try {
        take(co_await drive.lifecycle(writer->create_new(work)));
        const auto initial = writer->progress()->written;
        target.object
          = take(
              fake_file_test_access::lookup(
                files,
                take(fake_file_test_access::resolve(files, path.value()))))
              .value();
        auto inode = [&] {
            for (auto& item :
                 take(fake_file_test_access::snapshot(files)).objects)
                if (item.id == target.object) return std::move(item);
            throw std::runtime_error("missing segment ordering inode");
        };
        const auto before = inode();
        constexpr auto writes = static_cast<std::size_t>(
          runtime::builtin_fault_point::file_write);
        constexpr auto flushes = static_cast<std::size_t>(
          runtime::builtin_fault_point::file_flush);
        target.occurrence = before.occurrences[writes] + 1;
        std::vector<segment_frozen_group> groups;
        groups.reserve(4);
        for (std::uint64_t i = 0; i < 4; ++i) {
            groups.push_back(
              co_await contract::freeze_child(
                *writer,
                co_await contract::execution_child(100 + i, work),
                resources,
                work));
            take(co_await writer->encode_group(groups.back(), work));
            const auto& group = groups.back();
            const auto cut = group.layout().boundary();
            const auto block = storage::testing::flat(
              group.blocks()[0].bytes());
            expected += block;
            expected += storage::testing::footer_wire(
              {cut.covered(),
               cut.end().blocks,
               group.layout().blocks().back().records,
               storage::testing::crc(expected)},
              {cut.history(), cut.footer()->begin()});
        }
        const auto last = groups.back().layout().boundary();
        for (std::size_t i = 0; i < groups.size(); ++i) {
            auto submitted = take(writer->submit(std::move(groups[i]), work));
            interests[i].emplace(std::move(submitted.written));
        }
        groups.clear();
        pending_barrier.emplace(
          std::move(*interests.back())
            .then([&writer, &reentrant, last](segment_write_completion done) {
                reentrant = done.failure.failed()
                            || writer->progress()->written == last.end();
                return writer->barrier(last);
            }));
        interests.back().reset();
        if (delayed) {
            co_await runtime::testing::drain_reactor_tasks();
            seastar::abort_source caller;
            const auto deadline = simulation::monotonic_clock::now()
                                    .checked_add(
                                      runtime::monotonic_duration{100})
                                    .value();
            take(
              co_await drive.lifecycle(
                env.timer().sleep_until(deadline, caller)));
            const auto middle = inode();
            const bool held = !interests[0]->available()
                              && !pending_barrier->available()
                              && writer->progress()->written == initial
                              && writer->progress()->durable == initial;
            const auto checked = take(
              fake_file_test_access::verify_pending_write_buffers(files));
            // Caller timeout detaches only its result; accepted storage still
            // owns immutable DMA and must finish its complete prefix.
            interests[0].reset();
            held_prefix = held
                          && middle.occurrences[flushes]
                               == before.occurrences[flushes];
            intact_dma = checked != 0;
            later_visible = failing
                            || middle.visible_size > initial.bytes.value();
        }
        if (abort_environment) env.request_abort();
        auto waiting = std::move(*pending_barrier);
        pending_barrier.reset();
        const auto outcome = co_await drive.lifecycle(std::move(waiting));
        for (auto& interest : interests) {
            if (!interest) continue;
            auto waiting = std::move(*interest);
            interest.reset();
            const auto completed = co_await drive.lifecycle(std::move(waiting));
            require(
              completed.failure.failed() == failing,
              "failed predecessor did not settle every dependent successor");
            if (failing)
                require(
                  completed.failure.error() == outcome.failure.error(),
                  "dependent write changed the first causal error");
        }
        require(
          held_prefix && intact_dma && later_visible,
          "parked DMA lost ownership, promoted a hole or failed to exercise "
          "physical reordering");
        require(
          reentrant && outcome.failure.failed() == failing
            && outcome.receipt.has_value() == !failing,
          "reentrant observer saw progress before ordered retirement");
        if (failing)
            require(
              writer->progress()->durable == initial
                && inode().occurrences[flushes] == before.occurrences[flushes],
              "failed reordered write produced a native barrier");
        else
            require(
              writer->progress()->durable == last.end()
                && inode().occurrences[flushes]
                     == before.occurrences[flushes] + 1,
              "successful captured barrier omitted or duplicated its flush");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    if (pending_barrier) {
        try {
            static_cast<void>(
              co_await drive.lifecycle(std::move(*pending_barrier)));
        } catch (...) {
            failed.observe(std::current_exception());
        }
        pending_barrier.reset();
    }
    for (auto& interest : interests) {
        if (!interest) continue;
        try {
            static_cast<void>(co_await drive.lifecycle(std::move(*interest)));
        } catch (...) {
            failed.observe(std::current_exception());
        }
        interest.reset();
    }
    try {
        static_cast<void>(co_await drive.lifecycle(writer->close()));
    } catch (...) {
        failed.observe(std::current_exception());
    }
    writer.reset();
    take(failed.outcome());
    if (!failing) {
        const auto bytes = co_await read_bytes(files, path, drive);
        require(
          bytes.substr(description.alignment.bytes().value()) == expected,
          "reordered DMA changed immutable blocks or footer history");
    }
    co_return target;
}
} // namespace

SEASTAR_TEST_CASE(
  segment_writer_fake_reordered_dma_failed_successors_and_timeout) {
    segment_write_fault_target target;
    co_await with_store_environment(
      config({}, runtime::fault_action::error, 1, {}, {}, {}, 4096),
      [&](auto& env, auto& budget, auto drive) -> seastar::future<> {
          target = co_await segment_ordering_history(env, budget, drive);
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
    auto delay = [&] {
        return take(
          fault_rule::make(
            take(fault_rule_id::make(2)),
            runtime::builtin_fault_point::file_write,
            runtime::fault_object_key::from_u64(target.object),
            take(runtime::fault_occurrence::make(target.occurrence)),
            take(runtime::fault_occurrence::make(target.occurrence)),
            fault_selector::once(),
            runtime::fault_decision::make_delay(
              runtime::monotonic_duration{10000})));
    };
    co_await with_store_environment(
      config({}, runtime::fault_action::error, 1, {}, {}, delay(), 4096),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          static_cast<void>(
            co_await segment_ordering_history(env, budget, drive, true));
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
    for (auto effect :
         {runtime::fault_action::file_failure_before_effect,
          runtime::fault_action::file_failure_after_effect,
          runtime::fault_action::file_failure_after_prefix}) {
        co_await with_store_environment(
          config(
            runtime::builtin_fault_point::file_write,
            effect,
            target.occurrence + 1,
            runtime::fault_object_key::from_u64(target.object),
            {},
            delay(),
            4096),
          [](auto& env, auto& budget, auto drive) -> seastar::future<> {
              static_cast<void>(co_await segment_ordering_history(
                env, budget, drive, true, true));
          },
          byte_count{18_MiB},
          resource::workload_class::foreground_protocol,
          64);
    }
    co_await with_store_environment(
      config({}, runtime::fault_action::error, 1, {}, {}, delay(), 4096),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          static_cast<void>(co_await segment_ordering_history(
            env, budget, drive, true, false, true));
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_close_preflight) {
    co_await with_store_environment(
      config(),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 68);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::segment_qualification_contract::
            close_entered_preflight<simulation::monotonic_clock>(
              files, owner, spec, budget, drive, false);
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_seal_preflight) {
    co_await with_store_environment(
      config(),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 68);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::segment_qualification_contract::
            close_entered_preflight<simulation::monotonic_clock>(
              files, owner, spec, budget, drive, true);
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

namespace {
std::string segment_sealed_pointer(
  std::string active,
  local_footer_reference footer,
  local_root_reference retry) {
    // Patch the independent common publication fixture, including its counted
    // payload/padding. The footer and one root have fixed wire widths.
    using storage::testing::put;
    put(active, 88, 2, 8);
    put(active, 96, 196, 4);
    put(active, 100, active.size() - 32 - 72 - 196, 4);
    put(active, 176, 3, 1);
    put(active, 184, 1, 1);
    put(active, 188, footer.position().value(), 8);
    put(active, 196, footer.bytes().value(), 4);
    put(active, 200, footer.family(), 2);
    const auto digest = footer.digest().bytes();
    for (std::size_t i = 0; i < digest.size(); ++i)
        active[204 + i] = std::bit_cast<char>(digest[i]);
    put(active, 236, 1, 4);
    put(active, 240, static_cast<std::uint16_t>(retry.kind()), 2);
    put(active, 244, retry.sequence().value(), 8);
    put(active, 252, retry.position().value(), 8);
    put(active, 260, retry.bytes().value(), 4);
    put(active, 264, retry.pages().value(), 4);
    for (std::size_t i = 0; i < digest.size(); ++i)
        active[268 + i] = std::bit_cast<char>(digest[i]);
    storage::testing::repair(active);
    return active;
}

// A crashed process cannot issue new path operations against the recovered
// filesystem. Existing file/cursor handles are invalidated by the device crash;
// this session also fences continuations that have not opened a handle yet.
class crash_file_session final {
public:
    using directory_cursor_type = fake_file_system::directory_cursor_type;
    explicit crash_file_session(fake_file_system& files)
      : files_(files) {}
    void stop() noexcept { active_ = false; }

    auto open(runtime::file_path path, runtime::file_open_options options) {
        return invoke<runtime::file>(
          [&] { return files_.open(std::move(path), options); });
    }
    auto
    open_directory(runtime::file_path path, runtime::file_close_policy policy) {
        return invoke<directory_cursor_type>(
          [&] { return files_.open_directory(std::move(path), policy); });
    }
    auto exists(runtime::file_path path) {
        return invoke<bool>([&] { return files_.exists(std::move(path)); });
    }
    auto stat(runtime::file_path path) {
        return invoke<runtime::file_status>(
          [&] { return files_.stat(std::move(path)); });
    }
    auto space(runtime::file_path path) {
        return invoke<runtime::file_system_space>(
          [&] { return files_.space(std::move(path)); });
    }
    auto
    list(runtime::file_path path, runtime::directory_listing_limits limits) {
        return invoke<runtime::directory_listing>(
          [&] { return files_.list(std::move(path), limits); });
    }
    auto create_directories(runtime::file_path path) {
        return invoke<void>(
          [&] { return files_.create_directories(std::move(path)); });
    }
    auto remove_file(runtime::file_path path) {
        return invoke<void>(
          [&] { return files_.remove_file(std::move(path)); });
    }
    auto remove_directory(runtime::file_path path) {
        return invoke<void>(
          [&] { return files_.remove_directory(std::move(path)); });
    }
    auto rename(
      runtime::file_path source,
      runtime::file_path destination,
      runtime::file_rename_policy policy) {
        return invoke<void>([&] {
            return files_.rename(
              std::move(source), std::move(destination), policy);
        });
    }
    auto
    sync_directory(runtime::file_path path, runtime::file_close_policy policy) {
        return invoke<void>(
          [&] { return files_.sync_directory(std::move(path), policy); });
    }

private:
    template<typename T, typename Function>
    seastar::future<runtime::result<T>> invoke(Function operation) {
        if (!active_)
            return seastar::make_ready_future<runtime::result<T>>(
              runtime::failure(
                runtime::make_file_error(
                  errc::closed,
                  runtime::file_failure_detail::admission_not_dispatched)));
        return operation();
    }
    fake_file_system& files_;
    bool active_{true};
};
static_assert(runtime::file_system_backend<crash_file_session>);

seastar::future<std::size_t> segment_crash_history(
  environment& env,
  workload_budget& resources,
  simulation::testing::scheduler_driver drive,
  std::optional<std::size_t> cut) {
    namespace contract = storage::testing::segment_writer_contract;
    using namespace storage::testing;
    using writer_type = segment_writer<
      crash_file_session,
      ownership_input,
      simulation::monotonic_clock>;
    auto& files = env.file_system();
    const auto root = take(runtime::file_path::make("/kwaque/store"));
    take(co_await drive.lifecycle(files.create_directories(root)));
    stabilize(files, take(runtime::file_path::make("/kwaque")));
    const auto spec = specification(root, {1, 1}, 68);
    const std::array specs{spec};
    ownership_input owner{specs};
    seastar::abort_source caller;
    codec::cooperative_work work{codec::limits::defaults(), caller};
    co_await installation_contract::bootstrap(
      files, owner, spec, resources, work, drive);
    const auto description = contract::descriptor();
    const auto head = segment_header::make(
                        description.segment,
                        description.logical_origin,
                        description.alignment)
                        .value();
    const segment_history_context history{
      description.segment,
      description.alignment,
      runtime::file_position{8192},
      description.logical_origin,
      description.physical_origin};
    const auto block = block_wire(
      assigned_wire(),
      {segment_write_context::make(
         description.segment,
         description.alignment,
         description.physical_origin,
         history.data_start)
         .value(),
       history.data_start,
       batch_expected()});
    const auto block_span = scope(100, 101, 0, 1, 8192, 16384);
    const auto footer = footer_wire(
      {block_span, 1, block_span, crc(block)},
      {history, runtime::file_position{16384}});
    const auto extent_bytes = block + footer;
    const auto covered = scope(100, 101, 0, 1, 8192, 24576);
    const footer_expectation location{history, runtime::file_position{24576}};
    const std::array facts{retry()};
    const auto page_bytes = retry_page_wire(facts, location);
    const std::array refs{reference(page_bytes)};
    const auto sealed_bytes = sealed_wire(
      {covered, 1, block_span, 0}, exact_digest(extent_bytes), refs, location);
    const auto digest = codec::immutable_object_digest{
      exact_digest(sealed_bytes)};
    const auto root_ref = local_root_reference::make(
                            local_root_kind::sealed_retry,
                            local_object_sequence::make(45).value(),
                            location.position,
                            byte_count{sealed_bytes.size()},
                            page_count::make(1).value(),
                            digest)
                            .value();
    const auto footer_ref = local_footer_reference::make(
                              location.position, root_ref.bytes(), 7, digest)
                              .value();
    const auto prefix = header_wire(head) + extent_bytes;
    const auto full = prefix + sealed_bytes;
    const auto active_pointer = local_fixture::read("publication_empty");
    const auto sealed_pointer = segment_sealed_pointer(
      active_pointer, footer_ref, root_ref);
    const auto paths = local_paths::make(root).value();
    const local_segment_name name{
      description.segment.segment(), description.segment.generation()};
    const auto data_path = take(
      paths.segment_file(0, name, local_segment_file::data));
    const auto pointer_path = take(
      paths.segment_file(0, name, local_segment_file::published));
    const auto pages_path = take(paths.object(0, name, root_ref.sequence()));
    auto options = contract::configuration();
    options.admission.working_bytes = byte_count{1_MiB};
    crash_file_session session{files};
    auto writer = take(
      writer_type::make_new(
        session, owner, spec, 0, description, resources, options));
    bool received_data = false, received_seal = false;
    auto activity = [&]() -> seastar::future<runtime::first_failure> {
        runtime::first_failure outcome;
        try {
            auto created = co_await writer->create_new(work);
            outcome.observe(created);
            if (outcome.failed()) co_return outcome;
            auto group = co_await contract::freeze_child(
              *writer, co_await contract::child(work), resources, work);
            outcome.observe(co_await writer->encode_group(group, work));
            if (outcome.failed()) co_return outcome;
            auto submission = take(writer->submit(std::move(group), work));
            auto written = co_await std::move(submission.written);
            outcome = written.failure;
            if (outcome.failed()) co_return outcome;
            auto barrier = co_await writer->barrier(submission.boundary);
            outcome = barrier.failure;
            if (outcome.failed()) co_return outcome;
            received_data = barrier.receipt.has_value();
            std::vector<completed_retry> completed{facts.begin(), facts.end()};
            auto held = take(resources.try_reserve(byte_count{4_KiB}));
            std::uint32_t calls = 0;
            auto sealed = co_await writer->seal(
              contract::completed_source{
                std::move(held), std::move(completed), &calls},
              1,
              0,
              work);
            outcome = sealed.failure;
            received_seal = !outcome.failed() && sealed.extent
                            && sealed.boundary && sealed.retry;
        } catch (...) {
            outcome.observe(std::current_exception());
        }
        co_return outcome;
    };
    std::optional<seastar::future<runtime::first_failure>> pending{activity()};
    std::optional<seastar::future<runtime::result<void>>> closing;
    runtime::first_failure failed, completed;
    std::size_t steps = 0;
    try {
        while ((!cut || steps < *cut) && steps < 512) {
            const auto boundary = co_await history_step(
              env.event_scheduler(), *pending);
            if (boundary != history_boundary::stepped) break;
            ++steps;
        }
        require(steps < 512, "segment history exceeded its event bound");
        if (!cut) {
            require(
              pending->available(),
              "segment lifecycle parked without an event");
            auto waiting = std::move(*pending);
            pending.reset();
            completed = co_await std::move(waiting);
            take(completed.outcome());
            require(
              received_data && received_seal,
              "uninterrupted segment lifecycle omitted a successful receipt");
        }
        // Close joins an entered seal; it does not cancel its namespace work.
        // Fence the old process before crashing, then join all its children.
        session.stop();
        caller.request_abort();
        closing.emplace(writer->close());
        take(co_await drive.lifecycle(files.crash()));
        if (pending) {
            auto waiting = std::move(*pending);
            pending.reset();
            completed = co_await drive.lifecycle(std::move(waiting));
        }
        auto waiting = std::move(*closing);
        closing.reset();
        static_cast<void>(co_await drive.lifecycle(std::move(waiting)));
    } catch (...) {
        failed.observe(std::current_exception());
    }
    session.stop();
    if (pending) {
        caller.request_abort();
        static_cast<void>(co_await drive.lifecycle(files.crash()));
        static_cast<void>(co_await drive.lifecycle(std::move(*pending)));
        pending.reset();
    }
    if (closing) {
        static_cast<void>(co_await drive.lifecycle(std::move(*closing)));
        closing.reset();
    }
    static_cast<void>(co_await drive.lifecycle(writer->close()));
    writer.reset();
    take(failed.outcome());
    auto image = [&](const runtime::file_path& path)
      -> seastar::future<std::optional<std::string>> {
        auto status = co_await drive.lifecycle(files.stat(path));
        if (!status) {
            require(
              status.error().code() == errc::not_found,
              "crash probe failed outside the survival model");
            co_return std::nullopt;
        }
        co_return co_await read_bytes(files, path, drive);
    };
    const auto pointer = co_await image(pointer_path);
    const auto data = co_await image(data_path);
    const auto pages = co_await image(pages_path);
    require(
      !pointer || *pointer == active_pointer || *pointer == sealed_pointer,
      "crash produced an unrecognized or mixed publication");
    if (received_data)
        require(
          pointer && data && data->starts_with(prefix),
          "returned segment receipt did not survive crash");
    if (received_seal)
        require(
          pointer && *pointer == sealed_pointer,
          "returned seal lost its durable pointer");
    const bool sealed = pointer && *pointer == sealed_pointer;
    if (sealed)
        require(
          data && *data == full && pages && *pages == page_bytes,
          "sealed pointer outlived exact data/root/retry dependencies");
    seastar::abort_source recovered_abort;
    codec::cooperative_work recovered_work{
      codec::limits::defaults(), recovered_abort};
    const std::array<local_bundle_context, 1> contexts{location};
    const local_object_publication publication
      = sealed
          ? local_object_publication{description.segment, local_object_state::sealed, footer_ref, {root_ref}}
          : local_object_publication{
              description.segment, local_object_state::active, {}, {}};
    const local_generation_expectation expected{
      local_publication_generation::make(sealed ? 2 : 1).value(),
      publication,
      description,
      sealed ? std::span<const local_bundle_context>{contexts}
             : std::span<const local_bundle_context>{}};
    std::optional<segment_immutable_expectation> immutable;
    if (sealed)
        immutable.emplace(
          segment_immutable_expectation{
            covered,
            codec::extent_digest{exact_digest(extent_bytes)},
            runtime::file_position{full.size()}});
    crash_file_session recovered_session{files};
    auto opened = co_await drive.lifecycle(
      writer_type::open_existing(
        recovered_session,
        owner,
        spec,
        0,
        expected,
        resources,
        contract::read_configuration(),
        recovered_work,
        immutable));
    if (pointer)
        require(
          opened.has_value(),
          "durably published generation could not be independently reopened");
    else
        require(
          !opened,
          "partial creation bootstrapped an empty existing generation");
    if (opened) {
        runtime::first_failure checked;
        try {
            require(
              !(*opened)->capture() && !(*opened)->first_acceptance(),
              "restart restored append or persisted monotonic age");
            if (!sealed)
                require(
                  take((*opened)->roll_required()),
                  "recovered active generation did not require rolling");
        } catch (...) {
            checked.observe(std::current_exception());
        }
        checked.observe(co_await drive.lifecycle((*opened)->close()));
        opened->reset();
        take(checked.outcome());
    }
    take(co_await drive.lifecycle(files.crash()));
    require(
      co_await image(pointer_path) == pointer
        && co_await image(data_path) == data
        && co_await image(pages_path) == pages,
      "read-only reopen changed the next crash image");
    co_return steps;
}
} // namespace

SEASTAR_TEST_CASE(
  segment_writer_fake_creation_append_seal_every_crash_boundary) {
    for (unsigned mask = 0; mask < 8; ++mask) {
        const fake_crash_policy survival{
          .data_percent = static_cast<std::uint8_t>((mask & 1U) ? 100 : 0),
          .namespace_percent = static_cast<std::uint8_t>((mask & 2U) ? 100 : 0),
          .eof_percent = static_cast<std::uint8_t>((mask & 4U) ? 100 : 0)};
        std::size_t count = 0;
        co_await with_store_environment(
          config({}, runtime::fault_action::error, 1, {}, survival),
          [&](auto& env, auto& budget, auto drive) -> seastar::future<> {
              count = co_await segment_crash_history(env, budget, drive, {});
          },
          byte_count{18_MiB},
          resource::workload_class::foreground_protocol,
          64);
        require(count != 0, "segment crash history had no storage boundaries");
        for (std::size_t cut = 0; cut <= count; ++cut) {
            BOOST_TEST_CONTEXT("survival=" << mask << " cut=" << cut) {
                co_await with_store_environment(
                  config({}, runtime::fault_action::error, 1, {}, survival),
                  [cut](
                    auto& env, auto& budget, auto drive) -> seastar::future<> {
                      static_cast<void>(co_await segment_crash_history(
                        env, budget, drive, cut));
                  },
                  byte_count{18_MiB},
                  resource::workload_class::foreground_protocol,
                  64);
            }
        }
    }
}

SEASTAR_TEST_CASE(
  segment_writer_fake_preallocated_paged_seal_and_retained_results) {
    co_await with_store_environment(
      config(),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 68);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::segment_qualification_contract::
            paged_seal_and_retained_results<simulation::monotonic_clock>(
              files, owner, spec, budget, drive, true);
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_paged_seal_and_retained_results) {
    co_await with_store_environment(
      config(),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 68);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::segment_qualification_contract::
            paged_seal_and_retained_results<simulation::monotonic_clock>(
              files, owner, spec, budget, drive);
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(
  segment_writer_fake_native_handle_pressure_blocks_activation) {
    co_await with_store_environment(
      config(),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          namespace contract = storage::testing::segment_writer_contract;
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 68);
          const std::array specs{spec};
          ownership_input owner{specs};
          seastar::abort_source abort;
          codec::cooperative_work work{codec::limits::defaults(), abort};
          co_await storage::testing::installation_contract::bootstrap(
            files, owner, spec, budget, work, drive);
          const auto control = take(local_paths::make(root)->control(0));
          std::vector<runtime::file> pressure;
          pressure.reserve(15);
          std::unique_ptr<segment_writer<
            fake_file_system,
            ownership_input,
            simulation::monotonic_clock>>
            writer;
          runtime::first_failure failed;
          try {
              for (unsigned i = 0; i < 15; ++i)
                  pressure.push_back(take(
                    co_await drive.lifecycle(files.open(
                      control,
                      {.close_policy = runtime::file_close_policy::checked}))));
              require(
                fake_file_test_access::open_handles(files) == 15,
                "native descriptor pressure was not exercised");
              writer = take(
                segment_writer<
                  fake_file_system,
                  ownership_input,
                  simulation::monotonic_clock>::
                  make_new(
                    files,
                    owner,
                    spec,
                    0,
                    contract::descriptor(),
                    budget,
                    contract::configuration()));
              const auto created = co_await drive.lifecycle(
                writer->create_new(work));
              require(
                !created && writer->failure().failed() && !writer->capture()
                  && !writer->publication_generation()
                  && !writer->first_acceptance(),
                "insufficient native handles activated an incompletable "
                "segment");
          } catch (...) {
              failed.observe(std::current_exception());
          }
          if (writer) {
              try {
                  static_cast<void>(co_await drive.lifecycle(writer->close()));
              } catch (...) {
                  failed.observe(std::current_exception());
              }
              writer.reset();
          }
          for (auto& file : pressure) {
              try {
                  failed.observe(co_await drive.lifecycle(file.close()));
              } catch (...) {
                  failed.observe(std::current_exception());
              }
          }
          pressure.clear();
          take(failed.outcome());
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(local_scan_reader_slots_fake) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 51);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::scan_contract::reader_slots(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(local_wal_scan_head_tails_fake) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 51);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::scan_contract::head_tails(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(local_wal_scan_rotated_chain_fake) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 51);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::scan_contract::rotated_chain(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(local_wal_scan_target_resolution_fake) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 51);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::scan_contract::target_resolution(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(local_segment_scan_resume_and_tails_fake) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 51);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::segment_scan_contract::resume_and_tails(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(local_recovery_publication_pins_fake) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 68);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::segment_scan_contract::publication_pins(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(local_recovery_classification_fake) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 51);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::segment_scan_contract::
            recovery_classification(files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(local_recovery_merge_pass_fake) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 51);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::recovery_contract::merge_pass(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(local_recovery_merge_cases_fake) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 51);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::recovery_contract::merge_cases(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(local_recovery_plan_cases_fake) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 51);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::recovery_contract::plan_cases(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(local_recovery_decisions_fake) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 51);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::recovery_contract::decisions(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(local_recovered_seal_fake) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 51);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::recovery_contract::recovered_seal<
            simulation::monotonic_clock>(files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(local_recovered_seal_crash_fake) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 51);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::recovery_contract::crash_after_seal<
            simulation::monotonic_clock>(
            files,
            owner,
            spec,
            budget,
            drive,
            [&files](
              const runtime::file_path& path) -> std::optional<std::uint64_t> {
                const auto object = take(
                  fake_file_test_access::lookup(
                    files,
                    take(fake_file_test_access::resolve(files, path.value()))));
                return take(
                  fake_file_test_access::occurrences(
                    files, object, runtime::builtin_fault_point::file_flush));
            },
            [&files] { fake_file_test_access::crash(files); });
      });
}

SEASTAR_TEST_CASE(local_recovery_reconstruction_fake) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 51);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::recovery_contract::reconstruction<
            simulation::monotonic_clock>(files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(local_recovery_interruptions_fake) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 51);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::recovery_contract::interruptions<
            simulation::monotonic_clock>(files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(local_recovery_device_gate_fake) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const std::array specs{
            specification(
              take(runtime::file_path::make("/kwaque/a")), {1, 1}, 0x44),
            specification(
              take(runtime::file_path::make("/kwaque/b")),
              {1, 2},
              0x55,
              local_device_role::data)};
          ownership_input owner{specs};
          for (const auto& spec : specs)
              take(
                co_await drive.lifecycle(files.create_directories(spec.root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          co_await storage::testing::recovery_contract::device_gate(
            files,
            owner,
            std::span<const local_device_spec>{specs},
            budget,
            drive);
      });
}

SEASTAR_TEST_CASE(local_recovering_publication_fake) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 51);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::recovery_contract::recovering_publication<
            simulation::monotonic_clock>(files, owner, spec, budget, drive);
      });
}

namespace {
namespace recovery = kwaque::storage::testing::recovery_contract;

// Writes through the backend without the flush or directory sync a
// publication performs; `flushed` adds only the file's flush.
seastar::future<> write_unsynced(
  fake_file_system& files,
  const runtime::file_path& path,
  std::uint64_t at,
  std::string_view bytes,
  bool flushed,
  simulation::testing::scheduler_driver drive) {
    auto file = take(
      co_await drive.lifecycle(files.open(
        path,
        {.access = runtime::file_access::read_write,
         .create = true,
         .close_policy = runtime::file_close_policy::checked})));
    runtime::first_failure failed;
    try {
        auto written = co_await drive.lifecycle(file.write(
          runtime::file_position{at},
          kwaque::bytes::fragmented_buffer::copy_of(
            std::span<const char>{bytes})
            .value()));
        failed.observe(written);
        if (written)
            require(written->value() == bytes.size(), "fixture short write");
        if (flushed && !failed.failed())
            failed.observe(co_await drive.lifecycle(file.flush()));
    } catch (...) {
        failed.observe(std::current_exception());
    }
    try {
        failed.observe(co_await drive.lifecycle(file.close()));
    } catch (...) {
        failed.observe(std::current_exception());
    }
    take(failed.outcome());
}

// Segment A as restart finds it: its header and a footer-covered prefix,
// active with no pin, under the interleaved WAL. With `flushed` false the
// prefix after the header reached the file but no flush covered it.
seastar::future<> recovered_a(
  fake_file_system& files,
  ownership_input& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  simulation::testing::scheduler_driver drive,
  bool flushed) {
    co_await recovery::seed_targets(files, owner, spec, budget, drive);
    co_await recovery::put_wal(
      files, spec, recovery::wal(1), recovery::interleaved(), drive);
    const auto data = recovery::data_path(spec, recovery::segment());
    if (flushed) {
        co_await write_bytes(
          files,
          data,
          storage::testing::segment_scan_contract::active_a(),
          drive);
    } else {
        const auto prefix
          = storage::testing::segment_scan_contract::active_a().substr(4096);
        co_await write_unsynced(files, data, 4096, prefix, false, drive);
    }
    co_await recovery::put_publication(
      files, spec, local_object_state::active, std::nullopt, 1, drive);
}
// The inventory and finished plan of segment A, then its recovering
// publication.
seastar::future<runtime::result<recovered_publications>> publish_a(
  fake_file_system& files,
  ownership_input& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  simulation::testing::scheduler_driver drive,
  std::uint64_t retry) {
    const std::array catalog{
      recovery_catalog_entry{recovery::descriptor(), recovery::segment_head()}};
    auto found = take(
      co_await recovery::inventory(
        files,
        owner,
        spec,
        budget,
        catalog,
        std::span<const recovery_decision_record>{},
        drive));
    auto planned = co_await recovery::plan(
      files, owner, spec, budget, found.targets, drive);
    require(
      planned.ready()
        && planned.segments()[0].action
             == recovery_plan_action::publish_recovering,
      "segment A was not planned for a recovering publication");
    co_return co_await recovery::publish<simulation::monotonic_clock>(
      files,
      owner,
      spec,
      budget,
      planned,
      found,
      std::span<const recovery_visibility>{},
      retry,
      drive);
}
} // namespace

// Bytes that reached a file survive a crash only once a flush covered them,
// so restart flushes before it publishes: after the recovering publication
// the recovered prefix and the publication both survive; without it the
// prefix is gone and the segment is as it was.
SEASTAR_TEST_CASE(local_recovery_fresh_flush_fake) {
    for (const bool published : {false, true}) {
        co_await with_store_environment(
          config(),
          [published](
            auto& env, auto& budget, auto drive) -> seastar::future<> {
              auto& files = env.file_system();
              const auto root = take(runtime::file_path::make("/kwaque/store"));
              take(co_await drive.lifecycle(files.create_directories(root)));
              stabilize(files, take(runtime::file_path::make("/kwaque")));
              const auto spec = specification(root, {1, 1}, 51);
              const std::array specs{spec};
              ownership_input owner{specs};
              co_await recovered_a(files, owner, spec, budget, drive, false);
              const auto paths = take(local_paths::make(spec.root));
              const auto data = recovery::data_path(spec, recovery::segment());
              const auto pointer = take(paths.segment_file(
                0,
                {recovery::segment().segment(),
                 recovery::segment().generation()},
                local_segment_file::published));
              const auto initial = co_await read_bytes(files, pointer, drive);
              std::string recovering = initial;
              if (published) {
                  auto outcome = take(
                    co_await publish_a(files, owner, spec, budget, drive, 80));
                  require(
                    outcome.complete && outcome.segments[0].published,
                    "the recovering publication failed");
                  recovering = co_await read_bytes(files, pointer, drive);
              }
              fake_file_test_access::crash(files);
              require(
                (co_await read_bytes(files, data, drive))
                    == (published ? storage::testing::segment_scan_contract::active_a() : storage::testing::local_fixture::read("data_header_a"))
                  && (co_await read_bytes(files, pointer, drive)) == recovering
                  && (recovering != initial) == published,
                "a recovered prefix outlived a crash without its fresh flush");
          });
    }
}

// A decision record renamed into place by a publication whose directory
// sync never ran is visible after a process restart, and a power loss could
// still remove it. Discovery syncs the directory before returning it, so it
// survives a crash only when discovery ran.
SEASTAR_TEST_CASE(local_recovery_decision_barrier_fake) {
    for (const bool discovered : {false, true}) {
        co_await with_store_environment(
          config(),
          [discovered](
            auto& env, auto& budget, auto drive) -> seastar::future<> {
              auto& files = env.file_system();
              const auto root = take(runtime::file_path::make("/kwaque/store"));
              take(co_await drive.lifecycle(files.create_directories(root)));
              stabilize(files, take(runtime::file_path::make("/kwaque")));
              const auto spec = specification(root, {1, 1}, 51);
              const std::array specs{spec};
              ownership_input owner{specs};
              co_await recovery::seed_targets(
                files, owner, spec, budget, drive);
              const auto pin = co_await recovery::put_decision(
                files,
                spec,
                65,
                local_recovery_action::seal_at,
                12288,
                recovery::suffix_identity(
                  recovery::segment(), 12288, recovery::planned_a(2)),
                16384,
                drive);
              const auto path = take(
                take(local_paths::make(spec.root))
                  .sequence_file(0, local_sequence_file::decision, 65));
              const auto record = co_await read_bytes(files, path, drive);
              take(co_await drive.lifecycle(files.remove_file(path)));
              stabilize(files, recovery::parent_of(path));
              // Written and flushed; its directory entry never synced.
              co_await write_unsynced(files, path, 0, record, true, drive);
              if (discovered) {
                  auto fields = recovery::head_control();
                  fields.decision_high = local_decision_high{128};
                  const std::array descriptors{recovery::descriptor()};
                  std::vector<recovery_decision_record> found;
                  auto result = take(
                    co_await recovery::discover(
                      files,
                      owner,
                      spec,
                      budget,
                      fields,
                      descriptors,
                      found,
                      drive));
                  require(
                    result.decisions == 1 && found.size() == 1
                      && found[0].pin == pin,
                    "the decision was not discovered");
              }
              fake_file_test_access::crash(files);
              require(
                take(co_await drive.lifecycle(files.exists(path)))
                  == discovered,
                "a discovered decision was not made durable");
          });
    }
}

// A failed fresh flush publishes nothing: the shard stays out of ready and
// the segment as it was, and the next restart classifies and publishes it.
// The first run finds the data file and how often it was flushed; the second
// fails its next flush.
SEASTAR_TEST_CASE(local_recovery_failed_fresh_flush_fake) {
    std::uint64_t object = 0, flushes = 0;
    co_await with_store_environment(
      config(),
      [&object,
       &flushes](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 51);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await recovered_a(files, owner, spec, budget, drive, true);
          const auto data = recovery::data_path(spec, recovery::segment());
          const auto found = take(
            fake_file_test_access::lookup(
              files,
              take(fake_file_test_access::resolve(files, data.value()))));
          object = found.value();
          flushes = take(
            fake_file_test_access::occurrences(
              files, found, runtime::builtin_fault_point::file_flush));
      });
    co_await with_store_environment(
      config(
        runtime::builtin_fault_point::file_flush,
        runtime::fault_action::file_failure_before_effect,
        flushes + 1,
        runtime::fault_object_key::from_u64(object)),
      [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 51);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await recovered_a(files, owner, spec, budget, drive, true);
          const auto pointer = take(take(local_paths::make(spec.root))
                                      .segment_file(
                                        0,
                                        {recovery::segment().segment(),
                                         recovery::segment().generation()},
                                        local_segment_file::published));
          const auto initial = co_await read_bytes(files, pointer, drive);
          auto failed = take(
            co_await publish_a(files, owner, spec, budget, drive, 81));
          require(
            !failed.complete && !failed.segments[0].published
              && failed.segments[0].failure
              && (co_await read_bytes(files, pointer, drive)) == initial,
            "a failed fresh flush did not keep the segment as it was");
          auto retried = take(
            co_await publish_a(files, owner, spec, budget, drive, 82));
          require(
            retried.complete && retried.segments[0].published
              && (co_await read_bytes(files, pointer, drive)) != initial,
            "the next restart did not publish the recovered segment");
      });
}

// A read that fails is unavailability, never an end. The first run counts
// the reads of the WAL head, then of segment A's data, through one merge; the
// second fails the last of them, which reaches the file's end.
SEASTAR_TEST_CASE(local_recovery_failed_read_fake) {
    for (const bool head : {true, false}) {
        std::uint64_t object = 0, reads = 0;
        const auto path = [head](const local_device_spec& spec) {
            return head ? take(take(local_paths::make(spec.root))
                                 .wal(0, recovery::wal(1)))
                        : recovery::data_path(spec, recovery::segment());
        };
        co_await with_store_environment(
          config(),
          [&object, &reads, &path](
            auto& env, auto& budget, auto drive) -> seastar::future<> {
              auto& files = env.file_system();
              const auto root = take(runtime::file_path::make("/kwaque/store"));
              take(co_await drive.lifecycle(files.create_directories(root)));
              stabilize(files, take(runtime::file_path::make("/kwaque")));
              const auto spec = specification(root, {1, 1}, 51);
              const std::array specs{spec};
              ownership_input owner{specs};
              co_await recovery::seed_interleaved(
                files, owner, spec, budget, drive);
              const auto found = take(
                fake_file_test_access::lookup(
                  files,
                  take(
                    fake_file_test_access::resolve(
                      files, path(spec).value()))));
              const std::array targets{
                recovery::target_a(), recovery::target_b()};
              std::vector<std::string> out;
              take(
                co_await recovery::merge(
                  files, owner, spec, budget, targets, out, drive));
              object = found.value();
              reads = take(
                fake_file_test_access::occurrences(
                  files, found, runtime::builtin_fault_point::file_read));
          });
        co_await with_store_environment(
          config(
            runtime::builtin_fault_point::file_read,
            runtime::fault_action::file_failure_before_effect,
            reads,
            runtime::fault_object_key::from_u64(object)),
          [head](auto& env, auto& budget, auto drive) -> seastar::future<> {
              auto& files = env.file_system();
              const auto root = take(runtime::file_path::make("/kwaque/store"));
              take(co_await drive.lifecycle(files.create_directories(root)));
              stabilize(files, take(runtime::file_path::make("/kwaque")));
              const auto spec = specification(root, {1, 1}, 51);
              const std::array specs{spec};
              ownership_input owner{specs};
              co_await recovery::seed_interleaved(
                files, owner, spec, budget, drive);
              co_await recovery::failed_read(
                files,
                owner,
                spec,
                budget,
                head ? "region wal1" : "region A",
                drive);
          });
    }
}

SEASTAR_TEST_CASE(local_recovery_source_cases_fake) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 51);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::recovery_contract::source_cases(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(local_recovery_torn_seals_fake) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 51);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::recovery_contract::torn_seals<
            simulation::monotonic_clock>(files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(local_recovery_torn_tails_fake) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 51);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::recovery_contract::torn_tails(
            files, owner, spec, budget, drive);
      });
}

SEASTAR_TEST_CASE(local_recovery_bounded_scanning_fake) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 51);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await storage::testing::recovery_contract::bounded_scanning(
            files, owner, spec, budget, drive);
      });
}
