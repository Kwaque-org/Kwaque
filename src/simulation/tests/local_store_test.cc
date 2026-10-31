#include "src/base/units.h"
#include "src/simulation/environment.h"
#include "src/simulation/fake_file_test_support.h"
#include "src/simulation/scheduler_driver.h"
#include "src/simulation/tests/local_store_environment.h"
#include "src/storage/tests/local_installation_contract.h"
#include "src/storage/tests/local_qualification_contract.h"
#include "src/storage/tests/local_reader_contract.h"
#include "src/storage/tests/local_store_contract.h"

#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <utility>

namespace {
using namespace kwaque;
using namespace kwaque::simulation;
using namespace kwaque::simulation::testing::local_store_environment;
using namespace kwaque::storage;
using namespace kwaque::storage::testing::store_contract;
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

// A store that was used gains a data device only by the intent that says
// so and names it. Opened as the store that must exist, or as a first
// store, the device that holds nothing refuses the start and nothing is
// written to it. The intent to add never makes a first store, never takes a
// device that holds something else, and never takes one it does not name:
// a device the store had and lost holds nothing either.
SEASTAR_TEST_CASE(local_store_adds_a_device_only_by_explicit_intent) {
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
          const std::array first{a};
          const std::array grown{a, b};
          ownership_input owns_first{first};
          ownership_input owns_grown{grown};
          for (const auto& spec : grown)
              take(
                co_await drive.lifecycle(files.create_directories(spec.root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          seastar::abort_source abort;
          codec::cooperative_work work{codec::limits::defaults(), abort};
          const auto b_identity
            = take(local_paths::make(b.root)).store().value();
          const std::array named{b.owner.device()};
          const auto open = [&](
                              auto& owner,
                              std::span<const local_device_spec> specs,
                              local_store_intent intent,
                              std::span<const device_store_id> added = {}) {
              return drive.lifecycle(initialize_local_stores(
                files,
                owner,
                specs,
                {intent, false, added},
                budget,
                limits(),
                work));
          };
          {
              // Nothing to extend: adding is no way to a first store.
              const auto none = co_await open(
                owns_grown, grown, local_store_intent::add_devices, named);
              BOOST_REQUIRE(none.failure.error().has_value());
              BOOST_CHECK(none.failure.error()->code() == errc::not_found);
              BOOST_CHECK(!none.mutation_attempted);
          }
          take((co_await open(
                  owns_first, first, local_store_intent::create_or_resume))
                 .failure.outcome());
          {
              // The store is used: its control record is no longer the
              // one a bootstrap wrote.
              using control_type
                = local_control_owner<fake_file_system, ownership_input>;
              using allocator_type
                = local_id_allocator<fake_file_system, ownership_input>;
              auto control = take(
                co_await drive.lifecycle(
                  control_type::open(
                    files, owns_first, a, 0, false, budget, limits(), work)));
              auto allocator = take(allocator_type::make(*control, budget, 4));
              runtime::first_failure failed;
              try {
                  static_cast<void>(take(
                    co_await drive.lifecycle(
                      allocator->allocate_object(work))));
              } catch (...) {
                  failed.observe(std::current_exception());
              }
              take(co_await drive.lifecycle(allocator->close()));
              allocator.reset();
              take(co_await drive.lifecycle(control->close()));
              control.reset();
              take(failed.outcome());
          }
          for (const auto intent :
               {local_store_intent::must_exist,
                local_store_intent::create_or_resume}) {
              const auto refused = co_await open(owns_grown, grown, intent);
              BOOST_REQUIRE(refused.failure.error().has_value());
              BOOST_CHECK(refused.failure.error()->code() == errc::not_found);
              BOOST_CHECK(!refused.mutation_attempted);
              BOOST_CHECK(
                refused.reports[0].state == local_store_state::existing);
              BOOST_CHECK(
                refused.reports[1].state == local_store_state::pristine);
              BOOST_CHECK(
                !take(co_await drive.lifecycle(files.exists(b_identity))));
          }
          {
              // A device that holds nothing and is not named as added is
              // one the store had and lost, not a new one; and a name that
              // is no configured data device adds nothing.
              const auto lost = co_await open(
                owns_grown, grown, local_store_intent::add_devices);
              BOOST_REQUIRE(lost.failure.error().has_value());
              BOOST_CHECK(lost.failure.error()->code() == errc::wrong_context);
              BOOST_CHECK(!lost.mutation_attempted);
              const std::array control{a.owner.device()};
              const auto misnamed = co_await open(
                owns_grown, grown, local_store_intent::add_devices, control);
              BOOST_REQUIRE(misnamed.failure.error().has_value());
              BOOST_CHECK(
                misnamed.failure.error()->code() == errc::invalid_argument);
              BOOST_CHECK(!misnamed.mutation_attempted);
              BOOST_CHECK(
                !take(co_await drive.lifecycle(files.exists(b_identity))));
          }
          {
              // A device that holds something else is not taken.
              const auto unknown = take(local_child_path(
                b.root, take(runtime::file_name::make("unexpected"))));
              seed_bytes(files, unknown, "keep");
              const auto polluted = co_await open(
                owns_grown, grown, local_store_intent::add_devices, named);
              BOOST_CHECK(polluted.failure.failed());
              BOOST_CHECK(!polluted.mutation_attempted);
              BOOST_CHECK(
                !take(co_await drive.lifecycle(files.exists(b_identity))));
              take(co_await drive.lifecycle(files.remove_file(unknown)));
          }
          {
              const auto added = co_await open(
                owns_grown, grown, local_store_intent::add_devices, named);
              take(added.failure.outcome());
              BOOST_CHECK(added.mutation_attempted);
              BOOST_CHECK(added.stages[0] == local_bootstrap_stage::complete);
              BOOST_CHECK(added.stages[1] == local_bootstrap_stage::complete);
              BOOST_CHECK(
                take(co_await drive.lifecycle(files.exists(b_identity))));
          }
          {
              // The store is now the one with both devices, and the intent
              // to add, left set with or without its name, changes nothing.
              const auto whole = co_await open(
                owns_grown, grown, local_store_intent::must_exist);
              take(whole.failure.outcome());
              BOOST_CHECK(!whole.mutation_attempted);
              const auto again = co_await open(
                owns_grown, grown, local_store_intent::add_devices);
              take(again.failure.outcome());
              BOOST_CHECK(!again.mutation_attempted);
          }
      });
}

// A segment's directory under its deleting name is a removal that a crash or
// a stop left unfinished. A start knows it by that name and does not look
// inside: the same directory under the segment's own name, with one of its
// three files, would be a store with a gap.
SEASTAR_TEST_CASE(local_store_knows_a_segment_being_removed_by_its_name) {
    co_await with_store_environment(
      config(), [](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto a = specification(
            take(runtime::file_path::make("/kwaque/a")), {1, 1});
          const std::array devices{a};
          ownership_input owns{devices};
          take(co_await drive.lifecycle(files.create_directories(a.root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          seastar::abort_source abort;
          codec::cooperative_work work{codec::limits::defaults(), abort};
          const auto open = [&](local_store_intent intent) {
              return drive.lifecycle(initialize_local_stores(
                files, owns, devices, {intent, false}, budget, limits(), work));
          };
          take((co_await open(local_store_intent::create_or_resume))
                 .failure.outcome());

          std::array<std::uint8_t, 16> id{};
          id.fill(0x21);
          const local_segment_name name{
            model::segment_id::make(id).value(),
            model::segment_generation::make(7).value()};
          const auto paths = take(local_paths::make(a.root));
          const auto deleting = take(paths.deleting(0, name));
          take(co_await drive.lifecycle(files.create_directories(deleting)));
          seed_bytes(
            files,
            take(local_child_path(
              deleting, take(runtime::file_name::make("data")))),
            "what a removal left");

          const auto found = co_await open(local_store_intent::must_exist);
          take(found.failure.outcome());
          BOOST_CHECK(found.reports[0].state == local_store_state::existing);
          BOOST_CHECK(found.reports[0].has_payload);
          BOOST_CHECK_EQUAL(found.reports[0].deleting_segments, 1U);
          BOOST_CHECK_EQUAL(found.reports[0].interrupted_creations, 0U);
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
      config(), on_one_device(51, [](auto&&... shared) {
          return storage::testing::installation_contract::allocation(shared...);
      }));
}

SEASTAR_TEST_CASE(local_installation_fake_descriptors) {
    co_await with_store_environment(
      config(), on_one_device(68, [](auto&&... shared) {
          return storage::testing::installation_contract::descriptors(
            shared...);
      }));
}

SEASTAR_TEST_CASE(local_installation_fake_checkpoint_bundle) {
    co_await with_store_environment(
      config(), on_one_device(51, [](auto&&... shared) {
          return storage::testing::installation_contract::checkpoint_bundle(
            shared...);
      }));
}

SEASTAR_TEST_CASE(local_installation_fake_segment_bundles) {
    co_await with_store_environment(
      config(), on_one_device(68, [](auto&&... shared) {
          return storage::testing::installation_contract::segment_bundles(
            shared...);
      }));
}

SEASTAR_TEST_CASE(local_installation_fake_maximum_bundle) {
    co_await with_store_environment(
      config(), on_one_device(68, [](auto&&... shared) {
          return storage::testing::installation_contract::maximum_bundle(
            shared...);
      }));
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
      config(), on_one_device(51, [](auto&&... shared) {
          return storage::testing::reader_contract::root_lifetime(shared...);
      }));
}

SEASTAR_TEST_CASE(local_readers_fake_root_errors) {
    co_await with_store_environment(
      config(), on_one_device(51, [](auto&&... shared) {
          return storage::testing::reader_contract::root_errors(shared...);
      }));
}

SEASTAR_TEST_CASE(local_readers_fake_generation_lifetime) {
    co_await with_store_environment(
      config(), on_one_device(68, [](auto&&... shared) {
          return storage::testing::reader_contract::generation_lifetime(
            shared...);
      }));
}

SEASTAR_TEST_CASE(local_readers_fake_generation_errors) {
    co_await with_store_environment(
      config(), on_one_device(68, [](auto&&... shared) {
          return storage::testing::reader_contract::generation_errors(
            shared...);
      }));
}

SEASTAR_TEST_CASE(local_readers_fake_wal_chain) {
    co_await with_store_environment(
      config(), on_one_device(51, [](auto&&... shared) {
          return storage::testing::reader_contract::wal_chain(shared...);
      }));
}

SEASTAR_TEST_CASE(local_readers_fake_wal_alignment) {
    co_await with_store_environment(
      config(), on_one_device(51, [](auto&&... shared) {
          return storage::testing::reader_contract::wal_alignment(shared...);
      }));
}

SEASTAR_TEST_CASE(local_readers_fake_discovery_cleanup) {
    co_await with_store_environment(
      config(), on_one_device(68, [](auto&&... shared) {
          return storage::testing::reader_contract::discovery_cleanup(
            shared...);
      }));
}

SEASTAR_TEST_CASE(local_readers_fake_empty_retry) {
    co_await with_store_environment(
      config(), on_one_device(68, [](auto&&... shared) {
          return storage::testing::reader_contract::empty_retry(shared...);
      }));
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
