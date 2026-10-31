#include "src/simulation/fake_file_test_support.h"
#include "src/simulation/scheduler_driver.h"
#include "src/simulation/tests/local_store_environment.h"
#include "src/simulation/virtual_time.h"
#include "src/storage/tests/local_store_contract.h"
#include "src/storage/tests/recovery_contract.h"
#include "src/storage/tests/segment_scan_contract.h"
#include "src/storage/tests/wal_scan_contract.h"

#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <utility>

namespace {
using namespace kwaque;
using namespace kwaque::simulation;
using namespace kwaque::simulation::testing::local_store_environment;
using namespace kwaque::storage;
using namespace kwaque::storage::testing::store_contract;
// The bytes the fake reads from each object while attached, as asked, and
// how many writes it is asked for in all; a contract asks them by path and
// in total.
class counted_io final {
public:
    explicit counted_io(fake_file_system& files)
      : files_(files) {
        fake_file_test_access::observe_reads(
          files_,
          [this](
            fake_object_id object, std::uint64_t, std::size_t length) noexcept {
              if (object.value() < bytes_.size())
                  bytes_[object.value()] += length;
          });
        fake_file_test_access::observe_writes(
          files_, [this](fake_object_id, std::uint64_t, std::size_t) noexcept {
              ++writes_;
          });
    }
    counted_io(const counted_io&) = delete;
    counted_io& operator=(const counted_io&) = delete;
    ~counted_io() {
        fake_file_test_access::observe_reads(files_, {});
        fake_file_test_access::observe_writes(files_, {});
    }
    [[nodiscard]] auto reads() {
        return [this](const runtime::file_path& path) noexcept
                 -> std::optional<std::uint64_t> {
            const auto resolved = fake_file_test_access::resolve(
              files_, path.value());
            if (!resolved) return std::nullopt;
            const auto object = fake_file_test_access::lookup(
              files_, *resolved);
            if (!object || object->value() >= bytes_.size())
                return std::nullopt;
            return bytes_[object->value()];
        };
    }
    [[nodiscard]] auto writes() {
        return
          [this]() noexcept -> std::optional<std::uint64_t> { return writes_; };
    }

private:
    fake_file_system& files_;
    std::vector<std::uint64_t> bytes_ = std::vector<std::uint64_t>(4096);
    std::uint64_t writes_{0};
};
} // namespace

SEASTAR_TEST_CASE(local_scan_reader_slots_fake) {
    co_await with_store_environment(
      config(), on_one_device(51, [](auto&&... shared) {
          return storage::testing::scan_contract::reader_slots(shared...);
      }));
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
          counted_io counted{files};
          co_await storage::testing::scan_contract::head_tails(
            files, owner, spec, budget, drive, counted.reads());
      });
}

SEASTAR_TEST_CASE(local_wal_scan_proven_heads_fake) {
    co_await with_store_environment(
      config(), on_one_device(51, [](auto&&... shared) {
          return storage::testing::scan_contract::proven_heads(shared...);
      }));
}

SEASTAR_TEST_CASE(local_wal_scan_packed_writes_fake) {
    co_await with_store_environment(
      config(), on_one_device(51, [](auto&&... shared) {
          return storage::testing::scan_contract::packed_writes(shared...);
      }));
}

SEASTAR_TEST_CASE(local_wal_scan_recycled_head_fake) {
    co_await with_store_environment(
      config(), on_one_device(51, [](auto&&... shared) {
          return storage::testing::scan_contract::recycled_head(shared...);
      }));
}

SEASTAR_TEST_CASE(local_wal_scan_rotated_chain_fake) {
    co_await with_store_environment(
      config(), on_one_device(51, [](auto&&... shared) {
          return storage::testing::scan_contract::rotated_chain(shared...);
      }));
}

SEASTAR_TEST_CASE(local_wal_scan_target_resolution_fake) {
    co_await with_store_environment(
      config(), on_one_device(51, [](auto&&... shared) {
          return storage::testing::scan_contract::target_resolution(shared...);
      }));
}

SEASTAR_TEST_CASE(local_segment_scan_stale_objects_fake) {
    co_await with_store_environment(
      config(), on_one_device(51, [](auto&&... shared) {
          return storage::testing::segment_scan_contract::stale_objects(
            shared...);
      }));
}

SEASTAR_TEST_CASE(local_segment_scan_stale_tails_fake) {
    co_await with_store_environment(
      config(), on_one_device(51, [](auto&&... shared) {
          return storage::testing::segment_scan_contract::stale_tails(
            shared...);
      }));
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
          counted_io counted{files};
          co_await storage::testing::segment_scan_contract::resume_and_tails(
            files, owner, spec, budget, drive, counted.reads());
      });
}

SEASTAR_TEST_CASE(local_recovery_publication_pins_fake) {
    co_await with_store_environment(
      config(), on_one_device(68, [](auto&&... shared) {
          return storage::testing::segment_scan_contract::publication_pins(
            shared...);
      }));
}

SEASTAR_TEST_CASE(local_recovery_classification_fake) {
    co_await with_store_environment(
      config(), on_one_device(51, [](auto&&... shared) {
          return storage::testing::segment_scan_contract::
            recovery_classification(shared...);
      }));
}

SEASTAR_TEST_CASE(local_recovery_merge_pass_fake) {
    co_await with_store_environment(
      config(), on_one_device(51, [](auto&&... shared) {
          return storage::testing::recovery_contract::merge_pass(shared...);
      }));
}

SEASTAR_TEST_CASE(local_recovery_merge_cases_fake) {
    co_await with_store_environment(
      config(), on_one_device(51, [](auto&&... shared) {
          return storage::testing::recovery_contract::merge_cases(shared...);
      }));
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
          counted_io counted{files};
          co_await storage::testing::recovery_contract::plan_cases(
            files,
            owner,
            spec,
            budget,
            drive,
            counted.reads(),
            counted.writes());
      });
}

SEASTAR_TEST_CASE(local_recovery_decisions_fake) {
    co_await with_store_environment(
      config(), on_one_device(51, [](auto&&... shared) {
          return storage::testing::recovery_contract::decisions(shared...);
      }));
}

SEASTAR_TEST_CASE(local_recovered_seal_fake) {
    co_await with_store_environment(
      config(), on_one_device(51, [](auto&&... shared) {
          return storage::testing::recovery_contract::recovered_seal<
            simulation::monotonic_clock>(shared...);
      }));
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
      config(), on_one_device(51, [](auto&&... shared) {
          return storage::testing::recovery_contract::reconstruction<
            simulation::monotonic_clock>(shared...);
      }));
}

SEASTAR_TEST_CASE(local_recovery_reconstruction_residues_fake) {
    co_await with_store_environment(
      config(), on_one_device(51, [](auto&&... shared) {
          return storage::testing::recovery_contract::reconstruction_residues<
            simulation::monotonic_clock>(shared...);
      }));
}

SEASTAR_TEST_CASE(local_recovery_interruptions_fake) {
    co_await with_store_environment(
      config(), on_one_device(51, [](auto&&... shared) {
          return storage::testing::recovery_contract::interruptions<
            simulation::monotonic_clock>(shared...);
      }));
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
      config(), on_one_device(51, [](auto&&... shared) {
          return storage::testing::recovery_contract::recovering_publication<
            simulation::monotonic_clock>(shared...);
      }));
}

// Restart's write-back fails at its first write, and then at its flush:
// each time the shard stays out of ready with the WAL unchanged, and the
// next restart publishes. A run with nothing failing finds the data file
// and how often it was written and flushed before the write-back.
SEASTAR_TEST_CASE(local_recovery_materialization_failure_fake) {
    const auto run =
      [](environment_config chosen, auto ready) -> seastar::future<> {
        co_await with_store_environment(
          std::move(chosen),
          [ready](
            auto& env, auto& budget, auto drive) mutable -> seastar::future<> {
              auto& files = env.file_system();
              const auto root = take(runtime::file_path::make("/kwaque/store"));
              take(co_await drive.lifecycle(files.create_directories(root)));
              stabilize(files, take(runtime::file_path::make("/kwaque")));
              const auto spec = specification(root, {1, 1}, 51);
              const std::array specs{spec};
              ownership_input owner{specs};
              co_await storage::testing::recovery_contract::
                materialization_failure<simulation::monotonic_clock>(
                  files,
                  owner,
                  spec,
                  budget,
                  drive,
                  [&files, &ready](const runtime::file_path& path) {
                      return ready(files, path);
                  });
          });
    };
    struct {
        std::uint64_t object{0}, writes{0}, flushes{0};
    } found;
    co_await run(
      config(), [&found](auto& files, const runtime::file_path& path) {
          const auto object = take(
            fake_file_test_access::lookup(
              files,
              take(fake_file_test_access::resolve(files, path.value()))));
          found.object = object.value();
          found.writes = take(
            fake_file_test_access::occurrences(
              files, object, runtime::builtin_fault_point::file_write));
          found.flushes = take(
            fake_file_test_access::occurrences(
              files, object, runtime::builtin_fault_point::file_flush));
          return false;
      });
    BOOST_REQUIRE_NE(found.object, 0U);
    const auto armed = [](auto&, const runtime::file_path&) { return true; };
    const auto key = runtime::fault_object_key::from_u64(found.object);
    co_await run(
      config(
        runtime::builtin_fault_point::file_write,
        runtime::fault_action::file_failure_before_effect,
        found.writes + 1,
        key),
      armed);
    co_await run(
      config(
        runtime::builtin_fault_point::file_flush,
        runtime::fault_action::file_failure_before_effect,
        found.flushes + 1,
        key),
      armed);
}

SEASTAR_TEST_CASE(local_recovery_materialization_fake) {
    co_await with_store_environment(
      config(), on_one_device(51, [](auto&&... shared) {
          return storage::testing::recovery_contract::materialization<
            simulation::monotonic_clock>(shared...);
      }));
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
        path, {.access = runtime::file_access::read_write, .create = true})));
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
    const std::array catalog{recovery_catalog_entry{
      recovery::descriptor(), recovery::segment_head(), recovery::origin()}};
    auto found = take(
      co_await recovery::inventory(
        files,
        owner,
        spec,
        budget,
        catalog,
        std::span<const recovery_decision_record>{},
        drive));
    recovery_tails tails;
    auto planned = co_await recovery::plan(
      files,
      owner,
      spec,
      budget,
      found.targets,
      drive,
      recovery::head_control(),
      &tails);
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
      tails,
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
      config(), on_one_device(51, [](auto&&... shared) {
          return storage::testing::recovery_contract::source_cases(shared...);
      }));
}

SEASTAR_TEST_CASE(local_recovery_torn_seals_fake) {
    co_await with_store_environment(
      config(), on_one_device(51, [](auto&&... shared) {
          return storage::testing::recovery_contract::torn_seals<
            simulation::monotonic_clock>(shared...);
      }));
}

SEASTAR_TEST_CASE(local_recovery_torn_tails_fake) {
    co_await with_store_environment(
      config(), on_one_device(51, [](auto&&... shared) {
          return storage::testing::recovery_contract::torn_tails(shared...);
      }));
}

SEASTAR_TEST_CASE(local_recovery_bounded_scanning_fake) {
    co_await with_store_environment(
      config(), on_one_device(51, [](auto&&... shared) {
          return storage::testing::recovery_contract::bounded_scanning(
            shared...);
      }));
}
