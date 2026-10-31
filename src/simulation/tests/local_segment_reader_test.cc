#include "src/base/units.h"
#include "src/simulation/fake_file_test_support.h"
#include "src/simulation/tests/local_store_environment.h"
#include "src/simulation/virtual_time.h"
#include "src/storage/tests/local_store_contract.h"
#include "src/storage/tests/segment_reader_contract.h"
#include "src/storage/tests/sparse_index_contract.h"

#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <utility>

namespace {
using namespace kwaque;
using namespace kwaque::simulation;
using namespace kwaque::simulation::testing::local_store_environment;
using namespace kwaque::storage;
using namespace kwaque::storage::testing::store_contract;
} // namespace

SEASTAR_TEST_CASE(sparse_index_fake_active_feed) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::sparse_index_contract::active_feed<
              simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(sparse_index_fake_sealed_index) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::sparse_index_contract::sealed_index<
              simulation::monotonic_clock>(shared..., false);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(sparse_index_fake_empty_extent) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::sparse_index_contract::empty_extent<
              simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(sparse_index_fake_index_named_after_seal) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::sparse_index_contract::sealed_index<
              simulation::monotonic_clock>(shared..., true);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(sparse_index_fake_owed_index) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::sparse_index_contract::owed_index<
              simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(sparse_index_fake_resident_roots) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::sparse_index_contract::resident_roots<
              simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(sparse_index_fake_scanned_lookups) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::sparse_index_contract::scanned_lookups<
              simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(sparse_index_fake_replaced_under_readers) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::sparse_index_contract::
              replaced_under_readers<simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(sparse_index_fake_paged_lookups) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::sparse_index_contract::paged_lookups<
              simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(sparse_index_fake_cached_lookups) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::sparse_index_contract::cached_lookups<
              simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(sparse_index_fake_located_index) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::sparse_index_contract::located_index<
              simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(sparse_index_fake_sealed_data) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::sparse_index_contract::sealed_data<
              simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_reader_fake_run_checks) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_reader_contract::run_checks<
              simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_reader_fake_sealed_reads) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_reader_contract::sealed_reads<
              simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_reader_fake_prefetched_runs) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_reader_contract::prefetched_runs<
              simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_reader_fake_ahead_reads) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_reader_contract::ahead_reads<
              simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_reader_fake_sealed_tail) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_reader_contract::sealed_tail<
              simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_reader_fake_moved_sessions) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_reader_contract::moved_sessions<
              simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_reader_fake_fed_tail) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_reader_contract::fed_tail<
              simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_reader_fake_established_tail) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_reader_contract::established_tail<
              simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_reader_fake_woken_tail) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_reader_contract::woken_tail<
              simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_reader_fake_extended_session) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_reader_contract::extended_session<
              simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_reader_fake_steady_tail) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_reader_contract::steady_tail<
              simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_reader_fake_reported_before_made) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_reader_contract::
              reported_before_made<simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_reader_fake_held_reads) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_reader_contract::held_reads<
              simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_reader_fake_packed_reads) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_reader_contract::packed_reads<
              simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_reader_fake_read_edges) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_reader_contract::read_edges<
              simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_reader_fake_empty_reads) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_reader_contract::empty_reads<
              simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_reader_fake_damaged_runs) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_reader_contract::damaged_runs<
              simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_reader_fake_mixed_batches) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_reader_contract::mixed_batches<
              simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_reader_fake_fed_bounds) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_reader_contract::fed_bounds<
              simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_reader_fake_fed_limit) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_reader_contract::fed_limit<
              simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_reader_fake_crowded_reads) {
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
          // The reads the fake device has had of the segment's data.
          const auto reads = [&files, &spec] {
              const auto path
                = storage::testing::sparse_index_contract::data_path(spec);
              const auto found = take(
                fake_file_test_access::lookup(
                  files,
                  take(fake_file_test_access::resolve(files, path.value()))));
              return std::optional{take(
                fake_file_test_access::occurrences(
                  files, found, runtime::builtin_fault_point::file_read))};
          };
          co_await storage::testing::segment_reader_contract::crowded_reads<
            simulation::monotonic_clock>(
            files, owner, spec, budget, drive, reads);
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

namespace {
// What the fake device says of one file: the object it is, and the reads it
// has had.
storage::testing::segment_reader_contract::device_probe
probe_file(fake_file_system& files, const runtime::file_path& path) {
    const auto found = take(
      fake_file_test_access::lookup(
        files, take(fake_file_test_access::resolve(files, path.value()))));
    return {
      found.value(),
      take(
        fake_file_test_access::occurrences(
          files, found, runtime::builtin_fault_point::file_read))};
}
// One store on the fake device for the two cases below, each of which runs
// once to learn what to fail and once more with that failing.
template<typename Case>
seastar::future<>
with_reader_store(environment_config configuration, Case run) {
    co_await with_store_environment(
      std::move(configuration),
      [&run](auto& env, auto& budget, auto drive) -> seastar::future<> {
          auto& files = env.file_system();
          const auto root = take(runtime::file_path::make("/kwaque/store"));
          take(co_await drive.lifecycle(files.create_directories(root)));
          stabilize(files, take(runtime::file_path::make("/kwaque")));
          const auto spec = specification(root, {1, 1}, 68);
          const std::array specs{spec};
          ownership_input owner{specs};
          co_await run(files, owner, spec, budget, drive);
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}
} // namespace

SEASTAR_TEST_CASE(segment_reader_fake_failed_reads) {
    using storage::testing::segment_reader_contract::device_probe;
    device_probe learned;
    for (const bool faulted : {false, true}) {
        device_probe seen;
        // The second read of the segment's data, which is its second run's.
        co_await with_reader_store(
          faulted ? config(
                      runtime::builtin_fault_point::file_read,
                      runtime::fault_action::file_failure_before_effect,
                      learned.reads + 2,
                      runtime::fault_object_key::from_u64(learned.object))
                  : config(),
          [faulted, &seen](
            auto& files,
            auto& owner,
            const auto& spec,
            auto& budget,
            auto drive) {
              return storage::testing::segment_reader_contract::failed_reads<
                simulation::monotonic_clock>(
                files,
                owner,
                spec,
                budget,
                drive,
                faulted,
                seen,
                [&files](const runtime::file_path& path) {
                    return probe_file(files, path);
                });
          });
        // The fake device is the same from one run to the next.
        BOOST_REQUIRE(
          !faulted
          || (seen.object == learned.object && seen.reads == learned.reads));
        learned = seen;
    }
}

SEASTAR_TEST_CASE(segment_reader_fake_starved_index) {
    using storage::testing::segment_reader_contract::device_probe;
    device_probe learned;
    for (const bool faulted : {false, true}) {
        device_probe seen;
        std::optional<fault_rule> starving;
        if (faulted)
            // The next three reads of the index's bundle.
            starving.emplace(take(
              fault_rule::make(
                take(fault_rule_id::make(2)),
                runtime::builtin_fault_point::file_read,
                runtime::fault_object_key::from_u64(learned.object),
                take(runtime::fault_occurrence::make(learned.reads + 1)),
                take(runtime::fault_occurrence::make(learned.reads + 3)),
                fault_selector::bounded_range(),
                take(
                  runtime::fault_decision::make_file_failure(
                    runtime::fault_action::file_failure_before_effect,
                    runtime::file_failure_detail::device_io,
                    byte_count{0})))));
        co_await with_reader_store(
          config(
            std::nullopt,
            runtime::fault_action::file_failure_before_effect,
            1,
            std::nullopt,
            std::nullopt,
            std::move(starving)),
          [faulted, &seen](
            auto& files,
            auto& owner,
            const auto& spec,
            auto& budget,
            auto drive) {
              return storage::testing::segment_reader_contract::starved_index<
                simulation::monotonic_clock>(
                files,
                owner,
                spec,
                budget,
                drive,
                faulted,
                seen,
                [&files](const runtime::file_path& path) {
                    return probe_file(files, path);
                });
          });
        BOOST_REQUIRE(
          !faulted
          || (seen.object == learned.object && seen.reads == learned.reads));
        learned = seen;
    }
}
