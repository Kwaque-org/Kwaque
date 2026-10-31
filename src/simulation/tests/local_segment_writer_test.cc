#include "src/base/units.h"
#include "src/simulation/fake_file_test_support.h"
#include "src/simulation/tests/local_store_environment.h"
#include "src/simulation/virtual_time.h"
#include "src/storage/tests/local_installation_contract.h"
#include "src/storage/tests/local_store_contract.h"
#include "src/storage/tests/segment_qualification_contract.h"
#include "src/storage/tests/segment_writer_contract.h"

#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <utility>

namespace {
using namespace kwaque;
using namespace kwaque::simulation;
using namespace kwaque::simulation::testing::local_store_environment;
using namespace kwaque::storage;
using namespace kwaque::storage::testing::store_contract;
// The writes the fake is asked for while attached, in order: a count of
// them, after which the rest are dropped and the record is full.
class recorded_writes final {
public:
    struct write final {
        std::uint64_t object, position, length;
    };
    recorded_writes(fake_file_system& files, std::size_t count)
      : files_(files) {
        writes_.reserve(count);
        fake_file_test_access::observe_writes(
          files_,
          [this](
            fake_object_id object,
            std::uint64_t position,
            std::size_t length) noexcept {
              if (writes_.size() < writes_.capacity())
                  writes_.push_back({object.value(), position, length});
          });
    }
    recorded_writes(const recorded_writes&) = delete;
    recorded_writes& operator=(const recorded_writes&) = delete;
    ~recorded_writes() { stop(); }
    // Stops recording and gives the writes kept.
    const std::vector<write>& stop() {
        fake_file_test_access::observe_writes(files_, {});
        return writes_;
    }

private:
    fake_file_system& files_;
    std::vector<write> writes_;
};
// The data file of the segment the writer contract describes.
fake_object_id
segment_data(fake_file_system& files, const runtime::file_path& root) {
    const auto description
      = storage::testing::segment_writer_contract::descriptor();
    const auto path = take(
      take(local_paths::make(root))
        .segment_file(
          0,
          {description.segment.segment(), description.segment.generation()},
          local_segment_file::data));
    return take(
      fake_file_test_access::lookup(
        files, take(fake_file_test_access::resolve(files, path.value()))));
}
} // namespace

SEASTAR_TEST_CASE(segment_writer_fake_creation) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_writer_contract::creation<
              simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol);
}

SEASTAR_TEST_CASE(segment_writer_fake_creation_handles) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_writer_contract::creation_handles<
              simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_admission) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_writer_contract::admission<
              simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol);
}

SEASTAR_TEST_CASE(segment_writer_fake_reserved_publication) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_writer_contract::
              reserved_publication(shared...);
        }),
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
              contract::logical_origin(),
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
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_writer_contract::execution<
              simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

// Every write to the data file, as the device sees it, starts and ends on
// the segment's alignment, and none touches bytes an earlier one wrote.
SEASTAR_TEST_CASE(segment_writer_fake_packed_spans) {
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
          recorded_writes recorded{files, 4096};
          co_await storage::testing::segment_writer_contract::packed_spans<
            simulation::monotonic_clock>(files, owner, spec, budget, drive);
          const auto& writes = recorded.stop();
          const auto description
            = storage::testing::segment_writer_contract::descriptor();
          const auto data = segment_data(files, root);
          const auto unit = description.alignment.bytes().value();
          std::size_t seen = 0;
          for (std::size_t i = 0; i < writes.size(); ++i) {
              const auto& one = writes[i];
              if (one.object != data.value()) continue;
              ++seen;
              require(
                one.position % unit == 0 && one.length % unit == 0,
                "a segment write did not start and end on its alignment");
              for (std::size_t j = 0; j < i; ++j) {
                  const auto& earlier = writes[j];
                  require(
                    earlier.object != data.value()
                      || one.position >= earlier.position + earlier.length
                      || one.position + one.length <= earlier.position,
                    "a segment write touched bytes an earlier one wrote");
              }
          }
          require(
            seen > 1 && writes.size() < writes.capacity(),
            "the segment's writes were not all seen");
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

// A run of held groups reaches the device as one write, from the run's start
// to its footer's padding, and nothing of it before.
SEASTAR_TEST_CASE(segment_writer_fake_held_runs) {
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
          recorded_writes recorded{files, 4096};
          std::optional<model::file_byte_span> run;
          co_await storage::testing::segment_writer_contract::held_runs<
            simulation::monotonic_clock>(
            files, owner, spec, budget, drive, &run);
          require(run.has_value(), "the held run's span was not given");
          const auto& writes = recorded.stop();
          const auto data = segment_data(files, root);
          std::size_t whole = 0, inside = 0;
          for (const auto& one : writes) {
              if (one.object != data.value()) continue;
              if (
                one.position == run->begin().value()
                && one.length == run->size().value())
                  ++whole;
              else if (
                one.position < run->end().value()
                && one.position + one.length > run->begin().value())
                  ++inside;
          }
          require(
            whole == 1 && inside == 0 && writes.size() < writes.capacity(),
            "a held run was not written as one write");
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

// The run closes while the extension the writer started is writing its
// zeros: its one write is submitted before the extension's last.
SEASTAR_TEST_CASE(segment_writer_fake_held_run_beside_extension) {
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
          recorded_writes recorded{files, 8192};
          std::optional<model::file_byte_span> run;
          co_await storage::testing::segment_writer_contract::
            held_run_beside_extension<simulation::monotonic_clock>(
              files, owner, spec, budget, drive, &run);
          require(run.has_value(), "the held run's span was not given");
          const auto& writes = recorded.stop();
          const auto data = segment_data(files, root);
          std::optional<std::size_t> whole, last;
          std::uint64_t furthest = 0;
          for (std::size_t i = 0; i != writes.size(); ++i) {
              const auto& one = writes[i];
              if (one.object != data.value()) continue;
              if (
                one.position == run->begin().value()
                && one.length == run->size().value())
                  whole = i;
              if (one.position + one.length > furthest) {
                  furthest = one.position + one.length;
                  last = i;
              }
          }
          require(
            whole && last && *whole < *last && furthest > run->end().value()
              && writes.size() < writes.capacity(),
            "a held run inside the zero-written range waited for the "
            "extension");
      },
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_held_closes) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_writer_contract::held_closes<
              simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_append_order) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_writer_contract::append_order<
              simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_certified_stamps) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_writer_contract::certified_stamps<
              simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_exact_fit) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_writer_contract::exact_fit<
              simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

// Each alignment in a store of its own. A 64-block group holds a reservation
// per child: two tasks per block.
SEASTAR_TEST_CASE(segment_writer_fake_prepared_extent) {
    for (const std::uint64_t unit : {4096U, 8192U})
        co_await with_store_environment(
          config(),
          [unit](auto& env, auto& budget, auto drive) -> seastar::future<> {
              auto& files = env.file_system();
              const auto root = take(runtime::file_path::make("/kwaque/store"));
              take(co_await drive.lifecycle(files.create_directories(root)));
              stabilize(files, take(runtime::file_path::make("/kwaque")));
              const auto spec = specification(root, {1, 1}, 68);
              const std::array specs{spec};
              ownership_input owner{specs};
              co_await storage::testing::segment_writer_contract::
                prepared_extent<simulation::monotonic_clock>(
                  files, owner, spec, budget, drive, unit);
          },
          byte_count{18_MiB},
          resource::workload_class::foreground_protocol,
          128);
}

// Unsynced bytes kept or dropped in 512-byte granules under a 4 KiB
// alignment: nothing a completed barrier certified changes.
SEASTAR_TEST_CASE(segment_writer_fake_certified_spans_survive_crashes) {
    for (const unsigned kept : {0U, 50U, 100U}) {
        BOOST_TEST_CONTEXT("data_percent=" << kept) {
            const fake_crash_policy survival{
              .data_percent = static_cast<std::uint8_t>(kept),
              .eof_percent = static_cast<std::uint8_t>(kept),
              .granule_bytes = 512};
            co_await with_store_environment(
              config({}, runtime::fault_action::error, 1, {}, survival),
              [](auto& env, auto& budget, auto drive) -> seastar::future<> {
                  auto& files = env.file_system();
                  const auto root = take(
                    runtime::file_path::make("/kwaque/store"));
                  take(
                    co_await drive.lifecycle(files.create_directories(root)));
                  stabilize(files, take(runtime::file_path::make("/kwaque")));
                  const auto spec = specification(root, {1, 1}, 68);
                  const std::array specs{spec};
                  ownership_input owner{specs};
                  co_await storage::testing::segment_writer_contract::
                    certified_spans<simulation::monotonic_clock>(
                      files, owner, spec, budget, drive);
              },
              byte_count{18_MiB},
              resource::workload_class::foreground_protocol,
              64);
        }
    }
}

SEASTAR_TEST_CASE(segment_writer_fake_barriers) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_writer_contract::execution<
              simulation::monotonic_clock>(shared..., true);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_grouped_execution) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_writer_contract::grouped_execution<
              simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_data_pins) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_writer_contract::data_pins<
              simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_preallocated_execution) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_writer_contract::
              preallocated_execution<simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_zero_written_by_default) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_writer_contract::
              zero_written_by_default<simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_concurrent_execution) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_writer_contract::
              concurrent_execution<simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_extended_execution) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_writer_contract::
              extended_execution<simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_abandoned_group) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_writer_contract::abandoned_group<
              simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_close_preserves_borrowed_blocks) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_writer_contract::abandoned_group<
              simulation::monotonic_clock>(shared..., true);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_seal) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_writer_contract::seal_lifecycle<
              simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_empty_seal) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_writer_contract::seal_lifecycle<
              simulation::monotonic_clock>(shared..., true);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_reserved_seal) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_writer_contract::seal_lifecycle<
              simulation::monotonic_clock>(shared..., false, true);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_changed_seal_source) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_writer_contract::seal_lifecycle<
              simulation::monotonic_clock>(shared..., false, false, true);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_immutable_empty_initial) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_writer_contract::immutable_import<
              simulation::monotonic_clock>(
              shared...,
              storage::testing::segment_writer_contract::immutable_import_kind::
                empty_initial);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_immutable_sparse_rewrite) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_writer_contract::immutable_import<
              simulation::monotonic_clock>(
              shared...,
              storage::testing::segment_writer_contract::immutable_import_kind::
                sparse_rewrite);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_immutable_dense_relocation) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_writer_contract::immutable_import<
              simulation::monotonic_clock>(
              shared...,
              storage::testing::segment_writer_contract::immutable_import_kind::
                dense_relocation);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_immutable_removed_terminal) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_writer_contract::immutable_import<
              simulation::monotonic_clock>(
              shared...,
              storage::testing::segment_writer_contract::immutable_import_kind::
                removed_terminal);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_immutable_empty_terminal) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_writer_contract::immutable_import<
              simulation::monotonic_clock>(
              shared...,
              storage::testing::segment_writer_contract::immutable_import_kind::
                empty_terminal);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_unresolved_seal) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_writer_contract::seal_lifecycle<
              simulation::monotonic_clock>(
              shared..., false, false, false, true);
        }),
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
                contract::logical_origin(),
                budget,
                options));
          runtime::first_failure failed;
          try {
              take(co_await drive.lifecycle(writer->create_new(work)));
              std::uint32_t calls = 0;
              const auto seal = co_await drive.lifecycle(writer->seal(
                contract::completed_source{{}, {}, &calls}, 0, work));
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
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_qualification_contract::
              age_boundaries(shared..., false);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_age_overflow_restart) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_qualification_contract::
              age_boundaries(shared..., true);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_completion_pressure) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_qualification_contract::
              reserved_completion<simulation::monotonic_clock>(shared...);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_close_preflight) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_qualification_contract::
              close_entered_preflight<simulation::monotonic_clock>(
                shared..., false);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_seal_preflight) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_qualification_contract::
              close_entered_preflight<simulation::monotonic_clock>(
                shared..., true);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(
  segment_writer_fake_preallocated_paged_seal_and_retained_results) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_qualification_contract::
              paged_seal_and_retained_results<simulation::monotonic_clock>(
                shared..., true);
        }),
      byte_count{18_MiB},
      resource::workload_class::foreground_protocol,
      64);
}

SEASTAR_TEST_CASE(segment_writer_fake_paged_seal_and_retained_results) {
    co_await with_store_environment(
      config(),
      on_one_device(
        68,
        [](auto&&... shared) {
            return storage::testing::segment_qualification_contract::
              paged_seal_and_retained_results<simulation::monotonic_clock>(
                shared...);
        }),
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
                  pressure.push_back(
                    take(co_await drive.lifecycle(files.open(control, {}))));
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
                    contract::logical_origin(),
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
