#include "src/base/units.h"
#include "src/runtime/testing/reactor_tasks.h"
#include "src/simulation/environment.h"
#include "src/simulation/fake_file_test_support.h"
#include "src/simulation/scheduler_driver.h"
#include "src/simulation/tests/local_store_environment.h"
#include "src/simulation/virtual_time.h"
#include "src/storage/tests/local_installation_contract.h"
#include "src/storage/tests/local_store_contract.h"
#include "src/storage/tests/segment_qualification_contract.h"
#include "src/storage/tests/segment_writer_contract.h"
#include "src/storage/tests/sparse_index_contract.h"

#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <bit>
#include <utility>

namespace {
using namespace kwaque;
using namespace kwaque::simulation;
using namespace kwaque::simulation::testing::local_store_environment;
using namespace kwaque::storage;
using namespace kwaque::storage::testing::store_contract;
// Folds a group's blocks, appended after those already folded, into what a
// footer or sealed root covering them records.
void fold_times(boundary_times& times, const segment_frozen_group& group) {
    for (const auto& block : group.blocks()) {
        const auto stamped = block.descriptor();
        const auto record = stamped.batch().max_record_time;
        if (times.first_append == runtime::wall_time{}) {
            times.first_append = stamped.stamps().append_time;
            times.max_record = record;
        }
        times.max_append = stamped.stamps().append_time;
        times.max_record = std::max(times.max_record, record);
    }
}
} // namespace

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
          contract::logical_origin(),
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
        boundary_times times;
        fold_times(times, group);
        const auto footer = storage::testing::footer_wire(
          {cut.covered(),
           cut.end().blocks,
           group.layout().blocks().back().records,
           times,
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
          files,
          owner,
          spec,
          0,
          contract::descriptor(),
          contract::logical_origin(),
          resources,
          options));
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
          files,
          owner,
          spec,
          0,
          contract::descriptor(),
          contract::logical_origin(),
          resources,
          options));
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
        boundary_times times;
        fold_times(times, group);
        const auto footer = storage::testing::footer_wire(
          {cut.covered(),
           cut.end().blocks,
           last,
           times,
           storage::testing::crc(block)},
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
                            cut.context(), cut.history().alignment)
                            .value())
                        + extent
                        + storage::testing::sealed_wire(
                          {coverage, cut.end().blocks, last, times, 0},
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
        // The seal slot's grants, reserved before admission closes.
        segment_seal_grants grants;
        take(grants.fill(resources, writer->config().metadata));
        const auto before = resources.snapshot();
        resources.close_admission();
        auto sealing = writer->seal(
          contract::completed_source{std::move(held), std::move(facts), &calls},
          1,
          work,
          segment_no_index{},
          &grants);
        auto joined = writer->seal(
          contract::completed_source{{}, {}, &calls}, 0, work);
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
              contract::completed_source{{}, {}, &calls}, 0, work));
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
        make_new(
          files,
          owner,
          spec,
          0,
          description,
          contract::logical_origin(),
          resources,
          options));
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
        boundary_times times;
        // The first group's block (4,401 bytes) and footer span two
        // units, so its write is two native chunks: the delayed first one
        // and a second that completes before it. The rest take one each.
        for (std::uint64_t i = 0; i < 4; ++i) {
            groups.push_back(
              co_await contract::freeze_child(
                *writer,
                i == 0
                  ? co_await contract::valued_execution_child(100, 4000, work)
                  : co_await contract::execution_child(100 + i, work),
                resources,
                work));
            take(co_await writer->encode_group(groups.back(), work));
            const auto& group = groups.back();
            const auto cut = group.layout().boundary();
            const auto block = storage::testing::flat(
              group.blocks()[0].bytes());
            expected += block;
            fold_times(times, group);
            expected += storage::testing::footer_wire(
              {cut.covered(),
               cut.end().blocks,
               group.layout().blocks().back().records,
               times,
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
    auto open_directory(runtime::file_path path) {
        return invoke<directory_cursor_type>(
          [&] { return files_.open_directory(std::move(path)); });
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
    auto sync_directory(runtime::file_path path) {
        return invoke<void>(
          [&] { return files_.sync_directory(std::move(path)); });
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
    const auto origin = contract::logical_origin();
    const auto head = segment_header::make(
                        description.segment, description.alignment)
                        .value();
    const segment_history_context history{
      description.segment,
      description.alignment,
      runtime::file_position{8192},
      origin,
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
    // The block packs from the data start; the footer after it pads the
    // span to 16384, where the sealed root begins.
    const auto block_end = 8192 + block.size();
    const auto block_span = scope(100, 101, 0, 1, 8192, block_end);
    const boundary_times times{
      runtime::wall_time{1},
      runtime::wall_time{1},
      runtime::wall_time{max_record_time(assigned_wire())}};
    const auto footer = footer_wire(
      {block_span, 1, block_span, times, crc(block)},
      {history, runtime::file_position{block_end}});
    const auto extent_bytes = block + footer;
    const auto covered = scope(100, 101, 0, 1, 8192, 16384);
    const footer_expectation location{history, runtime::file_position{16384}};
    const std::array facts{retry()};
    const auto page_bytes = retry_page_wire(facts, location);
    const std::array refs{reference(page_bytes)};
    const auto sealed_bytes = sealed_wire(
      {covered, 1, block_span, times, 0},
      exact_digest(extent_bytes),
      refs,
      location);
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
        session, owner, spec, 0, description, origin, resources, options));
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
      origin,
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

namespace {
struct index_history_steps final {
    // Storage steps before the index's own work began, and in all.
    std::size_t began{0}, total{0};
};

// One sealed segment's index through a crash. The seal publishes the index,
// or with `late` the segment is sealed without one and the index is named
// afterwards. The process is cut after `cut` storage steps, or runs to its
// end, and the device crashes. What a restart then finds must be a
// publication that opens: still active, or sealed over the whole extent, and
// a sealed one either names an index that is whole or names none, in which
// case the data builds it again. Whatever was returned before the cut is
// found. A bundle the crash left unnamed goes at the next open.
seastar::future<index_history_steps> index_crash_history(
  environment& env,
  workload_budget& resources,
  simulation::testing::scheduler_driver drive,
  std::optional<std::size_t> cut,
  bool late) {
    namespace contract = storage::testing::segment_writer_contract;
    namespace indexes = storage::testing::sparse_index_contract;
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
    auto options = contract::configuration();
    options.maximum_groups = 2;
    options.admission.working_bytes = byte_count{1_MiB};
    options.admission.maximum_blocks = 16;
    const sparse_index_stride stride{byte_count{0}};
    const auto object = local_object_sequence::make(46).value();
    auto index = take(
      active_sparse_index::make(
        stride,
        sparse_index_capacity(
          stride,
          description.maximum_data_bytes,
          options.admission.maximum_blocks),
        resources));
    crash_file_session session{files};
    auto writer = take(
      writer_type::make_new(
        session,
        owner,
        spec,
        0,
        description,
        contract::logical_origin(),
        resources,
        options));
    std::vector<complete_block_descriptor> blocks;
    blocks.reserve(2);
    bool began = false, sealed_returned = false, named_returned = false;
    auto activity = [&]() -> seastar::future<runtime::first_failure> {
        runtime::first_failure outcome;
        try {
            outcome.observe(co_await writer->create_new(work));
            if (outcome.failed()) co_return outcome;
            outcome.observe(writer->observe_blocks(index.observer()));
            if (outcome.failed()) co_return outcome;
            for (std::uint32_t i = 0; i < 2; ++i) {
                std::optional<encoded_assigned_batch> batch;
                if (i == 0)
                    batch.emplace(co_await contract::child(work));
                else
                    batch.emplace(
                      co_await contract::execution_child(101, work));
                auto group = co_await contract::freeze_child(
                  *writer, std::move(*batch), resources, work);
                outcome.observe(co_await writer->encode_group(group, work));
                if (outcome.failed()) co_return outcome;
                blocks.push_back(group.blocks()[0].descriptor());
                auto submission = take(writer->submit(std::move(group), work));
                auto written = co_await std::move(submission.written);
                outcome = written.failure;
                if (outcome.failed()) co_return outcome;
            }
            std::uint32_t calls = 0;
            began = !late;
            auto sealed = co_await writer->seal(
              contract::completed_source{{}, {}, &calls},
              0,
              work,
              sparse_index_seal{
                &index, late ? local_object_sequence{} : object});
            outcome = sealed.failure;
            if (outcome.failed()) co_return outcome;
            sealed_returned = sealed.boundary && sealed.retry && sealed.extent;
            named_returned = sealed.index.has_value();
            if (late) {
                began = true;
                const auto named = co_await writer->publish_index(
                  sparse_index_seal{&index, object}, work);
                outcome.observe(named);
                named_returned = named.has_value();
            }
        } catch (...) {
            outcome.observe(std::current_exception());
        }
        co_return outcome;
    };
    std::optional<seastar::future<runtime::first_failure>> pending{activity()};
    std::optional<seastar::future<runtime::result<void>>> closing;
    runtime::first_failure failed, completed;
    index_history_steps steps;
    bool marked = false;
    try {
        while ((!cut || steps.total < *cut) && steps.total < 1024) {
            if (began && !marked) {
                steps.began = steps.total;
                marked = true;
            }
            const auto boundary = co_await history_step(
              env.event_scheduler(), *pending);
            if (boundary != history_boundary::stepped) break;
            ++steps.total;
        }
        if (began && !marked) {
            steps.began = steps.total;
            marked = true;
        }
        require(steps.total < 1024, "index history exceeded its event bound");
        if (!cut) {
            require(
              pending->available(), "index history parked without an event");
            auto waiting = std::move(*pending);
            pending.reset();
            completed = co_await std::move(waiting);
            take(completed.outcome());
            require(
              marked && sealed_returned && named_returned,
              "an uninterrupted index history did not seal and name its "
              "index");
        }
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

    // All a restart has is what the device holds now.
    seastar::abort_source recovered_abort;
    codec::cooperative_work recovered_work{
      codec::limits::defaults(), recovered_abort};
    crash_file_session recovered{files};
    const auto reopen = [&] {
        return drive.lifecycle(open_recovery_generation(
          recovered,
          owner,
          spec,
          0,
          description,
          contract::logical_origin(),
          resources,
          limits(),
          recovered_work));
    };
    auto found = co_await reopen();
    require(found.has_value(), "a crash left a publication that does not open");
    auto generation = std::move(*found);
    const auto publication = generation.publication;
    const bool sealed = publication.state == local_object_state::sealed;
    const bool indexed = std::ranges::any_of(
      publication.roots, [](const local_root_reference& named) {
          return named.kind() == local_root_kind::index;
      });
    runtime::first_failure checked;
    try {
        require(
          sealed || publication.state == local_object_state::active,
          "a crash produced a publication no seal makes");
        require(
          (sealed || (!sealed_returned && !late))
            && (indexed || !named_returned),
          "a seal or an index that was returned did not survive the crash");
        require(
          !generation.index_rebuild && (sealed || !indexed),
          "a publication names an index that is not whole");
        if (sealed) {
            auto pin = take(generation.owner->pin());
            const auto& footer = std::get<sealed_footer>(*pin.boundary());
            const auto context = take(
              sparse_index_context::make(
                description.segment,
                footer.coverage(),
                footer.extent_digest(),
                description.alignment));
            if (indexed) {
                // It answers for every block, and the whole of it agrees
                // with the data, which the same walk verifies.
                auto named = take(pin.root(local_root_kind::index));
                for (const auto& block : blocks) {
                    const auto answer = take(
                      co_await drive.lifecycle(find_published_anchor(
                        named,
                        model::range_logical_offset::make(
                          block.coverage().logical().begin().value())
                          .value(),
                        recovered_work)));
                    require(
                      answer
                        && validate_sparse_index_anchor(
                             context, answer->anchor, block)
                             .has_value(),
                      "a named index does not lead to its blocks");
                }
                require(
                  !take(
                    co_await drive.lifecycle(scrub_published_index(
                      recovered,
                      owner,
                      spec,
                      0,
                      generation.history,
                      named,
                      resources,
                      indexes::scan_limits(),
                      recovered_work))),
                  "a named index is not the index of its data");
            } else {
                // Owed: the sealed data builds it, block for block.
                const auto rebuilt = take(
                  co_await drive.lifecycle(rebuild_sparse_index(
                    recovered,
                    owner,
                    spec,
                    0,
                    generation.history,
                    context,
                    stride,
                    resources,
                    indexes::scan_limits(),
                    recovered_work)));
                require(
                  rebuilt.size() == blocks.size(),
                  "the data did not build its index again");
                for (std::uint32_t i = 0; i < rebuilt.size(); ++i)
                    require(
                      validate_sparse_index_anchor(
                        context, rebuilt[i], blocks[i])
                        .has_value(),
                      "an index built again does not name its blocks");
            }
        }
    } catch (...) {
        checked.observe(std::current_exception());
    }
    const auto release = [&drive](local_recovery_generation& opened)
      -> seastar::future<runtime::result<void>> {
        if (!opened.owner) co_return runtime::result<void>{};
        opened.owner->retire();
        auto closed = co_await drive.lifecycle(opened.owner->close());
        opened.owner.reset();
        co_return closed;
    };
    try {
        checked.observe(co_await release(generation));
    } catch (...) {
        checked.observe(std::current_exception());
    }
    take(checked.outcome());
    // The next open removes whatever the crash left unnamed, and nothing a
    // publication names.
    static_cast<void>(take(
      co_await drive.lifecycle(remove_unreferenced_objects(
        recovered, owner, spec, 0, publication, recovered_work))));
    const auto bundle = take(
      take(local_paths::make(root))
        .object(
          0,
          {description.segment.segment(), description.segment.generation()},
          object));
    require(
      take(co_await drive.lifecycle(files.exists(bundle))) == indexed,
      "an index bundle that nothing names was left, or a named one removed");
    take(co_await drive.lifecycle(files.crash()));
    auto again = co_await reopen();
    require(again.has_value(), "a second crash left nothing that opens");
    const bool same = again->publication == publication
                      && !again->index_rebuild;
    take(co_await release(*again));
    require(same, "a second crash changed the publication a restart finds");
    co_return steps;
}

seastar::future<> index_crash_histories(bool late) {
    for (unsigned mask = 0; mask < 8; ++mask) {
        const fake_crash_policy survival{
          .data_percent = static_cast<std::uint8_t>((mask & 1U) ? 100 : 0),
          .namespace_percent = static_cast<std::uint8_t>((mask & 2U) ? 100 : 0),
          .eof_percent = static_cast<std::uint8_t>((mask & 4U) ? 100 : 0)};
        index_history_steps whole;
        co_await with_store_environment(
          config({}, runtime::fault_action::error, 1, {}, survival),
          [&](auto& env, auto& budget, auto drive) -> seastar::future<> {
              whole = co_await index_crash_history(
                env, budget, drive, {}, late);
          },
          byte_count{18_MiB},
          resource::workload_class::foreground_protocol,
          64);
        require(
          whole.began != 0 && whole.began < whole.total,
          "the index history had no storage boundaries of its own");
        for (std::size_t cut = whole.began; cut <= whole.total; ++cut) {
            BOOST_TEST_CONTEXT("survival=" << mask << " cut=" << cut) {
                co_await with_store_environment(
                  config({}, runtime::fault_action::error, 1, {}, survival),
                  [cut, late](
                    auto& env, auto& budget, auto drive) -> seastar::future<> {
                      static_cast<void>(co_await index_crash_history(
                        env, budget, drive, cut, late));
                  },
                  byte_count{18_MiB},
                  resource::workload_class::foreground_protocol,
                  64);
            }
        }
    }
}
} // namespace

// A crash at every storage step of a seal that publishes its segment's index.
SEASTAR_TEST_CASE(sparse_index_fake_seal_every_crash_boundary) {
    co_await index_crash_histories(false);
}

// A crash at every storage step of naming an index after the seal.
SEASTAR_TEST_CASE(sparse_index_fake_naming_every_crash_boundary) {
    co_await index_crash_histories(true);
}
