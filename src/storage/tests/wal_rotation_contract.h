#pragma once

#include "src/storage/tests/wal_append_contract.h"

namespace kwaque::storage::testing::wal_rotation_contract {
using store_contract::require;
using store_contract::take;
namespace append = wal_append_contract;

template<typename Writer>
seastar::future<wal_submission> submit_one(
  Writer& writer,
  workload_budget& budget,
  model::cluster_id cluster,
  codec::cooperative_work& work) {
    const std::array records{assigned_wire(true, 48)};
    auto group = co_await append::offer(budget, cluster, work, records);
    co_return take(co_await writer.submit(std::move(group), work));
}

template<typename Backend, typename Owner, typename Driver>
seastar::future<> exercise(
  Backend& files,
  Owner& ownership,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    co_await append::with_writer(
      files,
      ownership,
      spec,
      budget,
      drive,
      wal_writer_contract::configuration(),
      [&](auto& writer, auto& control, auto& ids, auto& work)
        -> seastar::future<> {
          const auto empty = take(writer.capture());
          const auto old_head = *writer.prepared_head();
          const auto empty_extent = take(
            co_await drive.lifecycle(writer.inspect_extent()));
          require(
            empty_extent.capacity_bytes == byte_count{1048576}
              && empty_extent.observed_eof.value() == 8192
              && empty_extent.positions.write_complete == empty.cursor(),
            "capacity was confused with header EOF or complete end");
          auto rejected = co_await writer.rotate(empty, byte_count{8192}, work);
          require(
            !rejected && !writer.rotation_pending(), "rotated an empty file");
          auto first = co_await submit_one(
            writer, budget, spec.owner.cluster(), work);
          auto first_done = co_await drive.lifecycle(std::move(first.written));
          take(first_done.failure.outcome());
          auto second = co_await submit_one(
            writer, budget, spec.owner.cluster(), work);
          auto second_done = co_await drive.lifecycle(
            std::move(second.written));
          take(second_done.failure.outcome());
          rejected = co_await writer.rotate(
            first.boundary, byte_count{8192}, work);
          require(
            !rejected && rejected.error().code() == errc::wrong_context
              && !writer.rotation_pending(),
            "stale cut froze a newer accepted group");
          const auto cut = second.boundary;
          const auto extent = take(
            co_await drive.lifecycle(writer.inspect_extent()));
          require(
            extent.observed_eof == cut.cursor().position()
              && extent.positions.write_complete == cut.cursor()
              && extent.positions.durable == empty.cursor(),
            "visible written bytes became durable evidence");
          const auto before = take(control.snapshot());
          rejected = co_await writer.rotate(cut, byte_count{1048576}, work);
          require(
            !rejected && !writer.rotation_pending()
              && take(control.snapshot()).generation == before.generation,
            "impossible group created a successor or published an ID block");
          const auto skipped = take(
            co_await drive.lifecycle(ids.allocate_wal(work)));
          static_cast<void>(
            take(co_await drive.lifecycle(ids.allocate_object(work))));
          const auto marks = take(control.snapshot()).fields;
          const auto paths = take(local_paths::make(spec.root));
          const auto old_path = take(paths.wal(0, old_head.incarnation));
          const auto old_bytes = co_await store_contract::read_bytes(
            files, old_path, drive);
          take(
            co_await drive.lifecycle(
              writer.rotate(cut, byte_count{8192}, work)));
          const auto next = *writer.prepared_head();
          const auto current = take(control.snapshot());
          require(
            !writer.rotation_pending() && current.fields.wal_head == next
              && next.incarnation != skipped
              && skipped.canonical_less(next.incarnation)
              && current.fields.object_high == marks.object_high
              && current.fields.checkpoint == marks.checkpoint,
            "rotation inferred a consecutive ID or overwrote other control "
            "fields");
          require(
            writer.statistics().rotations == 1
              && writer.statistics().flush_calls == 1,
            "rotation did not perform exactly the required old-tail barrier");
          require(
            !writer.validate_capture(cut),
            "old capture authorized the successor");
          auto old_barrier = co_await writer.barrier(cut);
          require(
            old_barrier.failure.failed() && !old_barrier.receipt,
            "old cut manufactured successor durability");
          const auto next_path = take(paths.wal(0, next.incarnation));
          const auto expected_header = wal_writer_contract::header_bytes(
            spec, 0, next.incarnation, cut.cursor());
          require(
            (co_await store_contract::read_bytes(files, next_path, drive))
              == expected_header,
            "successor header differs from independent predecessor bytes");
          require(
            (co_await store_contract::read_bytes(files, old_path, drive))
              == old_bytes,
            "rotation truncated or recycled the predecessor");
          auto next_extent = take(
            co_await drive.lifecycle(writer.inspect_extent()));
          require(
            next_extent.observed_eof.value() == 8192
              && next_extent.positions.reserved
                   == next_extent.positions.durable,
            "successor activated slack or an unpublished empty cut");
          auto third = co_await submit_one(
            writer, budget, spec.owner.cluster(), work);
          auto third_done = co_await drive.lifecycle(std::move(third.written));
          take(third_done.failure.outcome());
          const auto second_cut = third.boundary;
          take(
            co_await drive.lifecycle(
              writer.rotate(second_cut, byte_count{8192}, work)));
          require(
            writer.statistics().rotations == 2
              && writer.statistics().flush_calls == 2,
            "stable file slots failed a second rotation");
          const auto newest = *writer.prepared_head();
          require(
            (co_await store_contract::read_bytes(
              files, take(paths.wal(0, newest.incarnation)), drive))
              == wal_writer_contract::header_bytes(
                spec, 0, newest.incarnation, second_cut.cursor()),
            "second rotation lost its immediate predecessor");
      });
}

template<typename Backend, typename Owner, typename Driver>
seastar::future<> capacity(
  Backend& files,
  Owner& ownership,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    auto config = wal_writer_contract::configuration();
    config.capacity_bytes = byte_count{16384};
    co_await append::with_writer(
      files,
      ownership,
      spec,
      budget,
      drive,
      config,
      [&](auto& writer, auto& work) -> seastar::future<> {
          auto first = co_await submit_one(
            writer, budget, spec.owner.cluster(), work);
          auto written = co_await drive.lifecycle(std::move(first.written));
          take(written.failure.outcome());
          require(
            first.boundary.cursor().position().value() == 16384,
            "single complete group did not fill capacity");
          const std::array records{assigned_wire(true, 48)};
          auto extra = co_await append::offer(
            budget, spec.owner.cluster(), work, records);
          auto rejected = co_await writer.submit(std::move(extra), work);
          require(
            !rejected && writer.progress()->reserved == first.boundary.cursor(),
            "capacity rejection reserved a partial group");
          take(
            co_await drive.lifecycle(
              writer.rotate(first.boundary, byte_count{8192}, work)));
          auto second = co_await submit_one(
            writer, budget, spec.owner.cluster(), work);
          auto done = co_await drive.lifecycle(std::move(second.written));
          take(done.failure.outcome());
          require(
            second.boundary.cursor().position().value() == 16384
              && second.boundary.cursor().incarnation()
                   != first.boundary.cursor().incarnation(),
            "whole group did not fit the empty successor");
      });
}

template<typename Backend, typename Owner, typename Driver>
seastar::future<> pressure(
  Backend& files,
  Owner& ownership,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  bool abandon = false) {
    co_await append::with_writer(
      files,
      ownership,
      spec,
      budget,
      drive,
      wal_writer_contract::configuration(),
      [&](auto& writer, auto& control, auto&, auto& work) -> seastar::future<> {
          auto accepted = co_await submit_one(
            writer, budget, spec.owner.cluster(), work);
          const auto cut = accepted.boundary;
          const auto old = *writer.prepared_head();
          std::array<std::optional<workload_reservation>, 32> held;
          for (auto& slot : held) {
              auto reserve = budget.try_reserve(byte_count{4096});
              if (!reserve) break;
              slot.emplace(std::move(*reserve));
          }
          require(
            !budget.try_reserve(byte_count{1}),
            "ordinary tasks did not saturate");
          auto rejected = co_await drive.lifecycle(
            writer.rotate(cut, byte_count{8192}, work));
          require(
            !rejected && writer.rotation_pending() && !writer.successor_head()
              && !writer.failure().failed(),
            "rotation admission pressure fenced accepted work");
          auto done = co_await drive.lifecycle(std::move(accepted.written));
          take(done.failure.outcome());
          auto durable = co_await drive.lifecycle(writer.barrier(cut));
          take(durable.failure.outcome());
          require(
            durable.receipt.has_value()
              && writer.progress()->durable == cut.cursor(),
            "accepted cut could not drain under rotation admission pressure");
          for (auto& slot : held)
              slot.reset();

          // Hold an unrelated current-state edit across successor preparation.
          // It must not block accepted-work completion or be overwritten later.
          seastar::promise<> entered, release;
          auto entered_future = entered.get_future();
          seastar::abort_source edit_abort;
          codec::cooperative_work edit_work{work.policy(), edit_abort};
          auto update = control.update(
            [](local_shard_control& fields) -> runtime::result<void> {
                fields.deletion_high
                  = fields.deletion_high.checked_advance(1).value();
                return {};
            },
            [wait = release.get_future(), &entered](
              const auto&,
              const auto&,
              auto&) mutable -> seastar::future<runtime::result<void>> {
                entered.set_value();
                co_await std::move(wait);
                co_return runtime::result<void>{};
            },
            edit_work);
          runtime::first_failure failed;
          std::optional<local_wal_head> pending;
          try {
              co_await drive.lifecycle(std::move(entered_future));
              rejected = co_await drive.lifecycle(
                writer.rotate(cut, byte_count{8192}, work));
              pending = writer.successor_head();
              require(
                !rejected
                  && runtime::file_detail(rejected.error())
                       == runtime::file_failure_detail::admission_not_dispatched
                  && pending && writer.rotation_pending()
                  && writer.rotation_publication().admission_rejected
                  && !writer.failure().failed()
                  && take(control.snapshot()).fields.wal_head == old,
                "busy head publication activated, fenced, or lost its "
                "successor");
              const auto bytes = co_await store_contract::read_bytes(
                files,
                take(take(local_paths::make(spec.root))
                       .wal(0, pending->incarnation)),
                drive);
              require(
                bytes
                  == wal_writer_contract::header_bytes(
                    spec, 0, pending->incarnation, cut.cursor()),
                "pending header did not freeze the predecessor cut");
              const std::array records{assigned_wire()};
              auto group = co_await append::offer(
                budget, spec.owner.cluster(), work, records);
              auto submission = co_await writer.submit(std::move(group), work);
              require(
                !submission && writer.progress()->reserved == cut.cursor(),
                "pending successor admitted a PREPARE");
              auto again = co_await drive.lifecycle(
                writer.rotate(cut, byte_count{8192}, work));
              require(
                !again && writer.successor_head() == pending,
                "busy retry allocated another successor");
          } catch (...) {
              failed.observe(std::current_exception());
          }
          release.set_value();
          auto updated = co_await drive.lifecycle(std::move(update));
          failed.observe(updated.failure.outcome());
          take(failed.outcome());
          const auto marks = take(control.snapshot()).fields;
          if (abandon) {
              take(co_await drive.lifecycle(writer.close()));
              require(
                !writer.rotation_pending() && writer.statistics().rotations == 0
                  && writer.successor_head() == pending
                  && take(control.snapshot()).fields.wal_head == old,
                "close activated a pending successor");
              require(
                (co_await store_contract::read_bytes(
                  files,
                  take(take(local_paths::make(spec.root))
                         .wal(0, pending->incarnation)),
                  drive))
                  == wal_writer_contract::header_bytes(
                    spec, 0, pending->incarnation, cut.cursor()),
                "close deleted the prepared unreferenced header");
              co_return;
          }
          take(
            co_await drive.lifecycle(
              writer.rotate(cut, byte_count{8192}, work)));
          const auto after = take(control.snapshot()).fields;
          require(
            writer.prepared_head() == pending && after.wal_head == pending
              && after.deletion_high == marks.deletion_high
              && after.wal_high == marks.wal_high
              && after.checkpoint == marks.checkpoint
              && writer.statistics().flush_calls == 1,
            "retry lost a current edit, reissued an ID, or reflushed a "
            "certified cut");
      });
}
} // namespace kwaque::storage::testing::wal_rotation_contract
