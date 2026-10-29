#pragma once

#include "src/base/units.h"
#include "src/storage/retained_wal.h"
#include "src/storage/tests/wal_append_contract.h"
#include "src/storage/wal_reclaimer.h"

#include <array>
#include <cstdint>
#include <initializer_list>
#include <optional>
#include <string>
#include <type_traits>
#include <vector>

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
            empty_extent.capacity_bytes == byte_count{1_MiB}
              && empty_extent.observed_eof.value() == 8192
              && empty_extent.positions.write_complete == empty.cursor(),
            "capacity was confused with header EOF or complete end");
          auto rejected = co_await writer.rotate(
            empty, byte_count{8_KiB}, work);
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
            first.boundary, byte_count{8_KiB}, work);
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
          rejected = co_await writer.rotate(cut, byte_count{1_MiB}, work);
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
              writer.rotate(cut, byte_count{8_KiB}, work)));
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
              writer.rotate(second_cut, byte_count{8_KiB}, work)));
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
    config.capacity_bytes = byte_count{16_KiB};
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
              writer.rotate(first.boundary, byte_count{8_KiB}, work)));
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
              auto reserve = budget.try_reserve(byte_count{4_KiB});
              if (!reserve) break;
              slot.emplace(std::move(*reserve));
          }
          require(
            !budget.try_reserve(byte_count{1}),
            "ordinary tasks did not saturate");
          auto rejected = co_await drive.lifecycle(
            writer.rotate(cut, byte_count{8_KiB}, work));
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

          // Hold an unrelated current-state edit across successor preparation,
          // and two more waiting behind it, so every turn of the control is
          // taken. They must not block accepted-work completion or be
          // overwritten later.
          std::
            array<std::optional<seastar::future<local_publication_outcome>>, 2>
              queued;
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
              for (auto& waiting : queued)
                  waiting.emplace(control.update(
                    [](local_shard_control&) -> runtime::result<void> {
                        return {};
                    },
                    edit_work));
              require(
                control.waiting() == maximum_control_waiters,
                "control edits did not wait their turn");
              rejected = co_await drive.lifecycle(
                writer.rotate(cut, byte_count{8_KiB}, work));
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
                writer.rotate(cut, byte_count{8_KiB}, work));
              require(
                !again && writer.successor_head() == pending,
                "busy retry allocated another successor");
          } catch (...) {
              failed.observe(std::current_exception());
          }
          release.set_value();
          auto updated = co_await drive.lifecycle(std::move(update));
          failed.observe(updated.failure.outcome());
          for (auto& waiting : queued) {
              if (!waiting) continue;
              try {
                  auto done = co_await drive.lifecycle(std::move(*waiting));
                  failed.observe(done.failure.outcome());
              } catch (...) {
                  failed.observe(std::current_exception());
              }
              waiting.reset();
          }
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
              writer.rotate(cut, byte_count{8_KiB}, work)));
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

// The retained-file limit. A rotation that would add a file past it is
// refused before anything is allocated or published, as often as it is
// asked; the written group stays written; a removal reported to the writer
// lets the same rotation through. The head is never counted as removed, and
// a limit that could never rotate, or lies above the ceiling, is refused at
// construction.
template<typename Backend, typename Owner, typename Driver>
seastar::future<> retention(
  Backend& files,
  Owner& ownership,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    auto config = wal_writer_contract::configuration();
    config.retained_files = 2;
    co_await append::with_writer(
      files,
      ownership,
      spec,
      budget,
      drive,
      config,
      [&](auto& writer, auto& control, auto& ids, auto& work)
        -> seastar::future<> {
          for (const std::uint32_t limit :
               {std::uint32_t{1}, maximum_retained_wal_files + 1}) {
              auto invalid = config;
              invalid.retained_files = limit;
              auto made = std::remove_reference_t<decltype(writer)>::make(
                control,
                ids,
                budget,
                invalid,
                wal_start_intent::known_unactivated);
              require(
                !made && made.error().code() == errc::invalid_argument,
                "a retained limit that cannot hold was accepted");
          }
          require(
            writer.retention() == wal_retention{1, 2}
              && !writer.retention().full(),
            "a bootstrapped head is not the one file the shard holds");
          auto first = co_await submit_one(
            writer, budget, spec.owner.cluster(), work);
          auto first_done = co_await drive.lifecycle(std::move(first.written));
          take(first_done.failure.outcome());
          take(
            co_await drive.lifecycle(
              writer.rotate(first.boundary, byte_count{8_KiB}, work)));
          require(
            writer.retention() == wal_retention{2, 2}
              && writer.retention().full(),
            "a rotation did not count its successor");
          auto second = co_await submit_one(
            writer, budget, spec.owner.cluster(), work);
          auto second_done = co_await drive.lifecycle(
            std::move(second.written));
          take(second_done.failure.outcome());
          const auto head = writer.prepared_head();
          const auto before = take(control.snapshot());
          for (int asked = 0; asked != 2; ++asked) {
              auto refused = co_await drive.lifecycle(
                writer.rotate(second.boundary, byte_count{8_KiB}, work));
              require(
                !refused && refused.error().code() == errc::resource_exhausted,
                "a rotation past the retained limit was not refused");
              require(
                !writer.rotation_pending() && !writer.successor_head()
                  && !writer.failure().failed()
                  && writer.prepared_head() == head
                  && take(control.snapshot()).generation == before.generation
                  && writer.retention() == wal_retention{2, 2}
                  && writer.statistics().rotations == 1,
                "a refused rotation prepared a file, took an ID or latched");
          }
          require(
            writer.progress()->write_complete == second.boundary.cursor(),
            "the refusal disturbed the group already written");
          // The writer names the two files it made the head, oldest first.
          require(
            writer.retained().files() == 2
              && writer.retained().newest()
                   == writer.progress()->reserved.incarnation()
              && writer.retained().oldest() != writer.retained().newest(),
            "the files the shard holds are not named as they became the "
            "head");
          // More removals than it holds besides the head never drop the head.
          for (std::uint32_t i = 0; i != 5; ++i)
              writer.release_oldest();
          require(
            writer.retention() == wal_retention{1, 2}
              && writer.retained().oldest()
                   == writer.progress()->reserved.incarnation(),
            "a reported removal did not leave exactly the head");
          take(
            co_await drive.lifecycle(
              writer.rotate(second.boundary, byte_count{8_KiB}, work)));
          require(
            writer.retention() == wal_retention{2, 2}
              && writer.statistics().rotations == 2
              && writer.prepared_head() != head,
            "the rotation did not go through once a file was removed");
      });
}

// At the defaults a head's successor is made while it is the head: a fresh
// file zero-written whole under a temporary name, its incarnation allocated
// and its handles open. The rotation that takes it writes its header
// through the synchronized handle, names it and syncs its directory; with
// every group a synchronized write the rotation flushes nothing, and the
// successor holds its exact header and zeros to its capacity. Each
// rotation takes the one made ahead, and the next is made at once.
template<typename Backend, typename Owner, typename Driver>
seastar::future<> ready_successor(
  Backend& files,
  Owner& ownership,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    const auto config = wal_writer_contract::with_defaults();
    co_await append::with_writer(
      files,
      ownership,
      spec,
      budget,
      drive,
      config,
      [&](auto& writer, auto& work) -> seastar::future<> {
          const auto paths = take(local_paths::make(spec.root));
          for (std::uint64_t rotated = 1; rotated <= 2; ++rotated) {
              require(
                co_await drive.lifecycle(writer.successor_made()),
                "no successor was made ahead of its rotation");
              auto group = co_await submit_one(
                writer, budget, spec.owner.cluster(), work);
              auto done = co_await drive.lifecycle(std::move(group.written));
              take(done.failure.outcome());
              take(
                co_await drive.lifecycle(
                  writer.rotate(group.boundary, byte_count{8_KiB}, work)));
              const auto stats = writer.statistics();
              require(
                stats.rotations == rotated && stats.ahead_rotations == rotated
                  && stats.flush_calls == 0 && stats.spares_taken == 0,
                "a rotation did not take the successor made ahead, or "
                "flushed");
              const auto next = *writer.prepared_head();
              const auto header = wal_writer_contract::header_bytes(
                spec, 0, next.incarnation, group.boundary.cursor());
              const auto stored = co_await store_contract::read_all_bytes(
                files, take(paths.wal(0, next.incarnation)), drive);
              require(
                stored.size() == config.capacity_bytes.value()
                  && stored.substr(0, header.size()) == header
                  && stored.find_first_not_of('\0', header.size())
                       == std::string::npos
                  && writer.progress()->reserved.position().value()
                       == header.size(),
                "the successor made ahead is not its header and zeros");
          }
      });
}

// A successor whose making ahead fails is let go, and the rotation makes
// its successor itself, as with none made ahead: a file holding its exact
// header and zeros, which the next group is written to. `ready(spec)` is
// asked once the making ahead is over, and says whether it was set to fail.
template<typename Backend, typename Owner, typename Driver, typename Ready>
seastar::future<> failed_body_falls_back(
  Backend& files,
  Owner& ownership,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Ready ready) {
    const auto config = wal_writer_contract::with_defaults();
    co_await append::with_writer(
      files,
      ownership,
      spec,
      budget,
      drive,
      config,
      [&](auto& writer, auto& work) -> seastar::future<> {
          const auto made = co_await drive.lifecycle(writer.successor_made());
          const bool armed = ready(spec);
          require(
            made != armed,
            "a successor whose making failed was taken as made, or one with "
            "nothing failing was not made");
          auto group = co_await submit_one(
            writer, budget, spec.owner.cluster(), work);
          take((co_await drive.lifecycle(std::move(group.written)))
                 .failure.outcome());
          take(
            co_await drive.lifecycle(
              writer.rotate(group.boundary, byte_count{8_KiB}, work)));
          const auto stats = writer.statistics();
          const auto next = *writer.prepared_head();
          const auto header = wal_writer_contract::header_bytes(
            spec, 0, next.incarnation, group.boundary.cursor());
          const auto stored = co_await store_contract::read_all_bytes(
            files,
            take(take(local_paths::make(spec.root)).wal(0, next.incarnation)),
            drive);
          require(
            stats.rotations == 1 && stats.ahead_rotations == (armed ? 0U : 1U)
              && !writer.failure().failed()
              && stored.size() == config.capacity_bytes.value()
              && stored.substr(0, header.size()) == header
              && stored.find_first_not_of('\0', header.size())
                   == std::string::npos,
            "a rotation after a failed making did not make its successor "
            "itself");
          auto after = co_await submit_one(
            writer, budget, spec.owner.cluster(), work);
          take((co_await drive.lifecycle(std::move(after.written)))
                 .failure.outcome());
          take((co_await drive.lifecycle(writer.barrier(after.boundary)))
                 .failure.outcome());
      });
}

// A file the checkpoint reclaims is kept as a spare while the WAL's spares
// have room, and unlinked otherwise; the next file is made of the spare.
// The spare is the reclaimed file under a new name, its bytes unchanged,
// and the file made of it is that file again: its header written over the
// old one and everything after it the old file's, never read, with its
// groups synchronized writes over blocks written already.
template<typename Backend, typename Owner, typename Driver>
seastar::future<> recycled_files(
  Backend& files,
  Owner& ownership,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    auto config = wal_writer_contract::with_defaults();
    // Each successor is made at its rotation, which takes the spare.
    config.prepare_ahead = false;
    config.spares = 1;
    co_await append::with_writer(
      files,
      ownership,
      spec,
      budget,
      drive,
      config,
      [&](auto& writer, auto& work) -> seastar::future<> {
          using writer_type = std::remove_reference_t<decltype(writer)>;
          // What reclaims the WAL holds of its writer, as the append owner
          // gives it.
          struct reclaiming final {
              writer_type* writer;
              [[nodiscard]] const storage::retained_wal&
              retained_wal() const noexcept {
                  return writer->retained();
              }
              void wal_released() noexcept { writer->release_oldest(); }
              [[nodiscard]] auto& wal_spares() noexcept {
                  return writer->spares();
              }
          } holder{&writer};
          const auto paths = take(local_paths::make(spec.root));
          const auto write_one = [&]() -> seastar::future<wal_submission> {
              auto group = co_await submit_one(
                writer, budget, spec.owner.cluster(), work);
              auto done = co_await drive.lifecycle(std::move(group.written));
              take(done.failure.outcome());
              co_return group;
          };
          std::vector<model::wal_incarnation_id> made{
            writer.prepared_head()->incarnation};
          for (int i = 0; i != 2; ++i) {
              const auto group = co_await write_one();
              take(
                co_await drive.lifecycle(
                  writer.rotate(group.boundary, byte_count{8_KiB}, work)));
              made.push_back(writer.prepared_head()->incarnation);
          }
          const auto oldest = take(paths.wal(0, made[0]));
          const auto second = take(paths.wal(0, made[1]));
          const auto old_bytes = co_await store_contract::read_all_bytes(
            files, oldest, drive);
          // Both files before the head lie below a cutoff at its start.
          const auto reclaimed = co_await drive.lifecycle(reclaim_wal_prefix(
            files, spec, 0, writer.progress()->reserved, holder));
          const auto spare = take(paths.spare(0, made[0]));
          require(
            !reclaimed.failed && reclaimed.removed == 2
              && reclaimed.recycled == 1 && reclaimed.owed == 0
              && writer.spares().size() == 1 && writer.retained().files() == 1
              && !take(co_await drive.lifecycle(files.exists(oldest)))
              && !take(co_await drive.lifecycle(files.exists(second)))
              && (co_await store_contract::read_all_bytes(files, spare, drive))
                   == old_bytes,
            "a reclaimed file was not kept as a spare below the bound, or "
            "was kept above it");
          // The next file is made of the spare.
          const auto group = co_await write_one();
          take(
            co_await drive.lifecycle(
              writer.rotate(group.boundary, byte_count{8_KiB}, work)));
          const auto next = *writer.prepared_head();
          const auto header = wal_writer_contract::header_bytes(
            spec, 0, next.incarnation, group.boundary.cursor());
          const auto remade = co_await store_contract::read_all_bytes(
            files, take(paths.wal(0, next.incarnation)), drive);
          require(
            writer.statistics().spares_taken == 1 && writer.spares().size() == 0
              && !take(co_await drive.lifecycle(files.exists(spare)))
              && remade.size() == old_bytes.size()
              && remade.substr(0, header.size()) == header
              && remade.substr(header.size())
                   == old_bytes.substr(header.size()),
            "the next file was not the spare with its own header over the old "
            "one");
          const auto synchronized = writer.statistics().synchronized_writes;
          const auto flushes = writer.statistics().flush_calls;
          const auto last = co_await write_one();
          auto durable = co_await drive.lifecycle(
            writer.barrier(last.boundary));
          take(durable.failure.outcome());
          require(
            writer.statistics().synchronized_writes == synchronized + 1
              && writer.statistics().flush_calls == flushes,
            "a group in a file made of a spare was not a synchronized write");
      });
}
} // namespace kwaque::storage::testing::wal_rotation_contract
