#pragma once

#include "src/storage/tests/wal_rotation_contract.h"

#include <seastar/core/semaphore.hh>

#include <functional>

namespace kwaque::storage::testing::wal_lifecycle_contract {
using store_contract::require;
using store_contract::take;
namespace append = wal_append_contract;
namespace rotation = wal_rotation_contract;

template<typename Backend, typename Owner, typename Driver>
seastar::future<> inventory(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    co_await append::with_writer(
      files,
      owner,
      spec,
      budget,
      drive,
      wal_writer_contract::configuration(),
      [&](auto& writer, auto& control, auto& ids, auto& work)
        -> seastar::future<> {
          require(
            !writer.inventory_snapshot(),
            "live writer supplied a recovery handoff");
          const auto first_head = *writer.prepared_head();
          auto submitted = co_await rotation::submit_one(
            writer, budget, spec.owner.cluster(), work);
          const auto cut = submitted.boundary;
          auto done = co_await drive.lifecycle(std::move(submitted.written));
          take(done.failure.outcome());
          static_cast<void>(
            take(co_await drive.lifecycle(ids.allocate_wal(work))));
          take(
            co_await drive.lifecycle(
              writer.rotate(cut, byte_count{8192}, work)));
          const auto head = *writer.prepared_head();
          auto head_write = co_await rotation::submit_one(
            writer, budget, spec.owner.cluster(), work);
          auto head_done = co_await drive.lifecycle(
            std::move(head_write.written));
          take(head_done.failure.outcome());
          const auto extra = take(
            co_await drive.lifecycle(ids.allocate_wal(work)));
          require(
            head.incarnation.canonical_less(extra),
            "fixture lacks higher unreferenced header");
          const auto paths = take(local_paths::make(spec.root));
          const auto extra_path = take(paths.wal(0, extra));
          const auto extra_bytes = wal_writer_contract::header_bytes(
            spec, 0, extra);
          take(
            co_await drive.lifecycle(files.create_directories(take(
              runtime::file_path::make(extra_path.value().substr(
                0, extra_path.value().rfind('/')))))));
          co_await store_contract::write_bytes(
            files, extra_path, extra_bytes, drive);
          const auto unknown_path = take(local_child_path(
            spec.root, take(runtime::file_name::make(".unrecognized"))));
          co_await store_contract::write_bytes(
            files, unknown_path, "preserve", drive);
          take(co_await drive.lifecycle(writer.close()));
          const auto old_path = take(paths.wal(0, first_head.incarnation));
          // Header inventory intentionally does not validate PREPARE bytes.
          // Keep descriptor identity and extent while making the tail junk.
          co_await store_contract::write_bytes(
            files,
            old_path,
            wal_writer_contract::header_bytes(spec, 0, first_head.incarnation)
              + std::string(8192, '?'),
            drive);
          const auto anchor = take(writer.inventory_snapshot());
          require(
            anchor.control.fields.wal_head == head,
            "handoff selected a high-water filename");
          unsigned chain_count = 0, heads = 0, extras = 0, unknown = 0;
          auto chain_visit = [&](const local_wal_chain_entry& entry) {
              const auto& descriptor = std::get<local_wal_descriptor>(
                entry.record.value.payload());
              if (chain_count++ == 0)
                  require(
                    entry.head_digest_pinned
                      && descriptor.incarnation == head.incarnation
                      && !entry.sealed_end
                      && entry.record.value.encoded_bytes().value()
                           == descriptor.data_start.value()
                      && entry.record.file_bytes
                           > descriptor.data_start.value(),
                    "chain did not start at the selected SHA pin");
              else
                  require(
                    !entry.head_digest_pinned
                      && descriptor.incarnation == first_head.incarnation
                      && entry.sealed_end == cut.cursor().position(),
                    "predecessor inventory changed identity or certified its "
                    "bytes");
              return seastar::make_ready_future<runtime::result<bool>>(true);
          };
          auto names = [&](
                         const local_discovered_record& entry,
                         local_cleanup_observation cleanup) {
              if (
                entry.entry.wal == head.incarnation
                && entry.entry.kind == local_entry_kind::wal) {
                  ++heads;
                  require(
                    entry.decoded
                      && cleanup.classification
                           == local_cleanup_class::referenced,
                    "selected header lost its independent namespace pin");
              }
              if (
                entry.entry.wal == extra
                && entry.entry.kind == local_entry_kind::wal) {
                  ++extras;
                  require(
                    !entry.decoded && entry.claims && cleanup.debt
                      && cleanup.classification
                           == local_cleanup_class::recovery_required,
                    "unreferenced header became active or disposable");
              }
              if (entry.entry.kind == local_entry_kind::unknown) {
                  ++unknown;
                  require(
                    cleanup.classification == local_cleanup_class::unsupported,
                    "unknown entry was silently accepted");
              }
              return seastar::make_ready_future<runtime::result<bool>>(true);
          };
          auto inspect = [&](
                           auto chain,
                           auto visit,
                           std::optional<local_wal_cursor> cutoff = {},
                           local_discovery_limits bounds = {}) {
              return inspect_wal_inventory(
                files,
                owner,
                spec,
                anchor,
                cutoff,
                wal_inventory_intent::require_head,
                budget,
                store_contract::limits(),
                work,
                std::move(chain),
                std::move(visit),
                bounds);
          };
          auto found = take(
            co_await drive.lifecycle(inspect(chain_visit, names)));
          require(
            found.chain.complete && found.names.complete && chain_count == 2
              && heads == 1 && extras == 1 && unknown == 1,
            "inventory elected another head, skipped records or ignored the ID "
            "gap");
          auto yes_chain = [](const auto&) {
              return seastar::make_ready_future<runtime::result<bool>>(true);
          };
          auto yes_name = [](const auto&, auto) {
              return seastar::make_ready_future<runtime::result<bool>>(true);
          };
          seastar::abort_source cancel;
          codec::cooperative_work cancelled{work.policy(), cancel};
          cancel.request_abort();
          unsigned cancelled_visits = 0;
          auto cancelled_visit = [&](const auto&) {
              ++cancelled_visits;
              return seastar::make_ready_future<runtime::result<bool>>(true);
          };
          auto aborted = co_await drive.lifecycle(inspect_wal_inventory(
            files,
            owner,
            spec,
            anchor,
            {},
            wal_inventory_intent::require_head,
            budget,
            store_contract::limits(),
            cancelled,
            cancelled_visit,
            yes_name));
          require(
            !aborted && aborted.error().code() == errc::aborted
              && cancelled_visits == 0,
            "cancelled inventory issued observations");
          auto stop = [](const auto&) {
              return seastar::make_ready_future<runtime::result<bool>>(false);
          };
          auto partial = take(
            co_await drive.lifecycle(inspect(stop, yes_name)));
          require(
            !partial.chain.complete && partial.chain.visited == 1
              && partial.names.visited == 0,
            "stopped chain visitor caused an unbounded follow-on inventory");
          auto limited = co_await drive.lifecycle(
            inspect(yes_chain, yes_name, {}, {1000, 1}));
          require(
            !limited && limited.error().code() == errc::resource_exhausted,
            "chain limit was treated as a complete inventory");
          // A current-state edit invalidates the older anchor even if its WAL
          // head is unchanged. No visitor may infer a new pin from the
          // namespace.
          auto updated = co_await drive.lifecycle(control.update(
            [](local_shard_control& fields) -> runtime::result<void> {
                fields.deletion_high
                  = fields.deletion_high.checked_advance(1).value();
                return {};
            },
            work));
          take(updated.failure.outcome());
          auto stale = co_await drive.lifecycle(inspect(yes_chain, yes_name));
          require(
            !stale && stale.error().code() == errc::wrong_context,
            "stale control snapshot remained authoritative");
          const auto current = take(writer.inventory_snapshot());
          take(co_await drive.lifecycle(files.remove_file(old_path)));
          auto missing = co_await drive.lifecycle(inspect_wal_inventory(
            files,
            owner,
            spec,
            current,
            {},
            wal_inventory_intent::require_head,
            budget,
            store_contract::limits(),
            work,
            yes_chain,
            yes_name));
          require(
            !missing && missing.error().code() == errc::not_found,
            "higher unreferenced header hid a missing predecessor");
          // Test-supplied cutoff exercises only membership/bounds, not
          // checkpoint proof. The read-only adapter cannot authorize
          // reclamation from it.
          auto cut_inventory = take(
            co_await drive.lifecycle(inspect_wal_inventory(
              files,
              owner,
              spec,
              current,
              take(
                local_wal_cursor::make(
                  head.incarnation, runtime::file_position{8192})),
              wal_inventory_intent::require_head,
              budget,
              store_contract::limits(),
              work,
              yes_chain,
              yes_name)));
          require(
            cut_inventory.chain.complete && cut_inventory.chain.visited == 1,
            "independent cutoff did not bound the chain");
          take(
            co_await drive.lifecycle(
              files.remove_file(take(paths.wal(0, head.incarnation)))));
          auto missing_head = co_await drive.lifecycle(inspect_wal_inventory(
            files,
            owner,
            spec,
            current,
            {},
            wal_inventory_intent::require_head,
            budget,
            store_contract::limits(),
            work,
            yes_chain,
            yes_name));
          require(
            !missing_head && missing_head.error().code() == errc::not_found,
            "missing pin was repaired by selecting the highest file");
          require(
            (co_await store_contract::read_bytes(files, extra_path, drive))
                == extra_bytes
              && (co_await store_contract::read_bytes(
                   files, unknown_path, drive))
                   == "preserve",
            "read-only inventory deleted or rewrote unselected bytes");
      });
}

template<typename Backend, typename Owner, typename Driver, typename Stop>
seastar::future<> shutdown(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  seastar::abort_source& source,
  Stop stop,
  seastar::semaphore& workload_memory) {
    co_await append::with_writer(
      files,
      owner,
      spec,
      budget,
      drive,
      wal_writer_contract::configuration(),
      [&](auto& writer, auto& work) -> seastar::future<> {
          const std::array records{
            assigned_wire(false, 48), assigned_wire(true, 48)};
          auto rejected_input = co_await append::offer(
            budget, spec.owner.cluster(), work, records);
          auto one = co_await rotation::submit_one(
            writer, budget, spec.owner.cluster(), work);
          auto group = co_await append::offer(
            budget, spec.owner.cluster(), work, records);
          auto two = take(co_await writer.submit(std::move(group), work));
          const auto head = *writer.prepared_head();
          const auto cut = two.boundary;
          auto barrier = writer.barrier(one.boundary);
          std::array<std::optional<workload_reservation>, 32> pressure;
          for (auto& slot : pressure) {
              auto reserve = budget.try_reserve(byte_count{4096});
              if (!reserve) break;
              slot.emplace(std::move(*reserve));
          }
          auto memory = seastar::try_get_units(
            workload_memory, workload_memory.current());
          require(
            memory.has_value() && !budget.try_reserve(byte_count{1}),
            "workload pressure did not engage");
          stop();
          // Keep admission closed even as retiring writes return units. A
          // cleanup path that tries to reacquire ordinary capacity must fail.
          budget.close_admission();
          require(
            writer.admission_stopped(),
            "early stop did not close WAL admission");
          auto rejected = co_await writer.submit(
            std::move(rejected_input), work);
          require(
            !rejected && rejected.error().code() == errc::closed
              && writer.progress()->reserved == cut.cursor(),
            "stop accepted new WAL coordinates");
          auto closing = writer.close();
          const bool pending_close = !closing.available();
          auto overlapping = co_await writer.close();
          if (pending_close)
              require(
                !overlapping && overlapping.error().code() == errc::queue_full,
                "concurrent close allocated an unbounded waiter");
          else
              take(overlapping);
          take(co_await drive.lifecycle(std::move(closing)));
          auto one_done = co_await drive.lifecycle(std::move(one.written));
          auto two_done = co_await drive.lifecycle(std::move(two.written));
          auto flushed = co_await drive.lifecycle(std::move(barrier));
          take(one_done.failure.outcome());
          take(two_done.failure.outcome());
          take(flushed.failure.outcome());
          require(
            flushed.receipt
              && flushed.receipt->boundary().cursor() == one.boundary.cursor()
              && writer.progress()->durable == cut.cursor()
              && writer.progress()->write_complete == cut.cursor(),
            "shutdown skipped required work or enlarged an earlier barrier "
            "receipt");
          const auto stats = writer.statistics();
          take(co_await drive.lifecycle(writer.close()));
          require(
            writer.statistics().flush_calls == stats.flush_calls,
            "cached close repeated sync");
          memory.reset();
          for (auto& slot : pressure)
              slot.reset();
          auto expected = wal_writer_contract::header_bytes(
            spec, 0, head.incarnation);
          expected += append::prepare_bytes(
            assigned_wire(true, 48), head, spec.owner.cluster(), 8192);
          expected += append::prepare_bytes(
            records[0], head, spec.owner.cluster(), 16384);
          expected += append::prepare_bytes(
            records[1], head, spec.owner.cluster(), 24576);
          require(
            (co_await store_contract::read_bytes(
              files,
              take(take(local_paths::make(spec.root)).wal(0, head.incarnation)),
              drive))
              == expected,
            "drain changed independently specified record bytes");
      },
      false,
      {},
      &source);
}

template<typename Backend, typename Owner, typename Driver>
seastar::future<> completion_reentry(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    auto config = wal_writer_contract::configuration();
    config.maximum_descriptors = 1;
    co_await append::with_writer(
      files,
      owner,
      spec,
      budget,
      drive,
      config,
      [&](auto& writer, auto& work) -> seastar::future<> {
          const std::array records{assigned_wire()};
          auto next = co_await append::offer(
            budget, spec.owner.cluster(), work, records);
          auto first = co_await rotation::submit_one(
            writer, budget, spec.owner.cluster(), work);
          const auto cut = first.boundary;
          // Keep the coroutine callable in this joined frame. A temporary
          // coroutine lambda passed to then() would not own its captures.
          auto completed =
            [&](::kwaque::storage::detail::wal_write_completion done)
            -> seastar::future<> {
              take(done.failure.outcome());
              require(
                writer.progress()->write_complete == cut.cursor(),
                "write observer ran before prefix/slot installation");
              auto second = take(co_await writer.submit(std::move(next), work));
              const auto final_cut = second.boundary;
              auto written = co_await std::move(second.written);
              take(written.failure.outcome());
              take(co_await writer.close());
              require(
                writer.progress()->durable == final_cut.cursor(),
                "completion-triggered close skipped its final barrier");
          };
          co_await drive.lifecycle(
            std::move(first.written).then(std::ref(completed)));
      });
}

template<typename Owner>
struct failing_directory_owner final {
    Owner& owner;
    std::exception_ptr exception;
    unsigned remaining{0};
    seastar::future<runtime::result<void>>
    validate(const local_device_spec& spec) {
        if (remaining && --remaining == 0)
            return seastar::make_exception_future<runtime::result<void>>(
              exception);
        return owner.validate(spec);
    }
};
template<typename Backend, typename Owner, typename Driver>
seastar::future<> exceptional_rotation_close(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    const auto exception = std::make_exception_ptr(std::bad_alloc{});
    failing_directory_owner<Owner> checked{owner, exception};
    co_await append::with_writer(
      files,
      checked,
      spec,
      budget,
      drive,
      wal_writer_contract::configuration(),
      [&](auto& writer, auto& work) -> seastar::future<> {
          auto one = co_await rotation::submit_one(
            writer, budget, spec.owner.cluster(), work);
          auto done = co_await drive.lifecycle(std::move(one.written));
          take(done.failure.outcome());
          // Cached allocator range: validation #3 is the current head edit,
          // after durable header preparation and predecessor barrier/checked
          // close.
          checked.remaining = 3;
          std::exception_ptr observed;
          try {
              static_cast<void>(co_await drive.lifecycle(
                writer.rotate(one.boundary, byte_count{8192}, work)));
          } catch (...) {
              observed = std::current_exception();
          }
          require(
            observed == exception && writer.successor_head()
              && writer.failure().exception() == exception,
            "rotation swallowed its exceptional dependency failure");
          for (unsigned attempt = 0; attempt != 2; ++attempt) {
              observed = {};
              try {
                  static_cast<void>(co_await drive.lifecycle(writer.close()));
              } catch (...) {
                  observed = std::current_exception();
              }
              require(
                observed == exception,
                "joined/repeated close lost the first exception");
          }
          require(
            writer.progress()->durable == one.boundary.cursor(),
            "exception erased the already completed old-tail barrier");
      },
      false,
      exception);
}
} // namespace kwaque::storage::testing::wal_lifecycle_contract
