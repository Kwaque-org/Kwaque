#pragma once

#include "src/base/units.h"
#include "src/storage/tests/wal_writer_contract.h"

namespace kwaque::storage::testing::wal_append_contract {
using store_contract::require;
using store_contract::take;

template<typename Backend, typename Owner, typename Driver, typename Func>
seastar::future<> with_writer(
  Backend& files,
  Owner& ownership,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  wal_writer_config config,
  Func body,
  bool expected_close_failure = false,
  std::exception_ptr expected_close_exception = {},
  seastar::abort_source* shutdown = nullptr) {
    using writer_type = wal_writer<Backend, Owner>;
    using control_type = typename writer_type::control_type;
    using allocator_type = typename writer_type::allocator_type;
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    std::unique_ptr<control_type> control;
    std::unique_ptr<allocator_type> ids;
    std::unique_ptr<writer_type> writer;
    runtime::first_failure failed;
    try {
        co_await installation_contract::bootstrap(
          files, ownership, spec, budget, work, drive);
        control = take(
          co_await drive.lifecycle(
            control_type::open(
              files,
              ownership,
              spec,
              0,
              false,
              budget,
              store_contract::limits(),
              work)));
        ids = take(allocator_type::make(*control, budget, 4));
        writer = take(
          writer_type::make(
            *control,
            *ids,
            budget,
            config,
            wal_start_intent::known_unactivated));
        if (shutdown) take(writer->bind_shutdown(*shutdown));
        take(co_await drive.lifecycle(writer->bootstrap(work)));
        if constexpr (requires { body(*writer, *control, *ids, work); })
            co_await body(*writer, *control, *ids, work);
        else
            co_await body(*writer, work);
    } catch (...) {
        failed.observe(std::current_exception());
    }
    if (writer) {
        try {
            auto closed = co_await drive.lifecycle(writer->close());
            if (expected_close_exception)
                failed.observe(runtime::operation_error{errc::wrong_context});
            else if (expected_close_failure)
                require(!closed, "failed writer closed successfully");
            else
                failed.observe(closed);
        } catch (...) {
            if (std::current_exception() != expected_close_exception)
                failed.observe(std::current_exception());
        }
        writer.reset();
    }
    if (ids) {
        take(co_await drive.lifecycle(ids->close()));
        ids.reset();
    }
    if (control) {
        take(co_await drive.lifecycle(control->close()));
        control.reset();
    }
    take(failed.outcome());
}

inline seastar::future<wal_group> offer(
  workload_budget& budget,
  model::cluster_id cluster,
  codec::cooperative_work& work,
  std::span<const std::string> records,
  std::size_t width = 67) {
    auto group = take(
      wal_group::make(
        budget,
        static_cast<std::uint32_t>(std::max<std::size_t>(1, records.size()))));
    for (const auto& record : records) {
        auto child = co_await wal_writer_contract::offer(
          budget, record, work, width);
        const auto expected = wal_writer_contract::child_context(cluster);
        take(
          group.append(std::move(child), expected, expected.target_data_start));
    }
    co_return std::move(group);
}
// One PREPARE at `position`. It states `certified`, where the file's last
// completed barrier ended when the writer encoded it (the data start, 8192,
// before one has), and pads to the next boundary when it ends its write.
inline std::string prepare_bytes(
  std::string child,
  local_wal_head head,
  model::cluster_id cluster,
  std::uint64_t position,
  std::uint64_t certified = 8192,
  bool ends = true) {
    const auto e = wal_writer_contract::child_context(cluster);
    return wal_wire(
      std::move(child),
      {wal_write_context::make(
         head.incarnation, alignment(8192), runtime::file_position{position})
         .value(),
       runtime::file_position{8192},
       e.target,
       e.target_data_start,
       e.routing_epoch,
       e.batch,
       e.profile,
       e.target_profile},
      32,
      wal_prepare_stamps{
        ends,
        wal_writer_contract::offered_time,
        e.target_data_start,
        runtime::file_position{certified}});
}
// One group's PREPAREs back to back from `position`; the last ends the write
// and pads it to the next boundary. Each states `certified`.
inline std::string group_bytes(
  std::span<const std::string> children,
  local_wal_head head,
  model::cluster_id cluster,
  std::uint64_t position,
  std::uint64_t certified = 8192) {
    std::string output;
    for (std::size_t i = 0; i < children.size(); ++i)
        output += prepare_bytes(
          children[i],
          head,
          cluster,
          position + output.size(),
          certified,
          i + 1 == children.size());
    return output;
}

template<typename Backend, typename Owner, typename Driver>
seastar::future<> exercise(
  Backend& files,
  Owner& ownership,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    co_await with_writer(
      files,
      ownership,
      spec,
      budget,
      drive,
      wal_writer_contract::configuration(),
      [&](auto& writer, auto& work) -> seastar::future<> {
          const auto initial = take(writer.capture());
          auto empty_barrier = co_await drive.lifecycle(
            writer.barrier(initial));
          take(empty_barrier.failure.outcome());
          require(
            empty_barrier.receipt.has_value(),
            "published empty cut lacks receipt");
          require(
            writer.statistics().flush_calls == 0,
            "empty published cut flushed again");
          const auto before = writer.progress()->reserved;
          {
              auto empty = take(wal_group::make(budget, 1));
              auto rejected = co_await writer.submit(std::move(empty), work);
              require(
                !rejected && writer.progress()->reserved == before,
                "empty group reserved or wrote");
          }
          {
              auto group = take(wal_group::make(budget, 2));
              for (unsigned i = 0; i != 2; ++i) {
                  auto child = co_await wal_writer_contract::offer(
                    budget, assigned_wire(), work);
                  auto expected = wal_writer_contract::child_context(
                    spec.owner.cluster());
                  if (i != 0)
                      expected.routing_epoch
                        = model::range_routing_epoch::make(9).value();
                  take(group.append(
                    std::move(child), expected, expected.target_data_start));
              }
              auto rejected = co_await writer.submit(std::move(group), work);
              require(
                !rejected && writer.progress()->reserved == before
                  && writer.statistics().write_calls == 0,
                "second-member rejection accepted a prefix");
          }
          {
              const std::array records{assigned_wire()};
              auto group = co_await offer(
                budget, spec.owner.cluster(), work, records);
              auto cfg = codec::limits::defaults().config();
              cfg.max_buffer_fragments = item_count{4};
              seastar::abort_source stop;
              codec::cooperative_work narrow{
                codec::limits::make(cfg).value(), stop};
              auto rejected = co_await writer.submit(std::move(group), narrow);
              require(
                !rejected && writer.progress()->reserved == before,
                "outer fragments escaped preacceptance validation");
          }
          const std::array records{
            assigned_wire(false, 48), assigned_wire(true, 48, true)};
          {
              const std::array sparse{assigned_wire(true, 48, true)};
              auto offered = co_await offer(
                budget, spec.owner.cluster(), work, sparse);
              auto cfg = codec::limits::defaults().config();
              cfg.max_original_records = item_count{1};
              seastar::abort_source stop;
              codec::cooperative_work narrow{
                codec::limits::make(cfg).value(), stop};
              auto rejected = co_await writer.submit(
                std::move(offered), narrow);
              require(
                !rejected && writer.progress()->reserved == before,
                "narrow semantic policy changed an accepted cursor");
          }
          auto group = co_await offer(
            budget, spec.owner.cluster(), work, records);
          auto accepted = take(co_await writer.submit(std::move(group), work));
          // An entered group is transferred to the writer.
          // NOLINTNEXTLINE(bugprone-use-after-move)
          require(group.size() == 0, "entered group did not transfer");
          // Both members lie back to back from 8192; the second pads the
          // write to the next boundary.
          const auto cut = accepted.boundary;
          require(
            cut.cursor().position().value() == 16384,
            "whole-group extent is wrong");
          require(
            writer.progress()->durable == before,
            "reservation became durability");
          auto written = co_await drive.lifecycle(std::move(accepted.written));
          take(written.failure.outcome());
          require(
            written.written == byte_count{8_KiB},
            "group completion byte count changed");
          require(
            writer.progress()->write_complete == cut.cursor()
              && writer.progress()->durable == before,
            "write completion became durability");
          const auto head = *writer.prepared_head();
          const auto path = take(
            take(local_paths::make(spec.root)).wal(0, head.incarnation));
          auto expected = wal_writer_contract::header_bytes(
            spec, 0, head.incarnation);
          expected += group_bytes(records, head, spec.owner.cluster(), 8192);
          require(
            (co_await store_contract::read_bytes(files, path, drive))
              == expected,
            "group writes differ from independent envelopes");
          auto durable = co_await drive.lifecycle(writer.barrier(cut));
          take(durable.failure.outcome());
          require(
            durable.receipt
              && durable.receipt->boundary().cursor() == cut.cursor(),
            "wrong durable cut");
          require(
            writer.progress()->durable == cut.cursor()
              && writer.statistics().flush_calls == 1,
            "barrier failed to publish exactly its cut");
          auto repeated = co_await drive.lifecycle(writer.barrier(cut));
          take(repeated.failure.outcome());
          require(
            writer.statistics().flush_calls == 1,
            "successful covering cut was not reused");
          const std::array last{assigned_wire(true, 48)};
          auto tail = co_await offer(budget, spec.owner.cluster(), work, last);
          {
              auto detached = take(
                co_await writer.submit(std::move(tail), work));
              require(
                detached.boundary.cursor().position().value() == 24576,
                "tail was not reserved");
          }
          take(co_await drive.lifecycle(writer.close()));
          require(
            writer.progress()->reserved == writer.progress()->write_complete
              && writer.progress()->reserved == writer.progress()->durable
              && writer.statistics().flush_calls == 2,
            "close did not join and cover detached accepted work");
          // Encoded after the barrier over the first group: it states that
          // barrier's end.
          expected += prepare_bytes(
            last[0], head, spec.owner.cluster(), 16384, 16384);
          require(
            (co_await store_contract::read_bytes(files, path, drive))
              == expected,
            "detachment or close changed the accepted bytes");
      });
}

// A zero-written file: every small group is a synchronized write, durable at
// completion, so neither barriers nor close flush. The range keeps moving
// ahead of the reservations, frames stay exact and the file ends in zeros.
template<typename Backend, typename Owner, typename Driver>
seastar::future<> zero_written(
  Backend& files,
  Owner& ownership,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    constexpr std::uint64_t window = 32_KiB;
    constexpr unsigned groups = 6;
    auto config = wal_writer_contract::configuration();
    config.preallocation_bytes = byte_count{window};
    config.preallocation_extension_bytes = byte_count{window};
    config.synchronous_write_bytes = byte_count{16_KiB};
    std::optional<runtime::file_path> path;
    std::string expected;
    std::uint64_t start = 0;
    co_await with_writer(
      files,
      ownership,
      spec,
      budget,
      drive,
      config,
      [&](auto& writer, auto& work) -> seastar::future<> {
          const auto head = *writer.prepared_head();
          path = take(
            take(local_paths::make(spec.root)).wal(0, head.incarnation));
          expected = wal_writer_contract::header_bytes(
            spec, 0, head.incarnation);
          start = writer.progress()->reserved.position().value();
          const auto zeroed = co_await store_contract::read_all_bytes(
            files, *path, drive);
          require(
            zeroed.size() == start + window
              && zeroed.find_first_not_of('\0', start) == std::string::npos,
            "preparation did not zero-write the WAL window");
          std::optional<wal_captured_boundary> last;
          for (unsigned i = 0; i != groups; ++i) {
              const std::array records{assigned_wire()};
              const auto position = writer.progress()->reserved.position();
              auto group = co_await offer(
                budget, spec.owner.cluster(), work, records);
              auto accepted = take(
                co_await writer.submit(std::move(group), work));
              auto written = co_await drive.lifecycle(
                std::move(accepted.written));
              take(written.failure.outcome());
              expected += prepare_bytes(
                records[0], head, spec.owner.cluster(), position.value());
              last = accepted.boundary;
          }
          const auto stats = writer.statistics();
          require(
            last->cursor().position().value() > start + window
              && stats.write_calls == groups
              && stats.synchronized_writes == groups && stats.extensions != 0
              && stats.flush_calls == 0,
            "a group past the window took an ordinary write");
          auto durable = co_await drive.lifecycle(writer.barrier(*last));
          take(durable.failure.outcome());
          require(
            durable.receipt && writer.progress()->durable == last->cursor()
              && writer.statistics().flush_calls == 0,
            "a barrier over synchronized writes flushed");
          const auto extent = take(
            co_await drive.lifecycle(writer.inspect_extent()));
          require(
            extent.observed_eof.value() > last->cursor().position().value(),
            "the zero-written range did not stay ahead of the writes");
      });
    const auto stored = co_await store_contract::read_all_bytes(
      files, *path, drive);
    require(
      stored.size() > expected.size()
        && stored.substr(0, expected.size()) == expected
        && stored.find_first_not_of('\0', expected.size()) == std::string::npos,
      "zero-written WAL frames are not exact or its tail is not zeros");
}

// At the production defaults a file is zero-written to its end when it is
// made, and its successor is made beside it: every group of up to 1 MiB is
// a synchronized write, durable at completion, so neither a barrier nor
// close flushes, and the file ends in zeros at its capacity.
template<typename Backend, typename Owner, typename Driver>
seastar::future<> zero_written_by_default(
  Backend& files,
  Owner& ownership,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    constexpr unsigned groups = 6;
    const auto config = wal_writer_contract::with_defaults();
    const auto capacity = config.capacity_bytes.value();
    std::optional<runtime::file_path> path;
    std::string expected;
    co_await with_writer(
      files,
      ownership,
      spec,
      budget,
      drive,
      config,
      [&](auto& writer, auto& work) -> seastar::future<> {
          const auto head = *writer.prepared_head();
          path = take(
            take(local_paths::make(spec.root)).wal(0, head.incarnation));
          expected = wal_writer_contract::header_bytes(
            spec, 0, head.incarnation);
          const auto start = writer.progress()->reserved.position().value();
          const auto zeroed = co_await store_contract::read_all_bytes(
            files, *path, drive);
          require(
            zeroed.size() == capacity
              && zeroed.find_first_not_of('\0', start) == std::string::npos,
            "the default preparation did not zero-write the whole file");
          std::optional<wal_captured_boundary> last;
          for (unsigned i = 0; i != groups; ++i) {
              const std::array records{assigned_wire()};
              const auto position = writer.progress()->reserved.position();
              auto group = co_await offer(
                budget, spec.owner.cluster(), work, records);
              auto accepted = take(
                co_await writer.submit(std::move(group), work));
              auto written = co_await drive.lifecycle(
                std::move(accepted.written));
              take(written.failure.outcome());
              expected += prepare_bytes(
                records[0], head, spec.owner.cluster(), position.value());
              last = accepted.boundary;
          }
          const auto stats = writer.statistics();
          require(
            stats.write_calls == groups && stats.synchronized_writes == groups
              && stats.extensions == 0 && stats.flush_calls == 0,
            "a group inside the zero-written file took an ordinary write");
          auto durable = co_await drive.lifecycle(writer.barrier(*last));
          take(durable.failure.outcome());
          require(
            durable.receipt && writer.progress()->durable == last->cursor()
              && writer.statistics().flush_calls == 0,
            "a barrier over synchronized writes flushed");
          require(
            co_await drive.lifecycle(writer.successor_made()),
            "no successor was made ahead");
      });
    const auto stored = co_await store_contract::read_all_bytes(
      files, *path, drive);
    require(
      stored.size() == capacity && stored.substr(0, expected.size()) == expected
        && stored.find_first_not_of('\0', expected.size()) == std::string::npos,
      "zero-written WAL frames are not exact or its tail is not zeros");
}

// Groups of 1, 2, 3 and 64 PREPAREs whose unpadded end lands one byte
// short of a boundary, on it, and one byte past it. Each group's PREPAREs
// lie back to back from its aligned start, only the last sets ends_write
// and pads, and the next group begins on the boundary that pad reached. The
// file holds exactly the independent PREPAREs.
template<typename Backend, typename Owner, typename Driver>
seastar::future<> packed_groups(
  Backend& files,
  Owner& ownership,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    co_await with_writer(
      files,
      ownership,
      spec,
      budget,
      drive,
      wal_writer_contract::configuration(),
      [&](auto& writer, auto& work) -> seastar::future<> {
          constexpr std::uint64_t unit = 8192;
          const auto head = *writer.prepared_head();
          auto expected = wal_writer_contract::header_bytes(
            spec, 0, head.incarnation);
          for (const std::uint64_t members : {1U, 2U, 3U, 64U}) {
              // PREPAREs of 425 + value bytes for values of 121 to 8,191;
              // the fewest units their values can reach.
              const auto units = (members * 546 + unit - 1) / unit;
              for (const std::uint64_t end :
                   {units * unit - 1, units * unit, units * unit + 1}) {
                  const auto values = end - 425 * members;
                  std::vector<std::string> children;
                  for (std::uint64_t i = 0; i < members; ++i)
                      children.push_back(valued_child(
                        100,
                        values / members
                          + (i + 1 == members ? values % members : 0)));
                  const auto start = writer.progress()->reserved.position();
                  auto group = co_await offer(
                    budget, spec.owner.cluster(), work, children, 4096);
                  auto accepted = take(
                    co_await writer.submit(std::move(group), work));
                  const auto padded = (end + unit - 1) / unit * unit;
                  require(
                    accepted.boundary.cursor().position().value()
                      == start.value() + padded,
                    "a packed group did not end on the next boundary");
                  auto written = co_await drive.lifecycle(
                    std::move(accepted.written));
                  take(written.failure.outcome());
                  require(
                    written.written == byte_count{padded},
                    "a packed group's write was not its padded extent");
                  expected += group_bytes(
                    children, head, spec.owner.cluster(), start.value());
              }
          }
          const auto path = take(
            take(local_paths::make(spec.root)).wal(0, head.incarnation));
          require(
            (co_await store_contract::read_all_bytes(files, path, drive))
              == expected,
            "packed groups differ from independent PREPAREs");
      });
}

// Each PREPARE states where the last barrier of its file that had completed
// when its group's encoding began ended: the data start before any, never a
// cut still being made durable, never decreasing.
template<typename Backend, typename Owner, typename Driver>
seastar::future<> certified_stamps(
  Backend& files,
  Owner& ownership,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    co_await with_writer(
      files,
      ownership,
      spec,
      budget,
      drive,
      wal_writer_contract::configuration(),
      [&](auto& writer, auto& work) -> seastar::future<> {
          const auto head = *writer.prepared_head();
          auto expected = wal_writer_contract::header_bytes(
            spec, 0, head.incarnation);
          const std::array records{assigned_wire(), assigned_wire(true, 48)};
          // Submits one group and awaits its write; it states `certified`.
          const auto group = [&](std::uint64_t certified)
            -> seastar::future<wal_captured_boundary> {
              const auto start = writer.progress()->reserved.position();
              auto offered = co_await offer(
                budget, spec.owner.cluster(), work, records);
              auto accepted = take(
                co_await writer.submit(std::move(offered), work));
              take((co_await drive.lifecycle(std::move(accepted.written)))
                     .failure.outcome());
              expected += group_bytes(
                records, head, spec.owner.cluster(), start.value(), certified);
              co_return accepted.boundary;
          };
          const auto first = co_await group(8192);
          take((co_await drive.lifecycle(writer.barrier(first)))
                 .failure.outcome());
          const auto after = first.cursor().position().value();
          static_cast<void>(co_await group(after));
          // Nothing covers the second group yet: the third states the first.
          const auto third = co_await group(after);
          take((co_await drive.lifecycle(writer.barrier(third)))
                 .failure.outcome());
          static_cast<void>(co_await group(third.cursor().position().value()));
          const auto path = take(
            take(local_paths::make(spec.root)).wal(0, head.incarnation));
          require(
            (co_await store_contract::read_bytes(files, path, drive))
              == expected,
            "a PREPARE did not state the newest barrier completed before "
            "its group was encoded");
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
    config.capacity_bytes = byte_count{24_KiB};
    co_await with_writer(
      files,
      ownership,
      spec,
      budget,
      drive,
      config,
      [&](auto& writer, auto& work) -> seastar::future<> {
          // Members of 4,487 bytes (a 4,287-byte child): three pack into
          // 13,461 bytes and pad to exactly the 16 KiB left; four pad to
          // 24 KiB, past the capacity.
          const auto large = assigned_wire(false, 4096);
          const std::array too_many{large, large, large, large};
          auto group = co_await offer(
            budget, spec.owner.cluster(), work, too_many);
          auto rejected = co_await writer.submit(std::move(group), work);
          require(
            !rejected && writer.progress()->reserved.position().value() == 8192,
            "group straddled capacity or partially reserved");
          const std::array exact{large, large, large};
          auto exact_group = co_await offer(
            budget, spec.owner.cluster(), work, exact);
          auto accepted = take(
            co_await writer.submit(std::move(exact_group), work));
          auto done = co_await drive.lifecycle(std::move(accepted.written));
          take(done.failure.outcome());
          require(
            accepted.boundary.cursor().position().value() == 24576,
            "exactly full group rejected");
          const std::array next{assigned_wire()};
          auto tail = co_await offer(budget, spec.owner.cluster(), work, next);
          rejected = co_await writer.submit(std::move(tail), work);
          require(
            !rejected
              && writer.progress()->reserved == accepted.boundary.cursor(),
            "full incarnation reused coordinates or rotated implicitly");
      });
}
} // namespace kwaque::storage::testing::wal_append_contract
