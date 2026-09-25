#pragma once

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
  std::span<const std::string> records) {
    auto group = take(
      wal_group::make(
        budget,
        static_cast<std::uint32_t>(std::max<std::size_t>(1, records.size()))));
    for (const auto& record : records) {
        auto child = co_await wal_writer_contract::offer(budget, record, work);
        take(group.append(
          std::move(child), wal_writer_contract::child_context(cluster)));
    }
    co_return std::move(group);
}
inline std::string prepare_bytes(
  std::string child,
  local_wal_head head,
  model::cluster_id cluster,
  std::uint64_t position) {
    const auto e = wal_writer_contract::child_context(cluster);
    return wal_wire(
      std::move(child),
      {wal_write_context::make(
         head.incarnation, alignment(8192), runtime::file_position{position})
         .value(),
       e.target,
       e.target_data_start,
       e.routing_epoch,
       e.batch,
       e.profile,
       e.target_profile});
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
                  take(group.append(std::move(child), expected));
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
          require(group.size() == 0, "entered group did not transfer");
          const auto cut = accepted.boundary;
          require(
            cut.cursor().position().value() == 24576,
            "whole-group extent is wrong");
          require(
            writer.progress()->durable == before,
            "reservation became durability");
          auto written = co_await drive.lifecycle(std::move(accepted.written));
          take(written.failure.outcome());
          require(
            written.written == byte_count{16384},
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
          expected += prepare_bytes(
            records[0], head, spec.owner.cluster(), 8192);
          expected += prepare_bytes(
            records[1], head, spec.owner.cluster(), 16384);
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
                detached.boundary.cursor().position().value() == 32768,
                "tail was not reserved");
          }
          take(co_await drive.lifecycle(writer.close()));
          require(
            writer.progress()->reserved == writer.progress()->write_complete
              && writer.progress()->reserved == writer.progress()->durable
              && writer.statistics().flush_calls == 2,
            "close did not join and cover detached accepted work");
          expected += prepare_bytes(last[0], head, spec.owner.cluster(), 24576);
          require(
            (co_await store_contract::read_bytes(files, path, drive))
              == expected,
            "detachment or close changed the accepted bytes");
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
    config.capacity_bytes = byte_count{24576};
    co_await with_writer(
      files,
      ownership,
      spec,
      budget,
      drive,
      config,
      [&](auto& writer, auto& work) -> seastar::future<> {
          const std::array too_many{
            assigned_wire(), assigned_wire(), assigned_wire()};
          auto group = co_await offer(
            budget, spec.owner.cluster(), work, too_many);
          auto rejected = co_await writer.submit(std::move(group), work);
          require(
            !rejected && writer.progress()->reserved.position().value() == 8192,
            "group straddled capacity or partially reserved");
          const std::array exact{assigned_wire(), assigned_wire()};
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
