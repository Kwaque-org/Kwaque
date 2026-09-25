#pragma once

#include "src/storage/tests/wal_group_commit_contract.h"

namespace kwaque::storage::testing::wal_durability_contract {
using store_contract::require;
using store_contract::take;
using wal_group_commit_contract::slow_batch;
using wal_group_commit_contract::with_groups;

template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> completion(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    std::optional<wal_commit_result> retained;
    co_await wal_append_contract::with_writer(
      files,
      owner,
      spec,
      budget,
      drive,
      wal_writer_contract::configuration(),
      [&](auto& writer, auto& work) -> seastar::future<> {
          auto config = slow_batch();
          config.target_members = 2;
          config.outstanding_groups = 3;
          co_await with_groups(
            writer,
            budget,
            drive,
            config,
            [&](auto& groups) -> seastar::future<> {
                const std::array records{assigned_wire()};
                auto one = co_await wal_append_contract::offer(
                  budget, spec.owner.cluster(), work, records);
                auto two = co_await wal_append_contract::offer(
                  budget, spec.owner.cluster(), work, records);
                auto later = co_await wal_append_contract::offer(
                  budget, spec.owner.cluster(), work, records);
                auto first = take(
                  co_await groups.template submit<Clock>(
                    writer, std::move(one), work));
                auto second = take(
                  co_await groups.template submit<Clock>(
                    writer, std::move(two), work));
                auto observed = take(first.observe());
                auto duplicate = take(first.observe());
                auto second_observed = take(second.observe());
                {
                    auto dropped = take(second.observe());
                }
                require(
                  !observed.available() && !second_observed.available(),
                  "accepted/written state completed a durable observer");
                auto capture = groups.capture(Clock::now());
                require(
                  capture && capture->groups() == 2,
                  "missing two-group capture");
                const auto boundary = capture->boundary();
                take(groups.flush(writer, *capture));
                take(groups.flush(writer, *capture));
                auto result = co_await drive.lifecycle(std::move(observed));
                take(result.failure().outcome());
                // Resume directly into another submission. No auxiliary
                // continuation may outlive the fixture on assertion failure.
                require(
                  result.receipt() && result.receipt()->boundary() == boundary
                    && groups.queued_groups() == 0,
                  "notification preceded receipt installation or queue "
                  "retirement");
                auto next = take(
                  co_await groups.template submit<Clock>(
                    writer, std::move(later), work));
                require(
                  next.boundary().cursor().position()
                    > boundary.cursor().position(),
                  "reentrant submission reused a covered position");
                groups.request_stop();
                auto peer = co_await drive.lifecycle(std::move(duplicate));
                auto other = co_await drive.lifecycle(
                  std::move(second_observed));
                take(peer.failure().outcome());
                take(other.failure().outcome());
                require(
                  result.boundary() == first.boundary()
                    && other.boundary() == second.boundary() && result.receipt()
                    && other.receipt() && peer.receipt()
                    && peer.receipt()->boundary() == boundary
                    && result.receipt()->boundary() == boundary
                    && other.receipt()->boundary() == boundary
                    && writer.statistics().flush_calls == 1,
                  "coalesced waiters lost their own boundary or shared "
                  "receipt");
                take(groups.flush(writer, *capture));
                auto ready = take(first.observe());
                require(
                  ready.available(),
                  "completed observation missed its ready path");
                auto replay = co_await std::move(ready);
                require(
                  replay.receipt() && replay.receipt()->boundary() == boundary
                    && writer.statistics().flush_calls == 1,
                  "completed capture triggered another physical flush");
                retained.emplace(std::move(result));
            });
      });
    require(
      retained && retained->receipt() && !retained->failure().failed(),
      "result borrowed the destroyed cohort or writer owner");
    retained.reset();
}

template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> retained_pressure(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    co_await wal_append_contract::with_writer(
      files,
      owner,
      spec,
      budget,
      drive,
      wal_writer_contract::configuration(),
      [&](auto& writer, auto& work) -> seastar::future<> {
          auto config = slow_batch();
          config.target_members = 1;
          config.outstanding_groups = 2;
          co_await with_groups(
            writer,
            budget,
            drive,
            config,
            [&](auto& groups) -> seastar::future<> {
                const std::array records{assigned_wire()};
                std::optional<wal_commit_result> downstream;
                {
                    auto input = co_await wal_append_contract::offer(
                      budget, spec.owner.cluster(), work, records);
                    auto ticket = take(
                      co_await groups.template submit<Clock>(
                        writer, std::move(input), work));
                    auto observed = take(ticket.observe());
                    auto capture = groups.capture(Clock::now());
                    require(capture.has_value(), "first cohort missing");
                    take(groups.flush(writer, *capture));
                    downstream.emplace(
                      co_await drive.lifecycle(std::move(observed)));
                    take(downstream->failure().outcome());
                }
                require(
                  groups.queued_groups() == 0 && groups.retained_groups() == 1,
                  "durable result refunded its downstream slot");
                auto input = co_await wal_append_contract::offer(
                  budget, spec.owner.cluster(), work, records);
                auto second = take(
                  co_await groups.template submit<Clock>(
                    writer, std::move(input), work));
                auto observed = take(second.observe());
                auto capture = groups.capture(Clock::now());
                require(capture.has_value(), "later cohort missing");
                auto extra = co_await wal_append_contract::offer(
                  budget, spec.owner.cluster(), work, records);
                auto rejected = co_await groups.template submit<Clock>(
                  writer, std::move(extra), work);
                require(
                  !rejected && rejected.error().code() == errc::queue_full
                    && extra.size() == 1,
                  "stalled downstream work bypassed shared group admission");
                std::array<std::optional<workload_reservation>, 32> pressure;
                for (auto& held : pressure) {
                    auto next = budget.try_reserve(byte_count{4096});
                    if (!next) break;
                    held.emplace(std::move(*next));
                }
                require(
                  !budget.try_reserve(byte_count{1}),
                  "completion pressure did not saturate tasks");
                take(groups.flush(writer, *capture));
                auto done = co_await drive.lifecycle(std::move(observed));
                take(done.failure().outcome());
                require(
                  done.receipt() && writer.statistics().flush_calls == 2
                    && groups.retained_groups() == 2,
                  "later durability blocked on downstream work or refunded its "
                  "admission");
                for (auto& held : pressure)
                    held.reset();
                downstream.reset();
                require(
                  groups.retained_groups() == 1,
                  "last downstream result did not return its slot");
                auto next = take(
                  co_await groups.template submit<Clock>(
                    writer, std::move(extra), work));
                require(
                  next.members() == 1,
                  "returned capacity could not admit new work");
            });
      });
}

template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> observer_admission(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    co_await wal_append_contract::with_writer(
      files,
      owner,
      spec,
      budget,
      drive,
      wal_writer_contract::configuration(),
      [&](auto& writer, auto& work) -> seastar::future<> {
          const auto budget_before = budget.snapshot();
          for (auto limit : {0U, 2U, 7U, 257U}) {
              auto config = slow_batch();
              config.maximum_observers = limit;
              auto rejected = wal_group_commit::make(writer, budget, config);
              if (rejected)
                  take(co_await drive.lifecycle((*rejected)->close()));
              require(
                !rejected && rejected.error().code() == errc::invalid_argument,
                "invalid observer cap did not reject with invalid_argument");
              require(
                budget.snapshot().bytes == budget_before.bytes,
                "observer configuration rejection retained admission");
          }
          auto config = slow_batch();
          config.outstanding_groups = 2;
          config.maximum_observers = 2;
          config.target_members = 1;
          co_await with_groups(
            writer,
            budget,
            drive,
            config,
            [&](auto& groups) -> seastar::future<> {
                const std::array records{assigned_wire()};
                auto input = co_await wal_append_contract::offer(
                  budget, spec.owner.cluster(), work, records);
                auto ticket = take(
                  co_await groups.template submit<Clock>(
                    writer, std::move(input), work));
                auto first = take(ticket.observe());
                auto second = take(ticket.observe());
                auto full = ticket.observe();
                require(
                  !full && full.error().code() == errc::queue_full,
                  "observer cap allowed an uncharged peer");
                // Graceful close forces accepted work even when every observer
                // slot is occupied and no barrier was explicitly started.
                take(co_await drive.lifecycle(groups.close()));
                auto one = co_await drive.lifecycle(std::move(first));
                auto two = co_await drive.lifecycle(std::move(second));
                require(
                  one.receipt() && two.receipt() && !one.failure().failed()
                    && !two.failure().failed()
                    && one.receipt()->boundary() == ticket.boundary()
                    && two.receipt()->boundary() == ticket.boundary()
                    && writer.statistics().flush_calls == 1,
                  "graceful close failed to certify its accepted cohort");
            });
      });
}

template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> close_during_flush(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    co_await wal_append_contract::with_writer(
      files,
      owner,
      spec,
      budget,
      drive,
      wal_writer_contract::configuration(),
      [&](auto& writer, auto& work) -> seastar::future<> {
          auto config = slow_batch();
          config.target_members = 2;
          co_await with_groups(
            writer,
            budget,
            drive,
            config,
            [&](auto& groups) -> seastar::future<> {
                const std::array records{assigned_wire()};
                auto input = co_await wal_append_contract::offer(
                  budget, spec.owner.cluster(), work, records);
                auto ticket = take(
                  co_await groups.template submit<Clock>(
                    writer, std::move(input), work));
                auto tail_input = co_await wal_append_contract::offer(
                  budget, spec.owner.cluster(), work, records);
                auto tail = take(
                  co_await groups.template submit<Clock>(
                    writer, std::move(tail_input), work));
                // Observe only the final member. Close must still settle the
                // earlier group and retain its own requested boundary.
                auto observed = take(tail.observe());
                auto capture = groups.capture(Clock::now());
                require(
                  capture && capture->groups() == 2, "missing drain capture");
                take(groups.flush(writer, *capture));
                take(co_await drive.lifecycle(groups.close()));
                auto result = co_await drive.lifecycle(std::move(observed));
                take(result.failure().outcome());
                require(
                  result.receipt()
                    && result.receipt()->boundary() == capture->boundary()
                    && writer.statistics().flush_calls == 1
                    && groups.queued_groups() == 0,
                  "close stranded or revoked an already started barrier");
                auto late = take(ticket.observe());
                require(late.available(), "closed result lost its ready path");
                auto repeated = co_await std::move(late);
                take(repeated.failure().outcome());
                require(
                  repeated.boundary() == ticket.boundary()
                    && result.boundary() == tail.boundary()
                    && repeated.receipt()
                    && repeated.receipt()->boundary()
                         == result.receipt()->boundary(),
                  "cohort observer lost an unobserved group or its covering "
                  "receipt");
            });
      });
}

template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> result_allocation_cuts(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    co_await wal_append_contract::with_writer(
      files,
      owner,
      spec,
      budget,
      drive,
      wal_writer_contract::configuration(),
      [&](auto& writer, auto& work) -> seastar::future<> {
          auto config = slow_batch();
          config.maximum_observers = 1;
          config.outstanding_groups = 1;
          co_await with_groups(
            writer,
            budget,
            drive,
            config,
            [&](auto& groups) -> seastar::future<> {
                const std::array records{assigned_wire()};
                auto& injector = seastar::memory::local_failure_injector();
                for (std::size_t at = 0; at != 4; ++at) {
                    auto input = co_await wal_append_contract::offer(
                      budget, spec.owner.cluster(), work, records);
                    const auto before = budget.snapshot();
                    const auto accepted = writer.progress()->reserved;
                    injector.fail_after(at);
                    auto pending = groups.template submit<Clock>(
                      writer, std::move(input), work);
                    const bool injected = injector.failed();
                    injector.cancel();
                    bool caught = false;
                    try {
                        static_cast<void>(
                          co_await drive.lifecycle(std::move(pending)));
                    } catch (const std::bad_alloc&) {
                        caught = true;
                    }
                    require(
                      injected && caught && input.size() == 1
                        && writer.progress()->reserved == accepted
                        && groups.queued_groups() == 0
                        && groups.retained_groups() == 0
                        && budget.snapshot().bytes == before.bytes,
                      "result/promise allocation failure escaped preacceptance "
                      "rollback");
                }
                auto input = co_await wal_append_contract::offer(
                  budget, spec.owner.cluster(), work, records);
                auto ticket = take(
                  co_await groups.template submit<Clock>(
                    writer, std::move(input), work));
                auto observed = take(ticket.observe());
                groups.force();
                auto capture = groups.capture(Clock::now());
                require(
                  capture.has_value(), "result construction leaked admission");
                take(groups.flush(writer, *capture));
                auto result = co_await drive.lifecycle(std::move(observed));
                take(result.failure().outcome());
            });
      });
#else
    static_cast<void>(files);
    static_cast<void>(owner);
    static_cast<void>(spec);
    static_cast<void>(budget);
    static_cast<void>(drive);
#endif
    co_return;
}
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> stale_capture(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    co_await wal_append_contract::with_writer(
      files,
      owner,
      spec,
      budget,
      drive,
      wal_writer_contract::configuration(),
      [&](auto& writer, auto& work) -> seastar::future<> {
          auto config = slow_batch();
          config.target_members = 1;
          std::optional<wal_flush_capture> stale;
          co_await with_groups(
            writer,
            budget,
            drive,
            config,
            [&](auto& groups) -> seastar::future<> {
                const std::array records{assigned_wire()};
                auto input = co_await wal_append_contract::offer(
                  budget, spec.owner.cluster(), work, records);
                auto ticket = take(
                  co_await groups.template submit<Clock>(
                    writer, std::move(input), work));
                auto observer = take(ticket.observe());
                stale = groups.capture(Clock::now());
                require(stale.has_value(), "missing original capture");
                take(groups.flush(writer, *stale));
                auto result = co_await drive.lifecycle(std::move(observer));
                take(result.failure().outcome());
            });
          const auto old_end = stale->boundary();
          take(
            co_await drive.lifecycle(
              writer.rotate(take(writer.capture()), byte_count{8192}, work)));
          const auto flushes = writer.statistics().flush_calls;
          auto old_barrier = co_await writer.barrier(old_end);
          require(
            !old_barrier.receipt && old_barrier.failure.error()
              && old_barrier.failure.error()->code() == errc::wrong_context
              && writer.statistics().flush_calls == flushes,
            "rotated incarnation accepted an old capture");
          co_await with_groups(
            writer,
            budget,
            drive,
            config,
            [&](auto& groups) -> seastar::future<> {
                const auto before = writer.statistics().flush_calls;
                auto rejected = groups.flush(writer, *stale);
                require(
                  !rejected && rejected.error().code() == errc::wrong_context
                    && writer.statistics().flush_calls == before,
                  "old coordinator membership authorized a new operation");
                co_return;
            });
      });
}
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> observer_chunk_boundaries(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    co_await wal_append_contract::with_writer(
      files,
      owner,
      spec,
      budget,
      drive,
      wal_writer_contract::configuration(),
      [&](auto& writer, auto& work) -> seastar::future<> {
          for (auto limit : {1U, 2U, 129U, 130U, 256U}) {
              auto cfg = slow_batch();
              cfg.outstanding_groups = 1;
              cfg.target_members = 1;
              cfg.maximum_observers = limit;
              co_await with_groups(
                writer,
                budget,
                drive,
                cfg,
                [&](auto& groups) -> seastar::future<> {
                    const std::array records{assigned_wire()};
                    auto input = co_await wal_append_contract::offer(
                      budget, spec.owner.cluster(), work, records);
                    auto ticket = take(
                      co_await groups.template submit<Clock>(
                        writer, std::move(input), work));
                    std::array<
                      std::optional<seastar::future<wal_commit_result>>,
                      256>
                      waiters;
                    for (unsigned i = 0; i != limit; ++i) {
                        waiters[i].emplace(take(ticket.observe()));
                        require(
                          !waiters[i]->available(),
                          "peer completed without its barrier");
                    }
                    auto rejected = ticket.observe();
                    require(
                      !rejected && rejected.error().code() == errc::queue_full,
                      "observer chunk boundary exceeded the configured cap");
                    auto capture = groups.capture(Clock::now());
                    require(capture.has_value(), "peer capture missing");
                    take(groups.flush(writer, *capture));
                    for (unsigned i = 0; i != limit; ++i) {
                        auto result = co_await drive.lifecycle(
                          std::move(*waiters[i]));
                        take(result.failure().outcome());
                        require(
                          result.receipt()
                            && result.receipt()->boundary()
                                 == ticket.boundary(),
                          "chunked fan-out lost or mismatched a receipt");
                        waiters[i].reset();
                    }
                });
          }
      });
}
} // namespace kwaque::storage::testing::wal_durability_contract
