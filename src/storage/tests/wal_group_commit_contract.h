#pragma once

#include "src/runtime/testing/reactor_tasks.h"
#include "src/storage/tests/wal_append_contract.h"
#include "src/storage/wal_group_commit.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/preempt.hh>
#include <seastar/util/alloc_failure_injector.hh>

#include <array>
#include <atomic>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>

namespace kwaque::storage::testing::wal_group_commit_contract {
using store_contract::require;
using store_contract::take;

inline wal_group_commit_config slow_batch() {
    wal_group_commit_config config;
    config.outstanding_groups = 8;
    config.maximum_wait = runtime::monotonic_duration{3'600'000'000'000};
    return config;
}

// Count calls to the real timer capability; scheduling stays in that backend.
template<typename Timer>
struct counted_timer final {
    Timer& timer;
    unsigned waits{0};
    seastar::future<runtime::result<void>> sleep_until(
      runtime::monotonic_time deadline, seastar::abort_source& abort) {
        ++waits;
        return timer.sleep_until(deadline, abort);
    }
    void request_abort() noexcept { timer.request_abort(); }
    seastar::future<runtime::result<void>> stop() { return timer.stop(); }
};

template<typename Writer, typename Driver, typename Func>
seastar::future<> with_groups(
  Writer& writer,
  workload_budget& budget,
  Driver drive,
  wal_group_commit_config config,
  Func body,
  bool expected_failure = false) {
    std::unique_ptr<wal_group_commit> groups;
    runtime::first_failure failed;
    try {
        groups = take(wal_group_commit::make(writer, budget, config));
        co_await body(*groups);
    } catch (...) {
        failed.observe(std::current_exception());
    }
    if (groups) {
        try {
            auto closed = co_await drive.lifecycle(groups->close());
            if (expected_failure)
                require(!closed, "failed cohort closed successfully");
            else
                failed.observe(closed);
        } catch (...) {
            failed.observe(std::current_exception());
        }
        groups.reset();
    }
    take(failed.outcome());
}

template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> capacity_seals_forming_batch(
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
          config.outstanding_groups = 2;
          config.maximum_observers = config.outstanding_groups;
          co_await with_groups(
            writer,
            budget,
            drive,
            config,
            [&](auto& groups) -> seastar::future<> {
                const std::array records{assigned_wire()};
                const auto origin = Clock::now();
                const auto deadline
                  = origin.checked_add(config.maximum_wait).value();
                std::array<std::optional<wal_commit_ticket>, 2> tickets;
                std::array<std::optional<seastar::future<wal_commit_result>>, 2>
                  waiters;
                for (std::size_t i = 0; i != 2; ++i) {
                    auto input = co_await wal_append_contract::offer(
                      budget, spec.owner.cluster(), work, records);
                    tickets[i].emplace(take(
                      co_await groups.template submit<Clock>(
                        writer, std::move(input), work, origin)));
                    waiters[i].emplace(take(tickets[i]->observe()));
                    if (i == 0)
                        require(
                          !groups.capture(Clock::now()),
                          "nonfull cohort sealed early");
                }
                auto capture = groups.capture(Clock::now());
                require(
                  capture && capture->groups() == 2
                    && capture->deadline() == deadline
                    && Clock::now() < deadline,
                  "capacity did not seal the cohort with its original "
                  "deadline");
                const auto first_cut = capture->boundary();
                take(groups.flush(writer, *capture));
                std::array<std::optional<wal_commit_result>, 2> retained;
                for (std::size_t i = 0; i != 2; ++i) {
                    retained[i].emplace(
                      co_await drive.lifecycle(std::move(*waiters[i])));
                    take(retained[i]->failure().outcome());
                    waiters[i].reset();
                    tickets[i].reset();
                }
                capture.reset();
                require(
                  groups.queued_groups() == 0 && groups.retained_groups() == 2
                    && !groups.capture(Clock::now())
                    && writer.statistics().flush_calls == 1,
                  "retained results caused an empty flush or refunded "
                  "admission");
                auto extra = co_await wal_append_contract::offer(
                  budget, spec.owner.cluster(), work, records);
                auto blocked = co_await groups.template submit<Clock>(
                  writer, std::move(extra), work);
                // Local admission rejection preserves the offered group.
                // NOLINTBEGIN(bugprone-use-after-move)
                require(
                  !blocked && blocked.error().code() == errc::queue_full
                    && extra.size() == 1,
                  "full retained capacity consumed another offer");
                retained[0].reset();
                auto third = take(
                  co_await groups.template submit<Clock>(
                    writer, std::move(extra), work));
                // NOLINTEND(bugprone-use-after-move)
                auto observed = take(third.observe());
                auto next = groups.capture(Clock::now());
                require(
                  next && next->groups() == 1
                    && next->boundary() == third.boundary(),
                  "capacity held downstream left the forming batch waiting");
                take(groups.flush(writer, *next));
                auto done = co_await drive.lifecycle(std::move(observed));
                take(done.failure().outcome());
                require(
                  done.receipt() && retained[1]->receipt()
                    && retained[1]->receipt()->boundary() == first_cut
                    && writer.statistics().flush_calls == 2,
                  "capacity flush changed an earlier receipt or repeated I/O");
            });
      });
}

template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> formation(
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
          const std::array records{assigned_wire()};
          const auto before = writer.progress()->durable;
          const auto budget_before = budget.snapshot();
          for (unsigned which = 0; which != 8; ++which) {
              auto config = slow_batch();
              switch (which) {
              case 0:
                  config.target_members = 0;
                  break;
              case 1:
                  config.maximum_members = 129;
                  break;
              case 2:
                  config.target_bytes = byte_count{};
                  break;
              case 3:
                  config.maximum_bytes = byte_count{1};
                  break;
              case 4:
                  config.outstanding_groups = 0;
                  break;
              case 5:
                  config.outstanding_groups = 9;
                  break;
              case 6:
                  config.execution_bytes = byte_count{4095};
                  break;
              default:
                  config.execution_bytes = byte_count{131073};
                  break;
              }
              auto rejected = wal_group_commit::make(writer, budget, config);
              // Join even an incorrectly accepted candidate before asserting.
              if (rejected)
                  take(co_await drive.lifecycle((*rejected)->close()));
              require(!rejected, "invalid cohort configuration accepted");
          }
          require(
            budget.snapshot().bytes == budget_before.bytes,
            "configuration rejection retained admission");
          auto config = slow_batch();
          config.target_members = 2;
          config.outstanding_groups = 3;
          co_await with_groups(
            writer,
            budget,
            drive,
            config,
            [&](auto& groups) -> seastar::future<> {
                require(!groups.capture(Clock::now()), "empty cohort captured");
                auto duplicate_owner = wal_group_commit::make(
                  writer, budget, config);
                if (duplicate_owner)
                    take(co_await drive.lifecycle((*duplicate_owner)->close()));
                require(
                  !duplicate_owner,
                  "two cohort owners registered on one writer");
                auto premature_close = co_await writer.close();
                require(
                  !premature_close
                    && premature_close.error().code() == errc::queue_full,
                  "writer closed before its cohort owner drained");
                const auto direct_cut = take(writer.capture());
                const auto direct_budget = budget.snapshot();
                {
                    auto offered = co_await wal_append_contract::offer(
                      budget, spec.owner.cluster(), work, records);
                    auto direct = co_await writer.submit(
                      std::move(offered), work);
                    // Join even an incorrectly accepted write before asserting.
                    if (direct) {
                        auto completed = co_await drive.lifecycle(
                          std::move(direct->written));
                        take(completed.failure.outcome());
                    }
                    require(
                      !direct && direct.error().code() == errc::queue_full
                        && offered.size() == 0,
                      "direct submission bypassed the coordinator or did not "
                      "consume its offer");
                }
                require(
                  take(writer.capture()) == direct_cut
                    && writer.statistics().accepted_groups == 0
                    && writer.statistics().write_calls == 0
                    && groups.retained_groups() == 0
                    && !groups.failure().failed() && !writer.failure().failed(),
                  "direct rejection changed WAL admission or poisoned its "
                  "owner");
                require(
                  budget.snapshot().bytes == direct_budget.bytes
                    && budget.snapshot().tasks == direct_budget.tasks,
                  "direct rejection retained the offered resources");
                auto empty = take(wal_group::make(budget, 1));
                auto empty_result = co_await groups.template submit<Clock>(
                  writer, std::move(empty), work);
                require(
                  !empty_result && groups.retained_groups() == 0,
                  "empty offer acquired cohort admission");
                auto input = co_await wal_append_contract::offer(
                  budget, spec.owner.cluster(), work, records);
                std::optional<wal_commit_ticket> one{take(
                  co_await groups.template submit<Clock>(
                    writer, std::move(input), work))};
                require(
                  one->members() == 1
                    && one->encoded_bytes() == byte_count{8192}
                    && input.size() == 0,
                  "cohort accounting used child bytes instead of aligned "
                  "envelopes");
                require(
                  !groups.capture(Clock::now()),
                  "singleton ignored the batching window");
                auto next_input = co_await wal_append_contract::offer(
                  budget, spec.owner.cluster(), work, records);
                std::optional<wal_commit_ticket> two{take(
                  co_await groups.template submit<Clock>(
                    writer, std::move(next_input), work))};
                auto captured = groups.capture(Clock::now());
                require(
                  captured && captured->groups() == 2
                    && captured->members() == 2
                    && captured->encoded_bytes() == byte_count{16384}
                    && captured->boundary().cursor()
                         == two->boundary().cursor(),
                  "exact count threshold did not freeze the complete prefix");
                const auto cut = captured->boundary();
                auto last_input = co_await wal_append_contract::offer(
                  budget, spec.owner.cluster(), work, records);
                std::optional<wal_commit_ticket> three{take(
                  co_await groups.template submit<Clock>(
                    writer, std::move(last_input), work))};
                {
                    auto duplicate = groups.capture(Clock::now());
                    require(
                      duplicate
                        && duplicate->boundary().cursor() == cut.cursor()
                        && duplicate->groups() == 2,
                      "later arrival enlarged a frozen capture");
                }
                require(
                  !groups.retire_written(*captured), "unjoined cohort retired");
                take((co_await drive.lifecycle(groups.join_written(*captured)))
                       .outcome());
                take((co_await drive.lifecycle(groups.join_written(*captured)))
                       .outcome());
                take(groups.retire_written(*captured));
                require(
                  !groups.retire_written(*captured),
                  "stale capture retired another group");
                require(
                  groups.queued_groups() == 1 && groups.retained_groups() == 3,
                  "FIFO retirement refunded retained downstream admission");
                {
                    auto extra = co_await wal_append_contract::offer(
                      budget, spec.owner.cluster(), work, records);
                    auto rejected = co_await groups.template submit<Clock>(
                      writer, std::move(extra), work);
                    require(
                      !rejected && rejected.error().code() == errc::queue_full,
                      "retained tickets bypassed group capacity");
                    require(
                      extra.size() == 1,
                      "local pressure consumed an unentered offer");
                }
                one.reset();
                two.reset();
                captured.reset();
                require(
                  groups.retained_groups() == 1,
                  "last capture did not release group admission");
                groups.force();
                auto tail = groups.capture(Clock::now());
                require(
                  tail && tail->groups() == 1
                    && tail->boundary().cursor() == three->boundary().cursor(),
                  "forced cohort captured the wrong suffix");
                take((co_await drive.lifecycle(groups.join_written(*tail)))
                       .outcome());
                take(groups.retire_written(*tail));
                three.reset();
                tail.reset();
                require(
                  groups.retained_groups() == 0,
                  "drained cohort retained a slot");
                require(
                  writer.progress()->durable == before
                    && writer.statistics().flush_calls == 0,
                  "capture/write join manufactured durability");
            });
          const auto direct_cut = take(writer.capture());
          auto offered = co_await wal_append_contract::offer(
            budget, spec.owner.cluster(), work, records);
          auto direct = take(co_await writer.submit(std::move(offered), work));
          auto completed = co_await drive.lifecycle(std::move(direct.written));
          take(completed.failure.outcome());
          require(
            offered.size() == 0 && direct.members == 1
              && direct.extent.begin() == direct_cut.cursor().position(),
            "coordinator close did not restore direct writer admission");
      });
}

template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> boundaries(
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
          const std::array records{assigned_wire()};
          const std::array pair{assigned_wire(), assigned_wire()};
          for (unsigned which = 0; which != 4; ++which) {
              auto config = slow_batch();
              if (which == 0)
                  config.target_bytes = config.maximum_bytes = byte_count{8191};
              if (which == 1)
                  config.target_members = config.maximum_members = 1;
              if (which == 3)
                  config.maximum_wait = runtime::monotonic_duration::maximum();
              co_await with_groups(
                writer,
                budget,
                drive,
                config,
                [&](auto& groups) -> seastar::future<> {
                    const auto before = writer.progress()->reserved;
                    auto input = co_await wal_append_contract::offer(
                      budget,
                      spec.owner.cluster(),
                      work,
                      which == 1 ? std::span<const std::string>{pair}
                                 : std::span<const std::string>{records});
                    std::optional<runtime::monotonic_time> future_origin;
                    if (which == 2)
                        future_origin = Clock::now()
                                          .checked_add(config.maximum_wait)
                                          .value();
                    if (which == 3)
                        require(
                          Clock::now().nanoseconds() != 0,
                          "overflow case requires elapsed setup time");
                    auto rejected = co_await groups.template submit<Clock>(
                      writer, std::move(input), work, future_origin);
                    require(
                      !rejected && writer.progress()->reserved == before
                        && groups.queued_groups() == 0
                        && groups.retained_groups() == 0,
                      "preacceptance count/byte/time rejection changed "
                      "ownership or coordinates");
                    require(input.size() == (which == 0 ? 0U : which == 1 ? 2U : 1U),
                            "local rejection and consuming writer rejection were conflated");
                });
          }
          for (auto target : {8191U, 8192U}) {
              auto config = slow_batch();
              config.target_bytes = byte_count{target};
              config.maximum_bytes = byte_count{8192};
              co_await with_groups(
                writer,
                budget,
                drive,
                config,
                [&](auto& groups) -> seastar::future<> {
                    auto input = co_await wal_append_contract::offer(
                      budget, spec.owner.cluster(), work, records);
                    auto accepted = take(
                      co_await groups.template submit<Clock>(
                        writer, std::move(input), work));
                    auto capture = groups.capture(Clock::now());
                    require(
                      capture && capture->members() == 1
                        && capture->encoded_bytes() == byte_count{8192},
                      "exact-byte target or valid larger singleton did not "
                      "close");
                });
          }
          auto config = slow_batch();
          config.target_bytes = byte_count{12288};
          co_await with_groups(
            writer,
            budget,
            drive,
            config,
            [&](auto& groups) -> seastar::future<> {
                auto one = co_await wal_append_contract::offer(
                  budget, spec.owner.cluster(), work, records);
                auto first = take(
                  co_await groups.template submit<Clock>(
                    writer, std::move(one), work));
                auto two = co_await wal_append_contract::offer(
                  budget, spec.owner.cluster(), work, records);
                auto second = take(
                  co_await groups.template submit<Clock>(
                    writer, std::move(two), work));
                auto capture = groups.capture(Clock::now());
                require(
                  capture && capture->groups() == 1
                    && capture->boundary().cursor()
                         == first.boundary().cursor(),
                  "projected byte limit split or absorbed a frozen group");
            });
          co_await with_groups(
            writer,
            budget,
            drive,
            slow_batch(),
            [&](auto& groups) -> seastar::future<> {
                const auto origin = Clock::now();
                const auto deadline
                  = origin.checked_add(slow_batch().maximum_wait).value();
                auto one = co_await wal_append_contract::offer(
                  budget, spec.owner.cluster(), work, records);
                auto first = take(
                  co_await groups.template submit<Clock>(
                    writer, std::move(one), work, origin));
                auto two = co_await wal_append_contract::offer(
                  budget, spec.owner.cluster(), work, records);
                auto second = take(
                  co_await groups.template submit<Clock>(
                    writer, std::move(two), work));
                require(
                  !groups.capture(
                    deadline.checked_sub(runtime::monotonic_duration{1})
                      .value()),
                  "cohort captured before its oldest deadline");
                auto capture = groups.capture(deadline);
                require(
                  capture && capture->deadline() == deadline
                    && capture->groups() == 2,
                  "deadline equality or carried origin was lost");
            });
          config = slow_batch();
          config.maximum_wait = runtime::monotonic_duration{1};
          co_await with_groups(
            writer,
            budget,
            drive,
            config,
            [&](auto& groups) -> seastar::future<> {
                auto input = co_await wal_append_contract::offer(
                  budget, spec.owner.cluster(), work, records);
                const auto origin = Clock::now().checked_sub(
                  runtime::monotonic_duration{1});
                require(
                  origin.has_value(),
                  "expired-origin case requires elapsed setup time");
                auto ticket = take(
                  co_await groups.template submit<Clock>(
                    writer, std::move(input), work, origin));
                require(
                  groups.capture(Clock::now()).has_value(),
                  "expired origin restarted a batching interval");
            });
          config = slow_batch();
          config.maximum_wait = runtime::monotonic_duration{};
          config.outstanding_groups = 1;
          co_await with_groups(
            writer,
            budget,
            drive,
            config,
            [&](auto& groups) -> seastar::future<> {
                // Repeated retirement crosses native chunk boundaries while
                // preserving the startup-reserved FIFO capacity.
                for (unsigned i = 0; i != 20; ++i) {
                    auto input = co_await wal_append_contract::offer(
                      budget, spec.owner.cluster(), work, records);
                    auto ticket = take(
                      co_await groups.template submit<Clock>(
                        writer, std::move(input), work));
                    auto capture = groups.capture(Clock::now());
                    require(
                      capture && capture->groups() == 1,
                      "zero wait added deliberate delay");
                    take(
                      (co_await drive.lifecycle(groups.join_written(*capture)))
                        .outcome());
                    take(groups.retire_written(*capture));
                }
            });
      });
}

template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> timers(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
    co_await wal_append_contract::with_writer(
      files,
      owner,
      spec,
      budget,
      drive,
      wal_writer_contract::configuration(),
      [&](auto& writer, auto& work) -> seastar::future<> {
          const std::array records{assigned_wire()};
          auto config = slow_batch();
          config.target_members = 3;
          counted_timer<Timer> counted{timer};
          co_await with_groups(
            writer,
            budget,
            drive,
            config,
            [&](auto& groups) -> seastar::future<> {
                auto one = co_await wal_append_contract::offer(
                  budget, spec.owner.cluster(), work, records);
                auto first = take(
                  co_await groups.template submit<Clock>(
                    writer, std::move(one), work));
                auto waiting = groups.template wait_capture<Clock>(counted);
                auto two = co_await wal_append_contract::offer(
                  budget, spec.owner.cluster(), work, records);
                auto second = take(
                  co_await groups.template submit<Clock>(
                    writer, std::move(two), work));
                co_await runtime::testing::drain_reactor_tasks();
                require(
                  !waiting.available() && counted.waits == 1,
                  "later arrival rearmed an unchanged cohort deadline");
                auto three = co_await wal_append_contract::offer(
                  budget, spec.owner.cluster(), work, records);
                auto third = take(
                  co_await groups.template submit<Clock>(
                    writer, std::move(three), work));
                auto capture = take(
                  co_await drive.lifecycle(std::move(waiting)));
                require(
                  capture && capture->groups() == 3 && counted.waits == 1,
                  "threshold wake started an unnecessary timer interval");
            });
          config.target_members = 2;
          co_await with_groups(
            writer,
            budget,
            drive,
            config,
            [&](auto& groups) -> seastar::future<> {
                auto input = co_await wal_append_contract::offer(
                  budget, spec.owner.cluster(), work, records);
                auto first = take(
                  co_await groups.template submit<Clock>(
                    writer, std::move(input), work));
                auto waiting = groups.template wait_capture<Clock>(timer);
                require(!waiting.available(), "batch timer did not suspend");
                auto overlap = co_await groups.template wait_capture<Clock>(
                  timer);
                require(
                  !overlap && overlap.error().code() == errc::queue_full,
                  "unbounded capture waiter accepted");
                auto next = co_await wal_append_contract::offer(
                  budget, spec.owner.cluster(), work, records);
                auto second = take(
                  co_await groups.template submit<Clock>(
                    writer, std::move(next), work));
                auto captured = take(
                  co_await drive.lifecycle(std::move(waiting)));
                require(
                  captured && captured->groups() == 2,
                  "count trigger did not cancel and join timer");
                take((co_await drive.lifecycle(groups.join_written(*captured)))
                       .outcome());
                take(groups.retire_written(*captured));
                auto empty = groups.template wait_capture<Clock>(timer);
                auto closed = groups.close();
                auto empty_result = co_await drive.lifecycle(std::move(empty));
                take(co_await drive.lifecycle(std::move(closed)));
                require(
                  !take(std::move(empty_result)),
                  "stopped empty queue created a capture");
            });
          config = slow_batch();
          config.maximum_wait = runtime::monotonic_duration{1'000'000};
          co_await with_groups(
            writer,
            budget,
            drive,
            slow_batch(),
            [&](auto& groups) -> seastar::future<> {
                auto input = co_await wal_append_contract::offer(
                  budget, spec.owner.cluster(), work, records);
                auto pending = [&] {
                    seastar::internal::preemption_monitor requested{};
                    requested.head.store(1, std::memory_order_relaxed);
                    const auto* previous
                      = seastar::internal::get_need_preempt_var();
                    auto restore = seastar::defer([previous] noexcept {
                        seastar::internal::set_need_preempt_var(previous);
                    });
                    seastar::internal::set_need_preempt_var(&requested);
                    return groups.template submit<Clock>(
                      writer, std::move(input), work);
                }();
                const bool suspended = !pending.available();
                auto waiting = groups.template wait_capture<Clock>(timer);
                std::array<
                  std::optional<seastar::future<runtime::result<void>>>,
                  8>
                  closing;
                for (auto& interested : closing)
                    interested.emplace(groups.close());
                auto excess = groups.close();
                const bool excess_ready = excess.available();
                const auto rejected = co_await std::move(excess);
                auto accepted = co_await drive.lifecycle(std::move(pending));
                auto captured = co_await drive.lifecycle(std::move(waiting));
                for (auto& interested : closing)
                    take(co_await drive.lifecycle(std::move(*interested)));
                require(
                  suspended && accepted && captured && captured->has_value()
                    && (**captured).boundary().cursor()
                         == accepted->boundary().cursor(),
                  "stop lost a ticket installed by an entered preflight");
                require(
                  excess_ready && !rejected
                    && rejected.error().code() == errc::queue_full,
                  "pending close interests exceeded their separate bound");
                take(co_await groups.close());
                auto settled = co_await take(accepted->observe());
                require(
                  settled.receipt() && !settled.failure().failed(),
                  "close lost an accepted ticket installed during drain");
            });
          co_await with_groups(
            writer,
            budget,
            drive,
            config,
            [&](auto& groups) -> seastar::future<> {
                auto input = co_await wal_append_contract::offer(
                  budget, spec.owner.cluster(), work, records);
                auto first = take(
                  co_await groups.template submit<Clock>(
                    writer, std::move(input), work));
                auto captured = take(
                  co_await drive.lifecycle(
                    groups.template wait_capture<Clock>(timer)));
                require(
                  captured && Clock::now() >= captured->deadline(),
                  "runtime timer fired before the deadline");
            });
          co_await with_groups(
            writer,
            budget,
            drive,
            slow_batch(),
            [&](auto& groups) -> seastar::future<> {
                auto input = co_await wal_append_contract::offer(
                  budget, spec.owner.cluster(), work, records);
                auto first = take(
                  co_await groups.template submit<Clock>(
                    writer, std::move(input), work));
                auto waiting = groups.template wait_capture<Clock>(timer);
                require(!waiting.available(), "stop timer did not suspend");
                groups.request_stop();
                auto early = groups.capture(Clock::now());
                require(
                  early.has_value(),
                  "forced capture missing before timer join");
                auto busy = groups.flush(writer, *early);
                require(
                  !busy && busy.error().code() == errc::queue_full,
                  "another consumer stole an outstanding capture wait");
                auto captured = take(
                  co_await drive.lifecycle(std::move(waiting)));
                require(
                  captured && !groups.failure().failed(),
                  "forced timer cancellation became storage failure");
                auto observed = take(first.observe());
                take(groups.flush(writer, *captured));
                auto result = co_await drive.lifecycle(std::move(observed));
                take(result.failure().outcome());
                require(
                  result.receipt().has_value(),
                  "joined capture did not become flushable");
            });
          co_await with_groups(
            writer,
            budget,
            drive,
            slow_batch(),
            [&](auto& groups) -> seastar::future<> {
                auto input = co_await wal_append_contract::offer(
                  budget, spec.owner.cluster(), work, records);
                auto first = take(
                  co_await groups.template submit<Clock>(
                    writer, std::move(input), work));
                auto waiting = groups.template wait_capture<Clock>(timer);
                require(
                  !waiting.available(), "owner-abort timer did not suspend");
                timer.request_abort();
                auto captured = take(
                  co_await drive.lifecycle(std::move(waiting)));
                require(
                  captured && !groups.failure().failed(),
                  "runtime timer shutdown failed an otherwise flushable "
                  "cohort");
                take((co_await drive.lifecycle(groups.join_written(*captured)))
                       .outcome());
            });
      });
}

template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> allocation_cuts(
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
          auto& injector = seastar::memory::local_failure_injector();
          unsigned cuts = 0;
          for (std::size_t at = 0; at != 8; ++at) {
              const auto before = budget.snapshot();
              std::unique_ptr<wal_group_commit> groups;
              bool caught = false;
              injector.fail_after(at);
              try {
                  groups = take(
                    wal_group_commit::make(writer, budget, slow_batch()));
              } catch (const std::bad_alloc&) {
                  caught = true;
              }
              const bool injected = injector.failed();
              injector.cancel();
              if (groups) take(co_await drive.lifecycle(groups->close()));
              groups.reset();
              require(
                caught == injected,
                "construction cut did not preserve allocation failure");
              cuts += static_cast<unsigned>(injected);
              require(
                budget.snapshot().tasks == before.tasks
                  && budget.snapshot().bytes == before.bytes,
                "construction cut leaked cohort admission");
          }
          require(
            cuts != 0,
            "no ordinary cohort construction allocation was injected");
          co_await with_groups(
            writer,
            budget,
            drive,
            slow_batch(),
            [&](auto& groups) -> seastar::future<> {
                const std::array records{assigned_wire()};
                auto input = co_await wal_append_contract::offer(
                  budget, spec.owner.cluster(), work, records);
                const auto before = writer.progress()->reserved;
                injector.fail_after(0);
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
                  injected && caught && groups.retained_groups() == 0
                    && groups.queued_groups() == 0
                    && writer.progress()->reserved == before,
                  "registration allocation failure occurred after acceptance "
                  "or leaked a gate/slot");
                require(
                  input.size() == 1,
                  "failed local registration consumed its offer");
                auto accepted = take(
                  co_await groups.template submit<Clock>(
                    writer, std::move(input), work));
                groups.force();
                auto capture = groups.capture(Clock::now());
                require(
                  capture.has_value(),
                  "failed insertion stranded later admission");
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
seastar::future<> accepted_encoding_failure(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION) && !defined(SEASTAR_DEBUG)
    bool injected = false, cohort_closed = false, writer_closed = false;
    std::exception_ptr observed;
    try {
        co_await wal_append_contract::with_writer(
          files,
          owner,
          spec,
          budget,
          drive,
          wal_writer_contract::configuration(),
          [&](auto& writer, auto& work) -> seastar::future<> {
              try {
                  co_await with_groups(
                    writer,
                    budget,
                    drive,
                    slow_batch(),
                    [&](auto& groups) -> seastar::future<> {
                        const std::array records{assigned_wire()};
                        auto input = co_await wal_append_contract::offer(
                          budget, spec.owner.cluster(), work, records);
                        const auto before = *writer.progress();
                        seastar::abort_source caller_abort;
                        codec::cooperative_work caller{
                          work.policy(), caller_abort};
                        // The writer schedules encoding in its metadata group.
                        // Hold only this synchronous admission checkpoint;
                        // ordinary queued encoding allocations remain
                        // injectable.
                        auto pending = [&] {
                            seastar::internal::preemption_monitor
                              uninterrupted{};
                            const auto* previous
                              = seastar::internal::get_need_preempt_var();
                            auto restore = seastar::defer([previous] noexcept {
                                seastar::internal::set_need_preempt_var(
                                  previous);
                            });
                            seastar::internal::set_need_preempt_var(
                              &uninterrupted);
                            return groups.template submit<Clock>(
                              writer, std::move(input), caller);
                        }();
                        const bool checkpoint
                          = pending.available()
                            && writer.statistics().accepted_groups == 1
                            && writer.statistics().encoded_groups == 0
                            && writer.statistics().write_calls == 0;
                        auto ticket = take(
                          co_await seastar::coroutine::without_preemption_check(
                            std::move(pending)));
                        groups.force();
                        auto capture = groups.capture(Clock::now());
                        require(
                          capture.has_value(),
                          "accepted failure lost its captured membership");
                        auto& injector
                          = seastar::memory::local_failure_injector();
                        auto waiting = take(ticket.observe());
                        auto peer = take(ticket.observe());
                        if (checkpoint) injector.fail_after(0);
                        runtime::first_failure completion;
                        std::exception_ptr unexpected;
                        try {
                            take(groups.flush(writer, *capture));
                            auto result = co_await drive.lifecycle(
                              std::move(waiting));
                            completion = result.failure();
                            require(
                              !result.receipt(),
                              "encoding failure produced a durable receipt");
                        } catch (...) {
                            unexpected = std::current_exception();
                        }
                        injected = checkpoint && injector.failed();
                        injector.cancel();
                        if (unexpected) std::rethrow_exception(unexpected);
                        observed = completion.exception();
                        require(
                          checkpoint && injected && observed,
                          "accepted cohort encoding allocation cut did not "
                          "execute");
                        require(
                          groups.failure().exception() == observed
                            && writer.failure().exception() == observed
                            && writer.progress()->reserved
                                 == ticket.boundary().cursor()
                            && writer.progress()->reserved.position().value()
                                 == before.reserved.position().value() + 8192
                            && writer.progress()->write_complete
                                 == before.write_complete
                            && writer.progress()->durable == before.durable
                            && writer.statistics().write_calls == 0,
                          "accepted encoding failure lost ownership, identity "
                          "or its error channel");
                        auto duplicate = co_await drive.lifecycle(
                          std::move(peer));
                        require(
                          !duplicate.receipt()
                            && duplicate.failure().exception() == observed
                            && groups.queued_groups() == 0
                            && writer.statistics().flush_calls == 0,
                          "exception fan-out changed failure or submitted a "
                          "dependent flush");
                    });
              } catch (const std::bad_alloc&) {
                  require(
                    injected && observed == std::current_exception(),
                    "cohort close changed the encoding failure");
                  cohort_closed = true;
                  throw;
              }
          });
    } catch (const std::bad_alloc&) {
        require(
          injected && cohort_closed && observed == std::current_exception(),
          "writer close changed the cohort failure");
        writer_closed = true;
    }
    require(
      injected && cohort_closed && writer_closed,
      "accepted exceptional failure was not joined through both owners");
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
seastar::future<> maximum_members(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    auto writer_config = wal_writer_contract::configuration();
    writer_config.capacity_bytes = byte_count{2U << 20U};
    co_await wal_append_contract::with_writer(
      files,
      owner,
      spec,
      budget,
      drive,
      writer_config,
      [&](auto& writer, auto& work) -> seastar::future<> {
          auto config = slow_batch();
          config.target_members = 128;
          config.outstanding_groups = 2;
          co_await with_groups(
            writer,
            budget,
            drive,
            config,
            [&](auto& groups) -> seastar::future<> {
                std::array<std::string, 64> records;
                records.fill(assigned_wire());
                auto one = co_await wal_append_contract::offer(
                  budget, spec.owner.cluster(), work, records);
                auto first = take(
                  co_await groups.template submit<Clock>(
                    writer, std::move(one), work));
                require(
                  !groups.capture(Clock::now()),
                  "D group limit was mistaken for cohort threshold");
                auto two = co_await wal_append_contract::offer(
                  budget, spec.owner.cluster(), work, records);
                auto second = take(
                  co_await groups.template submit<Clock>(
                    writer, std::move(two), work));
                auto capture = groups.capture(Clock::now());
                require(
                  capture && capture->groups() == 2 && capture->members() == 128
                    && capture->encoded_bytes() == byte_count{128U * 8192U},
                  "legal D groups did not form the maximum cohort");
            });
      });
}
} // namespace kwaque::storage::testing::wal_group_commit_contract
