#pragma once

#include "src/storage/tests/wal_group_commit_contract.h"

#include <stdexcept>

namespace kwaque::storage::testing::wal_commit_lifecycle_contract {
using store_contract::require;
using store_contract::take;
using wal_group_commit_contract::slow_batch;
using wal_group_commit_contract::with_groups;
namespace append = wal_append_contract;

template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> cancellation(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
    co_await append::with_writer(
      files,
      owner,
      spec,
      budget,
      drive,
      wal_writer_contract::configuration(),
      [&](auto& writer, auto& work) -> seastar::future<> {
          auto config = slow_batch();
          config.maximum_observers = 130;
          co_await with_groups(
            writer,
            budget,
            drive,
            config,
            [&](auto& groups) -> seastar::future<> {
                const std::array records{assigned_wire()};
                auto input = co_await append::offer(
                  budget, spec.owner.cluster(), work, records);
                auto ticket = take(
                  co_await groups.template submit<Clock>(
                    writer, std::move(input), work));
                // Keep both the standalone front and the first chunk entry
                // live. Cancelling later peers then leaves physical tombstones
                // behind.
                auto front = take(ticket.observe());
                auto early = take(ticket.observe());
                for (unsigned i = 2; i != config.maximum_observers; ++i) {
                    seastar::abort_source caller;
                    auto pending = ticket.template observe<Clock>(
                      timer, caller);
                    require(
                      !pending.available(),
                      "pending cohort delivered success before flush");
                    caller.request_abort();
                    auto detached = co_await drive.lifecycle(
                      std::move(pending));
                    require(
                      !detached && detached.error().code() == errc::aborted,
                      "caller abort did not detach its native peer");
                }
                seastar::abort_source extra;
                auto full = co_await ticket.template observe<Clock>(
                  timer, extra);
                require(
                  !full && full.error().code() == errc::queue_full
                    && groups.retained_groups() == 1
                    && !groups.failure().failed() && !writer.failure().failed(),
                  "cancel/rejoin refunded retained peer slots or poisoned "
                  "storage");
                take(co_await drive.lifecycle(groups.close()));
                auto one = co_await drive.lifecycle(std::move(front));
                auto two = co_await drive.lifecycle(std::move(early));
                take(one.failure().outcome());
                take(two.failure().outcome());
                require(
                  one.receipt() && two.receipt()
                    && writer.statistics().flush_calls == 1,
                  "cancellation churn lost the live early peers");
            });
          config.outstanding_groups = 2;
          config.maximum_observers = 2;
          co_await with_groups(
            writer,
            budget,
            drive,
            config,
            [&](auto& groups) -> seastar::future<> {
                const std::array records{assigned_wire()};
                auto input = co_await append::offer(
                  budget, spec.owner.cluster(), work, records);
                auto ticket = take(
                  co_await groups.template submit<Clock>(
                    writer, std::move(input), work));
                seastar::abort_source caller;
                auto pending = ticket.template observe<Clock>(timer, caller);
                caller.request_abort();
                require(
                  !(co_await drive.lifecycle(std::move(pending))),
                  "detached waiter succeeded");
                const auto before = writer.statistics().flush_calls;
                take(co_await drive.lifecycle(groups.close()));
                auto ready = co_await take(ticket.observe());
                require(
                  ready.receipt() && !ready.failure().failed()
                    && writer.statistics().flush_calls == before + 1,
                  "all-detached accepted work did not drain");
            });
      });
}

template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> deadlines(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
    co_await append::with_writer(
      files,
      owner,
      spec,
      budget,
      drive,
      wal_writer_contract::configuration(),
      [&](auto& writer, auto& work) -> seastar::future<> {
          co_await with_groups(
            writer,
            budget,
            drive,
            slow_batch(),
            [&](auto& groups) -> seastar::future<> {
                const std::array records{assigned_wire()};
                auto input = co_await append::offer(
                  budget, spec.owner.cluster(), work, records);
                auto ticket = take(
                  co_await groups.template submit<Clock>(
                    writer, std::move(input), work));
                seastar::abort_source caller;
                auto expired = co_await ticket.template observe<Clock>(
                  timer, caller, Clock::now());
                require(
                  !expired && expired.error().code() == errc::timed_out,
                  "expired interest registered a pending observer");
                const auto deadline = Clock::now()
                                        .checked_add(
                                          runtime::monotonic_duration{1})
                                        .value();
                auto timeout = co_await drive.lifecycle(
                  ticket.template observe<Clock>(timer, caller, deadline));
                require(
                  !timeout && timeout.error().code() == errc::timed_out
                    && !groups.failure().failed() && !writer.failure().failed(),
                  "observer timeout became a storage failure");
                const auto later = Clock::now()
                                     .checked_add(
                                       runtime::monotonic_duration{
                                         3'600'000'000'000})
                                     .value();
                auto observed = ticket.template observe<Clock>(
                  timer, caller, later);
                groups.force();
                auto cut = groups.capture(Clock::now());
                require(cut.has_value(), "forced cohort missing");
                take(groups.flush(writer, *cut));
                auto done = take(co_await drive.lifecycle(std::move(observed)));
                take(done.failure().outcome());
                caller.request_abort();
                // Settled common state wins even with an already-aborted caller
                // and expired deadline; no new timer/subscription is needed.
                auto cached = take(
                  co_await ticket.template observe<Clock>(
                    timer, caller, Clock::now()));
                require(
                  cached.receipt() && done.receipt()
                    && cached.receipt()->boundary()
                         == done.receipt()->boundary(),
                  "late cancellation revoked terminal success");
            });
      });
}

template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> rotation(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
    co_await append::with_writer(
      files,
      owner,
      spec,
      budget,
      drive,
      wal_writer_contract::configuration(),
      [&](auto& writer, auto& work) -> seastar::future<> {
          co_await with_groups(
            writer,
            budget,
            drive,
            slow_batch(),
            [&](auto& groups) -> seastar::future<> {
                take(groups.template start<Clock>(timer));
                const std::array records{assigned_wire()};
                auto input = co_await append::offer(
                  budget, spec.owner.cluster(), work, records);
                auto first = take(
                  co_await groups.template submit<Clock>(
                    writer, std::move(input), work));
                auto observed = take(first.observe());
                const auto old = first.boundary();
                auto direct = co_await writer.rotate(
                  old, byte_count{8192}, work);
                require(
                  !direct && direct.error().code() == errc::queue_full,
                  "direct rotation bypassed the registered coordinator");
                // The first rotation itself must force the pending old-file
                // result before switching incarnations.
                take(
                  co_await drive.lifecycle(
                    groups.rotate(writer, byte_count{8192}, work)));
                auto retained = co_await drive.lifecycle(std::move(observed));
                take(retained.failure().outcome());
                require(
                  retained.receipt() && retained.receipt()->boundary() == old
                    && !writer.validate_capture(old),
                  "rotation attempted before old-file certification");
                auto middle_input = co_await append::offer(
                  budget, spec.owner.cluster(), work, records);
                auto middle = take(
                  co_await groups.template submit<Clock>(
                    writer, std::move(middle_input), work));
                auto middle_observed = take(middle.observe());
                groups.force();
                auto middle_result = co_await drive.lifecycle(
                  std::move(middle_observed));
                take(middle_result.failure().outcome());
                // Establish pressure after write/flush retirement so completion
                // cannot return ordinary capacity during successor preparation.
                std::array<std::optional<workload_reservation>, 32> pressure;
                for (auto& slot : pressure) {
                    auto held = budget.try_reserve(byte_count{4096});
                    if (!held) break;
                    slot.emplace(std::move(*held));
                }
                require(
                  !budget.try_reserve(byte_count{1}),
                  "rotation pressure did not engage");
                auto rejected = co_await drive.lifecycle(
                  groups.rotate(writer, byte_count{8192}, work));
                require(
                  !rejected && writer.rotation_pending()
                    && !groups.failure().failed(),
                  "untouched rotation pressure was converted into fatal "
                  "failure");
                for (auto& slot : pressure)
                    slot.reset();
                const auto flushes = writer.statistics().flush_calls;
                take(
                  co_await drive.lifecycle(
                    groups.rotate(writer, byte_count{8192}, work)));
                require(
                  !writer.validate_capture(middle.boundary())
                    && writer.statistics().flush_calls == flushes,
                  "rotation retained an old cut or repeated its durable "
                  "barrier");
                auto next = co_await append::offer(
                  budget, spec.owner.cluster(), work, records);
                auto second = take(
                  co_await groups.template submit<Clock>(
                    writer, std::move(next), work));
                require(
                  second.boundary().cursor().incarnation()
                    != old.cursor().incarnation(),
                  "coordinator failed to adopt the new writer incarnation");
                auto tail = take(second.observe());
                auto close_one = groups.close();
                auto close_two = groups.close();
                take(co_await drive.lifecycle(std::move(close_one)));
                take(co_await drive.lifecycle(std::move(close_two)));
                take(co_await groups.close());
                auto result = co_await drive.lifecycle(std::move(tail));
                require(
                  result.receipt() && retained.receipt()->boundary() == old
                    && writer.statistics().flush_calls == flushes + 1,
                  "concurrent close lost a cohort or revoked retained old-file "
                  "success");
            });
      });
}

template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> service(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
    co_await append::with_writer(
      files,
      owner,
      spec,
      budget,
      drive,
      wal_writer_contract::configuration(),
      [&](auto& writer, auto& work) -> seastar::future<> {
          auto config = slow_batch();
          config.target_members = 1;
          co_await with_groups(
            writer,
            budget,
            drive,
            config,
            [&](auto& groups) -> seastar::future<> {
                take(groups.template start<Clock>(timer));
                for (unsigned i = 0; i != 3; ++i) {
                    const std::array records{assigned_wire()};
                    auto input = co_await append::offer(
                      budget, spec.owner.cluster(), work, records);
                    auto ticket = take(
                      co_await groups.template submit<Clock>(
                        writer, std::move(input), work));
                    auto result = co_await drive.lifecycle(
                      take(ticket.observe()));
                    require(
                      result.receipt() && !result.failure().failed()
                        && result.receipt()->boundary() == ticket.boundary()
                        && writer.statistics().flush_calls == i + 1,
                      "automatic service needed an external flush or lost its "
                      "next cohort");
                }
                take(co_await drive.lifecycle(groups.close()));
            });
          co_await with_groups(
            writer,
            budget,
            drive,
            config,
            [&](auto& groups) -> seastar::future<> {
                seastar::abort_source already_stopped;
                already_stopped.request_abort();
                take(groups.bind_shutdown(already_stopped));
                auto started = groups.template start<Clock>(timer);
                require(
                  !started && started.error().code() == errc::closed,
                  "startup after environment abort opened service admission");
                take(co_await drive.lifecycle(groups.close()));
            });
      });
}

struct failed_timer final {
    std::exception_ptr exception;
    unsigned calls{0};
    seastar::future<runtime::result<void>>
    sleep_until(runtime::monotonic_time, seastar::abort_source&) {
        ++calls;
        if (exception)
            return seastar::make_exception_future<runtime::result<void>>(
              exception);
        return seastar::make_ready_future<runtime::result<void>>(
          runtime::failure(
            runtime::operation_error{
              errc::resource_exhausted, runtime::operation_kind::timer}));
    }
    void request_abort() noexcept {}
    seastar::future<runtime::result<void>> stop() {
        return seastar::make_ready_future<runtime::result<void>>(
          runtime::result<void>{});
    }
};

template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> observer_failures(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer) {
    co_await append::with_writer(
      files,
      owner,
      spec,
      budget,
      drive,
      wal_writer_contract::configuration(),
      [&](auto& writer, auto& work) -> seastar::future<> {
          auto config = slow_batch();
          config.outstanding_groups = 3;
          config.maximum_observers = 3;
          co_await with_groups(
            writer,
            budget,
            drive,
            config,
            [&](auto& groups) -> seastar::future<> {
                const std::array records{assigned_wire()};
                auto input = co_await append::offer(
                  budget, spec.owner.cluster(), work, records);
                auto ticket = take(
                  co_await groups.template submit<Clock>(
                    writer, std::move(input), work));
                seastar::abort_source caller;
                const auto deadline = Clock::now()
                                        .checked_add(
                                          runtime::monotonic_duration{
                                            3'600'000'000'000})
                                        .value();
                for (bool exceptional : {false, true}) {
                    failed_timer failing;
                    if (exceptional)
                        failing.exception = std::make_exception_ptr(
                          std::runtime_error("observer timer"));
                    runtime::first_failure seen;
                    try {
                        seen.observe(
                          co_await drive.lifecycle(
                            ticket.template observe<Clock>(
                              failing, caller, deadline)));
                    } catch (...) {
                        seen.observe(std::current_exception());
                    }
                    require(
                      seen.failed() && failing.calls == 1,
                      "observer timer did not fail exactly once");
                    if (exceptional)
                        require(
                          seen.exception() == failing.exception,
                          "observer timer exception identity changed");
                    else
                        require(
                          seen.error()
                            && seen.error()->code() == errc::resource_exhausted,
                          "observer timer error channel changed");
                }
                auto waiting = ticket.template observe<Clock>(
                  timer, caller, deadline);
                caller.request_abort();
                auto detached = co_await drive.lifecycle(std::move(waiting));
                require(
                  !detached && detached.error().code() == errc::aborted
                    && !ticket.observe() && !groups.failure().failed()
                    && !writer.failure().failed(),
                  "timer/caller detachment refunded storage or "
                  "fenced execution");
                take(co_await drive.lifecycle(groups.close()));
                auto done = co_await take(ticket.observe());
                require(
                  done.receipt() && !done.failure().failed(),
                  "failed observer timers stranded the "
                  "accepted cohort");
            });
      });
}

template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> timer_failure(
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
      [&](auto& writer, auto& work) -> seastar::future<> {
          for (bool exceptional : {false, true}) {
              failed_timer timer;
              if (exceptional)
                  timer.exception = std::make_exception_ptr(
                    std::runtime_error("timer failure"));
              auto groups = take(
                wal_group_commit::make(writer, budget, slow_batch()));
              runtime::first_failure assertion;
              try {
                  const std::array records{assigned_wire()};
                  auto input = co_await append::offer(
                    budget, spec.owner.cluster(), work, records);
                  auto ticket = take(
                    co_await groups->template submit<Clock>(
                      writer, std::move(input), work));
                  auto observed = take(ticket.observe());
                  const auto before = writer.statistics().flush_calls;
                  take(groups->template start<Clock>(timer));
                  auto result = co_await drive.lifecycle(std::move(observed));
                  require(
                    !result.receipt() && result.failure().failed()
                      && !groups->storage_failure().failed()
                      && !writer.failure().failed()
                      && writer.statistics().flush_calls == before + 1
                      && writer.progress()->durable
                           == ticket.boundary().cursor()
                      && timer.calls == 1,
                    "coordinator failure skipped accepted storage work or "
                    "fabricated certification");
                  if (exceptional)
                      require(
                        result.failure().exception() == timer.exception
                          && groups->coordinator_failure().exception()
                               == timer.exception,
                        "timer exception identity changed");
                  else
                      require(
                        result.failure().error()
                          && result.failure().error()->code()
                               == errc::resource_exhausted,
                        "typed timer failure changed channel");
              } catch (...) {
                  assertion.observe(std::current_exception());
              }
              runtime::first_failure closed;
              try {
                  closed.observe(co_await drive.lifecycle(groups->close()));
              } catch (...) {
                  closed.observe(std::current_exception());
              }
              groups.reset();
              take(assertion.outcome());
              require(
                closed.failed()
                  && (!exceptional || closed.exception() == timer.exception),
                "close erased first coordinator failure");
          }
      });
}

template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer,
  typename Stop>
seastar::future<> shutdown(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Timer& timer,
  seastar::abort_source& source,
  Stop stop) {
    co_await append::with_writer(
      files,
      owner,
      spec,
      budget,
      drive,
      wal_writer_contract::configuration(),
      [&](auto& writer, auto& work) -> seastar::future<> {
          co_await with_groups(
            writer,
            budget,
            drive,
            slow_batch(),
            [&](auto& groups) -> seastar::future<> {
                take(groups.bind_shutdown(source));
                take(groups.template start<Clock>(timer));
                const std::array records{assigned_wire()};
                auto input = co_await append::offer(
                  budget, spec.owner.cluster(), work, records);
                auto ticket = take(
                  co_await groups.template submit<Clock>(
                    writer, std::move(input), work));
                seastar::abort_source caller;
                auto interested = ticket.template observe<Clock>(timer, caller);
                std::array<std::optional<workload_reservation>, 32> pressure;
                for (auto& slot : pressure) {
                    auto held = budget.try_reserve(byte_count{4096});
                    if (!held) break;
                    slot.emplace(std::move(*held));
                }
                require(
                  !budget.try_reserve(byte_count{1}),
                  "shutdown pressure did not engage");
                stop();
                budget.close_admission();
                auto closing = groups.close();
                auto duplicate = groups.close();
                take(co_await drive.lifecycle(std::move(closing)));
                take(co_await drive.lifecycle(std::move(duplicate)));
                auto result = take(
                  co_await drive.lifecycle(std::move(interested)));
                take(result.failure().outcome());
                require(
                  result.receipt() && writer.statistics().flush_calls == 1
                    && !groups.coordinator_failure().failed(),
                  "environment abort poisoned or stranded the accepted cohort");
                take(co_await drive.lifecycle(writer.close()));
            });
      },
      false,
      {},
      &source);
}
} // namespace kwaque::storage::testing::wal_commit_lifecycle_contract
