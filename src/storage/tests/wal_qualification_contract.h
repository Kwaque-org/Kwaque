#pragma once

#include "src/storage/tests/wal_lifecycle_contract.h"
#include "src/storage/wal_writer_state.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/preempt.hh>
#include <seastar/util/alloc_failure_injector.hh>

namespace kwaque::storage::testing::wal_qualification_contract {
using store_contract::require;
using store_contract::take;

// Header-only is a legitimate selected empty WAL; zero-byte, junk and
// unsupported pinned headers are not alternate bootstrap opportunities.
template<typename Backend, typename Owner, typename Driver>
seastar::future<> pinned_headers(
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
          const auto head = *writer.prepared_head();
          const auto path = take(
            take(local_paths::make(spec.root)).wal(0, head.incarnation));
          const auto original = wal_writer_contract::header_bytes(
            spec, 0, head.incarnation);
          take(co_await drive.lifecycle(writer.close()));
          const auto anchor = take(writer.inventory_snapshot());
          auto chain = [](const local_wal_chain_entry&) {
              return seastar::make_ready_future<runtime::result<bool>>(true);
          };
          auto names =
            [](const local_discovered_record&, local_cleanup_observation) {
                return seastar::make_ready_future<runtime::result<bool>>(true);
            };
          auto inspect = [&] {
              return inspect_wal_inventory(
                files,
                owner,
                spec,
                anchor,
                {},
                wal_inventory_intent::require_head,
                budget,
                store_contract::limits(),
                work,
                chain,
                names);
          };
          auto valid = take(co_await drive.lifecycle(inspect()));
          require(
            valid.chain.complete && valid.chain.visited == 1,
            "correctly pinned header-only WAL rejected");
          for (unsigned kind = 0; kind != 4; ++kind) {
              if (kind == 0) {
                  auto file = take(
                    co_await drive.lifecycle(files.open(
                      path,
                      {.access = runtime::file_access::read_write,
                       .truncate = true,
                       .close_policy = runtime::file_close_policy::checked})));
                  runtime::first_failure failed;
                  try {
                      failed.observe(co_await drive.lifecycle(file.flush()));
                  } catch (...) {
                      failed.observe(std::current_exception());
                  }
                  failed.observe(co_await drive.lifecycle(file.close()));
                  take(failed.outcome());
              } else {
                  auto damaged = kind == 1 ? std::string(8192, 'x') : original;
                  if (kind == 2) {
                      put(damaged, 6, 99, 2);
                      repair(damaged);
                  }
                  if (kind == 3) {
                      put(damaged, 140, 2U * 1048576U, 8);
                      repair(damaged);
                  }
                  co_await store_contract::write_bytes(
                    files, path, std::move(damaged), drive);
              }
              auto rejected = co_await drive.lifecycle(inspect());
              require(!rejected, "invalid pinned WAL was accepted");
              const auto code = rejected.error().code();
              require(
                code == errc::corrupt_data || code == errc::malformed_data
                  || code == errc::unsupported_format
                  || code == errc::wrong_context
                  || code == errc::truncated_data,
                "invalid pinned WAL returned an unrelated failure");
          }
          co_await store_contract::write_bytes(files, path, original, drive);
          take(co_await drive.lifecycle(inspect()));
      });
}

template<typename Backend, typename Owner, typename Driver>
seastar::future<> bounded_admission(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    auto config = wal_writer_contract::configuration();
    config.maximum_descriptors = 1;
    // The queue caps both logical bytes and retained backing. One 8-KiB
    // envelope needs room for allocator-rounded padding and child storage.
    config.maximum_pending_bytes = byte_count{32768};
    co_await wal_append_contract::with_writer(
      files,
      owner,
      spec,
      budget,
      drive,
      config,
      [&](auto& writer, auto& work) -> seastar::future<> {
          const auto head = *writer.prepared_head();
          const std::array records{assigned_wire()};
          const std::array oversize{
            assigned_wire(),
            assigned_wire(),
            assigned_wire(),
            assigned_wire(),
            assigned_wire()};
          const auto initial = take(writer.capture());
          auto too_large = co_await wal_append_contract::offer(
            budget, spec.owner.cluster(), work, oversize);
          auto rejected = co_await writer.submit(std::move(too_large), work);
          require(
            !rejected && rejected.error().code() == errc::resource_exhausted
              && writer.statistics().accepted_groups == 0
              && writer.statistics().write_calls == 0
              && writer.progress()->reserved == initial.cursor(),
            "whole-group byte cap admitted a prefix");
          auto input = co_await wal_append_contract::offer(
            budget, spec.owner.cluster(), work, records);
          auto other = co_await wal_append_contract::offer(
            budget, spec.owner.cluster(), work, records);
          auto pending = [&] {
              seastar::internal::preemption_monitor requested{};
              requested.head.store(1, std::memory_order_relaxed);
              const auto* previous = seastar::internal::get_need_preempt_var();
              auto restore = seastar::defer([previous] noexcept {
                  seastar::internal::set_need_preempt_var(previous);
              });
              seastar::internal::set_need_preempt_var(&requested);
              return writer.submit(std::move(input), work);
          }();
          const bool suspended = !pending.available();
          seastar::abort_source separate_abort;
          codec::cooperative_work separate{work.policy(), separate_abort};
          auto overlap = co_await writer.submit(std::move(other), separate);
          auto accepted = take(co_await drive.lifecycle(std::move(pending)));
          require(
            suspended && !overlap && overlap.error().code() == errc::queue_full,
            "preflight failed to yield with bounded nonwaiting overlap");
          auto written = co_await drive.lifecycle(std::move(accepted.written));
          take(written.failure.outcome());
          auto flush = co_await drive.lifecycle(
            writer.barrier(accepted.boundary));
          take(flush.failure.outcome());
          require(flush.receipt.has_value(), "bounded admission lost barrier");
          const auto calls = writer.statistics().flush_calls;
          auto reuse = co_await drive.lifecycle(
            writer.barrier(accepted.boundary));
          take(reuse.failure.outcome());
          require(
            writer.statistics().flush_calls == calls && calls == 1,
            "same captured cut performed redundant flushes");
          const auto path = take(
            take(local_paths::make(spec.root)).wal(0, head.incarnation));
          require(
            (co_await store_contract::read_bytes(files, path, drive))
              == wal_writer_contract::header_bytes(spec, 0, head.incarnation)
                   + wal_append_contract::prepare_bytes(
                     records[0], head, spec.owner.cluster(), 8192),
            "admission qualification changed independent output bytes");
      });
}

template<typename Backend, typename Owner, typename Driver>
seastar::future<> accepted_encoding_allocation_cut(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION) && !defined(SEASTAR_DEBUG)
    // The always-preempt native debug mode has no synchronous admission
    // checkpoint. This cut targets the native/injection profile; ordinary
    // preflight and asynchronous I/O tests still cover the other profiles.
    bool injected = false, close_failed = false;
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
              const std::array records{assigned_wire()};
              auto group = co_await wal_append_contract::offer(
                budget, spec.owner.cluster(), work, records);
              const auto before = *writer.progress();
              seastar::abort_source caller_abort;
              codec::cooperative_work caller{work.policy(), caller_abort};
              // The checked child reuses semantic validation. Suppress native
              // preemption only during this synchronous admission so encoding
              // stays queued in its separate workload scheduling group.
              auto pending = [&] {
                  seastar::internal::preemption_monitor uninterrupted{};
                  const auto* previous
                    = seastar::internal::get_need_preempt_var();
                  auto restore = seastar::defer([previous] noexcept {
                      seastar::internal::set_need_preempt_var(previous);
                  });
                  seastar::internal::set_need_preempt_var(&uninterrupted);
                  return writer.submit(std::move(group), caller);
              }();
              const bool checkpoint = pending.available()
                                      && writer.statistics().accepted_groups
                                           == 1
                                      && writer.statistics().encoded_groups == 0
                                      && writer.statistics().write_calls == 0;
              auto accepted = take(
                co_await seastar::coroutine::without_preemption_check(
                  std::move(pending)));
              auto& injector = seastar::memory::local_failure_injector();
              if (checkpoint) injector.fail_after(0);
              std::optional<::kwaque::storage::detail::wal_write_completion>
                completion;
              std::exception_ptr unexpected;
              try {
                  completion.emplace(
                    co_await drive.lifecycle(std::move(accepted.written)));
              } catch (...) {
                  unexpected = std::current_exception();
              }
              injected = checkpoint && injector.failed();
              injector.cancel();
              if (unexpected) std::rethrow_exception(unexpected);
              observed = completion->failure.exception();
              require(
                checkpoint && injected && observed,
                "accepted encoding allocation cut did not execute");
              require(
                writer.failure().exception() == observed
                  && writer.statistics().encoded_groups == 0
                  && writer.statistics().write_calls == 0
                  && writer.progress()->reserved == accepted.boundary.cursor()
                  && writer.progress()->reserved.position().value()
                       == before.reserved.position().value() + 8192
                  && writer.progress()->write_complete == before.write_complete
                  && writer.progress()->durable == before.durable,
                "encoding OOM lost acceptance, dispatched bytes or certified a "
                "failed group");
              auto barrier = co_await writer.barrier(accepted.boundary);
              require(
                !barrier.receipt && barrier.failure.exception() == observed,
                "encoding OOM supplied a barrier receipt or changed its "
                "exception");
          });
    } catch (const std::bad_alloc&) {
        require(
          injected && observed == std::current_exception(),
          "joined close changed the accepted encoding failure");
        close_failed = true;
    }
    require(
      injected && observed && close_failed,
      "accepted encoding OOM was not observed through joined close");
#else
    static_cast<void>(files);
    static_cast<void>(owner);
    static_cast<void>(spec);
    static_cast<void>(budget);
    static_cast<void>(drive);
    co_return;
#endif
}

template<typename Backend, typename Owner, typename Driver>
seastar::future<> submission_allocation_cut(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  std::size_t at) {
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    bool injected = false, saw_allocation_failure = false;
    try {
        co_await wal_append_contract::with_writer(
          files,
          owner,
          spec,
          budget,
          drive,
          wal_writer_contract::configuration(),
          [&](auto& writer, auto& work) -> seastar::future<> {
              const std::array records{assigned_wire(true, 48)};
              auto group = co_await wal_append_contract::offer(
                budget, spec.owner.cluster(), work, records);
              const auto before = take(writer.capture());
              std::optional<seastar::future<runtime::result<wal_submission>>>
                pending;
              auto& injector = seastar::memory::local_failure_injector();
              injector.fail_after(at);
              try {
                  pending.emplace(writer.submit(std::move(group), work));
              } catch (const std::bad_alloc&) {
                  saw_allocation_failure = true;
              }
              injected = injector.failed();
              injector.cancel();
              bool accepted = false;
              if (pending) {
                  try {
                      auto result = co_await drive.lifecycle(
                        std::move(*pending));
                      require(
                        result.has_value(),
                        "allocation cut produced unrelated admission failure");
                      accepted = true;
                      auto done = co_await drive.lifecycle(
                        std::move(result->written));
                      take(done.failure.outcome());
                  } catch (const std::bad_alloc&) {
                      saw_allocation_failure = true;
                  }
              }
              require(
                !saw_allocation_failure || injected,
                "allocation failure occurred outside the selected cut");
              const auto published = writer.statistics().accepted_groups;
              require(
                published <= 1 && (!accepted || published == 1)
                  && writer.progress()->reserved.position().value()
                       == before.cursor().position().value()
                            + published * 8192U,
                "allocation failure reused or fabricated accepted coordinates");
          });
    } catch (const std::bad_alloc&) {
        require(
          injected && saw_allocation_failure,
          "joined close introduced an unrelated allocation failure");
    }
    if (at == 0)
        require(
          injected && saw_allocation_failure,
          "allocation entrance cut was not exercised");
#else
    static_cast<void>(files);
    static_cast<void>(owner);
    static_cast<void>(spec);
    static_cast<void>(budget);
    static_cast<void>(drive);
    static_cast<void>(at);
    co_return;
#endif
}
} // namespace kwaque::storage::testing::wal_qualification_contract
