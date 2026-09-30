#pragma once

#include "src/codec/crc32c.h"
#include "src/storage/tests/segment_writer_contract.h"

#include <seastar/core/preempt.hh>
#include <seastar/util/alloc_failure_injector.hh>
#include <seastar/util/defer.hh>

#include <array>
#include <atomic>
#include <new>
#include <optional>
#include <vector>

namespace kwaque::storage {
class segment_writer_test_access final {
public:
    template<
      runtime::file_system_backend Backend,
      typename Owner,
      runtime::monotonic_clock Clock>
    static runtime::file& data(segment_writer<Backend, Owner, Clock>& writer) {
        return writer.data_.value();
    }
};
} // namespace kwaque::storage

namespace kwaque::storage::testing::segment_qualification_contract {
using store_contract::require;
using store_contract::take;
namespace contract = segment_writer_contract;

// Only admission/control calls use this clock. The backend keeps its own I/O
// clock; advancing acceptance time never manufactures a completed device event.
struct controlled_clock final {
    static inline thread_local runtime::monotonic_time current;
    static runtime::monotonic_time now() noexcept { return current; }
};

template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Body>
seastar::future<> with_segment(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  Driver drive,
  segment_writer_config config,
  local_segment_descriptor descriptor,
  Body body) {
    using writer_type = segment_writer<Backend, Owner, Clock>;
    static_assert(
      !requires(writer_type& writer) { writer.truncate(std::uint64_t{}); });
    seastar::abort_source abort;
    codec::cooperative_work work{config.policy, abort};
    co_await installation_contract::bootstrap(
      files, owner, spec, resources, work, drive);
    auto writer = take(
      segment_writer<Backend, Owner, Clock>::make_new(
        files, owner, spec, 0, descriptor, resources, config));
    runtime::first_failure failed;
    try {
        take(co_await drive.lifecycle(writer->create_new(work)));
        co_await body(*writer, work);
    } catch (...) {
        failed.observe(std::current_exception());
    }
    try {
        failed.observe(co_await drive.lifecycle(writer->close()));
    } catch (...) {
        failed.observe(std::current_exception());
    }
    writer.reset();
    take(failed.outcome());
}

template<typename Backend, typename Owner, typename Driver>
seastar::future<> age_boundaries(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  Driver drive,
  bool overflow) {
    const auto original = controlled_clock::current;
    auto restore = seastar::defer(
      [original] noexcept { controlled_clock::current = original; });
    controlled_clock::current = runtime::monotonic_time{100};
    auto config = contract::configuration();
    config.admission.working_bytes = byte_count{1U << 20U};
    auto description = contract::descriptor();
    description.maximum_lifetime = runtime::monotonic_duration{10};
    co_await with_segment<controlled_clock>(
      files,
      owner,
      spec,
      resources,
      drive,
      config,
      description,
      [&](auto& writer, auto& work) -> seastar::future<> {
          require(
            !writer.first_acceptance() && !take(writer.roll_required()),
            "empty segment started age before acceptance");
          controlled_clock::current = runtime::monotonic_time{1000};
          auto batch = co_await contract::child(work);
          {
              auto prepared = take(
                writer.prepare_group(std::span{&batch, 1}, work));
              require(
                prepared.prepared.has_value() && !writer.first_acceptance(),
                "rejectable preparation started segment age");
          }
          if (overflow) {
              controlled_clock::current = runtime::monotonic_time{
                UINT64_MAX - 9};
              const auto rejected = writer.prepare_group(
                std::span{&batch, 1}, work);
              require(
                !rejected && rejected.error().code() == errc::out_of_range
                  && !writer.first_acceptance()
                  && !take(writer.roll_required()),
                "deadline overflow became a roll loop or changed empty state");
              controlled_clock::current = runtime::monotonic_time{
                UINT64_MAX - 10};
          }
          const auto first = controlled_clock::now();
          auto group = co_await contract::freeze_child(
            writer, std::move(batch), resources, work);
          const auto cut = group.layout().boundary();
          require(
            writer.first_acceptance() == first && !take(writer.roll_required())
              && writer.progress()->written.bytes == cut.history().data_start,
            "age did not start at acceptance before encoding/write completion");
          take(co_await writer.encode_group(group, work));
          auto submitted = take(writer.submit(std::move(group), work));
          auto next = co_await contract::execution_child(101, work);
          controlled_clock::current
            = first.checked_add(runtime::monotonic_duration{9}).value();
          auto prepared = take(writer.prepare_group(std::span{&next, 1}, work));
          require(
            prepared.prepared.has_value() && !take(writer.roll_required()),
            "segment rolled before its exact deadline");
          auto held = take(resources.try_reserve_buffer(next.bytes()));
          std::vector<admitted_wal_batch> children;
          children.push_back(take(
            admitted_wal_batch::make(
              std::move(next), std::move(held), charge)));
          controlled_clock::current
            = first.checked_add(description.maximum_lifetime).value();
          const auto rejected = co_await writer.freeze_group(
            std::move(*prepared.prepared), std::move(children), work);
          require(
            !rejected && rejected.error().code() == errc::timed_out
              && writer.progress()->reserved == cut.end()
              && !writer.failure().failed()
              && writer.first_acceptance() == first
              && take(writer.roll_required()),
            "expired preflight escaped coordinates, reset age or fenced "
            "accepted work");
          const auto done = co_await drive.lifecycle(
            std::move(submitted.written));
          take(done.failure.outcome());
          const auto synced = co_await drive.lifecycle(writer.barrier(cut));
          take(synced.failure.outcome());
          require(
            synced.receipt.has_value(),
            "age expiry revoked accepted durability");
          auto later = co_await contract::execution_child(101, work);
          const auto roll = take(
            writer.prepare_group(std::span{&later, 1}, work));
          require(
            roll.decision == segment_capacity_decision::roll_required
              && !roll.prepared,
            "expired active owner admitted another group");
          if (overflow) {
              take(co_await drive.lifecycle(writer.close()));
              controlled_clock::current = runtime::monotonic_time{};
              const local_generation_expectation expected{
                local_publication_generation::make(1).value(),
                {description.segment, local_object_state::active, {}, {}},
                description,
                {}};
              using reopened_type
                = segment_writer<Backend, Owner, controlled_clock>;
              auto reopened = take(
                co_await drive.lifecycle(
                  reopened_type::open_existing(
                    files,
                    owner,
                    spec,
                    0,
                    expected,
                    resources,
                    contract::read_configuration(),
                    work)));
              runtime::first_failure failure;
              try {
                  require(
                    reopened->recovered() && !reopened->first_acceptance()
                      && take(reopened->roll_required()),
                    "restart reused a monotonic timestamp or resumed active "
                    "append");
                  take(co_await drive.lifecycle(reopened->evict_read_handle()));
                  take(
                    co_await drive.lifecycle(
                      reopened->reopen_read_handle(work)));
                  const auto need_roll = take(
                    reopened->prepare_group(std::span{&later, 1}, work));
                  require(
                    need_roll.decision
                        == segment_capacity_decision::roll_required
                      && !reopened->first_acceptance(),
                    "recovered idle reopen restored append or manufactured "
                    "age");
              } catch (...) {
                  failure.observe(std::current_exception());
              }
              try {
                  failure.observe(co_await drive.lifecycle(reopened->close()));
              } catch (...) {
                  failure.observe(std::current_exception());
              }
              reopened.reset();
              take(failure.outcome());
              co_return;
          }
          std::uint32_t calls = 0;
          const auto sealed = co_await drive.lifecycle(writer.seal(
            contract::completed_source{{}, {}, &calls}, 0, 1, work));
          take(sealed.failure.outcome());
          take(co_await drive.lifecycle(writer.reopen_read_handle(work)));
          take(co_await drive.lifecycle(writer.evict_read_handle()));
          take(co_await drive.lifecycle(writer.reopen_read_handle(work)));
          require(
            writer.first_acceptance() == first && !writer.capture(),
            "idle read reopen reset age or restored append authority");
      });
}

template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> reserved_completion(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  Driver drive) {
    auto config = contract::configuration();
    config.admission.working_bytes = byte_count{1U << 20U};
    co_await with_segment<Clock>(
      files,
      owner,
      spec,
      resources,
      drive,
      config,
      contract::descriptor(),
      [&](auto& writer, auto& work) -> seastar::future<> {
          auto group = co_await contract::freeze_child(
            writer, co_await contract::child(work, 4096), resources, work);
          const auto cut = group.layout().boundary();
          take(co_await writer.encode_group(group, work));
          auto submitted = take(writer.submit(std::move(group), work));
          const auto written = co_await drive.lifecycle(
            std::move(submitted.written));
          take(written.failure.outcome());
          auto& file = segment_writer_test_access::data(writer);
          std::array<
            std::optional<runtime::file::metadata_reservation>,
            runtime::maximum_pending_file_metadata_operations>
            pressure;
          std::size_t count = 0;
          while (auto slot = file.try_reserve_metadata())
              pressure[count++].emplace(std::move(*slot));
          require(
            count != 0 && count < pressure.size(),
            "native metadata pressure did not saturate");
          const auto before = resources.snapshot();
          resources.close_admission();
          const auto barrier = co_await drive.lifecycle(writer.barrier(cut));
          // External native reservations must die before their file owner. This
          // is an injected pressure scope, never a second owner of the data FD.
          for (auto& slot : pressure)
              slot.reset();
          take(barrier.failure.outcome());
          require(
            barrier.receipt.has_value(),
            "held metadata unit did not cover the accepted barrier");
          std::uint32_t calls = 0;
          const auto sealed = co_await drive.lifecycle(writer.seal(
            contract::completed_source{{}, {}, &calls}, 0, 1, work));
          take(sealed.failure.outcome());
          require(
            sealed.extent && sealed.unresolved == 1
              && resources.snapshot().accepted == before.accepted
              && resources.snapshot().rejected == before.rejected,
            "finalization reacquired ordinary admission or lost unresolved "
            "facts");
      });
}

template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> freeze_allocation_cut(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  Driver drive,
  std::size_t at) {
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    auto config = contract::configuration();
    config.admission.working_bytes = byte_count{1U << 20U};
    bool injected = false, observed = false;
    co_await with_segment<Clock>(
      files,
      owner,
      spec,
      resources,
      drive,
      config,
      contract::descriptor(),
      [&](auto& writer, auto& work) -> seastar::future<> {
          auto batch = co_await contract::child(work);
          auto prepared = take(
            writer.prepare_group(std::span{&batch, 1}, work));
          auto held = take(resources.try_reserve_buffer(batch.bytes()));
          std::vector<admitted_wal_batch> children;
          children.push_back(take(
            admitted_wal_batch::make(
              std::move(batch), std::move(held), charge)));
          const auto before = take(writer.capture());
          std::optional<seastar::future<runtime::result<segment_frozen_group>>>
            pending;
          auto& injector = seastar::memory::local_failure_injector();
          auto cancel = seastar::defer(
            [&injector] noexcept { injector.cancel(); });
          injector.fail_after(at);
          try {
              pending.emplace(writer.freeze_group(
                std::move(*prepared.prepared), std::move(children), work));
          } catch (const std::bad_alloc&) {
              observed = true;
          }
          injected = injector.failed();
          injector.cancel();
          std::optional<segment_frozen_group> group;
          if (pending) {
              try {
                  group.emplace(
                    take(co_await drive.lifecycle(std::move(*pending))));
              } catch (const std::bad_alloc&) {
                  observed = true;
              }
          }
          require(
            !observed || injected,
            "freeze allocation failure came from outside the selected cut");
          if (!group) {
              require(
                observed && take(writer.capture()) == before
                  && !writer.first_acceptance(),
                "failed freeze published coordinates or started age");
          } else {
              require(
                writer.first_acceptance().has_value()
                  && writer.progress()->reserved
                       == group->layout().boundary().end(),
                "accepted freeze omitted its lifetime or complete footer "
                "position");
              take(co_await writer.encode_group(*group, work));
              auto submitted = take(writer.submit(std::move(*group), work));
              group.reset();
              const auto done = co_await drive.lifecycle(
                std::move(submitted.written));
              take(done.failure.outcome());
          }
      });
    if (at == 0)
        require(
          injected && observed,
          "first ordinary freeze allocation was not exercised");
#else
    static_cast<void>(files);
    static_cast<void>(owner);
    static_cast<void>(spec);
    static_cast<void>(resources);
    static_cast<void>(drive);
    static_cast<void>(at);
    co_return;
#endif
}

template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> close_entered_preflight(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  Driver drive,
  bool sealing) {
    auto config = contract::configuration();
    config.admission.working_bytes = byte_count{1U << 20U};
    co_await with_segment<Clock>(
      files,
      owner,
      spec,
      resources,
      drive,
      config,
      contract::descriptor(),
      [&](auto& writer, auto& work) -> seastar::future<> {
          auto batch = co_await contract::child(work);
          auto prepared = take(
            writer.prepare_group(std::span{&batch, 1}, work));
          auto held = take(resources.try_reserve_buffer(batch.bytes()));
          std::vector<admitted_wal_batch> children;
          children.push_back(take(
            admitted_wal_batch::make(
              std::move(batch), std::move(held), charge)));
          const auto before = writer.progress()->reserved;
          auto pending = [&] {
              seastar::internal::preemption_monitor requested{};
              requested.head.store(1, std::memory_order_relaxed);
              const auto* previous = seastar::internal::get_need_preempt_var();
              auto restore = seastar::defer([previous] noexcept {
                  seastar::internal::set_need_preempt_var(previous);
              });
              seastar::internal::set_need_preempt_var(&requested);
              return writer.freeze_group(
                std::move(*prepared.prepared), std::move(children), work);
          }();
          const bool suspended = !pending.available();
          seastar::abort_source seal_abort;
          codec::cooperative_work seal_admission{work.policy(), seal_abort};
          std::uint32_t calls = 0;
          std::optional<seastar::future<segment_seal_outcome>> seal;
          std::optional<seastar::future<runtime::result<void>>> close;
          if (sealing)
              seal.emplace(writer.seal(
                contract::completed_source{{}, {}, &calls},
                0,
                0,
                seal_admission));
          else
              close.emplace(writer.close());
          const auto frozen = co_await drive.lifecycle(std::move(pending));
          runtime::first_failure settled;
          if (seal)
              settled = (co_await drive.lifecycle(std::move(*seal))).failure;
          if (close)
              settled.observe(co_await drive.lifecycle(std::move(*close)));
          take(settled.outcome());
          require(
            suspended && !frozen && !writer.failure().failed()
              && writer.progress()->reserved == before
              && !writer.first_acceptance(),
            "terminal operation failed to join/reject entered preflight "
            "without escaping coordinates");
      });
}

template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> paged_seal_and_retained_results(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  Driver drive,
  bool preallocated = false) {
    auto config = contract::configuration();
    config.admission.working_bytes = byte_count{1U << 20U};
    config.maximum_groups = 1;
    // Zero-written past the sealed end: groups take synchronized writes and
    // sealing must truncate back to the exact end.
    if (preallocated) config.preallocation_bytes = byte_count{1U << 20U};
    auto policy = config.policy.config();
    policy.max_page_bytes = byte_count{4096};
    config.policy = codec::limits::make(policy).value();
    auto description = contract::descriptor();
    description.alignment = alignment(4096);
    co_await with_segment<Clock>(
      files,
      owner,
      spec,
      resources,
      drive,
      config,
      description,
      [&](auto& writer, auto& work) -> seastar::future<> {
          auto fact_grant = take(resources.try_reserve(byte_count{16384}));
          std::vector<completed_retry> facts;
          facts.reserve(32);
          codec::crc32c checksum;
          codec::xxh3_128_hasher hash;
          std::optional<storage::coverage> last;
          std::optional<segment_captured_boundary> end;
          std::optional<segment_write_completion> retained;
          std::optional<std::uint64_t> steady_tasks;
          for (std::uint64_t i = 0; i < 32; ++i) {
              if (i == 2) {
                  take(co_await writer.digest_caught_up());
                  steady_tasks = resources.snapshot().tasks;
              }
              auto batch = co_await contract::execution_child(100 + i, work);
              const auto info = batch.info();
              const auto original = info.context.submitted();
              facts.push_back(
                completed_retry::make(
                  original.id(),
                  info.fingerprint,
                  original.binding(),
                  info.context.logical_span(),
                  original.binding().generation())
                  .value());
              auto group = co_await contract::freeze_child(
                writer, std::move(batch), resources, work);
              take(co_await writer.encode_group(group, work));
              const auto cut = group.layout().boundary();
              end = cut;
              const auto block = flat(group.blocks()[0].bytes());
              last = group.layout().blocks()[0].records;
              checksum.extend(std::span<const char>{block});
              hash.update(block.data(), block.size());
              const auto footer = footer_wire(
                {cut.covered(), cut.end().blocks, last, checksum.value()},
                {cut.history(), cut.footer()->begin()});
              checksum.extend(std::span<const char>{footer});
              hash.update(footer.data(), footer.size());
              auto submitted = take(writer.submit(std::move(group), work));
              auto done = co_await drive.lifecycle(
                std::move(submitted.written));
              take(done.failure.outcome());
              if (i == 0) retained.emplace(std::move(done));
              if (i == 1) {
                  const auto held = resources.snapshot();
                  retained.reset();
                  const auto released = resources.snapshot();
                  require(
                    released.tasks + 1 == held.tasks
                      && released.bytes < held.bytes,
                    "retired result did not retain/release its independently "
                    "charged evidence");
              }
          }
          // Retry capacity grows with accepted groups; the workload task
          // units the owner holds must not.
          take(co_await writer.digest_caught_up());
          require(
            resources.snapshot().tasks == *steady_tasks,
            "accepted groups kept a workload task unit each until seal");
          const auto digest = std::move(hash).final();
          const auto coverage = storage::coverage{
            end->covered().logical(),
            end->covered().physical(),
            model::file_byte_span::make(
              end->history().data_start, end->end().bytes)
              .value()};
          const footer_expectation location{end->history(), end->end().bytes};
          const auto capacity = retry_page_capacity(
                                  byte_count{32},
                                  description.alignment,
                                  config.policy)
                                  .value();
          std::vector<page_ref> refs;
          refs.reserve((facts.size() + capacity - 1) / capacity);
          for (std::uint32_t first = 0; first < facts.size();) {
              const auto count = std::min(
                capacity, static_cast<std::uint32_t>(facts.size() - first));
              const auto ordinal = static_cast<std::uint32_t>(refs.size());
              const auto wire = retry_page_wire(
                std::span{facts}.subspan(first, count),
                location,
                ordinal,
                first);
              refs.push_back(reference(wire, ordinal, first, count));
              first += count;
          }
          require(
            refs.size() == 2,
            "qualified page geometry did not cross a page boundary");
          const auto expected = sealed_wire(
            {coverage, 32, last, 0}, digest, refs, location);
          const auto before = resources.snapshot();
          resources.close_admission();
          std::uint32_t calls = 0;
          const auto sealed = co_await drive.lifecycle(writer.seal(
            contract::completed_source{
              std::move(fact_grant), std::move(facts), &calls},
            32,
            0,
            work));
          take(sealed.failure.outcome());
          require(
            sealed.extent && sealed.boundary && sealed.retry
              && sealed.retry->pages().value() == 2 && calls == 4
              && sealed.extent->digest.bytes() == digest
              && sealed.extent->coverage == coverage
              && sealed.boundary->digest().bytes() == exact_digest(expected)
              && resources.snapshot().accepted == before.accepted
              && resources.snapshot().rejected == before.rejected,
            "paged finalization lost facts, changed history or reacquired "
            "ordinary capacity");
          const auto path = take(
            local_paths::make(spec.root)->segment_file(
              0,
              {description.segment.segment(), description.segment.generation()},
              local_segment_file::data));
          auto file = take(
            co_await drive.lifecycle(files.open(
              path, {.close_policy = runtime::file_close_policy::checked})));
          runtime::first_failure failed;
          try {
              const auto read = take(
                co_await drive.lifecycle(
                  file.read(location.position, byte_count{expected.size()})));
              require(
                read.data().content_equals(expected),
                "stored paged sealed root differs from the independent oracle");
              const auto size = take(co_await drive.lifecycle(file.size()));
              require(
                size == location.position.value() + expected.size(),
                "sealed extent does not end exactly after its root");
          } catch (...) {
              failed.observe(std::current_exception());
          }
          failed.observe(co_await drive.lifecycle(file.close()));
          take(failed.outcome());
      });
}

} // namespace kwaque::storage::testing::segment_qualification_contract
