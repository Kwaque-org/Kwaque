#pragma once

#include "src/base/units.h"
#include "src/storage/recovery_inventory.h"
#include "src/storage/recovery_plan.h"
#include "src/storage/segment_writer.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>

#include <algorithm>
#include <array>
#include <exception>
#include <optional>
#include <span>
#include <utility>
#include <vector>

namespace kwaque::storage {

// The logical end one segment's owner supplies as visible. Recovery never
// derives it from local bytes.
struct recovery_visibility final {
    segment_context segment;
    model::range_logical_end end;
};
// Where local reads of one recovered segment stop.
struct recovery_read_bound final {
    model::range_logical_end end{};
    // The supplied visible end lies after the recovered boundary: this copy
    // cannot serve it. A report of local damage, never a lower visible end.
    bool short_of_visibility{false};
    bool operator==(const recovery_read_bound&) const = default;
};
// Reads stop at the smaller of the supplied visible end and the recovered
// boundary's covered end, and never before the segment's logical origin.
// Candidates and the suffix lie after the boundary, so they stay outside
// reads; without a supplied end nothing is readable.
[[nodiscard]] recovery_read_bound bound_recovered_reads(
  model::range_logical_end origin,
  model::range_logical_end recovered,
  std::optional<model::range_logical_end> visible) noexcept;

struct recovered_segment_outcome final {
    segment_context segment;
    recovery_plan_action action{recovery_plan_action::retain};
    // The fresh flush completed and the recovering publication pinning the
    // recovered boundary is durable.
    bool published{false};
    recovery_read_bound reads;
    std::optional<runtime::operation_error> failure;
};
struct recovered_publications final {
    workload_reservation held;
    // One per active or recovering target, in target order.
    std::vector<recovered_segment_outcome> segments;
    // Every planned recovering publication is durable. Only then, and once
    // the planned WAL successor is active, may the shard become ready.
    bool complete{false};
};
struct recovered_publication_limits final {
    // Segments flushed and published at once. A job holds one recovered
    // writer's metadata operation, not an append workspace, and concurrent
    // flushes share the block layer's cache flushes, so the most allowed run.
    std::uint32_t jobs{maximum_recovery_jobs};
    [[nodiscard]] runtime::result<void> validate() const noexcept;
};

// Establishes fresh durability for every recovered segment whose classified
// prefix will seed evidence, then publishes it as recovering. A ready plan
// only: each `publish_recovering` segment is opened as the inventory found it,
// gets one new flush of its data file, and only after that succeeded a
// recovering publication pinning its recovered boundary, never below its pin.
// `retain` segments are not touched. Surviving bytes prove no earlier flush,
// so nothing is published on them alone. A failed or uncertain flush or
// publication leaves that segment as published before and the result
// incomplete: the shard stays out of ready, nothing it holds can seed a
// checkpoint or reclamation, and the next restart classifies it again. Up to
// `limits.jobs` segments run at once, each with its own cooperative work; the
// caller's work is polled before each, and an abort fails the call. A segment
// refused admission beside other jobs is published again alone.
// `visible` are the owner's supplied visible ends; each outcome's read bound
// honours them. The plan and inventory must come from the same targets.
template<
  runtime::monotonic_clock Clock,
  runtime::file_system_backend Backend,
  local_directory_owner Owner>
seastar::future<runtime::result<recovered_publications>>
publish_recovered_segments(
  Backend& files,
  Owner& ownership,
  std::span<const local_device_spec> devices,
  std::uint32_t shard,
  const recovery_planner& plan,
  const recovery_inventory& inventory,
  std::span<const recovery_visibility> visible,
  workload_budget& budget,
  segment_writer_config config,
  recovered_publication_limits limits,
  codec::cooperative_work& work) {
    using writer_type = segment_writer<Backend, Owner, Clock>;
    if (auto valid = limits.validate(); !valid)
        co_return runtime::failure(valid.error());
    if (auto valid = validate_local_device_set(devices); !valid)
        co_return runtime::failure(valid.error());
    // A plan that stopped authorizes nothing.
    if (!plan.ready())
        co_return runtime::failure(detail::path_error(errc::wrong_context));
    const auto planned = plan.segments();
    if (planned.size() != inventory.targets.size())
        co_return runtime::failure(detail::path_error(errc::invalid_argument));
    std::size_t scanned = 0;
    for (std::size_t i = 0; i < planned.size(); ++i) {
        const auto& target = inventory.targets[i];
        if (planned[i].segment != target.descriptor.segment)
            co_return runtime::failure(
              detail::path_error(errc::invalid_argument));
        if (target.state != local_object_state::sealed) ++scanned;
    }
    if (auto ready = work.poll(); !ready)
        co_return runtime::failure(detail::path_error(ready.error().code()));
    // Each publication: its outcome, the inventory entry and the target.
    struct job final {
        std::size_t outcome, entry, target;
    };
    std::size_t jobs = 0;
    for (const auto& segment : planned)
        if (segment.action == recovery_plan_action::publish_recovering) ++jobs;
    byte_count charge;
    for (const auto bytes :
         {scanned * sizeof(recovered_segment_outcome), jobs * sizeof(job)}) {
        if (bytes == 0) continue;
        auto charged = budget.allocation_charge(byte_count{bytes});
        if (!charged) co_return runtime::failure(charged.error());
        auto next = charge.checked_add(*charged);
        if (!next)
            co_return runtime::failure(detail::path_error(errc::out_of_range));
        charge = *next;
    }
    auto held = budget.try_reserve(charge);
    if (!held) co_return runtime::failure(held.error());
    recovered_publications output{std::move(*held), {}, false};
    output.segments.reserve(scanned);
    std::vector<job> queue;
    queue.reserve(jobs);
    for (std::size_t i = 0, entry = 0; i < planned.size(); ++i, ++entry) {
        while (entry < inventory.entries.size()
               && inventory.entries[entry].state
                    != recovery_entry_state::target)
            ++entry;
        if (entry == inventory.entries.size())
            co_return runtime::failure(
              detail::path_error(errc::invalid_argument));
        const auto& target = inventory.targets[i];
        if (target.state == local_object_state::sealed) continue;
        const auto& segment_plan = planned[i];
        std::optional<model::range_logical_end> supplied;
        for (const auto& value : visible)
            if (value.segment == segment_plan.segment) supplied = value.end;
        if (segment_plan.action == recovery_plan_action::publish_recovering)
            queue.push_back({output.segments.size(), entry, i});
        output.segments.push_back(
          {segment_plan.segment,
           segment_plan.action,
           false,
           bound_recovered_reads(
             target.descriptor.logical_origin,
             segment_plan.recovered_end,
             supplied),
           {}});
    }
    // One segment: open it as the inventory found it, publish, close.
    auto publish_one = [&](job item, codec::cooperative_work& job_work)
      -> seastar::future<runtime::result<void>> {
        const auto& found = inventory.entries[item.entry];
        const auto spec = std::find_if(
          devices.begin(), devices.end(), [&found](const auto& value) {
              return value.owner.device() == found.device;
          });
        if (spec == devices.end())
            co_return runtime::failure(detail::path_error(errc::wrong_context));
        auto contexts = detail::recovery_root_contexts(
          *spec, shard, found.history, found.publication, nullptr);
        if (!contexts) co_return runtime::failure(contexts.error());
        auto opened = co_await writer_type::open_existing(
          files,
          ownership,
          *spec,
          shard,
          {found.generation,
           found.publication,
           inventory.targets[item.target].descriptor,
           *contexts},
          budget,
          config,
          job_work);
        if (!opened) co_return runtime::failure(opened.error());
        auto writer = std::move(*opened);
        runtime::first_failure failed;
        try {
            failed.observe(
              co_await writer->publish_recovering(
                planned[item.target].boundary, job_work));
        } catch (...) {
            failed.observe(std::current_exception());
        }
        try {
            failed.observe(co_await writer->close());
        } catch (...) {
            failed.observe(std::current_exception());
        }
        if (failed.exception()) std::rethrow_exception(failed.exception());
        co_return failed.outcome();
    };
    // The caller's work is polled before each segment; once it is aborted no
    // further segment starts.
    std::exception_ptr thrown;
    std::optional<runtime::operation_error> aborted;
    std::size_t next = 0;
    auto worker = [&]() -> seastar::future<> {
        seastar::abort_source abort;
        codec::cooperative_work job_work{work.policy(), abort};
        while (next < queue.size() && !aborted) {
            if (auto ready = work.poll(); !ready) {
                aborted = detail::path_error(ready.error().code());
                break;
            }
            const auto item = queue[next++];
            auto& outcome = output.segments[item.outcome];
            try {
                auto done = co_await publish_one(item, job_work);
                if (done)
                    outcome.published = true;
                else
                    outcome.failure = done.error();
            } catch (...) {
                if (!thrown) thrown = std::current_exception();
            }
        }
    };
    std::array<std::optional<seastar::future<>>, maximum_recovery_jobs> running;
    const auto workers = std::min<std::size_t>(limits.jobs, queue.size());
    for (std::size_t k = 0; k < workers; ++k)
        running[k].emplace(worker());
    for (std::size_t k = 0; k < workers; ++k)
        co_await std::move(*running[k]);
    if (thrown) std::rethrow_exception(thrown);
    if (aborted) co_return runtime::failure(*aborted);
    // A refusal beside other jobs is retried alone: a refusal now is the
    // one-at-a-time outcome.
    for (const auto& item : queue) {
        auto& outcome = output.segments[item.outcome];
        if (
          outcome.published || !outcome.failure
          || !detail::recovery_admission_refused(*outcome.failure))
            continue;
        if (auto ready = work.poll(); !ready)
            co_return runtime::failure(
              detail::path_error(ready.error().code()));
        auto done = co_await publish_one(item, work);
        outcome.published = done.has_value();
        outcome.failure.reset();
        if (!done) outcome.failure = done.error();
    }
    output.complete = std::all_of(
      queue.begin(), queue.end(), [&output](const job& item) {
          return output.segments[item.outcome].published;
      });
    co_return output;
}

} // namespace kwaque::storage
