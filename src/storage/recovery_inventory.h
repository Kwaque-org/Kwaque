#pragma once

#include "src/base/units.h"
#include "src/storage/local_discovery.h"
#include "src/storage/local_metadata_file.h"
#include "src/storage/local_store_inspection.h"
#include "src/storage/page_internal.h"
#include "src/storage/recovery_decision.h"
#include "src/storage/recovery_pins.h"

#include <seastar/core/coroutine.hh>

#include <algorithm>
#include <array>
#include <exception>
#include <optional>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

namespace kwaque::storage {

// What restart concludes about one device, or the configured set, before any
// mutation. Ordered by severity; the set takes its most severe device.
enum class recovery_store_verdict : std::uint8_t {
    // An existing store this configuration owns.
    ready,
    // A read or ownership check failed: never absent, empty or torn.
    unavailable,
    // Required metadata is missing.
    incomplete,
    // Nothing where a store must exist: a lost disk never looks empty.
    lost,
    // A newer or unknown format: its bytes stay as they are.
    unsupported,
    // Metadata fails integrity or names another store or device.
    corrupt,
};
struct recovery_device_report final {
    recovery_store_verdict verdict{recovery_store_verdict::ready};
    // The store's classification, when its inspection ran to the end.
    std::optional<local_store_report> store;
    // What made the device anything but ready.
    std::optional<runtime::operation_error> error;
};
struct recovery_store_report final {
    // In configuration order.
    std::array<recovery_device_report, maximum_local_devices> devices{};
    std::size_t inspected{0};
    recovery_store_verdict verdict{recovery_store_verdict::ready};
};

// The verdict a failed read of authoritative metadata establishes, or nothing
// for a failure of the operation itself (budget, abort, a bad argument).
[[nodiscard]] std::optional<recovery_store_verdict>
  recovery_failure_verdict(errc) noexcept;
// A must-exist store's classification as a restart verdict.
[[nodiscard]] recovery_store_verdict
  recovery_state_verdict(local_store_state) noexcept;

// Inspects every configured device before restart changes anything: its
// ownership, its identity and the metadata its controls pin, as a store that
// must exist. Every device is inspected, so a set with several failed devices
// reports each. A pristine device is lost, never an empty store to start
// again; a store naming another identity or device is corrupt; a newer format
// is unsupported. Nothing is written, created or bootstrapped. A store whose
// WAL was never activated is ready; its WAL owner decides that.
template<runtime::file_system_backend Backend, local_directory_owner Owner>
seastar::future<runtime::result<recovery_store_report>> inspect_local_recovery(
  Backend& files,
  Owner& ownership,
  std::span<const local_device_spec> devices,
  workload_budget& budget,
  local_store_io_limits limits,
  codec::cooperative_work& work) {
    if (auto valid = validate_local_device_set(devices); !valid)
        co_return runtime::failure(valid.error());
    if (auto valid = limits.validate(); !valid)
        co_return runtime::failure(valid.error());
    const local_store_open_options options{
      local_store_intent::must_exist, false};
    recovery_store_report output;
    for (const auto& spec : devices) {
        auto& device = output.devices[output.inspected++];
        auto report = co_await inspect_local_store(
          files, ownership, spec, options, budget, limits, work);
        if (!report) {
            const auto verdict = recovery_failure_verdict(
              report.error().code());
            if (!verdict) co_return runtime::failure(report.error());
            device.verdict = *verdict;
            device.error = report.error();
        } else {
            device.verdict = recovery_state_verdict(report->state);
            device.error = report->reason;
            device.store = std::move(*report);
        }
        output.verdict = std::max(output.verdict, device.verdict);
    }
    co_return output;
}

// What one discovery of a shard's decisions found besides its records.
struct recovery_decision_discovery final {
    std::uint32_t decisions{0};
    // Temporaries of publications that never completed: never decisions.
    std::uint32_t temporaries{0};
    // Records naming a segment outside the catalog: retained, never acted on
    // here.
    std::uint32_t unmatched{0};
    // False when the visitor stopped early.
    bool complete{false};
};

// Every durable decision record of one shard, after a restart. A record a
// publication renamed into place survives a process restart even when that
// publication's directory sync never ran, and a power loss could still remove
// it; so the decisions directory is synced once before anything is read, and
// no record is acted on that is not durable. Each <sequence>.meta is read
// exactly, hashed whole into the pin its log minted, and its segment claim
// resolved against the owner's catalog before it is decoded under the full
// expectation. A sequence above the control's decision high mark was never
// allocated: wrong_context. Temporaries are counted and never read. Nothing
// is changed but the directory's durability.
// visit(const recovery_decision_record&) returns future<result<bool>>; false
// stops the discovery.
template<
  runtime::file_system_backend Backend,
  local_directory_owner Owner,
  typename Visitor>
seastar::future<runtime::result<recovery_decision_discovery>>
discover_recovery_decisions(
  Backend& files,
  Owner& ownership,
  const local_device_spec& control,
  std::uint32_t shard,
  const local_shard_control& fields,
  std::span<const local_segment_descriptor> catalog,
  workload_budget& budget,
  recovery_decision_limits limits,
  codec::cooperative_work& work,
  Visitor visit) {
    static_assert(sizeof(Visitor) <= 4_KiB);
    if (auto valid = limits.validate(); !valid)
        co_return runtime::failure(valid.error());
    const auto owner = control.shard_owner(shard);
    if (!owner || !control.controls())
        co_return runtime::failure(detail::path_error(errc::wrong_context));
    if (auto valid = co_await ownership.validate(control); !valid)
        co_return runtime::failure(valid.error());
    const auto paths = local_paths::make(control.root);
    if (!paths) co_return runtime::failure(paths.error());
    auto first = paths->sequence_file(shard, local_sequence_file::decision, 1);
    if (!first) co_return runtime::failure(first.error());
    auto directory = runtime::file_path::make(
      first->value().substr(0, first->value().rfind('/')));
    if (!directory) co_return runtime::failure(directory.error());
    auto held = budget.try_reserve(
      byte_count{limits.metadata.execution_bytes.value() + 16_KiB});
    if (!held) co_return runtime::failure(held.error());
    if (auto handles = held->try_acquire_handles(1); !handles)
        co_return runtime::failure(handles.error());
    if (
      auto inspected = co_await inspect_local_path(
        files, control.root, *directory, runtime::file_kind::directory, work);
      !inspected)
        co_return runtime::failure(inspected.error());
    auto opened = co_await files.open_directory(
      *directory, runtime::file_close_policy::checked);
    if (!opened) co_return runtime::failure(opened.error());
    auto cursor = std::move(*opened);
    recovery_decision_discovery output;
    runtime::first_failure failed;
    // One record: exact bytes, their pin, the claim, then the full decode.
    auto record = [&](const runtime::file_path& path, std::uint64_t sequence)
      -> seastar::future<runtime::result<bool>> {
        if (sequence > fields.decision_high.value())
            co_return runtime::failure(detail::path_error(errc::wrong_context));
        if (output.decisions == limits.decisions)
            co_return runtime::failure(
              detail::path_error(errc::resource_exhausted));
        auto raw = co_await read_local_metadata_file(
          files,
          control.root,
          path,
          budget,
          limits.metadata,
          work,
          local_metadata_extent::exact_file);
        if (!raw) co_return runtime::failure(raw.error());
        auto digest = co_await detail::hash_exact(raw->bytes, work, {});
        if (!digest)
            co_return runtime::failure(
              detail::path_error(digest.error().code()));
        const auto generation = local_publication_generation::make(sequence);
        const auto decided = local_decision_sequence::make(sequence);
        if (!generation || !decided)
            co_return runtime::failure(detail::path_error(errc::wrong_context));
        std::optional<segment_context> claimed;
        {
            bytes::fragmented_buffer_parser input{raw->bytes.share()};
            auto memory = detail::metadata_file_budget(
              input, limits.metadata, work);
            if (!memory)
                co_return runtime::failure(
                  detail::path_error(memory.error().code()));
            auto claims = co_await probe_local_metadata(
              input,
              control.identity.metadata_alignment,
              *memory,
              work,
              {},
              codec::input_boundary::complete);
            if (!claims)
                co_return runtime::failure(
                  detail::path_error(claims.error().code()));
            if (
              claims->header.kind() != local_metadata_kind::recovery_decision
              || claims->header.owner() != *owner
              || claims->header.generation() != *generation || !claims->segment)
                co_return runtime::failure(
                  detail::path_error(errc::wrong_context));
            claimed = claims->segment;
        }
        const auto found = std::find_if(
          catalog.begin(), catalog.end(), [&](const auto& descriptor) {
              return descriptor.segment == *claimed;
          });
        if (found == catalog.end()) {
            ++output.unmatched;
            co_return true;
        }
        const auto header = local_metadata_header::make(
          local_metadata_kind::recovery_decision, *owner, *generation);
        if (!header)
            co_return runtime::failure(
              detail::path_error(errc::invalid_argument));
        local_metadata_expectation expected{
          *header, control.identity.metadata_alignment};
        expected.segment = found->segment;
        expected.segment_alignment = found->alignment;
        expected.digest = *digest;
        expected.encoded_bytes = byte_count{raw->file_bytes};
        bytes::fragmented_buffer_parser input{std::move(raw->bytes)};
        auto memory = detail::metadata_file_budget(
          input, limits.metadata, work);
        if (!memory)
            co_return runtime::failure(
              detail::path_error(memory.error().code()));
        auto decoded = co_await decode_local_metadata(
          input, expected, *memory, work, {}, codec::input_boundary::complete);
        if (!decoded)
            co_return runtime::failure(
              detail::path_error(decoded.error().code()));
        if (!input.at_end())
            co_return runtime::failure(
              detail::path_error(errc::malformed_data));
        const recovery_decision_record value{
          {*decided, *digest, byte_count{raw->file_bytes}},
          std::get<local_recovery_decision>(decoded->value.payload())};
        ++output.decisions;
        co_return co_await visit(value);
    };
    try {
        do {
            // The fresh barrier: every record this walk can see is durable.
            if (auto synced = co_await cursor.sync(); !synced) {
                failed.observe(synced);
                break;
            }
            bool end = false, stopped = false;
            while (!end && !stopped && !failed.failed()) {
                if (
                  auto ready = co_await detail::path_checkpoint(work); !ready) {
                    failed.observe(ready);
                    break;
                }
                auto page = co_await cursor.next(
                  {.maximum_entries = item_count{16},
                   .maximum_name_bytes = byte_count{4_KiB}});
                if (!page) {
                    failed.observe(page);
                    break;
                }
                end = page->end();
                for (const auto& entry : page->entries()) {
                    const std::string_view name = entry.name.value();
                    auto path = local_child_path(*directory, entry.name);
                    if (!path) {
                        failed.observe(path);
                        break;
                    }
                    if (entry.kind != runtime::file_kind::regular) {
                        failed.observe(detail::path_error(errc::wrong_context));
                        break;
                    }
                    if (
                      auto sequence = parse_local_sequence_name(name, true);
                      sequence) {
                        auto kept = co_await record(*path, *sequence);
                        if (!kept) {
                            failed.observe(kept);
                            break;
                        }
                        if (!*kept) {
                            stopped = true;
                            break;
                        }
                        continue;
                    }
                    if (name.size() > 24) {
                        auto target = runtime::file_name::make(
                          name.substr(0, name.size() - 24));
                        if (
                          target && parse_local_temporary_name(name, *target)
                          && parse_local_sequence_name(target->value(), true)) {
                            ++output.temporaries;
                            continue;
                        }
                    }
                    failed.observe(
                      detail::path_error(errc::unsupported_format));
                    break;
                }
            }
            output.complete = end && !stopped;
        } while (false);
    } catch (...) {
        failed.observe(std::current_exception());
    }
    try {
        failed.observe(co_await cursor.close());
    } catch (...) {
        failed.observe(std::current_exception());
    }
    if (failed.failed()) {
        auto error = failed.outcome();
        co_return runtime::failure(error.error());
    }
    co_return output;
}

// One segment of the owner's catalog: its independent descriptor and header.
struct recovery_catalog_entry final {
    local_segment_descriptor descriptor;
    segment_header header;
};
enum class recovery_entry_state : std::uint8_t {
    // Active, recovering or sealed, its pins opened: the merge reconciles it.
    target,
    // A durable seal decision its active or recovering publication has not
    // carried out. Its root may already have replaced the pinned footer, so
    // the pins are not opened and the merge must not read it: the recovered
    // seal resumes first.
    sealing,
    // Its publication is deleting: deletion reconciliation owns it, and the
    // merge never reads it.
    deleting,
};
// One catalog segment as restart found it, from its fixed publication and the
// pins it carries. Only values are kept; nothing stays open.
struct recovery_entry final {
    recovery_entry_state state{recovery_entry_state::target};
    device_store_id device;
    segment_history_context history;
    local_publication_generation generation;
    local_object_publication publication;
    std::optional<runtime::operation_error> index_rebuild;
    // The decision a sealing entry's recovered seal resumes.
    std::optional<recovery_decision_pin> pending_seal;
};
struct recovery_inventory final {
    // Holds the charge for both vectors.
    workload_reservation held;
    // One per catalog segment, in catalog order.
    std::vector<recovery_entry> entries;
    // What the merge reconciles: every target entry, in catalog order.
    std::vector<recovery_target> targets;
};

// Segments a restart step works on at once: the inventory's finds and the
// recovering publications, each with its own cooperative work.
inline constexpr std::uint32_t maximum_recovery_jobs = 8;

namespace detail {
// The segment's current publication, selected by its fixed path on its device
// and checked against its pins' rules, with its generation. Nothing is opened.
template<runtime::file_system_backend Backend>
seastar::future<runtime::result<
  std::pair<local_publication_generation, local_object_publication>>>
select_recovery_publication(
  Backend& files,
  const local_device_spec& spec,
  std::uint32_t shard,
  const local_segment_descriptor& descriptor,
  workload_budget& budget,
  local_store_io_limits limits,
  codec::cooperative_work& work) {
    const auto owner = spec.shard_owner(shard);
    if (!owner) co_return runtime::failure(path_error(errc::wrong_context));
    const auto paths = local_paths::make(spec.root);
    if (!paths) co_return runtime::failure(paths.error());
    auto published = paths->segment_file(
      shard,
      {descriptor.segment.segment(), descriptor.segment.generation()},
      local_segment_file::published);
    if (!published) co_return runtime::failure(published.error());
    auto selected = co_await load_local_metadata_file(
      files,
      spec,
      *published,
      budget,
      limits,
      work,
      local_metadata_extent::exact_file,
      [owner = *owner,
       segment = descriptor.segment,
       metadata = spec.identity.metadata_alignment,
       data = descriptor.alignment](
        auto& input, byte_count, codec::decode_budget memory, auto& work) {
          return decode_selected_object_publication(
            input,
            owner,
            segment,
            metadata,
            data,
            memory,
            work,
            {},
            codec::input_boundary::complete);
      });
    if (!selected) co_return runtime::failure(selected.error());
    auto publication = std::get<local_object_publication>(
      selected->value.payload());
    if (auto valid = validate_recovery_publication(publication); !valid)
        co_return runtime::failure(valid.error());
    co_return std::pair{
      selected->value.header().generation(), std::move(publication)};
}

// The seal decision among `decisions` that `publication` has not carried out:
// one for an active or recovering segment. A segment sealed at the decided
// end consumed it; sealed anywhere else, or two different seal decisions for
// one segment, is wrong_context.
[[nodiscard]] runtime::result<std::optional<recovery_decision_pin>>
pending_recovery_seal(
  const local_object_publication& publication,
  std::span<const recovery_decision_record> decisions) noexcept;
} // namespace detail

namespace detail {
// One catalog segment as the inventory finds it: its entry, and the merge's
// target when it is one.
struct recovery_found final {
    recovery_entry entry;
    std::optional<recovery_target> target;
};

template<runtime::file_system_backend Backend, local_directory_owner Owner>
seastar::future<runtime::result<recovery_found>> find_recovery_segment(
  Backend& files,
  Owner& ownership,
  std::span<const local_device_spec> devices,
  std::uint32_t shard,
  const recovery_catalog_entry& item,
  std::span<const recovery_decision_record> decisions,
  workload_budget& budget,
  local_store_io_limits limits,
  codec::cooperative_work& work) {
    // Only the placement is kept, not the reservations that read it.
    std::optional<wal_target> placement;
    {
        auto located = co_await resolve_local_segment(
          files,
          ownership,
          devices,
          shard,
          item.descriptor,
          item.header,
          budget,
          limits,
          work);
        if (!located) co_return runtime::failure(located.error());
        placement = make_wal_target(*located);
    }
    const auto device = placement->device;
    const segment_history_context history{
      item.descriptor.segment,
      item.descriptor.alignment,
      placement->data_start,
      item.descriptor.logical_origin,
      item.descriptor.physical_origin,
      item.descriptor.profile};
    const auto spec = std::find_if(
      devices.begin(), devices.end(), [device](const auto& value) {
          return value.owner.device() == device;
      });
    if (spec == devices.end())
        co_return runtime::failure(path_error(errc::invariant_violation));
    // The publication alone first: a pending seal supersedes its pins.
    auto selected = co_await select_recovery_publication(
      files, *spec, shard, item.descriptor, budget, limits, work);
    if (!selected) co_return runtime::failure(selected.error());
    recovery_found output{
      {recovery_entry_state::target,
       device,
       history,
       selected->first,
       std::move(selected->second),
       {},
       {}},
      {}};
    auto& entry = output.entry;
    const auto& publication = entry.publication;
    if (publication.state == local_object_state::deleting) {
        entry.state = recovery_entry_state::deleting;
        co_return output;
    }
    auto pending = pending_recovery_seal(publication, decisions);
    if (!pending) co_return runtime::failure(pending.error());
    if (*pending) {
        entry.state = recovery_entry_state::sealing;
        entry.pending_seal = *pending;
        co_return output;
    }
    // The pins of the publication just selected, on the segment just
    // located: neither is read again.
    auto opened = co_await open_recovery_pins(
      files,
      ownership,
      *spec,
      shard,
      item.descriptor,
      history,
      entry.generation,
      publication,
      budget,
      limits,
      work);
    if (!opened) co_return runtime::failure(opened.error());
    if (opened->owner) {
        opened->owner->retire();
        auto closed = co_await opened->owner->close();
        opened->owner.reset();
        if (!closed) co_return runtime::failure(closed.error());
    }
    entry.index_rebuild = opened->index_rebuild;
    output.target = recovery_target{
      item.descriptor,
      item.header,
      publication.state,
      entry.generation,
      publication.state == local_object_state::sealed ? publication.boundary
                                                      : opened->resume,
      placement};
    co_return output;
}

// Admission refused for want of budget room, never for the data: a segment
// that met it beside other jobs is found again alone.
[[nodiscard]] inline bool
recovery_admission_refused(const runtime::operation_error& error) noexcept {
    return error.code() == errc::resource_exhausted
           || error.code() == errc::queue_full;
}
} // namespace detail

// Locates and opens every catalog segment as restart must see it before the
// merge: on the one device that holds it, under its fixed publication and the
// pins it carries, each opened owner closed again at once. A deleting segment
// is set aside. A durable seal decision (from discover_recovery_decisions)
// that the segment's publication has not carried out supersedes its pins, as
// it does for the recovered seal: the segment is sealing, its pins are not
// opened, and the caller resumes that seal first and opens the inventory
// again. A listed segment found nowhere is an error, never an empty one.
// Up to `jobs` segments are found at once, each with its own cooperative
// work; one refused admission beside other jobs is found again alone, so the
// outcome, including which failure is returned, is the one-at-a-time outcome.
// Each target carries the placement found for it. Reads only.
template<runtime::file_system_backend Backend, local_directory_owner Owner>
seastar::future<runtime::result<recovery_inventory>> open_recovery_inventory(
  Backend& files,
  Owner& ownership,
  std::span<const local_device_spec> devices,
  std::uint32_t shard,
  std::span<const recovery_catalog_entry> catalog,
  std::span<const recovery_decision_record> decisions,
  workload_budget& budget,
  local_store_io_limits limits,
  codec::cooperative_work& work,
  std::uint32_t jobs = maximum_recovery_jobs) {
    if (auto valid = validate_local_device_set(devices); !valid)
        co_return runtime::failure(valid.error());
    if (jobs == 0 || jobs > maximum_recovery_jobs)
        co_return runtime::failure(detail::path_error(errc::invalid_argument));
    for (std::size_t i = 0; i < catalog.size(); ++i)
        for (std::size_t j = 0; j < i; ++j)
            if (catalog[i].descriptor.segment == catalog[j].descriptor.segment)
                co_return runtime::failure(
                  detail::path_error(errc::invalid_argument));
    using found_type = std::optional<runtime::result<detail::recovery_found>>;
    byte_count total;
    for (const auto bytes :
         {catalog.size() * sizeof(recovery_entry),
          catalog.size() * sizeof(recovery_target),
          catalog.size() * sizeof(found_type)}) {
        if (bytes == 0) continue;
        auto charged = budget.allocation_charge(byte_count{bytes});
        if (!charged) co_return runtime::failure(charged.error());
        auto next = total.checked_add(*charged);
        if (!next)
            co_return runtime::failure(detail::path_error(errc::out_of_range));
        total = *next;
    }
    auto held = budget.try_reserve(total);
    if (!held) co_return runtime::failure(held.error());
    recovery_inventory output{std::move(*held), {}, {}};
    output.entries.reserve(catalog.size());
    output.targets.reserve(catalog.size());
    std::vector<found_type> found(catalog.size());
    const auto find = [&](std::size_t index, codec::cooperative_work& job) {
        return detail::find_recovery_segment(
          files,
          ownership,
          devices,
          shard,
          catalog[index],
          decisions,
          budget,
          limits,
          job);
    };
    // Segments are taken in catalog order. Once one fails, no later one is
    // started: every earlier one already was, so the first failure in
    // catalog order is the one a single job would return.
    std::size_t next = 0;
    bool failed = false;
    std::exception_ptr thrown;
    auto worker = [&]() -> seastar::future<> {
        seastar::abort_source abort;
        codec::cooperative_work job{work.policy(), abort};
        while (next < catalog.size() && !failed && !thrown) {
            const auto index = next++;
            try {
                if (auto ready = work.poll(); !ready)
                    found[index].emplace(
                      runtime::failure(
                        detail::path_error(ready.error().code())));
                else
                    found[index].emplace(co_await find(index, job));
                if (
                  !*found[index]
                  && !detail::recovery_admission_refused(found[index]->error()))
                    failed = true;
            } catch (...) {
                if (!thrown) thrown = std::current_exception();
            }
        }
    };
    std::array<std::optional<seastar::future<>>, maximum_recovery_jobs> running;
    const auto workers = std::min<std::size_t>(jobs, catalog.size());
    for (std::size_t k = 0; k < workers; ++k)
        running[k].emplace(worker());
    for (std::size_t k = 0; k < workers; ++k)
        co_await std::move(*running[k]);
    if (thrown) std::rethrow_exception(thrown);
    for (std::size_t i = 0; i < catalog.size(); ++i) {
        if (!found[i]) break;
        auto& value = *found[i];
        if (!value && detail::recovery_admission_refused(value.error())) {
            // Alone now: a refusal here is the one-at-a-time outcome.
            if (auto ready = work.poll(); !ready)
                co_return runtime::failure(
                  detail::path_error(ready.error().code()));
            value = co_await find(i, work);
        }
        if (!value) co_return runtime::failure(value.error());
        if (value->target) output.targets.push_back(std::move(*value->target));
        output.entries.push_back(std::move(value->entry));
    }
    co_return output;
}

} // namespace kwaque::storage
