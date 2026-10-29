#pragma once

#include "src/base/allocation.h"
#include "src/base/invariant.h"
#include "src/base/units.h"
#include "src/storage/local_discovery.h"
#include "src/storage/local_metadata_file.h"
#include "src/storage/local_store_inspection.h"
#include "src/storage/page_internal.h"
#include "src/storage/recovery_decision.h"
#include "src/storage/recovery_merge.h"
#include "src/storage/recovery_pins.h"
#include "src/storage/retained_wal.h"
#include "src/storage/retention.h"
#include "src/storage/segment_scan.h"
#include "src/storage/table_view.h"

#include <seastar/core/chunked_vector.hh>
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
  table_view<local_segment_descriptor> catalog,
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
    auto opened = co_await files.open_directory(*directory);
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
    // Where it starts in its range: its owner's activation record. None for
    // a segment that was prepared and never activated: nothing was appended
    // to it, and its origin is given when it is.
    std::optional<model::range_logical_end> logical_origin;
    // For a segment its owner marks as expiring: the authorization its
    // removal was accepted with, supplied again at every start, so that a
    // removal a restart resumes is checked as the first request was.
    std::optional<deletion_authorization> expiring{};
};
enum class recovery_entry_state : std::uint8_t {
    // Active, recovering or sealed, its pins opened: the merge reconciles it.
    target,
    // A durable seal decision its active or recovering publication has not
    // carried out. Its root may already have replaced the pinned footer, so
    // the pins are not opened and the merge must not read it: the recovered
    // seal resumes first.
    sealing,
    // Being removed: its directory is under its deleting name, its
    // publication is deleting, or its owner marks it expiring with an
    // authorization that names what the device holds. Whoever owns removals
    // goes on with it, and the merge never reads it.
    deleting,
    // Its owner marks it expiring and no device holds it under either name:
    // a removal that finished. Reported again, and nothing is read.
    gone,
    // Listed with no origin: prepared and never activated. Its first
    // pointer, active and naming nothing, is all it published. It is taken
    // in as prepared unless something says it was written (`conflict`), and
    // the merge never reads it.
    prepared,
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
    // Found under its deleting name. Nothing inside such a directory is
    // read, so of this entry only the state, the device and the segment
    // are known. A marked segment its owner still lists as retained is
    // removed all the same, and whoever starts the shard says so.
    bool marked{false};
    // What the catalog marks it expiring with, as supplied.
    std::optional<deletion_authorization> expiring;
    // For an expiring segment found whole under its own name: the sealed
    // extent the device holds of it, when it is sealed, and what its
    // authorization was refused with when it does not name that. A refused
    // one changes nothing: the segment is kept and read as any other.
    std::optional<segment_immutable_expectation> sealed;
    std::optional<authorization_verdict> refused;
    // A prepared entry that something says was written: a publication other
    // than its first, an object of its own at its data start, an entry of
    // the durable checkpoint or a PREPARE that names it. Its owner decides,
    // and the start changes nothing.
    bool conflict{false};
};
struct recovery_inventory final {
    // Holds the charge for both tables.
    workload_reservation held;
    // One per catalog segment, in catalog order, in fragments of at most
    // 128 KiB.
    seastar::chunked_vector<recovery_entry> entries;
    // What the merge reconciles: every target entry, in catalog order.
    seastar::chunked_vector<recovery_target> targets;
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
// Whether a prepared segment's data file holds anything of its own at its
// data start: the first object its scan meets there, or anything that is
// not the end of the file, zeros, or an intact object of another segment,
// which a spare's earlier life left. Reads one object at most.
template<runtime::file_system_backend Backend, local_directory_owner Owner>
seastar::future<runtime::result<bool>> written_since_prepared(
  Backend& files,
  Owner& ownership,
  const local_device_spec& spec,
  std::uint32_t shard,
  const segment_history_context& history,
  workload_budget& budget,
  local_store_io_limits limits,
  codec::cooperative_work& work) {
    bool met = false;
    auto scanned = co_await scan_local_segment(
      files,
      ownership,
      spec,
      shard,
      history,
      std::nullopt,
      budget,
      segment_scan_limits{.metadata = limits},
      work,
      [&met](const segment_scanned_object&) {
          met = true;
          return seastar::make_ready_future<runtime::result<bool>>(false);
      });
    if (!scanned) co_return runtime::failure(scanned.error());
    const auto& report = *scanned;
    co_return met
      || (report.stop != segment_scan_stop::end
          && report.stop != segment_scan_stop::unwritten
          && !(report.intact && report.stale));
}

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
    const auto unread = [&item](
                          recovery_entry_state state, device_store_id device) {
        recovery_found output{
          {state,
           device,
           segment_history_context{
             item.descriptor.segment,
             item.descriptor.alignment,
             runtime::file_position{},
             item.logical_origin.value_or(model::range_logical_end{}),
             item.descriptor.physical_origin,
             item.descriptor.profile},
           {},
           local_object_publication{
             item.descriptor.segment,
             local_object_state::deleting,
             std::nullopt,
             {}},
           {},
           {}},
          {}};
        output.entry.expiring = item.expiring;
        return output;
    };
    // Where its directory is decides before anything inside it is read.
    const auto place = co_await find_segment_directory(
      files, ownership, devices, shard, item.descriptor.segment, work);
    if (!place) co_return runtime::failure(place.error());
    // Under neither name: lost, unless its owner says it expires.
    if (!*place) {
        if (item.expiring)
            co_return unread(recovery_entry_state::gone, device_store_id{});
        co_return runtime::failure(path_error(errc::not_found));
    }
    // Under the deleting name it is being removed whatever it holds and
    // whatever the catalog says of the segment.
    if ((*place)->marked) {
        auto output = unread(recovery_entry_state::deleting, (*place)->device);
        output.entry.marked = true;
        co_return output;
    }
    // Under its own name. One its owner discards as not sealed is decided by
    // what is published of it, before anything else of it is read or
    // resumed: a creation that was interrupted published nothing, and what
    // it left goes as what it is. A sealed one is kept, below.
    if (
      item.expiring
      && authorize_deletion(
           *item.expiring,
           {item.descriptor.segment,
            segment_holding::unsealed,
            std::nullopt,
            std::nullopt})
           == authorization_verdict::authorized) {
        // The directory was found on one of these devices.
        const auto on = std::find_if(
          devices.begin(), devices.end(), [&place](const auto& value) {
              return value.owner.device() == (*place)->device;
          });
        KWAQUE_INVARIANT(
          invariant_id{"KQ-RECOVERY-DEVICE-KNOWN"},
          on != devices.end(),
          "a segment directory was found on no device of the set");
        const auto published = co_await select_recovery_publication(
          files, *on, shard, item.descriptor, budget, limits, work);
        if (!published && published.error().code() != errc::not_found)
            co_return runtime::failure(published.error());
        if (!published || published->second.state != local_object_state::sealed)
            co_return unread(recovery_entry_state::deleting, (*place)->device);
    }
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
        // Its directory is there: a file of it that is not is damage.
        if (!located) co_return runtime::failure(located.error());
        placement = make_wal_target(*located);
    }
    const auto device = placement->device;
    const segment_history_context history{
      item.descriptor.segment,
      item.descriptor.alignment,
      placement->data_start,
      item.logical_origin.value_or(model::range_logical_end{}),
      item.descriptor.physical_origin,
      item.descriptor.profile};
    // The segment was located on one of these devices.
    const auto spec = std::find_if(
      devices.begin(), devices.end(), [device](const auto& value) {
          return value.owner.device() == device;
      });
    KWAQUE_INVARIANT(
      invariant_id{"KQ-RECOVERY-DEVICE-KNOWN"},
      spec != devices.end(),
      "a segment was located on no device of the set");
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
    entry.expiring = item.expiring;
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
    if (!item.logical_origin) {
        // Prepared and never activated, as its owner says: its first pointer
        // and nothing of its own at its data start bear that out.
        entry.state = recovery_entry_state::prepared;
        if (
          publication.state != local_object_state::active
          || publication.boundary || !publication.roots.empty()) {
            entry.conflict = true;
            co_return output;
        }
        auto written = co_await written_since_prepared(
          files, ownership, *spec, shard, history, budget, limits, work);
        if (!written) co_return runtime::failure(written.error());
        entry.conflict = *written;
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
    // An expiring segment is checked as the first request for its removal
    // was: against what the device holds it to be.
    std::optional<authorization_verdict> verdict;
    if (item.expiring) {
        segment_held held{
          item.descriptor.segment,
          segment_holding::unsealed,
          std::nullopt,
          std::nullopt};
        if (publication.state == local_object_state::sealed) {
            held.holding = segment_holding::sealed;
            if (opened->owner) {
                if (const auto pinned = opened->owner->pin()) {
                    const auto& boundary = pinned->boundary();
                    const auto* root = boundary ? std::get_if<sealed_footer>(
                                                    &*boundary)
                                                : nullptr;
                    if (root)
                        held.extent = segment_immutable_expectation{
                          root->coverage(),
                          root->extent_digest(),
                          root->encoded_extent().end()};
                }
            }
        }
        entry.sealed = held.extent;
        verdict = authorize_deletion(*item.expiring, held);
    }
    if (opened->owner) {
        opened->owner->retire();
        auto closed = co_await opened->owner->close();
        opened->owner.reset();
        if (!closed) co_return runtime::failure(closed.error());
    }
    if (verdict == authorization_verdict::authorized) {
        entry.state = recovery_entry_state::deleting;
        co_return output;
    }
    entry.refused = verdict;
    entry.index_rebuild = opened->index_rebuild;
    output.target = recovery_target{
      item.descriptor,
      item.header,
      *item.logical_origin,
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

// The most segments one catalog may list. Every table a restart keeps per
// catalog segment, or per target, is chunked, so this bounds the work of one
// start rather than an allocation. A shard that could leave more than this
// writable, prepared or being removed could not be started again.
inline constexpr std::size_t maximum_recovery_catalog = 8192;
static_assert(
  std::uint64_t{maximum_recovery_catalog} * maximum_retained_wal_files
  == maximum_recovery_obligations);

// Locates and opens every catalog segment as restart must see it before the
// merge: on the one device that holds it, under its fixed publication and the
// pins it carries, each opened owner closed again at once. A segment that is
// being removed is set aside for whoever owns removals: one whose directory
// is under its deleting name, whatever is inside it and whatever the catalog
// says; one whose publication is deleting; and one its owner marks expiring
// with an authorization that names what the device holds. An expiring
// segment no device holds is gone, and is reported so. A durable seal decision
// (from discover_recovery_decisions) that the segment's publication has not
// carried out supersedes its pins, as it does for the recovered seal: the
// segment is sealing, its pins are not opened, and the caller resumes that seal
// first and opens the inventory again. A listed segment found nowhere is an
// error, never an empty one, unless its owner marks it expiring. Up to `jobs`
// segments are found at once, each with its own cooperative work; one refused
// admission beside other jobs is found again alone, so the outcome, including
// which failure is returned, is the one-at-a-time outcome. Each target carries
// the placement found for it. Reads only.
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
    for (std::size_t i = 0; i < catalog.size(); ++i) {
        // An entry says once which segment it is.
        if (
          catalog[i].expiring
          && catalog[i].expiring->segment != catalog[i].descriptor.segment)
            co_return runtime::failure(
              detail::path_error(errc::invalid_argument));
        for (std::size_t j = 0; j < i; ++j)
            if (catalog[i].descriptor.segment == catalog[j].descriptor.segment)
                co_return runtime::failure(
                  detail::path_error(errc::invalid_argument));
    }
    if (catalog.size() > maximum_recovery_catalog)
        co_return runtime::failure(detail::path_error(errc::out_of_range));
    using found_type = std::optional<runtime::result<detail::recovery_found>>;
    byte_count total;
    for (const auto element :
         {sizeof(recovery_entry),
          sizeof(recovery_target),
          sizeof(found_type)}) {
        auto charged = chunked_allocation_charge(
          budget, catalog.size(), element);
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
    seastar::chunked_vector<found_type> found;
    found.reserve(catalog.size());
    for (std::size_t i = 0; i != catalog.size(); ++i)
        found.emplace_back();
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
