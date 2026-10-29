#pragma once

#include "src/base/invariant.h"
#include "src/base/units.h"
#include "src/codec/transaction.h"
#include "src/storage/local_discovery.h"
#include "src/storage/page_internal.h"
#include "src/storage/retained_wal.h"
#include "src/storage/storage_scan.h"
#include "src/storage/table_view.h"
#include "src/storage/wal_format.h"

#include <seastar/core/chunked_vector.hh>
#include <seastar/core/coroutine.hh>

#include <algorithm>
#include <exception>
#include <memory>
#include <optional>
#include <span>
#include <utility>
#include <vector>

namespace kwaque::storage {

// Independently known placement of one segment generation, from its verified
// local descriptor and segment header, never from a PREPARE.
struct wal_target final {
    segment_context segment;
    storage_alignment alignment;
    runtime::file_position data_start;
    storage_profile profile{storage_profile::v1};
    device_store_id device;
};
[[nodiscard]] wal_target
make_wal_target(const local_located_segment& located) noexcept;

// Why one file's content ends where it does.
enum class wal_scan_stop : std::uint8_t {
    // Content runs exactly to the scan end.
    end,
    // An all-zero prefix: unwritten, zero-written or reverted space.
    unwritten,
    // A prefix, header or PREPARE extends past the file end.
    torn,
    // A header or body checksum does not match.
    corrupt,
    // Garbage, or an integrity-valid PREPARE with malformed content.
    malformed,
    // Integrity-valid bytes of another family, WAL file, position or layout.
    foreign,
};

// How a PREPARE's claimed target was resolved. Only a resolved PREPARE was
// decoded under independent context; the others stay WAL content without
// any authority.
enum class wal_target_resolution : std::uint8_t {
    resolved,
    // No independently known segment generation matched the claim.
    unknown,
    // The target is known, but the claimed position cannot be in it.
    misplaced,
};

// One PREPARE in WAL order, borrowed through a joined visit.
struct wal_scanned_prepare final {
    local_wal_cursor begin;
    local_wal_cursor end;
    wal_target_resolution resolution;
    const wal_prepare_claims& claims;
    // Present only when resolved. The visitor may take its child.
    wal_prepare* prepare;
    const wal_target* target;
    // The exact envelope's digest, when resolved and the scan asked for it.
    std::optional<codec::immutable_object_digest> digest{};
    // An intact PREPARE of the head's incarnation at its own position, found
    // after the head's content by a probe: never content and never resolved.
    // Its claims are as its checksums keep them, and only its stamps are
    // evidence: of this file's certified end and its target's.
    bool survival{false};
};

// One scanned file. A rotated file's predecessor cursor in its successor
// sealed its prefix after a completed barrier, so anything but `end` there is
// damage to certified bytes. The head has no such record: where its content
// stops says nothing about whether those bytes were ever certified.
struct wal_file_scan final {
    model::wal_incarnation_id incarnation;
    // Its layout, from its descriptor: what decoding its PREPAREs needs.
    storage_alignment alignment;
    replay_profile profile;
    runtime::file_position data_start;
    std::optional<runtime::file_position> sealed_end;
    // The data start, or the checkpoint cutoff in its file.
    runtime::file_position begin;
    // The end of the last whole write accepted as content: one whose
    // ends_write member is intact. Members of a later, unfinished write are
    // not content, and are visited only as survivals.
    runtime::file_position content_end;
    // Where this file's certified bytes provably end: a rotated file's
    // sealed end; otherwise the most of where the scan began and the WAL
    // stamp of every intact PREPARE of this file, the head's survivals
    // included when the scan probed. Damage below it was certified.
    runtime::file_position proven_end;
    std::uint64_t file_bytes{0};
    std::uint64_t prepares{0}, unresolved{0};
    wal_scan_stop stop{wal_scan_stop::end};
    std::optional<codec::error> cause;
    // A rotated file's bytes after its sealed end, which are never content.
    // Only the envelope prefix at the sealed end is examined: the slack is
    // nonzero unless that prefix reads as zeros, so slack shorter than a
    // prefix counts as nonzero. It is reported, never acted on.
    byte_count slack;
    bool nonzero_slack{false};
};

enum class wal_scan_verdict : std::uint8_t {
    // Every rotated file's sealed prefix scanned intact.
    intact,
    // Damage inside a rotated file's sealed prefix; scanning stopped there.
    corrupt,
};
struct wal_scan_result final {
    wal_scan_verdict verdict{wal_scan_verdict::intact};
    // False when a visitor stopped the scan early.
    bool complete{false};
    std::uint64_t files{0}, prepares{0}, unresolved{0};
    // The chain's files by name, oldest first: all of them, or the oldest
    // that fit when the chain has more than a shard may hold.
    retained_wal chain;
    // How many files the chain has.
    std::uint64_t chain_files{0};
    // The end of the head's content; a successor's predecessor cursor names
    // exactly this. Absent for a store whose WAL was never activated.
    std::optional<local_wal_cursor> content_end;
};

struct wal_scan_limits final {
    local_store_io_limits metadata{};
    scan_reader_limits reader{};
    // One PREPARE decode beyond its input: aliases, validation scratch and
    // decoded metadata. Reserved once for the scan.
    byte_count working_bytes{8_MiB};
    // The codec's own metadata ceiling. An object read across windows is
    // shared fragment by fragment, so a large object's share descriptors
    // outgrow a small allowance; this one covers the largest admitted object.
    byte_count decode_metadata_bytes{1_MiB};
    // Hash each resolved PREPARE's exact envelope for its visitor.
    bool digests{false};
    // Read the head on after its content, once, for intact PREPAREs of its
    // incarnation: their stamps prove what they cover certified, and each is
    // visited as a survival.
    bool prove{false};
    // A WAL still written is read to here and no further: no file after this
    // one's is read, and its file ends here for the scan, as though it were
    // its sealed end, which ends its content. A writer issues its writes one
    // at a time, so no complete write lies past one that is still running.
    std::optional<local_wal_cursor> live_end{};

    [[nodiscard]] runtime::result<void> validate() const noexcept;
};

namespace detail {
struct wal_chain_link final {
    model::wal_incarnation_id incarnation;
    std::optional<runtime::file_position> sealed_end;
    storage_alignment alignment;
    replay_profile profile;
    runtime::file_position data_start;
};

// Per-PREPARE state the resolving decode reports back to the scan.
struct wal_resolution final {
    model::wal_incarnation_id incarnation;
    storage_alignment alignment;
    replay_profile profile;
    runtime::file_position data_start;
    runtime::file_position position;
    std::optional<wal_prepare_claims> claims;
    std::optional<wal_target> target;
    std::optional<wal_target_resolution> refused;
    bool foreign{false};
    std::optional<runtime::operation_error> failure;

    void reset(runtime::file_position at) noexcept {
        position = at;
        claims.reset();
        target.reset();
        refused.reset();
        foreign = false;
        failure.reset();
    }
};

[[nodiscard]] wal_scan_stop wal_decode_stop(const codec::error&) noexcept;

// WAL identity comes from the scan, never from the claims: a PREPARE of
// another file, position, alignment or profile is foreign before any target
// lookup. The target is then resolved independently and only its known
// layout bounds the claimed position.
template<typename Resolver>
seastar::future<codec::result<wal_prepare_expectation>> resolve_wal_prepare(
  wal_resolution& state, Resolver& resolve, const wal_prepare_claims& claims) {
    state.claims = claims;
    const auto refuse = [](errc code) {
        return codec::failure(codec::error{code});
    };
    if (
      claims.incarnation != state.incarnation
      || claims.wal_position != state.position
      || claims.alignment != state.alignment
      || claims.profile != state.profile) {
        state.foreign = true;
        co_return refuse(errc::wrong_context);
    }
    auto found = co_await resolve(claims.target);
    if (!found) {
        state.failure = found.error();
        co_return refuse(found.error().code());
    }
    if (!*found) {
        state.refused = wal_target_resolution::unknown;
        co_return refuse(errc::not_found);
    }
    if ((*found)->segment != claims.target) {
        state.failure = path_error(errc::invalid_argument);
        co_return refuse(errc::invalid_argument);
    }
    const auto& target = **found;
    auto wal = wal_write_context::make(
      state.incarnation, state.alignment, state.position);
    // The decode checked the PREPARE's layout at this position first.
    if (!wal) {
        state.failure = path_error(errc::invariant_violation);
        co_return refuse(errc::invariant_violation);
    }
    auto placement = segment_write_context::make(
      target.segment,
      target.alignment,
      claims.physical_begin,
      claims.target_position);
    // Packed writes place a block anywhere after the data start.
    if (!placement || claims.target_position < target.data_start) {
        state.refused = wal_target_resolution::misplaced;
        co_return refuse(errc::out_of_range);
    }
    state.target = target;
    co_return wal_prepare_expectation{
      *wal,
      state.data_start,
      *placement,
      target.data_start,
      claims.routing_epoch,
      {target.segment.topic(), target.segment.range(), {}, {}, {}},
      state.profile,
      target.profile};
}

// Where a head's content ends and why. A write is certified only whole: its
// last member, the one that sets ends_write, is intact. Members of a write
// that stops before it are uncertified survivals, never content.
struct wal_head_content final {
    runtime::file_position end;
    wal_scan_stop stop{wal_scan_stop::end};
    std::optional<codec::error> cause;
};

// Reads the head from `begin` to `end` up to its first damage, checking each
// PREPARE's integrity and claims but decoding no child, and returns the end
// of its last complete write. Zeros where a write would begin are unwritten
// space; inside a write, or the file ending there, they are a tear. A begin
// off the boundary is inside a write.
inline seastar::future<runtime::result<wal_head_content>> scan_wal_head_content(
  runtime::file& file,
  workload_budget& budget,
  const wal_resolution& identity,
  runtime::file_position begin,
  runtime::file_position end,
  const wal_scan_limits& limits,
  codec::cooperative_work& work) {
    auto made = scan_reader::make(file, budget, begin, end, limits.reader);
    if (!made) co_return runtime::failure(made.error());
    auto reader = std::move(*made);
    std::optional<wal_prepare_claims> seen;
    // The claims are all this pass needs: refusing them ends the decode
    // before any child, with `seen` telling that refusal from a failure.
    wal_prepare_resolver claims_only{[&seen](const wal_prepare_claims& claims) {
        seen = claims;
        return seastar::make_ready_future<
          codec::result<wal_prepare_expectation>>(
          codec::failure(codec::error{errc::aborted}));
    }};
    wal_head_content output{begin};
    bool open = !identity.alignment.aligned(begin);
    runtime::first_failure failed;
    try {
        for (;;) {
            auto slot = co_await reader->next(work);
            if (!slot) {
                failed.observe(slot);
                break;
            }
            if (slot->kind != scan_slot_kind::object) {
                output.cause = slot->cause;
                switch (slot->kind) {
                case scan_slot_kind::end:
                    output.stop = open ? wal_scan_stop::torn
                                       : wal_scan_stop::end;
                    break;
                case scan_slot_kind::zero:
                    output.stop = open ? wal_scan_stop::torn
                                       : wal_scan_stop::unwritten;
                    break;
                case scan_slot_kind::torn:
                    output.stop = wal_scan_stop::torn;
                    break;
                case scan_slot_kind::corrupt:
                    output.stop = wal_scan_stop::corrupt;
                    break;
                default:
                    output.stop = wal_scan_stop::malformed;
                    break;
                }
                break;
            }
            auto bytes = reader->object();
            if (!bytes) {
                failed.observe(bytes);
                break;
            }
            bytes::fragmented_buffer_parser input{std::move(*bytes)};
            auto memory = codec::reserve_decode_input(
              input,
              work.policy(),
              {byte_count{
                 slot->prefix->encoded_bytes().value()
                 + limits.working_bytes.value()},
               limits.decode_metadata_bytes,
               limits.metadata.charge},
              {},
              codec::input_boundary::complete);
            if (!memory) {
                failed.observe(path_error(memory.error().code()));
                break;
            }
            seen.reset();
            auto decoded = co_await decode_wal_prepare_resolved(
              input,
              claims_only,
              *memory,
              work,
              {.origin = slot->position.value()},
              codec::input_boundary::complete);
            if (!seen) {
                const auto code = decoded.error().code();
                if (
                  code == errc::unsupported_format
                  || code == errc::resource_exhausted || code == errc::aborted
                  || code == errc::invalid_argument) {
                    failed.observe(path_error(code));
                    break;
                }
                output.cause = decoded.error();
                output.stop = wal_decode_stop(decoded.error());
                break;
            }
            if (
              seen->incarnation != identity.incarnation
              || seen->wal_position != slot->position
              || seen->alignment != identity.alignment
              || seen->profile != identity.profile) {
                output.stop = wal_scan_stop::foreign;
                break;
            }
            if (auto moved = reader->advance(); !moved) {
                failed.observe(moved);
                break;
            }
            open = !seen->stamps.ends_write;
            if (!open) output.end = reader->position();
        }
    } catch (...) {
        failed.observe(std::current_exception());
    }
    co_await reader->close();
    if (failed.failed()) {
        auto error = failed.outcome();
        co_return runtime::failure(error.error());
    }
    co_return output;
}

// Reads a head after its content to the reader's end, once, for intact
// PREPAREs of its incarnation at their own positions: each raises the
// file's proven end by its WAL stamp and is visited as a survival. An object
// whose header checksum holds is passed over by that verified length, and
// anything else at the next alignment boundary, where a write can begin.
// False once the visitor stops.
template<typename Visitor>
seastar::future<runtime::result<bool>> probe_wal_head(
  scan_reader& reader,
  const wal_resolution& identity,
  const wal_scan_limits& limits,
  codec::cooperative_work& work,
  wal_file_scan& report,
  Visitor& visit) {
    std::optional<wal_prepare_claims> seen;
    wal_prepare_resolver claims_only{[&seen](const wal_prepare_claims& claims) {
        seen = claims;
        return seastar::make_ready_future<
          codec::result<wal_prepare_expectation>>(
          codec::failure(codec::error{errc::aborted}));
    }};
    const auto unit = identity.alignment.bytes().value();
    for (;;) {
        auto slot = co_await reader.next(work);
        if (!slot) co_return runtime::failure(slot.error());
        if (slot->kind == scan_slot_kind::end) co_return true;
        if (slot->kind != scan_slot_kind::object) {
            const auto next = (slot->position.value() / unit + 1) * unit;
            if (next >= reader.end().value()) co_return true;
            if (
              auto skipped = reader.skip_to(runtime::file_position{next});
              !skipped)
                co_return runtime::failure(skipped.error());
            continue;
        }
        auto bytes = reader.object();
        if (!bytes) co_return runtime::failure(bytes.error());
        bytes::fragmented_buffer_parser input{std::move(*bytes)};
        auto memory = codec::reserve_decode_input(
          input,
          work.policy(),
          {byte_count{
             slot->prefix->encoded_bytes().value()
             + limits.working_bytes.value()},
           limits.decode_metadata_bytes,
           limits.metadata.charge},
          {},
          codec::input_boundary::complete);
        if (!memory)
            co_return runtime::failure(path_error(memory.error().code()));
        seen.reset();
        auto decoded = co_await decode_wal_prepare_resolved(
          input,
          claims_only,
          *memory,
          work,
          {.origin = slot->position.value()},
          codec::input_boundary::complete);
        if (!seen) {
            const auto code = decoded.error().code();
            if (
              code == errc::unsupported_format
              || code == errc::resource_exhausted || code == errc::aborted
              || code == errc::invalid_argument)
                co_return runtime::failure(path_error(code));
        } else if (
          seen->incarnation == identity.incarnation
          && seen->wal_position == slot->position
          && seen->alignment == identity.alignment
          && seen->profile == identity.profile) {
            report.proven_end = std::max(
              report.proven_end, seen->stamps.wal_certified_end);
            const auto begin = local_wal_cursor::make(
              identity.incarnation, slot->position);
            const auto end = local_wal_cursor::make(
              identity.incarnation,
              runtime::file_position{
                slot->position.value()
                + slot->prefix->encoded_bytes().value()});
            if (!begin || !end)
                co_return runtime::failure(path_error(errc::out_of_range));
            const wal_scanned_prepare survival{
              *begin,
              *end,
              wal_target_resolution::unknown,
              *seen,
              nullptr,
              nullptr,
              std::nullopt,
              true};
            auto proceed = co_await visit(survival);
            if (!proceed) co_return runtime::failure(proceed.error());
            if (!*proceed) co_return false;
        }
        if (auto moved = reader.advance(); !moved)
            co_return runtime::failure(moved.error());
    }
}
} // namespace detail

// Scans the pinned chain in WAL order, from the checkpoint cutoff (or the
// oldest file) to the head. A rotated file is scanned exactly to its sealed
// end: any damage there, or a file shorter than it, is corruption of
// certified bytes and stops the scan. The head is scanned to its first damage,
// which ends its content without changing any byte; its content is its whole
// writes before that damage, found by a first pass over their claims, and a
// write that stops before its ends_write member is not content. Each
// PREPARE's claimed target is resolved through `resolve` before its full
// contextual decode; `resolve(const segment_context&)` returns
// future<result<optional<wal_target>>> from independent catalog state, with
// nullopt for an unknown target and an error for an unavailable one.
// `resolve` stays alive and exclusive until joined completion. Visitors
// borrow their argument through a joined call; prepare visitors return
// future<result<bool>> (false stops), file visitors future<result<void>>.
template<
  runtime::file_system_backend Backend,
  local_directory_owner Owner,
  typename Resolver,
  typename PrepareVisitor,
  typename FileVisitor>
seastar::future<runtime::result<wal_scan_result>> scan_local_wal(
  Backend& files,
  Owner& ownership,
  const local_device_spec& spec,
  std::uint32_t shard,
  const local_shard_control& control,
  std::optional<local_wal_cursor> cutoff,
  workload_budget& budget,
  wal_scan_limits limits,
  codec::cooperative_work& work,
  Resolver& resolve,
  PrepareVisitor visit_prepare,
  FileVisitor visit_file,
  local_discovery_limits bounds = {}) {
    static_assert(sizeof(PrepareVisitor) + sizeof(FileVisitor) <= 8_KiB);
    if (auto valid = limits.validate(); !valid)
        co_return runtime::failure(valid.error());
    auto working = budget.try_reserve(
      byte_count{
        limits.working_bytes.value() + limits.decode_metadata_bytes.value()});
    if (!working) co_return runtime::failure(working.error());
    // The walk visits the head first; the scan needs WAL order.
    seastar::chunked_vector<detail::wal_chain_link> chain;
    std::optional<workload_reservation> chain_bytes;
    auto collect = [&](const local_wal_chain_entry& entry)
      -> seastar::future<runtime::result<bool>> {
        const auto& descriptor = std::get<local_wal_descriptor>(
          entry.record.value.payload());
        constexpr std::size_t batch = 64;
        if (chain.size() % batch == 0) {
            auto more = budget.try_reserve(
              byte_count{batch * sizeof(detail::wal_chain_link)});
            if (!more) co_return runtime::failure(more.error());
            if (!chain_bytes)
                chain_bytes.emplace(std::move(*more));
            else if (
              auto adopted = chain_bytes->adopt(std::move(*more)); !adopted)
                co_return runtime::failure(adopted.error());
        }
        chain.push_back(
          {descriptor.incarnation,
           entry.sealed_end,
           descriptor.alignment,
           descriptor.profile,
           descriptor.data_start});
        co_return true;
    };
    auto walked = co_await walk_local_wal_chain(
      files,
      ownership,
      spec,
      shard,
      control,
      cutoff,
      true,
      budget,
      limits.metadata,
      work,
      std::move(collect),
      bounds);
    if (!walked) co_return runtime::failure(walked.error());
    wal_scan_result output;
    // Only a visitor that stops leaves a walk incomplete, and this one
    // never stops.
    KWAQUE_INVARIANT(
      invariant_id{"KQ-WAL-SCAN-COMPLETE"},
      walked->complete,
      "the WAL chain walk stopped short");
    if (chain.empty()) {
        output.complete = true;
        co_return output;
    }
    // The walk ran from the head down; the names go oldest first.
    output.chain_files = chain.size();
    for (std::size_t named = chain.size(); named != 0; --named)
        if (!output.chain.extend(chain[named - 1].incarnation)) break;
    const auto paths = local_paths::make(spec.root);
    if (!paths) co_return runtime::failure(paths.error());
    detail::wal_resolution state{
      chain.back().incarnation,
      chain.back().alignment,
      chain.back().profile,
      chain.back().data_start,
      {}};
    wal_prepare_resolver resolver{
      [&state, &resolve](const wal_prepare_claims& claims) {
          return detail::resolve_wal_prepare(state, resolve, claims);
      }};
    for (auto link = chain.rbegin(); link != chain.rend(); ++link) {
        if (auto ready = co_await detail::path_checkpoint(work); !ready)
            co_return runtime::failure(ready.error());
        const bool head = link + 1 == chain.rend();
        const bool bounded = limits.live_end
                             && limits.live_end->incarnation()
                                  == link->incarnation;
        // The file the scan's content ends in.
        const bool last = head || bounded;
        wal_file_scan report{
          link->incarnation,
          link->alignment,
          link->profile,
          link->data_start,
          link->sealed_end,
          cutoff && cutoff->incarnation() == link->incarnation
            ? cutoff->position()
            : link->data_start,
          {}};
        report.content_end = report.begin;
        report.proven_end = link->sealed_end ? *link->sealed_end : report.begin;
        state.incarnation = link->incarnation;
        state.alignment = link->alignment;
        state.profile = link->profile;
        state.data_start = link->data_start;
        auto path = paths->wal(shard, link->incarnation);
        if (!path) co_return runtime::failure(path.error());
        auto handle = budget.try_reserve(limits.metadata.execution_bytes);
        if (!handle) co_return runtime::failure(handle.error());
        if (auto held = handle->try_acquire_handles(1); !held)
            co_return runtime::failure(held.error());
        if (
          auto inspected = co_await inspect_local_path(
            files, spec.root, *path, runtime::file_kind::regular, work);
          !inspected)
            co_return runtime::failure(inspected.error());
        auto opened = co_await files.open(*path, {});
        if (!opened) co_return runtime::failure(opened.error());
        auto file = std::move(*opened);
        runtime::first_failure failed;
        bool stopped = false;
        std::unique_ptr<scan_reader> reader;
        try {
            do {
                auto size = co_await file.size();
                if (!size) {
                    failed.observe(size);
                    break;
                }
                report.file_bytes = *size;
                auto end = link->sealed_end ? *link->sealed_end
                                            : runtime::file_position{*size};
                // Past a live bound nothing is read; what the file does not
                // reach yet is unwritten.
                if (bounded)
                    end = runtime::file_position{std::min(
                      {end.value(),
                       limits.live_end->position().value(),
                       *size})};
                if (end.value() > *size || report.begin > end) {
                    // Certified bytes are missing.
                    report.stop = wal_scan_stop::torn;
                    break;
                }
                // A rotated file's sealed end is a write's end. The head's
                // content ends with its last complete write, found first,
                // so no member of a later write is ever visited.
                std::optional<detail::wal_head_content> content;
                if (head && !bounded) {
                    state.reset(report.begin);
                    auto found = co_await detail::scan_wal_head_content(
                      file, budget, state, report.begin, end, limits, work);
                    if (!found) {
                        failed.observe(found);
                        break;
                    }
                    content = *found;
                    end = content->end;
                }
                auto made = scan_reader::make(
                  file, budget, report.begin, end, limits.reader);
                if (!made) {
                    failed.observe(made);
                    break;
                }
                reader = std::move(*made);
                // The end of the last whole write the scan passed, and the
                // WAL stamp of the last PREPARE, which never moves back.
                auto write_end = report.begin;
                auto stamp = link->data_start;
                for (;;) {
                    auto slot = co_await reader->next(work);
                    if (!slot) {
                        failed.observe(slot);
                        break;
                    }
                    if (slot->kind != scan_slot_kind::object) {
                        report.cause = slot->cause;
                        report.stop = slot->kind == scan_slot_kind::end
                                        ? wal_scan_stop::end
                                      : slot->kind == scan_slot_kind::zero
                                        ? wal_scan_stop::unwritten
                                      : slot->kind == scan_slot_kind::torn
                                        ? wal_scan_stop::torn
                                      : slot->kind == scan_slot_kind::corrupt
                                        ? wal_scan_stop::corrupt
                                        : wal_scan_stop::malformed;
                        // The head's content ended where the first pass
                        // found its damage.
                        if (content && slot->kind == scan_slot_kind::end) {
                            report.stop = content->stop;
                            report.cause = content->cause;
                        }
                        break;
                    }
                    auto bytes = reader->object();
                    if (!bytes) {
                        failed.observe(bytes);
                        break;
                    }
                    state.reset(slot->position);
                    bytes::fragmented_buffer_parser input{std::move(*bytes)};
                    auto memory = codec::reserve_decode_input(
                      input,
                      work.policy(),
                      {byte_count{
                         slot->prefix->encoded_bytes().value()
                         + limits.working_bytes.value()},
                       limits.decode_metadata_bytes,
                       limits.metadata.charge},
                      {},
                      codec::input_boundary::complete);
                    if (!memory) {
                        failed.observe(
                          detail::path_error(memory.error().code()));
                        break;
                    }
                    auto decoded = co_await decode_wal_prepare_resolved(
                      input,
                      resolver,
                      *memory,
                      work,
                      {.origin = slot->position.value()},
                      codec::input_boundary::complete);
                    if (state.failure) {
                        failed.observe(*state.failure);
                        break;
                    }
                    const auto begin = local_wal_cursor::make(
                      link->incarnation, slot->position);
                    const auto after = local_wal_cursor::make(
                      link->incarnation,
                      runtime::file_position{
                        slot->position.value()
                        + slot->prefix->encoded_bytes().value()});
                    if (!begin || !after) {
                        failed.observe(detail::path_error(errc::out_of_range));
                        break;
                    }
                    std::optional<wal_target_resolution> resolution;
                    if (decoded)
                        resolution = wal_target_resolution::resolved;
                    else if (state.claims && state.refused)
                        resolution = state.refused;
                    if (!resolution) {
                        const auto code = decoded.error().code();
                        if (
                          code == errc::unsupported_format
                          || code == errc::resource_exhausted
                          || code == errc::aborted
                          || code == errc::invalid_argument) {
                            failed.observe(detail::path_error(code));
                            break;
                        }
                        report.cause = decoded.error();
                        report.stop = state.foreign ? wal_scan_stop::foreign
                                                    : detail::wal_decode_stop(
                                                        decoded.error());
                        break;
                    }
                    const auto certified
                      = state.claims->stamps.wal_certified_end;
                    if (certified < stamp) {
                        report.cause = codec::error{
                          errc::malformed_data,
                          static_cast<std::uint16_t>(
                            codec::format_family::wal_prepare),
                          0,
                          slot->position.value()};
                        report.stop = wal_scan_stop::malformed;
                        break;
                    }
                    stamp = certified;
                    report.proven_end = std::max(report.proven_end, certified);
                    std::optional<codec::immutable_object_digest> digest;
                    if (decoded && limits.digests) {
                        auto exact = reader->object();
                        if (!exact) {
                            failed.observe(exact);
                            break;
                        }
                        auto hashed = co_await detail::hash_exact(
                          *exact, work, {.origin = slot->position.value()});
                        if (!hashed) {
                            failed.observe(
                              detail::path_error(hashed.error().code()));
                            break;
                        }
                        digest = *hashed;
                    }
                    const wal_scanned_prepare scanned{
                      *begin,
                      *after,
                      *resolution,
                      *state.claims,
                      decoded ? &decoded->value : nullptr,
                      decoded ? &*state.target : nullptr,
                      digest};
                    auto proceed = co_await visit_prepare(scanned);
                    if (!proceed) {
                        failed.observe(proceed);
                        break;
                    }
                    if (auto moved = reader->advance(); !moved) {
                        failed.observe(moved);
                        break;
                    }
                    report.content_end = reader->position();
                    if (state.claims->stamps.ends_write)
                        write_end = report.content_end;
                    ++report.prepares;
                    if (*resolution != wal_target_resolution::resolved)
                        ++report.unresolved;
                    if (!*proceed) {
                        stopped = true;
                        break;
                    }
                }
                // A head whose content the first pass proved whole, but whose
                // decode stopped inside a write, ends where that write began.
                if (
                  content && !stopped && !failed.failed()
                  && report.content_end != content->end)
                    report.content_end = write_end;
                if (
                  head && !bounded && limits.prove && !stopped
                  && !failed.failed()
                  && report.content_end.value() < report.file_bytes) {
                    co_await reader->close();
                    reader.reset();
                    auto probe = scan_reader::make(
                      file,
                      budget,
                      report.content_end,
                      runtime::file_position{report.file_bytes},
                      limits.reader);
                    if (!probe) {
                        failed.observe(probe);
                        break;
                    }
                    reader = std::move(*probe);
                    auto proved = co_await detail::probe_wal_head(
                      *reader, state, limits, work, report, visit_prepare);
                    if (!proved) {
                        failed.observe(proved);
                        break;
                    }
                    if (!*proved) {
                        stopped = true;
                        break;
                    }
                }
                if (
                  failed.failed() || stopped || !link->sealed_end || bounded
                  || report.stop != wal_scan_stop::end
                  || report.file_bytes == link->sealed_end->value())
                    break;
                // Slack is never content; only its first prefix is examined.
                report.slack = byte_count{
                  report.file_bytes - link->sealed_end->value()};
                co_await reader->close();
                reader.reset();
                const auto first = std::min(
                  report.file_bytes,
                  link->sealed_end->value()
                    + limits.reader.window_bytes.value());
                auto probe = scan_reader::make(
                  file,
                  budget,
                  *link->sealed_end,
                  runtime::file_position{first},
                  limits.reader);
                if (!probe) {
                    failed.observe(probe);
                    break;
                }
                reader = std::move(*probe);
                auto slot = co_await reader->next(work);
                if (!slot) {
                    failed.observe(slot);
                    break;
                }
                report.nonzero_slack = slot->kind != scan_slot_kind::zero;
            } while (false);
        } catch (...) {
            failed.observe(std::current_exception());
        }
        if (reader) {
            co_await reader->close();
            reader.reset();
        }
        try {
            failed.observe(co_await file.close());
        } catch (...) {
            failed.observe(std::current_exception());
        }
        if (failed.failed()) {
            auto error = failed.outcome();
            co_return runtime::failure(error.error());
        }
        ++output.files;
        output.prepares += report.prepares;
        output.unresolved += report.unresolved;
        const bool corrupt = !last && !stopped
                             && report.stop != wal_scan_stop::end;
        if (auto seen = co_await visit_file(report); !seen)
            co_return runtime::failure(seen.error());
        if (stopped) co_return output;
        if (corrupt) {
            output.verdict = wal_scan_verdict::corrupt;
            output.complete = true;
            co_return output;
        }
        if (last) {
            auto content = local_wal_cursor::make(
              link->incarnation, report.content_end);
            if (!content)
                co_return runtime::failure(
                  detail::path_error(errc::out_of_range));
            output.content_end = *content;
        }
        if (bounded) break;
    }
    output.complete = true;
    co_return output;
}

// Resolves claimed targets against a caller-supplied catalog of expected
// descriptors and headers: the independent catalog state. Each entry is
// verified on its device through resolve_local_segment once and then reused.
// A claim outside the catalog is unknown; a catalog entry missing from every
// device, or on an unavailable device, is an error. Backend, ownership,
// devices, catalog, budget and work outlive this object and the scan using it.
template<runtime::file_system_backend Backend, local_directory_owner Owner>
class local_wal_target_catalog final {
public:
    struct entry final {
        local_segment_descriptor descriptor;
        segment_header header;
    };
    [[nodiscard]] static runtime::result<
      std::unique_ptr<local_wal_target_catalog>>
    make(
      Backend& files,
      Owner& ownership,
      std::span<const local_device_spec> devices,
      std::uint32_t shard,
      table_view<entry> catalog,
      workload_budget& budget,
      local_store_io_limits limits,
      codec::cooperative_work& work) {
        // The placements, and the entries' places in their segments'
        // canonical order, each a table in fragments.
        const auto resolved = chunked_allocation_charge(
          budget, catalog.size(), sizeof(std::optional<wal_target>));
        const auto order = chunked_allocation_charge(
          budget, catalog.size(), sizeof(std::uint32_t));
        if (!resolved) return runtime::failure(resolved.error());
        if (!order) return runtime::failure(order.error());
        auto reservation = budget.try_reserve(
          resolved->checked_add(*order).value_or(byte_count{}));
        if (!reservation) return runtime::failure(reservation.error());
        auto output = std::unique_ptr<local_wal_target_catalog>(
          new local_wal_target_catalog(
            files,
            ownership,
            devices,
            shard,
            catalog,
            budget,
            limits,
            work,
            std::move(*reservation)));
        output->resolved_.reserve(catalog.size());
        output->order_.reserve(catalog.size());
        for (std::size_t i = 0; i != catalog.size(); ++i) {
            output->resolved_.emplace_back();
            output->order_.push_back(static_cast<std::uint32_t>(i));
        }
        std::sort(
          output->order_.begin(),
          output->order_.end(),
          [&catalog](std::uint32_t a, std::uint32_t b) {
              return catalog[a].descriptor.segment.canonical_less(
                catalog[b].descriptor.segment);
          });
        return output;
    }

    seastar::future<runtime::result<std::optional<wal_target>>>
    operator()(const segment_context& claimed) {
        const auto at = std::lower_bound(
          order_.begin(),
          order_.end(),
          claimed,
          [this](std::uint32_t index, const segment_context& wanted) {
              return catalog_[index].descriptor.segment.canonical_less(wanted);
          });
        if (at == order_.end() || catalog_[*at].descriptor.segment != claimed)
            co_return std::optional<wal_target>{};
        const auto i = *at;
        if (!resolved_[i]) {
            auto located = co_await resolve_local_segment(
              files_,
              ownership_,
              devices_,
              shard_,
              catalog_[i].descriptor,
              catalog_[i].header,
              budget_,
              limits_,
              work_);
            if (!located) co_return runtime::failure(located.error());
            resolved_[i] = make_wal_target(*located);
        }
        co_return resolved_[i];
    }
    // Entry `index` as the caller already located it through
    // resolve_local_segment: it is not resolved again. Only a placement of
    // that entry's segment, alignment and profile is taken.
    [[nodiscard]] runtime::result<void>
    located(std::size_t index, const wal_target& value) noexcept {
        if (index >= catalog_.size())
            return runtime::failure(detail::path_error(errc::invalid_argument));
        const auto& descriptor = catalog_[index].descriptor;
        if (
          value.segment != descriptor.segment
          || value.alignment != descriptor.alignment
          || value.profile != descriptor.profile)
            return runtime::failure(detail::path_error(errc::wrong_context));
        resolved_[index] = value;
        return {};
    }

private:
    local_wal_target_catalog(
      Backend& files,
      Owner& ownership,
      std::span<const local_device_spec> devices,
      std::uint32_t shard,
      table_view<entry> catalog,
      workload_budget& budget,
      local_store_io_limits limits,
      codec::cooperative_work& work,
      workload_reservation reservation) noexcept
      : files_(files)
      , ownership_(ownership)
      , devices_(devices)
      , shard_(shard)
      , catalog_(catalog)
      , budget_(budget)
      , limits_(limits)
      , work_(work)
      , reservation_(std::move(reservation)) {}
    Backend& files_;
    Owner& ownership_;
    std::span<const local_device_spec> devices_;
    std::uint32_t shard_;
    table_view<entry> catalog_;
    workload_budget& budget_;
    local_store_io_limits limits_;
    codec::cooperative_work& work_;
    workload_reservation reservation_;
    seastar::chunked_vector<std::optional<wal_target>> resolved_;
    seastar::chunked_vector<std::uint32_t> order_;
};

} // namespace kwaque::storage
