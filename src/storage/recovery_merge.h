#pragma once

#include "src/base/units.h"
#include "src/storage/page_internal.h"
#include "src/storage/recovery_cases.h"
#include "src/storage/recovery_tail.h"
#include "src/storage/retained_wal.h"
#include "src/storage/segment_scan.h"
#include "src/storage/segment_writer_state.h"
#include "src/storage/table_view.h"
#include "src/storage/wal_scan.h"

#include <seastar/core/coroutine.hh>

#include <algorithm>
#include <exception>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <utility>
#include <variant>
#include <vector>

namespace kwaque::storage {

// One segment generation the merge reconciles, from independent catalog state
// only: its verified descriptor and header, and the publication it was opened
// under.
struct recovery_target final {
    local_segment_descriptor descriptor;
    segment_header header;
    // From the catalog entry.
    model::range_logical_end logical_origin;
    // Active, recovering or sealed. Deleting objects belong to their
    // reconciliation, not to this merge.
    local_object_state state;
    local_publication_generation generation;
    // The newest independently pinned footer: the publication's boundary or
    // checkpoint evidence. Required for a sealed target: its sealed root.
    std::optional<local_footer_reference> pin;
    // Where restart already located it on its device from the same catalog
    // entry; absent, the merge locates it.
    std::optional<wal_target> placement;
};

// One batch slot, classified.
struct recovery_slot_report final {
    segment_context segment;
    runtime::file_position position;
    // The exact WAL source of the PREPARE that names the slot, if one does.
    std::optional<local_wal_cursor> begin, end;
    // The evidence and the one case it matches.
    recovery_parameters evidence{};
    const recovery_case* matched{nullptr};
    recovery_copy_difference difference{recovery_copy_difference::none};
};
// A PREPARE whose claimed target has no independent placement: WAL content
// without authority.
struct recovery_unresolved_report final {
    local_wal_cursor begin, end;
    wal_target_resolution resolution;
    segment_context claimed;
    runtime::file_position position;
};
// A byte range of one file: exactly one of wal and segment is present.
struct recovery_region_report final {
    std::optional<model::wal_incarnation_id> wal;
    std::optional<segment_context> segment;
    runtime::file_position begin, end;
    // The evidence and the one case it matches. A slack region's damage
    // predicate says whether it holds nonzero bytes.
    recovery_parameters evidence{};
    const recovery_case* matched{nullptr};
};
// One segment's recovered boundary, verdict and slot counts.
struct recovery_segment_report final {
    segment_context segment;
    local_object_state state;
    local_publication_generation generation;
    std::optional<local_footer_reference> pin;
    // The boundary restart publishes: the last footer whose covered history
    // verified before the first damage, or the pin itself; past it, the
    // last footer the WAL's PREPAREs let the walk verify, found intact or
    // derived. Absent when none exists or the pin is damaged.
    std::optional<segment_scan_boundary> boundary;
    // False for a sealed target, whose extent is cold and not read here.
    bool scanned{false};
    // Where the walk ended, and where it first met bytes it could not read.
    runtime::file_position content_end;
    std::optional<runtime::file_position> first_damage;
    std::uint64_t file_bytes{0};
    segment_scan_stop stop{segment_scan_stop::end};
    // Corrupt when the boundary does not reach what the segment proves
    // certified.
    segment_scan_verdict verdict{segment_scan_verdict::clean};
    // The most of the pin's end and the certified end every intact block of
    // the segment and every PREPARE naming it carries.
    runtime::file_position proven_end;
    std::uint32_t satisfied{0}, footer_only{0}, materialize{0}, candidates{0},
      suffix{0}, conflicts{0};
    // Unresolved PREPAREs claiming this segment, misplaced ones among them.
    std::uint32_t unresolved{0}, misplaced{0};
    // Slots filled below the proven end; and the blocks made from PREPAREs
    // and the footers derived, which restart writes before it publishes.
    std::uint32_t repaired{0}, blocks{0}, footers{0};
};
// The obligations one WAL file holds toward one segment, rebuilt for the
// checkpoint owner: the append path's in-memory rows do not survive restart.
struct recovery_obligation final {
    model::wal_incarnation_id wal;
    segment_context segment;
    // PREPAREs whose segment copy is certified: dischargeable once that
    // boundary is durable.
    std::uint32_t satisfied{0};
    // Candidates, suffix, conflicts and unresolved claims: pinned until a
    // supplied resolution.
    std::uint32_t pinned{0};
    // The first pinned PREPARE in WAL order.
    std::optional<local_wal_cursor> first_pinned;
};
using recovery_item = std::variant<
  recovery_slot_report,
  recovery_unresolved_report,
  recovery_region_report,
  recovery_segment_report,
  recovery_obligation>;

// Where an enumeration stopped by its visitor resumes: the number of items
// already delivered, bound to every object and generation the merge read. A
// mutation of any of them changes the binding, so the token cannot survive it.
struct recovery_resume final {
    std::uint64_t items{0};
    codec::content_digest binding{};
    bool operator==(const recovery_resume&) const = default;
};
struct recovery_merge_result final {
    wal_scan_result wal;
    // Items handed to the visitor by this call, after any resumed prefix.
    std::uint64_t delivered{0};
    // Segment walks reopened at their last verified footer after eviction.
    std::uint32_t reloads{0};
    // Present when the visitor stopped the merge early.
    std::optional<recovery_resume> resume;
};

inline constexpr std::uint32_t maximum_recovery_cursors = 8;
// The most obligation rows one restart rebuilds: a row for each segment a
// restart's catalog may list (`maximum_recovery_catalog`) in each WAL file a
// shard may keep.
inline constexpr std::uint32_t maximum_recovery_obligations
  = 8192 * maximum_retained_wal_files;
struct recovery_merge_limits final {
    wal_scan_limits wal{};
    segment_scan_limits segment{};
    // Segment walks open at once. The least recently advanced one is closed
    // and later reopened at its last verified footer. A walk runs only for
    // its segment's queued PREPAREs, so this changes how often walks reopen,
    // never what is delivered.
    std::uint32_t cursors{2};
    // Per segment, the PREPAREs queued for its walk in WAL order. A full
    // queue runs the walk at once, so this changes when walks run and the
    // delivery order, never what is classified.
    std::uint32_t pending{128};
    // (WAL file, segment) obligation rows. The table grows as rows appear,
    // charged a fragment at a time; this only bounds it.
    std::uint32_t obligations{maximum_recovery_obligations};

    [[nodiscard]] runtime::result<void> validate() const noexcept;
};

namespace detail {
// A slot whose classification waits for coverage to become known.
struct recovery_pending final {
    runtime::file_position position;
    std::optional<local_wal_cursor> begin, end;
    // The PREPARE's place in WAL order.
    std::uint64_t order{0};
    bool prepare{false}, block{false}, placed{false};
    recovery_copy_difference difference{recovery_copy_difference::none};
};

// A resolved PREPARE waiting for its segment's walk. Its copy is gone, so a
// slot the walk compares keeps the PREPARE's claims and child identity.
struct recovery_queued final {
    local_wal_cursor begin, end;
    runtime::file_position position;
    // The PREPARE's place in WAL order.
    std::uint64_t order{0};
    bool ordered{false};
    std::optional<recovery_copy_identity> copy;
};
// A segment's queue is kept in chunks of this many, each charged the first
// time the queue needs it: a target no PREPARE names takes none.
inline constexpr std::size_t recovery_queue_chunk = 16;
// No obligation row.
inline constexpr std::uint32_t recovery_no_row
  = std::numeric_limits<std::uint32_t>::max();

// Everything the merge keeps per target; it survives a walk's eviction.
struct recovery_segment_state final {
    explicit recovery_segment_state(const recovery_target& value)
      : target(&value)
      , resume(value.pin)
      , report{
          value.descriptor.segment, value.state, value.generation, value.pin} {}

    const recovery_target* target;
    std::optional<segment_history_context> history;
    const local_device_spec* device{nullptr};
    // The last target position in WAL order.
    std::optional<runtime::file_position> last_target;
    recovery_list<recovery_queued, recovery_queue_chunk> queued;
    // The first of its obligation rows, one for each WAL file its PREPAREs
    // lie in; each names the next.
    std::uint32_t rows{recovery_no_row};
    // Slots after the newest verified footer, until a later one covers them
    // or the walk ends; a held run's outnumber a group's.
    recovery_list<recovery_pending, recovery_list_chunk> pending;
    // The PREPAREs among them, and those after the walk ended: its tail.
    recovery_list<recovery_tail_prepare, recovery_list_chunk> tail;
    // The most certified end the WAL's PREPAREs naming it carry.
    runtime::file_position proven;
    // Where a reopened walk resumes: the newest verified footer, else the pin.
    std::optional<local_footer_reference> resume;
    // The end of the last object the merge processed.
    runtime::file_position processed;
    bool stopped{false};
    // Once stopped: where and why the walk ended.
    segment_scan_report ended;
    recovery_segment_report report;
};

struct recovery_obligation_row final {
    recovery_obligation value;
    std::uint64_t first{std::numeric_limits<std::uint64_t>::max()};
    // Its segment's next row.
    std::uint32_t next{recovery_no_row};
};

[[nodiscard]] runtime::result<void>
  validate_recovery_targets(table_view<recovery_target>) noexcept;
// The resume binding: the WAL head and checkpoint the control pins, the
// cutoff, every target's identity, state, generation and pin, and the queue
// bound, which decides where a full queue's slots fall in the enumeration.
[[nodiscard]] codec::content_digest recovery_binding(
  const local_shard_control&,
  std::optional<local_wal_cursor> cutoff,
  table_view<recovery_target>,
  std::uint32_t pending) noexcept;
[[nodiscard]] runtime::file_position
footer_end(const local_footer_reference&) noexcept;
// Where certified bytes end so far: the recovered boundary or pin, else the
// data start.
[[nodiscard]] runtime::file_position
certified_end(const recovery_segment_state&) noexcept;
// Counts a delivered slot against its segment.
void count_slot(recovery_segment_report&, recovery_classification) noexcept;

template<
  runtime::file_system_backend Backend,
  local_directory_owner Owner,
  typename Visitor>
class recovery_merge final {
public:
    recovery_merge(
      Backend& files,
      Owner& ownership,
      std::span<const local_device_spec> devices,
      const local_device_spec& wal_device,
      std::uint32_t shard,
      table_view<recovery_target> targets,
      workload_budget& budget,
      recovery_merge_limits limits,
      codec::cooperative_work& work,
      Visitor& visit,
      std::uint64_t skip,
      codec::content_digest binding,
      recovery_tails& tails) noexcept
      : files_(files)
      , ownership_(ownership)
      , devices_(devices)
      , wal_device_(wal_device)
      , shard_(shard)
      , targets_(targets)
      , budget_(budget)
      , limits_(limits)
      , work_(work)
      , visit_(visit)
      , skip_(skip)
      , binding_(binding)
      , tails_(tails) {}

    seastar::future<runtime::result<recovery_merge_result>> run(
      const local_shard_control& control,
      std::optional<local_wal_cursor> cutoff) {
        auto prepared = prepare();
        if (!prepared) co_return runtime::failure(prepared.error());
        runtime::first_failure failed;
        recovery_merge_result output;
        try {
            failed.observe(co_await merge(control, cutoff, output));
        } catch (...) {
            failed.observe(std::current_exception());
        }
        // Every walk is closed, whatever an earlier close threw.
        for (std::size_t i = 0; i < cursors_.size(); ++i) {
            try {
                failed.observe(co_await unload(i));
            } catch (...) {
                failed.observe(std::current_exception());
            }
        }
        catalog_.reset();
        if (failed.failed()) {
            auto error = failed.outcome();
            co_return runtime::failure(error.error());
        }
        output.delivered = delivered_;
        output.reloads = reloads_;
        if (stopped_) output.resume = recovery_resume{ordinal_, binding_};
        co_return output;
    }

private:
    using catalog_type = local_wal_target_catalog<Backend, Owner>;
    // One segment walk while it is open.
    struct cursor final {
        std::optional<workload_reservation> handle;
        std::optional<runtime::file> file;
        std::unique_ptr<segment_scanner> scanner;
        // Borrowed from the scanner until its next call.
        std::optional<segment_scanned_object> current;
        std::uint64_t used{0};
        bool opened{false};
    };
    using tail_walk = recovery_tail_walk<Backend, Owner>;
    // Hands what a segment's tail walk settles to the visitor.
    struct tail_sink final {
        recovery_merge* merge;
        std::size_t index;
        seastar::future<runtime::result<bool>>
        slot(const typename tail_walk::slot_event& value) {
            return merge->slot(
              index,
              value.position,
              value.begin,
              value.end,
              value.order,
              value.evidence);
        }
        seastar::future<runtime::result<bool>>
        region(const typename tail_walk::region_event& value) {
            return merge->deliver(merge->segment_region(
              index,
              value.begin,
              value.end,
              true,
              value.certified,
              value.filled));
        }
    };

    runtime::result<void> prepare() {
        if (auto valid = limits_.validate(); !valid) return valid;
        if (auto valid = validate_recovery_targets(targets_); !valid)
            return valid;
        const auto count = targets_.size();
        byte_count total;
        // The per-target tables are chunked: a restart reads up to a
        // catalog's worth of targets. A target's queue and the obligation
        // rows are charged as they grow, so that what no PREPARE names costs
        // nothing.
        for (const auto element :
             {sizeof(typename catalog_type::entry),
              sizeof(recovery_segment_state),
              sizeof(cursor),
              sizeof(std::uint32_t)}) {
            auto charged = chunked_allocation_charge(budget_, count, element);
            if (!charged) return runtime::failure(charged.error());
            auto next = total.checked_add(*charged);
            if (!next)
                return runtime::failure(detail::path_error(errc::out_of_range));
            total = *next;
        }
        auto held = budget_.try_reserve(total);
        if (!held) return runtime::failure(held.error());
        held_.emplace(std::move(*held));
        entries_.reserve(count);
        segments_.reserve(count);
        cursors_.reserve(count);
        order_index_.reserve(count);
        for (const auto& target : targets_) {
            entries_.push_back({target.descriptor, target.header});
            segments_.emplace_back(target);
            cursors_.emplace_back();
            order_index_.push_back(
              static_cast<std::uint32_t>(order_index_.size()));
        }
        // Each target's place in its segment's canonical order, for lookup.
        std::sort(
          order_index_.begin(),
          order_index_.end(),
          [this](std::uint32_t a, std::uint32_t b) {
              return targets_[a].descriptor.segment.canonical_less(
                targets_[b].descriptor.segment);
          });
        tails_.wal = limits_.wal;
        tails_.segment = limits_.segment;
        auto made = catalog_type::make(
          files_,
          ownership_,
          devices_,
          shard_,
          entries_,
          budget_,
          limits_.wal.metadata,
          work_);
        if (!made) return runtime::failure(made.error());
        catalog_ = std::move(*made);
        for (std::size_t i = 0; i < count; ++i)
            if (const auto& placement = targets_[i].placement)
                if (auto taken = catalog_->located(i, *placement); !taken)
                    return taken;
        return {};
    }

    seastar::future<runtime::result<void>> merge(
      const local_shard_control& control,
      std::optional<local_wal_cursor> cutoff,
      recovery_merge_result& output) {
        // The head is read on after its content for what its stamps prove.
        auto wal = limits_.wal;
        wal.prove = true;
        auto scanned = co_await scan_local_wal(
          files_,
          ownership_,
          wal_device_,
          shard_,
          control,
          cutoff,
          budget_,
          wal,
          work_,
          *catalog_,
          [this](const wal_scanned_prepare& value) {
              return on_prepare(value);
          },
          [this](const wal_file_scan& value) { return on_file(value); });
        if (!scanned) co_return runtime::failure(scanned.error());
        output.wal = *scanned;
        if (stopped_) co_return runtime::result<void>{};
        for (std::size_t i = 0; i < segments_.size(); ++i) {
            auto ended = co_await finish_segment(i);
            if (!ended) co_return runtime::failure(ended.error());
            if (!*ended) co_return runtime::result<void>{};
        }
        for (const auto& row : rows_) {
            auto proceed = co_await deliver(row.value);
            if (!proceed) co_return runtime::failure(proceed.error());
            if (!*proceed) co_return runtime::result<void>{};
        }
        co_return runtime::result<void>{};
    }

    // Hands one item to the visitor, or skips it when a resumed enumeration
    // already delivered it. False once the visitor stops: a stop inside a
    // file's regions lets the WAL scan reach the next file, but nothing more
    // is delivered or counted.
    seastar::future<runtime::result<bool>> deliver(recovery_item item) {
        if (stopped_) co_return false;
        if (ordinal_ < skip_) {
            ++ordinal_;
            co_return true;
        }
        auto proceed = co_await visit_(item);
        if (!proceed) co_return runtime::failure(proceed.error());
        ++ordinal_;
        ++delivered_;
        if (!*proceed) stopped_ = true;
        co_return *proceed;
    }

    std::optional<std::size_t> find(const segment_context& segment) const {
        const auto at = std::lower_bound(
          order_index_.begin(),
          order_index_.end(),
          segment,
          [this](std::uint32_t index, const segment_context& wanted) {
              return targets_[index].descriptor.segment.canonical_less(wanted);
          });
        if (
          at == order_index_.end()
          || targets_[*at].descriptor.segment != segment)
            return std::nullopt;
        return std::size_t{*at};
    }

    // Placement comes from the catalog's verified header and device, never
    // from a PREPARE.
    runtime::result<void> start(std::size_t index, const wal_target& target) {
        auto& state = segments_[index];
        if (state.history) return {};
        const auto& descriptor = state.target->descriptor;
        const auto device = std::find_if(
          devices_.begin(), devices_.end(), [&target](const auto& spec) {
              return spec.owner.device() == target.device;
          });
        if (device == devices_.end())
            return runtime::failure(detail::path_error(errc::unavailable));
        state.device = &*device;
        state.history = segment_history_context{
          descriptor.segment,
          target.alignment,
          target.data_start,
          state.target->logical_origin,
          descriptor.physical_origin,
          target.profile};
        state.processed = state.target->pin ? footer_end(*state.target->pin)
                                            : target.data_start;
        state.report.content_end = state.processed;
        return {};
    }

    // Records a PREPARE's slot against its WAL file's obligation row. A
    // target's rows are found through its own chain, one row for each WAL
    // file its PREPAREs lie in; a claim on a segment no target is, among
    // all rows.
    runtime::result<void> oblige(
      const local_wal_cursor& begin,
      const segment_context& segment,
      std::optional<std::size_t> index,
      std::uint64_t order,
      bool satisfied) {
        const auto matches = [&](const recovery_obligation_row& row) {
            return row.value.wal == begin.incarnation()
                   && row.value.segment == segment;
        };
        std::optional<std::size_t> found, last;
        if (index) {
            for (auto at = segments_[*index].rows; at != recovery_no_row;
                 at = rows_[at].next) {
                if (matches(rows_[at])) {
                    found = at;
                    break;
                }
                last = at;
            }
        } else {
            for (std::size_t at = 0; at != rows_.size(); ++at)
                if (matches(rows_[at])) {
                    found = at;
                    break;
                }
        }
        if (!found) {
            if (rows_.size() == limits_.obligations)
                return runtime::failure(
                  detail::path_error(errc::resource_exhausted));
            if (auto room = row_room(); !room) return room;
            try {
                rows_.push_back({{begin.incarnation(), segment, 0, 0, {}}});
            } catch (const std::bad_alloc&) {
                return runtime::failure(
                  detail::path_error(errc::resource_exhausted));
            }
            found = rows_.size() - 1;
            const auto added = static_cast<std::uint32_t>(*found);
            if (last)
                rows_[*last].next = added;
            else if (index)
                segments_[*index].rows = added;
        }
        auto& row = rows_[*found];
        if (satisfied) {
            ++row.value.satisfied;
        } else {
            ++row.value.pinned;
            if (order < row.first) {
                row.first = order;
                row.value.first_pinned = begin;
            }
        }
        return {};
    }

    // Room for one more obligation row. The table grows a fragment at a
    // time, each charged when the table first needs it; the first is made
    // whole at once, so that no row moves.
    runtime::result<void> row_room() {
        if (rows_.size() < rows_.capacity()) return {};
        const auto fragment = chunked_fragment_elements(
          sizeof(recovery_obligation_row));
        const auto before = chunked_allocation_charge(
          budget_, rows_.capacity(), sizeof(recovery_obligation_row));
        const auto after = chunked_allocation_charge(
          budget_,
          rows_.capacity() + fragment,
          sizeof(recovery_obligation_row));
        if (!before) return runtime::failure(before.error());
        if (!after) return runtime::failure(after.error());
        if (
          auto taken = hold_more(
            grown_, budget_, byte_count{after->value() - before->value()});
          !taken)
            return taken;
        try {
            if (rows_.empty()) rows_.reserve(fragment);
        } catch (const std::bad_alloc&) {
            return runtime::failure(
              detail::path_error(errc::resource_exhausted));
        }
        return {};
    }

    seastar::future<runtime::result<bool>> slot(
      std::size_t index,
      runtime::file_position position,
      std::optional<local_wal_cursor> begin,
      std::optional<local_wal_cursor> end,
      std::uint64_t order,
      recovery_slot_evidence evidence) {
        auto& state = segments_[index];
        const auto parameters = batch_parameters(evidence);
        const auto& matched = classify_recovery(parameters);
        count_slot(state.report, matched.classification);
        if (begin) {
            // A slot restart fills or adopts below what it publishes is met
            // once that publication is durable.
            const bool satisfied = matched.classification
                                     == recovery_classification::satisfied
                                   || matched.classification
                                        == recovery_classification::materialize;
            if (
              auto obliged = oblige(
                *begin,
                state.target->descriptor.segment,
                index,
                order,
                satisfied);
              !obliged)
                co_return runtime::failure(obliged.error());
        }
        co_return co_await deliver(
          recovery_slot_report{
            state.target->descriptor.segment,
            position,
            begin,
            end,
            parameters,
            &matched,
            evidence.difference});
    }

    // Classifies every waiting slot now that its coverage is known: a footer
    // covers them, or none will. Covered, they are no tail.
    seastar::future<runtime::result<bool>>
    settle(std::size_t index, bool covered) {
        auto& state = segments_[index];
        for (const auto& waiting : state.pending) {
            recovery_slot_evidence evidence;
            evidence.prepare = waiting.prepare;
            evidence.block = waiting.block;
            evidence.covered = covered;
            evidence.ordered = true;
            evidence.placed = waiting.placed;
            evidence.difference = waiting.difference;
            auto proceed = co_await slot(
              index,
              waiting.position,
              waiting.begin,
              waiting.end,
              waiting.order,
              evidence);
            if (!proceed || !*proceed) co_return proceed;
        }
        state.pending.clear();
        if (covered) state.tail.clear();
        co_return true;
    }

    runtime::result<void>
    wait(std::size_t index, const recovery_pending& waiting) {
        return segments_[index].pending.push(waiting, budget_, grown_);
    }
    // Keeps a PREPARE whose slot lies after the newest verified footer for
    // the segment's tail walk, unless a later footer covers it.
    runtime::result<void>
    defer(std::size_t index, const recovery_queued& queued) {
        if (!queued.copy)
            return runtime::failure(
              detail::path_error(errc::invariant_violation));
        return segments_[index].tail.push(
          {queued.begin, queued.end, queued.order, *queued.copy},
          budget_,
          tails_.held);
    }

    seastar::future<runtime::result<void>> unload(std::size_t index) {
        auto& walk = cursors_[index];
        walk.current.reset();
        runtime::first_failure failed;
        if (walk.scanner) {
            co_await walk.scanner->close();
            walk.scanner.reset();
        }
        if (walk.file) {
            try {
                failed.observe(co_await walk.file->close());
            } catch (...) {
                failed.observe(std::current_exception());
            }
            walk.file.reset();
        }
        walk.handle.reset();
        if (walk.opened) {
            walk.opened = false;
            --open_;
        }
        co_return failed.outcome();
    }

    // Closes the least recently advanced open walks until another may open.
    seastar::future<runtime::result<void>> make_room() {
        while (open_ >= limits_.cursors) {
            std::optional<std::size_t> oldest;
            for (std::size_t i = 0; i < cursors_.size(); ++i)
                if (
                  cursors_[i].opened
                  && (!oldest || cursors_[i].used < cursors_[*oldest].used))
                    oldest = i;
            if (!oldest) break;
            if (auto closed = co_await unload(*oldest); !closed)
                co_return closed;
        }
        co_return runtime::result<void>{};
    }

    // Opens the walk at its resume point, closing the least recently advanced
    // open walk first when the bound is reached.
    seastar::future<runtime::result<void>> load(std::size_t index) {
        auto& walk = cursors_[index];
        walk.used = ++clock_;
        if (walk.opened) co_return runtime::result<void>{};
        if (auto room = co_await make_room(); !room) co_return room;
        auto& state = segments_[index];
        auto handle = budget_.try_reserve(
          limits_.segment.metadata.execution_bytes);
        if (!handle) co_return runtime::failure(handle.error());
        walk.handle.emplace(std::move(*handle));
        walk.opened = true;
        ++open_;
        auto opened = co_await open_local_segment_data(
          files_,
          ownership_,
          *state.device,
          shard_,
          *state.history,
          *walk.handle,
          work_);
        if (!opened) co_return runtime::failure(opened.error());
        walk.file.emplace(std::move(*opened));
        const bool reopened = state.resume != state.target->pin
                              || state.processed != footer_start(state);
        if (reopened) ++reloads_;
        auto made = co_await segment_scanner::make(
          *walk.file,
          budget_,
          *state.history,
          state.resume,
          limits_.segment,
          work_);
        if (!made) co_return runtime::failure(made.error());
        walk.scanner = std::move(*made);
        // A reopened walk resumes at a footer this pass already verified;
        // damage there means the bytes changed under the merge.
        if (
          reopened
          && walk.scanner->report().verdict == segment_scan_verdict::corrupt)
            co_return runtime::failure(detail::path_error(errc::corrupt_data));
        co_return runtime::result<void>{};
    }

    runtime::file_position footer_start(const recovery_segment_state& state) {
        return state.target->pin ? footer_end(*state.target->pin)
                                 : state.history->data_start;
    }

    static runtime::file_position
    object_begin(const segment_scanned_object& object) noexcept {
        return object.block
                 ? object.block->descriptor().coverage().bytes().begin()
                 : object.footer->footer.position();
    }
    static runtime::file_position
    object_end(const segment_scanned_object& object) noexcept {
        return object.block
                 ? object.block->descriptor().coverage().bytes().end()
                 : footer_end(object.footer->footer);
    }

    // The next unprocessed object, or false once the walk has ended. Objects a
    // reopened walk reads again are skipped.
    seastar::future<runtime::result<bool>> next_object(std::size_t index) {
        auto& state = segments_[index];
        auto& walk = cursors_[index];
        for (;;) {
            if (walk.current) co_return true;
            if (state.stopped) co_return false;
            if (auto loaded = co_await load(index); !loaded)
                co_return runtime::failure(loaded.error());
            auto next = co_await walk.scanner->next(work_);
            if (!next) co_return runtime::failure(next.error());
            if (!*next) {
                // What survives after the damage still proves how far the
                // segment was certified.
                if (auto proved = co_await walk.scanner->prove(work_); !proved)
                    co_return runtime::failure(proved.error());
                state.stopped = true;
                state.ended = walk.scanner->report();
                if (auto closed = co_await unload(index); !closed)
                    co_return runtime::failure(closed.error());
                co_return false;
            }
            if (object_begin(**next) < state.processed) continue;
            walk.current = **next;
        }
    }

    // Consumes the current object: a block waits for coverage, a footer covers
    // every waiting slot.
    seastar::future<runtime::result<bool>> process(std::size_t index) {
        auto& state = segments_[index];
        auto& walk = cursors_[index];
        const auto object = *walk.current;
        walk.current.reset();
        state.processed = object_end(object);
        if (object.block) {
            recovery_pending waiting;
            waiting.position = object_begin(object);
            waiting.block = true;
            if (auto waited = wait(index, waiting); !waited)
                co_return runtime::failure(waited.error());
            co_return true;
        }
        state.resume = object.footer->footer;
        co_return co_await settle(index, true);
    }

    // Moves the walk to the first object at or after `position`, processing
    // everything before it. Once the walk ends, its waiting slots wait for
    // the segment's tail walk, or for none.
    seastar::future<runtime::result<bool>>
    advance(std::size_t index, runtime::file_position position) {
        auto& walk = cursors_[index];
        for (;;) {
            auto more = co_await next_object(index);
            if (!more) co_return runtime::failure(more.error());
            if (!*more) co_return true;
            if (object_begin(*walk.current) >= position) co_return true;
            auto proceed = co_await process(index);
            if (!proceed || !*proceed) co_return proceed;
        }
    }

    seastar::future<runtime::result<bool>>
    on_prepare(const wal_scanned_prepare& value) {
        const auto& stamps = value.claims.stamps;
        // An intact PREPARE after the head's content: its target's stamp is
        // evidence, nothing else of it.
        if (value.survival) {
            if (auto index = find(value.claims.target)) {
                auto& proven = segments_[*index].proven;
                proven = std::max(proven, stamps.target_certified_end);
            }
            co_return true;
        }
        const auto order = order_++;
        if (value.resolution != wal_target_resolution::resolved) {
            const auto& claims = value.claims;
            const auto index = find(claims.target);
            if (index) {
                auto& report = segments_[*index].report;
                ++report.unresolved;
                if (value.resolution == wal_target_resolution::misplaced)
                    ++report.misplaced;
            }
            if (
              auto obliged = oblige(
                value.begin, claims.target, index, order, false);
              !obliged)
                co_return runtime::failure(obliged.error());
            co_return co_await deliver(
              recovery_unresolved_report{
                value.begin,
                value.end,
                value.resolution,
                claims.target,
                claims.target_position});
        }
        const auto& prepare = *value.prepare;
        const auto placement = prepare.target();
        const auto index = find(placement.segment());
        if (!index)
            co_return runtime::failure(
              detail::path_error(errc::invariant_violation));
        if (auto started = start(*index, *value.target); !started)
            co_return runtime::failure(started.error());
        auto& state = segments_[*index];
        state.proven = std::max(state.proven, stamps.target_certified_end);
        const auto at = placement.position();
        const bool ordered = !state.last_target || at > *state.last_target;
        if (ordered) state.last_target = at;
        recovery_queued queued{
          value.begin, value.end, at, order, ordered, std::nullopt};
        // Only a slot the walk will compare with a block keeps its copy's
        // identity: not one inside pinned evidence, after a sealed root or
        // out of WAL order.
        const auto& pin = state.target->pin;
        if (
          ordered && !(pin && at < footer_end(*pin))
          && state.target->state != local_object_state::sealed) {
            auto child = co_await detail::hash_exact(
              prepare.batch().bytes(), work_, {});
            if (!child)
                co_return runtime::failure(
                  detail::path_error(child.error().code()));
            queued.copy = recovery_identity(prepare, *child);
        }
        if (state.queued.size() == limits_.pending) {
            auto served = co_await serve(*index);
            if (!served || !*served) co_return served;
        }
        if (
          auto pushed = state.queued.push(std::move(queued), budget_, grown_);
          !pushed)
            co_return runtime::failure(pushed.error());
        co_return true;
    }

    // Runs a segment's walk through its queued PREPAREs in WAL order, then
    // empties the queue. False once the visitor stops.
    seastar::future<runtime::result<bool>> serve(std::size_t index) {
        auto& queued = segments_[index].queued;
        for (const auto& waiting : queued) {
            auto proceed = co_await reconcile(index, waiting);
            if (!proceed || !*proceed) co_return proceed;
        }
        queued.clear();
        co_return true;
    }

    // One queued PREPARE's slot against its segment's walk.
    seastar::future<runtime::result<bool>>
    reconcile(std::size_t index, const recovery_queued& queued) {
        auto& state = segments_[index];
        const auto at = queued.position;
        recovery_slot_evidence evidence;
        evidence.prepare = true;
        evidence.ordered = queued.ordered;
        const auto& pin = state.target->pin;
        // A slot inside pinned evidence gets a layout check only: it must
        // start before the pinned footer or sealed root.
        if (pin && at < footer_end(*pin)) {
            evidence.pinned = true;
            evidence.covered = true;
            evidence.placed = at < pin->position();
            co_return co_await slot(
              index, at, queued.begin, queued.end, queued.order, evidence);
        }
        // Nothing follows a sealed root.
        if (state.target->state == local_object_state::sealed)
            co_return co_await slot(
              index, at, queued.begin, queued.end, queued.order, evidence);
        if (!queued.ordered) {
            // The walk only moves forward, so an earlier target is compared
            // with what is still known: certified bytes have no unmatched slot
            // start, and a waiting block may start there.
            evidence.covered = at < certified_end(state);
            if (!evidence.covered)
                for (const auto& waiting : state.pending)
                    if (waiting.block && waiting.position == at)
                        evidence.block = true;
            evidence.placed = evidence.block;
            co_return co_await slot(
              index, at, queued.begin, queued.end, queued.order, evidence);
        }
        auto moved = co_await advance(index, at);
        if (!moved || !*moved) co_return moved;
        auto& walk = cursors_[index];
        if (state.stopped) {
            // Inside certified content the slot starts within another
            // object. After it, the tail walk settles the slot.
            if (at < certified_end(state)) {
                evidence.covered = true;
                co_return co_await slot(
                  index, at, queued.begin, queued.end, queued.order, evidence);
            }
            if (auto deferred = defer(index, queued); !deferred)
                co_return runtime::failure(deferred.error());
            co_return true;
        }
        const auto& current = *walk.current;
        if (object_begin(current) == at && current.block) {
            if (!queued.copy)
                co_return runtime::failure(
                  detail::path_error(errc::invariant_violation));
            auto difference = co_await compare_recovery_identity(
              *queued.copy, *current.block, work_);
            if (!difference) co_return runtime::failure(difference.error());
            recovery_pending matched;
            matched.position = at;
            matched.begin = queued.begin;
            matched.end = queued.end;
            matched.order = queued.order;
            matched.prepare = true;
            matched.block = true;
            matched.placed = true;
            matched.difference = *difference;
            state.processed = object_end(current);
            walk.current.reset();
            if (auto waited = wait(index, matched); !waited)
                co_return runtime::failure(waited.error());
            if (auto deferred = defer(index, queued); !deferred)
                co_return runtime::failure(deferred.error());
            co_return true;
        }
        if (object_begin(current) == at) {
            // A verified footer occupies the slot.
            auto proceed = co_await process(index);
            if (!proceed || !*proceed) co_return proceed;
            evidence.covered = true;
            co_return co_await slot(
              index, at, queued.begin, queued.end, queued.order, evidence);
        }
        // The slot starts inside the last processed object.
        if (at < certified_end(state)) {
            evidence.covered = true;
            co_return co_await slot(
              index, at, queued.begin, queued.end, queued.order, evidence);
        }
        recovery_pending misplaced;
        misplaced.position = at;
        misplaced.begin = queued.begin;
        misplaced.end = queued.end;
        misplaced.order = queued.order;
        misplaced.prepare = true;
        if (auto waited = wait(index, misplaced); !waited)
            co_return runtime::failure(waited.error());
        if (auto deferred = defer(index, queued); !deferred)
            co_return runtime::failure(deferred.error());
        co_return true;
    }

    seastar::future<runtime::result<void>> on_file(const wal_file_scan& value) {
        // Where a tail walk reads this file's PREPAREs again.
        if (
          auto kept = tails_.files.push(
            {value.incarnation,
             value.alignment,
             value.profile,
             value.data_start},
            budget_,
            tails_.held);
          !kept)
            co_return runtime::failure(kept.error());
        const bool rotated = value.sealed_end.has_value();
        const auto region = [&](
                              runtime::file_position begin,
                              runtime::file_position end,
                              bool slack,
                              bool damage,
                              bool certified) {
            const auto parameters = region_parameters(slack, damage, certified);
            return recovery_region_report{
              value.incarnation,
              std::nullopt,
              begin,
              end,
              parameters,
              &classify_recovery(parameters)};
        };
        std::array<std::optional<recovery_region_report>, 3> regions;
        if (value.begin < value.content_end)
            regions[0] = region(
              value.begin, value.content_end, false, false, rotated);
        if (value.stop != wal_scan_stop::end) {
            // A rotated file's damage lies inside its sealed prefix; the
            // head's is certified when a stamp proves it was.
            const runtime::file_position end{
              rotated
                ? std::max(value.sealed_end->value(), value.content_end.value())
                : std::max(value.file_bytes, value.content_end.value())};
            regions[1] = region(
              value.content_end,
              end,
              false,
              true,
              rotated || value.content_end < value.proven_end);
        }
        if (rotated && value.slack.value() != 0)
            regions[2] = region(
              *value.sealed_end,
              runtime::file_position{value.file_bytes},
              true,
              value.nonzero_slack,
              false);
        for (const auto& item : regions) {
            if (!item) continue;
            auto proceed = co_await deliver(*item);
            if (!proceed) co_return runtime::failure(proceed.error());
            if (!*proceed) break;
        }
        co_return runtime::result<void>{};
    }

    recovery_region_report segment_region(
      std::size_t index,
      runtime::file_position begin,
      runtime::file_position end,
      bool damage,
      bool certified,
      bool filled = false) const {
        const auto parameters = region_parameters(
          false, damage, certified, filled);
        return recovery_region_report{
          std::nullopt,
          segments_[index].report.segment,
          begin,
          end,
          parameters,
          &classify_recovery(parameters)};
    }

    // Serves the segment's queue, walks the rest of it, then reports its
    // slots, regions and summary. A segment with PREPAREs after its
    // recovered boundary goes on with its tail walk.
    seastar::future<runtime::result<bool>> finish_segment(std::size_t index) {
        auto& state = segments_[index];
        auto& report = state.report;
        if (auto served = co_await serve(index); !served || !*served)
            co_return served;
        if (state.target->state == local_object_state::sealed)
            co_return co_await deliver(report);
        if (!state.history) {
            auto found = co_await (*catalog_)(state.target->descriptor.segment);
            if (!found) co_return runtime::failure(found.error());
            if (!*found)
                co_return runtime::failure(
                  detail::path_error(errc::invariant_violation));
            if (auto started = start(index, **found); !started)
                co_return runtime::failure(started.error());
        }
        auto moved = co_await advance(
          index,
          runtime::file_position{std::numeric_limits<std::uint64_t>::max()});
        if (!moved || !*moved) co_return moved;
        const auto& ended = state.ended;
        report.scanned = true;
        report.proven_end = std::max(state.proven, ended.proven_end);
        if (!state.tail.empty()) {
            if (ended.verdict != segment_scan_verdict::corrupt)
                co_return co_await finish_tail(index);
            // Its pinned footer is damaged: the segment stops, and its tail
            // is never walked.
            for (const auto& entry : state.tail) {
                recovery_slot_evidence evidence;
                evidence.prepare = true;
                evidence.ordered = true;
                evidence.placed = entry.position() >= ended.content_end;
                auto proceed = co_await slot(
                  index,
                  entry.position(),
                  entry.begin,
                  entry.end,
                  entry.order,
                  evidence);
                if (!proceed || !*proceed) co_return proceed;
            }
        }
        // Blocks after the boundary with no PREPARE: suffix.
        if (auto settled = co_await settle(index, false); !settled || !*settled)
            co_return settled;
        if (auto proceed = co_await content(index); !proceed || !*proceed)
            co_return proceed;
        report.boundary = ended.boundary;
        report.content_end = ended.content_end;
        report.file_bytes = ended.file_bytes;
        report.stop = ended.stop;
        report.verdict = ended.verdict;
        if (ended.stop != segment_scan_stop::end)
            report.first_damage = ended.content_end;
        if (certified_end(state) < report.proven_end)
            report.verdict = segment_scan_verdict::corrupt;
        if (ended.stop != segment_scan_stop::end) {
            const runtime::file_position end{
              std::max(ended.file_bytes, ended.content_end.value())};
            auto proceed = co_await deliver(segment_region(
              index,
              ended.content_end,
              end,
              true,
              ended.content_end < report.proven_end));
            if (!proceed || !*proceed) co_return proceed;
        }
        co_return co_await deliver(report);
    }

    // The verified content before the segment's first damage.
    seastar::future<runtime::result<bool>> content(std::size_t index) {
        const auto& state = segments_[index];
        const auto data_start = state.history->data_start;
        const auto end = state.ended.content_end;
        if (data_start >= end) co_return true;
        co_return co_await deliver(
          segment_region(index, data_start, end, false, true));
    }

    // Walks the segment from its recovered boundary through its tail
    // PREPAREs, reading each again from the WAL only to fill its slot, and
    // reports what the walk settles. The walk counts against the bound on
    // open walks.
    seastar::future<runtime::result<bool>> finish_tail(std::size_t index) {
        auto& state = segments_[index];
        auto& report = state.report;
        // Its slots are settled again by the tail walk.
        state.pending.clear();
        if (auto proceed = co_await content(index); !proceed || !*proceed)
            co_return proceed;
        if (auto room = co_await make_room(); !room)
            co_return runtime::failure(room.error());
        const auto& descriptor = state.target->descriptor;
        if (
          auto kept = tails_.segments.push(
            {index,
             *state.history,
             state.device->owner.device(),
             state.resume,
             runtime::file_position{descriptor.maximum_data_bytes.value()},
             report.proven_end,
             std::move(state.tail),
             std::nullopt,
             0,
             0},
            budget_,
            tails_.held);
          !kept)
            co_return runtime::failure(kept.error());
        auto& tail = tails_.segments.back();
        tail_sink sink{this, index};
        auto walked = co_await walk_recovery_tail(
          files_,
          ownership_,
          *state.device,
          wal_device_,
          shard_,
          tails_,
          tail,
          false,
          budget_,
          work_,
          sink);
        if (!walked) co_return runtime::failure(walked.error());
        if (!*walked) co_return false;
        const auto& outcome = **walked;
        tail.boundary = outcome.boundary;
        tail.blocks = outcome.blocks;
        tail.footers = outcome.footers;
        report.boundary = outcome.boundary;
        report.content_end = outcome.content_end;
        report.first_damage = outcome.first_damage;
        report.file_bytes = outcome.file_bytes;
        report.stop = outcome.stop;
        report.verdict = outcome.verdict;
        report.repaired = outcome.repaired;
        report.blocks = outcome.blocks;
        report.footers = outcome.footers;
        co_return co_await deliver(report);
    }

    Backend& files_;
    Owner& ownership_;
    std::span<const local_device_spec> devices_;
    const local_device_spec& wal_device_;
    std::uint32_t shard_;
    table_view<recovery_target> targets_;
    workload_budget& budget_;
    recovery_merge_limits limits_;
    codec::cooperative_work& work_;
    Visitor& visit_;
    std::uint64_t skip_;
    codec::content_digest binding_;
    recovery_tails& tails_;
    // The fixed tables, and what the waiting lists grew to.
    std::optional<workload_reservation> held_, grown_;
    seastar::chunked_vector<typename catalog_type::entry> entries_;
    std::unique_ptr<catalog_type> catalog_;
    seastar::chunked_vector<recovery_segment_state> segments_;
    seastar::chunked_vector<cursor> cursors_;
    // Targets in their segments' canonical order: see find().
    seastar::chunked_vector<std::uint32_t> order_index_;
    seastar::chunked_vector<recovery_obligation_row> rows_;
    std::uint64_t ordinal_{0}, delivered_{0}, order_{0}, clock_{0};
    std::uint32_t open_{0}, reloads_{0};
    bool stopped_{false};
};
} // namespace detail

// Reconciles every resolved PREPARE with its segment's surviving copy in one
// pass over the WAL. Formation submits one group at a time, so within a
// segment PREPARE targets strictly increase in WAL order: each segment's walk
// only moves forward, and a non-increasing target is a conflict. Each
// resolved PREPARE is queued for its segment, keeping its claims and its
// exact child's XXH3-128 identity; a segment's walk serves its queue in WAL
// order when the queue fills and once the WAL is scanned, so interleaved
// segments are each read forward in long runs rather than one slot at a time.
// A slot after the segment's last verified footer waits until a later footer
// covers it or the walk ends; a slot inside pinned evidence gets a layout
// check only. Sealed targets are not read. At most `limits.cursors` walks are
// open at once; the least recently advanced one closes and later reopens at
// its last verified footer.
//
// Stamps are evidence wherever an intact, self-identified object holds them:
// each segment's proven end is the most of its pin's end, the certified end
// of every intact block of it, read on after its first damage, and that of
// every PREPARE naming it, the head's intact PREPAREs after its content
// among them. The boundary restart publishes must reach it, or the segment
// is proven corrupt; damage to the head below its own proven end is too.
//
// A segment with PREPAREs after its recovered boundary, the last verified
// footer before its first damage, is walked again from there through those
// PREPAREs, each read again from the WAL where the scan found it, only to
// fill its slot: recovery_tail_walk, which writes nothing here. What it can
// fill and publish is its tail, kept in `tails` for the restart that writes
// it before publishing; a slot it settles below that boundary is
// materialized, and the rest are candidates and suffix.
//
// visit(const recovery_item&) returns future<result<bool>> and receives, in
// one deterministic order that no walk bound changes: unresolved PREPAREs and
// each WAL file's regions as the scan meets them, each segment's slots as its
// walk serves its queue, then in target order each segment's remaining slots,
// regions and report, then the obligation rows. false stops the merge; the
// result then carries a resume token, and a later call with it delivers only
// the items after it, provided the control, cutoff, targets and queue bound
// are unchanged. Devices list every device that may hold a target; the WAL
// and control live on `wal_device`. Every argument taken by reference or span
// is borrowed until the returned future resolves. Nothing is written.
template<
  runtime::file_system_backend Backend,
  local_directory_owner Owner,
  typename Visitor>
seastar::future<runtime::result<recovery_merge_result>>
reconcile_local_recovery(
  Backend& files,
  Owner& ownership,
  std::span<const local_device_spec> devices,
  const local_device_spec& wal_device,
  std::uint32_t shard,
  const local_shard_control& control,
  std::optional<local_wal_cursor> cutoff,
  table_view<recovery_target> targets,
  workload_budget& budget,
  recovery_merge_limits limits,
  codec::cooperative_work& work,
  Visitor visit,
  std::optional<recovery_resume> resume = std::nullopt,
  recovery_tails* tails = nullptr) {
    static_assert(sizeof(Visitor) <= 8_KiB);
    const auto binding = detail::recovery_binding(
      control, cutoff, targets, limits.pending);
    if (resume && resume->binding != binding)
        co_return runtime::failure(detail::path_error(errc::wrong_context));
    std::optional<recovery_tails> own;
    if (!tails) tails = &own.emplace();
    detail::recovery_merge<Backend, Owner, Visitor> merge{
      files,
      ownership,
      devices,
      wal_device,
      shard,
      targets,
      budget,
      limits,
      work,
      visit,
      resume ? resume->items : 0,
      binding,
      *tails};
    co_return co_await merge.run(control, cutoff);
}

} // namespace kwaque::storage
