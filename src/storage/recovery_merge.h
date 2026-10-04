#pragma once

#include "src/base/units.h"
#include "src/storage/page_internal.h"
#include "src/storage/recovery_cases.h"
#include "src/storage/segment_scan.h"
#include "src/storage/segment_writer_state.h"
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
    // The last footer whose covered history verified before the first damage,
    // or the pin itself. Absent when neither exists or the pin is damaged.
    std::optional<segment_scan_boundary> boundary;
    // False for a sealed target, whose extent is cold and not read here.
    bool scanned{false};
    runtime::file_position content_end;
    std::uint64_t file_bytes{0};
    segment_scan_stop stop{segment_scan_stop::end};
    segment_scan_verdict verdict{segment_scan_verdict::clean};
    std::uint32_t satisfied{0}, footer_only{0}, candidates{0}, suffix{0},
      conflicts{0};
    // Unresolved PREPAREs claiming this segment, misplaced ones among them.
    std::uint32_t unresolved{0}, misplaced{0};
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
    // (WAL file, segment) obligation rows. Like the pending slots of one
    // segment, the rows are a fixed table that must be one contiguous
    // allocation.
    std::uint32_t obligations{512};

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
    std::vector<recovery_queued> queued;
    std::vector<recovery_pending> pending;
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
};

[[nodiscard]] runtime::result<void>
  validate_recovery_targets(std::span<const recovery_target>) noexcept;
// The resume binding: the WAL head and checkpoint the control pins, the
// cutoff, every target's identity, state, generation and pin, and the queue
// bound, which decides where a full queue's slots fall in the enumeration.
[[nodiscard]] codec::content_digest recovery_binding(
  const local_shard_control&,
  std::optional<local_wal_cursor> cutoff,
  std::span<const recovery_target>,
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
      std::span<const recovery_target> targets,
      workload_budget& budget,
      recovery_merge_limits limits,
      codec::cooperative_work& work,
      Visitor& visit,
      std::uint64_t skip,
      codec::content_digest binding) noexcept
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
      , binding_(binding) {}

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

    runtime::result<void> prepare() {
        if (auto valid = limits_.validate(); !valid) return valid;
        if (auto valid = validate_recovery_targets(targets_); !valid)
            return valid;
        const auto count = targets_.size();
        byte_count total;
        const auto add = [&](std::size_t bytes) -> runtime::result<void> {
            if (bytes == 0) return {};
            auto charged = budget_.allocation_charge(byte_count{bytes});
            if (!charged) return runtime::failure(charged.error());
            auto next = total.checked_add(*charged);
            if (!next)
                return runtime::failure(detail::path_error(errc::out_of_range));
            total = *next;
            return {};
        };
        for (const auto bytes :
             {count * sizeof(typename catalog_type::entry),
              count * sizeof(recovery_segment_state),
              count * sizeof(cursor),
              std::size_t{limits_.obligations}
                * sizeof(recovery_obligation_row)})
            if (auto added = add(bytes); !added) return added;
        for (const auto& target : targets_)
            for (const auto bytes :
                 {std::size_t{limits_.pending} * sizeof(recovery_queued),
                  waiting_rows(target) * sizeof(recovery_pending)})
                if (auto added = add(bytes); !added) return added;
        auto held = budget_.try_reserve(total);
        if (!held) return runtime::failure(held.error());
        held_.emplace(std::move(*held));
        entries_.reserve(count);
        segments_.reserve(count);
        cursors_.resize(count);
        rows_.reserve(limits_.obligations);
        for (const auto& target : targets_) {
            entries_.push_back({target.descriptor, target.header});
            auto& state = segments_.emplace_back(target);
            state.queued.reserve(limits_.pending);
            state.pending.reserve(waiting_rows(target));
        }
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
        auto scanned = co_await scan_local_wal(
          files_,
          ownership_,
          wal_device_,
          shard_,
          control,
          cutoff,
          budget_,
          limits_.wal,
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
        for (std::size_t i = 0; i < segments_.size(); ++i)
            if (segments_[i].target->descriptor.segment == segment) return i;
        return std::nullopt;
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
          descriptor.logical_origin,
          descriptor.physical_origin,
          target.profile};
        state.processed = state.target->pin ? footer_end(*state.target->pin)
                                            : target.data_start;
        state.report.content_end = state.processed;
        return {};
    }

    // Records a PREPARE's slot against its WAL file's obligation row.
    runtime::result<void> oblige(
      const local_wal_cursor& begin,
      const segment_context& segment,
      std::uint64_t order,
      bool satisfied) {
        auto row = std::find_if(rows_.begin(), rows_.end(), [&](const auto& r) {
            return r.value.wal == begin.incarnation()
                   && r.value.segment == segment;
        });
        if (row == rows_.end()) {
            if (rows_.size() == limits_.obligations)
                return runtime::failure(
                  detail::path_error(errc::resource_exhausted));
            rows_.push_back({{begin.incarnation(), segment, 0, 0, {}}});
            row = rows_.end() - 1;
        }
        if (satisfied) {
            ++row->value.satisfied;
        } else {
            ++row->value.pinned;
            if (order < row->first) {
                row->first = order;
                row->value.first_pinned = begin;
            }
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
            const bool satisfied = matched.classification
                                   == recovery_classification::satisfied;
            if (
              auto obliged = oblige(
                *begin, state.target->descriptor.segment, order, satisfied);
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

    // Classifies every waiting slot now that its coverage is known.
    seastar::future<runtime::result<bool>>
    settle(std::size_t index, bool covered) {
        auto& state = segments_[index];
        for (const auto& waiting : state.pending) {
            auto proceed = co_await slot(
              index,
              waiting.position,
              waiting.begin,
              waiting.end,
              waiting.order,
              {waiting.prepare,
               waiting.block,
               false,
               covered,
               true,
               waiting.placed,
               waiting.difference});
            if (!proceed || !*proceed) co_return proceed;
        }
        state.pending.clear();
        co_return true;
    }

    // A walk holds the slots after its last verified footer until a later
    // footer covers them: one group, of at most this many blocks. A sealed
    // target is never walked.
    static std::size_t waiting_rows(const recovery_target& target) noexcept {
        return target.state == local_object_state::sealed
                 ? 0
                 : maximum_segment_group_blocks;
    }

    runtime::result<void>
    wait(std::size_t index, const recovery_pending& waiting) {
        auto& state = segments_[index];
        auto& pending = state.pending;
        if (pending.size() == waiting_rows(*state.target))
            return runtime::failure(
              detail::path_error(errc::resource_exhausted));
        pending.push_back(waiting);
        return {};
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

    // Opens the walk at its resume point, closing the least recently advanced
    // open walk first when the bound is reached.
    seastar::future<runtime::result<void>> load(std::size_t index) {
        auto& walk = cursors_[index];
        walk.used = ++clock_;
        if (walk.opened) co_return runtime::result<void>{};
        if (open_ == limits_.cursors) {
            std::optional<std::size_t> oldest;
            for (std::size_t i = 0; i < cursors_.size(); ++i)
                if (
                  cursors_[i].opened
                  && (!oldest || cursors_[i].used < cursors_[*oldest].used))
                    oldest = i;
            if (oldest)
                if (auto closed = co_await unload(*oldest); !closed)
                    co_return closed;
        }
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
    // everything before it. Once the walk ends, its waiting slots were never
    // covered.
    seastar::future<runtime::result<bool>>
    advance(std::size_t index, runtime::file_position position) {
        auto& walk = cursors_[index];
        for (;;) {
            auto more = co_await next_object(index);
            if (!more) co_return runtime::failure(more.error());
            if (!*more) co_return co_await settle(index, false);
            if (object_begin(*walk.current) >= position) co_return true;
            auto proceed = co_await process(index);
            if (!proceed || !*proceed) co_return proceed;
        }
    }

    seastar::future<runtime::result<bool>>
    on_prepare(const wal_scanned_prepare& value) {
        const auto order = order_++;
        if (value.resolution != wal_target_resolution::resolved) {
            const auto& claims = value.claims;
            if (auto index = find(claims.target)) {
                auto& report = segments_[*index].report;
                ++report.unresolved;
                if (value.resolution == wal_target_resolution::misplaced)
                    ++report.misplaced;
            }
            if (
              auto obliged = oblige(value.begin, claims.target, order, false);
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
        state.queued.push_back(std::move(queued));
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

    // compare_recovery_copies for a queued PREPARE: its claims and length,
    // then the block's child by the identity of its exact bytes.
    seastar::future<runtime::result<recovery_copy_difference>>
    compare(const recovery_copy_identity& prepare, const segment_block& block) {
        auto difference = compare_recovery_claims(prepare, block);
        if (!difference)
            co_return runtime::failure(
              detail::path_error(to_errc(difference.error())
                                   .value_or(errc::invariant_violation)));
        if (*difference != recovery_copy_difference::none)
            co_return *difference;
        auto child = block.child();
        if (!child)
            co_return runtime::failure(
              detail::path_error(
                to_errc(child.error()).value_or(errc::invariant_violation)));
        auto digest = co_await detail::hash_exact(*child, work_, {});
        if (!digest)
            co_return runtime::failure(
              detail::path_error(digest.error().code()));
        co_return *digest == prepare.child ? recovery_copy_difference::none
                                           : recovery_copy_difference::child;
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
            evidence.block = !evidence.covered
                             && std::any_of(
                               state.pending.begin(),
                               state.pending.end(),
                               [at](const auto& waiting) {
                                   return waiting.block
                                          && waiting.position == at;
                               });
            evidence.placed = evidence.block;
            co_return co_await slot(
              index, at, queued.begin, queued.end, queued.order, evidence);
        }
        auto moved = co_await advance(index, at);
        if (!moved || !*moved) co_return moved;
        auto& walk = cursors_[index];
        if (state.stopped) {
            // Beyond the content the slot is a candidate; inside it, the slot
            // starts within another object.
            evidence.covered = at < certified_end(state);
            evidence.placed = at >= state.ended.content_end;
            co_return co_await slot(
              index, at, queued.begin, queued.end, queued.order, evidence);
        }
        const auto& current = *walk.current;
        if (object_begin(current) == at && current.block) {
            if (!queued.copy)
                co_return runtime::failure(
                  detail::path_error(errc::invariant_violation));
            auto difference = co_await compare(*queued.copy, *current.block);
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
        co_return true;
    }

    seastar::future<runtime::result<void>> on_file(const wal_file_scan& value) {
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
            // A rotated file's damage lies inside its sealed prefix.
            const runtime::file_position end{
              rotated
                ? std::max(value.sealed_end->value(), value.content_end.value())
                : std::max(value.file_bytes, value.content_end.value())};
            regions[1] = region(value.content_end, end, false, true, rotated);
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

    // Serves the segment's queue, walks the rest of it, then reports its
    // regions and summary.
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
        report.boundary = ended.boundary;
        report.content_end = ended.content_end;
        report.file_bytes = ended.file_bytes;
        report.stop = ended.stop;
        report.verdict = ended.verdict;
        const auto region = [&](
                              runtime::file_position begin,
                              runtime::file_position end,
                              bool damage,
                              bool certified) {
            const auto parameters = region_parameters(false, damage, certified);
            return recovery_region_report{
              std::nullopt,
              report.segment,
              begin,
              end,
              parameters,
              &classify_recovery(parameters)};
        };
        const auto data_start = state.history->data_start;
        if (data_start < ended.content_end) {
            auto proceed = co_await deliver(
              region(data_start, ended.content_end, false, true));
            if (!proceed || !*proceed) co_return proceed;
        }
        if (ended.stop != segment_scan_stop::end) {
            const runtime::file_position end{
              std::max(ended.file_bytes, ended.content_end.value())};
            auto proceed = co_await deliver(region(
              ended.content_end,
              end,
              true,
              ended.verdict == segment_scan_verdict::corrupt));
            if (!proceed || !*proceed) co_return proceed;
        }
        co_return co_await deliver(report);
    }

    Backend& files_;
    Owner& ownership_;
    std::span<const local_device_spec> devices_;
    const local_device_spec& wal_device_;
    std::uint32_t shard_;
    std::span<const recovery_target> targets_;
    workload_budget& budget_;
    recovery_merge_limits limits_;
    codec::cooperative_work& work_;
    Visitor& visit_;
    std::uint64_t skip_;
    codec::content_digest binding_;
    std::optional<workload_reservation> held_;
    std::vector<typename catalog_type::entry> entries_;
    std::unique_ptr<catalog_type> catalog_;
    std::vector<recovery_segment_state> segments_;
    std::vector<cursor> cursors_;
    std::vector<recovery_obligation_row> rows_;
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
  std::span<const recovery_target> targets,
  workload_budget& budget,
  recovery_merge_limits limits,
  codec::cooperative_work& work,
  Visitor visit,
  std::optional<recovery_resume> resume = std::nullopt) {
    static_assert(sizeof(Visitor) <= 8_KiB);
    const auto binding = detail::recovery_binding(
      control, cutoff, targets, limits.pending);
    if (resume && resume->binding != binding)
        co_return runtime::failure(detail::path_error(errc::wrong_context));
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
      binding};
    co_return co_await merge.run(control, cutoff);
}

} // namespace kwaque::storage
