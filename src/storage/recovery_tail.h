#pragma once

#include "src/base/allocation.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer_builder.h"
#include "src/storage/recovery_cases.h"
#include "src/storage/segment_scan.h"
#include "src/storage/wal_scan.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/coroutine.hh>

#include <algorithm>
#include <array>
#include <cstdint>
#include <exception>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <utility>

namespace kwaque::storage {

namespace detail {
// Takes `bytes` more of `budget` into `held`.
[[nodiscard]] runtime::result<void> hold_more(
  std::optional<workload_reservation>& held,
  workload_budget& budget,
  byte_count bytes);

// A list kept in chunks of `Chunk` items, each charged to a budget the first
// time the list needs that many. Seastar's chunked_fifo keeps no freed chunk,
// so what it holds never exceeds what was charged.
template<typename T, std::size_t Chunk>
class recovery_list final {
public:
    [[nodiscard]] runtime::result<void> push(
      T value,
      workload_budget& budget,
      std::optional<workload_reservation>& held) {
        if (items_.size() / Chunk + 1 > charged_) {
            auto price = budget.allocation_charge(byte_count{chunk_bytes});
            if (!price) return runtime::failure(price.error());
            if (auto taken = hold_more(held, budget, *price); !taken)
                return taken;
            ++charged_;
        }
        try {
            items_.push_back(std::move(value));
        } catch (const std::bad_alloc&) {
            return runtime::failure(path_error(errc::resource_exhausted));
        }
        return {};
    }
    void clear() noexcept { items_.clear(); }
    [[nodiscard]] T& back() noexcept { return items_.back(); }
    [[nodiscard]] bool empty() const noexcept { return items_.empty(); }
    [[nodiscard]] std::size_t size() const noexcept { return items_.size(); }
    [[nodiscard]] auto begin() noexcept { return items_.begin(); }
    [[nodiscard]] auto end() noexcept { return items_.end(); }
    [[nodiscard]] auto begin() const noexcept { return items_.begin(); }
    [[nodiscard]] auto end() const noexcept { return items_.end(); }

private:
    // A chunk's items, its link and its bounds.
    static constexpr std::size_t chunk_bytes = Chunk * sizeof(T) + sizeof(void*)
                                               + 2 * sizeof(unsigned);
    // Within one charge under either charge model.
    static_assert(chunk_bytes <= maximum_contiguous_allocation_bytes / 2);

    seastar::chunked_fifo<T, Chunk> items_;
    std::size_t charged_{0};
};
inline constexpr std::size_t recovery_list_chunk = 64;
} // namespace detail

// A PREPARE whose slot lies after its segment's recovered boundary, kept by
// where it lies in the WAL, which the passes after the merge read again, and
// by what comparing it with a block needs.
struct recovery_tail_prepare final {
    local_wal_cursor begin, end;
    // Its place in WAL order.
    std::uint64_t order{0};
    recovery_copy_identity copy;

    [[nodiscard]] runtime::file_position position() const noexcept {
        return copy.target.position();
    }
};

// A WAL file such a PREPARE may lie in: what decoding it again needs.
struct recovery_tail_file final {
    model::wal_incarnation_id incarnation;
    storage_alignment alignment;
    replay_profile profile;
    runtime::file_position data_start;
};

// One segment with PREPAREs after its recovered boundary, whose tail restart
// fills from the WAL as far as it can and publishes.
struct recovery_tail final {
    // Its index among the merge's targets.
    std::size_t target{0};
    segment_history_context history;
    device_store_id device;
    // Where its walk begins: the recovered boundary, else the pin, else the
    // data start. And the furthest its objects may lie.
    std::optional<local_footer_reference> resume;
    runtime::file_position bound;
    // What the segment's file and the WAL prove certified.
    runtime::file_position proven_end;
    // In WAL order, which is file order within a segment.
    detail::recovery_list<recovery_tail_prepare, detail::recovery_list_chunk>
      prepares;
    // What the proving pass found: the boundary restart publishes, and the
    // blocks and footers it writes first.
    std::optional<segment_scan_boundary> boundary;
    std::uint32_t blocks{0}, footers{0};
};

// The tails one merge found, for the passes after it, and the WAL files their
// PREPAREs may lie in, charged to the merge's budget as they grow.
struct recovery_tails final {
    std::optional<workload_reservation> held;
    detail::recovery_list<recovery_tail, detail::recovery_list_chunk> segments;
    detail::recovery_list<recovery_tail_file, detail::recovery_list_chunk>
      files;
    // What the passes read with.
    wal_scan_limits wal{};
    segment_scan_limits segment{};

    [[nodiscard]] const recovery_tail_file*
    file(model::wal_incarnation_id incarnation) const noexcept {
        for (const auto& value : files)
            if (value.incarnation == incarnation) return &value;
        return nullptr;
    }
    [[nodiscard]] const recovery_tail* find(std::size_t target) const noexcept {
        for (const auto& value : segments)
            if (value.target == target) return &value;
        return nullptr;
    }
};

namespace detail {
// Failures that say a filled object cannot continue the verified layout, not
// that the walk could not run.
[[nodiscard]] bool recovered_layout_failure(errc) noexcept;

// compare_recovery_copies for a PREPARE whose copy is gone: its claims and
// length, then the block's child by the identity of its exact bytes.
[[nodiscard]] seastar::future<runtime::result<recovery_copy_difference>>
compare_recovery_identity(
  const recovery_copy_identity& prepare,
  const segment_block& block,
  codec::cooperative_work& work);

// Reads tail PREPAREs again where the merge found them, each decoded under
// its segment's placement: one file open at a time, and one read of exactly
// one PREPARE's bytes each.
template<runtime::file_system_backend Backend, local_directory_owner Owner>
class recovery_tail_reader final {
public:
    recovery_tail_reader(
      Backend& files,
      Owner& ownership,
      const local_device_spec& device,
      std::uint32_t shard,
      const recovery_tails& tails,
      workload_budget& budget,
      codec::cooperative_work& work) noexcept
      : files_(files)
      , ownership_(ownership)
      , device_(device)
      , shard_(shard)
      , tails_(tails)
      , budget_(budget)
      , work_(work) {}
    recovery_tail_reader(const recovery_tail_reader&) = delete;
    recovery_tail_reader& operator=(const recovery_tail_reader&) = delete;

    // The PREPARE `entry` names, under `target`'s placement. Anything else
    // there is an error: the merge found it there intact.
    seastar::future<runtime::result<wal_prepare>>
    operator()(const recovery_tail_prepare& entry, const wal_target& target) {
        const auto& limits = tails_.wal;
        const auto incarnation = entry.begin.incarnation();
        const auto* link = tails_.file(incarnation);
        if (
          !link || entry.end.incarnation() != incarnation
          || entry.end.position() <= entry.begin.position())
            co_return runtime::failure(path_error(errc::invariant_violation));
        if (!working_) {
            auto working = budget_.try_reserve(
              byte_count{
                limits.working_bytes.value()
                + limits.decode_metadata_bytes.value()});
            if (!working) co_return runtime::failure(working.error());
            working_.emplace(std::move(*working));
        }
        if (!file_ || open_ != incarnation) {
            if (auto closed = co_await close(); !closed)
                co_return runtime::failure(closed.error());
            const auto paths = local_paths::make(device_.root);
            if (!paths) co_return runtime::failure(paths.error());
            auto path = paths->wal(shard_, incarnation);
            if (!path) co_return runtime::failure(path.error());
            auto handle = budget_.try_reserve(limits.metadata.execution_bytes);
            if (!handle) co_return runtime::failure(handle.error());
            if (auto held = handle->try_acquire_handles(1); !held)
                co_return runtime::failure(held.error());
            handle_.emplace(std::move(*handle));
            if (auto valid = co_await ownership_.validate(device_); !valid)
                co_return runtime::failure(valid.error());
            if (
              auto inspected = co_await inspect_local_path(
                files_,
                device_.root,
                *path,
                runtime::file_kind::regular,
                work_);
              !inspected)
                co_return runtime::failure(inspected.error());
            auto opened = co_await files_.open(*path, {});
            if (!opened) co_return runtime::failure(opened.error());
            file_.emplace(std::move(*opened));
            open_ = incarnation;
        }
        const byte_count length{
          entry.end.position().value() - entry.begin.position().value()};
        read_.reset();
        // Read in the scan's windows, each ending at the next multiple of
        // the window size: no fragment is larger than the decode admits as
        // one allocation.
        bytes::fragmented_buffer_builder joined{
          {.max_total_bytes = runtime::maximum_file_io_bytes}};
        const auto window = limits.reader.window_bytes.value();
        for (auto at = entry.begin.position().value();
             at != entry.end.position().value();) {
            const auto piece = std::min(
              window - at % window, entry.end.position().value() - at);
            auto read = co_await file_->read(
              runtime::file_position{at}, byte_count{piece});
            if (!read) co_return runtime::failure(read.error());
            auto part = std::move(*read).take_data();
            if (part.size().value() != piece)
                co_return runtime::failure(path_error(errc::truncated_data));
            if (!joined.append_buffer(std::move(part)))
                co_return runtime::failure(
                  path_error(errc::resource_exhausted));
            at += piece;
        }
        auto data = joined.finish();
        if (!data)
            co_return runtime::failure(path_error(errc::resource_exhausted));
        auto held = budget_.try_reserve(data->retained_bytes());
        if (!held) co_return runtime::failure(held.error());
        read_.emplace(std::move(*held));
        bytes::fragmented_buffer_parser input{std::move(*data)};
        auto memory = codec::reserve_decode_input(
          input,
          work_.policy(),
          {byte_count{length.value() + limits.working_bytes.value()},
           limits.decode_metadata_bytes,
           limits.metadata.charge},
          {},
          codec::input_boundary::complete);
        if (!memory)
            co_return runtime::failure(path_error(memory.error().code()));
        wal_resolution state{
          link->incarnation,
          link->alignment,
          link->profile,
          link->data_start,
          entry.begin.position()};
        auto known = [&target](const segment_context& claimed) {
            return seastar::make_ready_future<
              runtime::result<std::optional<wal_target>>>(
              claimed == target.segment ? std::optional{target}
                                        : std::optional<wal_target>{});
        };
        wal_prepare_resolver resolver{
          [&state, &known](const wal_prepare_claims& claims) {
              return resolve_wal_prepare(state, known, claims);
          }};
        auto decoded = co_await decode_wal_prepare_resolved(
          input,
          resolver,
          *memory,
          work_,
          {.origin = entry.begin.position().value()},
          codec::input_boundary::complete);
        if (state.failure) co_return runtime::failure(*state.failure);
        if (!decoded)
            co_return runtime::failure(path_error(decoded.error().code()));
        if (decoded->value.target() != entry.copy.target)
            co_return runtime::failure(path_error(errc::corrupt_data));
        co_return std::move(decoded->value);
    }

    seastar::future<runtime::result<void>> close() {
        runtime::first_failure failed;
        read_.reset();
        if (file_) {
            try {
                failed.observe(co_await file_->close());
            } catch (...) {
                failed.observe(std::current_exception());
            }
            file_.reset();
        }
        handle_.reset();
        co_return failed.outcome();
    }

private:
    Backend& files_;
    Owner& ownership_;
    const local_device_spec& device_;
    std::uint32_t shard_;
    const recovery_tails& tails_;
    workload_budget& budget_;
    codec::cooperative_work& work_;
    std::optional<workload_reservation> working_, handle_, read_;
    std::optional<runtime::file> file_;
    model::wal_incarnation_id open_{};
};

// What one tail walk found, and wrote when it was given to write.
struct recovery_tail_outcome final {
    // The boundary restart publishes: the last footer the walk verified,
    // found intact or derived. Absent when there is none.
    std::optional<segment_scan_boundary> boundary;
    runtime::file_position content_end;
    std::uint64_t file_bytes{0};
    segment_scan_stop stop{segment_scan_stop::end};
    segment_scan_verdict verdict{segment_scan_verdict::clean};
    // Where the walk first met bytes it could not read.
    std::optional<runtime::file_position> first_damage;
    // Blocks made from PREPAREs and footers derived from the prefix.
    std::uint32_t blocks{0}, footers{0};
    // Slots filled below what the segment proves certified.
    std::uint32_t repaired{0};
};

// One segment's walk from its recovered boundary through its tail PREPAREs,
// in WAL order, which is file order within a segment.
//
// Each slot holds its verified intact object, or, where the walk finds
// damage, zeros or the file's end, the block made from the PREPARE that names
// it. Where the walk stops after a block and the next PREPARE's slot lies
// further on, the run that block ends lost its closing footer, which the
// verified prefix encodes: it is derived there when it ends at or before
// that slot, and the walk goes on after it. After the last PREPARE the walk
// reads on through what survives, and a run it then stops after is closed
// the same way. A footer is derived only over blocks made from or found
// identical to PREPAREs since the last footer, never over a block no PREPARE
// names: that is a segment-placed group's, whose own footer only the writer
// wrote, and a PREPARE's block never directly follows one. A filled block
// must continue the verified prefix with no record missing. Nothing is
// filled over an intact object of the segment.
//
// The slots a footer covers, found intact or derived, are below the boundary
// restart publishes; a block found intact under a footer found intact is
// covered as before the first damage. The first slot the walk cannot fill
// ends what can be published: every PREPARE after it is a candidate and
// every block after it suffix. A slot that differs from, or lies across,
// what its PREPARE names is a conflict, after which nothing is filled either.
// Damage the walk fills, and the damage it ends at, are regions, certified
// below what the segment proves certified; certified bytes the boundary does
// not reach are proven corruption.
//
// With `writes`, each filled object is written in place once the walk has
// verified it, in writes of whole units that copy the bytes around it as
// they survive: they are uncertified, so they are rewritten unchanged. The
// file is not flushed here.
template<runtime::file_system_backend Backend, local_directory_owner Owner>
class recovery_tail_walk final {
public:
    struct slot_event final {
        runtime::file_position position;
        std::optional<local_wal_cursor> begin, end;
        std::uint64_t order{0};
        recovery_slot_evidence evidence;
    };
    struct region_event final {
        runtime::file_position begin, end;
        bool certified{false}, filled{false};
    };

    recovery_tail_walk(
      Backend& files,
      Owner& ownership,
      const local_device_spec& data,
      std::uint32_t shard,
      const recovery_tail& tail,
      bool writes,
      workload_budget& budget,
      segment_scan_limits limits,
      codec::cooperative_work& work) noexcept
      : files_(files)
      , ownership_(ownership)
      , data_(data)
      , shard_(shard)
      , tail_(tail)
      , writes_(writes)
      , budget_(budget)
      , limits_(limits)
      , work_(work) {}
    recovery_tail_walk(const recovery_tail_walk&) = delete;
    recovery_tail_walk& operator=(const recovery_tail_walk&) = delete;

    seastar::future<runtime::result<void>> open() {
        auto handle = budget_.try_reserve(limits_.metadata.execution_bytes);
        if (!handle) co_return runtime::failure(handle.error());
        handle_.emplace(std::move(*handle));
        auto opened = co_await open_local_segment_data(
          files_,
          ownership_,
          data_,
          shard_,
          tail_.history,
          *handle_,
          work_,
          writes_ ? runtime::file_access::read_write
                  : runtime::file_access::read_only);
        if (!opened) co_return runtime::failure(opened.error());
        file_.emplace(std::move(*opened));
        auto made = co_await segment_scanner::make(
          *file_,
          budget_,
          tail_.history,
          tail_.resume,
          limits_,
          work_,
          tail_.bound);
        if (!made) co_return runtime::failure(made.error());
        scanner_ = std::move(*made);
        // The merge verified this footer: anything else there now means the
        // bytes changed under recovery.
        if (scanner_->report().verdict == segment_scan_verdict::corrupt)
            co_return runtime::failure(path_error(errc::corrupt_data));
        outcome_.boundary = scanner_->report().boundary;
        co_return runtime::result<void>{};
    }

    // One tail PREPARE, in WAL order. fetch(entry) returns
    // future<result<wal_prepare>>, the PREPARE itself, and is called only to
    // fill its slot.
    template<typename Fetch, typename Sink>
    seastar::future<runtime::result<bool>>
    visit(const recovery_tail_prepare& entry, Fetch& fetch, Sink& sink) {
        const auto at = entry.position();
        if (last_ && at <= *last_)
            co_return runtime::failure(path_error(errc::invariant_violation));
        last_ = at;
        const auto settled = [&](bool placed) {
            recovery_slot_evidence evidence;
            evidence.prepare = true;
            evidence.ordered = true;
            evidence.placed = placed;
            return sink.slot(
              slot_event{at, entry.begin, entry.end, entry.order, evidence});
        };
        for (;;) {
            // Everything before the slot.
            for (;;) {
                auto read = co_await read_object(sink);
                if (!read || !*read) co_return read;
                if (!current_ || object_begin(*current_) >= at) break;
                auto taken = co_await take(sink);
                if (!taken || !*taken) co_return taken;
            }
            if (current_) {
                if (object_begin(*current_) != at || !current_->block) {
                    // The slot lies inside the object before it, or a
                    // footer holds it.
                    filling_ = false;
                    co_return co_await settled(false);
                }
                auto difference = co_await compare_recovery_identity(
                  entry.copy, *current_->block, work_);
                if (!difference) co_return runtime::failure(difference.error());
                current_.reset();
                last_block_ = true;
                recovery_slot_evidence evidence;
                evidence.prepare = true;
                evidence.block = true;
                evidence.ordered = true;
                evidence.placed = true;
                evidence.difference = *difference;
                const slot_event slot{
                  at, entry.begin, entry.end, entry.order, evidence};
                if (*difference != recovery_copy_difference::none)
                    filling_ = false;
                if (!filling_) co_return co_await sink.slot(slot);
                co_return wait(slot);
            }
            // The walk stopped before the slot or at it.
            const auto& report = scanner_->report();
            const auto x = report.content_end;
            if (x > at) {
                filling_ = false;
                co_return co_await settled(false);
            }
            if (!filling_ || rejected(report)) {
                filling_ = false;
                co_return co_await settled(true);
            }
            if (x < at) {
                auto filled = co_await fill_footer(x, at);
                if (!filled) co_return runtime::failure(filled.error());
                if (!*filled) {
                    filling_ = false;
                    co_return co_await settled(true);
                }
                auto proceed = co_await commit(sink, false);
                if (!proceed || !*proceed) co_return proceed;
                // Read what lies at the slot now.
                continue;
            }
            if (!sourced_) {
                filling_ = false;
                co_return co_await settled(true);
            }
            auto prepare = co_await fetch(entry);
            if (!prepare) co_return runtime::failure(prepare.error());
            auto filled = co_await fill_block(std::move(*prepare));
            if (!filled) co_return runtime::failure(filled.error());
            if (!*filled) {
                filling_ = false;
                co_return co_await settled(true);
            }
            if (at < tail_.proven_end) ++outcome_.repaired;
            recovery_slot_evidence evidence;
            evidence.prepare = true;
            evidence.ordered = true;
            evidence.placed = true;
            co_return wait(
              slot_event{at, entry.begin, entry.end, entry.order, evidence});
        }
    }

    // After the last PREPARE: reads on through what survives, closes a run
    // of blocks with the footer that ends it where the walk stops, and
    // settles every slot still waiting and the regions. Flushes the last
    // write.
    template<typename Sink>
    seastar::future<runtime::result<bool>> finish(Sink& sink) {
        for (;;) {
            for (;;) {
                auto read = co_await read_object(sink);
                if (!read || !*read) co_return read;
                if (!current_) break;
                auto taken = co_await take(sink);
                if (!taken || !*taken) co_return taken;
            }
            if (!filling_ || !last_block_ || rejected(scanner_->report()))
                break;
            // The run the last PREPAREs filled lost its closing footer.
            auto filled = co_await fill_footer(
              scanner_->report().content_end, tail_.bound);
            if (!filled) co_return runtime::failure(filled.error());
            if (!*filled) {
                filling_ = false;
                break;
            }
            auto proceed = co_await commit(sink, false);
            if (!proceed || !*proceed) co_return proceed;
        }
        if (auto flushed = co_await flush(); !flushed)
            co_return runtime::failure(flushed.error());
        // What no footer covers was never certified.
        for (const auto& waiting : pending_) {
            auto proceed = co_await sink.slot(waiting);
            if (!proceed || !*proceed) co_return proceed;
        }
        pending_.clear();
        const auto& report = scanner_->report();
        outcome_.content_end = report.content_end;
        outcome_.file_bytes = report.file_bytes;
        outcome_.stop = report.stop;
        outcome_.verdict = report.verdict;
        const auto published = outcome_.boundary
                                 ? footer_end(outcome_.boundary->footer)
                                 : tail_.history.data_start;
        if (published < tail_.proven_end)
            outcome_.verdict = segment_scan_verdict::corrupt;
        if (region_ && *region_ < report.content_end) {
            auto proceed = co_await sink.region(
              region_event{
                *region_,
                report.content_end,
                *region_ < tail_.proven_end,
                true});
            if (!proceed || !*proceed) co_return proceed;
        }
        region_.reset();
        if (report.stop != segment_scan_stop::end) {
            const runtime::file_position end{
              std::max(report.file_bytes, report.content_end.value())};
            auto proceed = co_await sink.region(
              region_event{
                report.content_end,
                end,
                report.content_end < tail_.proven_end,
                false});
            if (!proceed || !*proceed) co_return proceed;
        }
        co_return true;
    }

    seastar::future<runtime::result<void>> close() {
        runtime::first_failure failed;
        // A write not flushed belongs to a pass that did not finish.
        pending_write_.reset();
        current_.reset();
        if (scanner_) {
            co_await scanner_->close();
            scanner_.reset();
        }
        if (file_) {
            try {
                failed.observe(co_await file_->close());
            } catch (...) {
                failed.observe(std::current_exception());
            }
            file_.reset();
        }
        handle_.reset();
        co_return failed.outcome();
    }

    [[nodiscard]] const recovery_tail_outcome& outcome() const noexcept {
        return outcome_;
    }

private:
    static runtime::file_position
    footer_end(const local_footer_reference& footer) noexcept {
        return runtime::file_position{
          footer.position().value() + footer.bytes().value()};
    }
    static runtime::file_position
    object_begin(const segment_scanned_object& object) noexcept {
        return object.block
                 ? object.block->descriptor().coverage().bytes().begin()
                 : object.footer->footer.position();
    }
    // The walk stopped at bytes this segment wrote, not at damage: an intact
    // object of this segment that the prefix rejects. Another segment's
    // intact object is a recycled file's earlier bytes, filled like zeros.
    static bool rejected(const segment_scan_report& report) noexcept {
        return report.intact && !report.stale;
    }

    // Makes the next object current, unless one is or the walk stopped. An
    // intact object after filled damage ends that region.
    template<typename Sink>
    seastar::future<runtime::result<bool>> read_object(Sink& sink) {
        if (current_ || stopped_) co_return true;
        auto next = co_await scanner_->next(work_);
        if (!next) co_return runtime::failure(next.error());
        if (!*next) {
            stopped_ = true;
            const auto at = scanner_->report().content_end;
            if (!outcome_.first_damage) outcome_.first_damage = at;
            if (!region_) region_ = at;
            co_return true;
        }
        current_ = **next;
        if (!region_) co_return true;
        const auto begin = *region_;
        region_.reset();
        co_return co_await sink.region(
          region_event{
            begin, object_begin(*current_), begin < tail_.proven_end, true});
    }

    // Consumes the current object, which lies before the next slot: a block
    // no PREPARE names waits for a footer the writer wrote; a footer covers
    // every slot waiting. Once nothing more can be published, blocks are
    // suffix and footers cover nothing.
    template<typename Sink>
    seastar::future<runtime::result<bool>> take(Sink& sink) {
        const auto object = *current_;
        current_.reset();
        if (object.block) {
            last_block_ = true;
            sourced_ = false;
            recovery_slot_evidence evidence;
            evidence.block = true;
            const slot_event slot{object_begin(object), {}, {}, 0, evidence};
            if (!filling_) co_return co_await sink.slot(slot);
            co_return wait(slot);
        }
        last_block_ = false;
        if (!filling_) co_return true;
        co_return co_await commit(sink, true);
    }

    runtime::result<bool> wait(const slot_event& slot) {
        if (auto pushed = pending_.push(slot, budget_, pending_held_); !pushed)
            return runtime::failure(pushed.error());
        return true;
    }

    // The footer just verified covers every slot waiting: each lies below
    // the boundary restart publishes. One found intact covers the blocks
    // found intact under it as before the first damage.
    template<typename Sink>
    seastar::future<runtime::result<bool>> commit(Sink& sink, bool intact) {
        outcome_.boundary = scanner_->report().boundary;
        sourced_ = true;
        for (auto waiting : pending_) {
            waiting.evidence.covered = intact && waiting.evidence.block;
            waiting.evidence.contiguous = true;
            auto proceed = co_await sink.slot(waiting);
            if (!proceed || !*proceed) co_return proceed;
        }
        pending_.clear();
        co_return true;
    }

    // The block `prepare` names, at the slot where the walk stopped. False
    // when it does not continue the verified prefix there.
    seastar::future<runtime::result<bool>> fill_block(wal_prepare prepare) {
        const auto location = prepare.target();
        // The block the writer made from this PREPARE's group carried these.
        const auto stamps = prepare.stamps();
        auto child = std::move(prepare).release_batch();
        const auto info = child.info();
        const auto original = info.context.submitted();
        const auto cost = child.bytes().allocation_cost(
          limits_.metadata.charge);
        if (!cost) co_return runtime::failure(path_error(errc::out_of_range));
        const auto& history = tail_.history;
        auto encoded = co_await encode_segment_block(
          std::move(child),
          {location,
           history.data_start,
           {history.segment.topic(),
            history.segment.range(),
            original.id(),
            original.binding(),
            info.fingerprint},
           history.profile},
          {stamps.append_time, stamps.target_certified_end},
          work_,
          byte_count{
            cost->backing.value() + cost->descriptors.value()
            + cost->share_controls.value() + limits_.working_bytes.value()},
          limits_.metadata.charge);
        if (!encoded) {
            if (recovered_layout_failure(encoded.error().code()))
                co_return false;
            co_return runtime::failure(path_error(encoded.error().code()));
        }
        if (encoded->descriptor().coverage().bytes().end() > tail_.bound)
            co_return false;
        auto placed = co_await scanner_->substitute(std::move(*encoded), work_);
        if (!placed) {
            if (recovered_layout_failure(placed.error().code()))
                co_return false;
            co_return runtime::failure(placed.error());
        }
        auto written = co_await place(
          location.position(), std::move(*placed).release_bytes());
        if (!written) co_return runtime::failure(written.error());
        ++outcome_.blocks;
        last_block_ = true;
        stopped_ = false;
        co_return true;
    }

    // The footer the verified prefix encodes at `at`, closing blocks that
    // all came from PREPAREs since the last footer, when it ends at or before
    // `limit`. False when it cannot be placed there.
    seastar::future<runtime::result<bool>>
    fill_footer(runtime::file_position at, runtime::file_position limit) {
        if (!last_block_ || !sourced_) co_return false;
        auto prefix = scanner_->prefix(work_);
        if (!prefix) co_return runtime::failure(prefix.error());
        auto encoded = co_await encode_durable_footer(
          *prefix,
          {tail_.history, at},
          work_,
          limits_.working_bytes,
          limits_.metadata.charge);
        if (!encoded) {
            if (recovered_layout_failure(encoded.error().code()))
                co_return false;
            co_return runtime::failure(path_error(encoded.error().code()));
        }
        const runtime::file_position end{
          at.value() + encoded->bytes().size().value()};
        if (end > limit) co_return false;
        auto placed = co_await scanner_->substitute(std::move(*encoded), work_);
        if (!placed) co_return runtime::failure(placed.error());
        auto written = co_await place(at, std::move(*placed).release_bytes());
        if (!written) co_return runtime::failure(written.error());
        ++outcome_.footers;
        last_block_ = false;
        stopped_ = false;
        co_return true;
    }

    // Adds a filled object at `at` to the write being assembled, after the
    // surviving bytes between it and the last object placed. A write ends
    // once an object ends on a boundary or the next lies past its last unit.
    seastar::future<runtime::result<void>>
    place(runtime::file_position at, bytes::fragmented_buffer object) {
        if (!writes_) co_return runtime::result<void>{};
        const auto unit = tail_.history.alignment.bytes().value();
        if (pending_write_ && round_up(pending_write_->end) <= at.value()) {
            auto flushed = co_await flush();
            if (!flushed) co_return flushed;
        }
        if (!pending_write_)
            pending_write_.emplace(
              runtime::file_position{at.value() - at.value() % unit});
        auto copied = co_await copy(at);
        if (!copied) co_return copied;
        const auto size = object.size().value();
        if (auto taken = append(std::move(object)); !taken) co_return taken;
        pending_write_->end = runtime::file_position{at.value() + size};
        if (pending_write_->end.value() % unit == 0) co_return co_await flush();
        co_return runtime::result<void>{};
    }
    // Writes the assembled write, through the boundary after its last
    // object.
    seastar::future<runtime::result<void>> flush() {
        if (!pending_write_) co_return runtime::result<void>{};
        auto copied = co_await copy(
          runtime::file_position{round_up(pending_write_->end)});
        if (!copied) co_return copied;
        auto write = std::move(*pending_write_);
        pending_write_.reset();
        auto bytes = write.bytes.finish();
        if (!bytes)
            co_return runtime::failure(path_error(errc::resource_exhausted));
        const auto length = bytes->size();
        auto written = co_await file_->write(write.begin, std::move(*bytes));
        if (!written) co_return runtime::failure(written.error());
        if (*written != length)
            co_return runtime::failure(path_error(errc::io_failure));
        co_return runtime::result<void>{};
    }
    // Appends the file's bytes from the end of the write being assembled to
    // `to`, as far as the file has them, then zeros: what survives of the
    // objects there.
    seastar::future<runtime::result<void>> copy(runtime::file_position to) {
        const auto from = pending_write_->end;
        if (from >= to) co_return runtime::result<void>{};
        auto size = co_await file_->size();
        if (!size) co_return runtime::failure(size.error());
        const auto have = std::clamp(*size, from.value(), to.value());
        if (have > from.value()) {
            auto read = co_await file_->read(
              from, byte_count{have - from.value()});
            if (!read) co_return runtime::failure(read.error());
            auto data = std::move(*read).take_data();
            if (data.size().value() != have - from.value())
                co_return runtime::failure(path_error(errc::truncated_data));
            if (auto taken = append(std::move(data)); !taken) co_return taken;
        }
        if (to.value() > have) {
            static constexpr std::array<char, 4_KiB> zeros{};
            auto held = budget_.try_reserve(byte_count{to.value() - have});
            if (!held) co_return runtime::failure(held.error());
            if (auto adopted = pending_write_->hold(std::move(*held)); !adopted)
                co_return runtime::failure(adopted.error());
            for (auto left = to.value() - have; left != 0;) {
                const auto take = std::min<std::uint64_t>(left, zeros.size());
                if (
                  auto appended = pending_write_->bytes.append(
                    std::span<const char>{zeros.data(), take});
                  !appended)
                    co_return runtime::failure(
                      path_error(errc::resource_exhausted));
                left -= take;
            }
        }
        pending_write_->end = to;
        co_return runtime::result<void>{};
    }
    runtime::result<void> append(bytes::fragmented_buffer data) {
        auto held = budget_.try_reserve(data.retained_bytes());
        if (!held) return runtime::failure(held.error());
        if (auto adopted = pending_write_->hold(std::move(*held)); !adopted)
            return runtime::failure(adopted.error());
        if (
          auto appended = pending_write_->bytes.append_buffer(std::move(data));
          !appended)
            return runtime::failure(path_error(errc::resource_exhausted));
        return {};
    }
    [[nodiscard]] std::uint64_t
    round_up(runtime::file_position at) const noexcept {
        const auto unit = tail_.history.alignment.bytes().value();
        return (at.value() + unit - 1) / unit * unit;
    }

    // A write being assembled from `begin`, a boundary, to `end`, and what
    // it holds of the budget.
    struct pending_write final {
        explicit pending_write(runtime::file_position at) noexcept
          : begin(at)
          , end(at) {}
        runtime::file_position begin, end;
        bytes::fragmented_buffer_builder bytes{
          {.max_total_bytes = runtime::maximum_file_io_bytes}};
        std::optional<workload_reservation> held;
        runtime::result<void> hold(workload_reservation more) {
            if (!held) {
                held.emplace(std::move(more));
                return {};
            }
            return held->adopt(std::move(more));
        }
    };

    Backend& files_;
    Owner& ownership_;
    const local_device_spec& data_;
    std::uint32_t shard_;
    const recovery_tail& tail_;
    bool writes_;
    workload_budget& budget_;
    segment_scan_limits limits_;
    codec::cooperative_work& work_;
    std::optional<workload_reservation> handle_;
    std::optional<runtime::file> file_;
    std::unique_ptr<segment_scanner> scanner_;
    // Borrowed from the scanner until its next call.
    std::optional<segment_scanned_object> current_;
    std::optional<pending_write> pending_write_;
    // Slots a footer may still cover, in file order.
    recovery_list<slot_event, recovery_list_chunk> pending_;
    std::optional<workload_reservation> pending_held_;
    // Where the damage the walk is filling began.
    std::optional<runtime::file_position> region_;
    std::optional<runtime::file_position> last_;
    recovery_tail_outcome outcome_;
    // stopped: the scanner met damage or the file's end. filling: nothing
    // has yet ended what can be published. last_block: the prefix ends with
    // a block. sourced: every block since the last footer came from, or was
    // found identical to, a PREPARE.
    bool stopped_{false}, filling_{true}, last_block_{false}, sourced_{true};
};

// Runs one tail's walk. sink.slot(const slot_event&) and
// sink.region(const region_event&) return future<result<bool>>; false stops
// the walk, and the result is then empty. The PREPAREs are read from the WAL
// on `wal`, each only to fill its slot.
template<
  runtime::file_system_backend Backend,
  local_directory_owner Owner,
  typename Sink>
seastar::future<runtime::result<std::optional<recovery_tail_outcome>>>
walk_recovery_tail(
  Backend& files,
  Owner& ownership,
  const local_device_spec& data,
  const local_device_spec& wal,
  std::uint32_t shard,
  const recovery_tails& tails,
  const recovery_tail& tail,
  bool writes,
  workload_budget& budget,
  codec::cooperative_work& work,
  Sink& sink) {
    const wal_target target{
      tail.history.segment,
      tail.history.alignment,
      tail.history.data_start,
      tail.history.profile,
      tail.device};
    recovery_tail_reader<Backend, Owner> reader{
      files, ownership, wal, shard, tails, budget, work};
    auto fetch = [&reader, &target](const recovery_tail_prepare& entry) {
        return reader(entry, target);
    };
    recovery_tail_walk<Backend, Owner> walk{
      files, ownership, data, shard, tail, writes, budget, tails.segment, work};
    runtime::first_failure failed;
    bool complete = false;
    try {
        do {
            if (auto opened = co_await walk.open(); !opened) {
                failed.observe(opened);
                break;
            }
            bool proceed = true;
            for (const auto& entry : tail.prepares) {
                auto visited = co_await walk.visit(entry, fetch, sink);
                if (!visited) {
                    failed.observe(visited);
                    break;
                }
                if (!*visited) {
                    proceed = false;
                    break;
                }
            }
            if (failed.failed() || !proceed) break;
            auto finished = co_await walk.finish(sink);
            if (!finished) {
                failed.observe(finished);
                break;
            }
            complete = *finished;
        } while (false);
    } catch (...) {
        failed.observe(std::current_exception());
    }
    failed.observe(co_await walk.close());
    failed.observe(co_await reader.close());
    if (failed.failed()) {
        auto error = failed.outcome();
        co_return runtime::failure(error.error());
    }
    if (!complete) co_return std::optional<recovery_tail_outcome>{};
    co_return std::optional{walk.outcome()};
}

// Fills one tail as its proving pass found it can be filled, writing each
// object once it is verified, and must reach the same boundary, or the bytes
// changed under recovery. An earlier attempt's writes are found intact and
// adopted, not written again. The caller flushes the file before anything is
// published on it.
template<runtime::file_system_backend Backend, local_directory_owner Owner>
seastar::future<runtime::result<void>> materialize_recovery_tail(
  Backend& files,
  Owner& ownership,
  const local_device_spec& data,
  const local_device_spec& wal,
  std::uint32_t shard,
  const recovery_tails& tails,
  const recovery_tail& tail,
  workload_budget& budget,
  codec::cooperative_work& work) {
    using walk_type = recovery_tail_walk<Backend, Owner>;
    struct ignore final {
        seastar::future<runtime::result<bool>>
        slot(const typename walk_type::slot_event&) const {
            return seastar::make_ready_future<runtime::result<bool>>(true);
        }
        seastar::future<runtime::result<bool>>
        region(const typename walk_type::region_event&) const {
            return seastar::make_ready_future<runtime::result<bool>>(true);
        }
    } sink;
    auto walked = co_await walk_recovery_tail(
      files,
      ownership,
      data,
      wal,
      shard,
      tails,
      tail,
      true,
      budget,
      work,
      sink);
    if (!walked) co_return runtime::failure(walked.error());
    const auto& reached = (*walked)->boundary;
    if (
      reached.has_value() != tail.boundary.has_value()
      || (reached && reached->footer != tail.boundary->footer))
        co_return runtime::failure(path_error(errc::wrong_context));
    co_return runtime::result<void>{};
}
} // namespace detail

} // namespace kwaque::storage
