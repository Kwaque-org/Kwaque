#include "src/storage/segment_reader.h"

#include "src/base/allocation.h"
#include "src/base/invariant.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/util/defer.hh>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <optional>
#include <utility>

namespace kwaque::storage {
namespace {
runtime::operation_error read_error(errc code) noexcept {
    return runtime::operation_error{code, runtime::operation_kind::file};
}
// Whether a run could not be had as the cache's own: there was no room for
// it, or it is not one the cache keeps. Its reader then reads under its own
// budget.
bool unkept(errc code) noexcept {
    return code == errc::queue_full || code == errc::out_of_range;
}
// Whether a request was turned away for want of room or of a turn: it says
// nothing of the bytes, and may be granted the next time.
bool refused(errc code) noexcept {
    return code == errc::queue_full || code == errc::resource_exhausted;
}
// What a batch adds to a read's table at most: its place in it, and the
// bookkeeping of its share of the run's bytes.
std::uint64_t table_cost(const segment_run_block& block) noexcept {
    const auto fragments = 1U
                           + block.batch_bytes
                               / maximum_contiguous_allocation_bytes;
    return sizeof(segment_read_batch)
           + fragments * 2U
               * bytes::fragmented_buffer::fragment_descriptor_size();
}
// What an index its writer feeds answers, from the anchors that are durable
// now.
sparse_index_answer fed_answer(
  const active_sparse_index& index,
  runtime::file_position bound,
  model::range_logical_offset target) noexcept {
    const auto found = index.find(target, bound);
    std::optional<model::range_logical_offset> next;
    if (found)
        if (const auto after = index.after(target))
            next = after->logical_anchor();
    return sparse_index_answer{found, std::nullopt, next};
}
} // namespace

segment_read_index active_read_index(
  const active_sparse_index& index, runtime::file_position bound) {
    return segment_read_index{
      [&index, bound](
        model::range_logical_offset target,
        seastar::abort_source&,
        sparse_index_place&) {
          return seastar::make_ready_future<
            runtime::result<sparse_index_answer>>(
            fed_answer(index, bound, target));
      },
      // Nothing rebuilds the index of a segment that is still written: a
      // read that meets an anchor its data does not bear out fails, and
      // says why.
      [](const sparse_index_answer&, runtime::operation_error) noexcept {},
      // It is all in memory.
      [&index, bound](
        model::range_logical_offset target,
        const sparse_index_place&) noexcept {
          return std::optional{fed_answer(index, bound, target)};
      }};
}

struct segment_reader::state final : detail::segment_session_link {
    state(
      segment_runs& runs,
      seastar::gate::holder entered,
      segment_data_source source,
      segment_read_bounds bounds,
      segment_read_index index,
      workload_budget& budget,
      segment_reader_config config) noexcept
      : runs(runs)
      , entered(std::move(entered))
      , source(std::move(source))
      , bounds(bounds)
      , index(std::move(index))
      , budget(budget)
      , config(config) {}
    state(const state&) = delete;
    state& operator=(const state&) = delete;
    ~state() { leave(); }

    segment_runs& runs;
    seastar::gate::holder entered;
    // Nothing once the session is revoked and out of its last read. A move
    // puts the new one where the old one was, so a read in flight that
    // refers to it goes on with the new one.
    std::optional<segment_data_source> source;
    segment_read_bounds bounds;
    segment_read_index index;
    workload_budget& budget;
    segment_reader_config config;
    // The list of its segment's owner, while the session is in it.
    segment_sessions* among{nullptr};
    bool reading{false};
    // Its segment's owner ended it; `released` once it let go of what it
    // was opened with, which a read in flight puts off until it returns.
    bool revoked{false};
    bool released{false};
    // Where the session stands in the index: see segment_read_index.
    sparse_index_place place{};
    // Where the run read last began and ended, and how many were read in a
    // row, each beginning where the one before it ended.
    std::optional<runtime::file_position> entered_run{};
    std::optional<runtime::file_position> last_end{};
    std::uint32_t streak{0};
    // How many runs ahead the session looks now.
    std::uint32_t reach{0};
    // The run of the cache's that this session marked its use of last.
    std::optional<runtime::file_position> used_run{};

    seastar::future<segment_read>
    read(segment_read_request request, seastar::abort_source& caller);
    // How far ahead a session looks when it opens, and again when what it
    // reads stops following what it read.
    [[nodiscard]] std::uint32_t opening() const noexcept {
        return config.read_ahead_grows
                 ? std::min<std::uint32_t>(1, config.read_ahead)
                 : config.read_ahead;
    }
    // This session's one use of a run that it was given by a lookup which
    // is not a use, or of one it reads while its writer fills it: a run is
    // used once by each reader that reads it, however many reads that
    // takes. A session that leaves nothing behind earns a run nothing.
    template<typename Held>
    void use(const segment_run& run, const Held& held) noexcept {
        if (!config.fill || used_run == std::optional{run.begin()}) return;
        used_run = run.begin();
        held.use();
    }
    // A run the cache keeps is being read, and its reader `waited` for it
    // or found it there. The second in a row asks for those after it.
    void enter(const segment_run& run, bool waited);
    void ahead(const segment_run& run);

    void leave() noexcept {
        if (auto* list = std::exchange(among, nullptr))
            list->open_.erase(list->open_.iterator_to(*this));
    }
    // Lets go of what the session was opened with. No read is in flight.
    void release() noexcept {
        if (std::exchange(released, true)) return;
        place = {};
        index = {};
        source.reset();
        entered.release();
    }
    void revoke() noexcept {
        leave();
        revoked = true;
        if (!reading) release();
    }
};

segment_sessions::~segment_sessions() {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-SEGMENT-SESSIONS-ENDED"},
      open_.empty(),
      "segment sessions destroyed with a session in the list");
}

void segment_sessions::move(
  const segment_data_source& sealed,
  const seastar::noncopyable_function<segment_read_index()>& lookups) noexcept {
    for (auto next = open_.begin(); next != open_.end();) {
        auto& session = static_cast<segment_reader::state&>(*next++);
        bool moved = false;
        try {
            if (auto shared = sealed.share()) {
                auto found = lookups();
                if (found.locate && found.report && found.peek) {
                    session.source.emplace(std::move(*shared));
                    session.index = std::move(found);
                    moved = true;
                }
            }
        } catch (...) {
        }
        if (!moved) session.revoke();
    }
}

void segment_sessions::revoke() noexcept {
    while (!open_.empty())
        static_cast<segment_reader::state&>(open_.front()).revoke();
}

runtime::result<segment_reader> segment_reader::open(
  segment_runs& runs,
  segment_data_source source,
  segment_read_bounds bounds,
  segment_read_index index,
  workload_budget& budget,
  segment_reader_config config,
  segment_sessions* among) {
    if (
      !index.locate || !index.report || !index.peek
      || config.output_bytes.value() < sizeof(segment_read_batch)
      || config.held_bytes.value() == 0
      || config.read_ahead > maximum_read_ahead
      || bounds.durable < runs.history().data_start
      || (bounds.sealed && *bounds.sealed < runs.history().logical_origin))
        return runtime::failure(read_error(errc::invalid_argument));
    auto entered = runs.enter();
    if (!entered) return runtime::failure(entered.error());
    try {
        auto made = std::unique_ptr<state>{new state{
          runs,
          std::move(*entered),
          std::move(source),
          bounds,
          std::move(index),
          budget,
          config}};
        made->reach = made->opening();
        if (among) {
            among->open_.push_back(*made);
            made->among = among;
        }
        return segment_reader{std::move(made)};
    } catch (const std::bad_alloc&) {
        return runtime::failure(read_error(errc::resource_exhausted));
    }
}

segment_reader::segment_reader(std::unique_ptr<state> made) noexcept
  : state_(std::move(made)) {}
segment_reader::segment_reader(segment_reader&&) noexcept = default;
segment_reader::~segment_reader() {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-SEGMENT-READER-IDLE"},
      !state_ || !state_->reading,
      "segment reader destroyed while a read was in flight");
}

const segment_read_bounds& segment_reader::bounds() const& noexcept {
    return state_->bounds;
}
bool segment_reader::revoked() const noexcept { return state_->revoked; }

seastar::future<segment_read> segment_reader::read(
  segment_read_request request, seastar::abort_source& caller) {
    return state_->read(request, caller);
}

void segment_reader::state::enter(const segment_run& run, bool waited) {
    if (config.read_ahead == 0 || !config.fill || run.blocks().empty()) return;
    if (entered_run == std::optional{run.begin()}) return;
    const bool follows = last_end == std::optional{run.begin()};
    streak = follows ? streak + 1 : 1;
    entered_run = run.begin();
    last_end = run.end();
    // What it learned of one sequence says nothing of the next.
    if (!follows) reach = opening();
    if (streak < 2) return;
    // What was asked for was not there when its reader came: it looks one
    // run further from here on.
    if (waited && config.read_ahead_grows && reach < config.read_ahead) ++reach;
    if (run.end() < bounds.durable) ahead(run);
}

void segment_reader::state::ahead(const segment_run& run) {
    std::optional<model::range_logical_offset> base;
    if (
      const auto first = model::range_logical_offset::make(
        run.blocks().back().logical.end().value()))
        base = *first;
    auto begins = run.end();
    std::uint64_t asked = 0;
    for (std::uint32_t looked_at = 0; base && looked_at < reach; ++looked_at) {
        if (bounds.visible <= base->as_end()) return;
        // What the index holds in memory or the session stands in, and
        // nothing it would have to read or wait for.
        const auto answer = index.peek(*base, place);
        if (!answer || !answer->position) return;
        const auto start = segment_run_at(
          runs.history(), answer->position->anchor);
        const auto end = answer->position->end;
        // The run that begins where the one before it ends, whole, and
        // below the session's bounds.
        if (
          !start || start->position != begins || start->base != *base
          || !(
            end < bounds.durable || (end == bounds.durable && bounds.sealed)))
            return;
        // No further ahead than the window, the first run whatever its
        // length: runs that are longer are fewer.
        asked += end.value() - begins.value();
        if (looked_at != 0 && asked > config.read_ahead_bytes.value()) return;
        // One that is there or on its way is passed over. One that is
        // dropped ends it: nothing is asked for past a run that was not.
        // Where readers behind this one were announced, its own read of
        // what it asks for is one more still to come.
        const auto readers
          = config.behind == 0
                || config.behind == std::numeric_limits<std::uint32_t>::max()
              ? config.behind
              : config.behind + 1U;
        if (
          !runs.holds(start->base)
          && !runs.prefetch(*start, end, *source, bounds.durable, readers))
            return;
        base = answer->next;
        begins = end;
    }
}

seastar::future<segment_read> segment_reader::state::read(
  segment_read_request request, seastar::abort_source& caller) {
    segment_read out;
    out.next = request.from;
    // Its segment's owner ended the session, and what it was opened with
    // may be gone: nothing of it is looked at.
    if (revoked) {
        out.stop = segment_read_stop::failed;
        out.error = read_error(errc::closed);
        co_return out;
    }
    runs.assert_current();
    // Until something else ends the read it ends because nothing more of
    // this copy lies below the session's durable bound: what that is called
    // is settled where the read returns, by where it stands then.
    bool drained = true;
    const auto stop_at = [&out, &drained](segment_read_stop why) {
        out.stop = why;
        drained = false;
    };
    const auto fail =
      [this, &out, &caller, &drained](const runtime::operation_error& why) {
          drained = false;
          // A caller that left is not a read that failed; a wait that was
          // ended for the cache's or the index's own reasons is not a caller
          // that left.
          if (why.code() == errc::aborted && caller.abort_requested()) {
              out.stop = segment_read_stop::cancelled;
          } else {
              out.stop = segment_read_stop::failed;
              out.error = why.code() == errc::aborted ? read_error(errc::closed)
                                                      : why;
              // A place handed back that misled the read was forgotten
              // before it came here: what fails here is the segment's.
              runs.read_failed(why);
          }
      };
    // One read at a time is its caller's to keep: a second is not a read
    // that waits its turn.
    if (reading) {
        fail(read_error(errc::invalid_argument));
        co_return out;
    }
    const auto& history = runs.history();
    if (request.from.offset < history.logical_origin) {
        out.stop = segment_read_stop::below_start;
        co_return out;
    }
    const auto durable = bounds.durable;
    const auto visible = bounds.visible.value();
    // What a read stops for at the end of a sealed copy: the end of the
    // segment, unless what its caller may see was cut to what this copy
    // holds, and that is where it was cut.
    const auto sealed_end = bounds.short_of_visibility && bounds.sealed
                                && bounds.visible <= *bounds.sealed
                              ? segment_read_stop::short_of_visibility
                              : segment_read_stop::end_of_segment;
    // And when the next offset is past what its caller may see.
    const auto unseen = bounds.short_of_visibility
                          ? segment_read_stop::short_of_visibility
                          : segment_read_stop::not_yet_visible;
    auto cursor = request.from.offset.value();
    // A session that leaves nothing behind is not a reason to feed the
    // segment. One that waits at its end for more to see is.
    if (config.fill) runs.touch();
    // Neither answer needs anything of the budget or of the device.
    if (bounds.sealed && cursor >= bounds.sealed->value()) {
        out.stop = sealed_end;
        co_return out;
    }
    if (cursor >= visible) {
        out.stop = unseen;
        co_return out;
    }
    reading = true;
    const auto idle = seastar::defer([this] noexcept {
        reading = false;
        // What its owner asked of it while it read.
        if (revoked) release();
    });
    try {
        auto table = budget.try_reserve(config.output_bytes);
        if (!table) {
            fail(table.error());
            co_return out;
        }
        out.held.emplace(std::move(*table));
    } catch (const std::bad_alloc&) {
        fail(read_error(errc::resource_exhausted));
        co_return out;
    }
    // Where the last read said the block that holds the cursor begins. A
    // place that proves wrong is forgotten, and not handed back by a read
    // that returns nothing; one no block can begin at is not followed.
    auto hint = request.from.block;
    const auto forget_hint = [&out, &hint] {
        hint.reset();
        if (out.batches.empty()) out.next.block.reset();
    };
    if (hint && !history.alignment.aligned(*hint)) forget_hint();
    std::uint64_t spent = 0;
    // The runs this read's batches keep in the cache, and the one charged
    // last: each is charged once.
    std::uint64_t kept_bytes = 0;
    std::optional<runtime::file_position> charged;

    enum class step : std::uint8_t { on, limit, durable, visible };
    const auto stopped = [&](step why) {
        if (why == step::limit) stop_at(segment_read_stop::limit);
        if (why == step::visible) stop_at(unseen);
    };
    // Takes the batches of one run from the cursor on, while they lie below
    // `readable` and the offset the session may see, and fit the limits.
    // `cached` says the cache is charged for the run: what this read keeps
    // of such a run is charged to its reader as well.
    const auto take = [&](
                        const segment_run& run,
                        const cache_hold<segment_run>& held,
                        runtime::file_position readable,
                        bool cached) -> runtime::result<step> {
        const auto wanted = model::range_logical_offset::make(cursor);
        if (!wanted) return step::durable;
        const auto blocks = run.blocks();
        for (auto at = run.seek(*wanted); at < blocks.size(); ++at) {
            const auto& block = blocks[at];
            // What its caller may not see yet is not what this copy lacks.
            if (block.logical.end().value() > visible) return step::visible;
            if (
              run.begin().value() + block.offset + block.bytes
              > readable.value())
                return step::durable;
            const auto cost = table_cost(block);
            if (out.batches.empty()) {
                if (
                  !request.at_least_one
                  && block.batch_bytes > request.limit.value()) {
                    out.wanted = byte_count{block.batch_bytes};
                    return step::limit;
                }
            } else if (
              out.bytes.value() + block.batch_bytes > request.limit.value()
              || out.batches.size() == maximum_read_batches
              || spent + cost > config.output_bytes.value()) {
                return step::limit;
            }
            if (cached && charged != std::optional{run.begin()}) {
                // Its reader's budget takes the run on, or the read ends
                // here: with what it has, or, having nothing, with the
                // refusal.
                const auto pinned = run.charge().value();
                if (
                  !out.batches.empty()
                  && pinned > config.held_bytes.value() - kept_bytes)
                    return step::limit;
                std::optional<runtime::operation_error> short_of;
                try {
                    auto more = budget.try_reserve(byte_count{pinned});
                    if (!more)
                        short_of = more.error();
                    else if (
                      auto grown = out.held->adopt(std::move(*more)); !grown)
                        short_of = grown.error();
                } catch (const std::bad_alloc&) {
                    short_of = read_error(errc::resource_exhausted);
                }
                if (short_of) {
                    if (out.batches.empty()) return runtime::failure(*short_of);
                    return step::limit;
                }
                charged = run.begin();
                kept_bytes += std::min(
                  pinned, config.held_bytes.value() - kept_bytes);
            }
            auto shared = run.batch(at);
            if (!shared) return runtime::failure(shared.error());
            try {
                if (out.batches.size() == out.batches.capacity())
                    out.batches.reserve(
                      std::min<std::size_t>(
                        maximum_read_batches,
                        std::max<std::size_t>(4, 2 * out.batches.capacity())));
            } catch (const std::bad_alloc&) {
                return runtime::failure(read_error(errc::resource_exhausted));
            }
            out.batches.push_back(
              segment_read_batch{
                block.logical, block.physical, std::move(*shared), held, {}});
            out.bytes = byte_count{out.bytes.value() + block.batch_bytes};
            out.next.offset = block.logical.end();
            out.next.block.reset();
            if (const auto following = run.after(at))
                out.next.block = following->position;
            spent += cost;
            cursor = block.logical.end().value();
            hint.reset();
            // Enough: the run after this one is not read to find that out.
            if (out.bytes.value() >= request.limit.value()) return step::limit;
        }
        return step::on;
    };

    // A stretch read in parts goes on from the last block read, with no
    // lookup, until it reaches `resume_end`.
    std::optional<segment_run_start> resume;
    runtime::file_position resume_end;
    // The run read last gave the reader nothing: one more that begins where
    // it did would be the same.
    std::optional<runtime::file_position> barren;
    bool reported = false;
    try {
        for (;;) {
            if (caller.abort_requested()) {
                stop_at(segment_read_stop::cancelled);
                break;
            }
            // Its segment's owner ended the session: the read ends here,
            // with what it has.
            if (revoked) {
                if (out.batches.empty())
                    fail(read_error(errc::closed));
                else
                    stop_at(segment_read_stop::limit);
                break;
            }
            if (bounds.sealed && cursor >= bounds.sealed->value()) {
                stop_at(sealed_end);
                break;
            }
            if (cursor >= visible) {
                stop_at(unseen);
                break;
            }
            const auto wanted = model::range_logical_offset::make(cursor);
            if (!wanted) break;

            std::optional<segment_run_start> start;
            std::optional<sparse_index_answer> answered;
            runtime::file_position end;
            bool whole = false;
            if (resume) {
                start = std::exchange(resume, std::nullopt);
                end = resume_end;
            } else {
                // What the cache keeps of the segment, and then what its
                // writer fed that the cache does not answer with yet. A run
                // that reaches this session's bound and holds nothing at or
                // after the offset says there is nothing to read: no file
                // is touched to find that out. Looking is not a use: the
                // run is used when it holds the offset.
                std::optional<runtime::result<step>> stepped;
                bool exhausted = false;
                if (auto kept = runs.floor(*wanted, false)) {
                    if (kept->seek(*wanted) < kept->blocks().size()) {
                        use(*kept, kept);
                        enter(*kept, false);
                        const bool cached = !kept.funded();
                        const auto& run = *kept;
                        stepped.emplace(take(run, kept, durable, cached));
                        exhausted = run.end() >= durable;
                    } else if (kept->end() >= durable) {
                        break;
                    }
                }
                if (!stepped) {
                    if (auto fed = runs.tail(*wanted)) {
                        if (
                          fed->run->seek(*wanted) < fed->run->blocks().size()) {
                            use(*fed->run, fed->held);
                            stepped.emplace(take(
                              *fed->run,
                              fed->held,
                              std::min(durable, fed->readable),
                              true));
                            exhausted = fed->run->end() >= durable;
                        } else if (fed->run->end() >= durable) {
                            break;
                        }
                    }
                }
                if (stepped) {
                    if (!*stepped) {
                        fail(stepped->error());
                        break;
                    }
                    if (**stepped != step::on) {
                        stopped(**stepped);
                        break;
                    }
                    if (exhausted) break;
                    continue;
                }
                // The index, which is waited for when it is owed.
                auto answer = co_await index.locate(*wanted, caller, place);
                // A caller that left while it was looked up is not read
                // for.
                if (caller.abort_requested()) {
                    stop_at(segment_read_stop::cancelled);
                    break;
                }
                if (!answer) {
                    // Turned away with batches in hand: they are what the
                    // read returns, and the next one asks again.
                    if (!out.batches.empty() && refused(answer.error().code()))
                        stop_at(segment_read_stop::limit);
                    else
                        fail(answer.error());
                    break;
                }
                // No anchor at or before the segment's first offset:
                // nothing is readable in it yet.
                if (!answer->position) break;
                start = segment_run_at(history, answer->position->anchor);
                if (!start) {
                    const auto wrong = read_error(errc::wrong_context);
                    index.report(*answer, wrong);
                    fail(wrong);
                    break;
                }
                end = answer->position->end;
                // A run the cache keeps reaches the next anchor, or the
                // end of a sealed extent, and lies below this session's
                // bound.
                whole = end < durable || (end == durable && bounds.sealed);
                answered.emplace(*answer);
            }
            if (start->position >= durable) break;
            if (barren && *barren == start->position) {
                fail(read_error(errc::malformed_data));
                break;
            }
            const auto until = std::min(end, durable);

            runtime::result<cache_ref<segment_run>> loaded = runtime::failure(
              read_error(errc::queue_full));
            // A read that was handed the place of the block it goes on
            // with begins there, not at the anchor, when the run is not the
            // cache's to keep.
            const bool placed = answered && hint && start->position < *hint
                                && *hint < until;
            bool hinted = false;
            bool alone = true;
            if (whole && config.fill) {
                loaded = co_await runs.fetch(
                  *start, end, *source, durable, caller, config.behind);
                // No room for it: read all the same, once for everyone who
                // misses it together, unless this reader goes on from a
                // place inside it.
                if (!loaded && unkept(loaded.error().code()) && !placed)
                    loaded = co_await runs.fetch(
                      *start,
                      end,
                      *source,
                      durable,
                      caller,
                      config.behind,
                      &budget);
                alone = !loaded;
            }
            if (!loaded && unkept(loaded.error().code())) {
                // For this reader alone.
                auto from = *start;
                if (placed) {
                    const auto physical = history.physical_origin.checked_add(
                      model::segment_record_count{
                        cursor - history.logical_origin.value()});
                    if (physical) {
                        from = segment_run_start{*hint, *wanted, *physical};
                        hinted = true;
                    }
                }
                loaded = co_await runs.read_alone(
                  from, until, *source, durable, budget, caller);
                if (hinted) start = from;
            }
            if (!loaded) {
                const auto code = loaded.error().code();
                // Whatever a place that was handed back led to, short of
                // the caller's leaving or a refusal, says the place was
                // wrong before it says anything of the data: it is
                // forgotten, and the anchor is read from.
                if (
                  hinted && code != errc::aborted && code != errc::closed
                  && !refused(code)) {
                    forget_hint();
                    continue;
                }
                // The block where the anchor points is whole and is not
                // the block it names. The index is told, once, and asked
                // again: it answers from what it rebuilds.
                if (answered && code == errc::wrong_context && !reported) {
                    index.report(*answered, loaded.error());
                    reported = true;
                    continue;
                }
                // Turned away with batches in hand.
                if (!out.batches.empty() && refused(code))
                    stop_at(segment_read_stop::limit);
                else
                    fail(loaded.error());
                break;
            }
            const auto& run = **loaded;
            // A place that led to no block at all was not the block's
            // place: nothing is taken on its word.
            if (hinted && run.blocks().empty()) {
                forget_hint();
                continue;
            }
            if (!alone) {
                // Its own load is not a use, and one it joined was marked
                // where it joined.
                used_run = run.begin();
                enter(run, true);
            }
            const auto before = out.batches.size();
            const auto stepped = take(
              run, *loaded, durable, !alone && !loaded->funded());
            if (!stepped) {
                fail(stepped.error());
                break;
            }
            if (*stepped != step::on) {
                stopped(*stepped);
                break;
            }
            if (run.end() >= durable) break;
            if (out.batches.size() == before) barren = start->position;
            // What follows a run that stopped short of its stretch's end
            // is whatever lies after its last complete object.
            if (run.end() < until) {
                if (run.blocks().empty()) {
                    resume = segment_run_start{
                      run.end(), start->base, start->physical};
                } else {
                    const auto last = run.blocks().size() - 1;
                    resume = run.after(last);
                    cursor = std::max(
                      cursor, run.blocks()[last].logical.end().value());
                }
                resume_end = until;
                if (resume) barren.reset();
            }
        }
    } catch (const std::bad_alloc&) {
        // A frame or a reservation that could not be made: what was read
        // is still returned.
        fail(read_error(errc::resource_exhausted));
    }
    // Nothing more of this copy lay below the durable bound. That is the
    // end of a sealed segment only where its records end.
    if (drained)
        out.stop = bounds.sealed && cursor >= bounds.sealed->value()
                     ? sealed_end
                     : segment_read_stop::short_of_visibility;
    // The charge lasts as long as any batch does.
    for (auto& batch : out.batches)
        batch.charged.emplace(out.held->share());
    co_return out;
}

} // namespace kwaque::storage
