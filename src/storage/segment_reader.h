#pragma once

#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/model/position.h"
#include "src/runtime/error.h"
#include "src/runtime/file_position.h"
#include "src/storage/page_cache.h"
#include "src/storage/segment_run.h"
#include "src/storage/sparse_index.h"
#include "src/storage/sparse_index_owner.h"
#include "src/storage/workload_budget.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/util/noncopyable_function.hh>

#include <boost/intrusive/list.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

namespace kwaque::storage {

// How a reader finds the anchor at or before an offset, and whom it tells of
// an anchor its data did not bear out. The lookup waits, with every other
// reader, for the rebuild of an index that is owed. Whatever these refer to
// outlives the session that is given them.
//
// A session stands in the index where its last answer came from, and hands
// that place to each lookup: an index that keeps its anchors in pages
// answers from the page the session holds when the offset falls there, and
// moves the place when it does not. An index that is all in memory leaves
// the place alone.
struct segment_read_index final {
    seastar::noncopyable_function<seastar::future<
      runtime::result<sparse_index_answer>>(
      model::range_logical_offset, seastar::abort_source&, sparse_index_place&)>
      locate;
    // The block where the anchor of an answer points is whole and is not
    // the block the anchor names.
    seastar::noncopyable_function<void(
      const sparse_index_answer&, runtime::operation_error) noexcept>
      report;
    // What `locate` would answer, when the index can say so from what is in
    // memory or from the place the session stands in: nothing is waited for
    // and nothing is read. Nothing when it cannot. A reader asks this of a
    // run it does not read yet.
    seastar::noncopyable_function<std::optional<sparse_index_answer>(
      model::range_logical_offset, const sparse_index_place&) noexcept>
      peek;
};
// The lookups of a sealed segment's index, through its owner.
template<typename Index>
[[nodiscard]] segment_read_index sealed_read_index(Index& index) {
    return segment_read_index{
      [&index](
        model::range_logical_offset target,
        seastar::abort_source& caller,
        sparse_index_place& at) { return index.locate(target, caller, &at); },
      [&index](
        const sparse_index_answer& given,
        runtime::operation_error why) noexcept { index.report(given, why); },
      [&index](
        model::range_logical_offset target,
        const sparse_index_place& at) noexcept {
          return index.peek(target, &at);
      }};
}
// The lookups of a segment that is still appended to, in the index its
// writer feeds: the anchors that are durable when the lookup is made. A run
// that reaches past `bound`, which is the session's, is not one the session
// reads whole.
[[nodiscard]] segment_read_index active_read_index(
  const active_sparse_index& index, runtime::file_position bound);

// How far a session reads. Taken when it opens and never widened: a new
// session takes new bounds.
struct segment_read_bounds final {
    // In the segment's file: the durable position its writer gave with the
    // pin, or the end of a sealed extent.
    runtime::file_position durable;
    // Where a sealed segment's records end. Nothing for a segment that is
    // still appended to. The run of a sealed segment's last anchor ends
    // where its extent does, and is one the cache keeps.
    std::optional<model::range_logical_end> sealed;
    // Nothing at or past this offset is returned: what the session's caller
    // may see. It is supplied, and nothing read here widens it.
    model::range_logical_end visible;
    // The supplied end was already cut to what this copy holds, as the read
    // bound of a recovered segment is: a read that reaches it says that
    // this copy is short of what its caller may see, not that there is no
    // more to see.
    bool short_of_visibility{false};
};

// The most runs a session asks for ahead of the one it reads.
inline constexpr std::uint32_t maximum_read_ahead = 4;

struct segment_reader_config final {
    // Whether what this session reads from the device is kept by the cache.
    // A session that would not come back, a scan, reads what the cache
    // holds, earns it nothing and leaves nothing behind.
    bool fill{true};
    // The readers its owner says are still behind this one: a run this
    // session loads is kept for that many reads.
    std::uint32_t behind{0};
    // How many runs past the one it reads the session asks for, once it has
    // read two in a row: none, or up to maximum_read_ahead. Each request
    // takes what it needs without waiting or is dropped, and none passes
    // the session's bounds. One run ahead is one load in flight, which a
    // reader that is done with a run before its device is gains nothing
    // from: it takes more than one for a load to overlap another.
    std::uint32_t read_ahead{maximum_read_ahead};
    // The session begins one run ahead and looks one further, up to
    // `read_ahead`, each time it had to wait for a run in a row, and begins
    // again at one when a run it reads does not follow the last: a reader
    // whose runs are there asks for one, and a short read wastes one.
    // Otherwise it looks `read_ahead` runs ahead from the start.
    bool read_ahead_grows{true};
    // The most bytes asked for ahead of the run being read, whatever the
    // runs' length: the first run ahead whatever its own.
    byte_count read_ahead_bytes{128_KiB};
    // What one read's output may hold beside the bytes it shares.
    byte_count output_bytes{64_KiB};
    // The most bytes of runs that one read's batches keep in the cache: its
    // first run whatever its length. A read that reaches it stops as for
    // any limit.
    byte_count held_bytes{1_MiB};
};

// One batch, whole: the bytes of its encoded form as they were written, a
// share of the run they were read in. `held` keeps those bytes where they
// are for as long as the batch lives, whatever becomes of the session, of
// the run's place in the cache, or of the cache; it is not a way to the run.
// `charged` is the batch's part in what its read's reader is charged: see
// segment_read.
struct segment_read_batch final {
    model::range_logical_span logical;
    model::segment_relative_span physical;
    bytes::fragmented_buffer bytes;
    cache_hold<segment_run> held;
    std::optional<workload_reservation> charged{};
};

// The most batches one read returns.
inline constexpr std::uint32_t maximum_read_batches = 1024;

// Where a read begins, and where the one after it goes on: an offset, and,
// when the read that handed it back knew, where in the file the block that
// follows the last batch returned begins. A read given that place and
// nothing in memory reads from it and not from the anchor before it, and
// checks the block it finds there as it checks an anchor's. A place that
// does not bear its offset out is forgotten and looked for again.
struct segment_read_position final {
    model::range_logical_end offset;
    std::optional<runtime::file_position> block;
    bool operator==(const segment_read_position&) const noexcept = default;
};

struct segment_read_request final {
    segment_read_position from;
    // The most encoded bytes of batches to return.
    byte_count limit;
    // The first batch is returned whatever the limit. Without this a first
    // batch over the limit is not returned, and the read says how large it
    // is.
    bool at_least_one{true};
};

// Why a read returned what it did and no more. Batches may come with any of
// these but the offset below the start.
enum class segment_read_stop : std::uint8_t {
    // The read's limits were reached, or its reader's budget had no more
    // to give for what the batches hold. Never the end of anything: the next
    // read says what follows, which may be nothing.
    limit,
    // The next offset is not one the session's caller may see yet.
    not_yet_visible,
    // The end of a sealed segment: nothing follows in this segment, ever.
    end_of_segment,
    // The offset lies before the segment's first.
    below_start,
    // This copy holds less than the session's caller may see: the read
    // stopped where its durable data ends, or where a bound that was cut to
    // what this copy holds ends, also when that is where a sealed copy
    // ends. A report about this copy, never a lower end of what is visible.
    short_of_visibility,
    // The caller left.
    cancelled,
    // The read could not go on: `error` says why.
    failed,
};

struct segment_read final {
    // In order, each whole. The first is the batch that holds the offset
    // asked for, or the next one when no batch holds it; its span says which.
    std::vector<segment_read_batch> batches;
    // Where the next read goes on: after the last batch returned, or where
    // this read was asked to begin when none was.
    segment_read_position next;
    segment_read_stop stop{segment_read_stop::failed};
    // The encoded bytes of the batches together.
    byte_count bytes;
    // The limit, and nothing returned: the encoded size of the batch that
    // did not fit, for a caller that asks again.
    byte_count wanted;
    std::optional<runtime::operation_error> error;
    // What the read's reader is charged for it: the table above, and the
    // runs its batches keep in the cache, each once. The cache is charged
    // for those runs too; this is what bounds what readers hold of it, by
    // what their budget grants. Every batch shares it, so the charge lasts
    // as long as any of them does.
    std::optional<workload_reservation> held;
};

namespace detail {
// A session's place in its segment's list.
struct segment_session_link {
    boost::intrusive::list_member_hook<> listed;
};
} // namespace detail

// The sessions open on one segment, for whoever owns the segment: a session
// opened with a list is in it until it is destroyed or given up here. The
// list lets its owner do to every session at once what a seal and a drain of
// the segment need, and holds nothing of a session itself. It is destroyed
// empty: revoke() empties it.
class segment_sessions final {
public:
    segment_sessions() noexcept = default;
    segment_sessions(const segment_sessions&) = delete;
    segment_sessions& operator=(const segment_sessions&) = delete;
    ~segment_sessions();

    // Sessions in the list.
    [[nodiscard]] std::size_t size() const noexcept { return open_.size(); }

    // The segment was sealed. From here on every session in the list reads
    // through its own share of `sealed`, the pin of the generation the
    // segment was published as, and looks up in what `lookups` gives it, the
    // sealed segment's index: in one step, with nothing waited for. No
    // session then holds the pin its writer gave it or refers to the index
    // its writer fed, so the writer and that index may go. A read in flight
    // goes on with what it has in hand and takes the new source and lookups
    // where it next needs them; what it has asked of the old source
    // completes by itself. Bounds, budget and what was returned are
    // unchanged: a session reads on to the bound it took.
    //
    // A session that cannot be given a share is revoked instead.
    void move(
      const segment_data_source& sealed,
      const seastar::noncopyable_function<segment_read_index()>&
        lookups) noexcept;

    // The segment's readers are ended: for its close, and before it is
    // removed. Every session leaves the list. One that is not in a read lets
    // go at once of its place among the segment's runs, of its source and of
    // its lookups; one in a read does when that read returns, and the read
    // ends where it stands, with the batches it has. A revoked session's
    // next read fails with closed. What a session returned is its holder's
    // and is unchanged.
    void revoke() noexcept;

private:
    friend class segment_reader;
    using link = detail::segment_session_link;
    boost::intrusive::list<
      link,
      boost::intrusive::
        member_hook<link, boost::intrusive::list_member_hook<>, &link::listed>>
      open_;
};

// One reader's session on one segment: it reads whole batches forward from
// an offset.
//
// A session takes its bounds when it opens and keeps them; see
// segment_read_bounds. A read looks for its offset in the runs the cache
// keeps of the segment, then in what its writer fed and the cache does not
// answer with yet, and then asks the index for an anchor and reads that
// anchor's run: as the cache's own work when it is a run the cache keeps,
// and otherwise under this session's budget, with whoever misses the same
// run at the same time when it is a whole run. An index that is owed is
// waited for. A block where an anchor points that is whole and is not the
// block the anchor names is reported to the index, once, and looked for
// again; a block there that does not read as one is the data's damage and
// that read's failure.
//
// A batch is returned whole or not at all. An offset inside a batch returns
// that batch; one that no batch of its run holds returns the next batch of
// that run. Every read says why it stopped, and where the next one goes on.
//
// What a read returns is charged to the session's budget: its table, and
// the runs its batches keep in the cache. A budget that has no more ends
// the read where it stands, as a limit does, and fails one that has
// nothing to return yet. The composition keeps what its readers' budgets
// can hold below what their cache holds.
//
// The session is move-only. What it was opened with outlives it, and a read
// in flight ends before it is destroyed. What a read returned outlives the
// session; the budget it is charged to outlives what was returned.
//
// A session opened with a list is its segment owner's to move and to revoke;
// see segment_sessions. The list outlives it or revokes it. Once revoked a
// session needs nothing it was opened with but its budget.
class segment_reader final {
public:
    [[nodiscard]] static runtime::result<segment_reader> open(
      segment_runs& runs,
      segment_data_source source,
      segment_read_bounds bounds,
      segment_read_index index,
      workload_budget& budget,
      segment_reader_config config = {},
      segment_sessions* among = nullptr);
    segment_reader(segment_reader&&) noexcept;
    segment_reader& operator=(segment_reader&&) noexcept = delete;
    segment_reader(const segment_reader&) = delete;
    segment_reader& operator=(const segment_reader&) = delete;
    ~segment_reader();

    [[nodiscard]] const segment_read_bounds& bounds() const& noexcept;
    const segment_read_bounds& bounds() const&& = delete;
    // Its segment's owner ended it: every read from here on fails with
    // closed.
    [[nodiscard]] bool revoked() const noexcept;

    // Batches from the one a reader that wants the request's offset is
    // given, in order, while they lie below the session's bounds and fit
    // the request's limit. `caller` ends this read alone. One read at a
    // time.
    [[nodiscard]] seastar::future<segment_read>
    read(segment_read_request request, seastar::abort_source& caller);
    // The same from an offset alone, the first batch returned whatever the
    // limit.
    [[nodiscard]] seastar::future<segment_read> read(
      model::range_logical_offset target,
      byte_count limit,
      seastar::abort_source& caller) {
        return read(
          segment_read_request{{target.as_end(), std::nullopt}, limit, true},
          caller);
    }

private:
    friend class segment_sessions;
    struct state;
    explicit segment_reader(std::unique_ptr<state> made) noexcept;
    std::unique_ptr<state> state_;
};

} // namespace kwaque::storage
