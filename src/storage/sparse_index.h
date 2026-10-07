#pragma once

#include "src/base/units.h"
#include "src/model/position.h"
#include "src/runtime/error.h"
#include "src/runtime/file_position.h"
#include "src/storage/format_context.h"
#include "src/storage/local_bundle.h"
#include "src/storage/local_generation.h"
#include "src/storage/local_root.h"
#include "src/storage/local_store_config.h"
#include "src/storage/local_types.h"
#include "src/storage/segment_format.h"
#include "src/storage/segment_scan.h"
#include "src/storage/segment_writer_state.h"
#include "src/storage/sparse_index_format.h"
#include "src/storage/workload_budget.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/coroutine/as_future.hh>

#include <algorithm>
#include <cstdint>
#include <exception>
#include <optional>
#include <span>
#include <type_traits>
#include <utility>
#include <vector>

namespace kwaque::storage {

// How far apart anchors lie. Both distances run from where the last anchor's
// block begins to where a later block begins, so each bounds what a reader
// passes over between an anchor and the block it wants.
struct sparse_index_stride final {
    // A block is an anchor when it begins at least this many file bytes
    // after the last anchor's block begins. Zero makes every block an
    // anchor.
    byte_count bytes{32_KiB};
    // A block is also an anchor when it begins at least this many records
    // after the last anchor's block begins. Zero turns this off.
    std::uint64_t records{0};
    bool operator==(const sparse_index_stride&) const noexcept = default;
};

// Anchors in one persisted page: 16 KiB of entries. A reader admits a page's
// entries as one array, and this is what the default reader budget holds
// however its allocator rounds; it also divides a chunk of the table, so a
// page is one contiguous run of it.
inline constexpr std::uint32_t sparse_index_page_entries = 1024;

// Whether a reader working under these limits can decode the largest page an
// index of `anchors` entries has, a full page or all of them when they are
// fewer, and whether the policy lets one object hold that many. An owner
// checks its limits with this when it is made, so that no index is written
// that cannot be named and no reader later meets a page it cannot admit.
[[nodiscard]] runtime::result<void> validate_sparse_index_pages(
  const local_store_io_limits&,
  storage_alignment,
  const codec::limits&,
  std::uint32_t anchors) noexcept;

// What a lookup answers: the nearest anchor at or before a logical offset,
// and how far a forward scan from that anchor's block reads at most. The end
// is the next anchor's block where the index holds that anchor beside this
// one, and otherwise the end the lookup was given. Decoding blocks from the
// anchor to the offset, and what a reader may see, belong to whoever reads.
//
// Only the anchor is borne out by the block read where it points. The end
// sizes a read and proves nothing: a reader stops at the first block whose
// base is past its offset, and a block at the end whose base is not past it
// is an index that named a block wrongly, reported like any other.
struct sparse_index_position final {
    sparse_index_entry anchor;
    runtime::file_position end;
    bool operator==(const sparse_index_position&) const noexcept = default;
};

// The lookup over one run of anchors in order. Nothing lies at or before an
// offset below the first anchor: the first block that holds a record is
// always an anchor, so the first record at or after such an offset is in the
// extent's first block.
[[nodiscard]] std::optional<sparse_index_position> find_sparse_index_anchor(
  std::span<const sparse_index_entry>,
  model::range_logical_offset,
  runtime::file_position end) noexcept;

// The one page that can hold the nearest anchor at or before an offset: the
// last page whose first anchor is at or below it.
[[nodiscard]] std::optional<std::uint32_t> find_sparse_index_page(
  std::span<const model::range_logical_offset> first_anchors,
  model::range_logical_offset) noexcept;

// The lookup in a published index, through the pin of its root: the root
// routes to one page, that page is read and verified against the reference
// the root pins it with, and it is searched. The scan's end is the next
// anchor in that page, or the end of the covered extent. A page that cannot
// be read or is not the page its root names is an error about the index
// alone: its segment's data and the generation that pinned it are untouched,
// and the index is built again. The pin stays alive until this completes.
// Whether the offset lies in the extent at all is its caller's to know, from
// the coverage the root carries: an offset past the extent is answered with
// the last anchor like any other after it.
[[nodiscard]] seastar::future<
  runtime::result<std::optional<sparse_index_position>>>
find_published_anchor(
  const local_root_pin& index,
  model::range_logical_offset,
  codec::cooperative_work&);
// The two steps of that lookup that wait for nothing, around its one page
// read, for a caller that makes the read in its own frame. The first names
// the page to read, or none when the offset lies before the first anchor;
// wrong_context for a pin of another kind of root. The second searches the
// page read through the same pin, after checking that it is the page the
// root routed to.
[[nodiscard]] runtime::result<std::optional<page_ordinal>>
route_published_anchor(
  const local_root_pin& index, model::range_logical_offset);
[[nodiscard]] runtime::result<std::optional<sparse_index_position>>
search_published_page(
  const local_root_pin& index,
  page_ordinal,
  const local_index_page&,
  model::range_logical_offset);

// Checks an index against the blocks of its extent, given in file order:
// every anchor must name a block that begins where it points and holds its
// base, in order, the first block that holds a record must be an anchor, and
// no anchor may be left when the blocks end. Anchors are supplied a run at a
// time, the next run once the last is used up. A reader makes the same check
// for the one anchor it follows, on the block it reads there; this is that
// check for all of them.
class sparse_index_scrub final {
public:
    explicit sparse_index_scrub(sparse_index_context context) noexcept
      : context_(context) {}
    // Every anchor supplied so far has met its block.
    [[nodiscard]] bool drained() const noexcept { return run_.empty(); }
    // The anchors that follow, once drained. Borrowed until drained again.
    void supply(std::span<const sparse_index_entry> run) noexcept {
        run_ = run;
    }
    // The next complete block of the extent.
    [[nodiscard]] runtime::result<void>
    block(const complete_block_descriptor&) noexcept;
    // After the last block, for an index of `anchors` entries.
    [[nodiscard]] runtime::result<void>
    finish(std::uint32_t anchors) const noexcept;

private:
    sparse_index_context context_;
    std::span<const sparse_index_entry> run_;
    std::uint32_t matched_{0};
};

// The most anchors a segment can hold: one per block, and under a byte
// stride alone no more than one per stride of its data file.
[[nodiscard]] std::uint32_t sparse_index_capacity(
  sparse_index_stride,
  byte_count maximum_data_bytes,
  std::uint32_t maximum_blocks) noexcept;
// The same for a sealed extent: no more blocks than it holds records.
[[nodiscard]] std::uint32_t sparse_index_capacity(
  sparse_index_stride, const sparse_index_context& sealed) noexcept;

// The index of a segment that is still written: absolute original batch
// bases and the file positions of the complete blocks that begin there, in
// file order, held in memory only.
//
// Its memory is admitted in full when it is made, and allocated a chunk at a
// time as the segment grows. A chunk that cannot be allocated costs anchors,
// never a block's result: the block is passed over and the next one is tried,
// so the index only becomes sparser.
//
// A block is offered when its bytes are written and becomes an anchor only
// once it is durable. Nothing here returns an entry before that, and none is
// ever removed. Whoever feeds it holds its address, so it stays in place
// from then on: it is moved only before it is fed or once it is frozen, and
// what it was moved from is empty.
//
// Blocks are offered once, in file order from the first. A feeder that
// starts its extent again is given another index.
class active_sparse_index final {
public:
    // `capacity` is the most anchors it will hold; see sparse_index_capacity.
    [[nodiscard]] static runtime::result<active_sparse_index>
    make(sparse_index_stride, std::uint32_t capacity, workload_budget&);
    // What an index of `capacity` anchors is admitted for when it is made.
    [[nodiscard]] static runtime::result<byte_count>
    admission(std::uint32_t capacity, const workload_budget&) noexcept;
    active_sparse_index(active_sparse_index&&) noexcept;
    active_sparse_index& operator=(active_sparse_index&&) = delete;
    active_sparse_index(const active_sparse_index&) = delete;
    active_sparse_index& operator=(const active_sparse_index&) = delete;

    // One complete block whose bytes are written. Blocks arrive in file
    // order, each at or after the end of the one before it.
    void written(const coverage& block) noexcept;
    // Every block offered that begins below `end` is durable. `end` is where
    // a block ends: no block lies across it.
    void durable(runtime::file_position end) noexcept;
    // What its segment's writer is given to report to it: each written
    // group's blocks, and each advance of the durable position. It refers to
    // this index, which stays where it is while that writer lives.
    [[nodiscard]] segment_block_observer observer() noexcept;

    // Anchors, oldest first.
    [[nodiscard]] std::uint32_t size() const noexcept { return anchors_; }
    [[nodiscard]] bool empty() const noexcept { return anchors_ == 0; }
    [[nodiscard]] const sparse_index_entry&
    operator[](std::uint32_t anchor) const noexcept;
    [[nodiscard]] std::uint32_t capacity() const noexcept { return capacity_; }
    [[nodiscard]] sparse_index_stride stride() const noexcept {
        return stride_;
    }
    // Blocks the stride chose that are not anchors, for want of memory or of
    // room.
    [[nodiscard]] std::uint64_t skipped() const noexcept { return skipped_; }

    // The nearest anchor at or before an offset. `end` is how far its reader
    // may read, no further than where the segment's durable blocks end; it
    // is the scan's end when that anchor is the last.
    [[nodiscard]] std::optional<sparse_index_position> find(
      model::range_logical_offset, runtime::file_position end) const noexcept;

    // Its segment is sealed: no block is offered and none becomes durable
    // after this, so its anchors are the segment's index from here on.
    void freeze() noexcept { frozen_ = true; }
    [[nodiscard]] bool frozen() const noexcept { return frozen_; }
    // The pages its anchors fill, and one page's anchors: a contiguous run
    // that stays where it is for as long as the index lives.
    [[nodiscard]] std::uint32_t pages() const noexcept {
        return (anchors_ + sparse_index_page_entries - 1)
               / sparse_index_page_entries;
    }
    [[nodiscard]] std::span<const sparse_index_entry>
    page(std::uint32_t ordinal) const noexcept;

private:
    active_sparse_index(
      sparse_index_stride stride,
      std::uint32_t capacity,
      workload_reservation held) noexcept
      : held_(std::move(held))
      , stride_(stride)
      , capacity_(capacity) {}
    [[nodiscard]] const sparse_index_entry&
    chosen(std::uint32_t entry) const noexcept {
        return chunks_[entry / chunk_entries][entry % chunk_entries];
    }
    // 64 KiB of entries, so that a chunk stays within the contiguous
    // allocation ceiling however its allocator rounds it.
    static constexpr std::uint32_t chunk_entries = 4096;
    workload_reservation held_;
    // Anchors, then the blocks chosen that are not durable yet. The list is
    // sized when the index is made, so only a chunk is ever allocated later.
    std::vector<std::vector<sparse_index_entry>> chunks_;
    sparse_index_stride stride_;
    std::uint32_t capacity_;
    std::uint32_t chosen_{0}, anchors_{0};
    std::uint64_t skipped_{0};
    bool frozen_{false};
    // Where the last chosen block begins, and where the last offered one
    // ends.
    runtime::file_position chosen_bytes_{}, offered_end_{};
    model::segment_relative_end chosen_records_{};
};

// What a shard asks of the budget that funds the indexes of the segments it
// writes.
struct sparse_index_demand final {
    // Segments written at once.
    std::uint32_t segments{0};
    // The most anchors each of them holds; see sparse_index_capacity.
    std::uint32_t capacity{0};
    // Admissions and bytes for whatever else draws on the budget.
    std::uint32_t reserved_tasks{0};
    byte_count reserved_bytes{};
};

// Whether the budget can hold that demand: every index is admitted in full
// when its segment is made, and holds that admission while the segment is
// written. A shard checks its configured counts with this before it creates
// any owner, as it checks its handles. A stride and a segment count the
// budget cannot hold together are then a configuration error at start,
// carrying the limit and what the configuration expects of it, and not a
// refusal at the first segment that no longer fits.
//
// It counts the indexes of segments being written against this budget's own
// limits. What else the budget funds is its caller's to put in the reserve:
// an index built again or adopted that is not named yet, a rebuild's walk,
// the publication of an index at a seal, open roots. Another budget of the
// same workload class draws on the same memory and is not seen here.
[[nodiscard]] runtime::result<void>
validate_sparse_index_budget(const workload_budget&, sparse_index_demand);

namespace detail {
// A frozen index one encoded page at a time, in page order. Encoding the same
// index twice yields the same pages; the bundle writer checks the second pass
// against the references the first produced.
struct sparse_index_pages final {
    const active_sparse_index* index;
    sparse_index_context context;
    local_store_io_limits limits;
    byte_count retained;
    std::uint32_t ordinal{0};
    std::optional<page_ref> last;

    seastar::future<runtime::result<std::optional<bytes::fragmented_buffer>>>
    next(codec::cooperative_work& work) {
        if (ordinal == index->pages())
            co_return std::optional<bytes::fragmented_buffer>{};
        auto page = co_await encode_sparse_index_page(
          index->page(ordinal),
          context,
          page_ordinal::make(ordinal).value(),
          ordinal * sparse_index_page_entries,
          work,
          limits.operation_bytes.checked_sub(retained).value_or(byte_count{}),
          limits.charge);
        if (!page) co_return runtime::failure(path_error(page.error().code()));
        ++ordinal;
        last = page->reference;
        co_return std::optional<bytes::fragmented_buffer>{
          std::move(page->bytes)};
    }
};
} // namespace detail

// Publishes a frozen index as one immutable bundle under `context`, the
// sealed coverage and extent digest of its segment: pages of
// sparse_index_page_entries anchors, then a root that names each page and the
// anchor it begins with. `object` is a sequence nothing else was given. The
// segment's data and sealed root are durable already. Returns the root to name
// in the segment's publication; until a publication names it the bundle is
// referenced by nothing. A refusal or failure changes nothing a reader can
// see: the index is derived, and its segment is then indexed again later.
template<runtime::file_system_backend Backend, local_directory_owner Owner>
seastar::future<runtime::result<local_root_reference>> publish_sparse_index(
  Backend& files,
  Owner& ownership,
  const local_device_spec& spec,
  std::uint32_t shard,
  const active_sparse_index& index,
  sparse_index_context context,
  local_object_sequence object,
  workload_budget& budget,
  local_store_io_limits limits,
  codec::cooperative_work& work) {
    if (!index.frozen() || index.empty() || !object.is_valid())
        co_return runtime::failure(detail::path_error(errc::invalid_argument));
    if (
      auto valid = validate_sparse_index_pages(
        limits, context.alignment(), work.policy(), index.size());
      !valid)
        co_return runtime::failure(valid.error());
    const auto pages = index.pages();
    const auto refs_charge = budget.allocation_charge(
      byte_count{pages * sizeof(page_ref)});
    const auto firsts_charge = budget.allocation_charge(
      byte_count{pages * sizeof(model::range_logical_offset)});
    if (!refs_charge) co_return runtime::failure(refs_charge.error());
    if (!firsts_charge) co_return runtime::failure(firsts_charge.error());
    const auto retained = refs_charge->checked_add(*firsts_charge).value();
    auto held = budget.try_reserve(retained);
    if (!held) co_return runtime::failure(held.error());
    std::vector<page_ref> refs;
    std::vector<model::range_logical_offset> firsts;
    refs.reserve(pages);
    firsts.reserve(pages);
    detail::sparse_index_pages first_pass{&index, context, limits, retained};
    for (std::uint32_t i = 0; i < pages; ++i) {
        auto page = co_await first_pass.next(work);
        if (!page) co_return runtime::failure(page.error());
        if (!*page || !first_pass.last)
            co_return runtime::failure(detail::path_error(errc::wrong_context));
        refs.push_back(*first_pass.last);
        firsts.push_back(index.page(i).front().logical_anchor());
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
    }
    auto root = co_await encode_sparse_index_root(
      context,
      index.size(),
      refs,
      firsts,
      work,
      limits.operation_bytes.checked_sub(retained).value_or(byte_count{}),
      limits.charge);
    if (!root)
        co_return runtime::failure(detail::path_error(root.error().code()));
    const auto reference = local_root_reference::make(
      local_root_kind::index,
      object,
      runtime::file_position{},
      root->bytes.size(),
      page_count::make(pages).value(),
      root->digest);
    if (!reference)
        co_return runtime::failure(detail::path_error(errc::invalid_argument));
    auto bundle = co_await local_bundle::make(
      *reference, context, std::move(root->bytes), budget, limits, work);
    if (!bundle) co_return runtime::failure(bundle.error());
    auto published = co_await publish_local_bundle(
      files,
      ownership,
      spec,
      shard,
      std::move(*bundle),
      detail::sparse_index_pages{&index, context, limits, retained},
      [](codec::cooperative_work&) {
          return seastar::make_ready_future<runtime::result<void>>(
            runtime::result<void>{});
      },
      budget,
      work);
    if (published.publication.failure.failed()) {
        const auto failed = published.publication.failure.outcome();
        co_return runtime::failure(failed.error());
    }
    if (!published.reference)
        co_return runtime::failure(detail::path_error(errc::io_failure));
    co_return *published.reference;
}

// What a check of a whole index against its data found: nothing, or why the
// index is not the index of that data. Such an index is owed.
using sparse_index_verdict = std::optional<runtime::operation_error>;

// Walks a sealed segment's data from its start to its sealed end and checks
// the published index against every block: the walk verifies each object and
// the extent's digest, the index must be bound to exactly that extent, and
// every anchor must name a block. Pages are read one at a time and the pin
// stays alive until this completes. Nothing is changed.
//
// The verdict is about the index alone. A failure of the call is not one:
// the data could not be walked to its sealed end, or a page could not be
// read for a reason that is not the page's, and nothing was learned of the
// index.
template<runtime::file_system_backend Backend, local_directory_owner Owner>
seastar::future<runtime::result<sparse_index_verdict>> scrub_published_index(
  Backend& files,
  Owner& ownership,
  const local_device_spec& spec,
  std::uint32_t shard,
  segment_history_context history,
  const local_root_pin& index,
  workload_budget& budget,
  segment_scan_limits limits,
  codec::cooperative_work& work) {
    const auto* root = std::get_if<sparse_index_root>(&index.metadata());
    if (
      !root || root->context().segment() != history.segment
      || root->context().alignment() != history.alignment
      || root->context().profile() != history.profile)
        co_return runtime::failure(detail::path_error(errc::wrong_context));
    sparse_index_scrub scrub{root->context()};
    std::optional<local_index_page> page;
    std::uint32_t next = 0;
    // Why the index is wrong. The walk stops there: what it then reports of
    // itself is that it was stopped.
    sparse_index_verdict wrong;
    auto visit = [&](this auto, const segment_scanned_object& object)
      -> seastar::future<runtime::result<bool>> {
        if (!object.block) co_return true;
        const auto described = object.block->descriptor();
        if (scrub.drained() && next < root->pages().size()) {
            page.reset();
            auto read = co_await index.read_index(
              page_ordinal::make(next).value(), work);
            if (!read) {
                if (!detail::rebuildable_local_index_error(read.error().code()))
                    co_return runtime::failure(read.error());
                wrong = read.error();
                co_return false;
            }
            page.emplace(std::move(*read));
            const auto anchors = page->value.entries();
            if (
              anchors.front().logical_anchor() != root->first_anchors()[next]) {
                wrong = detail::path_error(errc::malformed_data);
                co_return false;
            }
            ++next;
            scrub.supply(anchors);
        }
        if (auto valid = scrub.block(described); !valid) {
            wrong = valid.error();
            co_return false;
        }
        co_return true;
    };
    auto extent = co_await verify_local_segment_extent(
      files,
      ownership,
      spec,
      shard,
      history,
      root->context().coverage().bytes().end(),
      budget,
      limits,
      work,
      std::move(visit));
    if (wrong) co_return wrong;
    if (!extent) co_return runtime::failure(extent.error());
    if (auto bound = validate_sparse_index_extent(*root, *extent); !bound)
        co_return sparse_index_verdict{
          detail::path_error(bound.error().code())};
    if (next != root->pages().size())
        co_return sparse_index_verdict{
          detail::path_error(errc::malformed_data)};
    if (auto left = scrub.finish(root->entry_count()); !left)
        co_return sparse_index_verdict{left.error()};
    co_return sparse_index_verdict{};
}

// Builds a sealed segment's index again from its data: one walk from the data
// start to the sealed end that verifies every object and hashes the extent,
// each block offered to a fresh index in file order. `expected` is what the
// segment's publication sealed. The walk must find exactly that coverage and
// digest, so a missing or changed data file is an error and never a new
// index over other bytes. Nothing is written, truncated or renamed. The
// index returned is frozen; it answers lookups at once and is published
// whenever its segment's pointer can name it. A walk that had to pass a
// block over for want of memory builds nothing: the index it would return is
// published for good, and the next request has the memory or is refused.
template<runtime::file_system_backend Backend, local_directory_owner Owner>
seastar::future<runtime::result<active_sparse_index>> rebuild_sparse_index(
  Backend& files,
  Owner& ownership,
  const local_device_spec& spec,
  std::uint32_t shard,
  segment_history_context history,
  sparse_index_context expected,
  sparse_index_stride stride,
  workload_budget& budget,
  segment_scan_limits limits,
  codec::cooperative_work& work) {
    const auto covered = expected.coverage();
    if (
      covered.physical().empty() || expected.segment() != history.segment
      || expected.alignment() != history.alignment
      || expected.profile() != history.profile)
        co_return runtime::failure(detail::path_error(errc::invalid_argument));
    const auto end = covered.bytes().end();
    auto made = active_sparse_index::make(
      stride, sparse_index_capacity(stride, expected), budget);
    if (!made) co_return runtime::failure(made.error());
    auto index = std::move(*made);
    auto extent = co_await verify_local_segment_extent(
      files,
      ownership,
      spec,
      shard,
      history,
      end,
      budget,
      limits,
      work,
      [&index](const segment_scanned_object& object) {
          if (object.block)
              index.written(object.block->descriptor().coverage());
          return seastar::make_ready_future<runtime::result<bool>>(true);
      });
    if (!extent) co_return runtime::failure(extent.error());
    if (
      extent->boundary().coverage != covered || !extent->digest()
      || *extent->digest() != expected.digest())
        co_return runtime::failure(detail::path_error(errc::corrupt_data));
    if (index.skipped() != 0)
        co_return runtime::failure(
          detail::path_error(errc::resource_exhausted));
    index.durable(end);
    index.freeze();
    co_return std::move(index);
}

namespace detail {
// The failure an exception out of index work stands for: memory that could
// not be had is not a device error.
[[nodiscard]] runtime::operation_error
  thrown_index_error(std::exception_ptr) noexcept;
} // namespace detail

// How many index rebuilds one shard runs at once, at most
// maximum_sparse_index_rebuilds whatever is asked. Every segment's rebuilder
// on the shard shares one, which outlives them. With none allowed, every
// rebuild is refused.
inline constexpr std::uint32_t default_sparse_index_rebuilds = 2;
inline constexpr std::uint32_t maximum_sparse_index_rebuilds = 8;
class sparse_index_rebuild_limit final {
public:
    explicit sparse_index_rebuild_limit(
      std::uint32_t most = default_sparse_index_rebuilds) noexcept
      : most_(std::min(most, maximum_sparse_index_rebuilds)) {}
    sparse_index_rebuild_limit(const sparse_index_rebuild_limit&) = delete;
    sparse_index_rebuild_limit&
    operator=(const sparse_index_rebuild_limit&) = delete;
    ~sparse_index_rebuild_limit();
    [[nodiscard]] std::uint32_t running() const noexcept { return running_; }
    [[nodiscard]] std::uint32_t most() const noexcept { return most_; }

private:
    friend class sparse_index_rebuilder;
    std::uint32_t most_;
    std::uint32_t running_{0};
};

// One segment's rebuild. At most one runs; whoever asks while it runs waits
// for the same outcome. This owner holds the work, not the caller that
// started it, so a caller that stops waiting leaves alone: the rebuild goes
// on for the others, and only stop() ends it. A rebuild that fails tells
// everyone waiting then and is forgotten, so the next request starts
// another. At the shard's limit, or with every waiter's place taken, a
// request is refused; it is never run beside the limit.
//
// The work starts in the scheduling group of the caller that started it.
// Work that belongs to another group moves there itself.
class sparse_index_rebuilder final {
public:
    // The most callers that wait for one rebuild. A caller that left still
    // counts until that rebuild ends.
    static constexpr std::uint32_t maximum_waiters = 16;

    explicit sparse_index_rebuilder(sparse_index_rebuild_limit& shard) noexcept
      : shard_(shard) {}
    sparse_index_rebuilder(const sparse_index_rebuilder&) = delete;
    sparse_index_rebuilder& operator=(const sparse_index_rebuilder&) = delete;
    ~sparse_index_rebuilder();

    [[nodiscard]] bool running() const noexcept { return flight_.has_value(); }
    [[nodiscard]] std::uint32_t waiters() const noexcept {
        return flight_ ? flight_->waiters : 0;
    }

    // Waits for the rebuild in flight, or starts `work` when none runs and
    // waits for that. `work(stop)` returns the rebuild's outcome and ends
    // early when `stop` is requested, which only stop() does. `caller`
    // releases this caller alone, with `aborted`; a caller already released
    // on entry starts nothing.
    template<typename Work>
    requires std::is_invocable_r_v<
      seastar::future<runtime::result<void>>,
      Work&,
      seastar::abort_source&>
    seastar::future<runtime::result<void>>
    run(Work work, seastar::abort_source& caller);

    // The owner's alone, and called once: asks a rebuild in flight to end,
    // joins it, and starts no other. Its waiters receive whatever the work
    // then returns.
    seastar::future<> stop() noexcept;

private:
    struct flight final {
        seastar::shared_promise<runtime::result<void>> done;
        std::uint32_t waiters{0};
    };
    template<typename Work>
    seastar::future<> drive(Work work);

    sparse_index_rebuild_limit& shard_;
    std::optional<flight> flight_;
    seastar::abort_source stop_;
    seastar::future<> running_{seastar::make_ready_future<>()};
    bool stopped_{false};
};

template<typename Work>
requires std::is_invocable_r_v<
  seastar::future<runtime::result<void>>,
  Work&,
  seastar::abort_source&>
seastar::future<runtime::result<void>>
sparse_index_rebuilder::run(Work work, seastar::abort_source& caller) {
    if (caller.abort_requested())
        co_return runtime::failure(detail::path_error(errc::aborted));
    if (stopped_) co_return runtime::failure(detail::path_error(errc::closed));
    const bool starts = !flight_;
    if (starts) {
        if (shard_.running_ >= shard_.most_)
            co_return runtime::failure(detail::path_error(errc::queue_full));
        // The shard's place is given back if the flight cannot be made.
        ++shard_.running_;
        try {
            flight_.emplace();
        } catch (...) {
            --shard_.running_;
            co_return runtime::failure(
              detail::thrown_index_error(std::current_exception()));
        }
    } else if (flight_->waiters == maximum_waiters)
        co_return runtime::failure(detail::path_error(errc::queue_full));
    ++flight_->waiters;
    // Taken before the work starts: work that finishes at once has already
    // told everyone who was waiting.
    auto joined = flight_->done.get_shared_future(caller);
    if (starts) running_ = drive(std::move(work));
    auto waited = co_await seastar::coroutine::as_future(std::move(joined));
    if (waited.failed()) {
        // Only this caller's release ends the wait any other way.
        waited.ignore_ready_future();
        co_return runtime::failure(detail::path_error(errc::aborted));
    }
    co_return waited.get();
}

template<typename Work>
seastar::future<> sparse_index_rebuilder::drive(Work work) {
    // Work reports failure in its outcome. Work that throws is a failed
    // rebuild like any other, and its waiters are still told.
    runtime::result<void> outcome{};
    auto worked = co_await seastar::coroutine::as_future(
      seastar::futurize_invoke(work, stop_));
    if (worked.failed())
        outcome = runtime::failure(
          detail::thrown_index_error(worked.get_exception()));
    else
        outcome = worked.get();
    auto done = std::move(flight_->done);
    flight_.reset();
    --shard_.running_;
    done.set_value(std::move(outcome));
}

// What a seal is given to publish its segment's index beside its retry
// summary: the index, and an object sequence reserved for the bundle. A live
// segment's writer fed the index; a recovered seal's own walk of the extent
// feeds it here, block by block. The seal asks once its sealed root is
// durable, which is when every block of the sealed extent is, and the index
// is frozen then.
//
// The index is borrowed until the seal it was given to is joined, which its
// segment's close does even when the seal's caller stopped waiting. One
// attempt of a recovered seal uses up the index and the sequence it was
// given, whatever its outcome: its walk has offered blocks and may have
// written a bundle, so another attempt is given an index nothing was offered
// to and a sequence nothing was given.
struct sparse_index_seal final {
    active_sparse_index* index;
    local_object_sequence object;

    void block(const complete_block_descriptor& described) noexcept {
        index->written(described.coverage());
    }

    template<runtime::file_system_backend Backend, local_directory_owner Owner>
    seastar::future<runtime::result<local_root_reference>> publish(
      Backend& files,
      Owner& ownership,
      const local_device_spec& spec,
      std::uint32_t shard,
      const sparse_index_context& context,
      workload_budget& budget,
      const local_store_io_limits& limits,
      codec::cooperative_work& work) {
        if (!index->frozen()) {
            index->durable(context.coverage().bytes().end());
            index->freeze();
        }
        return publish_sparse_index(
          files,
          ownership,
          spec,
          shard,
          *index,
          context,
          object,
          budget,
          limits,
          work);
    }
};

} // namespace kwaque::storage
