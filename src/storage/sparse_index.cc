#include "src/storage/sparse_index.h"

#include "src/base/allocation.h"
#include "src/base/invariant.h"
#include "src/codec/envelope.h"
#include "src/storage/page_ref.h"

#include <algorithm>
#include <iterator>
#include <new>
#include <ranges>
#include <vector>

namespace kwaque::storage {
namespace {
static_assert(
  sizeof(sparse_index_entry) == sparse_index_entry_wire_bytes.value());

runtime::operation_error index_error(errc code) noexcept {
    return runtime::operation_error{code, runtime::operation_kind::resource};
}
} // namespace

namespace detail {
runtime::operation_error
thrown_index_error(std::exception_ptr thrown) noexcept {
    try {
        std::rethrow_exception(std::move(thrown));
    } catch (const std::bad_alloc&) {
        return path_error(errc::resource_exhausted);
    } catch (...) {
        return path_error(errc::io_failure);
    }
}
} // namespace detail

runtime::result<void> validate_sparse_index_pages(
  const local_store_io_limits& limits,
  storage_alignment alignment,
  const codec::limits& policy,
  std::uint32_t anchors) noexcept {
    if (auto valid = limits.validate(); !valid) return valid;
    // What the encoder refuses of a whole index, before one is fed.
    const auto pages = (std::uint64_t{anchors} + sparse_index_page_entries - 1)
                       / sparse_index_page_entries;
    if (
      anchors > policy.config().max_object_entries.value()
      || pages > policy.config().max_object_pages.value())
        return runtime::failure(index_error(errc::resource_exhausted));
    // The largest page such an index has: a full one, or all of its anchors
    // when they are fewer. An index without an anchor has no page.
    const auto page = std::min(anchors, sparse_index_page_entries);
    if (page == 0) return {};
    const auto encoded = sparse_index_page_capacity(
      byte_count{codec::envelope_prefix_bytes}, alignment, policy);
    if (!encoded) return runtime::failure(index_error(errc::invalid_argument));
    // The rule every page writer here cuts by: the decoded array takes no
    // more than half the reader's metadata and a quarter of its operation.
    const byte_count requested{
      std::uint64_t{page} * sizeof(sparse_index_entry)};
    const auto served = limits.charge(requested);
    const auto room = std::min(
      limits.metadata_bytes.value() / 2, limits.operation_bytes.value() / 4);
    if (
      *encoded < page || served < requested || served.value() > room
      || !policy.validate_allocation(served))
        return runtime::failure(index_error(errc::resource_exhausted));
    return {};
}

std::optional<sparse_index_position> find_sparse_index_anchor(
  std::span<const sparse_index_entry> anchors,
  model::range_logical_offset target,
  runtime::file_position end) noexcept {
    const auto after = std::ranges::upper_bound(
      anchors, target, {}, &sparse_index_entry::logical_anchor);
    if (after == anchors.begin()) return std::nullopt;
    return sparse_index_position{
      *std::prev(after),
      after == anchors.end() ? end : after->block_position()};
}

std::optional<std::uint32_t> find_sparse_index_page(
  std::span<const model::range_logical_offset> first_anchors,
  model::range_logical_offset target) noexcept {
    const auto after = std::ranges::upper_bound(first_anchors, target);
    if (after == first_anchors.begin()) return std::nullopt;
    return static_cast<std::uint32_t>(after - first_anchors.begin()) - 1;
}

runtime::result<std::optional<page_ordinal>> route_published_anchor(
  const local_root_pin& index, model::range_logical_offset target) {
    const auto* root = std::get_if<sparse_index_root>(&index.metadata());
    if (!root) return runtime::failure(detail::path_error(errc::wrong_context));
    const auto ordinal = find_sparse_index_page(root->first_anchors(), target);
    if (!ordinal) return std::optional<page_ordinal>{};
    return std::optional{page_ordinal::make(*ordinal).value()};
}

runtime::result<std::optional<sparse_index_position>> search_published_page(
  const local_root_pin& index,
  page_ordinal ordinal,
  const local_index_page& page,
  model::range_logical_offset target) {
    const auto* root = std::get_if<sparse_index_root>(&index.metadata());
    if (!root || ordinal.value() >= root->first_anchors().size())
        return runtime::failure(detail::path_error(errc::wrong_context));
    const auto anchors = page.value.entries();
    // The root routed here by the anchor this page begins with.
    if (
      anchors.front().logical_anchor()
      != root->first_anchors()[ordinal.value()])
        return runtime::failure(detail::path_error(errc::malformed_data));
    return find_sparse_index_anchor(
      anchors, target, root->context().coverage().bytes().end());
}

seastar::future<runtime::result<std::optional<sparse_index_position>>>
find_published_anchor(
  const local_root_pin& index,
  model::range_logical_offset target,
  codec::cooperative_work& work) {
    const auto routed = route_published_anchor(index, target);
    if (!routed) co_return runtime::failure(routed.error());
    if (!*routed) co_return std::optional<sparse_index_position>{};
    auto page = co_await index.read_index(**routed, work);
    if (!page) co_return runtime::failure(page.error());
    co_return search_published_page(index, **routed, *page, target);
}

runtime::result<void>
sparse_index_scrub::block(const complete_block_descriptor& block) noexcept {
    if (run_.empty()) return {};
    const auto& anchor = run_.front();
    const auto begin = block.coverage().bytes().begin();
    if (begin < anchor.block_position()) {
        // A block between two anchors. Before the first anchor there is
        // none that holds a record: a lookup below the first anchor answers
        // that nothing precedes it.
        if (matched_ == 0 && !block.coverage().logical().empty())
            return runtime::failure(detail::path_error(errc::malformed_data));
        return {};
    }
    // Blocks arrive in file order, so no block begins where this one points.
    if (anchor.block_position() < begin)
        return runtime::failure(detail::path_error(errc::malformed_data));
    if (
      auto named = validate_sparse_index_anchor(context_, anchor, block);
      !named)
        return runtime::failure(detail::path_error(named.error().code()));
    run_ = run_.subspan(1);
    ++matched_;
    return {};
}

runtime::result<void>
sparse_index_scrub::finish(std::uint32_t anchors) const noexcept {
    if (!run_.empty() || matched_ != anchors)
        return runtime::failure(detail::path_error(errc::malformed_data));
    return {};
}

sparse_index_rebuild_limit::~sparse_index_rebuild_limit() {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-INDEX-REBUILD-JOINED"},
      running_ == 0,
      "index rebuild limit destroyed before its rebuilds were joined");
}

sparse_index_rebuilder::~sparse_index_rebuilder() {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-INDEX-REBUILD-JOINED"},
      !flight_,
      "index rebuild owner destroyed before its rebuild was joined");
}

seastar::future<> sparse_index_rebuilder::stop() noexcept {
    stopped_ = true;
    if (!stop_.abort_requested()) stop_.request_abort();
    return std::exchange(running_, seastar::make_ready_future<>());
}

std::uint32_t sparse_index_capacity(
  sparse_index_stride stride,
  byte_count maximum_data_bytes,
  std::uint32_t maximum_blocks) noexcept {
    const auto most = std::min(maximum_blocks, maximum_object_entries);
    if (stride.records != 0 || stride.bytes.value() == 0) return most;
    const auto strides = maximum_data_bytes.value() / stride.bytes.value();
    return strides < most ? static_cast<std::uint32_t>(strides) + 1 : most;
}

std::uint32_t sparse_index_capacity(
  sparse_index_stride stride, const sparse_index_context& sealed) noexcept {
    const auto covered = sealed.coverage();
    return sparse_index_capacity(
      stride,
      byte_count{covered.bytes().end().value()},
      static_cast<std::uint32_t>(std::min<std::uint64_t>(
        maximum_object_entries, covered.physical().count().value())));
}

active_sparse_index::active_sparse_index(active_sparse_index&& other) noexcept
  : held_(std::move(other.held_))
  , chunks_(std::move(other.chunks_))
  , stride_(other.stride_)
  , capacity_(other.capacity_)
  , chosen_(std::exchange(other.chosen_, 0))
  , anchors_(std::exchange(other.anchors_, 0))
  , skipped_(other.skipped_)
  , frozen_(other.frozen_)
  , chosen_bytes_(other.chosen_bytes_)
  , offered_end_(other.offered_end_)
  , chosen_records_(other.chosen_records_) {
    // Its table went with it: it holds no anchor and takes none.
    other.capacity_ = 0;
    other.chunks_.clear();
}

segment_block_observer active_sparse_index::observer() noexcept {
    return {
      [this](std::span<const segment_block_layout> blocks) noexcept {
          for (const auto& block : blocks)
              written(block.records);
      },
      [this](runtime::file_position end) noexcept { durable(end); }};
}

runtime::result<byte_count> active_sparse_index::admission(
  std::uint32_t capacity, const workload_budget& budget) noexcept {
    static_assert(
      chunk_entries * sizeof(sparse_index_entry)
      <= maximum_contiguous_allocation_bytes / 2);
    if (capacity == 0 || capacity > maximum_object_entries)
        return runtime::failure(index_error(errc::invalid_argument));
    const auto chunks = (capacity + chunk_entries - 1) / chunk_entries;
    // The list of chunks, then every chunk at its exact size.
    const auto list = budget.allocation_charge(
      byte_count{chunks * sizeof(std::vector<sparse_index_entry>)});
    if (!list) return runtime::failure(list.error());
    auto total = *list;
    for (std::uint32_t left = capacity; left != 0;) {
        const auto entries = std::min(left, chunk_entries);
        const auto charged = budget.allocation_charge(
          byte_count{entries * sizeof(sparse_index_entry)});
        if (!charged) return runtime::failure(charged.error());
        const auto next = total.checked_add(*charged);
        if (!next) return runtime::failure(index_error(errc::out_of_range));
        total = *next;
        left -= entries;
    }
    return total;
}

runtime::result<active_sparse_index> active_sparse_index::make(
  sparse_index_stride stride, std::uint32_t capacity, workload_budget& budget) {
    const auto total = admission(capacity, budget);
    if (!total) return runtime::failure(total.error());
    auto held = budget.try_reserve(*total);
    if (!held) return runtime::failure(held.error());
    active_sparse_index output{stride, capacity, std::move(*held)};
    output.chunks_.reserve((capacity + chunk_entries - 1) / chunk_entries);
    // The first block is always an anchor, so its chunk is never waited for.
    output.chunks_.emplace_back().reserve(std::min(capacity, chunk_entries));
    return output;
}

runtime::result<void> validate_sparse_index_budget(
  const workload_budget& budget, sparse_index_demand demand) {
    const auto limits = budget.limits();
    const auto refused = [](std::uint64_t limit, std::uint64_t expected) {
        auto error = index_error(errc::resource_exhausted);
        static_cast<void>(
          error.add_context(runtime::operation_context_key::limit, limit));
        static_cast<void>(error.add_context(
          runtime::operation_context_key::expected, expected));
        return runtime::failure(error);
    };
    // Each index holds one admission for as long as its segment is written.
    const auto tasks = std::uint64_t{demand.segments} + demand.reserved_tasks;
    if (tasks > limits.tasks) return refused(limits.tasks, tasks);
    std::optional<byte_count> expected{demand.reserved_bytes};
    if (demand.segments != 0) {
        const auto table = active_sparse_index::admission(
          demand.capacity, budget);
        if (!table) return runtime::failure(table.error());
        const auto held = budget.reservation_charge(*table);
        if (!held) return runtime::failure(held.error());
        const auto indexes = held->checked_mul(demand.segments);
        expected = indexes ? indexes->checked_add(demand.reserved_bytes)
                           : std::nullopt;
    }
    if (!expected) return runtime::failure(index_error(errc::out_of_range));
    if (*expected > limits.bytes)
        return refused(limits.bytes.value(), expected->value());
    return {};
}

const sparse_index_entry&
active_sparse_index::operator[](std::uint32_t anchor) const noexcept {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-INDEX-ANCHOR"},
      anchor < anchors_,
      "index entry read past its durable anchors");
    return chosen(anchor);
}

std::optional<sparse_index_position> active_sparse_index::find(
  model::range_logical_offset target,
  runtime::file_position end) const noexcept {
    // The same search over a table kept in chunks: the first anchor after
    // the offset, then the one before it.
    const auto anchors = std::views::iota(std::uint32_t{0}, anchors_);
    const auto after = static_cast<std::uint32_t>(
      std::ranges::upper_bound(
        anchors,
        target,
        {},
        [this](std::uint32_t anchor) {
            return chosen(anchor).logical_anchor();
        })
      - anchors.begin());
    if (after == 0) return std::nullopt;
    return sparse_index_position{
      chosen(after - 1),
      after == anchors_ ? end : chosen(after).block_position()};
}

std::span<const sparse_index_entry>
active_sparse_index::page(std::uint32_t ordinal) const noexcept {
    static_assert(chunk_entries % sparse_index_page_entries == 0);
    KWAQUE_INVARIANT(
      invariant_id{"KQ-INDEX-ANCHOR"},
      ordinal < pages(),
      "index page read past its durable anchors");
    const auto first = ordinal * sparse_index_page_entries;
    return {
      chunks_[first / chunk_entries].data() + first % chunk_entries,
      std::min(sparse_index_page_entries, anchors_ - first)};
}

void active_sparse_index::written(const coverage& block) noexcept {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-INDEX-FROZEN"},
      !frozen_,
      "index offered a block after its segment was sealed");
    const auto begin = block.bytes().begin();
    KWAQUE_INVARIANT(
      invariant_id{"KQ-INDEX-ORDER"},
      offered_end_ <= begin,
      "index offered a block out of file order");
    offered_end_ = block.bytes().end();
    // An anchor is the base of a record, so a block whose original span is
    // empty has none.
    const auto base = model::range_logical_offset::make(
      block.logical().begin().value());
    if (block.logical().empty() || !base) return;
    const auto records = block.physical().begin();
    KWAQUE_INVARIANT(
      invariant_id{"KQ-INDEX-ORDER"},
      chosen_ == 0 || chosen_records_.value() <= records.value(),
      "index offered a block out of record order");
    if (
      chosen_ != 0
      && begin.value() - chosen_bytes_.value() < stride_.bytes.value()
      && (stride_.records == 0
          || records.value() - chosen_records_.value() < stride_.records))
        return;
    KWAQUE_INVARIANT(
      invariant_id{"KQ-INDEX-ORDER"},
      chosen_ == 0 || chosen(chosen_ - 1).logical_anchor() < *base,
      "index offered a block out of logical order");
    if (chosen_ == capacity_) {
        ++skipped_;
        return;
    }
    if (chosen_ / chunk_entries == chunks_.size()) {
        try {
            std::vector<sparse_index_entry> chunk;
            chunk.reserve(std::min(chunk_entries, capacity_ - chosen_));
            chunks_.push_back(std::move(chunk));
        } catch (...) {
            // Tried again at the next block: the distances still run from
            // the last block that was chosen.
            ++skipped_;
            return;
        }
    }
    chunks_.back().emplace_back(*base, begin);
    ++chosen_;
    chosen_bytes_ = begin;
    chosen_records_ = records;
}

void active_sparse_index::durable(runtime::file_position end) noexcept {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-INDEX-FROZEN"},
      !frozen_,
      "index advanced after its segment was sealed");
    while (anchors_ < chosen_ && chosen(anchors_).block_position() < end)
        ++anchors_;
}

} // namespace kwaque::storage
