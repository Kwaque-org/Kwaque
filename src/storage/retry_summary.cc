#include "src/storage/retry_summary.h"

#include "src/base/invariant.h"
#include "src/storage/local_metadata.h"
#include "src/storage/local_paths.h"
#include "src/storage/page_ref.h"

#include <seastar/core/coroutine.hh>

#include <algorithm>
#include <array>

namespace kwaque::storage {

std::optional<completed_retry> retry_ring::fact(
  segment_context segment,
  const retry_prefix& prefix,
  std::uint32_t i) const noexcept {
    const auto& value = at(i);
    const auto id = model::batch_id::make(
      prefix.producer, prefix.epoch, prefix.stream, value.sequence);
    const auto binding = model::producer_stream_binding::make(
      segment.topic(),
      segment.range(),
      routing_,
      segment.segment(),
      segment.generation());
    if (!id || !binding) return std::nullopt;
    auto made = completed_retry::make(
      *id, value.digest, *binding, value.span, segment.generation());
    if (!made) return std::nullopt;
    return *made;
}

runtime::result<retry_summary_builder> retry_summary_builder::make(
  segment_context segment, std::uint8_t depth, workload_budget& budget) {
    if (depth == 0 || depth > maximum_retry_depth)
        return runtime::failure(detail::path_error(errc::invalid_argument));
    return retry_summary_builder{segment, depth, budget};
}

std::uint32_t retry_summary_builder::prefix_limit(std::uint8_t depth) noexcept {
    return maximum_object_entries / std::max<std::uint32_t>(depth, 1);
}

runtime::result<retry_ring*> retry_summary_builder::admit(
  const model::batch_id& id, model::range_routing_epoch routing) {
    const auto prefix = retry_prefix::of(id);
    if (const auto found = rings_.find(prefix); found != rings_.end()) {
        auto& ring = *found->second;
        if (ring.routing_ != routing)
            return runtime::failure(detail::path_error(errc::wrong_context));
        if (ring.frozen_ && id.sequence() <= *ring.frozen_)
            return runtime::failure(detail::path_error(errc::invalid_argument));
        return &ring;
    }
    return open(prefix, routing);
}

runtime::result<retry_ring*> retry_summary_builder::open(
  const retry_prefix& prefix, model::range_routing_epoch routing) {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-RETRY-SUMMARY-OPENING"},
      !opening_,
      "a prefix was made while another was neither kept nor dropped");
    if (rings_.size() >= prefix_limit(depth_))
        return static_cast<retry_ring*>(nullptr);
    // The ring and its entries are allocations of their own. The map keeps
    // its values and buckets in chunks that grow geometrically, so a prefix
    // pays twice its value and four buckets, and the first one also the
    // lists of those chunks: a map at the prefix limit has at most 32 of
    // each.
    using chunk_list = std::vector<std::vector<ring_map::value_type>>;
    const std::array<std::size_t, 5> requests{
      sizeof(retry_ring),
      std::size_t{depth_} * sizeof(retry_ring::entry),
      2 * sizeof(ring_map::value_type),
      4 * sizeof(ring_map::bucket_type),
      rings_.empty() ? 2 * 64 * sizeof(chunk_list::value_type) : 0};
    byte_count cost{};
    for (const auto request : requests) {
        if (request == 0) continue;
        auto charge = budget_->allocation_charge(byte_count{request});
        if (!charge) return runtime::failure(charge.error());
        const auto sum = cost.checked_add(*charge);
        if (!sum)
            return runtime::failure(detail::path_error(errc::out_of_range));
        cost = *sum;
    }
    auto taken = budget_->try_reserve(cost);
    if (!taken) return runtime::failure(taken.error());
    std::vector<retry_ring::entry> entries;
    entries.reserve(depth_);
    auto ring = ring_ptr(new retry_ring(routing, std::move(entries), depth_));
    auto* made = ring.get();
    rings_.try_emplace(prefix, std::move(ring));
    opening_.emplace(opening{prefix, std::move(*taken)});
    return made;
}

void retry_summary_builder::keep() noexcept {
    if (!opening_) return;
    if (held_) {
        const auto adopted = held_->adopt(std::move(opening_->held));
        KWAQUE_INVARIANT(
          invariant_id{"KQ-RETRY-SUMMARY-GRANT"},
          adopted.has_value(),
          "a prefix's grant was not adopted");
    } else {
        held_.emplace(std::move(opening_->held));
    }
    opening_.reset();
}

void retry_summary_builder::drop() noexcept {
    if (!opening_) return;
    rings_.erase(opening_->prefix);
    // An empty map gives back its buckets and chunk lists too.
    if (rings_.empty()) rings_ = ring_map{};
    opening_.reset();
}

void retry_summary_builder::froze(
  retry_ring& ring, model::batch_sequence sequence, bool entry) const noexcept {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-RETRY-SUMMARY-FROZEN"},
      (!ring.frozen_ || *ring.frozen_ < sequence)
        && (!entry || ring.claimed_ < depth_),
      "a batch froze out of its prefix's order or past its depth");
    ring.frozen_ = sequence;
    if (entry) ++ring.claimed_;
}

void retry_summary_builder::add(
  retry_ring& ring,
  model::batch_sequence sequence,
  codec::semantic_batch_digest digest,
  model::range_logical_span span) noexcept {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-RETRY-SUMMARY-ORDER"},
      ring.size_ == 0 || ring.newest().sequence < sequence,
      "a batch did not follow its prefix's newest");
    const retry_ring::entry value{sequence, digest, span};
    if (ring.size_ < ring.depth_) {
        // Within the capacity reserved when the ring was made.
        ring.entries_.push_back(value);
        ++ring.size_;
        ++facts_;
        return;
    }
    ring.entries_[ring.head_] = value;
    ring.head_ = (ring.head_ + 1) % ring.depth_;
}

runtime::result<bool>
retry_summary_builder::derive(const assigned_batch_info& batch) {
    const auto submitted = batch.context.submitted();
    const auto binding = submitted.binding();
    if (
      binding.topic() != segment_.topic() || binding.range() != segment_.range()
      || binding.segment() != segment_.segment()
      || binding.generation() != segment_.generation())
        return runtime::failure(detail::path_error(errc::malformed_data));
    const auto id = submitted.id();
    const auto prefix = retry_prefix::of(id);
    retry_ring* ring = nullptr;
    if (const auto found = rings_.find(prefix); found != rings_.end()) {
        ring = found->second.get();
    } else {
        auto made = open(prefix, binding.routing_epoch());
        if (!made) return runtime::failure(made.error());
        if (!*made) return false;
        keep();
        ring = *made;
    }
    if (
      ring->routing_ != binding.routing_epoch()
      || (ring->size_ != 0 && !(ring->newest().sequence < id.sequence())))
        return runtime::failure(detail::path_error(errc::malformed_data));
    add(*ring, id.sequence(), batch.fingerprint, batch.context.logical_span());
    return true;
}

std::optional<completed_retry>
retry_summary_builder::find(const model::batch_id& id) const {
    const auto prefix = retry_prefix::of(id);
    const auto found = rings_.find(prefix);
    if (found == rings_.end()) return std::nullopt;
    const auto& ring = *found->second;
    for (std::uint32_t i = 0; i != ring.size_; ++i)
        if (ring.at(i).sequence == id.sequence())
            return ring.fact(segment_, prefix, i);
    return std::nullopt;
}

std::optional<completed_retry>
retry_summary_builder::last(const retry_prefix& prefix) const {
    const auto found = rings_.find(prefix);
    if (found == rings_.end() || found->second->size_ == 0) return std::nullopt;
    return found->second->fact(segment_, prefix, found->second->size_ - 1);
}

seastar::future<runtime::result<retry_summary_source>>
retry_summary_builder::seal(
  retry_summary_builder builder, codec::cooperative_work& work) {
    auto sorted = std::move(builder.rings_).extract();
    builder.rings_ = ring_map{};
    const auto less = [](const auto& left, const auto& right) noexcept {
        return left.first.canonical_less(right.first);
    };
    // A heap sort in place: each push or pop is one bounded step.
    for (std::size_t i = 2; i <= sorted.size(); ++i) {
        auto admitted = co_await work.admit(byte_count{}, item_count{1});
        if (!admitted)
            co_return runtime::failure(
              detail::path_error(admitted.error().code()));
        std::push_heap(
          sorted.begin(),
          sorted.begin() + static_cast<std::ptrdiff_t>(i),
          less);
    }
    for (std::size_t i = sorted.size(); i > 1; --i) {
        auto admitted = co_await work.admit(byte_count{}, item_count{1});
        if (!admitted)
            co_return runtime::failure(
              detail::path_error(admitted.error().code()));
        std::pop_heap(
          sorted.begin(),
          sorted.begin() + static_cast<std::ptrdiff_t>(i),
          less);
    }
    co_return retry_summary_source{
      builder.segment_,
      std::move(builder.held_),
      std::move(sorted),
      builder.facts_};
}

seastar::future<runtime::result<std::vector<completed_retry>>>
retry_summary_source::read(
  std::uint32_t first, std::uint32_t count, codec::cooperative_work&) {
    using output = runtime::result<std::vector<completed_retry>>;
    if (first > facts_ || count > facts_ - first)
        return seastar::make_ready_future<output>(
          runtime::failure(detail::path_error(errc::out_of_range)));
    try {
        // A seal reads the summary twice in order; anything before where the
        // last read stopped starts again from the front.
        if (first < next_) {
            prefix_ = 0;
            offset_ = 0;
            next_ = 0;
        }
        std::vector<completed_retry> facts;
        facts.reserve(count);
        while (facts.size() != count) {
            const auto& [prefix, ring] = sorted_[prefix_];
            if (offset_ == ring->size()) {
                ++prefix_;
                offset_ = 0;
                continue;
            }
            if (next_ < first) {
                const auto skipped = std::min(
                  first - next_, ring->size() - offset_);
                offset_ += skipped;
                next_ += skipped;
                continue;
            }
            auto fact = ring->fact(segment_, prefix, offset_);
            if (!fact)
                return seastar::make_ready_future<output>(
                  runtime::failure(detail::path_error(errc::malformed_data)));
            facts.push_back(*fact);
            ++offset_;
            ++next_;
        }
        return seastar::make_ready_future<output>(std::move(facts));
    } catch (...) {
        return seastar::current_exception_as_future<output>();
    }
}

} // namespace kwaque::storage
