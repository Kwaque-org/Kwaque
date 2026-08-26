#include "src/bytes/fragmented_buffer_builder.h"

#include "src/base/error.h"

#include <algorithm>
#include <cstring>
#include <stdexcept>
#include <utility>

namespace kwaque::bytes {

result<void> fragmented_buffer_builder_config::validate() const noexcept {
    if (
      initial_fragment_bytes.value() == 0 || max_total_bytes.value() == 0
      || max_fragments == 0 || max_fragments > max_buffer_fragments
      || max_fragment_bytes < initial_fragment_bytes
      // A fragment cannot exceed the whole buffer, and the whole buffer stays
      // inside the range where size arithmetic and allocation rounding are
      // exact. Without this an enormous ceiling makes the rounding wrap and
      // report capacity that was never allocated.
      || max_fragment_bytes > max_total_bytes
      || max_total_bytes > max_builder_total_bytes) {
        return failure(errc::invalid_argument);
    }
    return {};
}

fragmented_buffer_builder::fragmented_buffer_builder()
  : fragmented_buffer_builder(fragmented_buffer_builder_config{}) {}

fragmented_buffer_builder::fragmented_buffer_builder(
  fragmented_buffer_builder_config config)
  : config_(config) {
    if (auto valid = config_.validate(); !valid) {
        throw std::invalid_argument("invalid fragmented buffer builder config");
    }
}

fragmented_buffer_builder::fragmented_buffer_builder(
  fragmented_buffer_builder&& other) noexcept
  : config_(other.config_)
  , fragments_(std::move(other.fragments_))
  , size_(std::exchange(other.size_, byte_count{}))
  , tail_used_(std::exchange(other.tail_used_, 0))
  , last_allocation_(std::exchange(other.last_allocation_, 0))
  , finished_(std::exchange(other.finished_, true)) {
    other.fragments_.clear();
}

fragmented_buffer_builder& fragmented_buffer_builder::operator=(
  fragmented_buffer_builder&& other) noexcept {
    if (this != &other) {
        config_ = other.config_;
        fragments_ = std::move(other.fragments_);
        other.fragments_.clear();
        size_ = std::exchange(other.size_, byte_count{});
        tail_used_ = std::exchange(other.tail_used_, 0);
        last_allocation_ = std::exchange(other.last_allocation_, 0);
        finished_ = std::exchange(other.finished_, true);
    }
    return *this;
}

byte_count fragmented_buffer_builder::tail_capacity() const noexcept {
    if (fragments_.empty()) {
        return byte_count{};
    }
    const auto tail_size = static_cast<std::uint64_t>(fragments_.back().size());
    return byte_count{tail_size - tail_used_};
}

std::uint64_t fragmented_buffer_builder::next_allocation(
  std::uint64_t requested) const noexcept {
    const auto ceiling = config_.max_fragment_bytes.value();
    std::uint64_t candidate = config_.initial_fragment_bytes.value();
    if (last_allocation_ != 0) {
        // Half again rather than double. The tail is trimmed at publication but
        // trimming does not release the allocation, so a gentler ramp leaves
        // less permanently held slack behind the final fragment.
        candidate = last_allocation_ > ceiling - last_allocation_ / 2
                      ? ceiling
                      : last_allocation_ + last_allocation_ / 2;
    }
    candidate = std::min(candidate, ceiling);
    // Never allocate less than the caller needs contiguously, and never more
    // than the fragment ceiling: a longer append continues in the next
    // fragment instead.
    const auto target = std::min(std::max(candidate, requested), ceiling);
    // Ask for what the allocator would serve anyway so tracked capacity and
    // served capacity agree. Honouring the ceiling matters more than filling a
    // size class, so a rounded size that would exceed it falls back.
    const auto served = served_allocation_size(target);
    return served <= ceiling ? served : target;
}

result<void>
fragmented_buffer_builder::ensure_appendable(byte_count incoming) noexcept {
    if (finished_) {
        return failure(errc::closed);
    }
    const auto next = size_.checked_add(incoming);
    if (!next || *next > config_.max_total_bytes) {
        return failure(errc::resource_exhausted);
    }
    return {};
}

void fragmented_buffer_builder::seal_tail() noexcept {
    if (fragments_.empty()) {
        return;
    }
    auto& tail = fragments_.back();
    if (tail_used_ < tail.size()) {
        tail.trim(static_cast<std::size_t>(tail_used_));
    }
    tail_used_ = tail.size();
}

result<void> fragmented_buffer_builder::grow_tail(std::uint64_t requested) {
    if (fragments_.size() == config_.max_fragments) {
        return failure(errc::resource_exhausted);
    }
    grow_tail_unchecked(requested);
    return {};
}

void fragmented_buffer_builder::grow_tail_unchecked(std::uint64_t requested) {
    seal_tail();
    const auto allocation = next_allocation(requested);
    fragments_.emplace_back(static_cast<std::size_t>(allocation));
    tail_used_ = 0;
    last_allocation_ = allocation;
}

fragmented_buffer_builder::rollback_point
fragmented_buffer_builder::mark() const noexcept {
    return rollback_point{
      .fragments = fragments_.size(),
      .tail_used = tail_used_,
      .last_allocation = last_allocation_};
}

void fragmented_buffer_builder::rewind(const rollback_point& point) noexcept {
    fragments_.resize(point.fragments);
    tail_used_ = fragments_.empty()
                   ? 0
                   : std::min(
                       point.tail_used,
                       static_cast<std::uint64_t>(fragments_.back().size()));
    last_allocation_ = point.last_allocation;
}

result<void> fragmented_buffer_builder::append(std::span<const char> bytes) {
    if (
      auto appendable = ensure_appendable(
        byte_count{static_cast<std::uint64_t>(bytes.size())});
      !appendable) {
        return appendable;
    }
    if (bytes.empty()) {
        return {};
    }

    const auto point = mark();
    std::uint64_t written = 0;
    const auto total = static_cast<std::uint64_t>(bytes.size());
    while (written < total) {
        if (tail_capacity().value() == 0) {
            if (auto grown = grow_tail(total - written); !grown) {
                rewind(point);
                return grown;
            }
        }
        const auto span = std::min(tail_capacity().value(), total - written);
        auto& tail = fragments_.back();
        std::memcpy(
          tail.get_write() + tail_used_,
          bytes.data() + written,
          static_cast<std::size_t>(span));
        tail_used_ += span;
        written += span;
    }
    size_ = *size_.checked_add(byte_count{written});
    return {};
}

result<void>
fragmented_buffer_builder::append_fragment(fragment_type fragment) {
    const auto length = static_cast<std::uint64_t>(fragment.size());
    if (auto appendable = ensure_appendable(byte_count{length}); !appendable) {
        return appendable;
    }
    if (length == 0) {
        return {};
    }
    if (!fragmented_buffer::has_lifetime_owner(fragment)) {
        return failure(errc::invalid_argument);
    }

    const auto packable = length <= pack_copy_threshold.value()
                          && length <= config_.max_fragment_bytes.value();
    if (
      (!packable || tail_capacity().value() < length)
      && fragments_.size() == config_.max_fragments) {
        return failure(errc::resource_exhausted);
    }

    append_prevalidated_fragment(std::move(fragment));
    return {};
}

void fragmented_buffer_builder::append_prevalidated_fragment(
  fragment_type fragment) {
    const auto length = static_cast<std::uint64_t>(fragment.size());
    if (
      length <= pack_copy_threshold.value()
      && length <= config_.max_fragment_bytes.value()) {
        if (tail_capacity().value() < length) {
            grow_tail_unchecked(length);
        }
        auto& tail = fragments_.back();
        std::memcpy(
          tail.get_write() + tail_used_,
          fragment.get(),
          static_cast<std::size_t>(length));
        tail_used_ += length;
    } else {
        seal_tail();
        fragments_.push_back(std::move(fragment));
        tail_used_ = fragments_.back().size();
    }

    // The caller has already checked the complete incoming byte count. Keep
    // accounting in lockstep with each committed fragment so the builder stays
    // internally consistent if a later allocation throws.
    size_ = byte_count{size_.value() + length};
}

result<void>
fragmented_buffer_builder::append_buffer(fragmented_buffer&& other) {
    if (auto appendable = ensure_appendable(other.size()); !appendable) {
        return appendable;
    }
    // Validated before anything is moved: without packing each donated fragment
    // needs its own link, so this is the worst case. Passing it means no
    // per-fragment append below can fail and leave a partial splice.
    if (fragments_.size() + other.fragment_count() > config_.max_fragments) {
        return failure(errc::resource_exhausted);
    }
    auto donated = std::move(other.fragments_);
    other.size_ = byte_count{};
    for (auto& fragment : donated) {
        append_prevalidated_fragment(std::move(fragment));
    }
    return {};
}

result<void> fragmented_buffer_builder::reserve(byte_count bytes) {
    if (finished_) {
        return failure(errc::closed);
    }
    if (bytes.value() == 0) {
        return {};
    }
    if (bytes > config_.max_fragment_bytes) {
        return failure(errc::out_of_range);
    }
    if (auto appendable = ensure_appendable(bytes); !appendable) {
        return appendable;
    }
    if (tail_capacity() >= bytes) {
        return {};
    }
    return grow_tail(bytes.value());
}

result<fragmented_buffer> fragmented_buffer_builder::finish() {
    if (finished_) {
        return failure(errc::closed);
    }
    seal_tail();
    finished_ = true;
    auto fragments = std::move(fragments_);
    const auto size = size_;
    fragments_.clear();
    size_ = byte_count{};
    tail_used_ = 0;
    last_allocation_ = 0;

    fragmented_buffer published{std::move(fragments), size};
    published.drop_empty_fragments();
    if (
      auto actual = fragmented_buffer::total_size(published.fragments_);
      !actual || *actual != size) {
        return failure(errc::malformed_data);
    }
    if (published.fragment_count() > max_buffer_fragments) {
        return failure(errc::resource_exhausted);
    }
    return published;
}

} // namespace kwaque::bytes
