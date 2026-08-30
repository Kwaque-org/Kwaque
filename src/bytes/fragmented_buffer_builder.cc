#include "src/bytes/fragmented_buffer_builder.h"

#include "src/base/error.h"

#include <algorithm>
#include <bit>
#include <cstring>
#include <deque>
#include <stdexcept>
#include <utility>

namespace kwaque::bytes {

namespace {

[[nodiscard]] constexpr std::uint64_t
served_allocation_size(std::uint64_t size) noexcept {
    constexpr unsigned fraction_bits = 2;
    constexpr std::uint64_t fraction_mask = (std::uint64_t{1} << fraction_bits)
                                            - 1;
    constexpr std::uint64_t max_pooled = 16384;
    constexpr std::uint64_t minimum_cell = sizeof(void*);
    if (size == 0) {
        return 0;
    }
    size = std::max(size, minimum_cell);
    const auto log2_floor = static_cast<unsigned>(std::bit_width(size) - 1);
    if (size > max_pooled) {
        const auto span = std::uint64_t{1} << log2_floor;
        return span == size || log2_floor >= 63 ? size : span << 1;
    }
    const auto index = ((static_cast<std::uint64_t>(log2_floor)
                         << fraction_bits)
                        - fraction_mask)
                       + ((size - 1) >> (log2_floor - fraction_bits));
    auto served
      = (((std::uint64_t{1} << fraction_bits) | (index & fraction_mask))
         << (index >> fraction_bits))
        >> fraction_bits;
    constexpr std::uint64_t alignment = alignof(std::max_align_t);
    if (served > alignment) {
        served = (served + alignment - 1) / alignment * alignment;
    }
    return served;
}

} // namespace

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
      || max_fragment_bytes.value() > maximum_contiguous_allocation_bytes
      || max_total_bytes > max_retained_bytes
      || max_retained_bytes > max_builder_total_bytes) {
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
  , retained_bytes_(std::exchange(other.retained_bytes_, byte_count{}))
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
        retained_bytes_ = std::exchange(other.retained_bytes_, byte_count{});
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
    const auto tail_size = static_cast<std::uint64_t>(
      fragments_.back().storage.size());
    return byte_count{tail_size - tail_used_};
}

std::uint64_t fragmented_buffer_builder::next_allocation(
  std::uint64_t requested) const noexcept {
    return next_allocation(requested, last_allocation_);
}

std::uint64_t fragmented_buffer_builder::next_allocation(
  std::uint64_t requested, std::uint64_t previous_allocation) const noexcept {
    const auto ceiling = config_.max_fragment_bytes.value();
    std::uint64_t candidate = config_.initial_fragment_bytes.value();
    if (previous_allocation != 0) {
        // Half again rather than double. The tail is trimmed at publication but
        // trimming does not release the allocation, so a gentler ramp leaves
        // less permanently held slack behind the final fragment.
        candidate = previous_allocation > ceiling - previous_allocation / 2
                      ? ceiling
                      : previous_allocation + previous_allocation / 2;
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
    if (tail_used_ < tail.storage.size()) {
        tail.storage.trim(static_cast<std::size_t>(tail_used_));
    }
    tail_used_ = tail.storage.size();
}

result<void> fragmented_buffer_builder::grow_tail(std::uint64_t requested) {
    if (fragments_.size() == config_.max_fragments) {
        return failure(errc::resource_exhausted);
    }
    const auto allocation = next_allocation(requested);
    const auto retained = retained_bytes_.checked_add(byte_count{allocation});
    if (!retained || *retained > config_.max_retained_bytes) {
        return failure(errc::resource_exhausted);
    }
    grow_tail_unchecked(requested);
    return {};
}

void fragmented_buffer_builder::grow_tail_unchecked(std::uint64_t requested) {
    seal_tail();
    const auto allocation = next_allocation(requested);
    fragments_.push_back(
      fragmented_buffer::owned_fragment{
        .storage = fragment_type{static_cast<std::size_t>(allocation)},
        .retained_bytes = byte_count{allocation},
      });
    retained_bytes_ = *retained_bytes_.checked_add(byte_count{allocation});
    tail_used_ = 0;
    last_allocation_ = allocation;
}

fragmented_buffer_builder::rollback_point
fragmented_buffer_builder::mark() const noexcept {
    return rollback_point{
      .fragments = fragments_.size(),
      .tail_used = tail_used_,
      .last_allocation = last_allocation_,
      .retained_bytes = retained_bytes_};
}

void fragmented_buffer_builder::rewind(const rollback_point& point) noexcept {
    fragments_.resize(point.fragments);
    tail_used_ = fragments_.empty() ? 0
                                    : std::min(
                                        point.tail_used,
                                        static_cast<std::uint64_t>(
                                          fragments_.back().storage.size()));
    last_allocation_ = point.last_allocation;
    retained_bytes_ = point.retained_bytes;
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
          tail.storage.get_write() + tail_used_,
          bytes.data() + written,
          static_cast<std::size_t>(span));
        tail_used_ += span;
        written += span;
    }
    size_ = *size_.checked_add(byte_count{written});
    return {};
}

result<void>
fragmented_buffer_builder::append_fragment_copy(const fragment_type& fragment) {
    const auto length = static_cast<std::uint64_t>(fragment.size());
    if (auto appendable = ensure_appendable(byte_count{length}); !appendable) {
        return appendable;
    }
    if (length == 0) {
        return {};
    }
    const auto packable = length <= pack_copy_threshold.value()
                          && length <= config_.max_fragment_bytes.value();
    if (packable) {
        if (tail_capacity().value() < length) {
            if (fragments_.size() == config_.max_fragments) {
                return failure(errc::resource_exhausted);
            }
            if (auto grown = grow_tail(length); !grown) {
                return grown;
            }
        }
        auto& tail = fragments_.back().storage;
        std::memcpy(
          tail.get_write() + tail_used_,
          fragment.get(),
          static_cast<std::size_t>(length));
        tail_used_ += length;
        size_ = byte_count{size_.value() + length};
        return {};
    }
    if (fragments_.size() == config_.max_fragments) {
        return failure(errc::resource_exhausted);
    }
    const auto retained = retained_bytes_.checked_add(byte_count{length});
    if (!retained || *retained > config_.max_retained_bytes) {
        return failure(errc::resource_exhausted);
    }
    append_prevalidated_fragment(
      fragmented_buffer::owned_fragment{
        .storage = fragment.clone(),
        .retained_bytes = byte_count{length},
      });
    return {};
}

void fragmented_buffer_builder::append_prevalidated_fragment(
  fragmented_buffer::owned_fragment fragment) {
    const auto length = static_cast<std::uint64_t>(fragment.storage.size());
    if (
      length <= pack_copy_threshold.value()
      && length <= config_.max_fragment_bytes.value()) {
        if (tail_capacity().value() < length) {
            grow_tail_unchecked(length);
        }
        auto& tail = fragments_.back();
        std::memcpy(
          tail.storage.get_write() + tail_used_,
          fragment.storage.get(),
          static_cast<std::size_t>(length));
        tail_used_ += length;
    } else {
        seal_tail();
        const auto retained = *retained_bytes_.checked_add(
          fragment.retained_bytes);
        fragments_.push_back(std::move(fragment));
        retained_bytes_ = retained;
        tail_used_ = fragments_.back().storage.size();
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
    auto projected_retained = retained_bytes_;
    auto projected_tail = tail_capacity().value();
    auto projected_last_allocation = last_allocation_;
    for (const auto& fragment : other.fragments_) {
        const auto length = static_cast<std::uint64_t>(fragment.storage.size());
        if (
          length <= pack_copy_threshold.value()
          && length <= config_.max_fragment_bytes.value()) {
            if (projected_tail < length) {
                const auto allocation = next_allocation(
                  length, projected_last_allocation);
                const auto next = projected_retained.checked_add(
                  byte_count{allocation});
                if (!next || *next > config_.max_retained_bytes) {
                    return failure(errc::resource_exhausted);
                }
                projected_retained = *next;
                projected_tail = allocation;
                projected_last_allocation = allocation;
            }
            projected_tail -= length;
        } else {
            const auto next = projected_retained.checked_add(
              fragment.retained_bytes);
            if (!next || *next > config_.max_retained_bytes) {
                return failure(errc::resource_exhausted);
            }
            projected_retained = *next;
            projected_tail = 0;
        }
    }
    fragments_.reserve(fragments_.size() + other.fragment_count());
    auto donated = std::move(other.fragments_);
    other.fragments_.clear();
    other.size_ = byte_count{};
    other.retained_bytes_ = byte_count{};
    ++other.generation_;
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
    const auto retained = retained_bytes_;
    fragments_.clear();
    size_ = byte_count{};
    retained_bytes_ = byte_count{};
    tail_used_ = 0;
    last_allocation_ = 0;

    std::deque<fragmented_buffer::owned_fragment> published_fragments;
    for (auto& fragment : fragments) {
        published_fragments.push_back(std::move(fragment));
    }
    fragmented_buffer published{std::move(published_fragments), size, retained};
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
