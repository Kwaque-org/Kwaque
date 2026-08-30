#include "src/bytes/fragmented_buffer.h"

#include "src/base/error.h"

#include <algorithm>
#include <bit>
#include <cstring>
#include <limits>
#include <ranges>
#include <utility>

namespace kwaque::bytes {

namespace {

[[nodiscard]] bool
add_would_overflow(std::uint64_t left, std::uint64_t right) noexcept {
    return right > std::numeric_limits<std::uint64_t>::max() - left;
}

// Deep copies use exact small allocations, then lower-bound powers of two
// capped at the process-wide contiguous-allocation ceiling.
[[nodiscard]] constexpr std::uint64_t
deep_copy_allocation_size(std::uint64_t remaining) noexcept {
    constexpr std::uint64_t max_small_allocation = 16UL * 1024UL;
    constexpr std::uint64_t max_chunk_size
      = maximum_contiguous_allocation_bytes;
    if (remaining <= max_small_allocation) {
        return remaining;
    }
    return std::min(std::bit_floor(remaining), max_chunk_size);
}

} // namespace

fragmented_buffer::fragmented_buffer(
  std::deque<owned_fragment> fragments,
  byte_count size,
  byte_count retained_bytes) noexcept
  : fragments_(std::move(fragments))
  , size_(size)
  , retained_bytes_(retained_bytes) {}

fragmented_buffer::fragmented_buffer(fragmented_buffer&& other) noexcept
  : fragments_(std::move(other.fragments_))
  , size_(std::exchange(other.size_, byte_count{}))
  , retained_bytes_(std::exchange(other.retained_bytes_, byte_count{}))
  , generation_(std::exchange(other.generation_, 0)) {
    other.fragments_.clear();
}

fragmented_buffer&
fragmented_buffer::operator=(fragmented_buffer&& other) noexcept {
    if (this != &other) {
        fragments_ = std::move(other.fragments_);
        other.fragments_.clear();
        size_ = std::exchange(other.size_, byte_count{});
        retained_bytes_ = std::exchange(other.retained_bytes_, byte_count{});
        generation_ = std::exchange(other.generation_, 0);
    }
    return *this;
}

result<byte_count> fragmented_buffer::total_size(
  const std::deque<owned_fragment>& fragments) noexcept {
    std::uint64_t total = 0;
    for (const auto& fragment : fragments) {
        const auto fragment_size = static_cast<std::uint64_t>(
          fragment.storage.size());
        if (add_would_overflow(total, fragment_size)) {
            return failure(errc::out_of_range);
        }
        total += fragment_size;
    }
    return byte_count{total};
}

void fragmented_buffer::drop_empty_fragments() {
    for (auto current = fragments_.begin(); current != fragments_.end();) {
        if (!current->storage.empty()) {
            ++current;
            continue;
        }
        retained_bytes_ = *retained_bytes_.checked_sub(current->retained_bytes);
        current = fragments_.erase(current);
    }
}

result<fragmented_buffer>
fragmented_buffer::copy_from_fragment(const fragment_type& fragment) {
    return copy_from_fragments(std::span<const fragment_type>{&fragment, 1});
}

result<fragmented_buffer>
fragmented_buffer::copy_from_fragments(std::span<const fragment_type> source) {
    std::size_t fragment_count = 0;
    std::uint64_t total = 0;
    for (const auto& fragment : source) {
        if (fragment.empty()) {
            continue;
        }
        if (
          fragment.size() > maximum_contiguous_allocation_bytes
          || fragment_count == max_buffer_fragments) {
            return failure(errc::resource_exhausted);
        }
        ++fragment_count;
        const auto fragment_size = static_cast<std::uint64_t>(fragment.size());
        if (add_would_overflow(total, fragment_size)) {
            return failure(errc::out_of_range);
        }
        total += fragment_size;
        if (total > max_buffer_bytes.value()) {
            return failure(errc::resource_exhausted);
        }
    }

    std::deque<owned_fragment> fragments;
    for (const auto& fragment : source) {
        if (!fragment.empty()) {
            const byte_count size{static_cast<std::uint64_t>(fragment.size())};
            fragments.push_back(
              owned_fragment{
                .storage = fragment.clone(),
                .retained_bytes = size,
              });
        }
    }
    const byte_count size{total};
    return fragmented_buffer{std::move(fragments), size, size};
}

result<fragmented_buffer>
fragmented_buffer::copy_of(std::span<const char> bytes) {
    if (bytes.empty()) {
        return fragmented_buffer{};
    }
    if (bytes.size() > max_buffer_bytes.value()) {
        return failure(errc::resource_exhausted);
    }
    std::deque<owned_fragment> fragments;
    std::size_t offset = 0;
    while (offset < bytes.size()) {
        const auto size = std::min(
          maximum_contiguous_allocation_bytes, bytes.size() - offset);
        fragment_type fragment{size};
        std::memcpy(fragment.get_write(), bytes.data() + offset, size);
        fragments.push_back(
          owned_fragment{
            .storage = std::move(fragment),
            .retained_bytes = byte_count{size},
          });
        offset += size;
    }
    const byte_count size{static_cast<std::uint64_t>(bytes.size())};
    return fragmented_buffer{std::move(fragments), size, size};
}

result<fragment_view> fragmented_buffer::fragment_at(std::size_t index) const {
    if (index >= fragments_.size()) {
        return failure(errc::out_of_range);
    }
    const auto& fragment = fragments_[index];
    return fragment_view{fragment.storage.get(), fragment.storage.size()};
}

fragmented_buffer fragmented_buffer::share() {
    std::deque<owned_fragment> shared;
    for (auto& fragment : fragments_) {
        shared.push_back(
          owned_fragment{
            .storage = fragment.storage.share(),
            .retained_bytes = fragment.retained_bytes,
          });
    }
    return fragmented_buffer{std::move(shared), size_, retained_bytes_};
}

result<fragmented_buffer>
fragmented_buffer::share(byte_count offset, byte_count length) {
    const auto end = offset.checked_add(length);
    if (!end || *end > size_) {
        return failure(errc::out_of_range);
    }
    if (length.value() == 0) {
        return fragmented_buffer{};
    }

    std::deque<owned_fragment> shared;
    byte_count retained;
    std::uint64_t skip = offset.value();
    std::uint64_t remaining = length.value();
    for (auto& fragment : fragments_) {
        if (remaining == 0) {
            break;
        }
        const auto fragment_size = static_cast<std::uint64_t>(
          fragment.storage.size());
        if (skip >= fragment_size) {
            skip -= fragment_size;
            continue;
        }
        const auto available = fragment_size - skip;
        const auto take = std::min(available, remaining);
        if (shared.size() == max_buffer_fragments) {
            return failure(errc::resource_exhausted);
        }
        shared.push_back(
          owned_fragment{
            .storage = fragment.storage.share(
              static_cast<std::size_t>(skip), static_cast<std::size_t>(take)),
            .retained_bytes = fragment.retained_bytes,
          });
        retained = *retained.checked_add(fragment.retained_bytes);
        skip = 0;
        remaining -= take;
    }
    if (remaining != 0) {
        return failure(errc::out_of_range);
    }
    return fragmented_buffer{std::move(shared), length, retained};
}

fragmented_buffer fragmented_buffer::copy() const {
    if (empty()) {
        return fragmented_buffer{};
    }

    const auto minimum_chunk = size_.value() / max_buffer_fragments
                               + static_cast<std::uint64_t>(
                                 size_.value() % max_buffer_fragments != 0);
    std::deque<owned_fragment> copied;
    std::size_t source_index = 0;
    std::size_t source_offset = 0;
    std::uint64_t remaining = size_.value();
    while (remaining != 0) {
        const auto allocation = std::min(
          remaining,
          std::max(deep_copy_allocation_size(remaining), minimum_chunk));
        fragment_type storage{static_cast<std::size_t>(allocation)};
        std::size_t written = 0;
        while (written < allocation) {
            const auto& source = fragments_[source_index].storage;
            const auto available = source.size() - source_offset;
            const auto span = std::min(
              available, static_cast<std::size_t>(allocation) - written);
            std::memcpy(
              storage.get_write() + written,
              source.get() + source_offset,
              span);
            written += span;
            source_offset += span;
            if (source_offset == source.size()) {
                ++source_index;
                source_offset = 0;
            }
        }
        copied.push_back(
          owned_fragment{
            .storage = std::move(storage),
            .retained_bytes = byte_count{allocation},
          });
        remaining -= allocation;
    }
    return fragmented_buffer{std::move(copied), size_, size_};
}

result<void> fragmented_buffer::trim_front(byte_count bytes) {
    if (bytes > size_) {
        return failure(errc::out_of_range);
    }
    if (bytes.value() == 0) {
        return {};
    }

    std::uint64_t remaining = bytes.value();
    std::size_t dropped = 0;
    byte_count released;
    for (auto& fragment : fragments_) {
        if (remaining == 0) {
            break;
        }
        const auto fragment_size = static_cast<std::uint64_t>(
          fragment.storage.size());
        if (remaining >= fragment_size) {
            remaining -= fragment_size;
            released = *released.checked_add(fragment.retained_bytes);
            ++dropped;
            continue;
        }
        fragment.storage.trim_front(static_cast<std::size_t>(remaining));
        remaining = 0;
    }
    while (dropped-- != 0) {
        fragments_.pop_front();
    }
    size_ = *size_.checked_sub(bytes);
    retained_bytes_ = *retained_bytes_.checked_sub(released);
    ++generation_;
    drop_empty_fragments();
    return {};
}

result<void> fragmented_buffer::trim_back(byte_count bytes) {
    if (bytes > size_) {
        return failure(errc::out_of_range);
    }
    if (bytes.value() == 0) {
        return {};
    }

    std::uint64_t remaining = bytes.value();
    std::size_t dropped = 0;
    byte_count released;
    for (auto& fragment : std::views::reverse(fragments_)) {
        if (remaining == 0) {
            break;
        }
        const auto fragment_size = static_cast<std::uint64_t>(
          fragment.storage.size());
        if (remaining >= fragment_size) {
            remaining -= fragment_size;
            released = *released.checked_add(fragment.retained_bytes);
            ++dropped;
            continue;
        }
        fragment.storage.trim(
          static_cast<std::size_t>(fragment_size - remaining));
        remaining = 0;
    }
    while (dropped-- != 0) {
        fragments_.pop_back();
    }
    size_ = *size_.checked_sub(bytes);
    retained_bytes_ = *retained_bytes_.checked_sub(released);
    ++generation_;
    drop_empty_fragments();
    return {};
}

result<scatter_batch> fragmented_buffer::export_scatter(
  std::size_t max_vectors, byte_count max_bytes, scatter_cursor& cursor) {
    if (max_vectors == 0 || max_bytes.value() == 0) {
        return failure(errc::invalid_argument);
    }
    if (
      cursor.owner_ != nullptr
      && (cursor.owner_ != this || cursor.generation_ != generation_)) {
        return failure(errc::invalid_argument);
    }
    if (
      cursor.fragment_ > fragments_.size()
      || (cursor.fragment_ == fragments_.size() && cursor.offset_.value() != 0)) {
        return failure(errc::out_of_range);
    }

    const auto capacity = std::min(max_vectors, max_scatter_vectors);
    scatter_batch batch;
    std::uint64_t offset = cursor.offset().value();
    std::size_t index = cursor.fragment_;
    std::uint64_t budget = max_bytes.value();
    batch.ownership_.reserve(std::min(capacity, fragments_.size() - index));

    while (index < fragments_.size() && batch.ownership_.size() < capacity
           && budget > 0) {
        auto& fragment = fragments_[index];
        const auto fragment_size = static_cast<std::uint64_t>(
          fragment.storage.size());
        if (offset > fragment_size) {
            return failure(errc::out_of_range);
        }
        const auto available = fragment_size - offset;
        if (available == 0) {
            ++index;
            offset = 0;
            continue;
        }
        // A fragment wider than the remaining budget is exported in part; the
        // cursor then resumes inside it on the next batch.
        const auto span = std::min(available, budget);
        batch.ownership_.push_back(fragment.storage.share(
          static_cast<std::size_t>(offset), static_cast<std::size_t>(span)));
        // `span` is at most the remaining budget, so the accumulated count can
        // never exceed the caller's uint64_t byte cap.
        batch.bytes_ = byte_count{batch.bytes_.value() + span};
        budget -= span;
        offset += span;
        if (offset == fragment_size) {
            ++index;
            offset = 0;
        }
    }

    cursor.owner_ = this;
    cursor.generation_ = generation_;
    cursor.fragment_ = index;
    cursor.offset_ = byte_count{offset};
    batch.complete_ = index == fragments_.size();
    return batch;
}

result<fragmented_buffer::fragment_type>
fragmented_buffer::linearize(byte_count max_bytes) const {
    if (
      size_ > max_bytes
      || size_.value() > maximum_contiguous_allocation_bytes) {
        return failure(errc::resource_exhausted);
    }
    fragment_type linear{static_cast<std::size_t>(size_.value())};
    std::size_t written = 0;
    for (const auto& fragment : fragments_) {
        std::memcpy(
          linear.get_write() + written,
          fragment.storage.get(),
          fragment.storage.size());
        written += fragment.storage.size();
    }
    return linear;
}

result<byte_count>
fragmented_buffer::copy_to(std::span<char> destination) const {
    if (destination.size() < size_.value()) {
        return failure(errc::out_of_range);
    }
    std::size_t written = 0;
    for (const auto& fragment : fragments_) {
        std::memcpy(
          destination.data() + written,
          fragment.storage.get(),
          fragment.storage.size());
        written += fragment.storage.size();
    }
    return byte_count{static_cast<std::uint64_t>(written)};
}

result<seastar::net::packet> fragmented_buffer::copy_to_packet() const {
    // The transfer type records its total in an unsigned int, so a buffer wider
    // than that cannot be handed over without wrapping. Refuse rather than
    // silently truncate; the caller can split or send in batches instead.
    if (size_.value() > std::numeric_limits<unsigned>::max()) {
        return failure(errc::out_of_range);
    }
    if (empty()) {
        return seastar::net::packet{};
    }
    // Coalesce hostile tiny-fragment layouts using the bounded deep-copy
    // policy, then transfer those independent chunks directly into the packet.
    // Pre-sizing the packet avoids a descriptor vector and repeated growth.
    auto copied = copy();
    seastar::net::packet packet{copied.fragments_.size()};
    for (auto& fragment : copied.fragments_) {
        packet = seastar::net::packet(
          std::move(packet), std::move(fragment.storage));
    }
    return packet;
}

bool fragmented_buffer::content_equals(std::string_view bytes) const noexcept {
    if (bytes.size() != size_.value()) {
        return false;
    }
    std::size_t offset = 0;
    for (const auto& fragment : fragments_) {
        if (
          std::memcmp(
            bytes.data() + offset,
            fragment.storage.get(),
            fragment.storage.size())
          != 0) {
            return false;
        }
        offset += fragment.storage.size();
    }
    return true;
}

bool fragmented_buffer::content_equals(
  const fragmented_buffer& other) const noexcept {
    if (size_ != other.size_) {
        return false;
    }

    std::size_t left_index = 0;
    std::size_t right_index = 0;
    std::size_t left_offset = 0;
    std::size_t right_offset = 0;
    while (left_index < fragments_.size()
           && right_index < other.fragments_.size()) {
        const auto& left = fragments_[left_index];
        const auto& right = other.fragments_[right_index];
        const auto span = std::min(
          left.storage.size() - left_offset,
          right.storage.size() - right_offset);
        if (span == 0) {
            if (left.storage.size() == left_offset) {
                ++left_index;
                left_offset = 0;
            }
            if (right.storage.size() == right_offset) {
                ++right_index;
                right_offset = 0;
            }
            continue;
        }
        if (
          std::memcmp(
            left.storage.get() + left_offset,
            right.storage.get() + right_offset,
            span)
          != 0) {
            return false;
        }
        left_offset += span;
        right_offset += span;
    }
    return true;
}

} // namespace kwaque::bytes
