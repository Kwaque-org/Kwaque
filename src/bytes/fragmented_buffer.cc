#include "src/bytes/fragmented_buffer.h"

#include "src/base/error.h"

#include <seastar/core/deleter.hh>

#include <sys/uio.h>

#include <algorithm>
#include <cstring>
#include <limits>
#include <ranges>
#include <utility>

namespace kwaque::bytes {

namespace {

thread_local buffer_counters shard_counters;

[[nodiscard]] bool
add_would_overflow(std::uint64_t left, std::uint64_t right) noexcept {
    return right > std::numeric_limits<std::uint64_t>::max() - left;
}

} // namespace

const buffer_counters& counters() noexcept { return shard_counters; }

void reset_counters() noexcept { shard_counters = buffer_counters{}; }

fragmented_buffer::fragmented_buffer(
  std::vector<fragment_type> fragments, byte_count size) noexcept
  : fragments_(std::move(fragments))
  , size_(size) {}

fragmented_buffer::fragmented_buffer(fragmented_buffer&& other) noexcept
  : fragments_(std::move(other.fragments_))
  , size_(std::exchange(other.size_, byte_count{})) {
    other.fragments_.clear();
}

fragmented_buffer&
fragmented_buffer::operator=(fragmented_buffer&& other) noexcept {
    if (this != &other) {
        fragments_ = std::move(other.fragments_);
        other.fragments_.clear();
        size_ = std::exchange(other.size_, byte_count{});
    }
    return *this;
}

result<byte_count> fragmented_buffer::total_size(
  const std::vector<fragment_type>& fragments) noexcept {
    std::uint64_t total = 0;
    for (const auto& fragment : fragments) {
        const auto fragment_size = static_cast<std::uint64_t>(fragment.size());
        if (add_would_overflow(total, fragment_size)) {
            return failure(errc::out_of_range);
        }
        total += fragment_size;
    }
    return byte_count{total};
}

bool fragmented_buffer::has_lifetime_owner(fragment_type& fragment) noexcept {
    if (fragment.empty()) {
        return true;
    }
    auto* data = fragment.get_write();
    const auto size = fragment.size();
    auto owner = fragment.release();
    const auto owned = static_cast<bool>(owner);
    fragment = fragment_type::maybe_unsafe_from_deleter(
      data, size, std::move(owner));
    return owned;
}

void fragmented_buffer::drop_empty_fragments() {
    const auto removed = std::ranges::remove_if(
      fragments_,
      [](const fragment_type& fragment) { return fragment.empty(); });
    fragments_.erase(removed.begin(), removed.end());
}

result<fragmented_buffer>
fragmented_buffer::from_fragment(fragment_type fragment) {
    if (fragment.empty()) {
        return fragmented_buffer{};
    }
    if (!has_lifetime_owner(fragment)) {
        return failure(errc::invalid_argument);
    }
    const byte_count size{static_cast<std::uint64_t>(fragment.size())};
    std::vector<fragment_type> fragments;
    fragments.reserve(1);
    fragments.push_back(std::move(fragment));
    return fragmented_buffer{std::move(fragments), size};
}

result<fragmented_buffer>
fragmented_buffer::from_fragments(std::vector<fragment_type> fragments) {
    if (fragments.size() > max_buffer_fragments) {
        return failure(errc::resource_exhausted);
    }
    for (auto& fragment : fragments) {
        if (!has_lifetime_owner(fragment)) {
            return failure(errc::invalid_argument);
        }
    }
    auto size = total_size(fragments);
    if (!size) {
        return failure(size.error());
    }
    fragmented_buffer buffer{std::move(fragments), *size};
    buffer.drop_empty_fragments();
    return buffer;
}

result<fragmented_buffer>
fragmented_buffer::copy_of(std::span<const char> bytes) {
    if (bytes.empty()) {
        return fragmented_buffer{};
    }
    fragment_type fragment{bytes.size()};
    std::memcpy(fragment.get_write(), bytes.data(), bytes.size());
    return from_fragment(std::move(fragment));
}

result<fragment_view> fragmented_buffer::fragment_at(std::size_t index) const {
    if (index >= fragments_.size()) {
        return failure(errc::out_of_range);
    }
    const auto& fragment = fragments_[index];
    return fragment_view{fragment.get(), fragment.size()};
}

result<fragmented_buffer> fragmented_buffer::share() {
    std::vector<fragment_type> shared;
    shared.reserve(fragments_.size());
    for (auto& fragment : fragments_) {
        shared.push_back(fragment.share());
    }
    return fragmented_buffer{std::move(shared), size_};
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

    std::vector<fragment_type> shared;
    std::uint64_t skip = offset.value();
    std::uint64_t remaining = length.value();
    for (auto& fragment : fragments_) {
        if (remaining == 0) {
            break;
        }
        const auto fragment_size = static_cast<std::uint64_t>(fragment.size());
        if (skip >= fragment_size) {
            skip -= fragment_size;
            continue;
        }
        const auto available = fragment_size - skip;
        const auto take = std::min(available, remaining);
        if (shared.size() == max_buffer_fragments) {
            return failure(errc::resource_exhausted);
        }
        shared.push_back(fragment.share(
          static_cast<std::size_t>(skip), static_cast<std::size_t>(take)));
        skip = 0;
        remaining -= take;
    }
    if (remaining != 0) {
        return failure(errc::out_of_range);
    }
    return fragmented_buffer{std::move(shared), length};
}

result<fragmented_buffer> fragmented_buffer::copy() const {
    std::vector<fragment_type> copied;
    copied.reserve(fragments_.size());
    for (const auto& fragment : fragments_) {
        copied.push_back(fragment.clone());
    }
    return fragmented_buffer{std::move(copied), size_};
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
    for (auto& fragment : fragments_) {
        if (remaining == 0) {
            break;
        }
        const auto fragment_size = static_cast<std::uint64_t>(fragment.size());
        if (remaining >= fragment_size) {
            remaining -= fragment_size;
            ++dropped;
            continue;
        }
        fragment.trim_front(static_cast<std::size_t>(remaining));
        remaining = 0;
    }
    fragments_.erase(
      fragments_.begin(),
      fragments_.begin() + static_cast<std::ptrdiff_t>(dropped));
    size_ = *size_.checked_sub(bytes);
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
    for (auto& fragment : std::views::reverse(fragments_)) {
        if (remaining == 0) {
            break;
        }
        const auto fragment_size = static_cast<std::uint64_t>(fragment.size());
        if (remaining >= fragment_size) {
            remaining -= fragment_size;
            ++dropped;
            continue;
        }
        fragment.trim(static_cast<std::size_t>(fragment_size - remaining));
        remaining = 0;
    }
    fragments_.erase(
      fragments_.end() - static_cast<std::ptrdiff_t>(dropped),
      fragments_.end());
    size_ = *size_.checked_sub(bytes);
    drop_empty_fragments();
    return {};
}

result<scatter_batch> fragmented_buffer::export_scatter(
  std::span<::iovec> vectors,
  byte_count max_bytes,
  scatter_cursor& cursor) const {
    if (vectors.empty() || max_bytes.value() == 0) {
        return failure(errc::invalid_argument);
    }
    if (
      cursor.fragment_ > fragments_.size()
      || (cursor.fragment_ == fragments_.size() && cursor.offset_.value() != 0)) {
        return failure(errc::out_of_range);
    }

    const auto capacity = std::min(vectors.size(), max_scatter_vectors);
    scatter_batch batch;
    std::uint64_t offset = cursor.offset().value();
    std::size_t index = cursor.fragment_;
    std::uint64_t budget = max_bytes.value();

    while (index < fragments_.size() && batch.vectors < capacity
           && budget > 0) {
        const auto& fragment = fragments_[index];
        const auto fragment_size = static_cast<std::uint64_t>(fragment.size());
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
        vectors[batch.vectors] = ::iovec{
          .iov_base = const_cast<char*>(fragment.get() + offset),
          .iov_len = static_cast<std::size_t>(span)};
        ++batch.vectors;
        // `span` is at most the remaining budget, so the accumulated count can
        // never exceed the caller's uint64_t byte cap.
        batch.bytes = byte_count{batch.bytes.value() + span};
        budget -= span;
        offset += span;
        if (offset == fragment_size) {
            ++index;
            offset = 0;
        }
    }

    cursor.fragment_ = index;
    cursor.offset_ = byte_count{offset};
    batch.complete = index == fragments_.size();
    return batch;
}

result<fragmented_buffer::fragment_type>
fragmented_buffer::linearize(byte_count max_bytes) const {
    if (size_ > max_bytes) {
        ++shard_counters.linearize_rejections;
        return failure(errc::resource_exhausted);
    }
    fragment_type linear{static_cast<std::size_t>(size_.value())};
    std::size_t written = 0;
    for (const auto& fragment : fragments_) {
        std::memcpy(
          linear.get_write() + written, fragment.get(), fragment.size());
        written += fragment.size();
    }
    ++shard_counters.linearizations;
    shard_counters.linearized_bytes += size_.value();
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
          destination.data() + written, fragment.get(), fragment.size());
        written += fragment.size();
    }
    return byte_count{static_cast<std::uint64_t>(written)};
}

result<seastar::net::packet> fragmented_buffer::release_to_packet() && {
    // The transfer type records its total in an unsigned int, so a buffer wider
    // than that cannot be handed over without wrapping. Refuse rather than
    // silently truncate; the caller can split or send in batches instead.
    if (size_.value() > std::numeric_limits<unsigned>::max()) {
        return failure(errc::out_of_range);
    }
    // Keep all temporary buffers in one aggregate owner. Passing them through
    // the span constructor would append one separately allocated packet
    // deleter per fragment; the aggregate keeps transfer allocation count
    // constant while retaining every backing buffer without copying bytes.
    auto owned_fragments = std::move(fragments_);
    auto packet_fragments = owned_fragments
                            | std::views::transform(
                              [](fragment_type& fragment) {
                                  return seastar::net::fragment{
                                    fragment.get_write(), fragment.size()};
                              });
    // Vector element iterators remain valid when its storage is moved into the
    // aggregate owner. The packet copies only the small fragment descriptors
    // before taking that owner.
    const auto begin = packet_fragments.begin();
    const auto end = packet_fragments.end();
    auto owner = seastar::make_object_deleter(std::move(owned_fragments));
    seastar::net::packet transferred{begin, end, std::move(owner)};
    fragments_.clear();
    size_ = byte_count{};
    return transferred;
}

bool fragmented_buffer::content_equals(std::string_view bytes) const noexcept {
    if (bytes.size() != size_.value()) {
        return false;
    }
    std::size_t offset = 0;
    for (const auto& fragment : fragments_) {
        if (
          std::memcmp(bytes.data() + offset, fragment.get(), fragment.size())
          != 0) {
            return false;
        }
        offset += fragment.size();
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
          left.size() - left_offset, right.size() - right_offset);
        if (span == 0) {
            if (left.size() == left_offset) {
                ++left_index;
                left_offset = 0;
            }
            if (right.size() == right_offset) {
                ++right_index;
                right_offset = 0;
            }
            continue;
        }
        if (
          std::memcmp(
            left.get() + left_offset, right.get() + right_offset, span)
          != 0) {
            return false;
        }
        left_offset += span;
        right_offset += span;
    }
    return true;
}

} // namespace kwaque::bytes
