#ifndef KWAQUE_SRC_RUNTIME_FRAGMENTED_BUFFER_INTERNAL_H_
#define KWAQUE_SRC_RUNTIME_FRAGMENTED_BUFFER_INTERNAL_H_

#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"

#include <seastar/core/temporary_buffer.hh>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <span>
#include <utility>

namespace kwaque::runtime::detail {

// Runtime-I/O-only destructive ownership transfer. It deliberately has no
// public forwarding API: concrete native adapters consume and adopt fragments
// without exposing writable storage to callers.
class fragmented_buffer_io_access final {
public:
    class consumer final {
    public:
        consumer(consumer&& other) noexcept
          : buffer_(std::exchange(other.buffer_, nullptr)) {}
        consumer& operator=(consumer&&) = delete;
        consumer(const consumer&) = delete;
        consumer& operator=(const consumer&) = delete;

        [[nodiscard]] seastar::temporary_buffer<char> take_front() noexcept {
            return buffer_ == nullptr
                     ? seastar::temporary_buffer<char>{}
                     : fragmented_buffer_io_access::take_front(*buffer_);
        }

        [[nodiscard]] bytes::fragment_view front() const noexcept {
            return buffer_ == nullptr
                     ? bytes::fragment_view{}
                     : fragmented_buffer_io_access::front(*buffer_);
        }

        [[nodiscard]] seastar::temporary_buffer<char>
        take_front(std::size_t maximum_bytes) {
            return buffer_ == nullptr ? seastar::temporary_buffer<char>{}
                                      : fragmented_buffer_io_access::take_front(
                                          *buffer_, maximum_bytes);
        }

        [[nodiscard]] std::size_t
        copy_front_to(std::span<char> destination) noexcept {
            return buffer_ == nullptr
                     ? 0
                     : fragmented_buffer_io_access::copy_front_to(
                         *buffer_, destination);
        }

    private:
        friend class fragmented_buffer_io_access;

        explicit consumer(bytes::fragmented_buffer& buffer) noexcept
          : buffer_(&buffer) {}

        bytes::fragmented_buffer* buffer_;
    };

    [[nodiscard]] static consumer
    consume(bytes::fragmented_buffer& buffer) noexcept {
        if (!buffer.fragments_.empty()) {
            ++buffer.generation_;
        }
        return consumer{buffer};
    }

    [[nodiscard]] static bytes::fragmented_buffer
    adopt(seastar::temporary_buffer<char> fragment) {
        if (fragment.empty()) {
            return {};
        }
        const byte_count size{static_cast<std::uint64_t>(fragment.size())};
        std::deque<bytes::fragmented_buffer::owned_fragment> fragments;
        fragments.push_back(
          bytes::fragmented_buffer::owned_fragment{
            .storage = std::move(fragment),
            .retained_bytes = size,
          });
        return bytes::fragmented_buffer{std::move(fragments), size, size};
    }

private:
    [[nodiscard]] static bytes::fragment_view
    front(const bytes::fragmented_buffer& buffer) noexcept {
        if (buffer.fragments_.empty()) {
            return {};
        }
        const auto& fragment = buffer.fragments_.front().storage;
        return bytes::fragment_view{fragment.get(), fragment.size()};
    }

    [[nodiscard]] static seastar::temporary_buffer<char>
    take_front(bytes::fragmented_buffer& buffer, std::size_t maximum_bytes) {
        if (buffer.fragments_.empty() || maximum_bytes == 0) {
            return {};
        }
        auto& fragment = buffer.fragments_.front();
        if (fragment.storage.size() <= maximum_bytes) {
            return take_front(buffer);
        }
        auto prefix = fragment.storage.share(0, maximum_bytes);
        fragment.storage.trim_front(maximum_bytes);
        buffer.size_ = byte_count{
          buffer.size_.value() - static_cast<std::uint64_t>(maximum_bytes)};
        return prefix;
    }

    [[nodiscard]] static std::size_t copy_front_to(
      bytes::fragmented_buffer& buffer, std::span<char> destination) noexcept {
        std::size_t copied = 0;
        while (copied < destination.size() && !buffer.fragments_.empty()) {
            auto& fragment = buffer.fragments_.front();
            const auto count = std::min(
              fragment.storage.size(), destination.size() - copied);
            std::memcpy(
              destination.data() + copied, fragment.storage.get(), count);
            copied += count;
            if (count == fragment.storage.size()) {
                static_cast<void>(take_front(buffer));
                continue;
            }
            fragment.storage.trim_front(count);
            buffer.size_ = byte_count{
              buffer.size_.value() - static_cast<std::uint64_t>(count)};
        }
        return copied;
    }

    [[nodiscard]] static seastar::temporary_buffer<char>
    take_front(bytes::fragmented_buffer& buffer) noexcept {
        if (buffer.fragments_.empty()) {
            return {};
        }
        auto fragment = std::move(buffer.fragments_.front());
        buffer.fragments_.pop_front();
        buffer.size_ = byte_count{
          buffer.size_.value()
          - static_cast<std::uint64_t>(fragment.storage.size())};
        buffer.retained_bytes_ = byte_count{
          buffer.retained_bytes_.value() - fragment.retained_bytes.value()};
        return std::move(fragment.storage);
    }
};

} // namespace kwaque::runtime::detail

#endif // KWAQUE_SRC_RUNTIME_FRAGMENTED_BUFFER_INTERNAL_H_
