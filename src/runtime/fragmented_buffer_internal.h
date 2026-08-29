#ifndef KWAQUE_SRC_RUNTIME_FRAGMENTED_BUFFER_INTERNAL_H_
#define KWAQUE_SRC_RUNTIME_FRAGMENTED_BUFFER_INTERNAL_H_

#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"

#include <seastar/core/temporary_buffer.hh>

#include <cstdint>
#include <utility>

namespace kwaque::runtime::detail {

// File-implementation-only destructive ownership transfer. It deliberately has
// no public forwarding API: the concrete file owner consumes native fragments
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

private:
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
