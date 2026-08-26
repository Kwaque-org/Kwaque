#pragma once

#include "src/base/result.h"
#include "src/base/units.h"

#include <seastar/core/temporary_buffer.hh>
#include <seastar/net/packet.hh>

#include <climits>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <span>
#include <string_view>
#include <vector>

struct iovec;

namespace kwaque::bytes {

// Structural ceilings for a published buffer. They bound the work any single
// buffer can impose on iteration, export, and parsing regardless of how it was
// built, and are internal defaults rather than configuration.
inline constexpr std::size_t max_buffer_fragments = 1024;
// A scatter batch never exceeds what one vectored write can accept, taken from
// the platform rather than assumed, so a full batch is always submittable.
inline constexpr std::size_t max_scatter_vectors = static_cast<std::size_t>(
  IOV_MAX);
static_assert(
  max_scatter_vectors >= 16,
  "a usable scatter batch needs room for several fragments");

// Shard-local observations of operations that are deliberately explicit because
// they copy or bound bytes. Reset only by tests.
struct buffer_counters final {
    std::uint64_t linearizations{0};
    std::uint64_t linearize_rejections{0};
    std::uint64_t linearized_bytes{0};

    bool operator==(const buffer_counters&) const = default;
};

[[nodiscard]] const buffer_counters& counters() noexcept;
void reset_counters() noexcept;

// A borrowed read-only window onto one fragment's bytes. Valid only while the
// buffer that produced it is alive and untrimmed.
class fragment_view final {
public:
    fragment_view() noexcept = default;
    fragment_view(const char* data, std::size_t size) noexcept
      : data_(data)
      , size_(size) {}

    [[nodiscard]] const char* data() const noexcept { return data_; }
    [[nodiscard]] std::size_t size() const noexcept { return size_; }
    [[nodiscard]] bool empty() const noexcept { return size_ == 0; }
    [[nodiscard]] std::string_view bytes() const noexcept {
        return std::string_view{data_, size_};
    }

    bool operator==(const fragment_view&) const = default;

private:
    const char* data_{nullptr};
    std::size_t size_{0};
};

// Where a bounded scatter export stopped so the next batch resumes exactly
// where the previous one ended. A default-constructed cursor starts at the
// beginning; a cursor is only meaningful for the buffer that advanced it.
class scatter_cursor final {
public:
    scatter_cursor() noexcept = default;

    [[nodiscard]] std::size_t fragment() const noexcept { return fragment_; }
    [[nodiscard]] byte_count offset() const noexcept { return offset_; }

    bool operator==(const scatter_cursor&) const = default;

private:
    friend class fragmented_buffer;

    std::size_t fragment_{0};
    byte_count offset_{};
};

struct scatter_batch final {
    std::size_t vectors{0};
    byte_count bytes{};
    // True when this batch exported the buffer's final bytes.
    bool complete{false};

    bool operator==(const scatter_batch&) const = default;
};

// A move-only sequence of owning fragments whose bytes are immutable once
// published. Every fragment owns its backing storage, so a share or slice stays
// valid after the buffer it came from is destroyed. Construction and mutation
// of content belong to fragmented_buffer_builder; the operations here either
// observe bytes or adjust which of the owned bytes this buffer presents.
//
// One owner at a time: the type is move-only and must not cross shards. Use an
// explicit deep copy for that.
class fragmented_buffer final {
public:
    using fragment_type = seastar::temporary_buffer<char>;

    class const_iterator final {
    public:
        using difference_type = std::ptrdiff_t;
        using value_type = fragment_view;
        using reference = fragment_view;
        using pointer = void;
        using iterator_category = std::input_iterator_tag;

        const_iterator() noexcept = default;

        [[nodiscard]] reference operator*() const noexcept {
            return fragment_view{position_->get(), position_->size()};
        }
        const_iterator& operator++() noexcept {
            ++position_;
            return *this;
        }
        const_iterator operator++(int) noexcept {
            auto previous = *this;
            ++position_;
            return previous;
        }

        bool operator==(const const_iterator&) const = default;

    private:
        friend class fragmented_buffer;

        explicit const_iterator(const fragment_type* position) noexcept
          : position_(position) {}

        const fragment_type* position_{nullptr};
    };

    fragmented_buffer() noexcept = default;
    // Moves reset the source rather than leaving its size behind. A memberwise
    // move would move the fragments but copy the total, so the source would
    // claim bytes it no longer holds and every size-driven operation on it
    // would read past what exists.
    fragmented_buffer(fragmented_buffer&& other) noexcept;
    fragmented_buffer& operator=(fragmented_buffer&& other) noexcept;
    fragmented_buffer(const fragmented_buffer&) = delete;
    fragmented_buffer& operator=(const fragmented_buffer&) = delete;
    ~fragmented_buffer() = default;

    // Takes ownership of already-allocated storage. Empty fragments are dropped
    // so a published buffer never carries a zero-length link.
    [[nodiscard]] static result<fragmented_buffer>
    from_fragment(fragment_type fragment);
    [[nodiscard]] static result<fragmented_buffer>
    from_fragments(std::vector<fragment_type> fragments);
    // A character array is refused for the same reason as on the builder: its
    // span would include the terminator.
    [[nodiscard]] static result<fragmented_buffer>
    copy_of(std::span<const char> bytes);
    template<std::size_t N>
    static result<fragmented_buffer> copy_of(const char (&literal)[N]) = delete;

    [[nodiscard]] byte_count size() const noexcept { return size_; }
    [[nodiscard]] bool empty() const noexcept { return size_.value() == 0; }
    [[nodiscard]] std::size_t fragment_count() const noexcept {
        return fragments_.size();
    }

    [[nodiscard]] result<fragment_view> fragment_at(std::size_t index) const;
    [[nodiscard]] const_iterator begin() const noexcept {
        return const_iterator{fragments_.empty() ? nullptr : fragments_.data()};
    }
    [[nodiscard]] const_iterator end() const noexcept {
        return const_iterator{
          fragments_.empty() ? nullptr : fragments_.data() + fragments_.size()};
    }

    // Zero-copy: the result references the same bytes through independent
    // ownership. Not const because taking a share converts fragment ownership
    // to a counted form, which the source records.
    [[nodiscard]] result<fragmented_buffer> share();
    [[nodiscard]] result<fragmented_buffer>
    share(byte_count offset, byte_count length);

    // Deep copy of the presented bytes into fresh contiguous-per-fragment
    // storage.
    [[nodiscard]] result<fragmented_buffer> copy() const;

    // Adjust which owned bytes this buffer presents. Fragments emptied by the
    // adjustment are dropped, and an exhausted buffer becomes the canonical
    // empty representation.
    [[nodiscard]] result<void> trim_front(byte_count bytes);
    [[nodiscard]] result<void> trim_back(byte_count bytes);

    // Fills up to vectors.size() entries, never more than max_scatter_vectors,
    // and never more than max_bytes bytes, resuming from and advancing
    // `cursor`. A fragment larger than the remaining byte budget is emitted
    // partially and the cursor stops inside it. No bytes are copied and no
    // linearization happens. The vectors borrow this buffer's storage, so the
    // buffer must outlive the I/O they are handed to.
    [[nodiscard]] result<scatter_batch> export_scatter(
      std::span<::iovec> vectors,
      byte_count max_bytes,
      scatter_cursor& cursor) const;

    // Contiguous conversion is always explicit and always bounded. Rejection
    // happens before any allocation.
    [[nodiscard]] result<fragment_type> linearize(byte_count max_bytes) const;
    [[nodiscard]] result<byte_count> copy_to(std::span<char> destination) const;

    // Hands owned fragments to the reactor's transfer type. Rvalue-only so a
    // buffer cannot keep presenting bytes it no longer owns; the source is left
    // empty. Any share taken beforehand keeps its own claim on the bytes. Fails
    // rather than wraps when the total exceeds what the transfer type can
    // record, leaving this buffer untouched.
    [[nodiscard]] result<seastar::net::packet> release_to_packet() &&;

    [[nodiscard]] bool content_equals(std::string_view bytes) const noexcept;
    [[nodiscard]] bool
    content_equals(const fragmented_buffer& other) const noexcept;

private:
    friend class fragmented_buffer_builder;

    fragmented_buffer(
      std::vector<fragment_type> fragments, byte_count size) noexcept;

    [[nodiscard]] static result<byte_count>
    total_size(const std::vector<fragment_type>& fragments) noexcept;
    // A non-empty lifetime manager is the temporary_buffer contract that its
    // bytes remain valid. Inspecting it moves the manager out and immediately
    // back into the same fragment without allocating or copying payload bytes.
    [[nodiscard]] static bool
    has_lifetime_owner(fragment_type& fragment) noexcept;
    void drop_empty_fragments();

    std::vector<fragment_type> fragments_;
    byte_count size_;
};

} // namespace kwaque::bytes
