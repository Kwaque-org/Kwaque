#ifndef KWAQUE_SRC_BYTES_FRAGMENTED_BUFFER_H_
#define KWAQUE_SRC_BYTES_FRAGMENTED_BUFFER_H_

#include "src/base/allocation.h"
#include "src/base/result.h"
#include "src/base/units.h"

#include <seastar/core/temporary_buffer.hh>
#include <seastar/net/packet.hh>

#include <boost/container/devector.hpp>

#include <cstddef>
#include <cstdint>
#include <iterator>
#include <limits>
#include <memory>
#include <span>
#include <string_view>
#include <vector>

namespace kwaque::runtime::detail {
class fragmented_buffer_io_access;
}

namespace kwaque::bytes {

class fragmented_buffer_parser;

// Structural ceilings for a published buffer. They bound the work any single
// buffer can impose on iteration, export, and parsing regardless of how it was
// built, and are internal defaults rather than configuration.
inline constexpr std::size_t max_buffer_fragments = 1024;
inline constexpr byte_count max_buffer_bytes{
  max_buffer_fragments * maximum_contiguous_allocation_bytes};
// A scatter batch never exceeds what one vectored write can accept, taken from
// the platform rather than assumed, so a full batch is always submittable.
inline constexpr std::size_t max_scatter_vectors = 1024;

// A stable, nonallocating, nondecreasing upper-bound function verified for the
// caller's allocator profile. It charges served capacity for a requested
// allocation, not shard RSS.
using allocation_charge_fn = byte_count (*)(byte_count) noexcept;

struct buffer_allocation_cost final {
    byte_count backing;
    byte_count descriptors;
    byte_count share_controls;
    // Largest individual charge among the reported components.
    byte_count largest_allocation;
    item_count fragments;

    bool operator==(const buffer_allocation_cost&) const = default;
};

// Cost queries cover recorded backing, descriptor capacity and possible raw
// deleter promotion. Any other resources retained by an opaque native deleter
// remain the producer's accounting responsibility; they cannot be introspected.

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
// where the previous one ended. The first successful export binds the cursor
// to that buffer and its presentation generation. Moving from, replacing, or
// trimming the originating buffer invalidates the cursor. A cursor is usable
// only during the lifetime of its originating buffer object.
class fragmented_buffer;

class scatter_cursor final {
public:
    scatter_cursor() noexcept = default;

    [[nodiscard]] std::size_t fragment() const noexcept { return fragment_; }
    [[nodiscard]] byte_count offset() const noexcept { return offset_; }

    bool operator==(const scatter_cursor&) const = default;

private:
    friend class fragmented_buffer;

    const fragmented_buffer* owner_{nullptr};
    std::uint64_t generation_{0};
    std::size_t fragment_{0};
    byte_count offset_{};
};

struct scatter_segment final {
    const char* data{nullptr};
    std::size_t size{0};

    bool operator==(const scatter_segment&) const = default;
};

class scatter_batch final {
public:
    class const_iterator final {
    public:
        using difference_type = std::ptrdiff_t;
        using value_type = scatter_segment;
        using reference = scatter_segment;
        using pointer = void;
        using iterator_category = std::input_iterator_tag;

        const_iterator() noexcept = default;

        [[nodiscard]] reference operator*() const noexcept {
            const auto& fragment = owner_->ownership_[index_];
            return scatter_segment{
              .data = fragment.get(), .size = fragment.size()};
        }
        const_iterator& operator++() noexcept {
            ++index_;
            return *this;
        }
        const_iterator operator++(int) noexcept {
            auto previous = *this;
            ++index_;
            return previous;
        }

        bool operator==(const const_iterator&) const = default;

    private:
        friend class scatter_batch;

        const_iterator(const scatter_batch* owner, std::size_t index) noexcept
          : owner_(owner)
          , index_(index) {}

        const scatter_batch* owner_{nullptr};
        std::size_t index_{0};
    };

    [[nodiscard]] scatter_segment operator[](std::size_t index) const noexcept {
        const auto& fragment = ownership_[index];
        return scatter_segment{.data = fragment.get(), .size = fragment.size()};
    }
    [[nodiscard]] const_iterator begin() const noexcept {
        return const_iterator{this, 0};
    }
    [[nodiscard]] const_iterator end() const noexcept {
        return const_iterator{this, ownership_.size()};
    }
    [[nodiscard]] std::size_t vector_count() const noexcept {
        return ownership_.size();
    }
    [[nodiscard]] byte_count bytes() const noexcept { return bytes_; }
    [[nodiscard]] bool complete() const noexcept { return complete_; }

private:
    friend class fragmented_buffer;

    std::vector<seastar::temporary_buffer<char>> ownership_;
    byte_count bytes_{};
    bool complete_{false};
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
private:
    struct owned_fragment final {
        seastar::temporary_buffer<char> storage;
        byte_count retained_bytes;
    };
    using fragment_storage = boost::container::
      devector<owned_fragment, std::allocator<owned_fragment>>;
    // Default 1.6x growth with relocation below 90% occupancy keeps capacity
    // below twice the fragment ceiling. Exact reservations use fresh storage.
    static_assert(
      sizeof(owned_fragment) * 2U * max_buffer_fragments
      <= maximum_contiguous_allocation_bytes);

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
            return fragment_view{
              position_->storage.get(), position_->storage.size()};
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

        explicit const_iterator(
          fragment_storage::const_iterator position) noexcept
          : position_(position) {}

        fragment_storage::const_iterator position_{};
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

    // External temporary buffers may have writable aliases. Copy them into
    // frozen backing so publication is actually immutable through every owner.
    // Empty fragments are dropped from the resulting presentation.
    [[nodiscard]] static result<fragmented_buffer>
    copy_from_fragment(const fragment_type& fragment);
    [[nodiscard]] static result<fragmented_buffer>
    copy_from_fragments(std::span<const fragment_type> fragments);
    // A character array is refused for the same reason as on the builder: its
    // span would include the terminator.
    [[nodiscard]] static result<fragmented_buffer>
    copy_of(std::span<const char> bytes);
    template<std::size_t N>
    static result<fragmented_buffer> copy_of(const char (&literal)[N]) = delete;

    [[nodiscard]] byte_count size() const noexcept { return size_; }
    [[nodiscard]] byte_count retained_bytes() const noexcept {
        return retained_bytes_;
    }
    [[nodiscard]] bool empty() const noexcept { return size_.value() == 0; }
    [[nodiscard]] std::size_t fragment_count() const noexcept {
        return fragments_.size();
    }
    [[nodiscard]] static constexpr std::size_t
    fragment_descriptor_size() noexcept {
        return sizeof(owned_fragment);
    }

    // Conservative charges under a nonnull verified profile function, without
    // allocating, sharing or changing the buffer. Includes descriptor capacity
    // retained by earlier trims and one potential share control per fragment;
    // aliases are not deduplicated. This is not allocator introspection or an
    // admission decision. The function must never charge less than requested.
    [[nodiscard]] result<buffer_allocation_cost>
    allocation_cost(allocation_charge_fn charge) const noexcept;

    // Bounded accounting for cooperative owners. The range beginning at zero
    // includes the physical descriptor allocation (even for an empty buffer);
    // other ranges include only their backing and possible share controls.
    // Sum disjoint consecutive ranges to obtain the whole-buffer charge.
    [[nodiscard]] result<buffer_allocation_cost> allocation_cost(
      std::size_t first,
      std::size_t count,
      allocation_charge_fn charge) const noexcept;

    // Charges the touched backing and potential share controls, plus both old
    // and new child descriptor allocations during bounded slice construction.
    // It excludes the parent's descriptors. A zero-length slice has zero cost
    // and makes no charge-function calls, even if the parent retains capacity.
    [[nodiscard]] result<buffer_allocation_cost> slice_allocation_cost(
      byte_count offset,
      byte_count length,
      allocation_charge_fn charge) const noexcept;

    [[nodiscard]] result<fragment_view> fragment_at(std::size_t index) const;
    [[nodiscard]] const_iterator begin() const noexcept {
        return const_iterator{fragments_.cbegin()};
    }
    [[nodiscard]] const_iterator end() const noexcept {
        return const_iterator{fragments_.cend()};
    }

    // Zero-copy: the result references the same bytes through independent
    // ownership. Not const because taking a share converts fragment ownership
    // to a counted form, which the source records.
    [[nodiscard]] fragmented_buffer share() {
        fragmented_buffer shared;
        shared.fragments_.reserve_back(fragments_.size());
        for (auto& fragment : fragments_) {
            shared.fragments_.emplace_back(
              fragment.storage.share(), fragment.retained_bytes);
        }
        shared.size_ = size_;
        shared.retained_bytes_ = retained_bytes_;
        return shared;
    }
    [[nodiscard]] result<fragmented_buffer>
    share(byte_count offset, byte_count length);

    // Deep copy of the presented bytes into fresh allocator-sized chunks.
    // Existing fragmentation is deliberately coalesced so a hostile or
    // accidental tiny-fragment layout does not multiply payload allocations.
    [[nodiscard]] fragmented_buffer copy() const;

    // Adjust which owned bytes this buffer presents. Fragments emptied by the
    // adjustment are dropped, and an exhausted buffer becomes the canonical
    // empty representation.
    [[nodiscard]] result<void> trim_front(byte_count bytes);
    [[nodiscard]] result<void> trim_back(byte_count bytes);

    // Returns at most max_vectors entries, never more than max_scatter_vectors,
    // and never more than max_bytes bytes, resuming from and advancing
    // `cursor`. A fragment larger than the remaining byte budget is emitted
    // partially and the cursor stops inside it. No bytes are copied and no
    // linearization happens. The batch owns shared claims on every exported
    // span, so it remains valid if this buffer is trimmed, moved, or destroyed.
    // Conversion to mutable native iovec belongs inside the I/O adapter.
    // Rejection leaves the cursor unchanged. Exhausting the presentation
    // generation space rejects further exports with resource_exhausted.
    [[nodiscard]] result<scatter_batch> export_scatter(
      std::size_t max_vectors, byte_count max_bytes, scatter_cursor& cursor);

    // Contiguous conversion is always explicit and always bounded. Rejection
    // happens before any allocation.
    [[nodiscard]] result<fragment_type> linearize(byte_count max_bytes) const;
    [[nodiscard]] result<byte_count> copy_to(std::span<char> destination) const;

    // Produces an independent native packet after coalescing tiny source
    // fragments into bounded deep-copy chunks. Packet exposes mutable storage,
    // so the source and all published shares remain immutable.
    [[nodiscard]] result<seastar::net::packet> copy_to_packet() const;

    [[nodiscard]] bool content_equals(std::string_view bytes) const noexcept;
    [[nodiscard]] bool
    content_equals(const fragmented_buffer& other) const noexcept;

private:
    friend class fragmented_buffer_builder;
    friend class fragmented_buffer_parser;
    friend class fragmented_buffer_test_access;
    friend class runtime::detail::fragmented_buffer_io_access;

    fragmented_buffer(
      fragment_storage fragments,
      byte_count size,
      byte_count retained_bytes) noexcept;

    [[nodiscard]] static result<byte_count>
    total_size(const fragment_storage& fragments) noexcept;
    void drop_empty_fragments();
    void invalidate_presentation() noexcept {
        // Never reuse a presentation number. At exhaustion the buffer remains
        // usable, but scatter export rejects both existing and fresh cursors.
        if (generation_ != std::numeric_limits<std::uint64_t>::max()) {
            ++generation_;
        }
    }

    fragment_storage fragments_;
    byte_count size_;
    byte_count retained_bytes_;
    std::uint64_t generation_{0};
};

} // namespace kwaque::bytes

#endif // KWAQUE_SRC_BYTES_FRAGMENTED_BUFFER_H_
