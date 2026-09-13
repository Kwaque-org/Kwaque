#pragma once

#include "src/base/result.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"

#include <seastar/core/temporary_buffer.hh>

#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>

namespace kwaque::bytes {

// A buffer remains representable using only bounded contiguous fragments.
inline constexpr byte_count max_builder_total_bytes{max_buffer_bytes};

struct fragmented_buffer_builder_config final {
    // Size of the first allocation. Later allocations grow geometrically from
    // here so a stream of tiny appends cannot produce a fragment per append.
    byte_count initial_fragment_bytes{byte_count{512}};
    // Absolute ceiling on any single allocation. Growth stops here and long
    // appends continue into further fragments instead of one huge allocation.
    byte_count max_fragment_bytes{byte_count{128UL * 1024UL}};
    byte_count max_total_bytes{byte_count{64UL * 1024UL * 1024UL}};
    // Bounds backing allocations retained by tails and zero-copy published
    // fragments independently of their logical presented bytes.
    byte_count max_retained_bytes{byte_count{128UL * 1024UL * 1024UL}};
    std::size_t max_fragments{max_buffer_fragments};

    [[nodiscard]] result<void> validate() const noexcept;

    bool operator==(const fragmented_buffer_builder_config&) const = default;
};

// The only surface that mutates buffer content. It accumulates owned storage
// and publishes it exactly once through finish(), after which the bytes are
// immutable.
class fragmented_buffer_builder final {
public:
    using fragment_type = seastar::temporary_buffer<char>;

    // A donated fragment at or below this size is copied into spare tail
    // capacity instead of becoming its own link, trading a small copy for a
    // shorter fragment list. Fixed rather than configured: packing is a
    // deterministic property of the builder, and the copy is bounded again by
    // whatever tail capacity actually exists.
    static constexpr byte_count pack_copy_threshold{4096};

    fragmented_buffer_builder();
    explicit fragmented_buffer_builder(fragmented_buffer_builder_config config);
    // Moves reset the source, which becomes finished. A memberwise move would
    // leave its byte total and tail accounting behind with no fragments to
    // match, so it would report content it no longer holds.
    fragmented_buffer_builder(fragmented_buffer_builder&& other) noexcept;
    fragmented_buffer_builder&
    operator=(fragmented_buffer_builder&& other) noexcept;
    fragmented_buffer_builder(const fragmented_buffer_builder&) = delete;
    fragmented_buffer_builder&
    operator=(const fragmented_buffer_builder&) = delete;
    ~fragmented_buffer_builder() = default;

    [[nodiscard]] const fragmented_buffer_builder_config&
    config() const noexcept {
        return config_;
    }
    [[nodiscard]] byte_count size() const noexcept { return size_; }
    [[nodiscard]] byte_count retained_bytes() const noexcept {
        return retained_bytes_;
    }
    [[nodiscard]] bool empty() const noexcept { return size_.value() == 0; }
    [[nodiscard]] std::size_t fragment_count() const noexcept {
        return fragments_.size();
    }
    [[nodiscard]] bool finished() const noexcept { return finished_; }
    // Unused bytes in the current tail allocation.
    [[nodiscard]] byte_count tail_capacity() const noexcept;

    // Copies bytes in, filling spare tail capacity first and then allocating
    // further fragments. A long append is split across fragments rather than
    // triggering one oversized allocation.
    //
    // Accepts any contiguous range of chars, so strings, string views, vectors,
    // and arrays all convert. A character array is refused because its span
    // would include the terminator; say which bytes you mean.
    [[nodiscard]] result<void> append(std::span<const char> bytes);
    template<std::size_t N>
    result<void> append(const char (&literal)[N]) = delete;

    // Copies external storage into frozen builder ownership. External
    // temporary buffers may have writable aliases, so zero-copy publication is
    // reserved for append_buffer() of an already-published Kwaque buffer.
    // Rejects a fragment above max_fragment_bytes before allocating or changing
    // the builder. Use append() to split a longer contiguous byte range.
    [[nodiscard]] result<void>
    append_fragment_copy(const fragment_type& fragment);
    // Copy at most pack_copy_threshold bytes from complete leading fragments
    // into existing mutable tail room, then splice the remaining owners.
    // No new payload backing is allocated. Rejection or allocation failure
    // leaves both byte sequences unchanged; success empties the source.
    [[nodiscard]] result<void> append_buffer(fragmented_buffer&& other);

    // Guarantees at least `bytes` of contiguous tail capacity, bounded by
    // max_fragment_bytes.
    [[nodiscard]] result<void> reserve(byte_count bytes);

    // Reserve descriptor storage before bounded splices. This does not add
    // bytes or fragments. The caller admits the allocation and any migration
    // of existing descriptors; an empty builder has no migration work.
    [[nodiscard]] result<void> reserve_fragments(item_count count);

    // Publishes the accumulated bytes. One-way: the builder holds nothing
    // afterwards and every later append, reserve, or finish reports closed. The
    // descriptor storage transfers directly, without allocating at publication.
    [[nodiscard]] result<fragmented_buffer> finish();

private:
    struct rollback_point final {
        std::size_t fragments;
        std::uint64_t tail_used;
        std::uint64_t last_allocation;
        byte_count retained_bytes;
    };

    [[nodiscard]] result<void> ensure_appendable(byte_count incoming) noexcept;
    [[nodiscard]] result<void> grow_tail(std::uint64_t requested);
    void grow_tail_unchecked(std::uint64_t requested);
    // Insert before sealing the old tail, so allocation failure changes no
    // existing presentation. Callers have checked fragment/backing bounds.
    void push_fragment(
      fragmented_buffer::owned_fragment fragment, std::uint64_t used);
    // Trims the tail allocation to the bytes actually written so donated
    // fragments can follow it without exposing unwritten capacity.
    void seal_tail() noexcept;
    [[nodiscard]] rollback_point mark() const noexcept;
    // Restores the fragment list to a mark so a failed append leaves exactly
    // the content that preceded it.
    void rewind(const rollback_point& mark) noexcept;
    [[nodiscard]] std::uint64_t
    next_allocation(std::uint64_t requested) const noexcept;
    fragmented_buffer_builder_config config_;
    fragmented_buffer::fragment_storage fragments_;
    byte_count size_;
    byte_count retained_bytes_;
    std::uint64_t tail_used_{0};
    std::uint64_t last_allocation_{0};
    bool finished_{false};
};

} // namespace kwaque::bytes
