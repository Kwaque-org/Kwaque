#pragma once

#include "src/base/result.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"

#include <seastar/core/temporary_buffer.hh>

#include <bit>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>
#include <vector>

namespace kwaque::bytes {

// Smallest size the shard allocator serves for a request of `size`.
//
// Seastar pools small allocations in size classes indexed with two fractional
// bits, never hands out a cell smaller than its free-list link, and serves
// larger requests from a buddy allocator that only has power-of-two spans.
// Requesting a size that falls between classes still consumes the whole class,
// so growing through these sizes keeps the bytes served and the bytes tracked
// equal instead of leaving the difference silently unusable.
//
// Two honest limits on this. It is a model of the allocator, not a query: there
// is no public way to ask, so the golden values in the tests pin what this
// function computes rather than what the allocator does, and a change in the
// allocator's pooling would need re-deriving here. And builds that substitute
// the system allocator have no such classes at all, so there the rounding is
// merely a slightly larger request. It earns its keep in release builds.
[[nodiscard]] constexpr std::uint64_t
served_allocation_size(std::uint64_t size) noexcept {
    constexpr unsigned fraction_bits = 2;
    constexpr std::uint64_t fraction_mask = (std::uint64_t{1} << fraction_bits)
                                            - 1;
    // Above this, allocations come from power-of-two spans instead of pools.
    constexpr std::uint64_t max_pooled = 16384;
    // The allocator never serves a cell smaller than one free-list link.
    constexpr std::uint64_t minimum_cell = sizeof(void*);

    if (size == 0) {
        return 0;
    }
    if (size < minimum_cell) {
        size = minimum_cell;
    }

    const auto log2_floor = static_cast<unsigned>(std::bit_width(size) - 1);

    if (size > max_pooled) {
        const auto span = std::uint64_t{1} << log2_floor;
        if (span == size) {
            return size;
        }
        // Doubling past the top of the range would wrap to zero and report
        // capacity that does not exist. Report the request unchanged instead so
        // the caller's own ceiling remains the bound.
        if (log2_floor >= 63) {
            return size;
        }
        return span << 1;
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

// A single buffer's bytes stay well inside the range where size arithmetic and
// allocation rounding cannot overflow.
inline constexpr byte_count max_builder_total_bytes{std::uint64_t{1} << 40};

struct fragmented_buffer_builder_config final {
    // Size of the first allocation. Later allocations grow geometrically from
    // here so a stream of tiny appends cannot produce a fragment per append.
    byte_count initial_fragment_bytes{byte_count{512}};
    // Absolute ceiling on any single allocation. Growth stops here and long
    // appends continue into further fragments instead of one huge allocation.
    byte_count max_fragment_bytes{byte_count{128UL * 1024UL}};
    byte_count max_total_bytes{byte_count{64UL * 1024UL * 1024UL}};
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

    // Takes ownership without copying, except that a fragment at or below
    // pack_copy_threshold is copied into a bounded growth tail. This keeps a
    // stream of tiny owned slices from becoming a fragment per slice while
    // larger fragments retain their original storage.
    [[nodiscard]] result<void> append_fragment(fragment_type fragment);
    // Splices another published buffer's fragments in, applying the same
    // packing rule to its leading fragments.
    [[nodiscard]] result<void> append_buffer(fragmented_buffer&& other);

    // Guarantees at least `bytes` of contiguous tail capacity, bounded by
    // max_fragment_bytes.
    [[nodiscard]] result<void> reserve(byte_count bytes);

    // Publishes the accumulated bytes. One-way: the builder holds nothing
    // afterwards and every later append, reserve, or finish reports closed. The
    // flag rather than a reference qualifier enforces this, so a caller can
    // still inspect a builder whose finish failed.
    [[nodiscard]] result<fragmented_buffer> finish();

private:
    struct rollback_point final {
        std::size_t fragments;
        std::uint64_t tail_used;
        std::uint64_t last_allocation;
    };

    [[nodiscard]] result<void> ensure_appendable(byte_count incoming) noexcept;
    [[nodiscard]] result<void> grow_tail(std::uint64_t requested);
    void grow_tail_unchecked(std::uint64_t requested);
    // Appends non-empty storage whose lifetime, total size, and worst-case link
    // count have already been validated by the caller.
    void append_prevalidated_fragment(fragment_type fragment);
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
    std::vector<fragment_type> fragments_;
    byte_count size_;
    std::uint64_t tail_used_{0};
    std::uint64_t last_allocation_{0};
    bool finished_{false};
};

} // namespace kwaque::bytes
