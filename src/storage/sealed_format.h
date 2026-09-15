#pragma once

#include "src/storage/footer_format.h"
#include "src/storage/page_ref.h"

#include <span>
#include <vector>

namespace kwaque::storage {
namespace detail {
class sealed_codec;
}
inline constexpr byte_count sealed_footer_fixed_bytes{232};

// One structurally valid footer, verified against an externally supplied exact
// root digest. It has not verified the named extent or supplied retry pages.
// At most 256 PageRefs are retained; copying is disabled to keep reservation
// and lifetime explicit. A moved-from root has no usable page collection.
class sealed_footer final {
public:
    sealed_footer(sealed_footer&&) noexcept = default;
    sealed_footer& operator=(sealed_footer&&) noexcept = default;
    sealed_footer(const sealed_footer&) = delete;
    sealed_footer& operator=(const sealed_footer&) = delete;
    [[nodiscard]] footer_expectation location() const noexcept {
        return location_;
    }
    [[nodiscard]] storage::coverage coverage() const noexcept {
        return boundary_.coverage;
    }
    [[nodiscard]] std::uint32_t block_count() const noexcept {
        return boundary_.block_count;
    }
    [[nodiscard]] std::optional<storage::coverage> last_block() const noexcept {
        return boundary_.last_block;
    }
    [[nodiscard]] codec::extent_digest extent_digest() const noexcept {
        return extent_digest_;
    }
    [[nodiscard]] codec::immutable_object_digest digest() const noexcept {
        return digest_;
    }
    [[nodiscard]] std::uint32_t retry_count() const noexcept {
        return retry_count_;
    }
    [[nodiscard]] std::span<const page_ref> pages() const& noexcept {
        return pages_;
    }
    std::span<const page_ref> pages() const&& = delete;
    [[nodiscard]] std::size_t page_capacity() const noexcept {
        return pages_.capacity();
    }
    [[nodiscard]] model::file_byte_span encoded_extent() const noexcept {
        return extent_;
    }

private:
    friend class detail::sealed_codec;
    sealed_footer(
      footer_expectation location,
      boundary_fields boundary,
      codec::extent_digest extent_digest,
      codec::immutable_object_digest digest,
      std::uint32_t retry_count,
      std::vector<page_ref>&& pages,
      model::file_byte_span extent) noexcept
      : location_(location)
      , boundary_(boundary)
      , extent_digest_(extent_digest)
      , digest_(digest)
      , retry_count_(retry_count)
      , pages_(std::move(pages))
      , extent_(extent) {}
    footer_expectation location_;
    boundary_fields boundary_;
    codec::extent_digest extent_digest_;
    codec::immutable_object_digest digest_;
    std::uint32_t retry_count_;
    std::vector<page_ref> pages_;
    model::file_byte_span extent_;
};
struct decoded_sealed_footer final {
    sealed_footer value;
    codec::decode_budget remaining;
};
struct encoded_sealed_footer final {
    bytes::fragmented_buffer bytes;
    codec::immutable_object_digest digest;
};

// Preserves independently supplied original logical coverage when data was
// removed. Requires finished SHA evidence, not a parsed tuple or bare hash.
[[nodiscard]] codec::result<void> validate_sealed_footer(
  const sealed_footer&, const verified_extent&, codec::field_context = {});

// Supplied refs have contiguous ordinals and entry indices. Their count, sum
// and capacity are checked; their named pages are not supplied to this call.
// Only the summary verifier establishes those pages' contents and cross-page
// ordering. No entry is evicted to make a root fit. The completed-request owner
// reserves summary capacity before accepting work. References and work stay
// alive and exclusive across this call. parent_remaining excludes their served
// storage and native/frame costs, and includes all new staging/output
// allocations.
[[nodiscard]] seastar::future<codec::result<encoded_sealed_footer>>
encode_sealed_footer(
  verified_extent,
  footer_expectation,
  std::uint32_t retry_count,
  std::span<const page_ref>,
  codec::cooperative_work&,
  byte_count parent_remaining,
  bytes::allocation_charge_fn,
  codec::field_context = {});

// Reserve the input once before entry. memory excludes that reservation and
// other live/native/frame costs. The returned residual charges only retained
// root metadata; temporary alias charges end before publication. Two free
// parent marks are required. Failure restores cursor/marks
// and drains staging. expected_digest must be independently pinned; deriving
// it from this untrusted input establishes no identity. No I/O or ACK proof.
// Fixed validation requires a 1024-byte / 64-item work quantum.
[[nodiscard]] seastar::future<codec::result<decoded_sealed_footer>>
decode_sealed_footer(
  bytes::fragmented_buffer_parser&,
  footer_expectation,
  codec::immutable_object_digest expected_digest,
  codec::decode_budget,
  codec::cooperative_work&,
  codec::field_context = {},
  codec::input_boundary = codec::input_boundary::open);
} // namespace kwaque::storage
