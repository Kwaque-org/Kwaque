#pragma once

#include "src/codec/envelope_decode.h"
#include "src/storage/format_context.h"
#include "src/storage/page_ref.h"

#include <optional>
#include <span>
#include <utility>
#include <vector>

namespace kwaque::storage {
class complete_block_descriptor;
class verified_extent;
namespace detail {
class sparse_index_codec;
}

inline constexpr byte_count sparse_index_root_fixed_bytes{168};
inline constexpr byte_count sparse_index_page_fixed_bytes{172};
inline constexpr byte_count sparse_index_entry_wire_bytes{16};

// Independently pinned target extent and layout. This checked representation
// does not establish that data was supplied or persisted. There is no index
// file position; diagnostic parser origins do not identify physical placement.
class sparse_index_context final {
public:
    [[nodiscard]] static result<sparse_index_context> make(
      segment_context,
      storage::coverage,
      codec::extent_digest,
      storage_alignment,
      storage_profile = storage_profile::v1) noexcept;
    [[nodiscard]] segment_context segment() const noexcept { return segment_; }
    [[nodiscard]] storage::coverage coverage() const noexcept {
        return coverage_;
    }
    [[nodiscard]] codec::extent_digest digest() const noexcept {
        return digest_;
    }
    [[nodiscard]] storage_alignment alignment() const noexcept {
        return alignment_;
    }
    [[nodiscard]] storage_profile profile() const noexcept { return profile_; }
    bool operator==(const sparse_index_context&) const noexcept = default;

private:
    sparse_index_context(
      segment_context segment,
      storage::coverage coverage,
      codec::extent_digest digest,
      storage_alignment alignment,
      storage_profile profile) noexcept
      : segment_(segment)
      , coverage_(coverage)
      , digest_(digest)
      , alignment_(alignment)
      , profile_(profile) {}
    segment_context segment_;
    storage::coverage coverage_;
    codec::extent_digest digest_;
    storage_alignment alignment_;
    storage_profile profile_;
};

// Absolute original logical batch base and complete block-envelope start.
// Construction preserves domains; contextual containment/order and actual
// block membership are separate checks. No affine mapping or stride is implied.
class sparse_index_entry final {
public:
    sparse_index_entry(
      model::range_logical_offset anchor,
      runtime::file_position position) noexcept
      : anchor_(anchor)
      , position_(position) {}
    [[nodiscard]] model::range_logical_offset logical_anchor() const noexcept {
        return anchor_;
    }
    [[nodiscard]] runtime::file_position block_position() const noexcept {
        return position_;
    }
    bool operator==(const sparse_index_entry&) const noexcept = default;

private:
    model::range_logical_offset anchor_;
    runtime::file_position position_;
};

// One exact-digest-pinned root; its named pages and target data are not
// verified by parsing this envelope. Only its at-most-256 PageRefs remain
// allocated.
class sparse_index_root final {
public:
    sparse_index_root(sparse_index_root&&) noexcept = default;
    sparse_index_root& operator=(sparse_index_root&&) noexcept = default;
    sparse_index_root(const sparse_index_root&) = delete;
    sparse_index_root& operator=(const sparse_index_root&) = delete;
    [[nodiscard]] sparse_index_context context() const noexcept {
        return context_;
    }
    [[nodiscard]] codec::immutable_object_digest digest() const noexcept {
        return digest_;
    }
    [[nodiscard]] std::uint32_t entry_count() const noexcept { return count_; }
    [[nodiscard]] std::span<const page_ref> pages() const& noexcept {
        return pages_;
    }
    std::span<const page_ref> pages() const&& = delete;
    [[nodiscard]] std::size_t page_capacity() const noexcept {
        return pages_.capacity();
    }
    [[nodiscard]] byte_count encoded_bytes() const noexcept { return bytes_; }

private:
    friend class detail::sparse_index_codec;
    sparse_index_root(
      sparse_index_context context,
      codec::immutable_object_digest digest,
      std::uint32_t count,
      std::vector<page_ref>&& pages,
      byte_count bytes) noexcept
      : context_(context)
      , digest_(digest)
      , count_(count)
      , pages_(std::move(pages))
      , bytes_(bytes) {}
    sparse_index_context context_;
    codec::immutable_object_digest digest_;
    std::uint32_t count_;
    std::vector<page_ref> pages_;
    byte_count bytes_;
};

class sparse_index_page final {
public:
    sparse_index_page(sparse_index_page&&) noexcept = default;
    sparse_index_page& operator=(sparse_index_page&&) noexcept = default;
    sparse_index_page(const sparse_index_page&) = delete;
    sparse_index_page& operator=(const sparse_index_page&) = delete;
    [[nodiscard]] page_ref reference() const noexcept { return reference_; }
    [[nodiscard]] std::span<const sparse_index_entry>
    entries() const& noexcept {
        return entries_;
    }
    std::span<const sparse_index_entry> entries() const&& = delete;
    [[nodiscard]] std::size_t entry_capacity() const noexcept {
        return entries_.capacity();
    }

private:
    friend class detail::sparse_index_codec;
    sparse_index_page(
      page_ref ref, std::vector<sparse_index_entry>&& entries) noexcept
      : reference_(ref)
      , entries_(std::move(entries)) {}
    page_ref reference_;
    std::vector<sparse_index_entry> entries_;
};

struct decoded_sparse_index_root final {
    sparse_index_root value;
    codec::decode_budget remaining;
};
struct decoded_sparse_index_page final {
    sparse_index_page value;
    codec::decode_budget remaining;
};
struct encoded_sparse_index_root final {
    bytes::fragmented_buffer bytes;
    codec::immutable_object_digest digest;
};
struct encoded_sparse_index_page final {
    bytes::fragmented_buffer bytes;
    page_ref reference;
};

[[nodiscard]] result<std::uint32_t> sparse_index_page_capacity(
  byte_count header_bytes, storage_alignment, const codec::limits&) noexcept;

// These bounded synchronous leaves read no data. The caller supplies evidence
// for the same pinned immutable extent. Visit page entries and their matching
// descriptors in order, one at a time; no descriptor-history vector is needed.
// A descriptor cannot authenticate replacement bytes at the same coordinates.
// An outer cooperative driver admits these fixed scalar comparisons when
// iterating over entries; neither leaf performs an unbounded traversal.
[[nodiscard]] codec::result<void> validate_sparse_index_anchor(
  const sparse_index_context&,
  const sparse_index_entry&,
  const complete_block_descriptor&,
  codec::field_context = {});
[[nodiscard]] codec::result<void> validate_sparse_index_extent(
  const sparse_index_root&, const verified_extent&, codec::field_context = {});

// Borrow one ordered page or at most 256 root refs until joined completion.
// parent_remaining excludes those inputs and other live/native/frame costs;
// new output/staging is admitted here. work and abort stay alive and exclusive.
// These are representation writers; they do not select stride or prove that
// referenced pages/data were supplied. Exact hashes cover complete envelopes.
// Fixed validation needs a 1024-byte / 64-item work quantum.
// Page zero starts at entry zero; each preceding nonempty page accounts for
// at least one earlier entry. Complete PageRef topology is checked by the root.
[[nodiscard]] seastar::future<codec::result<encoded_sparse_index_page>>
encode_sparse_index_page(
  std::span<const sparse_index_entry>,
  sparse_index_context,
  page_ordinal,
  std::uint32_t first_entry,
  codec::cooperative_work&,
  byte_count parent_remaining,
  bytes::allocation_charge_fn,
  codec::field_context = {});
[[nodiscard]] seastar::future<codec::result<encoded_sparse_index_root>>
encode_sparse_index_root(
  sparse_index_context,
  std::uint32_t entry_count,
  std::span<const page_ref>,
  codec::cooperative_work&,
  byte_count parent_remaining,
  bytes::allocation_charge_fn,
  codec::field_context = {});

// Reserve input backing/metadata once before entry. memory excludes that and
// other live/root/native/frame costs. The residual charges only the returned
// vector; temporary aliases end before publication. Two parent marks required.
// Input, work, abort and any borrowed root stay alive/unmoved/exclusive until
// return. Failure restores cursor/marks and drains staging. Same fixed quantum
// as encoding. expected_digest comes from an independent pin, not this input.
[[nodiscard]] seastar::future<codec::result<decoded_sparse_index_root>>
decode_sparse_index_root(
  bytes::fragmented_buffer_parser&,
  sparse_index_context,
  codec::immutable_object_digest expected_digest,
  codec::decode_budget,
  codec::cooperative_work&,
  codec::field_context = {},
  codec::input_boundary = codec::input_boundary::open);
[[nodiscard]] seastar::future<codec::result<decoded_sparse_index_page>>
decode_sparse_index_page(
  bytes::fragmented_buffer_parser&,
  const sparse_index_root&,
  page_ordinal,
  codec::decode_budget,
  codec::cooperative_work&,
  codec::field_context = {},
  codec::input_boundary = codec::input_boundary::open);

// Evidence for complete metadata-page ordering, not target data or persistence.
class verified_sparse_index_pages final {
public:
    [[nodiscard]] codec::immutable_object_digest root_digest() const noexcept {
        return digest_;
    }
    [[nodiscard]] std::uint32_t entry_count() const noexcept { return count_; }

private:
    friend class sparse_index_verifier;
    verified_sparse_index_pages(
      codec::immutable_object_digest digest, std::uint32_t count) noexcept
      : digest_(digest)
      , count_(count) {}
    codec::immutable_object_digest digest_;
    std::uint32_t count_;
};

// Borrows a pinned root for its entire lifetime. Keep it alive and unmoved.
// One active call and one returned page at a time; drop or separately reserve
// that page before next(). No history is retained. Every entered failure closes
// the walk. next() needs three free parser marks. Only finish() proves that all
// pages were supplied; previously returned pages remain partial results.
class sparse_index_verifier final {
public:
    sparse_index_verifier(
      const sparse_index_root& root, codec::limits policy) noexcept
      : root_(root)
      , policy_(policy) {}
    sparse_index_verifier(sparse_index_root&&, codec::limits) = delete;
    sparse_index_verifier(const sparse_index_root&&, codec::limits) = delete;
    sparse_index_verifier(const sparse_index_verifier&) = delete;
    sparse_index_verifier& operator=(const sparse_index_verifier&) = delete;
    [[nodiscard]] seastar::future<codec::result<decoded_sparse_index_page>>
    next(
      bytes::fragmented_buffer_parser&,
      codec::decode_budget,
      codec::cooperative_work&,
      codec::field_context = {},
      codec::input_boundary = codec::input_boundary::open);
    [[nodiscard]] codec::result<verified_sparse_index_pages>
    finish(codec::cooperative_work&, codec::field_context = {});
    [[nodiscard]] bool closed() const noexcept {
        return state_ == state::closed;
    }

private:
    enum class state { open, active, closed };
    [[nodiscard]] codec::result<void>
    ready(codec::cooperative_work&, codec::field_context);
    const sparse_index_root& root_;
    codec::limits policy_;
    std::optional<sparse_index_entry> last_;
    std::uint32_t next_{0};
    state state_{state::open};
};
} // namespace kwaque::storage
