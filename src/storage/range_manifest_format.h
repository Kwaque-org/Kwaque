#pragma once

#include "src/bytes/fragmented_buffer.h"
#include "src/codec/cooperative.h"
#include "src/codec/envelope_decode.h"
#include "src/codec/integer.h"
#include "src/storage/format_context.h"
#include "src/storage/page_ref.h"

#include <span>
#include <utility>
#include <vector>

namespace kwaque::storage {
class verified_extent;
namespace detail {
class range_manifest_codec;
}
inline constexpr byte_count range_manifest_root_fixed_bytes{88};
inline constexpr byte_count range_manifest_page_fixed_bytes{92};
inline constexpr byte_count range_manifest_entry_wire_bytes{104};

// One immutable manifest identity in a topic/range namespace. Its generation
// is independent of every referenced segment generation. No allocation of IDs,
// publication, cluster membership or authorization is established here.
class manifest_context final {
public:
    [[nodiscard]] static result<manifest_context> make(
      model::topic_id,
      model::range_id,
      model::manifest_id,
      model::range_manifest_generation) noexcept;
    [[nodiscard]] model::topic_id topic() const noexcept { return topic_; }
    [[nodiscard]] model::range_id range() const noexcept { return range_; }
    [[nodiscard]] model::manifest_id manifest() const noexcept {
        return manifest_;
    }
    [[nodiscard]] model::range_manifest_generation generation() const noexcept {
        return generation_;
    }
    [[nodiscard]] result<void>
    validate_expected(const manifest_context&) const noexcept;
    bool operator==(const manifest_context&) const noexcept = default;

private:
    manifest_context(
      model::topic_id topic,
      model::range_id range,
      model::manifest_id manifest,
      model::range_manifest_generation generation) noexcept
      : topic_(topic)
      , range_(range)
      , manifest_(manifest)
      , generation_(generation) {}
    model::topic_id topic_;
    model::range_id range_;
    model::manifest_id manifest_;
    model::range_manifest_generation generation_;
};

// Names an exact immutable extent. Logical coverage is nonempty, including
// when all records were removed. Physical/byte spans do not imply an affine
// logical mapping. The digest is a claimed value, not proof of supplied data.
// Cluster/topic/range membership and each segment's own alignment are checked
// against independent extent context by its owner, not the manifest's
// alignment.
class range_manifest_entry final {
public:
    [[nodiscard]] static result<range_manifest_entry> make(
      model::segment_id,
      model::segment_generation,
      storage::coverage,
      codec::extent_digest) noexcept;
    [[nodiscard]] model::segment_id segment() const noexcept {
        return segment_;
    }
    [[nodiscard]] model::segment_generation generation() const noexcept {
        return generation_;
    }
    [[nodiscard]] storage::coverage coverage() const noexcept {
        return coverage_;
    }
    [[nodiscard]] codec::extent_digest digest() const noexcept {
        return digest_;
    }
    bool operator==(const range_manifest_entry&) const noexcept = default;

private:
    range_manifest_entry(
      model::segment_id segment,
      model::segment_generation generation,
      storage::coverage coverage,
      codec::extent_digest digest) noexcept
      : segment_(segment)
      , generation_(generation)
      , coverage_(coverage)
      , digest_(digest) {}
    model::segment_id segment_;
    model::segment_generation generation_;
    storage::coverage coverage_;
    codec::extent_digest digest_;
};

// Immutable fixed-header claims, without an owning entry/ref array or a page
// digest pin. Factories check scalar shape/counts. Encoders additionally check
// the supplied tail. These values do not claim complete-object validation.
class range_manifest_root_header final {
public:
    [[nodiscard]] static result<range_manifest_root_header> make(
      manifest_context,
      model::range_logical_span,
      std::uint32_t entry_count,
      storage::page_count) noexcept;
    [[nodiscard]] manifest_context context() const noexcept { return context_; }
    [[nodiscard]] model::range_logical_span logical_span() const noexcept {
        return logical_;
    }
    [[nodiscard]] std::uint32_t entry_count() const noexcept { return count_; }
    [[nodiscard]] storage::page_count page_count() const noexcept {
        return pages_;
    }
    bool operator==(const range_manifest_root_header&) const noexcept = default;

private:
    range_manifest_root_header(
      manifest_context context,
      model::range_logical_span logical,
      std::uint32_t count,
      storage::page_count pages) noexcept
      : context_(context)
      , logical_(logical)
      , count_(count)
      , pages_(pages) {}
    manifest_context context_;
    model::range_logical_span logical_;
    std::uint32_t count_;
    storage::page_count pages_;
};

class range_manifest_page_header final {
public:
    [[nodiscard]] static result<range_manifest_page_header> make(
      manifest_context,
      model::range_logical_span,
      page_ordinal,
      std::uint32_t first_entry,
      std::uint32_t entry_count) noexcept;
    [[nodiscard]] manifest_context context() const noexcept { return context_; }
    [[nodiscard]] model::range_logical_span logical_span() const noexcept {
        return logical_;
    }
    [[nodiscard]] page_ordinal ordinal() const noexcept { return ordinal_; }
    [[nodiscard]] std::uint32_t first_entry() const noexcept { return first_; }
    [[nodiscard]] std::uint32_t entry_count() const noexcept { return count_; }
    bool operator==(const range_manifest_page_header&) const noexcept = default;

private:
    range_manifest_page_header(
      manifest_context context,
      model::range_logical_span logical,
      page_ordinal ordinal,
      std::uint32_t first,
      std::uint32_t count) noexcept
      : context_(context)
      , logical_(logical)
      , ordinal_(ordinal)
      , first_(first)
      , count_(count) {}
    manifest_context context_;
    model::range_logical_span logical_;
    page_ordinal ordinal_;
    std::uint32_t first_;
    std::uint32_t count_;
};

struct encoded_range_manifest_root final {
    bytes::fragmented_buffer bytes;
    codec::immutable_object_digest digest;
};
struct encoded_range_manifest_page final {
    bytes::fragmented_buffer bytes;
    page_ref reference;
};

// An independently digest-pinned metadata root. Only its bounded PageRefs
// remain allocated; referenced pages and extent bytes have not been supplied.
class range_manifest_root final {
public:
    range_manifest_root(range_manifest_root&&) noexcept = default;
    range_manifest_root& operator=(range_manifest_root&&) noexcept = default;
    range_manifest_root(const range_manifest_root&) = delete;
    range_manifest_root& operator=(const range_manifest_root&) = delete;
    [[nodiscard]] range_manifest_root_header header() const noexcept {
        return header_;
    }
    [[nodiscard]] manifest_context context() const noexcept {
        return header_.context();
    }
    [[nodiscard]] model::range_logical_span logical_span() const noexcept {
        return header_.logical_span();
    }
    [[nodiscard]] std::uint32_t entry_count() const noexcept {
        return header_.entry_count();
    }
    [[nodiscard]] storage_alignment alignment() const noexcept {
        return alignment_;
    }
    [[nodiscard]] codec::immutable_object_digest digest() const noexcept {
        return digest_;
    }
    [[nodiscard]] std::span<const page_ref> pages() const& noexcept {
        return pages_;
    }
    std::span<const page_ref> pages() const&& = delete;
    [[nodiscard]] std::size_t page_capacity() const noexcept {
        return pages_.capacity();
    }
    [[nodiscard]] byte_count encoded_bytes() const noexcept { return bytes_; }

private:
    friend class detail::range_manifest_codec;
    range_manifest_root(
      range_manifest_root_header header,
      storage_alignment alignment,
      codec::immutable_object_digest digest,
      std::vector<page_ref>&& pages,
      byte_count bytes) noexcept
      : header_(header)
      , alignment_(alignment)
      , digest_(digest)
      , pages_(std::move(pages))
      , bytes_(bytes) {}
    range_manifest_root_header header_;
    storage_alignment alignment_;
    codec::immutable_object_digest digest_;
    std::vector<page_ref> pages_;
    byte_count bytes_;
};

class range_manifest_page final {
public:
    range_manifest_page(range_manifest_page&&) noexcept = default;
    range_manifest_page& operator=(range_manifest_page&&) noexcept = default;
    range_manifest_page(const range_manifest_page&) = delete;
    range_manifest_page& operator=(const range_manifest_page&) = delete;
    [[nodiscard]] range_manifest_page_header header() const noexcept {
        return header_;
    }
    [[nodiscard]] page_ref reference() const noexcept { return reference_; }
    [[nodiscard]] std::span<const range_manifest_entry>
    entries() const& noexcept {
        return entries_;
    }
    std::span<const range_manifest_entry> entries() const&& = delete;
    [[nodiscard]] std::size_t entry_capacity() const noexcept {
        return entries_.capacity();
    }

private:
    friend class detail::range_manifest_codec;
    range_manifest_page(
      range_manifest_page_header header,
      page_ref reference,
      std::vector<range_manifest_entry>&& entries) noexcept
      : header_(header)
      , reference_(reference)
      , entries_(std::move(entries)) {}
    range_manifest_page_header header_;
    page_ref reference_;
    std::vector<range_manifest_entry> entries_;
};
struct decoded_range_manifest_root final {
    range_manifest_root value;
    codec::decode_budget remaining;
};
struct decoded_range_manifest_page final {
    range_manifest_page value;
    codec::decode_budget remaining;
};

// Bounded scalar comparison against SHA-bearing evidence for independently
// pinned extent bytes. expected_cluster is enclosing context, absent from MC.
// Checks the full namespace, segment generation, all spans and exact SHA;
// the evidence carries the data file's own alignment, independent of metadata
// alignment. No data fetch, persistence or publication is implied. A driver
// iterating entries must admit this work and retain at most one supplied proof.
[[nodiscard]] codec::result<void> validate_range_manifest_extent(
  manifest_context,
  model::cluster_id expected_cluster,
  const range_manifest_entry&,
  const verified_extent&,
  codec::field_context = {});

// memory excludes input backing/metadata and other live/root/native/frame
// reservations. Residuals debit only the returned vector after temporary
// aliases end. Keep input/work/abort and any borrowed root alive, unmoved and
// exclusive until return. Two free parent marks required; failure restores
// cursor/marks and drains staging. Same fixed quantum as encoding.
// The expected MC, retained logical interval and root digest are independent
// pins, not values recovered from this input. A standalone page is partial
// metadata; only the sequential verifier checks the complete partition.
[[nodiscard]] seastar::future<codec::result<decoded_range_manifest_root>>
decode_range_manifest_root(
  bytes::fragmented_buffer_parser&,
  manifest_context,
  model::range_logical_span expected_logical,
  storage_alignment,
  codec::immutable_object_digest expected_digest,
  codec::decode_budget,
  codec::cooperative_work&,
  codec::field_context = {},
  codec::input_boundary = codec::input_boundary::open);
[[nodiscard]] seastar::future<codec::result<decoded_range_manifest_page>>
decode_range_manifest_page(
  bytes::fragmented_buffer_parser&,
  const range_manifest_root&,
  page_ordinal,
  codec::decode_budget,
  codec::cooperative_work&,
  codec::field_context = {},
  codec::input_boundary = codec::input_boundary::open);

// Complete metadata partition only; does not authenticate referenced data.
class verified_range_manifest_pages final {
public:
    [[nodiscard]] codec::immutable_object_digest root_digest() const noexcept {
        return digest_;
    }
    [[nodiscard]] std::uint32_t entry_count() const noexcept { return count_; }

private:
    friend class range_manifest_verifier;
    verified_range_manifest_pages(
      codec::immutable_object_digest digest, std::uint32_t count) noexcept
      : digest_(digest)
      , count_(count) {}
    codec::immutable_object_digest digest_;
    std::uint32_t count_;
};

// Borrows a stable pinned root for its entire lifetime. One active call and
// one returned page at a time; drop or separately reserve that page before
// next(). No history retained. Every entered failure closes the walk and
// restores the current parser transaction. next() requires three free marks.
// Only finish() establishes all pages supplied and exact root-end coverage.
class range_manifest_verifier final {
public:
    range_manifest_verifier(
      const range_manifest_root& root, codec::limits policy) noexcept
      : root_(root)
      , policy_(policy)
      , end_(root.logical_span().begin()) {}
    range_manifest_verifier(range_manifest_root&&, codec::limits) = delete;
    range_manifest_verifier(const range_manifest_root&&, codec::limits)
      = delete;
    range_manifest_verifier(const range_manifest_verifier&) = delete;
    range_manifest_verifier& operator=(const range_manifest_verifier&) = delete;
    [[nodiscard]] seastar::future<codec::result<decoded_range_manifest_page>>
    next(
      bytes::fragmented_buffer_parser&,
      codec::decode_budget,
      codec::cooperative_work&,
      codec::field_context = {},
      codec::input_boundary = codec::input_boundary::open);
    [[nodiscard]] codec::result<verified_range_manifest_pages>
    finish(codec::cooperative_work&, codec::field_context = {});
    [[nodiscard]] bool closed() const noexcept {
        return state_ == state::closed;
    }

private:
    enum class state { open, active, closed };
    [[nodiscard]] codec::result<void>
    ready(codec::cooperative_work&, codec::field_context);
    const range_manifest_root& root_;
    codec::limits policy_;
    model::range_logical_end end_;
    std::uint32_t next_{0};
    state state_{state::open};
};

// Wire capacity including the actual header and alignment padding. Served
// allocation and enclosing operation limits are checked separately by owners.
[[nodiscard]] result<std::uint32_t> range_manifest_page_capacity(
  byte_count header_bytes, storage_alignment, const codec::limits&) noexcept;

// Current writers emit v1 bodies with the extension-free envelope header.
// alignment belongs to this metadata object, not the referenced data files.
// header is copied into the coroutine; the tail span remains borrowed, alive
// and unchanged until completion. remaining excludes its storage and all other
// live/native/frame reservations, and includes newly allocated output/staging.
// work and its abort source stay alive, unmoved and exclusive through the call.
// Validation precedes tail allocation; joined cleanup and a final abort check
// precede publication. Fixed work requires at least 1024 bytes / 64 items.
//
// Page entries must partition the declared logical span in ascending order;
// there is no implicit sorting, duplicate overwrite or full-object collection.
// Root PageRefs must have contiguous ordinals/entry indices and the exact
// total. Their pages and target data are not supplied to the root encoder, so
// cross-page coverage, extent integrity and whole-manifest proof are separate
// operations.
[[nodiscard]] seastar::future<codec::result<encoded_range_manifest_page>>
encode_range_manifest_page(
  range_manifest_page_header,
  std::span<const range_manifest_entry>,
  storage_alignment,
  codec::cooperative_work&,
  byte_count remaining,
  bytes::allocation_charge_fn,
  codec::field_context = {});
[[nodiscard]] seastar::future<codec::result<encoded_range_manifest_root>>
encode_range_manifest_root(
  range_manifest_root_header,
  std::span<const page_ref>,
  storage_alignment,
  codec::cooperative_work&,
  byte_count remaining,
  bytes::allocation_charge_fn,
  codec::field_context = {});
} // namespace kwaque::storage
