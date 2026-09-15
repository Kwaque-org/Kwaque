#pragma once

#include "src/storage/completed_retry.h"
#include "src/storage/sealed_format.h"

namespace kwaque::storage {
namespace detail {
class retry_codec;
}
inline constexpr byte_count retry_page_fixed_bytes{100};

// Intersects the complete-page byte cap (including actual header and padding),
// per-object entry policy and the fixed entry width. Allocation/metadata caps
// are separate and are admitted by each owner before reserve or staging.
[[nodiscard]] result<std::uint32_t> retry_page_capacity(
  byte_count header_bytes, storage_alignment, const codec::limits&) noexcept;

class retry_page final {
public:
    retry_page(retry_page&&) noexcept = default;
    retry_page& operator=(retry_page&&) noexcept = default;
    retry_page(const retry_page&) = delete;
    retry_page& operator=(const retry_page&) = delete;
    [[nodiscard]] page_ref reference() const noexcept { return reference_; }
    [[nodiscard]] std::span<const completed_retry> entries() const& noexcept {
        return entries_;
    }
    std::span<const completed_retry> entries() const&& = delete;
    [[nodiscard]] std::size_t entry_capacity() const noexcept {
        return entries_.capacity();
    }

private:
    friend class detail::retry_codec;
    retry_page(page_ref ref, std::vector<completed_retry>&& entries) noexcept
      : reference_(ref)
      , entries_(std::move(entries)) {}
    page_ref reference_;
    std::vector<completed_retry> entries_;
};
struct decoded_retry_page final {
    retry_page value;
    codec::decode_budget remaining;
};
struct encoded_retry_page final {
    bytes::fragmented_buffer bytes;
    page_ref reference;
};

// No whole-summary vector. Input is one bounded page, sorted by full BID.
// Root identity is known before encoding pages; pages never include the root
// digest. Returned reference hashes every encoded byte, including padding.
// Input span/work remain alive and exclusive; remaining excludes their served
// storage plus native/frame costs and includes all new staging/output.
[[nodiscard]] seastar::future<codec::result<encoded_retry_page>>
encode_retry_page(
  std::span<const completed_retry>,
  footer_expectation root,
  page_ordinal,
  std::uint32_t first_entry,
  codec::cooperative_work&,
  byte_count parent_remaining,
  bytes::allocation_charge_fn,
  codec::field_context = {});

// Verifies one exact page against its reference in an independently pinned
// root. This checks only within-page ordering. The summary walker below also
// checks page order and keys across page edges. Input reservation, returned
// metadata residual, two free marks and work/lifetime contracts match root
// decoding. Only retained entry metadata is charged in the returned residual.
// Both codecs require a 1024-byte / 64-item fixed work quantum.
[[nodiscard]] seastar::future<codec::result<decoded_retry_page>>
decode_retry_page(
  bytes::fragmented_buffer_parser&,
  const sealed_footer&,
  page_ordinal,
  codec::decode_budget,
  codec::cooperative_work&,
  codec::field_context = {},
  codec::input_boundary = codec::input_boundary::open);

class verified_retry_summary final {
public:
    [[nodiscard]] codec::immutable_object_digest root_digest() const noexcept {
        return digest_;
    }
    [[nodiscard]] std::uint32_t entry_count() const noexcept { return count_; }

private:
    friend class retry_summary_verifier;
    verified_retry_summary(
      codec::immutable_object_digest digest, std::uint32_t count) noexcept
      : digest_(digest)
      , count_(count) {}
    codec::immutable_object_digest digest_;
    std::uint32_t count_;
};

// Borrows one immutable pinned root for the entire lifetime; keep it alive and
// unmoved. One active call, one returned page at a time. Caller drops that page
// or reserves its continuing cost before next(). No decoded history is kept.
// Every entered error/exception/abort closes the walker.
// next() requires three free parser marks, including its outer rollback.
// Earlier successful pages remain partial results; only finish() yields
// whole-summary evidence.
class retry_summary_verifier final {
public:
    retry_summary_verifier(
      const sealed_footer& root, codec::limits policy) noexcept
      : root_(root)
      , policy_(policy) {}
    retry_summary_verifier(sealed_footer&&, codec::limits) = delete;
    retry_summary_verifier(const sealed_footer&&, codec::limits) = delete;
    retry_summary_verifier(const retry_summary_verifier&) = delete;
    retry_summary_verifier& operator=(const retry_summary_verifier&) = delete;
    [[nodiscard]] seastar::future<codec::result<decoded_retry_page>> next(
      bytes::fragmented_buffer_parser&,
      codec::decode_budget,
      codec::cooperative_work&,
      codec::field_context = {},
      codec::input_boundary = codec::input_boundary::open);
    [[nodiscard]] codec::result<verified_retry_summary>
    finish(codec::cooperative_work&, codec::field_context = {});
    [[nodiscard]] bool closed() const noexcept {
        return state_ == state::closed;
    }

private:
    enum class state { open, active, closed };
    [[nodiscard]] codec::result<void>
    ready(codec::cooperative_work&, codec::field_context);
    const sealed_footer& root_;
    codec::limits policy_;
    std::optional<model::batch_id> last_;
    std::uint32_t next_{0};
    state state_{state::open};
};
} // namespace kwaque::storage
