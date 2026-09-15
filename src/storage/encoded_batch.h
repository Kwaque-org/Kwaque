#pragma once

#include "src/model/batch_codec.h"

#include <utility>

namespace kwaque::storage {
namespace detail {
class encoded_batch_codec;
}

struct assigned_batch_info final {
    model::assigned_batch_context context;
    codec::semantic_batch_digest fingerprint;
    item_count header_count;
    model::batch_fingerprint_verification verification;
    bool operator==(const assigned_batch_info&) const noexcept = default;
};

// Exactly one validated family-2 envelope. Compression and optional extensions
// are retained verbatim. Metadata is never independently attached to arbitrary
// bytes. This is format evidence, not persistence or completed-request
// evidence. Moves/extraction empty the byte owner; scalar metadata stays
// readable. Borrows end on move/destruction. Consuming operations reject an
// extracted owner.
class encoded_assigned_batch final {
public:
    encoded_assigned_batch(const encoded_assigned_batch&) = delete;
    encoded_assigned_batch& operator=(const encoded_assigned_batch&) = delete;
    encoded_assigned_batch(encoded_assigned_batch&&) noexcept = default;
    encoded_assigned_batch&
    operator=(encoded_assigned_batch&&) noexcept = default;
    [[nodiscard]] assigned_batch_info info() const noexcept { return info_; }
    [[nodiscard]] const kwaque::bytes::fragmented_buffer&
    bytes() const& noexcept {
        return bytes_;
    }
    const kwaque::bytes::fragmented_buffer& bytes() const&& = delete;
    [[nodiscard]] kwaque::bytes::fragmented_buffer release_bytes() && noexcept {
        return std::exchange(bytes_, kwaque::bytes::fragmented_buffer{});
    }

    // Borrow this owner exclusively through joined completion. memory excludes
    // its backing/descriptors/promotion and other live/native/frame costs.
    // Check current expectations and resource bounds every time. Reuse semantic
    // validation only for no-stricter limits; otherwise decode these exact
    // bytes under the new policy. Temporary raw records/parser are drained on
    // all exits.
    [[nodiscard]] seastar::future<codec::result<void>> validate(
      model::batch_decode_expectation expected,
      codec::decode_budget memory,
      codec::cooperative_work& work,
      codec::field_context context = {});

    // Same borrow/reservation contract. Only additional alias descriptors are
    // charged; the source backing reservation must outlive BOTH owners.
    [[nodiscard]] seastar::future<codec::result<encoded_assigned_batch>> share(
      codec::decode_budget memory,
      codec::cooperative_work& work,
      codec::field_context context = {});

private:
    friend class detail::encoded_batch_codec;
    encoded_assigned_batch(
      kwaque::bytes::fragmented_buffer bytes,
      assigned_batch_info info,
      codec::limits validated) noexcept
      : bytes_(std::move(bytes))
      , info_(info)
      , validated_(validated) {}
    kwaque::bytes::fragmented_buffer bytes_;
    assigned_batch_info info_;
    codec::limits validated_;
};

// Consumes before the first await, including failure/abort. Outer coroutine
// allocation failure precedes transfer. Input is already reserved exactly once;
// memory excludes that reservation and verified native/frame/other live costs.
// The reservation follows the returned exact bytes; temporary expansion dies
// before the final abort poll/publication. No input-budget refund is implied.
[[nodiscard]] seastar::future<codec::result<encoded_assigned_batch>>
validate_encoded_assigned_batch(
  kwaque::bytes::fragmented_buffer&& input,
  model::batch_decode_expectation expected,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context = {});

// Encode a checked raw owner without a redundant default-policy decode. A
// narrower record/header policy requires validation of the resulting bytes.
// parent_remaining includes the consumed raw owner and all new staging; other
// live/native/frame reservations are excluded. Work/abort/charge retain the
// model writer's stable, exclusive lifetime contract. No recompression
// fallback.
[[nodiscard]] seastar::future<codec::result<encoded_assigned_batch>>
make_encoded_assigned_batch(
  model::assigned_batch&& input,
  compression::codec_id encoding,
  codec::cooperative_work& work,
  byte_count parent_remaining,
  kwaque::bytes::allocation_charge_fn charge,
  codec::field_context context = {});
} // namespace kwaque::storage
