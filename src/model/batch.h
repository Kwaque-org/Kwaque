#pragma once

#include "src/base/error.h"
#include "src/base/result.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/codec/digest.h"
#include "src/model/batch_context.h"
#include "src/model/batch_identity.h"
#include "src/model/position.h"

#include <utility>

namespace kwaque::model {

class batch_builder;
namespace detail {
class batch_rewriter;
template<bool Assigned>
class batch_decoder;
} // namespace detail

// Validated original submission: dense canonical record bytes, immutable
// original context and the computed fingerprint. No assigned/global position
// or physical placement is implied. Construction and decoding verify the full
// original record stream and fingerprint before publishing this owner.
// Borrows end on move/replacement/destruction. Moving or extracting records
// empties the donor's byte storage; its scalar metadata remains readable, but
// consuming operations reject empty data. Reservations follow extracted/shared
// byte owners.
class submitted_batch final {
public:
    submitted_batch(const submitted_batch&) = delete;
    submitted_batch& operator=(const submitted_batch&) = delete;
    submitted_batch(submitted_batch&&) noexcept = default;
    submitted_batch& operator=(submitted_batch&&) noexcept = default;
    ~submitted_batch() = default;

    [[nodiscard]] submitted_batch_context context() const noexcept {
        return context_;
    }
    [[nodiscard]] codec::semantic_batch_digest fingerprint() const noexcept {
        return fingerprint_;
    }
    [[nodiscard]] item_count header_count() const noexcept { return headers_; }
    [[nodiscard]] const bytes::fragmented_buffer& records() const& noexcept {
        return records_;
    }
    const bytes::fragmented_buffer& records() const&& = delete;
    [[nodiscard]] bytes::fragmented_buffer release_records() && noexcept {
        return std::exchange(records_, bytes::fragmented_buffer{});
    }

private:
    friend class batch_builder;
    template<bool Assigned>
    friend class detail::batch_decoder;
    submitted_batch(
      submitted_batch_context context,
      codec::semantic_batch_digest fingerprint,
      item_count headers,
      bytes::fragmented_buffer&& records) noexcept
      : context_(context)
      , fingerprint_(fingerprint)
      , headers_(headers)
      , records_(std::move(records)) {}

    submitted_batch_context context_;
    codec::semantic_batch_digest fingerprint_;
    item_count headers_;
    bytes::fragmented_buffer records_;
};

// Validated dense or sparse data with its original logical coverage. Retained
// count describes these bytes; the original count/span/timestamp base and
// fingerprint continue to describe the original submission. A sparse persisted
// decode cannot recompute missing original content; its result reports that
// distinction separately. Assignment is not
// a durability/ACK proof and supplies no physical placement or broker counter.
// Borrows/extraction have the same lifetime and reservation rules as
// submission.
class assigned_batch final {
public:
    // Pure checked transfer: no allocation, sharing, hashing or byte rewrite.
    // expected_binding is independently pinned original append context. A
    // rejection leaves the donor intact; successful transfer consumes it.
    [[nodiscard]] static result<assigned_batch> assign(
      submitted_batch&& submitted,
      range_logical_end base,
      const producer_stream_binding& expected_binding) noexcept {
        if (submitted.records().empty()) return failure(errc::invalid_argument);
        const auto context = assigned_batch_context::assign(
          submitted.context(), base, expected_binding);
        if (!context) return failure(context.error());
        return assigned_batch{
          *context,
          submitted.fingerprint(),
          submitted.header_count(),
          std::move(submitted).release_records()};
    }

    assigned_batch(const assigned_batch&) = delete;
    assigned_batch& operator=(const assigned_batch&) = delete;
    assigned_batch(assigned_batch&&) noexcept = default;
    assigned_batch& operator=(assigned_batch&&) noexcept = default;
    ~assigned_batch() = default;

    [[nodiscard]] assigned_batch_context context() const noexcept {
        return context_;
    }
    [[nodiscard]] codec::semantic_batch_digest fingerprint() const noexcept {
        return fingerprint_;
    }
    [[nodiscard]] item_count header_count() const noexcept { return headers_; }
    [[nodiscard]] const bytes::fragmented_buffer& records() const& noexcept {
        return records_;
    }
    const bytes::fragmented_buffer& records() const&& = delete;
    [[nodiscard]] bytes::fragmented_buffer release_records() && noexcept {
        return std::exchange(records_, bytes::fragmented_buffer{});
    }

private:
    friend class detail::batch_rewriter;
    template<bool Assigned>
    friend class detail::batch_decoder;
    assigned_batch(
      assigned_batch_context context,
      codec::semantic_batch_digest fingerprint,
      item_count headers,
      bytes::fragmented_buffer&& records) noexcept
      : context_(context)
      , fingerprint_(fingerprint)
      , headers_(headers)
      , records_(std::move(records)) {}

    assigned_batch_context context_;
    codec::semantic_batch_digest fingerprint_;
    item_count headers_;
    bytes::fragmented_buffer records_;
};

} // namespace kwaque::model
