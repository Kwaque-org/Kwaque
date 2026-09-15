#pragma once

#include "src/codec/digest.h"
#include "src/codec/limits.h"
#include "src/model/batch_identity.h"
#include "src/model/position.h"

namespace kwaque::storage {

inline constexpr byte_count completed_retry_wire_bytes{160};

// Persisted completed-result representation. Only the completed-request owner
// may supply these facts: parsing a batch, PREPARE or footer does not establish
// completion. There is intentionally no pending state, clock or eviction rule.
class completed_retry final {
public:
    [[nodiscard]] static result<completed_retry> make(
      model::batch_id id,
      codec::semantic_batch_digest submitted_digest,
      model::producer_stream_binding original_binding,
      model::range_logical_span returned_span,
      model::segment_generation ack_generation) noexcept {
        if (
          returned_span.empty()
          || ack_generation != original_binding.generation())
            return failure(errc::invalid_argument);
        if (
          returned_span.count().value() > codec::absolute_max_original_records)
            return failure(errc::resource_exhausted);
        return completed_retry{
          id,
          submitted_digest,
          original_binding,
          returned_span,
          ack_generation};
    }
    [[nodiscard]] model::batch_id id() const noexcept { return id_; }
    [[nodiscard]] codec::semantic_batch_digest
    submitted_digest() const noexcept {
        return submitted_digest_;
    }
    [[nodiscard]] model::producer_stream_binding
    original_binding() const noexcept {
        return original_binding_;
    }
    [[nodiscard]] model::range_logical_span returned_span() const noexcept {
        return returned_span_;
    }
    [[nodiscard]] model::segment_generation ack_generation() const noexcept {
        return ack_generation_;
    }
    bool operator==(const completed_retry&) const noexcept = default;

private:
    completed_retry(
      model::batch_id id,
      codec::semantic_batch_digest digest,
      model::producer_stream_binding binding,
      model::range_logical_span span,
      model::segment_generation generation) noexcept
      : id_(id)
      , submitted_digest_(digest)
      , original_binding_(binding)
      , returned_span_(span)
      , ack_generation_(generation) {}
    model::batch_id id_;
    codec::semantic_batch_digest submitted_digest_;
    model::producer_stream_binding original_binding_;
    model::range_logical_span returned_span_;
    model::segment_generation ack_generation_;
};
} // namespace kwaque::storage
