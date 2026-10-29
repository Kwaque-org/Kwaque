#pragma once

#include "src/codec/digest.h"
#include "src/codec/limits.h"
#include "src/model/batch_identity.h"
#include "src/model/position.h"

#include <utility>

namespace kwaque::storage {

inline constexpr byte_count completed_retry_wire_bytes{160};

// One producer stream of one epoch: every batch identity of the stream
// shares it, and only the sequence tells its batches apart.
struct retry_prefix final {
    model::producer_id producer;
    model::producer_epoch epoch;
    model::producer_stream_id stream;

    [[nodiscard]] static retry_prefix of(const model::batch_id& id) noexcept {
        return {id.producer(), id.epoch(), id.stream()};
    }
    // The order of their batch identities, as model::batch_id::canonical_less.
    [[nodiscard]] bool
    canonical_less(const retry_prefix& other) const noexcept {
        if (producer != other.producer)
            return producer.canonical_less(other.producer);
        if (epoch != other.epoch) return epoch < other.epoch;
        return stream < other.stream;
    }
    bool operator==(const retry_prefix&) const noexcept = default;
    template<typename H>
    friend H AbslHashValue(H state, const retry_prefix& value) {
        return H::combine(
          std::move(state), value.producer, value.epoch, value.stream);
    }
};

// Persisted completed-result representation. A completed fact describes a
// batch inside a decided boundary of its segment generation: the end a live
// seal takes once its appends drained, the end a recovered seal's decision
// fixed, or the reported boundary a failed segment is sealed at. That
// decision commits every batch inside it, acknowledged or not, so the fact is
// derived from the batch itself: its identity, original binding, semantic
// digest and assigned span. While a segment is appended to, a footer or
// PREPARE alone proves nothing about commitment: facts are made only for the
// reported prefix, and the seal decides which stand. A segment keeps the last
// R of each stream; there is no clock and no other eviction.
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
