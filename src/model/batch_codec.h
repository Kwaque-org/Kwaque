#pragma once

#include "src/codec/envelope_encode.h"
#include "src/codec/transaction.h"
#include "src/model/batch.h"

#include <cstdint>
#include <optional>

namespace kwaque::model {

inline constexpr byte_count submitted_batch_fixed_bytes{168};
inline constexpr byte_count assigned_batch_fixed_bytes{184};

enum class batch_field : std::uint16_t {
    fixed_body = 95,
    producer = 96,
    producer_epoch = 97,
    producer_stream = 98,
    sequence = 99,
    topic = 100,
    range = 101,
    routing_epoch = 102,
    original_segment = 103,
    original_generation = 104,
    fingerprint = 105,
    timestamp_base = 106,
    original_count = 107,
    retained_count = 108,
    header_count = 109,
    codec = 110,
    reserved = 111,
    record_profile = 112,
    encoded_record_bytes = 113,
    expanded_record_bytes = 114,
    logical_begin = 115,
    logical_end = 116,
    records = 117,
};

// Topic/range must be independently known and nonnil. Optional expectations
// come from original append/retry context, never from decoded bytes. A current
// physical segment/generation is not an expectation for the original binding.
// A supplied original binding must agree with the required topic/range.
struct batch_decode_expectation final {
    topic_id topic;
    range_id range;
    std::optional<batch_id> id;
    std::optional<producer_stream_binding> original_binding;
    std::optional<codec::semantic_batch_digest> fingerprint;
};

enum class batch_fingerprint_verification : std::uint8_t {
    recomputed,
    carried,
};

struct decoded_submitted_batch final {
    submitted_batch value;
    codec::decode_budget remaining;
};

struct decoded_assigned_batch final {
    assigned_batch value;
    codec::decode_budget remaining;
    // Describes this decode, not a completion/authorization certificate. Sparse
    // data cannot prove absent original content or earlier survivor membership.
    batch_fingerprint_verification fingerprint_verification;
};

// Consumes the validated submitted owner before the first await, on rejection,
// exception and cancellation too (outer frame allocation failure precedes
// entry). Final context/digest/count/length bytes precede record bytes in the
// body. The existing envelope writer computes body CRC before header CRC and
// publishes only complete output. This operation never recomputes a different
// projection.
//
// parent_remaining excludes other live inputs, verified SHA/CRC/native/frame
// and opaque reservations, but includes this batch's backing/descriptors and
// all new prefix/assembly/envelope staging. Work and abort remain exclusively
// alive and unmoved until completion. Cleanup is joined before final polling.
[[nodiscard]] seastar::future<codec::result<bytes::fragmented_buffer>>
encode_submitted_batch(
  submitted_batch&& batch,
  codec::cooperative_work& work,
  byte_count parent_remaining,
  bytes::allocation_charge_fn charge,
  codec::field_context context = {});

// Same ownership, work and reservation contract as encode_submitted_batch.
// Retained counts/bytes may be sparse; original context/digest stay unchanged.
// Logical begin/end follow the 168-byte context in the body and participate in
// fresh envelope checksums. No physical wrapper or retry result is constructed.
[[nodiscard]] seastar::future<codec::result<bytes::fragmented_buffer>>
encode_assigned_batch(
  assigned_batch&& batch,
  codec::cooperative_work& work,
  byte_count parent_remaining,
  bytes::allocation_charge_fn charge,
  codec::field_context context = {});

// Decode exactly one envelope. Integrity and family precede body parsing; all
// fixed fields, counts, records, timestamps and final bytes are checked before
// publication. Submitted and dense assigned data recompute the original SHA.
// Sparse assigned data validates current bytes/context and carries its digest,
// including an independently supplied fingerprint expectation when present.
// Returned model values retain known body semantics, not optional envelope
// extensions or exact-object byte identity; the enclosing input owner keeps
// those.
//
// Reserve this parent's backing/descriptors/promotion ONCE before entry using
// reserve_decode_input. memory is the residual after that reservation and
// verified SHA/CRC/native/frame/callback/opaque and other live costs. Each
// alias is admitted before allocation. remaining reserves only the returned
// record-region metadata; temporary body/record aliases are gone before the
// envelope commits. Keep the parent backing reservation until ALL resulting
// aliases are freed, even after the parser is destroyed. Pass remaining while
// retaining another result; there is no automatic reservation/refund service.
//
// Expected values and input coordinates are checked before entering framing.
// Parent/work/abort remain alive, unmoved and exclusive through completion.
// Errors/exceptions/cancellation leave the parent position and marks unchanged.
// Source/temporary/result cleanup is joined before the final poll and commit.
[[nodiscard]] seastar::future<codec::result<decoded_submitted_batch>>
decode_submitted_batch(
  bytes::fragmented_buffer_parser& input,
  batch_decode_expectation expected,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context = {},
  codec::input_boundary boundary = codec::input_boundary::open);

[[nodiscard]] seastar::future<codec::result<decoded_assigned_batch>>
decode_assigned_batch(
  bytes::fragmented_buffer_parser& input,
  batch_decode_expectation expected,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context = {},
  codec::input_boundary boundary = codec::input_boundary::open);

} // namespace kwaque::model
