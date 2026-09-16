#pragma once

#include "src/model/batch_codec.h"
#include "src/protocol/frame_codec.h"

namespace kwaque::protocol {

struct decoded_submitted_frame final {
    frame_header header;
    model::submitted_batch batch;
    codec::decode_budget remaining;
};

struct decoded_assigned_frame final {
    frame_header header;
    model::assigned_batch batch;
    codec::decode_budget remaining;
    model::batch_fingerprint_verification fingerprint_verification;
};

// Same one-frame transaction, input reservation, incremental, exception and
// work lifetime contract as decode_frame. The exact complete payload is decoded
// as one submitted/assigned envelope BEFORE the caller's frame commits. A
// wrong kind/family/context, inner shortage or trailing bytes restores the
// entire frame position. There is no trial dispatch, outer compression or
// Protobuf fallback; the batch codec owns none/LZ4 and semantic validation.
//
// Expectations are independently supplied topic/range and optional original
// append/retry context, never inferred from transport IDs or received bytes.
// Input/expected coordinate errors reject before framing. Returned residuals
// retain only the batch's record metadata and expanded backing after temporary
// payload-parser cleanup. Sparse assigned verification remains carried rather
// than claiming recomputation of absent original content. These are checked
// values, not completion, routing or producer authorization evidence.
[[nodiscard]] seastar::future<frame_read_result<decoded_submitted_frame>>
decode_submitted_frame(
  bytes::fragmented_buffer_parser& input,
  model::batch_decode_expectation expected,
  frame_extent_limits owner_limits,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context = {},
  codec::input_boundary boundary = codec::input_boundary::open);

[[nodiscard]] seastar::future<frame_read_result<decoded_assigned_frame>>
decode_assigned_frame(
  bytes::fragmented_buffer_parser& input,
  model::batch_decode_expectation expected,
  frame_extent_limits owner_limits,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context = {},
  codec::input_boundary boundary = codec::input_boundary::open);

} // namespace kwaque::protocol
