#pragma once

#include "src/protocol/control_codec.h"
#include "src/protocol/frame_codec.h"

namespace kwaque::protocol {
struct decoded_control_frame final {
    frame_header header;
    control value;
    codec::decode_budget remaining;
};

// One existing frame transaction: integrity, independently selected kind,
// exact control decoding, joined temporary cleanup, then final poll/commit.
// Expectations are checked before framing and never inferred from the wire.
// Uses decode_frame's input reservation, incremental and lifetime contracts.
// Raw kinds are invalid arguments; there is no trial payload dispatch.
[[nodiscard]] seastar::future<frame_read_result<decoded_control_frame>>
decode_control_frame(
  bytes::fragmented_buffer_parser&,
  frame_kind expected_kind,
  control_expectation,
  frame_extent_limits,
  codec::decode_budget,
  codec::cooperative_work&,
  codec::field_context = {},
  codec::input_boundary = codec::input_boundary::open);

// Borrow the immutable control through completion. Kind follows its concrete
// value and stream is zero; only connection-local correlation/sequence are
// supplied. other_live/parent_remaining have encode_control's contract and
// cover both serialization and frame assembly. Temporary payload and any
// abandoned frame are destroyed before failure or final publication.
[[nodiscard]] seastar::future<codec::result<bytes::fragmented_buffer>>
encode_control_frame(
  const control&,
  model::correlation_id,
  model::frame_sequence,
  codec::cooperative_work&,
  frame_extent_limits,
  codec::operation_usage other_live,
  byte_count parent_remaining,
  bytes::allocation_charge_fn,
  codec::field_context = {});
void encode_control_frame(
  const control&&,
  model::correlation_id,
  model::frame_sequence,
  codec::cooperative_work&,
  frame_extent_limits,
  codec::operation_usage,
  byte_count,
  bytes::allocation_charge_fn,
  codec::field_context = {}) = delete;
} // namespace kwaque::protocol
