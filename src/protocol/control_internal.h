#pragma once

#include "src/protocol/control_codec.h"

namespace kwaque::protocol::detail {
struct control_access final {
    static control finish(control_data&& data) noexcept {
        return control{std::move(data)};
    }
};
struct control_layout final {
    frame_kind kind;
    byte_count wire_bytes;
    byte_count storage;
    item_count units;
};

// Private composition state, not a transferable validation token.
// The same immutable owner must survive inspection and serialization.
seastar::future<codec::result<control_layout>> inspect_control_value(
  const control_data&,
  codec::cooperative_work&,
  bytes::allocation_charge_fn,
  codec::field_context);
seastar::future<codec::result<bytes::fragmented_buffer>> encode_control_checked(
  const control_data&,
  control_layout,
  codec::cooperative_work&,
  codec::operation_usage,
  byte_count,
  bytes::allocation_charge_fn,
  codec::field_context);

[[nodiscard]] bool
  valid_control_expectation(frame_kind, control_expectation) noexcept;
} // namespace kwaque::protocol::detail
