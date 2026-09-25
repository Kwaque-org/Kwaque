#pragma once

#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/codec/cooperative.h"
#include "src/codec/error.h"
#include "src/codec/integer.h"
#include "src/codec/limits.h"

#include <seastar/core/future.hh>

namespace kwaque::codec {

// Both inputs transfer into the coroutine before its first suspension. If the
// coroutine frame cannot be allocated, the caller retains them. The two input
// objects must be distinct. Only complete output is returned; moved inputs are
// consumed on later failure. work and its abort source outlive the future and
// are exclusively used by this operation and its joined children.
//
// other_live and parent_remaining exclude the additional input backing,
// descriptors, possible share controls and staging charged here. Include opaque
// native owners and the verified coroutine-frame reservation in other_live.
// charge is a verified monotone served-allocation
// bound. Destructive fragment transfer retains the same backing reservation;
// it transfers to the returned owner instead of being counted twice.
[[nodiscard]] seastar::future<result<bytes::fragmented_buffer>>
assemble_buffer_cooperatively(
  bytes::fragmented_buffer&& prefix,
  bytes::fragmented_buffer&& payload,
  cooperative_work& work,
  byte_count logical_cap,
  operation_usage other_live,
  byte_count parent_remaining,
  bytes::allocation_charge_fn charge,
  field_context context = {});

// The same consuming contract for a fixed prefix, payload and suffix. All
// three objects must be distinct; all descriptors are reserved in one pass.
[[nodiscard]] seastar::future<result<bytes::fragmented_buffer>>
assemble_buffer_cooperatively(
  bytes::fragmented_buffer&& prefix,
  bytes::fragmented_buffer&& payload,
  bytes::fragmented_buffer&& suffix,
  cooperative_work& work,
  byte_count logical_cap,
  operation_usage other_live,
  byte_count parent_remaining,
  bytes::allocation_charge_fn charge,
  field_context context = {});

// Four owners allow an independently bounded envelope/fixed prefix without
// an intermediate assembly of the payload. The consuming contract is the same.
[[nodiscard]] seastar::future<result<bytes::fragmented_buffer>>
assemble_buffer_cooperatively(
  bytes::fragmented_buffer&& header,
  bytes::fragmented_buffer&& fixed,
  bytes::fragmented_buffer&& payload,
  bytes::fragmented_buffer&& suffix,
  cooperative_work& work,
  byte_count logical_cap,
  operation_usage other_live,
  byte_count parent_remaining,
  bytes::allocation_charge_fn charge,
  field_context context = {});

} // namespace kwaque::codec
