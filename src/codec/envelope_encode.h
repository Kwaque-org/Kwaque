#pragma once

#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/codec/cooperative.h"
#include "src/codec/envelope.h"
#include "src/codec/error.h"
#include "src/codec/format_registry.h"
#include "src/codec/integer.h"
#include "src/codec/limits.h"

#include <seastar/core/future.hh>

namespace kwaque::codec {

// Wrap an already encoded body in the current extension-free envelope. The
// body moves into this coroutine before its first suspension or validation;
// subsequent rejection/exception/abort consumes the donor. If the native frame
// cannot be allocated, the caller retains it. Only complete immutable output
// escapes, after body CRC, header CRC, assembly and temporary-owner cleanup.
// This framing writer does not validate the family's body grammar or context.
//
// other_live and parent_remaining use the same convention as cooperative
// assembly: they exclude this body's backing/descriptors/share controls, the
// checksum alias, new header and assembly staging. Include verified native CRC,
// coroutine-frame and opaque-owner reservations in other_live, or exclude them
// from parent_remaining once. The writer checks the distinct live stages before
// allocating; assembly then accounts its inputs itself. Known shared backing
// is not charged twice. Returned ownership retains its reservation until freed.
// charge is a verified, monotone, nonallocating served-capacity bound; there is
// no guessed allocator profile, frame-size inference or fresh child allowance.
//
// work and its abort source stay alive, unmoved and exclusively used until the
// future completes. Existing bounded native cost/share/publication/release
// leaves are bracketed by work checkpoints. Growing CRC and splice work use
// their existing cooperative drivers. Opaque deleter work is bounded by its
// producer. Final cancellation polling and output transfer do not suspend.
[[nodiscard]] seastar::future<result<bytes::fragmented_buffer>> encode_envelope(
  bytes::fragmented_buffer&& body,
  format_family family,
  cooperative_work& work,
  envelope_extent_limits owner_limits,
  operation_usage other_live,
  byte_count parent_remaining,
  bytes::allocation_charge_fn charge,
  field_context context = {});

} // namespace kwaque::codec
