#pragma once

#include "src/bytes/fragmented_buffer.h"
#include "src/codec/integer.h"
#include "src/protocol/control_schema.h"

namespace kwaque::protocol::detail {

struct control_parse_cost final {
    byte_count generated_peak;
    byte_count input_copy;
    byte_count largest_allocation;
};

// An allocation-free upper bound for a FRESH, non-arena generated message and
// one exact contiguous input copy. Uses actual generated sizeof values and
// the bounded schema graph, string/array growth and unknown-field allocations.
// Includes migration overlap and the generated state of failed parse prefixes.
//
// Conditional on preflight enforcing this schema's field/byte/repeated limits,
// whole-message wire bounds, UTF-8 for known text, depth and aggregate
// tag+packed-element work count. The UTF-8 guard also keeps malformed ingress
// out of the native diagnostic-logging allocation path.
// This calculation neither parses nor validates input, reserves memory, nor
// constitutes a transferable admission token. Before native parsing, the owner
// debits generated_peak as metadata and input_copy as staging from its existing
// residual. The input parser's backing/aliases and native infrastructure/frame
// costs remain separately reserved. Conversion output must be admitted while
// these temporary owners are live; refund them only after joined destruction.
//
// charge is the caller's verified monotone, nonallocating served-capacity
// bound. No generated cache, arena, global allocator hook, or unknown group is
// covered. The synchronous calculation visits only the fixed schema graph and a
// bounded sequence of allocation-size buckets; it does not inspect payload
// bytes.
[[nodiscard]] codec::result<control_parse_cost> bound_control_parse(
  control_message,
  byte_count wire_bytes,
  const codec::limits&,
  bytes::allocation_charge_fn,
  codec::field_context = {}) noexcept;

} // namespace kwaque::protocol::detail
