#pragma once

#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/codec/error.h"
#include "src/codec/integer.h"
#include "src/codec/limits.h"

#include <cstddef>
#include <span>

namespace kwaque::codec {

// Assemble already-encoded parts in a private builder and publish once.
// Payload ownership transfers at entry, including on failure. Prefix storage
// remains alive and immutable through this synchronous call; it is copied.
// other_live covers other live allocations (including any borrowed prefix
// backing and opaque native owners), excluding the donor backing/descriptors/
// promotion reserve and new staging charged here.
// parent_remaining is the allowance before those additional charges, never a
// fresh per-child budget. charge supplies a verified, stable allocator-profile
// upper bound; it is not inferred from visible lengths or allocator RSS.
// The contiguous cap gates new staging allocations; the donor is already owned.
// The owner separately bounds teardown of opaque native resources.
// Work exceeding the bounded synchronous copy/descriptor leaf is rejected.
[[nodiscard]] result<bytes::fragmented_buffer> assemble_buffer(
  std::span<const char> prefix,
  bytes::fragmented_buffer payload,
  const limits& policy,
  byte_count logical_cap,
  const operation_usage& other_live,
  byte_count parent_remaining,
  bytes::allocation_charge_fn charge,
  field_context context = {});

// As with builder.append, do not silently include a literal's terminator.
template<std::size_t N>
result<bytes::fragmented_buffer> assemble_buffer(
  const char (&prefix)[N],
  bytes::fragmented_buffer payload,
  const limits& policy,
  byte_count logical_cap,
  const operation_usage& other_live,
  byte_count parent_remaining,
  bytes::allocation_charge_fn charge,
  field_context context = {}) = delete;

} // namespace kwaque::codec
