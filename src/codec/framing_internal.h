#pragma once

#include "src/bytes/fragmented_buffer.h"
#include "src/codec/cooperative.h"
#include "src/codec/integer.h"

namespace kwaque::codec::detail::framing {

// Shared accounting leaves for owning framing writers. Inputs are immutable;
// the caller supplies the verified capacity profile and all other live costs.
[[nodiscard]] error encode_error(errc, field_context) noexcept;
[[nodiscard]] result<void>
add_charge(byte_count&, byte_count, field_context) noexcept;
[[nodiscard]] result<void>
admit_usage(const operation_usage&, const limits&, byte_count, field_context);
[[nodiscard]] result<void> admit_header_owner(
  byte_count prefix_bytes,
  const bytes::buffer_allocation_cost&,
  operation_usage,
  const limits&,
  byte_count,
  bytes::allocation_charge_fn,
  field_context);

} // namespace kwaque::codec::detail::framing
