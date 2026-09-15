#pragma once

#include "src/compression/compression.h"

namespace kwaque::compression::detail {

// Query recorded capacities in bounded descriptor groups. This is accounting,
// not another reservation for an already admitted input or its aliases.
[[nodiscard]] seastar::future<codec::result<bytes::buffer_allocation_cost>>
inspect_buffer(
  const bytes::fragmented_buffer& input,
  byte_count logical_limit,
  codec::cooperative_work& work,
  bytes::allocation_charge_fn charge,
  codec::field_context context);

} // namespace kwaque::compression::detail
