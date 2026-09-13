#pragma once

#include "src/bytes/fragmented_buffer_builder.h"
#include "src/codec/error.h"
#include "src/codec/limits.h"

namespace kwaque::model::detail {

// Uniform bounded tails and one reserved descriptor array for a growing raw
// record region. Shared by original construction and survivor copying.
struct batch_staging_shape final {
    bytes::fragmented_buffer_builder_config config;
    byte_count descriptors;
};

[[nodiscard]] codec::result<batch_staging_shape> make_batch_staging(
  const codec::limits& policy,
  bytes::allocation_charge_fn charge,
  codec::error anchor);

// Counts all live staging. Publication transfers the reserved descriptors.
// Return the parent remainder with that entire reservation removed. This is a
// snapshot, not an extra reservation per append; old input/owners are excluded
// by the caller before entry. Native/frame/opaque costs stay with their owner.
[[nodiscard]] codec::result<byte_count> admit_batch_staging(
  batch_staging_shape shape,
  byte_count total,
  const codec::limits& policy,
  byte_count parent_remaining,
  bytes::allocation_charge_fn charge,
  codec::error anchor);

} // namespace kwaque::model::detail
