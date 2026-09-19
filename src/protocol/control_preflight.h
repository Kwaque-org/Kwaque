#pragma once

#include "src/codec/cooperative.h"
#include "src/codec/integer.h"
#include "src/protocol/control_schema.h"

#include <span>

namespace kwaque::protocol::detail {

// Private exact-span inspection. The immutable bytes, work and abort source
// remain alive and exclusive through completion. The returned count is work
// accounting, never a reusable validation capability. The owning decoder must
// parse these SAME bytes before releasing the private copy.
[[nodiscard]] seastar::future<codec::result<item_count>> preflight_control(
  std::span<const char>,
  control_message,
  codec::cooperative_work&,
  codec::field_context = {});

} // namespace kwaque::protocol::detail
