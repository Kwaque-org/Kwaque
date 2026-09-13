#pragma once

#include "src/base/units.h"
#include "src/bytes/fragmented_buffer_builder.h"
#include "src/codec/error.h"
#include "src/codec/integer.h"
#include "src/codec/limits.h"

#include <cstdint>
#include <span>

namespace kwaque::codec::testing {

struct header_extension final {
    std::uint16_t tag;
    std::uint16_t flags;
    std::span<const char> value;
};

// Synthetic future-header fixtures only. Ordinary writers have no assigned
// extensions. Input and private output remain alive through this synchronous
// call; a failure may leave an encoded prefix in private output.
[[nodiscard]] result<void> append_header_extensions(
  bytes::fragmented_buffer_builder& output,
  std::span<const header_extension> extensions,
  byte_count fixed_prefix_bytes,
  const limits& policy,
  field_context context = {});

} // namespace kwaque::codec::testing
