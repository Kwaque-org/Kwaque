#pragma once

#include <array>
#include <cstdint>
#include <span>

namespace kwaque::storage::testing {
// Called by the storage-format script dispatcher. The four object cases and
// two sequential walks and supplied-extent comparison share the existing byte
// cap and reactor/crypto owner.
void exercise_metadata_case(
  const std::array<std::uint8_t, 8>&,
  std::span<const std::uint8_t> payload,
  unsigned selected);
} // namespace kwaque::storage::testing
