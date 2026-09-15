#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

namespace kwaque::storage::testing {
inline constexpr std::size_t storage_fuzz_max_input = 16384;
// Runs on the existing reactor test thread and joins every input owner. The
// bounded script chooses a wire family or a short supplied extent, a raw or
// independently generated payload, mutations and local resource constraints.
void exercise_storage_case(std::span<const std::uint8_t>);
} // namespace kwaque::storage::testing
