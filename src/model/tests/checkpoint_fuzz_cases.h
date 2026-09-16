#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

namespace kwaque::model::testing {
inline constexpr std::size_t checkpoint_fuzz_max_input = 16384;
// Eight controls select raw/structured/owning input, mutation, count, header,
// fragmentation, boundary/context/resource flags, existing marks and operand.
// Runs inside the joined reactor bridge. All owners and futures end on return.
void exercise_checkpoint_case(std::span<const std::uint8_t> input);
} // namespace kwaque::model::testing
