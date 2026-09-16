#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

namespace kwaque::protocol::testing {
inline constexpr std::size_t frame_fuzz_max_input = 16384;
// Eight controls select raw/generic/submitted/assigned/sparse bytes, mutation,
// payload choice, header shape, fragmentation, context/resource/boundary flags,
// existing marks and operand. All owners/futures end within the joined reactor.
void exercise_frame_case(std::span<const std::uint8_t> input);
} // namespace kwaque::protocol::testing
