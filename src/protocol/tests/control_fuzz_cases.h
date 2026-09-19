#pragma once

#include "src/protocol/tests/control_test_payload.h"

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>

namespace kwaque::protocol::testing {
inline constexpr std::size_t control_fuzz_max_input = 16384;
// Eight controls: root kind plus framed/raw bits, shape, mutation, policy
// flags, fragmentation, existing marks, mutation offset and byte. Structured
// shapes can expand to the 64-KiB control ceiling without enlarging the fuzz
// wrapper.
void exercise_control_case(std::span<const std::uint8_t>);
} // namespace kwaque::protocol::testing
