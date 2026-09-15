#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

namespace kwaque::compression::testing {
inline constexpr std::size_t compression_fuzz_max_input = 16384;
// Runs on the reactor's test thread. All native and coroutine owners are joined
// before return; malformed data is compared with an independent bounded reader.
void exercise_compression_case(std::span<const std::uint8_t> input);
} // namespace kwaque::compression::testing
