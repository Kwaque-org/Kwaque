#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

namespace kwaque::model::testing {
inline constexpr std::size_t record_fuzz_max_input = 16384;
// Eight control bytes select grammar, mutation, operand, fragmentation,
// repairs/ boundary/expectation/budget/abort flags, caller marks, selection and
// queued cancellation/work quantum/absolute coordinates. Remaining bytes supply
// raw or structured payload. The existing reactor bridge owns input; every
// future/observer is joined here.
void exercise_record_case(std::span<const std::uint8_t> input);
void verify_record_oracle();
} // namespace kwaque::model::testing
