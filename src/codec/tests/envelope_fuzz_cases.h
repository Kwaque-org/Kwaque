#pragma once

#include <cstdint>
#include <span>

namespace kwaque::codec::testing {

struct envelope_fuzz_options final {
    bool initially_aborted{false};
    bool queued_abort{false};
    bool body_abort{false};
    bool cleanup_abort{false};
    bool exhaust_operation{false};
    bool exhaust_metadata{false};
    std::uint64_t work_bytes{65536};
    std::uint64_t work_items{256};
    std::uint8_t layout{0};
    std::uint8_t checkpoint_depth{0};
};

// Runs within the existing joined reactor bridge; no reactor is created here.
// Inputs are at most 16 KiB. Six controls select raw/structured input, repairs,
// mutation, header shape, independent expected context and existing marks.
// All futures, queued aborts and owned inputs finish before returning.
void exercise_envelope_case(
  std::span<const std::uint8_t> input, envelope_fuzz_options options = {});

// Independent literal/error checks for the oracle itself; called by its
// ordinary unit test without invoking a production encoder or decoder.
void verify_envelope_fuzz_oracle();

} // namespace kwaque::codec::testing
