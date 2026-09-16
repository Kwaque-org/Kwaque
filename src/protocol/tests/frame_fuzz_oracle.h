#pragma once

#include "src/base/error.h"
#include "src/model/tests/record_fuzz_oracle.h"

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace kwaque::protocol::testing {

inline constexpr unsigned complete_input = 1;
inline constexpr unsigned wrong_topic = 2;
inline constexpr unsigned nil_topic = 4;
inline constexpr unsigned initial_abort = 8;
inline constexpr unsigned deny_operation = 16;
inline constexpr unsigned deny_metadata = 32;
inline constexpr unsigned deny_work = 64;

struct frame_probe final {
    errc error{errc::success};
    std::size_t needed{0};
    std::size_t used{0};
    std::size_t header{0};
    std::size_t payload{0};
    std::uint16_t kind{0};
    model::testing::batch_probe batch{};
};

// Independent fixed-field framing grammar, then the existing independent
// batch oracle for expected_kind 16/17 (zero means framing only). No production
// frame parser, descriptor registry, encoder or model factory is used here.
frame_probe probe_frame(
  std::string_view wire,
  unsigned expected_kind = 0,
  unsigned flags = 0,
  unsigned depth = 0);

} // namespace kwaque::protocol::testing
