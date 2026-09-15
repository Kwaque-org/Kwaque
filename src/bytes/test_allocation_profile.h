#pragma once

#include "src/base/units.h"

#include <algorithm>
#include <bit>
#include <cstdint>
#include <limits>

namespace kwaque::bytes::testing {
// Shared test-only capacity bound. Native allocation qualification compares
// these charges with malloc_usable_size under each selected allocator profile.
// The system profile includes header/alignment slack before rounding.
inline byte_count charge(byte_count request) noexcept {
    if (request.value() == 0) return {};
    if (request.value() > (std::uint64_t{1} << 62U))
        return byte_count{std::numeric_limits<std::uint64_t>::max()};
#if defined(SEASTAR_DEFAULT_ALLOCATOR)
    return byte_count{
      std::bit_ceil(std::max(request.value() + 32U, std::uint64_t{32}))};
#else
    const auto rounded = std::bit_ceil(
      std::max(request.value(), std::uint64_t{16}));
    return byte_count{request.value() <= 16384 ? 2U * rounded : rounded};
#endif
}
} // namespace kwaque::bytes::testing
