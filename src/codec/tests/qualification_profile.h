#pragma once

#include "src/base/units.h"

namespace kwaque::codec::testing {

// Shared by the complete-owner benchmarks and their native memory probe.
// Qualification observes the selected instantiations; this is not a universal
// reservation for arbitrary body decoders or a production allocator profile.
inline constexpr byte_count execution_reservation{1024U * 1024U};

inline constexpr byte_count operation_budget{64U * 1024U * 1024U};
inline constexpr byte_count residual{
  operation_budget.value() - execution_reservation.value()};

} // namespace kwaque::codec::testing
