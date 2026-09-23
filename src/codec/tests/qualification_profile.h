#pragma once

#include "src/base/units.h"

namespace kwaque::codec::testing {

// Shared by the complete-owner benchmarks and their native memory probe.
// Qualification observes the selected instantiations; this is not a universal
// reservation for arbitrary body decoders or a production allocator profile.
inline constexpr byte_count execution_reservation{1024U * 1024U};

} // namespace kwaque::codec::testing
