#pragma once

#include "src/base/units.h"
#include "src/bytes/test_allocation_profile.h"
#include "src/compression/compression.h"

namespace kwaque::compression::testing {

using kwaque::bytes::testing::charge;

inline codec::decode_budget budget() noexcept {
    return {byte_count{64_MiB}, byte_count{1_MiB}, charge};
}

} // namespace kwaque::compression::testing
