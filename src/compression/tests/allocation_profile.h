#pragma once

#include "src/bytes/test_allocation_profile.h"
#include "src/compression/compression.h"

namespace kwaque::compression::testing {

using kwaque::bytes::testing::charge;

inline codec::decode_budget budget() noexcept {
    return {byte_count{64U << 20U}, byte_count{1U << 20U}, charge};
}

} // namespace kwaque::compression::testing
