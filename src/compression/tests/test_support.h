#pragma once

#include "src/compression/tests/allocation_profile.h"

#include <gtest/gtest.h>

namespace kwaque::compression::testing {

inline constexpr codec::field_context context{
  .origin = 71, .family = 4, .field = 8};

inline void expect_error(const auto& result, errc code) {
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(
      result.error(),
      codec::error(code, context.family, context.field, context.origin));
}

} // namespace kwaque::compression::testing
