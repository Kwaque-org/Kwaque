#pragma once

#include "src/bytes/fragmented_buffer_parser.h"
#include "src/codec/error.h"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>

namespace kwaque::codec::testing::format_fixture {

// Mutations of the seven-byte record artifact. Numeric field IDs and offsets
// are independent wire expectations, including the exact child-end shortage.
struct record_mutation {
    std::size_t offset;
    char replacement;
    errc code;
    std::uint16_t field;
    std::uint64_t error_offset;
};
inline constexpr std::array record_mutations{
  record_mutation{1, '\x01', errc::unsupported_format, 65, 1},
  record_mutation{3, '\x01', errc::malformed_data, 67, 3},
  record_mutation{4, '\x0c', errc::malformed_data, 68, 7}};

template<typename T>
void expect_rejection(
  const result<T>& rejected,
  error expected,
  const bytes::fragmented_buffer_parser& input,
  byte_count position,
  std::size_t depth) {
    EXPECT_EQ(input.bytes_consumed(), position);
    EXPECT_EQ(input.checkpoint_depth(), depth);
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error().code(), expected.code());
    EXPECT_EQ(rejected.error().family(), expected.family());
    EXPECT_EQ(rejected.error().field(), expected.field());
    EXPECT_EQ(rejected.error().byte_offset(), expected.byte_offset());
}

} // namespace kwaque::codec::testing::format_fixture
