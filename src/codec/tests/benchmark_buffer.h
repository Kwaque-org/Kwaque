#pragma once

#include "src/bytes/fragmented_buffer.h"
#include "src/codec/cooperative.h"

namespace kwaque::codec::bench {
// Test-only fixtures and checks. Borrowed inputs/work outlive the joined call;
// setup and byte comparisons belong outside the measured operation interval.
void require(bool condition, const char* message);
byte_count capacity_bound(byte_count request) noexcept;
void qualify_allocator();
seastar::future<bool> buffers_equal(
  const bytes::fragmented_buffer& left,
  const bytes::fragmented_buffer& right,
  cooperative_work& work);
seastar::future<bytes::fragmented_buffer> copy_layout(
  bytes::fragmented_buffer input,
  std::size_t width,
  cooperative_work& work,
  byte_count remaining,
  std::size_t fragment_limit = 512);
enum class payload_pattern { compressible, incompressible, mixed };
seastar::future<bytes::fragmented_buffer> patterned_buffer(
  std::size_t size,
  std::size_t width,
  payload_pattern pattern,
  cooperative_work& work,
  byte_count remaining);
} // namespace kwaque::codec::bench
