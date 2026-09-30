#pragma once

#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/codec/cooperative.h"
#include "src/runtime/error.h"
#include "src/runtime/file.h"

#include <seastar/core/future.hh>

#include <cstdint>

namespace kwaque::storage::detail {
// Shares of the zero chunk in one zero-writing request.
inline constexpr std::uint64_t zero_write_chunks = 64;

// The one zero-filled buffer an owner keeps; zero writes repeat shares of it,
// so its size, not the zero-written range, bounds the backing.
[[nodiscard]] bytes::fragmented_buffer make_zero_chunk(byte_count size);

// Zero-writes [begin, end) through bounded runtime writes of at most
// zero_write_chunks shares of chunk, which is borrowed until completion.
[[nodiscard]] seastar::future<runtime::result<void>> zero_fill(
  runtime::file& file,
  bytes::fragmented_buffer& chunk,
  std::uint64_t begin,
  std::uint64_t end,
  codec::cooperative_work& work);
} // namespace kwaque::storage::detail
