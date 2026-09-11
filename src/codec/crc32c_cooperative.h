#pragma once

#include "src/bytes/fragmented_buffer.h"
#include "src/codec/cooperative.h"
#include "src/codec/crc32c.h"

namespace kwaque::codec {

// The caller has admitted input backing/descriptors and the verified native
// initialization peak against its remaining memory allowance. The checks here
// use recorded buffer bounds, not allocator introspection or new admission.
// Input transfers to the coroutine even on failure. work and its abort source
// stay alive and exclusively used until completion; opaque input cleanup is
// separately bounded by its owner. No input share or payload copy is created.
[[nodiscard]] seastar::future<result<crc32c::value_type>> crc32c_cooperatively(
  bytes::fragmented_buffer input,
  cooperative_work& work,
  crc32c::value_type seed = 0,
  error anchor = error{errc::success});

} // namespace kwaque::codec
