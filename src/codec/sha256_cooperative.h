#pragma once

#include "src/bytes/fragmented_buffer.h"
#include "src/codec/cooperative.h"
#include "src/codec/sha256.h"

namespace kwaque::codec {

// The caller has admitted input backing/descriptors and the verified native
// provider/context initialization peak against its remaining memory allowance.
// Recorded input checks do not measure that native cost or allocator RSS.
// Input transfers on every path. work and its abort source must remain alive
// and exclusively used until completion. Its owner bounds opaque input cleanup.
// Nonmovable hash state lives in this coroutine; native failures propagate.
[[nodiscard]] seastar::future<result<sha256_digest>> sha256_cooperatively(
  bytes::fragmented_buffer input,
  cooperative_work& work,
  error anchor = error{errc::success});

} // namespace kwaque::codec
