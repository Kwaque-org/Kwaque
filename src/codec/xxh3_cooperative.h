#pragma once

#include "src/bytes/fragmented_buffer.h"
#include "src/codec/cooperative.h"
#include "src/codec/xxh3.h"

namespace kwaque::codec {

// The caller has admitted input backing/descriptors against its remaining
// memory allowance. Recorded input checks do not measure allocator RSS. Input
// transfers on every path. work and its abort source must remain alive and
// exclusively used until completion. Its owner bounds opaque input cleanup.
// The nonmovable hash state lives inside this coroutine and never allocates.
[[nodiscard]] seastar::future<result<content_digest>> xxh3_128_cooperatively(
  bytes::fragmented_buffer input,
  cooperative_work& work,
  error anchor = error{errc::success});

} // namespace kwaque::codec
