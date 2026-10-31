#pragma once

// XXH3-64 compiled into the caller for short input, as xxHash advises for
// small keys: its short-input path is scalar on every processor, so the only
// thing the dispatched call adds there is the call.
//
// Include only from source files that include no other xxHash header:
// XXH_INLINE_ALL gives every xxHash name a private, renamed meaning in the
// including unit.

#include "src/codec/xxh3.h"

#include <cstddef>
#include <cstdint>

#define XXH_INLINE_ALL
#include <xxhash/xxhash.h>

namespace kwaque::codec {

// Internal linkage, because it calls this unit's private copy of xxHash.
// Equal to xxh3_64 for every input.
[[nodiscard]] static inline std::uint64_t
xxh3_64_inline(const void* data, std::size_t size) noexcept {
    if (size <= xxh3_64_short_bytes) {
        return XXH3_64bits(data, size);
    }
    return xxh3_64(data, size);
}

} // namespace kwaque::codec
