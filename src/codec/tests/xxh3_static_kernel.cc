#include "src/codec/tests/xxh3_static_kernel.h"

#define XXH_STATIC_LINKING_ONLY
#include <xxhash/xxhash.h>
#if defined(__x86_64__)
// Declares the feature test without redirecting XXH3_64bits to dispatch.
#define XXH_DISPATCH_DISABLE_REPLACE
#include <xxhash/xxh_x86dispatch.h>
#endif

namespace kwaque::codec::testing {

static_assert(sse2_kernel == XXH_SSE2);
static_assert(avx2_kernel == XXH_AVX2);

std::uint64_t
static_kernel_xxh3_64(const void* data, std::size_t size) noexcept {
    return XXH3_64bits(data, size);
}

int dispatched_kernel() noexcept {
#if defined(__x86_64__)
    return XXH_featureTest();
#else
    return -1;
#endif
}

bool avx2_available() noexcept {
#if defined(__x86_64__)
    __builtin_cpu_init();
    return __builtin_cpu_supports("avx2") != 0;
#else
    return false;
#endif
}

} // namespace kwaque::codec::testing
