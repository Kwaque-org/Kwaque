#pragma once

#include <cstddef>
#include <cstdint>

namespace kwaque::codec::testing {

// xxHash's own XXH3-64, compiled for the build's baseline processor, never
// through the run-time dispatcher. Comparing it with the dispatched value
// checks the kernel the dispatcher selected.
[[nodiscard]] std::uint64_t
static_kernel_xxh3_64(const void* data, std::size_t size) noexcept;

// The kernel the dispatcher selected on x86-64 (xxHash's XXH_SSE2 or
// XXH_AVX2), or -1 where there is no dispatcher.
[[nodiscard]] int dispatched_kernel() noexcept;

// Whether the processor and the operating system support AVX2, so the
// dispatcher must have selected it. Always false where there is none.
[[nodiscard]] bool avx2_available() noexcept;

inline constexpr int sse2_kernel{1};
inline constexpr int avx2_kernel{2};

} // namespace kwaque::codec::testing
