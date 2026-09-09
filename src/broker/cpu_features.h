#ifndef KWAQUE_BROKER_CPU_FEATURES_H
#define KWAQUE_BROKER_CPU_FEATURES_H

#include <stdbool.h>
#include <stdint.h>

// Fail the build if target options accidentally raise the launcher's baseline.
#if defined(__x86_64__)                                                        \
  && (defined(__SSE3__) || defined(__SSSE3__) || defined(__SSE4_1__) || defined(__SSE4_2__) || defined(__POPCNT__) || defined(__PCLMUL__) || defined(__AES__) || defined(__AVX__))
#error "The broker launcher must be compiled for base x86-64"
#elif defined(__aarch64__)                                                     \
  && (defined(__ARM_FEATURE_CRC32) || defined(__ARM_FEATURE_CRYPTO) || defined(__ARM_FEATURE_AES) || defined(__ARM_FEATURE_SHA2) || defined(__ARM_FEATURE_ATOMICS) || defined(__ARM_FEATURE_SVE))
#error "The broker launcher must be compiled for base armv8-a"
#endif

// Additional requirements above x86-64 for the broker's westmere compilation.
static inline bool
kwaque_x86_64_cpu_supported(uint32_t leaf1_ecx, uint32_t extended_ecx) {
    const uint32_t required = (UINT32_C(1) << 0)     // SSE3
                              | (UINT32_C(1) << 1)   // PCLMULQDQ
                              | (UINT32_C(1) << 9)   // SSSE3
                              | (UINT32_C(1) << 13)  // CMPXCHG16B
                              | (UINT32_C(1) << 19)  // SSE4.1
                              | (UINT32_C(1) << 20)  // SSE4.2 and CRC32
                              | (UINT32_C(1) << 23); // POPCNT
    const uint32_t extended_required = UINT32_C(1);  // LAHF/SAHF in 64-bit mode
    return (leaf1_ecx & required) == required
           && (extended_ecx & extended_required) == extended_required;
}

// Linux AT_HWCAP requirements for armv8-a+crc+crypto compilation.
static inline bool kwaque_aarch64_cpu_supported(uint64_t hwcap) {
    const uint64_t required = (UINT64_C(1) << 0)    // FP
                              | (UINT64_C(1) << 1)  // ASIMD
                              | (UINT64_C(1) << 3)  // AES
                              | (UINT64_C(1) << 4)  // PMULL
                              | (UINT64_C(1) << 5)  // SHA1
                              | (UINT64_C(1) << 6)  // SHA2
                              | (UINT64_C(1) << 7); // CRC32
    return (hwcap & required) == required;
}

bool kwaque_cpu_supported(void);

#endif
