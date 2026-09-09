#include "src/broker/cpu_features.h"

#if defined(__x86_64__)
#include <cpuid.h>
#elif defined(__aarch64__)
#include <sys/auxv.h>
#else
#error "The broker launcher requires x86-64 or AArch64 Linux"
#endif

bool kwaque_cpu_supported(void) {
#if defined(__x86_64__)
    unsigned int eax = 0;
    unsigned int ebx = 0;
    unsigned int ecx = 0;
    unsigned int edx = 0;
    if (!__get_cpuid(1, &eax, &ebx, &ecx, &edx)) {
        return false;
    }
    const uint32_t leaf1_ecx = ecx;
    if (!__get_cpuid(UINT32_C(0x80000001), &eax, &ebx, &ecx, &edx)) {
        return false;
    }
    return kwaque_x86_64_cpu_supported(leaf1_ecx, ecx);
#elif defined(__aarch64__)
    return kwaque_aarch64_cpu_supported(getauxval(AT_HWCAP));
#endif
}
