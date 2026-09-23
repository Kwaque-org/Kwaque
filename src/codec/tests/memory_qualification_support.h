#pragma once

#include "src/base/allocation.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/bytes/test_allocation_profile.h"
#include "src/codec/tests/allocation_observer.h"
#include "src/codec/tests/qualification_profile.h"

#include <seastar/core/memory.hh>

#include <cinttypes>
#include <cstddef>
#include <cstdio>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <utility>

namespace kwaque::codec::testing {
using bytes::fragmented_buffer;
using bytes::testing::charge;
inline constexpr byte_count operation_budget{64U * 1024U * 1024U};
inline constexpr byte_count residual{
  operation_budget.value() - execution_reservation.value()};

// Leave room for the system profile's 32-byte slack before capacity rounding.
// Otherwise each 64-KiB input fragment is charged as 128 KiB, exhausting the
// maximum owner's allowance before its header/output can be admitted. Native
// measurements retain the benchmark's original fragment layout.
#if defined(SEASTAR_DEFAULT_ALLOCATOR)
inline constexpr std::size_t fixture_fragment_bytes = 65536 - 32;
#else
inline constexpr std::size_t fixture_fragment_bytes = 65536;
#endif

inline void require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}
template<typename T>
void opaque(T& value) noexcept {
    // NOLINTNEXTLINE(portability-no-assembler)
    asm volatile("" : "+m"(value) : : "memory");
}
inline void report_profile() {
#if defined(SEASTAR_DEFAULT_ALLOCATOR)
    constexpr bool native_allocator = false;
#else
    constexpr bool native_allocator = true;
#endif
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    constexpr bool injection_available = true;
#else
    constexpr bool injection_available = false;
#endif
#if defined(__OPTIMIZE__)
    constexpr bool optimized = true;
#else
    constexpr bool optimized = false;
#endif
#if __has_feature(address_sanitizer)
    constexpr bool asan = true;
#else
    constexpr bool asan = false;
#endif
#if __has_feature(undefined_behavior_sanitizer)
    constexpr bool ubsan = true;
#else
    constexpr bool ubsan = false;
#endif
    require(
      native_allocator || !injection_available,
      "system allocator cannot enable allocation injection");
    std::printf(
      "allocator=%s abort=%s injection=%s optimized=%s sanitized=%s asan=%s "
      "ubsan=%s\n",
      native_allocator ? "native" : "system",
      seastar::memory::is_abort_on_allocation_failure() ? "true" : "false",
      injection_available ? "true" : "false",
      optimized ? "true" : "false",
      (asan || ubsan) ? "true" : "false",
      asan ? "true" : "false",
      ubsan ? "true" : "false");
    std::fflush(stdout);
}

inline void require_effective_policy() {
#if defined(SEASTAR_DEFAULT_ALLOCATOR)
    constexpr bool expected_abort = false;
#else
    constexpr bool expected_abort = true;
#endif
    require(
      seastar::memory::is_abort_on_allocation_failure() == expected_abort,
      "unexpected effective OOM policy");
}

inline void report(
  std::string_view scenario,
  std::size_t size,
  byte_count retained,
  const allocation_observation& sample) {
    std::printf(
      "observation scenario=%.*s bytes=%zu retained_bound=%" PRIu64
      " execution_reservation=%" PRIu64 " observed=%s",
      static_cast<int>(scenario.size()),
      scenario.data(),
      size,
      retained.value(),
      execution_reservation.value(),
      sample.observed ? "true" : "false");
    if (sample.observed) {
        std::printf(
          " complete=%s mallocs=%" PRIu64 " frees=%" PRIu64
          " native_mallocs=%" PRIu64 " native_frees=%" PRIu64
          " peak_upper_bound=%" PRIu64 " live_upper_bound=%" PRIu64
          " critical_peak_upper_bound=%" PRIu64 " largest_allocation=%" PRIu64,
          sample.complete ? "true" : "false",
          sample.allocations,
          sample.frees,
          sample.native_allocations,
          sample.native_frees,
          sample.peak_upper_bound,
          sample.live_upper_bound,
          sample.critical_peak_upper_bound,
          sample.largest_allocation);
    }
    std::puts("");
    std::fflush(stdout);
    if (sample.observed) {
        require(
          sample.complete,
          "allocation observation missed native events or exhausted workspace");
        require(
          sample.largest_allocation
            <= kwaque::maximum_contiguous_allocation_bytes,
          "new allocation exceeded the contiguous ceiling");
        require(
          retained.value() <= operation_budget.value()
            && sample.peak_upper_bound
                 <= operation_budget.value() - retained.value(),
          "observed owner bound does not certify the operation allowance");
        require(
          sample.critical_peak_upper_bound <= execution_reservation.value(),
          "critical allocation bound does not certify the benchmark "
          "reservation");
    }
}
// A synchronous Seastar-thread caller prevents the measured root coroutine
// from being elided into an already allocated probe coroutine frame. Setup and
// diagnostics are outside the interval; output ownership is still live at end.
template<typename Function>
auto measure(
  std::string_view name, std::size_t size, byte_count retained, Function fn) {
    std::optional<decltype(fn())> result;
    begin_allocation_observation();
    try {
        result.emplace(fn());
    } catch (...) {
        const auto observed = end_allocation_observation();
        try {
            report(name, size, retained, observed);
        } catch (...) {
        }
        throw;
    }
    const auto observed = end_allocation_observation();
    report(name, size, retained, observed);
    return std::move(*result);
}
inline byte_count retained_cost(const fragmented_buffer& input) {
    const auto cost = input.allocation_cost(charge).value();
    return cost.backing.checked_add(cost.descriptors)
      .value()
      .checked_add(cost.share_controls)
      .value();
}

} // namespace kwaque::codec::testing
