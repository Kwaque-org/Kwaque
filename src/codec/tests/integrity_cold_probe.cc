#include "src/codec/crc32c.h"
#include "src/codec/digest.h"
#include "src/codec/sha256.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/alloc_failure_injector.hh>

#include <boost/program_options.hpp>
#include <crc32c/crc32c.h>

#include <array>
#include <bit>
#include <chrono>
#include <cinttypes>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <new>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace {

#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
constexpr std::uint32_t crc_seed = 0x13579bdfU;
constexpr int isolated_failure_observed = 86;
#endif

auto input = [] {
    std::array<char, 4096> bytes{};
    for (std::size_t index = 0; index < bytes.size(); ++index) {
        bytes[index] = std::bit_cast<char>(
          static_cast<unsigned char>(index % 256U));
    }
    return bytes;
}();

void require(bool condition, const char* message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

void report_profile() {
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

void require_effective_policy() {
#if defined(SEASTAR_DEFAULT_ALLOCATOR)
    constexpr bool expected_abort = false;
#else
    constexpr bool expected_abort = true;
#endif
    require(
      seastar::memory::is_abort_on_allocation_failure() == expected_abort,
      "unexpected effective OOM policy");
}

int cold_crc(std::size_t length, std::uint32_t expected) {
    kwaque::codec::crc32c checksum;
    checksum.extend(std::span<const char>{input}.first(length));
    require(checksum.value() == expected, "cold CRC known answer mismatch");
    std::printf("phase=cold-crc bytes=%zu status=ok\n", length);
    return 0;
}

int cold_sha() {
    constexpr kwaque::codec::sha256_digest expected{
      0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40,
      0xde, 0x5d, 0xae, 0x22, 0x23, 0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17,
      0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61, 0xf2, 0x00, 0x15, 0xad};
    kwaque::codec::sha256_hasher hasher;
    hasher.update("abc", 3);
    require(
      std::move(hasher).final() == expected, "cold SHA known answer mismatch");
    std::puts("phase=cold-sha input=abc status=ok");
    return 0;
}

// The process has not touched the selected engine before this call. Warm only
// the clock, and keep all formatting and result validation outside the sample.
// Native statistics include critical allocations, unlike the failure injector.
// They cover the current reactor, not alien threads. Requested bytes and
// allocator page occupancy are not peak live capacity.
template<typename Function, typename Validate>
int measure_first_use(
  const char* backend,
  std::size_t bytes,
  Function function,
  Validate validate) {
    using clock = std::chrono::steady_clock;
    static_cast<void>(clock::now());
    seastar::memory::scoped_large_allocation_warning_threshold warning{
      128U * 1024U + 1U};
    const auto before = seastar::memory::stats();
    const auto start = clock::now();
    // Make input opaque to the optimizer without emitting instructions.
    // NOLINTNEXTLINE(portability-no-assembler)
    asm volatile("" : "+m"(input) : : "memory");
    const auto result = function();
    // Retain the trailing compiler memory barrier; it emits no instructions.
    // NOLINTNEXTLINE(portability-no-assembler)
    asm volatile("" : : : "memory");
    const auto end = clock::now();
    const auto after = seastar::memory::stats();
    const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
                           end - start)
                           .count();
    require(elapsed >= 0, "cold measurement clock moved backwards");
    require(validate(result), "measured integrity known answer mismatch");
    std::printf(
      "measurement backend=%s bytes=%zu nanoseconds=%" PRIu64,
      backend,
      bytes,
      static_cast<std::uint64_t>(elapsed));
#if defined(SEASTAR_DEFAULT_ALLOCATOR)
    static_cast<void>(before);
    static_cast<void>(after);
    std::puts(" memory_observed=false");
#else
    require(
      after.mallocs() >= before.mallocs() && after.frees() >= before.frees()
        && after.total_bytes_allocated() >= before.total_bytes_allocated()
        && after.large_allocations() >= before.large_allocations()
        && after.foreign_mallocs() >= before.foreign_mallocs()
        && after.fallback_allocations() >= before.fallback_allocations(),
      "cold measurement counters moved backwards");
    const auto large = after.large_allocations() - before.large_allocations();
    const auto foreign = after.foreign_mallocs() - before.foreign_mallocs();
    const auto fallback = after.fallback_allocations()
                          - before.fallback_allocations();
    std::printf(
      " memory_observed=true mallocs=%" PRIu64 " frees=%" PRIu64
      " requested_bytes=%" PRIu64 " page_bytes_before=%zu page_bytes_after=%zu"
      " large_allocation_warnings=%" PRIu64 " foreign_allocations=%" PRIu64
      " fallback_allocations=%" PRIu64 "\n",
      after.mallocs() - before.mallocs(),
      after.frees() - before.frees(),
      after.total_bytes_allocated() - before.total_bytes_allocated(),
      before.allocated_memory(),
      after.allocated_memory(),
      large,
      foreign,
      fallback);
    // The threshold is reset in each fresh process; its first violation cannot
    // be hidden by the native warning backoff. Warnings are not an exact count
    // of oversized calls after that first violation.
    require(
      large == 0,
      "cold native call exceeded the contiguous allocation ceiling");
    require(
      foreign == 0 && fallback == 0,
      "cold reactor-local allocation escaped native accounting");
#endif
    return 0;
}

int measure_crc(bool google, std::size_t length, std::uint32_t expected) {
    const auto validate = [expected](std::uint32_t result) {
        return result == expected;
    };
    if (google) {
        return measure_first_use(
          "google_configured",
          length,
          [length] {
              return ::crc32c::Extend(
                0, reinterpret_cast<const std::uint8_t*>(input.data()), length);
          },
          validate);
    }
    return measure_first_use(
      "abseil_selected",
      length,
      [length] {
          kwaque::codec::crc32c crc;
          crc.extend(std::span<const char>{input}.first(length));
          return crc.value();
      },
      validate);
}

int measure_sha() {
    constexpr kwaque::codec::sha256_digest expected{
      0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40,
      0xde, 0x5d, 0xae, 0x22, 0x23, 0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17,
      0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61, 0xf2, 0x00, 0x15, 0xad};
    return measure_first_use(
      "openssl_sha256",
      3,
      [] {
          kwaque::codec::sha256_hasher hasher;
          hasher.update("abc", 3);
          return std::move(hasher).final();
      },
      [expected](const auto& result) { return result == expected; });
}

int warm_crc() {
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    // Only this explicitly warm scenario initializes the engine before arming.
    kwaque::codec::crc32c warmup;
    warmup.extend(std::span<const char>{input});
    require(warmup.value() == 0x9c71fe32U, "warm-up CRC mismatch");

    kwaque::codec::crc32c checksum{crc_seed};
    auto& injector = seastar::memory::local_failure_injector();
    const auto allocations_before = injector.alloc_count();
    bool threw = false;
    injector.fail_after(0);
    try {
        checksum.extend(std::span<const char>{input});
    } catch (...) {
        threw = true;
    }
    const bool injected = injector.failed();
    const auto allocations_after = injector.alloc_count();
    injector.cancel();

    require(!threw && !injected, "warm CRC attempted a failing allocation");
    require(allocations_after == allocations_before, "warm CRC allocated");
    require(checksum.value() == 0xa280ca1eU, "warm seeded CRC mismatch");
    require_effective_policy();
    std::puts("phase=warm-crc bytes=4096 allocation_free=true status=ok");
    return 0;
#else
    std::puts("phase=warm-crc status=skipped injection=false");
    return 77;
#endif
}

int cold_crc_injection(unsigned ordinal) {
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    kwaque::codec::crc32c checksum{crc_seed};
    auto& injector = seastar::memory::local_failure_injector();
    bool caught_bad_alloc = false;
    bool caught_other = false;
    injector.fail_after(ordinal);
    try {
        // More than 64 bytes forces the native engine initialization path.
        checksum.extend(std::span<const char>{input});
    } catch (const std::bad_alloc&) {
        caught_bad_alloc = true;
    } catch (...) {
        caught_other = true;
    }
    const bool injected = injector.failed();
    injector.cancel();
    const bool unchanged = checksum.value() == crc_seed;
    const bool abort_enabled
      = seastar::memory::is_abort_on_allocation_failure();
    const bool observed = caught_bad_alloc && !caught_other && injected
                          && unchanged && abort_enabled;
    std::printf(
      "phase=cold-crc-injection ordinal=%u caught_bad_alloc=%s injected=%s "
      "seed_unchanged=%s abort=%s exit=isolated status=%s\n",
      ordinal,
      caught_bad_alloc ? "true" : "false",
      injected ? "true" : "false",
      unchanged ? "true" : "false",
      abort_enabled ? "true" : "false",
      observed ? "ok" : "failed");
    std::fflush(stdout);
    // Failed native initialization can retain an unpublished engine. Observe
    // the failure in isolation; do not resume it or claim cleanup/rollback.
    std::_Exit(observed ? isolated_failure_observed : 1);
#else
    std::printf(
      "phase=cold-crc-injection ordinal=%u status=skipped injection=false\n",
      ordinal);
    return 77;
#endif
}

int exercise(std::string_view scenario) {
    report_profile();
    if (scenario == "capabilities") {
        std::puts("phase=capabilities status=ok");
        return 0;
    }
    require_effective_policy();
    if (scenario == "measure-sha") {
        return measure_sha();
    }
    for (const auto& [length, expected] :
         {std::pair{32U, 0x46dd794eU},
          std::pair{48U, 0x3c25332aU},
          std::pair{65U, 0x694420faU},
          std::pair{4096U, 0x9c71fe32U}}) {
        for (const bool google : {false, true}) {
            const auto name
              = std::string{google ? "measure-google-" : "measure-abseil-"}
                + std::to_string(length);
            if (scenario == name) {
                return measure_crc(google, length, expected);
            }
        }
    }
    if (scenario == "crc-32") {
        return cold_crc(32, 0x46dd794eU);
    }
    if (scenario == "crc-48") {
        return cold_crc(48, 0x3c25332aU);
    }
    if (scenario == "crc-65") {
        return cold_crc(65, 0x694420faU);
    }
    if (scenario == "crc-4096") {
        return cold_crc(4096, 0x9c71fe32U);
    }
    if (scenario == "sha-abc") {
        return cold_sha();
    }
    if (scenario == "crc-warm") {
        return warm_crc();
    }
    if (scenario == "crc-inject-0") {
        return cold_crc_injection(0);
    }
    if (scenario == "crc-inject-1") {
        return cold_crc_injection(1);
    }
    throw std::runtime_error("unknown integrity cold-probe scenario");
}

} // namespace

int main(int argc, char** argv) {
    seastar::app_template app;
    app.add_options()(
      "scenario", boost::program_options::value<std::string>()->required());
    return app.run(argc, argv, [&app] {
        return seastar::async([&app] {
            return exercise(app.configuration()["scenario"].as<std::string>());
        });
    });
}
