#include <seastar/core/memory.hh>
#include <seastar/testing/perf_tests.hh>

#include <cstdio>
#include <cstdlib>
#include <stdexcept>
#include <string_view>

namespace {

void report_benchmark_policy(const seastar::sstring&, const seastar::sstring&) {
#if defined(SEASTAR_DEFAULT_ALLOCATOR)
    constexpr bool native_allocator = false;
#else
    constexpr bool native_allocator = true;
#endif
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    constexpr bool injection = true;
#else
    constexpr bool injection = false;
#endif
#if defined(__OPTIMIZE__)
    constexpr bool optimized = true;
#else
    constexpr bool optimized = false;
#endif
    constexpr bool asan = __has_feature(address_sanitizer);
    constexpr bool ubsan = __has_feature(undefined_behavior_sanitizer);
    const bool oom_abort = seastar::memory::is_abort_on_allocation_failure();

    // The native runner invokes hooks before timing and allocation snapshots.
    static bool reported = false;
    if (!reported) {
        std::fprintf(
          stderr,
          "kwaque-benchmark-profile-v1 allocator=%s injection=%s optimized=%s "
          "asan=%s ubsan=%s oom_abort=%s\n",
          native_allocator ? "native" : "system",
          injection ? "true" : "false",
          optimized ? "true" : "false",
          asan ? "true" : "false",
          ubsan ? "true" : "false",
          oom_abort ? "true" : "false");
        std::fflush(stderr);
        reported = true;
    }
    if (
      const auto* required = std::getenv("KWAQUE_REQUIRE_BENCHMARK_PROFILE")) {
        if (std::string_view{required} != "production") {
            throw std::invalid_argument("unknown required benchmark profile");
        }
        if (
          !native_allocator || injection || !optimized || asan || ubsan
          || !oom_abort) {
            throw std::runtime_error(
              "benchmark comparison requires optimized native allocation, "
              "OOM abort, and disabled injection/sanitizers");
        }
    }
}

PERF_PRE_RUN_HOOK(report_benchmark_policy)

} // namespace
