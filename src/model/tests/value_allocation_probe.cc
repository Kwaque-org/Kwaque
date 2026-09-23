#include "src/base/error.h"
#include "src/base/units.h"
#include "src/codec/limits.h"
#include "src/model/batch_context.h"
#include "src/model/batch_identity.h"
#include "src/model/epoch.h"
#include "src/model/identity.h"
#include "src/model/keyspace.h"
#include "src/model/position.h"
#include "src/runtime/time.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/thread.hh>

#include <absl/hash/hash.h>
#include <boost/program_options.hpp>

#include <algorithm>
#include <array>
#include <cinttypes>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <limits>
#include <new>
#include <span>
#include <stdexcept>
#include <utility>

namespace {
namespace model = kwaque::model;
using kwaque::byte_count;
using kwaque::errc;
using kwaque::item_count;
constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();

void require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

// Keep fixed inputs and copies visible to the optimizer without runtime work.
template<typename T>
void opaque(T& value) noexcept {
    // NOLINTNEXTLINE(portability-no-assembler)
    asm volatile("" : "+m"(value) : : "memory");
}

void report_profile() {
#if defined(SEASTAR_DEFAULT_ALLOCATOR)
    constexpr bool native = false;
#else
    constexpr bool native = true;
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
    std::printf(
      "profile allocator=%s injection=%s optimized=%s sanitized=%s "
      "asan=%s ubsan=%s abort=%s\n",
      native ? "native" : "system",
      injection ? "true" : "false",
      optimized ? "true" : "false",
      (asan || ubsan) ? "true" : "false",
      asan ? "true" : "false",
      ubsan ? "true" : "false",
      seastar::memory::is_abort_on_allocation_failure() ? "true" : "false");
    std::printf("compiler=%s\n", __clang_version__);
    std::fflush(stdout);
}

// Each call is synchronous on one reactor. Setup, assertion reporting, error
// rendering and output are outside the sample. These are cumulative allocation
// counters, not net live bytes; allocate/free pairs cannot hide an allocation.
enum class allocation_expectation { observe, none, present };

template<typename Function, typename Validate>
void measure(
  const char* name,
  Function function,
  Validate validate,
  allocation_expectation expected = allocation_expectation::none) {
    const auto before = seastar::memory::stats();
    // NOLINTNEXTLINE(portability-no-assembler)
    asm volatile("" : : : "memory");
    const auto result = function();
    // NOLINTNEXTLINE(portability-no-assembler)
    asm volatile("" : : "m"(result) : "memory");
    const auto after = seastar::memory::stats();
    require(validate(result), name);
#if defined(SEASTAR_DEFAULT_ALLOCATOR)
    static_cast<void>(before);
    static_cast<void>(after);
    static_cast<void>(expected);
    std::printf("sample name=%s memory_observed=false\n", name);
#else
    require(
      after.mallocs() >= before.mallocs()
        && after.total_bytes_allocated() >= before.total_bytes_allocated()
        && after.foreign_mallocs() >= before.foreign_mallocs()
        && after.fallback_allocations() >= before.fallback_allocations(),
      "allocation counters moved backwards");
    const auto allocations = after.mallocs() - before.mallocs();
    const auto bytes = after.total_bytes_allocated()
                       - before.total_bytes_allocated();
    const auto foreign = after.foreign_mallocs() - before.foreign_mallocs();
    const auto fallback = after.fallback_allocations()
                          - before.fallback_allocations();
    std::printf(
      "sample name=%s memory_observed=true mallocs=%" PRIu64
      " requested_bytes=%" PRIu64 " foreign_allocations=%" PRIu64
      " fallback_allocations=%" PRIu64 "\n",
      name,
      allocations,
      bytes,
      foreign,
      fallback);
    if (expected == allocation_expectation::observe) return;
    require(foreign == 0 && fallback == 0, "allocation escaped accounting");
    require(
      expected == allocation_expectation::present
        ? allocations > 0 && bytes >= 32
        : allocations == 0 && bytes == 0,
      name);
#endif
}

int exercise() {
    const auto is_true = [](bool value) { return value; };
    std::array<std::uint8_t, 16> nil{};
    opaque(nil);
    const auto invalid_id = [&nil] { return model::cluster_id::make(nil); };
    const auto invalid_argument = [](const auto& result) {
        return !result && result.error() == errc::invalid_argument;
    };
    // No Kwaque factory, error-category access or error comparison precedes
    // this first invalid call in this standalone process. In particular there
    // is no test-runner setup that can warm the shared category first.
    // Shared error initialization is observed separately, without imposing a
    // new zero-allocation contract on the first invalid-input path.
    measure(
      "cold-invalid-id",
      invalid_id,
      invalid_argument,
      allocation_expectation::observe);
    measure("warm-invalid-id", invalid_id, invalid_argument);

    std::array<std::uint8_t, 16> octets{};
    octets.front() = 1;
    octets.back() = 255;
    opaque(octets);
    measure(
      "identity",
      [&] {
          const auto id = model::cluster_id::make(octets);
          if (!id) return false;
          auto copy = *id;
          opaque(copy);
          return copy == *id && std::ranges::equal(copy.bytes(), octets)
                 && absl::Hash<model::cluster_id>{}(copy)
                      == absl::Hash<model::cluster_id>{}(*id);
      },
      is_true);

    std::uint64_t epoch_value = 7;
    std::uint64_t begin = maximum - 64U;
    opaque(epoch_value);
    opaque(begin);
    measure(
      "epoch-span",
      [&] {
          const auto epoch = model::producer_epoch::make(epoch_value);
          if (!epoch) return false;
          const auto next = epoch->checked_successor();
          const auto span = model::range_logical_span::from_count(
            model::range_logical_end{begin}, model::range_logical_count{64});
          return next && next->value() == 8 && span
                 && span->begin().value() == begin
                 && span->end().value() == maximum
                 && span->count().value() == 64;
      },
      is_true);
    measure(
      "warm-invalid-span",
      [&] {
          return model::range_logical_span::from_count(
            model::range_logical_end{begin}, model::range_logical_count{65});
      },
      [](const auto& result) {
          return !result && result.error() == errc::out_of_range;
      });

    const auto id = model::batch_id::make(
                      model::producer_id::make(octets).value(),
                      model::producer_epoch::make(7).value(),
                      model::producer_stream_id::make(11).value(),
                      model::batch_sequence{19})
                      .value();
    const auto binding = model::producer_stream_binding::make(
                           model::topic_id::make(octets).value(),
                           model::range_id::make(octets).value(),
                           model::range_routing_epoch::make(13).value(),
                           model::segment_id::make(octets).value(),
                           model::segment_generation::make(17).value())
                           .value();
    measure(
      "batch-context",
      [&] {
          auto submitted = model::submitted_batch_context::make(
            id,
            binding,
            model::range_logical_count{4096},
            kwaque::runtime::wall_time{-5});
          opaque(submitted);
          if (!submitted) return false;
          auto assigned = model::assigned_batch_context::assign(
            *submitted, model::range_logical_end{maximum - 4096U}, binding);
          opaque(assigned);
          auto restored = model::assigned_batch_context::restore(
            *submitted,
            item_count{1},
            model::range_logical_end{maximum - 4096U},
            model::range_logical_end{maximum});
          opaque(restored);
          return assigned && restored && restored->submitted() == *submitted
                 && assigned->logical_span() == restored->logical_span()
                 && assigned->with_retained_count(item_count{1}) == restored;
      },
      is_true);
    measure(
      "warm-invalid-context",
      [&] {
          return model::submitted_batch_context::make(
            id,
            binding,
            model::range_logical_count{},
            kwaque::runtime::wall_time{});
      },
      invalid_argument);

    const auto root = model::keyspace_interval::root();
    std::array singleton{root};
    auto pieces = []<std::size_t... I>(std::index_sequence<I...>) {
        return std::array{model::keyspace_interval::make(
                            static_cast<std::uint64_t>(63U - I) << 58U, 6)
                            .value()...};
    }(std::make_index_sequence<64>{});
    opaque(pieces);
    const auto original_pieces = pieces;
    const auto success = [](const auto& result) { return result.has_value(); };
    measure(
      "coverage-0",
      [&] { return model::validate_keyspace_coverage({}, root); },
      invalid_argument);
    measure(
      "coverage-1",
      [&] { return model::validate_keyspace_coverage(singleton, root); },
      success);
    measure(
      "coverage-64",
      [&] { return model::validate_keyspace_coverage(pieces, root); },
      success);
    require(pieces == original_pieces, "coverage mutated caller input");

    kwaque::codec::limits_config config;
    opaque(config);
    measure(
      "limits",
      [&] {
          const auto policy = kwaque::codec::limits::make(config);
          if (!policy) return false;
          const kwaque::codec::operation_usage usage{
            byte_count{64},
            byte_count{64},
            byte_count{64},
            byte_count{64},
            byte_count{64},
            byte_count{64}};
          const auto remaining = policy->remaining_operation_bytes(
            usage, byte_count{1024});
          return remaining && *remaining == byte_count{640};
      },
      is_true);

    measure(
      "observer-control",
      [] {
          void* pointer = ::operator new(32);
          opaque(pointer);
          ::operator delete(pointer);
          return true;
      },
      is_true,
      allocation_expectation::present);
    std::puts("status=ok");
    return 0;
}
} // namespace

int main(int argc, char** argv) {
    seastar::app_template app;
    app.set_configuration_reader([](boost::program_options::variables_map&) {});
    app.add_options()(
      "capabilities",
      boost::program_options::bool_switch(),
      "Report the compiled allocator profile without exercising values");
    return app.run(argc, argv, [&app] {
        return seastar::async([&app] {
            report_profile();
            if (app.configuration()["capabilities"].as<bool>()) return 0;
#if defined(SEASTAR_DEFAULT_ALLOCATOR)
            constexpr bool expected_abort = false;
#else
            constexpr bool expected_abort = true;
#endif
            require(
              seastar::memory::is_abort_on_allocation_failure()
                == expected_abort,
              "unexpected effective OOM policy");
            return exercise();
        });
    });
}
