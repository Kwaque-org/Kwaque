#include "src/base/allocation.h"
#include "src/base/error.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/bytes/fragmented_buffer_parser.h"
#include "src/bytes/test_allocation_profile.h"
#include "src/codec/collection.h"
#include "src/codec/cooperative.h"
#include "src/codec/crc32c.h"
#include "src/codec/crc32c_cooperative.h"
#include "src/codec/digest.h"
#include "src/codec/envelope.h"
#include "src/codec/envelope_encode.h"
#include "src/codec/staging_cooperative.h"
#include "src/codec/tests/allocation_observer.h"
#include "src/codec/tests/envelope_bench_fixture.h"
#include "src/codec/tests/memory_qualification_support.h"
#include "src/codec/tests/qualification_profile.h"
#include "src/codec/transaction.h"
#include "src/codec/xxh3.h"
#include "src/codec/xxh3_cooperative.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/byteorder.hh>
#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/critical_alloc_section.hh>

#include <boost/program_options.hpp>
#include <crc32c/crc32c.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cerrno>
#include <cinttypes>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <malloc.h>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#if !defined(SEASTAR_DEFAULT_ALLOCATOR)
extern "C" void* __real_malloc(std::size_t) noexcept;
extern "C" void __real_free(void*) noexcept;
#endif

namespace {
namespace codec = kwaque::codec;
namespace testing = codec::testing;
using kwaque::byte_count;
using kwaque::errc;
using kwaque::item_count;
using kwaque::bytes::fragmented_buffer;
using kwaque::bytes::fragmented_buffer_parser;
using kwaque::bytes::testing::charge;
using kwaque::literals::operator""_MiB;
using testing::measure;
using testing::opaque;
using testing::report;
using testing::report_profile;
using testing::require;
using testing::require_effective_policy;
using testing::residual;
using testing::retained_cost;
constexpr codec::field_context context{.origin = 512, .family = 1, .field = 3};
constexpr codec::envelope_extent_limits extents{
  byte_count{16_MiB}, byte_count{32_MiB}};
constexpr codec::bench::envelope_expected_body expected{7, 11};
constexpr std::array<std::size_t, 8> hash_sizes{0, 1, 3, 55, 56, 64, 65, 4096};

fragmented_buffer make_body(
  std::size_t size, std::size_t width = testing::fixture_fragment_bytes) {
    std::vector<seastar::temporary_buffer<char>> fragments;
    fragments.reserve((size + width - 1U) / width);
    for (std::size_t offset = 0; offset < size; offset += width) {
        seastar::temporary_buffer<char> part{std::min(width, size - offset)};
        for (std::size_t i = 0; i < part.size(); ++i)
            part.get_write()[i] = static_cast<char>((offset + i) % 127U);
        if (offset == 0) {
            require(part.size() >= 20, "fixture first fragment is too small");
            seastar::write_le(part.get_write(), expected.object);
            seastar::write_le(part.get_write() + 8, expected.generation);
            seastar::write_le(
              part.get_write() + 16, static_cast<std::uint32_t>(size - 20U));
        }
        fragments.push_back(std::move(part));
        seastar::thread::maybe_yield();
    }
    return fragmented_buffer::copy_from_fragments(fragments).value();
}
std::uint32_t crc(const fragmented_buffer& input) {
    codec::crc32c value;
    for (const auto part : input) {
        value.extend(std::span<const char>{part.data(), part.size()});
        seastar::thread::maybe_yield();
    }
    return value.value();
}
void observer_control(std::unique_ptr<char[]>& startup_owner) {
#if !defined(SEASTAR_DEFAULT_ALLOCATOR)
    // Keep ordinary and critical-scope owners alive together. Both contribute
    // to the served peak; critical classification depends on the build.
    testing::begin_allocation_observation();
    auto* plain = ::operator new(37);
    opaque(plain);
    void* critical = nullptr;
    {
        seastar::memory::scoped_critical_alloc_section section;
        critical = std::malloc(79);
    }
    if (!critical) std::abort();
    opaque(critical);
    const auto plain_bytes = ::malloc_usable_size(plain);
    const auto critical_bytes = ::malloc_usable_size(critical);
    std::free(critical);
    ::operator delete(plain);
    const auto result = testing::end_allocation_observation();
    report("observer-control", 116, {}, result);
    require(
      result.peak_upper_bound == plain_bytes + critical_bytes,
      "observer missed overlapping owners");
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    require(
      result.critical_peak_upper_bound == critical_bytes,
      "observer missed critical allocation");
#else
    require(
      result.critical_peak_upper_bound == 0,
      "observer reported unavailable critical classification");
#endif
    require(
      result.live_upper_bound == 0 && result.allocations == 2
        && result.frees == 2,
      "observer lifetime mismatch");

    // Reallocation must count allocate-before-free overlap and native shrink's
    // synthetic balanced counter events, even for a pre-existing input owner.
    auto* before = std::malloc(19);
    if (!before) std::abort();
    testing::begin_allocation_observation();
    auto* zeroed = std::calloc(2, 41);
    void* aligned = nullptr;
    if (!zeroed || ::posix_memalign(&aligned, 64, 128) != 0) std::abort();
    auto* grown = std::realloc(before, 4096);
    if (!grown) std::abort();
    grown = std::realloc(grown, 17);
    if (!grown) std::abort();
    std::free(zeroed);
    std::free(aligned);
    std::free(grown);
    const auto resized = testing::end_allocation_observation();
    report("observer-reallocation", 4096, {}, resized);
    require(
      resized.live_upper_bound == 0, "observer retained a freed reallocation");

    // Neither a pre-existing native owner nor an owner allocated before the
    // reactor starts belongs to this interval. The latter is a system free,
    // which has its own native statistics counter.
    auto* preexisting = std::malloc(47);
    if (!preexisting) std::abort();
    opaque(preexisting);
    const auto before_release = seastar::memory::stats();
    testing::begin_allocation_observation();
    std::free(preexisting);
    startup_owner.reset();
    const auto initial_release = testing::end_allocation_observation();
    const auto after_release = seastar::memory::stats();
    report("observer-preexisting-free", 108, {}, initial_release);
    require(
      initial_release.allocations == 0 && initial_release.frees == 0
        && initial_release.native_frees == 1
        && initial_release.peak_upper_bound == 0
        && initial_release.live_upper_bound == 0
        && after_release.foreign_cross_frees()
             == before_release.foreign_cross_frees() + 1,
      "pre-existing owners changed the observation");

    testing::begin_allocation_observation();
    auto* conservative = std::malloc(53);
    if (!conservative) std::abort();
    opaque(conservative);
    const auto conservative_size = ::malloc_usable_size(conservative);
    __real_free(conservative);
    const auto retained = testing::end_allocation_observation();
    report("observer-conservative-free", 53, {}, retained);
    require(
      retained.complete && retained.frees == 0 && retained.native_frees == 1
        && retained.live_upper_bound == conservative_size,
      "unwrapped free was not conservatively retained");

    // Keep both exception messages alive across repeated libc translation.
    // Coverage must not depend on warming a particular error or locale cache.
    std::optional<std::system_error> missing, exists;
    testing::begin_allocation_observation();
    missing.emplace(ENOENT, std::system_category());
    exists.emplace(EEXIST, std::system_category());
    const auto messages = testing::end_allocation_observation();
    report("observer-error-message", 0, {}, messages);
    require(
      messages.complete && messages.allocations > 0
        && messages.allocations == messages.native_allocations
        && messages.live_upper_bound > 0
        && missing->code() == std::error_code(ENOENT, std::system_category())
        && exists->code() == std::error_code(EEXIST, std::system_category())
        && std::string_view{missing->what()}.contains(missing->code().message())
        && std::string_view{exists->what()}.contains(exists->code().message()),
      "system-error translation changed semantics or escaped observation");
    missing.reset();
    exists.reset();

    testing::begin_allocation_observation();
    missing.emplace(ENOENT, std::system_category());
    auto* missed = __real_malloc(61);
    if (!missed) std::abort();
    opaque(missed);
    __real_free(missed);
    const auto incomplete = testing::end_allocation_observation();
    missing.reset();
    require(
      incomplete.observed && !incomplete.complete,
      "observer silently missed a native allocation");
    std::puts("control missed_allocation_detected=true");
#else
    startup_owner.reset();
    report("observer-control", 0, {}, {});
#endif
}

// XXH3-128 of the first hash_sizes[i] probe bytes, in canonical form.
constexpr std::array<std::string_view, 8> expected_xxh3{
  "99aa06d3014798d86001c324468d497f",
  "a6cd5e9392000f6ac44bdff4074eecdb",
  "e3b55f57945a17cf5f4299fc161c9cbb",
  "d7420506a37184c24d27405399d46ba6",
  "9e4390d2170659904a1730efd65eb655",
  "9c6e140a465545e590c1971ddb04ce74",
  "ebedf05eeadc28f11aee64a1615de88f",
  "03916578969f7a66eb4b7c3707879151",
};
bool matches(const codec::content_digest& digest, std::string_view expected) {
    constexpr std::string_view digits{"0123456789abcdef"};
    if (expected.size() != 2 * digest.size()) return false;
    for (std::size_t i = 0; i < digest.size(); ++i)
        if (
          expected[2 * i] != digits[digest[i] >> 4U]
          || expected[2 * i + 1] != digits[digest[i] & 15U])
            return false;
    return true;
}

void engines(std::string_view scenario) {
    std::array<char, 4096> input{};
    for (std::size_t i = 0; i < input.size(); ++i)
        input[i] = std::bit_cast<char>(static_cast<std::uint8_t>(i % 256U));
    opaque(input);
    if (scenario.starts_with("crc-") || scenario == "google-cold-4096") {
        const auto size = scenario == "crc-cold-32" ? std::size_t{32}
                                                    : input.size();
        if (scenario == "crc-warm-4096") {
            codec::crc32c warm;
            warm.extend(input);
        }
        const auto value = measure(scenario, size, {}, [&] {
            if (scenario == "google-cold-4096")
                return ::crc32c::Extend(
                  0, reinterpret_cast<const std::uint8_t*>(input.data()), size);
            codec::crc32c checksum;
            checksum.extend(std::span<const char>{input}.first(size));
            return checksum.value();
        });
        require(
          value == (size == 32 ? 0x46dd794eU : 0x9c71fe32U),
          "CRC observation changed the result");
        return;
    }
    // The first XXH3 use in this process, or a churn of fresh hashers.
    const auto churn = scenario == "xxh3-churn";
    const auto answers = measure(scenario, churn ? 4096U : 3U, {}, [&] {
        std::array<codec::content_digest, 8> result{};
        for (unsigned round = 0; round < (churn ? 16U : 1U); ++round) {
            for (std::size_t i = 0; i < (churn ? hash_sizes.size() : 1U); ++i) {
                codec::xxh3_128_hasher hasher;
                hasher.update(input.data(), churn ? hash_sizes[i] : 3U);
                result[i] = std::move(hasher).final();
                opaque(result[i]);
            }
        }
        return result;
    });
    for (std::size_t i = 0; i < (churn ? answers.size() : 1U); ++i)
        require(
          matches(answers[i], expected_xxh3[churn ? i : 2]),
          "XXH3 observation changed the result");
}
void xxh3_owner() {
    auto input = make_body(1_MiB);
    const auto retained = retained_cost(input);
    const auto expected_digest = [&] {
        codec::xxh3_128_hasher hasher;
        for (const auto part : input) {
            hasher.update(part.data(), part.size());
            seastar::thread::maybe_yield();
        }
        return std::move(hasher).final();
    }();
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto result = measure("xxh3-owner", 1_MiB, retained, [&] {
        return codec::xxh3_128_cooperatively(std::move(input), work).get();
    });
    require(result && *result == expected_digest, "XXH3 owner changed bytes");
}
void envelope(std::string_view scenario) {
    const std::size_t size = scenario.ends_with("max") ? 16_MiB : 64U;
    auto body = make_body(size);
    seastar::abort_source setup_abort;
    codec::cooperative_work setup{codec::limits::defaults(), setup_abort};
    // Initialization is intentionally excluded from these warmed owner cases;
    // its cold peak is separately recorded and combined by the driver.
    static_cast<void>(crc(body));
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    if (scenario.starts_with("encode")) {
        const auto retained = retained_cost(body);
        if (scenario == "encode-abort") abort.request_abort();
        auto result = measure(scenario, size, retained, [&] {
            return codec::bench::codec_owned_encode(
                     std::move(body),
                     codec::format_family::submitted_batch,
                     work,
                     extents,
                     {},
                     residual,
                     charge,
                     context)
              .get();
        });
        if (scenario == "encode-abort")
            require(
              !result && result.error().code() == errc::aborted,
              "encoder abort mismatch");
        else
            require(
              result && result->size() == byte_count{size + 32U},
              "encoder extent mismatch");
        return;
    }
    auto wire = codec::encode_envelope(
                  std::move(body),
                  codec::format_family::submitted_batch,
                  setup,
                  extents,
                  {},
                  residual,
                  charge,
                  context)
                  .get()
                  .value();
    const auto retained = retained_cost(wire);
    fragmented_buffer_parser input{std::move(wire)};
    auto memory
      = codec::reserve_decode_input(
          input, work.policy(), {residual, byte_count{1_MiB}, charge}, context)
          .value();
    auto independent = expected;
    if (scenario == "decode-invalid") ++independent.object;
    if (scenario == "decode-abort") abort.request_abort();
    if (scenario == "decode-pressure") memory.metadata_remaining = byte_count{};
    auto result = measure(scenario, size, retained, [&] {
        return codec::bench::codec_owned_decode(
                 input,
                 codec::format_family::submitted_batch,
                 extents,
                 memory,
                 work,
                 independent,
                 context,
                 codec::input_boundary::complete)
          .get();
    });
    if (
      scenario == "decode-invalid" || scenario == "decode-abort"
      || scenario == "decode-pressure") {
        const auto reason = scenario == "decode-invalid" ? errc::wrong_context
                            : scenario == "decode-abort"
                              ? errc::aborted
                              : errc::resource_exhausted;
        require(
          !result && result.error().code() == reason,
          "decoder failure mismatch");
        require(
          input.bytes_consumed() == byte_count{}
            && input.checkpoint_depth() == 0,
          "decoder failure changed parent");
    } else {
        require(
          result && input.at_end() && input.checkpoint_depth() == 0,
          "decoder did not commit exact extent");
        require(
          result->object == expected.object
            && result->generation == expected.generation
            && result->payload.size() == byte_count{size - 20U},
          "decoded value mismatch");
    }
}
void staging() {
    auto prefix = make_body(32, 32);
    auto body = make_body(1023U * 4096U, 4096);
    const auto size = prefix.size().checked_add(body.size()).value();
    const auto retained
      = retained_cost(prefix).checked_add(retained_cost(body)).value();
    codec::crc32c expected_crc;
    for (const auto* part : {&prefix, &body})
        for (const auto fragment : *part)
            expected_crc.extend(
              std::span<const char>{fragment.data(), fragment.size()});
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto output = measure(
      "staging-fragments",
      static_cast<std::size_t>(size.value()),
      retained,
      [&] {
          return codec::assemble_buffer_cooperatively(
                   std::move(prefix),
                   std::move(body),
                   work,
                   size,
                   {},
                   residual,
                   charge,
                   context)
            .get();
      });
    require(
      output && output->size() == size && output->fragment_count() == 1024,
      "staging shape mismatch");
    require(crc(*output) == expected_crc.value(), "staging changed bytes");
}
void collection(std::string_view scenario) {
    struct entry {
        std::uint32_t key;
        std::uint32_t value;
    };
    seastar::chunked_fifo<entry, 16> input;
    for (std::uint32_t i = 8192; i != 0; --i) {
        input.push_back(entry{i, i ^ 0x1234U});
        if (i % 256 == 0) seastar::thread::maybe_yield();
    }
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    codec::decode_budget memory{residual, byte_count{1_MiB}, charge};
    if (scenario == "collection-pressure")
        memory.metadata_remaining = byte_count{1};
    const auto retained = byte_count{
      (8192U / 16U + 1U)
      * charge(byte_count{codec::detail::collection_chunk_bytes<entry, 16>()})
          .value()};
    auto result = measure(scenario, 8192, retained, [&] {
        return codec::canonicalize_unordered(
                 std::move(input),
                 memory,
                 work,
                 [](const entry& a, const entry& b) noexcept {
                     return a.key < b.key;
                 },
                 context)
          .get();
    });
    if (scenario == "collection-pressure") {
        require(
          !result && result.error().code() == errc::resource_exhausted,
          "collection pressure mismatch");
        return;
    }
    require(result && result->size() == 8192, "collection count mismatch");
    std::uint32_t next = 1;
    while (!result->empty()) {
        const auto item = result->front();
        require(
          item.key == next && item.value == (next ^ 0x1234U),
          "collection ordering mismatch");
        ++next;
        work.drain(byte_count{2U * sizeof(entry)}, item_count{2}).get();
        result->pop_front();
    }
}
int exercise(
  std::string_view scenario, std::unique_ptr<char[]>& startup_owner) {
    report_profile();
    std::printf("compiler=%s\n", __clang_version__);
    if (scenario == "capabilities") return 0;
    require_effective_policy();
    if (scenario == "observer-control")
        observer_control(startup_owner);
    else if (
      scenario == "crc-cold-32" || scenario == "crc-cold-4096"
      || scenario == "crc-warm-4096" || scenario == "google-cold-4096"
      || scenario == "xxh3-cold" || scenario == "xxh3-churn")
        engines(scenario);
    else if (
      scenario == "encode-small" || scenario == "encode-max"
      || scenario == "encode-abort" || scenario == "decode-small"
      || scenario == "decode-max" || scenario == "decode-invalid"
      || scenario == "decode-abort" || scenario == "decode-pressure")
        envelope(scenario);
    else if (scenario == "xxh3-owner")
        xxh3_owner();
    else if (scenario == "staging-fragments")
        staging();
    else if (scenario == "collection-8192" || scenario == "collection-pressure")
        collection(scenario);
    else
        throw std::invalid_argument("unknown memory qualification scenario");
    std::puts("status=ok");
    return 0;
}
} // namespace

int main(int argc, char** argv) {
    // Retain a system-allocated owner for the pre-existing-release control.
    auto startup_owner = std::make_unique<char[]>(61);
    opaque(startup_owner);
    seastar::app_template app;
    app.set_configuration_reader([](boost::program_options::variables_map&) {});
    app.add_options()(
      "scenario", boost::program_options::value<std::string>()->required());
    return app.run(argc, argv, [&app, &startup_owner] {
        return seastar::async([&app, &startup_owner] {
            return exercise(
              app.configuration()["scenario"].as<std::string>(), startup_owner);
        });
    });
}
