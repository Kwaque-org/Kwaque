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
#include "src/codec/sha256.h"
#include "src/codec/sha256_cooperative.h"
#include "src/codec/staging_cooperative.h"
#include "src/codec/tests/allocation_observer.h"
#include "src/codec/tests/envelope_bench_fixture.h"
#include "src/codec/tests/memory_qualification_support.h"
#include "src/codec/tests/qualification_profile.h"
#include "src/codec/transaction.h"

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
  byte_count{16U * 1024U * 1024U}, byte_count{32U * 1024U * 1024U}};
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
    // Keep two differently classified owners alive together. Calibrate actual
    // served capacity, not requested byte counts or net retained memory.
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
    require(
      result.critical_peak_upper_bound == critical_bytes,
      "observer missed critical allocation");
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

    testing::begin_allocation_observation();
    auto* missed = __real_malloc(61);
    if (!missed) std::abort();
    opaque(missed);
    __real_free(missed);
    const auto incomplete = testing::end_allocation_observation();
    require(
      incomplete.observed && !incomplete.complete,
      "observer silently missed a native allocation");
    std::puts("control missed_allocation_detected=true");
#else
    startup_owner.reset();
    report("observer-control", 0, {}, {});
#endif
}

constexpr std::array<codec::sha256_digest, 8> expected_hashes{{
  {0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14, 0x9a, 0xfb, 0xf4,
   0xc8, 0x99, 0x6f, 0xb9, 0x24, 0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b,
   0x93, 0x4c, 0xa4, 0x95, 0x99, 0x1b, 0x78, 0x52, 0xb8, 0x55},
  {0x6e, 0x34, 0x0b, 0x9c, 0xff, 0xb3, 0x7a, 0x98, 0x9c, 0xa5, 0x44,
   0xe6, 0xbb, 0x78, 0x0a, 0x2c, 0x78, 0x90, 0x1d, 0x3f, 0xb3, 0x37,
   0x38, 0x76, 0x85, 0x11, 0xa3, 0x06, 0x17, 0xaf, 0xa0, 0x1d},
  {0xae, 0x4b, 0x32, 0x80, 0xe5, 0x6e, 0x2f, 0xaf, 0x83, 0xf4, 0x14,
   0xa6, 0xe3, 0xda, 0xbe, 0x9d, 0x5f, 0xbe, 0x18, 0x97, 0x65, 0x44,
   0xc0, 0x5f, 0xed, 0x12, 0x1a, 0xcc, 0xb8, 0x5b, 0x53, 0xfc},
  {0x46, 0x3e, 0xb2, 0x8e, 0x72, 0xf8, 0x2e, 0x0a, 0x96, 0xc0, 0xa4,
   0xcc, 0x53, 0x69, 0x0c, 0x57, 0x12, 0x81, 0x13, 0x1f, 0x67, 0x2a,
   0xa2, 0x29, 0xe0, 0xd4, 0x5a, 0xe5, 0x9b, 0x59, 0x8b, 0x59},
  {0xda, 0x2a, 0xe4, 0xd6, 0xb3, 0x67, 0x48, 0xf2, 0xa3, 0x18, 0xf2,
   0x3e, 0x7a, 0xb1, 0xdf, 0xdf, 0x45, 0xac, 0xdc, 0x9d, 0x04, 0x9b,
   0xd8, 0x0e, 0x59, 0xde, 0x82, 0xa6, 0x08, 0x95, 0xf5, 0x62},
  {0xfd, 0xea, 0xb9, 0xac, 0xf3, 0x71, 0x03, 0x62, 0xbd, 0x26, 0x58,
   0xcd, 0xc9, 0xa2, 0x9e, 0x8f, 0x9c, 0x75, 0x7f, 0xcf, 0x98, 0x11,
   0x60, 0x3a, 0x8c, 0x44, 0x7c, 0xd1, 0xd9, 0x15, 0x11, 0x08},
  {0x4b, 0xfd, 0x2c, 0x8b, 0x6f, 0x1e, 0xec, 0x7a, 0x2a, 0xfe, 0xb4,
   0x8b, 0x93, 0x4e, 0xe4, 0xb2, 0x69, 0x41, 0x82, 0x02, 0x7e, 0x6d,
   0x0f, 0xc0, 0x75, 0x07, 0x4f, 0x2f, 0xab, 0xb3, 0x17, 0x81},
  {0xc8, 0xf5, 0xd0, 0x34, 0x1d, 0x54, 0xd9, 0x51, 0xa7, 0x1b, 0x13,
   0x6e, 0x6e, 0x2a, 0xfc, 0xb1, 0x4d, 0x11, 0xed, 0x84, 0x89, 0xa7,
   0xae, 0x12, 0x6a, 0x8f, 0xee, 0x0d, 0xf6, 0xec, 0xf1, 0x93},
}};

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
    if (scenario == "sha-cold")
        require(
          testing::crypto_allocation_calls() == 0,
          "cold SHA was preceded by crypto initialization");
    if (scenario == "sha-warm" || scenario == "sha-churn") {
        codec::sha256_hasher warm;
        warm.update(input.data(), 3);
        static_cast<void>(std::move(warm).final());
    }
    const auto answers = measure(
      scenario, scenario == "sha-churn" ? 4096U : 3U, {}, [&] {
          std::array<codec::sha256_digest, 8> result{};
          const auto rounds = scenario == "sha-churn" ? 16U : 1U;
          for (unsigned round = 0; round < rounds; ++round) {
              for (std::size_t i = 0;
                   i < (scenario == "sha-churn" ? hash_sizes.size() : 1U);
                   ++i) {
                  codec::sha256_hasher hasher;
                  hasher.update(
                    input.data(), scenario == "sha-churn" ? hash_sizes[i] : 3U);
                  result[i] = std::move(hasher).final();
                  opaque(result[i]);
              }
          }
          return result;
      });
    if (scenario == "sha-churn")
        require(answers == expected_hashes, "SHA churn mismatch");
    else
        require(
          answers[0] == expected_hashes[2],
          "SHA observation changed the result");
}
void sha_owner() {
    auto input = make_body(1024U * 1024U);
    const auto retained = retained_cost(input);
    const auto expected_digest = [&] {
        codec::sha256_hasher hasher;
        for (const auto part : input) {
            hasher.update(part.data(), part.size());
            seastar::thread::maybe_yield();
        }
        return std::move(hasher).final();
    }();
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto result = measure("sha-owner", 1024U * 1024U, retained, [&] {
        return codec::sha256_cooperatively(std::move(input), work).get();
    });
    require(result && *result == expected_digest, "SHA owner changed bytes");
}
void envelope(std::string_view scenario) {
    const std::size_t size = scenario.ends_with("max") ? 16U * 1024U * 1024U
                                                       : 64U;
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
    auto memory = codec::reserve_decode_input(
                    input,
                    work.policy(),
                    {residual, byte_count{1024U * 1024U}, charge},
                    context)
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
    codec::decode_budget memory{residual, byte_count{1024U * 1024U}, charge};
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
      || scenario == "sha-cold" || scenario == "sha-warm"
      || scenario == "sha-churn")
        engines(scenario);
    else if (
      scenario == "encode-small" || scenario == "encode-max"
      || scenario == "encode-abort" || scenario == "decode-small"
      || scenario == "decode-max" || scenario == "decode-invalid"
      || scenario == "decode-abort" || scenario == "decode-pressure")
        envelope(scenario);
    else if (scenario == "sha-owner")
        sha_owner();
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
    if (!testing::install_crypto_allocation_observation()) {
        std::fputs(
          "crypto allocation hooks require a fresh process before "
          "initialization\n",
          stderr);
        return 1;
    }
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
