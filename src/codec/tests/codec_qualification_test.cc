#include "src/base/error.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/bytes/fragmented_buffer_builder.h"
#include "src/bytes/fragmented_buffer_parser.h"
#include "src/bytes/fragmented_buffer_test_support.h"
#include "src/codec/collection.h"
#include "src/codec/cooperative.h"
#include "src/codec/crc32c_cooperative.h"
#include "src/codec/digest.h"
#include "src/codec/envelope_decode.h"
#include "src/codec/error.h"
#include "src/codec/limits.h"
#include "src/codec/sha256_cooperative.h"
#include "src/codec/staging_cooperative.h"
#include "src/codec/tests/envelope_decode_test_support.h"
#include "src/codec/transaction.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/deleter.hh>
#include <seastar/core/future.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/later.hh>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <bit>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <limits>
#include <malloc.h>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

namespace codec = kwaque::codec;
using kwaque::byte_count;
using kwaque::item_count;
using kwaque::bytes::fragmented_buffer;
using native_fragment = seastar::temporary_buffer<char>;
using namespace std::literals;

constexpr byte_count parent_budget{64U * 1024U * 1024U};

// This is a conservative fixture allowance. Usable-size observations below
// verify its payload bound only for the allocations made by that fixture.
byte_count charge(byte_count requested) noexcept {
    if (requested.value() == 0) {
        return {};
    }
    if (requested.value() > (std::uint64_t{1} << 62U)) {
        return byte_count{std::numeric_limits<std::uint64_t>::max()};
    }
    return byte_count{
      2U * std::bit_ceil(std::max(requested.value(), std::uint64_t{16}))};
}

fragmented_buffer fixed_layout(std::size_t count, std::size_t width) {
    std::vector<native_fragment> fragments;
    fragments.reserve(count);
    for (std::size_t index = 0; index < count; ++index) {
        native_fragment fragment{width};
        std::fill_n(
          fragment.get_write(), width, static_cast<char>(index % 127U));
        fragments.push_back(std::move(fragment));
        seastar::thread::maybe_yield();
    }
    return fragmented_buffer::copy_from_fragments(fragments).value();
}

TEST(CodecQualificationTest, LargestFragmentComposesWithBothIntegrityPasses) {
    auto payload = [] {
        native_fragment fragment{128U * 1024U};
        for (std::size_t index = 0; index < fragment.size(); ++index) {
            fragment.get_write()[index] = std::bit_cast<char>(
              static_cast<std::uint8_t>(index % 251U));
        }
        return fragmented_buffer::copy_from_fragment(fragment).value();
    }();
    ASSERT_EQ(payload.fragment_count(), 1);
    auto prefix = fragmented_buffer::copy_of("hdr:"sv).value();
    codec::limits_config config;
    config.max_work_bytes = byte_count{1024};
    config.max_work_items = item_count{8};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::make(config).value(), abort};
    auto assembled = codec::assemble_buffer_cooperatively(
                       std::move(prefix),
                       std::move(payload),
                       work,
                       byte_count{131076},
                       {},
                       parent_budget,
                       charge)
                       .get();
    ASSERT_TRUE(assembled.has_value());
    // Assembly transfers both buffers, whose move contract empties the sources.
    // NOLINTBEGIN(bugprone-use-after-move)
    EXPECT_TRUE(prefix.empty());
    EXPECT_TRUE(payload.empty());
    // NOLINTEND(bugprone-use-after-move)
    ASSERT_EQ(assembled->size(), byte_count{131076});
    auto crc_input = assembled->share();
    auto sha_input = assembled->share();
    assembled = fragmented_buffer{};
    const auto crc = codec::crc32c_cooperatively(
                       std::move(crc_input), work, 0x2468ace0U)
                       .get();
    const auto sha
      = codec::sha256_cooperatively(std::move(sha_input), work).get();
    ASSERT_TRUE(crc.has_value());
    EXPECT_EQ(*crc, 0xef65f5f5U);
    constexpr codec::sha256_digest expected{
      0x9f, 0x65, 0xb6, 0xeb, 0xa1, 0x97, 0x68, 0xd4, 0xa5, 0xc2, 0x9a,
      0x75, 0xde, 0xff, 0xaf, 0x26, 0xc6, 0xf3, 0x70, 0x7f, 0xbd, 0xe3,
      0x38, 0xe6, 0x27, 0xb8, 0x37, 0x32, 0x2d, 0x7b, 0xf0, 0x1d};
    ASSERT_TRUE(sha.has_value());
    EXPECT_EQ(*sha, expected);
}

struct releases final {
    std::size_t live{0};
    std::size_t destroyed{0};
    seastar::promise<>* first{nullptr};
};

fragmented_buffer
adopted_fragment(std::size_t size, char value, releases& state) {
    auto memory = std::make_unique<char[]>(size);
    std::fill_n(memory.get(), size, value);
    auto* raw = memory.get();
    auto deleter = seastar::make_deleter(
      [memory = std::move(memory), &state] noexcept {
          static_cast<void>(memory);
          --state.live;
          ++state.destroyed;
          if (auto* first = std::exchange(state.first, nullptr)) {
              first->set_value();
          }
      });
    ++state.live;
    auto storage = native_fragment::maybe_unsafe_from_deleter(
      raw, size, std::move(deleter));
    return kwaque::bytes::fragmented_buffer_test_access::adopt_fragment(
             std::move(storage), byte_count{size})
      .value();
}

fragmented_buffer
packing_input(std::size_t count, char value, releases& state) {
    kwaque::bytes::fragmented_buffer_builder_config config;
    config.initial_fragment_bytes = byte_count{1};
    config.max_fragment_bytes = byte_count{1};
    config.max_total_bytes = byte_count{count * 4096U};
    config.max_retained_bytes = byte_count{32U * 1024U * 1024U};
    config.max_fragments = count;
    kwaque::bytes::fragmented_buffer_builder builder{config};
    builder.reserve_fragments(item_count{count}).value();
    for (std::size_t index = 0; index < count; ++index) {
        auto fragment = adopted_fragment(4096, value, state);
        // Every donation exceeds this fixture builder's one-byte copy cap.
        builder.append_buffer(std::move(fragment)).value();
        seastar::thread::maybe_yield();
    }
    return builder.finish().value();
}

TEST(
  CodecQualificationTest,
  MaximumFragmentAssemblyMakesProgressWhileRetainingBacking) {
    bool progressed = false;
    const auto deadline = std::chrono::steady_clock::now()
                          + std::chrono::seconds{2};
    for (unsigned attempt = 0; attempt < 8 && !progressed
                               && std::chrono::steady_clock::now() < deadline;
         ++attempt) {
        releases state;
        auto prefix = packing_input(1, 'p', state);
        auto payload = packing_input(1023, 'q', state);
        ASSERT_EQ(prefix.fragment_count(), 1);
        ASSERT_EQ(payload.fragment_count(), 1023);
        for (const auto fragment : payload) {
            ASSERT_EQ(fragment.size(), 4096);
        }
        ASSERT_EQ(state.live, 1024);
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        bool active = false;
        bool observed = false;
        auto observer = seastar::yield().then([&] {
            observed = active && state.destroyed == 0 && state.live == 1024;
        });
        // Enter with a real pending preemption request, so the operation's
        // bounded checkpoints must let this queued observer run.
        while (!seastar::need_preempt()
               && std::chrono::steady_clock::now() < deadline) {
        }

        std::optional<codec::result<fragmented_buffer>> assembled;
        std::exception_ptr failure;
        active = true;
        try {
            assembled.emplace(
              codec::assemble_buffer_cooperatively(
                std::move(prefix),
                std::move(payload),
                work,
                byte_count{4U * 1024U * 1024U},
                codec::operation_usage{
                  .payload_bookkeeping = byte_count{128U * 1024U}},
                parent_budget,
                charge)
                .get());
        } catch (...) {
            failure = std::current_exception();
        }
        active = false;
        observer.get();
        if (failure) {
            std::rethrow_exception(failure);
        }
        ASSERT_TRUE(assembled.has_value());
        ASSERT_TRUE(assembled->has_value());
        EXPECT_EQ(state.live, 1024);
        EXPECT_EQ(state.destroyed, 0);
        const auto& output = **assembled;
        EXPECT_EQ(output.size(), byte_count{4U * 1024U * 1024U});
        EXPECT_LE(output.fragment_count(), 1024);
        std::size_t offset = 0;
        bool matches = true;
        for (const auto fragment : output) {
            for (const auto value : fragment.bytes()) {
                matches = matches && value == (offset < 4096 ? 'p' : 'q');
                ++offset;
            }
            seastar::thread::maybe_yield();
        }
        EXPECT_TRUE(matches);
        EXPECT_EQ(offset, 4U * 1024U * 1024U);
        assembled.reset();
        EXPECT_EQ(state.live, 0);
        EXPECT_EQ(state.destroyed, 1024);
        progressed = observed;
    }
    EXPECT_TRUE(progressed);
}

TEST(
  CodecQualificationTest,
  RetainedPayloadObservationsStayBelowTheirConservativeCharge) {
    auto input = fixed_layout(1024, 16U * 1024U);
    ASSERT_EQ(input.fragment_count(), 1024);
    std::array<const char*, 1024> bases{};
    std::array<std::size_t, 1024> served{};
    std::size_t observed_payload = 0;
    std::size_t index = 0;
    for (const auto fragment : input) {
        ASSERT_EQ(fragment.size(), 16U * 1024U);
        bases[index] = fragment.data();
        // This fixture copied whole native malloc allocations. These addresses
        // are allocation bases, never offsets into arbitrary shared slices.
        served[index] = ::malloc_usable_size(
          const_cast<char*>(fragment.data()));
        ASSERT_GE(served[index], fragment.size());
        ASSERT_LE(served[index], charge(byte_count{fragment.size()}).value());
        observed_payload += served[index];
        ++index;
    }
    const auto input_cost = input.allocation_cost(charge).value();
    ASSERT_EQ(input_cost.backing, byte_count{32U * 1024U * 1024U});
    EXPECT_LE(byte_count{observed_payload}, input_cost.backing);
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto output = codec::assemble_buffer_cooperatively(
                    fragmented_buffer{},
                    std::move(input),
                    work,
                    byte_count{16U * 1024U * 1024U},
                    {},
                    parent_budget,
                    charge)
                    .get();
    ASSERT_TRUE(output.has_value());
    // The buffer move contract guarantees an empty source after assembly.
    // NOLINTNEXTLINE(bugprone-use-after-move)
    EXPECT_TRUE(input.empty());
    ASSERT_EQ(output->fragment_count(), 1024);
    index = 0;
    for (const auto fragment : *output) {
        EXPECT_EQ(fragment.data(), bases[index]);
        EXPECT_EQ(
          ::malloc_usable_size(const_cast<char*>(fragment.data())),
          served[index]);
        EXPECT_EQ(fragment.bytes().front(), static_cast<char>(index % 127U));
        EXPECT_EQ(fragment.bytes().back(), static_cast<char>(index % 127U));
        ++index;
        seastar::thread::maybe_yield();
    }
    const auto output_cost = output->allocation_cost(charge).value();
    EXPECT_EQ(output_cost.backing, input_cost.backing);
    EXPECT_LE(
      output_cost.largest_allocation,
      work.policy().config().max_allocation_bytes);
    RecordProperty(
      "observed_retained_payload_bytes", static_cast<int>(observed_payload));
    RecordProperty("conservative_retained_backing_bytes", 32 * 1024 * 1024);
    // These observations exclude descriptor/share owners, coroutine/native
    // engine state, allocator pages and transient allocations. They are not an
    // operation peak measurement or a backend-selection performance result.
}

struct entry final {
    std::uint32_t key;
    std::uint32_t value;
};

TEST(
  CodecQualificationTest,
  ThousandsOfEntriesMergeCompletelyWithObservedControlProgress) {
    bool progressed = false;
    const auto deadline = std::chrono::steady_clock::now()
                          + std::chrono::seconds{2};
    for (unsigned attempt = 0; attempt < 8 && !progressed
                               && std::chrono::steady_clock::now() < deadline;
         ++attempt) {
        seastar::chunked_fifo<entry, 16> source;
        for (std::uint32_t key = 8192; key != 0; --key) {
            source.push_back(entry{key - 1U, (key - 1U) ^ 0x13579bdfU});
        }
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        seastar::promise<> first_comparison;
        auto* pending = &first_comparison;
        std::size_t comparisons = 0;
        std::size_t observed_comparisons = 0;
        auto observer = first_comparison.get_future().then(
          [&] { observed_comparisons = comparisons; });
        std::optional<codec::result<seastar::chunked_fifo<entry, 16>>> output;
        std::exception_ptr failure;
        try {
            output.emplace(
              codec::canonicalize_unordered(
                std::move(source),
                codec::decode_budget{
                  parent_budget, byte_count{1024U * 1024U}, charge},
                work,
                [&](const entry& left, const entry& right) noexcept {
                    ++comparisons;
                    if (auto* ready = std::exchange(pending, nullptr)) {
                        ready->set_value();
                    }
                    return left.key < right.key;
                })
                .get());
        } catch (...) {
            failure = std::current_exception();
        }
        if (auto* ready = std::exchange(pending, nullptr)) {
            ready->set_value();
        }
        observer.get();
        if (failure) {
            std::rethrow_exception(failure);
        }
        ASSERT_TRUE(output.has_value());
        ASSERT_TRUE(output->has_value());
        ASSERT_EQ((*output)->size(), 8192);
        std::uint32_t expected = 0;
        while (!(*output)->empty()) {
            EXPECT_EQ((*output)->front().key, expected);
            EXPECT_EQ((*output)->front().value, expected ^ 0x13579bdfU);
            ++expected;
            work.drain(byte_count{2U * sizeof(entry)}, item_count{2}).get();
            (*output)->pop_front();
        }
        EXPECT_EQ(expected, 8192);
        progressed = observed_comparisons != 0
                     && observed_comparisons < comparisons;
    }
    EXPECT_TRUE(progressed);
}

namespace envelope_fixture = codec::testing::envelope_fixture;

constexpr codec::field_context envelope_context{
  .origin = 700, .family = 1, .field = 91};
constexpr codec::envelope_extent_limits envelope_bounds{
  .max_body_bytes = byte_count{16U * 1024U * 1024U},
  .max_encoded_bytes = byte_count{32U * 1024U * 1024U}};

// Only this fixture's opaque body grammar: byte i contains (i / 65536) % 127.
// The four header checksums below were derived independently from literal
// prefix fields, zeroed self slot and the exact tagged bytes.
std::string envelope_header(
  std::uint32_t body_bytes,
  std::uint32_t body_checksum,
  std::uint16_t extensions,
  bool maximum_header) {
    const auto size = maximum_header ? std::size_t{4096}
                                     : 32U + 8U * extensions;
    std::string header(size, '\0');
    header.replace(0, 4, "KQBF");
    envelope_fixture::put_u16(header, 4, 1);
    envelope_fixture::put_u16(header, 6, extensions == 0 ? 1 : 2);
    envelope_fixture::put_u16(header, 8, 1);
    envelope_fixture::put_u16(header, 10, static_cast<std::uint16_t>(size));
    envelope_fixture::put_u32(header, 12, body_bytes);
    envelope_fixture::put_u32(header, 24, body_checksum);
    for (std::uint16_t index = 0; index < extensions; ++index) {
        const auto offset = std::size_t{32} + 8U * index;
        envelope_fixture::put_u16(
          header, offset, static_cast<std::uint16_t>(index + 1U));
        if (maximum_header && index == 63) {
            envelope_fixture::put_u32(header, offset + 4U, 3552);
            std::fill(
              header.begin() + static_cast<std::ptrdiff_t>(offset + 8U),
              header.end(),
              static_cast<char>(0xa5));
        }
    }
    envelope_fixture::repair_header_crc(header);
    return header;
}

fragmented_buffer
envelope_input(fragmented_buffer body, std::string_view header) {
    kwaque::bytes::fragmented_buffer_builder_config config;
    config.initial_fragment_bytes = byte_count{1};
    config.max_fragment_bytes = byte_count{1};
    config.max_total_bytes = byte_count{header.size() + body.size().value()};
    config.max_retained_bytes = byte_count{32U * 1024U * 1024U};
    config.max_fragments = 1 + body.fragment_count();
    kwaque::bytes::fragmented_buffer_builder builder{config};
    builder.reserve_fragments(item_count{config.max_fragments}).value();
    auto prefix = fragmented_buffer::copy_of(header).value();
    builder.append_buffer(std::move(prefix)).value();
    builder.append_buffer(std::move(body)).value();
    return builder.finish().value();
}

struct envelope_progress final {
    std::uint64_t verified{0};
    bool active{false};
    seastar::promise<>* first{nullptr};
    seastar::abort_source* abort_after_first{nullptr};
};

struct pattern_body_decoder final {
    envelope_progress& progress;
    byte_count expected_bytes;

    seastar::future<codec::result<std::uint64_t>> operator()(
      kwaque::bytes::fragmented_buffer_parser& body,
      codec::field_context context,
      codec::input_boundary boundary,
      codec::decode_budget,
      codec::cooperative_work& work) {
        progress.active = true;
        auto cleanup = seastar::defer(
          [this] noexcept { progress.active = false; });
        if (
          boundary != codec::input_boundary::complete
          || body.total_bytes() != expected_bytes) {
            co_return codec::failure(
              codec::error{
                kwaque::errc::wrong_context,
                context.family,
                context.field,
                context.origin});
        }
        const codec::error anchor{
          kwaque::errc::success, context.family, context.field, context.origin};
        while (!body.at_end()) {
            const auto view = body.peek_current_fragment();
            const auto count = std::min<std::uint64_t>(
              view.size(), work.byte_quantum().value() / 2U);
            if (count == 0) {
                co_return codec::failure(
                  codec::error{
                    kwaque::errc::resource_exhausted,
                    context.family,
                    context.field,
                    context.origin + progress.verified});
            }
            const auto admitted = co_await work.admit(
              byte_count{2U * count}, item_count{2}, anchor);
            if (!admitted) {
                co_return codec::failure(admitted.error());
            }
            if (auto ready = work.poll(anchor); !ready) {
                co_return codec::failure(ready.error());
            }
            for (std::uint64_t index = 0; index < count; ++index) {
                const auto expected = static_cast<char>(
                  ((progress.verified + index) / 65536U) % 127U);
                if (view.data()[index] != expected) {
                    co_return codec::failure(
                      codec::error{
                        kwaque::errc::malformed_data,
                        context.family,
                        context.field,
                        context.origin + progress.verified + index});
                }
            }
            body.skip(byte_count{count}).value();
            progress.verified += count;
            if (auto* first = std::exchange(progress.first, nullptr)) {
                first->set_value();
            }
            if (
              auto* abort = std::exchange(
                progress.abort_after_first, nullptr)) {
                abort->request_abort();
            }
        }
        co_return progress.verified;
    }
};

TEST(CodecQualificationTest, EnvelopeHeaderShapesUseTheSameExactBody) {
    struct shape final {
        std::uint16_t extensions;
        bool maximum;
        std::uint32_t header_checksum;
    };
    for (const auto sample : std::array{
           shape{0, false, 0xfa6aca26U},
           shape{1, false, 0x0c02a164U},
           shape{64, false, 0x48e17afaU},
           shape{64, true, 0x4efe4240U}}) {
        const auto header = envelope_header(
          65536, 0x72c0c4a4U, sample.extensions, sample.maximum);
        auto zeroed = header;
        envelope_fixture::put_u32(zeroed, 28, 0);
        ASSERT_EQ(envelope_fixture::crc32c(zeroed), sample.header_checksum);
        auto body = fixed_layout(1, 65536);
        kwaque::bytes::fragmented_buffer_parser input{
          envelope_input(std::move(body), header)};
        seastar::abort_source abort;
        codec::limits_config config;
        config.max_work_bytes = byte_count{1024};
        config.max_work_items = item_count{64};
        codec::cooperative_work work{
          codec::limits::make(config).value(), abort};
        const auto memory = codec::reserve_decode_input(
                              input,
                              work.policy(),
                              {byte_count{32U * 1024U * 1024U},
                               byte_count{1024U * 1024U},
                               charge},
                              envelope_context)
                              .value();
        envelope_progress progress;
        const auto result = codec::decode_envelope<std::uint64_t>(
                              input,
                              codec::format_family::submitted_batch,
                              envelope_bounds,
                              memory,
                              work,
                              pattern_body_decoder{progress, byte_count{65536}},
                              envelope_context)
                              .get();
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(*result, 65536U);
        EXPECT_EQ(input.bytes_consumed(), byte_count{header.size() + 65536U});
        EXPECT_EQ(input.checkpoint_depth(), 0U);
        EXPECT_FALSE(progress.active);
    }
}

TEST(CodecQualificationTest, EnvelopeBodyAbortAfterProgressRollsBackTheParent) {
    auto body = fixed_layout(1, 65536);
    const auto header = envelope_header(65536, 0x72c0c4a4U, 64, true);
    kwaque::bytes::fragmented_buffer_parser input{
      envelope_input(std::move(body), header)};
    seastar::abort_source abort;
    codec::limits_config config;
    config.max_work_bytes = byte_count{1024};
    config.max_work_items = item_count{64};
    codec::cooperative_work work{codec::limits::make(config).value(), abort};
    const auto memory
      = codec::reserve_decode_input(
          input,
          work.policy(),
          {byte_count{32U * 1024U * 1024U}, byte_count{1024U * 1024U}, charge},
          envelope_context)
          .value();
    envelope_progress progress{.abort_after_first = &abort};
    const auto result = codec::decode_envelope<std::uint64_t>(
                          input,
                          codec::format_family::submitted_batch,
                          envelope_bounds,
                          memory,
                          work,
                          pattern_body_decoder{progress, byte_count{65536}},
                          envelope_context)
                          .get();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), kwaque::errc::aborted);
    EXPECT_GT(progress.verified, 0U);
    EXPECT_LT(progress.verified, 65536U);
    EXPECT_FALSE(progress.active);
    EXPECT_EQ(input.bytes_consumed(), byte_count{});
    EXPECT_EQ(input.total_bytes(), byte_count{4096 + 65536});
    EXPECT_EQ(input.checkpoint_depth(), 0U);
}

#if !defined(SEASTAR_DEFAULT_ALLOCATOR)
// Native large allocations retain a power-of-two page span; their sampled
// bookkeeping lives outside the payload. Small requests retain a doubled
// power-of-two bound. This monotone profile is not valid for the system heap.
byte_count envelope_native_charge(byte_count request) noexcept {
    if (request.value() == 0) {
        return {};
    }
    if (request.value() > (std::uint64_t{1} << 62U)) {
        return byte_count{std::numeric_limits<std::uint64_t>::max()};
    }
    const auto power = std::bit_ceil(
      std::max(request.value(), std::uint64_t{16}));
    return byte_count{request.value() <= 16384 ? 2U * power : power};
}
#endif

TEST(CodecQualificationTest, MaximumEnvelopeBodyMakesObservedControlProgress) {
#if defined(SEASTAR_DEFAULT_ALLOCATOR)
    GTEST_SKIP() << "maximum-body geometry uses the native allocator profile";
#else
    constexpr byte_count maximum_body{16U * 1024U * 1024U};
    auto body = fixed_layout(256, 65536);
    ASSERT_EQ(body.size(), maximum_body);
    ASSERT_EQ(body.fragment_count(), 256U);
    std::uint64_t observed_payload = 0;
    for (const auto fragment : body) {
        ASSERT_EQ(fragment.size(), 65536U);
        const auto served = ::malloc_usable_size(
          const_cast<char*>(fragment.data()));
        ASSERT_GE(served, fragment.size());
        ASSERT_LE(
          served, envelope_native_charge(byte_count{fragment.size()}).value());
        observed_payload += served;
    }
    const auto header = envelope_header(
      static_cast<std::uint32_t>(maximum_body.value()), 0x0bc6b39fU, 64, true);
    auto zeroed = header;
    envelope_fixture::put_u32(zeroed, 28, 0);
    ASSERT_EQ(envelope_fixture::crc32c(zeroed), 0xdf53d157U);
    kwaque::bytes::fragmented_buffer_parser input{
      envelope_input(std::move(body), header)};
    seastar::abort_source abort;
    codec::limits_config config;
    config.max_work_bytes = byte_count{1024};
    config.max_work_items = item_count{64};
    codec::cooperative_work work{codec::limits::make(config).value(), abort};
    const auto memory = codec::reserve_decode_input(
                          input,
                          work.policy(),
                          {byte_count{63U * 1024U * 1024U},
                           byte_count{1024U * 1024U},
                           envelope_native_charge},
                          envelope_context)
                          .value();
    seastar::promise<> first_progress;
    envelope_progress progress{.first = &first_progress};
    bool observed = false;
    auto observer = first_progress.get_future().then([&] {
        observed = progress.active && progress.verified != 0
                   && progress.verified < maximum_body.value();
    });
    const auto allocations_before = seastar::memory::stats().mallocs();
    std::optional<codec::result<std::uint64_t>> result;
    std::exception_ptr failure;
    try {
        result.emplace(
          codec::decode_envelope<std::uint64_t>(
            input,
            codec::format_family::submitted_batch,
            envelope_bounds,
            memory,
            work,
            pattern_body_decoder{progress, maximum_body},
            envelope_context)
            .get());
    } catch (...) {
        failure = std::current_exception();
    }
    if (auto* first = std::exchange(progress.first, nullptr)) {
        first->set_value();
    }
    observer.get();
    const auto allocations_after = seastar::memory::stats().mallocs();
    if (failure) {
        std::rethrow_exception(failure);
    }
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result->has_value());
    EXPECT_EQ(**result, maximum_body.value());
    EXPECT_TRUE(observed);
    EXPECT_FALSE(progress.active);
    EXPECT_TRUE(input.at_end());
    EXPECT_EQ(input.checkpoint_depth(), 0U);
    EXPECT_GE(allocations_after, allocations_before);
    RecordProperty(
      "envelope_decode_native_allocation_count",
      std::to_string(allocations_after - allocations_before));
    RecordProperty(
      "envelope_observed_retained_payload_bytes",
      std::to_string(observed_payload));
    // Allocation counts include native/critical coroutine activity; payload
    // observations cover exact allocation bases. Neither measures transient
    // peak memory, allocator pages, frame sizes or end-to-end performance.
#endif
}

} // namespace
