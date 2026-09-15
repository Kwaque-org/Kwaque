#include "src/bytes/fragmented_buffer_test_support.h"
#include "src/compression/lz4.h"
#include "src/compression/tests/lz4_test_support.h"
#include "src/compression/tests/test_support.h"

#include <seastar/core/abort_source.hh>
#include <seastar/util/alloc_failure_injector.hh>

#include <array>
#include <bit>
#include <cstdint>
#include <limits>
#include <new>
#include <optional>
#include <string>

namespace kwaque::compression {
namespace {
using namespace testing;

void expect_wire_error(
  const auto& result,
  errc code,
  std::size_t size,
  codec::field_context where = context) {
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), code);
    EXPECT_EQ(result.error().family(), where.family);
    EXPECT_EQ(result.error().field(), where.field);
    EXPECT_GE(result.error().byte_offset(), where.origin);
    EXPECT_LE(result.error().byte_offset(), where.origin + size);
}

TEST(Lz4StreamTest, IndependentRawAndCompressedBlocksAndEmptyFrames) {
    for (const auto hex : {raw_abc_hex, compressed_abc_hex}) {
        const auto wire = from_hex(hex);
        for (std::size_t width = 1; width <= wire.size(); ++width) {
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            auto input = layout(wire, width);
            auto result
              = decompress_lz4(
                  std::move(input), byte_count{3}, work, budget(), context)
                  .get();
            ASSERT_TRUE(result.has_value());
            EXPECT_TRUE(result->value.content_equals("abc"));
        }
    }
    for (const auto hex :
         {empty_hex,
          std::string_view{"04224d187c400000000000000000c800000000055dcc02"}}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto input = layout(from_hex(hex), 1);
        auto result = decompress_lz4(
                        std::move(input), byte_count{}, work, budget(), context)
                        .get();
        ASSERT_TRUE(result.has_value());
        EXPECT_TRUE(result->value.empty());
        EXPECT_EQ(result->retained.backing, byte_count{});
    }
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto absent = bytes::fragmented_buffer{};
    expect_error(
      decompress_lz4(std::move(absent), byte_count{}, work, budget(), context)
        .get(),
      errc::malformed_data);
}

TEST(Lz4StreamTest, ExactSmallWriterBytesAndOutputCap) {
    for (const auto plain : {std::string_view{}, std::string_view{"abc"}}) {
        const auto expected = from_hex(plain.empty() ? empty_hex : raw_abc_hex);
        for (const bool short_cap : {false, true}) {
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            auto input = layout(plain, 1);
            auto result = compress_lz4(
                            std::move(input),
                            byte_count{expected.size() - (short_cap ? 1U : 0U)},
                            work,
                            budget(),
                            context)
                            .get();
            if (short_cap)
                expect_wire_error(
                  result, errc::resource_exhausted, plain.size());
            else {
                ASSERT_TRUE(result.has_value());
                EXPECT_EQ(flatten(result->value), expected);
                EXPECT_EQ(
                  native_decode(flatten(result->value), plain.size()), plain);
            }
        }
    }
}

TEST(Lz4StreamTest, NativeEndHonorsExactAndOneShortDestinations) {
    for (const std::size_t size : {0U, 3U, 65535U}) {
        std::string plain(size, '\0');
        std::uint32_t state = 0x12345678;
        for (auto& byte : plain) {
            state ^= state << 13U;
            state ^= state >> 17U;
            state ^= state << 5U;
            byte = std::bit_cast<char>(static_cast<std::uint8_t>(state));
        }
        // A buffered raw block contributes its size, header and checksum;
        // End contributes the end mark and content checksum even when empty.
        const auto exact = size == 0 ? 8U : size + 16U;
        for (const bool short_destination : {false, true}) {
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            const auto plan = detail::admit_lz4(
                                detail::lz4_direction::compress,
                                byte_count{size},
                                byte_count{100000},
                                work,
                                budget())
                                .value();
            detail::lz4_context owner{plan};
            ASSERT_TRUE(owner.memory().status());
            const auto prefs = detail::writer_preferences(byte_count{size});
            std::array<char, LZ4F_HEADER_SIZE_MAX> header;
            const auto header_size = LZ4F_compressBegin(
              owner.compressor(), header.data(), header.size(), &prefs);
            native_ok(header_size);
            ASSERT_TRUE(owner.memory().status());
            seastar::temporary_buffer<char> output{65554};
            const auto updated = LZ4F_compressUpdate(
              owner.compressor(),
              output.get_write() + 1,
              output.size() - 2U,
              plain.data(),
              plain.size(),
              nullptr);
            native_ok(updated);
            ASSERT_EQ(updated, 0U);
            const auto bound = LZ4F_compressBound(0, &prefs);
            native_ok(bound);
            ASSERT_LE(exact, bound);
            if (size == 65535) EXPECT_EQ(exact, bound);
            const auto capacity = exact - (short_destination ? 1U : 0U);
            output.get_write()[0] = 'a';
            output.get_write()[capacity + 1U] = 'z';
            const auto ended = LZ4F_compressEnd(
              owner.compressor(), output.get_write() + 1, capacity, nullptr);
            EXPECT_EQ(output.get()[0], 'a');
            EXPECT_EQ(output.get()[capacity + 1U], 'z');
            ASSERT_TRUE(owner.memory().status());
            if (short_destination) {
                ASSERT_TRUE(LZ4F_isError(ended));
                EXPECT_EQ(
                  LZ4F_getErrorCode(ended), LZ4F_ERROR_dstMaxSize_tooSmall);
                // Discard the partially finalized context after failure.
            } else {
                native_ok(ended);
                ASSERT_EQ(ended, exact);
                std::string frame;
                frame.reserve(header_size + ended);
                frame.append(header.data(), header_size);
                frame.append(output.get() + 1, ended);
                EXPECT_EQ(native_decode(frame, size), plain);
            }
        }
    }
}

TEST(Lz4StreamTest, NativeInteroperabilityAcrossBlockAndFragmentBoundaries) {
    for (const auto size : {1U, 7U, 65535U, 65536U, 65537U}) {
        std::string plain(size, 'x');
        std::uint32_t state = 0x12345678;
        for (auto& byte : plain) {
            state ^= state << 13U;
            state ^= state >> 17U;
            state ^= state << 5U;
            byte = std::bit_cast<char>(static_cast<std::uint8_t>(state));
        }
        for (const std::size_t width : {67U, 4096U, 65536U}) {
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            auto input = layout(plain, width);
            auto encoded
              = compress_lz4(
                  std::move(input), byte_count{100000}, work, budget(), context)
                  .get();
            ASSERT_TRUE(encoded.has_value());
            EXPECT_EQ(
              native_decode(flatten(encoded->value), plain.size()), plain);
            // Independent encoder, also split independently on the read path.
            const auto reference = native_frame(plain);
            auto wire = layout(reference, width);
            auto decoded = decompress_lz4(
                             std::move(wire),
                             byte_count{plain.size()},
                             work,
                             budget(),
                             context)
                             .get();
            ASSERT_TRUE(decoded.has_value());
            EXPECT_TRUE(decoded->value.content_equals(plain));
        }
    }
}

TEST(Lz4StreamTest, ReaderAcceptsDifferentPackingAndInvisibleWriterOptions) {
    std::string plain(90000, 'x');
    const auto reference = native_frame(plain, 30000, 1, 1);
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto input = layout(reference, 7);
    auto decoded
      = decompress_lz4(
          std::move(input), byte_count{plain.size()}, work, budget(), context)
          .get();
    ASSERT_TRUE(decoded.has_value());
    EXPECT_TRUE(decoded->value.content_equals(plain));
    // An empty raw block is valid and exceeds our writer's packing bound.
    const auto extra_block = from_hex(
      "04224d187c4003000000000000007400000080055dcc02"
      "03000080616263ff53d13200000000ff53d132");
    auto unpacked = layout(extra_block, 1);
    decoded = decompress_lz4(
                std::move(unpacked), byte_count{3}, work, budget(), context)
                .get();
    ASSERT_TRUE(decoded.has_value());
    EXPECT_TRUE(decoded->value.content_equals("abc"));
}

TEST(Lz4StreamTest, NativeBufferedOutputDrainsWithNoMoreBlockInput) {
    const std::string plain(65536, 'x');
    const auto frame = native_frame(plain);
    LZ4F_dctx* pointer = nullptr;
    native_ok(LZ4F_createDecompressionContext(&pointer, LZ4F_VERSION));
    const auto free = [](LZ4F_dctx* p) {
        (void)LZ4F_freeDecompressionContext(p);
    };
    std::unique_ptr<LZ4F_dctx, decltype(free)> owner{pointer, free};
    auto input = std::string_view{frame}.substr(0, frame.size() - 8U);
    std::array<char, 4098> output;
    std::size_t total = 0;
    bool drained_without_input = false;
    std::size_t code = 1;
    while (total < plain.size()) {
        const auto offered = input.size();
        auto consumed = offered;
        std::size_t produced = 4096;
        output.front() = 'a';
        output.back() = 'z';
        code = LZ4F_decompress(
          owner.get(),
          output.data() + 1,
          &produced,
          input.data(),
          &consumed,
          nullptr);
        native_ok(code);
        ASSERT_LE(consumed, offered);
        ASSERT_LE(produced, 4096U);
        ASSERT_TRUE(consumed != 0 || produced != 0);
        EXPECT_EQ(output.front(), 'a');
        EXPECT_EQ(output.back(), 'z');
        EXPECT_TRUE(
          std::all_of(
            output.begin() + 1,
            output.begin() + 1 + static_cast<std::ptrdiff_t>(produced),
            [](char c) { return c == 'x'; }));
        drained_without_input |= offered == 0 && produced != 0;
        total += produced;
        input.remove_prefix(consumed);
        seastar::thread::maybe_yield();
    }
    EXPECT_TRUE(drained_without_input);
    EXPECT_EQ(total, plain.size());
    EXPECT_TRUE(input.empty());
    EXPECT_NE(code, 0U);
    auto consumed = std::size_t{8};
    auto produced = std::size_t{0};
    code = LZ4F_decompress(
      owner.get(),
      output.data(),
      &produced,
      frame.data() + frame.size() - 8U,
      &consumed,
      nullptr);
    native_ok(code);
    EXPECT_EQ(code, 0U);
    EXPECT_EQ(consumed, 8U);
    EXPECT_EQ(produced, 0U);
}

TEST(Lz4StreamTest, EveryTruncationAndTrailingOrConcatenatedFrameRejects) {
    const auto complete = from_hex(compressed_abc_hex);
    for (std::size_t size = 0; size < complete.size(); ++size) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto input = layout(std::string_view{complete}.substr(0, size), 1);
        expect_wire_error(
          decompress_lz4(
            std::move(input), byte_count{3}, work, budget(), context)
            .get(),
          errc::malformed_data,
          size);
    }
    for (const auto& suffix :
         {std::string(1, '\0'), from_hex(empty_hex), complete}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto input = layout(complete + suffix, 7);
        expect_wire_error(
          decompress_lz4(
            std::move(input), byte_count{3}, work, budget(), context)
            .get(),
          errc::malformed_data,
          complete.size() + suffix.size());
    }
}

TEST(Lz4StreamTest, ChecksumsAndInvalidCompressedBlocksStayDistinct) {
    const auto complete = from_hex(raw_abc_hex);
    for (const std::size_t position : {14U, 19U, 22U, 30U}) {
        auto damaged = complete;
        damaged[position] = static_cast<char>(damaged[position] ^ 1);
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto input = layout(damaged, 7);
        expect_wire_error(
          decompress_lz4(
            std::move(input), byte_count{3}, work, budget(), context)
            .get(),
          errc::corrupt_data,
          damaged.size());
    }
    // Correct block checksum, invalid compressed block contents.
    const auto invalid = from_hex(
      "04224d187c4003000000000000007401000000ffccd8bc9600000000ff53d132");
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto input = layout(invalid, 1);
    expect_wire_error(
      decompress_lz4(std::move(input), byte_count{3}, work, budget(), context)
        .get(),
      errc::malformed_data,
      invalid.size());
}

TEST(Lz4StreamTest, ExpansionDisagreementAndMissingSizeCannotPublish) {
    const auto body = from_hex("03000080616263ff53d13200000000ff53d132");
    for (const auto header :
         {std::string_view{"04224d187c400000000000000000c8"},
          std::string_view{"04224d187440bd"},
          std::string_view{"04224d187c4004000000000000001f"}}) {
        const auto wire = from_hex(header) + body;
        const std::uint64_t expected = header.ends_with("1f") ? 4U : 0U;
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto input = layout(wire, 7);
        expect_wire_error(
          decompress_lz4(
            std::move(input), byte_count{expected}, work, budget(), context)
            .get(),
          errc::malformed_data,
          wire.size());
    }
}

TEST(Lz4StreamTest, EncodedCoordinatesAndReturnedReservationsArePreserved) {
    const std::string plain(65536, 'x');
    const auto encoded = native_frame(plain);
    const codec::field_context near_end{
      .origin = std::numeric_limits<std::uint64_t>::max() - encoded.size(),
      .family = context.family,
      .field = context.field};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto input = layout(encoded, 7);
    const auto memory = budget();
    auto result
      = decompress_lz4(
          std::move(input), byte_count{plain.size()}, work, memory, near_end)
          .get();
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->value.content_equals(plain));
    EXPECT_EQ(result->retained, result->value.allocation_cost(charge).value());
    const auto metadata = result->retained.descriptors.value()
                          + result->retained.share_controls.value();
    EXPECT_EQ(
      memory.operation_remaining.value()
        - result->remaining.operation_remaining.value(),
      result->retained.backing.value() + metadata);
    EXPECT_EQ(
      memory.metadata_remaining.value()
        - result->remaining.metadata_remaining.value(),
      metadata);
    EXPECT_GT(result->retained.backing, byte_count{encoded.size()});
}

TEST(Lz4StreamTest, SmallCompressedOutputKeepsItsReservedDescriptors) {
    const std::string plain(65536, 'x');
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto input = layout(plain, 4096);
    auto result
      = compress_lz4(
          std::move(input), byte_count{100000}, work, budget(), context)
          .get();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->value.fragment_count(), 1U);
    // The writer's maximum frame crosses one 64-KiB output fragment.
    EXPECT_EQ(
      result->retained.descriptors,
      charge(
        byte_count{2U * bytes::fragmented_buffer::fragment_descriptor_size()}));
    EXPECT_EQ(result->retained, result->value.allocation_cost(charge).value());
}

TEST(Lz4StreamTest, AbortDuringDonorCleanupPreventsPublication) {
    for (const bool compress : {false, true}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        bool released = false;
        const auto content = compress ? std::string{"abc"}
                                      : from_hex(raw_abc_hex);
        auto backing = std::make_unique<char[]>(content.size());
        std::copy(content.begin(), content.end(), backing.get());
        auto* pointer = backing.get();
        auto deleter = seastar::make_deleter(
          [backing = std::move(backing), &abort, &released] {
              static_cast<void>(backing);
              released = true;
              abort.request_abort();
          });
        auto storage
          = seastar::temporary_buffer<char>::maybe_unsafe_from_deleter(
            pointer, content.size(), std::move(deleter));
        auto input
          = kwaque::bytes::fragmented_buffer_test_access::adopt_fragment(
              std::move(storage), byte_count{content.size()})
              .value();
        const auto result
          = compress
              ? compress_lz4(
                  std::move(input), byte_count{100}, work, budget(), context)
                  .get()
              : decompress_lz4(
                  std::move(input), byte_count{3}, work, budget(), context)
                  .get();
        EXPECT_TRUE(released);
        expect_error(result, errc::aborted);
    }
}

TEST(Lz4StreamTest, PartialOperationAllocationFailuresLeaveNoPublishedOutput) {
#ifndef SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION
    GTEST_SKIP() << "allocation failure injection is unavailable";
#else
    const auto wire = from_hex(raw_abc_hex);
    for (const bool compress : {false, true}) {
        bool completed = false;
        std::size_t failures = 0;
        for (std::uint64_t ordinal = 0; ordinal < 256 && !completed;
             ++ordinal) {
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            auto input = layout(
              compress ? std::string_view{"abc"} : std::string_view{wire}, 7);
            std::optional<codec::result<owned_result>> result;
            bool threw = false;
            auto& injector = seastar::memory::local_failure_injector();
            injector.fail_after(ordinal);
            try {
                result.emplace(
                  compress ? compress_lz4(
                               std::move(input),
                               byte_count{100},
                               work,
                               budget(),
                               context)
                               .get()
                           : decompress_lz4(
                               std::move(input),
                               byte_count{3},
                               work,
                               budget(),
                               context)
                               .get());
            } catch (const std::bad_alloc&) {
                threw = true;
            } catch (...) {
                injector.cancel();
                throw;
            }
            injector.cancel();
            if (threw) {
                EXPECT_TRUE(injector.failed());
                EXPECT_FALSE(result.has_value());
                ++failures;
            } else {
                ASSERT_TRUE(result.has_value());
                ASSERT_TRUE(result->has_value());
                EXPECT_FALSE(injector.failed());
                completed = true;
            }
        }
        EXPECT_TRUE(completed);
        EXPECT_GE(failures, 3U);
    }
#endif
}

} // namespace
} // namespace kwaque::compression
