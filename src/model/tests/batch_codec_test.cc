#include "src/bytes/fragmented_buffer_parser.h"
#include "src/codec/sha256.h"
#include "src/model/batch_builder.h"
#include "src/model/batch_codec.h"
#include "src/model/batch_rewrite.h"
#include "src/model/record_scan.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/preempt.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/util/alloc_failure_injector.hh>
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
#include <new>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace {
using namespace std::literals;
namespace model = kwaque::model;
namespace codec = kwaque::codec;
using kwaque::byte_count;
using kwaque::errc;
using kwaque::item_count;
using kwaque::bytes::fragmented_buffer;
using kwaque::bytes::fragmented_buffer_parser;
using kwaque::runtime::wall_time;
using delta = model::range_logical_count;
using nullable = std::optional<fragmented_buffer>;

constexpr auto submitted_envelope
  = "4b5142460100010001002000af00000000000000000000009cc9e480c3709e26"
    "0102030405060708090a0b0c0d0e0f1002000000000000000300000000000000"
    "04000000000000002122232425262728292a2b2c2d2e2f304142434445464748"
    "494a4b4c4d4e4f5007000000000000006162636465666768696a6b6c6d6e6f70"
    "0900000000000000a094509035402abad319ae9b75972a8c3eb583f3c4829c435"
    "3c21f92bad9d9a7640000000000000001000000010000000000000000000100"
    "070000000700000006000000010100"sv;
constexpr auto dense_envelope
  = "4b5142460200010001002000db0000000000000000000000c5342bd5689c9ed7"
    "0102030405060708090a0b0c0d0e0f1002000000000000000300000000000000"
    "04000000000000002122232425262728292a2b2c2d2e2f304142434445464748"
    "494a4b4c4d4e4f5007000000000000006162636465666768696a6b6c6d6e6f70"
    "0900000000000000629cb7e9505f05b8ae49fe6ba38ea0ad5146c22f973fc119"
    "5b2da4fe00869ce3640000000000000005000000050000000000000000000100"
    "23000000230000006400000000000000690000000000000006000000010100"
    "06000101010100060014020101000600030301010006000204010100"sv;
constexpr auto sparse_envelope
  = "4b5142460200010001002000c6000000000000000000000051a3849d4be31590"
    "0102030405060708090a0b0c0d0e0f1002000000000000000300000000000000"
    "04000000000000002122232425262728292a2b2c2d2e2f304142434445464748"
    "494a4b4c4d4e4f5007000000000000006162636465666768696a6b6c6d6e6f70"
    "0900000000000000629cb7e9505f05b8ae49fe6ba38ea0ad5146c22f973fc119"
    "5b2da4fe00869ce3640000000000000005000000020000000000000000000100"
    "0e0000000e0000006400000000000000690000000000000006000101010100"
    "06000303010100"sv;
constexpr auto rich_envelope
  = "4b5142460100010001002000b5000000000000000000000056a0b5ece928823a"
    "0102030405060708090a0b0c0d0e0f1002000000000000000300000000000000"
    "04000000000000002122232425262728292a2b2c2d2e2f304142434445464748"
    "494a4b4c4d4e4f5007000000000000006162636465666768696a6b6c6d6e6f70"
    "0900000000000000256fc3357a0312ef7310b95fa1039b5f93dfbd296645eed3"
    "df76f1cd11444ec0640000000000000001000000010000000200000000000100"
    "0d0000000d0000000c000000026b0102000101ff00"sv;

static_assert(
  std::is_nothrow_move_constructible_v<model::decoded_submitted_batch>);
static_assert(
  std::is_nothrow_move_constructible_v<model::decoded_assigned_batch>);
static_assert(std::is_nothrow_destructible_v<model::decoded_assigned_batch>);
constexpr codec::field_context coordinates{.origin = 1000};
byte_count charge(byte_count request) noexcept {
    if (request.value() == 0) return {};
    if (request.value() > (std::uint64_t{1} << 62U))
        return byte_count{UINT64_MAX};
    return byte_count{
      2U * std::bit_ceil(std::max(request.value(), std::uint64_t{16}))};
}
codec::decode_budget memory() {
    // Other live fixtures and verified native/frame reservations use the
    // unclaimed half of the operation ceiling; these charges are not RSS.
    return {byte_count{32U << 20U}, byte_count{1U << 20U}, charge};
}
template<typename Id>
Id object(std::uint8_t first) {
    std::array<std::uint8_t, 16> value{};
    for (std::size_t i = 0; i < value.size(); ++i)
        value[i] = static_cast<std::uint8_t>(first + i);
    return Id::make(value).value();
}
model::batch_id identity() {
    return model::batch_id::make(
             object<model::producer_id>(1),
             model::producer_epoch::make(2).value(),
             model::producer_stream_id::make(3).value(),
             model::batch_sequence{4})
      .value();
}
model::producer_stream_binding
binding(std::uint8_t segment = 97, std::uint64_t generation = 9) {
    return model::producer_stream_binding::make(
             object<model::topic_id>(33),
             object<model::range_id>(65),
             model::range_routing_epoch::make(7).value(),
             object<model::segment_id>(segment),
             model::segment_generation::make(generation).value())
      .value();
}
model::batch_decode_expectation expected() {
    return {object<model::topic_id>(33), object<model::range_id>(65)};
}
std::string unhex(std::string_view hex) {
    const auto digit = [](char c) { return c >= 'a' ? c - 'a' + 10 : c - '0'; };
    std::string out;
    for (std::size_t i = 0; i < hex.size(); i += 2)
        out.push_back(
          static_cast<char>(16 * digit(hex[i]) + digit(hex[i + 1])));
    return out;
}
fragmented_buffer bytes(std::string_view raw, std::size_t width = 65536) {
    std::vector<seastar::temporary_buffer<char>> parts;
    for (std::size_t i = 0; i < raw.size(); i += width) {
        auto piece = raw.substr(i, width);
        seastar::temporary_buffer<char> part{piece.size()};
        std::copy(piece.begin(), piece.end(), part.get_write());
        parts.push_back(std::move(part));
    }
    return fragmented_buffer::copy_from_fragments(parts).value();
}
fragmented_buffer split(std::string_view raw, std::size_t cut) {
    std::vector<seastar::temporary_buffer<char>> parts;
    for (const auto piece : {raw.substr(0, cut), raw.substr(cut)}) {
        seastar::temporary_buffer<char> part{piece.size()};
        std::copy(piece.begin(), piece.end(), part.get_write());
        parts.push_back(std::move(part));
    }
    return fragmented_buffer::copy_from_fragments(parts).value();
}
std::uint64_t
load(std::string_view wire, std::size_t offset, std::size_t width) {
    std::uint64_t value = 0;
    for (std::size_t i = 0; i < width; ++i)
        value |= static_cast<std::uint64_t>(
                   static_cast<unsigned char>(wire[offset + i]))
                 << (8U * i);
    return value;
}
void store(
  std::string& wire,
  std::size_t offset,
  std::uint64_t value,
  std::size_t width) {
    for (std::size_t i = 0; i < width; ++i)
        wire[offset + i] = static_cast<char>((value >> (8U * i)) & 255U);
}
std::uint32_t crc(std::string_view raw) {
    std::uint32_t value = UINT32_MAX;
    for (const char byte : raw) {
        value ^= static_cast<unsigned char>(byte);
        for (int bit = 0; bit < 8; ++bit)
            value = (value >> 1U) ^ ((value & 1U) != 0 ? 0x82f63b78U : 0U);
    }
    return ~value;
}
// Test-only repair is independent of the production envelope writer. Fixtures
// using it are small; maximum cases use the bounded production builder.
void repair(std::string& wire) {
    const auto header = load(wire, 10, 2);
    store(wire, 24, crc(std::string_view{wire}.substr(header)), 4);
    store(wire, 28, 0, 4);
    store(wire, 28, crc(std::string_view{wire}.substr(0, header)), 4);
}
void repair_extent(std::string& wire) {
    store(wire, 12, wire.size() - load(wire, 10, 2), 4);
    repair(wire);
}
codec::decode_budget reserve(
  const fragmented_buffer_parser& input,
  codec::limits policy = codec::limits::defaults(),
  codec::field_context context = coordinates) {
    return codec::reserve_decode_input(input, policy, memory(), context)
      .value();
}
template<typename T>
void error(const codec::result<T>& value, errc code) {
    ASSERT_FALSE(value.has_value());
    EXPECT_EQ(value.error().code(), code);
}
codec::result<void> decode_status(
  fragmented_buffer_parser& input,
  bool assigned,
  model::batch_decode_expectation expectation,
  codec::decode_budget budget,
  codec::cooperative_work& work,
  codec::field_context context = coordinates,
  codec::input_boundary boundary = codec::input_boundary::open) {
    if (assigned) {
        auto result = model::decode_assigned_batch(
                        input, expectation, budget, work, context, boundary)
                        .get();
        if (!result) return codec::failure(result.error());
    } else {
        auto result = model::decode_submitted_batch(
                        input, expectation, budget, work, context, boundary)
                        .get();
        if (!result) return codec::failure(result.error());
    }
    return {};
}
void rejected(
  std::string_view raw,
  bool assigned,
  errc code,
  model::batch_decode_expectation expectation = expected()) {
    fragmented_buffer_parser input{bytes(raw)};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto result = decode_status(
      input, assigned, expectation, reserve(input), work);
    error(result, code);
    EXPECT_EQ(input.bytes_consumed(), byte_count{});
    EXPECT_EQ(input.checkpoint_depth(), 0U);
}

TEST(BatchCodecTest, IndependentSubmittedBytesDecodeAndReencodeExactly) {
    const auto raw = unhex(submitted_envelope);
    fragmented_buffer_parser input{bytes(raw, 1)};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto expectation = expected();
    expectation.id = identity();
    expectation.original_binding = binding();
    auto result = model::decode_submitted_batch(
                    input, expectation, reserve(input), work, coordinates)
                    .get();
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(input.at_end());
    EXPECT_EQ(result->value.context().id(), identity());
    EXPECT_EQ(result->value.context().binding(), binding());
    EXPECT_EQ(result->value.context().original_count(), delta{1});
    EXPECT_EQ(
      result->value.context().original_timestamp_base(), wall_time{100});
    EXPECT_TRUE(
      result->value.records().content_equals("\x06\x00\x00\x00\x01\x01\x00"sv));
    auto encoded
      = model::encode_submitted_batch(
          std::move(result->value), work, memory().operation_remaining, charge)
          .get();
    ASSERT_TRUE(encoded.has_value());
    EXPECT_TRUE(encoded->content_equals(raw));
}

TEST(
  BatchCodecTest, DenseAndSparseAssignedResultsReportTheirActualVerification) {
    for (const bool sparse : {false, true}) {
        const auto raw = unhex(sparse ? sparse_envelope : dense_envelope);
        fragmented_buffer_parser input{bytes(raw, 1)};
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto decoded = model::decode_assigned_batch(
                         input, expected(), reserve(input), work, coordinates)
                         .get();
        ASSERT_TRUE(decoded.has_value());
        EXPECT_TRUE(input.at_end());
        EXPECT_EQ(
          decoded->fingerprint_verification,
          sparse ? model::batch_fingerprint_verification::carried
                 : model::batch_fingerprint_verification::recomputed);
        const auto context = decoded->value.context();
        EXPECT_EQ(context.submitted().original_count(), delta{5});
        EXPECT_EQ(context.retained_count(), item_count{sparse ? 2U : 5U});
        EXPECT_EQ(context.logical_span().begin().value(), 100U);
        EXPECT_EQ(context.logical_span().end().value(), 105U);
        auto encoded = model::encode_assigned_batch(
                         std::move(decoded->value),
                         work,
                         memory().operation_remaining,
                         charge)
                         .get();
        ASSERT_TRUE(encoded.has_value());
        EXPECT_TRUE(encoded->content_equals(raw));
    }
}

TEST(BatchCodecTest, ReturnedRegionsOwnTheirBackingAfterInputAndParserDie) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto decoded = [&] {
        auto donor = bytes(unhex(rich_envelope), 1);
        fragmented_buffer_parser input{std::move(donor)};
        return model::decode_submitted_batch(
                 input, expected(), reserve(input), work)
          .get()
          .value();
    }();
    auto scanner = model::record_region_scanner::make(
                     std::move(decoded.value).release_records(),
                     {wall_time{100}, delta{1}, item_count{1}, item_count{2}},
                     memory(),
                     work)
                     .get()
                     .value();
    ASSERT_TRUE(scanner.next(work).get().value());
    auto record
      = scanner.materialize_current(scanner.remaining(), work).get().value();
    scanner.close(work).get();
    EXPECT_TRUE(record.value.key()->content_equals("k"sv));
    EXPECT_TRUE(record.value.is_tombstone());
    ASSERT_EQ(record.value.headers().size(), 2U);
    EXPECT_TRUE(record.value.headers()[0].name().empty());
    EXPECT_FALSE(record.value.headers()[0].value());
    EXPECT_TRUE(record.value.headers()[1].name().content_equals("\xff"sv));
    ASSERT_TRUE(record.value.headers()[1].value());
    EXPECT_TRUE(record.value.headers()[1].value()->empty());
}

TEST(BatchCodecTest, EverySmallSplitAndAdjacentEnvelopeCommitsOnlyOne) {
    for (const bool assigned : {false, true}) {
        const auto raw = unhex(assigned ? dense_envelope : submitted_envelope);
        for (std::size_t cut = 0; cut <= raw.size(); ++cut) {
            fragmented_buffer_parser input{split(raw + "tail", cut)};
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            ASSERT_TRUE(
              decode_status(input, assigned, expected(), reserve(input), work)
                .has_value());
            EXPECT_EQ(input.bytes_consumed(), byte_count{raw.size()});
            EXPECT_EQ(input.bytes_remaining(), byte_count{4});
        }
    }
    const auto raw = unhex(submitted_envelope);
    fragmented_buffer_parser input{bytes(raw + raw)};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto budget = reserve(input);
    auto first = model::decode_submitted_batch(input, expected(), budget, work)
                   .get()
                   .value();
    auto second = model::decode_submitted_batch(
                    input, expected(), first.remaining, work)
                    .get()
                    .value();
    EXPECT_TRUE(input.at_end());
    EXPECT_LT(
      second.remaining.metadata_remaining, first.remaining.metadata_remaining);
    EXPECT_EQ(first.value.fingerprint(), second.value.fingerprint());
}

TEST(BatchCodecTest, EveryOuterTruncationRespectsOpenAndCompleteBoundaries) {
    for (const bool assigned : {false, true}) {
        const auto raw = unhex(assigned ? dense_envelope : submitted_envelope);
        for (const auto boundary :
             {codec::input_boundary::open, codec::input_boundary::complete}) {
            for (std::size_t cut = 0; cut < raw.size(); ++cut) {
                fragmented_buffer_parser input{
                  bytes(std::string_view{raw}.substr(0, cut))};
                seastar::abort_source abort;
                codec::cooperative_work work{codec::limits::defaults(), abort};
                error(
                  decode_status(
                    input,
                    assigned,
                    expected(),
                    reserve(input),
                    work,
                    coordinates,
                    boundary),
                  boundary == codec::input_boundary::open
                    ? errc::truncated_data
                    : errc::malformed_data);
                EXPECT_EQ(input.bytes_consumed(), byte_count{});
                EXPECT_EQ(input.checkpoint_depth(), 0U);
            }
        }
    }
}

TEST(BatchCodecTest, WrongExpectedFamilyAndIntegrityPrecedeBodyInterpretation) {
    rejected(unhex(dense_envelope), false, errc::wrong_context);
    rejected(unhex(submitted_envelope), true, errc::wrong_context);
    auto raw = unhex(submitted_envelope);
    raw[32 + 156] = 2;
    rejected(raw, false, errc::corrupt_data); // body CRC is not repaired
    raw[28] ^= 1;
    rejected(raw, false, errc::corrupt_data); // header is earlier
    repair(raw);
    rejected(raw, false, errc::unsupported_format);
}

TEST(BatchCodecTest, RepairedShortFixedBodiesNeverExposePartialContext) {
    for (const bool assigned : {false, true}) {
        const auto complete = unhex(
          assigned ? dense_envelope : submitted_envelope);
        const auto fixed = assigned ? 184U : 168U;
        for (std::size_t cut = 0; cut < fixed; ++cut) {
            auto raw = complete.substr(0, 32 + cut);
            repair_extent(raw);
            fragmented_buffer_parser input{bytes(raw)};
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            const auto result = decode_status(
              input, assigned, expected(), reserve(input), work);
            error(result, errc::malformed_data);
            EXPECT_EQ(
              result.error().field(),
              static_cast<std::uint16_t>(model::batch_field::fixed_body));
            EXPECT_EQ(result.error().byte_offset(), 1032U + cut);
            EXPECT_EQ(input.bytes_consumed(), byte_count{});
        }
    }
}

TEST(
  BatchCodecTest, NilIdentitiesAndZeroCountersAreMalformedAtFixedFieldOffsets) {
    struct field {
        std::size_t offset, width;
    };
    for (const auto entry : std::array{
           field{0, 16},
           field{16, 8},
           field{24, 8},
           field{40, 16},
           field{56, 16},
           field{72, 8},
           field{80, 16},
           field{96, 8}}) {
        auto raw = unhex(submitted_envelope);
        std::fill_n(
          raw.begin() + static_cast<std::ptrdiff_t>(32 + entry.offset),
          entry.width,
          '\0');
        repair(raw);
        fragmented_buffer_parser input{bytes(raw)};
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        const auto result
          = model::decode_submitted_batch(
              input, expected(), reserve(input), work, coordinates)
              .get();
        error(result, errc::malformed_data);
        EXPECT_EQ(result.error().byte_offset(), 1032U + entry.offset);
    }
}

TEST(BatchCodecTest, EverySuppliedOriginalIdentityAndBindingFieldIsCompared) {
    for (const std::size_t offset :
         {0U, 16U, 24U, 32U, 40U, 56U, 72U, 80U, 96U, 104U}) {
        auto raw = unhex(sparse_envelope);
        raw[32 + offset] ^= 1;
        repair(raw);
        auto expectation = expected();
        expectation.id = identity();
        expectation.original_binding = binding();
        codec::sha256_digest digest{};
        const auto original = unhex(sparse_envelope);
        for (std::size_t i = 0; i < digest.size(); ++i)
            digest[i] = static_cast<unsigned char>(original[32 + 104 + i]);
        expectation.fingerprint = codec::semantic_batch_digest{digest};
        rejected(raw, true, errc::wrong_context, expectation);
    }
}

TEST(BatchCodecTest, InvalidCallerExpectationsRejectBeforeFramingOrMutation) {
    for (int mode = 0; mode < 3; ++mode) {
        auto expectation = expected();
        if (mode == 0) expectation.topic = model::topic_id{};
        if (mode == 1) expectation.range = model::range_id{};
        if (mode == 2) {
            expectation.topic = object<model::topic_id>(34);
            expectation.original_binding = binding();
        }
        auto raw = unhex(submitted_envelope);
        raw[0] = 'X';
        rejected(raw, false, errc::invalid_argument, expectation);
    }
}

TEST(
  BatchCodecTest,
  RelocatedSparseDataKeepsOriginalBindingAndStableLogicalIdentity) {
    fragmented_buffer_parser input{bytes(unhex(sparse_envelope))};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto current = binding(80, 2);
    model::batch_decode_expectation relocated{current.topic(), current.range()};
    auto decoded = model::decode_assigned_batch(
                     input, relocated, reserve(input), work)
                     .get()
                     .value();
    EXPECT_EQ(decoded.value.context().submitted().binding(), binding());
    EXPECT_EQ(
      decoded.fingerprint_verification,
      model::batch_fingerprint_verification::carried);
    auto source_context = decoded.value.context();
    auto scanner = model::record_region_scanner::make(
                     std::move(decoded.value).release_records(),
                     {wall_time{100},
                      delta{5},
                      item_count{2},
                      item_count{},
                      model::record_region_kind::sparse},
                     memory(),
                     work)
                     .get()
                     .value();
    for (const auto slot : {delta{1}, delta{3}}) {
        ASSERT_TRUE(scanner.next(work).get().value());
        EXPECT_EQ(scanner.current()->fields.logical_delta, slot);
        const auto logical = model::range_logical_offset::make(
                               source_context.logical_span().begin().value()
                               + slot.value())
                               .value();
        EXPECT_EQ(
          model::record_id::make(current.topic(), current.range(), logical)
            ->offset()
            .value(),
          100U + slot.value());
    }
    EXPECT_TRUE(scanner.complete());
    relocated.original_binding = current;
    rejected(unhex(sparse_envelope), true, errc::wrong_context, relocated);
}

TEST(BatchCodecTest, UnsupportedCodecAndProfileAndNonzeroReservedBytesReject) {
    for (const bool assigned : {false, true}) {
        for (int mode = 0; mode < 4; ++mode) {
            auto raw = unhex(assigned ? sparse_envelope : submitted_envelope);
            if (mode == 0) raw[32 + 156] = 2;
            if (mode == 1) raw[32 + 157] = 1;
            if (mode == 2) store(raw, 32 + 158, 0, 2);
            if (mode == 3) store(raw, 32 + 158, 2, 2);
            repair(raw);
            rejected(
              raw,
              assigned,
              mode == 1 ? errc::malformed_data : errc::unsupported_format);
        }
    }
}

TEST(
  BatchCodecTest,
  RepairedCountsAndLengthsCannotBypassExactRecordRegionValidation) {
    for (const bool assigned : {false, true}) {
        for (int mode = 0; mode < 10; ++mode) {
            auto raw = unhex(assigned ? dense_envelope : submitted_envelope);
            if (mode == 0) store(raw, 32 + 144, 0, 4);
            if (mode == 1) store(raw, 32 + 144, 4097, 4);
            if (mode == 2) store(raw, 32 + 148, 0, 4);
            if (mode == 3) store(raw, 32 + 148, 6, 4);
            if (mode == 4) store(raw, 32 + 152, 4097, 4);
            if (mode == 5) store(raw, 32 + 160, load(raw, 32 + 160, 4) + 1, 4);
            if (mode == 6) store(raw, 32 + 164, load(raw, 32 + 164, 4) + 1, 4);
            if (mode == 7) {
                raw.push_back('x');
                repair_extent(raw);
            }
            if (mode == 8) {
                store(raw, 32 + 160, 0, 4);
                store(raw, 32 + 164, 0, 4);
            }
            if (mode == 9) store(raw, 32 + 164, (8U << 20U) + 1U, 4);
            repair(raw);
            rejected(
              raw,
              assigned,
              mode == 1 || mode == 4 || mode == 9 ? errc::resource_exhausted
                                                  : errc::malformed_data);
        }
    }
}

TEST(BatchCodecTest, TooFewAndTooManyRecordsAndHeadersRejectAfterCompleteScan) {
    for (const auto count : {4U, 6U}) {
        auto raw = unhex(dense_envelope);
        store(raw, 32 + 144, count, 4);
        store(raw, 32 + 148, count, 4);
        store(raw, 32 + 176, 100U + count, 8);
        repair(raw);
        rejected(raw, true, errc::malformed_data);
    }
    for (const auto count : {0U, 1U, 3U}) {
        auto raw = unhex(rich_envelope);
        store(raw, 32 + 152, count, 4);
        repair(raw);
        rejected(raw, false, errc::malformed_data);
    }
    auto raw = unhex(submitted_envelope);
    store(raw, 32 + 152, 1, 4);
    repair(raw);
    rejected(raw, false, errc::malformed_data);
}

TEST(BatchCodecTest, SubmittedBytesCannotClaimSparseRetention) {
    auto raw = unhex(sparse_envelope);
    store(raw, 4, 1, 2);
    raw.erase(32 + 168, 16);
    repair_extent(raw);
    rejected(raw, false, errc::malformed_data);
}

TEST(BatchCodecTest, ZeroAndWrongExclusiveEndsAreNotPersistedCoverage) {
    for (const std::uint64_t end : {0U, 99U, 100U, 104U, 106U}) {
        auto raw = unhex(sparse_envelope);
        store(raw, 32 + 176, end, 8);
        repair(raw);
        rejected(raw, true, errc::malformed_data);
    }
    auto raw = unhex(sparse_envelope);
    store(raw, 32 + 168, UINT64_MAX - 4U, 8);
    store(raw, 32 + 176, UINT64_MAX, 8);
    repair(raw);
    rejected(raw, true, errc::malformed_data);
    store(raw, 32 + 168, UINT64_MAX - 5U, 8);
    repair(raw);
    fragmented_buffer_parser input{bytes(raw)};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto result = model::decode_assigned_batch(
                    input, expected(), reserve(input), work)
                    .get();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->value.context().logical_span().end().value(), UINT64_MAX);
}

TEST(BatchCodecTest, DenseAndSparseDeltasRejectDuplicatesDisorderAndOutOfSpan) {
    for (const bool sparse : {false, true}) {
        for (int mode = 0; mode < 3; ++mode) {
            auto raw = unhex(sparse ? sparse_envelope : dense_envelope);
            const std::size_t second = 32 + 184 + 7;
            raw[second + 3] = static_cast<char>(
              mode == 0   ? 0
              : mode == 1 ? (sparse ? 1 : 2)
                          : 5);
            repair(raw);
            rejected(raw, true, errc::malformed_data);
        }
    }
}

TEST(
  BatchCodecTest, TimestampReconstructionAndOriginalFirstTimestampAreChecked) {
    auto raw = unhex(dense_envelope);
    raw[32 + 184 + 2] = 2;
    repair(raw);
    rejected(raw, true, errc::malformed_data);
    raw = unhex(sparse_envelope);
    store(raw, 32 + 136, std::uint64_t{1} << 63U, 8);
    repair(raw);
    rejected(raw, true, errc::malformed_data);
}

TEST(
  BatchCodecTest, MalformedLastRecordAndFieldLengthsCannotStealAdjacentBytes) {
    auto raw = unhex(dense_envelope);
    raw.back() = 1;
    repair(raw);
    rejected(raw, true, errc::malformed_data);
    raw = unhex(submitted_envelope);
    raw[32 + 168 + 4] = 4;
    repair(raw); // key declares two bytes; remaining grammar cannot fit
    rejected(raw + unhex(submitted_envelope), false, errc::malformed_data);
    raw = unhex(rich_envelope);
    raw.back() = 4;
    repair(raw);
    rejected(raw, false, errc::malformed_data);
    raw = unhex(submitted_envelope);
    raw[32 + 168 + 1] = 1;
    repair(raw);
    rejected(raw, false, errc::unsupported_format);
}

TEST(BatchCodecTest, RepairedSemanticMutationsFailOriginalDigestVerification) {
    for (int mode = 0; mode < 5; ++mode) {
        auto raw = unhex(mode >= 2 ? rich_envelope : submitted_envelope);
        if (mode == 0) raw[32 + 168 + 4] = 0; // null key becomes present-empty
        if (mode == 1)
            raw[32 + 168 + 5] = 0; // tombstone becomes present-empty value
        if (mode == 2) raw[32 + 168 + 5] = 'j';
        if (mode == 3) raw[32 + 168 + 11] = static_cast<char>(0xfe);
        if (mode == 4) raw.replace(32 + 168 + 8, 5, "\x01\xff\x00\x00\x01"sv);
        repair(raw);
        rejected(raw, false, errc::corrupt_data);
    }
    auto raw = unhex(dense_envelope);
    raw[32 + 184 + 7 + 2] = 3;
    repair(raw);
    rejected(raw, true, errc::corrupt_data);
}

TEST(
  BatchCodecTest,
  UnpinnedOriginalIdentityAndTimestampMutationsStillChangeDenseDigest) {
    for (const std::size_t offset : {0U, 16U, 24U, 32U, 72U, 80U, 96U, 136U}) {
        auto raw = unhex(submitted_envelope);
        raw[32 + offset] ^= 1;
        repair(raw);
        rejected(raw, false, errc::corrupt_data);
    }
}

TEST(BatchCodecTest, SparseDigestIsCarriedAndNeverReplacedByHashOfSurvivors) {
    auto raw = unhex(sparse_envelope);
    std::fill_n(raw.begin() + 32 + 104, 32, '\0');
    repair(raw);
    fragmented_buffer_parser input{bytes(raw)};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto decoded = model::decode_assigned_batch(
                     input, expected(), reserve(input), work)
                     .get()
                     .value();
    EXPECT_EQ(decoded.value.fingerprint().bytes(), codec::sha256_digest{});
    EXPECT_EQ(
      decoded.fingerprint_verification,
      model::batch_fingerprint_verification::carried);
    const std::array keep{delta{3}};
    auto rewritten = model::rewrite_assigned_batch(
                       std::move(decoded.value), keep, memory(), work)
                       .get()
                       .value();
    EXPECT_EQ(rewritten.fingerprint().bytes(), codec::sha256_digest{});
    EXPECT_EQ(rewritten.context().logical_span().end().value(), 105U);
    auto full = unhex(dense_envelope);
    std::fill_n(full.begin() + 32 + 104, 32, '\0');
    repair(full);
    rejected(full, true, errc::corrupt_data);
}

TEST(BatchCodecTest, EightParentMarksWorkAndNinthFailsWithoutDisturbingCaller) {
    for (const bool assigned : {false, true}) {
        for (const std::size_t depth : {0U, 1U, 7U, 8U}) {
            for (const bool corrupt : {false, true}) {
                auto raw = unhex(
                  assigned ? dense_envelope : submitted_envelope);
                if (corrupt) {
                    raw.back() = 1;
                    repair(raw);
                }
                fragmented_buffer_parser input{bytes("p" + raw)};
                for (std::size_t i = 0; i < depth; ++i)
                    input.push_checkpoint().value();
                input.skip(byte_count{1}).value();
                seastar::abort_source abort;
                codec::cooperative_work work{codec::limits::defaults(), abort};
                auto result = decode_status(
                  input, assigned, expected(), reserve(input), work);
                if (depth == 8)
                    error(result, errc::resource_exhausted);
                else if (corrupt)
                    error(result, errc::malformed_data);
                else
                    ASSERT_TRUE(result.has_value());
                EXPECT_EQ(
                  input.bytes_consumed(),
                  byte_count{depth != 8 && !corrupt ? raw.size() + 1U : 1U});
                EXPECT_EQ(input.checkpoint_depth(), depth);
                for (std::size_t i = 0; i < depth; ++i)
                    input.rollback().value();
            }
        }
    }
}

TEST(
  BatchCodecTest,
  ExactPeakBudgetAndPersistentResidualExcludeReleasedTemporaries) {
    for (int mode = 0; mode < 3; ++mode) {
        fragmented_buffer_parser input{bytes(unhex(submitted_envelope))};
        const auto admitted = reserve(input);
        input.push_checkpoint().value();
        input.skip(byte_count{32}).value();
        const auto body
          = input.next_buffer_allocation_cost(byte_count{175}, charge)
              .value()
              .descriptors;
        input.skip(byte_count{168}).value();
        const auto records
          = input.next_buffer_allocation_cost(byte_count{7}, charge)
              .value()
              .descriptors;
        input.skip(byte_count{1}).value();
        const auto record_body
          = input.next_buffer_allocation_cost(byte_count{6}, charge)
              .value()
              .descriptors;
        input.rollback().value();
        auto budget = admitted;
        const auto peak = body.value() + records.value() + record_body.value();
        budget.operation_remaining = byte_count{peak - (mode == 1 ? 1U : 0U)};
        budget.metadata_remaining = byte_count{peak - (mode == 2 ? 1U : 0U)};
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto result = model::decode_submitted_batch(
                        input, expected(), budget, work)
                        .get();
        if (mode == 0) {
            ASSERT_TRUE(result.has_value());
            EXPECT_EQ(
              result->remaining.operation_remaining,
              byte_count{peak - records.value()});
            EXPECT_EQ(
              result->remaining.metadata_remaining,
              byte_count{peak - records.value()});
        } else {
            error(result, errc::resource_exhausted);
            EXPECT_EQ(input.bytes_consumed(), byte_count{});
        }
    }
}

TEST(BatchCodecTest, FutureOptionalHeaderExtensionsDoNotChangeSemanticDigest) {
    auto raw = unhex(submitted_envelope);
    raw.insert(32, "\xfe\x7f\x00\x00\x00\x00\x00\x00"sv);
    store(raw, 6, 2, 2);
    store(raw, 10, 40, 2);
    repair(raw);
    fragmented_buffer_parser input{bytes(raw, 1)};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto result = model::decode_submitted_batch(
                    input, expected(), reserve(input), work)
                    .get();
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(input.at_end());
    auto canonical
      = model::encode_submitted_batch(
          std::move(result->value), work, memory().operation_remaining, charge)
          .get()
          .value();
    EXPECT_TRUE(canonical.content_equals(unhex(submitted_envelope)));
}

TEST(BatchCodecTest, AbsoluteOriginMaximumAndCallerBudgetErrorsStayTyped) {
    for (int mode = 0; mode < 4; ++mode) {
        fragmented_buffer_parser input{bytes(unhex(submitted_envelope))};
        auto context = coordinates;
        context.origin = UINT64_MAX - input.total_bytes().value()
                         + (mode == 1 ? 1U : 0U);
        auto budget = memory();
        if (mode != 1)
            budget = reserve(input, codec::limits::defaults(), context);
        if (mode == 2) budget.charge = nullptr;
        if (mode == 3) budget.operation_remaining = byte_count{};
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto result = model::decode_submitted_batch(
                        input, expected(), budget, work, context)
                        .get();
        if (mode == 0) {
            ASSERT_TRUE(result.has_value());
            EXPECT_TRUE(input.at_end());
        } else {
            error(
              result,
              mode == 3 ? errc::resource_exhausted : errc::invalid_argument);
            EXPECT_EQ(input.bytes_consumed(), byte_count{});
        }
    }
}

thread_local seastar::abort_source* abort_on_charge = nullptr;
thread_local std::uint64_t charge_calls = 0, cancel_at = 0;
byte_count observed_charge(byte_count amount) noexcept {
    if (++charge_calls == cancel_at && abort_on_charge != nullptr)
        abort_on_charge->request_abort();
    return charge(amount);
}

TEST(BatchCodecTest, InitialAndLateCancellationRollBackWithNoPublishedBatch) {
    for (const bool assigned : {false, true}) {
        std::uint64_t last = 0;
        for (int mode = 0; mode < 3; ++mode) {
            fragmented_buffer_parser input{
              bytes(unhex(assigned ? dense_envelope : rich_envelope))};
            auto budget = reserve(input);
            budget.charge = observed_charge;
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            charge_calls = 0;
            cancel_at = mode == 2 ? last : 0;
            abort_on_charge = &abort;
            auto reset = seastar::defer(
              [] noexcept { abort_on_charge = nullptr; });
            if (mode == 1) abort.request_abort();
            const auto result = decode_status(
              input, assigned, expected(), budget, work);
            if (mode == 0) {
                ASSERT_TRUE(result.has_value());
                last = charge_calls;
                EXPECT_GT(last, 0U);
            } else {
                error(result, errc::aborted);
                EXPECT_EQ(input.bytes_consumed(), byte_count{});
            }
            EXPECT_EQ(input.checkpoint_depth(), 0U);
        }
    }
}

fragmented_buffer payload(std::size_t size) {
    std::vector<seastar::temporary_buffer<char>> parts;
    for (std::size_t at = 0; at < size; at += 65536) {
        seastar::temporary_buffer<char> part{
          std::min<std::size_t>(65536, size - at)};
        std::fill_n(part.get_write(), part.size(), 'x');
        parts.push_back(std::move(part));
    }
    return fragmented_buffer::copy_from_fragments(parts).value();
}
fragmented_buffer
built_envelope(codec::cooperative_work& work, bool assigned, bool maximum) {
    auto builder = model::batch_builder::make(
                     identity(), binding(), work.policy(), charge)
                     .value();
    std::vector<model::record_header> headers;
    if (!maximum)
        headers.push_back(
          model::make_record_header(fragmented_buffer{}, std::nullopt).value());
    auto value = model::make_record(
                   {},
                   std::nullopt,
                   maximum ? nullable{payload(1048565)} : std::nullopt,
                   std::move(headers))
                   .value();
    const auto reserved
      = model::reserve_record_input(value, work, memory()).get().value();
    const std::uint64_t count = maximum ? 8U : 4096U;
    for (std::uint64_t i = 0; i < count; ++i) {
        builder.add(value, wall_time{100}, work, reserved.operation_remaining)
          .get()
          .value();
    }
    auto batch
      = builder.finalize(work, memory().operation_remaining).get().value();
    if (assigned) {
        auto placed = model::assigned_batch::assign(
                        std::move(batch),
                        model::range_logical_end{100},
                        binding())
                        .value();
        return model::encode_assigned_batch(
                 std::move(placed), work, memory().operation_remaining, charge)
          .get()
          .value();
    }
    return model::encode_submitted_batch(
             std::move(batch), work, memory().operation_remaining, charge)
      .get()
      .value();
}

TEST(
  BatchCodecTest,
  CompleteMaximumRegionsAndAggregateHeadersFitWithoutRecordTrees) {
    for (const bool assigned : {false, true}) {
        for (const bool maximum : {false, true}) {
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            fragmented_buffer_parser input{
              built_envelope(work, assigned, maximum)};
            auto budget = reserve(input);
            budget.metadata_remaining = byte_count{128U << 10U};
            ASSERT_TRUE(decode_status(input, assigned, expected(), budget, work)
                          .has_value());
            EXPECT_TRUE(input.at_end());
        }
    }
}

TEST(BatchCodecTest, QueuedCancellationJoinsDecodeAndPreservesCallerMarks) {
    seastar::abort_source build_abort;
    codec::cooperative_work build_work{codec::limits::defaults(), build_abort};
    fragmented_buffer_parser input{built_envelope(build_work, true, true)};
    const auto budget = reserve(input);
    input.push_checkpoint().value();
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto deadline = std::chrono::steady_clock::now()
                          + std::chrono::seconds{2};
    while (!seastar::need_preempt()
           && std::chrono::steady_clock::now() < deadline) {
    }
    ASSERT_TRUE(seastar::need_preempt());
    bool observed = false;
    auto observer = seastar::yield().then([&] {
        observed = true;
        abort.request_abort();
    });
    std::optional<codec::result<model::decoded_assigned_batch>> result;
    std::exception_ptr exception;
    bool pending = false;
    try {
        auto decoded = model::decode_assigned_batch(
          input, expected(), budget, work, coordinates);
        pending = !decoded.available();
        result.emplace(decoded.get());
    } catch (...) {
        exception = std::current_exception();
    }
    const auto during = observed;
    observer.get();
    if (exception) std::rethrow_exception(exception);
    EXPECT_TRUE(pending);
    EXPECT_TRUE(during);
    ASSERT_TRUE(result.has_value());
    error(*result, errc::aborted);
    EXPECT_EQ(input.bytes_consumed(), byte_count{});
    EXPECT_EQ(input.checkpoint_depth(), 1U);
    input.rollback().value();
}

TEST(
  BatchCodecTest,
  ReachedAllocationAndNativeHashFailuresPreserveEnclosingInput) {
#if !defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    GTEST_SKIP() << "ordinary allocation injection is disabled";
#else
    static_cast<void>(kwaque::error_category());
    {
        codec::sha256_hasher warm;
        static_cast<void>(std::move(warm).final());
    }
    for (int kind = 0; kind < 3; ++kind) {
        bool failed_once = false, succeeded = false;
        for (std::uint64_t ordinal = 0; ordinal < 256 && !succeeded;
             ++ordinal) {
            const auto raw = unhex(
              kind == 0   ? rich_envelope
              : kind == 1 ? dense_envelope
                          : sparse_envelope);
            fragmented_buffer_parser input{bytes("p" + raw)};
            input.push_checkpoint().value();
            input.skip(byte_count{1}).value();
            const auto budget = reserve(input);
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            auto& injector = seastar::memory::local_failure_injector();
            bool threw = false, reached = false;
            injector.fail_after(ordinal);
            try {
                const auto result = decode_status(
                  input, kind != 0, expected(), budget, work);
                reached = injector.failed();
                injector.cancel();
                ASSERT_TRUE(result.has_value());
                succeeded = !reached;
            } catch (const std::bad_alloc&) {
                reached = injector.failed();
                threw = true;
            } catch (const std::runtime_error&) {
                reached = injector.failed();
                if (!reached || kind == 2) {
                    injector.cancel();
                    throw;
                }
                threw = true;
            } catch (...) {
                injector.cancel();
                throw;
            }
            injector.cancel();
            if (threw) {
                EXPECT_TRUE(reached);
                failed_once = true;
                EXPECT_EQ(input.bytes_consumed(), byte_count{1});
            } else
                EXPECT_EQ(input.bytes_consumed(), byte_count{raw.size() + 1U});
            EXPECT_EQ(input.checkpoint_depth(), 1U);
            input.rollback().value();
            EXPECT_EQ(input.bytes_consumed(), byte_count{});
        }
        EXPECT_TRUE(failed_once);
        EXPECT_TRUE(succeeded);
    }
#endif
}
} // namespace
