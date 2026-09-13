#include "src/base/error.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/bytes/fragmented_buffer_parser.h"
#include "src/codec/cooperative.h"
#include "src/codec/limits.h"
#include "src/codec/transaction.h"
#include "src/model/record.h"
#include "src/model/record_codec.h"
#include "src/runtime/time.h"

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
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <limits>
#include <new>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace {

using namespace std::literals;
namespace codec = kwaque::codec;
namespace model = kwaque::model;
using kwaque::byte_count;
using kwaque::errc;
using kwaque::item_count;
using kwaque::bytes::fragmented_buffer;
using kwaque::bytes::fragmented_buffer_parser;
using nullable_bytes = std::optional<fragmented_buffer>;

constexpr auto empty_record = "\x06\x00\x00\x00\x01\x01\x00"sv;
constexpr auto full_record
  = "\x0c\x00\x03\x03\x02\x6b\x01\x02\x00\x01\x01\xff\x00"sv;
constexpr codec::field_context coordinates{
  .origin = 1000, .family = 2, .field = 91};
constexpr model::record_decode_context expected{
  kwaque::runtime::wall_time{100},
  model::range_logical_count{5},
  item_count{4096}};

template<typename T>
concept encoding_argument = requires(T&& value, codec::cooperative_work& work) {
    model::encode_record(std::forward<T>(value), work, byte_count{}, nullptr);
};
static_assert(encoding_argument<const model::record&>);
static_assert(!encoding_argument<model::record>);
static_assert(std::is_nothrow_move_constructible_v<model::decoded_record>);
static_assert(std::is_nothrow_destructible_v<model::decoded_record>);

byte_count charge(byte_count request) noexcept {
    if (request.value() == 0) return {};
    if (request.value() > (std::uint64_t{1} << 62U))
        return byte_count{std::numeric_limits<std::uint64_t>::max()};
    return byte_count{
      2U * std::bit_ceil(std::max(request.value(), std::uint64_t{16}))};
}

codec::decode_budget memory() {
    // The unclaimed half of the operation ceiling covers fixture owners and
    // native/frame costs; these tests do not measure whole-operation peaks.
    return {byte_count{32U * 1024U * 1024U}, byte_count{1024U * 1024U}, charge};
}

fragmented_buffer text(std::string_view value) {
    return fragmented_buffer::copy_of(value).value();
}

fragmented_buffer fragmented(std::string_view value, std::size_t width) {
    std::vector<seastar::temporary_buffer<char>> parts;
    for (std::size_t offset = 0; offset < value.size(); offset += width) {
        const auto view = value.substr(offset, width);
        seastar::temporary_buffer<char> part{view.size()};
        std::ranges::copy(view, part.get_write());
        parts.push_back(std::move(part));
    }
    return fragmented_buffer::copy_from_fragments(parts).value();
}

fragmented_buffer split(std::string_view value, std::size_t cut) {
    std::vector<seastar::temporary_buffer<char>> parts;
    for (const auto view : {value.substr(0, cut), value.substr(cut)}) {
        seastar::temporary_buffer<char> part{view.size()};
        std::ranges::copy(view, part.get_write());
        parts.push_back(std::move(part));
    }
    return fragmented_buffer::copy_from_fragments(parts).value();
}

fragmented_buffer payload(std::size_t size) {
    std::vector<seastar::temporary_buffer<char>> parts;
    for (std::size_t offset = 0; offset < size; offset += 65536) {
        seastar::temporary_buffer<char> part{
          std::min<std::size_t>(65536, size - offset)};
        std::fill_n(part.get_write(), part.size(), 'x');
        parts.push_back(std::move(part));
    }
    return fragmented_buffer::copy_from_fragments(parts).value();
}

model::record full_value(std::size_t width = 64) {
    std::vector<model::record_header> headers;
    headers.push_back(
      model::make_record_header(fragmented_buffer{}, std::nullopt).value());
    headers.push_back(
      model::make_record_header(
        fragmented("\xff"sv, width), nullable_bytes{fragmented_buffer{}})
        .value());
    return model::make_record(
             {.timestamp_delta = -2,
              .logical_delta = model::range_logical_count{3}},
             nullable_bytes{fragmented("k"sv, width)},
             std::nullopt,
             std::move(headers))
      .value();
}

model::record null_value() {
    return model::make_record({}, std::nullopt, std::nullopt, {}).value();
}

codec::result<fragmented_buffer> encode(
  const model::record& value,
  codec::limits policy = codec::limits::defaults()) {
    seastar::abort_source abort;
    codec::cooperative_work work{policy, abort};
    const auto reserved
      = model::reserve_record_input(value, work, memory()).get().value();
    return model::encode_record(
             value, work, reserved.operation_remaining, charge, coordinates)
      .get();
}

codec::decode_budget reserve(
  const fragmented_buffer_parser& input,
  codec::limits policy = codec::limits::defaults(),
  codec::field_context context = coordinates) {
    return codec::reserve_decode_input(input, policy, memory(), context)
      .value();
}

codec::result<model::decoded_record> decode(
  fragmented_buffer_parser& input,
  model::record_decode_context target = expected,
  codec::input_boundary boundary = codec::input_boundary::open,
  codec::field_context context = coordinates) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    return model::decode_record(
             input,
             target,
             reserve(input, work.policy(), context),
             work,
             context,
             boundary)
      .get();
}

std::string unread(const fragmented_buffer_parser& input) {
    std::string result(input.bytes_remaining().value(), '\0');
    input.peek_to(std::span<char>{result}).value();
    return result;
}

void expect_error(const auto& value, errc reason) {
    ASSERT_FALSE(value.has_value());
    EXPECT_EQ(value.error().code(), reason);
    EXPECT_EQ(value.error().family(), coordinates.family);
}

void expect_full(const model::record& value) {
    EXPECT_EQ(value.attributes(), 0);
    EXPECT_EQ(value.timestamp_delta(), -2);
    EXPECT_EQ(value.logical_delta(), model::range_logical_count{3});
    ASSERT_TRUE(value.key().has_value());
    EXPECT_TRUE(value.key()->content_equals("k"sv));
    EXPECT_FALSE(value.value().has_value());
    ASSERT_EQ(value.headers().size(), 2U);
    EXPECT_TRUE(value.headers()[0].name().empty());
    EXPECT_FALSE(value.headers()[0].value().has_value());
    EXPECT_TRUE(value.headers()[1].name().content_equals("\xff"sv));
    ASSERT_TRUE(value.headers()[1].value().has_value());
    EXPECT_TRUE(value.headers()[1].value()->empty());
}

// Fixture-only unsigned/ZigZag byte construction, independent of the codec.
void unsigned_integer(std::string& bytes, std::uint64_t value) {
    do {
        const auto next = static_cast<unsigned char>(value % 128U);
        value /= 128U;
        bytes.push_back(static_cast<char>(next + (value == 0 ? 0 : 128U)));
    } while (value != 0);
}
std::string record_bytes(std::string_view body) {
    std::string result;
    unsigned_integer(result, body.size());
    result.append(body);
    return result;
}

TEST(RecordCodecTest, IndependentLiteralsEncodeWithoutChangingInput) {
    auto null = null_value();
    auto full = full_value();
    const auto first = encode(null);
    const auto second = encode(full);
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());
    EXPECT_TRUE(first->content_equals(empty_record));
    EXPECT_TRUE(second->content_equals(full_record));
    expect_full(full);
    EXPECT_FALSE(null.has_key());
    EXPECT_FALSE(null.has_value());
}

TEST(RecordCodecTest, EverySmallSplitDecodesOneCompleteOwnedRecord) {
    const auto wire = std::string{"pre"} + std::string{full_record}
                      + std::string{empty_record};
    for (std::size_t cut = 0; cut <= wire.size(); ++cut) {
        SCOPED_TRACE(cut);
        std::optional<model::decoded_record> value;
        {
            fragmented_buffer_parser input{split(wire, cut)};
            input.skip(byte_count{3}).value();
            auto decoded = decode(input);
            ASSERT_TRUE(decoded.has_value());
            EXPECT_EQ(
              input.bytes_consumed(), byte_count{3 + full_record.size()});
            EXPECT_EQ(input.checkpoint_depth(), 0U);
            EXPECT_EQ(unread(input), empty_record);
            value.emplace(std::move(*decoded));
        }
        expect_full(value->value);
    }
}

TEST(RecordCodecTest, NullEmptyAndNonemptyKeysAndValuesHaveDistinctWireBytes) {
    const std::array<std::optional<std::string_view>, 3> values{
      std::nullopt, ""sv, "x"sv};
    for (const auto key : values) {
        for (const auto value : values) {
            auto source = model::make_record(
                            {},
                            key ? nullable_bytes{text(*key)} : std::nullopt,
                            value ? nullable_bytes{text(*value)} : std::nullopt,
                            {})
                            .value();
            std::string body{"\x00\x00\x00"sv};
            for (const auto bytes : {key, value}) {
                body.push_back(
                  bytes ? static_cast<char>(2U * bytes->size()) : '\x01');
                if (bytes) body.append(*bytes);
            }
            body.push_back('\0');
            auto wire = encode(source).value();
            EXPECT_TRUE(wire.content_equals(record_bytes(body)));
            fragmented_buffer_parser input{std::move(wire)};
            const auto decoded = decode(input);
            ASSERT_TRUE(decoded.has_value());
            EXPECT_EQ(decoded->value.has_key(), key.has_value());
            EXPECT_EQ(decoded->value.has_value(), value.has_value());
            if (key) EXPECT_TRUE(decoded->value.key()->content_equals(*key));
            if (value)
                EXPECT_TRUE(decoded->value.value()->content_equals(*value));
        }
    }
}

TEST(RecordCodecTest, DuplicateHeaderNamesRetainBothValuesInOrder) {
    std::vector<model::record_header> headers;
    headers.push_back(
      model::make_record_header(text("a"sv), nullable_bytes{text("1"sv)})
        .value());
    headers.push_back(
      model::make_record_header(text("a"sv), nullable_bytes{text("2"sv)})
        .value());
    auto source = model::make_record(
                    {}, std::nullopt, std::nullopt, std::move(headers))
                    .value();
    constexpr auto literal
      = "\x0e\x00\x00\x00\x01\x01\x02\x01\x61\x02\x31\x01\x61\x02\x32"sv;
    auto wire = encode(source).value();
    EXPECT_TRUE(wire.content_equals(literal));
    fragmented_buffer_parser input{std::move(wire)};
    auto value = decode(input);
    ASSERT_TRUE(value.has_value());
    ASSERT_EQ(value->value.headers().size(), 2U);
    for (const auto& header : value->value.headers()) {
        EXPECT_TRUE(header.name().content_equals("a"sv));
        ASSERT_TRUE(header.value().has_value());
    }
    EXPECT_TRUE(value->value.headers()[0].value()->content_equals("1"sv));
    EXPECT_TRUE(value->value.headers()[1].value()->content_equals("2"sv));
}

TEST(RecordCodecTest, EveryTruncationUsesItsOuterBoundaryAndRollsBack) {
    for (std::size_t cut = 0; cut < full_record.size(); ++cut) {
        for (const auto boundary :
             {codec::input_boundary::open, codec::input_boundary::complete}) {
            const auto wire = std::string{"p"}
                              + std::string{full_record.substr(0, cut)};
            fragmented_buffer_parser input{fragmented(wire, 1)};
            input.skip(byte_count{1}).value();
            const auto value = decode(input, expected, boundary);
            expect_error(
              value,
              boundary == codec::input_boundary::open ? errc::truncated_data
                                                      : errc::malformed_data);
            ASSERT_FALSE(value.has_value());
            EXPECT_EQ(
              value.error().byte_offset(), coordinates.origin + 1U + cut);
            EXPECT_EQ(input.bytes_consumed(), byte_count{1});
            EXPECT_EQ(input.checkpoint_depth(), 0U);
            EXPECT_EQ(unread(input), full_record.substr(0, cut));
        }
    }
}

TEST(RecordCodecTest, DeclaredChildrenCannotStealSiblingBytes) {
    for (const auto body :
         {"\x00\x00\x00\x06\x61\x62"sv,
          "\x00\x00\x00\x01\x06\x61"sv,
          "\x00\x00\x00\x01\x01\x01\x03\x61"sv,
          "\x00\x00\x00\x01\x01\x01\x00\x06"sv}) {
        const auto bad = record_bytes(body);
        const auto siblings = bad + std::string{empty_record};
        fragmented_buffer_parser input{fragmented("pre" + siblings, 1)};
        input.push_checkpoint().value();
        input.skip(byte_count{3}).value();
        const auto value = decode(input);
        expect_error(value, errc::malformed_data);
        ASSERT_FALSE(value.has_value());
        EXPECT_EQ(
          value.error().byte_offset(), coordinates.origin + 3U + bad.size());
        EXPECT_EQ(input.bytes_consumed(), byte_count{3});
        EXPECT_EQ(input.checkpoint_depth(), 1U);
        EXPECT_EQ(unread(input), siblings);
        input.rollback().value();
        EXPECT_EQ(input.bytes_consumed(), byte_count{});
    }
}

TEST(RecordCodecTest, ExtraBytesAndHeaderCountMismatchesRejectExactly) {
    auto extra = std::string{empty_record.substr(1)} + "x";
    fragmented_buffer_parser extra_input{text(record_bytes(extra))};
    const auto extra_value = decode(extra_input);
    expect_error(extra_value, errc::malformed_data);
    ASSERT_FALSE(extra_value.has_value());
    EXPECT_EQ(extra_value.error().byte_offset(), coordinates.origin + 7U);
    for (const unsigned count : {1U, 3U, 65U}) {
        auto bytes = std::string{full_record};
        bytes[7] = static_cast<char>(count);
        fragmented_buffer_parser input{text(bytes)};
        const auto value = decode(input);
        expect_error(
          value, count == 65 ? errc::resource_exhausted : errc::malformed_data);
        EXPECT_EQ(input.bytes_consumed(), byte_count{});
        EXPECT_EQ(unread(input), bytes);
    }
}

TEST(RecordCodecTest, NullableNegativeTwoAndMalformedVarintsReject) {
    for (const auto body :
         {"\x00\x00\x00\x03\x01\x00"sv,
          "\x00\x00\x00\x01\x03\x00"sv,
          "\x00\x00\x00\x01\x01\x01\x00\x03"sv,
          "\x00\x80\x00\x00\x01\x01\x00"sv,
          "\x00\x80\x80\x80\x80\x80"sv}) {
        fragmented_buffer_parser input{fragmented(record_bytes(body), 1)};
        expect_error(decode(input), errc::malformed_data);
        EXPECT_EQ(input.bytes_consumed(), byte_count{});
    }
    for (const auto prefix :
         {"\x86\x00"sv, "\xff\xff\xff\xff\x10"sv, "\xff\xff\xff\xff\x80"sv}) {
        fragmented_buffer_parser input{
          text(std::string{prefix} + std::string{empty_record.substr(1)})};
        expect_error(decode(input), errc::malformed_data);
        EXPECT_EQ(input.bytes_consumed(), byte_count{});
    }
}

TEST(RecordCodecTest, SmallImpossibleBodiesRejectBeforeSharing) {
    for (unsigned length = 0; length < 6; ++length) {
        const auto bytes = record_bytes(std::string(length, '\0'));
        fragmented_buffer_parser input{text(bytes)};
        const auto value = decode(input);
        expect_error(value, errc::malformed_data);
        ASSERT_FALSE(value.has_value());
        EXPECT_EQ(value.error().byte_offset(), coordinates.origin);
        EXPECT_EQ(
          value.error().field(),
          static_cast<std::uint16_t>(model::record_field::body_bytes));
        EXPECT_EQ(input.bytes_consumed(), byte_count{});
    }
}

TEST(
  RecordCodecTest, AttributesTimestampAndLogicalBoundsUseIndependentContext) {
    for (unsigned attribute = 1; attribute <= 255; ++attribute) {
        auto wire = std::string{full_record};
        wire[1] = static_cast<char>(attribute);
        fragmented_buffer_parser input{text(wire)};
        const auto value = decode(input);
        expect_error(value, errc::unsupported_format);
        ASSERT_FALSE(value.has_value());
        EXPECT_EQ(
          value.error().field(),
          static_cast<std::uint16_t>(model::record_field::attributes));
        EXPECT_EQ(value.error().byte_offset(), coordinates.origin + 1U);
    }
    fragmented_buffer_parser logical{text(full_record)};
    auto target = expected;
    target.original_count = model::range_logical_count{3};
    const auto bad_logical = decode(logical, target);
    expect_error(bad_logical, errc::malformed_data);
    ASSERT_FALSE(bad_logical.has_value());
    EXPECT_EQ(bad_logical.error().byte_offset(), coordinates.origin + 3U);
    fragmented_buffer_parser timestamp{text(full_record)};
    target = expected;
    target.timestamp_base = kwaque::runtime::wall_time{
      std::numeric_limits<std::int64_t>::min()};
    const auto bad_time = decode(timestamp, target);
    expect_error(bad_time, errc::malformed_data);
    ASSERT_FALSE(bad_time.has_value());
    EXPECT_EQ(bad_time.error().byte_offset(), coordinates.origin + 2U);
}

TEST(RecordCodecTest, TimestampExtremaEncodeCanonicallyAndReconstructSafely) {
    for (const auto delta :
         {std::numeric_limits<std::int64_t>::min(),
          std::numeric_limits<std::int64_t>::max()}) {
        auto source
          = model::make_record(
              {.timestamp_delta = delta}, std::nullopt, std::nullopt, {})
              .value();
        auto encoded = encode(source).value();
        std::string body(1, '\0');
        const auto zigzag = delta < 0
                              ? std::numeric_limits<std::uint64_t>::max()
                              : std::numeric_limits<std::uint64_t>::max() - 1U;
        unsigned_integer(body, zigzag);
        body.append("\x00\x01\x01\x00"sv);
        EXPECT_TRUE(encoded.content_equals(record_bytes(body)));
        fragmented_buffer_parser input{std::move(encoded)};
        auto target = expected;
        target.timestamp_base = kwaque::runtime::wall_time{0};
        auto decoded = decode(input, target);
        ASSERT_TRUE(decoded.has_value());
        EXPECT_EQ(decoded->value.timestamp_delta(), delta);
    }
}

TEST(RecordCodecTest, AggregateHeaderAllowanceCannotBeResetForEachRecord) {
    fragmented_buffer_parser input{
      text(std::string{full_record} + std::string{full_record})};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto initial = reserve(input);
    auto target = expected;
    target.headers_remaining = item_count{3};
    auto first
      = model::decode_record(input, target, initial, work, coordinates).get();
    ASSERT_TRUE(first.has_value());
    target.headers_remaining = target.headers_remaining
                                 .checked_sub(
                                   item_count{first->value.headers().size()})
                                 .value();
    const auto second = model::decode_record(
                          input, target, first->remaining, work, coordinates)
                          .get();
    expect_error(second, errc::resource_exhausted);
    EXPECT_EQ(input.bytes_consumed(), byte_count{full_record.size()});
    EXPECT_EQ(unread(input), full_record);
}

TEST(RecordCodecTest, ExistingEighthAndFailedNinthMarksRemainOwnedByCaller) {
    for (unsigned depth : {0U, 1U, 7U, 8U}) {
        for (const bool malformed : {false, true}) {
            auto wire = std::string{full_record};
            if (malformed) wire[1] = 1;
            fragmented_buffer_parser input{text("p" + wire)};
            for (unsigned i = 0; i < depth; ++i)
                input.push_checkpoint().value();
            input.skip(byte_count{1}).value();
            const auto value = decode(input);
            if (depth == 8)
                expect_error(value, errc::resource_exhausted);
            else if (malformed)
                expect_error(value, errc::unsupported_format);
            else
                ASSERT_TRUE(value.has_value());
            EXPECT_EQ(
              input.bytes_consumed(),
              byte_count{
                depth < 8 && !malformed ? full_record.size() + 1U : 1U});
            EXPECT_EQ(input.checkpoint_depth(), depth);
            while (input.checkpoint_depth() != 0) {
                input.rollback().value();
                EXPECT_EQ(input.bytes_consumed(), byte_count{});
            }
        }
    }
}

TEST(RecordCodecTest, ReturnedMetadataRemaindersAccountEveryRetainedResult) {
    fragmented_buffer_parser input{
      text(std::string{full_record} + std::string{full_record})};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto initial = reserve(input);
    auto first
      = model::decode_record(input, expected, initial, work, coordinates).get();
    ASSERT_TRUE(first.has_value());
    const auto spent = initial.metadata_remaining
                         .checked_sub(first->remaining.metadata_remaining)
                         .value();
    EXPECT_GT(spent.value(), 0U);
    EXPECT_EQ(
      initial.operation_remaining.checked_sub(
        first->remaining.operation_remaining),
      std::optional{spent});
    auto second = model::decode_record(
                    input, expected, first->remaining, work, coordinates)
                    .get();
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(
      first->remaining.metadata_remaining.checked_sub(
        second->remaining.metadata_remaining),
      std::optional{spent});
    EXPECT_TRUE(input.at_end());
    expect_full(first->value);
    expect_full(second->value);
}

TEST(RecordCodecTest, TemporaryChildAllowanceIsReusableAfterTeardown) {
    fragmented_buffer_parser input{text(empty_record)};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto initial = reserve(input);
    const auto value
      = model::decode_record(input, expected, initial, work, coordinates).get();
    ASSERT_TRUE(value.has_value());
    EXPECT_EQ(value->remaining, initial);
    EXPECT_TRUE(input.at_end());
}

TEST(RecordCodecTest, ExactChildAndOutputPeaksRespectOneShortResiduals) {
    for (bool metadata : {false, true}) {
        for (bool one_short : {false, true}) {
            fragmented_buffer_parser input{text(full_record)};
            input.push_checkpoint().value();
            input.skip(byte_count{1}).value();
            const auto temporary
              = input.next_buffer_allocation_cost(byte_count{12}, charge)
                  .value()
                  .descriptors;
            input.rollback().value();
            const auto field_peak = byte_count{
              2U
              * charge(
                  byte_count{
                    2U * fragmented_buffer::fragment_descriptor_size()})
                  .value()};
            const auto output = charge(
                                  byte_count{2U * sizeof(model::record_header)})
                                  .checked_add(field_peak)
                                  .value()
                                  .checked_add(field_peak)
                                  .value();
            const auto peak = temporary.checked_add(output).value();
            auto budget = reserve(input);
            const auto allowance = one_short
                                     ? peak.checked_sub(byte_count{1}).value()
                                     : peak;
            if (metadata)
                budget.metadata_remaining = allowance;
            else
                budget.operation_remaining = allowance;
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            auto value = model::decode_record(
                           input, expected, budget, work, coordinates)
                           .get();
            if (one_short) {
                expect_error(value, errc::resource_exhausted);
                EXPECT_EQ(input.bytes_consumed(), byte_count{});
            } else {
                ASSERT_TRUE(value.has_value());
                EXPECT_EQ(
                  metadata ? value->remaining.metadata_remaining
                           : value->remaining.operation_remaining,
                  temporary);
            }
        }
    }
}

TEST(RecordCodecTest, EncoderExactBudgetAndOneShortNeverExposePartialOutput) {
    auto source = null_value();
    const auto required
      = charge(byte_count{7})
          .checked_add(
            byte_count{
              charge(byte_count{fragmented_buffer::fragment_descriptor_size()})
                .value()})
          .value();
    for (const auto allowance :
         {byte_count{},
          required.checked_sub(byte_count{1}).value(),
          required}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        const auto value = model::encode_record(
                             source, work, allowance, charge, coordinates)
                             .get();
        if (allowance == required) {
            ASSERT_TRUE(value.has_value());
            EXPECT_TRUE(value->content_equals(empty_record));
        } else
            expect_error(value, errc::resource_exhausted);
        EXPECT_FALSE(source.has_key());
        EXPECT_FALSE(source.has_value());
    }
}

TEST(RecordCodecTest, RecordHeaderNameAndPayloadCapsPrecedeVariableRequests) {
    auto huge_body = std::string{};
    unsigned_integer(huge_body, 1048576);
    fragmented_buffer_parser huge{text(huge_body)};
    expect_error(decode(huge), errc::resource_exhausted);
    for (const bool name : {false, true}) {
        std::string body{"\x00\x00\x00\x01\x01\x01"sv};
        if (name)
            unsigned_integer(body, 4097);
        else {
            unsigned_integer(body, 0);
            unsigned_integer(body, 2U * 65537U);
        }
        fragmented_buffer_parser input{text(record_bytes(body))};
        expect_error(decode(input), errc::resource_exhausted);
        EXPECT_EQ(input.bytes_consumed(), byte_count{});
    }
}

TEST(RecordCodecTest, SixtyFourEmptyHeadersAndNarrowedHeaderLimits) {
    std::string body{"\x00\x00\x00\x01\x01\x40"sv};
    for (unsigned i = 0; i < 64; ++i)
        body.append("\x00\x01"sv);
    const auto wire = record_bytes(body);
    ASSERT_EQ(wire.size(), 136U);
    fragmented_buffer_parser input{fragmented(wire, 1)};
    auto value = decode(input);
    ASSERT_TRUE(value.has_value());
    EXPECT_EQ(value->value.headers().size(), 64U);
    EXPECT_TRUE(encode(value->value).value().content_equals(wire));
    auto target = expected;
    target.headers_remaining = item_count{63};
    fragmented_buffer_parser limited{text(wire)};
    expect_error(decode(limited, target), errc::resource_exhausted);
}

TEST(RecordCodecTest, MaximumRecordAndNarrowNewAllocationsKeepSourceIntact) {
    for (const auto size : {std::size_t{65536}, std::size_t{1048565}}) {
        auto source = model::make_record(
                        {}, std::nullopt, nullable_bytes{payload(size)}, {})
                        .value();
        const auto* original = source.value()->fragment_at(0)->data();
        seastar::abort_source abort;
        codec::cooperative_work admission{codec::limits::defaults(), abort};
        const auto budget = model::reserve_record_input(
                              source, admission, memory())
                              .get()
                              .value();
        codec::limits_config config;
        config.max_work_bytes = byte_count{128};
        config.max_work_items = item_count{64};
        if (size == 65536) config.max_allocation_bytes = byte_count{4096};
        codec::cooperative_work work{
          codec::limits::make(config).value(), abort};
        auto encoded
          = model::encode_record(
              source, work, budget.operation_remaining, charge, coordinates)
              .get();
        ASSERT_TRUE(encoded.has_value());
        EXPECT_EQ(encoded->size(), byte_count{size + 11U});
        for (const auto part : *encoded) {
            EXPECT_LE(
              charge(byte_count{part.size()}).value(),
              config.max_allocation_bytes.value());
        }
        EXPECT_EQ(source.value()->fragment_at(0)->data(), original);
        fragmented_buffer_parser input{std::move(*encoded)};
        auto decoded = decode(input);
        ASSERT_TRUE(decoded.has_value());
        EXPECT_TRUE(decoded->value.value()->content_equals(*source.value()));
        EXPECT_TRUE(input.at_end());
    }
}

TEST(RecordCodecTest, HeaderPayloadTotalIsCumulativeAcrossAllHeaders) {
    std::vector<model::record_header> headers;
    headers.push_back(
      model::make_record_header(payload(4096), nullable_bytes{payload(61440)})
        .value());
    auto source = model::make_record(
                    {}, std::nullopt, std::nullopt, std::move(headers))
                    .value();
    auto wire = encode(source).value();
    fragmented_buffer_parser input{std::move(wire)};
    const auto value = decode(input);
    ASSERT_TRUE(value.has_value());
    ASSERT_EQ(value->value.headers().size(), 1U);
    EXPECT_EQ(value->value.headers()[0].name().size(), byte_count{4096});
    EXPECT_EQ(value->value.headers()[0].value()->size(), byte_count{61440});
    EXPECT_EQ(
      model::record_encoded_size(value->value).value().header_payload_bytes,
      byte_count{65536});

    std::string oversized{"\x00\x00\x00\x01\x01\x02"sv};
    for (const unsigned length : {40000U, 25537U}) {
        oversized.push_back('\0');
        unsigned_integer(oversized, 2U * length);
        oversized.append(length, 'x');
    }
    fragmented_buffer_parser bad{fragmented(record_bytes(oversized), 4096)};
    expect_error(decode(bad), errc::resource_exhausted);
    EXPECT_EQ(bad.bytes_consumed(), byte_count{});
}

TEST(RecordCodecTest, CoordinatesAllowExactMaximumEndsAndRejectOverflow) {
    constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();
    auto source = full_value();
    for (const bool overflow : {false, true}) {
        const codec::field_context origin{
          .origin = maximum - full_record.size() + (overflow ? 1U : 0U),
          .family = coordinates.family};
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        const auto encoded
          = model::encode_record(
              source, work, memory().operation_remaining, charge, origin)
              .get();
        if (overflow)
            expect_error(encoded, errc::invalid_argument);
        else {
            ASSERT_TRUE(encoded.has_value());
            EXPECT_TRUE(encoded->content_equals(full_record));
            fragmented_buffer_parser input{text(full_record)};
            const auto value = model::decode_record(
                                 input,
                                 expected,
                                 reserve(input, work.policy(), origin),
                                 work,
                                 origin)
                                 .get();
            ASSERT_TRUE(value.has_value());
            EXPECT_TRUE(input.at_end());
        }
    }
    const codec::field_context origin{
      .origin = maximum - 1U, .family = coordinates.family};
    fragmented_buffer_parser declared{text("\x06"sv)};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    expect_error(
      model::decode_record(
        declared,
        expected,
        reserve(declared, work.policy(), origin),
        work,
        origin)
        .get(),
      errc::malformed_data);
}

TEST(RecordCodecTest, InvalidCallerContextsAndInsufficientWorkRejectEarly) {
    for (unsigned mode = 0; mode < 5; ++mode) {
        fragmented_buffer_parser input{text(full_record)};
        auto target = expected;
        auto budget = reserve(input);
        codec::limits_config config;
        if (mode == 0) target.original_count = model::range_logical_count{};
        if (mode == 1) target.original_count = model::range_logical_count{4097};
        if (mode == 2) budget.charge = nullptr;
        if (mode == 3) config.max_work_bytes = byte_count{127};
        if (mode == 4) config.max_work_items = item_count{63};
        seastar::abort_source abort;
        codec::cooperative_work work{
          codec::limits::make(config).value(), abort};
        const auto value = model::decode_record(
                             input, target, budget, work, coordinates)
                             .get();
        expect_error(
          value,
          mode == 0 || mode == 2 ? errc::invalid_argument
                                 : errc::resource_exhausted);
        EXPECT_EQ(input.bytes_consumed(), byte_count{});
        EXPECT_EQ(input.checkpoint_depth(), 0U);
    }
}

TEST(RecordCodecTest, InitialAndLateAbortRespectPublicationBoundaries) {
    auto source = full_value();
    fragmented_buffer_parser input{text(full_record)};
    auto budget = reserve(input);
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto complete
      = model::decode_record(input, expected, budget, work, coordinates).get();
    ASSERT_TRUE(complete.has_value());
    abort.request_abort();
    expect_full(complete->value);
    EXPECT_TRUE(input.at_end());
    fragmented_buffer_parser pending{text(full_record)};
    expect_error(
      model::decode_record(pending, expected, budget, work, coordinates).get(),
      errc::aborted);
    expect_error(
      model::encode_record(
        source, work, memory().operation_remaining, charge, coordinates)
        .get(),
      errc::aborted);
    EXPECT_EQ(pending.bytes_consumed(), byte_count{});
    expect_full(source);
}

thread_local seastar::abort_source* charge_abort = nullptr;
thread_local unsigned charge_calls = 0;
thread_local unsigned cancel_at = 0;
byte_count observed_charge(byte_count request) noexcept {
    if (++charge_calls == cancel_at && charge_abort != nullptr)
        charge_abort->request_abort();
    return charge(request);
}

TEST(RecordCodecTest, AbortAtTheLastFieldAdmissionDiscardsEarlierOwners) {
    unsigned calls = 0;
    for (const bool cancel : {false, true}) {
        fragmented_buffer_parser input{text(full_record)};
        auto budget = reserve(input);
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        charge_calls = 0;
        cancel_at = cancel ? calls : 0;
        charge_abort = &abort;
        auto reset = seastar::defer([] noexcept { charge_abort = nullptr; });
        budget.charge = observed_charge;
        const auto value = model::decode_record(
                             input, expected, budget, work, coordinates)
                             .get();
        if (cancel) {
            expect_error(value, errc::aborted);
            EXPECT_EQ(charge_calls, calls);
            EXPECT_EQ(input.bytes_consumed(), byte_count{});
            EXPECT_EQ(input.checkpoint_depth(), 0U);
            EXPECT_EQ(unread(input), full_record);
        } else {
            ASSERT_TRUE(value.has_value());
            calls = charge_calls;
            EXPECT_GT(
              calls,
              8U); // Body alias, key alias and header storage were reached.
        }
    }
}

TEST(RecordCodecTest, QueuedAbortInterruptsPendingEncodingAndPreservesInput) {
    auto source = model::make_record(
                    {}, std::nullopt, nullable_bytes{payload(1048565)}, {})
                    .value();
    seastar::abort_source abort;
    codec::limits_config config;
    config.max_work_bytes = byte_count{128};
    config.max_work_items = item_count{64};
    codec::cooperative_work work{codec::limits::make(config).value(), abort};
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
    std::optional<codec::result<fragmented_buffer>> value;
    std::exception_ptr exception;
    bool pending = false;
    try {
        auto encoded = model::encode_record(
          source, work, memory().operation_remaining, charge, coordinates);
        pending = !encoded.available();
        value.emplace(encoded.get());
    } catch (...) {
        exception = std::current_exception();
    }
    const auto during = observed;
    observer.get();
    if (exception) std::rethrow_exception(exception);
    EXPECT_TRUE(pending);
    EXPECT_TRUE(during);
    ASSERT_TRUE(value.has_value());
    expect_error(*value, errc::aborted);
    EXPECT_EQ(source.value()->size(), byte_count{1048565});
}

TEST(RecordCodecTest, ReachedAllocationFailuresPreserveBothRecordAndParser) {
#if !defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    GTEST_SKIP() << "ordinary allocation injection is disabled";
#else
    static_cast<void>(kwaque::error_category());
    for (const bool decoding : {false, true}) {
        bool failed_once = false;
        bool succeeded = false;
        for (std::uint64_t ordinal = 0; ordinal < 128 && !succeeded;
             ++ordinal) {
            auto source = full_value(1);
            fragmented_buffer_parser input{
              fragmented("p" + std::string{full_record}, 1)};
            input.push_checkpoint().value();
            input.skip(byte_count{1}).value();
            const auto budget = reserve(input);
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            auto& injector = seastar::memory::local_failure_injector();
            bool threw = false;
            bool injected = false;
            injector.fail_after(ordinal);
            try {
                if (decoding) {
                    auto result = model::decode_record(
                                    input, expected, budget, work, coordinates)
                                    .get();
                    injected = injector.failed();
                    injector.cancel();
                    ASSERT_TRUE(result.has_value());
                    succeeded = true;
                } else {
                    auto result = model::encode_record(
                                    source,
                                    work,
                                    memory().operation_remaining,
                                    charge,
                                    coordinates)
                                    .get();
                    injected = injector.failed();
                    injector.cancel();
                    ASSERT_TRUE(result.has_value());
                    EXPECT_TRUE(result->content_equals(full_record));
                    succeeded = true;
                }
            } catch (const std::bad_alloc&) {
                injected = injector.failed();
                threw = true;
            } catch (...) {
                injector.cancel();
                throw;
            }
            injector.cancel();
            if (threw) {
                EXPECT_TRUE(injected);
                failed_once = true;
            } else {
                EXPECT_FALSE(injected);
            }
            EXPECT_EQ(input.checkpoint_depth(), 1U);
            EXPECT_EQ(
              input.bytes_consumed(),
              byte_count{decoding && succeeded ? 1U + full_record.size() : 1U});
            expect_full(source);
            input.rollback().value();
            EXPECT_EQ(input.bytes_consumed(), byte_count{});
        }
        EXPECT_TRUE(failed_once);
        EXPECT_TRUE(succeeded);
    }
#endif
}

} // namespace
