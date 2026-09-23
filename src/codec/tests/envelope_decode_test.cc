#include "src/base/error.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/bytes/fragmented_buffer_parser.h"
#include "src/codec/cooperative.h"
#include "src/codec/envelope.h"
#include "src/codec/envelope_decode.h"
#include "src/codec/envelope_encode.h"
#include "src/codec/error.h"
#include "src/codec/integer.h"
#include "src/codec/limits.h"
#include "src/codec/tests/envelope_decode_test_support.h"
#include "src/codec/transaction.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/preempt.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/alloc_failure_injector.hh>
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
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

namespace {

using namespace std::literals;
namespace codec = kwaque::codec;
namespace fixture = codec::testing::envelope_fixture;
using kwaque::byte_count;
using kwaque::errc;
using kwaque::item_count;
using kwaque::bytes::fragmented_buffer;
using kwaque::bytes::fragmented_buffer_parser;

constexpr codec::field_context context{.origin = 100, .family = 1, .field = 91};
constexpr codec::envelope_extent_limits owner_limits{
  .max_body_bytes = byte_count{16'777'216},
  .max_encoded_bytes = byte_count{33'554'432}};
constexpr std::uint64_t expected_object = UINT64_C(0x0102030405060708);
constexpr std::uint64_t expected_generation = UINT64_C(0x1112131415161718);
constexpr std::uint64_t expected_position = UINT64_C(0x2122232425262728);

enum class body_field : std::uint16_t {
    object = 101,
    generation = 102,
    position = 103,
    payload_length = 104,
    padding_length = 105,
    payload = 106,
    padding = 107,
};

struct expected_body final {
    std::uint64_t object{expected_object};
    std::uint64_t generation{expected_generation};
    std::uint64_t position{expected_position};
};

struct tiny_body final {
    std::uint64_t object;
    std::uint64_t generation;
    std::uint64_t position;
    std::array<char, 3> payload;

    bool operator==(const tiny_body&) const = default;
};

template<typename T>
struct declared_async_decoder {
    seastar::future<codec::result<T>> operator()(
      fragmented_buffer_parser&,
      codec::field_context,
      codec::input_boundary,
      codec::decode_budget,
      codec::cooperative_work&);
};

template<typename T>
concept accepts_body_result = requires(
  fragmented_buffer_parser& input,
  codec::cooperative_work& work,
  codec::decode_budget memory) {
    codec::decode_envelope<T>(
      input,
      codec::format_family::submitted_batch,
      owner_limits,
      memory,
      work,
      declared_async_decoder<T>{});
};

static_assert(accepts_body_result<tiny_body>);
static_assert(!accepts_body_result<char*>);
static_assert(!accepts_body_result<std::span<const char>>);
static_assert(!accepts_body_result<std::string_view>);
static_assert(!accepts_body_result<fragmented_buffer_parser>);

constexpr tiny_body expected_value{
  expected_object, expected_generation, expected_position, {'a', 'b', 'c'}};

constexpr auto future_extensions
  = "\x02\x00\x00\x00\x03\x00\x00\x00\x01\x02\x03"
    "\x05\x00\x00\x00\x02\x00\x00\x00\x04\x05"sv;
constexpr auto future_prefix
  = "\x4b\x51\x42\x46\x01\x00\x02\x00\x01\x00\x35\x00\x24\x00\x00\x00"
    "\x00\x00\x00\x00\x00\x00\x00\x00\xa2\x49\x09\xdd\x85\x1e\x50\x19"sv;
static_assert(future_extensions.size() == 21);
static_assert(future_prefix.size() == codec::envelope_prefix_bytes);

// Synthetic conservative served-capacity profile for bounded test fixtures.
byte_count test_charge(byte_count requested) noexcept {
    if (requested.value() == 0) {
        return {};
    }
    if (requested.value() > (std::uint64_t{1} << 62U)) {
        return byte_count{std::numeric_limits<std::uint64_t>::max()};
    }
    return byte_count{
      2U * std::bit_ceil(std::max(requested.value(), std::uint64_t{16}))};
}

codec::decode_budget generous_budget() noexcept {
    return {
      byte_count{64U * 1024U * 1024U}, byte_count{1024U * 1024U}, test_charge};
}

codec::field_context
body_context(codec::field_context origin, body_field field) {
    origin.field = static_cast<std::uint16_t>(field);
    return origin;
}

codec::result<tiny_body> read_tiny_body(
  fragmented_buffer_parser& input,
  codec::field_context origin,
  codec::input_boundary boundary,
  expected_body expected) {
    const auto object = codec::read_le<std::uint64_t>(
      input, body_context(origin, body_field::object), boundary);
    if (!object) {
        return codec::failure(object.error());
    }
    const auto generation = codec::read_le<std::uint64_t>(
      input, body_context(origin, body_field::generation), boundary);
    if (!generation) {
        return codec::failure(generation.error());
    }
    const auto position = codec::read_le<std::uint64_t>(
      input, body_context(origin, body_field::position), boundary);
    if (!position) {
        return codec::failure(position.error());
    }
    for (const auto& field : std::array{
           std::tuple{*object, expected.object, body_field::object, 0U},
           std::tuple{
             *generation, expected.generation, body_field::generation, 8U},
           std::tuple{
             *position, expected.position, body_field::position, 16U}}) {
        if (std::get<0>(field) != std::get<1>(field)) {
            return codec::failure(
              codec::error{
                errc::wrong_context,
                origin.family,
                static_cast<std::uint16_t>(std::get<2>(field)),
                origin.origin + std::get<3>(field)});
        }
    }
    const auto payload_length = codec::read_le<std::uint32_t>(
      input, body_context(origin, body_field::payload_length), boundary);
    if (!payload_length) {
        return codec::failure(payload_length.error());
    }
    const auto padding_length = codec::read_le<std::uint32_t>(
      input, body_context(origin, body_field::padding_length), boundary);
    if (!padding_length) {
        return codec::failure(padding_length.error());
    }
    if (*payload_length != 3) {
        return codec::failure(
          codec::error{
            errc::malformed_data,
            origin.family,
            static_cast<std::uint16_t>(body_field::payload_length),
            origin.origin + 24});
    }
    if (*padding_length > 8) {
        return codec::failure(
          codec::error{
            errc::resource_exhausted,
            origin.family,
            static_cast<std::uint16_t>(body_field::padding_length),
            origin.origin + 28});
    }
    tiny_body value{*object, *generation, *position, {}};
    for (auto& octet : value.payload) {
        const auto next = codec::read_le<std::uint8_t>(
          input, body_context(origin, body_field::payload), boundary);
        if (!next) {
            return codec::failure(next.error());
        }
        octet = static_cast<char>(*next);
    }
    for (std::uint32_t index = 0; index < *padding_length; ++index) {
        const auto offset = input.bytes_consumed().value();
        const auto padding = codec::read_le<std::uint8_t>(
          input, body_context(origin, body_field::padding), boundary);
        if (!padding) {
            return codec::failure(padding.error());
        }
        if (*padding != 0) {
            return codec::failure(
              codec::error{
                errc::malformed_data,
                origin.family,
                static_cast<std::uint16_t>(body_field::padding),
                origin.origin + offset});
        }
    }
    return value;
}

enum class decoder_behavior {
    normal,
    leave_unread,
    overread,
    typed_failure,
    exception,
    parked,
};

struct decoder_failure final : std::runtime_error {
    decoder_failure()
      : std::runtime_error("test body decoder failure") {}
};

struct decoder_state final {
    decoder_behavior behavior{decoder_behavior::normal};
    std::size_t calls{0};
    std::size_t destroyed{0};
    byte_count child_bytes;
    codec::field_context child_context{};
    codec::input_boundary child_boundary{codec::input_boundary::open};
    codec::decode_budget child_budget{};
    codec::cooperative_work* child_work{nullptr};
    std::optional<seastar::promise<>> entered;
    std::optional<seastar::promise<>> release;
    seastar::abort_source* abort_on_destroy{nullptr};
};

class tiny_decoder final {
public:
    tiny_decoder(decoder_state& state, expected_body expected = {}) noexcept
      : state_(&state)
      , expected_(expected) {}
    tiny_decoder(const tiny_decoder&) = delete;
    tiny_decoder& operator=(const tiny_decoder&) = delete;
    tiny_decoder(tiny_decoder&& other) noexcept
      : state_(std::exchange(other.state_, nullptr))
      , expected_(other.expected_) {}
    ~tiny_decoder() noexcept {
        if (state_ != nullptr) {
            ++state_->destroyed;
            if (state_->abort_on_destroy != nullptr) {
                state_->abort_on_destroy->request_abort();
            }
        }
    }

    seastar::future<codec::result<tiny_body>> operator()(
      fragmented_buffer_parser& input,
      codec::field_context origin,
      codec::input_boundary boundary,
      codec::decode_budget memory,
      codec::cooperative_work& work) {
        ++state_->calls;
        state_->child_bytes = input.total_bytes();
        state_->child_context = origin;
        state_->child_boundary = boundary;
        state_->child_budget = memory;
        state_->child_work = &work;
        if (state_->behavior == decoder_behavior::parked) {
            auto released = state_->release->get_future();
            state_->entered->set_value();
            co_await std::move(released);
        }
        // This fixture's complete body is at most 43 bytes. Its scalar reads,
        // copies and inline result fit one fixed work leaf.
        const codec::error anchor{
          errc::success, origin.family, 101, origin.origin};
        const auto admitted = co_await work.admit(
          byte_count{128}, item_count{32}, anchor);
        if (!admitted) {
            co_return codec::failure(admitted.error());
        }
        if (const auto valid = work.poll(anchor); !valid) {
            co_return codec::failure(valid.error());
        }
        if (state_->behavior == decoder_behavior::leave_unread) {
            input.skip(byte_count{1}).value();
            co_return expected_value;
        }
        if (state_->behavior == decoder_behavior::typed_failure) {
            input.skip(byte_count{1}).value();
            co_return codec::failure(
              codec::error{
                errc::invalid_argument, origin.family, 181, origin.origin + 1});
        }
        if (state_->behavior == decoder_behavior::exception) {
            input.skip(byte_count{1}).value();
            throw decoder_failure{};
        }
        auto decoded = read_tiny_body(input, origin, boundary, expected_);
        if (decoded && state_->behavior == decoder_behavior::overread) {
            const auto next = codec::read_le<std::uint8_t>(
              input, body_context(origin, body_field::payload), boundary);
            if (!next) {
                co_return codec::failure(next.error());
            }
        }
        co_return decoded;
    }

private:
    decoder_state* state_;
    expected_body expected_;
};

codec::result<codec::decode_budget> admitted_input(
  const fragmented_buffer_parser& input,
  const codec::limits& policy,
  codec::field_context origin = context,
  codec::input_boundary boundary = codec::input_boundary::open) {
    // The unclaimed half of the operation budget remains reserved for fixture
    // native-engine/coroutine/callback storage, independently of input sharing.
    // This generous fixture reserve does not measure native/frame peak usage.
    auto budget = generous_budget();
    budget.operation_remaining = byte_count{32U * 1024U * 1024U};
    return codec::reserve_decode_input(input, policy, budget, origin, boundary);
}

codec::result<tiny_body> decode(
  fragmented_buffer_parser& input,
  decoder_state& state,
  expected_body expected = {},
  codec::format_family family = codec::format_family::submitted_batch,
  codec::field_context origin = context,
  codec::input_boundary boundary = codec::input_boundary::open) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto memory = admitted_input(input, work.policy(), origin, boundary);
    if (!memory) {
        return codec::failure(memory.error());
    }
    return codec::decode_envelope<tiny_body>(
             input,
             family,
             owner_limits,
             *memory,
             work,
             tiny_decoder{state, expected},
             origin,
             boundary)
      .get();
}

void expect_code(const auto& outcome, errc code) {
    ASSERT_FALSE(outcome.has_value());
    EXPECT_EQ(outcome.error().code(), code);
}

std::string unread(const fragmented_buffer_parser& input) {
    std::string bytes(input.bytes_remaining().value(), '\0');
    input.peek_to(std::span<char>{bytes}).value();
    return bytes;
}

TEST(EnvelopeDecodeTest, IndependentFixtureMatchesLiteralCompleteBytes) {
    EXPECT_EQ(fixture::crc32c("123456789"sv), 0xe3069283U);
    EXPECT_EQ(fixture::crc32c(fixture::fixed_body), 0xdd0949a2U);
    EXPECT_EQ(
      fixture::make_envelope(),
      std::string{fixture::fixed_prefix} + std::string{fixture::fixed_body});
}

TEST(EnvelopeDecodeTest, LiteralFutureHeaderKeepsBothOptionalValuesOpaque) {
    EXPECT_EQ(
      fixture::make_envelope(fixture::fixed_body, future_extensions, 1, 2, 1),
      std::string{future_prefix} + std::string{future_extensions}
        + std::string{fixture::fixed_body});
    for (const std::uint16_t writer :
         {std::uint16_t{2}, std::uint16_t{65535}}) {
        const auto encoded = fixture::make_envelope(
          fixture::fixed_body, future_extensions, 1, writer, 1);
        fragmented_buffer_parser input{fixture::fragmented(encoded, 1)};
        decoder_state state;
        const auto value = decode(input, state);
        ASSERT_TRUE(value.has_value());
        EXPECT_EQ(*value, expected_value);
        EXPECT_EQ(state.calls, 1U);
        EXPECT_EQ(state.child_context.origin, 153U);
        EXPECT_EQ(state.child_bytes, byte_count{36});
        EXPECT_EQ(input.bytes_consumed(), byte_count{89});
        EXPECT_TRUE(input.at_end());
    }
}

TEST(EnvelopeDecodeTest, OptionalReadSupportDoesNotActivateFutureWriterBytes) {
    const auto original = fixture::make_envelope(
      fixture::fixed_body, future_extensions, 1, 2, 1);
    fragmented_buffer_parser input{fixture::fragmented(original, 3)};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto memory = admitted_input(input, work.policy()).value();
    decoder_state state;
    const auto decoded = codec::decode_envelope<tiny_body>(
                           input,
                           codec::format_family::submitted_batch,
                           owner_limits,
                           memory,
                           work,
                           tiny_decoder{state},
                           context)
                           .get();
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, expected_value);

    // Re-encode the known body projection. The skipped extension bytes are
    // deliberately absent from the value and cannot be reconstructed from it.
    auto body_bytes = std::string{fixture::fixed_body};
    fixture::put_u64(body_bytes, 0, decoded->object);
    fixture::put_u64(body_bytes, 8, decoded->generation);
    fixture::put_u64(body_bytes, 16, decoded->position);
    std::ranges::copy(decoded->payload, body_bytes.begin() + 32);
    auto body = fragmented_buffer::copy_of(body_bytes).value();
    const auto encoded = codec::encode_envelope(
                           std::move(body),
                           codec::format_family::submitted_batch,
                           work,
                           owner_limits,
                           {},
                           memory.operation_remaining,
                           test_charge,
                           context)
                           .get();
    ASSERT_TRUE(encoded.has_value());
    EXPECT_TRUE(encoded->content_equals(fixture::make_envelope(body_bytes)));
    EXPECT_FALSE(encoded->content_equals(original));
    EXPECT_EQ(encoded->size(), byte_count{68});
    EXPECT_EQ(input.bytes_consumed(), byte_count{original.size()});
    EXPECT_EQ(input.checkpoint_depth(), 0U);
}

TEST(EnvelopeDecodeTest, FutureOptionalHeadersCannotLegalizeChangedBodyTails) {
    const auto valid = std::string{fixture::fixed_body};
    for (const auto& body :
         {valid.substr(0, 30),
          valid.substr(0, 32),
          valid.substr(0, 35),
          valid + std::string{"\x34\x12"sv},
          valid + std::string{"\x78\x56\x34\x12"sv}}) {
        const auto encoded = fixture::make_envelope(
          body, future_extensions, 1, 2, 1);
        fragmented_buffer_parser input{
          fixture::fragmented(encoded + fixture::make_envelope(), 7)};
        decoder_state state;
        const auto value = decode(input, state);
        expect_code(value, errc::malformed_data);
        EXPECT_EQ(state.calls, 1U);
        EXPECT_EQ(state.child_bytes, byte_count{body.size()});
        EXPECT_EQ(input.bytes_consumed(), byte_count{});
        EXPECT_EQ(input.checkpoint_depth(), 0U);
        EXPECT_EQ(unread(input), encoded + fixture::make_envelope());
    }
}

TEST(EnvelopeDecodeTest, FutureUnknownTagsStillEnforceMandatoryAndWireOrder) {
    for (const auto mutation : {0U, 1U, 2U}) {
        auto extensions = std::string{future_extensions};
        if (mutation == 0) {
            fixture::put_u16(extensions, 2, 1);
        } else if (mutation == 1) {
            fixture::put_u16(extensions, 11, 2);
        } else {
            fixture::put_u16(extensions, 0, 5);
            fixture::put_u16(extensions, 11, 2);
        }
        const auto encoded = fixture::make_envelope(
          fixture::fixed_body, extensions, 1, 2, 1);
        fragmented_buffer_parser input{fixture::fragmented(encoded, 3)};
        decoder_state state;
        const auto value = decode(input, state);
        expect_code(
          value,
          mutation == 0 ? errc::unsupported_format : errc::malformed_data);
        EXPECT_EQ(state.calls, 0U);
        EXPECT_EQ(input.bytes_consumed(), byte_count{});
        EXPECT_EQ(unread(input), encoded);
    }
}

TEST(EnvelopeDecodeTest, SharedSlicesRetainOffsetsAfterEveryDonorIsDestroyed) {
    constexpr std::size_t prefix_size = 13;
    constexpr std::size_t visible_preamble = 3;
    constexpr std::uint64_t backing_origin = 1000;
    const codec::field_context view_context{
      .origin = backing_origin + prefix_size - visible_preamble,
      .family = 1,
      .field = 91};
    for (const std::size_t fragment_size :
         {7U, 31U, 32U, 33U, 67U, 68U, 69U, 128U}) {
        for (const bool duplicate_layers : {false, true}) {
            for (const bool malformed_padding : {false, true}) {
                SCOPED_TRACE(
                  ::testing::Message()
                  << fragment_size << ':' << duplicate_layers << ':'
                  << malformed_padding);
                auto body = std::string{fixture::fixed_body};
                if (malformed_padding) {
                    body.back() = 1;
                }
                const auto encoded = fixture::make_envelope(body);
                fragmented_buffer surviving;
                {
                    const auto storage = std::string(prefix_size, 'H') + encoded
                                         + std::string(29, 'Z');
                    auto source = fixture::fragmented(storage, fragment_size);
                    constexpr auto offset = prefix_size - visible_preamble;
                    const auto original = source.fragment_at(
                      offset / fragment_size);
                    ASSERT_TRUE(original.has_value());
                    const char* expected_begin = original->data()
                                                 + offset % fragment_size;
                    if (duplicate_layers) {
                        auto first_duplicate = source.share();
                        auto second_duplicate = first_duplicate.share();
                        auto slice = second_duplicate.share(
                          byte_count{offset},
                          byte_count{visible_preamble + encoded.size()});
                        ASSERT_TRUE(slice.has_value());
                        surviving = slice->share();
                    } else {
                        auto slice = source.share(
                          byte_count{offset},
                          byte_count{visible_preamble + encoded.size()});
                        ASSERT_TRUE(slice.has_value());
                        surviving = std::move(*slice);
                    }
                    ASSERT_TRUE(surviving.fragment_at(0).has_value());
                    EXPECT_EQ(surviving.fragment_at(0)->data(), expected_begin);
                    EXPECT_GT(surviving.retained_bytes(), surviving.size());
                }
                fragmented_buffer_parser input{std::move(surviving)};
                ASSERT_TRUE(
                  input.skip(byte_count{visible_preamble}).has_value());
                decoder_state state;
                const auto value = decode(
                  input,
                  state,
                  {},
                  codec::format_family::submitted_batch,
                  view_context);
                EXPECT_EQ(state.calls, 1U);
                EXPECT_EQ(
                  state.child_context.origin,
                  backing_origin + prefix_size + 32);
                if (malformed_padding) {
                    expect_code(value, errc::malformed_data);
                    ASSERT_FALSE(value.has_value());
                    EXPECT_EQ(
                      value.error().field(),
                      static_cast<std::uint16_t>(body_field::padding));
                    EXPECT_EQ(
                      value.error().byte_offset(),
                      backing_origin + prefix_size + 32 + 35);
                    EXPECT_EQ(
                      input.bytes_consumed(), byte_count{visible_preamble});
                    EXPECT_EQ(unread(input), encoded);
                } else {
                    ASSERT_TRUE(value.has_value());
                    EXPECT_EQ(*value, expected_value);
                    EXPECT_EQ(
                      input.bytes_consumed(),
                      byte_count{visible_preamble + encoded.size()});
                    EXPECT_TRUE(input.at_end());
                }
                EXPECT_EQ(input.checkpoint_depth(), 0U);
            }
        }
    }
}

TEST(EnvelopeDecodeTest, ACommittedEnvelopeDoesNotHideAnIncompleteSuccessor) {
    const auto first = fixture::make_envelope();
    for (const std::size_t cut : {0U, 1U, 31U, 32U, 67U}) {
        for (const auto boundary :
             {codec::input_boundary::open, codec::input_boundary::complete}) {
            SCOPED_TRACE(
              ::testing::Message()
              << cut << ':' << static_cast<unsigned>(boundary));
            const auto tail = first.substr(0, cut);
            fragmented_buffer_parser input{
              fixture::fragmented(first + tail, 17)};
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            const auto memory
              = admitted_input(input, work.policy(), context, boundary).value();
            decoder_state first_state;
            const auto first_value = codec::decode_envelope<tiny_body>(
                                       input,
                                       codec::format_family::submitted_batch,
                                       owner_limits,
                                       memory,
                                       work,
                                       tiny_decoder{first_state},
                                       context,
                                       boundary)
                                       .get();
            ASSERT_TRUE(first_value.has_value());
            EXPECT_EQ(*first_value, expected_value);
            EXPECT_EQ(input.bytes_consumed(), byte_count{68});
            decoder_state second_state;
            const auto second_value = codec::decode_envelope<tiny_body>(
                                        input,
                                        codec::format_family::submitted_batch,
                                        owner_limits,
                                        memory,
                                        work,
                                        tiny_decoder{second_state},
                                        context,
                                        boundary)
                                        .get();
            expect_code(
              second_value,
              boundary == codec::input_boundary::open ? errc::truncated_data
                                                      : errc::malformed_data);
            ASSERT_FALSE(second_value.has_value());
            EXPECT_EQ(second_value.error().byte_offset(), 168 + cut);
            EXPECT_EQ(second_state.calls, 0U);
            EXPECT_EQ(input.bytes_consumed(), byte_count{68});
            EXPECT_EQ(input.checkpoint_depth(), 0U);
            EXPECT_EQ(unread(input), tail);
        }
    }
}

TEST(EnvelopeDecodeTest, InvalidSuccessorsAreNotSkippedAsRecoveryOrZeroEof) {
    const auto valid = fixture::make_envelope();
    auto bad_header = valid;
    bad_header[28] ^= 1;
    auto bad_length = valid;
    fixture::put_u32(bad_length, 12, 37);
    fixture::repair_header_crc(bad_length);
    struct tail_case final {
        std::string bytes;
        errc expected;
    };
    for (const auto& tail : std::array{
           tail_case{std::string(32, '\0'), errc::malformed_data},
           tail_case{bad_header, errc::corrupt_data},
           tail_case{bad_length, errc::corrupt_data},
           tail_case{
             fixture::make_envelope(fixture::fixed_body, {}, 12),
             errc::unsupported_format}}) {
        fragmented_buffer_parser input{
          fixture::fragmented(valid + tail.bytes + valid, 31)};
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        const auto memory = admitted_input(input, work.policy()).value();
        decoder_state first_state;
        const auto first_value = codec::decode_envelope<tiny_body>(
                                   input,
                                   codec::format_family::submitted_batch,
                                   owner_limits,
                                   memory,
                                   work,
                                   tiny_decoder{first_state},
                                   context)
                                   .get();
        ASSERT_TRUE(first_value.has_value());
        EXPECT_EQ(*first_value, expected_value);
        decoder_state second_state;
        const auto second_value = codec::decode_envelope<tiny_body>(
                                    input,
                                    codec::format_family::submitted_batch,
                                    owner_limits,
                                    memory,
                                    work,
                                    tiny_decoder{second_state},
                                    context)
                                    .get();
        expect_code(second_value, tail.expected);
        EXPECT_EQ(second_state.calls, 0U);
        EXPECT_EQ(input.bytes_consumed(), byte_count{68});
        EXPECT_EQ(input.checkpoint_depth(), 0U);
        EXPECT_EQ(unread(input), tail.bytes + valid);
    }
}

TEST(EnvelopeDecodeTest, EverySplitCommitsOneEnvelopeAndReturnsOwningValue) {
    const auto encoded = fixture::make_envelope();
    const auto combined = "pre" + encoded + encoded;
    for (std::size_t cut = 0; cut <= combined.size(); ++cut) {
        SCOPED_TRACE(cut);
        std::optional<tiny_body> owned;
        decoder_state state;
        {
            fragmented_buffer_parser input{fixture::split_at(combined, cut)};
            ASSERT_TRUE(input.skip(byte_count{3}).has_value());
            const auto value = decode(input, state);
            ASSERT_TRUE(value.has_value());
            owned = *value;
            EXPECT_EQ(state.calls, 1U);
            EXPECT_EQ(state.destroyed, 1U);
            EXPECT_EQ(state.child_bytes, byte_count{36});
            EXPECT_EQ(state.child_context.origin, 135U);
            EXPECT_EQ(state.child_boundary, codec::input_boundary::complete);
            EXPECT_EQ(input.bytes_consumed(), byte_count{71});
            EXPECT_EQ(input.checkpoint_depth(), 0U);
            EXPECT_EQ(unread(input), encoded);
        }
        ASSERT_TRUE(owned.has_value());
        EXPECT_EQ(*owned, expected_value);
    }
}

TEST(EnvelopeDecodeTest, EveryCutRejectsAtFirstMissingByteWithoutCallback) {
    const auto encoded = fixture::make_envelope();
    for (std::size_t cut = 0; cut < encoded.size(); ++cut) {
        for (const auto boundary :
             {codec::input_boundary::open, codec::input_boundary::complete}) {
            SCOPED_TRACE(
              ::testing::Message()
              << cut << ':' << static_cast<unsigned>(boundary));
            const auto bytes = std::string{"pre"} + encoded.substr(0, cut);
            fragmented_buffer_parser input{fixture::fragmented(bytes, 1)};
            ASSERT_TRUE(input.skip(byte_count{3}).has_value());
            decoder_state state;
            const auto value = decode(
              input,
              state,
              {},
              codec::format_family::submitted_batch,
              context,
              boundary);
            expect_code(
              value,
              boundary == codec::input_boundary::open ? errc::truncated_data
                                                      : errc::malformed_data);
            ASSERT_FALSE(value.has_value());
            EXPECT_EQ(value.error().byte_offset(), 103 + cut);
            EXPECT_EQ(state.calls, 0U);
            EXPECT_EQ(input.bytes_consumed(), byte_count{3});
            EXPECT_EQ(input.checkpoint_depth(), 0U);
            EXPECT_EQ(unread(input), encoded.substr(0, cut));
        }
    }
}

TEST(
  EnvelopeDecodeTest, HeaderIntegrityWinsOverVersionsFeaturesBodyAndContext) {
    for (const auto offset : {4U, 6U, 8U, 12U, 16U, 24U, 28U}) {
        auto encoded = fixture::make_envelope();
        encoded[offset] ^= 1;
        encoded.back() ^= 1;
        fragmented_buffer_parser input{fixture::fragmented(encoded, 3)};
        decoder_state state;
        const auto value = decode(
          input, state, {}, codec::format_family::assigned_batch);
        expect_code(value, errc::corrupt_data);
        ASSERT_FALSE(value.has_value());
        EXPECT_EQ(
          value.error().field(),
          static_cast<std::uint16_t>(codec::envelope_field::header_crc32c));
        EXPECT_EQ(state.calls, 0U);
        EXPECT_EQ(input.bytes_consumed(), byte_count{});
    }
}

TEST(
  EnvelopeDecodeTest, HeaderIntegrityPrecedesMissingBodyAndExtensionGrammar) {
    const std::string malformed_extension(8, '\0');
    auto encoded = fixture::make_envelope(
      fixture::fixed_body, malformed_extension);
    encoded[32] = 1;
    encoded.resize(40);
    fragmented_buffer_parser input{fixture::fragmented(encoded, 1)};
    decoder_state state;
    expect_code(decode(input, state), errc::corrupt_data);
    EXPECT_EQ(state.calls, 0U);
    EXPECT_EQ(input.bytes_consumed(), byte_count{});

    auto invalid_tag = fixture::make_envelope(
      fixture::fixed_body, malformed_extension);
    invalid_tag.resize(40);
    fragmented_buffer_parser invalid{fixture::fragmented(invalid_tag, 3)};
    decoder_state tag_state;
    expect_code(decode(invalid, tag_state), errc::malformed_data);
    EXPECT_EQ(tag_state.calls, 0U);
}

TEST(EnvelopeDecodeTest, CompleteHeaderShortagePrecedesBodyAndCompatibility) {
    const auto encoded = fixture::make_envelope(
      fixture::fixed_body, std::string(8, '\0'), 0, 0, 1, 1);
    for (const auto boundary :
         {codec::input_boundary::open, codec::input_boundary::complete}) {
        fragmented_buffer_parser input{
          fixture::fragmented(std::string_view{encoded}.substr(0, 39), 2)};
        decoder_state state;
        const auto value = decode(
          input,
          state,
          {},
          codec::format_family::submitted_batch,
          context,
          boundary);
        expect_code(
          value,
          boundary == codec::input_boundary::open ? errc::truncated_data
                                                  : errc::malformed_data);
        ASSERT_FALSE(value.has_value());
        EXPECT_EQ(value.error().byte_offset(), 139U);
        EXPECT_EQ(state.calls, 0U);
    }
}

TEST(EnvelopeDecodeTest, BodyChecksumPrecedesExpectedFamilyAndBodyDecoder) {
    auto encoded = fixture::make_envelope();
    encoded[32] ^= 1;
    fragmented_buffer_parser corrupt{fixture::fragmented(encoded, 1)};
    decoder_state never_called;
    const auto failure = decode(
      corrupt, never_called, {}, codec::format_family::assigned_batch);
    expect_code(failure, errc::corrupt_data);
    ASSERT_FALSE(failure.has_value());
    EXPECT_EQ(
      failure.error().field(),
      static_cast<std::uint16_t>(codec::envelope_field::body_crc32c));
    EXPECT_EQ(never_called.calls, 0U);

    fragmented_buffer_parser valid{
      fixture::fragmented(fixture::make_envelope(), 3)};
    decoder_state wrong_family;
    expect_code(
      decode(valid, wrong_family, {}, codec::format_family::assigned_batch),
      errc::wrong_context);
    EXPECT_EQ(wrong_family.calls, 0U);
    EXPECT_EQ(valid.bytes_consumed(), byte_count{});
}

TEST(
  EnvelopeDecodeTest, RepairedIntegrityStillRejectsIndependentExpectedTuple) {
    for (const auto field :
         {body_field::object, body_field::generation, body_field::position}) {
        expected_body expected;
        std::uint64_t offset = 0;
        if (field == body_field::object) {
            ++expected.object;
        } else if (field == body_field::generation) {
            ++expected.generation;
            offset = 8;
        } else {
            ++expected.position;
            offset = 16;
        }
        auto body = std::string{fixture::fixed_body};
        fixture::put_u64(
          body,
          static_cast<std::size_t>(offset),
          field == body_field::object       ? expected.object
          : field == body_field::generation ? expected.generation
                                            : expected.position);
        fragmented_buffer_parser input{
          fixture::fragmented(fixture::make_envelope(body), 1)};
        decoder_state state;
        const auto value = decode(input, state);
        expect_code(value, errc::wrong_context);
        ASSERT_FALSE(value.has_value());
        EXPECT_EQ(value.error().field(), static_cast<std::uint16_t>(field));
        EXPECT_EQ(value.error().byte_offset(), 132 + offset);
        EXPECT_EQ(state.calls, 1U);
        EXPECT_EQ(input.bytes_consumed(), byte_count{});
    }
}

TEST(
  EnvelopeDecodeTest, AddedRemovedAndPartialBodyFieldsAreExactGrammarFailures) {
    const auto valid = std::string{fixture::fixed_body};
    for (const auto& body :
         {valid.substr(0, 0),
          valid.substr(0, 7),
          valid.substr(0, 26),
          valid.substr(0, 35),
          valid + "extra"}) {
        const auto encoded = fixture::make_envelope(body);
        fragmented_buffer_parser input{
          fixture::fragmented(encoded + "adjacent", 3)};
        decoder_state state;
        expect_code(decode(input, state), errc::malformed_data);
        EXPECT_EQ(state.calls, 1U);
        EXPECT_EQ(input.bytes_consumed(), byte_count{});
        EXPECT_EQ(unread(input), encoded + "adjacent");
    }
    auto bad_padding = valid;
    bad_padding.back() = 1;
    fragmented_buffer_parser input{
      fixture::fragmented(fixture::make_envelope(bad_padding), 1)};
    decoder_state state;
    const auto value = decode(input, state);
    expect_code(value, errc::malformed_data);
    ASSERT_FALSE(value.has_value());
    EXPECT_EQ(
      value.error().field(), static_cast<std::uint16_t>(body_field::padding));
    EXPECT_EQ(value.error().byte_offset(), 167U);
}

TEST(
  EnvelopeDecodeTest,
  CompatibleOptionalHeaderExtensionPreservesExactBodyGrammar) {
    std::string extension(11, '\0');
    fixture::put_u16(extension, 0, 0x7ffe);
    fixture::put_u32(extension, 4, 3);
    extension.replace(8, 3, "xyz");
    const auto encoded = fixture::make_envelope(
      fixture::fixed_body, extension, 1, 2, 1);
    fragmented_buffer_parser input{fixture::fragmented(encoded + "next", 1)};
    decoder_state state;
    const auto value = decode(input, state);
    ASSERT_TRUE(value.has_value());
    EXPECT_EQ(*value, expected_value);
    EXPECT_EQ(state.child_context.origin, 143U);
    EXPECT_EQ(state.child_bytes, byte_count{36});
    EXPECT_EQ(input.bytes_consumed(), byte_count{79});
    EXPECT_EQ(unread(input), "next");
}

TEST(
  EnvelopeDecodeTest, HeaderCompatibilityAndMandatoryExtensionsPrecedeBodyCrc) {
    struct version_case final {
        std::uint16_t family;
        std::uint16_t writer;
        std::uint16_t minimum;
        std::uint64_t features;
        errc expected;
    };
    for (const auto test : std::array{
           version_case{1, 0, 1, 0, errc::malformed_data},
           version_case{1, 1, 2, 0, errc::malformed_data},
           version_case{1, 0, 0, 0, errc::unsupported_format},
           version_case{1, 1, 0, 0, errc::unsupported_format},
           version_case{1, 2, 2, 0, errc::unsupported_format},
           version_case{0, 1, 1, 0, errc::malformed_data},
           version_case{65535, 1, 1, 0, errc::unsupported_format},
           version_case{1, 1, 1, 1, errc::unsupported_format}}) {
        auto encoded = fixture::make_envelope(
          fixture::fixed_body,
          {},
          test.family,
          test.writer,
          test.minimum,
          test.features);
        encoded.back() ^= 1;
        fragmented_buffer_parser input{fixture::fragmented(encoded, 2)};
        decoder_state state;
        expect_code(decode(input, state), test.expected);
        EXPECT_EQ(state.calls, 0U);
        EXPECT_EQ(input.bytes_consumed(), byte_count{});
    }
    std::string extension(8, '\0');
    fixture::put_u16(extension, 0, 0x7ffe);
    fixture::put_u16(extension, 2, 1);
    auto encoded = fixture::make_envelope(fixture::fixed_body, extension);
    encoded.back() ^= 1;
    fragmented_buffer_parser input{fixture::fragmented(encoded, 1)};
    decoder_state state;
    expect_code(decode(input, state), errc::unsupported_format);
    EXPECT_EQ(state.calls, 0U);
}

TEST(EnvelopeDecodeTest, SenderStructuralFailurePrecedesUnknownOrZeroFamily) {
    for (const std::uint16_t family :
         {std::uint16_t{0}, std::uint16_t{12}, std::uint16_t{65535}}) {
        for (const std::uint16_t writer :
             {std::uint16_t{0}, std::uint16_t{1}}) {
            SCOPED_TRACE(::testing::Message() << family << ':' << writer);
            const auto encoded = fixture::make_envelope(
              fixture::fixed_body,
              {},
              family,
              writer,
              static_cast<std::uint16_t>(writer + 1U));
            fragmented_buffer_parser input{
              fixture::fragmented("pre" + encoded + "next", 1)};
            ASSERT_TRUE(input.skip(byte_count{3}).has_value());
            decoder_state state;
            const auto value = decode(input, state);
            expect_code(value, errc::malformed_data);
            ASSERT_FALSE(value.has_value());
            EXPECT_EQ(value.error().family(), context.family);
            EXPECT_EQ(
              value.error().field(),
              static_cast<std::uint16_t>(
                codec::envelope_field::minimum_reader_version));
            EXPECT_EQ(value.error().byte_offset(), 111U);
            EXPECT_EQ(state.calls, 0U);
            EXPECT_EQ(input.bytes_consumed(), byte_count{3});
            EXPECT_EQ(input.checkpoint_depth(), 0U);
            EXPECT_EQ(unread(input), encoded + "next");
        }
    }
}

TEST(
  EnvelopeDecodeTest, ExactChildCannotReadAdjacentEnvelopeOrLeaveBytesUnread) {
    const auto encoded = fixture::make_envelope();
    for (const auto behavior :
         {decoder_behavior::overread, decoder_behavior::leave_unread}) {
        fragmented_buffer_parser input{
          fixture::fragmented(encoded + encoded, 1)};
        decoder_state state{.behavior = behavior};
        const auto value = decode(input, state);
        expect_code(value, errc::malformed_data);
        ASSERT_FALSE(value.has_value());
        EXPECT_EQ(
          value.error().byte_offset(),
          behavior == decoder_behavior::overread ? 168U : 133U);
        EXPECT_EQ(state.child_bytes, byte_count{36});
        EXPECT_EQ(state.child_boundary, codec::input_boundary::complete);
        EXPECT_EQ(input.bytes_consumed(), byte_count{});
        EXPECT_EQ(unread(input), encoded + encoded);
    }
}

TEST(
  EnvelopeDecodeTest, OwnedMarkPreservesEveryExistingCheckpointOnAllOutcomes) {
    for (const auto behavior :
         {decoder_behavior::normal,
          decoder_behavior::typed_failure,
          decoder_behavior::exception}) {
        for (const std::size_t depth : {0U, 1U, 7U, 8U}) {
            fragmented_buffer_parser input{
              fixture::fragmented("p" + fixture::make_envelope(), 1)};
            for (std::size_t index = 0; index < depth; ++index) {
                ASSERT_TRUE(input.push_checkpoint().has_value());
            }
            ASSERT_TRUE(input.skip(byte_count{1}).has_value());
            decoder_state state{.behavior = behavior};
            if (depth == 8) {
                expect_code(decode(input, state), errc::resource_exhausted);
                EXPECT_EQ(state.calls, 0U);
            } else if (behavior == decoder_behavior::exception) {
                EXPECT_THROW(
                  static_cast<void>(decode(input, state)), decoder_failure);
            } else {
                const auto value = decode(input, state);
                EXPECT_EQ(
                  value.has_value(), behavior == decoder_behavior::normal);
                if (behavior == decoder_behavior::typed_failure) {
                    expect_code(value, errc::invalid_argument);
                }
            }
            EXPECT_EQ(input.checkpoint_depth(), depth);
            EXPECT_EQ(
              input.bytes_consumed(),
              byte_count{
                depth < 8 && behavior == decoder_behavior::normal ? 69U : 1U});
            if (depth != 0) {
                ASSERT_TRUE(input.rollback().has_value());
                EXPECT_EQ(input.bytes_consumed(), byte_count{});
                while (input.checkpoint_depth() != 0) {
                    ASSERT_TRUE(input.rollback().has_value());
                }
            }
        }
    }
}

TEST(
  EnvelopeDecodeTest,
  TemporaryCallableLivesAcrossSuspensionAndDiesBeforePublication) {
    fragmented_buffer_parser input{
      fixture::fragmented(fixture::make_envelope(), 1)};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto memory = admitted_input(input, work.policy()).value();
    decoder_state state{.behavior = decoder_behavior::parked};
    state.entered.emplace();
    state.release.emplace();
    auto entered = state.entered->get_future();
    auto waiting = codec::decode_envelope<tiny_body>(
      input,
      codec::format_family::submitted_batch,
      owner_limits,
      memory,
      work,
      tiny_decoder{state},
      context);
    entered.get();
    EXPECT_FALSE(waiting.available());
    EXPECT_EQ(state.destroyed, 0U);
    EXPECT_EQ(state.child_work, &work);
    EXPECT_LT(
      state.child_budget.operation_remaining, memory.operation_remaining);
    EXPECT_LT(state.child_budget.metadata_remaining, memory.metadata_remaining);
    state.release->set_value();
    const auto value = waiting.get();
    ASSERT_TRUE(value.has_value());
    EXPECT_EQ(*value, expected_value);
    EXPECT_EQ(state.destroyed, 1U);
    EXPECT_EQ(input.bytes_consumed(), byte_count{68});
}

TEST(EnvelopeDecodeTest, CallerAbortAndCallbackDestructionPreventFinalCommit) {
    for (const bool before_start : {false, true}) {
        fragmented_buffer_parser input{
          fixture::fragmented(fixture::make_envelope(), 1)};
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        const auto memory = admitted_input(input, work.policy()).value();
        decoder_state state;
        if (before_start) {
            abort.request_abort();
        } else {
            state.abort_on_destroy = &abort;
        }
        const auto value = codec::decode_envelope<tiny_body>(
                             input,
                             codec::format_family::submitted_batch,
                             owner_limits,
                             memory,
                             work,
                             tiny_decoder{state},
                             context)
                             .get();
        expect_code(value, errc::aborted);
        EXPECT_EQ(state.calls, before_start ? 0U : 1U);
        EXPECT_EQ(state.destroyed, 1U);
        EXPECT_EQ(input.bytes_consumed(), byte_count{});
        EXPECT_EQ(input.checkpoint_depth(), 0U);
    }
}

TEST(EnvelopeDecodeTest, EarlierBodyFailureWinsOverAbortDuringCallableCleanup) {
    for (const auto behavior :
         {decoder_behavior::typed_failure, decoder_behavior::exception}) {
        fragmented_buffer_parser input{
          fixture::fragmented(fixture::make_envelope(), 1)};
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        const auto memory = admitted_input(input, work.policy()).value();
        decoder_state state{.behavior = behavior};
        state.abort_on_destroy = &abort;
        auto waiting = codec::decode_envelope<tiny_body>(
          input,
          codec::format_family::submitted_batch,
          owner_limits,
          memory,
          work,
          tiny_decoder{state},
          context);
        if (behavior == decoder_behavior::exception) {
            EXPECT_THROW(static_cast<void>(waiting.get()), decoder_failure);
        } else {
            expect_code(waiting.get(), errc::invalid_argument);
        }
        EXPECT_TRUE(abort.abort_requested());
        EXPECT_EQ(state.destroyed, 1U);
        EXPECT_EQ(input.bytes_consumed(), byte_count{});
        EXPECT_EQ(input.checkpoint_depth(), 0U);
    }
}

struct result_release_probe final {
    std::size_t& releases;
    seastar::abort_source& abort;

    void operator()(char* value) const noexcept {
        delete value;
        ++releases;
        abort.request_abort();
    }
};

TEST(EnvelopeDecodeTest, OwningResultTransfersOrReleasesBeforeFailureReturns) {
    using owned_result = std::unique_ptr<char, result_release_probe>;
    for (const auto behavior :
         {decoder_behavior::normal, decoder_behavior::leave_unread}) {
        for (const bool cleanup_abort : {false, true}) {
            SCOPED_TRACE(
              ::testing::Message()
              << static_cast<int>(behavior) << ':' << cleanup_abort);
            const auto encoded = fixture::make_envelope();
            fragmented_buffer_parser input{
              fixture::fragmented(encoded + "next", 1)};
            ASSERT_TRUE(input.push_checkpoint().has_value());
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            // The fixture's existing opaque-owner reserve covers this result.
            const auto memory = admitted_input(input, work.policy()).value();
            std::size_t releases = 0;
            decoder_state state{.behavior = behavior};
            if (cleanup_abort) {
                state.abort_on_destroy = &abort;
            }
            auto decoder =
              [body = tiny_decoder{state},
               value
               = owned_result{new char{'r'}, result_release_probe{releases, abort}}](
                fragmented_buffer_parser& child,
                codec::field_context origin,
                codec::input_boundary boundary,
                codec::decode_budget remaining,
                codec::cooperative_work& shared_work) mutable
              -> seastar::future<codec::result<owned_result>> {
                auto decoded = co_await body(
                  child, origin, boundary, remaining, shared_work);
                if (!decoded) {
                    co_return codec::failure(decoded.error());
                }
                co_return std::move(value);
            };
            auto value = codec::decode_envelope<owned_result>(
                           input,
                           codec::format_family::submitted_batch,
                           owner_limits,
                           memory,
                           work,
                           std::move(decoder),
                           context)
                           .get();
            EXPECT_EQ(state.calls, 1U);
            EXPECT_EQ(state.destroyed, 1U);
            EXPECT_EQ(input.checkpoint_depth(), 1U);
            if (behavior == decoder_behavior::normal && !cleanup_abort) {
                ASSERT_TRUE(value.has_value());
                ASSERT_NE(value->get(), nullptr);
                EXPECT_EQ(**value, 'r');
                EXPECT_EQ(releases, 0U);
                EXPECT_FALSE(abort.abort_requested());
                EXPECT_EQ(input.bytes_consumed(), byte_count{68});
                EXPECT_EQ(unread(input), "next");
            } else {
                expect_code(
                  value,
                  behavior == decoder_behavior::leave_unread
                    ? errc::malformed_data
                    : errc::aborted);
                EXPECT_EQ(releases, 1U);
                EXPECT_TRUE(abort.abort_requested());
                EXPECT_EQ(input.bytes_consumed(), byte_count{});
                EXPECT_EQ(unread(input), encoded + "next");
            }
            ASSERT_TRUE(input.rollback().has_value());
            input = fragmented_buffer_parser{};
            if (value) {
                // Successful publication keeps the result alive independently
                // of both parsers; disposal is now the caller's responsibility.
                EXPECT_EQ(**value, 'r');
                value->reset();
            }
            EXPECT_EQ(releases, 1U);
        }
    }
}

TEST(
  EnvelopeDecodeTest, ResidualMemoryAndMetadataCannotAcquireFreshAllowances) {
    for (const bool metadata : {false, true}) {
        fragmented_buffer_parser input{
          fixture::fragmented(fixture::make_envelope(), 1)};
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto memory = admitted_input(input, work.policy()).value();
        if (metadata) {
            memory.metadata_remaining = byte_count{};
        } else {
            memory.operation_remaining = byte_count{};
        }
        decoder_state state;
        const auto value = codec::decode_envelope<tiny_body>(
                             input,
                             codec::format_family::submitted_batch,
                             owner_limits,
                             memory,
                             work,
                             tiny_decoder{state},
                             context)
                             .get();
        expect_code(value, errc::resource_exhausted);
        EXPECT_EQ(state.calls, 0U);
        EXPECT_EQ(input.bytes_consumed(), byte_count{});
        EXPECT_EQ(input.checkpoint_depth(), 0U);
    }
}

TEST(
  EnvelopeDecodeTest, SequentialAliasesReuseExactResidualDescriptorAllowance) {
    for (const bool metadata : {false, true}) {
        for (const bool one_short : {false, true}) {
            SCOPED_TRACE(::testing::Message() << metadata << ':' << one_short);
            const auto encoded = fixture::make_envelope();
            fragmented_buffer_parser input{fixture::fragmented(encoded, 1)};
            const auto header_cost = input.next_buffer_allocation_cost(
              byte_count{32}, test_charge);
            ASSERT_TRUE(header_cost.has_value());
            ASSERT_TRUE(input.push_checkpoint().has_value());
            ASSERT_TRUE(input.skip(byte_count{32}).has_value());
            const auto body_cost = input.next_buffer_allocation_cost(
              byte_count{36}, test_charge);
            ASSERT_TRUE(body_cost.has_value());
            ASSERT_TRUE(input.rollback().has_value());
            ASSERT_GT(header_cost->descriptors.value(), 0U);
            ASSERT_GT(body_cost->descriptors.value(), 0U);
            const auto required = std::max(
              header_cost->descriptors, body_cost->descriptors);

            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            auto memory = admitted_input(input, work.policy()).value();
            // Query the established alias-cost contract before any sharing.
            // Parent backing/share controls are reserved once; only one
            // header, body-CRC or semantic-child alias is live at a time.
            // These costs test ownership accounting, not measured native peaks.
            const auto allowance
              = one_short ? required.checked_sub(byte_count{1}).value()
                          : required;
            if (metadata) {
                memory.metadata_remaining = allowance;
            } else {
                memory.operation_remaining = allowance;
            }
            decoder_state state;
            const auto value = codec::decode_envelope<tiny_body>(
                                 input,
                                 codec::format_family::submitted_batch,
                                 owner_limits,
                                 memory,
                                 work,
                                 tiny_decoder{state},
                                 context)
                                 .get();
            if (one_short) {
                expect_code(value, errc::resource_exhausted);
                EXPECT_EQ(state.calls, 0U);
                EXPECT_EQ(input.bytes_consumed(), byte_count{});
                EXPECT_EQ(unread(input), encoded);
            } else {
                ASSERT_TRUE(value.has_value());
                EXPECT_EQ(*value, expected_value);
                EXPECT_EQ(state.calls, 1U);
                EXPECT_EQ(input.bytes_consumed(), byte_count{68});
                const auto remaining = required.checked_sub(
                  body_cost->descriptors);
                ASSERT_TRUE(remaining.has_value());
                EXPECT_EQ(
                  metadata ? state.child_budget.metadata_remaining
                           : state.child_budget.operation_remaining,
                  *remaining);
            }
            EXPECT_EQ(input.checkpoint_depth(), 0U);
        }
    }
}

TEST(EnvelopeDecodeTest, AbortWhileBodyIsParkedDrainsItAndRollsBackOwner) {
    fragmented_buffer_parser input{
      fixture::fragmented(fixture::make_envelope(), 1)};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto memory = admitted_input(input, work.policy()).value();
    decoder_state state{.behavior = decoder_behavior::parked};
    state.entered.emplace();
    state.release.emplace();
    auto entered = state.entered->get_future();
    auto waiting = codec::decode_envelope<tiny_body>(
      input,
      codec::format_family::submitted_batch,
      owner_limits,
      memory,
      work,
      tiny_decoder{state},
      context);
    entered.get();
    abort.request_abort();
    EXPECT_FALSE(waiting.available());
    EXPECT_EQ(state.destroyed, 0U);
    state.release->set_value();
    expect_code(waiting.get(), errc::aborted);
    EXPECT_EQ(state.destroyed, 1U);
    EXPECT_EQ(input.bytes_consumed(), byte_count{});
    EXPECT_EQ(input.checkpoint_depth(), 0U);
}

TEST(EnvelopeDecodeTest, QueuedAbortStopsPendingHeaderWorkBeforeBodyDispatch) {
    std::string extensions(4064, '\0');
    fixture::put_u16(extensions, 0, 0x7ffe);
    fixture::put_u32(extensions, 4, 4056);
    const auto encoded = fixture::make_envelope(
      fixture::fixed_body, extensions);
    fragmented_buffer_parser input{fixture::fragmented(encoded, 16)};
    codec::limits_config config;
    config.max_work_bytes = byte_count{128};
    config.max_work_items = item_count{64};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::make(config).value(), abort};
    const auto memory = admitted_input(input, work.policy()).value();
    decoder_state state;
    // Establish the public native quota signal before queueing cancellation.
    // This watchdog is independent of the bytes and parser's semantic result.
    const auto deadline = std::chrono::steady_clock::now()
                          + std::chrono::seconds(2);
    while (!seastar::need_preempt()
           && std::chrono::steady_clock::now() < deadline) {
    }
    ASSERT_TRUE(seastar::need_preempt());
    bool observed = false;
    auto observer = seastar::yield().then([&] {
        observed = true;
        abort.request_abort();
    });
    std::optional<codec::result<tiny_body>> outcome;
    std::exception_ptr failure;
    bool pending = false;
    try {
        auto waiting = codec::decode_envelope<tiny_body>(
          input,
          codec::format_family::submitted_batch,
          owner_limits,
          memory,
          work,
          tiny_decoder{state},
          context);
        pending = !waiting.available();
        outcome.emplace(waiting.get());
    } catch (...) {
        failure = std::current_exception();
    }
    const auto during_work = observed;
    observer.get();
    if (failure) {
        std::rethrow_exception(failure);
    }
    EXPECT_TRUE(pending);
    EXPECT_TRUE(during_work);
    ASSERT_TRUE(outcome.has_value());
    expect_code(*outcome, errc::aborted);
    EXPECT_EQ(state.calls, 0U);
    EXPECT_EQ(input.bytes_consumed(), byte_count{});
    EXPECT_EQ(input.checkpoint_depth(), 0U);
}

TEST(
  EnvelopeDecodeTest, RealAllocationFailuresKeepParentAndEarlierMarksIntact) {
#if !defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    GTEST_SKIP() << "allocation failure injection is not enabled";
#else
    bool saw_failure = false;
    bool saw_success = false;
    for (std::uint64_t index = 0; index < 256; ++index) {
        fragmented_buffer_parser input{
          fixture::fragmented("p" + fixture::make_envelope(), 1)};
        ASSERT_TRUE(input.push_checkpoint().has_value());
        ASSERT_TRUE(input.skip(byte_count{1}).has_value());
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        const auto memory = admitted_input(input, work.policy()).value();
        decoder_state state;
        auto& injector = seastar::memory::local_failure_injector();
        injector.fail_after(index);
        bool threw = false;
        std::optional<codec::result<tiny_body>> outcome;
        try {
            outcome.emplace(
              codec::decode_envelope<tiny_body>(
                input,
                codec::format_family::submitted_batch,
                owner_limits,
                memory,
                work,
                tiny_decoder{state},
                context)
                .get());
        } catch (const std::bad_alloc&) {
            threw = true;
        } catch (...) {
            injector.cancel();
            throw;
        }
        const bool injected = injector.failed();
        injector.cancel();
        EXPECT_EQ(input.checkpoint_depth(), 1U);
        if (threw) {
            saw_failure = true;
            EXPECT_TRUE(injected);
            EXPECT_EQ(input.bytes_consumed(), byte_count{1});
        } else {
            ASSERT_TRUE(outcome.has_value());
            ASSERT_TRUE(outcome->has_value());
            saw_success = true;
            EXPECT_FALSE(injected);
            EXPECT_EQ(input.bytes_consumed(), byte_count{69});
        }
        ASSERT_TRUE(input.rollback().has_value());
        EXPECT_EQ(input.bytes_consumed(), byte_count{});
        if (saw_success) {
            break;
        }
        seastar::thread::maybe_yield();
    }
    EXPECT_TRUE(saw_failure);
    EXPECT_TRUE(saw_success);
#endif
}

} // namespace
