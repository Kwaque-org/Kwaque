#include "src/codec/tests/envelope_fuzz_cases.h"

#include "src/base/error.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/bytes/fragmented_buffer_parser.h"
#include "src/codec/cooperative.h"
#include "src/codec/envelope.h"
#include "src/codec/envelope_decode.h"
#include "src/codec/envelope_encode.h"
#include "src/codec/error.h"
#include "src/codec/format_registry.h"
#include "src/codec/header_extensions.h"
#include "src/codec/integer.h"
#include "src/codec/limits.h"
#include "src/codec/tests/envelope_decode_test_support.h"
#include "src/codec/transaction.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/util/later.hh>

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace kwaque::codec::testing {
namespace {

namespace fixture = envelope_fixture;
using bytes::fragmented_buffer;
using bytes::fragmented_buffer_parser;
using namespace std::literals;

constexpr std::size_t maximum_input = 16U * 1024U;
constexpr field_context context{.origin = 1024, .family = 1, .field = 90};
constexpr std::uint64_t object_start = context.origin + 2U;
constexpr std::array<std::uint64_t, 3> original_context{
  0x0102030405060708ULL, 0x1112131415161718ULL, 0x2122232425262728ULL};
constexpr std::uint16_t body_field_first = 100;

void require(bool condition) noexcept {
    if (!condition) {
        __builtin_trap();
    }
}

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

std::uint64_t
little(std::string_view input, std::size_t offset, std::size_t width) {
    require(offset <= input.size() && width <= input.size() - offset);
    std::uint64_t value = 0;
    for (std::size_t index = width; index != 0; --index) {
        value = value * 256U
                + static_cast<unsigned char>(input[offset + index - 1U]);
    }
    return value;
}

struct body_value final {
    std::array<std::uint64_t, 3> identity{};
    std::vector<char> payload;
    std::uint32_t padding{0};

    bool operator==(const body_value&) const = default;
};

struct case_data final {
    std::string wire;
    std::array<std::uint64_t, 3> expected_identity{original_context};
    format_family expected_family{format_family::submitted_batch};
    input_boundary boundary{input_boundary::open};
    std::size_t depth{0};
    std::uint8_t layout{0};
};

std::string extension_bytes(std::uint8_t shape) {
    const auto count = shape % 5U == 2   ? 64U
                       : shape % 5U == 4 ? 65U
                       : shape % 5U == 0 ? 0U
                                         : 1U;
    std::string result;
    for (std::uint16_t index = 0; index < count; ++index) {
        const auto offset = result.size();
        const auto size = shape % 5U == 3 ? 4056U : shape % 5U == 1 ? 3U : 0U;
        result.resize(offset + 8U + size, 'x');
        fixture::put_u16(
          result, offset, static_cast<std::uint16_t>(index + 1U));
        fixture::put_u16(result, offset + 2, 0);
        fixture::put_u32(result, offset + 4, size);
    }
    return result;
}

void mutate_wire(std::string& wire, std::uint8_t mutation, std::uint8_t index) {
    if (mutation % 32U == 1 && !wire.empty()) {
        auto& value = wire[index % wire.size()];
        value = static_cast<char>(static_cast<unsigned char>(value) ^ 0x80U);
    } else if (mutation % 32U == 2) {
        wire.resize(index % (wire.size() + 1U));
    }
    if (wire.size() < 32) {
        return;
    }
    const auto header = static_cast<std::size_t>(little(wire, 10, 2));
    const auto body = header >= 32 && header <= wire.size() ? header
                                                            : wire.size();
    switch (mutation % 32U) {
    case 3:
        fixture::put_u16(wire, 10, 31);
        break;
    case 4:
        fixture::put_u16(wire, 10, 4097);
        break;
    case 5:
        fixture::put_u32(wire, 12, 0xffffffffU);
        break;
    case 6:
        fixture::put_u16(wire, 4, 0);
        break;
    case 7:
        fixture::put_u16(wire, 4, 65535);
        break;
    case 8:
        fixture::put_u16(wire, 6, 0);
        fixture::put_u16(wire, 8, 1);
        break;
    case 9:
        fixture::put_u16(wire, 6, 2);
        fixture::put_u16(wire, 8, 2);
        break;
    case 10:
        fixture::put_u16(wire, 6, 2);
        fixture::put_u16(wire, 8, 1);
        break;
    case 11:
        fixture::put_u64(wire, 16, 1);
        break;
    case 12:
        if (header >= 40 && wire.size() >= 40) {
            fixture::put_u16(wire, 32, 0);
        }
        break;
    case 13:
    case 14:
        if (header >= 48 && wire.size() >= 48) {
            fixture::put_u16(wire, 32, 5);
            fixture::put_u32(wire, 36, 0);
            fixture::put_u16(wire, 40, mutation % 32U == 13 ? 5 : 2);
        }
        break;
    case 15:
    case 16:
        if (header >= 40 && wire.size() >= 40) {
            fixture::put_u16(wire, 34, mutation % 32U == 15 ? 2 : 1);
        }
        break;
    case 17:
        if (header >= 40 && wire.size() >= 40) {
            fixture::put_u32(wire, 36, 0xffffffffU);
        }
        break;
    case 18:
    case 19:
    case 20: {
        const auto offset = body + 8U * (mutation % 32U - 18U);
        if (offset <= wire.size() && wire.size() - offset >= 8) {
            fixture::put_u64(wire, offset, 0);
        }
        break;
    }
    case 21:
    case 22: {
        const auto offset = body + (mutation % 32U == 21 ? 24U : 28U);
        if (offset <= wire.size() && wire.size() - offset >= 4) {
            fixture::put_u32(wire, offset, 0xffffffffU);
        }
        break;
    }
    case 23:
        if (body < wire.size()) {
            wire.back() = 1;
        }
        break;
    case 24:
        if (
          wire.size() < maximum_input - 70U
          && little(wire, 12, 4) < 0xffffffffU) {
            fixture::put_u32(
              wire, 12, static_cast<std::uint32_t>(little(wire, 12, 4) + 1U));
            wire.push_back('x');
        }
        break;
    case 25:
        if (body < wire.size() && little(wire, 12, 4) != 0) {
            fixture::put_u32(
              wire, 12, static_cast<std::uint32_t>(little(wire, 12, 4) - 1U));
            wire.pop_back();
        }
        break;
    case 26:
        wire[28] = static_cast<char>(static_cast<unsigned char>(wire[28]) ^ 1U);
        break;
    case 27:
        wire[24] = static_cast<char>(static_cast<unsigned char>(wire[24]) ^ 1U);
        break;
    case 28:
        fixture::put_u16(wire, 4, 11);
        fixture::put_u16(wire, 8, 2);
        break;
    case 29:
        fixture::put_u16(wire, 8, 2);
        break;
    case 30:
        if (header >= 32 && header <= wire.size()) {
            wire.resize(header);
            fixture::put_u32(wire, 12, 0);
        }
        break;
    default:
        break;
    }
}

case_data
make_case(std::span<const std::uint8_t> input, envelope_fuzz_options options) {
    std::array<std::uint8_t, 6> control{};
    const auto count = std::min(input.size(), control.size());
    std::copy_n(input.begin(), count, control.begin());
    const auto bytes = input.subspan(count);
    case_data value;
    value.boundary = (control[0] & 8U) != 0 ? input_boundary::complete
                                            : input_boundary::open;
    value.depth
      = (static_cast<std::size_t>(control[5]) + options.checkpoint_depth) % 9U;
    value.layout = static_cast<std::uint8_t>(control[3] ^ options.layout);
    if ((control[4] & 1U) != 0) {
        value.expected_family = format_family::assigned_batch;
    }
    for (std::size_t index = 0; index < value.expected_identity.size();
         ++index) {
        if ((control[4] & (2U << index)) != 0) {
            ++value.expected_identity[index];
        }
    }
    const auto suffix = (control[0] & 16U) != 0 ? fixture::make_envelope()
                                                : std::string{"tail"};
    const auto wire_cap = maximum_input - 2U - suffix.size();
    if ((control[0] & 1U) == 0) {
        const auto length = std::min(bytes.size(), wire_cap);
        if (length != 0) {
            value.wire.assign(
              reinterpret_cast<const char*>(bytes.data()), length);
        }
    } else {
        const auto extensions = extension_bytes(control[3]);
        const auto length = std::min(
          bytes.size(), wire_cap - 64U - extensions.size() - 1U);
        std::string body{fixture::fixed_body.substr(0, 32)};
        fixture::put_u32(body, 24, static_cast<std::uint32_t>(length));
        fixture::put_u32(body, 28, 1);
        if (length != 0) {
            body.append(reinterpret_cast<const char*>(bytes.data()), length);
        }
        body.push_back('\0');
        value.wire = fixture::make_envelope(body, extensions);
    }
    mutate_wire(value.wire, control[1], control[2]);
    if (value.wire.size() >= 32) {
        const auto header = static_cast<std::size_t>(little(value.wire, 10, 2));
        const auto body = static_cast<std::size_t>(little(value.wire, 12, 4));
        if (header >= 32 && header <= value.wire.size()) {
            if ((control[0] & 4U) != 0 && body <= value.wire.size() - header) {
                fixture::put_u32(
                  value.wire,
                  24,
                  fixture::crc32c(
                    std::string_view{value.wire}.substr(header, body)));
            }
            if ((control[0] & 2U) != 0) {
                fixture::repair_header_crc(value.wire);
            }
        }
    }
    value.wire.append(suffix);
    require(value.wire.size() + 2U <= maximum_input);
    return value;
}

struct oracle_result final {
    std::optional<error> failed;
    std::optional<body_value> value;
    std::size_t used{0};
    bool callback{false};
};

error expected_error(errc code, std::uint16_t field, std::size_t offset) {
    return error{code, context.family, field, object_start + offset};
}

oracle_result oracle(const case_data& test, envelope_fuzz_options options) {
    const auto wire = std::string_view{test.wire};
    const auto fail = [](
                        errc code,
                        std::uint16_t field,
                        std::size_t offset,
                        bool callback = false) {
        return oracle_result{
          .failed = expected_error(code, field, offset), .callback = callback};
    };
    const auto short_input = test.boundary == input_boundary::open
                               ? errc::truncated_data
                               : errc::malformed_data;
    if (test.depth == 8) {
        return fail(errc::resource_exhausted, context.field, 0);
    }
    if (options.work_bytes < 128 || options.work_items < 64) {
        return fail(errc::resource_exhausted, 1, 0);
    }
    if (wire.size() < 32) {
        return fail(short_input, 1, wire.size());
    }
    if (wire.substr(0, 4) != "KQBF") {
        return fail(errc::malformed_data, 1, 0);
    }
    const auto header = static_cast<std::size_t>(little(wire, 10, 2));
    const auto body_size = static_cast<std::size_t>(little(wire, 12, 4));
    if (header < 32) {
        return fail(errc::malformed_data, 5, 10);
    }
    if (header > 4096) {
        return fail(errc::resource_exhausted, 5, 10);
    }
    if (body_size > maximum_input) {
        return fail(errc::resource_exhausted, 6, 12);
    }
    const auto total = header + body_size;
    if (total > maximum_input) {
        return fail(errc::resource_exhausted, 10, 12);
    }
    if (header > wire.size()) {
        return fail(short_input, 5, wire.size());
    }
    if (options.exhaust_operation || options.exhaust_metadata) {
        return fail(errc::resource_exhausted, context.field, 0);
    }
    auto header_input = std::string{wire.substr(0, header)};
    fixture::put_u32(header_input, 28, 0);
    if (fixture::crc32c(header_input) != little(wire, 28, 4)) {
        return fail(errc::corrupt_data, 9, 28);
    }
    const auto family = little(wire, 4, 2);
    const auto writer = little(wire, 6, 2);
    const auto minimum = little(wire, 8, 2);
    if (minimum > writer) {
        return fail(errc::malformed_data, 4, 8);
    }
    if (writer == 0) {
        return fail(errc::unsupported_format, 3, 6);
    }
    if (minimum == 0) {
        return fail(errc::unsupported_format, 4, 8);
    }
    if (family == 0) {
        return fail(errc::malformed_data, 2, 4);
    }
    if (family > 10) {
        return fail(errc::unsupported_format, 2, 4);
    }
    if (minimum > 1) {
        return fail(errc::unsupported_format, 4, 8);
    }
    if (little(wire, 16, 8) != 0) {
        return fail(errc::unsupported_format, 7, 16);
    }
    std::size_t at = 32;
    std::uint64_t previous_tag = 0;
    unsigned count = 0;
    while (at < header) {
        if (header - at < 8) {
            return fail(errc::malformed_data, 34, header);
        }
        if (count == 64) {
            return fail(errc::resource_exhausted, 33, at);
        }
        ++count;
        const auto tag = little(wire, at, 2);
        const auto flags = little(wire, at + 2, 2);
        const auto length = little(wire, at + 4, 4);
        if (tag == 0 || tag <= previous_tag) {
            return fail(errc::malformed_data, 34, at);
        }
        previous_tag = tag;
        if (flags > 1) {
            return fail(errc::unsupported_format, 35, at + 2);
        }
        if (length > header - at - 8) {
            return fail(errc::malformed_data, 37, header);
        }
        if (flags == 1) {
            return fail(errc::unsupported_format, 34, at);
        }
        at += 8U + static_cast<std::size_t>(length);
    }
    if (total > wire.size()) {
        return fail(short_input, 6, wire.size());
    }
    const auto body = wire.substr(header, body_size);
    if (fixture::crc32c(body) != little(wire, 24, 4)) {
        return fail(errc::corrupt_data, 8, 24);
    }
    if (family != static_cast<std::uint16_t>(test.expected_family)) {
        return fail(errc::wrong_context, 2, 4);
    }
    body_value value;
    for (std::size_t index = 0; index < 3; ++index) {
        if (body.size() < 8U * (index + 1U)) {
            return fail(
              errc::malformed_data,
              static_cast<std::uint16_t>(body_field_first + index),
              total,
              true);
        }
        value.identity[index] = little(body, index * 8U, 8);
    }
    for (std::size_t index = 0; index < 3; ++index) {
        if (value.identity[index] != test.expected_identity[index]) {
            return fail(
              errc::wrong_context,
              static_cast<std::uint16_t>(body_field_first + index),
              header + 8U * index,
              true);
        }
    }
    if (body.size() < 28) {
        return fail(errc::malformed_data, 103, total, true);
    }
    const auto length = static_cast<std::size_t>(little(body, 24, 4));
    if (body.size() < 32) {
        return fail(errc::malformed_data, 104, total, true);
    }
    const auto padding = static_cast<std::size_t>(little(body, 28, 4));
    if (length > maximum_input) {
        return fail(errc::resource_exhausted, 103, header + 24, true);
    }
    if (padding > 64) {
        return fail(errc::resource_exhausted, 104, header + 28, true);
    }
    if (length + padding > body.size() - 32U) {
        return fail(errc::malformed_data, 103, total, true);
    }
    for (std::size_t index = 0; index < padding; ++index) {
        if (body[32U + length + index] != 0) {
            return fail(
              errc::malformed_data, 104, header + 32U + length + index, true);
        }
    }
    if (32U + length + padding != body.size()) {
        return fail(
          errc::malformed_data, 6, header + 32U + length + padding, true);
    }
    value.payload.assign(
      body.begin() + 32,
      body.begin() + static_cast<std::ptrdiff_t>(32U + length));
    value.padding = static_cast<std::uint32_t>(padding);
    return {.value = std::move(value), .used = total, .callback = true};
}

struct observation final {
    std::size_t calls{0};
    bool destroyed{false};
};

class body_decoder final {
public:
    body_decoder(
      const std::array<std::uint64_t, 3>& expected,
      seastar::abort_source& abort,
      observation& seen,
      envelope_fuzz_options options) noexcept
      : expected_(expected)
      , abort_(abort)
      , seen_(seen)
      , options_(options) {}
    body_decoder(body_decoder&& other) noexcept
      : expected_(other.expected_)
      , abort_(other.abort_)
      , seen_(other.seen_)
      , options_(other.options_)
      , active_(true) {
        other.active_ = false;
    }
    body_decoder(const body_decoder&) = delete;
    ~body_decoder() noexcept {
        if (active_) {
            seen_.destroyed = true;
            if (options_.cleanup_abort) {
                abort_.request_abort();
            }
        }
    }

    seastar::future<result<body_value>> operator()(
      fragmented_buffer_parser& input,
      field_context origin,
      input_boundary boundary,
      decode_budget memory,
      cooperative_work& work) {
        ++seen_.calls;
        require(boundary == input_boundary::complete);
        if (options_.body_abort) {
            abort_.request_abort();
        }
        const error anchor{
          errc::success, origin.family, body_field_first, origin.origin};
        if (
          auto admitted = co_await work.admit(
            byte_count{128}, item_count{64}, anchor);
          !admitted) {
            co_return failure(admitted.error());
        }
        if (auto ready = work.poll(anchor); !ready) {
            co_return failure(ready.error());
        }
        body_value value;
        for (std::size_t index = 0; index < 3; ++index) {
            auto field = origin;
            field.field = static_cast<std::uint16_t>(body_field_first + index);
            const auto decoded = read_le<std::uint64_t>(input, field, boundary);
            if (!decoded) {
                co_return failure(decoded.error());
            }
            value.identity[index] = *decoded;
        }
        for (std::size_t index = 0; index < 3; ++index) {
            if (value.identity[index] != expected_[index]) {
                co_return failure(
                  error{
                    errc::wrong_context,
                    origin.family,
                    static_cast<std::uint16_t>(body_field_first + index),
                    origin.origin + 8U * index});
            }
        }
        auto field = origin;
        field.field = 103;
        const auto length = read_le<std::uint32_t>(input, field, boundary);
        if (!length) {
            co_return failure(length.error());
        }
        field.field = 104;
        const auto padding = read_le<std::uint32_t>(input, field, boundary);
        if (!padding) {
            co_return failure(padding.error());
        }
        if (*length > maximum_input) {
            co_return failure(
              error{
                errc::resource_exhausted,
                origin.family,
                103,
                origin.origin + 24});
        }
        if (*padding > 64) {
            co_return failure(
              error{
                errc::resource_exhausted,
                origin.family,
                104,
                origin.origin + 28});
        }
        if (
          static_cast<std::uint64_t>(*length) + *padding
          > input.bytes_remaining().value()) {
            co_return failure(
              error{
                errc::malformed_data,
                origin.family,
                103,
                origin.origin + input.total_bytes().value()});
        }
        const auto allocation = charge(byte_count{*length});
        if (
          allocation > memory.operation_remaining
          || allocation > memory.metadata_remaining
          || allocation > work.policy().config().max_allocation_bytes) {
            co_return failure(
              error{
                errc::resource_exhausted,
                origin.family,
                103,
                origin.origin + 24});
        }
        value.payload.reserve(*length);
        value.padding = *padding;
        std::array<char, 128> scratch;
        while (value.payload.size() < *length) {
            const auto count = std::min(
              {static_cast<std::size_t>(*length) - value.payload.size(),
               scratch.size(),
               static_cast<std::size_t>(work.byte_quantum().value() / 2U),
               static_cast<std::size_t>(work.item_quantum().value() / 2U)});
            require(count != 0);
            if (
              auto admitted = co_await work.admit(
                byte_count{2U * count}, item_count{2U * count}, anchor);
              !admitted) {
                co_return failure(admitted.error());
            }
            if (auto ready = work.poll(anchor); !ready) {
                co_return failure(ready.error());
            }
            require(
              input.read_to(std::span<char>{scratch}.first(count)).has_value());
            value.payload.insert(
              value.payload.end(),
              scratch.begin(),
              scratch.begin() + static_cast<std::ptrdiff_t>(count));
        }
        for (std::uint32_t index = 0; index < *padding; ++index) {
            if (
              auto admitted = co_await work.admit(
                byte_count{1}, item_count{1}, anchor);
              !admitted) {
                co_return failure(admitted.error());
            }
            if (auto ready = work.poll(anchor); !ready) {
                co_return failure(ready.error());
            }
            const auto position = origin.origin
                                  + input.bytes_consumed().value();
            const auto octet = read_le<std::uint8_t>(input, field, boundary);
            require(octet.has_value());
            if (*octet != 0) {
                co_return failure(
                  error{errc::malformed_data, origin.family, 104, position});
            }
        }
        co_return value;
    }

private:
    std::array<std::uint64_t, 3> expected_;
    seastar::abort_source& abort_;
    observation& seen_;
    envelope_fuzz_options options_;
    // Only the move into the decoder's frame arms the owned cleanup probe.
    // A temporary rejected before transfer has no owned cleanup obligation.
    bool active_{false};
};

} // namespace

void exercise_envelope_case(
  std::span<const std::uint8_t> input, envelope_fuzz_options options) {
    require(input.size() <= maximum_input);
    require(
      options.work_bytes != 0 && options.work_bytes <= 65536
      && options.work_items != 0 && options.work_items <= 256);
    require(fixture::crc32c("123456789") == 0xe3069283U);
    const auto test = make_case(input, options);
    const auto expected = oracle(test, options);
    limits_config config;
    config.max_work_bytes = byte_count{options.work_bytes};
    config.max_work_items = item_count{options.work_items};
    const auto policy = limits::make(config).value();
    const auto owned = std::string{"px"} + test.wire;
    // Small cases can split every byte; larger cases use at most 512
    // ownership descriptors at the input cap.
    const auto width = test.layout == 0 ? owned.size()
                                        : std::max<std::size_t>(
                                            1, (owned.size() + 511U) / 512U);
    fragmented_buffer_parser parser{fixture::fragmented(owned, width)};
    parser.skip(byte_count{2}).value();
    for (std::size_t depth = 0; depth < test.depth; ++depth) {
        parser.push_checkpoint().value();
    }
    // Harness/oracle vectors and native/frame storage are reserved outside
    // these residuals; the input owner is admitted once before alias creation.
    auto memory
      = reserve_decode_input(
          parser,
          policy,
          decode_budget{
            byte_count{8U * 1024U * 1024U}, byte_count{1024U * 1024U}, charge},
          context,
          test.boundary)
          .value();
    if (options.exhaust_operation) {
        memory.operation_remaining = byte_count{};
    }
    if (options.exhaust_metadata) {
        memory.metadata_remaining = byte_count{};
    }
    seastar::abort_source abort;
    cooperative_work work{policy, abort};
    if (options.initially_aborted) {
        abort.request_abort();
    }
    observation seen;
    std::optional<seastar::future<>> observer;
    if (options.queued_abort) {
        observer.emplace(
          seastar::yield().then([&abort] { abort.request_abort(); }));
    }
    std::optional<result<body_value>> actual;
    std::exception_ptr exception;
    try {
        actual.emplace(
          decode_envelope<body_value>(
            parser,
            test.expected_family,
            envelope_extent_limits{
              byte_count{maximum_input}, byte_count{maximum_input}},
            memory,
            work,
            body_decoder{test.expected_identity, abort, seen, options},
            context,
            test.boundary)
            .get());
    } catch (...) {
        exception = std::current_exception();
    }
    if (observer) {
        observer->get();
    }
    if (exception) {
        std::rethrow_exception(exception);
    }
    require(actual.has_value());
    require(seen.destroyed == !options.initially_aborted);
    const bool cancelled = !actual->has_value()
                           && actual->error().code() == errc::aborted;
    const bool body_cancel = options.body_abort && expected.callback;
    if (options.initially_aborted) {
        require(cancelled && seen.calls == 0);
        require(
          actual->error() == expected_error(errc::aborted, context.field, 0));
    } else if (cancelled) {
        require(abort.abort_requested()
          && (options.queued_abort || body_cancel || (options.cleanup_abort && !expected.failed)));
    } else {
        require(actual->has_value() == expected.value.has_value());
        require(seen.calls == static_cast<std::size_t>(expected.callback));
        if (expected.failed) {
            require(actual->error() == *expected.failed);
        } else {
            require(!options.body_abort && !options.cleanup_abort);
            require(**actual == *expected.value);
        }
    }
    if (body_cancel && !options.queued_abort && !options.initially_aborted) {
        require(cancelled);
        require(seen.calls == 1);
        require(
          actual->error()
          == expected_error(
            errc::aborted,
            body_field_first,
            static_cast<std::size_t>(little(test.wire, 10, 2))));
    }
    if (
      options.cleanup_abort && !expected.failed && !options.initially_aborted) {
        require(cancelled);
        if (!options.queued_abort && !body_cancel) {
            require(
              actual->error()
              == expected_error(errc::aborted, context.field, 0));
        }
    }
    require(seen.calls <= 1);
    if (!expected.callback) {
        require(seen.calls == 0);
    }
    const auto used = actual->has_value() ? expected.used : 0;
    require(parser.bytes_consumed() == byte_count{2U + used});
    require(parser.total_bytes() == byte_count{owned.size()});
    require(parser.checkpoint_depth() == test.depth);
    std::string remaining(parser.bytes_remaining().value(), '\0');
    require(parser.peek_to(std::span<char>{remaining}).has_value());
    require(remaining == std::string_view{test.wire}.substr(used));
    for (std::size_t depth = test.depth; depth != 0; --depth) {
        parser.rollback().value();
        require(parser.bytes_consumed() == byte_count{2});
    }
    if (
      actual->has_value() && !options.queued_abort
      && !options.initially_aborted) {
        const auto header = static_cast<std::size_t>(little(test.wire, 10, 2));
        const auto body = std::string_view{test.wire}.substr(
          header, expected.used - header);
        auto body_owner = fragmented_buffer::copy_of(body).value();
        const auto encoded = encode_envelope(
                               std::move(body_owner),
                               test.expected_family,
                               work,
                               envelope_extent_limits{
                                 byte_count{maximum_input},
                                 byte_count{maximum_input}},
                               {},
                               byte_count{2U * 1024U * 1024U},
                               charge,
                               context)
                               .get();
        require(encoded.has_value());
        require(encoded->content_equals(
          fixture::make_envelope(
            body, {}, static_cast<std::uint16_t>(test.expected_family))));
        // NOLINTNEXTLINE(bugprone-use-after-move) -- empty donor contract.
        require(body_owner.empty());
    }
}

void verify_envelope_fuzz_oracle() {
    case_data good;
    good.wire = std::string{fixture::fixed_prefix}
                + std::string{fixture::fixed_body};
    const auto valid = oracle(good, {});
    require(!valid.failed && valid.callback && valid.used == 68);
    require(
      valid.value && valid.value->identity == original_context
      && valid.value->payload == std::vector<char>{'a', 'b', 'c'}
      && valid.value->padding == 1);

    const auto rejected = [](
                            const case_data& test,
                            errc reason,
                            std::uint16_t field,
                            std::size_t offset,
                            bool callback = false) {
        const auto value = oracle(test, {});
        require(value.failed == expected_error(reason, field, offset));
        require(!value.value && value.used == 0 && value.callback == callback);
    };
    auto changed = good;
    changed.wire.resize(31);
    rejected(changed, errc::truncated_data, 1, 31);
    changed.boundary = input_boundary::complete;
    rejected(changed, errc::malformed_data, 1, 31);
    changed = good;
    changed.wire[24] = static_cast<char>(
      static_cast<unsigned char>(changed.wire[24]) ^ 1U);
    rejected(changed, errc::corrupt_data, 9, 28);
    fixture::repair_header_crc(changed.wire);
    rejected(changed, errc::corrupt_data, 8, 24);
    changed = good;
    changed.expected_family = format_family::assigned_batch;
    rejected(changed, errc::wrong_context, 2, 4);
    changed = good;
    fixture::put_u16(changed.wire, 4, 65535);
    fixture::put_u16(changed.wire, 8, 2);
    fixture::repair_header_crc(changed.wire);
    rejected(changed, errc::malformed_data, 4, 8);
    changed = good;
    ++changed.expected_identity[1];
    rejected(changed, errc::wrong_context, 101, 40, true);
    changed = good;
    changed.depth = 8;
    rejected(changed, errc::resource_exhausted, context.field, 0);
    const auto exhausted = oracle(good, {.exhaust_metadata = true});
    require(
      exhausted.failed
      == expected_error(errc::resource_exhausted, context.field, 0));
    require(!exhausted.callback);
}

} // namespace kwaque::codec::testing
