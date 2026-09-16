#include "src/model/tests/checkpoint_fuzz_cases.h"

#include "src/bytes/test_allocation_profile.h"
#include "src/model/checkpoint_codec.h"
#include "src/model/tests/checkpoint_test_support.h"

#include <seastar/core/abort_source.hh>

#include <algorithm>
#include <array>
#include <string>
#include <utility>
#include <vector>

namespace kwaque::model::testing {
namespace {
namespace fixture = checkpoint_fixture;
using bytes::fragmented_buffer_parser;
using bytes::testing::charge;
using source_type = seastar::chunked_fifo<range_cursor, 16>;
constexpr unsigned complete_input = 1U;
constexpr unsigned wrong_expected_topic = 2U;
constexpr unsigned nil_expected_topic = 4U;
constexpr unsigned initial_abort = 8U;
constexpr unsigned deny_operation_memory = 16U;
constexpr unsigned deny_metadata_memory = 32U;
constexpr unsigned deny_work = 64U;
constexpr unsigned deny_heap_work = 128U;
constexpr unsigned decode_denials = initial_abort | deny_operation_memory
                                    | deny_metadata_memory | deny_work;
constexpr unsigned construction_denials = deny_operation_memory
                                          | deny_metadata_memory
                                          | deny_heap_work;
void require(bool condition) {
    if (!condition) __builtin_trap();
}
codec::decode_budget memory() {
    return {byte_count{32U << 20U}, byte_count{1U << 20U}, charge};
}

// These controls force rejection. Keep their entry-point precedence explicit
// instead of accepting any resource/cancellation error from a restricted run.
constexpr bool matches_decode(
  errc actual,
  errc reference,
  unsigned flags,
  unsigned depth,
  bool restricted) noexcept {
    if ((flags & nil_expected_topic) != 0)
        return actual == errc::invalid_argument;
    const bool denied = restricted && (flags & decode_denials) != 0;
    if (denied && (flags & initial_abort) != 0) return actual == errc::aborted;
    if (depth == 8 || (denied && (flags & deny_work) != 0))
        return actual == errc::resource_exhausted;
    if (denied) {
        // Malformed framing can reject before memory admission. Either path
        // must fail, and a memory limit cannot manufacture cancellation.
        return actual != errc::success && actual != errc::aborted
               && (actual == errc::resource_exhausted || actual == reference);
    }
    return actual == reference;
}

constexpr errc construction_error(unsigned flags, bool duplicate) noexcept {
    if ((flags & deny_work) != 0) return errc::resource_exhausted;
    if ((flags & initial_abort) != 0) return errc::aborted;
    if ((flags & nil_expected_topic) != 0) return errc::invalid_argument;
    if ((flags & construction_denials) != 0) return errc::resource_exhausted;
    return duplicate ? errc::malformed_data : errc::success;
}

// Reject deliberately incorrect observations even when the production codec
// is correct, so the oracle cannot silently lose its fault checks again.
static_assert(!matches_decode(errc::success, errc::success, 8, 0, true));
static_assert(!matches_decode(errc::success, errc::success, 64, 0, true));
static_assert(!matches_decode(errc::aborted, errc::success, 16, 0, true));
static_assert(
  !matches_decode(errc::resource_exhausted, errc::success, 8, 8, true));
static_assert(
  matches_decode(errc::invalid_argument, errc::invalid_argument, 12, 8, true));
static_assert(construction_error(76, true) == errc::resource_exhausted);
static_assert(construction_error(28, true) == errc::aborted);
static_assert(construction_error(20, true) == errc::invalid_argument);

void mutate(std::string& wire, unsigned mutation, unsigned operand) {
    auto header = static_cast<std::size_t>(fixture::little(wire, 10, 2));
    const auto count = fixture::little(wire, header + 16U, 4);
    const auto first = header + 20U;
    if (mutation == 0) return;
    if (mutation == 1) {
        wire.resize(operand % wire.size());
        return;
    }
    if (mutation == 2) {
        wire[operand % wire.size()] ^= 1;
        return;
    }
    switch (mutation) {
    case 3:
        fixture::put(wire, 4, 0, 2);
        break;
    case 4:
        fixture::put(wire, 4, 99, 2);
        break;
    case 5:
        fixture::put(wire, 6, 2, 2);
        break;
    case 6:
        fixture::put(wire, 6, 2, 2);
        fixture::put(wire, 8, 2, 2);
        break;
    case 7:
        std::fill_n(
          wire.begin() + static_cast<std::ptrdiff_t>(header), 16, '\0');
        break;
    case 8:
        wire[header] ^= 1;
        break;
    case 9:
        fixture::put(wire, header + 16U, 0, 4);
        break;
    case 10:
        fixture::put(wire, header + 16U, 4097, 4);
        break;
    case 11:
        if (count > 1) wire.replace(first + 24U, 16, wire.substr(first, 16));
        break;
    case 12:
        if (count > 1) {
            const auto a = wire.substr(first, 24);
            const auto b = wire.substr(first + 24U, 24);
            wire.replace(first, 24, b);
            wire.replace(first + 24U, 24, a);
        }
        break;
    case 13:
        std::fill_n(
          wire.begin() + static_cast<std::ptrdiff_t>(first), 16, '\0');
        break;
    case 14:
        fixture::put(wire, first + 24U * (count - 1U) + 16U, UINT64_MAX, 8);
        break;
    case 15:
        if (header == 32) {
            wire = fixture::extended(std::move(wire), 41);
            header = 41;
        }
        fixture::put(wire, 34, 1, 2);
        break;
    case 16:
        fixture::put(wire, 12, wire.size() - header + 1U, 4);
        break;
    case 17: {
        const auto next = wire;
        wire.reserve(wire.size() + next.size());
        wire += next;
        return;
    }
    case 18:
        wire.push_back('\0');
        fixture::put(wire, 12, wire.size() - header, 4);
        break;
    case 19:
        fixture::put(wire, 16, 1, 8);
        break;
    default:
        __builtin_trap();
    }
    fixture::repair(wire, header);
}

void decode_case(
  std::string_view wire,
  std::size_t width,
  unsigned flags,
  unsigned depth,
  bool restricted) {
    const auto full = "p" + std::string{wire};
    fragmented_buffer_parser input{
      fixture::fragmented(full, std::max(width, (full.size() + 511U) / 512U))};
    input.skip(byte_count{1}).value();
    for (unsigned i = 0; i < depth; ++i)
        input.push_checkpoint().value();
    auto expected = (flags & wrong_expected_topic) != 0
                      ? fixture::object<topic_id>(2)
                      : fixture::topic();
    if ((flags & nil_expected_topic) != 0) expected = {};
    const bool complete = (flags & complete_input) != 0;
    const auto reference = fixture::probe(wire, expected, complete);
    auto config = codec::limits::defaults().config();
    if (restricted && (flags & deny_work) != 0)
        config.max_work_items = item_count{1};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::make(config).value(), abort};
    auto budget
      = codec::reserve_decode_input(input, work.policy(), memory()).value();
    if (restricted && (flags & initial_abort) != 0) abort.request_abort();
    if (restricted && (flags & deny_operation_memory) != 0)
        budget.operation_remaining = {};
    if (restricted && (flags & deny_metadata_memory) != 0)
        budget.metadata_remaining = {};
    constexpr codec::field_context context{.origin = 71, .family = 10};
    auto decoded = decode_read_checkpoint(
                     input,
                     expected,
                     budget,
                     work,
                     context,
                     complete ? codec::input_boundary::complete
                              : codec::input_boundary::open)
                     .get();
    require(input.checkpoint_depth() == depth);
    require(matches_decode(
      decoded ? errc::success : decoded.error().code(),
      reference.error,
      flags,
      depth,
      restricted));
    if (!decoded) {
        const auto error = decoded.error();
        require(input.bytes_consumed() == byte_count{1});
        require(
          error.family() == 10 && error.byte_offset() >= 71
          && error.byte_offset() <= 71U + full.size());
    } else {
        require(input.bytes_consumed().value() == reference.used + 1U);
        const auto body = fixture::body_from_value(decoded->value);
        require(
          body
          == wire.substr(reference.header, reference.used - reference.header));
        require(decoded->fingerprint == fixture::digest(body));
        const auto retained = charge(
          byte_count{
            decoded->value.cursor_capacity().value() * sizeof(range_cursor)});
        require(
          decoded->remaining.operation_remaining
          == budget.operation_remaining.checked_sub(retained).value());
        require(
          decoded->remaining.metadata_remaining
          == budget.metadata_remaining.checked_sub(retained).value());
        seastar::abort_source verify_abort;
        codec::cooperative_work verify{codec::limits::defaults(), verify_abort};
        auto encoded = encode_read_checkpoint(
                         decoded->value,
                         verify,
                         decoded->remaining.operation_remaining,
                         charge)
                         .get();
        require(encoded.has_value());
        require(
          fixture::flatten(encoded->bytes).substr(32) == body
          && encoded->fingerprint == decoded->fingerprint);
    }
    const auto used = decoded ? reference.used : 0U;
    std::string remaining(wire.size() - used, '\0');
    require(input.peek_to(std::span<char>{remaining}).has_value());
    require(remaining == wire.substr(used));
}

void construction_case(const std::array<unsigned, 8>& control) {
    const auto count = 1U + control[2] % 129U;
    std::vector<range_cursor> expected;
    expected.reserve(count);
    source_type source;
    for (unsigned i = 0; i < count; ++i) {
        auto value = fixture::numbered(1U + (i + control[7]) % count);
        if (i + 1U == count && count > 1 && control[1] % 3U != 0) {
            value = expected.front();
            if (control[1] % 3U == 2)
                value = range_cursor::make(
                          value.range(), range_logical_end{UINT64_MAX})
                          .value();
        }
        source.push_back(value);
        expected.push_back(value);
    }
    const bool duplicate = count > 1 && control[1] % 3U != 0;
    const auto flags = control[5];
    auto target = (flags & nil_expected_topic) != 0 ? topic_id{}
                                                    : fixture::topic();
    seastar::abort_source abort;
    auto config = codec::limits::defaults().config();
    if ((flags & deny_heap_work) != 0) config.max_work_items = item_count{8};
    if ((flags & deny_work) != 0) config.max_work_items = item_count{1};
    codec::cooperative_work work{codec::limits::make(config).value(), abort};
    auto budget = memory();
    if ((flags & initial_abort) != 0) abort.request_abort();
    if ((flags & deny_operation_memory) != 0) budget.operation_remaining = {};
    if ((flags & deny_metadata_memory) != 0) budget.metadata_remaining = {};
    const auto result = make_read_checkpoint_from_unordered(
                          target, std::move(source), budget, work)
                          .get();
    require(
      (result ? errc::success : result.error().code())
      == construction_error(flags, duplicate));
    if (result) {
        std::ranges::sort(expected, range_cursor_less{});
        require(std::ranges::equal(expected, result->value.cursors()));
        const auto retained = charge(
          byte_count{
            result->value.cursor_capacity().value() * sizeof(range_cursor)});
        require(
          result->remaining.operation_remaining
          == budget.operation_remaining.checked_sub(retained).value());
        require(
          result->remaining.metadata_remaining
          == budget.metadata_remaining.checked_sub(retained).value());
    }
    // Impossible cleanup quanta reject before transfer. Once cleanup is
    // possible, entered failures consume even an insufficient heap quantum.
    // NOLINTBEGIN(bugprone-use-after-move)
    if ((flags & deny_work) != 0) {
        require(source.size() == count);
        std::size_t i = 0;
        for (const auto& value : source)
            require(value == expected[i++]);
    } else
        require(source.empty());
    // NOLINTEND(bugprone-use-after-move)
}
} // namespace

void exercise_checkpoint_case(std::span<const std::uint8_t> input) {
    if (input.size() > checkpoint_fuzz_max_input) return;
    std::array<unsigned, 8> control{};
    for (auto& value : control) {
        if (!input.empty()) {
            value = input.front();
            input = input.subspan(1);
        }
    }
    if (control[0] % 3U == 2) {
        construction_case(control);
        return;
    }
    constexpr std::array<std::size_t, 3> headers{32, 41, 4096};
    std::string wire;
    if (control[0] % 3U == 0) {
        wire.assign(input.begin(), input.end());
    } else {
        wire = fixture::wire(
          1U + control[2] % 32U, headers[control[3] % headers.size()]);
        mutate(wire, control[1] % 20U, control[7]);
    }
    constexpr std::array<std::size_t, 4> widths{1, 7, 67, 4096};
    for (const auto width :
         {widths[control[4] % widths.size()], std::size_t{4096}}) {
        decode_case(wire, width, control[5], control[6] % 9U, false);
        if ((control[5] & decode_denials) != 0)
            decode_case(wire, width, control[5], control[6] % 9U, true);
    }
}
} // namespace kwaque::model::testing
