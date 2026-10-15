#include "src/storage/tests/local_metadata_fuzz_cases.h"

#include "src/base/error.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer_parser.h"
#include "src/codec/cooperative.h"
#include "src/codec/digest.h"
#include "src/codec/limits.h"
#include "src/runtime/testing/fuzz_verdict.h"
#include "src/storage/local_metadata.h"
#include "src/storage/page_ref.h"
#include "src/storage/tests/local_metadata_fixture.h"
#include "src/storage/tests/local_metadata_fixture_cases.h"
#include "src/storage/tests/local_metadata_fixture_expectation.h"
#include "src/storage/tests/retry_test_support.h"
#include "src/storage/tests/segment_test_support.h"

#include <seastar/core/abort_source.hh>

#include <algorithm>
#include <array>
#include <iterator>
#include <optional>
#include <string>

namespace kwaque::storage::testing {
namespace {
// What a decode made of some bytes: its refusal, or how far it read and
// what the value it decoded encodes to.
struct seen final {
    std::optional<errc> error;
    std::uint64_t consumed{0};
    std::optional<errc> refused;
    std::string canonical;
    bool operator==(const seen&) const = default;
};

seen observe(
  const local_metadata_expectation& expected,
  const std::string& wire,
  std::size_t width) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    // A fixture buffer holds a thousand fragments and a few more.
    width = std::max(width, (wire.size() + 999U) / 1000U);
    bytes::fragmented_buffer_parser input{buffer(wire, width)};
    auto decoded = decode_local_metadata(
                     input,
                     expected,
                     reserve(input, work),
                     work,
                     {},
                     codec::input_boundary::complete)
                     .get();
    seen result;
    result.consumed = input.bytes_consumed().value();
    if (!decoded) {
        result.error = decoded.error().code();
        return result;
    }
    auto encoded = encode_local_metadata(
                     {expected.header,
                      expected.alignment,
                      expected.segment_alignment,
                      expected.data_metadata_alignment},
                     decoded->value.payload(),
                     work,
                     decoded->remaining.operation_remaining,
                     charge)
                     .get();
    if (encoded) {
        result.canonical = flat(encoded->bytes);
    } else {
        result.refused = encoded.error().code();
    }
    return result;
}

// Every checked-in record, read once.
const std::string& checked_in(std::size_t record) {
    static std::array<std::string, std::size(local_fixture_cases)> read;
    static std::array<bool, std::size(local_fixture_cases)> held{};
    if (!held[record]) {
        read[record] = local_fixture::read(local_fixture_cases[record].name);
        held[record] = true;
    }
    return read[record];
}

// A record's frame can be sealed: it holds a header that says where its body
// begins.
bool sealable(const std::string& wire) {
    if (wire.size() < 32) return false;
    const auto header = get(wire, 10, 2);
    return header >= 32 && header <= 4096 && header <= wire.size();
}
// What a row's record is encoded as by a writer of this version.
const std::string& canonical_of(const local_fixture_case& test) {
    for (std::size_t record = 0; record != std::size(local_fixture_cases);
         ++record)
        if (local_fixture_cases[record].name == test.canonical)
            return checked_in(record);
    static const std::string none;
    return none;
}

// The pin names the bytes as they now are.
void pin(
  const local_fixture_case& test,
  local_metadata_expectation& expected,
  const std::string& wire) {
    expected.digest = codec::immutable_object_digest{exact_digest(wire)};
    expected.encoded_bytes = byte_count{wire.size()};
    if (test.kind != 7 && test.kind != 12) return;
    const auto page = page_ref::make(
      page_ordinal::make(test.ordinal).value(),
      test.first,
      test.count,
      byte_count{wire.size()},
      *expected.digest);
    // A size no page has leaves the row's own page: the decode refuses it.
    if (page) expected.page = *page;
}
} // namespace

bool local_metadata_probe_refuses(std::string_view wire) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    bytes::fragmented_buffer_parser input{buffer(std::string{wire}, 4096)};
    return !probe_local_metadata(
              input,
              alignment(4096),
              reserve(input, work),
              work,
              {},
              codec::input_boundary::complete)
              .get();
}

std::string_view check_local_metadata_case(
  local_metadata_control control, std::span<const std::uint8_t> script) {
    const auto record = control.record % std::size(local_fixture_cases);
    const auto& test = local_fixture_cases[record];
    const auto& original = checked_in(record);
    const auto mutation = static_cast<local_metadata_mutation>(
      control.mutation % local_metadata_mutation_count);
    const auto index = std::size_t{control.low}
                       | (std::size_t{control.high} << 8U);
    const auto bit = static_cast<char>(1U << (control.bit % 8U));
    // The frame says where its body begins. The body opens with the kind
    // and the owner, whose device is thirty-six bytes in; sixty-eight in it
    // says how much is left over after the payload.
    const std::size_t body = sealable(original) ? get(original, 10, 2) : 32;
    const auto padding = body + 72 <= original.size()
                           ? std::min<std::size_t>(
                               get(original, body + 68, 4), original.size())
                           : 0;
    auto expected = local_fixture::expectation(test);
    std::string wire = original;
    bool sealed = false;
    switch (mutation) {
    case local_metadata_mutation::none:
        break;
    case local_metadata_mutation::truncated:
        wire.resize(index % wire.size());
        break;
    case local_metadata_mutation::flipped:
        wire[index % wire.size()] ^= bit;
        break;
    case local_metadata_mutation::flipped_sealed:
        wire[index % wire.size()] ^= bit;
        sealed = true;
        break;
    case local_metadata_mutation::misdirected:
        wire[std::min(body + 36, wire.size() - 1)] ^= 1;
        sealed = true;
        break;
    case local_metadata_mutation::padded:
        // The last byte, when nothing is left over.
        wire[wire.size() - 1 - (padding == 0 ? 0 : index % padding)] |= bit;
        sealed = true;
        break;
    case local_metadata_mutation::edited:
        for (std::size_t at = 0; at + 3 <= script.size() && at != 192; at += 3)
            wire
              [(std::size_t{script[at]} | (std::size_t{script[at + 1]} << 8U))
               % wire.size()] = static_cast<char>(script[at + 2]);
        sealed = true;
        break;
    case local_metadata_mutation::raw:
        wire.assign(script.begin(), script.end());
        sealed = (control.flags & 1U) != 0;
        break;
    }
    if (sealed && sealable(wire)) repair(wire);
    if (sealed || mutation == local_metadata_mutation::raw)
        pin(test, expected, wire);

    constexpr std::array<std::size_t, 4> widths{1, 7, 67, 4096};
    const auto observed = observe(
      expected, wire, widths[control.layout % widths.size()]);
    const auto other = observe(expected, wire, 4096);
    // What a decode says does not follow how its input is cut up, but for
    // where it runs out of room.
    if (observed != other) {
        const auto spent = [](const seen& one) {
            return one.error == errc::resource_exhausted
                   || one.refused == errc::resource_exhausted;
        };
        return spent(observed) || spent(other) ? std::string_view{} : "layout";
    }
    // A refusal reads nothing.
    if (observed.error && observed.consumed != 0) return "cursor";
    if (!observed.error) {
        // What a decoded value is encoded as decodes to the same thing. A
        // writer may refuse to write a form it only reads.
        if (!observed.refused) {
            auto again = expected;
            pin(test, again, observed.canonical);
            const auto second = observe(again, observed.canonical, 4096);
            if (second.error || second.canonical != observed.canonical)
                return "fixpoint";
        }
    }
    const bool listed = test.expected == errc::success;
    const bool changed = wire != original;
    switch (mutation) {
    case local_metadata_mutation::none:
        if (
          listed ? observed.error.has_value() : observed.error != test.expected)
            return "listed";
        if (!listed) break;
        if (observed.consumed != wire.size()) return "consumed";
        if (
          test.reserved_disposition
            ? observed.refused != errc::unsupported_format
            : observed.canonical != canonical_of(test))
            return "canonical";
        break;
    case local_metadata_mutation::truncated:
        if (!observed.error) return "truncated";
        break;
    case local_metadata_mutation::flipped:
        // Neither the decode nor the probe that knows no owner takes it.
        if (!observed.error) return "flipped";
        if (!local_metadata_probe_refuses(wire)) return "covered";
        break;
    case local_metadata_mutation::misdirected:
        if (!observed.error) return "misdirected";
        if (listed && observed.error != errc::wrong_context)
            return "misdirected";
        break;
    case local_metadata_mutation::padded:
        // With nothing left over the byte was the payload's, and anything
        // may come of it.
        if (!changed || padding == 0) break;
        if (!observed.error) return "padded";
        if (listed && observed.error != errc::malformed_data) return "padded";
        break;
    case local_metadata_mutation::flipped_sealed:
    case local_metadata_mutation::edited:
    case local_metadata_mutation::raw:
        break;
    }
    return {};
}

void exercise_local_metadata_case(std::span<const std::uint8_t> script) {
    if (script.size() > local_metadata_fuzz_max_input) return;
    std::array<std::uint8_t, 8> head{};
    const auto prefix = std::min(script.size(), head.size());
    std::copy_n(script.begin(), prefix, head.begin());
    const auto broke = check_local_metadata_case(
      {head[0], head[1], head[2], head[3], head[4], head[5], head[6], head[7]},
      script.subspan(prefix));
    if (!broke.empty())
        runtime::testing::fail_fuzz_verdict(
          broke,
          local_fixture_cases[head[0] % std::size(local_fixture_cases)].name);
}
} // namespace kwaque::storage::testing
