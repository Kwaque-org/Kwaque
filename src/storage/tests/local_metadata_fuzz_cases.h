#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>

namespace kwaque::storage::testing {
inline constexpr std::size_t local_metadata_fuzz_max_input = 16384;

// What is done to one checked-in local metadata record before it is decoded.
// Whatever is sealed has its two checksums made again and is decoded under
// a pin of the bytes it then has, so that what answers is the decoder and
// not a checksum.
enum class local_metadata_mutation : std::uint8_t {
    // Nothing: the record answers as its table row says.
    none,
    // Cut short.
    truncated,
    // One bit flipped and nothing made again.
    flipped,
    // One bit flipped, sealed.
    flipped_sealed,
    // The owner's device changed, sealed: another device's record.
    misdirected,
    // One byte of the padding set, sealed.
    padded,
    // Bytes of the script written over the record, three a byte (where, low
    // then high, and what), sealed.
    edited,
    // The script itself in the record's place, sealed when its first bit of
    // flags asks.
    raw,
};
inline constexpr std::uint8_t local_metadata_mutation_count = 8;

// The eight bytes that begin a script: the record by its place in the table,
// the layout the decoder is fed, flags, the mutation, where (low then high),
// which bit, and one spare.
struct local_metadata_control final {
    std::uint8_t record{0}, layout{0}, flags{0}, mutation{0};
    std::uint8_t low{0}, high{0}, bit{0}, spare{0};
};

// Decodes one mutation of one record under two layouts and holds it to what
// is declared of it. Nothing, when it holds; else one word for the rule that
// broke. Runs on a reactor thread.
[[nodiscard]] std::string_view check_local_metadata_case(
  local_metadata_control control, std::span<const std::uint8_t> script);

// Whether the decoder that is told nothing of a record's owner refuses these
// bytes: what a checksum that covers every bit must do to a flipped one.
[[nodiscard]] bool local_metadata_probe_refuses(std::string_view wire);

// The fuzz entry's body: the first eight bytes are the control, the rest the
// script. Ends the process on a rule that broke.
void exercise_local_metadata_case(std::span<const std::uint8_t>);
} // namespace kwaque::storage::testing
