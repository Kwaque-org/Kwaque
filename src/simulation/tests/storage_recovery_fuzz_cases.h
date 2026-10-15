#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>

namespace kwaque::simulation::testing {

inline constexpr std::size_t storage_recovery_fuzz_max_input = 8 + 4096;

// The one thing done to the store image before the shard is started on it.
enum class image_damage : std::uint8_t {
    none,
    // A run of bytes zeroed.
    zeros,
    // A run of bytes overwritten with the script's, or with a pattern.
    junk,
    // One bit flipped.
    bit,
    // The file cut short.
    cut,
    // The tail of one sector of 4096 bytes lost from a multiple of 512: a
    // write that was torn.
    torn,
    // A record whose header says its body is longer than it is, with both
    // checksums right for what the header says.
    overlong,
    // The same file of another object in its place.
    foreign,
    // An earlier whole version of the file in its place.
    stale,
    // Bytes that are no record after the file's end: an append that was
    // never finished.
    appended,
    // The file gone.
    removed,
};
inline constexpr std::uint8_t image_damage_count = 11;

// The eight bytes that begin an input: which file of the image, the damage,
// where in the file (low then high), how long a run or how much of a sector
// is kept, which bit, and two spare.
struct recovery_control final {
    std::uint8_t file{0}, damage{0}, low{0}, high{0};
    std::uint8_t length{0}, bit{0}, spare{0}, other{0};
};

// The bytes of a file after one damage. Pure: `foreign` and `stale` are what
// can stand in the file's place, empty where nothing can. A file that is
// removed has no bytes to give, and is given back as it was.
[[nodiscard]] std::string damaged_bytes(
  std::string bytes,
  recovery_control control,
  std::span<const std::uint8_t> script,
  std::string_view foreign,
  std::string_view stale);

// What one start on a damaged image came to.
struct recovery_seen final {
    // The damage changed a byte, and on which of the three devices.
    bool changed{false};
    std::size_t device{0};
    // The start left the shard ready.
    bool ready{false};
    bool restarted{false};
    std::uint32_t recovered{0};
    std::uint64_t unresolved{0};
    // Whether the table holds each segment as one a restart recovered.
    bool a_recovering{false}, b_recovering{false};
    // The image when the start had answered is the image it was given.
    bool untouched{false};
    std::optional<std::string> failure;
};

// What is declared of a start on the image whatever was done to it; nothing
// when it holds, else one word for the rule that broke.
//  - `baseline`: with no byte changed, the start is the one the undamaged
//    image gets: a restart, ready, both segments recovered, nothing
//    unexplained.
//  - `wrote`: a start that does not leave the shard ready changed nothing.
//  - `spared`: when one data device was damaged and the shard is ready, the
//    segment on the other data device is still held as recovered.
[[nodiscard]] std::string_view recovery_verdict(const recovery_seen&) noexcept;

struct recovery_case_result final {
    // Nothing when everything declared held; else the rule that broke,
    // `hang`, `exhausted` or `threw`.
    std::string word;
    std::string about;
    recovery_seen seen;
};

// Lays the checked-in store image out on the simulated file system, does one
// damage to one of its files, starts the shard on it and stops it. Called on
// a Seastar thread.
[[nodiscard]] recovery_case_result exercise_recovery_case(
  recovery_control control, std::span<const std::uint8_t> script);

// The fuzz entry's body: the first eight bytes are the control, the rest the
// script. Ends the process on a rule that broke.
void fuzz_recovery_case(std::span<const std::uint8_t> input);

} // namespace kwaque::simulation::testing
