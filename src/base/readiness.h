#ifndef KWAQUE_SRC_BASE_READINESS_H_
#define KWAQUE_SRC_BASE_READINESS_H_

#include <array>
#include <cstdint>
#include <string_view>

namespace kwaque {

// Why a started broker withdraws its readiness while it keeps running. Each
// reason is one bit of a set; reasons are appended and never renumbered.
enum class readiness_reason : std::uint8_t {
    // A WAL rotation was refused at the WAL's ceiling.
    wal_ceiling = 1U << 0U,
    // A device has no space for new appends.
    no_space = 1U << 1U,
};

// A set of readiness reasons, one bit each.
using readiness_reasons = std::uint8_t;

inline constexpr std::array readiness_reason_values{
  readiness_reason::wal_ceiling,
  readiness_reason::no_space,
};

[[nodiscard]] constexpr readiness_reasons
readiness_bit(readiness_reason reason) noexcept {
    return static_cast<readiness_reasons>(reason);
}

[[nodiscard]] constexpr std::string_view
readiness_reason_name(readiness_reason reason) noexcept {
    switch (reason) {
    case readiness_reason::wal_ceiling:
        return "wal_ceiling";
    case readiness_reason::no_space:
        return "no_space";
    }
    return {};
}

[[nodiscard]] consteval bool readiness_reasons_are_valid() {
    readiness_reasons seen = 0;
    for (const auto reason : readiness_reason_values) {
        const auto bit = readiness_bit(reason);
        if (
          bit == 0 || (bit & (bit - 1U)) != 0 || (seen & bit) != 0
          || readiness_reason_name(reason).empty()) {
            return false;
        }
        seen |= bit;
    }
    return true;
}

static_assert(readiness_reasons_are_valid());

} // namespace kwaque

#endif // KWAQUE_SRC_BASE_READINESS_H_
