#pragma once

#include "src/base/units.h"

#include <compare>
#include <cstdint>
#include <limits>
#include <optional>

namespace kwaque::runtime {

class file_position final {
public:
    using rep = std::uint64_t;

    constexpr file_position() noexcept = default;
    constexpr explicit file_position(rep value) noexcept
      : value_(value) {}

    [[nodiscard]] constexpr rep value() const noexcept { return value_; }

    [[nodiscard]] constexpr std::optional<file_position>
    checked_add(byte_count bytes) const noexcept {
        if (bytes.value() > std::numeric_limits<rep>::max() - value_) {
            return std::nullopt;
        }
        return file_position{value_ + bytes.value()};
    }

    auto operator<=>(const file_position&) const = default;

private:
    rep value_{0};
};

} // namespace kwaque::runtime
