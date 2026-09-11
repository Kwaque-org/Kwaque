#pragma once

#include "src/base/error.h"

#include <cstdint>
#include <expected>
#include <utility>

namespace kwaque::codec {

class error final {
public:
    explicit constexpr error(
      errc code,
      std::uint16_t family = 0,
      std::uint16_t field = 0,
      std::uint64_t byte_offset = 0) noexcept
      : code_(code)
      , family_(family)
      , field_(field)
      , byte_offset_(byte_offset) {}

    [[nodiscard]] constexpr errc code() const noexcept { return code_; }
    [[nodiscard]] constexpr std::uint16_t family() const noexcept {
        return family_;
    }
    [[nodiscard]] constexpr std::uint16_t field() const noexcept {
        return field_;
    }
    [[nodiscard]] constexpr std::uint64_t byte_offset() const noexcept {
        return byte_offset_;
    }

    bool operator==(const error&) const noexcept = default;

private:
    errc code_;
    std::uint16_t family_;
    std::uint16_t field_;
    std::uint64_t byte_offset_;
};

static_assert(sizeof(error) == 16);

template<typename T>
using result = std::expected<T, error>;

[[nodiscard]] constexpr std::unexpected<error> failure(error value) noexcept {
    return std::unexpected(std::move(value));
}

} // namespace kwaque::codec
