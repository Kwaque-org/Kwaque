#pragma once

#include <absl/crc/crc32c.h>

#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>

namespace kwaque::codec {

// Incremental Castagnoli checksum over explicitly encoded bytes. The seed and
// value are finalized CRC values; use zero to start a new checksum.
class crc32c final {
public:
    using value_type = std::uint32_t;

    explicit constexpr crc32c(value_type seed = 0) noexcept
      : value_(seed) {}

    [[nodiscard]] constexpr value_type value() const noexcept { return value_; }

    // Native initialization can throw. Growing traversals must bound their own
    // synchronous byte work; this update neither retains nor copies the span.
    void extend(std::span<const char> data) {
        if (data.empty()) {
            return;
        }
        value_ = static_cast<value_type>(absl::ExtendCrc32c(
          absl::crc32c_t{value_}, std::string_view{data.data(), data.size()}));
    }

    // Append an already checked suffix without reading its bytes again. Both
    // CRCs use finalized values; the suffix length must describe those bytes.
    void extend_checksum(value_type suffix, std::size_t length) {
        value_ = static_cast<value_type>(absl::ConcatCrc32c(
          absl::crc32c_t{value_}, absl::crc32c_t{suffix}, length));
    }

    // Advance over known zero padding without scanning a temporary zero buffer.
    void extend_zeroes(std::size_t length) {
        value_ = static_cast<value_type>(
          absl::ExtendCrc32cByZeroes(absl::crc32c_t{value_}, length));
    }

    // An implicit character-array span would also include its terminator.
    template<std::size_t N>
    void extend(const char (&literal)[N]) = delete;

private:
    value_type value_;
};

} // namespace kwaque::codec
