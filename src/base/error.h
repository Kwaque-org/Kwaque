#pragma once

#include <system_error>
#include <type_traits>

namespace kwaque {

enum class errc {
  success = 0,
  invalid_argument = 1,
  out_of_range = 2,
  malformed_data = 3,
  unavailable = 4,
};

[[nodiscard]] const std::error_category &error_category() noexcept;
[[nodiscard]] std::error_code make_error_code(errc error) noexcept;

} // namespace kwaque

template <> struct std::is_error_code_enum<kwaque::errc> : std::true_type {};
