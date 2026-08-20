#pragma once

#include "src/base/error.h"

#include <expected>
#include <system_error>

namespace kwaque {

template<typename T>
using result = std::expected<T, std::error_code>;

[[nodiscard]] inline std::unexpected<std::error_code>
failure(std::error_code error) noexcept {
    return std::unexpected(error);
}

[[nodiscard]] inline std::unexpected<std::error_code>
failure(errc error) noexcept {
    return failure(make_error_code(error));
}

} // namespace kwaque
