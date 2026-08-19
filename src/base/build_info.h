#pragma once

#include <string>
#include <string_view>

namespace kwaque::build_info {

[[nodiscard]] std::string_view version() noexcept;
[[nodiscard]] std::string_view git_revision() noexcept;
[[nodiscard]] bool git_dirty() noexcept;
[[nodiscard]] std::string_view build_timestamp() noexcept;
[[nodiscard]] std::string_view build_mode() noexcept;
[[nodiscard]] std::string_view compiler() noexcept;
[[nodiscard]] std::string_view protobuf_version() noexcept;
[[nodiscard]] std::string_view seastar_version() noexcept;

[[nodiscard]] std::string version_line();

} // namespace kwaque::build_info
