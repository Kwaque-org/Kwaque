#pragma once

#include "proto/kwaque/common/v1/build_info.pb.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace kwaque::admin {

inline constexpr std::size_t max_json_code_bytes = 64;
inline constexpr std::size_t max_json_message_bytes = 256;
inline constexpr std::size_t max_json_correlation_bytes = 128;
inline constexpr std::size_t max_json_build_field_bytes = 128;

struct json_response final {
    std::uint16_t status;
    std::string body;
};

[[nodiscard]] json_response liveness_response(bool live);
[[nodiscard]] json_response readiness_response(bool ready);

[[nodiscard]] kwaque::common::v1::BuildInfo current_build_info();

[[nodiscard]] std::string
build_info_json(const kwaque::common::v1::BuildInfo& info);

[[nodiscard]] std::string error_json(
  std::string_view code,
  std::string_view message,
  std::optional<std::string_view> correlation_id = std::nullopt);

} // namespace kwaque::admin
