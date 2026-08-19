#pragma once

#include "proto/kwaque/common/v1/build_info.pb.h"

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace kwaque::admin {

struct json_response final {
  std::uint16_t status;
  std::string body;
};

[[nodiscard]] json_response liveness_response(bool live);
[[nodiscard]] json_response readiness_response(bool ready);

[[nodiscard]] kwaque::common::v1::BuildInfo current_build_info();

[[nodiscard]] std::string
build_info_json(const kwaque::common::v1::BuildInfo &info);

[[nodiscard]] std::string
error_json(std::string_view code, std::string_view message,
           std::optional<std::string_view> correlation_id = std::nullopt);

} // namespace kwaque::admin
