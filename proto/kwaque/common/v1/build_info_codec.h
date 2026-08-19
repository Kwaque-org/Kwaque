#pragma once

#include "proto/kwaque/common/v1/build_info.pb.h"

#include <cstddef>
#include <optional>
#include <string_view>

namespace kwaque::common::v1 {

inline constexpr std::size_t max_build_info_payload_size = 4096;

[[nodiscard]] std::optional<BuildInfo>
parse_build_info(std::string_view payload);

} // namespace kwaque::common::v1
