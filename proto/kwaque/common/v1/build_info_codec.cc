#include "proto/kwaque/common/v1/build_info_codec.h"

namespace kwaque::common::v1 {

std::optional<BuildInfo> parse_build_info(std::string_view payload) {
    if (payload.size() > max_build_info_payload_size) {
        return std::nullopt;
    }

    BuildInfo result;
    if (!result.ParseFromArray(
          payload.data(), static_cast<int>(payload.size()))) {
        return std::nullopt;
    }
    return result;
}

} // namespace kwaque::common::v1
