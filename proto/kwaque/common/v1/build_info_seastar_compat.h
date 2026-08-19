#pragma once

#include "proto/kwaque/common/v1/build_info.pb.h"

#include <seastar/core/future.hh>

namespace kwaque::common::v1 {

[[nodiscard]] seastar::future<BuildInfo> make_ready_build_info(BuildInfo info);

} // namespace kwaque::common::v1
