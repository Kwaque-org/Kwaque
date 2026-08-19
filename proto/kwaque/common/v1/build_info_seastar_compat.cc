#include "proto/kwaque/common/v1/build_info_seastar_compat.h"

#include <utility>

namespace kwaque::common::v1 {

seastar::future<BuildInfo> make_ready_build_info(BuildInfo info) {
  return seastar::make_ready_future<BuildInfo>(std::move(info));
}

} // namespace kwaque::common::v1
