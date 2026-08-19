#pragma once

#include <seastar/util/log.hh>

namespace kwaque::log {

[[nodiscard]] seastar::logger &broker();

} // namespace kwaque::log
