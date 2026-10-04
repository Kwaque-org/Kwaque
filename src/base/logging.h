#pragma once

#include <seastar/util/log.hh>

// Loggers are defined at namespace scope, one per package that logs, named
// "kwaque-<package>". Static initialization registers them before the runtime
// applies --default-log-level and --logger-log-level, so those options and
// --help-loggers see every Kwaque logger. A library that defines a logger sets
// linkstatic: the logger must be a static of the executable, as Seastar's
// registry is, or it outlives the registry at exit.
namespace kwaque::log {

[[nodiscard]] seastar::logger& broker();

} // namespace kwaque::log
