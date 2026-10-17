#pragma once

#include <seastar/core/future.hh>

#include <string_view>

namespace kwaque::broker {

// Opens the directory, flushes it and closes it checked, after a failed flush
// too: throws the first failure of the three. The flush covers the directory
// opened. Used in place of seastar::sync_directory, whose close drops its
// error.
[[nodiscard]] seastar::future<> sync_directory_checked(std::string_view path);

} // namespace kwaque::broker
