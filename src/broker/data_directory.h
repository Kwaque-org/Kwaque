#pragma once

#include <seastar/core/future.hh>

#include <filesystem>

namespace kwaque::broker {

[[nodiscard]] seastar::future<>
prepare_data_directory(const std::filesystem::path& path);

} // namespace kwaque::broker
