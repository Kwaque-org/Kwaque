#pragma once

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>

#include <filesystem>

namespace kwaque::broker {

// A supplied abort source belongs to the calling shard and must outlive
// the returned future.
[[nodiscard]] seastar::future<> prepare_data_directory(
  const std::filesystem::path& path,
  const seastar::abort_source* startup_abort = nullptr,
  bool require_mount_marker = false);

} // namespace kwaque::broker
