#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>

namespace kwaque::admin {

inline constexpr std::uint64_t admin_reservation_bytes = 4U * 1024U * 1024U;
inline constexpr std::size_t connections_per_shard = 4;
inline constexpr std::size_t request_line_bytes = 2048;
inline constexpr std::size_t header_bytes = 8192;
inline constexpr std::size_t header_count = 32;
inline constexpr std::chrono::seconds header_timeout{5};
inline constexpr std::chrono::seconds exchange_timeout{15};
inline constexpr std::size_t metrics_snapshot_bytes = 640U * 1024U;
inline constexpr std::size_t metrics_response_bytes = 4U * 1024U * 1024U;
// A scrape retains one foreign pointer and a bounded number of iterator
// positions per shard; it submits only one cross-shard collection at a time.
inline constexpr std::size_t max_scrape_shards = 1024;
inline constexpr float scheduling_shares = 100.0F;

} // namespace kwaque::admin
