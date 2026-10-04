#pragma once

#include "src/base/units.h"

#include <chrono>
#include <cstddef>
#include <cstdint>

namespace kwaque::admin {

inline constexpr std::uint64_t admin_reservation_bytes = 4_MiB;
inline constexpr std::size_t connections_per_shard = 4;
inline constexpr std::size_t request_line_bytes = 2_KiB;
inline constexpr std::size_t header_bytes = 8_KiB;
inline constexpr std::size_t header_count = 32;
inline constexpr std::chrono::seconds header_timeout{5};
inline constexpr std::chrono::seconds exchange_timeout{15};
inline constexpr std::size_t metrics_snapshot_bytes = 640_KiB;
// Snapshot caps count only enabled series, so they do not grow with the shard
// count. A broker shard registers about 300 enabled series in about 120
// families; the composition test keeps both at most three quarters of a cap.
inline constexpr std::size_t metrics_snapshot_families = 256;
inline constexpr std::size_t metrics_snapshot_series = 1024;
// Unaggregated exposition is about 18 KiB per shard and is streamed rather
// than retained, so the response cap scales with the shard count.
inline constexpr std::size_t metrics_response_bytes_per_shard = 128_KiB;
// A scrape queues for each source shard's snapshot slot for at most the
// lifetime its connection is allowed.
inline constexpr std::chrono::milliseconds metrics_snapshot_wait
  = exchange_timeout;
// A scrape retains one foreign pointer and a bounded number of iterator
// positions per shard; it submits only one cross-shard collection at a time.
inline constexpr std::size_t max_scrape_shards = 1024;
inline constexpr float scheduling_shares = 100.0F;

[[nodiscard]] constexpr std::size_t
metrics_response_bytes(unsigned shard_count) noexcept {
    return metrics_response_bytes_per_shard * shard_count;
}

} // namespace kwaque::admin
