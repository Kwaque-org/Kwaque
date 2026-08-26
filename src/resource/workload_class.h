#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string_view>

namespace kwaque::resource {

enum class workload_class : std::uint8_t {
    foreground_protocol,
    replication,
    metadata,
    repair,
    compaction,
    offload,
    maintenance,
};

inline constexpr std::size_t workload_class_count = 7;
inline constexpr std::array<workload_class, workload_class_count>
  all_workload_classes{
    workload_class::foreground_protocol,
    workload_class::replication,
    workload_class::metadata,
    workload_class::repair,
    workload_class::compaction,
    workload_class::offload,
    workload_class::maintenance,
};

struct workload_descriptor final {
    workload_class classification;
    std::string_view metric_name;
    std::uint32_t scheduling_shares;
    unsigned max_nonlocal_requests;
    std::optional<std::uint64_t> io_bandwidth_bytes_per_second;
    std::uint32_t memory_weight;

    bool operator==(const workload_descriptor&) const = default;
};

[[nodiscard]] constexpr std::size_t
workload_index(workload_class classification) noexcept {
    return static_cast<std::size_t>(classification);
}

[[nodiscard]] std::string_view
to_string(workload_class classification) noexcept;
[[nodiscard]] const workload_descriptor&
descriptor_for(workload_class classification);
[[nodiscard]] std::span<const workload_descriptor>
workload_descriptors() noexcept;

} // namespace kwaque::resource
