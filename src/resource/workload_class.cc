#include "src/resource/workload_class.h"

#include <array>
#include <limits>
#include <stdexcept>

namespace kwaque::resource {

namespace {

constexpr std::size_t max_metric_name_size = 32;

constexpr std::array<workload_descriptor, workload_class_count> descriptors{
  workload_descriptor{
    .classification = workload_class::foreground_protocol,
    .metric_name = "foreground_protocol",
    .scheduling_shares = 1000,
    .max_nonlocal_requests = 1024,
    .io_bandwidth_bytes_per_second = std::nullopt,
    .memory_weight = 30,
  },
  workload_descriptor{
    .classification = workload_class::replication,
    .metric_name = "replication",
    .scheduling_shares = 1000,
    .max_nonlocal_requests = 1024,
    .io_bandwidth_bytes_per_second = std::nullopt,
    .memory_weight = 24,
  },
  workload_descriptor{
    .classification = workload_class::metadata,
    .metric_name = "metadata",
    .scheduling_shares = 600,
    .max_nonlocal_requests = 512,
    .io_bandwidth_bytes_per_second = std::nullopt,
    .memory_weight = 12,
  },
  workload_descriptor{
    .classification = workload_class::repair,
    .metric_name = "repair",
    .scheduling_shares = 200,
    .max_nonlocal_requests = 128,
    .io_bandwidth_bytes_per_second = 64ULL * 1024ULL * 1024ULL,
    .memory_weight = 10,
  },
  workload_descriptor{
    .classification = workload_class::compaction,
    .metric_name = "compaction",
    .scheduling_shares = 100,
    .max_nonlocal_requests = 64,
    .io_bandwidth_bytes_per_second = 32ULL * 1024ULL * 1024ULL,
    .memory_weight = 10,
  },
  workload_descriptor{
    .classification = workload_class::offload,
    .metric_name = "offload",
    .scheduling_shares = 150,
    .max_nonlocal_requests = 64,
    .io_bandwidth_bytes_per_second = 64ULL * 1024ULL * 1024ULL,
    .memory_weight = 8,
  },
  workload_descriptor{
    .classification = workload_class::maintenance,
    .metric_name = "maintenance",
    .scheduling_shares = 100,
    .max_nonlocal_requests = 32,
    .io_bandwidth_bytes_per_second = 16ULL * 1024ULL * 1024ULL,
    .memory_weight = 6,
  },
};

consteval bool descriptors_are_valid() {
    std::uint64_t shares = 0;
    std::uint64_t memory_weights = 0;
    for (std::size_t index = 0; index < descriptors.size(); ++index) {
        const auto& descriptor = descriptors[index];
        if (
          workload_index(descriptor.classification) != index
          || descriptor.metric_name.empty()
          || descriptor.metric_name.size() > max_metric_name_size
          || descriptor.scheduling_shares == 0
          || descriptor.scheduling_shares > 1000
          || descriptor.max_nonlocal_requests == 0
          || descriptor.memory_weight == 0) {
            return false;
        }
        if (
          shares > std::numeric_limits<std::uint64_t>::max()
                     - descriptor.scheduling_shares
          || memory_weights > std::numeric_limits<std::uint64_t>::max()
                                - descriptor.memory_weight) {
            return false;
        }
        shares += descriptor.scheduling_shares;
        memory_weights += descriptor.memory_weight;
        for (std::size_t other = index + 1; other < descriptors.size();
             ++other) {
            if (descriptor.metric_name == descriptors[other].metric_name) {
                return false;
            }
        }
    }
    return shares > 0 && memory_weights == 100
           && descriptors[0].scheduling_shares
                >= descriptors[3].scheduling_shares
           && descriptors[1].scheduling_shares
                >= descriptors[3].scheduling_shares;
}

static_assert(descriptors_are_valid());

} // namespace

std::string_view to_string(workload_class classification) noexcept {
    const auto index = workload_index(classification);
    return index < descriptors.size() ? descriptors[index].metric_name
                                      : std::string_view{"unknown"};
}

const workload_descriptor& descriptor_for(workload_class classification) {
    const auto index = workload_index(classification);
    if (index >= descriptors.size()) {
        throw std::out_of_range("unknown workload class");
    }
    return descriptors[index];
}

std::span<const workload_descriptor> workload_descriptors() noexcept {
    return descriptors;
}

} // namespace kwaque::resource
