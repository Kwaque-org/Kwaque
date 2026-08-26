#include "src/resource/resource_config.h"

#include "src/base/error.h"

#include <algorithm>
#include <cstdint>
#include <stdexcept>

namespace kwaque::resource {

namespace {

constexpr std::uint64_t minimum_headroom_bytes = 16ULL * 1024ULL * 1024ULL;
constexpr std::uint64_t memory_weight_total = 100;

} // namespace

resource_config::resource_config(
  byte_count total_memory,
  byte_count reactor_headroom,
  std::array<byte_count, workload_class_count> budgets) noexcept
  : total_memory_(total_memory)
  , reactor_headroom_(reactor_headroom)
  , budgets_(budgets) {}

result<resource_config>
resource_config::from_total_memory(byte_count total_memory) noexcept {
    if (total_memory < minimum_total_memory()) {
        return failure(errc::resource_exhausted);
    }

    const byte_count headroom{std::max(
      minimum_headroom_bytes, total_memory.value() / std::uint64_t{4})};
    const auto allocatable = total_memory.checked_sub(headroom);
    if (!allocatable) {
        return failure(errc::resource_exhausted);
    }

    const std::uint64_t whole_share = allocatable->value()
                                      / memory_weight_total;
    std::uint64_t remainder = allocatable->value() % memory_weight_total;
    std::array<byte_count, workload_class_count> budgets{};
    byte_count allocated;
    for (const auto classification : all_workload_classes) {
        const auto index = workload_index(classification);
        const auto weight = descriptor_for(classification).memory_weight;
        const auto remainder_share = std::min<std::uint64_t>(remainder, weight);
        remainder -= remainder_share;
        const byte_count budget{
          whole_share * static_cast<std::uint64_t>(weight) + remainder_share};
        const auto next_allocated = allocated.checked_add(budget);
        if (!next_allocated) {
            return failure(errc::out_of_range);
        }
        allocated = *next_allocated;
        budgets[index] = budget;
    }

    const auto accounted = allocated.checked_add(headroom);
    if (!accounted || *accounted > total_memory) {
        return failure(errc::out_of_range);
    }
    return resource_config{total_memory, headroom, budgets};
}

byte_count resource_config::budget(workload_class classification) const {
    const auto index = workload_index(classification);
    if (index >= budgets_.size()) {
        throw std::out_of_range("unknown workload class");
    }
    return budgets_[index];
}

} // namespace kwaque::resource
