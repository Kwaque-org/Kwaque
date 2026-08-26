#pragma once

#include "src/base/result.h"
#include "src/base/units.h"
#include "src/resource/workload_class.h"

#include <array>

namespace kwaque::resource {

class resource_config final {
public:
    // The input is one reactor shard's memory, not process-wide aggregate
    // memory. The returned budgets and headroom account for that same shard.
    [[nodiscard]] static result<resource_config>
    from_total_memory(byte_count total_memory) noexcept;

    [[nodiscard]] static constexpr byte_count minimum_total_memory() noexcept {
        return byte_count{64ULL * 1024ULL * 1024ULL};
    }

    [[nodiscard]] byte_count total_memory() const noexcept {
        return total_memory_;
    }
    [[nodiscard]] byte_count reactor_headroom() const noexcept {
        return reactor_headroom_;
    }
    [[nodiscard]] byte_count budget(workload_class classification) const;
    [[nodiscard]] const std::array<byte_count, workload_class_count>&
    budgets() const noexcept {
        return budgets_;
    }

    bool operator==(const resource_config&) const = default;

private:
    resource_config(
      byte_count total_memory,
      byte_count reactor_headroom,
      std::array<byte_count, workload_class_count> budgets) noexcept;

    byte_count total_memory_;
    byte_count reactor_headroom_;
    std::array<byte_count, workload_class_count> budgets_;
};

} // namespace kwaque::resource
