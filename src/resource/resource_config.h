#pragma once

#include "src/base/result.h"
#include "src/base/units.h"
#include "src/resource/workload_class.h"

#include <array>

namespace kwaque::resource {

struct memory_reservations final {
    // Reactor/service state and bounded task/waiter metadata that is not
    // charged to a workload. Admin-owned state is reserved separately.
    byte_count reactor_headroom;
    // Disjoint listener, connection, parser, metrics and output allowance.
    byte_count admin_memory;

    bool operator==(const memory_reservations&) const = default;
};

class resource_config final {
public:
    // The input is the memory already available to one reactor shard's
    // allocator, not process-wide memory. Headroom is retained inside that
    // amount for work not charged to a workload class; the overload lets
    // bootstrap supply an exact reservation when those consumers are known.
    [[nodiscard]] static result<resource_config>
    from_total_memory(byte_count total_memory) noexcept;
    [[nodiscard]] static result<resource_config> from_total_memory(
      byte_count total_memory, byte_count reactor_headroom) noexcept;
    [[nodiscard]] static result<resource_config> from_total_memory(
      byte_count total_memory, memory_reservations reservations) noexcept;
    // The production suitability floor and budget accounting are separate:
    // the baseline is not deducted again from the workload shares.
    [[nodiscard]] static result<resource_config> from_production_memory(
      byte_count total_memory, memory_reservations reservations) noexcept;

    [[nodiscard]] static constexpr byte_count minimum_total_memory() noexcept {
        return byte_count{64ULL * 1024ULL * 1024ULL};
    }
    [[nodiscard]] static constexpr byte_count
    default_reactor_headroom() noexcept {
        return byte_count{16ULL * 1024ULL * 1024ULL};
    }
    [[nodiscard]] static constexpr byte_count
    production_baseline_memory() noexcept {
        return byte_count{128ULL * 1024ULL * 1024ULL};
    }
    [[nodiscard]] static constexpr byte_count
    recommended_total_memory() noexcept {
        return byte_count{1024ULL * 1024ULL * 1024ULL};
    }

    [[nodiscard]] byte_count total_memory() const noexcept {
        return total_memory_;
    }
    [[nodiscard]] byte_count reactor_headroom() const noexcept {
        return reactor_headroom_;
    }
    [[nodiscard]] byte_count admin_memory_reservation() const noexcept {
        return admin_memory_reservation_;
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
      memory_reservations reservations,
      std::array<byte_count, workload_class_count> budgets) noexcept;

    byte_count total_memory_;
    byte_count reactor_headroom_;
    byte_count admin_memory_reservation_;
    std::array<byte_count, workload_class_count> budgets_;
};

} // namespace kwaque::resource
