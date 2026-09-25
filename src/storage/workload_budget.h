#ifndef KWAQUE_SRC_STORAGE_WORKLOAD_BUDGET_H_
#define KWAQUE_SRC_STORAGE_WORKLOAD_BUDGET_H_

#include "src/bytes/fragmented_buffer.h"
#include "src/resource/resource_manager.h"
#include "src/runtime/error.h"
#include "src/runtime/shard_affinity.h"

#include <seastar/core/shared_ptr.hh>

#include <cstdint>
#include <utility>

namespace kwaque::storage {

struct workload_budget_limits final {
    std::uint32_t tasks{64};
    byte_count bytes{64U * 1024U * 1024U};
    std::uint32_t handles{64};
};

struct workload_budget_snapshot final {
    std::uint64_t tasks{0}, bytes{0}, handles{0}, accepted{0}, rejected{0};
};

namespace detail {
struct workload_budget_state;
struct workload_reservation_state;
} // namespace detail

// The last alias releases admission. Queue occupancy is a separate bound and
// never substitutes for this owner. Sharing retains the same workload lease,
// including task/handle capacity, conservatively until the last output dies.
class workload_reservation final {
public:
    workload_reservation(workload_reservation&&) noexcept;
    workload_reservation& operator=(workload_reservation&&) noexcept;
    workload_reservation(const workload_reservation&) = delete;
    workload_reservation& operator=(const workload_reservation&) = delete;
    ~workload_reservation();
    [[nodiscard]] workload_reservation share() const noexcept;
    [[nodiscard]] byte_count bytes() const noexcept;
    // Requested storage allowance, excluding this reservation's own control
    // allocation. Only this amount can cover a transferred payload.
    [[nodiscard]] byte_count retained_bytes() const noexcept;
    // Acquire after the caller pins its object, before obtaining an I/O slot.
    // No wait or new allocation; failure leaves the existing reservation owned.
    [[nodiscard]] runtime::result<void>
    try_acquire_handles(std::uint32_t count);

private:
    friend class workload_budget;
    explicit workload_reservation(
      seastar::lw_shared_ptr<detail::workload_reservation_state>) noexcept;
    seastar::lw_shared_ptr<detail::workload_reservation_state> state_;
};

// Takes an already acquired startup lease, never a resource manager. Admission
// is nonwaiting: all units are obtained in order or rolled back before return.
// Caller budgets cover frames, metadata, staging and retained output, not wire
// length alone. Add object pins before handle/I/O admission; never wait while
// holding resources that the freeing path needs.
class workload_budget final : public runtime::shard_affine {
public:
    workload_budget(
      resource::workload_handle lease,
      workload_budget_limits limits,
      bytes::allocation_charge_fn charge);
    workload_budget(const workload_budget&) = delete;
    workload_budget& operator=(const workload_budget&) = delete;
    ~workload_budget();

    [[nodiscard]] runtime::result<workload_reservation>
    try_reserve(byte_count retained);
    [[nodiscard]] runtime::result<byte_count>
    allocation_charge(byte_count requested) const noexcept;
    [[nodiscard]] runtime::result<workload_reservation> try_reserve_buffer(
      const bytes::fragmented_buffer& buffer, byte_count additional = {});
    // New alias descriptor/control storage must be admitted separately before
    // constructing it. Reserve its full buffer cost conservatively when the
    // allocator cannot prove disjoint backing.
    void close_admission() noexcept;
    [[nodiscard]] workload_budget_snapshot snapshot() const noexcept;
    [[nodiscard]] seastar::scheduling_group scheduling_group() const;

private:
    seastar::lw_shared_ptr<detail::workload_budget_state> state_;
};

} // namespace kwaque::storage

#endif // KWAQUE_SRC_STORAGE_WORKLOAD_BUDGET_H_
