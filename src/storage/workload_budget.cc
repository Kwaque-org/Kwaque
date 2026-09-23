#include "src/storage/workload_budget.h"

#include "src/base/allocation.h"
#include "src/base/invariant.h"

#include <seastar/core/semaphore.hh>

#include <initializer_list>
#include <limits>
#include <stdexcept>
#include <system_error>

namespace kwaque::storage {
namespace {
runtime::operation_error budget_error(errc code) noexcept {
    return runtime::operation_error{code, runtime::operation_kind::resource};
}

runtime::operation_error buffer_cost_error(std::error_code source) noexcept {
    for (const auto code : {errc::invalid_argument, errc::out_of_range}) {
        if (source == code) return budget_error(code);
    }
    KWAQUE_INVARIANT(
      invariant_id{"KQ-STORAGE-ALLOCATION-COST"},
      false,
      "buffer cost query returned an unexpected error category");
}
// Includes the shared allocation's control words and alignment. The profile
// still determines the allocator-served charge for this bounded request.
constexpr std::uint64_t reservation_allocation_bound = 512;
} // namespace

namespace detail {
struct workload_budget_state final : runtime::shard_affine {
    workload_budget_state(
      resource::workload_handle value,
      workload_budget_limits bounds,
      bytes::allocation_charge_fn profile)
      : lease(std::move(value))
      , limits(bounds)
      , charge(profile)
      , workload_memory(&lease.memory_admission())
      , group(lease.scheduling_group())
      , tasks(bounds.tasks)
      , local_bytes(bounds.bytes.value())
      , handles(bounds.handles) {}
    ~workload_budget_state() { assert_current(); }
    resource::workload_handle lease;
    workload_budget_limits limits;
    bytes::allocation_charge_fn charge;
    seastar::semaphore* workload_memory;
    seastar::scheduling_group group;
    seastar::semaphore tasks, local_bytes, handles;
    std::uint64_t accepted{0}, rejected{0};
    bool closed{false};
};

struct workload_reservation_state final : runtime::shard_affine {
    workload_reservation_state(
      seastar::lw_shared_ptr<workload_budget_state> owner,
      seastar::semaphore_units<> task,
      seastar::semaphore_units<> local,
      seastar::semaphore_units<> workload,
      seastar::semaphore_units<> handle,
      byte_count charged)
      : owner(std::move(owner))
      , task(std::move(task))
      , local(std::move(local))
      , workload(std::move(workload))
      , handle(std::move(handle))
      , charged(charged) {}
    ~workload_reservation_state() { assert_current(); }
    // Units die before the lease and its semaphores, even after budget
    // teardown.
    seastar::lw_shared_ptr<workload_budget_state> owner;
    seastar::semaphore_units<> task, local, workload, handle;
    byte_count charged;
};
static_assert(
  sizeof(workload_reservation_state) + 64 <= reservation_allocation_bound);
} // namespace detail

workload_reservation::workload_reservation(
  seastar::lw_shared_ptr<detail::workload_reservation_state> value) noexcept
  : state_(std::move(value)) {}
workload_reservation::workload_reservation(
  workload_reservation&& other) noexcept {
    if (other.state_) other.state_->assert_current();
    state_ = std::move(other.state_);
}
workload_reservation&
workload_reservation::operator=(workload_reservation&& other) noexcept {
    if (state_) state_->assert_current();
    if (other.state_) other.state_->assert_current();
    if (this != &other) state_ = std::move(other.state_);
    return *this;
}
workload_reservation::~workload_reservation() {
    if (state_) state_->assert_current();
}
workload_reservation workload_reservation::share() const noexcept {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-STORAGE-RESERVATION-LIVE"},
      bool(state_),
      "cannot share a moved reservation");
    state_->assert_current();
    return workload_reservation{state_};
}
byte_count workload_reservation::bytes() const noexcept {
    if (state_) state_->assert_current();
    return state_ ? state_->charged : byte_count{};
}

runtime::result<void>
workload_reservation::try_acquire_handles(std::uint32_t count) {
    if (!state_) return runtime::failure(budget_error(errc::closed));
    state_->assert_current();
    auto& owner = *state_->owner;
    const auto reject = [&owner](errc code) {
        ++owner.rejected;
        return runtime::failure(budget_error(code));
    };
    if (owner.closed) return reject(errc::closed);
    if (count == 0 || state_->handle.count() != 0)
        return reject(errc::invalid_argument);
    if (count > owner.limits.handles) return reject(errc::out_of_range);
    auto units = seastar::try_get_units(owner.handles, count);
    if (!units) return reject(errc::queue_full);
    state_->handle = std::move(*units);
    return {};
}

workload_budget::workload_budget(
  resource::workload_handle lease,
  workload_budget_limits limits,
  bytes::allocation_charge_fn charge) {
    if (
      !charge || limits.tasks == 0 || limits.tasks > 65536
      || limits.handles == 0 || limits.handles > 65536
      || limits.bytes.value() == 0 || limits.bytes > lease.hard_budget()
      || limits.bytes.value() > seastar::semaphore::max_counter())
        throw std::invalid_argument("invalid storage workload budget");
    const auto overhead = charge(byte_count{reservation_allocation_bound});
    if (
      overhead.value() < reservation_allocation_bound
      || overhead.value() > maximum_contiguous_allocation_bytes
      || overhead >= limits.bytes)
        throw std::invalid_argument("invalid storage allocation profile");
    state_ = seastar::make_lw_shared<detail::workload_budget_state>(
      std::move(lease), limits, charge);
}
workload_budget::~workload_budget() {
    assert_current();
    close_admission();
}
void workload_budget::close_admission() noexcept {
    assert_current();
    state_->closed = true;
}
seastar::scheduling_group workload_budget::scheduling_group() const {
    assert_current();
    return state_->group;
}
runtime::result<byte_count>
workload_budget::allocation_charge(byte_count requested) const noexcept {
    assert_current();
    if (
      requested.value() == 0
      || requested.value() > maximum_contiguous_allocation_bytes)
        return runtime::failure(budget_error(errc::out_of_range));
    const auto served = state_->charge(requested);
    if (
      served < requested
      || served.value() > maximum_contiguous_allocation_bytes)
        return runtime::failure(budget_error(errc::out_of_range));
    return served;
}
workload_budget_snapshot workload_budget::snapshot() const noexcept {
    assert_current();
    return {
      .tasks = state_->limits.tasks
               - static_cast<std::uint64_t>(state_->tasks.current()),
      .bytes = state_->limits.bytes.value()
               - static_cast<std::uint64_t>(state_->local_bytes.current()),
      .handles = state_->limits.handles
                 - static_cast<std::uint64_t>(state_->handles.current()),
      .accepted = state_->accepted,
      .rejected = state_->rejected};
}
runtime::result<workload_reservation>
workload_budget::try_reserve(byte_count retained) {
    assert_current();
    const auto reject = [this](errc code) {
        ++state_->rejected;
        return runtime::failure(budget_error(code));
    };
    if (state_->closed) return reject(errc::closed);
    const auto overhead = state_->charge(
      byte_count{reservation_allocation_bound});
    const auto cost = retained.checked_add(overhead);
    if (!cost || *cost > state_->limits.bytes)
        return reject(errc::out_of_range);
    auto task = seastar::try_get_units(state_->tasks, 1);
    if (!task) return reject(errc::queue_full);
    auto local = seastar::try_get_units(state_->local_bytes, cost->value());
    if (!local) return reject(errc::queue_full);
    auto workload = seastar::try_get_units(
      *state_->workload_memory, cost->value());
    if (!workload) return reject(errc::queue_full);
    auto owned = seastar::make_lw_shared<detail::workload_reservation_state>(
      state_,
      std::move(*task),
      std::move(*local),
      std::move(*workload),
      seastar::semaphore_units<>{},
      *cost);
    ++state_->accepted;
    return workload_reservation{std::move(owned)};
}
runtime::result<workload_reservation> workload_budget::try_reserve_buffer(
  const bytes::fragmented_buffer& buffer, byte_count additional) {
    assert_current();
    auto cost = buffer.allocation_cost(state_->charge);
    if (!cost) {
        ++state_->rejected;
        return runtime::failure(buffer_cost_error(cost.error()));
    }
    if (
      cost->largest_allocation.value() > maximum_contiguous_allocation_bytes) {
        ++state_->rejected;
        return runtime::failure(budget_error(errc::out_of_range));
    }
    auto total = cost->backing.checked_add(cost->descriptors);
    if (total) total = total->checked_add(cost->share_controls);
    if (total) total = total->checked_add(additional);
    if (!total) {
        ++state_->rejected;
        return runtime::failure(budget_error(errc::out_of_range));
    }
    return try_reserve(*total);
}

} // namespace kwaque::storage
