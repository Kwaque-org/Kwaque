#include "src/storage/completion_resources.h"

#include "src/base/allocation.h"
#include "src/base/invariant.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>

namespace kwaque::storage {

runtime::result<void> completion_resource_limits::validate() const noexcept {
    const auto reject = [](errc code) {
        return runtime::failure(
          runtime::operation_error{code, runtime::operation_kind::resource});
    };
    if (
      scratch_bytes.value() == 0
      || execution_bytes.value() < sizeof(completion_resources))
        return reject(errc::invalid_argument);
    if (
      scratch_bytes.value() > maximum_contiguous_allocation_bytes
      || execution_bytes.value() > maximum_contiguous_allocation_bytes)
        return reject(errc::out_of_range);
    return {};
}

runtime::result<completion_resources> completion_resources::make(
  workload_budget& budget,
  runtime::file& file,
  completion_resource_limits limits) {
    if (auto valid = limits.validate(); !valid)
        return runtime::failure(valid.error());
    const auto geometry = file.geometry();
    if (!geometry) return runtime::failure(geometry.error());
    const auto alignment = std::max<std::uint64_t>(
      geometry->memory_alignment().value(), sizeof(void*));
    const auto size = (limits.scratch_bytes.value() + alignment - 1U)
                      & ~(alignment - 1U);
    const auto scratch_charge = budget.allocation_charge(byte_count{size});
    if (!scratch_charge) return runtime::failure(scratch_charge.error());
    const auto cost = scratch_charge->checked_add(limits.execution_bytes);
    if (!cost)
        return runtime::failure(
          runtime::operation_error{
            errc::out_of_range, runtime::operation_kind::resource});
    auto reservation = budget.try_reserve(*cost);
    if (!reservation) return runtime::failure(reservation.error());
    auto scratch = seastar::temporary_buffer<char>::aligned(
      static_cast<std::size_t>(alignment), static_cast<std::size_t>(size));
    std::fill_n(scratch.get_write(), scratch.size(), char{});
    const auto handles = reservation->try_acquire_handles(1);
    if (!handles) return runtime::failure(handles.error());
    auto metadata = file.try_reserve_metadata();
    if (!metadata) return runtime::failure(metadata.error());
    return completion_resources{
      std::move(*reservation), std::move(scratch), std::move(*metadata)};
}

completion_resources::completion_resources(
  workload_reservation reservation,
  seastar::temporary_buffer<char> scratch,
  runtime::file::metadata_reservation metadata) noexcept
  : reservation_(std::move(reservation))
  , scratch_(std::move(scratch))
  , metadata_(std::move(metadata)) {}

completion_resources::completion_resources(
  completion_resources&& other) noexcept
  : owner_(other.owner_)
  , reservation_(std::move(other.reservation_))
  , scratch_(std::move(other.scratch_))
  , metadata_(std::move(other.metadata_)) {
    owner_.assert_current();
    other.metadata_.reset();
}

completion_resources::~completion_resources() { owner_.assert_current(); }

std::span<char> completion_resources::scratch() noexcept {
    owner_.assert_current();
    return {scratch_.get_write(), scratch_.size()};
}

byte_count completion_resources::charged_bytes() const noexcept {
    owner_.assert_current();
    return reservation_.bytes();
}

runtime::file::metadata_reservation& completion_resources::metadata() {
    owner_.assert_current();
    KWAQUE_INVARIANT(
      invariant_id{"KQ-STORAGE-COMPLETION-METADATA"},
      metadata_.has_value(),
      "completion metadata reservation was released");
    return *metadata_;
}

void completion_resources::release_metadata() noexcept {
    owner_.assert_current();
    metadata_.reset();
}

} // namespace kwaque::storage
