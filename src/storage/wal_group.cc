#include "src/storage/wal_group.h"

#include "src/codec/transaction.h"
#include "src/runtime/file.h"
#include "src/storage/format_internal.h"

#include <seastar/core/deleter.hh>

#include <algorithm>

namespace kwaque::storage {
namespace {
runtime::operation_error group_error(errc code) {
    return runtime::operation_error{code, runtime::operation_kind::file};
}
} // namespace
runtime::result<wal_group>
wal_group::make(workload_budget& budget, std::uint32_t capacity) {
    static_assert(
      maximum_wal_group_members * sizeof(detail::wal_group_member)
      <= maximum_contiguous_allocation_bytes);
    if (capacity == 0 || capacity > maximum_wal_group_members)
        return runtime::failure(group_error(errc::invalid_argument));
    // The native vector reserves one bounded member fragment plus its outer
    // vector entry. No growth or migration is needed while filling this offer.
    auto members = budget.allocation_charge(
      byte_count{capacity * sizeof(detail::wal_group_member)});
    auto outer = budget.allocation_charge(byte_count{64});
    auto object = budget.allocation_charge(byte_count{sizeof(wal_group) + 64});
    if (!members) return runtime::failure(members.error());
    if (!outer) return runtime::failure(outer.error());
    if (!object) return runtime::failure(object.error());
    auto held = budget.try_reserve(
      byte_count{members->value() + outer->value() + object->value()});
    if (!held) return runtime::failure(held.error());
    return wal_group{std::move(*held), capacity};
}
wal_group::wal_group(workload_reservation held, std::uint32_t capacity)
  : metadata_(std::move(held))
  , capacity_(capacity) {
    members_.reserve(capacity);
}
wal_group::wal_group(wal_group&& other) noexcept
  : metadata_(std::move(other.metadata_))
  , owner_(other.owner_)
  , encoding_(std::move(other.encoding_))
  , members_(std::move(other.members_))
  , capacity_(std::exchange(other.capacity_, 0))
  , offered_bytes_(other.offered_bytes_)
  , offered_retained_(other.offered_retained_)
  , offered_fragments_(other.offered_fragments_)
  , input_bytes_(other.input_bytes_)
  , additional_bytes_(other.additional_bytes_)
  , retained_bound_(other.retained_bound_)
  , fragment_bound_(other.fragment_bound_) {}
runtime::result<void>
wal_group::append(admitted_wal_batch batch, wal_child_expectation expected) {
    owner_.assert_current();
    if (capacity_ == 0) return runtime::failure(group_error(errc::closed));
    if (members_.size() == capacity_)
        return runtime::failure(group_error(errc::queue_full));
    if (batch.batch().bytes().empty())
        return runtime::failure(group_error(errc::invalid_argument));
    const auto& bytes = batch.batch().bytes();
    const auto logical = offered_bytes_.checked_add(bytes.size());
    const auto retained = offered_retained_.checked_add(bytes.retained_bytes());
    const auto fragments = offered_fragments_ + bytes.fragment_count();
    if (
      !logical || !retained || *logical > runtime::maximum_file_io_bytes
      || *retained > runtime::maximum_file_io_bytes
      || fragments > bytes::max_buffer_fragments)
        return runtime::failure(group_error(errc::resource_exhausted));
    members_.push_back(
      detail::wal_group_member{
        std::move(batch).release(), std::move(expected), {}, {}});
    offered_bytes_ = *logical;
    offered_retained_ = *retained;
    offered_fragments_ = fragments;
    return {};
}

namespace detail {
runtime::result<wal_member_memory> wal_prepare_memory(
  const encoded_assigned_batch& child,
  aligned_envelope_layout layout,
  const codec::limits& policy,
  bytes::allocation_charge_fn charge) {
    const auto bad = [](errc code) {
        return runtime::failure(group_error(code));
    };
    if (!charge) return bad(errc::invalid_argument);
    const auto cost = child.bytes().allocation_cost(charge);
    if (!cost)
        return bad(
          codec::detail::allocation_cost_error(cost.error(), {}, 0).code());
    const auto prefix = plan_padded_prefix(
      wal_prepare_fixed_bytes, policy, charge);
    if (!prefix) return bad(prefix.error().code());
    const auto nodes = child.bytes().fragment_count() + prefix->fragments
                       + (layout.padding_bytes().value() != 0 ? 1U : 0U);
    const auto limits = policy.config();
    if (
      nodes > limits.max_buffer_fragments.value()
      || nodes > bytes::max_buffer_fragments
      || cost->largest_allocation > limits.max_allocation_bytes
      || limits.max_work_items < codec::envelope_prefix_work_items
      || limits.max_work_bytes.value() < 512)
        return bad(errc::resource_exhausted);
    auto new_backing = prefix->backing;
    if (layout.padding_bytes().value() != 0) {
        const auto request = layout.padding_bytes();
        const auto served = charge(request);
        if (served < request) return bad(errc::invalid_argument);
        if (!policy.validate_allocation(served))
            return bad(errc::resource_exhausted);
        new_backing = *new_backing.checked_add(served);
    }
    const byte_count descriptor_request{
      nodes * bytes::fragmented_buffer::fragment_descriptor_size()};
    const byte_count small_descriptor_request{
      bytes::fragmented_buffer::fragment_descriptor_size()};
    const byte_count control_request{sizeof(seastar::free_deleter_impl)};
    const auto descriptors = charge(descriptor_request);
    const auto small_descriptor = charge(small_descriptor_request);
    const auto control = charge(control_request);
    if (
      descriptors < descriptor_request
      || small_descriptor < small_descriptor_request
      || control < control_request)
        return bad(errc::invalid_argument);
    for (const auto amount : {descriptors, small_descriptor, control})
        if (!policy.validate_allocation(amount))
            return bad(errc::resource_exhausted);
    // One final descriptor array overlaps the input, private prefix owners and
    // optional padding. There are no checksum or splice aliases.
    const auto small_owners = prefix->fragments
                              + (layout.padding_bytes().value() != 0 ? 1U : 0U);
    const byte_count extra{
      new_backing.value() + descriptors.value()
      + small_owners * (small_descriptor.value() + control.value())};
    auto input = cost->backing.checked_add(cost->descriptors);
    if (input) input = input->checked_add(cost->share_controls);
    const auto total = input ? input->checked_add(extra) : std::nullopt;
    const auto retained = cost->backing.checked_add(new_backing);
    if (!input || !total || !retained) return bad(errc::out_of_range);
    if (
      *total > limits.max_operation_bytes
      || *retained > limits.max_retained_bytes
      || *retained > runtime::maximum_file_io_bytes)
        return bad(errc::resource_exhausted);
    return wal_member_memory{
      *input, extra, *total, *retained, item_count{nodes}};
}
} // namespace detail
} // namespace kwaque::storage
