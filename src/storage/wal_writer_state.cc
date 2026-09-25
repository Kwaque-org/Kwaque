#include "src/storage/wal_writer_state.h"

#include "src/base/invariant.h"
#include "src/codec/transaction.h"

#include <algorithm>

namespace kwaque::storage::detail {
namespace {
runtime::operation_error state_error(errc code) {
    return runtime::operation_error{code, runtime::operation_kind::file};
}
} // namespace
runtime::result<wal_write_descriptor::pointer> wal_write_descriptor::make(
  workload_budget& budget,
  model::file_byte_span extent,
  codec::limits policy,
  byte_count execution_bytes) {
    if (
      extent.empty() || execution_bytes < minimum_execution_bytes
      || execution_bytes.value() > maximum_contiguous_allocation_bytes)
        return runtime::failure(state_error(errc::invalid_argument));
    // Native lw_shared control and a 16-entry FIFO chunk have fixed overhead.
    // Charge a whole chunk per node, including while a front chunk is sparse.
    auto node = budget.allocation_charge(
      byte_count{sizeof(wal_write_descriptor) + 64});
    auto chunk = budget.allocation_charge(
      byte_count{16 * sizeof(pointer) + 64});
    if (!node) return runtime::failure(node.error());
    if (!chunk) return runtime::failure(chunk.error());
    auto held = budget.try_reserve(
      byte_count{node->value() + chunk->value() + execution_bytes.value()});
    if (!held) return runtime::failure(held.error());
    return seastar::make_lw_shared<wal_write_descriptor>(
      std::move(*held), extent, policy);
}
wal_write_descriptor::wal_write_descriptor(
  workload_reservation held, model::file_byte_span extent, codec::limits policy)
  : held_(std::move(held))
  , extent_(extent)
  , work_(policy, abort_) {}
wal_write_descriptor::~wal_write_descriptor() {
    owner_.assert_current();
    KWAQUE_INVARIANT(
      invariant_id{"KQ-WAL-DESCRIPTOR-DRAINED"},
      !linked_ || notified_,
      "linked WAL descriptor destroyed before retirement");
}
seastar::future<wal_write_completion> wal_write_descriptor::observe() {
    owner_.assert_current();
    KWAQUE_INVARIANT(
      invariant_id{"KQ-WAL-OBSERVER"},
      linked_ && !observed_,
      "unlinked or duplicate WAL observer");
    observed_ = true;
    return completion_.get_future();
}
runtime::result<void> wal_write_descriptor::encoded(
  bytes::fragmented_buffer payload,
  workload_reservation held,
  bytes::allocation_charge_fn charge) {
    owner_.assert_current();
    KWAQUE_INVARIANT(
      invariant_id{"KQ-WAL-ENCODED"},
      state_ == wal_write_state::encoding,
      "WAL descriptor encoded out of order");
    auto cost = payload.allocation_cost(charge);
    if (!cost)
        return runtime::failure(state_error(
          codec::detail::allocation_cost_error(cost.error(), {}, 0).code()));
    auto total = cost->backing.checked_add(cost->descriptors);
    if (total) total = total->checked_add(cost->share_controls);
    if (
      !total || *total > held.retained_bytes()
      || payload.size() != extent_.size())
        return runtime::failure(state_error(errc::invalid_argument));
    payload_held_.emplace(std::move(held));
    payload_ = std::move(payload);
    state_ = wal_write_state::queued;
    return {};
}
runtime::result<void> wal_write_descriptor::encoded_group(
  bytes::fragmented_buffer payload, bytes::allocation_charge_fn charge) {
    owner_.assert_current();
    KWAQUE_INVARIANT(
      invariant_id{"KQ-WAL-GROUP-ENCODED"},
      state_ == wal_write_state::encoding && group_ && group_->encoding_,
      "WAL group encoding without admitted ownership");
    const auto cost = payload.allocation_cost(charge);
    if (!cost)
        return runtime::failure(state_error(
          codec::detail::allocation_cost_error(cost.error(), {}, 0).code()));
    auto total = cost->backing.checked_add(cost->descriptors);
    if (total) total = total->checked_add(cost->share_controls);
    const auto allowance = group_->input_bytes_.checked_add(
      group_->encoding_->retained_bytes());
    if (
      !total || !allowance || *total > *allowance
      || payload.size() != extent_.size()
      || payload.retained_bytes() > group_->retained_bound_
      || payload.fragment_count() > group_->fragment_bound_)
        return runtime::failure(state_error(errc::resource_exhausted));
    payload_ = std::move(payload);
    state_ = wal_write_state::queued;
    return {};
}
void wal_write_descriptor::dispatch() noexcept {
    owner_.assert_current();
    KWAQUE_INVARIANT(
      invariant_id{"KQ-WAL-DISPATCH"},
      state_ == wal_write_state::queued,
      "WAL descriptor dispatched out of order");
    state_ = wal_write_state::dispatched;
}
void wal_write_descriptor::complete(
  runtime::result<byte_count> written) noexcept {
    owner_.assert_current();
    KWAQUE_INVARIANT(
      invariant_id{"KQ-WAL-COMPLETE"},
      state_ == wal_write_state::dispatched,
      "WAL descriptor completed without dispatch");
    result_.failure.observe(written);
    if (written) {
        if (*written != extent_.size())
            result_.failure.observe(state_error(errc::io_failure));
        else
            result_.written = *written;
    }
    state_ = wal_write_state::done;
}
void wal_write_descriptor::fail(std::exception_ptr error) noexcept {
    owner_.assert_current();
    KWAQUE_INVARIANT(
      invariant_id{"KQ-WAL-FAILURE"},
      !notified_ && error,
      "invalid WAL failure after notification");
    result_.failure.observe(std::move(error));
    state_ = wal_write_state::done;
}
void wal_write_descriptor::fail(runtime::operation_error error) noexcept {
    owner_.assert_current();
    KWAQUE_INVARIANT(
      invariant_id{"KQ-WAL-FAILURE"},
      !notified_,
      "WAL failure after notification");
    result_.failure.observe(error);
    state_ = wal_write_state::done;
}
void wal_write_descriptor::notify() noexcept {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-WAL-NOTIFY"},
      state_ == wal_write_state::done && !notified_,
      "invalid WAL notification");
    notified_ = true;
    completion_.set_value(
      wal_write_completion{held_.share(), result_.failure, result_.written});
}
wal_descriptor_queue::~wal_descriptor_queue() {
    owner_.assert_current();
    KWAQUE_INVARIANT(
      invariant_id{"KQ-WAL-QUEUE-DRAINED"},
      empty(),
      "WAL descriptor queue destroyed before retirement");
}
runtime::result<void>
wal_descriptor_queue::push(wal_write_descriptor::pointer node) {
    owner_.assert_current();
    if (!node || node->linked_ || node->state() != wal_write_state::encoding)
        return runtime::failure(state_error(errc::invalid_argument));
    auto next = bytes_.checked_add(node->extent().size());
    const auto retained = node->group_ ? node->group_->retained_bound_
                                       : node->extent().size();
    auto next_retained = retained_.checked_add(retained);
    if (
      size() >= maximum_ || !next || *next > maximum_bytes_ || !next_retained
      || *next_retained > maximum_bytes_)
        return runtime::failure(state_error(errc::queue_full));
    if (!empty() && inflight_.back()->extent().end() != node->extent().begin())
        return runtime::failure(state_error(errc::wrong_context));
    // Only link after fallible container insertion. No promise has escaped yet.
    inflight_.push_back(node);
    node->linked_ = true;
    bytes_ = *next;
    retained_ = *next_retained;
    return {};
}
wal_write_descriptor::pointer wal_descriptor_queue::retire_front() noexcept {
    auto node = pop_completed_front();
    if (node) node->notify();
    return node;
}
wal_descriptor_queue::ready_gather wal_descriptor_queue::ready_prefix(
  byte_count maximum_bytes, std::size_t maximum_fragments) const noexcept {
    owner_.assert_current();
    ready_gather result;
    maximum_bytes = std::min(maximum_bytes, runtime::maximum_file_io_bytes);
    maximum_fragments = std::min(
      maximum_fragments, bytes::max_buffer_fragments);
    runtime::file_position end;
    for (const auto& node : inflight_) {
        if (node->state() != wal_write_state::queued) break;
        const auto next = result.logical.checked_add(node->payload().size());
        const auto backing = result.retained.checked_add(
          node->payload().retained_bytes());
        const auto fragments = result.fragments
                               + node->payload().fragment_count();
        if (
          !next || !backing || *next > maximum_bytes || *backing > maximum_bytes
          || fragments > maximum_fragments)
            break;
        KWAQUE_INVARIANT(
          invariant_id{"KQ-WAL-ADJACENT"},
          result.groups == 0 || node->extent().begin() == end,
          "WAL gather crossed a reservation hole");
        ++result.groups;
        result.logical = *next;
        result.retained = *backing;
        result.fragments = fragments;
        end = node->extent().end();
    }
    return result;
}
wal_write_descriptor::pointer
wal_descriptor_queue::pop_completed_front() noexcept {
    owner_.assert_current();
    if (empty() || inflight_.front()->state() != wal_write_state::done)
        return {};
    auto node = inflight_.front();
    bytes_ = byte_count{bytes_.value() - node->extent().size().value()};
    const auto retained = node->group_ ? node->group_->retained_bound_
                                       : node->extent().size();
    retained_ = byte_count{retained_.value() - retained.value()};
    inflight_.pop_front();
    return node;
}
} // namespace kwaque::storage::detail
