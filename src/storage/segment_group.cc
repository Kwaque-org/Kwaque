#include "src/storage/segment_group.h"

#include "src/base/invariant.h"

namespace kwaque::storage {
namespace detail {
segment_write_descriptor::segment_write_descriptor(
  workload_reservation control,
  workload_reservation work,
  seastar::lw_shared_ptr<segment_lifetime> lifetime,
  segment_captured_boundary before,
  segment_group_layout layout,
  std::vector<admitted_wal_batch::contents> children,
  codec::limits policy)
  : control(std::move(control))
  , working(std::move(work))
  , lifetime(std::move(lifetime))
  , before(std::move(before))
  , layout(std::move(layout))
  , children(std::move(children))
  , policy(policy) {}
segment_write_descriptor::~segment_write_descriptor() {
    owner.assert_current();
    KWAQUE_INVARIANT(
      invariant_id{"KQ-SEGMENT-NODE-DRAINED"},
      !linked || notified,
      "linked segment descriptor destroyed before retirement");
}
model::file_byte_span segment_write_descriptor::extent() const noexcept {
    owner.assert_current();
    return model::file_byte_span::make(
             before.end().bytes, layout.boundary().end().bytes)
      .value();
}
void segment_write_descriptor::fail(runtime::operation_error error) noexcept {
    owner.assert_current();
    failure.observe(error);
    lifetime->failure.observe(error);
    state = segment_write_state::done;
    lifetime->changed.broadcast();
}
void segment_write_descriptor::fail(std::exception_ptr error) noexcept {
    owner.assert_current();
    failure.observe(error);
    lifetime->failure.observe(error);
    state = segment_write_state::done;
    lifetime->changed.broadcast();
}
void segment_write_descriptor::abandon() noexcept {
    owner.assert_current();
    if (
      state == segment_write_state::frozen
      || state == segment_write_state::encoded)
        fail(
          runtime::operation_error{
            errc::aborted, runtime::operation_kind::file});
}
void segment_write_descriptor::notify() noexcept {
    owner.assert_current();
    KWAQUE_INVARIANT(
      invariant_id{"KQ-SEGMENT-NOTIFY"},
      linked && state == segment_write_state::done && !notified,
      "invalid segment completion notification");
    notified = true;
    if (observed)
        completion.set_value(
          segment_write_completion{
            control.share(),
            failure,
            failure.failed() ? byte_count{} : extent().size()});
}
} // namespace detail
segment_frozen_group::~segment_frozen_group() {
    if (node_) node_->abandon();
}
const segment_group_layout& segment_frozen_group::layout() const& noexcept {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-SEGMENT-HANDOFF"}, node_, "empty segment handoff");
    node_->owner.assert_current();
    return node_->layout;
}
std::span<const segment_block> segment_frozen_group::blocks() const& noexcept {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-SEGMENT-BLOCKS"},
      node_ && node_->state == detail::segment_write_state::encoded,
      "segment blocks borrowed before joined encoding");
    node_->owner.assert_current();
    return node_->blocks;
}
} // namespace kwaque::storage
