#include "src/storage/segment_data.h"

#include "src/base/invariant.h"

#include <seastar/core/coroutine.hh>

#include <utility>

namespace kwaque::storage {
namespace {
runtime::operation_error data_error(errc code) noexcept {
    return runtime::operation_error{code, runtime::operation_kind::file};
}
} // namespace

seastar::future<runtime::result<bytes::fragmented_buffer>> read_segment_data(
  runtime::file& file,
  runtime::file_position position,
  byte_count length,
  runtime::file_position bound) {
    const auto end = position.checked_add(length);
    if (length.value() == 0 || !end || *end > bound)
        co_return runtime::failure(data_error(errc::out_of_range));
    auto read = co_await file.read(position, length);
    if (!read) co_return runtime::failure(read.error());
    if (read->data().size() != length)
        co_return runtime::failure(data_error(errc::truncated_data));
    co_return std::move(*read).take_data();
}

namespace detail {
segment_data_handle::~segment_data_handle() {
    assert_current();
    KWAQUE_INVARIANT(
      invariant_id{"KQ-SEGMENT-DATA-CLOSED"},
      !file || closed,
      "segment data handle destroyed before joined close");
}
segment_data_pin segment_data_handle::pin() {
    assert_current();
    return segment_data_pin{shared_from_this(), readers.hold()};
}
} // namespace detail

segment_data_pin::segment_data_pin(segment_data_pin&&) noexcept = default;
segment_data_pin::segment_data_pin(
  seastar::lw_shared_ptr<detail::segment_data_handle> state,
  seastar::gate::holder holder) noexcept
  : state_(std::move(state))
  , holder_(std::move(holder)) {}
segment_data_pin::~segment_data_pin() {
    if (state_) state_->assert_current();
}
segment_data_pin segment_data_pin::share() const noexcept {
    state_->assert_current();
    return segment_data_pin{state_, holder_};
}
seastar::future<runtime::result<bytes::fragmented_buffer>>
segment_data_pin::read(
  runtime::file_position position,
  byte_count length,
  runtime::file_position bound) const {
    state_->assert_current();
    // Kept until the read ends, whatever becomes of this pin meanwhile.
    const auto state = state_;
    const auto holder = holder_;
    co_return co_await read_segment_data(*state->file, position, length, bound);
}

} // namespace kwaque::storage
