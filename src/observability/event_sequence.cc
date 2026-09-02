#include "src/observability/event_sequence.h"

#include "src/base/invariant.h"

#include <limits>
#include <utility>

namespace kwaque::observability {

namespace {

[[nodiscard]] runtime::operation_error sequence_error(errc code) noexcept {
    return runtime::operation_error{
      code, runtime::operation_kind::observability};
}

} // namespace

runtime::result<event_sink_epoch>
event_sink_epoch::make(std::uint64_t value) noexcept {
    if (value == 0) {
        return runtime::failure(sequence_error(errc::invalid_argument));
    }
    return event_sink_epoch{value};
}

event_sequence::~event_sequence() {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-EVENT-SEQUENCE-RESERVATION"},
      !reserved_,
      "event sequence destroyed with an active reservation");
}

event_sequence::reservation::~reservation() {
    if (owner_ != nullptr) {
        owner_->release();
    }
}

event_sequence::reservation::reservation(reservation&& other) noexcept
  : owner_(std::exchange(other.owner_, nullptr))
  , value_(std::move(other.value_)) {}

void event_sequence::reservation::commit() noexcept {
    if (owner_ == nullptr) {
        return;
    }
    owner_->last_sequence_ = value_.sequence();
    owner_->release();
    owner_ = nullptr;
}

runtime::result<event_sequence::reservation> event_sequence::prepare(
  const event_request& request, event_shard shard) noexcept {
    if (reserved_) {
        return runtime::failure(sequence_error(errc::unavailable));
    }
    if (last_sequence_ == std::numeric_limits<std::uint64_t>::max()) {
        auto error = sequence_error(errc::out_of_range);
        static_cast<void>(error.add_context(
          runtime::operation_context_key::sequence, last_sequence_));
        static_cast<void>(error.add_context(
          runtime::operation_context_key::limit, last_sequence_));
        return runtime::failure(std::move(error));
    }
    auto value = event::from_request(request, shard, last_sequence_ + 1U);
    reserved_ = true;
    return reservation{*this, std::move(value)};
}

} // namespace kwaque::observability
