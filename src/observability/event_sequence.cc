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

event_sequence::~event_sequence() {
    assert_current();
    KWAQUE_INVARIANT(
      invariant_id{"KQ-EVENT-SEQUENCE-RESERVATION"},
      !reserved_,
      "event sequence destroyed with an active reservation");
}

event_sequence::reservation::~reservation() {
    if (owner_ != nullptr) {
        owner_->finish(value_, false);
    }
}

event_sequence::reservation::reservation(reservation&& other) noexcept
  : owner_(std::exchange(other.owner_, nullptr))
  , value_(std::move(other.value_)) {}

void event_sequence::reservation::commit() noexcept {
    if (owner_ == nullptr) {
        return;
    }
    owner_->assert_current();
    owner_->last_sequence_ = value_.sequence();
    owner_->finish(value_, true);
    owner_ = nullptr;
}

void event_sequence::finish(const event& value, bool committed) noexcept {
    assert_current();
    reserved_ = false;
    if (!event_kind_is_rate_bounded(value.kind())) {
        return;
    }
    auto& bound = bounds_[event_rate_bound_slot(value.kind())];
    if (committed) {
        // The event carried every drop so far.
        bound.pending = 0;
    } else {
        // An event that was never emitted does not spend its kind's grant.
        bound.word.refund(event_rate_bound_interval);
    }
}

runtime::result<std::optional<event_sequence::reservation>>
event_sequence::prepare(const event_request& request) noexcept {
    assert_current();
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
    std::uint64_t suppressed = 0;
    if (event_kind_is_rate_bounded(request.kind())) {
        auto& bound = bounds_[event_rate_bound_slot(request.kind())];
        if (!bound.word
               .try_take(
                 request.monotonic(),
                 event_rate_bound_interval,
                 event_rate_bound_burst)
               .granted) {
            ++bound.pending;
            ++suppressed_total_;
            return std::optional<reservation>{};
        }
        suppressed = bound.pending;
    }
    auto value = event::from_request(
      request,
      event_shard::from_owner(owner()),
      last_sequence_ + 1U,
      suppressed);
    reserved_ = true;
    return std::optional<reservation>{reservation{*this, std::move(value)}};
}

} // namespace kwaque::observability
