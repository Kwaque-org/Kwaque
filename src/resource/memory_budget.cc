#include "src/resource/memory_budget.h"

#include "src/base/error.h"
#include "src/base/invariant.h"

#include <seastar/core/coroutine.hh>

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <utility>

namespace kwaque::resource {

namespace {

byte_count scaled_floor(byte_count value, std::uint64_t numerator) noexcept {
    constexpr std::uint64_t denominator = 8;
    const auto quotient = value.value() / denominator;
    const auto remainder = value.value() % denominator;
    return byte_count{
      quotient * numerator + (remainder * numerator) / denominator};
}

} // namespace

result<memory_budget_config> memory_budget_config::with_defaults(
  byte_count capacity, std::size_t max_waiters) noexcept {
    if (capacity.value() < 2) {
        return failure(errc::invalid_argument);
    }

    auto soft = scaled_floor(capacity, 4);
    auto high = scaled_floor(capacity, 7);
    if (soft.value() == 0) {
        soft = byte_count{1};
    }
    if (high <= soft) {
        high = byte_count{soft.value() + 1};
    }

    memory_budget_config config{
      .capacity = capacity,
      .soft_watermark = soft,
      .high_watermark = high,
      .max_waiters = max_waiters,
    };
    if (auto valid = config.validate(); !valid) {
        return failure(valid.error());
    }
    return config;
}

result<void> memory_budget_config::validate() const noexcept {
    if (
      capacity.value() == 0 || soft_watermark.value() == 0
      || soft_watermark >= high_watermark || high_watermark > capacity
      || capacity.value() > seastar::semaphore::max_counter()) {
        return failure(errc::invalid_argument);
    }
    return {};
}

memory_units::memory_units(
  memory_budget& budget, seastar::semaphore_units<> units) noexcept
  : budget_(&budget)
  , units_(std::move(units)) {}

memory_units::memory_units(memory_units&& other) noexcept
  : budget_(std::exchange(other.budget_, nullptr))
  , units_(std::move(other.units_)) {}

memory_units& memory_units::operator=(memory_units&& other) noexcept {
    if (this != &other) {
        static_cast<void>(release());
        budget_ = std::exchange(other.budget_, nullptr);
        units_ = std::move(other.units_);
    }
    return *this;
}

memory_units::~memory_units() { static_cast<void>(release()); }

byte_count memory_units::count() const noexcept {
    return byte_count{static_cast<std::uint64_t>(units_.count())};
}

memory_units::operator bool() const noexcept {
    return budget_ != nullptr && units_.count() != 0;
}

result<memory_units> memory_units::split(byte_count bytes) noexcept {
    if (
      budget_ == nullptr || bytes.value() == 0
      || bytes.value() > units_.count()) {
        return failure(errc::invalid_argument);
    }
    return memory_units{*budget_, units_.split(bytes.value())};
}

result<void> memory_units::merge(memory_units&& other) noexcept {
    if (
      budget_ == nullptr || other.budget_ == nullptr
      || budget_ != other.budget_) {
        return failure(errc::invalid_argument);
    }
    units_.adopt(std::move(other.units_));
    other.budget_ = nullptr;
    return {};
}

byte_count memory_units::release() noexcept {
    const auto released = count();
    if (budget_ != nullptr && released.value() != 0) {
        auto* budget = std::exchange(budget_, nullptr);
        budget->release_owned(units_);
    } else {
        budget_ = nullptr;
    }
    return released;
}

memory_budget::memory_budget(
  memory_budget_config config, memory_reclaim_trigger* reclaim_trigger)
  : config_(config)
  , admission_(validated_capacity(config_))
  , reclaim_trigger_(reclaim_trigger) {
    admission_.ensure_space_for_waiters(config_.max_waiters);
}

memory_budget::~memory_budget() {
    assert_current();
    KWAQUE_INVARIANT(
      invariant_id{"KQ-MEMORY-BUDGET-DRAINED"},
      waiting_ == 0 && used_.value() == 0 && granted_pending_.value() == 0
        && scheduled_release_.value() == 0,
      "memory budget destroyed with outstanding units or waiters");
    KWAQUE_INVARIANT(
      invariant_id{"KQ-MEMORY-BUDGET-CAPACITY"},
      admission_.current() == config_.capacity.value(),
      "memory budget semaphore did not recover its capacity");
}

void memory_budget::increment(std::uint64_t& value) {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-MEMORY-BUDGET-COUNTER"},
      value != std::numeric_limits<std::uint64_t>::max(),
      "memory budget counter overflow");
    ++value;
}

std::size_t
memory_budget::validated_capacity(const memory_budget_config& config) {
    if (auto valid = config.validate(); !valid) {
        throw std::invalid_argument("invalid memory budget configuration");
    }
    return static_cast<std::size_t>(config.capacity.value());
}

byte_count memory_budget::capacity() const {
    assert_current();
    return config_.capacity;
}

byte_count memory_budget::available() const {
    assert_current();
    return byte_count{static_cast<std::uint64_t>(admission_.current())};
}

byte_count memory_budget::used() const {
    assert_current();
    return used_;
}

byte_count memory_budget::granted_pending() const {
    assert_current();
    return granted_pending_;
}

byte_count memory_budget::committed() const {
    assert_current();
    return committed_bytes();
}

byte_count memory_budget::scheduled_release() const {
    assert_current();
    return scheduled_release_;
}

byte_count memory_budget::active() const {
    assert_current();
    const auto active = committed_bytes().checked_sub(scheduled_release_);
    KWAQUE_INVARIANT(
      invariant_id{"KQ-MEMORY-SCHEDULED-BOUNDED"},
      active.has_value(),
      "scheduled release exceeds reserved memory");
    return *active;
}

std::size_t memory_budget::waiting() const {
    assert_current();
    return waiting_;
}

std::size_t memory_budget::max_waiters() const {
    assert_current();
    return config_.max_waiters;
}

bool memory_budget::under_pressure() const {
    assert_current();
    return under_pressure_;
}

memory_budget_counters memory_budget::counters() const {
    assert_current();
    return counters_;
}

runtime::operation_error
memory_budget::admission_error(errc code, byte_count bytes) const noexcept {
    runtime::operation_error error{code, runtime::operation_kind::resource};
    static_cast<void>(
      error.add_context(runtime::operation_context_key::bytes, bytes.value()));
    return error;
}

runtime::result<void> memory_budget::validate_request(byte_count bytes) const {
    if (bytes.value() == 0) {
        return runtime::failure(admission_error(errc::invalid_argument, bytes));
    }
    if (bytes > config_.capacity) {
        return runtime::failure(
          admission_error(errc::resource_exhausted, bytes));
    }
    return {};
}

memory_units memory_budget::adopt(
  seastar::semaphore_units<> units, bool from_grant) noexcept {
    const byte_count bytes{static_cast<std::uint64_t>(units.count())};
    if (from_grant) {
        // The admission counter deducted these bytes when this waiter was
        // readied. Move them from committed-but-unowned to owned.
        const auto outstanding = granted_pending_.checked_sub(bytes);
        KWAQUE_INVARIANT(
          invariant_id{"KQ-MEMORY-GRANT-UNDERFLOW"},
          outstanding.has_value(),
          "adopted more granted bytes than the budget handed out");
        granted_pending_ = *outstanding;
    }
    const auto next = used_.checked_add(bytes);
    KWAQUE_INVARIANT(
      invariant_id{"KQ-MEMORY-BUDGET-OVERFLOW"},
      next.has_value() && *next <= config_.capacity,
      "memory admission exceeded the hard budget");
    used_ = *next;
    increment(counters_.admitted);
    update_pressure();
    return memory_units{*this, std::move(units)};
}

runtime::result<memory_units> memory_budget::try_acquire(byte_count bytes) {
    assert_current();
    if (auto valid = validate_request(bytes); !valid) {
        increment(counters_.rejected);
        return runtime::failure(std::move(valid.error()));
    }
    auto units = seastar::try_get_units(admission_, bytes.value());
    if (!units) {
        increment(counters_.rejected);
        return runtime::failure(
          admission_error(errc::resource_exhausted, bytes));
    }
    return adopt(std::move(*units), false);
}

seastar::future<runtime::result<memory_units>>
memory_budget::acquire(byte_count bytes, seastar::abort_source& abort_source) {
    assert_current();
    if (auto valid = validate_request(bytes); !valid) {
        increment(counters_.rejected);
        co_return runtime::failure(std::move(valid.error()));
    }
    if (abort_source.abort_requested()) {
        increment(counters_.rejected);
        co_return runtime::failure(admission_error(errc::aborted, bytes));
    }
    if (auto units = seastar::try_get_units(admission_, bytes.value())) {
        co_return adopt(std::move(*units), false);
    }
    if (waiting_ == config_.max_waiters) {
        increment(counters_.rejected);
        co_return runtime::failure(
          admission_error(errc::resource_exhausted, bytes));
    }

    ++waiting_;
    try {
        // try_acquire just failed with no suspension in between, so the
        // admission counter cannot satisfy this request inline. These units can
        // therefore only arrive through a release that readies this waiter and
        // records the bytes as granted-pending.
        auto units = co_await seastar::get_units(
          admission_, bytes.value(), abort_source);
        --waiting_;
        co_return adopt(std::move(units), true);
    } catch (...) {
        KWAQUE_INVARIANT(
          invariant_id{"KQ-MEMORY-WAITER-NONZERO"},
          waiting_ != 0,
          "memory waiter counter underflow");
        --waiting_;
        if (abort_source.abort_requested()) {
            increment(counters_.rejected);
            co_return runtime::failure(admission_error(errc::aborted, bytes));
        }
        throw;
    }
}

result<void> memory_budget::schedule_release(byte_count bytes) {
    assert_current();
    if (bytes.value() == 0) {
        return failure(errc::invalid_argument);
    }
    const auto unscheduled = used_.checked_sub(scheduled_release_);
    if (!unscheduled || bytes > *unscheduled) {
        return failure(errc::out_of_range);
    }
    const auto scheduled = scheduled_release_.checked_add(bytes);
    KWAQUE_INVARIANT(
      invariant_id{"KQ-MEMORY-SCHEDULE-OVERFLOW"},
      scheduled.has_value(),
      "scheduled memory release overflow");
    scheduled_release_ = *scheduled;
    update_pressure();
    return {};
}

void memory_budget::release_owned(seastar::semaphore_units<>& units) noexcept {
    assert_current();
    const byte_count bytes{static_cast<std::uint64_t>(units.count())};
    const auto remaining = used_.checked_sub(bytes);
    KWAQUE_INVARIANT(
      invariant_id{"KQ-MEMORY-RELEASE-UNDERFLOW"},
      remaining.has_value(),
      "released more memory than the budget reserved");

    // Publish the ownership transition before signaling the admission counter.
    // A newly readied waiter must never observe the released bytes as still
    // owned by this reservation when it adopts its own units.
    used_ = *remaining;
    scheduled_release_ = byte_count{
      scheduled_release_.value()
      - std::min(scheduled_release_.value(), bytes.value())};

    const byte_count available_before{
      static_cast<std::uint64_t>(admission_.current())};
    units.return_all();
    const byte_count available_after{
      static_cast<std::uint64_t>(admission_.current())};

    // Returning units readies waiters synchronously and the admission counter
    // deducts their bytes immediately, while each waiter only adopts them when
    // it resumes. Track that difference so the accounting identity below holds
    // through the window rather than only at rest.
    const auto returned = available_before.checked_add(bytes);
    KWAQUE_INVARIANT(
      invariant_id{"KQ-MEMORY-RETURN-OVERFLOW"},
      returned.has_value(),
      "returned memory exceeds the representable range");
    const auto granted = returned->checked_sub(available_after);
    KWAQUE_INVARIANT(
      invariant_id{"KQ-MEMORY-GRANT-EXCESS"},
      granted.has_value(),
      "admission counter gained units it was not given");
    const auto outstanding = granted_pending_.checked_add(*granted);
    KWAQUE_INVARIANT(
      invariant_id{"KQ-MEMORY-GRANT-OVERFLOW"},
      outstanding.has_value(),
      "granted-pending memory overflow");
    granted_pending_ = *outstanding;

    // Pressure is evaluated after the grant is recorded: bytes handed straight
    // to a waiter never became available, so this must not read as relief.
    update_pressure();

    const auto accounted = committed_bytes().checked_add(available_after);
    KWAQUE_INVARIANT(
      invariant_id{"KQ-MEMORY-BUDGET-ACCOUNTING"},
      accounted.has_value() && *accounted == config_.capacity,
      "owned, granted, and available memory do not equal capacity");
}

byte_count memory_budget::committed_bytes() const noexcept {
    const auto committed = used_.checked_add(granted_pending_);
    KWAQUE_INVARIANT(
      invariant_id{"KQ-MEMORY-COMMITTED-BOUNDED"},
      committed.has_value() && *committed <= config_.capacity,
      "committed memory exceeds the budget capacity");
    return *committed;
}

void memory_budget::update_pressure() noexcept {
    const auto active_bytes = committed_bytes().checked_sub(scheduled_release_);
    KWAQUE_INVARIANT(
      invariant_id{"KQ-MEMORY-ACTIVE-BOUNDED"},
      active_bytes.has_value(),
      "scheduled release exceeds total committed memory");

    if (!under_pressure_ && *active_bytes >= config_.high_watermark) {
        under_pressure_ = true;
        increment(counters_.high_transitions);
        if (reclaim_trigger_ != nullptr) {
            const auto target = active_bytes->checked_sub(
              config_.soft_watermark);
            KWAQUE_INVARIANT(
              invariant_id{"KQ-MEMORY-RECLAIM-TARGET"},
              target.has_value(),
              "high pressure produced an invalid reclaim target");
            reclaim_trigger_->request_reclaim(*target);
        }
    } else if (under_pressure_ && *active_bytes <= config_.soft_watermark) {
        under_pressure_ = false;
        increment(counters_.relief_transitions);
    }
}

} // namespace kwaque::resource
