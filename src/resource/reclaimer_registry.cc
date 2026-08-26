#include "src/resource/reclaimer_registry.h"

#include "src/base/error.h"
#include "src/base/invariant.h"

#include <seastar/core/metrics.hh>
#include <seastar/util/defer.hh>

#include <algorithm>
#include <exception>
#include <limits>
#include <stdexcept>
#include <utility>
#include <vector>

namespace kwaque::resource {

namespace {

thread_local reclaimer_registry* active_registry = nullptr;

} // namespace

reclaimer_registration::reclaimer_registration(
  reclaimer_registry& registry, std::uint64_t identifier) noexcept
  : registry_(&registry)
  , identifier_(identifier) {}

reclaimer_registration::reclaimer_registration(
  reclaimer_registration&& other) noexcept
  : registry_(std::exchange(other.registry_, nullptr))
  , identifier_(std::exchange(other.identifier_, 0)) {}

reclaimer_registration&
reclaimer_registration::operator=(reclaimer_registration&& other) noexcept {
    if (this != &other) {
        reset();
        registry_ = std::exchange(other.registry_, nullptr);
        identifier_ = std::exchange(other.identifier_, 0);
    }
    return *this;
}

reclaimer_registration::~reclaimer_registration() { reset(); }

reclaimer_registration::operator bool() const noexcept {
    return registry_ != nullptr;
}

void reclaimer_registration::reset() noexcept {
    if (registry_ != nullptr) {
        auto* registry = std::exchange(registry_, nullptr);
        const auto identifier = std::exchange(identifier_, 0);
        registry->deregister(identifier);
    }
}

reclaimer_registry::reclaimer_registry(std::size_t maximum_registrations)
  : maximum_registrations_(maximum_registrations) {
    if (maximum_registrations_ == 0) {
        throw std::invalid_argument(
          "reclaimer registry capacity must be nonzero");
    }
    entries_.reserve(maximum_registrations_);
}

reclaimer_registry::~reclaimer_registry() {
    assert_current();
    KWAQUE_INVARIANT(
      invariant_id{"KQ-RECLAIMER-REGISTRY-STOPPED"},
      state_ == reclaimer_registry_state::constructed
        || state_ == reclaimer_registry_state::stopped,
      "reclaimer registry destroyed while started");
    KWAQUE_INVARIANT(
      invariant_id{"KQ-RECLAIMER-REGISTRY-EMPTY"},
      entries_.empty() && !bridge_,
      "reclaimer registry destroyed with registrations or bridge");
}

void reclaimer_registry::increment(std::uint64_t& value) {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-RECLAIMER-COUNTER"},
      value != std::numeric_limits<std::uint64_t>::max(),
      "reclaimer registry counter overflow");
    ++value;
}

void reclaimer_registry::add(std::uint64_t& value, std::uint64_t delta) {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-RECLAIMER-BYTES"},
      delta <= std::numeric_limits<std::uint64_t>::max() - value,
      "reclaimer progress byte counter overflow");
    value += delta;
}

void reclaimer_registry::register_metrics() {
    namespace metrics = seastar::metrics;
    std::vector<metrics::metric_definition> definitions;
    definitions.reserve(6);
    definitions.emplace_back(
      metrics::make_counter(
        "attempts_total",
        [this] { return counters_.attempts; },
        metrics::description("Bounded application reclaim passes")));
    definitions.emplace_back(
      metrics::make_counter(
        "callbacks_total",
        [this] { return counters_.callbacks; },
        metrics::description("Application reclaim callbacks invoked")));
    definitions.emplace_back(
      metrics::make_counter(
        "progress_bytes_total",
        [this] { return counters_.progress_bytes; },
        metrics::description("Bytes reported reclaimable by callbacks")));
    definitions.emplace_back(
      metrics::make_counter(
        "reentries_total",
        [this] { return counters_.reentries; },
        metrics::description("Recursive reclaim passes rejected")));
    definitions.emplace_back(
      metrics::make_gauge(
        "allocator_free_bytes",
        [this] { return counters_.last_allocator_free_bytes; },
        metrics::description("Free shard allocator bytes at the last pass")));
    definitions.emplace_back(
      metrics::make_gauge(
        "allocator_total_bytes",
        [this] { return counters_.last_allocator_total_bytes; },
        metrics::description("Total shard allocator bytes at the last pass")));
    metrics_.add_group("reclaimer_registry", definitions);
}

void reclaimer_registry::start() {
    assert_current();
    if (state_ != reclaimer_registry_state::constructed) {
        throw std::logic_error("reclaimer registry cannot be started");
    }
    if (active_registry != nullptr) {
        throw std::logic_error(
          "another reclaimer registry is active on this shard");
    }

    try {
        register_metrics();
        bridge_.emplace(
          [this](seastar::memory::reclaimer::request request) {
              return bridge_reclaim(request);
          },
          requested_scope_);
        active_registry = this;
        state_ = reclaimer_registry_state::started;
    } catch (...) {
        metrics_.clear();
        bridge_.reset();
        state_ = reclaimer_registry_state::stopped;
        throw;
    }
}

void reclaimer_registry::stop() {
    assert_current();
    if (state_ == reclaimer_registry_state::stopped) {
        return;
    }
    if (state_ == reclaimer_registry_state::constructed) {
        state_ = reclaimer_registry_state::stopped;
        return;
    }
    if (!entries_.empty()) {
        throw std::logic_error(
          "reclaimer registrations must be released before shutdown");
    }
    KWAQUE_INVARIANT(
      invariant_id{"KQ-RECLAIMER-NOT-ACTIVE"},
      !reclaiming_,
      "reclaimer registry stopped during a reclaim pass");

    bridge_.reset();
    metrics_.clear();
    active_registry = nullptr;
    state_ = reclaimer_registry_state::stopped;
}

reclaimer_registry_state reclaimer_registry::state() const {
    assert_current();
    return state_;
}

std::size_t reclaimer_registry::size() const {
    assert_current();
    return entries_.size();
}

std::size_t reclaimer_registry::capacity() const {
    assert_current();
    return maximum_registrations_;
}

bool reclaimer_registry::reclaiming() const {
    assert_current();
    return reclaiming_;
}

reclaimer_registry_counters reclaimer_registry::counters() const {
    assert_current();
    return counters_;
}

seastar::memory::reclaimer_scope reclaimer_registry::bridge_scope() const {
    assert_current();
    if (!bridge_) {
        throw std::logic_error("reclaimer registry is not started");
    }
    return requested_scope_;
}

result<reclaimer_registration> reclaimer_registry::register_impl(
  std::int32_t priority, callback_type callback) {
    assert_current();
    if (state_ != reclaimer_registry_state::started) {
        return failure(errc::unavailable);
    }
    if (entries_.size() == maximum_registrations_) {
        return failure(errc::resource_exhausted);
    }
    KWAQUE_INVARIANT(
      invariant_id{"KQ-RECLAIMER-NOT-REGISTERING"},
      !reclaiming_,
      "reclaimer registration changed during a reclaim pass");
    KWAQUE_INVARIANT(
      invariant_id{"KQ-RECLAIMER-SEQUENCE"},
      next_sequence_ != std::numeric_limits<std::uint64_t>::max()
        && next_identifier_ != 0
        && next_identifier_ != std::numeric_limits<std::uint64_t>::max(),
      "reclaimer registration identity exhausted");

    const auto identifier = next_identifier_++;
    entry added{
      .priority = priority,
      .sequence = next_sequence_++,
      .identifier = identifier,
      .callback = std::move(callback),
    };
    const auto position = std::ranges::find_if(
      entries_, [priority](const entry& existing) {
          return priority > existing.priority;
      });
    entries_.insert(position, std::move(added));
    return reclaimer_registration{*this, identifier};
}

void reclaimer_registry::deregister(std::uint64_t identifier) noexcept {
    assert_current();
    KWAQUE_INVARIANT(
      invariant_id{"KQ-RECLAIMER-NOT-DEREGISTERING"},
      !reclaiming_,
      "reclaimer registration released during a reclaim pass");
    const auto position = std::ranges::find_if(
      entries_, [identifier](const entry& existing) {
          return identifier == existing.identifier;
      });
    KWAQUE_INVARIANT(
      invariant_id{"KQ-RECLAIMER-REGISTRATION-FOUND"},
      position != entries_.end(),
      "unknown reclaimer registration released");
    entries_.erase(position);
}

byte_count reclaimer_registry::request_reclaim(byte_count target) {
    assert_current();
    if (state_ != reclaimer_registry_state::started) {
        throw std::logic_error("reclaimer registry is not started");
    }
    if (target.value() == 0) {
        return byte_count{};
    }
    if (reclaiming_) {
        increment(counters_.reentries);
        return byte_count{};
    }

    reclaiming_ = true;
    auto clear_reclaiming = seastar::defer(
      [this] noexcept { reclaiming_ = false; });

    increment(counters_.attempts);
    const auto allocator = seastar::memory::stats();
    counters_.last_allocator_free_bytes = allocator.free_memory();
    counters_.last_allocator_total_bytes = allocator.total_memory();

    byte_count reclaimed;
    for (auto& registered : entries_) {
        if (reclaimed >= target) {
            break;
        }
        const auto remaining = target.checked_sub(reclaimed);
        KWAQUE_INVARIANT(
          invariant_id{"KQ-RECLAIMER-TARGET-BOUNDED"},
          remaining.has_value(),
          "reclaimer pass exceeded its target before callback dispatch");
        increment(counters_.callbacks);
        const auto progress = registered.callback(*remaining);
        const auto total = reclaimed.checked_add(progress);
        KWAQUE_INVARIANT(
          invariant_id{"KQ-RECLAIMER-PROGRESS-OVERFLOW"},
          total.has_value(),
          "reclaimer callback progress overflow");
        reclaimed = *total;
        add(counters_.progress_bytes, progress.value());
    }
    return reclaimed;
}

seastar::memory::reclaiming_result reclaimer_registry::bridge_reclaim(
  seastar::memory::reclaimer::request request) {
    return request_reclaim(byte_count{request.bytes_to_reclaim}).value() == 0
             ? seastar::memory::reclaiming_result::reclaimed_nothing
             : seastar::memory::reclaiming_result::reclaimed_something;
}

} // namespace kwaque::resource
