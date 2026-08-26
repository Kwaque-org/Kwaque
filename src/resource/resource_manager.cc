#include "src/resource/resource_manager.h"

#include "src/base/error.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/sstring.hh>

#include <exception>
#include <limits>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace kwaque::resource {

namespace {

thread_local resource_manager* active_manager = nullptr;

} // namespace

resource_manager::resource_manager(resource_handle_set handles) noexcept
  : handles_(std::move(handles)) {}

resource_manager::~resource_manager() {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-RESOURCE-MANAGER-STOPPED"},
      state_ == resource_manager_state::constructed
        || state_ == resource_manager_state::stopped,
      "resource manager destroyed while active");
    KWAQUE_INVARIANT(
      invariant_id{"KQ-RESOURCE-MANAGER-LEASE-RELEASED"},
      !registry_lease_acquired_,
      "resource manager destroyed with a registry lease");
}

void resource_manager::increment(std::uint64_t& value) {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-RESOURCE-COUNTER-OVERFLOW"},
      value != std::numeric_limits<std::uint64_t>::max(),
      "resource counter overflow");
    ++value;
}

std::size_t resource_manager::checked_index(workload_class classification) {
    const auto index = workload_index(classification);
    if (index >= workload_class_count) {
        throw std::out_of_range("unknown workload class");
    }
    return index;
}

void resource_manager::register_metrics() {
    namespace metrics = seastar::metrics;
    std::vector<metrics::metric_definition> definitions;
    definitions.reserve(workload_class_count * 7);
    for (const auto classification : all_workload_classes) {
        const auto index = workload_index(classification);
        const auto value = seastar::sstring{
          std::string{descriptor_for(classification).metric_name}};
        std::vector<metrics::label_instance> labels{
          metrics::label_instance{seastar::sstring{"workload"}, value}};

        definitions.emplace_back(
          metrics::make_gauge(
            "queued",
            [this, index] { return counters_[index].queued; },
            metrics::description("Accepted workload operations waiting to run"),
            labels));
        definitions.emplace_back(
          metrics::make_gauge(
            "executing",
            [this, index] { return counters_[index].executing; },
            metrics::description("Workload operations currently executing"),
            labels));
        definitions.emplace_back(
          metrics::make_counter(
            "rejected_total",
            [this, index] { return counters_[index].rejected; },
            metrics::description("Rejected workload operations"),
            labels));
        definitions.emplace_back(
          metrics::make_counter(
            "completed_total",
            [this, index] { return counters_[index].completed; },
            metrics::description("Successfully completed workload operations"),
            labels));
        definitions.emplace_back(
          metrics::make_counter(
            "failed_total",
            [this, index] { return counters_[index].failed; },
            metrics::description("Failed workload operations"),
            labels));
        definitions.emplace_back(
          metrics::make_gauge(
            "bytes_reserved",
            [this, index] { return counters_[index].bytes_reserved; },
            metrics::description("Bytes reserved against the workload budget"),
            labels));
        definitions.emplace_back(
          metrics::make_counter(
            "reclaim_attempts_total",
            [this, index] { return counters_[index].reclaim_attempts; },
            metrics::description("Workload memory reclaim attempts"),
            std::move(labels)));
    }
    metrics_.add_group("resource_manager", definitions);
}

void resource_manager::rollback_start() {
    initialized_.fill(false);
    for (auto& admission : smp_admission_) {
        admission.reset();
    }
    if (registry_lease_acquired_) {
        handles_.release_manager_lease();
        registry_lease_acquired_ = false;
    }
    active_manager = nullptr;
    state_ = resource_manager_state::stopped;
    metrics_.clear();
}

seastar::future<> resource_manager::start() {
    assert_current();
    if (state_ != resource_manager_state::constructed) {
        throw std::logic_error("resource manager cannot be started");
    }
    if (active_manager != nullptr) {
        throw std::logic_error(
          "another resource manager is active on this shard");
    }

    active_manager = this;
    state_ = resource_manager_state::starting;
    try {
        if (!handles_.try_acquire_manager_lease()) {
            throw std::logic_error("resource handles are no longer valid");
        }
        registry_lease_acquired_ = true;
        for (const auto classification : all_workload_classes) {
            const auto index = workload_index(classification);
            if (
              fail_before_start_point_ && index == *fail_before_start_point_) {
                throw std::runtime_error(
                  "injected resource manager start failure");
            }
            smp_admission_[index].emplace(
              descriptor_for(classification).max_nonlocal_requests);
            initialized_[index] = true;
        }
        register_metrics();
        state_ = resource_manager_state::started;
        return seastar::make_ready_future<>();
    } catch (...) {
        auto failure = std::current_exception();
        try {
            rollback_start();
        } catch (...) {
        }
        return seastar::make_exception_future<>(std::move(failure));
    }
}

void resource_manager::request_abort() {
    assert_current();
    if (!abort_source_.abort_requested()) {
        abort_source_.request_abort();
    }
}

seastar::future<> resource_manager::stop_once() {
    std::exception_ptr failure;
    try {
        request_abort();
        if (!work_.is_closed()) {
            co_await work_.close();
        }
    } catch (...) {
        failure = std::current_exception();
    }
    try {
        metrics_.clear();
    } catch (...) {
        if (!failure) {
            failure = std::current_exception();
        }
    }
    initialized_.fill(false);
    active_manager = nullptr;
    for (const auto classification : all_workload_classes) {
        const auto index = workload_index(classification);
        if (!smp_admission_[index]) {
            continue;
        }
        KWAQUE_INVARIANT(
          invariant_id{"KQ-RESOURCE-SMP-ADMISSION-DRAINED"},
          smp_admission_[index]->waiters() == 0
            && smp_admission_[index]->current()
                 == descriptor_for(classification).max_nonlocal_requests,
          "resource manager stopped with outstanding SMP admission");
        smp_admission_[index].reset();
    }
    for (const auto& counters : counters_) {
        KWAQUE_INVARIANT(
          invariant_id{"KQ-RESOURCE-WORK-DRAINED"},
          counters.queued == 0 && counters.executing == 0,
          "resource manager stopped with active workload operations");
        KWAQUE_INVARIANT(
          invariant_id{"KQ-RESOURCE-BYTES-RELEASED"},
          counters.bytes_reserved == 0,
          "resource manager stopped with reserved bytes");
    }
    if (registry_lease_acquired_) {
        handles_.release_manager_lease();
        registry_lease_acquired_ = false;
    }
    if (failure) {
        std::rethrow_exception(failure);
    }
}

seastar::future<> resource_manager::stop() {
    assert_current();
    if (state_ == resource_manager_state::stopping) {
        return stop_done_.get_shared_future();
    }
    if (state_ == resource_manager_state::stopped) {
        return stop_done_.available() ? stop_done_.get_shared_future()
                                      : seastar::make_ready_future<>();
    }
    if (state_ == resource_manager_state::starting) {
        return seastar::make_exception_future<>(
          std::logic_error("resource manager startup is in progress"));
    }
    if (state_ == resource_manager_state::constructed) {
        state_ = resource_manager_state::stopped;
        return seastar::make_ready_future<>();
    }

    state_ = resource_manager_state::stopping;
    auto completion = stop_once().then_wrapped(
      [this](seastar::future<> stopped) noexcept {
          state_ = resource_manager_state::stopped;
          try {
              stopped.get();
              stop_done_.set_value();
          } catch (...) {
              stop_done_.set_exception(std::current_exception());
          }
      });
    static_cast<void>(completion);
    return stop_done_.get_shared_future();
}

void resource_manager::assert_ready() const {
    assert_current();
    if (state_ != resource_manager_state::started) {
        throw std::logic_error("resource manager is not ready");
    }
}

bool resource_manager::ready() const {
    assert_current();
    return state_ == resource_manager_state::started;
}

resource_manager_state resource_manager::state() const {
    assert_current();
    return state_;
}

byte_count resource_manager::hard_budget(workload_class classification) const {
    assert_ready();
    return handles_.config().budget(classification);
}

seastar::scheduling_group
resource_manager::scheduling_group(workload_class classification) const {
    assert_ready();
    return handles_.scheduling_group(classification);
}

unsigned
resource_manager::smp_admission_limit(workload_class classification) const {
    assert_ready();
    return descriptor_for(classification).max_nonlocal_requests;
}

workload_counters
resource_manager::counters(workload_class classification) const {
    assert_ready();
    return counters_[checked_index(classification)];
}

seastar::abort_source& resource_manager::abort_source() {
    assert_ready();
    return abort_source_;
}

result<void> resource_manager::reserve_bytes(
  workload_class classification, byte_count bytes) {
    assert_ready();
    const auto index = checked_index(classification);
    const byte_count current{counters_[index].bytes_reserved};
    const auto next = current.checked_add(bytes);
    if (!next || *next > handles_.config().budget(classification)) {
        increment(counters_[index].rejected);
        return failure(errc::resource_exhausted);
    }
    counters_[index].bytes_reserved = next->value();
    return {};
}

void resource_manager::release_bytes(
  workload_class classification, byte_count bytes) {
    assert_ready();
    const auto index = checked_index(classification);
    const byte_count current{counters_[index].bytes_reserved};
    const auto remaining = current.checked_sub(bytes);
    KWAQUE_INVARIANT(
      invariant_id{"KQ-RESOURCE-RESERVATION-UNDERFLOW"},
      remaining.has_value(),
      "released more bytes than the workload reserved");
    counters_[index].bytes_reserved = remaining->value();
}

void resource_manager::record_reclaim_attempt(workload_class classification) {
    assert_ready();
    increment(counters_[checked_index(classification)].reclaim_attempts);
}

} // namespace kwaque::resource
