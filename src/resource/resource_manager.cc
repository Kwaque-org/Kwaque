#include "src/resource/resource_manager.h"

#include "src/base/metric_schema.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/sstring.hh>

#include <cstdint>
#include <exception>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace kwaque::resource {

namespace {

thread_local resource_manager* active_manager = nullptr;

} // namespace

workload_handle::workload_handle(
  resource_manager& manager,
  seastar::semaphore& memory,
  byte_count hard_budget,
  seastar::scheduling_group scheduling_group,
  seastar::smp_service_group smp_service_group,
  seastar::gate::holder lifetime) noexcept
  : manager_(&manager)
  , memory_(&memory)
  , hard_budget_(hard_budget)
  , scheduling_group_(scheduling_group)
  , smp_service_group_(smp_service_group)
  , lifetime_(std::move(lifetime)) {}

workload_handle::workload_handle(workload_handle&& other) noexcept
  : owner_(other.owner_)
  , manager_(std::exchange(other.manager_, nullptr))
  , memory_(std::exchange(other.memory_, nullptr))
  , hard_budget_(other.hard_budget_)
  , scheduling_group_(other.scheduling_group_)
  , smp_service_group_(other.smp_service_group_)
  , lifetime_(std::move(other.lifetime_)) {}

workload_handle::~workload_handle() { owner_.assert_current(); }

seastar::scheduling_group workload_handle::scheduling_group() const {
    assert_live();
    return scheduling_group_;
}

seastar::smp_service_group workload_handle::smp_service_group() const {
    assert_live();
    return smp_service_group_;
}

resource_manager::resource_manager(resource_handle_set handles)
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

void resource_manager::register_metrics() {
    if (metrics_) {
        throw std::logic_error("resource metrics are already registered");
    }
    namespace metrics = seastar::metrics;
    try {
        metrics_.emplace();
        std::vector<metrics::metric_definition> definitions;
        definitions.reserve(workload_class_count * 4U);
        const std::vector<metrics::label> aggregate{metrics::shard_label};
        const auto& configured = *kwaque::descriptor_for(
          metric_id::memory_configured_bytes);
        const auto& used = *kwaque::descriptor_for(
          metric_id::memory_used_bytes);
        const auto& available = *kwaque::descriptor_for(
          metric_id::memory_available_bytes);
        const auto& waiters = *kwaque::descriptor_for(
          metric_id::memory_waiters);
        for (const auto classification : all_workload_classes) {
            const auto index = workload_index(classification);
            const auto value = seastar::sstring{
              metric_workload_label_values[index]};
            std::vector<metrics::label_instance> labels{metrics::label_instance{
              seastar::sstring{metric_workload_label}, value}};

            definitions.emplace_back(
              metrics::make_gauge(
                seastar::sstring{configured.name},
                [this, classification] {
                    return handles_.config().budget(classification).value();
                },
                metrics::description(seastar::sstring{configured.help}),
                labels)
                .aggregate(aggregate));
            definitions.emplace_back(
              metrics::make_gauge(
                seastar::sstring{used.name},
                [this, classification, index] {
                    const auto capacity
                      = handles_.config().budget(classification).value();
                    return capacity
                           - static_cast<std::uint64_t>(
                             memory_admissions_[index]->current());
                },
                metrics::description(seastar::sstring{used.help}),
                labels)
                .aggregate(aggregate));
            definitions.emplace_back(
              metrics::make_gauge(
                seastar::sstring{available.name},
                [this, index] {
                    return static_cast<std::uint64_t>(
                      memory_admissions_[index]->current());
                },
                metrics::description(seastar::sstring{available.help}),
                labels)
                .aggregate(aggregate));
            definitions.emplace_back(
              metrics::make_gauge(
                seastar::sstring{waiters.name},
                [this, index] { return memory_admissions_[index]->waiters(); },
                metrics::description(seastar::sstring{waiters.help}),
                labels)
                .aggregate(aggregate));
        }
        metrics_->add_group(seastar::sstring{configured.group}, definitions);
    } catch (...) {
        metrics_.reset();
        throw;
    }
}

void resource_manager::rollback_start() {
    metrics_.reset();
    for (auto& admission : memory_admissions_) {
        admission.reset();
    }
    if (registry_lease_acquired_) {
        handles_.release_manager_lease();
        registry_lease_acquired_ = false;
    }
    active_manager = nullptr;
    state_ = resource_manager_state::stopped;
}

seastar::future<> resource_manager::start() {
    return start_with([](std::size_t) noexcept {});
}

void resource_manager::prepare_start() {
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
}

void resource_manager::request_abort() {
    assert_current();
    // The manager owns no asynchronous operations. Components abort and drain
    // their bounded queues before releasing the leases drained by stop().
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
    metrics_.reset();
    active_manager = nullptr;
    for (const auto classification : all_workload_classes) {
        const auto index = workload_index(classification);
        if (!memory_admissions_[index]) {
            continue;
        }
        const auto capacity = handles_.config().budget(classification);
        KWAQUE_INVARIANT(
          invariant_id{"KQ-RESOURCE-MEMORY-DRAINED"},
          memory_admissions_[index]->waiters() == 0
            && memory_admissions_[index]->current() == capacity.value(),
          "resource manager stopped with outstanding memory admission");
        memory_admissions_[index].reset();
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

byte_count resource_manager::memory_used(workload_class classification) const {
    const auto available = memory_available(classification);
    const auto used
      = handles_.config().budget(classification).checked_sub(available);
    KWAQUE_INVARIANT(
      invariant_id{"KQ-RESOURCE-MEMORY-USED"},
      used.has_value(),
      "workload memory admission exceeded its capacity");
    return *used;
}

byte_count
resource_manager::memory_available(workload_class classification) const {
    assert_ready();
    const auto index = checked_index(classification);
    KWAQUE_INVARIANT(
      invariant_id{"KQ-RESOURCE-MEMORY-READY"},
      memory_admissions_[index].has_value(),
      "started manager has no class memory admission");
    KWAQUE_INVARIANT(
      invariant_id{"KQ-RESOURCE-MEMORY-AVAILABLE"},
      memory_admissions_[index]->current() >= 0,
      "workload memory admission counter became negative");
    return byte_count{
      static_cast<std::uint64_t>(memory_admissions_[index]->current())};
}

workload_handle
resource_manager::acquire_workload(workload_class classification) {
    assert_ready();
    const auto index = checked_index(classification);
    auto lifetime = work_.try_hold();
    KWAQUE_INVARIANT(
      invariant_id{"KQ-WORKLOAD-HANDLE-ADMITTED"},
      lifetime.has_value(),
      "ready manager rejected a workload handle");
    return workload_handle{
      *this,
      *memory_admissions_[index],
      handles_.config().budget(classification),
      handles_.scheduling_groups_[index],
      handles_.smp_service_groups_[index],
      std::move(*lifetime)};
}

} // namespace kwaque::resource
