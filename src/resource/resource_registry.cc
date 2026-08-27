#include "src/resource/resource_registry.h"

#include "src/base/invariant.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sstring.hh>

#include <algorithm>
#include <atomic>
#include <exception>
#include <limits>
#include <stdexcept>
#include <string>
#include <utility>

namespace kwaque::resource {

namespace {

resource_registry* active_registry = nullptr;
std::atomic<std::uint64_t> active_generation{0};
std::atomic<std::uint64_t> next_generation{1};
std::atomic<std::uint64_t> manager_lease_state{0};
std::atomic<bool> registry_poisoned{false};

constexpr std::uint64_t closing_generation
  = std::numeric_limits<std::uint64_t>::max();
constexpr std::uint64_t lease_closing_bit = std::uint64_t{1} << 63;
constexpr std::uint64_t lease_count_mask = lease_closing_bit - 1;

std::string group_name(const workload_descriptor& descriptor) {
    return "kwaque_" + std::string(descriptor.metric_name);
}

template<std::size_t... Index>
std::array<seastar::smp_service_group, sizeof...(Index)>
default_smp_groups(std::index_sequence<Index...>) {
    return {
      (static_cast<void>(Index), seastar::default_smp_service_group())...};
}

} // namespace

resource_handle_set::resource_handle_set(
  resource_config config,
  std::uint64_t generation,
  std::array<seastar::scheduling_group, workload_class_count> scheduling_groups,
  std::array<seastar::smp_service_group, workload_class_count>
    smp_service_groups) noexcept
  : config_(config)
  , generation_(generation)
  , scheduling_groups_(scheduling_groups)
  , smp_service_groups_(smp_service_groups) {}

bool resource_handle_set::valid() const noexcept {
    return active_generation.load(std::memory_order_acquire) == generation_;
}

void resource_handle_set::assert_valid() const {
    if (!valid()) {
        throw std::logic_error("resource handles are no longer valid");
    }
}

bool resource_handle_set::try_acquire_manager_lease() const noexcept {
    if (!valid()) {
        return false;
    }

    auto current = manager_lease_state.load(std::memory_order_acquire);
    while ((current & lease_closing_bit) == 0) {
        if ((current & lease_count_mask) == lease_count_mask) {
            std::terminate();
        }
        if (
          manager_lease_state.compare_exchange_weak(
            current,
            current + 1,
            std::memory_order_acq_rel,
            std::memory_order_acquire)) {
            if (valid()) {
                return true;
            }
            release_manager_lease();
            return false;
        }
    }
    return false;
}

void resource_handle_set::release_manager_lease() const noexcept {
    auto current = manager_lease_state.load(std::memory_order_acquire);
    while ((current & lease_closing_bit) == 0
           && (current & lease_count_mask) != 0
           && !manager_lease_state.compare_exchange_weak(
             current,
             current - 1,
             std::memory_order_acq_rel,
             std::memory_order_acquire)) {
    }
    if (
      (current & lease_closing_bit) != 0 || (current & lease_count_mask) == 0) {
        std::terminate();
    }
}

resource_registry::~resource_registry() {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-RESOURCE-REGISTRY-STOPPED"},
      state_ == resource_registry_state::constructed
        || state_ == resource_registry_state::stopped
        || state_ == resource_registry_state::failed,
      "resource registry destroyed while active");
}

void resource_registry::assert_coordinator() const {
    assert_current();
    if (seastar::this_shard_id() != 0) {
        throw std::logic_error("resource registry must run on shard zero");
    }
}

void resource_registry::inject_before_creation() {
    if (fail_before_creation_ && creation_point_ == *fail_before_creation_) {
        throw std::runtime_error("injected resource group creation failure");
    }
    ++creation_point_;
}

seastar::future<> resource_registry::start(resource_config config) {
    assert_coordinator();
    if (state_ != resource_registry_state::constructed) {
        throw std::logic_error("resource registry cannot be started");
    }
    if (active_registry != nullptr) {
        throw std::logic_error("another resource registry is active");
    }
    if (registry_poisoned.load(std::memory_order_acquire)) {
        throw std::logic_error(
          "resource registry cannot restart after an incomplete cleanup");
    }
    KWAQUE_INVARIANT(
      invariant_id{"KQ-RESOURCE-LEASE-STATE-CLEAR"},
      manager_lease_state.load(std::memory_order_acquire) == 0,
      "resource registry started with a nonempty manager lease state");

    active_registry = this;
    state_ = resource_registry_state::starting;
    config_ = config;
    creation_point_ = 0;

    std::exception_ptr startup_failure;
    try {
        for (const auto classification : all_workload_classes) {
            const auto index = workload_index(classification);
            const auto& descriptor = descriptor_for(classification);
            const auto name = group_name(descriptor);

            inject_before_creation();
            auto scheduling_group = co_await seastar::create_scheduling_group(
              seastar::sstring{name},
              static_cast<float>(descriptor.scheduling_shares));
            scheduling_groups_[index] = scheduling_group;

            inject_before_creation();
            seastar::smp_service_group_config smp_config;
            const auto minimum_limit = seastar::this_smp_shard_count() - 1;
            const auto effective_limit = std::max(
              descriptor.max_nonlocal_requests, minimum_limit);
            smp_config.max_nonlocal_requests = effective_limit;
            smp_config.group_name = seastar::sstring{name};
            smp_service_groups_[index]
              = co_await seastar::create_smp_service_group(
                std::move(smp_config));
        }
    } catch (...) {
        startup_failure = std::current_exception();
    }

    if (startup_failure) {
        bool cleanup_failed = false;
        try {
            co_await destroy_created_groups();
        } catch (...) {
            cleanup_failed = true;
        }
        config_.reset();
        active_registry = nullptr;
        if (cleanup_failed) {
            registry_poisoned.store(true, std::memory_order_release);
            state_ = resource_registry_state::failed;
        } else {
            state_ = resource_registry_state::stopped;
        }
        std::rethrow_exception(startup_failure);
    }
    generation_ = next_generation.fetch_add(1, std::memory_order_acq_rel);
    KWAQUE_INVARIANT(
      invariant_id{"KQ-RESOURCE-GENERATION-VALID"},
      generation_ != 0 && generation_ != closing_generation,
      "resource registry generation space exhausted");
    active_generation.store(generation_, std::memory_order_release);
    state_ = resource_registry_state::started;
}

seastar::future<> resource_registry::destroy_created_groups() {
    std::exception_ptr first_failure;
    for (std::size_t offset = 0; offset < workload_class_count; ++offset) {
        const auto index = workload_class_count - offset - 1;
        if (!smp_service_groups_[index]) {
            continue;
        }
        try {
            co_await seastar::destroy_smp_service_group(
              *smp_service_groups_[index]);
            smp_service_groups_[index].reset();
        } catch (...) {
            if (!first_failure) {
                first_failure = std::current_exception();
            }
        }
    }
    for (std::size_t offset = 0; offset < workload_class_count; ++offset) {
        const auto index = workload_class_count - offset - 1;
        if (!scheduling_groups_[index]) {
            continue;
        }
        try {
            co_await seastar::destroy_scheduling_group(
              *scheduling_groups_[index]);
            scheduling_groups_[index].reset();
        } catch (...) {
            if (!first_failure) {
                first_failure = std::current_exception();
            }
        }
    }
    if (first_failure) {
        std::rethrow_exception(first_failure);
    }
}

seastar::future<> resource_registry::stop_once() {
    std::exception_ptr failure;
    try {
        co_await destroy_created_groups();
    } catch (...) {
        failure = std::current_exception();
    }
    config_.reset();
    if (failure) {
        registry_poisoned.store(true, std::memory_order_release);
        active_registry = nullptr;
        std::rethrow_exception(failure);
    }
    active_generation.store(0, std::memory_order_release);
    manager_lease_state.store(0, std::memory_order_release);
    active_registry = nullptr;
}

seastar::future<> resource_registry::stop() {
    assert_coordinator();
    if (state_ == resource_registry_state::stopping) {
        return stop_done_.get_shared_future();
    }
    if (state_ == resource_registry_state::stopped) {
        return stop_done_.available() ? stop_done_.get_shared_future()
                                      : seastar::make_ready_future<>();
    }
    if (state_ == resource_registry_state::failed) {
        return stop_done_.available()
                 ? stop_done_.get_shared_future()
                 : seastar::make_exception_future<>(std::logic_error(
                     "resource registry cleanup previously failed"));
    }
    if (state_ == resource_registry_state::starting) {
        return seastar::make_exception_future<>(
          std::logic_error("resource registry startup is in progress"));
    }
    if (state_ == resource_registry_state::constructed) {
        state_ = resource_registry_state::stopped;
        return seastar::make_ready_future<>();
    }

    for (const auto& scheduling_group : scheduling_groups_) {
        if (
          scheduling_group
          && *scheduling_group == seastar::current_scheduling_group()) {
            return seastar::make_exception_future<>(std::logic_error(
              "resource registry cannot destroy the current scheduling group"));
        }
    }
    auto expected_lease_state = std::uint64_t{0};
    if (!manager_lease_state.compare_exchange_strong(
          expected_lease_state,
          lease_closing_bit,
          std::memory_order_acq_rel,
          std::memory_order_acquire)) {
        return seastar::make_exception_future<>(std::logic_error(
          (expected_lease_state & lease_count_mask) != 0
            ? "resource registry cannot stop while shard managers are active"
            : "resource registry shutdown is already in progress"));
    }
    auto expected_generation = generation_;
    if (!active_generation.compare_exchange_strong(
          expected_generation,
          closing_generation,
          std::memory_order_acq_rel,
          std::memory_order_acquire)) {
        manager_lease_state.store(0, std::memory_order_release);
        return seastar::make_exception_future<>(
          std::logic_error("resource registry generation is not active"));
    }

    state_ = resource_registry_state::stopping;
    auto completion = stop_once().then_wrapped(
      [this](seastar::future<> stopped) noexcept {
          try {
              stopped.get();
              state_ = resource_registry_state::stopped;
              stop_done_.set_value();
          } catch (...) {
              state_ = resource_registry_state::failed;
              stop_done_.set_exception(std::current_exception());
          }
      });
    static_cast<void>(completion);
    return stop_done_.get_shared_future();
}

bool resource_registry::ready() const {
    assert_coordinator();
    return state_ == resource_registry_state::started;
}

resource_registry_state resource_registry::state() const {
    assert_coordinator();
    return state_;
}

resource_handle_set resource_registry::handles() const {
    assert_coordinator();
    if (
      state_ != resource_registry_state::started || !config_
      || active_generation.load(std::memory_order_acquire) != generation_) {
        throw std::logic_error("resource registry is not ready");
    }

    std::array<seastar::scheduling_group, workload_class_count>
      scheduling_groups;
    auto smp_service_groups = default_smp_groups(
      std::make_index_sequence<workload_class_count>{});
    for (const auto classification : all_workload_classes) {
        const auto index = workload_index(classification);
        KWAQUE_INVARIANT(
          invariant_id{"KQ-RESOURCE-HANDLES-COMPLETE"},
          scheduling_groups_[index].has_value()
            && smp_service_groups_[index].has_value(),
          "ready resource registry has an incomplete handle set");
        scheduling_groups[index] = *scheduling_groups_[index];
        smp_service_groups[index] = *smp_service_groups_[index];
    }
    return resource_handle_set{
      *config_, generation_, scheduling_groups, smp_service_groups};
}

} // namespace kwaque::resource
