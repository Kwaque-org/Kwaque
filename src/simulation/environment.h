#ifndef KWAQUE_SRC_SIMULATION_ENVIRONMENT_H_
#define KWAQUE_SRC_SIMULATION_ENVIRONMENT_H_

#include "src/base/invariant.h"
#include "src/observability/event_log.h"
#include "src/resource/resource_config.h"
#include "src/resource/resource_manager.h"
#include "src/resource/resource_registry.h"
#include "src/runtime/environment.h"
#include "src/runtime/task_scope.h"
#include "src/runtime/testing/failure_probe/failure_probe.h"
#include "src/simulation/deterministic_random.h"
#include "src/simulation/event_sink.h"
#include "src/simulation/event_trace.h"
#include "src/simulation/fake_dns.h"
#include "src/simulation/fake_file.h"
#include "src/simulation/fake_network.h"
#include "src/simulation/fault_schedule.h"
#include "src/simulation/metrics.h"
#include "src/simulation/scheduler.h"
#include "src/simulation/timer.h"
#include "src/simulation/virtual_time.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/chunked_vector.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>

#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <optional>
#include <stdexcept>
#include <system_error>
#include <type_traits>

namespace kwaque::simulation {

class environment_test_access;

struct environment_config_values final {
    environment_config_values();

    scheduler_limit_values scheduler;
    virtual_time_config_values virtual_time;
    trace_limit_values trace;
    observability::event_log_limit_values event_log;
    fake_file_system_config file;
    fake_network_config network;
    fake_dns_config dns;
    byte_count resource_total_memory;
    byte_count resource_headroom;
    std::uint32_t maximum_fault_rules;
    std::uint64_t master_seed;
    std::uint64_t runtime_stream_stable_id;
    std::uint64_t event_epoch;
    trace_digest configuration_digest{};
    trace_digest input_digest{};
    seastar::chunked_vector<fault_rule> fault_rules;
};

class environment_config final {
public:
    [[nodiscard]] static runtime::result<environment_config>
    make(environment_config_values values);

    environment_config(const environment_config&) = delete;
    environment_config& operator=(const environment_config&) = delete;
    environment_config(environment_config&&) noexcept = default;
    environment_config& operator=(environment_config&&) noexcept = default;

    [[nodiscard]] const scheduler_limits& scheduler_budget() const noexcept {
        return scheduler_budget_;
    }
    [[nodiscard]] const virtual_time_config& time_config() const noexcept {
        return time_config_;
    }
    [[nodiscard]] const trace_limits& trace_budget() const noexcept {
        return trace_budget_;
    }
    [[nodiscard]] const observability::event_log_limits&
    event_budget() const noexcept {
        return event_budget_;
    }
    [[nodiscard]] const fake_file_system_config& file_config() const noexcept {
        return file_config_;
    }
    [[nodiscard]] const fake_network_config& network_config() const noexcept {
        return network_config_;
    }
    [[nodiscard]] const fake_dns_config& dns_config() const noexcept {
        return dns_config_;
    }
    [[nodiscard]] const resource::resource_config& resources() const noexcept {
        return resources_;
    }
    [[nodiscard]] const fault_schedule_limits& fault_budget() const noexcept {
        return fault_budget_;
    }
    [[nodiscard]] std::uint64_t master_seed() const noexcept {
        return master_seed_;
    }
    [[nodiscard]] std::uint64_t runtime_stream_stable_id() const noexcept {
        return runtime_stream_stable_id_;
    }
    [[nodiscard]] const observability::event_sink_identity&
    event_identity() const noexcept {
        return event_identity_;
    }
    [[nodiscard]] const trace_digest& input_digest() const noexcept {
        return input_digest_;
    }

private:
    friend class environment;

    environment_config(
      scheduler_limits scheduler_budget,
      virtual_time_config time_config,
      trace_limits trace_budget,
      observability::event_log_limits event_budget,
      fake_file_system_config file_config,
      fake_network_config network_config,
      fake_dns_config dns_config,
      resource::resource_config resources,
      fault_schedule_limits fault_budget,
      std::uint64_t master_seed,
      std::uint64_t runtime_stream_stable_id,
      observability::event_sink_identity event_identity,
      std::uint64_t lifecycle_event_bytes,
      trace_digest input_digest,
      seastar::chunked_vector<fault_rule> fault_rules) noexcept;

    scheduler_limits scheduler_budget_;
    virtual_time_config time_config_;
    trace_limits trace_budget_;
    observability::event_log_limits event_budget_;
    fake_file_system_config file_config_;
    fake_network_config network_config_;
    fake_dns_config dns_config_;
    resource::resource_config resources_;
    fault_schedule_limits fault_budget_;
    std::uint64_t master_seed_;
    std::uint64_t runtime_stream_stable_id_;
    observability::event_sink_identity event_identity_;
    std::uint64_t lifecycle_event_bytes_;
    trace_digest input_digest_;
    seastar::chunked_vector<fault_rule> fault_rules_;
};

class environment final : public runtime::shard_affine {
public:
    using monotonic_clock = simulation::monotonic_clock;
    using wall_clock = simulation::wall_clock;
    using timer_type = simulation::timer;
    using random_type = sequential_random_source;
    using file_system_type = fake_file_system;
    using network_type = fake_network;
    using dns_type = fake_dns;
    using fault_injector_type = environment;

    static constexpr bool faults_enabled = true;

    [[nodiscard]] static runtime::result<std::unique_ptr<environment>>
    make(environment_config config);
    [[nodiscard]] static runtime::result<std::unique_ptr<environment>> replay(
      environment_config config,
      decoded_event_trace expected_trace,
      std::unique_ptr<observability::event_log> expected_events);

    ~environment();

    environment(const environment&) = delete;
    environment& operator=(const environment&) = delete;
    environment(environment&&) = delete;
    environment& operator=(environment&&) = delete;

    [[nodiscard]] seastar::future<> start();
    void request_abort();
    [[nodiscard]] seastar::future<> stop();

    [[nodiscard]] runtime::runtime_lifetime& lifetime() {
        assert_current();
        return lifetime_;
    }
    [[nodiscard]] timer_type& timer() {
        assert_runtime_available(timer_ != nullptr);
        return *timer_;
    }
    [[nodiscard]] random_type& random() {
        assert_runtime_available(true);
        return runtime_random_;
    }
    [[nodiscard]] file_system_type& file_system() {
        assert_runtime_available(files_ != nullptr);
        return *files_;
    }
    [[nodiscard]] network_type& network() {
        assert_runtime_available(network_ != nullptr);
        return *network_;
    }
    [[nodiscard]] dns_type& dns() {
        assert_runtime_available(dns_ != nullptr);
        return *dns_;
    }
    [[nodiscard]] fault_injector_type& faults() noexcept {
        KWAQUE_INVARIANT(
          invariant_id{"KQ-SIM-ENVIRONMENT-AFFINITY"},
          owner().is_current(),
          "simulation environment fault access crossed shards");
        return *this;
    }
    [[nodiscard]] runtime::result<runtime::fault_decision>
    evaluate(const runtime::fault_request& request) noexcept;

    [[nodiscard]] runtime::environment_state state() const;
    [[nodiscard]] bool abort_requested() const;
    [[nodiscard]] runtime::task_scope& tasks();
    [[nodiscard]] resource::resource_manager& resource_manager();
    [[nodiscard]] event_log_sink& event_sink();
    [[nodiscard]] runtime::testing::failure_probe& failure_probe();
    [[nodiscard]] scheduler& event_scheduler();
    [[nodiscard]] virtual_time& time();
    [[nodiscard]] const event_trace& trace() const;
    [[nodiscard]] runtime::result<void> finish_replay() noexcept;

private:
    friend class environment_test_access;

    environment(
      environment_config config,
      std::unique_ptr<event_trace> trace,
      std::unique_ptr<scheduler> event_scheduler,
      std::unique_ptr<virtual_time> time,
      std::unique_ptr<clock_binding> binding,
      deterministic_random random,
      sequential_random_source runtime_random,
      std::unique_ptr<fault_schedule> faults,
      std::unique_ptr<timer_type> control_timer,
      std::unique_ptr<timer_type> timer,
      std::unique_ptr<fake_file_system> files,
      std::unique_ptr<fake_network> network,
      std::unique_ptr<fake_dns> dns,
      std::unique_ptr<event_log_sink> event_sink,
      observability::event_log::reservation lifecycle_events);
    void assert_runtime_available(bool owner_present) const;
    [[nodiscard]] seastar::future<> start_registry();
    [[nodiscard]] static runtime::result<std::unique_ptr<environment>>
    make_composed(
      environment_config config,
      std::unique_ptr<event_trace> trace,
      std::unique_ptr<event_log_sink> event_sink);

    [[nodiscard]] seastar::future<> apply_probe(
      runtime::builtin_fault_point point,
      runtime::fault_object_key object,
      seastar::abort_source& abort_source);
    [[nodiscard]] runtime::result<runtime::testing::failure_evaluation>
    evaluate_probe(
      runtime::builtin_fault_point point,
      runtime::fault_object_key object) noexcept;
    static void throw_if_failed(const runtime::result<void>& outcome);
    template<typename Checkpoint>
    [[nodiscard]] seastar::future<> start_with(Checkpoint checkpoint);
    [[nodiscard]] seastar::future<> check_resource_group(std::size_t point);
    [[nodiscard]] runtime::result<void> emit_lifecycle(
      observability::event_public_text state,
      observability::event_public_text operation,
      observability::event_severity severity) noexcept;
    void request_abort_unchecked() noexcept;
    [[nodiscard]] seastar::future<> stop_once(bool inject_stop);
    [[nodiscard]] seastar::future<> rollback_start() noexcept;
    void release_active() noexcept;

    static thread_local environment* active_;

    environment_config config_;
    runtime::runtime_lifetime lifetime_;
    std::optional<runtime::task_scope> tasks_;
    std::optional<resource::resource_registry> registry_;
    std::unique_ptr<event_trace> trace_;
    std::unique_ptr<scheduler> scheduler_;
    std::unique_ptr<virtual_time> time_;
    std::unique_ptr<clock_binding> binding_;
    deterministic_random random_;
    sequential_random_source runtime_random_;
    std::unique_ptr<fault_schedule> faults_;
    std::unique_ptr<timer_type> control_timer_;
    std::unique_ptr<timer_type> timer_;
    std::unique_ptr<fake_file_system> files_;
    std::unique_ptr<fake_network> network_;
    std::unique_ptr<fake_dns> dns_;
    std::unique_ptr<event_log_sink> event_sink_;
    observability::event_log::reservation lifecycle_events_;
    runtime::testing::failure_probe failure_probe_;
    std::unique_ptr<resource::resource_manager> resource_manager_;
    std::unique_ptr<simulation_metrics> metrics_;
    seastar::abort_source stop_control_abort_;
    std::optional<seastar::shared_promise<>> stop_done_;
    runtime::environment_state state_{runtime::environment_state::constructed};
    bool abort_requested_{false};
    bool active_owner_{false};
};

static_assert(std::is_nothrow_move_constructible_v<environment_config>);
static_assert(!std::is_copy_constructible_v<environment>);
static_assert(!std::is_move_constructible_v<environment>);
static_assert(runtime::runtime_backend<environment>);
static_assert(runtime::environment_lifecycle<environment>);

template<typename Checkpoint>
seastar::future<> environment::start_with(Checkpoint checkpoint) {
    assert_current();
    if (state_ != runtime::environment_state::constructed) {
        throw std::logic_error("simulation environment cannot be started");
    }
    state_ = runtime::environment_state::starting;

    std::exception_ptr startup_failure;
    try {
        throw_if_failed(emit_lifecycle(
          observability::event_public_text::state_starting,
          observability::event_public_text::operation_environment_start,
          observability::event_severity::info));
        checkpoint(0);
        tasks_.emplace();
        if (abort_requested_) {
            tasks_->request_abort();
        }
        checkpoint(1);
        if (tasks_->abort_requested()) {
            throw seastar::abort_requested_exception{};
        }
        co_await apply_probe(
          runtime::builtin_fault_point::environment_start,
          runtime::fault_object_key::none(),
          tasks_->abort_source());
        checkpoint(2);
        registry_.emplace();
        co_await start_registry();
        checkpoint(3);
        resource_manager_ = std::make_unique<resource::resource_manager>(
          registry_->handles());
        checkpoint(4);
        co_await resource_manager_->start();
        checkpoint(5);
        metrics_ = std::make_unique<simulation_metrics>(
          *scheduler_, *trace_, *faults_, *files_, *network_, *dns_);
        checkpoint(6);
        metrics_->start();
        checkpoint(7);
        if (tasks_->abort_requested()) {
            throw seastar::abort_requested_exception{};
        }
        throw_if_failed(emit_lifecycle(
          observability::event_public_text::state_ready,
          observability::event_public_text::operation_environment_start,
          observability::event_severity::info));
        lifetime_.activate();
    } catch (...) {
        startup_failure = std::current_exception();
    }
    if (startup_failure) {
        co_await rollback_start();
        state_ = runtime::environment_state::stopped;
        std::rethrow_exception(startup_failure);
    }
    state_ = runtime::environment_state::started;
}

} // namespace kwaque::simulation

#endif // KWAQUE_SRC_SIMULATION_ENVIRONMENT_H_
