#ifndef KWAQUE_SRC_RUNTIME_PRODUCTION_ENVIRONMENT_H_
#define KWAQUE_SRC_RUNTIME_PRODUCTION_ENVIRONMENT_H_

#include "src/observability/event_sink.h"
#include "src/resource/resource_manager.h"
#include "src/runtime/environment.h"
#include "src/runtime/operation_statistics.h"
#include "src/runtime/production/clocks.h"
#include "src/runtime/production/dns.h"
#include "src/runtime/production/file.h"
#include "src/runtime/production/network.h"
#include "src/runtime/production/random.h"
#include "src/runtime/production/timer.h"
#include "src/runtime/sharded_service.h"
#include "src/runtime/task_scope.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/shared_future.hh>

#include <cstddef>
#include <cstdint>
#include <exception>
#include <optional>
#include <stdexcept>
#include <system_error>
#include <type_traits>
#include <utility>

namespace kwaque::runtime::production {

class environment_test_access;

class environment_dependencies final {
public:
    environment_dependencies(
      resource::resource_handle_set resources,
      seastar::logger& logger,
      observability::event_sink_identity event_identity,
      std::optional<seastar::net::dns_resolver::options> dns_options
      = std::nullopt)
      : resources_(std::move(resources))
      , logger_(&logger)
      , event_identity_(event_identity)
      , dns_options_(std::move(dns_options)) {}

    environment_dependencies(const environment_dependencies&) = default;
    environment_dependencies&
    operator=(const environment_dependencies&) = default;
    environment_dependencies(environment_dependencies&&) noexcept = default;
    environment_dependencies&
    operator=(environment_dependencies&&) noexcept = default;

private:
    friend class environment;

    resource::resource_handle_set resources_;
    seastar::logger* logger_;
    observability::event_sink_identity event_identity_;
    std::optional<seastar::net::dns_resolver::options> dns_options_;
};

class environment final : public shard_affine {
public:
    using monotonic_clock = production::monotonic_clock;
    using wall_clock = production::wall_clock;
    using timer_type = production::timer;
    using random_type = production::random_source;
    using file_system_type = production::file_system;
    using network_type = production::network;
    using dns_type = production::resolver;

    static constexpr bool faults_enabled = false;

    explicit environment(environment_dependencies dependencies);
    ~environment();

    environment(const environment&) = delete;
    environment& operator=(const environment&) = delete;
    environment(environment&&) = delete;
    environment& operator=(environment&&) = delete;

    [[nodiscard]] seastar::future<> start();
    void request_abort();
    [[nodiscard]] seastar::future<> stop();

    [[nodiscard]] runtime_lifetime& lifetime() {
        assert_current();
        return lifetime_;
    }
    [[nodiscard]] timer_type& timer() {
        assert_runtime_available(timer_.has_value());
        return *timer_;
    }
    [[nodiscard]] random_type& random() {
        assert_runtime_available(random_.has_value());
        return *random_;
    }
    [[nodiscard]] file_system_type& file_system() {
        assert_runtime_available(file_system_.has_value());
        return *file_system_;
    }
    [[nodiscard]] network_type& network() {
        assert_runtime_available(network_.has_value());
        return *network_;
    }
    [[nodiscard]] dns_type& dns() {
        assert_runtime_available(dns_.has_value());
        return *dns_;
    }

    [[nodiscard]] environment_state state() const;
    [[nodiscard]] bool abort_requested() const;
    [[nodiscard]] task_scope& tasks();
    [[nodiscard]] resource::resource_manager& resource_manager();
    [[nodiscard]] observability::production_event_sink& event_sink();

private:
    friend class environment_test_access;

    void assert_runtime_available(bool owner_present) const;
    void request_abort_unchecked() noexcept;
    static void throw_if_failed(const result<void>& outcome);
    [[nodiscard]] result<void> emit_lifecycle(
      observability::event_public_text state,
      observability::event_public_text operation,
      observability::event_severity severity) noexcept;
    template<typename Checkpoint>
    [[nodiscard]] seastar::future<> start_with(Checkpoint checkpoint);
    [[nodiscard]] seastar::future<> stop_once(bool report_failure);
    [[nodiscard]] seastar::future<> rollback_start() noexcept;
    void register_metrics();

    runtime_lifetime lifetime_;
    std::optional<task_scope> tasks_;
    resource::resource_manager resource_manager_;
    observability::production_event_sink event_sink_;
    std::optional<seastar::net::dns_resolver::options> dns_options_;
    operation_statistics_owner timer_statistics_;
    operation_statistics_owner file_statistics_;
    operation_statistics_owner network_statistics_;
    operation_statistics_owner dns_statistics_;
    std::optional<random_type> random_;
    std::optional<timer_type> timer_;
    std::optional<file_system_type> file_system_;
    std::optional<network_type> network_;
    std::optional<dns_type> dns_;
    std::optional<seastar::metrics::metric_groups> metrics_;
    std::optional<seastar::shared_promise<>> stop_done_;
    environment_state state_{environment_state::constructed};
    bool abort_requested_{false};
};

using environment_owner = sharded_service<environment>;

static_assert(!std::is_copy_constructible_v<environment>);
static_assert(!std::is_move_constructible_v<environment>);
static_assert(runtime_backend<environment>);
static_assert(environment_lifecycle<environment>);

template<typename Checkpoint>
seastar::future<> environment::start_with(Checkpoint checkpoint) {
    assert_current();
    if (state_ != environment_state::constructed) {
        throw std::logic_error("production environment cannot be started");
    }
    state_ = environment_state::starting;

    std::exception_ptr startup_failure;
    try {
        throw_if_failed(emit_lifecycle(
          observability::event_public_text::state_starting,
          observability::event_public_text::operation_environment_start,
          observability::event_severity::info));
        checkpoint(0);
        tasks_.emplace();
        if (abort_requested_) {
            throw seastar::abort_requested_exception{};
        }
        checkpoint(1);
        auto source = random_type::make();
        if (!source) {
            throw std::system_error(make_error_code(source.error().code()));
        }
        random_.emplace(std::move(*source));
        checkpoint(2);
        timer_.emplace(timer_statistics_);
        checkpoint(3);
        file_system_.emplace(file_statistics_);
        checkpoint(4);
        network_.emplace(network_statistics_);
        checkpoint(5);
        if (dns_options_) {
            dns_.emplace(dns_statistics_, dns_config{}, *dns_options_);
        } else {
            dns_.emplace(dns_statistics_);
        }
        dns_options_.reset();
        checkpoint(6);
        co_await resource_manager_.start();
        checkpoint(7);
        register_metrics();
        checkpoint(8);
        if (abort_requested_) {
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
        state_ = environment_state::stopped;
        std::rethrow_exception(startup_failure);
    }
    state_ = environment_state::started;
}

} // namespace kwaque::runtime::production

#endif // KWAQUE_SRC_RUNTIME_PRODUCTION_ENVIRONMENT_H_
