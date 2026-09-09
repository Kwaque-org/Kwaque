#pragma once

#include "src/admin/admin_server.h"
#include "src/broker/pid_file.h"
#include "src/broker/service_lifecycle.h"
#include "src/broker/startup_policy.h"
#include "src/config/bootstrap_config.h"
#include "src/resource/resource_config.h"
#include "src/resource/resource_registry.h"
#include "src/runtime/production/environment.h"
#include "src/runtime/stop_signal.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>

#include <boost/program_options/variables_map.hpp>

#include <chrono>
#include <cstdint>
#include <exception>
#include <expected>
#include <filesystem>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>

namespace kwaque::broker::detail {

struct loaded_bootstrap_configuration final {
    config::bootstrap_config settings;
    configuration_identity identity;
};

using loaded_configuration_result
  = std::expected<loaded_bootstrap_configuration, config::config_error>;

[[nodiscard]] seastar::future<seastar::temporary_buffer<char>>
read_configuration_bytes(
  seastar::input_stream<char>& input, seastar::abort_source& abort_source);
[[nodiscard]] loaded_configuration_result
decode_configuration_snapshot(std::string_view contents);

[[nodiscard]] constexpr byte_count
reduce_minimum_shard_memory(byte_count current, byte_count observed) noexcept {
    return observed < current ? observed : current;
}

[[nodiscard]] resource::resource_config
production_resource_config(byte_count minimum_shard_memory);

class application_test_access;

class application_state final {
public:
    application_state() = default;
    ~application_state() = default;

    application_state(const application_state&) = delete;
    application_state& operator=(const application_state&) = delete;
    application_state(application_state&&) = delete;
    application_state& operator=(application_state&&) = delete;

    int execute(
      const boost::program_options::variables_map& options,
      const seastar::app_template::seastar_options& runtime_options);

    void initialize_stop_signal(bool install_signal_handlers = true);
    [[nodiscard]] seastar::future<>
    load_configuration(const boost::program_options::variables_map& options);
    void construct_services(bool install_signal_handlers = true);
    [[nodiscard]] seastar::future<> start_services();
    [[nodiscard]] seastar::future<> start_services(
      const seastar::app_template::seastar_options& runtime_options);
    [[nodiscard]] seastar::future<> request_service_abort();
    [[nodiscard]] seastar::future<> shutdown();

    [[nodiscard]] bool services_constructed() const noexcept;
    [[nodiscard]] bool runtime_started() const;
    [[nodiscard]] const service_lifecycle* lifecycle() const noexcept;

private:
    friend class application_test_access;

    void capture_or_assert_owner();
    void assert_owner() const;
    [[nodiscard]] seastar::future<byte_count> observe_minimum_shard_memory();
    [[nodiscard]] seastar::future<> start_data_directory();
    [[nodiscard]] seastar::future<> start_pid_file();
    [[nodiscard]] seastar::future<>
    start_resource_registry(resource::resource_config configuration);
    [[nodiscard]] seastar::future<> start_environments();
    [[nodiscard]] seastar::future<> start_admin();
    template<typename Checkpoint>
    [[nodiscard]] seastar::future<> start_services_with(
      Checkpoint checkpoint,
      const seastar::app_template::seastar_options* runtime_options = nullptr);
    void freeze_startup_policy(
      const seastar::app_template::seastar_options& runtime_options,
      const resource::resource_config& resources);

    std::filesystem::path config_path_;
    std::optional<config::bootstrap_config> configuration_;
    std::optional<configuration_identity> configuration_identity_;
    std::optional<std::string> startup_policy_;
    std::unique_ptr<runtime::stop_signal> stop_signal_;
    std::unique_ptr<service_lifecycle> lifecycle_;
    std::unique_ptr<pid_file> pid_file_;
    std::unique_ptr<admin::admin_server> admin_server_;
    std::unique_ptr<resource::resource_registry> resource_registry_;
    std::unique_ptr<runtime::production::environment_owner> environments_;
    std::chrono::steady_clock::time_point startup_started_at_{};
    std::optional<runtime::owner_shard> owner_;
};

template<typename Checkpoint>
seastar::future<> application_state::start_services_with(
  Checkpoint checkpoint,
  const seastar::app_template::seastar_options* runtime_options) {
    assert_owner();
    if (!configuration_ || !services_constructed()) {
        throw std::logic_error(
          "configuration and services must be ready before startup");
    }

    std::exception_ptr startup_failure;
    try {
        stop_signal_->abort_source().check();
        const auto minimum_shard_memory
          = co_await observe_minimum_shard_memory();
        stop_signal_->abort_source().check();
        auto resources = production_resource_config(minimum_shard_memory);
        if (runtime_options != nullptr) {
            // The caller keeps native options alive until this startup
            // finishes.
            freeze_startup_policy(*runtime_options, resources);
        }
        checkpoint(0);
        co_await start_data_directory();
        checkpoint(1);
        co_await start_pid_file();
        checkpoint(2);
        co_await start_resource_registry(std::move(resources));
        checkpoint(3);
        co_await start_environments();
        checkpoint(4);
        co_await start_admin();
        checkpoint(5);
    } catch (...) {
        startup_failure = std::current_exception();
    }

    if (startup_failure) {
        try {
            co_await shutdown();
        } catch (...) {
        }
        std::rethrow_exception(startup_failure);
    }
}

} // namespace kwaque::broker::detail
