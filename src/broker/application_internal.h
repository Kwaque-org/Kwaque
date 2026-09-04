#pragma once

#include "src/admin/admin_server.h"
#include "src/broker/pid_file.h"
#include "src/broker/service_lifecycle.h"
#include "src/config/bootstrap_config.h"
#include "src/resource/resource_config.h"
#include "src/resource/resource_registry.h"
#include "src/runtime/production/environment.h"
#include "src/runtime/stop_signal.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

#include <boost/program_options/variables_map.hpp>

#include <chrono>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>

namespace kwaque::broker::detail {

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

    int execute(const boost::program_options::variables_map& options);

    [[nodiscard]] seastar::future<>
    load_configuration(const boost::program_options::variables_map& options);
    void construct_services(bool install_signal_handlers = true);
    [[nodiscard]] seastar::future<> start_services();
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
    start_resource_registry(byte_count minimum_shard_memory);
    [[nodiscard]] seastar::future<> start_environments();
    [[nodiscard]] seastar::future<> start_admin();
    template<typename Checkpoint>
    [[nodiscard]] seastar::future<> start_services_with(Checkpoint checkpoint);

    std::filesystem::path config_path_;
    std::optional<config::bootstrap_config> configuration_;
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
seastar::future<>
application_state::start_services_with(Checkpoint checkpoint) {
    assert_owner();
    if (!configuration_ || !services_constructed()) {
        throw std::logic_error(
          "configuration and services must be ready before startup");
    }

    std::exception_ptr startup_failure;
    try {
        const auto minimum_shard_memory
          = co_await observe_minimum_shard_memory();
        checkpoint(0);
        co_await start_data_directory();
        checkpoint(1);
        co_await start_pid_file();
        checkpoint(2);
        co_await start_resource_registry(minimum_shard_memory);
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
