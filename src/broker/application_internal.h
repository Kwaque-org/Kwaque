#pragma once

#include "src/admin/admin_server.h"
#include "src/broker/pid_file.h"
#include "src/broker/service_lifecycle.h"
#include "src/config/bootstrap_config.h"
#include "src/runtime/production/backend.h"
#include "src/runtime/runtime_service.h"
#include "src/runtime/sharded_service.h"
#include "src/runtime/stop_signal.h"

#include <seastar/core/future.hh>

#include <boost/program_options/variables_map.hpp>

#include <chrono>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>

namespace kwaque::broker::detail {

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
    void capture_or_assert_owner();
    void assert_owner() const;

    std::filesystem::path config_path_;
    std::optional<config::bootstrap_config> configuration_;
    std::unique_ptr<runtime::stop_signal> stop_signal_;
    std::unique_ptr<service_lifecycle> lifecycle_;
    std::unique_ptr<pid_file> pid_file_;
    std::unique_ptr<admin::admin_server> admin_server_;
    std::unique_ptr<runtime::sharded_service<runtime::runtime_service>>
      runtime_service_;
    std::unique_ptr<runtime::production::backend_owner> production_backends_;
    std::chrono::steady_clock::time_point startup_started_at_{};
    std::optional<runtime::owner_shard> owner_;
};

} // namespace kwaque::broker::detail
