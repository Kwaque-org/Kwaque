#ifndef KWAQUE_SRC_RUNTIME_TESTING_CONTRACTS_ENVIRONMENT_LIFECYCLE_CONTRACT_H_
#define KWAQUE_SRC_RUNTIME_TESTING_CONTRACTS_ENVIRONMENT_LIFECYCLE_CONTRACT_H_

#include "src/runtime/environment.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

#include <optional>
#include <stdexcept>
#include <utility>

namespace kwaque::runtime::testing {

template<environment_lifecycle Environment, typename Driver>
seastar::future<>
run_environment_lifecycle_contract(Environment& environment, Driver driver) {
    if (environment.state() != environment_state::constructed) {
        throw std::runtime_error("environment did not begin constructed");
    }

    bool prestart_runtime_rejected = false;
    try {
        [[maybe_unused]] basic_runtime<Environment> runtime{environment};
    } catch (const std::logic_error&) {
        prestart_runtime_rejected = true;
    }
    if (!prestart_runtime_rejected) {
        throw std::runtime_error(
          "environment published runtime capabilities before start");
    }

    auto starting = environment.start();
    co_await driver.lifecycle(std::move(starting));
    if (environment.state() != environment_state::started) {
        throw std::runtime_error("environment did not become started");
    }

    std::optional<basic_runtime<Environment>> retained{
      std::in_place, environment};
    environment.request_abort();
    if (!environment.abort_requested()) {
        throw std::runtime_error("environment did not retain abort");
    }

    const auto require_closed_admission = [&] {
        bool rejected = false;
        try {
            [[maybe_unused]] basic_runtime<Environment> fresh{environment};
        } catch (const std::logic_error&) {
            rejected = true;
        }
        if (!rejected) {
            throw std::runtime_error(
              "aborted environment admitted a new runtime lease");
        }
        bool invoked = false;
        auto task = environment.tasks().spawn([&invoked] {
            invoked = true;
            return seastar::make_ready_future<>();
        });
        if (task || invoked || task.error().code() != errc::closed) {
            throw std::runtime_error("aborted environment admitted a new task");
        }
    };
    require_closed_admission();
    auto first_stop = environment.stop();
    require_closed_admission();
    if (first_stop.available()) {
        throw std::runtime_error(
          "stop failed to retain an existing runtime lease");
    }
    retained.reset();
    auto second_stop = environment.stop();
    co_await driver.lifecycle(std::move(first_stop));
    co_await std::move(second_stop);
    if (environment.state() != environment_state::stopped) {
        throw std::runtime_error("environment did not become stopped");
    }

    bool stopped_runtime_rejected = false;
    try {
        [[maybe_unused]] basic_runtime<Environment> runtime{environment};
    } catch (const std::logic_error&) {
        stopped_runtime_rejected = true;
    }
    if (!stopped_runtime_rejected) {
        throw std::runtime_error(
          "environment published runtime capabilities after stop");
    }

    co_await environment.stop();

    bool restart_rejected = false;
    try {
        co_await environment.start();
    } catch (const std::logic_error&) {
        restart_rejected = true;
    }
    if (!restart_rejected) {
        throw std::runtime_error("environment allowed restart after stop");
    }
}

} // namespace kwaque::runtime::testing

#endif // KWAQUE_SRC_RUNTIME_TESTING_CONTRACTS_ENVIRONMENT_LIFECYCLE_CONTRACT_H_
