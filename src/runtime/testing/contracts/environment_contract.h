#ifndef KWAQUE_SRC_RUNTIME_TESTING_CONTRACTS_ENVIRONMENT_CONTRACT_H_
#define KWAQUE_SRC_RUNTIME_TESTING_CONTRACTS_ENVIRONMENT_CONTRACT_H_

#include "src/observability/event_sink_concept.h"
#include "src/resource/resource_manager.h"
#include "src/runtime/environment.h"
#include "src/runtime/testing/contracts/environment_component.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

#include <cstdint>
#include <exception>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace kwaque::runtime::testing {

struct environment_contract_expectation final {
    std::vector<dns_answer> dns_answers;
    std::optional<std::uint64_t> random_word;
};

struct environment_contract_observation final {
    std::uint64_t random_word;
    byte_count file_bytes;
    byte_count network_request_bytes;
    byte_count network_response_bytes;
    std::vector<dns_answer> dns_answers;
    std::size_t directory_entries;
    fault_action queue_decision;
    byte_count admitted_memory;
    bool file_eof;
    bool network_request_eof;
    bool network_response_eof;

    bool operator==(const environment_contract_observation&) const = default;
};

template<typename Environment>
using environment_sink_type = std::remove_reference_t<
  decltype(std::declval<Environment&>().event_sink())>;

template<typename Environment>
concept contract_environment
  = runtime_backend<Environment> && environment_lifecycle<Environment>
    && observability::event_sink<environment_sink_type<Environment>>
    && requires(Environment& environment) {
           {
               environment.resource_manager()
           } -> std::same_as<resource::resource_manager&>;
           {
               environment.event_sink().last_sequence()
           } -> std::same_as<std::uint64_t>;
       };

namespace environment_contract_detail {

[[noreturn]] inline void fail(std::string message) {
    throw std::runtime_error(std::move(message));
}

inline void require(bool condition, const char* message) {
    if (!condition) {
        fail(message);
    }
}

template<typename T, typename Driver>
seastar::future<T>
drive_lifecycle(seastar::future<T> operation, Driver& driver) {
    return driver.lifecycle(std::move(operation));
}

template<typename T, typename Driver>
seastar::future<T>
drive_operation(seastar::future<T> operation, Driver& driver) {
    return driver.operation(std::move(operation));
}

template<typename T, typename Driver, typename NativeOwner>
seastar::future<T> drive_operation(
  seastar::future<T> operation, Driver& driver, NativeOwner& native_owner) {
    return driver.operation(std::move(operation), native_owner);
}

} // namespace environment_contract_detail

template<contract_environment Environment, typename Driver>
seastar::future<environment_contract_observation> run_environment_contract(
  Environment& environment,
  environment_component_input input,
  environment_contract_expectation expectation,
  Driver driver) {
    using component_type
      = environment_component<Environment, environment_sink_type<Environment>>;
    const auto admitted_memory = input.memory;
    const auto root_path = input.root_path;
    std::unique_ptr<basic_runtime<Environment>> runtime;
    std::unique_ptr<component_type> component;
    std::optional<environment_contract_observation> observation;
    std::exception_ptr first_failure;
    bool start_attempted = false;

    try {
        environment_contract_detail::require(
          environment.state() == environment_state::constructed,
          "environment contract did not begin constructed");
        environment_contract_detail::require(
          environment.event_sink().last_sequence() == 0,
          "environment contract began with existing events");

        start_attempted = true;
        co_await environment_contract_detail::drive_lifecycle(
          environment.start(), driver);
        environment_contract_detail::require(
          environment.state() == environment_state::started,
          "environment contract did not start");
        environment_contract_detail::require(
          input.memory.value() != 0
            && input.memory <= environment.resource_manager().hard_budget(
                 resource::workload_class::maintenance),
          "environment contract requested invalid component memory");

        runtime = std::make_unique<basic_runtime<Environment>>(environment);
        auto acquired = runtime->template view<
          runtime_capability::timer,
          runtime_capability::random,
          runtime_capability::file_system,
          runtime_capability::network,
          runtime_capability::dns,
          runtime_capability::fault>();
        if (!acquired) {
            environment_contract_detail::fail(
              "environment contract could not acquire its runtime view");
        }
        auto workload = environment.resource_manager().acquire_workload(
          resource::workload_class::maintenance);
        component = std::make_unique<component_type>(
          std::move(*acquired), environment.event_sink(), std::move(workload));
        co_await environment_contract_detail::drive_lifecycle(
          component->start(), driver);
        environment_contract_detail::require(
          component->state() == environment_component_state::started
            && component->resources_cached(),
          "environment component did not cache its resource handles");

        auto component_work = component->run(std::move(input));
        environment_contract_detail::require(
          component_work.available() || component->active_tasks() != 0,
          "environment component has no registered progress owner");
        auto outcome = co_await environment_contract_detail::drive_operation(
          std::move(component_work), driver, *component);
        if (!outcome) {
            environment_contract_detail::fail(
              "environment component returned a typed failure: "
              + outcome.error().render());
        }
        auto result = std::move(*outcome);
        environment_contract_detail::require(
          result.file_read.eof()
            && result.file_read.data().content_equals(
              environment_component_file_payload),
          "environment component changed its file payload");
        environment_contract_detail::require(
          !result.network_request.eof()
            && result.network_request.data().content_equals(
              environment_component_request_payload),
          "environment component changed its network request");
        environment_contract_detail::require(
          !result.network_response.eof()
            && result.network_response.data().content_equals(
              environment_component_response_payload),
          "environment component changed its network response");
        environment_contract_detail::require(
          result.dns.answers() == expectation.dns_answers,
          "environment component changed its DNS result");
        environment_contract_detail::require(
          !expectation.random_word
            || result.random_word == *expectation.random_word,
          "environment component changed its deterministic random result");
        environment_contract_detail::require(
          result.directory_entries == 1,
          "environment component returned the wrong directory contents");
        environment_contract_detail::require(
          result.queue_decision == fault_action::none,
          "environment component unexpectedly injected queue admission");
        environment_contract_detail::require(
          result.admitted_memory == admitted_memory,
          "environment component changed its memory admission");
        environment_contract_detail::require(
          environment.resource_manager().memory_used(
            resource::workload_class::maintenance)
            == byte_count{},
          "environment component retained memory admission after completion");
        const auto root_exists
          = co_await environment_contract_detail::drive_operation(
            environment.file_system().exists(root_path), driver);
        environment_contract_detail::require(
          root_exists.has_value() && !*root_exists,
          "environment component retained its filesystem root");
        environment_contract_detail::require(
          environment.event_sink().last_sequence() == 3,
          "environment component emitted the wrong ready event sequence");
        observation.emplace(
          environment_contract_observation{
            .random_word = result.random_word,
            .file_bytes = result.file_read.data().size(),
            .network_request_bytes = result.network_request.data().size(),
            .network_response_bytes = result.network_response.data().size(),
            .dns_answers = result.dns.answers(),
            .directory_entries = result.directory_entries,
            .queue_decision = result.queue_decision,
            .admitted_memory = result.admitted_memory,
            .file_eof = result.file_read.eof(),
            .network_request_eof = result.network_request.eof(),
            .network_response_eof = result.network_response.eof(),
          });

        co_await environment_contract_detail::drive_lifecycle(
          component->stop(), driver);
        environment_contract_detail::require(
          component->state() == environment_component_state::stopped
            && component->active_tasks() == 0,
          "environment component did not drain its task scope");
        component.reset();
        runtime.reset();

        environment.request_abort();
        co_await environment_contract_detail::drive_lifecycle(
          environment.stop(), driver);
        environment_contract_detail::require(
          environment.state() == environment_state::stopped,
          "environment contract did not stop");
        environment_contract_detail::require(
          environment.resource_manager().state()
            == resource::resource_manager_state::stopped,
          "environment contract retained its resource manager");
        environment_contract_detail::require(
          environment.event_sink().last_sequence() == 5,
          "environment contract emitted the wrong terminal event sequence");
    } catch (...) {
        first_failure = std::current_exception();
    }

    if (first_failure) {
        if (component) {
            component->request_abort();
            try {
                co_await environment_contract_detail::drive_lifecycle(
                  component->stop(), driver);
            } catch (...) {
            }
            component.reset();
        }
        runtime.reset();
        if (start_attempted) {
            environment.request_abort();
            try {
                co_await environment_contract_detail::drive_lifecycle(
                  environment.stop(), driver);
            } catch (...) {
            }
        }
        std::rethrow_exception(first_failure);
    }
    if (!observation) {
        environment_contract_detail::fail(
          "environment contract did not retain its typed observation");
    }
    co_return std::move(*observation);
}

} // namespace kwaque::runtime::testing

#endif // KWAQUE_SRC_RUNTIME_TESTING_CONTRACTS_ENVIRONMENT_CONTRACT_H_
