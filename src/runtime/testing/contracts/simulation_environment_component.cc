#include "src/runtime/testing/contracts/environment_contract.h"
#include "src/simulation/environment.h"
#include "src/simulation/event_sink.h"

namespace kwaque::runtime::testing {

namespace {

struct simulation_contract_compile_driver final {
    template<typename T>
    seastar::future<T> lifecycle(seastar::future<T> waiting) const {
        return waiting;
    }

    template<typename T>
    seastar::future<T> operation(seastar::future<T> waiting) const {
        return waiting;
    }

    template<typename T, typename NativeOwner>
    seastar::future<T>
    operation(seastar::future<T> waiting, NativeOwner&) const {
        return waiting;
    }
};

} // namespace

template class environment_component<
  simulation::environment,
  simulation::event_log_sink>;
template seastar::future<environment_contract_observation>
run_environment_contract<
  simulation::environment,
  simulation_contract_compile_driver>(
  simulation::environment&,
  environment_component_input,
  environment_contract_expectation,
  simulation_contract_compile_driver);

} // namespace kwaque::runtime::testing
