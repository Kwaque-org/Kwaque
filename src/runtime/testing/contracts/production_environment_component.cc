#include "src/observability/event_sink.h"
#include "src/runtime/production/environment.h"
#include "src/runtime/testing/contracts/environment_contract.h"

namespace kwaque::runtime::testing {

namespace {

struct production_contract_compile_driver final {
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
  production::environment,
  observability::production_event_sink>;
template seastar::future<environment_contract_observation>
run_environment_contract<
  production::environment,
  production_contract_compile_driver>(
  production::environment&,
  environment_component_input,
  environment_contract_expectation,
  production_contract_compile_driver);

} // namespace kwaque::runtime::testing
