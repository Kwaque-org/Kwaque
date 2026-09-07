#ifndef KWAQUE_SRC_SIMULATION_TESTS_FUZZ_NETWORK_CASES_H_
#define KWAQUE_SRC_SIMULATION_TESTS_FUZZ_NETWORK_CASES_H_

#include "src/simulation/fake_network.h"
#include "src/simulation/scheduler.h"

#include <seastar/core/abort_source.hh>

#include <string_view>

namespace kwaque::simulation::testing {

// Called on a Seastar thread after the sender has published its output FIN.
// Reads through EOF, including when the expected payload has been exhausted.
[[nodiscard]] bool network_stream_is_exact(
  scheduler& events,
  fake_connection& receiver,
  std::string_view expected,
  seastar::abort_source& abort_source);

} // namespace kwaque::simulation::testing

#endif // KWAQUE_SRC_SIMULATION_TESTS_FUZZ_NETWORK_CASES_H_
