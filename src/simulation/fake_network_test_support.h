#ifndef KWAQUE_SRC_SIMULATION_FAKE_NETWORK_TEST_SUPPORT_H_
#define KWAQUE_SRC_SIMULATION_FAKE_NETWORK_TEST_SUPPORT_H_

#include "src/simulation/fake_network.h"

namespace kwaque::simulation {

class fake_network_test_access final {
public:
    static void force_discard(
      fake_network& network, const runtime::operation_error& failure) noexcept {
        network.force_discard_for_test(failure);
    }
};

} // namespace kwaque::simulation

#endif // KWAQUE_SRC_SIMULATION_FAKE_NETWORK_TEST_SUPPORT_H_
