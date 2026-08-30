#ifndef KWAQUE_SRC_SIMULATION_VIRTUAL_TIME_TEST_SUPPORT_H_
#define KWAQUE_SRC_SIMULATION_VIRTUAL_TIME_TEST_SUPPORT_H_

#include "src/simulation/virtual_time.h"

namespace kwaque::simulation {

class virtual_time_test_access final {
public:
    [[nodiscard]] static bool clock_is_bound() noexcept {
        return virtual_time::active_ != nullptr;
    }
};

} // namespace kwaque::simulation

#endif // KWAQUE_SRC_SIMULATION_VIRTUAL_TIME_TEST_SUPPORT_H_
