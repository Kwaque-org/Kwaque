#ifndef KWAQUE_SRC_SIMULATION_DETERMINISTIC_RANDOM_TEST_SUPPORT_H_
#define KWAQUE_SRC_SIMULATION_DETERMINISTIC_RANDOM_TEST_SUPPORT_H_

#include "src/simulation/deterministic_random.h"

#include <cstdint>

namespace kwaque::simulation {

class deterministic_random_test_access final {
public:
    static void set_next_draw(
      sequential_random_source& source, std::uint64_t draw_index) noexcept {
        source.next_draw_ = draw_index;
        source.cache_valid_ = false;
        source.exhausted_ = false;
    }
};

} // namespace kwaque::simulation

#endif // KWAQUE_SRC_SIMULATION_DETERMINISTIC_RANDOM_TEST_SUPPORT_H_
