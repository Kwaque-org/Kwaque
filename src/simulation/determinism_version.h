#ifndef KWAQUE_SRC_SIMULATION_DETERMINISM_VERSION_H_
#define KWAQUE_SRC_SIMULATION_DETERMINISM_VERSION_H_

#include <cstdint>

namespace kwaque::simulation {

inline constexpr std::uint32_t deterministic_random_algorithm_version{1};
inline constexpr std::uint32_t deterministic_random_coordinate_version{1};

} // namespace kwaque::simulation

#endif // KWAQUE_SRC_SIMULATION_DETERMINISM_VERSION_H_
