#ifndef KWAQUE_SRC_SIMULATION_SCHEDULER_TEST_SUPPORT_H_
#define KWAQUE_SRC_SIMULATION_SCHEDULER_TEST_SUPPORT_H_

#include "src/simulation/scheduler.h"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>

namespace kwaque::simulation {

class scheduler_test_access final {
public:
    static void use_final_event_id(scheduler& target) noexcept {
        target.assert_current();
        target.next_event_id_ = std::numeric_limits<std::uint64_t>::max();
        target.event_ids_exhausted_ = false;
    }

    [[nodiscard]] static std::optional<event_id>
    event_at(const scheduler& target, std::size_t index) {
        target.assert_current();
        if (index >= target.pending_events()) {
            return std::nullopt;
        }
        return target.heap_[index + 1U].id;
    }
};

} // namespace kwaque::simulation

#endif // KWAQUE_SRC_SIMULATION_SCHEDULER_TEST_SUPPORT_H_
