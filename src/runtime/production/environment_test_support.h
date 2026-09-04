#ifndef KWAQUE_SRC_RUNTIME_PRODUCTION_ENVIRONMENT_TEST_SUPPORT_H_
#define KWAQUE_SRC_RUNTIME_PRODUCTION_ENVIRONMENT_TEST_SUPPORT_H_

#include "src/runtime/production/environment.h"

#include <seastar/core/future.hh>

#include <cstddef>
#include <stdexcept>

namespace kwaque::runtime::production {

class environment_test_access final {
public:
    static constexpr std::size_t start_point_count{9};

    [[nodiscard]] static seastar::future<>
    fail_before_start_point(environment& target, std::size_t failure_point) {
        return target.start_with([failure_point](std::size_t current) {
            if (current == failure_point) {
                throw std::runtime_error(
                  "injected production environment startup failure");
            }
        });
    }

    [[nodiscard]] static bool components_released(const environment& target) {
        return !target.random_ && !target.timer_ && !target.file_system_
               && !target.network_ && !target.dns_ && !target.metrics_
               && !target.dns_options_ && !target.tasks_
               && target.resource_manager_.state()
                    == resource::resource_manager_state::stopped
               && target.event_sink_.stopped();
    }
};

} // namespace kwaque::runtime::production

#endif // KWAQUE_SRC_RUNTIME_PRODUCTION_ENVIRONMENT_TEST_SUPPORT_H_
