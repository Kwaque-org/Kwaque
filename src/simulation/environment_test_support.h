#ifndef KWAQUE_SRC_SIMULATION_ENVIRONMENT_TEST_SUPPORT_H_
#define KWAQUE_SRC_SIMULATION_ENVIRONMENT_TEST_SUPPORT_H_

#include "src/simulation/environment.h"

#include <seastar/core/future.hh>

#include <cstddef>
#include <new>

namespace kwaque::simulation {

class environment_test_access final {
public:
    static constexpr std::size_t start_point_count{8};

    [[nodiscard]] static seastar::future<>
    fail_before_start_point(environment& target, std::size_t failure_point) {
        return target.start_with([failure_point](std::size_t current) {
            if (current == failure_point) {
                throw std::bad_alloc{};
            }
        });
    }

    [[nodiscard]] static timer& timer_owner(environment& target) {
        target.assert_current();
        return *target.timer_;
    }
    [[nodiscard]] static sequential_random_source&
    random_owner(environment& target) {
        target.assert_current();
        return target.runtime_random_;
    }
    [[nodiscard]] static fake_file_system&
    file_system_owner(environment& target) {
        target.assert_current();
        return *target.files_;
    }
    [[nodiscard]] static fake_network& network_owner(environment& target) {
        target.assert_current();
        return *target.network_;
    }
    [[nodiscard]] static fake_dns& dns_owner(environment& target) {
        target.assert_current();
        return *target.dns_;
    }
};

} // namespace kwaque::simulation

#endif // KWAQUE_SRC_SIMULATION_ENVIRONMENT_TEST_SUPPORT_H_
