#pragma once

#include "src/resource/resource_manager.h"
#include "src/resource/resource_registry.h"

#include <cstddef>

namespace kwaque::resource {

class resource_registry_test_access final {
public:
    static constexpr std::size_t creation_point_count = workload_class_count
                                                        * 2;

    static void fail_before_creation(
      resource_registry& registry, std::size_t point) noexcept {
        registry.fail_before_creation_ = point;
    }
};

class resource_manager_test_access final {
public:
    static constexpr std::size_t start_point_count = workload_class_count;

    static void fail_before_start_point(
      resource_manager& manager, std::size_t point) noexcept {
        manager.fail_before_start_point_ = point;
    }
};

} // namespace kwaque::resource
