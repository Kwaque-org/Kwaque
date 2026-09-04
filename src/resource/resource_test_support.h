#pragma once

#include "src/resource/resource_manager.h"
#include "src/resource/resource_registry.h"

#include <seastar/core/future.hh>

#include <cstddef>
#include <stdexcept>
#include <utility>

namespace kwaque::resource {

class resource_registry_test_access final {
public:
    static constexpr std::size_t creation_point_count = workload_class_count
                                                        * 2;

    [[nodiscard]] static seastar::future<> fail_before_creation(
      resource_registry& registry, resource_config config, std::size_t point) {
        return start_with(
          registry, std::move(config), [point](std::size_t current) {
              if (current == point) {
                  throw std::runtime_error(
                    "injected resource group creation failure");
              }
          });
    }

    template<typename Checkpoint>
    [[nodiscard]] static seastar::future<> start_with(
      resource_registry& registry,
      resource_config config,
      Checkpoint checkpoint) {
        return registry.start_with(std::move(config), std::move(checkpoint));
    }
};

class resource_manager_test_access final {
public:
    static constexpr std::size_t start_point_count = workload_class_count;

    [[nodiscard]] static seastar::future<>
    fail_before_start_point(resource_manager& manager, std::size_t point) {
        return manager.start_with([point](std::size_t current) {
            if (current == point) {
                throw std::runtime_error(
                  "injected resource manager start failure");
            }
        });
    }
};

} // namespace kwaque::resource
