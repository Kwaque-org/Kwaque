#ifndef KWAQUE_SRC_RUNTIME_PRODUCTION_BACKEND_TEST_SUPPORT_H_
#define KWAQUE_SRC_RUNTIME_PRODUCTION_BACKEND_TEST_SUPPORT_H_

#include "src/runtime/production/backend.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/net/dns.hh>

#include <cstddef>
#include <memory>
#include <stdexcept>
#include <utility>

namespace kwaque::runtime::production {

class backend_test_access final {
public:
    static constexpr std::size_t start_point_count = 5;

    [[nodiscard]] static std::unique_ptr<backend>
    make(seastar::abort_source& parent_abort) {
        return std::make_unique<backend>(backend_dependencies{parent_abort});
    }

    [[nodiscard]] static std::unique_ptr<backend> make(
      seastar::abort_source& parent_abort,
      seastar::net::dns_resolver::options dns_options) {
        return std::make_unique<backend>(
          backend_dependencies{parent_abort, std::move(dns_options)});
    }

    [[nodiscard]] static seastar::future<>
    fail_before_start_point(backend& target, std::size_t failure_point) {
        return target.start_with([failure_point](std::size_t current) {
            if (current == failure_point) {
                throw std::runtime_error("injected backend startup failure");
            }
        });
    }

    [[nodiscard]] static bool components_released(const backend& target) {
        return !target.random_ && !target.timer_ && !target.file_system_
               && !target.network_ && !target.dns_ && !target.dns_options_;
    }
};

} // namespace kwaque::runtime::production

#endif // KWAQUE_SRC_RUNTIME_PRODUCTION_BACKEND_TEST_SUPPORT_H_
