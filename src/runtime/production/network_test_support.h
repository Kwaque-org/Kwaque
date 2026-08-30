#ifndef KWAQUE_SRC_RUNTIME_PRODUCTION_NETWORK_TEST_SUPPORT_H_
#define KWAQUE_SRC_RUNTIME_PRODUCTION_NETWORK_TEST_SUPPORT_H_

#include "src/runtime/production/network.h"

#include <seastar/core/semaphore.hh>

#include <cstdint>
#include <optional>

namespace kwaque::runtime::production {

class network_test_access final {
public:
    [[nodiscard]] static std::optional<seastar::semaphore_units<>>
    hold_write_serializer(connection& target) {
        target.owner_.assert_current();
        return seastar::try_get_units(target.write_serializer_, 1);
    }

    static void set_unflushed_bytes(connection& target, std::uint64_t bytes) {
        target.owner_.assert_current();
        target.unflushed_bytes_ = bytes;
    }

    [[nodiscard]] static std::uint64_t
    unflushed_bytes(const connection& target) {
        target.owner_.assert_current();
        return target.unflushed_bytes_;
    }
};

} // namespace kwaque::runtime::production

#endif // KWAQUE_SRC_RUNTIME_PRODUCTION_NETWORK_TEST_SUPPORT_H_
