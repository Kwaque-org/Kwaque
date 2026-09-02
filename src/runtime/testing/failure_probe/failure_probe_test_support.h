#ifndef KWAQUE_SRC_RUNTIME_TESTING_FAILURE_PROBE_FAILURE_PROBE_TEST_SUPPORT_H_
#define KWAQUE_SRC_RUNTIME_TESTING_FAILURE_PROBE_FAILURE_PROBE_TEST_SUPPORT_H_

#include "src/runtime/testing/failure_probe/failure_probe.h"

#include <cstdint>

namespace kwaque::runtime::testing {

class failure_probe_test_access final {
public:
    static result<void> seed(
      failure_probe& probe,
      builtin_fault_point point,
      std::uint64_t occurrence) noexcept {
        const auto index = failure_probe_point_index(point);
        if (!index) {
            return failure(
              operation_error{errc::invalid_argument, operation_kind::fault});
        }
        probe.occurrences_[*index] = occurrence;
        return {};
    }
};

} // namespace kwaque::runtime::testing

#endif // KWAQUE_SRC_RUNTIME_TESTING_FAILURE_PROBE_FAILURE_PROBE_TEST_SUPPORT_H_
