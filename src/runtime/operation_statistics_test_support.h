#ifndef KWAQUE_SRC_RUNTIME_OPERATION_STATISTICS_TEST_SUPPORT_H_
#define KWAQUE_SRC_RUNTIME_OPERATION_STATISTICS_TEST_SUPPORT_H_

#include "src/runtime/operation_statistics.h"

namespace kwaque::runtime {

class operation_statistics_test_access final {
public:
    static void seed(
      operation_statistics& statistics,
      operation_statistics_snapshot values) noexcept {
        statistics.values_ = values;
    }
};

} // namespace kwaque::runtime

#endif // KWAQUE_SRC_RUNTIME_OPERATION_STATISTICS_TEST_SUPPORT_H_
