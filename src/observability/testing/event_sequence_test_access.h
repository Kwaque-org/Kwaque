#ifndef KWAQUE_SRC_OBSERVABILITY_TESTING_EVENT_SEQUENCE_TEST_ACCESS_H_
#define KWAQUE_SRC_OBSERVABILITY_TESTING_EVENT_SEQUENCE_TEST_ACCESS_H_

#include "src/observability/event_sequence.h"

#include <cstdint>

namespace kwaque::observability {

class event_sequence_test_access final {
public:
    static void
    set_last_sequence(event_sequence& sequence, std::uint64_t value) noexcept {
        sequence.last_sequence_ = value;
    }
};

} // namespace kwaque::observability

#endif // KWAQUE_SRC_OBSERVABILITY_TESTING_EVENT_SEQUENCE_TEST_ACCESS_H_
