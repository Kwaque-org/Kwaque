#ifndef KWAQUE_SRC_OBSERVABILITY_TESTING_EVENT_SEQUENCE_TEST_ACCESS_H_
#define KWAQUE_SRC_OBSERVABILITY_TESTING_EVENT_SEQUENCE_TEST_ACCESS_H_

#include "src/observability/event_sequence.h"

#include <cstdint>
#include <memory>

namespace kwaque::observability {

class event_sequence_test_access final {
public:
    [[nodiscard]] static std::unique_ptr<event_sequence>
    make(event_sink_identity identity) {
        return std::unique_ptr<event_sequence>{new event_sequence{identity}};
    }

    [[nodiscard]] static runtime::result<event_sequence::reservation>
    prepare(event_sequence& sequence, const event_request& request) noexcept {
        return sequence.prepare(request);
    }

    static void
    set_last_sequence(event_sequence& sequence, std::uint64_t value) noexcept {
        sequence.last_sequence_ = value;
    }
};

} // namespace kwaque::observability

#endif // KWAQUE_SRC_OBSERVABILITY_TESTING_EVENT_SEQUENCE_TEST_ACCESS_H_
