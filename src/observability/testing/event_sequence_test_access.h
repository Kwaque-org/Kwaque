#ifndef KWAQUE_SRC_OBSERVABILITY_TESTING_EVENT_SEQUENCE_TEST_ACCESS_H_
#define KWAQUE_SRC_OBSERVABILITY_TESTING_EVENT_SEQUENCE_TEST_ACCESS_H_

#include "src/observability/event_sequence.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <utility>

namespace kwaque::observability {

class event_sequence_test_access final {
public:
    [[nodiscard]] static std::unique_ptr<event_sequence>
    make(event_sink_identity identity) {
        return std::unique_ptr<event_sequence>{new event_sequence{identity}};
    }

    // For a kind with no rate bound, which always yields a reservation.
    [[nodiscard]] static runtime::result<event_sequence::reservation>
    prepare(event_sequence& sequence, const event_request& request) noexcept {
        auto prepared = sequence.prepare(request);
        if (!prepared) {
            return runtime::failure(prepared.error());
        }
        if (!*prepared) {
            return runtime::failure(
              runtime::operation_error{
                errc::invalid_argument,
                runtime::operation_kind::observability});
        }
        return std::move(**prepared);
    }

    // No reservation means the rate bound dropped the event.
    [[nodiscard]] static runtime::result<
      std::optional<event_sequence::reservation>>
    prepare_bounded(
      event_sequence& sequence, const event_request& request) noexcept {
        return sequence.prepare(request);
    }

    static void
    set_last_sequence(event_sequence& sequence, std::uint64_t value) noexcept {
        sequence.last_sequence_ = value;
    }
};

} // namespace kwaque::observability

#endif // KWAQUE_SRC_OBSERVABILITY_TESTING_EVENT_SEQUENCE_TEST_ACCESS_H_
