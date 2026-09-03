#include "src/observability/event_identity.h"

namespace kwaque::observability {

runtime::result<event_sink_epoch>
event_sink_epoch::make(std::uint64_t value) noexcept {
    if (value == 0) {
        return runtime::failure(
          runtime::operation_error{
            errc::invalid_argument, runtime::operation_kind::observability});
    }
    return event_sink_epoch{value};
}

} // namespace kwaque::observability
