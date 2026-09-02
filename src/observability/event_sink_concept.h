#ifndef KWAQUE_SRC_OBSERVABILITY_EVENT_SINK_CONCEPT_H_
#define KWAQUE_SRC_OBSERVABILITY_EVENT_SINK_CONCEPT_H_

#include "src/observability/event.h"
#include "src/runtime/error.h"

#include <concepts>

namespace kwaque::observability {

template<typename Sink>
concept event_sink = requires(Sink& sink, const event_request& request) {
    { sink.emit(request) } noexcept -> std::same_as<runtime::result<void>>;
    { sink.stop() } noexcept -> std::same_as<runtime::result<void>>;
};

} // namespace kwaque::observability

#endif // KWAQUE_SRC_OBSERVABILITY_EVENT_SINK_CONCEPT_H_
