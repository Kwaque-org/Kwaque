#include "src/simulation/event_sink.h"

namespace kwaque::simulation {

namespace {

[[nodiscard]] runtime::operation_error sink_error(errc code) noexcept {
    return runtime::operation_error{
      code, runtime::operation_kind::observability};
}

} // namespace

runtime::result<void>
event_log_sink::emit(const observability::event_request& request) noexcept {
    assert_current();
    if (stopped_) {
        return runtime::failure(sink_error(errc::closed));
    }
    auto prepared = sequence_.prepare(
      request, observability::event_shard::from_owner(owner()));
    if (!prepared) {
        return runtime::failure(prepared.error());
    }
    if (auto appended = events_.append(prepared->value()); !appended) {
        return runtime::failure(appended.error());
    }
    prepared->commit();
    return {};
}

runtime::result<void> event_log_sink::stop() noexcept {
    assert_current();
    stopped_ = true;
    return {};
}

} // namespace kwaque::simulation
