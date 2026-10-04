#include "src/storage/recovery_inventory.h"

namespace kwaque::storage {

std::optional<recovery_store_verdict>
recovery_failure_verdict(errc code) noexcept {
    switch (code) {
    case errc::unsupported_format:
        return recovery_store_verdict::unsupported;
    case errc::malformed_data:
    case errc::corrupt_data:
    case errc::wrong_context:
    case errc::truncated_data:
    case errc::is_a_directory:
    case errc::not_a_directory:
        return recovery_store_verdict::corrupt;
    case errc::not_found:
        return recovery_store_verdict::lost;
    case errc::io_failure:
    case errc::unavailable:
    case errc::permission_denied:
    case errc::timed_out:
    case errc::fault_injected:
        return recovery_store_verdict::unavailable;
    default:
        return std::nullopt;
    }
}

recovery_store_verdict
recovery_state_verdict(local_store_state state) noexcept {
    switch (state) {
    case local_store_state::existing:
        return recovery_store_verdict::ready;
    case local_store_state::pristine:
        return recovery_store_verdict::lost;
    case local_store_state::incomplete:
        return recovery_store_verdict::incomplete;
    case local_store_state::unsupported:
        return recovery_store_verdict::unsupported;
    case local_store_state::corrupt:
        return recovery_store_verdict::corrupt;
    }
    return recovery_store_verdict::corrupt;
}

namespace detail {
runtime::result<std::optional<recovery_decision_pin>> pending_recovery_seal(
  const local_object_publication& publication,
  std::span<const recovery_decision_record> decisions) noexcept {
    const recovery_decision_record* found = nullptr;
    for (const auto& stored : decisions) {
        const auto& value = stored.value;
        if (
          value.segment != publication.segment
          || value.action != local_recovery_action::seal_at)
            continue;
        if (!found) {
            found = &stored;
            continue;
        }
        // One segment is sealed at one end, by one owner decision.
        if (
          value.target_position != found->value.target_position
          || value.owner_decision_id != found->value.owner_decision_id
          || value.prepare_digest != found->value.prepare_digest)
            return runtime::failure(path_error(errc::wrong_context));
        if (stored.pin.sequence < found->pin.sequence) found = &stored;
    }
    if (!found) return std::optional<recovery_decision_pin>{};
    switch (publication.state) {
    case local_object_state::active:
    case local_object_state::recovering:
        return std::optional{found->pin};
    case local_object_state::sealed:
        // An earlier attempt finished at the decided end.
        if (
          publication.boundary
          && publication.boundary->position() == found->value.target_position)
            return std::optional<recovery_decision_pin>{};
        return runtime::failure(path_error(errc::wrong_context));
    case local_object_state::deleting:
        return std::optional<recovery_decision_pin>{};
    }
    return runtime::failure(path_error(errc::wrong_context));
}
} // namespace detail

} // namespace kwaque::storage
