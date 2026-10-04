#include "src/storage/recovery_seal.h"

namespace kwaque::storage::detail {

runtime::result<runtime::file_position> recovered_seal_end(
  const local_recovery_decision& decision,
  const local_segment_descriptor& descriptor) noexcept {
    if (
      decision.action != local_recovery_action::seal_at
      || decision.segment != descriptor.segment
      || !descriptor.alignment.aligned(decision.target_position))
        return runtime::failure(path_error(errc::wrong_context));
    return decision.target_position;
}

bool recovered_layout_failure(errc code) noexcept {
    switch (code) {
    case errc::wrong_context:
    case errc::malformed_data:
    case errc::out_of_range:
    case errc::truncated_data:
    case errc::corrupt_data:
        return true;
    default:
        return false;
    }
}

} // namespace kwaque::storage::detail
