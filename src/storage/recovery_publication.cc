#include "src/storage/recovery_publication.h"

namespace kwaque::storage {

recovery_read_bound bound_recovered_reads(
  model::range_logical_end origin,
  model::range_logical_end recovered,
  std::optional<model::range_logical_end> visible) noexcept {
    if (!visible) return {origin, false};
    return {
      std::max(origin, std::min(*visible, recovered)), *visible > recovered};
}

runtime::result<void> recovered_publication_limits::validate() const noexcept {
    if (jobs == 0 || jobs > maximum_recovery_jobs)
        return runtime::failure(detail::path_error(errc::invalid_argument));
    return {};
}

} // namespace kwaque::storage
