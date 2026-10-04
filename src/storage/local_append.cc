#include "src/storage/local_append.h"

#include "src/base/units.h"

namespace kwaque::storage {

runtime::result<void> local_append_config::validate() const noexcept {
    if (
      maximum_segments == 0 || maximum_segments > maximum_local_append_segments
      || maximum_requests == 0
      || maximum_requests > maximum_local_append_requests
      || execution_bytes.value() < 4_KiB
      || execution_bytes.value() > maximum_contiguous_allocation_bytes)
        return runtime::failure(
          runtime::operation_error{
            errc::invalid_argument, runtime::operation_kind::file});
    return {};
}

namespace detail {
local_append_item::local_append_item(
  workload_reservation held,
  workload_reservation fact,
  wal_prepared_children children,
  std::uint32_t slot,
  runtime::monotonic_time accepted)
  : held(std::move(held))
  , fact(std::move(fact))
  , children(std::move(children))
  , info(this->children.wal.batch().info())
  , slot(slot)
  , accepted(accepted) {}

void merge_failure(
  runtime::first_failure& into, const runtime::first_failure& from) noexcept {
    if (from.exception())
        into.observe(from.exception());
    else if (from.error())
        into.observe(*from.error());
}
} // namespace detail

} // namespace kwaque::storage
