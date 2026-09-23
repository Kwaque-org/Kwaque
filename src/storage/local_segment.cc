#include "src/storage/local_segment.h"

namespace kwaque::storage {
runtime::result<void> validate_local_segment(
  const local_device_spec& spec,
  std::uint32_t shard,
  const local_segment_descriptor& descriptor,
  segment_header header) {
    if (auto valid = validate_local_device_spec(spec); !valid) return valid;
    if (
      !spec.stores_data() || !spec.shard_owner(shard)
      || descriptor.segment.cluster() != spec.owner.cluster()
      || descriptor.segment != header.context()
      || descriptor.logical_origin != header.logical_origin()
      || descriptor.alignment != header.alignment()
      || descriptor.profile != header.profile())
        return runtime::failure(detail::path_error(errc::wrong_context));
    if (
      descriptor.profile != storage_profile::v1
      || descriptor.record_profile != 1)
        return runtime::failure(detail::path_error(errc::unsupported_format));
    if ((descriptor.layout != local_layout_kind::initial
         && descriptor.layout != local_layout_kind::rewrite)
        || (descriptor.layout == local_layout_kind::initial && descriptor.physical_origin.value() != 0)
        || descriptor.maximum_lifetime.nanoseconds() == 0
        || descriptor.maximum_data_bytes.value() < descriptor.alignment.bytes().value()
             * (descriptor.layout == local_layout_kind::initial ? 4U : 2U))
        return runtime::failure(detail::path_error(errc::invalid_argument));
    return {};
}
} // namespace kwaque::storage
