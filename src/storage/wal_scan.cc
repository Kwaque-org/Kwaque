#include "src/storage/wal_scan.h"

namespace kwaque::storage {

wal_target make_wal_target(const local_located_segment& located) noexcept {
    const auto& header = located.loaded.header;
    return {
      header.value.context(),
      header.value.alignment(),
      header.bytes.end(),
      header.value.profile(),
      located.device};
}

runtime::result<void> wal_scan_limits::validate() const noexcept {
    if (auto valid = metadata.validate(); !valid) return valid;
    if (auto valid = reader.validate(); !valid) return valid;
    if (
      working_bytes.value() == 0 || decode_metadata_bytes.value() == 0
      || !working_bytes.checked_add(reader.maximum_object_bytes))
        return runtime::failure(detail::path_error(errc::invalid_argument));
    return {};
}

namespace detail {
// Envelope and contextual decode failures of an integrity-checked object.
// Only foreign identity is decided before this, by the resolution itself.
wal_scan_stop wal_decode_stop(const codec::error& error) noexcept {
    switch (error.code()) {
    case errc::corrupt_data:
        return wal_scan_stop::corrupt;
    case errc::wrong_context:
        return wal_scan_stop::foreign;
    default:
        return wal_scan_stop::malformed;
    }
}
} // namespace detail

} // namespace kwaque::storage
