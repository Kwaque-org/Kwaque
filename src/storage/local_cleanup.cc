#include "src/storage/local_cleanup.h"

namespace kwaque::storage {
local_cleanup_observation classify_local_cleanup(
  const local_discovered_record& record,
  local_cleanup_references facts) noexcept {
    const auto kind = record.entry.kind;
    const auto target = kind == local_entry_kind::temporary
                          ? record.entry.temporary_target
                          : kind;
    if (facts.persisted == local_reference_status::referenced)
        return {local_cleanup_class::referenced, false};
    if (facts.live_pins) return {local_cleanup_class::reader_pinned, false};
    if (record.entry.unexpected_kind)
        return {local_cleanup_class::damaged, true};
    if (record.damage)
        return {
          record.damage->code() == errc::unsupported_format
            ? local_cleanup_class::unsupported
            : local_cleanup_class::damaged,
          true};
    if (kind == local_entry_kind::unknown)
        return {local_cleanup_class::unsupported, true};
    if (kind == local_entry_kind::broker_file)
        return {local_cleanup_class::external, false};
    if (facts.recovery_candidate)
        return {local_cleanup_class::recovery_required, true};
    if (target == local_entry_kind::wal) {
        const auto envelope = record.decoded
                                ? record.decoded->value.encoded_bytes().value()
                              : record.claims
                                ? record.claims->encoded_bytes.value()
                                : 0;
        if (!envelope || record.file_bytes != envelope)
            return {local_cleanup_class::recovery_required, true};
    }
    if (
      kind != local_entry_kind::temporary
      && (kind == local_entry_kind::store || kind == local_entry_kind::control || kind == local_entry_kind::descriptor || kind == local_entry_kind::data || kind == local_entry_kind::publication || kind == local_entry_kind::directory || kind == local_entry_kind::segment_directory))
        return {local_cleanup_class::structural, false};
    if (facts.persisted != local_reference_status::absent)
        return {local_cleanup_class::unresolved, true};
    if (!record.decoded && !record.claims)
        return {local_cleanup_class::unresolved, true};
    if (
      (target == local_entry_kind::object
       || target == local_entry_kind::checkpoint)
      && !facts.complete_content)
        return {local_cleanup_class::unresolved, true};
    return {
      kind == local_entry_kind::temporary
        ? local_cleanup_class::temporary_debt
        : local_cleanup_class::orphan_candidate,
      true};
}
} // namespace kwaque::storage
