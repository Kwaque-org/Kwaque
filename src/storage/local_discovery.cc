#include "src/storage/local_discovery.h"

namespace kwaque::storage {
bool local_discovery_damage(errc code) noexcept {
    return code == errc::unsupported_format || code == errc::malformed_data
           || code == errc::truncated_data || code == errc::corrupt_data
           || code == errc::wrong_context;
}
seastar::future<runtime::result<void>> validate_discovered_file(
  const local_loaded_metadata& loaded, codec::cooperative_work& work) {
    const auto& payload = loaded.value.payload();
    if (
      const auto* wal = std::get_if<local_wal_descriptor>(&payload);
      wal && loaded.file_bytes > wal->capacity_bytes.value())
        co_return runtime::failure(detail::path_error(errc::malformed_data));
    std::optional<std::span<const page_ref>> pages;
    if (const auto* root = std::get_if<local_checkpoint_root>(&payload))
        pages = root->pages;
    if (const auto* root = std::get_if<local_completed_retry_root>(&payload))
        pages = root->pages;
    if (pages) {
        auto bytes = loaded.value.encoded_bytes();
        for (const auto& page : *pages) {
            auto ready = co_await detail::path_checkpoint(work);
            if (!ready) co_return ready;
            auto end = bytes.checked_add(page.encoded_bytes());
            if (!end)
                co_return runtime::failure(
                  detail::path_error(errc::out_of_range));
            bytes = *end;
        }
        if (loaded.file_bytes != bytes.value())
            co_return runtime::failure(
              detail::path_error(errc::malformed_data));
    }
    co_return runtime::result<void>{};
}
runtime::result<void> validate_discovery_expectation(
  const local_device_spec& spec,
  const local_namespace_entry& entry,
  const local_record_expectation& expected) {
    const auto& record = expected.record;
    const auto kind = entry.kind == local_entry_kind::temporary
                        ? entry.temporary_target
                        : entry.kind;
    auto wrong = [] {
        return runtime::failure(detail::path_error(errc::wrong_context));
    };
    const auto owner = kind == local_entry_kind::store
                         ? result<local_store_context>{spec.owner}
                         : spec.shard_owner(entry.shard);
    if (
      !owner || record.header.owner() != *owner
      || (kind != local_entry_kind::wal && record.alignment != spec.identity.metadata_alignment))
        return wrong();
    const auto code = record.header.kind();
    bool matches = false;
    switch (kind) {
    case local_entry_kind::store:
        matches = code == local_metadata_kind::store_identity;
        break;
    case local_entry_kind::control:
        matches = code == local_metadata_kind::shard_control;
        break;
    case local_entry_kind::wal:
        matches = code == local_metadata_kind::wal_descriptor
                  && entry.wal == record.wal_incarnation;
        break;
    case local_entry_kind::descriptor:
        matches = code == local_metadata_kind::segment_descriptor;
        break;
    case local_entry_kind::publication:
        matches = code == local_metadata_kind::object_publication;
        break;
    case local_entry_kind::object:
        matches = code == local_metadata_kind::completed_retry_root;
        break;
    case local_entry_kind::checkpoint:
        matches = code == local_metadata_kind::checkpoint_root;
        break;
    case local_entry_kind::evidence:
        matches = code == local_metadata_kind::boundary_evidence;
        break;
    case local_entry_kind::decision:
        matches = code == local_metadata_kind::recovery_decision;
        break;
    case local_entry_kind::deletion:
        matches = code == local_metadata_kind::deletion_intent;
        break;
    default:
        break;
    }
    if (!matches) return wrong();
    if (entry.sequence && record.header.generation().value() != entry.sequence)
        return wrong();
    if (
      entry.temporary
      && record.header.generation() != entry.temporary->generation)
        return wrong();
    if (entry.segment && (!record.segment || record.segment->segment() != entry.segment->segment
        || record.segment->generation() != entry.segment->generation))
        return wrong();
    if (
      expected.select_current
      && (entry.temporary || (kind != local_entry_kind::control && kind != local_entry_kind::publication)))
        return runtime::failure(detail::path_error(errc::invalid_argument));
    if (expected.select_current &&
        (record.digest || record.encoded_bytes || record.page || record.wal_incarnation
         || record.data_device || record.data_metadata_alignment || record.previous_retry
         || record.previous_checkpoint_end
         || (kind == local_entry_kind::control && (record.segment || record.segment_alignment))))
        return runtime::failure(detail::path_error(errc::invalid_argument));
    if (
      expected.select_current && kind == local_entry_kind::publication
      && (!record.segment || !record.segment_alignment))
        return runtime::failure(detail::path_error(errc::invalid_argument));
    return {};
}
} // namespace kwaque::storage
