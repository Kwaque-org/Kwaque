#pragma once

#include "src/storage/local_namespace.h"
#include "src/storage/local_segment.h"

namespace kwaque::storage {
struct local_record_expectation final {
    local_metadata_expectation record;
    // Only fixed final control/published paths may select a mutable generation.
    bool select_current{false};
};
struct local_discovered_record final {
    const local_namespace_entry& entry;
    std::optional<local_metadata_claims> claims;
    std::optional<local_loaded_metadata> decoded;
    std::uint64_t file_bytes{0};
    std::optional<runtime::operation_error> damage;
};
[[nodiscard]] bool local_discovery_damage(errc) noexcept;
[[nodiscard]] seastar::future<runtime::result<void>> validate_discovered_file(
  const local_loaded_metadata&, codec::cooperative_work&);
[[nodiscard]] runtime::result<void> validate_discovery_expectation(
  const local_device_spec&,
  const local_namespace_entry&,
  const local_record_expectation&);

// The resolver receives only the deterministic namespace key. Expectations
// must come from owned configuration/catalog/control/checkpoint state, not
// from the bytes being authenticated. Nullopt requests claims-only observation.
// Each output is borrowed through a joined callback; keep/copy nothing past it
// without an independent owning reservation. No whole-store inventory remains.
template<
  runtime::file_system_backend Backend,
  local_directory_owner Owner,
  typename Resolver,
  typename Visitor>
seastar::future<runtime::result<local_inventory_progress>>
discover_local_records(
  Backend& files,
  Owner& ownership,
  const local_device_spec& spec,
  workload_budget& budget,
  local_store_io_limits limits,
  codec::cooperative_work& work,
  Resolver resolve,
  Visitor visit,
  local_discovery_limits bounds = {}) {
    static_assert(sizeof(Resolver) + sizeof(Visitor) <= 8192);
    auto inspect = [&](const local_namespace_entry& entry)
      -> seastar::future<runtime::result<bool>> {
        local_discovered_record observation{entry};
        const auto kind = entry.kind == local_entry_kind::temporary
                            ? entry.temporary_target
                            : entry.kind;
        if (
          entry.unexpected_kind
          || entry.file_kind != runtime::file_kind::regular
          || kind == local_entry_kind::unknown || kind == local_entry_kind::data
          || kind == local_entry_kind::broker_file)
            co_return co_await visit(observation);
        auto selected = co_await resolve(entry);
        if (!selected) co_return runtime::failure(selected.error());
        if (*selected) {
            auto checked = validate_discovery_expectation(
              spec, entry, **selected);
            if (!checked) co_return runtime::failure(checked.error());
            auto expected = **selected;
            auto loaded = co_await load_local_metadata_file(
              files,
              spec,
              entry.path,
              budget,
              limits,
              work,
              (kind == local_entry_kind::wal
               || kind == local_entry_kind::checkpoint
               || kind == local_entry_kind::object)
                ? local_metadata_extent::first_envelope
                : local_metadata_extent::exact_file,
              [expected](
                auto& input,
                byte_count,
                codec::decode_budget memory,
                auto& work) {
                  const auto& record = expected.record;
                  if (expected.select_current) {
                      if (
                        record.header.kind()
                        == local_metadata_kind::shard_control)
                          return decode_selected_shard_control(
                            input,
                            record.header.owner(),
                            record.alignment,
                            memory,
                            work,
                            {},
                            codec::input_boundary::complete);
                      return decode_selected_object_publication(
                        input,
                        record.header.owner(),
                        *record.segment,
                        record.alignment,
                        *record.segment_alignment,
                        memory,
                        work,
                        {},
                        codec::input_boundary::complete);
                  }
                  return decode_local_metadata(
                    input,
                    record,
                    memory,
                    work,
                    {},
                    codec::input_boundary::complete);
              });
            if (!loaded) {
                if (!local_discovery_damage(loaded.error().code()))
                    co_return runtime::failure(loaded.error());
                observation.damage = loaded.error();
            } else {
                observation.file_bytes = loaded->file_bytes;
                auto complete_extent = co_await validate_discovered_file(
                  *loaded, work);
                if (!complete_extent) {
                    if (!local_discovery_damage(complete_extent.error().code()))
                        co_return runtime::failure(complete_extent.error());
                    observation.damage = complete_extent.error();
                } else
                    observation.decoded.emplace(std::move(*loaded));
            }
        } else if (kind != local_entry_kind::object) {
            // Objects may be a family-8 root, family-7 page-only file,
            // including zero bytes, or family-11 bundle. Without an independent
            // reference do not guess which encoding gives those bytes
            // authority.
            auto raw = co_await read_local_metadata_file(
              files,
              spec.root,
              entry.path,
              budget,
              limits,
              work,
              (kind == local_entry_kind::wal
               || kind == local_entry_kind::checkpoint)
                ? local_metadata_extent::first_envelope
                : local_metadata_extent::exact_file);
            if (!raw) {
                if (!local_discovery_damage(raw.error().code()))
                    co_return runtime::failure(raw.error());
                observation.damage = raw.error();
            } else {
                observation.file_bytes = raw->file_bytes;
                bytes::fragmented_buffer_parser input{std::move(raw->bytes)};
                auto memory = detail::metadata_file_budget(input, limits, work);
                if (!memory)
                    co_return runtime::failure(
                      detail::path_error(memory.error().code()));
                auto claims = co_await probe_local_metadata(
                  input,
                  spec.identity.metadata_alignment,
                  *memory,
                  work,
                  {},
                  codec::input_boundary::complete);
                if (!claims) {
                    if (!local_discovery_damage(claims.error().code()))
                        co_return runtime::failure(
                          detail::path_error(claims.error().code()));
                    observation.damage = detail::path_error(
                      claims.error().code());
                } else {
                    // Independently enforce deterministic owner/path bindings
                    // without turning these claims into a verified catalog.
                    local_record_expectation context{
                      {claims->header, claims->alignment}};
                    context.record.segment = claims->segment;
                    context.record.wal_incarnation = claims->wal_incarnation;
                    auto matched = validate_discovery_expectation(
                      spec, entry, context);
                    if (!matched) observation.damage = matched.error();
                    observation.claims = *claims;
                }
            }
        }
        co_return co_await visit(observation);
    };
    co_return co_await walk_local_namespace(
      files, ownership, spec, budget, limits, work, std::move(inspect), bounds);
}

struct local_wal_chain_entry final {
    const local_loaded_metadata& record;
    // Only the first header has an exact external SHA in shard_control. Earlier
    // links are contextual CRC-checked headers selected by predecessor
    // identity.
    bool head_digest_pinned;
    std::optional<runtime::file_position> sealed_end;
};

// Only the pinned head chooses the chain. A supplied cutoff is independently
// checkpoint-covered; this function checks membership/bounds, not proof. No
// PREPARE replay, valid-tail repair or adjacent-ID requirement is performed.
template<
  runtime::file_system_backend Backend,
  local_directory_owner Owner,
  typename Visitor>
seastar::future<runtime::result<local_inventory_progress>> walk_local_wal_chain(
  Backend& files,
  Owner& ownership,
  const local_device_spec& spec,
  std::uint32_t shard,
  const local_shard_control& control,
  std::optional<local_wal_cursor> cutoff,
  bool allow_unactivated,
  workload_budget& budget,
  local_store_io_limits limits,
  codec::cooperative_work& work,
  Visitor visit,
  local_discovery_limits bounds = {}) {
    static_assert(sizeof(Visitor) <= 4096);
    if (auto valid = validate_local_device_spec(spec); !valid)
        co_return runtime::failure(valid.error());
    if (auto valid = limits.validate(); !valid)
        co_return runtime::failure(valid.error());
    if (auto valid = bounds.validate(); !valid)
        co_return runtime::failure(valid.error());
    auto owner = spec.shard_owner(shard);
    if (!owner || !spec.controls())
        co_return runtime::failure(detail::path_error(errc::wrong_context));
    if (auto ready = work.poll(); !ready)
        co_return runtime::failure(detail::path_error(ready.error().code()));
    auto held = budget.try_reserve(
      byte_count{limits.execution_bytes.value() + 16384});
    if (!held) co_return runtime::failure(held.error());
    auto valid = co_await ownership.validate(spec);
    if (!valid) co_return runtime::failure(valid.error());
    if (!control.wal_head) {
        if (!allow_unactivated || cutoff || control.checkpoint)
            co_return runtime::failure(detail::path_error(errc::not_found));
        co_return local_inventory_progress{0, true};
    }
    auto high = local_wal_high::from_incarnation(control.wal_head->incarnation);
    if (!high || *high > control.wal_high)
        co_return runtime::failure(detail::path_error(errc::wrong_context));
    const auto paths = local_paths::make(spec.root).value();
    auto incarnation = control.wal_head->incarnation;
    std::optional<runtime::file_position> sealed_end;
    local_inventory_progress progress;
    for (;;) {
        if (progress.visited == bounds.maximum_wal_files)
            co_return runtime::failure(
              detail::path_error(errc::resource_exhausted));
        valid = co_await detail::path_checkpoint(work);
        if (!valid) co_return runtime::failure(valid.error());
        const bool head = progress.visited == 0;
        auto path = paths.wal(shard, incarnation);
        if (!path) co_return runtime::failure(path.error());
        auto loaded
          = head
              ? co_await load_local_wal_head(
                  files, spec, shard, *control.wal_head, budget, limits, work)
              : co_await load_local_metadata_file(
                  files,
                  spec,
                  *path,
                  budget,
                  limits,
                  work,
                  local_metadata_extent::first_envelope,
                  [owner = *owner, incarnation](
                    auto& input,
                    byte_count,
                    codec::decode_budget memory,
                    auto& work) {
                      return decode_local_wal_descriptor(
                        input,
                        owner,
                        incarnation,
                        std::nullopt,
                        memory,
                        work,
                        {},
                        codec::input_boundary::complete);
                  });
        if (!loaded) co_return runtime::failure(loaded.error());
        const auto descriptor = std::get<local_wal_descriptor>(
          loaded->value.payload());
        const auto end = sealed_end ? sealed_end->value() : loaded->file_bytes;
        if (
          loaded->file_bytes > descriptor.capacity_bytes.value()
          || end < descriptor.data_start.value() || end > loaded->file_bytes
          || (sealed_end && end % descriptor.alignment.bytes().value() != 0))
            co_return runtime::failure(detail::path_error(errc::wrong_context));
        if (cutoff && incarnation.canonical_less(cutoff->incarnation()))
            co_return runtime::failure(detail::path_error(errc::wrong_context));
        const bool at_cutoff = cutoff && incarnation == cutoff->incarnation();
        if (at_cutoff && (cutoff->position().value() < descriptor.data_start.value() || cutoff->position().value() > end
            || cutoff->position().value() % descriptor.alignment.bytes().value() != 0))
            co_return runtime::failure(detail::path_error(errc::wrong_context));
        ++progress.visited;
        const local_wal_chain_entry entry{*loaded, head, sealed_end};
        auto consumed = co_await visit(entry);
        if (!consumed) co_return runtime::failure(consumed.error());
        if (!*consumed) co_return progress;
        if (at_cutoff || !descriptor.predecessor) {
            if (cutoff && !at_cutoff)
                co_return runtime::failure(
                  detail::path_error(errc::wrong_context));
            valid = co_await ownership.validate(spec);
            if (!valid) co_return runtime::failure(valid.error());
            progress.complete = true;
            co_return progress;
        }
        if (!descriptor.predecessor->incarnation().canonical_less(incarnation))
            co_return runtime::failure(
              detail::path_error(errc::malformed_data));
        incarnation = descriptor.predecessor->incarnation();
        sealed_end = descriptor.predecessor->position();
    }
}

struct local_located_segment final {
    local_loaded_segment loaded;
    device_store_id device;
};
// Search the bounded configured set, not a process-wide object map. Missing
// parent namespaces mean not placed here; an existing segment directory with
// missing/corrupt required files is an error. A second writable copy conflicts.
template<runtime::file_system_backend Backend, local_directory_owner Owner>
seastar::future<runtime::result<local_located_segment>> resolve_local_segment(
  Backend& files,
  Owner& ownership,
  std::span<const local_device_spec> devices,
  std::uint32_t shard,
  local_segment_descriptor expected,
  segment_header header,
  workload_budget& budget,
  local_store_io_limits limits,
  codec::cooperative_work& work) {
    if (auto valid = validate_local_device_set(devices); !valid)
        co_return runtime::failure(valid.error());
    if (auto valid = limits.validate(); !valid)
        co_return runtime::failure(valid.error());
    auto held = budget.try_reserve(
      byte_count{limits.execution_bytes.value() + 16384});
    if (!held) co_return runtime::failure(held.error());
    for (const auto& spec : devices) {
        if (!spec.shard_owner(shard))
            co_return runtime::failure(detail::path_error(errc::wrong_context));
        if (spec.stores_data()) {
            auto valid = validate_local_segment(spec, shard, expected, header);
            if (!valid) co_return runtime::failure(valid.error());
        }
        auto valid = co_await ownership.validate(spec);
        if (!valid) co_return runtime::failure(valid.error());
    }
    std::optional<local_located_segment> found;
    for (const auto& spec : devices) {
        if (!spec.stores_data()) continue;
        const auto paths = local_paths::make(spec.root).value();
        auto path = paths.segment(
          shard, {expected.segment.segment(), expected.segment.generation()});
        if (!path) co_return runtime::failure(path.error());
        auto present = co_await inspect_local_path(
          files, spec.root, *path, runtime::file_kind::directory, work);
        if (!present) {
            if (present.error().code() == errc::not_found) continue;
            co_return runtime::failure(present.error());
        }
        auto loaded = co_await load_local_segment(
          files,
          ownership,
          spec,
          shard,
          expected,
          header,
          budget,
          limits,
          work);
        if (!loaded) co_return runtime::failure(loaded.error());
        if (found)
            co_return runtime::failure(detail::path_error(errc::wrong_context));
        found.emplace(
          local_located_segment{std::move(*loaded), spec.owner.device()});
    }
    if (!found) co_return runtime::failure(detail::path_error(errc::not_found));
    co_return std::move(*found);
}
} // namespace kwaque::storage
