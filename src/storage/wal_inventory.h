#pragma once

#include "src/storage/local_cleanup.h"
#include "src/storage/local_control.h"

namespace kwaque::storage {
// A selected control anchor, not a recovered-tail certificate. The coordinator
// owns the quiescent namespace/reference pins for the entire joined inventory.
// Recovery may form this from a context-checked control load; a stopped
// writer supplies the same facts through inventory_snapshot(). Neither
// authorizes I/O on an existing WAL tail or release of a checkpoint/recovery
// pin.
struct wal_inventory_snapshot final {
    local_store_context owner;
    local_control_snapshot control;
};
struct wal_inventory_progress final {
    local_inventory_progress chain;
    local_inventory_progress names;
};
enum class wal_inventory_intent : std::uint8_t {
    require_head,
    known_unactivated
};

// The optional cutoff has independently established checkpoint coverage
// supplied by the coordinator. This walk verifies its membership/bounds, never
// its proof. Callbacks borrow one admitted record through a joined await. No
// retained file list, head election, PREPARE scan, repair, truncation or
// deletion occurs here.
template<
  runtime::file_system_backend Backend,
  local_directory_owner Owner,
  typename ChainVisitor,
  typename NamespaceVisitor>
seastar::future<runtime::result<wal_inventory_progress>> inspect_wal_inventory(
  Backend& files,
  Owner& ownership,
  const local_device_spec& spec,
  wal_inventory_snapshot snapshot,
  std::optional<local_wal_cursor> cutoff,
  wal_inventory_intent intent,
  workload_budget& budget,
  local_store_io_limits limits,
  codec::cooperative_work& work,
  ChainVisitor chain_visit,
  NamespaceVisitor namespace_visit,
  local_discovery_limits bounds = {}) {
    static_assert(sizeof(ChainVisitor) + sizeof(NamespaceVisitor) <= 8192);
    if (auto checked = validate_local_device_spec(spec); !checked)
        co_return runtime::failure(checked.error());
    const auto shard = snapshot.owner.shard();
    if (
      spec.shard_owner(shard) != snapshot.owner
      || (intent != wal_inventory_intent::require_head && intent != wal_inventory_intent::known_unactivated))
        co_return runtime::failure(detail::path_error(errc::wrong_context));
    if (auto checked = limits.validate(); !checked)
        co_return runtime::failure(checked.error());
    if (auto checked = bounds.validate(); !checked)
        co_return runtime::failure(checked.error());
    if (auto ready = work.poll(); !ready)
        co_return runtime::failure(detail::path_error(ready.error().code()));
    auto held = budget.try_reserve(
      byte_count{limits.execution_bytes.value() + 16384});
    if (!held) co_return runtime::failure(held.error());
    auto checked = co_await ownership.validate(spec);
    if (!checked) co_return runtime::failure(checked.error());
    const auto paths = local_paths::make(spec.root);
    if (!paths) co_return runtime::failure(paths.error());
    auto control_path = paths->control(shard);
    if (!control_path) co_return runtime::failure(control_path.error());
    auto confirm = [&]() -> seastar::future<runtime::result<void>> {
        auto selected = co_await load_local_control(
          files, spec, shard, *control_path, budget, limits, work);
        if (!selected) co_return runtime::failure(selected.error());
        if (
          selected->value.header().generation() != snapshot.control.generation
          || std::get<local_shard_control>(selected->value.payload())
               != snapshot.control.fields)
            co_return runtime::failure(detail::path_error(errc::wrong_context));
        co_return runtime::result<void>{};
    };
    checked = co_await confirm();
    if (!checked) co_return runtime::failure(checked.error());
    std::optional<local_record_expectation> head;
    auto visit_chain = [&](const local_wal_chain_entry& entry)
      -> seastar::future<runtime::result<bool>> {
        if (entry.head_digest_pinned) {
            const auto& descriptor = std::get<local_wal_descriptor>(
              entry.record.value.payload());
            head.emplace(
              local_record_expectation{
                {entry.record.value.header(), descriptor.alignment}});
            head->record.wal_incarnation
              = snapshot.control.fields.wal_head->incarnation;
            head->record.digest
              = snapshot.control.fields.wal_head->header_digest;
            head->record.encoded_bytes = entry.record.value.encoded_bytes();
        }
        co_return co_await chain_visit(entry);
    };
    auto chain = co_await walk_local_wal_chain(
      files,
      ownership,
      spec,
      shard,
      snapshot.control.fields,
      cutoff,
      intent == wal_inventory_intent::known_unactivated,
      budget,
      limits,
      work,
      std::move(visit_chain),
      bounds);
    if (!chain) co_return runtime::failure(chain.error());
    if (!chain->complete) co_return wal_inventory_progress{*chain, {}};
    const auto is_head = [&](const local_namespace_entry& entry) {
        return entry.kind == local_entry_kind::wal && entry.shard == shard
               && snapshot.control.fields.wal_head
               && entry.wal == snapshot.control.fields.wal_head->incarnation;
    };
    auto resolve = [&](const local_namespace_entry& entry) {
        return seastar::make_ready_future<
          runtime::result<std::optional<local_record_expectation>>>(
          is_head(entry) ? head : std::nullopt);
    };
    auto references = [&](const local_discovered_record& entry) {
        local_cleanup_references facts;
        if (
          is_head(entry.entry) && entry.decoded && !entry.damage
          && !entry.entry.unexpected_kind)
            facts.persisted = local_reference_status::referenced;
        else if (
          entry.entry.kind == local_entry_kind::wal
          || (entry.entry.kind == local_entry_kind::temporary && entry.entry.temporary_target == local_entry_kind::wal))
            facts.recovery_candidate = true;
        return seastar::make_ready_future<
          runtime::result<local_cleanup_references>>(facts);
    };
    bool saw_head = false;
    auto visit_name = [&](
                        const local_discovered_record& entry,
                        local_cleanup_observation cleanup)
      -> seastar::future<runtime::result<bool>> {
        if (is_head(entry.entry)) {
            saw_head = true;
            if (entry.damage) co_return runtime::failure(*entry.damage);
            if (!entry.decoded || entry.entry.unexpected_kind)
                co_return runtime::failure(
                  detail::path_error(errc::wrong_context));
        }
        co_return co_await namespace_visit(entry, cleanup);
    };
    auto names = co_await enumerate_local_cleanup(
      files,
      ownership,
      spec,
      budget,
      limits,
      work,
      std::move(resolve),
      std::move(references),
      std::move(visit_name),
      bounds);
    if (!names) co_return runtime::failure(names.error());
    if (names->complete && snapshot.control.fields.wal_head && !saw_head)
        co_return runtime::failure(detail::path_error(errc::not_found));
    checked = co_await confirm();
    if (!checked) co_return runtime::failure(checked.error());
    co_return wal_inventory_progress{*chain, *names};
}
} // namespace kwaque::storage
