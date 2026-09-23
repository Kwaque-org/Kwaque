#pragma once

#include "src/storage/local_discovery.h"

namespace kwaque::storage {
enum class local_reference_status : std::uint8_t {
    unknown,
    referenced,
    absent
};
struct local_cleanup_references final {
    // absent requires a complete, independently owned scan of all persisted
    // publication/checkpoint/evidence/decision references, including retired
    // generations. It cannot be inferred from zero process readers.
    local_reference_status persisted{local_reference_status::unknown};
    bool live_pins{false};
    bool recovery_candidate{false};
    // Required for bundles: a valid root alone does not classify its pages.
    bool complete_content{false};
};
enum class local_cleanup_class : std::uint8_t {
    structural,
    external,
    referenced,
    reader_pinned,
    unsupported,
    damaged,
    recovery_required,
    unresolved,
    temporary_debt,
    orphan_candidate
};
struct local_cleanup_observation final {
    local_cleanup_class classification;
    // Debt describes further reconciliation work, never erase permission.
    bool debt;
};
[[nodiscard]] local_cleanup_observation classify_local_cleanup(
  const local_discovered_record&, local_cleanup_references) noexcept;

template<
  runtime::file_system_backend Backend,
  local_directory_owner Owner,
  typename Resolver,
  typename References,
  typename Visitor>
seastar::future<runtime::result<local_inventory_progress>>
enumerate_local_cleanup(
  Backend& files,
  Owner& ownership,
  const local_device_spec& spec,
  workload_budget& budget,
  local_store_io_limits limits,
  codec::cooperative_work& work,
  Resolver resolve,
  References references,
  Visitor visit,
  local_discovery_limits bounds = {}) {
    static_assert(
      sizeof(Resolver) + sizeof(References) + sizeof(Visitor) <= 8192);
    auto observe = [&](const local_discovered_record& record)
      -> seastar::future<runtime::result<bool>> {
        auto facts = co_await references(record);
        if (!facts) co_return runtime::failure(facts.error());
        co_return co_await visit(
          record, classify_local_cleanup(record, *facts));
    };
    co_return co_await discover_local_records(
      files,
      ownership,
      spec,
      budget,
      limits,
      work,
      std::move(resolve),
      std::move(observe),
      bounds);
}
} // namespace kwaque::storage
