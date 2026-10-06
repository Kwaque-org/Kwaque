#pragma once

#include "src/storage/recovery_plan.h"
#include "src/storage/recovery_publication.h"
#include "src/storage/wal_writer.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/when_all.hh>

#include <cstdint>
#include <exception>
#include <limits>
#include <span>
#include <utility>

namespace kwaque::storage {

// Closes the recovered WAL head as a finished plan calls for: a writer made
// for the recovered head activates a successor whose predecessor cursor is
// the head's classified content end, after the head's fresh barrier. The old
// head is never resumed, written or truncated; the files holding unresolved
// candidates stay in the chain. A plan with any stopped scope permits no
// mutation, so nothing runs.
template<runtime::file_system_backend Backend, typename Owner>
seastar::future<runtime::result<void>> activate_recovered_wal(
  wal_writer<Backend, Owner>& writer,
  const recovery_planner& plan,
  codec::cooperative_work& work) {
    if (!plan.ready())
        co_return runtime::failure(detail::path_error(errc::wrong_context));
    const auto& wal = plan.wal();
    if (
      wal.action != recovery_plan_action::activate_successor || !wal.predecessor
      || wal.chain.empty())
        co_return runtime::failure(detail::path_error(errc::invalid_argument));
    // The files the scan found are the ones the shard still holds, and the
    // writer continues that chain by name. One it cannot name in full has
    // more files than a shard may hold: what pins them is resolved first.
    if (wal.chain.files() != wal.chain_files) {
        auto refused = detail::path_error(errc::resource_exhausted);
        static_cast<void>(refused.add_context(
          runtime::operation_context_key::limit, maximum_retained_wal_files));
        static_cast<void>(refused.add_context(
          runtime::operation_context_key::actual, wal.chain_files));
        co_return runtime::failure(refused);
    }
    co_return co_await writer.activate_recovered(
      *wal.predecessor, wal.chain, work);
}

// Everything a ready plan establishes before the shard is ready, at once:
// each recovered segment's fresh flush and recovering publication, and the
// WAL head's successor. They change disjoint state, each segment's own
// publication and the control's WAL head, and a crash after any subset of
// their effects leaves a store the next restart classifies the same way, so
// neither waits for the other. Each runs under its own cooperative work on
// `abort`. Both are joined before returning; a publication failure is
// returned first, then the successor's. A plan that activates no successor,
// a WAL never activated, is refused before anything starts: its segments
// are published alone. The shard is ready only once this succeeds with
// every publication complete.
template<
  runtime::monotonic_clock Clock,
  runtime::file_system_backend Backend,
  local_directory_owner Owner,
  typename WalOwner>
seastar::future<runtime::result<recovered_publications>>
establish_recovered_state(
  Backend& files,
  Owner& ownership,
  std::span<const local_device_spec> devices,
  std::uint32_t shard,
  const recovery_planner& plan,
  const recovery_inventory& inventory,
  std::span<const recovery_visibility> visible,
  workload_budget& budget,
  segment_writer_config config,
  recovered_publication_limits limits,
  wal_writer<Backend, WalOwner>& writer,
  codec::limits policy,
  seastar::abort_source& abort) {
    if (!plan.ready())
        co_return runtime::failure(detail::path_error(errc::wrong_context));
    if (
      plan.wal().action != recovery_plan_action::activate_successor
      || !plan.wal().predecessor)
        co_return runtime::failure(detail::path_error(errc::invalid_argument));
    codec::cooperative_work publishing{policy, abort};
    codec::cooperative_work activating{policy, abort};
    auto [published, activated] = co_await seastar::when_all(
      publish_recovered_segments<Clock>(
        files,
        ownership,
        devices,
        shard,
        plan,
        inventory,
        visible,
        budget,
        config,
        limits,
        publishing),
      activate_recovered_wal(writer, plan, activating));
    std::exception_ptr thrown;
    if (published.failed()) thrown = published.get_exception();
    if (activated.failed()) {
        auto exception = activated.get_exception();
        if (!thrown) thrown = std::move(exception);
    }
    if (thrown) std::rethrow_exception(thrown);
    auto publications = published.get();
    auto successor = activated.get();
    if (!publications) co_return runtime::failure(publications.error());
    if (!successor) co_return runtime::failure(successor.error());
    co_return std::move(*publications);
}

} // namespace kwaque::storage
