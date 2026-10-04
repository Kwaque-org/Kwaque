#pragma once

#include "src/storage/local_generation.h"

#include <seastar/core/coroutine.hh>

#include <algorithm>
#include <exception>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

namespace kwaque::storage {

// One segment's current publication as restart selects it, opened against the
// pins it carries.
struct local_recovery_generation final {
    segment_history_context history;
    local_publication_generation generation;
    local_object_publication publication;
    // Holds the verified boundary and every opened root; the caller closes it.
    // Absent for a deleting publication, which deletion reconciliation owns.
    std::unique_ptr<local_generation_owner> owner;
    // The durable footer an active or recovering publication pins: where the
    // segment's scan resumes.
    std::optional<local_footer_reference> resume;
    // Only the index is missing or damaged: derived state that can be rebuilt
    // from data. Missing or damaged mandatory evidence is an error instead.
    std::optional<runtime::operation_error> index_rebuild;
};

namespace detail {
// State and pins agree. An active or recovering publication pins at most a
// durable footer and a completed-retry snapshot, which needs that footer. A
// sealed one pins its sealed root and exactly one sealed retry root, and may
// add an index and a snapshot. Each kind appears once.
[[nodiscard]] runtime::result<void>
validate_recovery_publication(const local_object_publication&);
// Each root's context from independent state only: the verified history, the
// publication's own references and, for an index, the sealed root they pin.
// Nothing is taken from the root bytes the open authenticates.
[[nodiscard]] runtime::result<std::vector<local_bundle_context>>
recovery_root_contexts(
  const local_device_spec& spec,
  std::uint32_t shard,
  const segment_history_context& history,
  const local_object_publication& publication,
  const sealed_footer* sealed);
} // namespace detail

namespace detail {
// open_recovery_generation once the segment is loaded and its publication
// selected: validates the publication's state against its pins and opens
// them through the generation owner.
template<runtime::file_system_backend Backend, local_directory_owner Owner>
seastar::future<runtime::result<local_recovery_generation>> open_recovery_pins(
  Backend& files,
  Owner& ownership,
  const local_device_spec& spec,
  std::uint32_t shard,
  const local_segment_descriptor& descriptor,
  segment_history_context history,
  local_publication_generation generation,
  local_object_publication selected,
  workload_budget& budget,
  local_store_io_limits limits,
  codec::cooperative_work& work,
  local_reader_limits readers = {}) {
    local_recovery_generation output{
      history, generation, std::move(selected), {}, {}, {}};
    const auto& publication = output.publication;
    if (auto valid = detail::validate_recovery_publication(publication); !valid)
        co_return runtime::failure(valid.error());
    if (publication.state == local_object_state::deleting) co_return output;
    const auto paths = local_paths::make(spec.root);
    if (!paths) co_return runtime::failure(paths.error());
    const local_segment_name identity{
      descriptor.segment.segment(), descriptor.segment.generation()};
    // An index is checked against the sealed extent its root names, so that
    // root is read first, under its own pin.
    std::optional<sealed_footer> sealed;
    const bool indexed = std::any_of(
      publication.roots.begin(), publication.roots.end(), [](const auto& root) {
          return root.kind() == local_root_kind::index;
      });
    if (indexed) {
        auto data_path = paths->segment_file(
          shard, identity, local_segment_file::data);
        if (!data_path) co_return runtime::failure(data_path.error());
        auto handle = budget.try_reserve(limits.execution_bytes);
        if (!handle) co_return runtime::failure(handle.error());
        if (auto held = handle->try_acquire_handles(1); !held)
            co_return runtime::failure(held.error());
        if (
          auto inspected = co_await inspect_local_path(
            files, spec.root, *data_path, runtime::file_kind::regular, work);
          !inspected)
            co_return runtime::failure(inspected.error());
        auto opened = co_await files.open(
          *data_path, {.close_policy = runtime::file_close_policy::checked});
        if (!opened) co_return runtime::failure(opened.error());
        auto data = std::move(*opened);
        runtime::first_failure failed;
        try {
            auto boundary = co_await detail::read_local_boundary(
              data, history, *publication.boundary, limits, work);
            if (!boundary)
                failed.observe(boundary);
            else
                sealed.emplace(std::move(std::get<sealed_footer>(*boundary)));
        } catch (...) {
            failed.observe(std::current_exception());
        }
        try {
            failed.observe(co_await data.close());
        } catch (...) {
            failed.observe(std::current_exception());
        }
        if (failed.failed()) {
            auto error = failed.outcome();
            co_return runtime::failure(error.error());
        }
    }
    auto contexts = detail::recovery_root_contexts(
      spec, shard, history, publication, sealed ? &*sealed : nullptr);
    if (!contexts) co_return runtime::failure(contexts.error());
    const local_generation_expectation expected{
      output.generation, publication, descriptor, *contexts};
    auto opened = co_await local_generation_owner::open(
      files, ownership, spec, shard, expected, budget, limits, work, readers);
    if (!opened) co_return runtime::failure(opened.error());
    auto owner = std::move(*opened);
    std::optional<runtime::operation_error> rebuild;
    std::optional<runtime::operation_error> failed;
    if (auto pin = owner->pin(); !pin)
        failed = pin.error();
    else
        rebuild = pin->index_rebuild_reason();
    if (failed) {
        owner->retire();
        static_cast<void>(co_await owner->close());
        co_return runtime::failure(*failed);
    }
    output.index_rebuild = rebuild;
    if (publication.state != local_object_state::sealed && publication.boundary)
        output.resume = *publication.boundary;
    output.owner = std::move(owner);
    co_return output;
}
} // namespace detail

// Selects the segment's publication by its fixed path, validates its state
// against its pins, and opens its boundary and roots through the generation
// owner: each boundary and root must match its pinned identity and position
// exactly, so a correct checksum on the wrong object, digest, page or
// position rejects. The descriptor is the independent catalog entry; nothing
// is inferred from names. Reads only.
template<runtime::file_system_backend Backend, local_directory_owner Owner>
seastar::future<runtime::result<local_recovery_generation>>
open_recovery_generation(
  Backend& files,
  Owner& ownership,
  const local_device_spec& spec,
  std::uint32_t shard,
  const local_segment_descriptor& descriptor,
  workload_budget& budget,
  local_store_io_limits limits,
  codec::cooperative_work& work,
  local_reader_limits readers = {}) {
    if (auto valid = limits.validate(); !valid)
        co_return runtime::failure(valid.error());
    const auto owner = spec.shard_owner(shard);
    if (!owner)
        co_return runtime::failure(detail::path_error(errc::wrong_context));
    auto header = segment_header::make(
      descriptor.segment,
      descriptor.logical_origin,
      descriptor.alignment,
      descriptor.profile);
    if (!header)
        co_return runtime::failure(detail::path_error(errc::invalid_argument));
    auto segment = co_await load_local_segment(
      files, ownership, spec, shard, descriptor, *header, budget, limits, work);
    if (!segment) co_return runtime::failure(segment.error());
    const segment_history_context history{
      descriptor.segment,
      descriptor.alignment,
      segment->header.bytes.end(),
      descriptor.logical_origin,
      descriptor.physical_origin,
      descriptor.profile};
    const auto paths = local_paths::make(spec.root);
    if (!paths) co_return runtime::failure(paths.error());
    auto published = paths->segment_file(
      shard,
      {descriptor.segment.segment(), descriptor.segment.generation()},
      local_segment_file::published);
    if (!published) co_return runtime::failure(published.error());
    auto selected = co_await load_local_metadata_file(
      files,
      spec,
      *published,
      budget,
      limits,
      work,
      local_metadata_extent::exact_file,
      [owner = *owner,
       segment = descriptor.segment,
       metadata = spec.identity.metadata_alignment,
       data = descriptor.alignment](
        auto& input, byte_count, codec::decode_budget memory, auto& work) {
          return decode_selected_object_publication(
            input,
            owner,
            segment,
            metadata,
            data,
            memory,
            work,
            {},
            codec::input_boundary::complete);
      });
    if (!selected) co_return runtime::failure(selected.error());
    co_return co_await detail::open_recovery_pins(
      files,
      ownership,
      spec,
      shard,
      descriptor,
      history,
      selected->value.header().generation(),
      std::get<local_object_publication>(selected->value.payload()),
      budget,
      limits,
      work,
      readers);
}

} // namespace kwaque::storage
