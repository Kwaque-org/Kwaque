#include "src/storage/segment_writer_state.h"

#include "src/storage/format_internal.h"
#include "src/storage/local_paths.h"

#include <seastar/core/deleter.hh>

#include <algorithm>
#include <array>
#include <limits>

namespace kwaque::storage {
runtime::result<void> segment_admission_limits::validate() const noexcept {
    if (
      !maximum_blocks || !maximum_retry_entries || !maximum_retry_pages
      || maximum_retry_entries > maximum_object_entries
      || maximum_retry_pages > maximum_object_pages
      || metadata_bytes.value() == 0 || metadata_bytes > byte_count{1U << 20U}
      || working_bytes < metadata_bytes
      || working_bytes > runtime::maximum_file_io_bytes
      || execution_bytes < byte_count{4096}
      || execution_bytes > byte_count{maximum_contiguous_allocation_bytes})
        return runtime::failure(detail::path_error(errc::invalid_argument));
    return {};
}

namespace {
struct staging_allocation final {
    byte_count additional, backing;
    std::uint32_t fragments;
};
std::optional<staging_allocation> staging_cost(
  std::size_t child_fragments,
  byte_count fixed,
  aligned_envelope_layout layout,
  const codec::limits& policy,
  bytes::allocation_charge_fn charge) {
    const auto prefix = detail::plan_padded_prefix(fixed, policy, charge);
    if (!prefix) return std::nullopt;
    const auto padding = layout.padding_bytes().value() != 0 ? 1U : 0U;
    const auto nodes = child_fragments + prefix->fragments + padding;
    if (nodes > policy.config().max_buffer_fragments.value())
        return std::nullopt;
    const auto checked =
      [&](byte_count requested) -> std::optional<byte_count> {
        if (!requested.value()) return byte_count{};
        const auto served = charge(requested);
        if (served < requested || !policy.validate_allocation(served))
            return std::nullopt;
        return served;
    };
    const auto pad = checked(layout.padding_bytes());
    const auto descriptors = checked(
      byte_count{nodes * bytes::fragmented_buffer::fragment_descriptor_size()});
    const auto single = checked(
      byte_count{bytes::fragmented_buffer::fragment_descriptor_size()});
    const auto control = checked(
      byte_count{sizeof(seastar::free_deleter_impl)});
    if (!pad || !descriptors || !single || !control) return std::nullopt;
    const byte_count backing{prefix->backing.value() + pad->value()};
    const byte_count extra{
      backing.value() + descriptors->value()
      + (prefix->fragments + padding) * (single->value() + control->value())};
    auto tail = layout.body_bytes().checked_sub(layout.padding_bytes());
    if (tail) tail = tail->checked_sub(fixed);
    if (!tail) return std::nullopt;
    const auto minimum_retained = backing.checked_add(*tail);
    const auto minimum_operation = extra.checked_add(*tail);
    // The caller owns the input. Bound only new staging here; the consuming
    // codec validates input accounting under its policy. Lookahead must not
    // rescan every fragment in every offered child.
    if (
      !minimum_operation || !minimum_retained
      || *minimum_operation > policy.config().max_operation_bytes
      || *minimum_retained > policy.config().max_retained_bytes)
        return std::nullopt;
    return staging_allocation{
      extra, backing, static_cast<std::uint32_t>(nodes)};
}

std::optional<byte_count> metadata_charge(
  std::uint64_t count,
  std::size_t width,
  byte_count remaining,
  const codec::limits& policy,
  bytes::allocation_charge_fn charge) {
    if (!count) return byte_count{};
    if (count > maximum_contiguous_allocation_bytes / width)
        return std::nullopt;
    const byte_count request{count * width};
    const auto served = charge(request);
    if (
      served < request || served > remaining
      || !policy.validate_allocation(served))
        return std::nullopt;
    return served;
}

bool object_fits_workspace(
  byte_count fixed,
  byte_count tail,
  byte_count decoded,
  aligned_envelope_layout layout,
  byte_count working,
  byte_count metadata_limit,
  const codec::limits& policy,
  bytes::allocation_charge_fn charge) {
    auto extra = staging_cost(
      tail.value() ? 1 : 0, fixed, layout, policy, charge);
    if (!extra) return false;
    const auto nodes = extra->fragments;
    const byte_count descriptor_request{
      nodes * bytes::fragmented_buffer::fragment_descriptor_size()};
    const byte_count control_request{sizeof(seastar::free_deleter_impl)};
    const auto descriptors = charge(descriptor_request);
    const auto controls = charge(control_request);
    const auto backing = tail.value() ? charge(tail) : byte_count{};
    if (
      backing < tail || descriptors < descriptor_request
      || controls < control_request || !policy.validate_allocation(backing)
      || !policy.validate_allocation(descriptors)
      || !policy.validate_allocation(controls))
        return false;
    // encode_entries owns one tail; encode_padded overlaps its bookkeeping
    // with the final descriptors. Also cover decoded entries and two parser
    // alias sets for the existing contextual validation path.
    const byte_count retained{extra->backing.value() + backing.value()};
    const byte_count metadata{
      decoded.value() + 3 * (descriptors.value() + nodes * controls.value())};
    const byte_count needed{
      extra->additional.value() + backing.value() + decoded.value()
      + 3 * (descriptors.value() + nodes * controls.value())};
    return needed <= working && needed <= policy.config().max_operation_bytes
           && retained <= policy.config().max_retained_bytes
           && metadata
                <= std::min(metadata_limit, policy.config().max_metadata_bytes);
}

struct retry_projection final {
    std::uint32_t pages, entries_per_page;
    byte_count root, disk;
};
// Entries of one full retry page on the wire; zero when none fit.
std::uint32_t
wire_capacity_for(storage_alignment alignment, const codec::limits& policy) {
    auto capacity = retry_page_capacity(
      byte_count{codec::envelope_prefix_bytes}, alignment, policy);
    return capacity ? *capacity : 0;
}
std::optional<retry_projection> project_retries(
  std::uint64_t entries,
  storage_alignment alignment,
  segment_admission_limits limits,
  const codec::limits& policy,
  bytes::allocation_charge_fn charge,
  byte_count working,
  std::uint32_t wire_capacity) {
    const auto config = policy.config();
    if (
      entries > std::min<std::uint64_t>(
        limits.maximum_retry_entries, config.max_object_entries.value()))
        return std::nullopt;
    if (!wire_capacity) return std::nullopt;
    const auto page_limit = std::min<std::uint64_t>(
      limits.maximum_retry_pages, config.max_object_pages.value());
    const auto metadata = std::min(
      limits.metadata_bytes, config.max_metadata_bytes);
    // Intersect wire capacity with the served vector allocation and root refs.
    // The page cap is at most 408 entries, so this bounded descending search
    // cannot scale with the segment's record history.
    const auto first_capacity = std::min(
      wire_capacity,
      static_cast<std::uint32_t>(std::max<std::uint64_t>(1, entries)));
    for (auto capacity = first_capacity; capacity != 0; --capacity) {
        const auto pages = (entries + capacity - 1) / capacity;
        if (pages > page_limit) return std::nullopt;
        auto refs = metadata_charge(
          pages, sizeof(page_ref), metadata, policy, charge);
        if (!refs) continue;
        auto page_memory = metadata_charge(
          std::min<std::uint64_t>(entries, capacity),
          sizeof(completed_retry),
          byte_count{metadata.value() - refs->value()},
          policy,
          charge);
        if (!page_memory) continue;
        auto root = aligned_envelope_layout::make(
          {byte_count{codec::envelope_prefix_bytes},
           sealed_footer_fixed_bytes,
           byte_count{pages * page_ref_wire_bytes.value()}},
          alignment,
          policy,
          {config.max_page_bytes, config.max_page_bytes});
        if (!root) return std::nullopt;
        if (!object_fits_workspace(
              sealed_footer_fixed_bytes,
              byte_count{pages * page_ref_wire_bytes.value()},
              *refs,
              *root,
              working,
              metadata,
              policy,
              charge))
            continue;
        const auto full_pages = entries / capacity;
        const auto tail_entries = entries % capacity;
        auto full = aligned_envelope_layout::make(
          {byte_count{codec::envelope_prefix_bytes},
           retry_page_fixed_bytes,
           byte_count{capacity * completed_retry_wire_bytes.value()}},
          alignment,
          policy,
          {config.max_page_bytes, config.max_page_bytes});
        if (!full) return std::nullopt;
        if (
          entries
          && !object_fits_workspace(
            retry_page_fixed_bytes,
            byte_count{capacity * completed_retry_wire_bytes.value()},
            page_memory->checked_add(*refs).value(),
            *full,
            working,
            metadata,
            policy,
            charge))
            continue;
        auto disk = byte_count{full_pages * full->encoded_bytes().value()};
        if (tail_entries) {
            auto tail = aligned_envelope_layout::make(
              {byte_count{codec::envelope_prefix_bytes},
               retry_page_fixed_bytes,
               byte_count{tail_entries * completed_retry_wire_bytes.value()}},
              alignment,
              policy,
              {config.max_page_bytes, config.max_page_bytes});
            if (!tail) return std::nullopt;
            const auto tail_metadata = metadata_charge(
              tail_entries,
              sizeof(completed_retry),
              byte_count{metadata.value() - refs->value()},
              policy,
              charge);
            if (
              !tail_metadata
              || !object_fits_workspace(
                retry_page_fixed_bytes,
                byte_count{tail_entries * completed_retry_wire_bytes.value()},
                tail_metadata->checked_add(*refs).value(),
                *tail,
                working,
                metadata,
                policy,
                charge))
                continue;
            disk = disk.checked_add(tail->encoded_bytes()).value();
        }
        return retry_projection{
          static_cast<std::uint32_t>(pages),
          capacity,
          root->encoded_bytes(),
          disk};
    }
    return std::nullopt;
}

// Every supported populated publication-pointer shape must fit the
// finalization workspace. Returns the largest pointer's encoded bytes.
std::optional<byte_count> publication_pointer_bytes(
  storage_alignment metadata_alignment,
  segment_admission_limits limits,
  const codec::limits& policy,
  bytes::allocation_charge_fn charge,
  byte_count finalization_bytes) {
    const auto publication = local_metadata_descriptor_for(
                               static_cast<std::uint16_t>(
                                 local_metadata_kind::object_publication))
                               .value();
    auto pointer = local_metadata_layout(
      local_metadata_kind::object_publication,
      byte_count{
        publication.maximum_fixed_payload_bytes
        + publication.entry_bytes * publication.maximum_entries},
      byte_count{codec::envelope_prefix_bytes},
      metadata_alignment,
      policy);
    if (!pointer) return std::nullopt;
    // Padding/allocator rounding need not be monotonic in root count. Reserve
    // every supported populated pointer shape, not only its largest wire form.
    for (std::uint32_t roots = 1; roots <= publication.maximum_entries;
         ++roots) {
        const byte_count tail{publication.entry_bytes * roots};
        const auto layout = local_metadata_layout(
          local_metadata_kind::object_publication,
          byte_count{publication.maximum_fixed_payload_bytes + tail.value()},
          byte_count{codec::envelope_prefix_bytes},
          metadata_alignment,
          policy);
        if (
          !layout
          || !object_fits_workspace(
            byte_count{
              local_metadata_prefix_bytes.value()
              + publication.maximum_fixed_payload_bytes},
            tail,
            {},
            *layout,
            finalization_bytes,
            limits.metadata_bytes,
            policy,
            charge))
            return std::nullopt;
    }
    return pointer->encoded_bytes();
}
} // namespace

runtime::result<void> detail::validate_empty_segment_seal(
  storage_alignment alignment,
  segment_admission_limits limits,
  const codec::limits& policy,
  bytes::allocation_charge_fn charge,
  byte_count working) {
    if (!project_retries(
          0,
          alignment,
          limits,
          policy,
          charge,
          working,
          wire_capacity_for(alignment, policy)))
        return runtime::failure(path_error(errc::resource_exhausted));
    return {};
}

detail::segment_plan_input
detail::plan_input(const encoded_assigned_batch& batch) noexcept {
    return {batch.info(), batch.bytes().size(), batch.bytes().fragment_count()};
}

runtime::result<detail::segment_capacity_constants>
detail::segment_capacity_constants::make(
  const local_segment_descriptor& descriptor,
  runtime::file_position data_start,
  segment_admission_limits limits,
  const codec::limits& policy,
  bytes::allocation_charge_fn charge,
  storage_alignment metadata_alignment,
  byte_count finalization_bytes) {
    if (auto valid = limits.validate(); !valid)
        return runtime::failure(valid.error());
    if (
      descriptor.profile != storage_profile::v1
      || descriptor.record_profile != 1)
        return runtime::failure(detail::path_error(errc::unsupported_format));
    if (
      !charge || descriptor.layout != local_layout_kind::initial
      || descriptor.physical_origin.value() != 0
      || data_start.value() < descriptor.alignment.bytes().value()
      || !descriptor.alignment.aligned(data_start))
        return runtime::failure(detail::path_error(errc::invalid_argument));
    const auto config = policy.config();
    segment_capacity_constants output{
      .descriptor = descriptor,
      .data_start = data_start,
      .limits = limits,
      .policy = policy,
      .charge = charge,
      .finalization_bytes = finalization_bytes,
      .tiny = config.max_work_bytes < byte_count{1024}
              || config.max_work_items < item_count{64},
      .block_limits = {
        config.max_encoded_body_bytes,
        byte_count{
          config.max_encoded_body_bytes.value()
          + config.max_header_bytes.value()}}};
    if (output.tiny) return output;
    auto footer = aligned_envelope_layout::make(
      {byte_count{codec::envelope_prefix_bytes},
       durable_footer_fixed_bytes,
       {}},
      descriptor.alignment,
      policy,
      {config.max_page_bytes, config.max_page_bytes});
    if (footer) {
        auto memory = staging_cost(
          0, durable_footer_fixed_bytes, *footer, policy, charge);
        if (memory) {
            output.footer_bytes = footer->encoded_bytes();
            output.footer_staging = memory->additional;
        }
    }
    output.pointer_bytes = publication_pointer_bytes(
      metadata_alignment, limits, policy, charge, finalization_bytes);
    const auto descriptor_format = local_metadata_descriptor_for(
                                     static_cast<std::uint16_t>(
                                       local_metadata_kind::segment_descriptor))
                                     .value();
    const auto descriptor_layout = local_metadata_layout(
      local_metadata_kind::segment_descriptor,
      byte_count{descriptor_format.minimum_payload_bytes},
      byte_count{codec::envelope_prefix_bytes},
      metadata_alignment,
      policy);
    if (descriptor_layout)
        output.descriptor_bytes = descriptor_layout->encoded_bytes();
    // One retained grant node per accepted group, independent of its larger
    // encoding workspace. The finalizer supplies actual completion facts.
    const byte_count control_request{
      sizeof(workload_reservation) + 2 * sizeof(void*) + 64};
    const auto fact_control = charge(control_request);
    if (
      fact_control >= control_request
      && policy.validate_allocation(fact_control))
        output.fact_control = fact_control;
    output.retry_wire_capacity = wire_capacity_for(
      descriptor.alignment, policy);
    return output;
}

std::optional<byte_count>
detail::segment_capacity_constants::fresh_root(std::size_t batches) {
    if (batches > maximum_segment_group_blocks) return std::nullopt;
    if (!fresh_known[batches]) {
        auto fresh = project_retries(
          batches,
          descriptor.alignment,
          limits,
          policy,
          charge,
          finalization_bytes,
          retry_wire_capacity);
        if (fresh) fresh_roots[batches] = fresh->root;
        fresh_known[batches] = true;
    }
    return fresh_roots[batches];
}

runtime::result<segment_capacity_plan> plan_segment_capacity(
  const local_segment_descriptor& descriptor,
  runtime::file_position data_start,
  segment_writer_position current,
  std::span<const encoded_assigned_batch> batches,
  segment_admission_limits limits,
  const codec::limits& policy,
  bytes::allocation_charge_fn charge,
  storage_alignment metadata_alignment,
  byte_count finalization_bytes) {
    if (batches.size() > maximum_segment_group_blocks)
        return runtime::failure(detail::path_error(errc::invalid_argument));
    std::array<const encoded_assigned_batch*, maximum_segment_group_blocks>
      input{};
    for (std::size_t i = 0; i < batches.size(); ++i)
        input[i] = &batches[i];
    auto constants = detail::segment_capacity_constants::make(
      descriptor,
      data_start,
      limits,
      policy,
      charge,
      metadata_alignment,
      finalization_bytes);
    if (!constants) return runtime::failure(constants.error());
    return detail::plan_segment_capacity(
      *constants, current, std::span{input}.first(batches.size()));
}

runtime::result<segment_capacity_plan> detail::plan_segment_capacity(
  segment_capacity_constants& fixed,
  segment_writer_position current,
  std::span<const encoded_assigned_batch* const> batches) {
    const auto& descriptor = fixed.descriptor;
    const auto& policy = fixed.policy;
    const auto& limits = fixed.limits;
    if (
      batches.empty() || batches.size() > maximum_segment_group_blocks
      || current.bytes < fixed.data_start
      || current.logical < descriptor.logical_origin
      || !descriptor.alignment.aligned(current.bytes))
        return runtime::failure(detail::path_error(errc::invalid_argument));
    const auto config = policy.config();
    segment_capacity_plan output;
    if (fixed.tiny) return output;
    auto logical = current.logical;
    std::uint64_t records = 0;
    for (const auto* input : batches) {
        if (!input)
            return runtime::failure(detail::path_error(errc::invalid_argument));
        const auto& batch = *input;
        auto location = segment_write_context::make(
          descriptor.segment,
          descriptor.alignment,
          current.physical,
          current.bytes);
        if (!location)
            return runtime::failure(detail::path_error(errc::invalid_argument));
        auto initial = validate_initial_append(batch, *location);
        if (!initial)
            return runtime::failure(
              detail::path_error(
                initial.error() == errc::wrong_context
                  ? errc::wrong_context
                  : errc::invalid_argument));
        const auto info = batch.info();
        if (info.context.logical_span().begin() != logical)
            return runtime::failure(detail::path_error(errc::wrong_context));
        logical = info.context.logical_span().end();
        if (!policy.validate_batch_counts(
              item_count{info.context.submitted().original_count().value()},
              info.context.retained_count(),
              info.header_count))
            return output;
        auto layout = aligned_envelope_layout::make(
          {byte_count{codec::envelope_prefix_bytes},
           segment_block_fixed_bytes,
           batch.bytes().size()},
          descriptor.alignment,
          policy,
          fixed.block_limits);
        if (!layout) return output;
        auto staging = staging_cost(
          batch.bytes().fragment_count(),
          segment_block_fixed_bytes,
          *layout,
          policy,
          fixed.charge);
        if (!staging) return output;
        output.staging_bytes
          = output.staging_bytes.checked_add(staging->additional).value();
        output.blocks
          = output.blocks.checked_add(layout->encoded_bytes()).value();
        records += info.context.retained_count().value();
    }
    if (!fixed.footer_bytes) return output;
    output.staging_bytes
      = output.staging_bytes.checked_add(*fixed.footer_staging).value();
    output.footer = *fixed.footer_bytes;
    const auto incoming = output.blocks.checked_add(output.footer);
    if (
      !incoming || *incoming > runtime::maximum_file_io_bytes
      || batches.size() > limits.maximum_blocks)
        return output;
    if (!fixed.pointer_bytes || !fixed.descriptor_bytes) return output;
    auto fresh = fixed.fresh_root(batches.size());
    if (!fresh) return output;
    auto new_facts = metadata_charge(
      batches.size(),
      sizeof(completed_retry),
      std::min(limits.metadata_bytes, config.max_metadata_bytes),
      policy,
      fixed.charge);
    if (!new_facts || !fixed.fact_control) return output;
    output.retry_metadata = new_facts->checked_add(*fixed.fact_control).value();
    auto empty_end = fixed.data_start.checked_add(*incoming);
    if (
      !empty_end || !empty_end->checked_add(*fresh)
      || empty_end->checked_add(*fresh)->value()
           > descriptor.maximum_data_bytes.value())
        return output;
    output.decision = segment_capacity_decision::roll_required;
    auto retry = project_retries(
      std::uint64_t{current.retry_entries} + batches.size(),
      descriptor.alignment,
      limits,
      policy,
      fixed.charge,
      fixed.finalization_bytes,
      fixed.retry_wire_capacity);
    const auto physical = current.physical.checked_add(
      model::segment_record_count{records});
    const auto data_end = current.bytes.checked_add(*incoming);
    if (
      !retry || !physical || !data_end
      || batches.size() > limits.maximum_blocks
                            - std::min(current.blocks, limits.maximum_blocks)
      || current.footers == std::numeric_limits<std::uint32_t>::max())
        return output;
    auto sealed_end = data_end->checked_add(retry->root);
    if (
      !sealed_end
      || sealed_end->value() > descriptor.maximum_data_bytes.value())
        return output;
    output.decision = segment_capacity_decision::fits;
    output.end = segment_writer_position{
      logical,
      *physical,
      *data_end,
      current.blocks + static_cast<std::uint32_t>(batches.size()),
      current.footers + 1,
      current.retry_entries + static_cast<std::uint32_t>(batches.size())};
    output.retry_pages = retry->pages;
    output.retry_page_entries = retry->entries_per_page;
    output.sealed_root = retry->root;
    output.disk = {
      byte_count{sealed_end->value()},
      retry->disk,
      byte_count{2 * fixed.pointer_bytes->value()},
      *fixed.descriptor_bytes};
    return output;
}
} // namespace kwaque::storage
