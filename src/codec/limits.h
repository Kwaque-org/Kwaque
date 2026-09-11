#pragma once

#include "src/base/allocation.h"
#include "src/base/error.h"
#include "src/base/result.h"
#include "src/base/units.h"

#include <cstdint>

namespace kwaque::codec {

inline constexpr std::uint64_t absolute_max_original_records{4096};

// The defaults are the absolute ceilings for this codec profile. Callers can
// narrow individual ceilings; they need not make every maximum fit at once.
struct limits_config final {
    byte_count max_allocation_bytes{maximum_contiguous_allocation_bytes};
    item_count max_buffer_fragments{1024};
    // Complete record encoding, including its lengths and header framing.
    byte_count max_record_bytes{1'048'576};
    item_count max_record_headers{64};
    byte_count max_header_name_bytes{4096};
    // Sum of header-name/value payload bytes; framing also counts above.
    byte_count max_record_header_bytes{65'536};
    item_count max_original_records{absolute_max_original_records};
    item_count max_batch_headers{4096};
    byte_count max_expanded_batch_bytes{8'388'608};
    // Decoded batch/control descriptors, shares and metadata migration.
    byte_count max_metadata_bytes{1'048'576};
    // The body cap excludes the enclosing binary/frame header.
    byte_count max_encoded_body_bytes{16'777'216};
    // Backing per accepted object, not aggregate input across several objects.
    byte_count max_retained_bytes{33'554'432};
    byte_count max_operation_bytes{67'108'864};
    byte_count max_scratch_bytes{1'048'576};
    byte_count max_header_bytes{4096};
    item_count max_extensions{64};
    byte_count max_control_bytes{65'536};
    item_count max_nesting_depth{8};
    item_count max_control_fields{256};
    item_count max_control_repeated{256};
    byte_count max_control_field_bytes{4096};
    item_count max_checkpoint_cursors{4096};
    // Checkpoint and page/root caps include their enclosing headers/padding.
    byte_count max_checkpoint_bytes{131'072};
    byte_count max_page_bytes{65'536};
    item_count max_object_entries{65'536};
    item_count max_object_pages{256};
    byte_count max_work_bytes{65'536};
    item_count max_work_items{256};

    bool operator==(const limits_config&) const noexcept = default;
};

// Disjoint live allocation charges, including served capacities and native
// reservations. These are not encoded lengths or a measurement of shard RSS.
struct operation_usage final {
    byte_count retained_input;
    byte_count staged_output;
    // All decoded descriptors, auxiliary/share bookkeeping and overlapping
    // old/new metadata allocations belong under this local ceiling.
    byte_count decoded_metadata;
    byte_count scratch;
    // Only non-decoded payload buffer descriptors/share bookkeeping here.
    byte_count payload_bookkeeping;
    // Additional overlapping payload allocations not already charged above.
    // Metadata migration belongs in decoded_metadata, not in this field.
    byte_count payload_migration;

    bool operator==(const operation_usage&) const noexcept = default;
};

class limits final {
public:
    [[nodiscard]] static constexpr limits defaults() noexcept {
        return limits{limits_config{}};
    }

    [[nodiscard]] static result<limits> make(limits_config config) noexcept;

    // An owning snapshot; editing it cannot mutate a validated limits value.
    [[nodiscard]] constexpr limits_config config() const noexcept {
        return config_;
    }

    // Child ceilings can never widen the parent's policy. This does not reserve
    // memory: nested operations must also carry the remaining parent budget.
    [[nodiscard]] limits intersect(const limits& requested) const noexcept;

    // Supply the served/reserved allocation capacity, including rounding.
    [[nodiscard]] result<void>
    validate_allocation(byte_count charged_capacity) const noexcept {
        if (charged_capacity > config_.max_allocation_bytes) {
            return failure(errc::resource_exhausted);
        }
        return {};
    }

    // Conservative per-fragment backing charges, not deduplicated aliases.
    // Empty allocated staging is valid. The owner supplies the complete logical
    // cap for this buffer; a body cap alone omits envelope headers/padding.
    // Each actual allocation still needs its own capacity check.
    [[nodiscard]] result<void> validate_buffer(
      byte_count logical_bytes,
      byte_count retained_bytes,
      item_count fragments,
      byte_count logical_limit) const noexcept;

    // Data-bearing batches only. Zero retained data uses separate extent/retry
    // metadata. These bounds do not prove per-record counts or payload
    // contents.
    [[nodiscard]] result<void> validate_batch_counts(
      item_count original_records,
      item_count retained_records,
      item_count headers) const noexcept;

    // The owner charges each accepted object's backing separately and passes
    // additional live charges here. Deduplicate already-charged shared storage
    // only with ownership/accounting evidence. parent_remaining is mandatory,
    // including zero; no child receives a fresh allowance. This check does not
    // reserve or mutate the supplied budget.
    [[nodiscard]] result<byte_count> remaining_operation_bytes(
      const operation_usage& usage, byte_count parent_remaining) const noexcept;

    bool operator==(const limits&) const noexcept = default;

private:
    constexpr explicit limits(limits_config config) noexcept
      : config_(config) {}

    limits_config config_;
};

} // namespace kwaque::codec
