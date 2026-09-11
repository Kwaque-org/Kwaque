#include "src/codec/limits.h"

#include "src/base/error.h"
#include "src/base/result.h"
#include "src/base/units.h"

#include <algorithm>
#include <array>
#include <cstdint>
#include <limits>

namespace kwaque::codec {

namespace {

constexpr limits_config absolute_limits;

constexpr std::array byte_limits{
  &limits_config::max_allocation_bytes,
  &limits_config::max_record_bytes,
  &limits_config::max_header_name_bytes,
  &limits_config::max_record_header_bytes,
  &limits_config::max_expanded_batch_bytes,
  &limits_config::max_metadata_bytes,
  &limits_config::max_encoded_body_bytes,
  &limits_config::max_retained_bytes,
  &limits_config::max_operation_bytes,
  &limits_config::max_scratch_bytes,
  &limits_config::max_header_bytes,
  &limits_config::max_control_bytes,
  &limits_config::max_control_field_bytes,
  &limits_config::max_checkpoint_bytes,
  &limits_config::max_page_bytes,
  &limits_config::max_work_bytes};

constexpr std::array count_limits{
  &limits_config::max_buffer_fragments,
  &limits_config::max_record_headers,
  &limits_config::max_original_records,
  &limits_config::max_batch_headers,
  &limits_config::max_extensions,
  &limits_config::max_nesting_depth,
  &limits_config::max_control_fields,
  &limits_config::max_control_repeated,
  &limits_config::max_checkpoint_cursors,
  &limits_config::max_object_entries,
  &limits_config::max_object_pages,
  &limits_config::max_work_items};

// Keep both validation and narrowing complete when the fixed profile changes.
static_assert(
  sizeof(limits_config)
  == byte_limits.size() * sizeof(byte_count)
       + count_limits.size() * sizeof(item_count));

} // namespace

kwaque::result<limits> limits::make(limits_config config) noexcept {
    for (const auto member : byte_limits) {
        if (
          (config.*member).value() == 0
          || config.*member > absolute_limits.*member) {
            return kwaque::failure(errc::invalid_argument);
        }
    }
    for (const auto member : count_limits) {
        if (
          (config.*member).value() == 0
          || config.*member > absolute_limits.*member) {
            return kwaque::failure(errc::invalid_argument);
        }
    }
    return limits{config};
}

limits limits::intersect(const limits& requested) const noexcept {
    auto narrowed = config_;
    for (const auto member : byte_limits) {
        narrowed.*member = std::min(config_.*member, requested.config_.*member);
    }
    for (const auto member : count_limits) {
        narrowed.*member = std::min(config_.*member, requested.config_.*member);
    }
    return limits{narrowed};
}

kwaque::result<void> limits::validate_buffer(
  byte_count logical_bytes,
  byte_count retained_bytes,
  item_count fragments,
  byte_count logical_limit) const noexcept {
    if (
      logical_bytes > retained_bytes
      || (retained_bytes.value() == 0) != (fragments.value() == 0)) {
        return kwaque::failure(errc::invalid_argument);
    }
    if (
      logical_bytes > logical_limit
      || retained_bytes > config_.max_retained_bytes
      || fragments > config_.max_buffer_fragments) {
        return kwaque::failure(errc::resource_exhausted);
    }
    return {};
}

kwaque::result<void> limits::validate_batch_counts(
  item_count original_records,
  item_count retained_records,
  item_count headers) const noexcept {
    if (
      original_records.value() == 0 || retained_records.value() == 0
      || retained_records > original_records) {
        return kwaque::failure(errc::invalid_argument);
    }
    if (
      original_records > config_.max_original_records
      || headers > config_.max_batch_headers) {
        return kwaque::failure(errc::resource_exhausted);
    }
    static_assert(
      absolute_max_original_records
      <= std::numeric_limits<std::uint64_t>::max()
           / absolute_limits.max_record_headers.value());
    // Construction and the checks above bound both factors before multiplying.
    const auto possible_headers = retained_records.value()
                                  * config_.max_record_headers.value();
    if (headers.value() > possible_headers) {
        return kwaque::failure(errc::resource_exhausted);
    }
    return {};
}

kwaque::result<byte_count> limits::remaining_operation_bytes(
  const operation_usage& usage, byte_count parent_remaining) const noexcept {
    byte_count accounted;
    for (const auto cost : std::array{
           usage.retained_input,
           usage.staged_output,
           usage.decoded_metadata,
           usage.scratch,
           usage.payload_bookkeeping,
           usage.payload_migration}) {
        const auto next = accounted.checked_add(cost);
        if (!next) {
            return kwaque::failure(errc::out_of_range);
        }
        accounted = *next;
    }
    if (
      usage.decoded_metadata > config_.max_metadata_bytes
      || usage.scratch > config_.max_scratch_bytes) {
        return kwaque::failure(errc::resource_exhausted);
    }
    const auto available = std::min(
      config_.max_operation_bytes, parent_remaining);
    const auto remaining = available.checked_sub(accounted);
    if (!remaining) {
        return kwaque::failure(errc::resource_exhausted);
    }
    return *remaining;
}

} // namespace kwaque::codec
