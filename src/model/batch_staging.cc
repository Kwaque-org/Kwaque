#include "src/model/batch_staging.h"

#include <algorithm>
#include <cstdint>
#include <limits>

namespace kwaque::model::detail {
namespace {
codec::error at(codec::error anchor, errc code) noexcept {
    return codec::error{
      code, anchor.family(), anchor.field(), anchor.byte_offset()};
}
} // namespace

codec::result<batch_staging_shape> make_batch_staging(
  const codec::limits& policy,
  bytes::allocation_charge_fn charge,
  codec::error anchor) {
    if (charge == nullptr)
        return codec::failure(at(anchor, errc::invalid_argument));
    const auto cap = policy.config();
    auto width = std::min(
      {std::uint64_t{65536},
       cap.max_allocation_bytes.value(),
       cap.max_expanded_batch_bytes.value()});
    while (width != 0) {
        const auto served = charge(byte_count{width});
        if (served < byte_count{width})
            return codec::failure(at(anchor, errc::invalid_argument));
        if (policy.validate_allocation(served)) break;
        width /= 2U;
    }
    if (width == 0) return codec::failure(at(anchor, errc::resource_exhausted));
    // A fixed tail size packs tiny records and gives an exact backing equation.
    // Reserve a single bounded descriptor array; no per-record container grows.
    auto count = std::min(
      cap.max_buffer_fragments.value(),
      1U + (cap.max_expanded_batch_bytes.value() - 1U) / width);
    byte_count descriptors;
    while (count != 0) {
        const byte_count request{
          count * bytes::fragmented_buffer::fragment_descriptor_size()};
        descriptors = charge(request);
        if (descriptors < request)
            return codec::failure(at(anchor, errc::invalid_argument));
        if (policy.validate_allocation(descriptors)) break;
        count /= 2U;
    }
    if (count == 0) return codec::failure(at(anchor, errc::resource_exhausted));
    bytes::fragmented_buffer_builder_config config;
    config.initial_fragment_bytes = byte_count{width};
    config.max_fragment_bytes = byte_count{width};
    config.max_total_bytes = byte_count{
      std::min(cap.max_expanded_batch_bytes.value(), width * count)};
    config.max_retained_bytes = byte_count{width * count};
    config.max_fragments = count;
    if (!config.validate())
        return codec::failure(at(anchor, errc::resource_exhausted));
    return batch_staging_shape{config, descriptors};
}

codec::result<byte_count> admit_batch_staging(
  batch_staging_shape shape,
  byte_count total,
  const codec::limits& policy,
  byte_count remaining,
  bytes::allocation_charge_fn charge,
  codec::error anchor) {
    if (total.value() == 0 || total > shape.config.max_total_bytes)
        return codec::failure(at(anchor, errc::resource_exhausted));
    const auto count = 1U
                       + (total.value() - 1U)
                           / shape.config.max_fragment_bytes.value();
    const auto tail = charge(shape.config.max_fragment_bytes);
    if (tail < shape.config.max_fragment_bytes)
        return codec::failure(at(anchor, errc::invalid_argument));
    if (
      !policy.validate_allocation(tail)
      || tail.value() > std::numeric_limits<std::uint64_t>::max() / count)
        return codec::failure(at(anchor, errc::resource_exhausted));
    const byte_count backing{count * tail.value()};
    if (!policy.validate_buffer(
          total,
          backing,
          item_count{count},
          policy.config().max_expanded_batch_bytes))
        return codec::failure(at(anchor, errc::resource_exhausted));
    const auto available = policy.remaining_operation_bytes(
      {.staged_output = backing, .payload_bookkeeping = shape.descriptors},
      remaining);
    if (!available) return codec::failure(at(anchor, errc::resource_exhausted));
    return *available;
}

} // namespace kwaque::model::detail
