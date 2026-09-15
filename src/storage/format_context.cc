#include "src/storage/format_context.h"

#include "src/base/error.h"

#include <bit>

namespace kwaque::storage {

result<storage_profile> parse_storage_profile(std::uint16_t value) noexcept {
    if (value == 0) return failure(errc::invalid_argument);
    if (value != 1) return failure(errc::unsupported_format);
    return storage_profile::v1;
}
result<replay_profile> parse_replay_profile(std::uint16_t value) noexcept {
    if (value == 0) return failure(errc::invalid_argument);
    if (value != 1) return failure(errc::unsupported_format);
    return replay_profile::v1;
}

result<storage_alignment> storage_alignment::make(byte_count bytes) noexcept {
    if (
      bytes.value() < 512 || bytes.value() > 65536
      || !std::has_single_bit(bytes.value()))
        return failure(errc::invalid_argument);
    return storage_alignment{bytes};
}

result<segment_context> segment_context::make(
  model::cluster_id cluster,
  model::topic_id topic,
  model::range_id range,
  model::segment_id segment,
  model::segment_generation generation) noexcept {
    if (
      cluster.is_nil() || topic.is_nil() || range.is_nil() || segment.is_nil()
      || !generation.is_valid())
        return failure(errc::invalid_argument);
    return segment_context{cluster, topic, range, segment, generation};
}
result<void> segment_context::validate_expected(
  const segment_context& expected) const noexcept {
    if (*this != expected) return failure(errc::wrong_context);
    return {};
}

result<segment_write_context> segment_write_context::make(
  segment_context segment,
  storage_alignment alignment,
  model::segment_relative_end physical_begin,
  runtime::file_position position) noexcept {
    if (!alignment.aligned(position)) return failure(errc::invalid_argument);
    return segment_write_context{segment, alignment, physical_begin, position};
}
result<void> segment_write_context::validate_expected(
  const segment_write_context& expected) const noexcept {
    if (*this != expected) return failure(errc::wrong_context);
    return {};
}

result<wal_write_context> wal_write_context::make(
  model::wal_incarnation_id incarnation,
  storage_alignment alignment,
  runtime::file_position position) noexcept {
    if (incarnation.is_nil() || !alignment.aligned(position))
        return failure(errc::invalid_argument);
    return wal_write_context{incarnation, alignment, position};
}
result<void> wal_write_context::validate_expected(
  const wal_write_context& expected) const noexcept {
    if (*this != expected) return failure(errc::wrong_context);
    return {};
}

} // namespace kwaque::storage
