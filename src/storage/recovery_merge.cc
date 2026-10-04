#include "src/storage/recovery_merge.h"

#include "src/base/allocation.h"
#include "src/codec/xxh3.h"
#include "src/storage/recovery_hash.h"

namespace kwaque::storage {
namespace {
using detail::path_error;

constexpr auto durable_footer_family = static_cast<std::uint16_t>(
  codec::format_family::durable_boundary_footer);
constexpr auto sealed_root_family = static_cast<std::uint16_t>(
  codec::format_family::sealed_extent);

} // namespace

static_assert(
  std::size_t{maximum_segment_group_blocks} * sizeof(detail::recovery_pending)
  <= maximum_contiguous_allocation_bytes);

runtime::result<void> recovery_merge_limits::validate() const noexcept {
    if (auto valid = wal.validate(); !valid) return valid;
    if (auto valid = segment.validate(); !valid) return valid;
    if (
      cursors == 0 || cursors > maximum_recovery_cursors || pending == 0
      || obligations == 0
      || std::size_t{pending} * sizeof(detail::recovery_queued)
           > maximum_contiguous_allocation_bytes
      || std::size_t{obligations} * sizeof(detail::recovery_obligation_row)
           > maximum_contiguous_allocation_bytes)
        return runtime::failure(path_error(errc::invalid_argument));
    return {};
}

namespace detail {
runtime::result<void>
validate_recovery_targets(std::span<const recovery_target> targets) noexcept {
    for (std::size_t i = 0; i < targets.size(); ++i) {
        const auto& target = targets[i];
        const auto& segment = target.descriptor.segment;
        if (
          target.header.context() != segment
          || (target.placement && target.placement->segment != segment))
            return runtime::failure(path_error(errc::invalid_argument));
        for (std::size_t j = 0; j < i; ++j)
            if (targets[j].descriptor.segment == segment)
                return runtime::failure(path_error(errc::invalid_argument));
        const auto& pin = target.pin;
        if (pin && !pin->validate_alignment(target.descriptor.alignment))
            return runtime::failure(path_error(errc::invalid_argument));
        switch (target.state) {
        case local_object_state::active:
        case local_object_state::recovering:
            if (pin && pin->family() != durable_footer_family)
                return runtime::failure(path_error(errc::invalid_argument));
            break;
        case local_object_state::sealed:
            if (!pin || pin->family() != sealed_root_family)
                return runtime::failure(path_error(errc::invalid_argument));
            break;
        case local_object_state::deleting:
            return runtime::failure(path_error(errc::invalid_argument));
        }
    }
    return {};
}

codec::content_digest recovery_binding(
  const local_shard_control& control,
  std::optional<local_wal_cursor> cutoff,
  std::span<const recovery_target> targets,
  std::uint32_t pending) noexcept {
    static constexpr std::string_view domain{"KQ/RECOVERY-MERGE/1"};
    codec::xxh3_128_hasher hash;
    hash.update(domain.data(), domain.size());
    hash_u64(hash, control.wal_head ? 1U : 0U);
    if (control.wal_head) {
        hash_id(hash, control.wal_head->incarnation);
        hash_digest(hash, control.wal_head->header_digest.bytes());
    }
    hash_u64(hash, control.checkpoint ? 1U : 0U);
    if (control.checkpoint) {
        const auto& root = *control.checkpoint;
        hash_u64(hash, static_cast<std::uint64_t>(root.kind()));
        hash_u64(hash, root.sequence().value());
        hash_u64(hash, root.position().value());
        hash_u64(hash, root.bytes().value());
        hash_u64(hash, root.pages().value());
        hash_digest(hash, root.digest().bytes());
    }
    hash_cursor(hash, cutoff);
    hash_u64(hash, targets.size());
    for (const auto& target : targets) {
        hash_segment(hash, target.descriptor.segment);
        hash_u64(hash, static_cast<std::uint64_t>(target.state));
        hash_u64(hash, target.generation.value());
        hash_footer(hash, target.pin);
    }
    hash_u64(hash, pending);
    return std::move(hash).final();
}

runtime::file_position
footer_end(const local_footer_reference& footer) noexcept {
    return runtime::file_position{
      footer.position().value() + footer.bytes().value()};
}

runtime::file_position
certified_end(const recovery_segment_state& state) noexcept {
    return state.resume ? footer_end(*state.resume) : state.history->data_start;
}

void count_slot(
  recovery_segment_report& report,
  recovery_classification classification) noexcept {
    switch (classification) {
    case recovery_classification::satisfied:
        ++report.satisfied;
        break;
    case recovery_classification::footer_only:
        ++report.footer_only;
        break;
    case recovery_classification::candidate:
        ++report.candidates;
        break;
    case recovery_classification::suffix:
        ++report.suffix;
        break;
    case recovery_classification::conflict:
        ++report.conflicts;
        break;
    case recovery_classification::content:
    case recovery_classification::uncertified_tail:
    case recovery_classification::corruption:
    case recovery_classification::slack:
        break;
    }
}
} // namespace detail

} // namespace kwaque::storage
