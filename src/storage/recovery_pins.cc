#include "src/storage/recovery_pins.h"

#include <array>

namespace kwaque::storage::detail {
namespace {
constexpr std::uint16_t durable_footer_family = static_cast<std::uint16_t>(
  codec::format_family::durable_boundary_footer);
constexpr std::uint16_t sealed_root_family = static_cast<std::uint16_t>(
  codec::format_family::sealed_extent);
} // namespace

runtime::result<void>
validate_recovery_publication(const local_object_publication& publication) {
    const auto wrong = [] {
        return runtime::failure(path_error(errc::wrong_context));
    };
    std::array<std::uint32_t, 5> seen{};
    for (const auto& root : publication.roots) {
        const auto kind = static_cast<std::size_t>(root.kind());
        if (kind >= seen.size() || seen[kind]++ != 0) return wrong();
    }
    const auto count = [&seen](local_root_kind kind) {
        return seen[static_cast<std::size_t>(kind)];
    };
    if (
      count(local_root_kind::checkpoint) != 0
      || count(local_root_kind::manifest) != 0)
        return wrong();
    const auto family = publication.boundary
                          ? std::optional{publication.boundary->family()}
                          : std::nullopt;
    switch (publication.state) {
    case local_object_state::active:
    case local_object_state::recovering:
        if (
          (family && *family != durable_footer_family)
          || count(local_root_kind::index) != 0
          || count(local_root_kind::sealed_retry) != 0)
            return wrong();
        return {};
    case local_object_state::sealed:
        if (
          family != sealed_root_family
          || count(local_root_kind::sealed_retry) != 1)
            return wrong();
        return {};
    case local_object_state::deleting:
        return {};
    }
    return wrong();
}

runtime::result<std::vector<local_bundle_context>> recovery_root_contexts(
  const local_device_spec& spec,
  std::uint32_t shard,
  const segment_history_context& history,
  const local_object_publication& publication,
  const sealed_footer* sealed) {
    const auto owner = spec.shard_owner(shard);
    if (!owner) return runtime::failure(path_error(errc::wrong_context));
    std::vector<local_bundle_context> output;
    output.reserve(publication.roots.size());
    for (const auto& root : publication.roots) {
        switch (root.kind()) {
        case local_root_kind::index: {
            if (!sealed)
                return runtime::failure(path_error(errc::wrong_context));
            auto context = sparse_index_context::make(
              history.segment,
              sealed->coverage(),
              sealed->extent_digest(),
              history.alignment,
              history.profile);
            if (!context)
                return runtime::failure(path_error(errc::wrong_context));
            output.emplace_back(*context);
            break;
        }
        case local_root_kind::sealed_retry:
            if (!publication.boundary)
                return runtime::failure(path_error(errc::wrong_context));
            output.emplace_back(
              footer_expectation{history, publication.boundary->position()});
            break;
        case local_root_kind::checkpoint:
        case local_root_kind::manifest:
            return runtime::failure(path_error(errc::wrong_context));
        }
    }
    return output;
}

} // namespace kwaque::storage::detail
