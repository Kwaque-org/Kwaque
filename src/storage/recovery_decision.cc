#include "src/storage/recovery_decision.h"

#include "src/storage/recovery_hash.h"

#include <string_view>

namespace kwaque::storage {

recovery_suffix_identity::recovery_suffix_identity(
  const segment_context& segment, runtime::file_position end) noexcept {
    static constexpr std::string_view domain{"KQ/RECOVERY-SUFFIX-PLAN/1"};
    hash_.update(domain.data(), domain.size());
    detail::hash_segment(hash_, segment);
    detail::hash_u64(hash_, end.value());
}

void recovery_suffix_identity::add(
  const recovery_suffix_prepare& prepare) noexcept {
    const auto incarnation = prepare.begin.incarnation();
    detail::hash_id(hash_, incarnation);
    detail::hash_u64(hash_, prepare.begin.position().value());
    detail::hash_u64(hash_, prepare.end.position().value());
    detail::hash_u64(hash_, prepare.target.value());
    detail::hash_digest(hash_, prepare.digest.bytes());
    ++count_;
}

codec::immutable_object_digest recovery_suffix_identity::finish() && noexcept {
    detail::hash_u64(hash_, count_);
    return codec::immutable_object_digest{std::move(hash_).final()};
}

runtime::result<void> recovery_decision_limits::validate() const noexcept {
    if (auto valid = metadata.validate(); !valid) return valid;
    if (decisions == 0)
        return runtime::failure(detail::path_error(errc::invalid_argument));
    return {};
}

namespace detail {
bool recovery_decisions_agree(
  const local_recovery_decision& candidate,
  runtime::file_position end) noexcept {
    return candidate.action
           == (candidate.target_position < end ? local_recovery_action::reconstruct : local_recovery_action::discard);
}
} // namespace detail

} // namespace kwaque::storage
