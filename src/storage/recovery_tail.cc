#include "src/storage/recovery_tail.h"

#include "src/storage/page_internal.h"

namespace kwaque::storage::detail {

runtime::result<void> hold_more(
  std::optional<workload_reservation>& held,
  workload_budget& budget,
  byte_count bytes) {
    auto more = budget.try_reserve(bytes);
    if (!more) return runtime::failure(more.error());
    if (!held) {
        held.emplace(std::move(*more));
        return {};
    }
    return held->adopt(std::move(*more));
}

bool recovered_layout_failure(errc code) noexcept {
    switch (code) {
    case errc::wrong_context:
    case errc::malformed_data:
    case errc::out_of_range:
    case errc::truncated_data:
    case errc::corrupt_data:
        return true;
    default:
        return false;
    }
}

seastar::future<runtime::result<recovery_copy_difference>>
compare_recovery_identity(
  const recovery_copy_identity& prepare,
  const segment_block& block,
  codec::cooperative_work& work) {
    auto difference = compare_recovery_claims(prepare, block);
    if (!difference)
        co_return runtime::failure(path_error(
          to_errc(difference.error()).value_or(errc::invariant_violation)));
    if (*difference != recovery_copy_difference::none) co_return *difference;
    auto child = block.child();
    if (!child)
        co_return runtime::failure(path_error(
          to_errc(child.error()).value_or(errc::invariant_violation)));
    auto digest = co_await hash_exact(*child, work, {});
    if (!digest) co_return runtime::failure(path_error(digest.error().code()));
    co_return *digest == prepare.child ? recovery_copy_difference::none
                                       : recovery_copy_difference::child;
}

} // namespace kwaque::storage::detail
