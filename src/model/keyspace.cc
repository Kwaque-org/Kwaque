#include "src/model/keyspace.h"

#include "src/base/error.h"
#include "src/base/result.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <span>

namespace kwaque::model {

result<void> validate_keyspace_coverage(
  std::span<const keyspace_interval> intervals,
  keyspace_interval target) noexcept {
    if (intervals.size() > max_unordered_keyspace_intervals) {
        return failure(errc::resource_exhausted);
    }

    std::array<const keyspace_interval*, max_unordered_keyspace_intervals>
      scratch{};
    static_assert(sizeof(scratch) <= 8U * 1024U);
    auto sorted = std::span{scratch}.first(intervals.size());
    for (std::size_t index = 0; index < intervals.size(); ++index) {
        sorted[index] = &intervals[index];
    }
    std::ranges::sort(
      sorted,
      [](
        const keyspace_interval* left,
        const keyspace_interval* right) noexcept {
          if (left->prefix() != right->prefix()) {
              return left->prefix() < right->prefix();
          }
          return left->depth() < right->depth();
      });

    ordered_keyspace_coverage coverage{target};
    for (const auto* interval : sorted) {
        if (auto added = coverage.append(*interval); !added) {
            return added;
        }
    }
    return coverage.finish();
}

} // namespace kwaque::model
