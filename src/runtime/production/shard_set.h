#pragma once

#include "src/runtime/cross_shard.h"

#include <seastar/core/smp.hh>

#include <type_traits>
#include <utility>

namespace kwaque::runtime::production {

// The broker's reactor shards: Seastar's own cross-shard calls, through the
// one submission path every Kwaque call shares.
class shard_set final {
public:
    [[nodiscard]] seastar::shard_id current() const noexcept {
        return seastar::this_shard_id();
    }
    [[nodiscard]] unsigned count() const noexcept {
        return seastar::this_smp_shard_count();
    }

    template<typename Func, typename... Args>
    auto invoke_on(
      seastar::shard_id target,
      seastar::smp_service_group group,
      Func func,
      Args... args) {
        return runtime::detail::invoke_on_shard<
          runtime::detail::cross_shard_result<Func, Args...>>(
          target, group, std::move(func), std::move(args)...);
    }
};

} // namespace kwaque::runtime::production
