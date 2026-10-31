#pragma once

#include "src/runtime/shard_identity.h"

#include <seastar/core/shard_id.hh>

#include <cstddef>
#include <utility>

namespace kwaque::simulation {

// Makes the calling thread virtual shard `shard` until destroyed, then
// restores what it was. Only the deterministic simulation, which runs on
// reactor shard 0, makes these.
class virtual_shard_turn final {
public:
    explicit virtual_shard_turn(std::size_t shard) noexcept
      : previous_(
          std::exchange(
            runtime::detail::virtual_shard_offset,
            static_cast<seastar::shard_id>(shard - seastar::this_shard_id()))) {
    }

    virtual_shard_turn(const virtual_shard_turn&) = delete;
    virtual_shard_turn& operator=(const virtual_shard_turn&) = delete;
    virtual_shard_turn(virtual_shard_turn&&) = delete;
    virtual_shard_turn& operator=(virtual_shard_turn&&) = delete;

    ~virtual_shard_turn() { runtime::detail::virtual_shard_offset = previous_; }

private:
    seastar::shard_id previous_;
};

} // namespace kwaque::simulation
