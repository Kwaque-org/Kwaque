#pragma once

#include <seastar/core/shard_id.hh>

namespace kwaque::runtime {

namespace detail {

// How far Kwaque's shard identity is from Seastar's on this thread. Only the
// deterministic simulation writes it, to run several virtual shards on one
// reactor; production never does, so there the identity is Seastar's.
inline constinit thread_local seastar::shard_id virtual_shard_offset = 0;

} // namespace detail

// The shard Kwaque code runs on: Seastar's, or the virtual shard the
// simulation is running. One more load from the same thread-local block in
// production, with no branch.
[[nodiscard]] inline seastar::shard_id current_shard() noexcept {
    return seastar::this_shard_id() + detail::virtual_shard_offset;
}

} // namespace kwaque::runtime
