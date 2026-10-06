#ifndef KWAQUE_SRC_RUNTIME_DIRECTORY_CURSOR_INTERNAL_H_
#define KWAQUE_SRC_RUNTIME_DIRECTORY_CURSOR_INTERNAL_H_

#include "src/runtime/shard_affinity.h"

#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>

#include <utility>

namespace kwaque::runtime::detail {

// A slot covers what one cursor costs its backend beyond its caller's own
// admission. Keeping this owner in returned pages prevents repeated open/close
// from bypassing the cap.
//
// A backend that models a descriptor limit takes the slot when the cursor
// opens. The native one bounds only the page a listing cursor holds, so its
// cursors take the slot with their first page: a directory kept open to be
// synced lists nothing, is already paid for by its owner's handle credit,
// and must not use up the room of those that list.
struct directory_cursor_memory final : shard_affine {
    explicit directory_cursor_memory(
      seastar::lw_shared_ptr<seastar::semaphore> owner) noexcept
      : pool(std::move(owner)) {}
    directory_cursor_memory(
      seastar::lw_shared_ptr<seastar::semaphore> owner,
      seastar::semaphore_units<> slot) noexcept
      : pool(std::move(owner))
      , handle(std::move(slot)) {}
    ~directory_cursor_memory() { assert_current(); }
    seastar::lw_shared_ptr<seastar::semaphore> pool;
    seastar::semaphore_units<> handle;
    seastar::semaphore page{1};
};

} // namespace kwaque::runtime::detail

#endif // KWAQUE_SRC_RUNTIME_DIRECTORY_CURSOR_INTERNAL_H_
