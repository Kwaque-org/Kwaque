#ifndef KWAQUE_SRC_RUNTIME_DIRECTORY_CURSOR_INTERNAL_H_
#define KWAQUE_SRC_RUNTIME_DIRECTORY_CURSOR_INTERNAL_H_

#include "src/runtime/shard_affinity.h"

#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>

#include <utility>

namespace kwaque::runtime::detail {

// A slot covers one fixed native cursor plus one bounded page. Keeping this
// owner in returned pages prevents repeated open/close from bypassing the cap.
struct directory_cursor_memory final : shard_affine {
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
