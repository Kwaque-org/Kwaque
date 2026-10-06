#include "src/storage/sparse_index_owner.h"

namespace kwaque::storage {

sparse_index_residency::~sparse_index_residency() {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-INDEX-ROOTS-CLOSED"},
      open_.empty() && resident_ == 0,
      "index roots left open by the sealed indexes that opened them");
}

seastar::future<bool> sparse_index_residency::admit() {
    while (resident_ >= most_)
        if (!co_await evict()) co_return false;
    ++resident_;
    co_return true;
}

seastar::future<bool> sparse_index_residency::evict() {
    for (auto& held : open_) {
        if (held.root->pins() != 0) continue;
        const auto closed = co_await release(held);
        if (closed.exception())
            failed_.observe(closed.exception());
        else if (closed.error())
            failed_.observe(*closed.error());
        co_return true;
    }
    co_return false;
}

seastar::future<runtime::first_failure>
sparse_index_residency::release(root& held) {
    open_.erase(open_.iterator_to(held));
    const auto opened = std::move(held.root);
    held.retired = false;
    runtime::first_failure closed;
    try {
        closed.observe(co_await opened->close());
    } catch (...) {
        closed.observe(std::current_exception());
    }
    --resident_;
    co_return closed;
}

} // namespace kwaque::storage
