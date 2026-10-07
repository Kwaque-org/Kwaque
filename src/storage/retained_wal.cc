#include "src/storage/retained_wal.h"

namespace kwaque::storage {
namespace {
runtime::operation_error chain_error(errc code) {
    return runtime::operation_error{code, runtime::operation_kind::file};
}
} // namespace

std::uint32_t
retained_wal::below(const local_wal_cursor& cutoff) const noexcept {
    std::uint32_t count = 0;
    while (count + 1 < count_
           && ring_[(first_ + count) % ring_.size()].canonical_less(
             cutoff.incarnation()))
        ++count;
    return count;
}

runtime::result<void>
retained_wal::extend(model::wal_incarnation_id file) noexcept {
    if (file.is_nil())
        return runtime::failure(chain_error(errc::invalid_argument));
    if (full()) return runtime::failure(chain_error(errc::resource_exhausted));
    if (count_ != 0 && !newest()->canonical_less(file))
        return runtime::failure(chain_error(errc::wrong_context));
    ring_[(first_ + count_) % ring_.size()] = file;
    ++count_;
    return {};
}

void retained_wal::drop_oldest() noexcept {
    if (count_ < 2) return;
    first_ = (first_ + 1) % static_cast<std::uint32_t>(ring_.size());
    --count_;
}

} // namespace kwaque::storage
