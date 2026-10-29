#pragma once

#include "src/storage/local_append.h"

#include <seastar/core/semaphore.hh>

namespace kwaque::storage {
class local_append_test_access final {
public:
    // Holds the formation entrance as a formation in progress holds it:
    // requests accepted meanwhile join one forming group, which forms on
    // release.
    template<
      runtime::file_system_backend Backend,
      typename Owner,
      runtime::monotonic_clock Clock>
    static seastar::future<seastar::semaphore_units<>>
    hold_formation(local_append<Backend, Owner, Clock>& owner) {
        return seastar::get_units(owner.form_lock_, 1);
    }
    // Asks every attachment for a sync through its newest frozen group, as
    // sync() asks one.
    template<
      runtime::file_system_backend Backend,
      typename Owner,
      runtime::monotonic_clock Clock>
    static void sync_all(local_append<Backend, Owner, Clock>& owner) {
        for (const auto& slot : owner.slots_)
            if (slot->writer) owner.request_sync(*slot);
    }
};
} // namespace kwaque::storage
