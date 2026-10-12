#pragma once

#include "src/runtime/error.h"
#include "src/storage/local_metadata.h"
#include "src/storage/local_store_config.h"
#include "src/storage/local_types.h"
#include "src/storage/recovery_inventory.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>

namespace kwaque::storage {

// Why a store may not be opened, by the device that keeps it closed.
enum class store_refusal : std::uint8_t {
    none,
    // Two configured devices are one: the same identity, or one directory.
    duplicated,
    // Its directory could not be read, or a check of who owns it failed.
    missing,
    // Nothing where a store must be: emptied, or a new disk in its place.
    emptied,
    // A record a start needs is not there: its identity, a shard's
    // control record, or the WAL head a control record pins.
    incomplete,
    // A format this build does not read.
    unsupported,
    // Its records name another store or another device, or fail their
    // integrity check: swapped, or of another store.
    foreign,
    // The durable checkpoint names a device that is not configured.
    unconfigured,
};

struct store_admission final {
    store_refusal refusal{store_refusal::none};
    // The device that keeps the store closed. For `unconfigured`, the
    // device the checkpoint names.
    std::optional<device_store_id> device;
    // What its inspection failed with, when it failed.
    std::optional<runtime::operation_error> error;
    // In configuration order: devices the runtime schedules on no queue of
    // their own. Reported; never a refusal.
    std::array<bool, maximum_local_devices> unqueued{};
    [[nodiscard]] bool admitted() const noexcept {
        return refusal == store_refusal::none;
    }
};

// What a restart's verdict on one device means for opening the store.
[[nodiscard]] constexpr store_refusal
store_refusal_of(recovery_store_verdict verdict) noexcept {
    switch (verdict) {
    case recovery_store_verdict::ready:
        return store_refusal::none;
    case recovery_store_verdict::unavailable:
        return store_refusal::missing;
    case recovery_store_verdict::incomplete:
        return store_refusal::incomplete;
    case recovery_store_verdict::lost:
        return store_refusal::emptied;
    case recovery_store_verdict::unsupported:
        return store_refusal::unsupported;
    case recovery_store_verdict::corrupt:
        return store_refusal::foreign;
    }
    return store_refusal::foreign;
}

// Whether the store that was there may be opened on the configured
// devices, before anything is admitted. Every configured device must be
// that store's and ready, as its inspection found it; no two may be one;
// and every device the durable checkpoint names must be configured to hold
// data, since a segment the checkpoint pins on a device that is not there
// would look as if it had never been written. The first refusal by
// severity names its device.
//
// `pinned` is the entries of every shard's durable checkpoint. `queued`,
// when given, says for each configured device whether the runtime
// schedules it on a queue of its own.
[[nodiscard]] inline store_admission admit_local_store(
  std::span<const local_device_spec> devices,
  const recovery_store_report& inspected,
  std::span<const local_checkpoint_entry> pinned,
  std::span<const bool> queued = {}) noexcept {
    store_admission output;
    for (std::size_t i = 0; i != devices.size(); ++i)
        for (std::size_t j = 0; j != i; ++j)
            if (
              devices[i].owner.device() == devices[j].owner.device()
              || devices[i].directory == devices[j].directory) {
                output.refusal = store_refusal::duplicated;
                output.device = devices[i].owner.device();
                return output;
            }
    if (
      devices.empty() || devices.size() > maximum_local_devices
      || inspected.inspected != devices.size()) {
        output.refusal = store_refusal::missing;
        output.error = detail::path_error(errc::invalid_argument);
        return output;
    }
    for (std::size_t i = 0; i != devices.size(); ++i)
        output.unqueued[i] = i < queued.size() && !queued[i];
    auto worst = recovery_store_verdict::ready;
    for (std::size_t i = 0; i != devices.size(); ++i) {
        const auto& found = inspected.devices[i];
        if (found.verdict <= worst) continue;
        worst = found.verdict;
        output.refusal = store_refusal_of(found.verdict);
        output.device = devices[i].owner.device();
        output.error = found.error;
    }
    if (!output.admitted()) return output;
    for (const auto& entry : pinned) {
        if (entry.device.is_nil()) continue;
        bool configured = false;
        for (const auto& spec : devices)
            configured
              = configured
                || (spec.stores_data() && spec.owner.device() == entry.device);
        if (configured) continue;
        output.refusal = store_refusal::unconfigured;
        output.device = entry.device;
        return output;
    }
    return output;
}

} // namespace kwaque::storage
