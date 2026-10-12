#pragma once

#include "src/base/units.h"
#include "src/storage/local_metadata.h"
#include "src/storage/local_paths.h"

#include <concepts>
#include <cstdint>
#include <optional>
#include <span>

namespace kwaque::storage {
// As many as the runtime has I/O queues: a device beyond them would share an
// unlimited one, where no scheduling share holds.
inline constexpr std::size_t maximum_local_devices = 8;
// Configuration admission, not a change to the u32 wire representation.
inline constexpr std::uint32_t maximum_local_shards = 65536;
struct local_directory_identity final {
    std::uint64_t device, inode;
    bool operator==(const local_directory_identity&) const = default;
};
struct local_device_spec final {
    runtime::file_path root;
    local_store_context owner;
    local_store_identity identity;
    // Supplied by the host owner, never read from store.meta or generic stat.
    local_directory_identity directory;
    bool allow_pid_file{true};
    std::optional<runtime::file_name> mount_marker;
    [[nodiscard]] bool controls() const noexcept {
        return identity.role != local_device_role::data;
    }
    [[nodiscard]] bool stores_data() const noexcept {
        return identity.role != local_device_role::wal_control;
    }
    [[nodiscard]] result<local_store_context>
    shard_owner(std::uint32_t shard) const noexcept {
        if (shard >= identity.shard_count)
            return failure(errc::invalid_argument);
        return local_store_context::make(
          owner.cluster(), owner.broker(), owner.device(), shard);
    }
    bool operator==(const local_device_spec&) const = default;
};
[[nodiscard]] runtime::result<void>
validate_local_device_spec(const local_device_spec&);
[[nodiscard]] runtime::result<void>
  validate_local_device_set(std::span<const local_device_spec>);

// The external owner holds every directory lock through all storage work and
// validates the independently expected directory/device/alias/mount binding.
// The fake backend's provider checks supplied ownership inputs, not host flock.
template<typename Owner>
concept local_directory_owner = requires(
  Owner& owner, const local_device_spec& spec) {
    {
        owner.validate(spec)
    } -> std::same_as<seastar::future<runtime::result<void>>>;
};
// Creation is for a newly assigned device context or its known interrupted
// bootstrap. It is never a way to recover erased metadata from an existing
// device; that independently expected device uses must_exist.
//
// add_devices is for a store that exists and gains data devices: every
// device that holds a store must be that store's, and a device named as
// added that holds nothing, or only the interrupted bootstrap of itself, is
// initialized. A device that is not named must hold the store already: no
// record lists the devices a store had, so one it had and lost looks like a
// new one, and is told apart only by not being named. It never makes a first
// store, and it never initializes a control device: a store that was used is
// extended only by this intent.
enum class local_store_intent : std::uint8_t {
    create_or_resume,
    must_exist,
    add_devices
};
struct local_store_open_options final {
    local_store_intent intent{local_store_intent::must_exist};
    // Explicitly false only for a store known not to have activated its WAL.
    bool require_wal_head{true};
    // With add_devices: the data devices this start adds, by identity,
    // borrowed for the call. Left set after they were added it changes
    // nothing.
    std::span<const device_store_id> added{};
};
struct local_store_io_limits final {
    bytes::allocation_charge_fn charge{nullptr};
    byte_count operation_bytes{256_KiB};
    byte_count metadata_bytes{64_KiB};
    // Excluded from codec budgets; caller-qualified native/control/frame cost.
    byte_count execution_bytes{64_KiB};
    [[nodiscard]] runtime::result<void> validate() const noexcept;
};
enum class local_store_state : std::uint8_t {
    pristine,
    incomplete,
    existing,
    unsupported,
    corrupt
};
struct local_store_report final {
    local_store_state state{local_store_state::pristine};
    bool identity_present{false};
    bool bootstrap_compatible{false};
    bool has_payload{false};
    bool all_heads_present{true};
    std::uint32_t controls{0};
    // Segment directories holding a creation prefix but no publication: no
    // append can have reached them. Retained; never a store-wide gap.
    std::uint32_t interrupted_creations{0};
    // Segment directories under their deleting name: removals a crash or a
    // stop left. What is inside one may be in any state and is not looked
    // at; never a store-wide gap.
    std::uint32_t deleting_segments{0};
    std::optional<runtime::operation_error> reason;
};
} // namespace kwaque::storage
