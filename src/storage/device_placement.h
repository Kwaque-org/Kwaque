#pragma once

#include "src/base/error.h"
#include "src/base/units.h"
#include "src/runtime/error.h"
#include "src/runtime/file_error.h"
#include "src/runtime/shard_affinity.h"
#include "src/runtime/time.h"
#include "src/storage/local_paths.h"
#include "src/storage/local_store_config.h"
#include "src/storage/local_types.h"

#include <seastar/util/noncopyable_function.hh>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <utility>

namespace kwaque::storage {

// What a shard holds a device to be. The worst that holds is the state.
enum class device_state : std::uint8_t {
    healthy,
    // Its filesystem has no room left to promise: no new segment.
    low,
    // No sample of its filesystem that is recent enough: no share is
    // trusted, and no new segment.
    stale,
    // A write on it was refused for space: no new segment until a sample
    // shows room again.
    full,
    // Its oldest operation has been in flight for longer than a device
    // answers in: no new segment until that operation ends.
    stalled,
    // It failed. Nothing returns it in this process.
    failed,
};
// Why a device failed.
enum class device_failure : std::uint8_t { none, io_error, read_only, gone };

// What a storage failure does to its device. Space that ran out comes back;
// an I/O error, a filesystem turned read-only and a device that is gone do
// not. Everything else is no fault of the device.
struct device_verdict final {
    device_state state{device_state::healthy};
    device_failure cause{device_failure::none};
};
[[nodiscard]] constexpr device_verdict
device_verdict_of(runtime::file_failure_detail detail) noexcept {
    switch (detail) {
    case runtime::file_failure_detail::no_space:
    case runtime::file_failure_detail::quota:
        return {device_state::full, device_failure::none};
    case runtime::file_failure_detail::read_only:
        return {device_state::failed, device_failure::read_only};
    case runtime::file_failure_detail::device_io:
        return {device_state::failed, device_failure::io_error};
    case runtime::file_failure_detail::device_gone:
        return {device_state::failed, device_failure::gone};
    case runtime::file_failure_detail::unknown:
    case runtime::file_failure_detail::descriptor_limit:
    case runtime::file_failure_detail::permission:
    case runtime::file_failure_detail::admission_not_dispatched:
        return {};
    }
    return {};
}

struct device_placement_config final {
    // An operation in flight for longer than this makes its device stalled.
    std::uint64_t stalled_nanoseconds{5'000'000'000};
    // A share older than this is trusted no more.
    std::uint64_t stale_nanoseconds{3'000'000'000};
    [[nodiscard]] runtime::result<void> validate() const noexcept {
        if (stalled_nanoseconds == 0 || stale_nanoseconds == 0)
            return runtime::failure(detail::path_error(errc::invalid_argument));
        return {};
    }
};

// One filesystem's sample as a shard is handed it: whether it could be
// read, and the bytes this shard may promise to new segments on it.
struct device_pool_share final {
    bool sampled{false};
    bool read_only{false};
    byte_count share;
};
// A device whose state changed, for whoever acts on it: its segments' owner
// is told, and the segments of a failed device are failed.
struct device_report final {
    device_store_id device;
    device_state state{device_state::healthy};
    device_failure cause{device_failure::none};
    // How long its oldest operation has been in flight, when it is stalled.
    std::uint64_t age_nanoseconds{0};
};
// Told on the shard, after the table is consistent. It must not throw.
using device_sink
  = seastar::noncopyable_function<void(const device_report&) noexcept>;

// One shard's view of the store's data devices: what each one's state is,
// how many of the shard's writable segments are on it, and how much of its
// filesystem the shard may still promise. It chooses the device of a new
// segment and keeps the shard inside its share.
//
// Devices on one filesystem are one pool: one share, taken from by each.
// The shares come from the one sampler of the process; nothing here
// crosses shards. Every time is the caller's monotonic clock.
class device_table final : public runtime::shard_affine {
public:
    static constexpr std::size_t maximum_devices = maximum_local_devices;

    [[nodiscard]] static runtime::result<std::unique_ptr<device_table>> make(
      std::span<const local_device_spec> devices,
      device_placement_config config,
      device_sink sink = {}) {
        if (auto valid = config.validate(); !valid)
            return runtime::failure(valid.error());
        if (auto valid = validate_local_device_set(devices); !valid)
            return runtime::failure(valid.error());
        std::unique_ptr<device_table> made;
        try {
            made.reset(new device_table(config, std::move(sink)));
        } catch (const std::bad_alloc&) {
            return runtime::failure(
              detail::path_error(errc::resource_exhausted));
        }
        for (const auto& spec : devices) {
            if (!spec.stores_data()) continue;
            auto& placed = made->devices_[made->count_++];
            placed.id = spec.owner.device();
            placed.filesystem = spec.directory.device;
        }
        if (made->count_ == 0)
            return runtime::failure(detail::path_error(errc::invalid_argument));
        // In the order of their identities, which decides a tie.
        std::sort(
          made->devices_.begin(),
          made->devices_.begin() + static_cast<std::ptrdiff_t>(made->count_),
          [](const entry& a, const entry& b) {
              return a.id.canonical_less(b.id);
          });
        for (std::size_t i = 0; i != made->count_; ++i) {
            auto& placed = made->devices_[i];
            placed.pool = made->pools_;
            for (std::size_t j = 0; j != i; ++j)
                if (made->devices_[j].filesystem == placed.filesystem)
                    placed.pool = made->devices_[j].pool;
            if (placed.pool == made->pools_) ++made->pools_;
        }
        return made;
    }
    device_table(const device_table&) = delete;
    device_table& operator=(const device_table&) = delete;

    [[nodiscard]] std::size_t size() const noexcept { return count_; }
    [[nodiscard]] std::size_t pools() const noexcept { return pools_; }
    // The pool a device's share is taken from: one for each filesystem, in
    // the order of the lowest identity on it.
    [[nodiscard]] std::optional<std::size_t>
    pool(device_store_id device) const noexcept {
        const auto* found = find(device);
        return found ? std::optional{found->pool} : std::nullopt;
    }
    [[nodiscard]] std::optional<device_store_id>
    device(std::size_t index) const noexcept {
        return index < count_ ? std::optional{devices_[index].id}
                              : std::nullopt;
    }
    [[nodiscard]] std::optional<device_state>
    state(device_store_id device, runtime::monotonic_time now) const noexcept {
        const auto* found = find(device);
        return found ? std::optional{state_of(*found, now)} : std::nullopt;
    }
    [[nodiscard]] device_failure cause(device_store_id device) const noexcept {
        const auto* found = find(device);
        return found ? found->cause : device_failure::none;
    }
    // The shard's writable segments on a device, and what it may still
    // promise on the device's filesystem.
    [[nodiscard]] std::uint32_t
    writable(device_store_id device) const noexcept {
        const auto* found = find(device);
        return found ? found->writable : 0;
    }
    [[nodiscard]] byte_count share(device_store_id device) const noexcept {
        const auto* found = find(device);
        return found ? shares_[found->pool].share : byte_count{};
    }

    // The device of a new segment whose whole obligation is `bound`: among
    // the devices that are healthy and whose share covers it, the one with
    // the fewest of this shard's writable segments, then the larger share,
    // then the lower identity. The same table chooses the same device.
    // `named` is taken when it is eligible and refused when it is not.
    // With no eligible device the refusal says why the best one was not:
    // resource_exhausted for space, unavailable otherwise.
    [[nodiscard]] runtime::result<device_store_id> choose(
      byte_count bound,
      runtime::monotonic_time now,
      std::optional<device_store_id> named = std::nullopt) noexcept {
        assert_current();
        if (named && !find(*named))
            return runtime::failure(detail::path_error(errc::wrong_context));
        entry* chosen = nullptr;
        bool space = false;
        for (std::size_t i = 0; i != count_; ++i) {
            auto& candidate = devices_[i];
            if (named && candidate.id != *named) continue;
            const auto held = state_of(candidate, now);
            if (held == device_state::full) space = true;
            if (held != device_state::healthy && held != device_state::low)
                continue;
            auto& offered = shares_[candidate.pool];
            if (held == device_state::low || offered.share < bound) {
                // What a pool was asked for is what brings it back.
                offered.wanted = std::max(offered.wanted, bound);
                space = true;
                continue;
            }
            // In identity order, so the first of equals stays chosen.
            if (
              !chosen || candidate.writable < chosen->writable
              || (candidate.writable == chosen->writable
                  && offered.share > shares_[chosen->pool].share))
                chosen = &candidate;
        }
        if (chosen) return chosen->id;
        return runtime::failure(
          detail::path_error(
            space ? errc::resource_exhausted : errc::unavailable));
    }
    // A segment was placed: it counts on its device, and its bound is taken
    // from the device's share at once.
    void placed(device_store_id device, byte_count bound) noexcept {
        assert_current();
        auto* found = find(device);
        if (!found) return;
        ++found->writable;
        auto& share = shares_[found->pool].share;
        share = byte_count{
          share.value() > bound.value() ? share.value() - bound.value() : 0};
    }
    // A segment is written no more: sealed, failed or closed. `unused` is
    // what a creation that made nothing gives back.
    void unplaced(device_store_id device, byte_count unused = {}) noexcept {
        assert_current();
        auto* found = find(device);
        if (!found || found->writable == 0) return;
        --found->writable;
        auto& share = shares_[found->pool].share;
        if (const auto back = share.checked_add(unused)) share = *back;
    }

    // The sampler asks what the shard owes: from here until its shares
    // arrive the shard promises nothing new, so that nothing is promised
    // twice.
    void await_shares() noexcept {
        assert_current();
        for (std::size_t pool = 0; pool != pools_; ++pool)
            shares_[pool].share = {};
    }
    // The shares of one sample, a pool each. A pool that could not be
    // sampled has no share and is stale; one that is read-only failed. A
    // pool with no room is low, and returns when what was last asked of it
    // fits again. A device a write found full returns with any room.
    void sampled(
      std::span<const device_pool_share> shares,
      runtime::monotonic_time now) noexcept {
        assert_current();
        for (std::size_t pool = 0; pool != pools_; ++pool) {
            auto& held = shares_[pool];
            const auto given = pool < shares.size() ? shares[pool]
                                                    : device_pool_share{};
            held.share = given.sampled ? given.share : byte_count{};
            // The last good sample is not used in place of a failed one.
            held.sampled = given.sampled ? std::optional{now} : std::nullopt;
            const bool room = given.sampled && held.share.value() != 0
                              && held.share >= held.wanted;
            if (room) held.wanted = {};
            for (std::size_t i = 0; i != count_; ++i) {
                auto& entry = devices_[i];
                if (entry.pool != pool) continue;
                if (given.sampled && given.read_only)
                    change(
                      entry, device_state::failed, device_failure::read_only);
                else if (given.sampled)
                    set(entry.low, !room, entry, device_state::low);
                if (room && entry.full) set(entry.full, false, entry, {});
            }
        }
        flush();
    }
    // A storage failure on a device, by the detail its file failure
    // carried: see device_verdict_of.
    void failed(
      device_store_id device, runtime::file_failure_detail detail) noexcept {
        assert_current();
        auto* found = find(device);
        if (!found) return;
        const auto verdict = device_verdict_of(detail);
        if (verdict.state == device_state::failed)
            change(*found, device_state::failed, verdict.cause);
        else if (verdict.state == device_state::full)
            set(found->full, true, *found, device_state::full);
        flush();
    }
    // When a device's oldest operation in flight started, as its file
    // system says; none when nothing is in flight. Older than the first
    // threshold the device is stalled, and it returns when that operation
    // has ended.
    void aged(
      device_store_id device,
      std::optional<runtime::monotonic_time> oldest,
      runtime::monotonic_time now) noexcept {
        assert_current();
        auto* found = find(device);
        if (!found) return;
        const auto age = oldest ? now.checked_elapsed_since(*oldest)
                                : std::nullopt;
        const bool stalled = age
                             && age->nanoseconds()
                                  > config_.stalled_nanoseconds;
        found->age = stalled ? age->nanoseconds() : 0;
        set(found->stalled, stalled, *found, device_state::stalled);
        flush();
    }

private:
    struct entry final {
        device_store_id id;
        std::uint64_t filesystem{0};
        std::size_t pool{0};
        std::uint32_t writable{0};
        device_failure cause{device_failure::none};
        std::uint64_t age{0};
        bool low{false}, full{false}, stalled{false}, failed{false};
    };
    struct pool_state final {
        byte_count share;
        // The largest bound a request was refused for while there was no
        // room: what must fit before the pool is no longer low.
        byte_count wanted;
        std::optional<runtime::monotonic_time> sampled;
    };

    device_table(device_placement_config config, device_sink sink) noexcept
      : config_(config)
      , sink_(std::move(sink)) {}

    [[nodiscard]] entry* find(device_store_id device) noexcept {
        for (std::size_t i = 0; i != count_; ++i)
            if (devices_[i].id == device) return &devices_[i];
        return nullptr;
    }
    [[nodiscard]] const entry* find(device_store_id device) const noexcept {
        for (std::size_t i = 0; i != count_; ++i)
            if (devices_[i].id == device) return &devices_[i];
        return nullptr;
    }
    [[nodiscard]] device_state
    state_of(const entry& of, runtime::monotonic_time now) const noexcept {
        if (of.failed) return device_state::failed;
        if (of.stalled) return device_state::stalled;
        if (of.full) return device_state::full;
        const auto& held = shares_[of.pool];
        const auto age = held.sampled ? now.checked_elapsed_since(*held.sampled)
                                      : std::nullopt;
        if (!age || age->nanoseconds() > config_.stale_nanoseconds)
            return device_state::stale;
        return of.low ? device_state::low : device_state::healthy;
    }
    // One flag of a device changed: whoever acts on it is told when it was
    // raised, with what the flag means.
    void
    set(bool& flag, bool raised, entry& of, device_state meaning) noexcept {
        if (flag == raised) return;
        flag = raised;
        if (raised) tell(of, meaning);
    }
    void change(entry& of, device_state to, device_failure cause) noexcept {
        if (of.failed) return;
        of.failed = to == device_state::failed;
        of.cause = cause;
        tell(of, to);
    }
    // Kept until the table is consistent: see flush().
    void tell(const entry& of, device_state state) noexcept {
        if (sink_ && telling_ != told_.size())
            told_[telling_++] = device_report{of.id, state, of.cause, of.age};
    }
    // Tells what changed, once the change is whole: its receiver may read
    // the table and take segments off a device.
    void flush() noexcept {
        if (flushing_) return;
        flushing_ = true;
        for (std::size_t i = 0; i != telling_; ++i)
            sink_(told_[i]);
        telling_ = 0;
        flushing_ = false;
    }

    device_placement_config config_;
    device_sink sink_;
    std::array<entry, maximum_devices> devices_{};
    std::array<pool_state, maximum_devices> shares_{};
    std::size_t count_{0};
    std::size_t pools_{0};
    // A device changes at most three flags in one call.
    std::array<device_report, 3 * maximum_devices> told_{};
    std::size_t telling_{0};
    bool flushing_{false};
};

} // namespace kwaque::storage
