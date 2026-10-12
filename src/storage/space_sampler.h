#pragma once

#include "src/base/error.h"
#include "src/base/units.h"
#include "src/runtime/error.h"
#include "src/runtime/file.h"
#include "src/runtime/first_failure.h"
#include "src/runtime/shard_affinity.h"
#include "src/runtime/time.h"
#include "src/runtime/timer.h"
#include "src/storage/device_placement.h"
#include "src/storage/local_paths.h"
#include "src/storage/local_store_config.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/util/noncopyable_function.hh>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <utility>
#include <vector>

namespace kwaque::storage {

struct space_sampler_config final {
    // How often each filesystem is read.
    std::uint64_t period_nanoseconds{1'000'000'000};
    // What is never promised to a segment: this part of the filesystem, and
    // at least this much. It is for what no segment's bound counts: index
    // bundles, checkpoints, decisions, pointers and temporaries. Seals,
    // checkpoints and removals draw on it when nothing else is left.
    std::uint32_t floor_percent{1};
    byte_count floor_minimum{1_GiB};
    [[nodiscard]] runtime::result<void> validate() const noexcept {
        if (period_nanoseconds == 0 || floor_percent > 100)
            return runtime::failure(detail::path_error(errc::invalid_argument));
        return {};
    }
};

// What one filesystem may still be promised, and one shard's part of it.
struct space_division final {
    byte_count floor, headroom, share;
    bool operator==(const space_division&) const = default;
};
// `available` is what an unprivileged process may use, and `owed` what the
// segments and logs that exist may still come to write. What is left after
// those and the floor is divided evenly: a shard that promises only inside
// its part can never promise what another already did.
[[nodiscard]] constexpr space_division divide_space(
  byte_count capacity,
  byte_count available,
  byte_count owed,
  std::uint32_t shards,
  const space_sampler_config& config) noexcept {
    space_division divided;
    divided.floor = std::max(
      byte_count{capacity.value() / 100 * config.floor_percent},
      config.floor_minimum);
    const auto held = divided.floor.checked_add(owed);
    if (held && *held < available && shards != 0) {
        divided.headroom = byte_count{available.value() - held->value()};
        divided.share = byte_count{divided.headroom.value() / shards};
    }
    return divided;
}

// The one sampler of a process, on one shard. Each period it reads every
// filesystem that holds data, asks each shard what it still owes there,
// and hands each shard its share of what is left. It is the only thing
// about space that crosses shards, and no append and no creation waits
// for it.
//
// A filesystem that could not be read, and every filesystem when a shard
// could not be asked, is handed as not sampled: the shards then promise
// nothing there, and the last good number is not used. A failed sample
// never ends the sampling.
class space_sampler final : public runtime::shard_affine {
public:
    static constexpr std::size_t maximum_pools = maximum_local_devices;
    using owed_bytes = std::array<byte_count, maximum_pools>;
    using pool_shares = std::array<device_pool_share, maximum_pools>;

    struct providers final {
        // One directory on each filesystem, in the device tables' pool
        // order; borrowed until stop().
        std::span<const runtime::file_path> pools;
        std::uint32_t shards{0};
        // Reads a filesystem: the runtime's sample.
        seastar::noncopyable_function<
          seastar::future<runtime::result<runtime::file_system_space>>(
            const runtime::file_path&)>
          space;
        // Asks a shard what it owes on each pool. From its answer until its
        // shares arrive that shard promises nothing new.
        seastar::noncopyable_function<
          seastar::future<runtime::result<owed_bytes>>(std::uint32_t)>
          owed;
        // Hands a shard its shares, a pool each.
        seastar::noncopyable_function<seastar::future<>(
          std::uint32_t, pool_shares)>
          hand;
    };
    // What the last sample found of one pool.
    struct view final {
        bool sampled{false};
        bool read_only{false};
        byte_count capacity, available, owed;
        space_division divided;
    };

    [[nodiscard]] static runtime::result<std::unique_ptr<space_sampler>>
    make(providers given, space_sampler_config config) {
        if (auto valid = config.validate(); !valid)
            return runtime::failure(valid.error());
        if (
          given.pools.empty() || given.pools.size() > maximum_pools
          || given.shards == 0 || given.shards > maximum_local_shards
          || !given.space || !given.owed || !given.hand)
            return runtime::failure(detail::path_error(errc::invalid_argument));
        try {
            auto made = std::unique_ptr<space_sampler>{
              new space_sampler(std::move(given), config)};
            made->asked_.reserve(made->given_.shards);
            made->handed_.reserve(made->given_.shards);
            return made;
        } catch (const std::bad_alloc&) {
            return runtime::failure(
              detail::path_error(errc::resource_exhausted));
        }
    }
    space_sampler(const space_sampler&) = delete;
    space_sampler& operator=(const space_sampler&) = delete;

    [[nodiscard]] view last(std::size_t pool) const noexcept {
        assert_current();
        return pool < last_.size() ? last_[pool] : view{};
    }
    [[nodiscard]] std::uint64_t samples() const noexcept {
        assert_current();
        return samples_;
    }
    // Why the sampling ended, when it was not stop() that ended it.
    [[nodiscard]] runtime::first_failure failure() const {
        assert_current();
        return ended_;
    }

    // One sample: every filesystem read, every shard asked, every shard
    // handed its shares. One at a time.
    [[nodiscard]] seastar::future<> sample() {
        assert_current();
        if (sampling_) co_return;
        sampling_ = true;
        std::array<view, maximum_pools> seen{};
        for (std::size_t pool = 0; pool != given_.pools.size(); ++pool) {
            try {
                const auto read = co_await given_.space(given_.pools[pool]);
                if (!read) continue;
                seen[pool].sampled = true;
                seen[pool].read_only = read->read_only();
                seen[pool].capacity = read->capacity();
                seen[pool].available = read->available();
            } catch (...) {
            }
        }
        // Every shard is asked at once; the answers are added up.
        bool answered = true;
        asked_.clear();
        for (std::uint32_t shard = 0; shard != given_.shards; ++shard) {
            try {
                asked_.push_back(given_.owed(shard));
            } catch (...) {
                answered = false;
            }
        }
        for (auto& asked : asked_) {
            try {
                const auto owed = co_await std::move(asked);
                if (!owed) {
                    answered = false;
                    continue;
                }
                for (std::size_t pool = 0; pool != given_.pools.size();
                     ++pool) {
                    const auto more = seen[pool].owed.checked_add(
                      (*owed)[pool]);
                    if (more)
                        seen[pool].owed = *more;
                    else
                        answered = false;
                }
            } catch (...) {
                answered = false;
            }
        }
        asked_.clear();
        pool_shares shares{};
        for (std::size_t pool = 0; pool != given_.pools.size(); ++pool) {
            auto& found = seen[pool];
            // What a shard owes is not known: nothing is promised anywhere.
            found.sampled = found.sampled && answered;
            if (!found.sampled) continue;
            found.divided = divide_space(
              found.capacity,
              found.available,
              found.owed,
              given_.shards,
              config_);
            shares[pool] = {true, found.read_only, found.divided.share};
        }
        handed_.clear();
        for (std::uint32_t shard = 0; shard != given_.shards; ++shard) {
            try {
                handed_.push_back(given_.hand(shard, shares));
            } catch (...) {
            }
        }
        for (auto& handed : handed_) {
            try {
                co_await std::move(handed);
            } catch (...) {
            }
        }
        handed_.clear();
        last_ = seen;
        ++samples_;
        sampling_ = false;
    }

    // Samples every period from here on, on `timer`. A period that a
    // sample overran is not caught up.
    template<runtime::monotonic_clock Clock, runtime::timer_service Timer>
    [[nodiscard]] runtime::result<void> start(Timer& timer) {
        assert_current();
        if (running_ || stopping_)
            return runtime::failure(detail::path_error(errc::closed));
        timer_ = &timer;
        sleep_ = sleep_function<Timer>;
        now_ = []() noexcept { return Clock::now(); };
        try {
            running_.emplace(run());
        } catch (...) {
            return runtime::failure(
              detail::path_error(errc::resource_exhausted));
        }
        return {};
    }
    // Ends the sampling and joins it. The shards' shares then go stale.
    [[nodiscard]] seastar::future<> stop() {
        assert_current();
        stopping_ = true;
        abort_.request_abort();
        if (running_) {
            auto joined = std::move(*running_);
            running_.reset();
            co_await std::move(joined);
        }
    }

private:
    using sleep_fn = seastar::future<runtime::result<void>> (*)(
      void*, runtime::monotonic_time, seastar::abort_source&);
    template<runtime::timer_service Timer>
    static seastar::future<runtime::result<void>> sleep_function(
      void* timer,
      runtime::monotonic_time deadline,
      seastar::abort_source& abort) {
        return static_cast<Timer*>(timer)->sleep_until(deadline, abort);
    }

    space_sampler(providers given, space_sampler_config config) noexcept
      : given_(std::move(given))
      , config_(config) {}

    seastar::future<> run() {
        while (!stopping_) {
            const auto began = now_();
            co_await sample();
            if (stopping_) break;
            const auto next = began.checked_add(
              runtime::monotonic_duration{config_.period_nanoseconds});
            runtime::result<void> slept = runtime::failure(
              detail::path_error(errc::out_of_range));
            try {
                if (next) slept = co_await sleep_(timer_, *next, abort_);
            } catch (...) {
                slept = runtime::failure(detail::path_error(errc::io_failure));
            }
            // Its timer stopped: no sample follows, the shards' shares go
            // stale, and why is kept.
            if (!slept && !stopping_) {
                ended_.observe(slept.error());
                break;
            }
        }
    }

    providers given_;
    space_sampler_config config_;
    std::array<view, maximum_pools> last_{};
    std::vector<seastar::future<runtime::result<owed_bytes>>> asked_;
    std::vector<seastar::future<>> handed_;
    std::uint64_t samples_{0};
    void* timer_{nullptr};
    sleep_fn sleep_{nullptr};
    runtime::monotonic_time (*now_)() noexcept {nullptr};
    seastar::abort_source abort_;
    std::optional<seastar::future<>> running_;
    runtime::first_failure ended_;
    bool sampling_{false}, stopping_{false};
};

} // namespace kwaque::storage
