#ifndef KWAQUE_SRC_RUNTIME_STALL_WATCH_H_
#define KWAQUE_SRC_RUNTIME_STALL_WATCH_H_

#include "src/runtime/file_flight.h"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>

namespace kwaque::runtime {

// A file operation that has been in flight for too long, as a thread that
// is no shard's sees it.
struct stalled_operation final {
    std::uint32_t shard{0};
    std::uint32_t device{0};
    std::uint64_t age_nanoseconds{0};
    bool operator==(const stalled_operation&) const = default;
};

// One stamp for each shard and device: the start of the oldest file
// operation in flight there. A shard writes its own stamps, one store each
// time its oldest operation changes, and any thread may read them. They are
// made before the first file is opened and outlive the last one closed.
//
// Each shard's stamps are one cache line, so a shard's stores never share a
// line with another's.
class stall_stamps final {
public:
    static constexpr std::size_t devices = file_flights::maximum_devices;

    // A native allocation failure throws.
    explicit stall_stamps(std::uint32_t shards)
      : rows_(std::make_unique<row[]>(shards))
      , shards_(shards) {
        for (std::uint32_t shard = 0; shard != shards_; ++shard)
            for (auto& start : rows_[shard].starts)
                start.store(
                  no_file_operation_in_flight, std::memory_order_relaxed);
    }
    stall_stamps(const stall_stamps&) = delete;
    stall_stamps& operator=(const stall_stamps&) = delete;

    [[nodiscard]] std::uint32_t shards() const noexcept { return shards_; }
    // The stamp of one shard's device; none outside the table.
    [[nodiscard]] std::atomic<std::uint64_t>*
    at(std::uint32_t shard, std::size_t device) noexcept {
        return shard < shards_ && device < devices
                 ? &rows_[shard].starts[device]
                 : nullptr;
    }
    // The operation that has been in flight for longest, when that is
    // longer than `threshold`. `now` is on the clock the stamps were
    // written from. It reads each stamp once and waits for nothing.
    [[nodiscard]] std::optional<stalled_operation> oldest_past(
      std::uint64_t now_nanoseconds,
      std::uint64_t threshold_nanoseconds) const noexcept {
        std::optional<stalled_operation> found;
        for (std::uint32_t shard = 0; shard != shards_; ++shard)
            for (std::size_t device = 0; device != devices; ++device) {
                const auto start = rows_[shard].starts[device].load(
                  std::memory_order_relaxed);
                if (
                  start == no_file_operation_in_flight
                  || start >= now_nanoseconds)
                    continue;
                const auto age = now_nanoseconds - start;
                if (
                  age > threshold_nanoseconds
                  && (!found || age > found->age_nanoseconds))
                    found = stalled_operation{
                      shard, static_cast<std::uint32_t>(device), age};
            }
        return found;
    }

private:
    struct alignas(64) row final {
        std::array<std::atomic<std::uint64_t>, devices> starts;
    };
    std::unique_ptr<row[]> rows_;
    std::uint32_t shards_;
};

} // namespace kwaque::runtime

#endif // KWAQUE_SRC_RUNTIME_STALL_WATCH_H_
