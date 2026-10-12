#ifndef KWAQUE_SRC_RUNTIME_FILE_FLIGHT_H_
#define KWAQUE_SRC_RUNTIME_FILE_FLIGHT_H_

#include "src/runtime/error.h"
#include "src/runtime/operation_statistics.h"
#include "src/runtime/time.h"

#include <seastar/core/shared_ptr.hh>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

namespace kwaque::runtime {

// What a stamp holds while no file operation is in flight.
inline constexpr std::uint64_t no_file_operation_in_flight
  = std::numeric_limits<std::uint64_t>::max();

// How one device's operations in flight are timed and shown. `now` is read
// once for each admitted operation. `stamp`, when given, is written with the
// start of the oldest operation in flight each time that operation changes,
// and with no_file_operation_in_flight when none is left. It is the one
// value another thread may read, so that a stall can be judged while the
// shard that owns the operation does not run. It outlives the file system.
struct file_flight_watch final {
    monotonic_time (*now)() noexcept {nullptr};
    std::atomic<std::uint64_t>* stamp{nullptr};
};

// The file operations in flight on one device of one shard, in the order
// they started. An operation links itself when it is admitted and unlinks
// when it ends, whatever its outcome: no allocation, no task and no timer.
// The oldest one's start is a read, so a device that stopped answering is
// seen by how long its oldest operation has been in flight.
class file_flight final {
public:
    // One operation's place in the order; empty when its device keeps none.
    class entry final {
    public:
        explicit entry(file_flight* flight) noexcept
          : flight_(flight) {
            if (flight_ == nullptr) return;
            start_ = flight_->watch_.now().nanoseconds();
            earlier_ = flight_->newest_;
            (earlier_ ? earlier_->later_ : flight_->oldest_) = this;
            flight_->newest_ = this;
            if (flight_->oldest_ == this) flight_->publish();
        }
        entry(const entry&) = delete;
        entry& operator=(const entry&) = delete;
        // The token moves with its operation; its place does not change.
        entry(entry&& other) noexcept
          : flight_(std::exchange(other.flight_, nullptr))
          , earlier_(std::exchange(other.earlier_, nullptr))
          , later_(std::exchange(other.later_, nullptr))
          , start_(other.start_) {
            if (flight_ == nullptr) return;
            (earlier_ ? earlier_->later_ : flight_->oldest_) = this;
            (later_ ? later_->earlier_ : flight_->newest_) = this;
        }
        entry& operator=(entry&&) = delete;
        ~entry() {
            if (flight_ == nullptr) return;
            const bool first = flight_->oldest_ == this;
            (earlier_ ? earlier_->later_ : flight_->oldest_) = later_;
            (later_ ? later_->earlier_ : flight_->newest_) = earlier_;
            if (first) flight_->publish();
        }

    private:
        friend class file_flight;
        file_flight* flight_;
        entry* earlier_{nullptr};
        entry* later_{nullptr};
        std::uint64_t start_{0};
    };

    explicit file_flight(file_flight_watch watch) noexcept
      : watch_(watch) {
        publish();
    }
    file_flight(const file_flight&) = delete;
    file_flight& operator=(const file_flight&) = delete;
    ~file_flight() {
        if (watch_.stamp != nullptr)
            watch_.stamp->store(
              no_file_operation_in_flight, std::memory_order_relaxed);
    }

    // When the oldest operation in flight started.
    [[nodiscard]] std::optional<monotonic_time> oldest() const noexcept {
        return oldest_ != nullptr
                 ? std::optional{monotonic_time{oldest_->start_}}
                 : std::nullopt;
    }

private:
    // One store, and only when the oldest operation changed.
    void publish() noexcept {
        if (watch_.stamp == nullptr) return;
        watch_.stamp->store(
          oldest_ != nullptr ? oldest_->start_ : no_file_operation_in_flight,
          std::memory_order_relaxed);
    }

    file_flight_watch watch_;
    entry* oldest_{nullptr};
    entry* newest_{nullptr};
};

// Shared by a file system and every file it opened on the device, so that an
// admitted operation cannot outlive the order it is in.
using file_flight_ptr = seastar::lw_shared_ptr<file_flight>;

// One admitted file operation: its terminal statistics token and, on a
// device whose operations are kept in order, its place in that order.
class file_operation final {
public:
    file_operation(
      operation_statistics& statistics, file_flight* flight) noexcept
      : metric_(statistics.accept())
      , flight_(flight) {}
    file_operation(file_operation&&) noexcept = default;
    file_operation& operator=(file_operation&&) = delete;

    void add_completed_bytes(std::uint64_t bytes) noexcept {
        metric_.add_completed_bytes(bytes);
    }

private:
    operation_statistics::reservation metric_;
    file_flight::entry flight_;
};

// One shard's file system keeps the operations of each watched device
// apart, by the root its files are under. Roots do not nest.
class file_flights final {
public:
    static constexpr std::size_t maximum_devices = 8;

    // Operations under `root` are kept in start order from here on. Files
    // already open are not: watch a device before anything on it is opened.
    // A native allocation failure throws.
    [[nodiscard]] result<void>
    watch(std::string_view root, file_flight_watch watching) {
        if (watching.now == nullptr || root.empty() || root.back() == '/')
            return failure(
              operation_error{errc::invalid_argument, operation_kind::file});
        for (std::size_t i = 0; i != count_; ++i)
            if (
              covers(devices_[i].root, root) || covers(root, devices_[i].root))
                return failure(
                  operation_error{errc::already_exists, operation_kind::file});
        if (count_ == maximum_devices)
            return failure(
              operation_error{errc::resource_exhausted, operation_kind::file});
        devices_[count_].root = std::string{root};
        devices_[count_].flight = seastar::make_lw_shared<file_flight>(
          watching);
        ++count_;
        return {};
    }
    // The order a path's operations are kept in; none for a path under no
    // watched root.
    [[nodiscard]] const file_flight_ptr&
    under(std::string_view path) const noexcept {
        for (std::size_t i = 0; i != count_; ++i)
            if (covers(devices_[i].root, path)) return devices_[i].flight;
        return none_;
    }
    // When the oldest operation in flight under a watched root started.
    [[nodiscard]] std::optional<monotonic_time>
    oldest(std::string_view root) const noexcept {
        const auto& flight = under(root);
        return flight ? flight->oldest() : std::nullopt;
    }

private:
    [[nodiscard]] static bool
    covers(std::string_view root, std::string_view path) noexcept {
        return path.starts_with(root)
               && (path.size() == root.size() || path[root.size()] == '/');
    }
    struct device final {
        std::string root;
        file_flight_ptr flight;
    };
    std::array<device, maximum_devices> devices_;
    std::size_t count_{0};
    file_flight_ptr none_;
};

} // namespace kwaque::runtime

#endif // KWAQUE_SRC_RUNTIME_FILE_FLIGHT_H_
