#pragma once

#include "src/base/invariant.h"
#include "src/runtime/shard_affinity.h"
#include "src/runtime/time.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace kwaque::runtime {

class deadline_wheel;

// A deadline's place in its shard's wheel, kept in the frame that waits: the
// wheel links and unlinks it but never allocates for it. When the deadline
// passes, the wheel unlinks the entry and calls `on_expiry` with it.
class wheel_entry {
public:
    using expiry = void (*)(wheel_entry&) noexcept;

    explicit wheel_entry(expiry on_expiry) noexcept
      : on_expiry_(on_expiry) {}

    wheel_entry(const wheel_entry&) = delete;
    wheel_entry& operator=(const wheel_entry&) = delete;
    wheel_entry(wheel_entry&&) = delete;
    wheel_entry& operator=(wheel_entry&&) = delete;

    [[nodiscard]] bool linked() const noexcept { return pprev_ != nullptr; }

    // Unlinks the entry from its wheel; false if it was not linked.
    bool cancel() noexcept;

protected:
    ~wheel_entry() {
        KWAQUE_INVARIANT(
          invariant_id{"KQ-WHEEL-ENTRY-LINKED"},
          !linked(),
          "a deadline was destroyed while armed");
    }

private:
    friend class deadline_wheel;

    wheel_entry* next_{nullptr};
    wheel_entry** pprev_{nullptr};
    deadline_wheel* wheel_{nullptr};
    expiry on_expiry_;
    // Its deadline rounded up to a tick. An exact entry ends no earlier; one
    // that may end early ends at its bucket, which can come first.
    std::uint64_t tick_{0};
    // The bucket or the collected chain it sits in.
    std::uint16_t place_{0};
    std::uint8_t flags_{0};
};

// One shard's deadlines: a non-cascading timer wheel of five levels of 64
// slots, each level eight times coarser (1 ms ticks: 1, 8, 64, 512 and
// 4,096 ms, about 262 s), every bucket rounded down to its first tick. An
// exact deadline whose bucket ends before it is placed again on a finer
// level, so it ends no earlier than its deadline and at most one tick after
// it, and moves at most once per level on the way; a deadline that
// may end early ends at its bucket. One beyond the top level is parked in the
// farthest top-level bucket and placed again from there.
//
// Work is proportional to the entries that end: a cancel is constant time,
// and expiry first moves every due bucket to a collected chain, then ends or
// re-places at most a budget of entries. Nothing allocates.
class deadline_wheel final : public shard_affine {
public:
    static constexpr std::uint64_t tick_nanoseconds{1'000'000};
    static constexpr unsigned level_count{5};
    static constexpr unsigned level_shift{3};
    static constexpr unsigned level_slots{64};
    static constexpr unsigned bucket_count{level_count * level_slots};

    enum class precision : std::uint8_t {
        // Ends at or after its deadline.
        exact,
        // A consume stream's maximum wait: ends at its bucket, up to an
        // eighth of the wait early while the driver keeps up (one that has
        // fallen behind places it against an older clock, so earlier), and
        // is never placed again.
        may_end_early,
    };

    enum class insertion : std::uint8_t {
        linked,
        // Its deadline has passed: nothing was linked.
        due,
    };

    explicit deadline_wheel(monotonic_time now) noexcept;
    ~deadline_wheel();

    // Links `entry`. Forwards the wheel's clock to `now` first, never past a
    // bucket that is still pending.
    [[nodiscard]] insertion insert(
      wheel_entry& entry,
      monotonic_time deadline,
      precision kind,
      monotonic_time now) noexcept;

    // Unlinks `entry` wherever it is; false if it was not linked.
    bool cancel(wheel_entry& entry) noexcept;

    // Moves every bucket due at `now` to a collected chain, then ends or
    // re-places collected entries until `budget` have been touched. Returns
    // how many were touched; collected entries left over stay for the next
    // call.
    std::size_t expire(monotonic_time now, std::size_t budget) noexcept;

    // When the next expiry call has work: now, while collected entries are
    // left; otherwise the first pending bucket's time; none when empty.
    [[nodiscard]] std::optional<monotonic_time>
    next_expiry(monotonic_time now) const noexcept;

    [[nodiscard]] std::size_t size() const noexcept { return size_; }
    [[nodiscard]] bool empty() const noexcept { return size_ == 0; }
    [[nodiscard]] bool has_collected() const noexcept;

    // Called when a cancel empties the wheel, so its driver can stop its
    // alarm.
    void on_emptied(void (*callback)(void*) noexcept, void* context) noexcept {
        emptied_ = callback;
        emptied_context_ = context;
    }

private:
    static constexpr std::uint8_t may_end_early_flag{1U << 0U};
    static constexpr std::uint8_t parked_flag{1U << 1U};

    [[nodiscard]] static std::uint64_t floor_tick(monotonic_time time) noexcept;
    [[nodiscard]] static std::uint64_t ceil_tick(monotonic_time time) noexcept;
    [[nodiscard]] std::optional<std::uint64_t>
    next_bucket_tick() const noexcept;
    [[nodiscard]] std::uint64_t
    bucket_tick(unsigned level, unsigned slot) const noexcept;
    void forward(std::uint64_t now_tick) noexcept;
    void place(wheel_entry& entry) noexcept;
    void push(wheel_entry*& head, wheel_entry& entry) noexcept;
    static void unlink(wheel_entry& entry) noexcept;
    void collect(std::uint64_t now_tick) noexcept;
    [[nodiscard]] wheel_entry* pop_collected() noexcept;

    std::array<wheel_entry*, bucket_count> buckets_{};
    std::array<std::uint64_t, level_count> pending_{};
    std::array<wheel_entry*, bucket_count> collected_{};
    std::array<std::uint64_t, level_count> collected_pending_{};
    // Every tick up to and including this one has been processed.
    std::uint64_t clock_;
    std::size_t size_{0};
    void (*emptied_)(void*) noexcept {nullptr};
    void* emptied_context_{nullptr};
};

inline bool wheel_entry::cancel() noexcept {
    return wheel_ != nullptr && wheel_->cancel(*this);
}

} // namespace kwaque::runtime
