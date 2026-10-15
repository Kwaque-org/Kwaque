#pragma once

#include <seastar/core/memory.hh>
#include <seastar/util/alloc_failure_injector.hh>

#include <algorithm>
#include <cstdint>
#include <new>
#include <vector>

// Failing the allocations of one window of a test, a point at a time, and
// reading what the allocator holds live. A coroutine's frame, a continuation,
// a shared future's waiter, an item sent to another shard and a task's place
// in its queue are allocated where a failure ends the process: they are
// neither failed nor counted here.
namespace kwaque::runtime::testing {

// Whether this build can fail an allocation at a chosen point.
inline constexpr bool allocation_failures_available =
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
  true;
#else
  false;
#endif
// Whether this build's allocator counts the objects it holds.
inline constexpr bool live_objects_counted =
#if defined(SEASTAR_DEFAULT_ALLOCATOR)
  false;
#else
  true;
#endif

// What this shard's allocator holds live. Zero where it does not count.
[[nodiscard]] inline std::uint64_t live_objects() noexcept {
    return seastar::memory::stats().live_objects();
}

// How a window fails: its chosen allocation alone, or that one and every
// one after it while the window is open. What throws a failure takes one
// allocation of its own, which passes.
enum class allocation_failures : std::uint8_t { once, from_the_first };

// One round's window. Nothing fails before open() or after close().
class allocation_window final {
public:
    // A window that fails nothing counts its allocation points.
    static constexpr std::uint64_t never = UINT64_MAX;

    allocation_window(allocation_failures mode, std::uint64_t at) noexcept
      : mode_(mode)
      , at_(at) {}
    allocation_window(const allocation_window&) = delete;
    allocation_window& operator=(const allocation_window&) = delete;
    ~allocation_window() { close(); }

    void open() noexcept {
        auto& injector = seastar::memory::local_failure_injector();
        begin_ = injector.alloc_count();
        open_ = true;
        if (at_ != never) injector.fail_after(at_);
    }
    void close() noexcept {
        if (!open_) return;
        auto& injector = seastar::memory::local_failure_injector();
        injector.cancel();
        open_ = false;
        points_ += injector.alloc_count() - begin_;
    }
    // The allocation point this round fails, or `never`.
    [[nodiscard]] std::uint64_t at() const noexcept { return at_; }
    // Allocations failed while it was open.
    [[nodiscard]] std::uint64_t failures() const noexcept { return failures_; }
    // Allocation points passed while it was open.
    [[nodiscard]] std::uint64_t points() const noexcept { return points_; }

    // The injector's action while a round runs.
    void fail() {
        ++failures_;
        // The exception below is the next allocation point.
        if (open_ && mode_ == allocation_failures::from_the_first)
            seastar::memory::local_failure_injector().fail_after(1);
        throw std::bad_alloc();
    }

private:
    allocation_failures mode_;
    std::uint64_t at_;
    std::uint64_t begin_{0}, points_{0}, failures_{0};
    bool open_{false};
};

struct allocation_sweep final {
    // Allocation points of the window in the round that failed none.
    std::uint64_t points{0};
    std::uint64_t rounds{0};
    // Rounds in which an allocation failed, and the allocations failed.
    std::uint64_t failed{0}, failures{0};
    // Rounds that left more objects live than they found, twice running.
    std::uint64_t leaks{0};
};

// Runs round(window) once with nothing failing, which counts the window's
// allocation points, and then once for each point with that point failing.
// With `sample` not zero, about that many points spread over the window are
// taken in place of all of them. A round builds what it needs, opens the
// window round what is under test and closes it before it checks anything;
// what it built is gone when it returns.
//
// A round that leaves more objects live than it found is run again: state a
// process makes once is there the second time, and a leak grows again.
// Called on a Seastar thread; a round may block.
template<typename Round>
allocation_sweep sweep_allocation_failures(
  allocation_failures mode, std::uint64_t sample, Round round) {
    auto& injector = seastar::memory::local_failure_injector();
    allocation_sweep swept;
    // What a round left live beyond what it found.
    const auto run = [&](std::uint64_t at) {
        const auto before = live_objects();
        allocation_window window{mode, at};
        injector.run_with_callback(
          [&window] { window.fail(); }, [&] { round(window); });
        window.close();
        ++swept.rounds;
        swept.failed += window.failures() != 0 ? 1 : 0;
        swept.failures += window.failures();
        if (at == allocation_window::never) swept.points = window.points();
        const auto after = live_objects();
        return after > before ? after - before : 0;
    };
    // The first round also makes what the process makes once.
    static_cast<void>(run(allocation_window::never));
    static_cast<void>(run(allocation_window::never));
    std::vector<std::uint64_t> chosen;
    const auto count = sample == 0 ? swept.points
                                   : std::min(sample, swept.points);
    chosen.reserve(count);
    for (std::uint64_t i = 0; i != count; ++i)
        chosen.push_back(i * swept.points / count);
    for (const auto at : chosen)
        if (run(at) != 0 && run(at) != 0) ++swept.leaks;
    return swept;
}

} // namespace kwaque::runtime::testing
