#pragma once

#include <cstdint>

namespace kwaque::codec::testing {

// Test-only observation of new native allocations on the calling reactor.
// Input owners and shared engine state that predate begin() are excluded.
// Critical allocations include coroutine frames, but are not a frame-size API.
// Classification is available only with allocation-failure injection compiled
// in; otherwise its zero counter does not establish a critical-memory bound.
// Internal frees can bypass executable link wrapping. Their charges remain
// until pointer reuse proves release, so live/peak values are conservative
// upper bounds. Synchronous libc error-message translation is bounded from its
// native allocation count and requested-byte total, retaining every charge and
// assigning it to the critical subset when supported. Every allocation must
// still match the native allocation count.
// Frees count only tracked owners; releasing a pre-existing native or system
// owner cannot subtract from the bound or count as an observed native free.
struct allocation_observation final {
    bool observed{false};
    bool complete{false};
    std::uint64_t allocations{0};
    std::uint64_t frees{0};
    std::uint64_t peak_upper_bound{0};
    std::uint64_t live_upper_bound{0};
    std::uint64_t critical_peak_upper_bound{0};
    std::uint64_t largest_allocation{0};
    std::uint64_t native_allocations{0};
    std::uint64_t native_frees{0};
};

// Only one interval on one reactor may use the static observer workspace.
// Observation allocates nothing. Overflow or missed native allocations make
// the result incomplete instead of silently understating its peak. Reallocation
// includes the old/new overlap; retained input plus peak_upper_bound is a
// conservative bound, not a claim that all initial input remained live
// throughout the call.
void begin_allocation_observation() noexcept;
[[nodiscard]] allocation_observation end_allocation_observation() noexcept;

} // namespace kwaque::codec::testing
