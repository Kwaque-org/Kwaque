#include "src/runtime/deadline_wheel.h"

#include <algorithm>
#include <bit>
#include <limits>
#include <utility>

namespace kwaque::runtime {

namespace {

constexpr std::uint64_t slot_mask{deadline_wheel::level_slots - 1U};

constexpr unsigned level_of(std::uint16_t place) noexcept {
    return place / deadline_wheel::level_slots;
}

constexpr unsigned slot_of(std::uint16_t place) noexcept {
    return place % deadline_wheel::level_slots;
}

constexpr unsigned shift_of(unsigned level) noexcept {
    return level * deadline_wheel::level_shift;
}

} // namespace

deadline_wheel::deadline_wheel(monotonic_time now) noexcept
  : clock_(floor_tick(now)) {}

deadline_wheel::~deadline_wheel() {
    assert_current();
    KWAQUE_INVARIANT(
      invariant_id{"KQ-WHEEL-NOT-EMPTY"},
      size_ == 0,
      "a deadline wheel was destroyed with deadlines armed");
}

std::uint64_t deadline_wheel::floor_tick(monotonic_time time) noexcept {
    return time.nanoseconds() / tick_nanoseconds;
}

std::uint64_t deadline_wheel::ceil_tick(monotonic_time time) noexcept {
    const auto whole = time.nanoseconds() / tick_nanoseconds;
    return time.nanoseconds() % tick_nanoseconds == 0 ? whole : whole + 1U;
}

std::uint64_t
deadline_wheel::bucket_tick(unsigned level, unsigned slot) const noexcept {
    // Every pending bucket of a level lies in the 64 slots after the clock's,
    // so its slot names exactly one first tick.
    const auto base = clock_ >> shift_of(level);
    const auto offset = (slot - (base + 1U)) & slot_mask;
    return (base + 1U + offset) << shift_of(level);
}

std::optional<std::uint64_t> deadline_wheel::next_bucket_tick() const noexcept {
    std::optional<std::uint64_t> earliest;
    for (unsigned level = 0; level < level_count; ++level) {
        if (pending_[level] == 0) {
            continue;
        }
        const auto base = clock_ >> shift_of(level);
        const auto start = static_cast<int>((base + 1U) & slot_mask);
        const auto rotated = std::rotr(pending_[level], start);
        const auto offset = static_cast<std::uint64_t>(
          std::countr_zero(rotated));
        const auto tick = (base + 1U + offset) << shift_of(level);
        if (!earliest || tick < *earliest) {
            earliest = tick;
        }
    }
    return earliest;
}

bool deadline_wheel::has_collected() const noexcept {
    return std::ranges::any_of(
      collected_pending_, [](std::uint64_t bits) { return bits != 0; });
}

std::optional<monotonic_time>
deadline_wheel::next_expiry(monotonic_time now) const noexcept {
    if (has_collected()) {
        return now;
    }
    const auto tick = next_bucket_tick();
    if (!tick) {
        return std::nullopt;
    }
    constexpr auto latest = std::numeric_limits<monotonic_time::rep>::max()
                            / tick_nanoseconds;
    return monotonic_time{
      *tick > latest ? monotonic_time::maximum().nanoseconds()
                     : *tick * tick_nanoseconds};
}

void deadline_wheel::forward(std::uint64_t now_tick) noexcept {
    // Catch the clock up with time, but never past a bucket that is still
    // pending, so an idle wheel skips empty ticks without visiting them.
    if (now_tick <= clock_) {
        return;
    }
    auto target = now_tick;
    if (const auto next = next_bucket_tick(); next && *next <= target) {
        target = *next - 1U;
    }
    clock_ = std::max(clock_, target);
}

void deadline_wheel::push(wheel_entry*& head, wheel_entry& entry) noexcept {
    entry.next_ = head;
    if (head != nullptr) {
        head->pprev_ = &entry.next_;
    }
    head = &entry;
    entry.pprev_ = &head;
}

void deadline_wheel::unlink(wheel_entry& entry) noexcept {
    *entry.pprev_ = entry.next_;
    if (entry.next_ != nullptr) {
        entry.next_->pprev_ = entry.pprev_;
    }
    entry.next_ = nullptr;
    entry.pprev_ = nullptr;
}

void deadline_wheel::place(wheel_entry& entry) noexcept {
    for (unsigned level = 0; level < level_count; ++level) {
        const auto key = entry.tick_ >> shift_of(level);
        const auto base = clock_ >> shift_of(level);
        if (key > base && key - base <= level_slots) {
            const auto place = static_cast<std::uint16_t>(
              level * level_slots + (key & slot_mask));
            auto& head = buckets_[place];
            KWAQUE_DEBUG_ASSERT(
              invariant_id{"KQ-WHEEL-BUCKET-SHARED"},
              head == nullptr || (head->flags_ & parked_flag) != 0
                || (head->tick_ >> shift_of(level)) == key,
              "two expiries share one wheel bucket");
            push(head, entry);
            pending_[level] |= std::uint64_t{1} << (key & slot_mask);
            entry.place_ = place;
            return;
        }
    }
    // Beyond the top level: park in its farthest bucket and place again from
    // there, once the deadline is within the wheel's span.
    constexpr unsigned top = level_count - 1U;
    const auto key = (clock_ >> shift_of(top)) + level_slots;
    const auto place = static_cast<std::uint16_t>(
      top * level_slots + (key & slot_mask));
    entry.flags_ |= parked_flag;
    push(buckets_[place], entry);
    pending_[top] |= std::uint64_t{1} << (key & slot_mask);
    entry.place_ = place;
}

deadline_wheel::insertion deadline_wheel::insert(
  wheel_entry& entry,
  monotonic_time deadline,
  precision kind,
  monotonic_time now) noexcept {
    assert_current();
    KWAQUE_INVARIANT(
      invariant_id{"KQ-WHEEL-ENTRY-LINKED"},
      !entry.linked(),
      "a deadline was armed twice");
    const auto now_tick = floor_tick(now);
    forward(now_tick);
    const bool early = kind == precision::may_end_early;
    const auto tick = ceil_tick(deadline);
    // Against the time itself, not the wheel's clock, which a driver that has
    // fallen behind leaves short of it.
    if (tick <= std::max(clock_, now_tick)) {
        return insertion::due;
    }
    entry.tick_ = tick;
    entry.flags_ = early ? may_end_early_flag : std::uint8_t{0};
    entry.wheel_ = this;
    place(entry);
    ++size_;
    return insertion::linked;
}

bool deadline_wheel::cancel(wheel_entry& entry) noexcept {
    assert_current();
    if (!entry.linked()) {
        return false;
    }
    KWAQUE_INVARIANT(
      invariant_id{"KQ-WHEEL-ENTRY-FOREIGN"},
      entry.wheel_ == this,
      "a deadline was cancelled on another wheel");
    const auto place = entry.place_;
    unlink(entry);
    const auto bit = std::uint64_t{1} << slot_of(place);
    if (buckets_[place] == nullptr) {
        pending_[level_of(place)] &= ~bit;
    }
    if (collected_[place] == nullptr) {
        collected_pending_[level_of(place)] &= ~bit;
    }
    entry.wheel_ = nullptr;
    --size_;
    if (size_ == 0 && emptied_ != nullptr) {
        emptied_(emptied_context_);
    }
    return true;
}

void deadline_wheel::collect(std::uint64_t now_tick) noexcept {
    if (now_tick <= clock_) {
        return;
    }
    // Every due bucket moves before anything ends, so nothing an expiry
    // inserts can land in a bucket that is still being emptied.
    for (unsigned level = 0; level < level_count; ++level) {
        auto bits = pending_[level];
        while (bits != 0) {
            const auto slot = static_cast<unsigned>(std::countr_zero(bits));
            bits &= bits - 1U;
            if (bucket_tick(level, slot) > now_tick) {
                continue;
            }
            const auto place = level * level_slots + slot;
            auto* chain = std::exchange(buckets_[place], nullptr);
            pending_[level] &= ~(std::uint64_t{1} << slot);
            auto& collected = collected_[place];
            KWAQUE_DEBUG_ASSERT(
              invariant_id{"KQ-WHEEL-COLLECTED-LEFT"},
              collected == nullptr,
              "buckets were collected while collected entries were left");
            collected = chain;
            chain->pprev_ = &collected;
            collected_pending_[level] |= std::uint64_t{1} << slot;
        }
    }
    clock_ = now_tick;
}

wheel_entry* deadline_wheel::pop_collected() noexcept {
    for (unsigned level = 0; level < level_count; ++level) {
        if (collected_pending_[level] == 0) {
            continue;
        }
        const auto slot = static_cast<unsigned>(
          std::countr_zero(collected_pending_[level]));
        const auto place = level * level_slots + slot;
        auto* entry = collected_[place];
        unlink(*entry);
        if (collected_[place] == nullptr) {
            collected_pending_[level] &= ~(std::uint64_t{1} << slot);
        }
        return entry;
    }
    return nullptr;
}

std::size_t
deadline_wheel::expire(monotonic_time now, std::size_t budget) noexcept {
    assert_current();
    // Entries a budgeted call left are ended first, so moving a bucket never
    // walks a chain.
    if (!has_collected()) {
        collect(floor_tick(now));
    }
    std::size_t touched = 0;
    while (touched < budget) {
        auto* entry = pop_collected();
        if (entry == nullptr) {
            break;
        }
        ++touched;
        const bool parked = (entry->flags_ & parked_flag) != 0;
        const bool early = (entry->flags_ & may_end_early_flag) != 0;
        entry->flags_ &= static_cast<std::uint8_t>(~parked_flag);
        // Still ahead: an exact deadline, or any parked one, is placed again.
        // A parked deadline already passed (its bucket was collected late)
        // ends now rather than parking again.
        if (entry->tick_ > clock_ && (parked || !early)) {
            place(*entry);
            continue;
        }
        entry->wheel_ = nullptr;
        --size_;
        entry->on_expiry_(*entry);
    }
    return touched;
}

} // namespace kwaque::runtime
