#pragma once

#include "src/base/invariant.h"

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/parent_from_member.hpp>

#include <algorithm>
#include <concepts>
#include <cstdint>

namespace kwaque::storage {

namespace testing {
struct cache_hook_access;
} // namespace testing

// The queue an entry is taken from. See cache_evictor for what each means for
// the entry.
enum class cache_queue : std::uint8_t { small, main, ghost };

// What the policy asks before it takes an entry, and tells when it has.
//
// The function is given a candidate and the queue it would leave. Returning
// false keeps the entry where it is: an entry in use is not evicted, and the
// policy goes on to the next.
//
// An entry that leaves the small queue is remembered: it goes onto the ghost
// queue when the function returns, so the function must leave the entry
// valid and where its owner can find it again. What it frees is the entry's
// value. An entry that leaves the main queue, or that the ghost queue stops
// remembering, has left the policy entirely and is unlinked already, so the
// function may destroy it. The ghost queue takes no refusal.
template<typename F, typename E>
concept cache_evictor = requires(F func, E& entry, cache_queue from) {
    requires noexcept(func(entry, from));
    { func(entry, from) } -> std::same_as<bool>;
};

// What an entry costs, in the units of the policy's capacity. It is read
// when an entry enters a queue and again before it is asked to leave one, and
// must be the same both times.
template<typename F, typename E>
concept cache_cost = requires(F func, const E& entry) {
    requires noexcept(func(entry));
    { func(entry) } -> std::same_as<std::uint64_t>;
};

// What the policy keeps in each entry.
//
// An entry that replaces one the ghost queue remembers starts from that
// entry's hook: a copy carries what was remembered and none of the queues.
//
// An owner that knows how many uses of an entry are still to come says so
// before the entry is put in the cache. Such an entry waits in the main
// queue for them, and each of them, as it comes, is taken off what is
// expected and earns the entry nothing more. A use nobody announced is
// counted, up to three, and earns a further turn as in any cache.
class cache_hook final {
public:
    cache_hook() noexcept = default;
    cache_hook(const cache_hook& other) noexcept
      : ghost_insertion_time_(other.ghost_insertion_time_)
      , has_ghost_insertion_time_(other.has_ghost_insertion_time_)
      , freq_(other.freq_) {}
    cache_hook& operator=(const cache_hook&) = delete;
    ~cache_hook() noexcept = default;

    // Says how many uses of the entry are still to come. Called before the
    // entry is put in the cache.
    void expect(std::uint32_t uses) noexcept {
        expected_ = static_cast<std::uint8_t>(
          std::min<std::uint32_t>(uses, max_expected));
    }
    // Marks a use of the entry.
    void touch() noexcept {
        if (expected_ > 0) {
            --expected_;
            return;
        }
        freq_ = static_cast<std::uint8_t>(std::min(freq_ + 1, 3));
    }
    // Whether the entry is in neither the small nor the main queue. True of
    // an entry never inserted and of one only remembered.
    [[nodiscard]] bool evicted() const noexcept {
        return location_ != location::small && location_ != location::main;
    }

private:
    using hook_type = boost::intrusive::list_member_hook<
      boost::intrusive::link_mode<boost::intrusive::safe_link>>;
    enum class location : std::uint8_t { none, small, main, ghost };
    static constexpr std::uint32_t max_expected = 255;

    template<
      typename T,
      cache_hook T::* Hook,
      cache_evictor<T> Evictor,
      cache_cost<T> Cost>
    friend class cache_policy;
    friend struct testing::cache_hook_access;

    // An entry is in one queue at most, so one link serves all three.
    hook_type hook_;
    // This is conceptually an optional, but that increases the struct size.
    std::uint64_t ghost_insertion_time_{0};
    bool has_ghost_insertion_time_{false};
    // Uses nobody announced, up to three.
    std::uint8_t freq_{0};
    // Uses still to come that its owner announced, and the further turns the
    // entry was given in the main queue while it waited for them.
    std::uint8_t expected_{0};
    std::uint8_t turns_{0};
    location location_{location::none};
};

// The order in which a bounded cache gives its entries up: three queues, the
// small, the main and the ghost.
//
// A new entry waits in the small queue. When that queue is over its share
// its oldest entry goes: to the main queue if it was used more than once
// meanwhile, and otherwise out, remembered by the ghost queue. An entry that
// comes back while it is remembered enters the main queue at once. The main
// queue gives its oldest entry another turn for each use it still has, up to
// three, and evicts the first that has none. The ghost queue remembers an
// entry until as much has left the small queue since as the main queue
// holds.
//
// An entry with uses still expected (see cache_hook) enters the main queue
// at once, and while they are to come the main queue gives it another turn,
// `expected_turns` times at most. What was used has less left to come, not
// more: of two entries that were announced, the one all its uses have reached
// goes first.
//
// The policy holds no capacity of its own to enforce: its owner asks for one
// entry at a time, as often as it needs room. An entry that refuses stays,
// so the order is not strictly first in, first out.
//
// One request does a bounded amount of work that frees nothing, counted in
// each queue against `effort_cap` for every entry the request has freed, and
// that many before the first:
//
// - It gives the main queue's entries that many further turns. After that it
//   takes the oldest entry whatever it has left, so a cache whose every
//   value was used or is waited for still makes room, in the order the
//   values came.
// - It passes that many refusals by, and that many promotions out of the
//   small queue, and then gives the queue up for this request. What it
//   passed over goes to the back of the queue, in order, so that the next
//   request continues from where this one stopped. Short of that bound an
//   entry that refused keeps its place.
template<
  typename T,
  cache_hook T::* Hook,
  cache_evictor<T> Evictor,
  cache_cost<T> Cost>
class cache_policy final {
public:
    static constexpr std::uint64_t default_effort_cap = 30;
    static constexpr std::uint8_t default_expected_turns = 5;
    struct config final {
        // What the two queues hold together when the cache is full.
        std::uint64_t cache_size;
        // The small queue's share of it.
        std::uint64_t small_size;
        // The work that frees nothing which a request may do in one queue
        // for each entry it has freed, and before the first.
        std::uint64_t effort_cap{default_effort_cap};
        // The further turns an entry gets in the main queue while uses of it
        // are still expected.
        std::uint8_t expected_turns{default_expected_turns};
    };
    struct sizes final {
        std::uint64_t small_queue_size;
        std::uint64_t main_queue_size;
        // Entries the ghost queue holds, expired ones included.
        std::uint64_t ghost_queue_entries;
    };
    // What the evictions of one request have done so far.
    struct effort final {
        std::uint64_t freed{0};
        // Promotions and refusals passed by in the small queue.
        std::uint64_t small_wasted{0};
        // Further turns given in the main queue, and refusals passed by.
        std::uint64_t main_turns{0};
        std::uint64_t main_refused{0};
        std::uint64_t forgotten{0};
    };

    explicit cache_policy(
      config config,
      const Evictor& evict = Evictor(),
      const Cost& cost = Cost()) noexcept
      : max_small_queue_size_(config.small_size)
      , max_main_queue_size_(config.cache_size - config.small_size)
      , effort_cap_(config.effort_cap)
      , expected_turns_(config.expected_turns)
      , evict_(evict)
      , cost_(cost) {
        KWAQUE_INVARIANT(
          invariant_id{"KQ-CACHE-POLICY-SHARES"},
          config.cache_size > config.small_size,
          "the small queue's share is the whole cache");
    }
    cache_policy(const cache_policy&) = delete;
    cache_policy& operator=(const cache_policy&) = delete;
    ~cache_policy() noexcept = default;

    // Puts an entry in the cache: in the main queue while uses of it are
    // expected or the ghost queue remembers it, and otherwise in the small
    // queue. Nothing is evicted for it: its owner made the room.
    void insert(T& entry) noexcept {
        auto& hook = entry.*Hook;
        hook.turns_ = 0;
        if (hook.expected_ > 0 || ghost_queue_contains(entry)) {
            hook.has_ghost_insertion_time_ = false;
            main_fifo_.push_back(entry);
            hook.location_ = cache_hook::location::main;
            main_queue_size_ += cost_(entry);
        } else {
            small_fifo_.push_back(entry);
            hook.location_ = cache_hook::location::small;
            small_queue_size_ += cost_(entry);
        }
    }

    // Takes an entry out of whichever queue holds it. Nothing happens for an
    // entry in none.
    void remove(T& entry) noexcept {
        auto& hook = entry.*Hook;
        switch (hook.location_) {
        case cache_hook::location::none:
            return;
        case cache_hook::location::main:
            main_queue_size_ -= cost_(entry);
            main_fifo_.erase(main_fifo_.iterator_to(entry));
            break;
        case cache_hook::location::small:
            small_queue_size_ -= cost_(entry);
            small_fifo_.erase(small_fifo_.iterator_to(entry));
            break;
        case cache_hook::location::ghost:
            ghost_fifo_.erase(ghost_fifo_.iterator_to(entry));
            --ghost_queue_entries_;
            break;
        }
        hook.location_ = cache_hook::location::none;
    }

    // Whether the ghost queue still remembers the entry, or the entry it
    // replaces.
    [[nodiscard]] bool ghost_queue_contains(const T& entry) const noexcept {
        /*
         * if the insertion time is unspecified then the entry is not on the
         * ghost queue. otherwise, the entry is on the ghost queue if it
         * hasn't yet "timed out":
         *
         *    expires = ghost_insertion_time + main queue capacity
         *        now = ghost_queue_age
         *   on_ghost = expires > now
         */
        const auto& hook = entry.*Hook;
        if (hook.has_ghost_insertion_time_) {
            const auto expires = hook.ghost_insertion_time_
                                 + max_main_queue_size_;
            return expires > ghost_queue_age_;
        }
        return false;
    }

    // Evicts one entry: from the small queue while it is over its share, and
    // from the main queue otherwise. When that queue gives nothing the other
    // is asked, where a cache that may exceed its size would exceed it.
    // False when nothing went.
    bool evict(effort& spent) noexcept {
        if (small_queue_size_ > max_small_queue_size_)
            return evict_small(spent) || evict_main(spent);
        return evict_main(spent) || evict_small(spent);
    }

    // Stops remembering the entries the ghost queue has kept long enough, at
    // most `effort_cap` of them a request.
    void forget_expired(effort& spent) noexcept {
        while (!ghost_fifo_.empty() && spent.forgotten < effort_cap_) {
            auto& entry = ghost_fifo_.front();
            // The ghost queue is in fifo order, so whatever follows an entry
            // still remembered is remembered too.
            if (ghost_queue_contains(entry)) return;
            forget(entry, spent);
        }
    }
    // Stops remembering the oldest entry, expired or not. False when the
    // ghost queue is empty or the request has forgotten its share.
    bool forget_oldest(effort& spent) noexcept {
        if (ghost_fifo_.empty() || spent.forgotten >= effort_cap_) return false;
        forget(ghost_fifo_.front(), spent);
        return true;
    }

    // Some entry a queue holds, or nothing when all three are empty.
    [[nodiscard]] T* any() noexcept {
        for (auto* fifo : {&small_fifo_, &main_fifo_, &ghost_fifo_})
            if (!fifo->empty()) return &fifo->front();
        return nullptr;
    }

    [[nodiscard]] sizes size() const noexcept {
        return {
          .small_queue_size = small_queue_size_,
          .main_queue_size = main_queue_size_,
          .ghost_queue_entries = ghost_queue_entries_};
    }
    [[nodiscard]] std::uint64_t small_capacity() const noexcept {
        return max_small_queue_size_;
    }

private:
    struct hook_converter final {
        using hook_type = cache_hook::hook_type;
        using hook_ptr = hook_type*;
        using const_hook_ptr = const hook_type*;
        using value_type = T;
        using pointer = value_type*;
        using const_pointer = const value_type*;

        static hook_ptr to_hook_ptr(value_type& value) {
            return &((value.*Hook).hook_);
        }
        static const_hook_ptr to_hook_ptr(const value_type& value) {
            return &((value.*Hook).hook_);
        }
        static pointer to_value_ptr(hook_ptr hook) {
            return boost::intrusive::get_parent_from_member(
              boost::intrusive::get_parent_from_member(
                hook, &cache_hook::hook_),
              Hook);
        }
        static const_pointer to_value_ptr(const_hook_ptr hook) {
            return boost::intrusive::get_parent_from_member(
              boost::intrusive::get_parent_from_member(
                hook, &cache_hook::hook_),
              Hook);
        }
    };
    using list_type = boost::intrusive::list<
      T,
      boost::intrusive::function_hook<hook_converter>,
      boost::intrusive::constant_time_size<false>>;

    bool evict_small(effort& spent) noexcept {
        for (auto it = small_fifo_.begin(); it != small_fifo_.end();) {
            if (spent.small_wasted >= (spent.freed + 1) * effort_cap_) {
                small_fifo_.splice(
                  small_fifo_.end(), small_fifo_, small_fifo_.begin(), it);
                return false;
            }
            auto& entry = *it;
            auto& hook = entry.*Hook;
            if (hook.freq_ > 1) {
                hook.freq_ = 0;

                // evict from small queue
                it = small_fifo_.erase(it);
                const auto cost = cost_(entry);
                small_queue_size_ -= cost;

                // promote to main queue
                main_fifo_.push_back(entry);
                hook.location_ = cache_hook::location::main;
                main_queue_size_ += cost;
                ++spent.small_wasted;

                if (main_queue_size_ > max_main_queue_size_) {
                    if (evict_main(spent)) return true;
                }
                continue;
            }
            // What the evictor leaves of the entry may cost less, so the
            // cost is read first.
            const auto cost = cost_(entry);
            if (evict_(entry, cache_queue::small)) {
                // evict from small queue
                it = small_fifo_.erase(it);
                small_queue_size_ -= cost;

                // insert into ghost queue
                ghost_fifo_.push_back(entry);
                hook.location_ = cache_hook::location::ghost;
                hook.ghost_insertion_time_ = ghost_queue_age_;
                hook.has_ghost_insertion_time_ = true;
                ghost_queue_age_ += cost;
                ++ghost_queue_entries_;
                ++spent.freed;
                return true;
            }
            ++spent.small_wasted;
            ++it;
        }
        return false;
    }

    bool evict_main(effort& spent) noexcept {
        for (auto it = main_fifo_.begin(); it != main_fifo_.end();) {
            if (spent.main_refused >= (spent.freed + 1) * effort_cap_) {
                main_fifo_.splice(
                  main_fifo_.end(), main_fifo_, main_fifo_.begin(), it);
                return false;
            }
            auto& entry = *it;
            auto& hook = entry.*Hook;
            // Uses still to come keep an entry before uses it has had do.
            const bool awaited = hook.expected_ > 0
                                 && hook.turns_ < expected_turns_;
            if (
              (awaited || hook.freq_ > 0)
              && spent.main_turns < (spent.freed + 1) * effort_cap_) {
                if (awaited) {
                    ++hook.turns_;
                } else {
                    --hook.freq_;
                }

                // promote within main queue
                it = main_fifo_.erase(it);
                main_fifo_.push_back(entry);
                ++spent.main_turns;
                continue;
            }
            // The evictor may destroy the entry on approval, so read the
            // cost and unlink before invoking it.
            const auto cost = cost_(entry);
            auto next = main_fifo_.erase(it);
            hook.location_ = cache_hook::location::none;
            if (evict_(entry, cache_queue::main)) {
                // evict from main queue
                main_queue_size_ -= cost;
                ++spent.freed;
                return true;
            }
            // the entry declined eviction; restore its position
            main_fifo_.insert(next, entry);
            hook.location_ = cache_hook::location::main;
            ++spent.main_refused;
            it = next;
        }
        return false;
    }

    void forget(T& entry, effort& spent) noexcept {
        ghost_fifo_.erase(ghost_fifo_.iterator_to(entry));
        (entry.*Hook).location_ = cache_hook::location::none;
        --ghost_queue_entries_;
        // Not among what was freed: a record holds no value, and what was
        // freed is what a request's further work is allowed by.
        ++spent.forgotten;
        // The entry is out of the policy, and may be destroyed here.
        evict_(entry, cache_queue::ghost);
    }

    std::uint64_t max_small_queue_size_;
    std::uint64_t max_main_queue_size_;
    std::uint64_t effort_cap_;
    std::uint8_t expected_turns_;

    Evictor evict_;
    Cost cost_;

    std::uint64_t small_queue_size_{0};
    std::uint64_t main_queue_size_{0};
    std::uint64_t ghost_queue_age_{0};
    std::uint64_t ghost_queue_entries_{0};

    list_type small_fifo_;
    list_type main_fifo_;
    list_type ghost_fifo_;
};

} // namespace kwaque::storage
