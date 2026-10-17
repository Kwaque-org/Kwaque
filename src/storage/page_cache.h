#pragma once

#include "src/base/error.h"
#include "src/base/invariant.h"
#include "src/base/units.h"
#include "src/runtime/error.h"
#include "src/runtime/shard_affinity.h"
#include "src/storage/cache_policy.h"
#include "src/storage/local_paths.h"
#include "src/storage/workload_budget.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/preempt.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <boost/intrusive/avl_set.hpp>
#include <boost/intrusive/list.hpp>

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <iterator>
#include <memory>
#include <new>
#include <optional>
#include <type_traits>
#include <utility>

namespace kwaque::storage {

inline constexpr std::uint32_t default_cache_load_slots = 16;
inline constexpr std::uint32_t maximum_cache_load_slots = 64;

struct page_cache_config final {
    // The most the cache is charged at once: what its entries hold, what its
    // loads in flight are about to hold, and the entries it only remembers.
    // A tenth of it is the share of values not yet used twice, and a value
    // as large as that share is not kept.
    byte_count capacity{};
    // The most loads the cache runs at once.
    std::uint32_t load_slots{default_cache_load_slots};
    [[nodiscard]] runtime::result<void> validate() const noexcept;
};

struct page_cache_snapshot final {
    // Bytes charged now, of `capacity`: for entries in their objects'
    // indexes, entries out of them that are still held, and loads in flight.
    std::uint64_t capacity{0}, bytes{0};
    // Loads started, callers that waited for a load in flight, loads their
    // reader funded, and requests refused for want of room.
    std::uint64_t loads{0}, waits{0}, uncached{0}, refused{0};
    // Bytes of values not yet used twice and of values that were, entries
    // only remembered, and values evicted for room.
    std::uint64_t small_bytes{0}, main_bytes{0}, remembered{0}, evicted{0};
};

template<typename Value>
class cache_ref;
template<typename Value>
class cache_hold;
template<typename Value>
class cache_load;
template<typename Value>
class cache_slot;
template<typename Key, typename Value>
class cache_index;
class page_cache;

namespace detail {

[[nodiscard]] runtime::operation_error cache_error(errc) noexcept;

// What loads a value for the cache: called once, on the cache's own behalf,
// with the source the cache requests when it stops. It owns everything it
// reads through, and no caller can end it.
template<typename Loader, typename Value>
concept cache_loader
  = std::is_nothrow_move_constructible_v<Loader>
    && requires(Loader& loader, seastar::abort_source& stop) {
           {
               loader(stop)
           } -> std::same_as<seastar::future<runtime::result<Value>>>;
       };

// A value that says what it holds. Its entry is charged exactly that when
// its load ends, whatever the load was admitted for: a read's buffers are
// priced only once they are there.
template<typename Value>
concept priced_value = requires(const Value& value) {
    { value.charge() } noexcept -> std::same_as<byte_count>;
};

struct cache_account;
struct cache_admission;
class cache_entry;

// What an entry's own type does for it. A cache holds entries of any value
// and knows none of their types: each type has one of these, and every entry
// of the type points at it.
struct cache_entry_kind final {
    // Takes the entry out of its object's index.
    void (*unlink)(cache_entry&) noexcept;
    // Gives its value up.
    void (*discard)(cache_entry&) noexcept;
    // Ends it and frees it.
    void (*destroy)(cache_entry&) noexcept;
};

// One caller waiting for a load, in that caller's own frame: waiting
// allocates nothing, and a load takes any number of callers. Whoever takes a
// waiter off its load's list tells it, once: the load when it ends, with why
// there is no value or with nothing, or the caller's own release.
struct cache_waiter final
  : boost::intrusive::list_base_hook<
      boost::intrusive::link_mode<boost::intrusive::auto_unlink>> {
    seastar::promise<std::optional<runtime::operation_error>> told;
};

// One cached value's place in the cache: what it is charged, who holds it,
// the load that fills it and its turn to be evicted.
//
// An entry is in its object's index or out of it, and lives until it is out
// of the index and nothing holds it. Its bytes are given back then and not
// before. In the index it is loading, or holds its value, or is only
// remembered: its value was evicted before it was used twice, and the entry
// itself stays for a while so that the value's return is recognized.
class cache_entry : public runtime::shard_affine {
public:
    cache_entry(const cache_entry&) = delete;
    cache_entry& operator=(const cache_entry&) = delete;

    // What the cache is charged for it: its value and itself, or itself
    // alone once it is only remembered. Nothing for an entry its reader
    // funds.
    [[nodiscard]] byte_count cost() const noexcept { return byte_count{cost_}; }
    // Whether a reader's reservation is charged for it, and not the cache.
    [[nodiscard]] bool funded() const noexcept { return funding_.has_value(); }
    [[nodiscard]] std::uint32_t holders() const noexcept { return holders_; }
    [[nodiscard]] bool loading() const noexcept { return loading_; }
    // Whether the load in flight is the cache's own: one a reader asked for,
    // which whoever asks next waits for. A load its object's owner leads
    // itself, as a writer fills a value part by part, is not, and no reader
    // is made to wait on it by asking.
    [[nodiscard]] bool fetching() const noexcept {
        return loading_ && fetched_;
    }
    void fetched() noexcept { fetched_ = true; }
    [[nodiscard]] bool resident() const noexcept { return resident_; }

    void hold() noexcept {
        assert_current();
        ++holders_;
    }
    void release() noexcept {
        assert_current();
        if (--holders_ == 0 && !resident_) destroy();
    }
    // In its object's index from now on.
    void installed() noexcept { resident_ = true; }
    // What its value will hold is now known to be `backing`, for a load
    // still in flight: more is taken as an admission takes it, evicting for
    // it, and less is given back. Refused as an admission is, and then
    // nothing changed. An entry its reader funds is checked against what
    // that reservation can hold.
    [[nodiscard]] runtime::result<void> resize(std::uint64_t backing) noexcept;
    // Out of its object's index, which has let go of it already.
    void retired() noexcept;
    // Takes it out of its object's index, if it is in it.
    void retire() noexcept;

    // Puts one more caller on the load in flight.
    void enlist(cache_waiter& waiting) noexcept;
    // Ends the load and tells whoever waits. A value is kept by the cache
    // from here on. A failure is kept by nothing, and neither is a value its
    // reader funds or one that outlived its object or its cache: the entry
    // leaves its index.
    // The load ended: with its value, its failure, or an exception of no
    // expected family, which every waiter is given as it is.
    void settle(
      std::optional<runtime::operation_error> failed,
      std::exception_ptr thrown = {}) noexcept;
    // The policy's question and its verdict: see cache_evictor.
    [[nodiscard]] bool evicted_from(cache_queue from) noexcept;

private:
    // Beside what follows it, so that a hit touches one cache line.
    std::uint32_t holders_{0};

public:
    // Its turn in the cache's queues, and how often it was used.
    cache_hook hook;

protected:
    cache_entry(
      cache_admission&& admitted, const cache_entry_kind& kind) noexcept;
    // Ended by its own type alone: see cache_entry_kind.
    ~cache_entry();

private:
    // The end of an entry out of its index that nothing holds.
    void destroy() noexcept;

    const cache_entry_kind* kind_;
    seastar::lw_shared_ptr<cache_account> account_;
    // The reservation of the reader that funds it, when the cache does not.
    std::optional<workload_reservation> funding_;
    // Those waiting for its load, each in its own frame.
    boost::intrusive::
      list<cache_waiter, boost::intrusive::constant_time_size<false>>
        waiters_;
    std::uint64_t cost_;
    // What the entry itself is charged, without its value.
    std::uint32_t shell_;
    bool resident_{false};
    bool loading_{true};
    bool fetched_{false};
};

struct cache_evict final {
    bool operator()(cache_entry& entry, cache_queue from) noexcept {
        return entry.evicted_from(from);
    }
};
struct cache_entry_cost final {
    std::uint64_t operator()(const cache_entry& entry) noexcept {
        return entry.cost().value();
    }
};
using cache_queues = cache_policy<
  cache_entry,
  &cache_entry::hook,
  cache_evict,
  cache_entry_cost>;

// What a cache is charged and the order it evicts in, shared with every
// entry: an entry released after its cache is gone still gives its bytes
// back, and one that leaves its index leaves its queue.
struct cache_account final : runtime::shard_affine {
    cache_account(workload_reservation held, std::uint64_t capacity) noexcept;
    ~cache_account();
    // Evicts until `need` more bytes fit, or nothing more can go within one
    // request's work.
    void make_room(std::uint64_t need) noexcept;
    // Lets every entry the queues hold go, in bounded steps.
    [[nodiscard]] seastar::future<> release();

    // The capacity, admitted once for the cache's whole life.
    workload_reservation held;
    std::uint64_t capacity;
    std::uint64_t used{0};
    std::uint64_t loads{0}, waits{0}, uncached{0}, refused{0}, evicted{0};
    cache_queues queues;
    bool stopped{false};
};

// An entry's bytes, taken before the entry exists and given back unless an
// entry takes them.
struct cache_admission final {
    explicit cache_admission(const cache_hook* remembered) noexcept
      : remembered(remembered != nullptr ? *remembered : cache_hook{}) {}
    cache_admission(cache_admission&&) noexcept = default;
    cache_admission& operator=(cache_admission&&) = delete;
    cache_admission(const cache_admission&) = delete;
    cache_admission& operator=(const cache_admission&) = delete;
    ~cache_admission() {
        if (account) account->used -= cost;
    }
    seastar::lw_shared_ptr<cache_account> account;
    std::optional<workload_reservation> funding;
    // What the entry this one replaces was remembered by.
    cache_hook remembered;
    std::uint64_t cost{0};
    std::uint32_t shell{0};
    // Reads of the value that are known to be still to come.
    std::uint32_t expected{0};
};

template<typename Value>
struct cached : cache_entry {
    static_assert(std::is_nothrow_move_constructible_v<Value>);
    static_assert(std::is_nothrow_destructible_v<Value>);
    cached(cache_admission&& admitted, const cache_entry_kind& kind) noexcept
      : cache_entry(std::move(admitted), kind) {}

    // Waits for the load in flight and holds what it loaded.
    seastar::future<runtime::result<cache_ref<Value>>>
    join(seastar::abort_source& caller) {
        using output = runtime::result<cache_ref<Value>>;
        // Held and listed before anything suspends: the waiter needs no
        // second lookup, whatever becomes of the entry meanwhile.
        cache_ref<Value> held{*this};
        cache_waiter waiting;
        enlist(waiting);
        // A waiter the load has told already is past leaving.
        const auto leaving = caller.subscribe([&waiting]() noexcept {
            if (!waiting.is_linked()) return;
            waiting.unlink();
            waiting.told.set_value(cache_error(errc::aborted));
        });
        const auto failed = co_await waiting.told.get_future();
        if (failed) co_return output{runtime::failure(*failed)};
        co_return output{std::move(held)};
    }

    std::optional<Value> value;

protected:
    ~cached() = default;
};

// The kind of the entries of type Entry: the one place that knows the type
// of an entry the cache holds.
template<typename Entry>
inline constexpr cache_entry_kind entry_kind{
  .unlink =
    [](cache_entry& entry) noexcept { static_cast<Entry&>(entry).unlink(); },
  .discard =
    [](cache_entry& entry) noexcept {
        static_cast<Entry&>(entry).value.reset();
    },
  .destroy =
    [](cache_entry& entry) noexcept { delete &static_cast<Entry&>(entry); },
};

template<typename Value>
struct slot_entry final : cached<Value> {
    slot_entry(cache_admission&& admitted, slot_entry*& place) noexcept
      : cached<Value>(std::move(admitted), entry_kind<slot_entry>)
      , home(&place) {
        place = this;
        this->installed();
    }
    void unlink() noexcept { *home = nullptr; }
    slot_entry** home;
};

template<typename Key, typename Value>
struct keyed_entry final : cached<Value> {
    static_assert(std::is_nothrow_copy_constructible_v<Key>);
    using tree_hook = boost::intrusive::avl_set_member_hook<
      boost::intrusive::link_mode<boost::intrusive::auto_unlink>>;
    keyed_entry(cache_admission&& admitted, const Key& key) noexcept
      : cached<Value>(std::move(admitted), entry_kind<keyed_entry>)
      , key(key) {}
    void unlink() noexcept { link.unlink(); }
    tree_hook link;
    Key key;
};

} // namespace detail

// A cached value, held. While something holds a value it stays what it was
// and where it was, whatever its object or its cache does meanwhile, and the
// cache stays charged for it: a value in use is never evicted. Never empty
// when a lookup returned it as found.
template<typename Value>
class cache_ref final {
public:
    cache_ref() noexcept = default;
    cache_ref(const cache_ref& other) noexcept
      : entry_(other.entry_) {
        if (entry_) entry_->hold();
    }
    cache_ref(cache_ref&& other) noexcept
      : entry_(std::exchange(other.entry_, nullptr)) {}
    cache_ref& operator=(const cache_ref& other) noexcept {
        if (this != &other) *this = cache_ref{other};
        return *this;
    }
    cache_ref& operator=(cache_ref&& other) noexcept {
        if (this != &other) {
            reset();
            entry_ = std::exchange(other.entry_, nullptr);
        }
        return *this;
    }
    ~cache_ref() { reset(); }

    explicit operator bool() const noexcept { return entry_ != nullptr; }
    [[nodiscard]] const Value& operator*() const noexcept {
        return *entry_->value;
    }
    [[nodiscard]] const Value* operator->() const noexcept {
        return &*entry_->value;
    }
    // The value, when the entry has one: nothing for an empty reference and
    // for an entry whose load has not ended.
    [[nodiscard]] const Value* value() const noexcept {
        return entry_ != nullptr && entry_->value ? &*entry_->value : nullptr;
    }
    void reset() noexcept {
        if (auto* held = std::exchange(entry_, nullptr)) held->release();
    }
    // Marks a use of the value, for whoever was given it by a lookup that
    // is not one: see cache_index::floor().
    void use() const noexcept { entry_->hook.touch(); }
    // Whether a reader's reservation is charged for the value, and not the
    // cache: one the cache had no room for.
    [[nodiscard]] bool funded() const noexcept { return entry_->funded(); }

private:
    friend struct detail::cached<Value>;
    friend class cache_slot<Value>;
    friend class cache_load<Value>;
    template<typename Key, typename Other>
    friend class cache_index;
    explicit cache_ref(detail::cached<Value>& held) noexcept
      : entry_(&held) {
        held.hold();
    }
    detail::cached<Value>* entry_{nullptr};
};

// An entry, held for what it is charged and not for its value: what keeps a
// part of a value where it is for someone who was given that part, whether
// or not the entry has its value yet, or ever will. The value is reached
// only by asking whether there is one. A cache_ref is one of these.
template<typename Value>
class cache_hold final {
public:
    cache_hold() noexcept = default;
    // NOLINTNEXTLINE(google-explicit-constructor,hicpp-explicit-conversions)
    cache_hold(cache_ref<Value> held) noexcept
      : held_(std::move(held)) {}

    explicit operator bool() const noexcept { return static_cast<bool>(held_); }
    // The value, when the entry has one by now.
    [[nodiscard]] const Value* value() const noexcept { return held_.value(); }
    void reset() noexcept { held_.reset(); }
    // Marks a use of the entry: see cache_ref::use().
    void use() const noexcept { held_.use(); }
    [[nodiscard]] bool funded() const noexcept { return held_.funded(); }

private:
    cache_ref<Value> held_;
};

// The one load of an entry, for whoever does the work: it ends with the value
// or with why there is none, once, and everyone waiting is told the same. A
// load let go of without either has failed with `closed`. It may outlive the
// object its entry belonged to and the cache: the value then reaches those
// waiting and is kept by nothing.
template<typename Value>
class cache_load final {
public:
    cache_load(cache_load&& other) noexcept
      : entry_(std::exchange(other.entry_, nullptr)) {}
    cache_load& operator=(cache_load&& other) noexcept {
        if (this != &other) {
            if (entry_) end(detail::cache_error(errc::closed));
            entry_ = std::exchange(other.entry_, nullptr);
        }
        return *this;
    }
    cache_load(const cache_load&) = delete;
    cache_load& operator=(const cache_load&) = delete;
    ~cache_load() {
        if (entry_) end(detail::cache_error(errc::closed));
    }

    // `value` holds no more than the load was admitted for.
    void publish(Value value) && noexcept {
        entry_->value.emplace(std::move(value));
        end(std::nullopt);
    }
    // The same, and the value, held: for a load whose own caller is the one
    // who wants the value, which one nothing keeps would not outlive its
    // load otherwise.
    [[nodiscard]] cache_ref<Value> publish_held(Value value) && noexcept {
        cache_ref<Value> held{*entry_};
        entry_->value.emplace(std::move(value));
        end(std::nullopt);
        return held;
    }
    void fail(runtime::operation_error why) && noexcept { end(why); }
    void fail(std::exception_ptr thrown) && noexcept {
        end(std::nullopt, std::move(thrown));
    }

    // The value will hold `backing` and not what the load was admitted for:
    // a read whose buffers are priced once they are there, or a value that
    // is filled part by part, each part taken before it is copied. See
    // cache_entry::resize().
    [[nodiscard]] runtime::result<void> resize(byte_count backing) noexcept {
        return entry_->resize(backing.value());
    }
    // The entry, held, before its load ends: what keeps a part of the value
    // charged for someone who was given that part early. It has no value
    // until the load publishes one, and none ever if the load fails, so what
    // is handed out gives no way to one.
    [[nodiscard]] cache_hold<Value> hold() const noexcept {
        return cache_hold<Value>{cache_ref<Value>{*entry_}};
    }
    // The load is the cache's own from here on: see
    // cache_entry::fetching().
    void fetched() noexcept { entry_->fetched(); }

private:
    friend class cache_slot<Value>;
    template<typename Key, typename Other>
    friend class cache_index;
    explicit cache_load(detail::cached<Value>& loading) noexcept
      : entry_(&loading) {
        loading.hold();
    }
    void end(
      std::optional<runtime::operation_error> failed,
      std::exception_ptr thrown = {}) noexcept {
        KWAQUE_INVARIANT(
          invariant_id{"KQ-CACHE-LOAD-LIVE"},
          entry_ != nullptr,
          "a cache load ended twice");
        auto* loaded = std::exchange(entry_, nullptr);
        loaded->settle(failed, std::move(thrown));
        loaded->release();
    }
    detail::cached<Value>* entry_;
};

// A bounded shard-local cache of immutable values, each loaded once.
//
// A value belongs to one object and is found through that object's own
// index, a cache_slot or a cache_index, and through nothing else: the cache
// keeps no map of its own, so what another object cached is never reached.
// The cache is what every such index charges.
//
// The cache is admitted once, for its capacity, and counts bytes inside it.
// An entry's whole cost, the bytes its value will hold and the entry itself,
// is taken before its load starts. It is given back when the entry is out of
// its index and its last holder has let go. Nothing waits for bytes. When
// there is no room the cache evicts, oldest first, what was not used again
// and is not held; a load there is still no room for is refused, and its
// reader then loads under a reservation of its own. A value whose readers
// were announced is kept for those still to come, and goes before the
// others once they all have.
//
// A load a reader asks for is the cache's own work. It takes one of a bounded
// number of slots, runs in the scheduling group of the budget, and ends
// whether or not the reader that asked is still there. A reader waits for a
// slot and for a load, and may leave either wait.
//
// stop() is joined before the cache is destroyed. `budget` outlives the
// cache. Entries, loads and those holding them may outlive it: what they are
// charged is given back as each lets go.
class page_cache final : public runtime::shard_affine {
public:
    [[nodiscard]] static runtime::result<std::unique_ptr<page_cache>>
    make(workload_budget& budget, page_cache_config config);
    page_cache(const page_cache&) = delete;
    page_cache& operator=(const page_cache&) = delete;
    ~page_cache();

    [[nodiscard]] page_cache_snapshot snapshot() const noexcept;
    // What the cache is charged for one allocation of `requested` bytes: what
    // a value's owner adds up, for the memory its value will hold, before it
    // asks for a load.
    [[nodiscard]] runtime::result<byte_count>
    allocation_charge(byte_count requested) const noexcept {
        assert_current();
        return budget_.allocation_charge(requested);
    }

    // Refuses whatever comes next, ends the wait of every reader waiting for
    // a slot, joins the loads in flight, each of which tells its waiters,
    // and lets every value go, in bounded steps. A value something still
    // holds lives on with its charge until it is released. Called once.
    [[nodiscard]] seastar::future<> stop();

private:
    template<typename Value>
    friend class cache_slot;
    template<typename Key, typename Value>
    friend class cache_index;
    using slot = seastar::semaphore_units<>;
    page_cache(
      workload_budget& budget,
      seastar::lw_shared_ptr<detail::cache_account> account,
      std::uint32_t slots);

    // Takes the cost of an entry of `entry_bytes` whose value will hold
    // `backing`, evicting for it. With `funding`, the cache is charged
    // nothing and that reservation must be able to hold it all. Refused with
    // queue_full when no room can be made now, and with out_of_range for
    // more than the cache keeps in one value.
    [[nodiscard]] runtime::result<detail::cache_admission> admit(
      byte_count backing,
      std::size_t entry_bytes,
      std::optional<workload_reservation> funding,
      std::uint32_t expected,
      const cache_hook* remembered);

    [[nodiscard]] std::optional<slot> try_slot() noexcept {
        if (stopped_) return std::nullopt;
        return seastar::try_get_units(slots_, 1);
    }
    [[nodiscard]] seastar::future<runtime::result<slot>>
    wait_slot(seastar::abort_source& caller);

    // The value at `place`, held: from the load in flight, or from a load
    // this starts as the cache's own.
    template<typename Place, typename Loader>
    seastar::future<runtime::result<cache_ref<typename Place::value_type>>>
    fetch(
      Place place,
      byte_count backing,
      Loader loader,
      seastar::abort_source& caller,
      const workload_reservation* funding,
      std::uint32_t readers) {
        using output = runtime::result<cache_ref<typename Place::value_type>>;
        assert_current();
        if (!place.loading()) {
            // Joined by stop() from here until the load is started or the
            // caller is turned away: a caller waiting for a slot is still
            // in the cache. Not while it waits for a load, which is joined
            // through the load.
            auto entered = loads_.try_hold();
            if (!entered)
                co_return output{
                  runtime::failure(detail::cache_error(errc::closed))};
            auto taken = try_slot();
            if (!taken) {
                auto waited = co_await wait_slot(caller);
                if (!waited) co_return output{runtime::failure(waited.error())};
                taken.emplace(std::move(*waited));
                // Whoever was let in meanwhile may have loaded it.
                if (auto hit = place.get()) co_return output{std::move(hit)};
            }
            if (!place.loading()) {
                auto led = place.lead(*this, backing, std::nullopt, readers);
                if (
                  !led && funding != nullptr
                  && (led.error().code() == errc::queue_full || led.error().code() == errc::out_of_range))
                    led = place.lead(*this, backing, funding->share(), 0);
                if (!led) co_return output{runtime::failure(led.error())};
                // Listed before the load starts: a load that ends at once
                // has told its waiters by the time it returns.
                auto waiting = place.wait(caller);
                start(std::move(*led), std::move(loader), std::move(*taken));
                entered.reset();
                co_return co_await std::move(waiting);
            }
            // The slot goes back here: another caller's load is in flight.
        }
        // Its owner's own load, begun before this caller came or while it
        // waited for a slot, is not one to wait for: there is no room for
        // the value as the cache's own.
        if (!place.fetching())
            co_return output{
              runtime::failure(detail::cache_error(errc::queue_full))};
        co_return co_await place.join(caller);
    }

    // The load of the value at `place`, started for nobody: see
    // cache_index::prefetch().
    template<typename Place, typename Loader>
    bool prefetch(
      Place place, byte_count backing, Loader loader, std::uint32_t readers) {
        assert_current();
        if (place.ready() || place.loading()) return false;
        auto taken = try_slot();
        if (!taken) return false;
        auto led = place.lead(*this, backing, std::nullopt, readers);
        if (!led) return false;
        start(std::move(*led), std::move(loader), std::move(*taken));
        return true;
    }

    // Starts `load` as the cache's own work. A load that cannot start has
    // failed, and its waiters are told.
    template<typename Value, typename Loader>
    void start(cache_load<Value>&& load, Loader loader, slot&& taken) {
        load.fetched();
        auto running = loads_.try_hold();
        if (!running) {
            std::move(load).fail(detail::cache_error(errc::closed));
            return;
        }
        try {
            // It runs to its first wait here, and by then holds all three.
            static_cast<void>(run<Value, Loader>(
              std::move(load),
              std::move(loader),
              std::move(taken),
              std::move(*running)));
        } catch (const std::bad_alloc&) {
            // No frame was made, so nothing was taken over.
            std::move(load).fail(detail::cache_error(errc::resource_exhausted));
        }
    }
    template<typename Value, typename Loader>
    seastar::future<> run(
      cache_load<Value>&& led,
      std::type_identity_t<Loader>&& loading,
      slot&& taken,
      seastar::gate::holder running) {
        // Taken over before anything suspends: whoever started this keeps
        // none of it, and may be gone when it ends.
        auto load = std::move(led);
        auto loader = std::move(loading);
        const auto held = std::move(taken);
        const auto joined = std::move(running);
        auto loaded = co_await seastar::coroutine::as_future(
          seastar::with_scheduling_group(
            group_, [this, &loader] { return loader(stop_); }));
        // A loader that throws has failed like any other: an exception of
        // an expected family as its code, and any other as itself.
        if (loaded.failed()) {
            auto thrown = loaded.get_exception();
            if (const auto code = detail::recognized(thrown))
                std::move(load).fail(detail::cache_error(code->code()));
            else
                std::move(load).fail(std::move(thrown));
            co_return;
        }
        auto value = loaded.get();
        if (!value) {
            std::move(load).fail(value.error());
            co_return;
        }
        if constexpr (detail::priced_value<Value>) {
            if (auto sized = load.resize(value->charge()); !sized) {
                std::move(load).fail(sized.error());
                co_return;
            }
        }
        std::move(load).publish(std::move(*value));
    }

    workload_budget& budget_;
    seastar::lw_shared_ptr<detail::cache_account> account_;
    seastar::scheduling_group group_;
    seastar::semaphore slots_;
    seastar::gate loads_;
    // Requested at stop, and by nothing else: a load is no caller's to end.
    seastar::abort_source stop_;
    bool stopped_{false};
};

// The one cached value of an object that has one. The slot stays where it is
// while it has an entry.
//
// It is empty, loading or ready. fetch() is what a reader calls: it answers
// from the slot, from the load in flight, or from a load the cache starts.
// lead() and join() are its two halves, for an owner that does the loading
// itself. A load that fails leaves the slot empty, and the next request
// loads again. Destroying the slot lets its entry go: a value something still
// holds lives on with its charge until it is released, and a load in flight
// still ends for those waiting on it, with a value nothing keeps.
template<typename Value>
class cache_slot final {
public:
    cache_slot() noexcept = default;
    cache_slot(const cache_slot&) = delete;
    cache_slot& operator=(const cache_slot&) = delete;
    ~cache_slot() { drop(); }

    // Whether there is neither a value nor a load.
    [[nodiscard]] bool empty() const noexcept {
        return entry_ == nullptr || (!entry_->loading() && !entry_->value);
    }
    [[nodiscard]] bool loading() const noexcept {
        return entry_ && entry_->loading();
    }
    // The value, held, when it is ready. This is a use of it.
    [[nodiscard]] cache_ref<Value> get() noexcept {
        if (entry_ == nullptr || !entry_->value) return {};
        entry_->hook.touch();
        return cache_ref<Value>{*entry_};
    }

    // The value, held: at once when it is ready, and otherwise after its
    // load, which the cache starts if none is in flight. `loader` loads a
    // value that holds `backing`, and is not called when another load
    // answers. It owns whatever it uses: its load may still be running when
    // this caller has left. The slot outlives the call. `caller` releases this
    // caller alone, with `aborted`: a load goes on for the others and for the
    // cache. A value the cache has no room for is loaded all the same when
    // `funding` can hold it: it is shared like any other, charged to that
    // reservation until its last holder lets go, and never kept. `readers`
    // is as for lead(), for a load this call starts.
    template<typename Loader>
    requires detail::cache_loader<Loader, Value>
    [[nodiscard]] seastar::future<runtime::result<cache_ref<Value>>> fetch(
      page_cache& cache,
      byte_count backing,
      Loader loader,
      seastar::abort_source& caller,
      const workload_reservation* funding = nullptr,
      std::uint32_t readers = 0) {
        using output = runtime::result<cache_ref<Value>>;
        if (auto hit = get())
            return seastar::make_ready_future<output>(std::move(hit));
        if (caller.abort_requested())
            return seastar::make_ready_future<output>(
              runtime::failure(detail::cache_error(errc::aborted)));
        return cache.fetch(
          place{*this}, backing, std::move(loader), caller, funding, readers);
    }

    // Starts the load of an empty slot, for a value that will hold `backing`.
    // The cost is taken here, before any work, and a refusal changes
    // nothing. `funding` is the reservation of a reader the cache refused.
    //
    // `readers` is how many reads of the value are known to be still to
    // come, the read of whoever asks for the load not among them: what its
    // writer knows of those reading behind it, or a reader of those behind
    // itself. The value is kept for them. Each of them, as it comes, is
    // taken off, and when all have come the value goes before one that is
    // still waited for. With none known, a value is kept for as long as its
    // use earns it.
    [[nodiscard]] runtime::result<cache_load<Value>> lead(
      page_cache& cache,
      byte_count backing,
      std::optional<workload_reservation> funding = std::nullopt,
      std::uint32_t readers = 0) {
        std::optional<cache_hook> remembered;
        if (entry_) {
            if (entry_->loading() || entry_->value)
                return runtime::failure(
                  detail::cache_error(errc::already_exists));
            // What it remembers passes to the entry that replaces it.
            remembered.emplace(entry_->hook);
            entry_->retire();
        }
        auto admitted = cache.admit(
          backing,
          sizeof(entry),
          std::move(funding),
          readers,
          remembered ? &*remembered : nullptr);
        if (!admitted) return runtime::failure(admitted.error());
        entry* made = nullptr;
        try {
            made = new entry{std::move(*admitted), entry_};
        } catch (const std::bad_alloc&) {
            return runtime::failure(
              detail::cache_error(errc::resource_exhausted));
        }
        return cache_load<Value>{*made};
    }

    // The value, held, after the load in flight if there is one: a load
    // takes any number of callers. This is a use of the value. `caller`
    // releases this caller alone, with `aborted`, and the load goes on for
    // the others. Refused with not_found when the slot is empty.
    [[nodiscard]] seastar::future<runtime::result<cache_ref<Value>>>
    join(seastar::abort_source& caller) {
        using output = runtime::result<cache_ref<Value>>;
        const auto refused = [](errc code) {
            return seastar::make_ready_future<output>(
              runtime::failure(detail::cache_error(code)));
        };
        if (empty()) return refused(errc::not_found);
        entry_->hook.touch();
        if (!entry_->loading())
            return seastar::make_ready_future<output>(
              cache_ref<Value>{*entry_});
        if (caller.abort_requested()) return refused(errc::aborted);
        return entry_->join(caller);
    }

    // Lets the entry go, whatever its state.
    void drop() noexcept {
        if (entry_) entry_->retire();
    }

private:
    friend class page_cache;
    using entry = detail::slot_entry<Value>;
    // The slot, as the cache's own load reaches it.
    struct place final {
        using value_type = Value;
        cache_slot& at;
        [[nodiscard]] cache_ref<Value> get() const noexcept { return at.get(); }
        [[nodiscard]] bool loading() const noexcept { return at.loading(); }
        [[nodiscard]] bool fetching() const noexcept {
            return at.entry_ != nullptr && at.entry_->fetching();
        }
        [[nodiscard]] auto join(seastar::abort_source& caller) const {
            return at.join(caller);
        }
        // The wait of the caller that asked for the load: asking is not a
        // second use.
        [[nodiscard]] auto wait(seastar::abort_source& caller) const {
            return at.entry_->join(caller);
        }
        [[nodiscard]] auto lead(
          page_cache& cache,
          byte_count backing,
          std::optional<workload_reservation> funding,
          std::uint32_t readers) const {
            return at.lead(cache, backing, std::move(funding), readers);
        }
    };
    entry* entry_{nullptr};
};

// The cached values of an object that has many, in the order of their keys.
// Each key is a cache_slot: empty, loading or ready, loaded once.
template<typename Key, typename Value>
class cache_index final {
public:
    cache_index() noexcept = default;
    cache_index(const cache_index&) = delete;
    cache_index& operator=(const cache_index&) = delete;
    ~cache_index() { clear(); }

    // Whether it holds no entry, not even one it only remembers.
    [[nodiscard]] bool empty() const noexcept { return entries_.empty(); }
    [[nodiscard]] bool loading(const Key& key) const noexcept {
        const auto found = entries_.find(key);
        return found != entries_.end() && found->loading();
    }
    // Whether `key` holds a value or the load of one. Not a use of either.
    [[nodiscard]] bool holds(const Key& key) const noexcept {
        const auto found = entries_.find(key);
        return found != entries_.end() && (found->loading() || found->value);
    }
    // The value under `key`, held, when it is ready. This is a use of it.
    [[nodiscard]] cache_ref<Value> get(const Key& key) noexcept {
        const auto found = entries_.find(key);
        if (found == entries_.end() || !found->value) return {};
        found->hook.touch();
        return cache_ref<Value>{*found};
    }
    // The value under the greatest key at or below `key`, held, when it is
    // ready. This is a use of it, unless whoever asks says it is not: a
    // reader that will not come back reads what is kept and earns it
    // nothing.
    [[nodiscard]] cache_ref<Value>
    floor(const Key& key, bool use = true) noexcept {
        auto after = entries_.upper_bound(key);
        if (after == entries_.begin()) return {};
        --after;
        if (!after->value) return {};
        if (use) after->hook.touch();
        return cache_ref<Value>{*after};
    }

    // As cache_slot::fetch(), for the key.
    template<typename Loader>
    requires detail::cache_loader<Loader, Value>
    [[nodiscard]] seastar::future<runtime::result<cache_ref<Value>>> fetch(
      page_cache& cache,
      const Key& key,
      byte_count backing,
      Loader loader,
      seastar::abort_source& caller,
      const workload_reservation* funding = nullptr,
      std::uint32_t readers = 0) {
        using output = runtime::result<cache_ref<Value>>;
        if (auto hit = get(key))
            return seastar::make_ready_future<output>(std::move(hit));
        if (caller.abort_requested())
            return seastar::make_ready_future<output>(
              runtime::failure(detail::cache_error(errc::aborted)));
        return cache.fetch(
          place{*this, key},
          backing,
          std::move(loader),
          caller,
          funding,
          readers);
    }

    // Starts the load of the value under `key` for nobody in particular:
    // work a reader expects to want and does not wait for. It takes a load
    // slot and the value's bytes without waiting, and is dropped when
    // either is not to be had at once, or when the key is loading or ready
    // already. True when a load was started; what it loads is an entry like
    // any other, and nobody is owed anything when it is not wanted after
    // all.
    template<typename Loader>
    requires detail::cache_loader<Loader, Value>
    bool prefetch(
      page_cache& cache,
      const Key& key,
      byte_count backing,
      Loader loader,
      std::uint32_t readers = 0) {
        return cache.prefetch(
          place{*this, key}, backing, std::move(loader), readers);
    }

    // As cache_slot::lead(), for the key.
    [[nodiscard]] runtime::result<cache_load<Value>> lead(
      page_cache& cache,
      const Key& key,
      byte_count backing,
      std::optional<workload_reservation> funding = std::nullopt,
      std::uint32_t readers = 0) {
        std::optional<cache_hook> remembered;
        if (const auto found = entries_.find(key); found != entries_.end()) {
            if (found->loading() || found->value)
                return runtime::failure(
                  detail::cache_error(errc::already_exists));
            // What it remembers passes to the entry that replaces it.
            remembered.emplace(found->hook);
            found->retire();
        }
        // Admission evicts, and what it evicts may be this index's own.
        auto admitted = cache.admit(
          backing,
          sizeof(entry),
          std::move(funding),
          readers,
          remembered ? &*remembered : nullptr);
        if (!admitted) return runtime::failure(admitted.error());
        entry* made = nullptr;
        try {
            made = new entry{std::move(*admitted), key};
        } catch (const std::bad_alloc&) {
            return runtime::failure(
              detail::cache_error(errc::resource_exhausted));
        }
        // Found only now: making room may have changed the index.
        typename set::insert_commit_data spot;
        entries_.insert_check(key, spot);
        entries_.insert_commit(*made, spot);
        made->installed();
        return cache_load<Value>{*made};
    }

    // As cache_slot::join(), for the key.
    [[nodiscard]] seastar::future<runtime::result<cache_ref<Value>>>
    join(const Key& key, seastar::abort_source& caller) {
        using output = runtime::result<cache_ref<Value>>;
        const auto refused = [](errc code) {
            return seastar::make_ready_future<output>(
              runtime::failure(detail::cache_error(code)));
        };
        const auto found = entries_.find(key);
        if (found == entries_.end() || (!found->loading() && !found->value))
            return refused(errc::not_found);
        found->hook.touch();
        if (!found->loading())
            return seastar::make_ready_future<output>(cache_ref<Value>{*found});
        if (caller.abort_requested()) return refused(errc::aborted);
        return found->join(caller);
    }

    // Lets the entry under `key` go, if there is one.
    void drop(const Key& key) noexcept {
        const auto found = entries_.find(key);
        if (found != entries_.end()) found->retire();
    }
    // Lets every entry at or above `from` go, and with them the entry below
    // unless `below` says of its value that it ends before `from`. An entry
    // below that has no value yet goes too.
    template<typename Below>
    requires std::is_nothrow_invocable_r_v<bool, Below&, const Value&>
    void truncate(const Key& from, Below below) noexcept {
        auto first = entries_.lower_bound(from);
        if (first != entries_.begin()) {
            const auto before = std::prev(first);
            // rule out if possible, otherwise always be pessimistic
            if (!before->value || !below(*before->value)) first = before;
        }
        while (first != entries_.end()) {
            auto& gone = *first;
            ++first;
            gone.retire();
        }
    }
    // Lets every entry go, in bounded steps: what an object does when it is
    // closed or removed. A value something still holds lives on with its
    // charge until it is released. Nothing is added to the index meanwhile.
    [[nodiscard]] seastar::future<> clear_gently() {
        // It restarts from the beginning of the index at each scheduling
        // point, so that no position in it lives across one.
        while (!entries_.empty()) {
            do {
                entries_.begin()->retire();
            } while (!entries_.empty() && !seastar::need_preempt());
            co_await seastar::coroutine::maybe_yield();
        }
    }
    // Lets every entry go at once.
    void clear() noexcept {
        entries_.clear_and_dispose(
          [](entry* gone) noexcept { gone->retired(); });
    }

private:
    friend class page_cache;
    using entry = detail::keyed_entry<Key, Value>;
    struct key_of final {
        using type = Key;
        const Key& operator()(const entry& held) const noexcept {
            return held.key;
        }
    };
    // Looked into far more often than it changes.
    using set = boost::intrusive::avl_set<
      entry,
      boost::intrusive::
        member_hook<entry, typename entry::tree_hook, &entry::link>,
      boost::intrusive::key_of_value<key_of>,
      boost::intrusive::constant_time_size<false>>;
    // One key of the index, as the cache's own load reaches it: looked up
    // again at each use.
    struct place final {
        using value_type = Value;
        cache_index& at;
        Key key;
        [[nodiscard]] cache_ref<Value> get() const noexcept {
            return at.get(key);
        }
        [[nodiscard]] bool loading() const noexcept { return at.loading(key); }
        [[nodiscard]] bool fetching() const noexcept {
            const auto found = at.entries_.find(key);
            return found != at.entries_.end() && found->fetching();
        }
        // Whether the key holds a value. Not a use of it.
        [[nodiscard]] bool ready() const noexcept {
            const auto found = at.entries_.find(key);
            return found != at.entries_.end() && found->value.has_value();
        }
        [[nodiscard]] auto join(seastar::abort_source& caller) const {
            return at.join(key, caller);
        }
        // The wait of the caller that asked for the load: asking is not a
        // second use.
        [[nodiscard]] auto wait(seastar::abort_source& caller) const {
            return at.entries_.find(key)->join(caller);
        }
        [[nodiscard]] auto lead(
          page_cache& cache,
          byte_count backing,
          std::optional<workload_reservation> funding,
          std::uint32_t readers) const {
            return at.lead(cache, key, backing, std::move(funding), readers);
        }
    };
    set entries_;
};

} // namespace kwaque::storage
