#include "src/storage/cache_policy.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace kwaque::storage::testing {
struct cache_hook_access final {
    static std::optional<std::uint64_t>
    ghost_insertion_time(const cache_hook& hook) {
        return hook.has_ghost_insertion_time_
                 ? std::make_optional(hook.ghost_insertion_time_)
                 : std::nullopt;
    }
    static void set_ghost_insertion_time(
      cache_hook& hook, std::optional<std::uint64_t> at) {
        hook.ghost_insertion_time_ = at.value_or(0);
        hook.has_ghost_insertion_time_ = at.has_value();
    }
    static int freq(const cache_hook& hook) { return hook.freq_; }
    static int expected(const cache_hook& hook) { return hook.expected_; }
    static int turns(const cache_hook& hook) { return hook.turns_; }
};
} // namespace kwaque::storage::testing

namespace {
using namespace kwaque::storage;
using access = kwaque::storage::testing::cache_hook_access;

struct entry {
    cache_hook hook;
    bool may_evict{true};
    bool evicted{false};
    bool forgotten{false};
    std::optional<cache_queue> evicted_from{};
};
struct evict {
    bool operator()(entry& e, cache_queue from) noexcept {
        if (from == cache_queue::ghost) {
            e.forgotten = true;
            return true;
        }
        if (e.may_evict) {
            e.evicted = true;
            e.evicted_from = from;
            return true;
        }
        return false;
    }
};
struct entry_cost {
    std::uint64_t operator()(const entry& /*entry*/) noexcept { return 1; }
};
using policy_type = cache_policy<entry, &entry::hook, evict, entry_cost>;

void touch(entry& e, int times) {
    for (int n = 0; n < times; ++n)
        e.hook.touch();
}
// Puts an entry in the main queue as one that came back while remembered.
void insert_main(policy_type& cache, entry& e) {
    access::set_ghost_insertion_time(e.hook, 0);
    cache.insert(e);
}

class CachePolicyTest : public ::testing::Test {
public:
    static constexpr std::uint64_t cache_size = 10;
    static constexpr std::uint64_t small_size = 2;
    static constexpr std::uint64_t main_size = cache_size - small_size;

    void SetUp() override {
        cache = std::make_unique<policy_type>(policy_type::config{
          .cache_size = cache_size, .small_size = small_size});
    }

    void cache_insert_main(entry& e) const {
        // force entry on ghost queue
        access::set_ghost_insertion_time(e.hook, 0);
        cache->insert(e);
    }
    void cache_insert_small(entry& e) const {
        // force entry not on ghost queue
        access::set_ghost_insertion_time(e.hook, {});
        cache->insert(e);
    }
    // One eviction as a request of its own makes it.
    [[nodiscard]] bool evict_one() const {
        policy_type::effort spent;
        return cache->evict(spent);
    }
    [[nodiscard]] std::uint64_t small_queue_size() const {
        return cache->size().small_queue_size;
    }
    [[nodiscard]] std::uint64_t main_queue_size() const {
        return cache->size().main_queue_size;
    }

    template<typename... Entries>
    void cleanup(Entries&... entries) {
        (cache->remove(entries), ...);
    }
    template<typename Entries>
    void cleanup_all(Entries& entries) {
        for (auto& e : entries)
            cache->remove(e);
    }

    std::unique_ptr<policy_type> cache;
};

TEST_F(CachePolicyTest, DefaultState) {
    EXPECT_EQ(main_queue_size(), 0U);
    EXPECT_EQ(small_queue_size(), 0U);
    EXPECT_EQ(cache->size().ghost_queue_entries, 0U);
    EXPECT_EQ(cache->any(), nullptr);
    EXPECT_FALSE(evict_one());
}

TEST_F(CachePolicyTest, DefaultHookState) {
    entry e;
    EXPECT_TRUE(e.hook.evicted());
    EXPECT_FALSE(cache->ghost_queue_contains(e));

    cleanup(e);
}

TEST_F(CachePolicyTest, HookCopy) {
    entry e0;
    touch(e0, 2);
    cache->insert(e0);
    // after insert since insert will reset
    access::set_ghost_insertion_time(e0.hook, 3);
    EXPECT_EQ(access::freq(e0.hook), 2);
    EXPECT_EQ(access::ghost_insertion_time(e0.hook), 3U);
    EXPECT_FALSE(e0.hook.evicted());

    entry e1{.hook = e0.hook};
    EXPECT_EQ(access::freq(e1.hook), 2);
    EXPECT_EQ(access::ghost_insertion_time(e1.hook), 3U);
    EXPECT_TRUE(e1.hook.evicted());

    // What was expected of an entry is not what is expected of the one that
    // replaces it: its owner says that anew.
    entry e2;
    e2.hook.expect(4);
    entry e3{.hook = e2.hook};
    EXPECT_EQ(access::expected(e2.hook), 4);
    EXPECT_EQ(access::expected(e3.hook), 0);

    cleanup(e0, e1);
}

TEST_F(CachePolicyTest, Insert) {
    entry e0;
    cache_insert_main(e0);
    EXPECT_EQ(main_queue_size(), 1U);
    EXPECT_EQ(small_queue_size(), 0U);
    EXPECT_FALSE(cache->ghost_queue_contains(e0));
    EXPECT_FALSE(e0.hook.evicted());

    entry e1;
    cache_insert_small(e1);
    EXPECT_EQ(small_queue_size(), 1U);
    EXPECT_EQ(main_queue_size(), 1U);
    EXPECT_FALSE(cache->ghost_queue_contains(e1));
    EXPECT_FALSE(e1.hook.evicted());

    cleanup(e0, e1);
}

// An entry with uses still expected waits for them in the main queue.
// Nothing was remembered of it, and nothing is when it leaves.
TEST_F(CachePolicyTest, InsertIntoMainWhenUsesAreExpected) {
    entry e0;
    e0.hook.expect(1);
    cache->insert(e0);
    EXPECT_EQ(main_queue_size(), 1U);
    EXPECT_EQ(small_queue_size(), 0U);
    EXPECT_FALSE(cache->ghost_queue_contains(e0));
    EXPECT_FALSE(e0.hook.evicted());

    // The use it waited for comes, and it leaves at its turn.
    touch(e0, 1);
    EXPECT_TRUE(evict_one());
    EXPECT_EQ(e0.evicted_from, cache_queue::main);
    EXPECT_FALSE(cache->ghost_queue_contains(e0));
    EXPECT_EQ(cache->size().ghost_queue_entries, 0U);

    cleanup(e0);
}

// A use that was expected is taken off what is expected and earns nothing.
// A use nobody announced is counted, as for any entry.
TEST_F(CachePolicyTest, ExpectedUsesEarnNoFurtherTurn) {
    entry announced;
    entry surprised;
    entry behind;
    announced.hook.expect(2);
    surprised.hook.expect(1);
    cache->insert(announced);
    cache->insert(surprised);
    cache_insert_main(behind);
    touch(announced, 2);
    touch(surprised, 3);
    EXPECT_EQ(access::expected(announced.hook), 0);
    EXPECT_EQ(access::freq(announced.hook), 0);
    EXPECT_EQ(access::expected(surprised.hook), 0);
    EXPECT_EQ(access::freq(surprised.hook), 2);

    // Both its uses came: it leaves at its first turn.
    EXPECT_TRUE(evict_one());
    EXPECT_TRUE(announced.evicted);
    // Two more came than were expected: two further turns. The entry behind
    // it leaves first.
    EXPECT_TRUE(evict_one());
    EXPECT_TRUE(behind.evicted);
    EXPECT_FALSE(surprised.evicted);
    EXPECT_EQ(access::freq(surprised.hook), 1);
    // Alone now, it has its second turn in a request that frees nothing.
    EXPECT_FALSE(evict_one());
    EXPECT_FALSE(surprised.evicted);
    EXPECT_TRUE(evict_one());
    EXPECT_TRUE(surprised.evicted);

    cleanup(announced, surprised, behind);
}

// Of three entries, the one nobody used or waits for leaves first, the one
// that was used leaves next, and the one still waited for leaves last.
TEST_F(CachePolicyTest, EntriesAwaitedLeaveLast) {
    entry used;
    entry awaited;
    entry plain;
    awaited.hook.expect(3);
    cache_insert_main(used);
    touch(used, 1);
    cache->insert(awaited);
    cache_insert_main(plain);

    EXPECT_TRUE(evict_one());
    EXPECT_TRUE(plain.evicted);
    EXPECT_FALSE(used.evicted);
    EXPECT_FALSE(awaited.evicted);
    EXPECT_EQ(access::turns(awaited.hook), 1);

    EXPECT_TRUE(evict_one());
    EXPECT_TRUE(used.evicted);
    EXPECT_FALSE(awaited.evicted);

    // Alone, it has one turn a request, as a lone entry that was used has.
    for (int turn = 2; turn <= policy_type::default_expected_turns; ++turn) {
        policy_type::effort spent;
        EXPECT_FALSE(cache->evict(spent));
        EXPECT_EQ(access::turns(awaited.hook), turn);
        EXPECT_EQ(spent.main_turns, 1U);
    }
    // For as long as room is needed: when its turns are spent it leaves,
    // still waited for.
    policy_type::effort spent;
    EXPECT_TRUE(cache->evict(spent));
    EXPECT_TRUE(awaited.evicted);
    EXPECT_EQ(access::expected(awaited.hook), 3);
    EXPECT_EQ(spent.main_turns, 0U);

    cleanup(used, awaited, plain);
}

// An insert evicts nothing: the policy has no size of its own to hold, and
// its owner evicts for room before an entry exists.
TEST_F(CachePolicyTest, InsertEvictsNothing) {
    // fill main queue to capacity
    std::vector<entry> mains(main_size);
    for (auto& e : mains) {
        cache_insert_main(e);
    }

    // fill small queue to capacity
    std::vector<entry> smalls(small_size);
    for (auto& e : smalls) {
        cache->insert(e);
    }

    EXPECT_EQ(small_queue_size() + main_queue_size(), cache_size);

    std::vector<entry> more(4);
    std::uint64_t over = 0;
    for (auto& e : more) {
        cache->insert(e);
        EXPECT_EQ(small_queue_size() + main_queue_size(), cache_size + ++over);
    }
    for (const auto& e : mains) {
        EXPECT_FALSE(e.evicted);
    }
    for (const auto& e : smalls) {
        EXPECT_FALSE(e.evicted);
    }

    cleanup_all(mains);
    cleanup_all(smalls);
    cleanup_all(more);
}

TEST_F(CachePolicyTest, Remove) {
    entry e0;
    cache->remove(e0);
    EXPECT_EQ(main_queue_size(), 0U);
    EXPECT_EQ(small_queue_size(), 0U);

    entry e1;
    cache_insert_main(e0);
    cache_insert_small(e1);
    EXPECT_EQ(main_queue_size(), 1U);
    EXPECT_EQ(small_queue_size(), 1U);

    cache->remove(e0);
    EXPECT_TRUE(e0.hook.evicted());
    EXPECT_EQ(main_queue_size(), 0U);
    EXPECT_EQ(small_queue_size(), 1U);

    cache->remove(e1);
    EXPECT_TRUE(e1.hook.evicted());
    EXPECT_EQ(main_queue_size(), 0U);
    EXPECT_EQ(small_queue_size(), 0U);

    cleanup(e0, e1);
}

TEST_F(CachePolicyTest, EvictMain) {
    entry e0;
    entry e1;
    cache_insert_main(e0);
    cache_insert_main(e1);
    EXPECT_EQ(main_queue_size(), 2U);
    EXPECT_EQ(small_queue_size(), 0U);

    // e0 is first evicted
    EXPECT_TRUE(evict_one());
    EXPECT_TRUE(e0.evicted);
    EXPECT_EQ(e0.evicted_from, cache_queue::main);
    EXPECT_TRUE(e0.hook.evicted());
    EXPECT_FALSE(e1.evicted);
    EXPECT_EQ(main_queue_size(), 1U);
    EXPECT_EQ(small_queue_size(), 0U);

    // then e1
    EXPECT_TRUE(evict_one());
    EXPECT_TRUE(e0.evicted);
    EXPECT_TRUE(e1.evicted);
    EXPECT_EQ(main_queue_size(), 0U);
    EXPECT_EQ(small_queue_size(), 0U);

    cleanup(e0, e1);
}

TEST_F(CachePolicyTest, EvictMainPinned) {
    entry e0;
    entry e1;
    entry e2;
    cache_insert_main(e0);
    cache_insert_main(e1);
    cache_insert_main(e2);
    EXPECT_EQ(main_queue_size(), 3U);
    EXPECT_EQ(small_queue_size(), 0U);

    e0.may_evict = false;
    e1.may_evict = false;
    e2.may_evict = false;

    EXPECT_FALSE(evict_one());
    EXPECT_FALSE(evict_one());
    EXPECT_FALSE(evict_one());
    EXPECT_EQ(main_queue_size(), 3U);
    EXPECT_EQ(small_queue_size(), 0U);
    EXPECT_FALSE(e0.evicted);
    EXPECT_FALSE(e1.evicted);
    EXPECT_FALSE(e2.evicted);
    // An entry that refused is in its queue as before.
    EXPECT_FALSE(e0.hook.evicted());

    e1.may_evict = true;
    EXPECT_TRUE(evict_one());
    EXPECT_FALSE(evict_one());
    EXPECT_FALSE(evict_one());
    EXPECT_EQ(main_queue_size(), 2U);
    EXPECT_EQ(small_queue_size(), 0U);
    EXPECT_FALSE(e0.evicted);
    EXPECT_TRUE(e1.evicted);
    EXPECT_FALSE(e2.evicted);

    // Short of a request's bound an entry that refused keeps its place:
    // e0 is still ahead of e2.
    e0.may_evict = true;
    e2.may_evict = true;
    EXPECT_TRUE(evict_one());
    EXPECT_EQ(main_queue_size(), 1U);
    EXPECT_EQ(small_queue_size(), 0U);
    EXPECT_TRUE(e0.evicted);
    EXPECT_TRUE(e1.evicted);
    EXPECT_FALSE(e2.evicted);

    EXPECT_TRUE(evict_one());
    EXPECT_EQ(main_queue_size(), 0U);
    EXPECT_EQ(small_queue_size(), 0U);
    EXPECT_TRUE(e0.evicted);
    EXPECT_TRUE(e1.evicted);
    EXPECT_TRUE(e2.evicted);

    cleanup(e0, e1, e2);
}

TEST_F(CachePolicyTest, EvictMainReinsert) {
    entry e0;
    entry e1;
    cache_insert_main(e0);
    cache_insert_main(e1);
    EXPECT_FALSE(e0.evicted);
    EXPECT_FALSE(e1.evicted);
    EXPECT_EQ(main_queue_size(), 2U);
    EXPECT_EQ(small_queue_size(), 0U);

    // e0 is reinserted instead of evicted
    // and e1 is actually evicted
    touch(e0, 2); // freq = 2
    EXPECT_TRUE(evict_one());
    EXPECT_FALSE(e0.evicted);
    EXPECT_TRUE(e1.evicted);

    // freq was 2 so two promotions allowed
    EXPECT_FALSE(evict_one());
    EXPECT_FALSE(e0.evicted);

    EXPECT_TRUE(evict_one());
    EXPECT_TRUE(e0.evicted);

    EXPECT_EQ(main_queue_size(), 0U);
    EXPECT_EQ(small_queue_size(), 0U);

    cleanup(e0, e1);
}

TEST_F(CachePolicyTest, EvictMainSingleHotEntry) {
    entry e0;
    cache_insert_main(e0);
    EXPECT_EQ(main_queue_size(), 1U);
    EXPECT_EQ(small_queue_size(), 0U);

    // e0 is reinserted instead of evicted
    touch(e0, 1);
    EXPECT_FALSE(evict_one());
    EXPECT_FALSE(e0.evicted);
    EXPECT_EQ(main_queue_size(), 1U);
    EXPECT_EQ(small_queue_size(), 0U);

    // e0 stays in if it remains hot
    touch(e0, 5);
    EXPECT_FALSE(evict_one());
    EXPECT_FALSE(evict_one());
    EXPECT_FALSE(evict_one()); // touched 5x, freq capped at 3
    EXPECT_FALSE(e0.evicted);
    EXPECT_EQ(main_queue_size(), 1U);
    EXPECT_EQ(small_queue_size(), 0U);

    // became cold and evicted
    EXPECT_TRUE(evict_one());
    EXPECT_TRUE(e0.evicted);
    EXPECT_EQ(main_queue_size(), 0U);
    EXPECT_EQ(small_queue_size(), 0U);

    cleanup(e0);
}

TEST_F(CachePolicyTest, EvictSmall) {
    entry e0;
    entry e1;
    entry e2;
    entry e3;
    cache->insert(e0);
    cache->insert(e1);
    cache->insert(e2);
    cache->insert(e3);
    EXPECT_EQ(small_queue_size(), 4U);
    EXPECT_EQ(main_queue_size(), 0U);

    EXPECT_FALSE(cache->ghost_queue_contains(e0));
    EXPECT_FALSE(cache->ghost_queue_contains(e1));
    EXPECT_FALSE(cache->ghost_queue_contains(e2));
    EXPECT_FALSE(cache->ghost_queue_contains(e3));

    // e0 is first evicted
    EXPECT_TRUE(evict_one());
    EXPECT_TRUE(e0.evicted);
    EXPECT_EQ(e0.evicted_from, cache_queue::small);
    EXPECT_TRUE(cache->ghost_queue_contains(e0));
    EXPECT_FALSE(e1.evicted);
    EXPECT_FALSE(e2.evicted);
    EXPECT_FALSE(e3.evicted);
    EXPECT_EQ(small_queue_size(), 3U);
    EXPECT_EQ(main_queue_size(), 0U);
    EXPECT_EQ(cache->size().ghost_queue_entries, 1U);

    // then e1
    EXPECT_TRUE(evict_one());
    EXPECT_TRUE(e0.evicted);
    EXPECT_TRUE(cache->ghost_queue_contains(e0));
    EXPECT_TRUE(e1.evicted);
    EXPECT_TRUE(cache->ghost_queue_contains(e1));
    EXPECT_FALSE(e2.evicted);
    EXPECT_FALSE(e3.evicted);
    EXPECT_EQ(small_queue_size(), 2U);
    EXPECT_EQ(main_queue_size(), 0U);
    EXPECT_EQ(cache->size().ghost_queue_entries, 2U);

    cleanup(e0, e1, e2, e3);
    EXPECT_EQ(cache->size().ghost_queue_entries, 0U);
}

TEST_F(CachePolicyTest, EvictSmallPinned) {
    entry e0;
    entry e1;
    entry e2;
    entry e3;
    entry e4;
    cache->insert(e0);
    cache->insert(e1);
    cache->insert(e2);
    cache->insert(e3);
    cache->insert(e4);
    EXPECT_EQ(small_queue_size(), 5U);
    EXPECT_EQ(main_queue_size(), 0U);

    EXPECT_FALSE(e0.hook.evicted());
    EXPECT_FALSE(e1.hook.evicted());
    EXPECT_FALSE(e2.hook.evicted());
    EXPECT_FALSE(e3.hook.evicted());
    EXPECT_FALSE(e4.hook.evicted());

    e0.may_evict = false;
    e1.may_evict = false;
    e2.may_evict = false;
    e3.may_evict = false;
    e4.may_evict = false;

    EXPECT_FALSE(evict_one());
    EXPECT_FALSE(evict_one());
    EXPECT_FALSE(evict_one());
    EXPECT_FALSE(evict_one());
    EXPECT_FALSE(evict_one());
    EXPECT_EQ(small_queue_size(), 5U);
    EXPECT_EQ(main_queue_size(), 0U);
    EXPECT_FALSE(e0.evicted);
    EXPECT_FALSE(e1.evicted);
    EXPECT_FALSE(e2.evicted);
    EXPECT_FALSE(e3.evicted);
    EXPECT_FALSE(e4.evicted);

    e1.may_evict = true;
    EXPECT_TRUE(evict_one());
    EXPECT_FALSE(evict_one());
    EXPECT_FALSE(evict_one());
    EXPECT_EQ(small_queue_size(), 4U);
    EXPECT_EQ(main_queue_size(), 0U);
    EXPECT_FALSE(e0.evicted);
    EXPECT_TRUE(e1.evicted);
    EXPECT_TRUE(e1.hook.evicted());
    EXPECT_TRUE(cache->ghost_queue_contains(e1));
    EXPECT_FALSE(e2.evicted);
    EXPECT_FALSE(e3.evicted);
    EXPECT_FALSE(e4.evicted);

    e0.may_evict = true;
    e2.may_evict = true;
    EXPECT_TRUE(evict_one());
    EXPECT_EQ(small_queue_size(), 3U);
    EXPECT_EQ(main_queue_size(), 0U);
    EXPECT_TRUE(e0.evicted);
    EXPECT_TRUE(e0.hook.evicted());
    EXPECT_TRUE(cache->ghost_queue_contains(e0));
    EXPECT_TRUE(e1.evicted);
    EXPECT_TRUE(e1.hook.evicted());
    EXPECT_TRUE(cache->ghost_queue_contains(e1));
    EXPECT_FALSE(e2.evicted);
    EXPECT_FALSE(e3.evicted);
    EXPECT_FALSE(e4.evicted);

    EXPECT_TRUE(evict_one());
    EXPECT_EQ(small_queue_size(), 2U);
    EXPECT_EQ(main_queue_size(), 0U);
    EXPECT_TRUE(e0.evicted);
    EXPECT_TRUE(e0.hook.evicted());
    EXPECT_TRUE(cache->ghost_queue_contains(e0));
    EXPECT_TRUE(e1.evicted);
    EXPECT_TRUE(e1.hook.evicted());
    EXPECT_TRUE(cache->ghost_queue_contains(e1));
    EXPECT_TRUE(e2.evicted);
    EXPECT_TRUE(e2.hook.evicted());
    EXPECT_TRUE(cache->ghost_queue_contains(e2));
    EXPECT_FALSE(e3.evicted);
    EXPECT_FALSE(e4.evicted);

    cleanup(e0, e1, e2, e3, e4);
}

TEST_F(CachePolicyTest, EvictSmallWarm) {
    entry e0;
    entry e1;
    entry e2;
    cache->insert(e0);
    cache->insert(e1);
    cache->insert(e2);
    EXPECT_EQ(small_queue_size(), 3U);
    EXPECT_EQ(main_queue_size(), 0U);

    // promotion from small to main queue requires more than one re-access. so
    // for a single re-access we expect the entry to be evicted.
    touch(e0, 1);
    EXPECT_TRUE(evict_one());
    EXPECT_TRUE(e0.evicted);
    EXPECT_TRUE(e0.hook.evicted());
    EXPECT_FALSE(e1.evicted);
    EXPECT_FALSE(e2.evicted);
    EXPECT_EQ(small_queue_size(), 2U);
    EXPECT_EQ(main_queue_size(), 0U);

    cleanup(e0, e1, e2);
}

TEST_F(CachePolicyTest, EvictSmallHot) {
    entry e0;
    entry e1;
    entry e2;
    entry e3;
    cache->insert(e0);
    cache->insert(e1);
    cache->insert(e2);
    cache->insert(e3);
    EXPECT_EQ(small_queue_size(), 4U);
    EXPECT_EQ(main_queue_size(), 0U);

    // hot items in small queue are evicted to main
    touch(e0, 2);
    EXPECT_TRUE(evict_one());
    EXPECT_FALSE(e0.evicted);
    EXPECT_FALSE(e0.hook.evicted());
    EXPECT_EQ(access::freq(e0.hook), 0);
    EXPECT_EQ(small_queue_size(), 2U);
    EXPECT_EQ(main_queue_size(), 1U);

    // e0 was evicted to main, but evict() will try to actually evict some data
    // so the next entry considered is e1 which is evicted
    EXPECT_TRUE(e1.evicted);
    EXPECT_TRUE(e1.hook.evicted());

    cleanup(e0, e1, e2, e3);
}

TEST_F(CachePolicyTest, EvictSmallToFullMain) {
    // fill main queue to capacity
    std::vector<entry> mains(main_size);
    for (auto& e : mains) {
        cache_insert_main(e);
    }
    EXPECT_EQ(main_queue_size(), main_size);

    // fill small queue right past capacity
    std::vector<entry> smalls(small_size + 1);
    for (auto& e : smalls) {
        cache->insert(e);
    }
    EXPECT_EQ(small_queue_size(), small_size + 1);

    // the small queue is over capacity, so it will be evicted, but since it is
    // a hot item we'll evict it to the main queue.
    touch(smalls[0], 2);
    touch(mains[0], 1);
    EXPECT_TRUE(evict_one());
    EXPECT_FALSE(smalls[0].evicted);
    EXPECT_FALSE(smalls[0].hook.evicted());
    EXPECT_EQ(small_queue_size(), small_size);

    // but since main is at capacity, this will trigger main eviction. the first
    // main entry is promoted, so the second is evicted. main queue size remains
    // unchanged--it added one and evicted one.
    EXPECT_FALSE(mains[0].evicted);
    EXPECT_FALSE(mains[0].hook.evicted());
    EXPECT_TRUE(mains[1].evicted);
    EXPECT_TRUE(mains[1].hook.evicted());
    EXPECT_EQ(main_queue_size(), main_size);

    cleanup_all(mains);
    cleanup_all(smalls);
}

/*
 * same as EvictSmallToFullMain except none of the entries in main are evictable
 * so we'll fall back to evicting from the small queue.
 */
TEST_F(CachePolicyTest, EvictSmallToFullMainPinned) {
    // fill main queue to capacity
    std::vector<entry> mains(main_size);
    for (auto& e : mains) {
        cache_insert_main(e);
        e.may_evict = false;
    }
    EXPECT_EQ(main_queue_size(), main_size);

    // fill small queue right past capacity
    std::vector<entry> smalls(small_size + 1);
    for (auto& e : smalls) {
        cache->insert(e);
    }
    EXPECT_EQ(small_queue_size(), small_size + 1);

    // the small queue is over capacity, so it will be evicted, but since it is
    // a hot item we'll evict it to the main queue.
    touch(smalls[0], 2);
    smalls[0].may_evict = false;
    EXPECT_TRUE(evict_one());
    EXPECT_FALSE(smalls[0].evicted);
    EXPECT_FALSE(smalls[0].hook.evicted());

    // but since main is at capacity, this will trigger main eviction. but none
    // of the entries in main are evictable (including the small that was just
    // promoted to main). so we'll fall back to small queue eviction.
    EXPECT_EQ(small_queue_size(), small_size - 1);
    EXPECT_EQ(main_queue_size(), main_size + 1);
    for (const auto& e : mains) {
        EXPECT_FALSE(e.evicted);
        EXPECT_FALSE(e.hook.evicted());
    }
    EXPECT_TRUE(smalls[1].evicted);
    EXPECT_TRUE(smalls[1].hook.evicted());

    cleanup_all(mains);
    cleanup_all(smalls);
}

TEST_F(CachePolicyTest, NoEvictable) {
    // fill main queue to capacity
    std::vector<entry> mains(main_size);
    for (auto& e : mains) {
        cache_insert_main(e);
    }

    for (auto& e : mains) {
        e.may_evict = false;
    }

    EXPECT_FALSE(evict_one());

    // fill small queue to capacity
    std::vector<entry> smalls(small_size + 1);
    for (auto& e : smalls) {
        cache->insert(e);
    }

    for (auto& e : smalls) {
        e.may_evict = false;
    }

    EXPECT_FALSE(evict_one());

    EXPECT_EQ(small_queue_size() + main_queue_size(), cache_size + 1);

    cleanup_all(mains);
    cleanup_all(smalls);
}

// Where a cache that may exceed its size would exceed it, the other queue
// is asked: the owner of this one may not.
TEST_F(CachePolicyTest, EvictFromTheOtherQueueWhenOneGivesNothing) {
    {
        // The small queue is over its share and wholly in use.
        std::vector<entry> smalls(small_size + 1);
        for (auto& e : smalls) {
            cache->insert(e);
            e.may_evict = false;
        }
        entry kept;
        cache_insert_main(kept);
        EXPECT_TRUE(evict_one());
        EXPECT_TRUE(kept.evicted);
        EXPECT_EQ(kept.evicted_from, cache_queue::main);
        EXPECT_EQ(small_queue_size(), small_size + 1);
        cleanup_all(smalls);
        cleanup(kept);
    }
    {
        // The small queue is within its share, and the main queue is wholly
        // in use.
        std::vector<entry> mains(3);
        for (auto& e : mains) {
            cache_insert_main(e);
            e.may_evict = false;
        }
        entry waiting;
        cache->insert(waiting);
        EXPECT_LE(small_queue_size(), small_size);
        EXPECT_TRUE(evict_one());
        EXPECT_TRUE(waiting.evicted);
        EXPECT_EQ(waiting.evicted_from, cache_queue::small);
        EXPECT_EQ(main_queue_size(), 3U);
        cleanup_all(mains);
        cleanup(waiting);
    }
}

// The ghost queue remembers an entry until as much has left the small queue
// since as the main queue holds.
TEST_F(CachePolicyTest, GhostQueueRemembersForAMainQueueOfEvictions) {
    std::vector<entry> smalls(main_size + small_size + 1);
    for (auto& e : smalls) {
        cache->insert(e);
    }
    // smalls[0] is remembered from here, and each eviction ages it by one.
    for (std::size_t n = 0; n < main_size - 1; ++n) {
        EXPECT_TRUE(evict_one());
        EXPECT_TRUE(smalls[n].evicted);
        EXPECT_TRUE(cache->ghost_queue_contains(smalls[0]));
    }
    EXPECT_EQ(cache->size().ghost_queue_entries, main_size - 1);
    {
        // Nothing has expired yet, so nothing is forgotten.
        policy_type::effort spent;
        cache->forget_expired(spent);
        EXPECT_EQ(spent.forgotten, 0U);
        EXPECT_FALSE(smalls[0].forgotten);
    }
    EXPECT_TRUE(evict_one());
    EXPECT_FALSE(cache->ghost_queue_contains(smalls[0]));
    EXPECT_TRUE(cache->ghost_queue_contains(smalls[1]));
    {
        // The oldest has, and only it: the queue is in the order of entry.
        policy_type::effort spent;
        cache->forget_expired(spent);
        EXPECT_EQ(spent.forgotten, 1U);
        EXPECT_EQ(spent.freed, 0U);
        EXPECT_TRUE(smalls[0].forgotten);
        EXPECT_FALSE(smalls[1].forgotten);
        EXPECT_EQ(cache->size().ghost_queue_entries, main_size - 1);
    }
    // A forgotten entry that comes back is new again.
    entry again{.hook = smalls[0].hook};
    cache->insert(again);
    EXPECT_EQ(small_queue_size(), small_size + 2);
    EXPECT_EQ(main_queue_size(), 0U);

    cleanup_all(smalls);
    cleanup(again);
}

// An entry that replaces one still remembered enters the main queue.
TEST_F(CachePolicyTest, GhostToMain) {
    std::vector<entry> smalls(small_size + 2);
    for (auto& e : smalls) {
        cache->insert(e);
    }
    EXPECT_TRUE(evict_one());
    EXPECT_TRUE(smalls[0].evicted);
    EXPECT_TRUE(cache->ghost_queue_contains(smalls[0]));
    EXPECT_EQ(cache->size().ghost_queue_entries, 1U);

    // Its owner replaces the remembered entry: the hook is what passes on.
    entry again{.hook = smalls[0].hook};
    cache->remove(smalls[0]);
    EXPECT_EQ(cache->size().ghost_queue_entries, 0U);
    EXPECT_TRUE(cache->ghost_queue_contains(again));
    cache->insert(again);
    EXPECT_EQ(main_queue_size(), 1U);
    EXPECT_EQ(small_queue_size(), small_size + 1);
    EXPECT_FALSE(cache->ghost_queue_contains(again));
    EXPECT_FALSE(again.hook.evicted());

    cleanup_all(smalls);
    cleanup(again);
}

TEST_F(CachePolicyTest, RemoveFromTheGhostQueue) {
    std::vector<entry> smalls(small_size + 3);
    for (auto& e : smalls) {
        cache->insert(e);
    }
    EXPECT_TRUE(evict_one());
    EXPECT_TRUE(evict_one());
    EXPECT_TRUE(evict_one());
    EXPECT_EQ(cache->size().ghost_queue_entries, 3U);
    // From the middle: the others keep their order.
    cache->remove(smalls[1]);
    EXPECT_EQ(cache->size().ghost_queue_entries, 2U);
    EXPECT_TRUE(smalls[1].hook.evicted());
    {
        policy_type::effort spent;
        EXPECT_TRUE(cache->forget_oldest(spent));
        EXPECT_TRUE(smalls[0].forgotten);
        EXPECT_FALSE(smalls[2].forgotten);
        EXPECT_TRUE(cache->forget_oldest(spent));
        EXPECT_TRUE(smalls[2].forgotten);
        EXPECT_FALSE(cache->forget_oldest(spent));
        EXPECT_FALSE(smalls[1].forgotten);
        EXPECT_EQ(spent.forgotten, 2U);
    }
    EXPECT_EQ(cache->size().ghost_queue_entries, 0U);

    cleanup_all(smalls);
}

TEST_F(CachePolicyTest, AnyEntryUntilNoneIsLeft) {
    entry s;
    entry m;
    entry g0;
    entry g1;
    entry g2;
    // Three leave the small queue and are remembered; one stays.
    cache->insert(g0);
    cache->insert(g1);
    cache->insert(g2);
    cache->insert(s);
    cache_insert_main(m);
    // A fourth entry pushes the small queue over its share for each.
    entry pad;
    cache->insert(pad);
    EXPECT_TRUE(evict_one());
    EXPECT_TRUE(evict_one());
    EXPECT_TRUE(evict_one());
    EXPECT_EQ(cache->size().ghost_queue_entries, 3U);

    std::size_t found = 0;
    while (auto* some = cache->any()) {
        cache->remove(*some);
        ++found;
    }
    EXPECT_EQ(found, 6U);
    EXPECT_EQ(small_queue_size() + main_queue_size(), 0U);
    EXPECT_EQ(cache->size().ghost_queue_entries, 0U);
}

TEST(CachePolicyCustomTest, CustomCost) {
    struct sized {
        cache_hook hook;
        std::string data;
    };
    struct sized_cost {
        std::uint64_t operator()(const sized& e) noexcept {
            return e.data.size();
        }
    };
    struct always {
        bool operator()(sized& /*e*/, cache_queue /*from*/) noexcept {
            return true;
        }
    };
    using sized_policy = cache_policy<sized, &sized::hook, always, sized_cost>;

    sized_policy cache(
      sized_policy::config{.cache_size = 100, .small_size = 5});

    sized e1{.data = "."};
    sized e2{.data = ".."};
    sized e10{.data = ".........."};
    const auto total = [&cache] {
        return cache.size().main_queue_size + cache.size().small_queue_size;
    };

    cache.insert(e1);
    EXPECT_EQ(total(), 1U);
    cache.insert(e2);
    EXPECT_EQ(total(), 3U);
    cache.insert(e10);
    EXPECT_EQ(total(), 13U);

    sized_policy::effort spent;
    EXPECT_TRUE(cache.evict(spent));
    EXPECT_EQ(total(), 12U);

    // The ghost queue ages by what left, not by how many: e1 is remembered
    // until 95 more has left the small queue.
    EXPECT_TRUE(cache.ghost_queue_contains(e1));
    EXPECT_TRUE(cache.evict(spent));
    EXPECT_TRUE(cache.evict(spent));
    EXPECT_EQ(total(), 0U);
    EXPECT_TRUE(cache.ghost_queue_contains(e1));

    cache.remove(e1);
    cache.remove(e2);
    cache.remove(e10);
    EXPECT_EQ(cache.size().ghost_queue_entries, 0U);
}

// With every entry of the main queue used since its last turn, a request
// still frees one: it gives a bounded number of further turns and then takes
// the oldest entry whatever uses it has left. No request walks the queue
// three times, and none is refused because what is cached was used.
TEST(CachePolicyEffortTest, UsedMainQueueStillMakesRoom) {
    constexpr std::size_t entries = 100;
    constexpr auto cap = policy_type::default_effort_cap;
    policy_type cache(policy_type::config{.cache_size = 200, .small_size = 20});
    std::vector<entry> mains(entries);
    for (auto& e : mains) {
        insert_main(cache, e);
        touch(e, 4);
        EXPECT_EQ(access::freq(e.hook), 3);
    }
    // Each request gives `cap` entries a turn and takes the one after them.
    for (std::size_t request = 1; request <= 3; ++request) {
        policy_type::effort spent;
        EXPECT_TRUE(cache.evict(spent));
        EXPECT_EQ(spent.main_turns, cap);
        EXPECT_EQ(spent.main_refused, 0U);
        EXPECT_EQ(spent.freed, 1U);
        const auto taken = request * (cap + 1) - 1;
        EXPECT_TRUE(mains[taken].evicted);
        EXPECT_EQ(access::freq(mains[taken].hook), 3);
        EXPECT_EQ(access::freq(mains[taken - 1].hook), 2);
        EXPECT_EQ(cache.size().main_queue_size, entries - request);
    }
    for (auto& e : mains)
        cache.remove(e);
}

// The same when every entry is still waited for: the turns a request gives
// are bounded, and then the oldest goes although its uses have yet to come.
TEST(CachePolicyEffortTest, AwaitedMainQueueStillMakesRoom) {
    constexpr std::size_t entries = 100;
    constexpr auto cap = policy_type::default_effort_cap;
    policy_type cache(policy_type::config{.cache_size = 200, .small_size = 20});
    std::vector<entry> mains(entries);
    for (auto& e : mains) {
        e.hook.expect(1);
        cache.insert(e);
    }
    EXPECT_EQ(cache.size().main_queue_size, entries);

    policy_type::effort spent;
    EXPECT_TRUE(cache.evict(spent));
    EXPECT_EQ(spent.main_turns, cap);
    EXPECT_EQ(spent.freed, 1U);
    EXPECT_TRUE(mains[cap].evicted);
    EXPECT_EQ(access::expected(mains[cap].hook), 1);
    EXPECT_EQ(access::turns(mains[cap - 1].hook), 1);
    EXPECT_EQ(cache.size().main_queue_size, entries - 1);
    for (auto& e : mains)
        cache.remove(e);
}

// An entry is given another turn while uses of it are expected, a bounded
// number of times.
TEST(CachePolicyExpectedTest, AwaitedEntryGetsABoundedNumberOfTurns) {
    // One further turn for an entry that still has expected uses.
    policy_type cache(
      policy_type::config{
        .cache_size = 10, .small_size = 2, .expected_turns = 1});
    entry awaited;
    entry plain;
    awaited.hook.expect(1);
    cache.insert(awaited);
    insert_main(cache, plain);

    // It goes behind the other, which leaves in its place.
    {
        policy_type::effort spent;
        EXPECT_TRUE(cache.evict(spent));
        EXPECT_TRUE(plain.evicted);
        EXPECT_FALSE(awaited.evicted);
        EXPECT_EQ(access::turns(awaited.hook), 1);
        EXPECT_EQ(spent.main_turns, 1U);
    }
    // It has had its turn: still waited for, it leaves.
    {
        policy_type::effort spent;
        EXPECT_TRUE(cache.evict(spent));
        EXPECT_TRUE(awaited.evicted);
        EXPECT_EQ(access::expected(awaited.hook), 1);
        EXPECT_EQ(spent.main_turns, 0U);
    }
    cache.remove(awaited);
    cache.remove(plain);
}

// With no further turn allowed, what is expected of an entry still puts it
// in the main queue and still earns its expected uses nothing.
TEST(CachePolicyExpectedTest, NoTurnsWhenNoneAreAllowed) {
    policy_type cache(
      policy_type::config{
        .cache_size = 10, .small_size = 2, .expected_turns = 0});
    entry awaited;
    entry plain;
    awaited.hook.expect(2);
    cache.insert(awaited);
    insert_main(cache, plain);
    EXPECT_EQ(cache.size().main_queue_size, 2U);

    policy_type::effort spent;
    EXPECT_TRUE(cache.evict(spent));
    EXPECT_TRUE(awaited.evicted);
    EXPECT_FALSE(plain.evicted);
    EXPECT_EQ(spent.main_turns, 0U);
    cache.remove(awaited);
    cache.remove(plain);
}

// What a request has freed buys it more work that frees nothing.
TEST(CachePolicyEffortTest, EffortGrowsWithWhatARequestFrees) {
    constexpr auto cap = policy_type::default_effort_cap;
    policy_type cache(policy_type::config{.cache_size = 200, .small_size = 20});
    entry first;
    std::vector<entry> used(cap + cap / 2);
    entry last;
    insert_main(cache, first);
    for (auto& e : used) {
        insert_main(cache, e);
        touch(e, 2);
    }
    insert_main(cache, last);

    policy_type::effort spent;
    EXPECT_TRUE(cache.evict(spent));
    EXPECT_TRUE(first.evicted);
    // With one entry freed the same request may give twice as many turns,
    // which reaches the unused entry behind them all.
    EXPECT_TRUE(cache.evict(spent));
    EXPECT_TRUE(last.evicted);
    EXPECT_EQ(spent.freed, 2U);
    EXPECT_EQ(spent.main_turns, cap + cap / 2);
    for (const auto& e : used) {
        EXPECT_FALSE(e.evicted);
    }

    for (auto& e : used)
        cache.remove(e);
}

// Entries in use ahead of one that is not: a request that gives up hands
// its place on, so the next reaches what the first did not.
TEST(CachePolicyEffortTest, EntriesInUseDoNotHoldTheQueue) {
    constexpr auto cap = policy_type::default_effort_cap;
    for (const auto queue : {cache_queue::main, cache_queue::small}) {
        policy_type cache(
          policy_type::config{.cache_size = 200, .small_size = 20});
        std::vector<entry> held(cap + 10);
        entry free;
        const auto put = [&cache, queue](entry& e) {
            if (queue == cache_queue::main) {
                insert_main(cache, e);
            } else {
                cache.insert(e);
            }
        };
        for (auto& e : held) {
            put(e);
            e.may_evict = false;
        }
        put(free);
        {
            policy_type::effort spent;
            EXPECT_FALSE(cache.evict(spent));
            EXPECT_FALSE(free.evicted);
            EXPECT_EQ(
              queue == cache_queue::main ? spent.main_refused
                                         : spent.small_wasted,
              cap);
        }
        {
            // The ten it had not reached, and then the free one.
            policy_type::effort spent;
            EXPECT_TRUE(cache.evict(spent));
            EXPECT_TRUE(free.evicted);
            EXPECT_EQ(free.evicted_from, queue);
            EXPECT_EQ(
              queue == cache_queue::main ? spent.main_refused
                                         : spent.small_wasted,
              10U);
        }
        for (auto& e : held)
            cache.remove(e);
        cache.remove(free);
    }
}

// The ghost queue is cleared a bounded number a request.
TEST(CachePolicyEffortTest, ForgettingIsBoundedARequest) {
    constexpr auto cap = policy_type::default_effort_cap;
    constexpr std::size_t entries = 2 * cap + 12;
    // A main queue of two: an entry is remembered for two evictions.
    policy_type cache(policy_type::config{.cache_size = 3, .small_size = 1});
    std::vector<entry> smalls(entries);
    for (auto& e : smalls) {
        cache.insert(e);
    }
    for (std::size_t n = 0; n < entries; ++n) {
        policy_type::effort spent;
        EXPECT_TRUE(cache.evict(spent));
    }
    EXPECT_EQ(cache.size().ghost_queue_entries, entries);
    // An entry is remembered until two more have left: all but the last
    // have expired.
    const auto forgotten = [&smalls] {
        std::size_t count = 0;
        for (const auto& e : smalls)
            count += e.forgotten ? 1U : 0U;
        return count;
    };
    {
        policy_type::effort spent;
        cache.forget_expired(spent);
        EXPECT_EQ(spent.forgotten, cap);
        EXPECT_EQ(forgotten(), cap);
        // Its share is spent for the oldest too.
        EXPECT_FALSE(cache.forget_oldest(spent));
    }
    {
        policy_type::effort spent;
        cache.forget_expired(spent);
        EXPECT_EQ(forgotten(), 2 * cap);
    }
    {
        policy_type::effort spent;
        cache.forget_expired(spent);
        EXPECT_EQ(spent.forgotten, 11U);
        EXPECT_EQ(forgotten(), entries - 1);
        // In order: the youngest is the one still remembered.
        EXPECT_TRUE(cache.ghost_queue_contains(smalls[entries - 1]));
        EXPECT_FALSE(cache.ghost_queue_contains(smalls[entries - 2]));
        EXPECT_FALSE(smalls[entries - 1].forgotten);
        EXPECT_EQ(cache.size().ghost_queue_entries, 1U);
        // It goes when asked, expired or not.
        EXPECT_TRUE(cache.forget_oldest(spent));
        EXPECT_TRUE(smalls[entries - 1].forgotten);
        EXPECT_FALSE(cache.forget_oldest(spent));
    }
    EXPECT_EQ(cache.size().ghost_queue_entries, 0U);
}
} // namespace
