#include "src/base/error.h"
#include "src/base/units.h"
#include "src/bytes/test_allocation_profile.h"
#include "src/resource/resource_registry.h"
#include "src/runtime/error.h"
#include "src/storage/page_cache.h"
#include "src/storage/workload_budget.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/thread.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/alloc_failure_injector.hh>
#include <seastar/util/later.hh>
#include <seastar/util/optimized_optional.hh>

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

namespace {
using namespace kwaque;
using namespace kwaque::storage;
constexpr auto workload = resource::workload_class::metadata;
using value = std::uint64_t;
using value_slot = cache_slot<value>;
using value_index = cache_index<std::uint64_t, value>;
using loaded = runtime::result<value>;
using fetched = runtime::result<cache_ref<value>>;

template<typename Func>
seastar::future<> with_budget(Func body) {
    auto config = resource::resource_config::from_total_memory(
      byte_count{seastar::memory::stats().total_memory()});
    BOOST_REQUIRE(config.has_value());
    resource::resource_registry registry;
    co_await registry.start(*config);
    resource::resource_manager manager{registry.handles()};
    std::exception_ptr failure;
    try {
        co_await manager.start();
        workload_budget budget{
          manager.acquire_workload(workload),
          {.tasks = 8, .bytes = byte_count{4_MiB}, .handles = 1},
          bytes::testing::charge};
        co_await body(budget);
        // Everything a cache was charged went back with its last holder.
        BOOST_CHECK_EQUAL(budget.snapshot().tasks, 0U);
        BOOST_CHECK_EQUAL(budget.snapshot().bytes, 0U);
    } catch (...) {
        failure = std::current_exception();
    }
    co_await manager.stop();
    co_await registry.stop();
    if (failure) std::rethrow_exception(failure);
}

std::unique_ptr<page_cache> make_cache(
  workload_budget& budget,
  std::uint64_t capacity,
  std::uint32_t slots = default_cache_load_slots) {
    auto made = page_cache::make(
      budget,
      page_cache_config{.capacity = byte_count{capacity}, .load_slots = slots});
    BOOST_REQUIRE(made.has_value());
    return std::move(*made);
}

// A cache for `body`, stopped whatever becomes of it. Whatever `body` made
// of the cache's is gone by then.
template<typename Func>
seastar::future<> with_cache(
  Func body,
  std::uint64_t capacity = 1_MiB,
  std::uint32_t slots = default_cache_load_slots) {
    co_await with_budget(
      [&body, capacity, slots](workload_budget& budget) -> seastar::future<> {
          const auto cache = make_cache(budget, capacity, slots);
          std::exception_ptr failure;
          try {
              co_await body(budget, *cache);
          } catch (...) {
              failure = std::current_exception();
          }
          co_await cache->stop();
          BOOST_CHECK_EQUAL(cache->snapshot().bytes, 0U);
          if (failure) std::rethrow_exception(failure);
      });
}

template<typename T>
bool is(const runtime::result<T>& outcome, errc code) {
    return !outcome && outcome.error().code() == code;
}
runtime::operation_error failed(errc code) {
    return runtime::operation_error{code, runtime::operation_kind::file};
}
// Whether a future failed with what a loader threw, as it was thrown.
template<typename T>
bool boom(seastar::future<T>& settled) {
    if (!settled.failed()) return false;
    try {
        std::rethrow_exception(settled.get_exception());
    } catch (const std::runtime_error& thrown) {
        return std::string_view{thrown.what()} == "boom";
    } catch (...) {
        return false;
    }
}
// Loads a value at once.
void fill(
  page_cache& cache,
  value_slot& place,
  byte_count backing,
  value loaded,
  std::uint32_t readers = 0) {
    auto load = place.lead(cache, backing, std::nullopt, readers);
    BOOST_REQUIRE(load.has_value());
    std::move(*load).publish(loaded);
}
void fill(
  page_cache& cache,
  value_index& places,
  std::uint64_t key,
  byte_count backing,
  value loaded) {
    auto load = places.lead(cache, key, backing);
    BOOST_REQUIRE(load.has_value());
    std::move(*load).publish(loaded);
}
// A loader for a fetch that must find its value loaded or loading.
seastar::future<loaded> never_loads(seastar::abort_source&) {
    BOOST_ERROR("a fetch started a second load");
    return seastar::make_ready_future<loaded>(
      runtime::failure(failed(errc::invalid_argument)));
}
// One load in a test's hands: it counts its calls and ends when told.
struct held_load final {
    seastar::promise<loaded> outcome;
    std::uint32_t calls{0};
    auto loader() {
        return [this](seastar::abort_source&) {
            ++calls;
            return outcome.get_future();
        };
    }
};
} // namespace

SEASTAR_TEST_CASE(page_cache_is_admitted_once_for_its_capacity) {
    co_await with_budget([](workload_budget& budget) -> seastar::future<> {
        BOOST_CHECK(is(page_cache::make(budget, {}), errc::invalid_argument));
        // Neither of its two queues is the whole of it.
        BOOST_CHECK(is(
          page_cache::make(
            budget, page_cache_config{.capacity = byte_count{1}}),
          errc::invalid_argument));
        for (const auto slots : {0U, maximum_cache_load_slots + 1})
            BOOST_CHECK(is(
              page_cache::make(
                budget,
                page_cache_config{
                  .capacity = byte_count{1_MiB}, .load_slots = slots}),
              errc::invalid_argument));
        // More than its budget has.
        BOOST_CHECK(is(
          page_cache::make(
            budget, page_cache_config{.capacity = byte_count{64_MiB}}),
          errc::out_of_range));
        BOOST_CHECK_EQUAL(budget.snapshot().tasks, 0U);
        {
            const auto cache = make_cache(budget, 1_MiB);
            BOOST_CHECK_EQUAL(cache->snapshot().capacity, 1_MiB);
            BOOST_CHECK_EQUAL(cache->snapshot().bytes, 0U);
            const auto admitted = budget.snapshot();
            BOOST_CHECK_EQUAL(admitted.tasks, 1U);
            BOOST_CHECK_GE(admitted.bytes, 1_MiB);
            // What it caches is counted inside that one admission.
            value_slot place;
            fill(*cache, place, byte_count{64_KiB}, 1);
            BOOST_CHECK_GT(cache->snapshot().bytes, 64_KiB);
            BOOST_CHECK_EQUAL(budget.snapshot().tasks, admitted.tasks);
            BOOST_CHECK_EQUAL(budget.snapshot().bytes, admitted.bytes);
            co_await cache->stop();
            // Stopped, it holds nothing of its own, and its slot is empty.
            BOOST_CHECK(place.empty());
            BOOST_CHECK_EQUAL(cache->snapshot().bytes, 0U);
        }
    });
}

SEASTAR_TEST_CASE(page_cache_value_is_reached_through_its_own_slot_alone) {
    co_await with_cache(
      [](workload_budget&, page_cache& cache) -> seastar::future<> {
          value_slot first, second;
          BOOST_CHECK(first.empty() && !first.loading() && !first.get());
          {
              auto load = first.lead(cache, byte_count{4_KiB});
              BOOST_REQUIRE(load.has_value());
              BOOST_CHECK(first.loading() && !first.empty());
              // Nothing is borrowed from a load in flight.
              BOOST_CHECK(!first.get());
              std::move(*load).publish(7);
          }
          BOOST_CHECK(!first.loading() && !first.empty());
          // Another object's slot does not reach it.
          BOOST_CHECK(second.empty() && !second.get());
          // A hit holds the one value there is.
          auto held = first.get();
          const auto again = first.get();
          BOOST_REQUIRE(held && again);
          BOOST_CHECK_EQUAL(*held, 7U);
          BOOST_CHECK(&*held == &*again);
          const auto charged = cache.snapshot().bytes;
          {
              // Every copy of a holder holds it.
              const auto copied = held;
              cache_ref<value> assigned;
              assigned = copied;
              auto moved = std::move(assigned);
              held = std::move(moved);
              first.drop();
              BOOST_CHECK(first.empty() && !first.get());
              BOOST_CHECK_EQUAL(*copied, 7U);
          }
          // The slot let it go, and it is still what it was for its holders.
          BOOST_CHECK_EQUAL(*held, 7U);
          BOOST_CHECK(&*held == &*again);
          BOOST_CHECK_EQUAL(cache.snapshot().bytes, charged);
          // A slot that let its value go loads another.
          fill(cache, first, byte_count{4_KiB}, 8);
          BOOST_CHECK_EQUAL(*first.get(), 8U);
          BOOST_CHECK_EQUAL(*held, 7U);
          co_return;
      });
}

SEASTAR_TEST_CASE(page_cache_index_answers_from_the_key_at_or_below) {
    co_await with_cache(
      [](workload_budget&, page_cache& cache) -> seastar::future<> {
          const auto most = std::numeric_limits<std::uint64_t>::max();
          value_index places, other;
          BOOST_CHECK(places.empty() && !places.get(5) && !places.floor(5));
          for (const std::uint64_t key : {20U, 10U, 30U})
              fill(cache, places, key, byte_count{4_KiB}, key * 10);
          BOOST_CHECK(!places.empty());
          // Another object's index holds none of them.
          BOOST_CHECK(other.empty() && !other.floor(most));
          BOOST_CHECK(!places.get(15) && !places.get(0) && !places.get(most));
          BOOST_CHECK_EQUAL(*places.get(20), 200U);
          // Before the first key there is nothing.
          BOOST_CHECK(!places.floor(0) && !places.floor(9));
          struct probe final {
              std::uint64_t key;
              value found;
          };
          for (const auto expected :
               {probe{10, 100},
                probe{11, 100},
                probe{19, 100},
                probe{20, 200},
                probe{29, 200},
                probe{30, 300},
                probe{most, 300}}) {
              const auto found = places.floor(expected.key);
              BOOST_REQUIRE(found);
              BOOST_CHECK_EQUAL(*found, expected.found);
          }
          // One load a key.
          BOOST_CHECK(is(
            places.lead(cache, 20, byte_count{4_KiB}), errc::already_exists));
          {
              // A key still loading answers nothing, and no key below it
              // answers for it.
              auto load = places.lead(cache, 25, byte_count{4_KiB});
              BOOST_REQUIRE(load.has_value());
              BOOST_CHECK(places.loading(25) && !places.loading(20));
              BOOST_CHECK(!places.get(25) && !places.floor(27));
              BOOST_CHECK_EQUAL(*places.floor(24), 200U);
              std::move(*load).publish(250);
          }
          BOOST_CHECK_EQUAL(*places.floor(27), 250U);
          // A key let go of is no longer found, and the one below answers.
          places.drop(20);
          places.drop(21);
          BOOST_CHECK(!places.get(20));
          BOOST_CHECK_EQUAL(*places.floor(24), 100U);
          places.clear();
          BOOST_CHECK(places.empty() && !places.floor(most));
          BOOST_CHECK_EQUAL(cache.snapshot().bytes, 0U);
          co_return;
      });
}

SEASTAR_TEST_CASE(page_cache_index_lets_go_from_a_key_upwards) {
    co_await with_cache(
      [](workload_budget&, page_cache& cache) -> seastar::future<> {
          // A value here is the last offset its entry covers.
          const auto covered = [](value_index& places, std::uint64_t offset) {
              const auto found = places.floor(offset);
              return found && *found >= offset;
          };
          const auto ends_before = [](std::uint64_t from) {
              return [from](const value& last) noexcept { return last < from; };
          };
          {
              // Entries of growing length, one after another.
              value_index places;
              std::vector<std::uint64_t> bases;
              std::uint64_t end = 0;
              for (std::uint64_t length = 1; length < 10; ++length) {
                  fill(cache, places, end, byte_count{1_KiB}, end + length - 1);
                  bases.push_back(end);
                  end += length;
              }
              for (std::uint64_t offset = 0; offset < end; ++offset)
                  BOOST_CHECK(covered(places, offset));
              for (auto from = end + 10; from-- > 0;) {
                  places.truncate(from, ends_before(from));
                  // The entry that holds `from` went with everything above.
                  for (auto offset = from; offset < end; ++offset)
                      BOOST_CHECK(!covered(places, offset));
                  // The entry below stayed if it ends there, which it does
                  // where the next one began.
                  const bool boundary = from == end
                                        || std::find(
                                             bases.begin(), bases.end(), from)
                                             != bases.end();
                  if (from > 0)
                      BOOST_CHECK_EQUAL(covered(places, from - 1), boundary);
              }
              BOOST_CHECK(places.empty());
          }
          {
              // [10][11:20]  [41:50], cut in the hole: what ends before the
              // cut is ruled out, and stays.
              value_index places;
              fill(cache, places, 10, byte_count{1_KiB}, 10);
              fill(cache, places, 11, byte_count{1_KiB}, 20);
              fill(cache, places, 41, byte_count{1_KiB}, 50);
              places.truncate(25, ends_before(25));
              BOOST_CHECK(covered(places, 10) && covered(places, 15));
              BOOST_CHECK(!places.get(41));
          }
          {
              // The same with the entry below the cut still loading: what
              // cannot be ruled out goes.
              value_index places;
              fill(cache, places, 10, byte_count{1_KiB}, 10);
              auto load = places.lead(cache, 11, byte_count{1_KiB});
              BOOST_REQUIRE(load.has_value());
              fill(cache, places, 41, byte_count{1_KiB}, 50);
              places.truncate(25, ends_before(25));
              BOOST_CHECK(covered(places, 10));
              BOOST_CHECK(!places.loading(11) && !places.get(41));
              // Its load ends for nobody, and nothing keeps the value.
              std::move(*load).publish(20);
              BOOST_CHECK(!places.get(11));
          }
          BOOST_CHECK_EQUAL(cache.snapshot().bytes, 0U);
          co_return;
      });
}

SEASTAR_TEST_CASE(page_cache_index_lets_its_entries_go_in_steps) {
    co_await with_cache(
      [](workload_budget&, page_cache& cache) -> seastar::future<> {
          constexpr std::uint64_t entries = 1000;
          seastar::abort_source caller;
          value_index places;
          for (std::uint64_t key = 0; key < entries; ++key)
              fill(cache, places, key, byte_count{512}, key);
          const auto each = cache.snapshot().bytes / entries;
          BOOST_CHECK_EQUAL(cache.snapshot().bytes, each * entries);
          auto first = places.get(0);
          auto last = places.get(entries - 1);
          BOOST_REQUIRE(first && last);
          auto load = places.lead(cache, entries, byte_count{512});
          BOOST_REQUIRE(load.has_value());
          auto waiting = places.join(entries, caller);
          // The object is closed with two values held and a load in flight.
          co_await places.clear_gently();
          BOOST_CHECK(places.empty());
          // What nothing held is given back; what is held is what it was.
          BOOST_CHECK_EQUAL(cache.snapshot().bytes, 3 * each);
          BOOST_CHECK_EQUAL(*first, 0U);
          BOOST_CHECK_EQUAL(*last, entries - 1);
          // The load ends for those waiting on it, and nothing keeps it.
          std::move(*load).publish(entries);
          {
              const auto settled = co_await std::move(waiting);
              BOOST_REQUIRE(settled.has_value());
              BOOST_CHECK_EQUAL(**settled, entries);
          }
          BOOST_CHECK(places.empty());
          first.reset();
          BOOST_CHECK_EQUAL(cache.snapshot().bytes, each);
          last.reset();
          BOOST_CHECK_EQUAL(cache.snapshot().bytes, 0U);
          // It is an index like any other afterwards.
          fill(cache, places, 5, byte_count{512}, 50);
          BOOST_CHECK_EQUAL(*places.get(5), 50U);
      },
      2_MiB);
}

SEASTAR_TEST_CASE(
  page_cache_charges_an_entry_from_its_load_to_its_last_holder) {
    co_await with_cache(
      [](workload_budget&, page_cache& cache) -> seastar::future<> {
          value_slot small, large;
          // The whole cost is taken before the load does anything: the
          // bytes asked for and the entry itself.
          auto loading = small.lead(cache, byte_count{4_KiB});
          BOOST_REQUIRE(loading.has_value());
          const auto resident = cache.snapshot().bytes;
          BOOST_CHECK_GT(resident, 4_KiB);
          // It is the same when the value is there.
          std::move(*loading).publish(1);
          BOOST_CHECK_EQUAL(cache.snapshot().bytes, resident);
          const auto entry = resident - 4_KiB;
          // The charge is the bytes asked for and the same entry each time.
          fill(cache, large, byte_count{64_KiB}, 2);
          BOOST_CHECK_EQUAL(cache.snapshot().bytes, resident + 64_KiB + entry);
          {
              // A value its object let go of stays charged while it is held.
              const auto held = large.get();
              BOOST_REQUIRE(held);
              large.drop();
              BOOST_CHECK(large.empty());
              BOOST_CHECK_EQUAL(
                cache.snapshot().bytes, resident + 64_KiB + entry);
              BOOST_CHECK_EQUAL(*held, 2U);
          }
          // Its last holder gave it back.
          BOOST_CHECK_EQUAL(cache.snapshot().bytes, resident);
          // A value nothing holds goes back when its object lets go.
          small.drop();
          BOOST_CHECK_EQUAL(cache.snapshot().bytes, 0U);
          BOOST_CHECK_EQUAL(cache.snapshot().loads, 2U);
          BOOST_CHECK_EQUAL(cache.snapshot().refused, 0U);
          co_return;
      });
}

SEASTAR_TEST_CASE(page_cache_evicts_what_was_not_used_again_for_room) {
    co_await with_cache(
      [](workload_budget&, page_cache& cache) -> seastar::future<> {
          const auto capacity = cache.snapshot().capacity;
          const byte_count backing{1_KiB};
          std::vector<std::unique_ptr<value_slot>> slots;
          const auto add = [&](value loaded) {
              slots.push_back(std::make_unique<value_slot>());
              fill(cache, *slots.back(), backing, loaded);
          };
          add(0);
          const auto each = cache.snapshot().bytes;
          const auto shell = each - backing.value();
          // As many as fit, each waiting in the small queue.
          while (cache.snapshot().bytes + each <= capacity)
              add(slots.size());
          const auto full = slots.size();
          BOOST_REQUIRE_GE(full, 8U);
          BOOST_CHECK_EQUAL(cache.snapshot().small_bytes, full * each);
          BOOST_CHECK_EQUAL(cache.snapshot().main_bytes, 0U);
          BOOST_CHECK_EQUAL(cache.snapshot().evicted, 0U);
          // The oldest is held, and the next was used twice since.
          const auto held = slots[0]->get();
          BOOST_REQUIRE(held);
          BOOST_CHECK(slots[1]->get() && slots[1]->get());
          const auto before = cache.snapshot().bytes;
          add(100);
          // Room was made from the oldest that was neither: the held value
          // was passed over and the used one went on to the main queue.
          BOOST_CHECK_EQUAL(*held, 0U);
          BOOST_CHECK(!slots[0]->empty());
          BOOST_CHECK(slots[1]->get());
          BOOST_CHECK_EQUAL(cache.snapshot().main_bytes, each);
          BOOST_CHECK(slots[2]->empty() && !slots[2]->get());
          const auto evicted = cache.snapshot().evicted;
          BOOST_CHECK_GE(evicted, 1U);
          BOOST_CHECK_LE(cache.snapshot().bytes, capacity);
          // A value that went is remembered: its entry stays, charged for
          // itself alone.
          BOOST_CHECK_EQUAL(cache.snapshot().remembered, evicted);
          BOOST_CHECK_EQUAL(
            cache.snapshot().bytes, before + each - evicted * (each - shell));
          BOOST_CHECK_EQUAL(*slots.back()->get(), 100U);
          // It comes back while it is remembered, and is taken for a value
          // that is used: it enters the main queue.
          fill(cache, *slots[2], backing, 2);
          BOOST_CHECK_EQUAL(*slots[2]->get(), 2U);
          BOOST_CHECK_EQUAL(cache.snapshot().main_bytes, 2 * each);
          BOOST_CHECK_LE(cache.snapshot().bytes, capacity);
          // Nothing was refused on the way.
          BOOST_CHECK_EQUAL(cache.snapshot().refused, 0U);
          co_return;
      },
      64_KiB);
}

SEASTAR_TEST_CASE(page_cache_refuses_what_it_cannot_make_room_for) {
    co_await with_cache(
      [](workload_budget&, page_cache& cache) -> seastar::future<> {
          const auto capacity = cache.snapshot().capacity;
          const byte_count backing{1_KiB};
          value_slot place;
          // More than its small queue's share is more than it keeps in one
          // value, whatever room there is.
          BOOST_CHECK(is(
            place.lead(cache, byte_count{capacity / 10}), errc::out_of_range));
          BOOST_CHECK(is(
            place.lead(
              cache, byte_count{std::numeric_limits<std::uint64_t>::max()}),
            errc::out_of_range));
          BOOST_CHECK(place.empty());
          BOOST_CHECK_EQUAL(cache.snapshot().bytes, 0U);
          BOOST_CHECK_EQUAL(cache.snapshot().refused, 2U);
          BOOST_CHECK_EQUAL(cache.snapshot().loads, 0U);

          std::vector<std::unique_ptr<value_slot>> slots;
          std::vector<cache_ref<value>> holders;
          slots.push_back(std::make_unique<value_slot>());
          fill(cache, *slots.back(), backing, 0);
          const auto each = cache.snapshot().bytes;
          while (cache.snapshot().bytes + each <= capacity) {
              slots.push_back(std::make_unique<value_slot>());
              fill(cache, *slots.back(), backing, slots.size() - 1);
          }
          for (auto& held : slots)
              holders.push_back(held->get());
          const auto full = cache.snapshot().bytes;
          // Every value is in use: nothing is evicted, the load is refused
          // before any work, and nothing changed.
          BOOST_CHECK(is(place.lead(cache, backing), errc::queue_full));
          BOOST_CHECK(place.empty());
          BOOST_CHECK_EQUAL(cache.snapshot().bytes, full);
          BOOST_CHECK_EQUAL(cache.snapshot().evicted, 0U);
          BOOST_CHECK_EQUAL(cache.snapshot().refused, 3U);
          for (std::size_t n = 0; n < slots.size(); ++n)
              BOOST_CHECK_EQUAL(*holders[n], n);
          // One is let go of, wherever it stands in the queue, and the next
          // load has its room.
          holders.back().reset();
          fill(cache, place, backing, 9);
          BOOST_CHECK_EQUAL(*place.get(), 9U);
          BOOST_CHECK_EQUAL(cache.snapshot().evicted, 1U);
          BOOST_CHECK(slots.back()->empty());
          BOOST_CHECK_LE(cache.snapshot().bytes, capacity);
          co_return;
      },
      64_KiB);
}

SEASTAR_TEST_CASE(page_cache_keeps_a_value_for_the_readers_still_to_come) {
    co_await with_cache(
      [](workload_budget&, page_cache& cache) -> seastar::future<> {
          const auto capacity = cache.snapshot().capacity;
          const byte_count backing{1_KiB};
          std::vector<std::unique_ptr<value_slot>> slots;
          // Written with one reader known to be behind it.
          const auto add = [&](value written) {
              slots.push_back(std::make_unique<value_slot>());
              fill(cache, *slots.back(), backing, written, 1);
          };
          add(0);
          const auto each = cache.snapshot().bytes;
          // A value that is waited for waits in the main queue.
          BOOST_CHECK_EQUAL(cache.snapshot().main_bytes, each);
          BOOST_CHECK_EQUAL(cache.snapshot().small_bytes, 0U);
          while (cache.snapshot().bytes + each <= capacity)
              add(slots.size());
          const auto full = slots.size();
          BOOST_REQUIRE_GE(full, 8U);
          BOOST_CHECK_EQUAL(cache.snapshot().main_bytes, full * each);
          // Its reader has come for every value but the oldest.
          for (std::size_t n = 1; n < full; ++n)
              BOOST_CHECK_EQUAL(*slots[n]->get(), n);
          add(100);
          // Room was made from the oldest value nobody is still to read:
          // the one that is waited for was passed over.
          BOOST_CHECK(!slots[0]->empty());
          BOOST_CHECK(slots[1]->empty());
          BOOST_CHECK(!slots[2]->empty());
          BOOST_CHECK_EQUAL(cache.snapshot().evicted, 1U);
          // What leaves the main queue is not remembered.
          BOOST_CHECK_EQUAL(cache.snapshot().remembered, 0U);
          BOOST_CHECK_EQUAL(cache.snapshot().main_bytes, full * each);
          // Its reader comes, and finds it.
          BOOST_CHECK_EQUAL(*slots[0]->get(), 0U);
          // It has its place in the order like the others from then on: the
          // next room comes from the oldest ahead of it.
          add(101);
          BOOST_CHECK(slots[2]->empty());
          BOOST_CHECK(!slots[0]->empty());
          BOOST_CHECK_EQUAL(cache.snapshot().evicted, 2U);
          BOOST_CHECK_EQUAL(cache.snapshot().refused, 0U);
          // A value nobody was announced for waits in the small queue.
          value_slot read;
          fill(cache, read, backing, 7);
          BOOST_CHECK_EQUAL(cache.snapshot().small_bytes, each);
          co_return;
      },
      64_KiB);
}

SEASTAR_TEST_CASE(page_cache_makes_room_among_values_that_hold_nothing) {
    co_await with_cache(
      [](workload_budget&, page_cache& cache) -> seastar::future<> {
          // A value that holds no bytes frees none when it is evicted: its
          // entry is all it costs. Room is made all the same.
          const auto capacity = cache.snapshot().capacity;
          std::vector<std::unique_ptr<value_slot>> slots;
          for (std::uint64_t n = 0; n < 200; ++n) {
              slots.push_back(std::make_unique<value_slot>());
              fill(cache, *slots.back(), byte_count{}, n);
              BOOST_CHECK_LE(cache.snapshot().bytes, capacity);
          }
          BOOST_CHECK_EQUAL(cache.snapshot().refused, 0U);
          BOOST_CHECK_GE(cache.snapshot().evicted, 100U);
          BOOST_CHECK_EQUAL(*slots.back()->get(), 199U);
          BOOST_CHECK(slots.front()->empty());
          co_return;
      },
      8_KiB);
}

SEASTAR_TEST_CASE(page_cache_load_it_cannot_take_is_shared_under_its_reader) {
    co_await with_cache(
      [](workload_budget& budget, page_cache& cache) -> seastar::future<> {
          value_slot place;
          seastar::abort_source caller;
          BOOST_CHECK(
            is(place.lead(cache, byte_count{64_KiB}), errc::out_of_range));
          {
              // A reservation that cannot hold the load funds none.
              auto narrow = budget.try_reserve(byte_count{4_KiB});
              BOOST_REQUIRE(narrow.has_value());
              BOOST_CHECK(is(
                place.lead(cache, byte_count{64_KiB}, narrow->share()),
                errc::out_of_range));
              BOOST_CHECK(place.empty());
          }
          std::optional<workload_reservation> reader{
            budget.try_reserve(byte_count{256_KiB}).value()};
          auto load = place.lead(cache, byte_count{64_KiB}, reader->share());
          BOOST_REQUIRE(load.has_value());
          // The cache is charged nothing for it.
          BOOST_CHECK_EQUAL(cache.snapshot().bytes, 0U);
          BOOST_CHECK_EQUAL(cache.snapshot().uncached, 1U);
          // It is one load all the same: whoever arrives waits for it.
          BOOST_CHECK(place.loading());
          BOOST_CHECK(is(
            place.lead(cache, byte_count{64_KiB}, reader->share()),
            errc::already_exists));
          auto first = place.join(caller);
          auto second = place.join(caller);
          // Its reader's reservation stands behind it, not the reader.
          reader.reset();
          BOOST_CHECK_EQUAL(budget.snapshot().tasks, 2U);
          std::move(*load).publish(9);
          // It is told to those waiting and kept by nothing.
          BOOST_CHECK(place.empty());
          auto one = co_await std::move(first);
          auto other = co_await std::move(second);
          BOOST_REQUIRE(one.has_value() && other.has_value());
          BOOST_CHECK_EQUAL(**one, 9U);
          BOOST_CHECK(&**one == &**other);
          BOOST_CHECK_EQUAL(cache.snapshot().loads, 1U);
          BOOST_CHECK_EQUAL(cache.snapshot().small_bytes, 0U);
          one->reset();
          BOOST_CHECK_EQUAL(**other, 9U);
          BOOST_CHECK_EQUAL(budget.snapshot().tasks, 2U);
          // The reservation goes back with the last holder.
          other->reset();
          BOOST_CHECK_EQUAL(budget.snapshot().tasks, 1U);
          // The next request loads again.
          BOOST_CHECK(is(co_await place.join(caller), errc::not_found));
      },
      128_KiB);
}

SEASTAR_TEST_CASE(page_cache_load_is_started_once_and_shared) {
    co_await with_cache(
      [](workload_budget&, page_cache& cache) -> seastar::future<> {
          value_index places;
          seastar::abort_source caller;
          // Nothing is there to wait for.
          BOOST_CHECK(is(co_await places.join(3, caller), errc::not_found));
          auto load = places.lead(cache, 3, byte_count{4_KiB});
          BOOST_REQUIRE(load.has_value());
          auto first = places.join(3, caller);
          auto second = places.join(3, caller);
          BOOST_CHECK(!first.available() && !second.available());
          // Whoever arrives during the load finds it and starts no other.
          BOOST_CHECK(
            is(places.lead(cache, 3, byte_count{4_KiB}), errc::already_exists));
          BOOST_CHECK_EQUAL(cache.snapshot().loads, 1U);
          BOOST_CHECK_EQUAL(cache.snapshot().waits, 2U);
          std::move(*load).publish(30);
          const auto one = co_await std::move(first);
          const auto other = co_await std::move(second);
          BOOST_REQUIRE(one.has_value() && other.has_value());
          BOOST_CHECK_EQUAL(**one, 30U);
          // What they hold is the one value, and the one the index answers
          // with from now on.
          BOOST_CHECK(&**one == &**other);
          const auto hit = places.get(3);
          BOOST_REQUIRE(hit);
          BOOST_CHECK(&*hit == &**one);
          // A caller that asks once it is ready waits for nothing.
          auto ready = places.join(3, caller);
          BOOST_CHECK(ready.available());
          const auto late = co_await std::move(ready);
          BOOST_REQUIRE(late.has_value());
          BOOST_CHECK(&**late == &*hit);
          BOOST_CHECK_EQUAL(cache.snapshot().loads, 1U);
          BOOST_CHECK_EQUAL(cache.snapshot().waits, 2U);
      });
}

SEASTAR_TEST_CASE(page_cache_failed_load_is_told_to_everyone_and_not_kept) {
    co_await with_cache(
      [](workload_budget&, page_cache& cache) -> seastar::future<> {
          value_slot place;
          seastar::abort_source caller;
          {
              auto load = place.lead(cache, byte_count{4_KiB});
              BOOST_REQUIRE(load.has_value());
              auto first = place.join(caller);
              auto second = place.join(caller);
              std::move(*load).fail(failed(errc::corrupt_data));
              // The entry left its slot at once.
              BOOST_CHECK(place.empty());
              {
                  const auto settled = co_await std::move(first);
                  BOOST_CHECK(is(settled, errc::corrupt_data));
              }
              {
                  const auto settled = co_await std::move(second);
                  BOOST_CHECK(is(settled, errc::corrupt_data));
              }
          }
          // Nothing remembers the failure, and its bytes are back.
          BOOST_CHECK_EQUAL(cache.snapshot().bytes, 0U);
          BOOST_CHECK_EQUAL(cache.snapshot().small_bytes, 0U);
          BOOST_CHECK(is(co_await place.join(caller), errc::not_found));
          {
              // A load let go of without an outcome has failed like any
              // other.
              auto load = place.lead(cache, byte_count{4_KiB});
              BOOST_REQUIRE(load.has_value());
              auto waiting = place.join(caller);
              [](cache_load<value>) {}(std::move(*load));
              BOOST_CHECK(place.empty());
              {
                  const auto settled = co_await std::move(waiting);
                  BOOST_CHECK(is(settled, errc::closed));
              }
          }
          BOOST_CHECK_EQUAL(cache.snapshot().bytes, 0U);
          // The next request loads again.
          fill(cache, place, byte_count{4_KiB}, 5);
          BOOST_CHECK_EQUAL(*place.get(), 5U);
          BOOST_CHECK_EQUAL(cache.snapshot().loads, 3U);
      });
}

SEASTAR_TEST_CASE(page_cache_waiter_that_leaves_leaves_alone) {
    co_await with_cache(
      [](workload_budget&, page_cache& cache) -> seastar::future<> {
          value_slot place;
          seastar::abort_source staying, leaving, gone;
          gone.request_abort();
          auto load = place.lead(cache, byte_count{4_KiB});
          BOOST_REQUIRE(load.has_value());
          const auto charged = cache.snapshot().bytes;
          // A caller already released does not wait.
          BOOST_CHECK(is(co_await place.join(gone), errc::aborted));
          BOOST_CHECK_EQUAL(cache.snapshot().waits, 0U);
          auto first = place.join(staying);
          auto second = place.join(leaving);
          leaving.request_abort();
          {
              const auto settled = co_await std::move(second);
              BOOST_CHECK(is(settled, errc::aborted));
          }
          // The load goes on for the other, and for whoever comes next.
          BOOST_CHECK(place.loading() && !first.available());
          auto third = place.join(staying);
          // Waiting costs the cache nothing.
          BOOST_CHECK_EQUAL(cache.snapshot().bytes, charged);
          BOOST_CHECK_EQUAL(cache.snapshot().waits, 3U);
          std::move(*load).publish(4);
          {
              const auto one = co_await std::move(first);
              const auto other = co_await std::move(third);
              BOOST_REQUIRE(one.has_value() && other.has_value());
              BOOST_CHECK_EQUAL(**one, 4U);
              BOOST_CHECK(&**one == &**other);
          }
          // A caller released after the load ended and before it resumed
          // is past leaving: it has the value.
          place.drop();
          auto next = place.lead(cache, byte_count{4_KiB});
          BOOST_REQUIRE(next.has_value());
          seastar::abort_source late;
          auto told = place.join(late);
          std::move(*next).publish(5);
          late.request_abort();
          {
              const auto settled = co_await std::move(told);
              BOOST_REQUIRE(settled.has_value());
              BOOST_CHECK_EQUAL(**settled, 5U);
          }
          // Everyone waiting leaves, and the load still ends with its value
          // in the slot.
          place.drop();
          auto last = place.lead(cache, byte_count{4_KiB});
          BOOST_REQUIRE(last.has_value());
          seastar::abort_source all;
          auto fourth = place.join(all);
          auto fifth = place.join(all);
          all.request_abort();
          {
              const auto settled = co_await std::move(fourth);
              BOOST_CHECK(is(settled, errc::aborted));
          }
          {
              const auto settled = co_await std::move(fifth);
              BOOST_CHECK(is(settled, errc::aborted));
          }
          BOOST_CHECK(place.loading());
          std::move(*last).publish(6);
          BOOST_CHECK_EQUAL(*place.get(), 6U);
          BOOST_CHECK_EQUAL(cache.snapshot().bytes, charged);
      });
}

SEASTAR_TEST_CASE(page_cache_load_takes_any_number_of_callers) {
    co_await with_cache(
      [](workload_budget&, page_cache& cache) -> seastar::future<> {
          constexpr std::size_t callers = 1000;
          value_index places;
          for (const bool succeeds : {true, false}) {
              seastar::abort_source caller, leaving;
              auto load = places.lead(cache, 9, byte_count{4_KiB});
              BOOST_REQUIRE(load.has_value());
              const auto charged = cache.snapshot().bytes;
              const auto waited = cache.snapshot().waits;
              std::vector<seastar::future<fetched>> waiting;
              waiting.reserve(callers);
              // Every third caller leaves before the load ends.
              for (std::size_t n = 0; n < callers; ++n)
                  waiting.push_back(
                    places.join(9, n % 3 == 0 ? leaving : caller));
              // None was refused, and none cost the cache a byte.
              BOOST_CHECK_EQUAL(cache.snapshot().waits, waited + callers);
              BOOST_CHECK_EQUAL(cache.snapshot().bytes, charged);
              BOOST_CHECK_EQUAL(cache.snapshot().loads, succeeds ? 1U : 2U);
              leaving.request_abort();
              if (succeeds)
                  std::move(*load).publish(90);
              else
                  std::move(*load).fail(failed(errc::io_failure));
              std::size_t told = 0, left = 0;
              for (std::size_t n = 0; n < callers; ++n) {
                  const auto settled = co_await std::move(waiting[n]);
                  const bool expected = n % 3 == 0 ? is(settled, errc::aborted)
                                        : succeeds
                                          ? settled.has_value()
                                              && **settled == 90
                                          : is(settled, errc::io_failure);
                  if (!expected) continue;
                  if (n % 3 == 0)
                      ++left;
                  else
                      ++told;
              }
              BOOST_CHECK_EQUAL(left, (callers + 2) / 3);
              BOOST_CHECK_EQUAL(told, callers - (callers + 2) / 3);
              // A value is kept once and a failure not at all.
              BOOST_CHECK_EQUAL(
                cache.snapshot().bytes, succeeds ? charged : std::uint64_t{0});
              places.drop(9);
          }
      });
}

SEASTAR_TEST_CASE(page_cache_fetch_loads_once_as_the_caches_own_work) {
    co_await with_cache(
      [](workload_budget&, page_cache& cache) -> seastar::future<> {
          value_index places;
          seastar::abort_source asked, staying;
          held_load load;
          // The first to miss starts the load, and it is the cache's from
          // then on.
          auto first = places.fetch(
            cache, 7, byte_count{4_KiB}, load.loader(), asked);
          BOOST_CHECK(places.loading(7) && !first.available());
          // Whoever misses meanwhile waits for that load.
          auto second = places.fetch(
            cache, 7, byte_count{4_KiB}, never_loads, staying);
          auto third = places.fetch(
            cache, 7, byte_count{4_KiB}, never_loads, staying);
          BOOST_CHECK_EQUAL(cache.snapshot().loads, 1U);
          BOOST_CHECK_EQUAL(cache.snapshot().waits, 3U);
          // The caller that asked for it leaves, alone.
          asked.request_abort();
          {
              const auto settled = co_await std::move(first);
              BOOST_CHECK(is(settled, errc::aborted));
          }
          BOOST_CHECK(places.loading(7) && !second.available());
          load.outcome.set_value(value{70});
          {
              const auto one = co_await std::move(second);
              const auto other = co_await std::move(third);
              BOOST_REQUIRE(one.has_value() && other.has_value());
              BOOST_CHECK_EQUAL(**one, 70U);
              BOOST_CHECK(&**one == &**other);
          }
          BOOST_CHECK_EQUAL(load.calls, 1U);
          // The value is the index's now: a fetch is a hit, at once.
          auto hit = places.fetch(
            cache, 7, byte_count{4_KiB}, never_loads, staying);
          BOOST_CHECK(hit.available());
          {
              const auto settled = co_await std::move(hit);
              BOOST_REQUIRE(settled.has_value());
              BOOST_CHECK_EQUAL(**settled, 70U);
          }
          // A caller already released asks for nothing.
          {
              const auto settled = co_await places.fetch(
                cache, 8, byte_count{4_KiB}, never_loads, asked);
              BOOST_CHECK(is(settled, errc::aborted));
          }
          BOOST_CHECK(!places.loading(8));
          BOOST_CHECK_EQUAL(cache.snapshot().loads, 1U);
          // The same through a slot.
          value_slot place;
          held_load other;
          auto slotted = place.fetch(
            cache, byte_count{4_KiB}, other.loader(), staying);
          auto joined = place.fetch(
            cache, byte_count{4_KiB}, never_loads, staying);
          other.outcome.set_value(value{80});
          {
              const auto one = co_await std::move(slotted);
              const auto two = co_await std::move(joined);
              BOOST_REQUIRE(one.has_value() && two.has_value());
              BOOST_CHECK_EQUAL(**one, 80U);
              BOOST_CHECK(&**one == &**two);
          }
          BOOST_CHECK_EQUAL(other.calls, 1U);
          BOOST_CHECK_EQUAL(*place.get(), 80U);
      });
}

SEASTAR_TEST_CASE(page_cache_fetch_runs_a_bounded_number_of_loads) {
    co_await with_cache(
      [](workload_budget&, page_cache& cache) -> seastar::future<> {
          value_index places;
          seastar::abort_source caller, leaving;
          held_load one, two, three, four;
          auto first = places.fetch(
            cache, 1, byte_count{4_KiB}, one.loader(), caller);
          auto second = places.fetch(
            cache, 2, byte_count{4_KiB}, two.loader(), caller);
          BOOST_CHECK(places.loading(1) && places.loading(2));
          // Both slots are taken: the next two wait for one, with nothing
          // charged and nothing started.
          const auto charged = cache.snapshot().bytes;
          auto third = places.fetch(
            cache, 3, byte_count{4_KiB}, three.loader(), caller);
          auto fourth = places.fetch(
            cache, 4, byte_count{4_KiB}, four.loader(), leaving);
          BOOST_CHECK(!places.loading(3) && !places.loading(4));
          BOOST_CHECK_EQUAL(cache.snapshot().loads, 2U);
          BOOST_CHECK_EQUAL(cache.snapshot().bytes, charged);
          // A reader waiting for a slot leaves, and nothing was started for
          // it.
          leaving.request_abort();
          {
              const auto settled = co_await std::move(fourth);
              BOOST_CHECK(is(settled, errc::aborted));
          }
          BOOST_CHECK(!places.loading(4));
          BOOST_CHECK_EQUAL(cache.snapshot().loads, 2U);
          // A load that ends gives its slot to the one waiting.
          one.outcome.set_value(value{10});
          three.outcome.set_value(value{30});
          {
              const auto settled = co_await std::move(first);
              BOOST_REQUIRE(settled.has_value());
              BOOST_CHECK_EQUAL(**settled, 10U);
          }
          {
              const auto settled = co_await std::move(third);
              BOOST_REQUIRE(settled.has_value());
              BOOST_CHECK_EQUAL(**settled, 30U);
          }
          BOOST_CHECK_EQUAL(cache.snapshot().loads, 3U);
          two.outcome.set_value(value{20});
          {
              const auto settled = co_await std::move(second);
              BOOST_REQUIRE(settled.has_value());
              BOOST_CHECK_EQUAL(**settled, 20U);
          }
          BOOST_CHECK_EQUAL(one.calls + two.calls + three.calls, 3U);
          BOOST_CHECK_EQUAL(four.calls, 0U);
      },
      1_MiB,
      2);
}

SEASTAR_TEST_CASE(page_cache_fetch_at_capacity_is_still_one_load) {
    co_await with_cache(
      [](workload_budget&, page_cache& cache) -> seastar::future<> {
          value_index places;
          seastar::abort_source caller;
          held_load one, two, three, four;
          // As many loads in flight as the cache runs at once.
          auto first = places.fetch(
            cache, 1, byte_count{4_KiB}, one.loader(), caller);
          auto second = places.fetch(
            cache, 2, byte_count{4_KiB}, two.loader(), caller);
          // Two callers want a third key together. Neither loads beside
          // the other for want of a slot: both wait, and nothing starts.
          auto third = places.fetch(
            cache, 3, byte_count{4_KiB}, three.loader(), caller);
          auto again = places.fetch(
            cache, 3, byte_count{4_KiB}, never_loads, caller);
          BOOST_CHECK(!places.loading(3));
          BOOST_CHECK_EQUAL(cache.snapshot().loads, 2U);
          BOOST_CHECK_EQUAL(three.calls, 0U);
          // A slot is given back: the key is loaded once, for both.
          three.outcome.set_value(value{30});
          one.outcome.set_value(value{10});
          {
              const auto settled = co_await std::move(first);
              BOOST_REQUIRE(settled.has_value());
              BOOST_CHECK_EQUAL(**settled, 10U);
          }
          {
              const auto led = co_await std::move(third);
              const auto joined = co_await std::move(again);
              BOOST_REQUIRE(led.has_value() && joined.has_value());
              BOOST_CHECK_EQUAL(**led, 30U);
              BOOST_CHECK(&**led == &**joined);
          }
          BOOST_CHECK_EQUAL(three.calls, 1U);
          BOOST_CHECK_EQUAL(cache.snapshot().loads, 3U);
          // The slot that load held is free for another key, which is led
          // once and joined like any other.
          auto fourth = places.fetch(
            cache, 4, byte_count{4_KiB}, four.loader(), caller);
          BOOST_CHECK(places.loading(4));
          auto merged = places.fetch(
            cache, 4, byte_count{4_KiB}, never_loads, caller);
          four.outcome.set_value(value{40});
          {
              const auto led = co_await std::move(fourth);
              const auto joined = co_await std::move(merged);
              BOOST_REQUIRE(led.has_value() && joined.has_value());
              BOOST_CHECK_EQUAL(**led, 40U);
              BOOST_CHECK(&**led == &**joined);
          }
          two.outcome.set_value(value{20});
          {
              const auto settled = co_await std::move(second);
              BOOST_REQUIRE(settled.has_value());
              BOOST_CHECK_EQUAL(**settled, 20U);
          }
          BOOST_CHECK_EQUAL(
            one.calls + two.calls + three.calls + four.calls, 4U);
          BOOST_CHECK_EQUAL(cache.snapshot().loads, 4U);
      },
      1_MiB,
      2);
}

SEASTAR_TEST_CASE(page_cache_fetch_tells_everyone_why_a_load_failed) {
    co_await with_cache(
      [](workload_budget&, page_cache& cache) -> seastar::future<> {
          value_slot place;
          seastar::abort_source caller;
          {
              // The load's own failure.
              held_load load;
              auto first = place.fetch(
                cache, byte_count{4_KiB}, load.loader(), caller);
              auto second = place.fetch(
                cache, byte_count{4_KiB}, never_loads, caller);
              load.outcome.set_value(
                runtime::failure(failed(errc::corrupt_data)));
              {
                  const auto settled = co_await std::move(first);
                  BOOST_CHECK(is(settled, errc::corrupt_data));
              }
              {
                  const auto settled = co_await std::move(second);
                  BOOST_CHECK(is(settled, errc::corrupt_data));
              }
              BOOST_CHECK(place.empty());
          }
          {
              // A loader that throws has failed like any other: its caller
              // is given what it threw, as it is, and nothing is kept.
              auto settled = co_await seastar::coroutine::as_future(place.fetch(
                cache,
                byte_count{4_KiB},
                [](seastar::abort_source&) -> seastar::future<loaded> {
                    throw std::runtime_error("boom");
                },
                caller));
              BOOST_CHECK(boom(settled));
              BOOST_CHECK(place.empty());
          }
          {
              // One that throws after it began, with a caller waiting
              // beside the one that asked: both are told the same.
              seastar::promise<> begun;
              auto first = place.fetch(
                cache,
                byte_count{4_KiB},
                [&begun](seastar::abort_source&) -> seastar::future<loaded> {
                    co_await begun.get_future();
                    throw std::runtime_error("boom");
                },
                caller);
              auto second = place.fetch(
                cache, byte_count{4_KiB}, never_loads, caller);
              begun.set_value();
              {
                  auto settled = co_await seastar::coroutine::as_future(
                    std::move(first));
                  BOOST_CHECK(boom(settled));
              }
              {
                  auto settled = co_await seastar::coroutine::as_future(
                    std::move(second));
                  BOOST_CHECK(boom(settled));
              }
              BOOST_CHECK(place.empty());
          }
          {
              // Memory that could not be had is reported as that.
              const auto settled = co_await place.fetch(
                cache,
                byte_count{4_KiB},
                [](seastar::abort_source&) -> seastar::future<loaded> {
                    throw std::bad_alloc();
                },
                caller);
              BOOST_CHECK(is(settled, errc::resource_exhausted));
          }
          BOOST_CHECK(place.empty());
          BOOST_CHECK_EQUAL(cache.snapshot().bytes, 0U);
          // Nothing remembers a failure: the next fetch loads again.
          held_load again;
          auto retried = place.fetch(
            cache, byte_count{4_KiB}, again.loader(), caller);
          again.outcome.set_value(value{5});
          {
              const auto settled = co_await std::move(retried);
              BOOST_REQUIRE(settled.has_value());
              BOOST_CHECK_EQUAL(**settled, 5U);
          }
          BOOST_CHECK_EQUAL(cache.snapshot().loads, 5U);
      });
}

SEASTAR_TEST_CASE(page_cache_fetch_is_funded_by_its_reader_without_room) {
    co_await with_cache(
      [](workload_budget& budget, page_cache& cache) -> seastar::future<> {
          value_slot place;
          seastar::abort_source caller;
          // More than the cache keeps in one value, and no reservation to
          // load it under: refused before any work.
          {
              const auto settled = co_await place.fetch(
                cache, byte_count{64_KiB}, never_loads, caller);
              BOOST_CHECK(is(settled, errc::out_of_range));
          }
          BOOST_CHECK(place.empty());
          auto reader = budget.try_reserve(byte_count{256_KiB});
          BOOST_REQUIRE(reader.has_value());
          held_load load;
          auto first = place.fetch(
            cache, byte_count{64_KiB}, load.loader(), caller, &*reader);
          BOOST_CHECK(place.loading());
          // Whoever arrives shares it, with or without a reservation.
          auto second = place.fetch(
            cache, byte_count{64_KiB}, never_loads, caller);
          BOOST_CHECK_EQUAL(cache.snapshot().bytes, 0U);
          BOOST_CHECK_EQUAL(cache.snapshot().uncached, 1U);
          load.outcome.set_value(value{64});
          {
              const auto one = co_await std::move(first);
              const auto other = co_await std::move(second);
              BOOST_REQUIRE(one.has_value() && other.has_value());
              BOOST_CHECK_EQUAL(**one, 64U);
              BOOST_CHECK(&**one == &**other);
              // It is nobody's but theirs.
              BOOST_CHECK(place.empty());
          }
          BOOST_CHECK_EQUAL(load.calls, 1U);
          BOOST_CHECK_EQUAL(cache.snapshot().bytes, 0U);
      },
      128_KiB);
}

// A value that says what it holds.
struct sized final {
    std::uint64_t held;
    [[nodiscard]] byte_count charge() const noexcept {
        return byte_count{held};
    }
};

SEASTAR_TEST_CASE(page_cache_charges_a_load_what_its_value_turns_out_to_hold) {
    co_await with_cache(
      [](workload_budget&, page_cache& cache) -> seastar::future<> {
          value_slot place;
          // Admitted for one size and then for what is there: more is
          // taken, and less is given back.
          auto led = place.lead(cache, byte_count{4_KiB});
          BOOST_REQUIRE(led.has_value());
          const auto admitted = cache.snapshot().bytes;
          BOOST_REQUIRE(led->resize(byte_count{12_KiB}).has_value());
          BOOST_CHECK_EQUAL(cache.snapshot().bytes, admitted + 8_KiB);
          BOOST_REQUIRE(led->resize(byte_count{1_KiB}).has_value());
          BOOST_CHECK_EQUAL(cache.snapshot().bytes, admitted - 3_KiB);
          // More than the cache keeps in one value: refused, and nothing
          // changed.
          const auto refused = cache.snapshot().refused;
          BOOST_CHECK(is(led->resize(byte_count{512_KiB}), errc::out_of_range));
          BOOST_CHECK_EQUAL(cache.snapshot().bytes, admitted - 3_KiB);
          BOOST_CHECK_EQUAL(cache.snapshot().refused, refused + 1);
          // Held before it holds a value: what was given of it early stays
          // charged when its object lets go of it.
          auto early = led->hold();
          // No value yet, and no way to one but to ask.
          BOOST_CHECK(early && early.value() == nullptr);
          std::move(*led).publish(value{7});
          BOOST_REQUIRE(early.value() != nullptr);
          BOOST_CHECK_EQUAL(*early.value(), 7U);
          place.drop();
          BOOST_CHECK(place.empty());
          BOOST_CHECK_EQUAL(cache.snapshot().bytes, admitted - 3_KiB);
          early.reset();
          BOOST_CHECK_EQUAL(cache.snapshot().bytes, 0U);

          // A value that says what it holds is charged that when its load
          // ends, whatever it was admitted for: as much as one admitted
          // for exactly that.
          seastar::abort_source caller;
          cache_slot<sized> priced, exact, large;
          const auto loads = [](std::uint64_t holds) {
              return [holds](seastar::abort_source&) {
                  return seastar::make_ready_future<runtime::result<sized>>(
                    sized{holds});
              };
          };
          {
              const auto held = co_await priced.fetch(
                cache, byte_count{16_KiB}, loads(2_KiB), caller);
              BOOST_REQUIRE(held.has_value());
              BOOST_CHECK_EQUAL((*held)->held, 2_KiB);
          }
          const auto kept = cache.snapshot().bytes;
          auto same = exact.lead(cache, byte_count{2_KiB});
          BOOST_REQUIRE(same.has_value());
          BOOST_CHECK_EQUAL(cache.snapshot().bytes, 2 * kept);
          std::move(*same).publish(sized{2_KiB});
          // One that holds more than the cache keeps is not kept: its load
          // has failed, and everyone is told.
          {
              const auto settled = co_await large.fetch(
                cache, byte_count{1_KiB}, loads(512_KiB), caller);
              BOOST_CHECK(is(settled, errc::out_of_range));
          }
          BOOST_CHECK(large.empty());
          BOOST_CHECK_EQUAL(cache.snapshot().bytes, 2 * kept);
          priced.drop();
          exact.drop();

          // A reader that will not come back reads what is kept.
          value_index places;
          auto under = places.lead(cache, 5, byte_count{1_KiB});
          BOOST_REQUIRE(under.has_value());
          std::move(*under).publish(value{55});
          {
              const auto read = places.floor(9, false);
              BOOST_CHECK(read && *read == 55U);
              BOOST_CHECK(!places.floor(4, false));
          }
          places.clear();
      });
}

SEASTAR_TEST_CASE(page_cache_prefetch_takes_what_it_needs_or_is_dropped) {
    co_await with_cache(
      [](workload_budget&, page_cache& cache) -> seastar::future<> {
          value_index places;
          seastar::abort_source caller;
          held_load load;
          // Started for nobody. The one load slot is taken, and the key is
          // loading.
          BOOST_CHECK(
            places.prefetch(cache, 5, byte_count{1_KiB}, load.loader()));
          BOOST_CHECK(places.loading(5) && places.holds(5) && !places.holds(6));
          const auto charged = cache.snapshot().bytes;
          BOOST_CHECK(charged != 0);
          // The same key again, and another key while no slot is free:
          // dropped, and nothing changed.
          BOOST_CHECK(
            !places.prefetch(cache, 5, byte_count{1_KiB}, never_loads));
          BOOST_CHECK(
            !places.prefetch(cache, 6, byte_count{1_KiB}, never_loads));
          BOOST_CHECK(!places.holds(6));
          BOOST_CHECK_EQUAL(cache.snapshot().bytes, charged);
          BOOST_CHECK_EQUAL(cache.snapshot().loads, 1U);
          // A reader that comes for it joins the load that was started.
          auto joined = places.fetch(
            cache, 5, byte_count{1_KiB}, never_loads, caller);
          load.outcome.set_value(value{50});
          {
              const auto got = co_await std::move(joined);
              BOOST_REQUIRE(got.has_value());
              BOOST_CHECK_EQUAL(**got, 50U);
          }
          BOOST_CHECK_EQUAL(load.calls, 1U);
          // The slot is free again. A value the cache does not keep is not
          // asked for, and neither is one that is there.
          BOOST_CHECK(
            !places.prefetch(cache, 6, byte_count{512_KiB}, never_loads));
          BOOST_CHECK(
            !places.prefetch(cache, 5, byte_count{1_KiB}, never_loads));
          BOOST_CHECK(!places.holds(6));
          BOOST_CHECK_EQUAL(cache.snapshot().bytes, charged);
          // One nobody waited for is an entry like any other, there for
          // whoever comes.
          held_load other;
          BOOST_CHECK(
            places.prefetch(cache, 7, byte_count{1_KiB}, other.loader()));
          other.outcome.set_value(value{70});
          {
              const auto got = co_await places.fetch(
                cache, 7, byte_count{1_KiB}, never_loads, caller);
              BOOST_REQUIRE(got.has_value());
              BOOST_CHECK_EQUAL(**got, 70U);
          }
          BOOST_CHECK_EQUAL(other.calls, 1U);
          BOOST_CHECK_EQUAL(cache.snapshot().bytes, 2 * charged);
          places.clear();
      },
      1_MiB,
      1);
}

SEASTAR_TEST_CASE(page_cache_held_value_survives_every_eviction) {
    co_await with_cache(
      [](workload_budget&, page_cache& cache) -> seastar::future<> {
          const auto capacity = cache.snapshot().capacity;
          const byte_count backing{1_KiB};
          value_index places;
          seastar::abort_source caller;
          // A load is the cache's, so its loader owns what it counts in.
          const auto populated = seastar::make_lw_shared<std::uint32_t>(0);
          const auto get = [&](std::uint64_t key) {
              return places.fetch(
                cache,
                key,
                backing,
                [populated, key](seastar::abort_source&) {
                    ++*populated;
                    return seastar::make_ready_future<loaded>(key * 10);
                },
                caller);
          };
          std::uint32_t over = 0, unserved = 0;
          // Far more values than the cache holds, each read once.
          const auto churn = [&](std::uint64_t from) -> seastar::future<> {
              for (std::uint64_t key = from; key < from + 200; ++key) {
                  const auto got = co_await get(key);
                  if (!got || **got != key * 10) ++unserved;
                  if (cache.snapshot().bytes > capacity) ++over;
              }
          };
          for (std::uint64_t key = 0; key < 3; ++key) {
              const auto got = co_await get(key);
              BOOST_REQUIRE(got.has_value());
              BOOST_CHECK_EQUAL(**got, key * 10);
          }
          BOOST_CHECK_EQUAL(*populated, 3U);
          // Two of the three are held while everything else passes through.
          auto held = places.get(1);
          auto kept = places.get(2);
          BOOST_REQUIRE(held && kept);
          co_await churn(100);
          BOOST_CHECK_EQUAL(*populated, 203U);
          BOOST_CHECK_GT(cache.snapshot().evicted, 0U);
          // What nothing held went.
          BOOST_CHECK(!places.get(0));
          // What is held is what it was and where its object finds it: a
          // reader that comes for it loads nothing.
          BOOST_CHECK_EQUAL(*held, 10U);
          BOOST_CHECK_EQUAL(*kept, 20U);
          {
              const auto again = co_await places.fetch(
                cache, 1, backing, never_loads, caller);
              BOOST_REQUIRE(again.has_value());
              BOOST_CHECK(&**again == &*held);
              const auto found = places.floor(2, false);
              BOOST_CHECK(found && &*found == &*kept);
          }
          // A value that went loads again when it is asked for.
          {
              const auto got = co_await get(0);
              BOOST_REQUIRE(got.has_value());
              BOOST_CHECK_EQUAL(**got, 0U);
          }
          BOOST_CHECK_EQUAL(*populated, 204U);
          // Let go of, a value goes like any other, and loads again.
          kept.reset();
          co_await churn(1000);
          BOOST_CHECK(!places.get(2));
          {
              const auto got = co_await get(2);
              BOOST_REQUIRE(got.has_value());
              BOOST_CHECK_EQUAL(**got, 20U);
          }
          BOOST_CHECK_EQUAL(*populated, 405U);
          BOOST_CHECK_EQUAL(*held, 10U);
          BOOST_CHECK_EQUAL(over, 0U);
          BOOST_CHECK_EQUAL(unserved, 0U);
          BOOST_CHECK_EQUAL(cache.snapshot().refused, 0U);
      },
      64_KiB);
}

SEASTAR_TEST_CASE(page_cache_read_that_is_not_a_use_earns_nothing) {
    co_await with_cache(
      [](workload_budget&, page_cache& cache) -> seastar::future<> {
          const auto capacity = cache.snapshot().capacity;
          const byte_count backing{1_KiB};
          value_index places;
          fill(cache, places, 0, backing, 0);
          const auto each = cache.snapshot().bytes;
          std::uint64_t full = 1;
          while (cache.snapshot().bytes + each <= capacity) {
              fill(cache, places, full, backing, full);
              ++full;
          }
          BOOST_REQUIRE_GE(full, 8U);
          // The oldest is read twice by a reader that will not come back,
          // and the next twice by one that will.
          BOOST_CHECK(places.floor(0, false) && places.floor(0, false));
          BOOST_CHECK(places.floor(1) && places.floor(1));
          // Room for three more is more than two evictions make.
          for (std::uint64_t key = full; key < full + 3; ++key)
              fill(cache, places, key, backing, key);
          // It was made from the oldest, as if nothing had read it, and
          // from the one after the next: the one between them was taken
          // for a value that is used.
          BOOST_CHECK(!places.get(0) && !places.get(2));
          BOOST_CHECK_EQUAL(cache.snapshot().main_bytes, each);
          BOOST_CHECK_EQUAL(*places.get(1), 1U);
          BOOST_CHECK_GE(cache.snapshot().evicted, 2U);
          BOOST_CHECK_LE(cache.snapshot().bytes, capacity);
          BOOST_CHECK_EQUAL(cache.snapshot().refused, 0U);
          places.clear();
          co_return;
      },
      64_KiB);
}

SEASTAR_TEST_CASE(page_cache_serves_many_readers_under_pressure) {
    co_await with_cache(
      [](workload_budget&, page_cache& cache) -> seastar::future<> {
          // More readers than slots, more keys than fit, and readers that
          // leave or hold what they read. A seeded stream written here.
          struct stream final {
              std::uint64_t state;
              std::uint64_t below(std::uint64_t bound) {
                  state ^= state << 13U;
                  state ^= state >> 7U;
                  state ^= state << 17U;
                  return state % bound;
              }
          };
          constexpr std::uint64_t keys = 48, readers = 24, rounds = 40;
          const auto capacity = cache.snapshot().capacity;
          value_index places;
          std::uint64_t served = 0, left = 0, failures = 0;
          std::uint64_t pressed = 0, unexpected = 0;
          // A load is the cache's and may outlive whoever asked for it, so
          // its loader owns what it counts in.
          const auto started = seastar::make_lw_shared<std::uint64_t>(0);
          const auto loads = [started](std::uint64_t key) {
              return [started,
                      key](seastar::abort_source&) -> seastar::future<loaded> {
                  ++*started;
                  co_await seastar::yield();
                  if (key % 7 == 0)
                      co_return runtime::failure(failed(errc::corrupt_data));
                  co_return key * 3;
              };
          };
          const auto reads = [&](std::uint64_t seed) -> seastar::future<> {
              stream random{seed};
              for (std::uint64_t round = 0; round < rounds; ++round) {
                  const auto key = random.below(keys);
                  seastar::abort_source caller;
                  auto pending = places.fetch(
                    cache, key, byte_count{1_KiB}, loads(key), caller);
                  const bool leaves = random.below(8) == 0;
                  if (leaves) caller.request_abort();
                  const auto got = co_await std::move(pending);
                  if (got) {
                      ++served;
                      if (**got != key * 3 || key % 7 == 0) ++unexpected;
                      // Held across a turn now and then, as output is.
                      if (random.below(2) == 0) co_await seastar::yield();
                  } else if (is(got, errc::aborted)) {
                      ++left;
                      if (!leaves) ++unexpected;
                  } else if (is(got, errc::corrupt_data)) {
                      ++failures;
                      if (key % 7 != 0) ++unexpected;
                  } else if (is(got, errc::queue_full)) {
                      // Every value was in use just then.
                      ++pressed;
                  } else {
                      ++unexpected;
                  }
                  if (cache.snapshot().bytes > capacity) ++unexpected;
              }
          };
          std::vector<seastar::future<>> fibers;
          fibers.reserve(readers);
          for (std::uint64_t reader = 0; reader < readers; ++reader)
              fibers.push_back(reads(0x9e3779b97f4a7c15ULL * (reader + 1)));
          for (auto& fiber : fibers)
              co_await std::move(fiber);
          BOOST_CHECK_EQUAL(unexpected, 0U);
          BOOST_CHECK_EQUAL(
            served + left + failures + pressed, readers * rounds);
          BOOST_CHECK_GT(served, 0U);
          BOOST_CHECK_GT(left, 0U);
          BOOST_CHECK_GT(failures, 0U);
          // Far fewer loads than reads, and room was made again and again.
          BOOST_CHECK_LT(*started, readers * rounds);
          BOOST_CHECK_GT(cache.snapshot().evicted, 0U);
          // The object is closed. A load a reader left behind is still the
          // cache's, and is joined when the cache stops.
          co_await places.clear_gently();
          BOOST_CHECK(places.empty());
      },
      16_KiB,
      3);
}

SEASTAR_TEST_CASE(page_cache_stop_joins_loads_and_tells_every_waiter) {
    co_await with_budget([](workload_budget& budget) -> seastar::future<> {
        auto cache = make_cache(budget, 1_MiB, 1);
        value_slot ready, idle, loading;
        value_index places;
        seastar::abort_source caller;
        fill(*cache, ready, byte_count{4_KiB}, 3);
        fill(*cache, idle, byte_count{4_KiB}, 4);
        const auto each = cache->snapshot().bytes / 2;
        auto held = ready.get();
        BOOST_REQUIRE(held);
        // A load the cache runs, which ends when the cache stops.
        seastar::promise<loaded> outcome;
        seastar::optimized_optional<seastar::abort_source::subscription> asked;
        std::uint32_t calls = 0;
        auto first = loading.fetch(
          *cache,
          byte_count{4_KiB},
          [&](seastar::abort_source& stop) {
              ++calls;
              // A code no wait is ended with: what a waiter gets is what
              // the stopped load returned.
              if (stop.abort_requested())
                  outcome.set_value(
                    runtime::failure(failed(errc::unavailable)));
              else
                  asked = stop.subscribe([&outcome]() noexcept {
                      outcome.set_value(
                        runtime::failure(failed(errc::unavailable)));
                  });
              return outcome.get_future();
          },
          caller);
        // The one slot is taken: this reader waits for it.
        auto second = places.fetch(
          *cache, 1, byte_count{4_KiB}, never_loads, caller);
        BOOST_CHECK(loading.loading() && !places.loading(1));
        co_await cache->stop();
        BOOST_CHECK_EQUAL(calls, 1U);
        {
            const auto settled = co_await std::move(first);
            BOOST_CHECK(is(settled, errc::unavailable));
        }
        {
            const auto settled = co_await std::move(second);
            BOOST_CHECK(is(settled, errc::closed));
        }
        // It let every value go. One something holds is what it was, and
        // stays charged until that lets go.
        BOOST_CHECK(ready.empty() && idle.empty() && loading.empty());
        BOOST_CHECK_EQUAL(*held, 3U);
        BOOST_CHECK_EQUAL(cache->snapshot().bytes, each);
        BOOST_CHECK_EQUAL(cache->snapshot().small_bytes, 0U);
        // Nothing starts after it.
        {
            const auto settled = co_await places.fetch(
              *cache, 2, byte_count{4_KiB}, never_loads, caller);
            BOOST_CHECK(is(settled, errc::closed));
        }
        BOOST_CHECK(is(idle.lead(*cache, byte_count{4_KiB}), errc::closed));
        BOOST_CHECK(idle.empty() && places.empty());
        cache.reset();
        // The capacity stays admitted while anything is charged to it.
        BOOST_CHECK_EQUAL(budget.snapshot().tasks, 1U);
        BOOST_CHECK_EQUAL(*held, 3U);
        held.reset();
        BOOST_CHECK_EQUAL(budget.snapshot().tasks, 0U);
    });
}

// A reader that waits for a slot is still in the cache: the stop ends its
// wait and does not return before the reader has left, so a cache destroyed
// when its stop returns is not one a reader is about to look into.
SEASTAR_TEST_CASE(page_cache_stop_joins_a_reader_waiting_for_a_slot) {
    co_await with_budget([](workload_budget& budget) -> seastar::future<> {
        auto cache = make_cache(budget, 1_MiB, 1);
        value_slot loading;
        value_index places;
        seastar::abort_source caller;
        // A load the cache runs, which ends when the cache stops.
        seastar::promise<loaded> outcome;
        seastar::optimized_optional<seastar::abort_source::subscription> asked;
        auto first = loading.fetch(
          *cache,
          byte_count{4_KiB},
          [&](seastar::abort_source& stop) {
              if (stop.abort_requested())
                  outcome.set_value(
                    runtime::failure(failed(errc::unavailable)));
              else
                  asked = stop.subscribe([&outcome]() noexcept {
                      outcome.set_value(
                        runtime::failure(failed(errc::unavailable)));
                  });
              return outcome.get_future();
          },
          caller);
        // The one slot is taken: this reader waits for it.
        auto second = places.fetch(
          *cache, 1, byte_count{4_KiB}, never_loads, caller);
        co_await cache->stop();
        // Gone at once: neither reader was awaited yet.
        cache.reset();
        {
            const auto settled = co_await std::move(first);
            BOOST_CHECK(is(settled, errc::unavailable));
        }
        {
            const auto settled = co_await std::move(second);
            BOOST_CHECK(is(settled, errc::closed));
        }
        BOOST_CHECK(loading.empty() && places.empty());
    });
}

// A load its owner leads, as a writer fills a value over time, is not one a
// reader's request waits for: the reader is told there is no room for the
// value as the cache's own, at once, also when the owner's load began while
// the reader waited for its turn.
SEASTAR_TEST_CASE(page_cache_fetch_does_not_wait_for_a_load_its_owner_leads) {
    co_await with_cache(
      [](workload_budget&, page_cache& cache) -> seastar::future<> {
          value_index places;
          seastar::abort_source caller;
          auto own = places.lead(cache, 5, byte_count{1_KiB});
          BOOST_REQUIRE(own.has_value());
          {
              const auto asked = co_await places.fetch(
                cache, 5, byte_count{1_KiB}, never_loads, caller);
              BOOST_CHECK(is(asked, errc::queue_full));
          }
          // The one slot is taken by another key's load, and a reader waits
          // for it. Its key's owner begins a load of its own meanwhile.
          held_load busy;
          auto loading = places.fetch(
            cache, 1, byte_count{1_KiB}, busy.loader(), caller);
          auto waiting = places.fetch(
            cache, 2, byte_count{1_KiB}, never_loads, caller);
          auto late = places.lead(cache, 2, byte_count{1_KiB});
          BOOST_REQUIRE(late.has_value());
          busy.outcome.set_value(value{10});
          {
              const auto other = co_await std::move(loading);
              BOOST_REQUIRE(other.has_value());
              BOOST_CHECK_EQUAL(**other, 10U);
          }
          {
              const auto turned = co_await std::move(waiting);
              BOOST_CHECK(is(turned, errc::queue_full));
          }
          BOOST_CHECK(places.loading(2) && places.loading(5));
          // What the owners publish is the cache's like anything else, and
          // whoever joins an owner's load by asking to is still let.
          auto joined = places.join(2, caller);
          std::move(*late).publish(value{20});
          std::move(*own).publish(value{50});
          {
              const auto told = co_await std::move(joined);
              BOOST_REQUIRE(told.has_value());
              BOOST_CHECK_EQUAL(**told, 20U);
          }
          {
              const auto hit = co_await places.fetch(
                cache, 5, byte_count{1_KiB}, never_loads, caller);
              BOOST_REQUIRE(hit.has_value());
              BOOST_CHECK_EQUAL(**hit, 50U);
          }
      },
      1_MiB,
      1);
}

SEASTAR_TEST_CASE(page_cache_stop_settles_ten_thousand_waiters) {
    co_await with_budget([](workload_budget& budget) -> seastar::future<> {
        constexpr std::size_t callers = 10000;
        constexpr std::uint64_t keys = 64;
        auto cache = make_cache(budget, 1_MiB, 3);
        value_index places;
        seastar::abort_source caller;
        // A load that ends when the cache stops and not before. It is the
        // cache's, so its loader owns what it counts in.
        const auto started = seastar::make_lw_shared<std::uint32_t>(0);
        const auto loads = [started] {
            return [started](
                     seastar::abort_source& stop) -> seastar::future<loaded> {
                ++*started;
                seastar::promise<> stopped;
                const auto asked = stop.subscribe(
                  [&stopped]() noexcept { stopped.set_value(); });
                if (asked) co_await stopped.get_future();
                co_return runtime::failure(failed(errc::unavailable));
            };
        };
        // Every slot is taken by a load that was started for nobody.
        for (std::uint64_t key = 0; key < 3; ++key)
            BOOST_CHECK(
              places.prefetch(*cache, key, byte_count{1_KiB}, loads()));
        struct stream final {
            std::uint64_t state;
            std::uint64_t below(std::uint64_t bound) {
                state ^= state << 13U;
                state ^= state >> 7U;
                state ^= state << 17U;
                return state % bound;
            }
        };
        stream random{0x9e3779b97f4a7c15ULL};
        std::deque<seastar::future<fetched>> waiting;
        std::size_t joined = 0;
        // Readers come for any key: for one that is loading they wait on
        // its load, and for any other they wait for a slot.
        for (std::size_t n = 0; n < callers; ++n) {
            const auto key = random.below(keys);
            if (key < 3) ++joined;
            waiting.push_back(
              places.fetch(*cache, key, byte_count{1_KiB}, loads(), caller));
        }
        BOOST_CHECK_GT(joined, 0U);
        BOOST_CHECK_LT(joined, callers);
        BOOST_CHECK_EQUAL(cache->snapshot().loads, 3U);
        BOOST_CHECK_EQUAL(cache->snapshot().waits, joined);
        co_await cache->stop();
        // Nobody is left waiting. Whoever waited on a load is told what
        // the stopped load returned, and whoever waited for a slot that
        // the cache is closed.
        std::size_t told = 0, closed = 0, other = 0;
        for (auto& pending : waiting) {
            const auto settled = co_await std::move(pending);
            if (is(settled, errc::unavailable))
                ++told;
            else if (is(settled, errc::closed))
                ++closed;
            else
                ++other;
        }
        BOOST_CHECK_EQUAL(other, 0U);
        BOOST_CHECK_EQUAL(told, joined);
        BOOST_CHECK_EQUAL(closed, callers - joined);
        // No load was started for any of them.
        BOOST_CHECK_EQUAL(*started, 3U);
        BOOST_CHECK_EQUAL(cache->snapshot().loads, 3U);
        BOOST_CHECK(places.empty());
        BOOST_CHECK_EQUAL(cache->snapshot().bytes, 0U);
    });
}

SEASTAR_TEST_CASE(page_cache_entry_outlives_its_object_for_those_holding_it) {
    co_await with_cache(
      [](workload_budget&, page_cache& cache) -> seastar::future<> {
          seastar::abort_source caller;
          std::optional<value_index> places{std::in_place};
          fill(cache, *places, 1, byte_count{4_KiB}, 10);
          fill(cache, *places, 2, byte_count{4_KiB}, 20);
          const auto unheld = cache.snapshot().bytes / 2;
          auto held = places->get(1);
          BOOST_REQUIRE(held);
          auto load = places->lead(cache, 3, byte_count{4_KiB});
          BOOST_REQUIRE(load.has_value());
          auto waiting = places->join(3, caller);
          const auto charged = cache.snapshot().bytes;
          // The object goes with a value held and a load in flight. Only
          // the value nothing held is given back.
          places.reset();
          BOOST_CHECK_EQUAL(cache.snapshot().bytes, charged - unheld);
          BOOST_CHECK_EQUAL(cache.snapshot().small_bytes, 0U);
          BOOST_CHECK_EQUAL(*held, 10U);
          // The load ends for those waiting on it, and nothing keeps it.
          std::move(*load).publish(30);
          auto loaded = co_await std::move(waiting);
          BOOST_REQUIRE(loaded.has_value());
          BOOST_CHECK_EQUAL(**loaded, 30U);
          BOOST_CHECK_EQUAL(cache.snapshot().small_bytes, 0U);
          held.reset();
          BOOST_CHECK_EQUAL(cache.snapshot().bytes, unheld);
          loaded->reset();
          BOOST_CHECK_EQUAL(cache.snapshot().bytes, 0U);
      });
}

SEASTAR_TEST_CASE(page_cache_bytes_outlive_the_cache_until_they_are_released) {
    co_await with_budget([](workload_budget& budget) -> seastar::future<> {
        auto cache = make_cache(budget, 1_MiB);
        value_slot ready, pending;
        seastar::abort_source caller;
        fill(*cache, ready, byte_count{4_KiB}, 3);
        auto held = ready.get();
        BOOST_REQUIRE(held);
        // A load some owner does itself, still in flight when the cache
        // stops.
        auto load = pending.lead(*cache, byte_count{4_KiB});
        BOOST_REQUIRE(load.has_value());
        auto waiting = pending.join(caller);
        co_await cache->stop();
        cache.reset();
        // The capacity stays admitted while anything is charged to it.
        BOOST_CHECK_EQUAL(budget.snapshot().tasks, 1U);
        BOOST_CHECK_EQUAL(*held, 3U);
        BOOST_CHECK(ready.empty() && pending.loading());
        std::move(*load).publish(4);
        {
            const auto settled = co_await std::move(waiting);
            BOOST_REQUIRE(settled.has_value());
            BOOST_CHECK_EQUAL(**settled, 4U);
        }
        // A value that outlived its cache is kept by nothing.
        BOOST_CHECK(pending.empty());
        BOOST_CHECK_EQUAL(budget.snapshot().tasks, 1U);
        held.reset();
        BOOST_CHECK_EQUAL(budget.snapshot().tasks, 0U);
    });
}

SEASTAR_TEST_CASE(page_cache_load_that_cannot_allocate_changes_nothing) {
    co_await with_cache(
      [](workload_budget&, page_cache& cache) -> seastar::future<> {
          value_slot place;
          value_index places;
          std::uint32_t starved = 0;
          bool unchanged = true, both = false;
          // Nothing here allocates but the loads themselves, one entry
          // each, so each run fails the next of them.
          seastar::memory::with_allocation_failures([&] {
              auto single = place.lead(cache, byte_count{4_KiB});
              if (!single) {
                  ++starved;
                  unchanged = unchanged && is(single, errc::resource_exhausted)
                              && place.empty() && cache.snapshot().bytes == 0;
                  return;
              }
              const auto one = cache.snapshot().bytes;
              auto keyed = places.lead(cache, 7, byte_count{4_KiB});
              if (!keyed) {
                  ++starved;
                  unchanged = unchanged && is(keyed, errc::resource_exhausted)
                              && places.empty()
                              && cache.snapshot().bytes == one;
                  return;
              }
              std::move(*single).publish(1);
              std::move(*keyed).publish(2);
              both = true;
          });
          BOOST_CHECK(unchanged && both);
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
          BOOST_CHECK_GT(starved, 0U);
#else
          BOOST_CHECK_EQUAL(starved, 0U);
#endif
          BOOST_CHECK_EQUAL(*place.get(), 1U);
          BOOST_CHECK_EQUAL(*places.get(7), 2U);
          co_return;
      });
}

SEASTAR_TEST_CASE(page_cache_fetch_that_cannot_allocate_keeps_nothing) {
    co_await with_cache([](workload_budget&, page_cache& cache) {
        // In a thread, so that a read can be waited for while allocations
        // are failed one after another.
        return seastar::async([&cache] {
            value_index places;
            seastar::abort_source caller;
            const auto populated = seastar::make_lw_shared<std::uint32_t>(0);
            const auto loads = [populated] {
                return [populated](seastar::abort_source&) {
                    ++*populated;
                    return seastar::yield().then(
                      [] { return loaded{value{5}}; });
                };
            };
            std::uint32_t rounds = 0, starved = 0;
            bool sound = true;
            // Two readers want one key, cold each time. Each run fails the
            // next allocation on the way, which is an entry's.
            seastar::memory::with_allocation_failures([&] {
                ++rounds;
                places.drop(5);
                const auto before = *populated;
                auto first = places.fetch(
                  cache, 5, byte_count{4_KiB}, loads(), caller);
                auto second = places.fetch(
                  cache, 5, byte_count{4_KiB}, loads(), caller);
                const auto one = first.get();
                const auto other = second.get();
                for (const auto* got : {&one, &other}) {
                    if (*got)
                        sound = sound && ***got == 5;
                    else {
                        ++starved;
                        sound = sound && is(*got, errc::resource_exhausted);
                    }
                }
                // A reader refused its entry started nothing, and whoever
                // came next loaded the value, once, for everyone after.
                sound = sound && *populated == before + 1 && (one || other)
                        && (!one || !other || &**one == &**other);
                const auto kept = places.get(5);
                sound = sound && kept && *kept == 5;
            });
            BOOST_CHECK(sound);
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
            BOOST_CHECK_GT(starved, 0U);
            BOOST_CHECK_GT(rounds, 1U);
#else
            BOOST_CHECK_EQUAL(starved, 0U);
            BOOST_CHECK_EQUAL(rounds, 1U);
#endif
            // Nothing remembers that memory ran short.
            const auto settled
              = places.fetch(cache, 5, byte_count{4_KiB}, never_loads, caller)
                  .get();
            BOOST_REQUIRE(settled.has_value());
            BOOST_CHECK_EQUAL(**settled, 5U);
        });
    });
}
