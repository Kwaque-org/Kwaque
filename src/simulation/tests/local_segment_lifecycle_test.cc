#include "src/simulation/fake_file_test_support.h"
#include "src/simulation/tests/local_store_fixture.h"
#include "src/simulation/virtual_time.h"
#include "src/storage/tests/local_store_contract.h"
#include "src/storage/tests/segment_lifecycle_contract.h"

#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <memory>
#include <optional>
#include <utility>

namespace {
using namespace kwaque;
using namespace kwaque::simulation;
using namespace kwaque::storage;
using namespace kwaque::storage::testing::store_contract;
using namespace kwaque::simulation::testing::local_store_fixture;

// What the fake has applied that may change the device: every object's
// writes, flushes, truncations, renames, removals and directory
// operations, and the objects it holds and has made.
struct counted_mutations final {
    fake_file_system* files;
    std::optional<std::uint64_t> operator()() const {
        const auto [held, made] = fake_file_test_access::object_counts(*files);
        std::uint64_t total = held + made;
        for (const auto point :
             {runtime::builtin_fault_point::file_write,
              runtime::builtin_fault_point::file_flush,
              runtime::builtin_fault_point::file_truncate,
              runtime::builtin_fault_point::file_rename,
              runtime::builtin_fault_point::file_remove,
              runtime::builtin_fault_point::directory_create,
              runtime::builtin_fault_point::directory_remove,
              runtime::builtin_fault_point::directory_sync})
            total += fake_file_test_access::total_occurrences(*files, point);
        return total;
    }
};
// The writes the fake is asked for to one file that reach past a size,
// from when it is watched until they are asked for.
class watched_writes final {
public:
    explicit watched_writes(fake_file_system& files)
      : files_(&files)
      , beyond_(std::make_shared<std::uint64_t>(0)) {}
    void watch(const runtime::file_path& path, std::uint64_t size) const {
        const auto object = take(
          fake_file_test_access::lookup(
            *files_,
            take(fake_file_test_access::resolve(*files_, path.value()))));
        *beyond_ = 0;
        fake_file_test_access::observe_writes(
          *files_,
          [object, size, beyond = beyond_](
            fake_object_id id,
            std::uint64_t position,
            std::size_t length) noexcept {
              if (id == object && position + length > size) ++*beyond;
          });
    }
    std::optional<std::uint64_t> beyond() const {
        fake_file_test_access::observe_writes(*files_, {});
        return *beyond_;
    }

private:
    fake_file_system* files_;
    std::shared_ptr<std::uint64_t> beyond_;
};

// The file a contract asks about before the write it sets to fail, and the
// writes the fake had been asked for to it then, found by a run with nothing
// set to fail. Creation order is deterministic, so a later environment makes
// the file the same object, and its next write is the one to fail.
struct write_target final {
    std::uint64_t object{0}, writes{0};
    // The contract's question for the run that finds the file: nothing is
    // set to fail.
    auto finder(fake_file_system& files) {
        return [this, &files](const runtime::file_path& path) {
            const auto found = take(
              fake_file_test_access::lookup(
                files,
                take(fake_file_test_access::resolve(files, path.value()))));
            object = found.value();
            writes = take(
              fake_file_test_access::occurrences(
                files, found, runtime::builtin_fault_point::file_write));
            return false;
        };
    }
};

} // namespace

SEASTAR_TEST_CASE(segment_lifecycle_fake_sealed_under_readers) {
    co_await with_local_store([](auto&&... shared) {
        return storage::testing::segment_lifecycle_contract::
          sealed_under_readers<simulation::monotonic_clock>(shared...);
    });
}

SEASTAR_TEST_CASE(segment_lifecycle_fake_sealed_on_demand) {
    co_await with_local_store([](auto&&... shared) {
        return storage::testing::segment_lifecycle_contract::sealed_on_demand<
          simulation::monotonic_clock>(shared...);
    });
}

SEASTAR_TEST_CASE(segment_lifecycle_fake_one_transition_at_a_time) {
    co_await with_local_store([](auto&&... shared) {
        return storage::testing::segment_lifecycle_contract::
          one_transition_at_a_time<simulation::monotonic_clock>(shared...);
    });
}

SEASTAR_TEST_CASE(segment_lifecycle_fake_roll_reported) {
    co_await with_local_store([](auto&&... shared) {
        return storage::testing::segment_lifecycle_contract::roll_reported<
          simulation::monotonic_clock>(shared...);
    });
}

SEASTAR_TEST_CASE(segment_lifecycle_fake_age_reported) {
    co_await with_local_store([](auto&&... shared) {
        return storage::testing::segment_lifecycle_contract::age_reported<
          simulation::monotonic_clock>(shared...);
    });
}

SEASTAR_TEST_CASE(segment_lifecycle_fake_created_ahead) {
    co_await with_local_store([](auto&&... shared) {
        return storage::testing::segment_lifecycle_contract::created_ahead<
          simulation::monotonic_clock>(shared...);
    });
}

SEASTAR_TEST_CASE(segment_lifecycle_fake_prepared_ahead) {
    co_await with_local_store([](auto& files, auto&&... shared) {
        return storage::testing::segment_lifecycle_contract::prepared_ahead<
          simulation::monotonic_clock>(
          files, shared..., counted_mutations{&files});
    });
}

SEASTAR_TEST_CASE(segment_lifecycle_fake_recycled_on_removal) {
    co_await with_local_store([](auto& files, auto&&... shared) {
        return storage::testing::segment_lifecycle_contract::
          recycled_on_removal<simulation::monotonic_clock>(
            files, shared..., watched_writes{files});
    });
}

SEASTAR_TEST_CASE(segment_lifecycle_fake_recycled_hundred_times) {
    co_await with_local_store(
      [](auto&&... shared) {
          return storage::testing::segment_lifecycle_contract::
            recycled_hundred_times<simulation::monotonic_clock>(shared...);
      },
      {},
      std::nullopt,
      true);
}

SEASTAR_TEST_CASE(segment_lifecycle_fake_failure_kinds) {
    co_await with_local_store([](auto&&... shared) {
        return storage::testing::segment_lifecycle_contract::failure_kinds<
          simulation::monotonic_clock>(shared...);
    });
}

// Five seals over four slots hold more than the other cases: the long
// run's device handles, and more handle credits.
SEASTAR_TEST_CASE(segment_lifecycle_fake_sealed_in_slots) {
    co_await with_local_store(
      [](auto&&... shared) {
          return storage::testing::segment_lifecycle_contract::sealed_in_slots<
            simulation::monotonic_clock>(shared...);
      },
      {},
      std::nullopt,
      true,
      48);
}

// The run's write as its segment is let go fails for want of space, and
// the seal writes it from the WAL. A run with nothing failing finds that
// write's file and count first.
SEASTAR_TEST_CASE(segment_lifecycle_fake_running_seal_of_held) {
    write_target found;
    co_await with_local_store([&found](auto& files, auto&&... shared) {
        return storage::testing::segment_lifecycle_contract::
          running_seal_of_held<simulation::monotonic_clock>(
            files, shared..., found.finder(files));
    });
    BOOST_REQUIRE_NE(found.object, 0U);
    const auto next = take(runtime::fault_occurrence::make(found.writes + 1));
    co_await with_local_store(
      [](auto&&... shared) {
          return storage::testing::segment_lifecycle_contract::
            running_seal_of_held<simulation::monotonic_clock>(
              shared..., [](const runtime::file_path&) { return true; });
      },
      {take(
        fault_rule::make(
          take(fault_rule_id::make(1)),
          runtime::builtin_fault_point::file_write,
          runtime::fault_object_key::from_u64(found.object),
          next,
          next,
          fault_selector::once(),
          take(
            runtime::fault_decision::make_file_failure(
              runtime::fault_action::file_failure_before_effect,
              runtime::file_failure_detail::no_space))))});
}

// The segment's second group's write meets no space, and so does the
// seal's first write after it. A run with nothing failing finds that
// write's file and count first.
SEASTAR_TEST_CASE(segment_lifecycle_fake_running_seal_without_room) {
    write_target found;
    co_await with_local_store([&found](auto& files, auto&&... shared) {
        return storage::testing::segment_lifecycle_contract::
          running_seal_without_room<simulation::monotonic_clock>(
            files, shared..., found.finder(files));
    });
    BOOST_REQUIRE_NE(found.object, 0U);
    co_await with_local_store(
      [](auto&&... shared) {
          return storage::testing::segment_lifecycle_contract::
            running_seal_without_room<simulation::monotonic_clock>(
              shared..., [](const runtime::file_path&) { return true; });
      },
      {take(
        fault_rule::make(
          take(fault_rule_id::make(1)),
          runtime::builtin_fault_point::file_write,
          runtime::fault_object_key::from_u64(found.object),
          take(runtime::fault_occurrence::make(found.writes + 1)),
          take(runtime::fault_occurrence::make(found.writes + 2)),
          fault_selector::bounded_range(),
          take(
            runtime::fault_decision::make_file_failure(
              runtime::fault_action::file_failure_before_effect,
              runtime::file_failure_detail::no_space))))});
}

SEASTAR_TEST_CASE(segment_lifecycle_fake_running_seal) {
    co_await with_local_store([](auto&&... shared) {
        return storage::testing::segment_lifecycle_contract::running_seal<
          simulation::monotonic_clock>(shared...);
    });
}

SEASTAR_TEST_CASE(segment_lifecycle_fake_idle_cost) {
    co_await with_local_store([](auto&&... shared) {
        return storage::testing::segment_lifecycle_contract::idle_cost<
          simulation::monotonic_clock>(shared...);
    });
}

SEASTAR_TEST_CASE(segment_lifecycle_fake_device_watched) {
    co_await with_local_store([](auto&&... shared) {
        return storage::testing::segment_lifecycle_contract::device_watched<
          simulation::monotonic_clock>(shared...);
    });
}

SEASTAR_TEST_CASE(segment_lifecycle_fake_placed_by_share) {
    co_await with_local_store([](auto&&... shared) {
        return storage::testing::segment_lifecycle_contract::placed_by_share<
          simulation::monotonic_clock>(shared...);
    });
}

SEASTAR_TEST_CASE(segment_lifecycle_fake_kept_until_authorized) {
    co_await with_local_store([](auto&&... shared) {
        return storage::testing::segment_lifecycle_contract::
          kept_until_authorized<simulation::monotonic_clock>(shared...);
    });
}

SEASTAR_TEST_CASE(segment_lifecycle_fake_removed_when_authorized) {
    co_await with_local_store([](auto&&... shared) {
        return storage::testing::segment_lifecycle_contract::
          removed_when_authorized<simulation::monotonic_clock>(shared...);
    });
}

SEASTAR_TEST_CASE(segment_lifecycle_fake_failed_segment_discarded) {
    co_await with_local_store([](auto&&... shared) {
        return storage::testing::segment_lifecycle_contract::
          failed_segment_discarded<simulation::monotonic_clock>(shared...);
    });
}

// The segment's next write after its first request fails: the requests in
// flight behind it end above the prefix the failed segment reports. A run
// with nothing failing finds that write's file and count first.
SEASTAR_TEST_CASE(segment_lifecycle_fake_failed_write_above_prefix) {
    write_target found;
    co_await with_local_store([&found](auto& files, auto&&... shared) {
        return storage::testing::segment_lifecycle_contract::
          failed_write_above_prefix<simulation::monotonic_clock>(
            files, shared..., found.finder(files));
    });
    BOOST_REQUIRE_NE(found.object, 0U);
    const auto next = take(runtime::fault_occurrence::make(found.writes + 1));
    co_await with_local_store(
      [](auto&&... shared) {
          return storage::testing::segment_lifecycle_contract::
            failed_write_above_prefix<simulation::monotonic_clock>(
              shared..., [](const runtime::file_path&) { return true; });
      },
      {take(
        fault_rule::make(
          take(fault_rule_id::make(1)),
          runtime::builtin_fault_point::file_write,
          runtime::fault_object_key::from_u64(found.object),
          next,
          next,
          fault_selector::once(),
          take(
            runtime::fault_decision::make_file_failure(
              runtime::fault_action::file_failure_before_effect,
              runtime::file_failure_detail::device_io))))});
}

SEASTAR_TEST_CASE(segment_lifecycle_fake_removal_resumed) {
    co_await with_local_store([](auto&&... shared) {
        return storage::testing::segment_lifecycle_contract::removal_resumed<
          simulation::monotonic_clock>(shared...);
    });
}

SEASTAR_TEST_CASE(segment_lifecycle_fake_removal_against_transitions) {
    co_await with_local_store([](auto&&... shared) {
        return storage::testing::segment_lifecycle_contract::
          removal_against_transitions<simulation::monotonic_clock>(shared...);
    });
}

SEASTAR_TEST_CASE(segment_lifecycle_fake_revoked_in_flight) {
    co_await with_local_store([](auto&&... shared) {
        return storage::testing::segment_lifecycle_contract::revoked_in_flight<
          simulation::monotonic_clock>(shared...);
    });
}

// The unlink of the segment's data file fails once. A first environment
// finds the file: creation order is deterministic, so the later ones make
// it the same object. Once for a segment the table had let go, whose
// removal is found again by its name, and once for one it still held.
SEASTAR_TEST_CASE(segment_lifecycle_fake_removal_retried) {
    std::uint64_t object = 0;
    co_await with_local_store(
      [&object](
        auto& files,
        auto& owner,
        const auto& spec,
        auto& budget,
        auto drive,
        auto& timer) -> seastar::future<> {
          co_await storage::testing::segment_lifecycle_contract::
            removal_retried<simulation::monotonic_clock>(
              files,
              owner,
              spec,
              budget,
              drive,
              timer,
              false,
              false,
              [&](const runtime::file_path& data) {
                  object = take(
                             fake_file_test_access::lookup(
                               files,
                               take(
                                 fake_file_test_access::resolve(
                                   files, data.value()))))
                             .value();
              });
      });
    BOOST_REQUIRE(object != 0);
    const auto first = take(runtime::fault_occurrence::make(1));
    for (const bool held : {false, true}) {
        std::vector<fault_rule> faults;
        faults.push_back(take(
          fault_rule::make(
            take(fault_rule_id::make(1)),
            runtime::builtin_fault_point::file_remove,
            runtime::fault_object_key::from_u64(object),
            first,
            first,
            fault_selector::once(),
            runtime::fault_decision::make_error())));
        co_await with_local_store(
          [held](auto&&... shared) {
              return storage::testing::segment_lifecycle_contract::
                removal_retried<simulation::monotonic_clock>(
                  shared..., true, held, [](const runtime::file_path&) {});
          },
          std::move(faults));
    }
}
