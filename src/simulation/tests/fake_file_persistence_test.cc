#include "src/runtime/file.h"
#include "src/simulation/fake_file.h"
#include "src/simulation/fake_file_test_support.h"
#include "src/simulation/fault_schedule.h"
#include "src/simulation/scheduler_driver.h"
#include "src/simulation/storage_fault_key.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/memory.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/alloc_failure_injector.hh>
#include <seastar/util/later.hh>

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <array>
#include <exception>
#include <memory>
#include <optional>
#include <set>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

namespace {
using namespace kwaque;
using namespace kwaque::simulation;
using kwaque::simulation::testing::pump_until;
using runtime::builtin_fault_point;
using runtime::fault_action;
using runtime::fault_decision;
using runtime::file_failure_detail;
constexpr std::uint64_t seed = 71;

template<typename T, typename Error>
T checked(std::expected<T, Error> result) {
    BOOST_REQUIRE(result.has_value());
    return std::move(*result);
}
template<typename Error>
void checked(std::expected<void, Error> result) {
    BOOST_REQUIRE(result.has_value());
}

scheduler_limits scheduler_budget() {
    return checked(
      scheduler_limits::make(
        {.pending_events = 512,
         .events_per_pump = 64,
         .total_events = 100000,
         .maximum_deadline = runtime::monotonic_time{1000000}}));
}
trace_limits trace_budget(std::uint32_t entries = 65536) {
    return checked(
      trace_limits::make(
        {.entries = entries,
         .encoded_bytes = canonical_header_encoded_size
                          + static_cast<std::uint64_t>(entries)
                              * canonical_entry_encoded_size,
         .line_bytes = 1024}));
}
fake_file_system_config config(
  std::optional<fake_crash_policy> policy = std::nullopt,
  std::uint64_t scope = 1) {
    fake_file_system_config result;
    result.logical_capacity = byte_count{16U * 1024U * 1024U};
    result.maximum_objects = 256;
    result.maximum_open_handles = 16;
    result.maximum_pending_operations = 16;
    result.crash_policy = policy;
    result.device_scope = scope;
    return result;
}
trace_header
header(const scheduler_limits& scheduling, const trace_limits& tracing) {
    return trace_header::current(
      seed,
      deterministic_random_algorithm_version,
      deterministic_random_coordinate_version,
      simulation::trace_budget(scheduling),
      tracing,
      {},
      {});
}
struct fixture final {
    trace_limits limits;
    event_trace owned_trace;
    scheduler events;
    std::unique_ptr<fault_schedule> faults;
    std::unique_ptr<fake_file_system> files;
    explicit fixture(
      fake_file_system_config configuration = config(),
      seastar::chunked_vector<fault_rule> rules = {},
      std::uint32_t entries = 65536,
      event_trace* replay = nullptr)
      : limits(trace_budget(entries))
      , owned_trace(header(scheduler_budget(), limits), limits)
      , events(scheduler_budget(), replay ? replay : &owned_trace) {
        faults = checked(
          fault_schedule::make(
            events, replay ? *replay : owned_trace, seed, std::move(rules)));
        files = checked(
          fake_file_system::make(std::move(configuration), events, *faults));
    }
    template<typename T>
    seastar::future<T> drive(seastar::future<T> waiting) {
        co_await pump_until(events, waiting);
        co_return co_await std::move(waiting);
    }
    canonical_fake_path path(std::string_view name) {
        return checked(
          fake_file_test_access::resolve(
            *files, std::string{"/kwaque/"} + std::string{name}));
    }
    runtime::file_path public_path(std::string_view name) {
        return checked(
          runtime::file_path::make(
            std::string{"/kwaque/"} + std::string{name}));
    }
    void create(std::string_view name) {
        static_cast<void>(
          checked(fake_file_test_access::create_file(*files, path(name))));
    }
    void write(std::string_view name, char value, std::uint64_t position = 0) {
        const std::string data(4096, value);
        static_cast<void>(checked(
          fake_file_test_access::write(
            *files,
            path(name),
            position,
            std::as_bytes(std::span{data.data(), data.size()}))));
    }
    void sync(std::string_view name) {
        checked(fake_file_test_access::flush(*files, path(name)));
    }
    void sync_root() {
        checked(fake_file_test_access::sync_directory(*files, path("")));
    }
    std::string read(
      std::string_view name,
      std::uint64_t position = 0,
      std::size_t length = 4096) {
        std::array<std::byte, 8192> data{};
        BOOST_REQUIRE_LE(length, data.size());
        const auto bytes = checked(
          fake_file_test_access::read(
            *files,
            path(name),
            position,
            std::span<std::byte>{data.data(), length}));
        return std::string{
          reinterpret_cast<const char*>(data.data()),
          static_cast<std::size_t>(bytes.value())};
    }
    std::set<std::string> names() {
        auto entries = checked(fake_file_test_access::list(*files, path("")));
        std::set<std::string> result;
        for (const auto& entry : entries)
            result.insert(entry.name);
        return result;
    }
};

fault_rule rule(
  builtin_fault_point point,
  fault_decision decision,
  std::optional<runtime::fault_object_key> object = std::nullopt) {
    return checked(
      fault_rule::make(
        checked(fault_rule_id::make(1)),
        point,
        object,
        runtime::fault_occurrence::first(),
        runtime::fault_occurrence::first(),
        fault_selector::once(),
        decision));
}
bytes::fragmented_buffer payload(char value) {
    const std::string data(4096, value);
    return checked(
      bytes::fragmented_buffer::copy_of(std::span{data.data(), data.size()}));
}
void stable_file(fixture& test, std::string_view name, char value) {
    test.create(name);
    test.write(name, value);
    test.sync(name);
    test.sync_root();
}
} // namespace

SEASTAR_TEST_CASE(
  selective_crash_preserves_synced_bytes_and_zero_full_endpoints) {
    for (const auto percent : {std::uint8_t{0}, std::uint8_t{100}}) {
        fixture test{config(
          fake_crash_policy{
            .data_percent = percent,
            .namespace_percent = percent,
            .eof_percent = percent})};
        stable_file(test, "file", 'a');
        test.write("file", 'b');
        test.write("file", 'c', 4096);
        checked(co_await test.drive(test.files->crash()));
        BOOST_CHECK_EQUAL(
          test.read("file"), std::string(4096, percent == 0 ? 'a' : 'b'));
        BOOST_CHECK_EQUAL(
          test.read("file", 4096),
          percent == 0 ? std::string{} : std::string(4096, 'c'));
        const auto first = checked(
          fake_file_test_access::snapshot(*test.files));
        checked(co_await test.drive(test.files->crash()));
        const auto second = checked(
          fake_file_test_access::snapshot(*test.files));
        BOOST_CHECK_EQUAL(
          test.read("file"), std::string(4096, percent == 0 ? 'a' : 'b'));
        BOOST_CHECK_EQUAL(first.namespace_groups, 0U);
        BOOST_CHECK_EQUAL(second.namespace_groups, 0U);
        BOOST_CHECK_EQUAL(second.crash_epoch, first.crash_epoch + 1U);
    }
}

SEASTAR_TEST_CASE(selective_crash_separates_eof_from_bytes_and_cleared_tail) {
    fixture test{config(
      fake_crash_policy{
        .data_percent = 100, .namespace_percent = 0, .eof_percent = 100})};
    stable_file(test, "file", 'a');
    checked(
      fake_file_test_access::truncate(*test.files, test.path("file"), 1000));
    checked(
      fake_file_test_access::truncate(*test.files, test.path("file"), 4096));
    checked(co_await test.drive(test.files->crash()));
    BOOST_CHECK_EQUAL(
      test.read("file"), std::string(1000, 'a') + std::string(3096, '\0'));
    checked(
      fake_file_test_access::truncate(*test.files, test.path("file"), 700));
    checked(co_await test.drive(test.files->crash()));
    BOOST_CHECK_EQUAL(test.read("file"), std::string(700, 'a'));
    checked(
      fake_file_test_access::truncate(*test.files, test.path("file"), 4096));
    BOOST_CHECK_EQUAL(
      test.read("file"), std::string(700, 'a') + std::string(3396, '\0'));

    fixture length_only{config(
      fake_crash_policy{
        .data_percent = 0, .namespace_percent = 100, .eof_percent = 100})};
    length_only.create("file");
    length_only.write("file", 'x');
    checked(co_await length_only.drive(length_only.files->crash()));
    BOOST_CHECK_EQUAL(length_only.read("file"), std::string(4096, '\0'));
}

SEASTAR_TEST_CASE(
  selective_crash_granules_are_partial_stable_and_device_scoped) {
    std::array<std::string, 3> observed;
    for (std::size_t run = 0; run < observed.size(); ++run) {
        fixture test{config(
          fake_crash_policy{
            .data_percent = 50, .eof_percent = 0, .granule_bytes = 64},
          run == 2 ? 2 : 1)};
        stable_file(test, "file", 'a');
        if (run == 1) {
            deterministic_random unrelated{seed};
            auto stream = checked(
              unrelated.stream(random_domain::runtime_stream, 987));
            for (unsigned index = 0; index < 1000; ++index)
                static_cast<void>(stream.next_u64());
        }
        test.write("file", 'b');
        checked(co_await test.drive(test.files->crash()));
        observed[run] = test.read("file");
        for (std::size_t offset = 0; offset < observed[run].size();
             offset += 64) {
            const auto granule = observed[run].substr(offset, 64);
            BOOST_CHECK(
              granule == std::string(64, 'a')
              || granule == std::string(64, 'b'));
        }
        BOOST_CHECK(observed[run].find('a') != std::string::npos);
        BOOST_CHECK(observed[run].find('b') != std::string::npos);
    }
    BOOST_CHECK_EQUAL(observed[0], observed[1]);
    BOOST_CHECK(observed[0] != observed[2]);
}

SEASTAR_TEST_CASE(
  selective_namespace_rename_keeps_before_images_and_atomic_endpoints) {
    for (const auto percent : {std::uint8_t{0}, std::uint8_t{100}}) {
        fixture test{config(fake_crash_policy{.namespace_percent = percent})};
        stable_file(test, "a", 'a');
        stable_file(test, "b", 'b');
        checked(
          co_await test.drive(
            test.files->rename(test.public_path("a"), test.public_path("b"))));
        checked(co_await test.drive(test.files->crash()));
        BOOST_CHECK_EQUAL(
          test.read("b"), std::string(4096, percent == 0 ? 'b' : 'a'));
        BOOST_CHECK(
          test.names()
          == (percent == 0 ? std::set<std::string>{"a", "b"} : std::set<std::string>{"b"}));
        checked(co_await test.drive(test.files->crash()));
        BOOST_CHECK_EQUAL(
          test.read("b"), std::string(4096, percent == 0 ? 'b' : 'a'));
        const auto snapshot = checked(
          fake_file_test_access::snapshot(*test.files));
        for (const auto& object : snapshot.objects)
            BOOST_CHECK_EQUAL(object.history_references, 0U);
        BOOST_CHECK_EQUAL(snapshot.objects.size(), percent == 0 ? 3U : 2U);
    }
}

SEASTAR_TEST_CASE(
  selective_namespace_independent_names_and_parent_dependencies) {
    fixture test{config(fake_crash_policy{.namespace_percent = 50})};
    for (unsigned index = 0; index < 32; ++index)
        test.create("f" + std::to_string(index));
    checked(
      co_await test.drive(
        test.files->create_directories(test.public_path("parent/child"))));
    test.create("parent/child/file");
    checked(co_await test.drive(test.files->crash()));
    const auto names = test.names();
    BOOST_CHECK(!names.empty());
    BOOST_CHECK_LT(names.size(), 33U);
    const auto snapshot = checked(fake_file_test_access::snapshot(*test.files));
    for (const auto& object : snapshot.objects) {
        BOOST_CHECK_EQUAL(object.history_references, 0U);
        BOOST_CHECK(object.id == 1 || object.visible_links != 0);
        BOOST_CHECK(object.visible_entries == object.durable_entries);
    }
}

SEASTAR_TEST_CASE(
  selective_crash_cannot_undo_successful_file_or_directory_sync) {
    fixture test{config(fake_crash_policy{})};
    stable_file(test, "a", 'a');
    test.write("a", 'b');
    test.sync("a");
    checked(
      co_await test.drive(
        test.files->rename(test.public_path("a"), test.public_path("b"))));
    test.sync_root();
    checked(co_await test.drive(test.files->crash()));
    BOOST_CHECK(test.names() == std::set<std::string>{"b"});
    BOOST_CHECK_EQUAL(test.read("b"), std::string(4096, 'b'));
}

SEASTAR_TEST_CASE(typed_write_failures_report_no_partial_and_complete_effects) {
    for (const auto action :
         {fault_action::file_failure_before_effect,
          fault_action::file_failure_after_prefix,
          fault_action::file_failure_after_effect}) {
        seastar::chunked_vector<fault_rule> rules;
        rules.push_back(rule(
          builtin_fault_point::file_write,
          checked(
            fault_decision::make_file_failure(
              action,
              file_failure_detail::no_space,
              byte_count{
                action == fault_action::file_failure_after_prefix ? 512U
                                                                  : 0U}))));
        fixture test{config(), std::move(rules)};
        stable_file(test, "file", 'a');
        auto owner = checked(
          co_await test.drive(test.files->open(
            test.public_path("file"),
            {.access = runtime::file_access::read_write})));
        const auto failed = co_await test.drive(
          owner.write(runtime::file_position{}, payload('b')));
        const auto reflush = co_await test.drive(owner.flush());
        checked(co_await test.drive(owner.close()));
        BOOST_REQUIRE(!failed.has_value());
        BOOST_CHECK(failed.error().code() == errc::resource_exhausted);
        BOOST_CHECK(
          checked(runtime::file_detail(failed.error()))
          == file_failure_detail::no_space);
        BOOST_REQUIRE(!reflush.has_value());
        const auto bytes = action == fault_action::file_failure_before_effect
                             ? 0U
                           : action == fault_action::file_failure_after_prefix
                             ? 512U
                             : 4096U;
        BOOST_CHECK_EQUAL(
          test.read("file"),
          std::string(bytes, 'b') + std::string(4096U - bytes, 'a'));
        checked(co_await test.drive(test.files->crash()));
        BOOST_CHECK_EQUAL(test.read("file"), std::string(4096, 'a'));
    }
}

SEASTAR_TEST_CASE(
  failed_flush_notification_does_not_prove_loss_or_allow_healing) {
    for (const auto action :
         {fault_action::file_failure_before_effect,
          fault_action::file_failure_after_effect}) {
        seastar::chunked_vector<fault_rule> rules;
        rules.push_back(rule(
          builtin_fault_point::file_flush,
          checked(
            fault_decision::make_file_failure(
              action, file_failure_detail::device_io))));
        fixture test{config(), std::move(rules)};
        stable_file(test, "file", 'a');
        auto owner = checked(
          co_await test.drive(test.files->open(
            test.public_path("file"),
            {.access = runtime::file_access::read_write})));
        static_cast<void>(checked(
          co_await test.drive(
            owner.write(runtime::file_position{}, payload('b')))));
        const auto failed = co_await test.drive(owner.flush());
        const auto attempts = fake_file_test_access::submitted(
          *test.files, fake_submission_kind::flush);
        const auto repeated = co_await test.drive(owner.flush());
        checked(co_await test.drive(owner.close()));
        BOOST_REQUIRE(!failed.has_value());
        BOOST_REQUIRE(!repeated.has_value());
        BOOST_CHECK(repeated.error() == failed.error());
        BOOST_CHECK_EQUAL(
          fake_file_test_access::submitted(
            *test.files, fake_submission_kind::flush),
          attempts);
        checked(co_await test.drive(test.files->crash()));
        BOOST_CHECK_EQUAL(
          test.read("file"),
          std::string(
            4096,
            action == fault_action::file_failure_after_effect ? 'b' : 'a'));
    }
}

SEASTAR_TEST_CASE(
  typed_native_transport_preserves_every_cause_and_legacy_unknown) {
    for (const auto cause :
         {file_failure_detail::unknown,
          file_failure_detail::no_space,
          file_failure_detail::descriptor_limit,
          file_failure_detail::read_only,
          file_failure_detail::permission,
          file_failure_detail::device_io,
          file_failure_detail::quota}) {
        seastar::chunked_vector<fault_rule> rules;
        rules.push_back(rule(
          builtin_fault_point::file_flush,
          checked(
            fault_decision::make_file_failure(
              fault_action::file_failure_before_effect, cause))));
        fixture test{config(), std::move(rules)};
        stable_file(test, "file", 'a');
        auto owner = checked(
          co_await test.drive(test.files->open(test.public_path("file"), {})));
        const auto failed = co_await test.drive(owner.flush());
        checked(co_await test.drive(owner.close()));
        BOOST_REQUIRE(!failed.has_value());
        BOOST_CHECK(checked(runtime::file_detail(failed.error())) == cause);
        BOOST_CHECK(failed.error().code() == runtime::file_failure_code(cause));
        BOOST_REQUIRE_EQUAL(failed.error().context_size(), 4U);
        BOOST_CHECK(
          failed.error().context_at(0)->key
          == runtime::operation_context_key::detail);
        BOOST_CHECK(
          failed.error().context_at(1)->key
          == runtime::operation_context_key::bytes);
        BOOST_CHECK_EQUAL(failed.error().context_at(1)->value, 0U);
        BOOST_CHECK_EQUAL(failed.error().context_at(2)->value, 2U);
        BOOST_CHECK_EQUAL(failed.error().context_at(3)->value, 2U);
    }
    seastar::chunked_vector<fault_rule> rules;
    rules.push_back(
      rule(builtin_fault_point::file_flush, fault_decision::make_error()));
    fixture legacy{config(), std::move(rules)};
    stable_file(legacy, "file", 'a');
    auto owner = checked(
      co_await legacy.drive(
        legacy.files->open(legacy.public_path("file"), {})));
    auto failed = co_await legacy.drive(owner.flush());
    auto retried = co_await legacy.drive(owner.flush());
    checked(co_await legacy.drive(owner.close()));
    BOOST_REQUIRE(!failed.has_value());
    BOOST_CHECK(failed.error().code() == errc::io_failure);
    BOOST_CHECK(
      checked(runtime::file_detail(failed.error()))
      == file_failure_detail::unknown);
    BOOST_REQUIRE(retried.has_value());
}

SEASTAR_TEST_CASE(
  selective_crash_rejects_insufficient_scratch_and_trace_before_effect) {
    for (const bool trace_pressure : {false, true}) {
        auto policy = fake_crash_policy{
          .data_percent = 100, .namespace_percent = 100, .eof_percent = 100};
        if (!trace_pressure)
            policy.maximum_scratch_bytes = byte_count{16U * 1024U};
        fixture test{config(policy), {}, trace_pressure ? 32U : 65536U};
        stable_file(test, "file", 'a');
        test.write("file", 'b');
        const auto before = checked(
          fake_file_test_access::snapshot(*test.files));
        const auto failed = co_await test.drive(test.files->crash());
        const auto after = checked(
          fake_file_test_access::snapshot(*test.files));
        BOOST_REQUIRE(!failed.has_value());
        BOOST_CHECK(failed.error().code() == errc::resource_exhausted);
        BOOST_CHECK_EQUAL(before.crash_epoch, after.crash_epoch);
        BOOST_CHECK_EQUAL(before.retained_capacity, after.retained_capacity);
        BOOST_CHECK(before.objects == after.objects);
        BOOST_CHECK_EQUAL(test.read("file"), std::string(4096, 'b'));
        BOOST_CHECK(test.files->state() == fake_file_system_state::open);
    }
}

SEASTAR_TEST_CASE(
  selective_crash_stop_joins_suspended_work_and_rolls_back_preparation) {
    fixture test{config(
      fake_crash_policy{
        .data_percent = 50, .eof_percent = 100, .granule_bytes = 64})};
    stable_file(test, "file", 'a');
    for (std::uint64_t page = 0; page < 64; ++page)
        test.write("file", 'b', page * 4096U);
    auto crashing = test.files->crash();
    checked(test.events.advance_to_next());
    BOOST_REQUIRE(checked(test.events.step()));
    BOOST_REQUIRE(checked(test.events.step()));
    const bool suspended = !crashing.available()
                           && test.files->state()
                                == fake_file_system_state::crashing;
    auto during = co_await test.files->stat(test.public_path("file"));
    auto stopping = test.files->stop();
    const auto stopped = co_await test.drive(std::move(stopping));
    const auto crashed = co_await std::move(crashing);
    BOOST_CHECK(suspended);
    BOOST_REQUIRE(!during.has_value());
    BOOST_CHECK(during.error().code() == errc::unavailable);
    BOOST_REQUIRE(stopped.has_value());
    BOOST_REQUIRE(!crashed.has_value());
    BOOST_CHECK(crashed.error().code() == errc::aborted);
    BOOST_CHECK(test.files->state() == fake_file_system_state::stopped);
    BOOST_CHECK_EQUAL(
      checked(
        fake_file_test_access::volatile_page_count(
          *test.files, test.path("file"))),
      64U);
    BOOST_CHECK_EQUAL(test.read("file"), std::string(4096, 'b'));
    BOOST_CHECK_EQUAL(test.files->pending_operations(), 0U);
}

SEASTAR_TEST_CASE(selective_crash_allocation_failure_preserves_both_versions) {
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    fixture test{
      config(fake_crash_policy{.data_percent = 50, .granule_bytes = 64})};
    stable_file(test, "file", 'a');
    test.write("file", 'b');
    const auto before = checked(fake_file_test_access::snapshot(*test.files));
    auto crashing = test.files->crash();
    checked(test.events.advance_to_next());
    BOOST_REQUIRE(checked(test.events.step()));
    auto& injector = seastar::memory::local_failure_injector();
    injector.fail_after(0);
    const auto stepped = test.events.step();
    const bool injected = injector.failed();
    injector.cancel();
    bool allocation_failure = false;
    try {
        static_cast<void>(co_await test.drive(std::move(crashing)));
    } catch (const std::bad_alloc&) {
        allocation_failure = true;
    } catch (const std::runtime_error& error) {
        // The C allocator returns null; the digest wrapper reports that
        // failure as an exception at its own boundary.
        allocation_failure = std::string_view{error.what()}
                             == "failed to allocate SHA-256 context";
        BOOST_CHECK_MESSAGE(allocation_failure, error.what());
    }
    BOOST_REQUIRE(stepped.has_value());
    BOOST_CHECK(injected && allocation_failure);
    const auto after = checked(fake_file_test_access::snapshot(*test.files));
    BOOST_CHECK(before.objects == after.objects);
    BOOST_CHECK_EQUAL(before.retained_capacity, after.retained_capacity);
    BOOST_CHECK_EQUAL(after.crash_epoch, 0U);
    BOOST_CHECK(test.files->state() == fake_file_system_state::open);
    BOOST_CHECK_EQUAL(test.files->pending_operations(), 0U);
#endif
    co_return;
}

SEASTAR_TEST_CASE(
  recursive_create_allocation_failures_roll_back_staged_inodes) {
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    for (const bool history : {false, true}) {
        bool reached_success = false;
        unsigned failures = 0;
        for (unsigned allocation = 0; allocation < 256 && !reached_success;
             ++allocation) {
            fixture test{config(
              history ? std::optional{fake_crash_policy{}} : std::nullopt)};
            const auto before = checked(
              fake_file_test_access::snapshot(*test.files));
            auto creating = test.files->create_directories(test.public_path(
              std::string(96, 'a') + "/" + std::string(96, 'b') + "/c"));
            checked(test.events.advance_to_next());
            auto& injector = seastar::memory::local_failure_injector();
            injector.fail_after(allocation);
            const auto stepped = test.events.step();
            const bool injected = injector.failed();
            injector.cancel();
            bool failed = false;
            try {
                checked(co_await test.drive(std::move(creating)));
            } catch (const std::bad_alloc&) {
                failed = true;
                ++failures;
            }
            BOOST_REQUIRE(stepped.has_value() && *stepped);
            const auto after = checked(
              fake_file_test_access::snapshot(*test.files));
            BOOST_CHECK_EQUAL(after.pending_operations, 0U);
            BOOST_CHECK_EQUAL(after.open_handles, 0U);
            if (failed) {
                BOOST_CHECK(injected);
                BOOST_REQUIRE(before.objects == after.objects);
                BOOST_CHECK(
                  before.namespace_history == after.namespace_history);
                BOOST_CHECK_EQUAL(before.next_object_id, after.next_object_id);
                BOOST_CHECK_EQUAL(
                  before.next_namespace_sequence,
                  after.next_namespace_sequence);
                BOOST_CHECK_EQUAL(
                  before.history_name_bytes, after.history_name_bytes);
                BOOST_CHECK_EQUAL(
                  before.retained_path_bytes, after.retained_path_bytes);
            } else {
                reached_success = !injected;
                BOOST_CHECK_EQUAL(
                  after.objects.size(), before.objects.size() + 3U);
                BOOST_CHECK_EQUAL(after.namespace_groups, history ? 3U : 0U);
            }
        }
        BOOST_CHECK(reached_success);
        BOOST_CHECK_GT(failures, 0U);
    }
#endif
    co_return;
}

SEASTAR_TEST_CASE(typed_namespace_failure_can_have_a_complete_atomic_effect) {
    seastar::chunked_vector<fault_rule> rules;
    rules.push_back(rule(
      builtin_fault_point::file_rename,
      checked(
        fault_decision::make_file_failure(
          fault_action::file_failure_after_effect,
          file_failure_detail::permission))));
    fixture test{config(fake_crash_policy{}), std::move(rules)};
    stable_file(test, "a", 'a');
    stable_file(test, "b", 'b');
    const auto failed = co_await test.drive(
      test.files->rename(test.public_path("a"), test.public_path("b")));
    BOOST_REQUIRE(!failed.has_value());
    BOOST_CHECK(
      checked(runtime::file_detail(failed.error()))
      == file_failure_detail::permission);
    BOOST_CHECK(test.names() == std::set<std::string>{"b"});
    BOOST_CHECK_EQUAL(test.read("b"), std::string(4096, 'a'));
    checked(co_await test.drive(test.files->crash()));
    BOOST_CHECK(test.names() == (std::set<std::string>{"a", "b"}));
    BOOST_CHECK_EQUAL(test.read("b"), std::string(4096, 'b'));
}

SEASTAR_TEST_CASE(
  typed_partial_truncate_has_an_intermediate_effect_without_a_barrier) {
    seastar::chunked_vector<fault_rule> rules;
    rules.push_back(rule(
      builtin_fault_point::file_truncate,
      checked(
        fault_decision::make_file_failure(
          fault_action::file_failure_after_prefix,
          file_failure_detail::device_io,
          byte_count{2048}))));
    fixture test{config(), std::move(rules)};
    stable_file(test, "file", 'a');
    auto owner = checked(
      co_await test.drive(test.files->open(
        test.public_path("file"),
        {.access = runtime::file_access::read_write})));
    auto failed = co_await test.drive(owner.truncate(1024));
    const auto size = co_await test.drive(owner.size());
    checked(co_await test.drive(owner.close()));
    BOOST_REQUIRE(!failed.has_value());
    BOOST_CHECK(
      checked(runtime::file_detail(failed.error()))
      == file_failure_detail::device_io);
    BOOST_CHECK_EQUAL(checked(size), 2048U);
    checked(co_await test.drive(test.files->crash()));
    BOOST_CHECK_EQUAL(test.read("file"), std::string(4096, 'a'));
}

SEASTAR_TEST_CASE(
  contextual_fault_selector_distinguishes_position_without_changing_inode_authority) {
    const auto point
      = runtime::descriptor_for(builtin_fault_point::file_write)->id;
    const auto key = storage_fault_key(
      {(UINT64_C(1) << 8U) | point.value(), 2, 1, 0, 0});
    seastar::chunked_vector<fault_rule> rules;
    rules.push_back(checked(
      fault_rule::make(
        checked(fault_rule_id::make(1)),
        builtin_fault_point::file_write,
        key,
        runtime::fault_occurrence::first(),
        checked(runtime::fault_occurrence::make(2)),
        fault_selector::bounded_range(),
        checked(
          fault_decision::make_file_failure(
            fault_action::file_failure_before_effect,
            file_failure_detail::permission)))));
    fixture test{config(), std::move(rules)};
    stable_file(test, "file", 'a');
    auto first = checked(
      co_await test.drive(test.files->open(
        test.public_path("file"),
        {.access = runtime::file_access::read_write})));
    auto rejected = co_await test.drive(
      first.write(runtime::file_position{}, payload('b')));
    checked(co_await test.drive(first.close()));
    auto second = checked(
      co_await test.drive(test.files->open(
        test.public_path("file"),
        {.access = runtime::file_access::read_write})));
    auto written = co_await test.drive(
      second.write(runtime::file_position{4096}, payload('c')));
    checked(co_await test.drive(second.close()));
    BOOST_REQUIRE(!rejected.has_value());
    BOOST_REQUIRE(written.has_value());
    BOOST_CHECK_EQUAL(test.read("file"), std::string(4096, 'a'));
    BOOST_CHECK_EQUAL(test.read("file", 4096), std::string(4096, 'c'));
}

namespace {
seastar::future<std::string> replay_scenario(fixture& test) {
    stable_file(test, "a", 'a');
    stable_file(test, "b", 'b');
    test.write("a", 'x');
    checked(
      co_await test.drive(
        test.files->rename(test.public_path("a"), test.public_path("b"))));
    checked(co_await test.drive(test.files->crash()));
    co_return test.read("b");
}
} // namespace

SEASTAR_TEST_CASE(
  selective_crash_capture_replays_exactly_and_rejects_plan_tampering_before_commit) {
    const auto configuration = config(
      fake_crash_policy{
        .data_percent = 50, .namespace_percent = 100, .granule_bytes = 64});
    fixture capture{configuration};
    const auto expected = co_await replay_scenario(capture);
    const auto encoded = checked(capture.owned_trace.encode());
    auto decoded = checked(event_trace::decode(encoded, capture.limits));
    auto replay = checked(
      event_trace::replay(
        header(scheduler_budget(), capture.limits),
        capture.limits,
        std::move(decoded)));
    fixture repeated{configuration, {}, 65536, replay.get()};
    BOOST_CHECK_EQUAL(co_await replay_scenario(repeated), expected);
    checked(replay->finish_replay());
    BOOST_CHECK(checked(replay->encode()) == encoded);

    decoded = checked(event_trace::decode(encoded, capture.limits));
    auto selected = std::ranges::find(
      decoded.entries, trace_action::crash_selection, &trace_entry::action);
    BOOST_REQUIRE(selected != decoded.entries.end());
    const auto selection_sequence = selected->sequence;
    selected->context[0].value ^= 1U;
    auto invalid = checked(
      event_trace::replay(
        header(scheduler_budget(), capture.limits),
        capture.limits,
        std::move(decoded)));
    fixture target{configuration, {}, 65536, invalid.get()};
    stable_file(target, "a", 'a');
    stable_file(target, "b", 'b');
    target.write("a", 'x');
    checked(
      co_await target.drive(target.files->rename(
        target.public_path("a"), target.public_path("b"))));
    auto crashing = target.files->crash();
    std::exception_ptr pump_failure;
    try {
        // Use the capture driver's ready-before-advance ordering in replay.
        co_await pump_until(target.events, crashing);
    } catch (...) {
        pump_failure = std::current_exception();
    }
    const bool diverged = target.events.trace_failed();
    // Join cleanup before asserting: a failed trace resumes rollback through
    // the native reactor, and stop discards any remaining simulation events.
    auto stopping = target.files->stop();
    if (!diverged) co_await pump_until(target.events, stopping);
    const auto failed = co_await std::move(crashing);
    const auto stopped = co_await std::move(stopping);
    if (pump_failure) {
        try {
            std::rethrow_exception(pump_failure);
        } catch (const std::system_error& error) {
            BOOST_CHECK(
              error.code() == make_error_code(errc::replay_divergence));
        }
    }
    BOOST_REQUIRE(diverged);
    BOOST_CHECK_EQUAL(invalid->next_sequence(), selection_sequence);
    BOOST_REQUIRE(!failed.has_value());
    BOOST_CHECK(failed.error().code() == errc::replay_divergence);
    BOOST_REQUIRE(!stopped.has_value());
    BOOST_CHECK(stopped.error().code() == errc::replay_divergence);
    BOOST_CHECK_EQUAL(target.files->pending_operations(), 0U);
    BOOST_CHECK_EQUAL(target.events.pending_events(), 0U);
    BOOST_CHECK_EQUAL(target.read("b"), std::string(4096, 'x'));
    const auto snapshot = checked(
      fake_file_test_access::snapshot(*target.files));
    BOOST_CHECK_EQUAL(snapshot.crash_epoch, 0U);
}

SEASTAR_TEST_CASE(
  storage_fault_key_has_canonical_bytes_and_full_context_separation) {
    const storage_fault_context context{263, 2, 1, 0, 0};
    const auto key = storage_fault_key(context);
    constexpr std::array<std::uint8_t, 32> expected{
      0x1e, 0x4a, 0x49, 0xa9, 0xeb, 0x03, 0x81, 0xea, 0x3f, 0x49, 0xe4,
      0xf1, 0xe8, 0xc2, 0x1b, 0x98, 0xfd, 0x30, 0xec, 0xd9, 0x4e, 0xd2,
      0x73, 0x25, 0xef, 0xbc, 0x37, 0x73, 0x41, 0x17, 0x22, 0x4f};
    BOOST_REQUIRE_EQUAL(key.bytes().size(), expected.size());
    for (std::size_t index = 0; index < expected.size(); ++index)
        BOOST_CHECK_EQUAL(
          std::to_integer<std::uint8_t>(key.bytes()[index]), expected[index]);
    for (const auto changed : std::array{
           storage_fault_context{264, 2, 1, 0, 0},
           storage_fault_context{263, 3, 1, 0, 0},
           storage_fault_context{263, 2, 2, 0, 0},
           storage_fault_context{263, 2, 1, 1, 0},
           storage_fault_context{263, 2, 1, 0, 1}}) {
        BOOST_CHECK(changed != context);
        BOOST_CHECK(storage_fault_key(changed) != key);
    }
    // A deliberately shared selector can choose a rule for distinct requests;
    // it never replaces either request's owning object or full context.
    seastar::chunked_vector<fault_rule> rules;
    rules.push_back(rule(
      builtin_fault_point::file_write,
      checked(
        fault_decision::make_file_failure(
          fault_action::file_failure_before_effect,
          file_failure_detail::unknown)),
      key));
    fixture test{config(), std::move(rules)};
    const auto point
      = runtime::descriptor_for(builtin_fault_point::file_write)->id;
    for (const auto object : {2U, 3U}) {
        const runtime::fault_request request{
          point,
          runtime::fault_occurrence::first(),
          runtime::fault_object_key::from_u64(object)};
        auto prepared = checked(test.faults->prepare_contextual(request, key));
        BOOST_CHECK(prepared.preview().file_failure());
        static_cast<void>(checked(prepared.commit()));
        BOOST_CHECK(
          request.object == runtime::fault_object_key::from_u64(object));
    }
    co_return;
}

SEASTAR_TEST_CASE(selective_crash_trace_ignores_hash_capacity_noise) {
    const auto configuration = config(
      fake_crash_policy{
        .data_percent = 50, .namespace_percent = 100, .granule_bytes = 64});
    fixture ordinary{configuration};
    const auto first = co_await replay_scenario(ordinary);
    const auto trace = checked(ordinary.owned_trace.encode());
    fixture perturbed{configuration};
    fake_file_test_access::reserve_object_slots(*perturbed.files, 8192);
    BOOST_CHECK_EQUAL(co_await replay_scenario(perturbed), first);
    BOOST_CHECK(checked(perturbed.owned_trace.encode()) == trace);
}

SEASTAR_TEST_CASE(
  storage_configuration_mismatch_rejects_before_a_backend_is_published) {
    auto configuration = config(fake_crash_policy{.data_percent = 50});
    fixture captured{configuration};
    auto wire = checked(captured.owned_trace.encode());
    auto parsed = checked(event_trace::decode(wire, captured.limits));
    auto replay = checked(
      event_trace::replay(
        header(scheduler_budget(), captured.limits),
        captured.limits,
        std::move(parsed)));
    scheduler events{scheduler_budget(), replay.get()};
    auto faults = checked(fault_schedule::make(events, *replay, seed, {}));
    configuration.crash_policy->data_percent = 100;
    const auto rejected = fake_file_system::make(
      configuration, events, *faults);
    BOOST_REQUIRE(!rejected.has_value());
    BOOST_CHECK(rejected.error().code() == errc::replay_divergence);
    BOOST_CHECK_EQUAL(events.pending_events(), 0U);
    co_return;
}

SEASTAR_TEST_CASE(
  selective_policy_rejects_invalid_geometry_limits_and_missing_driver) {
    const auto valid = config(fake_crash_policy{});
    BOOST_CHECK(fake_file_system::validate_config(valid).has_value());
    BOOST_CHECK(!fake_file_system::make(valid).has_value());
    for (unsigned field = 0; field < 6; ++field) {
        auto invalid = valid;
        switch (field) {
        case 0:
            invalid.crash_policy->data_percent = 101;
            break;
        case 1:
            invalid.crash_policy->namespace_percent = 101;
            break;
        case 2:
            invalid.crash_policy->granule_bytes = 3;
            break;
        case 3:
            invalid.crash_policy->maximum_namespace_groups = 4097;
            break;
        case 4:
            invalid.crash_policy->maximum_scratch_bytes = byte_count{};
            break;
        default:
            invalid.device_scope = 0;
            break;
        }
        BOOST_CHECK(!fake_file_system::validate_config(invalid).has_value());
    }
    co_return;
}

SEASTAR_TEST_CASE(
  selective_cross_directory_rename_retains_synced_endpoint_dependencies) {
    for (const bool sync_destination : {false, true}) {
        fixture test{config(fake_crash_policy{})};
        checked(
          co_await test.drive(
            test.files->create_directories(test.public_path("left"))));
        checked(
          co_await test.drive(
            test.files->create_directories(test.public_path("right"))));
        test.sync_root();
        stable_file(test, "left/a", 'a');
        stable_file(test, "right/b", 'b');
        checked(
          fake_file_test_access::sync_directory(
            *test.files, test.path("left")));
        checked(
          fake_file_test_access::sync_directory(
            *test.files, test.path("right")));
        checked(
          co_await test.drive(test.files->rename(
            test.public_path("left/a"), test.public_path("right/b"))));
        if (sync_destination)
            checked(
              fake_file_test_access::sync_directory(
                *test.files, test.path("right")));
        checked(co_await test.drive(test.files->crash()));
        const auto source = co_await test.drive(
          test.files->exists(test.public_path("left/a")));
        BOOST_CHECK(checked(source) == !sync_destination);
        BOOST_CHECK_EQUAL(
          test.read("right/b"),
          std::string(4096, sync_destination ? 'a' : 'b'));
    }
}

SEASTAR_TEST_CASE(
  namespace_history_pressure_does_not_publish_a_partial_create) {
    fixture test{config(fake_crash_policy{.maximum_namespace_groups = 1})};
    auto first = checked(
      co_await test.drive(test.files->open(
        test.public_path("a"),
        {.access = runtime::file_access::read_write, .create = true})));
    checked(co_await test.drive(first.close()));
    const auto before = checked(fake_file_test_access::snapshot(*test.files));
    auto rejected = co_await test.drive(test.files->open(
      test.public_path("b"),
      {.access = runtime::file_access::read_write, .create = true}));
    const auto after = checked(fake_file_test_access::snapshot(*test.files));
    BOOST_REQUIRE(!rejected.has_value());
    BOOST_CHECK(rejected.error().code() == errc::resource_exhausted);
    BOOST_CHECK(
      checked(runtime::file_detail(rejected.error()))
      == file_failure_detail::unknown);
    BOOST_CHECK(before.objects == after.objects);
    BOOST_CHECK_EQUAL(before.next_object_id, after.next_object_id);
    BOOST_CHECK_EQUAL(after.open_handles, 0U);
    BOOST_CHECK(test.names() == std::set<std::string>{"a"});
}

SEASTAR_TEST_CASE(
  checked_fake_close_retains_cause_and_releases_failed_handles) {
    for (const auto policy :
         {runtime::file_close_policy::legacy,
          runtime::file_close_policy::checked}) {
        seastar::chunked_vector<fault_rule> rules;
        rules.push_back(rule(
          builtin_fault_point::file_close,
          checked(
            fault_decision::make_file_failure(
              fault_action::file_failure_before_effect,
              file_failure_detail::no_space))));
        fixture test{config(), std::move(rules)};
        stable_file(test, "file", 'a');
        auto owner = checked(
          co_await test.drive(test.files->open(
            test.public_path("file"),
            {.access = runtime::file_access::read_write,
             .close_policy = policy})));
        const auto first = co_await test.drive(owner.close());
        const auto second = co_await owner.close();
        const bool expects_error = policy
                                   == runtime::file_close_policy::checked;
        BOOST_CHECK_EQUAL(first.has_value(), !expects_error);
        BOOST_CHECK_EQUAL(second.has_value(), !expects_error);
        if (expects_error) {
            BOOST_REQUIRE(!first.has_value());
            BOOST_REQUIRE(!second.has_value());
            BOOST_CHECK(first.error() == second.error());
            BOOST_CHECK(
              checked(runtime::file_detail(first.error()))
              == file_failure_detail::no_space);
        }
        const auto state = checked(
          fake_file_test_access::snapshot(*test.files));
        BOOST_CHECK_EQUAL(state.open_handles, 0U);
        BOOST_CHECK_EQUAL(test.files->pending_operations(), 0U);
    }
}

SEASTAR_TEST_CASE(checked_fake_close_lost_notification_joins_backend_stop) {
    seastar::chunked_vector<fault_rule> rules;
    rules.push_back(rule(
      builtin_fault_point::file_close, fault_decision::make_drop_completion()));
    fixture test{config(), std::move(rules)};
    stable_file(test, "file", 'a');
    auto owner = checked(
      co_await test.drive(test.files->open(
        test.public_path("file"),
        {.close_policy = runtime::file_close_policy::checked})));
    auto closing = owner.close();
    auto parked = fake_file_test_access::wait_parked(
      *test.files, fake_submission_kind::close, 1);
    co_await test.drive(std::move(parked));
    const bool waiting = !closing.available();
    const auto duplicate = co_await owner.close();
    auto stopping = test.files->stop();
    checked(co_await test.drive(std::move(stopping)));
    const auto closed = co_await std::move(closing);
    BOOST_CHECK(waiting);
    BOOST_CHECK(!duplicate.has_value());
    BOOST_CHECK(!closed.has_value());
    BOOST_CHECK_EQUAL(
      checked(fake_file_test_access::snapshot(*test.files)).open_handles, 0U);
    BOOST_CHECK_EQUAL(test.files->pending_operations(), 0U);
}

SEASTAR_TEST_CASE(checked_fake_directory_cleanup_preserves_its_close_failure) {
    seastar::chunked_vector<fault_rule> rules;
    rules.push_back(rule(
      builtin_fault_point::directory_cursor_close,
      checked(
        fault_decision::make_file_failure(
          fault_action::file_failure_before_effect,
          file_failure_detail::device_io))));
    fixture test{config(), std::move(rules)};
    const auto result = co_await test.drive(test.files->sync_directory(
      test.public_path(""), runtime::file_close_policy::checked));
    BOOST_REQUIRE(!result.has_value());
    BOOST_CHECK(
      checked(runtime::file_detail(result.error()))
      == file_failure_detail::device_io);
    BOOST_CHECK_EQUAL(
      checked(fake_file_test_access::snapshot(*test.files)).open_handles, 0U);
    BOOST_CHECK_EQUAL(test.files->pending_operations(), 0U);
}
