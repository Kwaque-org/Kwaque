#include "src/bytes/fragmented_buffer.h"
#include "src/runtime/file.h"
#include "src/simulation/event_trace.h"
#include "src/simulation/fake_file.h"
#include "src/simulation/fake_file_test_support.h"
#include "src/simulation/fault_schedule.h"
#include "src/simulation/scheduler.h"
#include "src/simulation/tests/fake_file_model.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/smp.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/later.hh>

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>

namespace {

using kwaque::runtime::builtin_fault_point;
using kwaque::runtime::fault_decision;
using kwaque::runtime::fault_occurrence;
using kwaque::simulation::event_trace;
using kwaque::simulation::fake_file_system;
using kwaque::simulation::fake_file_system_config;
using kwaque::simulation::fake_file_test_access;
using kwaque::simulation::fake_submission_kind;
using kwaque::simulation::fault_rule;
using kwaque::simulation::fault_rule_id;
using kwaque::simulation::fault_schedule;
using kwaque::simulation::fault_selector;
using kwaque::simulation::scheduler;
using kwaque::simulation::scheduler_limit_values;
using kwaque::simulation::scheduler_limits;
using kwaque::simulation::trace_digest;
using kwaque::simulation::trace_header;
using kwaque::simulation::trace_limit_values;
using kwaque::simulation::trace_limits;
using kwaque::simulation::testing::dense_storage_model;
using kwaque::simulation::testing::storage_command;
using kwaque::simulation::testing::storage_command_kind;
using kwaque::simulation::testing::storage_fault_action;
using kwaque::simulation::testing::storage_fault_rule;
using kwaque::simulation::testing::storage_outcome;
using kwaque::simulation::testing::storage_workload_generator;

static_assert(!std::is_copy_constructible_v<fake_file_system>);
static_assert(!std::is_copy_assignable_v<fake_file_system>);
static_assert(!std::is_move_constructible_v<fake_file_system>);
static_assert(!std::is_move_assignable_v<fake_file_system>);

constexpr std::uint64_t seed{71};

scheduler_limits make_scheduler_limits() {
    auto limits = scheduler_limits::make(
      scheduler_limit_values{
        .pending_events = 128,
        .events_per_pump = 128,
        .total_events = 1'024,
        .maximum_deadline = kwaque::runtime::monotonic_time{1'000'000},
      });
    BOOST_REQUIRE(limits.has_value());
    return *limits;
}

trace_limits make_trace_limits() {
    auto limits = trace_limits::make(
      trace_limit_values{
        .entries = 4'096,
        .encoded_bytes = 4'096
                           * kwaque::simulation::canonical_entry_encoded_size
                         + kwaque::simulation::canonical_header_encoded_size,
        .line_bytes = 1'024,
      });
    BOOST_REQUIRE(limits.has_value());
    return *limits;
}

fault_rule rule(
  std::uint64_t id,
  builtin_fault_point point,
  std::uint64_t first,
  std::uint64_t last,
  fault_decision decision) {
    auto rule_id = fault_rule_id::make(id);
    auto first_occurrence = fault_occurrence::make(first);
    auto last_occurrence = fault_occurrence::make(last);
    BOOST_REQUIRE(rule_id.has_value());
    BOOST_REQUIRE(first_occurrence.has_value());
    BOOST_REQUIRE(last_occurrence.has_value());
    auto result = fault_rule::make(
      *rule_id,
      point,
      std::nullopt,
      *first_occurrence,
      *last_occurrence,
      first == last ? fault_selector::once() : fault_selector::bounded_range(),
      decision);
    BOOST_REQUIRE(result.has_value());
    return *result;
}

fault_rule rule(const storage_fault_rule& source) {
    fault_decision decision;
    switch (source.action) {
    case storage_fault_action::error:
        decision = fault_decision::make_error();
        break;
    case storage_fault_action::delay:
        decision = fault_decision::make_delay(
          kwaque::runtime::monotonic_duration{source.payload});
        break;
    case storage_fault_action::crash:
        decision = fault_decision::make_crash();
        break;
    case storage_fault_action::drop_completion:
        decision = fault_decision::make_drop_completion();
        break;
    case storage_fault_action::partial_resize:
        decision = fault_decision::make_partial_resize();
        break;
    }
    return rule(
      source.id,
      source.point == storage_command_kind::truncate
        ? builtin_fault_point::file_truncate
        : builtin_fault_point::file_write,
      source.first,
      source.last,
      decision);
}

struct fixture final {
    scheduler_limits scheduler_budget;
    trace_limits trace_budget;
    event_trace trace;
    scheduler events;
    std::unique_ptr<fault_schedule> faults;
    std::unique_ptr<fake_file_system> files;

    explicit fixture(
      seastar::chunked_vector<fault_rule> rules = {},
      fake_file_system_config config = {},
      scheduler_limits configured_scheduler = make_scheduler_limits(),
      trace_limits configured_trace = make_trace_limits())
      : scheduler_budget(configured_scheduler)
      , trace_budget(configured_trace)
      , trace(
          trace_header::current(
            seed,
            kwaque::simulation::deterministic_random_algorithm_version,
            kwaque::simulation::deterministic_random_coordinate_version,
            kwaque::simulation::trace_budget(scheduler_budget),
            trace_budget,
            trace_digest{},
            trace_digest{}),
          trace_budget)
      , events(scheduler_budget, &trace) {
        auto schedule = fault_schedule::make(
          events, trace, seed, std::move(rules));
        BOOST_REQUIRE(schedule.has_value());
        faults = std::move(*schedule);
        auto filesystem = fake_file_system::make(
          std::move(config), events, *faults);
        BOOST_REQUIRE(filesystem.has_value());
        files = std::move(*filesystem);
    }
};

template<typename T>
seastar::future<> pump_until(scheduler& events, seastar::future<T>& waiting) {
    while (!waiting.available()) {
        while (events.pending_events() == 0U && !waiting.available()) {
            co_await seastar::yield();
        }
        if (waiting.available()) {
            break;
        }
        if (!events.has_ready_events()) {
            const auto advanced = events.advance_to_next();
            BOOST_REQUIRE(advanced.has_value());
            BOOST_REQUIRE(advanced->has_value());
        }
        const auto ran = events.run_ready();
        BOOST_REQUIRE(ran.has_value());
        co_await seastar::yield();
    }
}

template<typename Future>
seastar::future<> require_ready_success(Future& waiting) {
    const auto result = co_await std::move(waiting);
    BOOST_REQUIRE(result.has_value());
}

kwaque::runtime::file_path path(std::string value) {
    auto result = kwaque::runtime::file_path::make(std::move(value));
    BOOST_REQUIRE(result.has_value());
    return std::move(*result);
}

kwaque::bytes::fragmented_buffer payload(std::string_view value) {
    auto result = kwaque::bytes::fragmented_buffer::copy_of(
      std::span{value.data(), value.size()});
    BOOST_REQUIRE(result.has_value());
    return std::move(*result);
}

struct partial_resize_observation final {
    kwaque::errc error{kwaque::errc::success};
    std::uint64_t visible_size{0};
    std::uint64_t durable_size{0};
    std::uint64_t traced_target{0};
    std::uint32_t scheduled_outcome{0};
    std::size_t applied_entries{0};
};

seastar::future<partial_resize_observation> run_partial_resize_case(
  std::uint64_t initial_size,
  std::uint64_t requested_size,
  std::uint64_t capacity
  = kwaque::simulation::default_fake_disk_capacity.value(),
  bool flush_initial = false,
  fault_decision decision = fault_decision::make_partial_resize()) {
    seastar::chunked_vector<fault_rule> rules;
    const auto selected_occurrence = initial_size == 0 ? 1U : 2U;
    rules.push_back(rule(
      120,
      builtin_fault_point::file_truncate,
      selected_occurrence,
      selected_occurrence,
      decision));
    fake_file_system_config config;
    config.logical_capacity = kwaque::byte_count{capacity};
    fixture environment{std::move(rules), config};

    auto creating = environment.files->create_directories(path("/kwaque/data"));
    co_await pump_until(environment.events, creating);
    co_await require_ready_success(creating);
    auto opening = environment.files->open(
      path("/kwaque/data/file"),
      {.access = kwaque::runtime::file_access::read_write, .create = true});
    co_await pump_until(environment.events, opening);
    auto opened = co_await std::move(opening);
    BOOST_REQUIRE(opened.has_value());
    auto file = std::move(*opened);
    if (initial_size != 0) {
        auto resizing = file.truncate(initial_size);
        co_await pump_until(environment.events, resizing);
        co_await require_ready_success(resizing);
        if (flush_initial) {
            auto flushing = file.flush();
            co_await pump_until(environment.events, flushing);
            co_await require_ready_success(flushing);
        }
    }

    auto truncating = file.truncate(requested_size);
    co_await pump_until(environment.events, truncating);
    const auto truncated = co_await std::move(truncating);
    BOOST_REQUIRE(!truncated.has_value());

    const auto file_path = fake_file_test_access::resolve(
      *environment.files, "/kwaque/data/file");
    BOOST_REQUIRE(file_path.has_value());
    const auto visible = fake_file_test_access::visible_size(
      *environment.files, *file_path);
    const auto durable = fake_file_test_access::durable_size(
      *environment.files, *file_path);
    BOOST_REQUIRE(visible.has_value());
    BOOST_REQUIRE(durable.has_value());

    partial_resize_observation observation{
      .error = truncated.error().code(),
      .visible_size = *visible,
      .durable_size = *durable,
    };
    for (const auto& entry : environment.trace.entries()) {
        if (
          entry.action
          == kwaque::simulation::trace_action::partial_resize_applied) {
            ++observation.applied_entries;
            observation.traced_target = entry.coordinate_a;
        }
        if (
          entry.action == kwaque::simulation::trace_action::scheduled
          && entry.kind == kwaque::simulation::trace_event_kind::file
          && entry.domain
               == kwaque::runtime::descriptor_for(
                    builtin_fault_point::file_truncate)
                    ->id.value()
          && (entry.result & UINT32_C(0xff))
               == static_cast<std::uint8_t>(
                 kwaque::runtime::fault_action::partial_resize)) {
            observation.scheduled_outcome = entry.result >> 8U;
            if (entry.coordinate_a != 0) {
                observation.traced_target = entry.coordinate_a;
            }
        }
    }

    auto closing = file.close();
    co_await pump_until(environment.events, closing);
    co_await require_ready_success(closing);
    auto stopping = environment.files->stop();
    if (!stopping.available()) {
        co_await pump_until(environment.events, stopping);
    }
    co_await require_ready_success(stopping);
    co_return observation;
}

std::string model_path(std::uint8_t slot) {
    return slot == 0 ? "/kwaque/data/alpha" : "/kwaque/data/beta";
}

storage_outcome outcome_for(const kwaque::runtime::operation_error& error) {
    switch (error.code()) {
    case kwaque::errc::not_found:
        return storage_outcome::not_found;
    case kwaque::errc::io_failure:
        return storage_outcome::io_failure;
    case kwaque::errc::aborted:
        return storage_outcome::aborted;
    default:
        BOOST_FAIL("unexpected model-driver error");
        return storage_outcome::not_found;
    }
}

template<typename T>
storage_outcome outcome_for(const kwaque::runtime::result<T>& result) {
    return result ? storage_outcome::success : outcome_for(result.error());
}

seastar::future<storage_outcome>
execute_model_command(fixture& environment, const storage_command& command) {
    if (command.kind == storage_command_kind::sync_directory) {
        auto pending = environment.files->sync_directory(path("/kwaque/data"));
        co_await pump_until(environment.events, pending);
        co_return outcome_for(co_await std::move(pending));
    }
    if (command.kind == storage_command_kind::crash) {
        auto pending = environment.files->crash();
        co_await pump_until(environment.events, pending);
        co_return outcome_for(co_await std::move(pending));
    }
    if (command.kind == storage_command_kind::rename) {
        auto pending = environment.files->rename(
          path(model_path(command.source)),
          path(model_path(command.destination)));
        co_await pump_until(environment.events, pending);
        co_return outcome_for(co_await std::move(pending));
    }
    if (command.kind == storage_command_kind::remove) {
        auto pending = environment.files->remove_file(
          path(model_path(command.source)));
        co_await pump_until(environment.events, pending);
        co_return outcome_for(co_await std::move(pending));
    }

    auto opening = environment.files->open(
      path(model_path(command.source)),
      {.access = kwaque::runtime::file_access::read_write,
       .create = command.kind == storage_command_kind::write});
    co_await pump_until(environment.events, opening);
    auto opened = co_await std::move(opening);
    if (!opened) {
        co_return outcome_for(opened.error());
    }
    auto file = std::move(*opened);
    storage_outcome outcome{storage_outcome::success};
    if (command.kind == storage_command_kind::write) {
        const std::string bytes(
          command.length,
          static_cast<char>(std::to_integer<unsigned>(command.value)));
        auto pending = file.write(
          kwaque::runtime::file_position{command.position}, payload(bytes));
        co_await pump_until(environment.events, pending);
        outcome = outcome_for(co_await std::move(pending));
    } else if (command.kind == storage_command_kind::truncate) {
        auto pending = file.truncate(command.length);
        co_await pump_until(environment.events, pending);
        outcome = outcome_for(co_await std::move(pending));
    } else if (command.kind == storage_command_kind::flush) {
        auto pending = file.flush();
        co_await pump_until(environment.events, pending);
        outcome = outcome_for(co_await std::move(pending));
    } else {
        auto pending = file.read(
          kwaque::runtime::file_position{command.position},
          kwaque::byte_count{command.length});
        co_await pump_until(environment.events, pending);
        outcome = outcome_for(co_await std::move(pending));
    }
    auto closing = file.close();
    co_await pump_until(environment.events, closing);
    const auto closed = co_await std::move(closing);
    BOOST_REQUIRE(closed.has_value());
    co_return outcome;
}

void compare_model_state(
  const fake_file_system& filesystem,
  const dense_storage_model& model,
  std::uint32_t expected_open_handles = 0,
  std::uint32_t expected_pending_operations = 0,
  std::uint64_t expected_pending_bytes = 0,
  std::uint32_t expected_pending_reads = 0,
  std::uint32_t expected_pending_writes = 0) {
    const auto captured = fake_file_test_access::snapshot(filesystem);
    BOOST_REQUIRE(captured.has_value());
    const auto& actual = *captured;
    const auto expected = model.snapshot();
    BOOST_REQUIRE(actual.objects.size() == expected.objects.size());
    BOOST_CHECK(actual.retained_capacity == expected.retained_capacity);
    BOOST_CHECK(actual.generation == expected.generation);
    BOOST_CHECK(actual.open_handles == expected_open_handles);
    BOOST_CHECK(actual.pending_operations == expected_pending_operations);
    BOOST_CHECK(actual.pending_bytes == expected_pending_bytes);
    BOOST_CHECK(actual.pending_reads == expected_pending_reads);
    BOOST_CHECK(actual.pending_writes == expected_pending_writes);
    std::uint32_t open_references = 0;
    std::uint32_t pending_references = 0;
    for (std::size_t index = 0; index < expected.objects.size(); ++index) {
        const auto& observed = actual.objects[index];
        const auto& wanted = expected.objects[index];
        BOOST_REQUIRE(observed.id == wanted.id);
        BOOST_CHECK(
          (observed.kind == kwaque::simulation::fake_file_kind::directory)
          == wanted.directory);
        open_references += observed.open_references;
        pending_references += observed.pending_references;
        BOOST_CHECK(observed.visible_links == wanted.visible_links);
        BOOST_CHECK(observed.durable_links == wanted.durable_links);
        BOOST_CHECK(observed.visible_bytes == wanted.visible_bytes);
        BOOST_CHECK(observed.durable_bytes == wanted.durable_bytes);
        BOOST_CHECK(
          std::ranges::equal(observed.visible_entries, wanted.visible_entries));
        BOOST_CHECK(
          std::ranges::equal(observed.durable_entries, wanted.durable_entries));
    }
    BOOST_CHECK(open_references == expected_open_handles);
    BOOST_CHECK(pending_references == expected_pending_operations);
}

} // namespace

SEASTAR_TEST_CASE(fake_filesystem_operations_are_scheduler_selected) {
    fixture environment;
    auto creating = environment.files->create_directories(path("/kwaque/data"));
    BOOST_CHECK(!creating.available());
    co_await pump_until(environment.events, creating);
    co_await require_ready_success(creating);

    auto opening = environment.files->open(
      path("/kwaque/data/file"),
      {.access = kwaque::runtime::file_access::read_write,
       .create = true,
       .exclusive = true});
    BOOST_CHECK(!opening.available());
    co_await pump_until(environment.events, opening);
    auto opened = co_await std::move(opening);
    BOOST_REQUIRE(opened.has_value());
    auto file = std::move(*opened);

    auto writing = file.write(
      kwaque::runtime::file_position{0}, payload("scheduled-payload"));
    co_await pump_until(environment.events, writing);
    auto written = co_await std::move(writing);
    BOOST_REQUIRE(written.has_value());
    BOOST_CHECK(written->value() == 17U);

    auto flushing = file.flush();
    co_await pump_until(environment.events, flushing);
    co_await require_ready_success(flushing);

    auto reading = file.read(
      kwaque::runtime::file_position{0}, kwaque::byte_count{64});
    co_await pump_until(environment.events, reading);
    auto read = co_await std::move(reading);
    BOOST_REQUIRE(read.has_value());
    BOOST_CHECK(read->eof());
    BOOST_CHECK(read->data().content_equals("scheduled-payload"));

    auto closing = file.close();
    co_await pump_until(environment.events, closing);
    co_await require_ready_success(closing);
    BOOST_CHECK(environment.files->pending_operations() == 0U);
    BOOST_CHECK(environment.files->pending_bytes().value() == 0U);
    co_return;
}

SEASTAR_TEST_CASE(fake_metadata_surface_is_scheduled_and_typed) {
    fixture environment;
    auto creating = environment.files->create_directories(path("/kwaque/data"));
    co_await pump_until(environment.events, creating);
    co_await require_ready_success(creating);
    auto opening = environment.files->open(
      path("/kwaque/data/file"),
      {.access = kwaque::runtime::file_access::read_write, .create = true});
    co_await pump_until(environment.events, opening);
    auto opened = co_await std::move(opening);
    BOOST_REQUIRE(opened.has_value());
    auto file = std::move(*opened);

    auto checking = environment.files->exists(path("/kwaque/data/file"));
    co_await pump_until(environment.events, checking);
    auto exists = co_await std::move(checking);
    BOOST_REQUIRE(exists.has_value());
    BOOST_CHECK(*exists);

    auto stating = environment.files->stat(path("/kwaque/data/file"));
    co_await pump_until(environment.events, stating);
    auto status = co_await std::move(stating);
    BOOST_REQUIRE(status.has_value());
    BOOST_CHECK(status->kind == kwaque::runtime::file_kind::regular);
    BOOST_CHECK(status->size.value() == 0U);

    auto listing = environment.files->list(
      path("/kwaque/data"), kwaque::runtime::directory_listing_limits{});
    co_await pump_until(environment.events, listing);
    auto listed = co_await std::move(listing);
    BOOST_REQUIRE(listed.has_value());
    BOOST_REQUIRE(listed->entries().size() == 1U);
    BOOST_CHECK(listed->entries()[0].name.value() == "file");

    auto renaming = environment.files->rename(
      path("/kwaque/data/file"), path("/kwaque/data/renamed"));
    co_await pump_until(environment.events, renaming);
    co_await require_ready_success(renaming);
    auto syncing = environment.files->sync_directory(path("/kwaque/data"));
    co_await pump_until(environment.events, syncing);
    co_await require_ready_success(syncing);

    auto removing = environment.files->remove_file(
      path("/kwaque/data/renamed"));
    co_await pump_until(environment.events, removing);
    co_await require_ready_success(removing);
    auto closing = file.close();
    co_await pump_until(environment.events, closing);
    co_await require_ready_success(closing);

    auto removing_directory = environment.files->remove_directory(
      path("/kwaque/data"));
    co_await pump_until(environment.events, removing_directory);
    co_await require_ready_success(removing_directory);
    co_return;
}

SEASTAR_TEST_CASE(
  fake_native_access_modes_and_intent_cancellation_match_runtime) {
    fixture environment;
    auto creating = environment.files->create_directories(path("/kwaque/data"));
    co_await pump_until(environment.events, creating);
    co_await require_ready_success(creating);
    auto creating_file = environment.files->open(
      path("/kwaque/data/file"),
      {.access = kwaque::runtime::file_access::read_write, .create = true});
    co_await pump_until(environment.events, creating_file);
    auto created = co_await std::move(creating_file);
    BOOST_REQUIRE(created.has_value());
    auto initial_write = created->write(
      kwaque::runtime::file_position{0}, payload(std::string(4'096, 'x')));
    co_await pump_until(environment.events, initial_write);
    co_await require_ready_success(initial_write);
    auto initial_size = created->size();
    co_await pump_until(environment.events, initial_size);
    const auto sized = co_await std::move(initial_size);
    BOOST_REQUIRE(sized.has_value());
    BOOST_CHECK(*sized == 4'096U);
    auto truncating = created->truncate(2'048);
    co_await pump_until(environment.events, truncating);
    co_await require_ready_success(truncating);
    auto truncated_size = created->size();
    co_await pump_until(environment.events, truncated_size);
    const auto resized = co_await std::move(truncated_size);
    BOOST_REQUIRE(resized.has_value());
    BOOST_CHECK(*resized == 2'048U);
    auto initial_close = created->close();
    co_await pump_until(environment.events, initial_close);
    co_await require_ready_success(initial_close);

    auto opening_read_only = environment.files->open(
      path("/kwaque/data/file"),
      {.access = kwaque::runtime::file_access::read_only});
    co_await pump_until(environment.events, opening_read_only);
    auto read_only = co_await std::move(opening_read_only);
    BOOST_REQUIRE(read_only.has_value());
    auto rejected_write = read_only->write(
      kwaque::runtime::file_position{0}, payload(std::string(4'096, 'w')));
    co_await pump_until(environment.events, rejected_write);
    const auto write_result = co_await std::move(rejected_write);
    BOOST_REQUIRE(!write_result.has_value());
    BOOST_CHECK(write_result.error().code() == kwaque::errc::permission_denied);
    auto close_read_only = read_only->close();
    co_await pump_until(environment.events, close_read_only);
    co_await require_ready_success(close_read_only);

    auto opening_write_only = environment.files->open(
      path("/kwaque/data/file"),
      {.access = kwaque::runtime::file_access::write_only});
    co_await pump_until(environment.events, opening_write_only);
    auto write_only = co_await std::move(opening_write_only);
    BOOST_REQUIRE(write_only.has_value());
    auto rejected_read = write_only->read(
      kwaque::runtime::file_position{0}, kwaque::byte_count{4'096});
    co_await pump_until(environment.events, rejected_read);
    const auto read_result = co_await std::move(rejected_read);
    BOOST_REQUIRE(!read_result.has_value());
    BOOST_CHECK(read_result.error().code() == kwaque::errc::permission_denied);
    auto close_write_only = write_only->close();
    co_await pump_until(environment.events, close_write_only);
    co_await require_ready_success(close_write_only);

    auto opening_abort = environment.files->open(
      path("/kwaque/data/file"),
      {.access = kwaque::runtime::file_access::read_write});
    co_await pump_until(environment.events, opening_abort);
    auto aborting = co_await std::move(opening_abort);
    BOOST_REQUIRE(aborting.has_value());
    auto canceled_read = aborting->read(
      kwaque::runtime::file_position{0}, kwaque::byte_count{4'096});
    BOOST_CHECK(!canceled_read.available());
    aborting->request_abort();
    co_await pump_until(environment.events, canceled_read);
    const auto canceled = co_await std::move(canceled_read);
    BOOST_REQUIRE(!canceled.has_value());
    BOOST_CHECK(canceled.error().code() == kwaque::errc::aborted);
    auto close_aborted = aborting->close();
    co_await pump_until(environment.events, close_aborted);
    co_await require_ready_success(close_aborted);
    co_return;
}

SEASTAR_TEST_CASE(fake_native_uses_distinct_append_and_overwrite_alignments) {
    fake_file_system_config config;
    config.disk_write_dma_alignment = 8'192;
    config.disk_overwrite_dma_alignment = 4'096;
    fixture environment{{}, config};
    auto creating = environment.files->create_directories(path("/kwaque/data"));
    co_await pump_until(environment.events, creating);
    co_await require_ready_success(creating);
    auto opening = environment.files->open(
      path("/kwaque/data/file"),
      {.access = kwaque::runtime::file_access::read_write, .create = true});
    co_await pump_until(environment.events, opening);
    auto opened = co_await std::move(opening);
    BOOST_REQUIRE(opened.has_value());
    auto file = std::move(*opened);

    auto appending = file.write(
      kwaque::runtime::file_position{0}, payload(std::string(8'192, 'a')));
    co_await pump_until(environment.events, appending);
    co_await require_ready_success(appending);
    auto overwriting = file.write(
      kwaque::runtime::file_position{0}, payload(std::string(4'096, 'b')));
    co_await pump_until(environment.events, overwriting);
    co_await require_ready_success(overwriting);
    auto reading = file.read(
      kwaque::runtime::file_position{0}, kwaque::byte_count{8'192});
    co_await pump_until(environment.events, reading);
    auto observed = co_await std::move(reading);
    BOOST_REQUIRE(observed.has_value());
    BOOST_CHECK(observed->data().content_equals(
      std::string(4'096, 'b') + std::string(4'096, 'a')));

    auto closing = file.close();
    co_await pump_until(environment.events, closing);
    co_await require_ready_success(closing);
    co_return;
}

SEASTAR_TEST_CASE(fake_native_unused_methods_fail_deterministically) {
    fixture environment;
    const auto file_path = fake_file_test_access::resolve(
      *environment.files, "/kwaque/file");
    BOOST_REQUIRE(file_path.has_value());
    BOOST_REQUIRE(
      fake_file_test_access::create_file(*environment.files, *file_path)
        .has_value());
    auto made = fake_file_test_access::make_native_file_probe(
      *environment.files, *file_path, kwaque::runtime::file_access::read_write);
    BOOST_REQUIRE(made.has_value());
    auto native = std::move(*made);

    std::uint32_t unsupported = 0;
    try {
        co_await native.allocate(0, 4'096);
    } catch (const std::system_error& error) {
        BOOST_CHECK(
          error.code()
          == std::make_error_code(std::errc::operation_not_supported));
        ++unsupported;
    }
    try {
        co_await native.discard(0, 4'096);
    } catch (const std::system_error& error) {
        BOOST_CHECK(
          error.code()
          == std::make_error_code(std::errc::operation_not_supported));
        ++unsupported;
    }
    try {
        static_cast<void>(co_await native.write_iovec());
    } catch (const std::system_error& error) {
        BOOST_CHECK(
          error.code()
          == std::make_error_code(std::errc::operation_not_supported));
        ++unsupported;
    }
    try {
        static_cast<void>(co_await native.read_iovec());
    } catch (const std::system_error& error) {
        BOOST_CHECK(
          error.code()
          == std::make_error_code(std::errc::operation_not_supported));
        ++unsupported;
    }
    BOOST_CHECK(unsupported == 4U);

    auto closing = native.close();
    co_await pump_until(environment.events, closing);
    co_await std::move(closing);
    BOOST_CHECK(fake_file_test_access::open_handles(*environment.files) == 0U);
    co_return;
}

SEASTAR_TEST_CASE(
  fake_pending_limits_reject_without_scheduler_or_trace_growth) {
    fake_file_system_config config;
    config.maximum_pending_operations = 1;
    fixture environment{{}, config};
    auto first = environment.files->exists(path("/kwaque/first"));
    BOOST_CHECK(!first.available());
    const auto trace_size = environment.trace.entries().size();
    auto second = environment.files->exists(path("/kwaque/second"));
    BOOST_CHECK(second.available());
    const auto rejected = co_await std::move(second);
    BOOST_REQUIRE(!rejected.has_value());
    BOOST_CHECK(rejected.error().code() == kwaque::errc::queue_full);
    BOOST_CHECK(environment.trace.entries().size() == trace_size);
    BOOST_CHECK(environment.files->pending_operations() == 1U);
    co_await pump_until(environment.events, first);
    co_await require_ready_success(first);
    BOOST_CHECK(environment.files->pending_operations() == 0U);
    co_return;
}

SEASTAR_TEST_CASE(fake_rejected_close_releases_its_handle_capacity) {
    fake_file_system_config config;
    config.maximum_pending_operations = 1;
    config.maximum_open_handles = 1;
    fixture environment{{}, config};

    auto creating = environment.files->create_directories(path("/kwaque/data"));
    co_await pump_until(environment.events, creating);
    co_await require_ready_success(creating);
    auto opening = environment.files->open(
      path("/kwaque/data/file"),
      {.access = kwaque::runtime::file_access::read_write, .create = true});
    co_await pump_until(environment.events, opening);
    auto opened = co_await std::move(opening);
    BOOST_REQUIRE(opened.has_value());
    auto file = std::move(*opened);
    BOOST_CHECK(fake_file_test_access::open_handles(*environment.files) == 1U);

    auto occupying = environment.files->exists(path("/kwaque/occupied"));
    BOOST_CHECK(!occupying.available());
    auto closing = file.close();
    for (std::size_t turns = 0; turns < 64U && !closing.available(); ++turns) {
        co_await seastar::yield();
    }
    BOOST_REQUIRE(closing.available());
    co_await require_ready_success(closing);
    BOOST_CHECK(fake_file_test_access::open_handles(*environment.files) == 0U);

    co_await pump_until(environment.events, occupying);
    co_await require_ready_success(occupying);
    auto reopening = environment.files->open(
      path("/kwaque/data/file"),
      {.access = kwaque::runtime::file_access::read_write});
    co_await pump_until(environment.events, reopening);
    auto reopened = co_await std::move(reopening);
    BOOST_REQUIRE(reopened.has_value());
    auto final_close = reopened->close();
    co_await pump_until(environment.events, final_close);
    co_await require_ready_success(final_close);
    BOOST_CHECK(fake_file_test_access::open_handles(*environment.files) == 0U);
    co_return;
}

SEASTAR_TEST_CASE(recursive_create_capacity_failure_is_all_or_nothing) {
    fake_file_system_config config;
    config.virtual_root = "/d";
    config.maximum_retained_path_bytes = kwaque::byte_count{13};
    fixture environment{{}, config};

    auto creating = environment.files->create_directories(path("/d/a/b"));
    BOOST_CHECK(!creating.available());
    co_await pump_until(environment.events, creating);
    const auto rejected = co_await std::move(creating);
    BOOST_REQUIRE(!rejected.has_value());
    BOOST_CHECK(rejected.error().code() == kwaque::errc::resource_exhausted);
    BOOST_CHECK(environment.files->object_count() == 1U);

    auto checking = environment.files->exists(path("/d/a"));
    co_await pump_until(environment.events, checking);
    const auto exists = co_await std::move(checking);
    BOOST_REQUIRE(exists.has_value());
    BOOST_CHECK(!*exists);
    co_return;
}

SEASTAR_TEST_CASE(fake_pending_byte_limit_rejects_without_coordinate_growth) {
    fake_file_system_config config;
    config.maximum_pending_operations = 2;
    config.maximum_pending_bytes = kwaque::byte_count{4'096};
    fixture environment{{}, config};
    auto creating = environment.files->create_directories(path("/kwaque/data"));
    co_await pump_until(environment.events, creating);
    co_await require_ready_success(creating);
    auto opening = environment.files->open(
      path("/kwaque/data/file"),
      {.access = kwaque::runtime::file_access::read_write, .create = true});
    co_await pump_until(environment.events, opening);
    auto opened = co_await std::move(opening);
    BOOST_REQUIRE(opened.has_value());
    auto file = std::move(*opened);

    auto first = file.read(
      kwaque::runtime::file_position{0}, kwaque::byte_count{4'096});
    for (std::size_t turns = 0;
         turns < 64U && environment.files->pending_operations() == 0U;
         ++turns) {
        co_await seastar::yield();
    }
    BOOST_REQUIRE(environment.files->pending_operations() == 1U);
    BOOST_CHECK(environment.files->pending_bytes().value() == 4'096U);
    const auto trace_size = environment.trace.entries().size();
    auto second = file.read(
      kwaque::runtime::file_position{0}, kwaque::byte_count{4'096});
    for (std::size_t turns = 0; turns < 64U && !second.available(); ++turns) {
        co_await seastar::yield();
    }
    BOOST_REQUIRE(second.available());
    const auto rejected = co_await std::move(second);
    BOOST_REQUIRE(!rejected.has_value());
    BOOST_CHECK(rejected.error().code() == kwaque::errc::resource_exhausted);
    BOOST_CHECK(environment.trace.entries().size() == trace_size);
    BOOST_CHECK(environment.files->pending_operations() == 1U);

    co_await pump_until(environment.events, first);
    co_await require_ready_success(first);
    auto closing = file.close();
    co_await pump_until(environment.events, closing);
    co_await require_ready_success(closing);
    co_return;
}

SEASTAR_TEST_CASE(fake_metadata_fault_is_applied_before_open_effect) {
    seastar::chunked_vector<fault_rule> rules;
    rules.push_back(rule(
      31, builtin_fault_point::file_open, 1, 1, fault_decision::make_error()));
    fixture environment{std::move(rules)};
    auto creating = environment.files->create_directories(path("/kwaque/data"));
    co_await pump_until(environment.events, creating);
    co_await require_ready_success(creating);
    auto opening = environment.files->open(
      path("/kwaque/data/file"),
      {.access = kwaque::runtime::file_access::read_write, .create = true});
    co_await pump_until(environment.events, opening);
    const auto rejected = co_await std::move(opening);
    BOOST_REQUIRE(!rejected.has_value());
    BOOST_CHECK(rejected.error().code() == kwaque::errc::fault_injected);

    auto checking = environment.files->exists(path("/kwaque/data/file"));
    co_await pump_until(environment.events, checking);
    const auto exists = co_await std::move(checking);
    BOOST_REQUIRE(exists.has_value());
    BOOST_CHECK(!*exists);
    co_return;
}

SEASTAR_TEST_CASE(fake_drop_completion_remains_bounded_and_parked) {
    seastar::chunked_vector<fault_rule> rules;
    rules.push_back(rule(
      32,
      builtin_fault_point::file_exists,
      1,
      1,
      fault_decision::make_drop_completion()));
    fixture environment{std::move(rules)};
    auto waiting = environment.files->exists(path("/kwaque/missing"));
    BOOST_CHECK(!waiting.available());
    const auto advanced = environment.events.advance_to_next();
    BOOST_REQUIRE(advanced.has_value());
    BOOST_REQUIRE(advanced->has_value());
    const auto ran = environment.events.run_ready();
    BOOST_REQUIRE(ran.has_value());
    co_await seastar::yield();
    BOOST_CHECK(!waiting.available());
    BOOST_CHECK(environment.files->pending_operations() == 1U);
    BOOST_CHECK(environment.files->pending_bytes().value() == 0U);
    auto stopping = environment.files->stop();
    BOOST_CHECK(!stopping.available());
    co_await pump_until(environment.events, waiting);
    const auto aborted = co_await std::move(waiting);
    BOOST_REQUIRE(!aborted.has_value());
    BOOST_CHECK(aborted.error().code() == kwaque::errc::aborted);
    BOOST_REQUIRE(stopping.available());
    co_await require_ready_success(stopping);
    BOOST_CHECK(environment.files->pending_operations() == 0U);
    BOOST_CHECK(
      environment.files->state()
      == kwaque::simulation::fake_file_system_state::stopped);
    co_return;
}

SEASTAR_TEST_CASE(fake_delayed_writes_follow_scheduler_completion_order) {
    seastar::chunked_vector<fault_rule> rules;
    rules.push_back(rule(
      1,
      builtin_fault_point::file_write,
      1,
      1,
      fault_decision::make_delay(kwaque::runtime::monotonic_duration{50})));
    fixture environment{std::move(rules)};
    auto creating = environment.files->create_directories(path("/kwaque/data"));
    co_await pump_until(environment.events, creating);
    co_await require_ready_success(creating);

    auto open_one = environment.files->open(
      path("/kwaque/data/file"),
      {.access = kwaque::runtime::file_access::read_write, .create = true});
    co_await pump_until(environment.events, open_one);
    auto first_handle = co_await std::move(open_one);
    BOOST_REQUIRE(first_handle.has_value());
    auto open_two = environment.files->open(
      path("/kwaque/data/file"),
      {.access = kwaque::runtime::file_access::read_write});
    co_await pump_until(environment.events, open_two);
    auto second_handle = co_await std::move(open_two);
    BOOST_REQUIRE(second_handle.has_value());

    auto first = first_handle->write(
      kwaque::runtime::file_position{0}, payload("first"));
    auto second = second_handle->write(
      kwaque::runtime::file_position{0}, payload("later"));
    co_await pump_until(environment.events, second);
    co_await require_ready_success(second);
    co_await pump_until(environment.events, first);
    co_await require_ready_success(first);

    auto reading = second_handle->read(
      kwaque::runtime::file_position{0}, kwaque::byte_count{16});
    co_await pump_until(environment.events, reading);
    auto observed = co_await std::move(reading);
    BOOST_REQUIRE(observed.has_value());
    BOOST_CHECK(observed->data().content_equals("first"));

    auto close_one = first_handle->close();
    auto close_two = second_handle->close();
    co_await pump_until(environment.events, close_one);
    co_await pump_until(environment.events, close_two);
    co_await require_ready_success(close_one);
    co_await require_ready_success(close_two);
    co_return;
}

SEASTAR_TEST_CASE(fake_read_corruption_and_misdirection_are_range_exact) {
    seastar::chunked_vector<fault_rule> rules;
    rules.push_back(rule(
      40,
      builtin_fault_point::file_read,
      1,
      1,
      fault_decision::make_corrupt()));
    rules.push_back(rule(
      41,
      builtin_fault_point::file_read,
      2,
      2,
      fault_decision::make_misdirect()));
    fixture environment{std::move(rules)};
    auto creating = environment.files->create_directories(path("/kwaque/data"));
    co_await pump_until(environment.events, creating);
    co_await require_ready_success(creating);
    auto opening = environment.files->open(
      path("/kwaque/data/file"),
      {.access = kwaque::runtime::file_access::read_write, .create = true});
    co_await pump_until(environment.events, opening);
    auto opened = co_await std::move(opening);
    BOOST_REQUIRE(opened.has_value());
    auto file = std::move(*opened);

    const std::string source = std::string(4'096, 'a')
                               + std::string(4'096, 'b');
    auto writing = file.write(
      kwaque::runtime::file_position{0}, payload(source));
    co_await pump_until(environment.events, writing);
    co_await require_ready_success(writing);

    auto corrupting = file.read(
      kwaque::runtime::file_position{0}, kwaque::byte_count{4'096});
    co_await pump_until(environment.events, corrupting);
    auto corrupted = co_await std::move(corrupting);
    BOOST_REQUIRE(corrupted.has_value());
    auto corrupted_bytes = corrupted->data().linearize(
      kwaque::byte_count{4'096});
    BOOST_REQUIRE(corrupted_bytes.has_value());
    const std::string_view corrupted_view{
      corrupted_bytes->get(), corrupted_bytes->size()};
    BOOST_CHECK(
      std::count(corrupted_view.begin(), corrupted_view.end(), 'a') == 4'095);

    auto misdirecting = file.read(
      kwaque::runtime::file_position{0}, kwaque::byte_count{4'096});
    co_await pump_until(environment.events, misdirecting);
    auto misdirected = co_await std::move(misdirecting);
    BOOST_REQUIRE(misdirected.has_value());
    BOOST_CHECK(misdirected->data().content_equals(std::string(4'096, 'b')));

    auto closing = file.close();
    co_await pump_until(environment.events, closing);
    co_await require_ready_success(closing);
    co_return;
}

SEASTAR_TEST_CASE(fake_write_misdirection_targets_a_disjoint_same_inode_range) {
    seastar::chunked_vector<fault_rule> rules;
    rules.push_back(rule(
      44,
      builtin_fault_point::file_write,
      1,
      1,
      fault_decision::make_misdirect()));
    rules.push_back(rule(
      42,
      builtin_fault_point::file_write,
      2,
      2,
      fault_decision::make_misdirect()));
    fixture environment{std::move(rules)};
    auto creating = environment.files->create_directories(path("/kwaque/data"));
    co_await pump_until(environment.events, creating);
    co_await require_ready_success(creating);
    auto opening = environment.files->open(
      path("/kwaque/data/file"),
      {.access = kwaque::runtime::file_access::read_write, .create = true});
    co_await pump_until(environment.events, opening);
    auto opened = co_await std::move(opening);
    BOOST_REQUIRE(opened.has_value());
    auto file = std::move(*opened);

    auto initial = file.write(
      kwaque::runtime::file_position{0}, payload(std::string(8'192, 'a')));
    co_await pump_until(environment.events, initial);
    co_await require_ready_success(initial);
    auto misdirecting = file.write(
      kwaque::runtime::file_position{0}, payload(std::string(4'096, 'b')));
    co_await pump_until(environment.events, misdirecting);
    co_await require_ready_success(misdirecting);

    auto reading = file.read(
      kwaque::runtime::file_position{0}, kwaque::byte_count{8'192});
    co_await pump_until(environment.events, reading);
    auto observed = co_await std::move(reading);
    BOOST_REQUIRE(observed.has_value());
    BOOST_CHECK(observed->data().content_equals(
      std::string(4'096, 'a') + std::string(4'096, 'b')));

    bool saw_skipped = false;
    bool saw_applied = false;
    for (const auto& entry : environment.trace.entries()) {
        if (
          entry.action != kwaque::simulation::trace_action::scheduled
          || entry.kind != kwaque::simulation::trace_event_kind::file
          || (entry.result & UINT32_C(0xff))
               != static_cast<std::uint8_t>(
                 kwaque::runtime::fault_action::misdirect)) {
            continue;
        }
        saw_applied = saw_applied || (entry.result >> 8U) == 1U;
        saw_skipped = saw_skipped || (entry.result >> 8U) == 2U;
    }
    BOOST_CHECK(saw_skipped);
    BOOST_CHECK(saw_applied);

    auto closing = file.close();
    co_await pump_until(environment.events, closing);
    co_await require_ready_success(closing);
    co_return;
}

SEASTAR_TEST_CASE(fake_short_read_reports_the_native_prefix_and_eof) {
    seastar::chunked_vector<fault_rule> rules;
    rules.push_back(rule(
      43,
      builtin_fault_point::file_read,
      1,
      1,
      fault_decision::make_short_operation(kwaque::byte_count{4'096})));
    fixture environment{std::move(rules)};
    auto creating = environment.files->create_directories(path("/kwaque/data"));
    co_await pump_until(environment.events, creating);
    co_await require_ready_success(creating);
    auto opening = environment.files->open(
      path("/kwaque/data/file"),
      {.access = kwaque::runtime::file_access::read_write, .create = true});
    co_await pump_until(environment.events, opening);
    auto opened = co_await std::move(opening);
    BOOST_REQUIRE(opened.has_value());
    auto file = std::move(*opened);
    auto writing = file.write(
      kwaque::runtime::file_position{0}, payload(std::string(8'192, 'r')));
    co_await pump_until(environment.events, writing);
    co_await require_ready_success(writing);

    auto reading = file.read(
      kwaque::runtime::file_position{0}, kwaque::byte_count{8'192});
    co_await pump_until(environment.events, reading);
    auto observed = co_await std::move(reading);
    BOOST_REQUIRE(observed.has_value());
    BOOST_CHECK(observed->data().size().value() == 4'096U);
    BOOST_CHECK(observed->data().content_equals(std::string(4'096, 'r')));
    BOOST_CHECK(observed->eof());

    auto closing = file.close();
    co_await pump_until(environment.events, closing);
    co_await require_ready_success(closing);
    co_return;
}

SEASTAR_TEST_CASE(fake_short_write_recovers_through_the_runtime_owner) {
    seastar::chunked_vector<fault_rule> rules;
    rules.push_back(rule(
      10,
      builtin_fault_point::file_write,
      1,
      1,
      fault_decision::make_short_operation(kwaque::byte_count{4'096})));
    fixture environment{std::move(rules)};
    auto creating = environment.files->create_directories(path("/kwaque/data"));
    co_await pump_until(environment.events, creating);
    co_await require_ready_success(creating);
    auto opening = environment.files->open(
      path("/kwaque/data/file"),
      {.access = kwaque::runtime::file_access::read_write, .create = true});
    co_await pump_until(environment.events, opening);
    auto opened = co_await std::move(opening);
    BOOST_REQUIRE(opened.has_value());
    auto file = std::move(*opened);

    const std::string first(8'192, 's');
    auto short_write = file.write(
      kwaque::runtime::file_position{0}, payload(first));
    co_await pump_until(environment.events, short_write);
    auto short_result = co_await std::move(short_write);
    BOOST_REQUIRE(short_result.has_value());
    BOOST_CHECK(short_result->value() == first.size());

    auto reading = file.read(
      kwaque::runtime::file_position{0}, kwaque::byte_count{8'192});
    co_await pump_until(environment.events, reading);
    auto observed = co_await std::move(reading);
    BOOST_REQUIRE(observed.has_value());
    BOOST_CHECK(observed->data().content_equals(first));

    auto closing = file.close();
    co_await pump_until(environment.events, closing);
    co_await require_ready_success(closing);
    co_return;
}

SEASTAR_TEST_CASE(fake_corrupt_write_does_not_mutate_caller_input) {
    seastar::chunked_vector<fault_rule> rules;
    rules.push_back(rule(
      11,
      builtin_fault_point::file_write,
      1,
      1,
      fault_decision::make_corrupt()));
    fixture environment{std::move(rules)};
    auto creating = environment.files->create_directories(path("/kwaque/data"));
    co_await pump_until(environment.events, creating);
    co_await require_ready_success(creating);
    auto opening = environment.files->open(
      path("/kwaque/data/file"),
      {.access = kwaque::runtime::file_access::read_write, .create = true});
    co_await pump_until(environment.events, opening);
    auto opened = co_await std::move(opening);
    BOOST_REQUIRE(opened.has_value());
    auto file = std::move(*opened);

    const std::string source(4'096, 'c');
    auto writing = file.write(
      kwaque::runtime::file_position{0}, payload(source));
    co_await pump_until(environment.events, writing);
    co_await require_ready_success(writing);
    BOOST_CHECK(source == std::string(4'096, 'c'));
    auto reading = file.read(
      kwaque::runtime::file_position{0}, kwaque::byte_count{4'096});
    co_await pump_until(environment.events, reading);
    auto observed = co_await std::move(reading);
    BOOST_REQUIRE(observed.has_value());
    auto linear = observed->data().linearize(kwaque::byte_count{4'096});
    BOOST_REQUIRE(linear.has_value());
    const std::string_view corrupted{linear->get(), linear->size()};
    BOOST_CHECK(corrupted != std::string_view{source});
    BOOST_CHECK(std::count(corrupted.begin(), corrupted.end(), 'c') == 4'095);

    auto closing = file.close();
    co_await pump_until(environment.events, closing);
    co_await require_ready_success(closing);
    co_return;
}

SEASTAR_TEST_CASE(fake_torn_write_reports_full_transfer_but_applies_a_prefix) {
    seastar::chunked_vector<fault_rule> rules;
    rules.push_back(rule(
      20,
      builtin_fault_point::file_write,
      1,
      1,
      fault_decision::make_torn_write()));
    fixture environment{std::move(rules)};
    auto creating = environment.files->create_directories(path("/kwaque/data"));
    co_await pump_until(environment.events, creating);
    co_await require_ready_success(creating);
    auto opening = environment.files->open(
      path("/kwaque/data/file"),
      {.access = kwaque::runtime::file_access::read_write, .create = true});
    co_await pump_until(environment.events, opening);
    auto opened = co_await std::move(opening);
    BOOST_REQUIRE(opened.has_value());
    auto file = std::move(*opened);

    const std::string source(4'096, 't');
    auto writing = file.write(
      kwaque::runtime::file_position{0}, payload(source));
    co_await pump_until(environment.events, writing);
    auto result = co_await std::move(writing);
    BOOST_REQUIRE(result.has_value());
    BOOST_CHECK(result->value() == source.size());

    auto sizing = file.size();
    co_await pump_until(environment.events, sizing);
    auto size = co_await std::move(sizing);
    BOOST_REQUIRE(size.has_value());
    BOOST_CHECK(*size > 0U);
    BOOST_CHECK(*size < source.size());

    auto closing = file.close();
    co_await pump_until(environment.events, closing);
    co_await require_ready_success(closing);
    co_return;
}

SEASTAR_TEST_CASE(fake_partial_resize_applies_a_strict_grow_or_shrink_target) {
    const auto grown = co_await run_partial_resize_case(0, 100);
    BOOST_CHECK(grown.error == kwaque::errc::io_failure);
    BOOST_CHECK(grown.visible_size > 0U);
    BOOST_CHECK(grown.visible_size < 100U);
    BOOST_CHECK(grown.durable_size == 0U);
    BOOST_CHECK(grown.traced_target == grown.visible_size);
    BOOST_CHECK(grown.scheduled_outcome == 1U);
    BOOST_CHECK(grown.applied_entries == 1U);

    const auto shrunk = co_await run_partial_resize_case(100, 10, 1'024, true);
    BOOST_CHECK(shrunk.error == kwaque::errc::io_failure);
    BOOST_CHECK(shrunk.visible_size > 10U);
    BOOST_CHECK(shrunk.visible_size < 100U);
    BOOST_CHECK(shrunk.durable_size == 100U);
    BOOST_CHECK(shrunk.traced_target == shrunk.visible_size);
    BOOST_CHECK(shrunk.scheduled_outcome == 1U);
    BOOST_CHECK(shrunk.applied_entries == 1U);
    co_return;
}

SEASTAR_TEST_CASE(fake_partial_resize_applies_to_unaligned_write_finalization) {
    seastar::chunked_vector<fault_rule> rules;
    rules.push_back(rule(
      122,
      builtin_fault_point::file_truncate,
      1,
      1,
      fault_decision::make_partial_resize()));
    fixture environment{std::move(rules)};
    auto creating = environment.files->create_directories(path("/kwaque/data"));
    co_await pump_until(environment.events, creating);
    co_await require_ready_success(creating);
    auto opening = environment.files->open(
      path("/kwaque/data/file"),
      {.access = kwaque::runtime::file_access::read_write, .create = true});
    co_await pump_until(environment.events, opening);
    auto opened = co_await std::move(opening);
    BOOST_REQUIRE(opened.has_value());
    auto file = std::move(*opened);

    auto writing = file.write(
      kwaque::runtime::file_position{0}, payload(std::string(100, 'p')));
    co_await pump_until(environment.events, writing);
    const auto written = co_await std::move(writing);
    BOOST_REQUIRE(!written.has_value());
    BOOST_CHECK(written.error().code() == kwaque::errc::io_failure);
    const auto file_path = fake_file_test_access::resolve(
      *environment.files, "/kwaque/data/file");
    BOOST_REQUIRE(file_path.has_value());
    const auto visible = fake_file_test_access::visible_size(
      *environment.files, *file_path);
    BOOST_REQUIRE(visible.has_value());
    BOOST_CHECK(*visible > 100U);
    BOOST_CHECK(*visible < 4'096U);
    BOOST_CHECK(
      *fake_file_test_access::durable_size(*environment.files, *file_path)
      == 0U);
    const auto applied = std::ranges::find_if(
      environment.trace.entries(), [](const auto& entry) {
          return entry.action
                 == kwaque::simulation::trace_action::partial_resize_applied;
      });
    BOOST_REQUIRE(applied != environment.trace.entries().end());
    BOOST_CHECK(applied->coordinate_a == *visible);
    BOOST_CHECK(applied->coordinate_b == 4'096U);
    BOOST_CHECK(applied->value == 100U);

    auto closing = file.close();
    co_await pump_until(environment.events, closing);
    co_await require_ready_success(closing);
    auto stopping = environment.files->stop();
    if (!stopping.available()) {
        co_await pump_until(environment.events, stopping);
    }
    co_await require_ready_success(stopping);
    co_return;
}

SEASTAR_TEST_CASE(fake_partial_resize_without_an_intermediate_changes_nothing) {
    const auto same = co_await run_partial_resize_case(0, 0);
    BOOST_CHECK(same.error == kwaque::errc::io_failure);
    BOOST_CHECK(same.visible_size == 0U);
    BOOST_CHECK(same.traced_target == 0U);
    BOOST_CHECK(same.scheduled_outcome == 2U);
    BOOST_CHECK(same.applied_entries == 0U);

    const auto adjacent = co_await run_partial_resize_case(0, 1);
    BOOST_CHECK(adjacent.error == kwaque::errc::io_failure);
    BOOST_CHECK(adjacent.visible_size == 0U);
    BOOST_CHECK(adjacent.traced_target == 0U);
    BOOST_CHECK(adjacent.scheduled_outcome == 2U);
    BOOST_CHECK(adjacent.applied_entries == 0U);

    const auto adjacent_shrink = co_await run_partial_resize_case(1, 0);
    BOOST_CHECK(adjacent_shrink.error == kwaque::errc::io_failure);
    BOOST_CHECK(adjacent_shrink.visible_size == 1U);
    BOOST_CHECK(adjacent_shrink.traced_target == 0U);
    BOOST_CHECK(adjacent_shrink.scheduled_outcome == 2U);
    BOOST_CHECK(adjacent_shrink.applied_entries == 0U);
    co_return;
}

SEASTAR_TEST_CASE(fake_nonpartial_truncate_error_does_not_change_size) {
    const auto failed = co_await run_partial_resize_case(
      10, 100, 1'024, true, fault_decision::make_error());
    BOOST_CHECK(failed.error == kwaque::errc::io_failure);
    BOOST_CHECK(failed.visible_size == 10U);
    BOOST_CHECK(failed.durable_size == 10U);
    BOOST_CHECK(failed.traced_target == 0U);
    BOOST_CHECK(failed.applied_entries == 0U);
    co_return;
}

SEASTAR_TEST_CASE(
  fake_partial_resize_propagates_intermediate_capacity_failure_without_mutation) {
    const auto constrained = co_await run_partial_resize_case(
      10, 100, 10, true);
    BOOST_CHECK(constrained.error == kwaque::errc::resource_exhausted);
    BOOST_CHECK(constrained.visible_size == 10U);
    BOOST_CHECK(constrained.durable_size == 10U);
    BOOST_CHECK(constrained.traced_target > 10U);
    BOOST_CHECK(constrained.traced_target < 100U);
    BOOST_CHECK(constrained.scheduled_outcome == 1U);
    BOOST_CHECK(constrained.applied_entries == 0U);
    co_return;
}

SEASTAR_TEST_CASE(fake_partial_resize_obeys_the_flush_crash_boundary) {
    seastar::chunked_vector<fault_rule> rules;
    rules.push_back(rule(
      121,
      builtin_fault_point::file_truncate,
      2,
      3,
      fault_decision::make_partial_resize()));
    fixture environment{std::move(rules)};
    auto creating = environment.files->create_directories(path("/kwaque/data"));
    co_await pump_until(environment.events, creating);
    co_await require_ready_success(creating);
    auto syncing_root = environment.files->sync_directory(path("/kwaque"));
    co_await pump_until(environment.events, syncing_root);
    co_await require_ready_success(syncing_root);
    auto opening = environment.files->open(
      path("/kwaque/data/file"),
      {.access = kwaque::runtime::file_access::read_write, .create = true});
    co_await pump_until(environment.events, opening);
    auto opened = co_await std::move(opening);
    BOOST_REQUIRE(opened.has_value());
    auto file = std::move(*opened);
    auto syncing_data = environment.files->sync_directory(path("/kwaque/data"));
    co_await pump_until(environment.events, syncing_data);
    co_await require_ready_success(syncing_data);
    auto initial_resize = file.truncate(100);
    co_await pump_until(environment.events, initial_resize);
    co_await require_ready_success(initial_resize);
    auto initial_flush = file.flush();
    co_await pump_until(environment.events, initial_flush);
    co_await require_ready_success(initial_flush);

    auto partial = file.truncate(10);
    co_await pump_until(environment.events, partial);
    const auto partial_result = co_await std::move(partial);
    BOOST_REQUIRE(!partial_result.has_value());
    BOOST_CHECK(partial_result.error().code() == kwaque::errc::io_failure);
    const auto file_path = fake_file_test_access::resolve(
      *environment.files, "/kwaque/data/file");
    BOOST_REQUIRE(file_path.has_value());
    const auto intermediate = fake_file_test_access::visible_size(
      *environment.files, *file_path);
    BOOST_REQUIRE(intermediate.has_value());
    BOOST_CHECK(*intermediate > 10U);
    BOOST_CHECK(*intermediate < 100U);
    BOOST_CHECK(
      *fake_file_test_access::durable_size(*environment.files, *file_path)
      == 100U);

    auto crashing = environment.files->crash();
    co_await pump_until(environment.events, crashing);
    co_await require_ready_success(crashing);
    BOOST_CHECK(
      *fake_file_test_access::visible_size(*environment.files, *file_path)
      == 100U);
    BOOST_CHECK(
      *fake_file_test_access::durable_size(*environment.files, *file_path)
      == 100U);

    auto stale_close = file.close();
    co_await pump_until(environment.events, stale_close);
    co_await require_ready_success(stale_close);

    auto reopening = environment.files->open(
      path("/kwaque/data/file"),
      {.access = kwaque::runtime::file_access::read_write});
    co_await pump_until(environment.events, reopening);
    auto reopened = co_await std::move(reopening);
    BOOST_REQUIRE(reopened.has_value());
    auto second_file = std::move(*reopened);
    auto persisted_partial = second_file.truncate(10);
    co_await pump_until(environment.events, persisted_partial);
    const auto persisted_result = co_await std::move(persisted_partial);
    BOOST_REQUIRE(!persisted_result.has_value());
    BOOST_CHECK(persisted_result.error().code() == kwaque::errc::io_failure);
    const auto persisted_size = fake_file_test_access::visible_size(
      *environment.files, *file_path);
    BOOST_REQUIRE(persisted_size.has_value());
    BOOST_CHECK(*persisted_size > 10U);
    BOOST_CHECK(*persisted_size < 100U);
    auto persisted_flush = second_file.flush();
    co_await pump_until(environment.events, persisted_flush);
    co_await require_ready_success(persisted_flush);
    auto second_crash = environment.files->crash();
    co_await pump_until(environment.events, second_crash);
    co_await require_ready_success(second_crash);
    BOOST_CHECK(
      *fake_file_test_access::visible_size(*environment.files, *file_path)
      == *persisted_size);
    BOOST_CHECK(
      *fake_file_test_access::durable_size(*environment.files, *file_path)
      == *persisted_size);
    auto second_close = second_file.close();
    co_await pump_until(environment.events, second_close);
    co_await require_ready_success(second_close);

    auto stopping = environment.files->stop();
    if (!stopping.available()) {
        co_await pump_until(environment.events, stopping);
    }
    co_await require_ready_success(stopping);
    co_return;
}

SEASTAR_TEST_CASE(fake_crash_cancels_pending_operations_in_operation_id_order) {
    seastar::chunked_vector<fault_rule> rules;
    rules.push_back(rule(
      70,
      builtin_fault_point::file_exists,
      1,
      1,
      fault_decision::make_delay(kwaque::runtime::monotonic_duration{40})));
    rules.push_back(rule(
      71,
      builtin_fault_point::file_exists,
      2,
      2,
      fault_decision::make_delay(kwaque::runtime::monotonic_duration{50})));
    fixture environment{std::move(rules)};

    auto first = environment.files->exists(path("/kwaque/first"));
    auto second = environment.files->exists(path("/kwaque/second"));
    auto crashing = environment.files->crash();
    BOOST_CHECK(!first.available());
    BOOST_CHECK(!second.available());
    BOOST_CHECK(!crashing.available());

    co_await pump_until(environment.events, crashing);
    co_await require_ready_success(crashing);
    BOOST_REQUIRE(first.available());
    BOOST_REQUIRE(second.available());
    const auto first_result = co_await std::move(first);
    const auto second_result = co_await std::move(second);
    BOOST_REQUIRE(!first_result.has_value());
    BOOST_REQUIRE(!second_result.has_value());
    BOOST_CHECK(first_result.error().code() == kwaque::errc::aborted);
    BOOST_CHECK(second_result.error().code() == kwaque::errc::aborted);
    BOOST_CHECK(environment.files->pending_operations() == 0U);
    BOOST_CHECK(environment.events.pending_events() == 0U);

    std::vector<std::uint64_t> canceled;
    std::vector<std::uint64_t> discarded;
    bool saw_crash_applied = false;
    for (const auto& entry : environment.trace.entries()) {
        if (entry.action == kwaque::simulation::trace_action::canceled) {
            canceled.push_back(entry.stable_id);
        } else if (
          entry.action
          == kwaque::simulation::trace_action::operation_discarded) {
            discarded.push_back(entry.stable_id);
        } else if (
          entry.action == kwaque::simulation::trace_action::crash_applied) {
            saw_crash_applied = true;
        }
    }
    BOOST_REQUIRE(canceled.size() == 2U);
    BOOST_REQUIRE(discarded.size() == 2U);
    BOOST_CHECK(std::ranges::is_sorted(canceled));
    BOOST_CHECK(std::ranges::is_sorted(discarded));
    BOOST_CHECK(canceled == discarded);
    BOOST_CHECK(saw_crash_applied);
    co_return;
}

SEASTAR_TEST_CASE(fake_crash_fences_new_admission_until_apply) {
    fixture environment;
    auto crashing = environment.files->crash();
    BOOST_CHECK(!crashing.available());

    const auto advanced = environment.events.advance_to_next();
    BOOST_REQUIRE(advanced.has_value());
    BOOST_REQUIRE(advanced->has_value());
    const auto selected = environment.events.step();
    BOOST_REQUIRE(selected.has_value());
    BOOST_REQUIRE(*selected);
    BOOST_CHECK(
      environment.files->state()
      == kwaque::simulation::fake_file_system_state::crashing);
    BOOST_CHECK(!crashing.available());

    const auto pending_before = environment.files->pending_operations();
    auto rejected = environment.files->exists(path("/kwaque/during-crash"));
    BOOST_REQUIRE(rejected.available());
    const auto rejection = co_await std::move(rejected);
    BOOST_REQUIRE(!rejection.has_value());
    BOOST_CHECK(rejection.error().code() == kwaque::errc::unavailable);
    BOOST_CHECK(environment.files->pending_operations() == pending_before);

    co_await pump_until(environment.events, crashing);
    co_await require_ready_success(crashing);
    BOOST_CHECK(
      environment.files->state()
      == kwaque::simulation::fake_file_system_state::open);

    auto accepted = environment.files->exists(path("/kwaque/after-crash"));
    BOOST_CHECK(!accepted.available());
    co_await pump_until(environment.events, accepted);
    const auto result = co_await std::move(accepted);
    BOOST_REQUIRE(result.has_value());
    BOOST_CHECK(!*result);
    co_return;
}

SEASTAR_TEST_CASE(fake_crash_restores_only_completed_durable_boundaries) {
    seastar::chunked_vector<fault_rule> rules;
    rules.push_back(rule(
      72, builtin_fault_point::file_write, 3, 3, fault_decision::make_crash()));
    fixture environment{std::move(rules)};

    auto creating = environment.files->create_directories(path("/kwaque/data"));
    co_await pump_until(environment.events, creating);
    co_await require_ready_success(creating);
    auto syncing_root = environment.files->sync_directory(path("/kwaque"));
    co_await pump_until(environment.events, syncing_root);
    co_await require_ready_success(syncing_root);

    auto opening = environment.files->open(
      path("/kwaque/data/file"),
      {.access = kwaque::runtime::file_access::read_write, .create = true});
    co_await pump_until(environment.events, opening);
    auto opened = co_await std::move(opening);
    BOOST_REQUIRE(opened.has_value());
    auto file = std::move(*opened);
    auto syncing_directory = environment.files->sync_directory(
      path("/kwaque/data"));
    co_await pump_until(environment.events, syncing_directory);
    co_await require_ready_success(syncing_directory);

    auto initial = file.write(
      kwaque::runtime::file_position{0}, payload(std::string(4'096, 'a')));
    co_await pump_until(environment.events, initial);
    co_await require_ready_success(initial);
    auto flushing = file.flush();
    co_await pump_until(environment.events, flushing);
    co_await require_ready_success(flushing);

    auto volatile_write = file.write(
      kwaque::runtime::file_position{0}, payload(std::string(4'096, 'b')));
    co_await pump_until(environment.events, volatile_write);
    co_await require_ready_success(volatile_write);
    auto first_crash = environment.files->crash();
    co_await pump_until(environment.events, first_crash);
    co_await require_ready_success(first_crash);

    auto stale_size = file.size();
    const auto stale = co_await std::move(stale_size);
    BOOST_REQUIRE(!stale.has_value());
    BOOST_CHECK(stale.error().code() == kwaque::errc::aborted);
    auto stale_close = file.close();
    co_await require_ready_success(stale_close);

    auto reopening = environment.files->open(
      path("/kwaque/data/file"),
      {.access = kwaque::runtime::file_access::read_write});
    co_await pump_until(environment.events, reopening);
    auto reopened = co_await std::move(reopening);
    BOOST_REQUIRE(reopened.has_value());
    auto current = std::move(*reopened);
    auto reading = current.read(
      kwaque::runtime::file_position{0}, kwaque::byte_count{4'096});
    co_await pump_until(environment.events, reading);
    auto observed = co_await std::move(reading);
    BOOST_REQUIRE(observed.has_value());
    BOOST_CHECK(observed->data().content_equals(std::string(4'096, 'a')));

    auto crash_write = current.write(
      kwaque::runtime::file_position{0}, payload(std::string(4'096, 'c')));
    co_await pump_until(environment.events, crash_write);
    const auto aborted = co_await std::move(crash_write);
    BOOST_REQUIRE(!aborted.has_value());
    BOOST_CHECK(aborted.error().code() == kwaque::errc::aborted);
    auto crash_close = current.close();
    co_await require_ready_success(crash_close);

    auto final_open = environment.files->open(
      path("/kwaque/data/file"),
      {.access = kwaque::runtime::file_access::read_write});
    co_await pump_until(environment.events, final_open);
    auto final_file = co_await std::move(final_open);
    BOOST_REQUIRE(final_file.has_value());
    auto final_read = final_file->read(
      kwaque::runtime::file_position{0}, kwaque::byte_count{4'096});
    co_await pump_until(environment.events, final_read);
    auto final_bytes = co_await std::move(final_read);
    BOOST_REQUIRE(final_bytes.has_value());
    BOOST_CHECK(final_bytes->data().content_equals(std::string(4'096, 'a')));
    auto final_close = final_file->close();
    co_await pump_until(environment.events, final_close);
    co_await require_ready_success(final_close);
    BOOST_CHECK(environment.files->pending_operations() == 0U);
    co_return;
}

SEASTAR_TEST_CASE(
  fake_crash_drains_bulk_and_scalar_reads_with_intent_cancellation) {
    seastar::chunked_vector<fault_rule> rules;
    rules.push_back(rule(
      73,
      builtin_fault_point::file_read,
      1,
      2,
      fault_decision::make_delay(kwaque::runtime::monotonic_duration{50})));
    fixture environment{std::move(rules)};
    auto creating = environment.files->create_directories(path("/kwaque/data"));
    co_await pump_until(environment.events, creating);
    co_await require_ready_success(creating);
    auto opening = environment.files->open(
      path("/kwaque/data/file"),
      {.access = kwaque::runtime::file_access::read_write, .create = true});
    co_await pump_until(environment.events, opening);
    auto opened = co_await std::move(opening);
    BOOST_REQUIRE(opened.has_value());
    auto file = std::move(*opened);
    auto writing = file.write(
      kwaque::runtime::file_position{0}, payload(std::string(4'096, 'r')));
    co_await pump_until(environment.events, writing);
    co_await require_ready_success(writing);
    auto flushing = file.flush();
    co_await pump_until(environment.events, flushing);
    co_await require_ready_success(flushing);

    auto resolved = fake_file_test_access::resolve(
      *environment.files, "/kwaque/data/file");
    BOOST_REQUIRE(resolved.has_value());
    auto made = fake_file_test_access::make_native_file_probe(
      *environment.files, *resolved, kwaque::runtime::file_access::read_only);
    BOOST_REQUIRE(made.has_value());
    auto scalar_file = std::move(*made);
    const auto reads_before = fake_file_test_access::submitted(
      *environment.files, fake_submission_kind::read);
    auto scalar_read = scalar_file.read_scalar();
    auto bulk_read = file.read(
      kwaque::runtime::file_position{0}, kwaque::byte_count{4'096});
    co_await fake_file_test_access::wait_submitted(
      *environment.files, fake_submission_kind::read, reads_before + 2U);
    scalar_file.cancel_intent();
    auto crashing = environment.files->crash();
    co_await pump_until(environment.events, crashing);
    co_await require_ready_success(crashing);

    const auto bulk_result = co_await std::move(bulk_read);
    BOOST_REQUIRE(!bulk_result.has_value());
    BOOST_CHECK(bulk_result.error().code() == kwaque::errc::aborted);
    try {
        static_cast<void>(co_await std::move(scalar_read));
        BOOST_FAIL("crash-canceled scalar read completed successfully");
    } catch (const std::system_error& error) {
        BOOST_CHECK(
          error.code() == std::make_error_code(std::errc::operation_canceled));
    }
    BOOST_CHECK(environment.files->pending_reads() == 0U);
    BOOST_CHECK(environment.files->pending_operations() == 0U);
    auto close_bulk = file.close();
    auto close_scalar = scalar_file.close();
    co_await require_ready_success(close_bulk);
    co_await std::move(close_scalar);
    co_return;
}

SEASTAR_TEST_CASE(
  fake_native_scalar_read_observes_intent_cancellation_at_dispatch) {
    fixture environment;
    const auto file_path = fake_file_test_access::resolve(
      *environment.files, "/kwaque/file");
    BOOST_REQUIRE(file_path.has_value());
    BOOST_REQUIRE(
      fake_file_test_access::create_file(*environment.files, *file_path)
        .has_value());
    auto made = fake_file_test_access::make_native_file_probe(
      *environment.files, *file_path, kwaque::runtime::file_access::read_only);
    BOOST_REQUIRE(made.has_value());
    auto scalar_file = std::move(*made);

    const auto reads_before = fake_file_test_access::submitted(
      *environment.files, fake_submission_kind::read);
    auto scalar_read = scalar_file.read_scalar();
    co_await fake_file_test_access::wait_submitted(
      *environment.files, fake_submission_kind::read, reads_before + 1U);
    scalar_file.cancel_intent();
    co_await pump_until(environment.events, scalar_read);
    try {
        static_cast<void>(co_await std::move(scalar_read));
        BOOST_FAIL("intent-canceled scalar read completed successfully");
    } catch (const seastar::cancelled_error&) {
        // The pinned intent_reference reports cancellation with this type.
    }
    BOOST_CHECK(environment.files->pending_reads() == 0U);
    BOOST_CHECK(environment.files->pending_operations() == 0U);

    auto closing = scalar_file.close();
    co_await pump_until(environment.events, closing);
    co_await std::move(closing);
    co_return;
}

SEASTAR_TEST_CASE(fake_read_and_write_iops_limits_are_independent) {
    seastar::chunked_vector<fault_rule> rules;
    rules.push_back(rule(
      74,
      builtin_fault_point::file_read,
      1,
      1,
      fault_decision::make_delay(kwaque::runtime::monotonic_duration{50})));
    rules.push_back(rule(
      75,
      builtin_fault_point::file_write,
      2,
      2,
      fault_decision::make_delay(kwaque::runtime::monotonic_duration{50})));
    fake_file_system_config config;
    config.maximum_pending_operations = 4;
    config.maximum_pending_reads = 1;
    config.maximum_pending_writes = 1;
    fixture environment{std::move(rules), config};
    auto creating = environment.files->create_directories(path("/kwaque/data"));
    co_await pump_until(environment.events, creating);
    co_await require_ready_success(creating);
    auto first_open = environment.files->open(
      path("/kwaque/data/file"),
      {.access = kwaque::runtime::file_access::read_write, .create = true});
    co_await pump_until(environment.events, first_open);
    auto first_result = co_await std::move(first_open);
    BOOST_REQUIRE(first_result.has_value());
    auto first = std::move(*first_result);
    auto initial = first.write(
      kwaque::runtime::file_position{0}, payload(std::string(4'096, 'i')));
    co_await pump_until(environment.events, initial);
    co_await require_ready_success(initial);
    auto second_open = environment.files->open(
      path("/kwaque/data/file"),
      {.access = kwaque::runtime::file_access::read_write});
    co_await pump_until(environment.events, second_open);
    auto second_result = co_await std::move(second_open);
    BOOST_REQUIRE(second_result.has_value());
    auto second = std::move(*second_result);

    const auto reads_before = fake_file_test_access::submitted(
      *environment.files, fake_submission_kind::read);
    auto first_read = first.read(
      kwaque::runtime::file_position{0}, kwaque::byte_count{4'096});
    co_await fake_file_test_access::wait_submitted(
      *environment.files, fake_submission_kind::read, reads_before + 1U);
    auto second_read = second.read(
      kwaque::runtime::file_position{0}, kwaque::byte_count{4'096});
    while (!second_read.available()) {
        co_await seastar::yield();
    }
    const auto rejected_read = co_await std::move(second_read);
    BOOST_REQUIRE(!rejected_read.has_value());
    BOOST_CHECK(
      rejected_read.error().code() == kwaque::errc::resource_exhausted);
    co_await pump_until(environment.events, first_read);
    co_await require_ready_success(first_read);

    const auto writes_before = fake_file_test_access::submitted(
      *environment.files, fake_submission_kind::write);
    auto first_write = first.write(
      kwaque::runtime::file_position{0}, payload(std::string(4'096, 'a')));
    co_await fake_file_test_access::wait_submitted(
      *environment.files, fake_submission_kind::write, writes_before + 1U);
    auto second_write = second.write(
      kwaque::runtime::file_position{0}, payload(std::string(4'096, 'b')));
    while (!second_write.available()) {
        co_await seastar::yield();
    }
    const auto rejected_write = co_await std::move(second_write);
    BOOST_REQUIRE(!rejected_write.has_value());
    BOOST_CHECK(
      rejected_write.error().code() == kwaque::errc::resource_exhausted);
    co_await pump_until(environment.events, first_write);
    co_await require_ready_success(first_write);
    BOOST_CHECK(environment.files->pending_reads() == 0U);
    BOOST_CHECK(environment.files->pending_writes() == 0U);
    auto close_first = first.close();
    auto close_second = second.close();
    co_await pump_until(environment.events, close_first);
    co_await pump_until(environment.events, close_second);
    co_await require_ready_success(close_first);
    co_await require_ready_success(close_second);
    co_return;
}

SEASTAR_TEST_CASE(fake_read_and_write_latency_are_operation_specific) {
    fake_file_system_config config;
    config.base_latency = kwaque::runtime::monotonic_duration{5};
    config.read_latency_min = kwaque::runtime::monotonic_duration{7};
    config.read_latency_mean = kwaque::runtime::monotonic_duration{7};
    config.write_latency_min = kwaque::runtime::monotonic_duration{11};
    config.write_latency_mean = kwaque::runtime::monotonic_duration{11};
    fixture environment{{}, config};
    auto creating = environment.files->create_directories(path("/kwaque/data"));
    co_await pump_until(environment.events, creating);
    co_await require_ready_success(creating);
    auto opening = environment.files->open(
      path("/kwaque/data/file"),
      {.access = kwaque::runtime::file_access::read_write, .create = true});
    co_await pump_until(environment.events, opening);
    auto opened = co_await std::move(opening);
    BOOST_REQUIRE(opened.has_value());
    auto file = std::move(*opened);

    const auto before_write = environment.events.now();
    auto writing = file.write(
      kwaque::runtime::file_position{0}, payload(std::string(4'096, 'l')));
    co_await fake_file_test_access::wait_submitted(
      *environment.files, fake_submission_kind::write, 1);
    const auto write_entry = std::ranges::find_if(
      environment.trace.entries(), [](const auto& entry) {
          return entry.action == kwaque::simulation::trace_action::scheduled
                 && entry.domain
                      == kwaque::runtime::descriptor_for(
                           builtin_fault_point::file_write)
                           ->id.value();
      });
    BOOST_REQUIRE(write_entry != environment.trace.entries().end());
    BOOST_CHECK(
      write_entry->deadline.nanoseconds() == before_write.nanoseconds() + 16U);
    co_await pump_until(environment.events, writing);
    co_await require_ready_success(writing);

    const auto before_read = environment.events.now();
    auto reading = file.read(
      kwaque::runtime::file_position{0}, kwaque::byte_count{4'096});
    co_await fake_file_test_access::wait_submitted(
      *environment.files, fake_submission_kind::read, 1);
    const auto read_entry = std::ranges::find_if(
      environment.trace.entries(), [](const auto& entry) {
          return entry.action == kwaque::simulation::trace_action::scheduled
                 && entry.domain
                      == kwaque::runtime::descriptor_for(
                           builtin_fault_point::file_read)
                           ->id.value();
      });
    BOOST_REQUIRE(read_entry != environment.trace.entries().end());
    BOOST_CHECK(
      read_entry->deadline.nanoseconds() == before_read.nanoseconds() + 12U);
    co_await pump_until(environment.events, reading);
    co_await require_ready_success(reading);
    auto closing = file.close();
    co_await pump_until(environment.events, closing);
    co_await require_ready_success(closing);
    co_return;
}

SEASTAR_TEST_CASE(fake_crash_trace_reservation_saturation_is_transactional) {
    auto constrained_trace = trace_limits::make(
      trace_limit_values{
        .entries = 7,
        .encoded_bytes = kwaque::simulation::canonical_header_encoded_size
                         + 7U
                             * kwaque::simulation::canonical_entry_encoded_size,
        .line_bytes = 1'024,
      });
    BOOST_REQUIRE(constrained_trace.has_value());
    fixture environment{{}, {}, make_scheduler_limits(), *constrained_trace};
    const auto before = fake_file_test_access::snapshot(*environment.files);
    BOOST_REQUIRE(before.has_value());
    auto crashing = environment.files->crash();
    BOOST_REQUIRE(crashing.available());
    const auto rejected = co_await std::move(crashing);
    BOOST_REQUIRE(!rejected.has_value());
    BOOST_CHECK(rejected.error().code() == kwaque::errc::resource_exhausted);
    const auto after = fake_file_test_access::snapshot(*environment.files);
    BOOST_REQUIRE(after.has_value());
    BOOST_CHECK(*after == *before);
    BOOST_CHECK(environment.trace.entries().empty());
    BOOST_CHECK(environment.events.pending_events() == 0U);
    auto stopping = environment.files->stop();
    co_await require_ready_success(stopping);
    co_return;
}

SEASTAR_TEST_CASE(
  fake_filesystem_matches_independent_model_across_seeded_histories) {
    constexpr std::uint64_t model_seed{UINT64_C(0x4b5146494c454d4f)};
    constexpr std::uint64_t histories{256};
    constexpr std::size_t commands_per_history{32};
    constexpr std::uint64_t model_capacity{8'192};
    constexpr std::string_view model_configuration{
      "root=/kwaque;capacity=8192;objects=65536;pending=96;pending-bytes="
      "134217728;read-latency=0:0;write-latency=0:0;read-iops=64;"
      "write-iops=64"};
    constexpr std::string_view model_fault_rules{
      "file_write[1:error,2:delay(7ns),3:crash]"};
    const std::vector<storage_fault_rule> canonical_rules{
      storage_fault_rule{
        .id = 80,
        .first = 1,
        .last = 1,
        .action = storage_fault_action::error,
      },
      storage_fault_rule{
        .id = 81,
        .first = 2,
        .last = 2,
        .action = storage_fault_action::delay,
        .payload = 7,
      },
      storage_fault_rule{
        .id = 82,
        .first = 3,
        .last = 3,
        .action = storage_fault_action::crash,
      },
    };

    for (std::uint64_t history = 1; history <= histories; ++history) {
        seastar::chunked_vector<fault_rule> rules;
        for (const auto& source : canonical_rules) {
            rules.push_back(rule(source));
        }
        fake_file_system_config config;
        config.logical_capacity = kwaque::byte_count{model_capacity};
        fixture environment{std::move(rules), config};

        auto creating = environment.files->create_directories(
          path("/kwaque/data"));
        co_await pump_until(environment.events, creating);
        co_await require_ready_success(creating);
        auto syncing = environment.files->sync_directory(path("/kwaque"));
        co_await pump_until(environment.events, syncing);
        co_await require_ready_success(syncing);

        dense_storage_model model{canonical_rules, seed};
        storage_workload_generator generator{model_seed, history};
        std::vector<storage_command> script;
        script.reserve(commands_per_history);
        compare_model_state(*environment.files, model);
        for (std::size_t index = 0; index < commands_per_history; ++index) {
            const auto command = generator.next(model);
            script.push_back(command);
            const auto observed = co_await execute_model_command(
              environment, command);
            BOOST_TEST_CONTEXT(
              kwaque::simulation::testing::describe(
                model_seed,
                history,
                model_configuration,
                model_fault_rules,
                script)) {
                BOOST_REQUIRE(model.reconcile(command, observed));
                compare_model_state(*environment.files, model);
            }
        }

        auto stopping = environment.files->stop();
        if (!stopping.available()) {
            co_await pump_until(environment.events, stopping);
        }
        co_await require_ready_success(stopping);
    }
    co_return;
}

SEASTAR_TEST_CASE(
  fake_partial_resize_matches_dense_model_after_an_aligned_write) {
    const std::vector<storage_fault_rule> canonical_rules{
      storage_fault_rule{
        .id = 83,
        .point = storage_command_kind::truncate,
        .first = 1,
        .last = 1,
        .action = storage_fault_action::partial_resize,
      },
    };
    seastar::chunked_vector<fault_rule> rules;
    for (const auto& source : canonical_rules) {
        rules.push_back(rule(source));
    }
    fake_file_system_config config;
    config.logical_capacity = kwaque::byte_count{8'192};
    fixture environment{std::move(rules), config};
    auto creating = environment.files->create_directories(path("/kwaque/data"));
    co_await pump_until(environment.events, creating);
    co_await require_ready_success(creating);
    auto syncing = environment.files->sync_directory(path("/kwaque"));
    co_await pump_until(environment.events, syncing);
    co_await require_ready_success(syncing);

    dense_storage_model model{canonical_rules, seed};
    compare_model_state(*environment.files, model);
    const storage_command write{
      .kind = storage_command_kind::write,
      .source = 0,
      .position = 0,
      .length = 4'096,
      .value = static_cast<std::byte>('p'),
    };
    const auto write_outcome = co_await execute_model_command(
      environment, write);
    BOOST_REQUIRE(model.reconcile(write, write_outcome));
    compare_model_state(*environment.files, model);

    const storage_command truncate{
      .kind = storage_command_kind::truncate,
      .source = 0,
      .length = 10,
    };
    const auto truncate_outcome = co_await execute_model_command(
      environment, truncate);
    BOOST_CHECK(truncate_outcome == storage_outcome::io_failure);
    BOOST_REQUIRE(model.reconcile(truncate, truncate_outcome));
    compare_model_state(*environment.files, model);

    auto stopping = environment.files->stop();
    if (!stopping.available()) {
        co_await pump_until(environment.events, stopping);
    }
    co_await require_ready_success(stopping);
    co_return;
}

SEASTAR_TEST_CASE(
  fake_independent_model_covers_overlap_drop_and_pending_saturation) {
    const std::vector<storage_fault_rule> canonical_rules{
      storage_fault_rule{
        .id = 90,
        .first = 1,
        .last = 1,
        .action = storage_fault_action::delay,
        .payload = 50,
      },
      storage_fault_rule{
        .id = 91,
        .first = 3,
        .last = 3,
        .action = storage_fault_action::drop_completion,
      },
    };
    seastar::chunked_vector<fault_rule> rules;
    for (const auto& source : canonical_rules) {
        rules.push_back(rule(source));
    }
    fake_file_system_config config;
    config.maximum_pending_operations = 2;
    fixture environment{std::move(rules), config};

    auto creating = environment.files->create_directories(path("/kwaque/data"));
    co_await pump_until(environment.events, creating);
    co_await require_ready_success(creating);
    auto syncing_root = environment.files->sync_directory(path("/kwaque"));
    co_await pump_until(environment.events, syncing_root);
    co_await require_ready_success(syncing_root);
    auto opening = environment.files->open(
      path("/kwaque/data/alpha"),
      {.access = kwaque::runtime::file_access::read_write, .create = true});
    co_await pump_until(environment.events, opening);
    auto opened = co_await std::move(opening);
    BOOST_REQUIRE(opened.has_value());
    auto first_handle = std::move(*opened);
    auto opening_again = environment.files->open(
      path("/kwaque/data/alpha"),
      {.access = kwaque::runtime::file_access::read_write});
    co_await pump_until(environment.events, opening_again);
    auto opened_again = co_await std::move(opening_again);
    BOOST_REQUIRE(opened_again.has_value());
    auto second_handle = std::move(*opened_again);
    auto syncing_directory = environment.files->sync_directory(
      path("/kwaque/data"));
    co_await pump_until(environment.events, syncing_directory);
    co_await require_ready_success(syncing_directory);

    const storage_command first_command{
      .kind = storage_command_kind::write,
      .source = 0,
      .position = 0,
      .length = 4'096,
      .value = static_cast<std::byte>('a'),
    };
    const storage_command second_command{
      .kind = storage_command_kind::write,
      .source = 0,
      .position = 0,
      .length = 4'096,
      .value = static_cast<std::byte>('b'),
    };
    auto delayed = first_handle.write(
      kwaque::runtime::file_position{0}, payload(std::string(4'096, 'a')));
    auto overtaking = second_handle.write(
      kwaque::runtime::file_position{0}, payload(std::string(4'096, 'b')));
    co_await pump_until(environment.events, overtaking);
    co_await require_ready_success(overtaking);
    co_await pump_until(environment.events, delayed);
    co_await require_ready_success(delayed);

    dense_storage_model model{canonical_rules};
    BOOST_REQUIRE(model.reconcile(second_command, storage_outcome::success));
    BOOST_REQUIRE(model.reconcile(first_command, storage_outcome::success));
    BOOST_REQUIRE(model.reconcile(
      storage_command{.kind = storage_command_kind::sync_directory},
      storage_outcome::success));
    compare_model_state(*environment.files, model, 2);

    const storage_command dropped_command{
      .kind = storage_command_kind::write,
      .source = 0,
      .position = 4'096,
      .length = 4'096,
      .value = static_cast<std::byte>('d'),
    };
    auto dropped = first_handle.write(
      kwaque::runtime::file_position{4'096}, payload(std::string(4'096, 'd')));
    co_await fake_file_test_access::wait_submitted(
      *environment.files, fake_submission_kind::write, 3);
    const auto advanced = environment.events.advance_to_next();
    BOOST_REQUIRE(advanced.has_value());
    BOOST_REQUIRE(advanced->has_value());
    BOOST_REQUIRE(environment.events.run_ready().has_value());
    co_await seastar::yield();
    BOOST_CHECK(!dropped.available());
    BOOST_REQUIRE(model.reconcile(dropped_command, storage_outcome::success));
    compare_model_state(*environment.files, model, 2, 1, 4'096, 0, 1);

    auto admitted = environment.files->exists(path("/kwaque/missing"));
    auto saturated = environment.files->exists(path("/kwaque/other"));
    BOOST_REQUIRE(saturated.available());
    const auto rejected = co_await std::move(saturated);
    BOOST_REQUIRE(!rejected.has_value());
    BOOST_CHECK(rejected.error().code() == kwaque::errc::queue_full);
    co_await pump_until(environment.events, admitted);
    co_await require_ready_success(admitted);

    auto crashing = environment.files->crash();
    co_await pump_until(environment.events, crashing);
    co_await require_ready_success(crashing);
    BOOST_REQUIRE(dropped.available());
    const auto dropped_result = co_await std::move(dropped);
    BOOST_REQUIRE(!dropped_result.has_value());
    BOOST_CHECK(dropped_result.error().code() == kwaque::errc::aborted);
    BOOST_REQUIRE(model.reconcile(
      storage_command{.kind = storage_command_kind::crash},
      storage_outcome::success));
    compare_model_state(*environment.files, model);

    auto close_first = first_handle.close();
    auto close_second = second_handle.close();
    co_await require_ready_success(close_first);
    co_await require_ready_success(close_second);
    co_return;
}

SEASTAR_TEST_CASE(
  fake_graceful_stop_drains_without_rolling_back_visible_state) {
    fixture environment;
    auto creating = environment.files->create_directories(path("/kwaque/data"));
    co_await pump_until(environment.events, creating);
    co_await require_ready_success(creating);
    auto opening = environment.files->open(
      path("/kwaque/data/file"),
      {.access = kwaque::runtime::file_access::read_write, .create = true});
    co_await pump_until(environment.events, opening);
    auto opened = co_await std::move(opening);
    BOOST_REQUIRE(opened.has_value());
    auto file = std::move(*opened);
    auto writing = file.write(
      kwaque::runtime::file_position{0}, payload(std::string(4'096, 'v')));
    co_await pump_until(environment.events, writing);
    co_await require_ready_success(writing);
    auto captured_before = fake_file_test_access::snapshot(*environment.files);
    BOOST_REQUIRE(captured_before.has_value());
    auto before = std::move(*captured_before);
    for (auto& object : before.objects) {
        object.open_references = 0;
    }

    auto stopping = environment.files->stop();
    if (!stopping.available()) {
        co_await pump_until(environment.events, stopping);
    }
    co_await require_ready_success(stopping);
    const auto captured_after = fake_file_test_access::snapshot(
      *environment.files);
    BOOST_REQUIRE(captured_after.has_value());
    const auto& after = *captured_after;
    BOOST_CHECK(after.objects == before.objects);
    BOOST_CHECK(after.retained_capacity == before.retained_capacity);
    BOOST_CHECK(after.generation != before.generation);
    BOOST_CHECK(after.open_handles == 0U);

    auto rejected = environment.files->exists(path("/kwaque/data/file"));
    BOOST_REQUIRE(rejected.available());
    const auto closed = co_await std::move(rejected);
    BOOST_REQUIRE(!closed.has_value());
    BOOST_CHECK(closed.error().code() == kwaque::errc::closed);
    auto stopped_again = environment.files->stop();
    BOOST_REQUIRE(stopped_again.available());
    co_await require_ready_success(stopped_again);
    auto close = file.close();
    co_await require_ready_success(close);
    co_return;
}

SEASTAR_TEST_CASE(fake_filesystem_has_one_nontransportable_shard_owner) {
    fixture environment;
    auto creating = environment.files->create_directories(path("/kwaque/data"));
    co_await pump_until(environment.events, creating);
    co_await require_ready_success(creating);
    auto opening = environment.files->open(
      path("/kwaque/data/file"),
      {.access = kwaque::runtime::file_access::read_write, .create = true});
    co_await pump_until(environment.events, opening);
    auto opened = co_await std::move(opening);
    BOOST_REQUIRE(opened.has_value());
    auto file = std::move(*opened);
    const auto filesystem_owner = environment.files->owner();
    const auto handle_owner = file.owner();
    BOOST_CHECK(filesystem_owner.is_current());
    BOOST_CHECK(handle_owner.is_current());
    const auto before = fake_file_test_access::snapshot(*environment.files);
    BOOST_REQUIRE(before.has_value());
    BOOST_REQUIRE_GE(seastar::this_smp_shard_count(), 2U);
    const auto rejected = co_await seastar::smp::submit_to(
      1, [filesystem_owner, handle_owner] {
          return !filesystem_owner.is_current() && !handle_owner.is_current();
      });
    BOOST_CHECK(rejected);
    const auto after = fake_file_test_access::snapshot(*environment.files);
    BOOST_REQUIRE(after.has_value());
    BOOST_CHECK(*after == *before);
    auto closing = file.close();
    co_await pump_until(environment.events, closing);
    co_await require_ready_success(closing);
    co_return;
}
