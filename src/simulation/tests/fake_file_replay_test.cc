#include "src/bytes/fragmented_buffer.h"
#include "src/runtime/file.h"
#include "src/simulation/determinism_version.h"
#include "src/simulation/event_trace.h"
#include "src/simulation/fake_file.h"
#include "src/simulation/fake_file_test_support.h"
#include "src/simulation/fault_schedule.h"
#include "src/simulation/scheduler.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/later.hh>

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

using kwaque::runtime::builtin_fault_point;
using kwaque::runtime::fault_decision;
using kwaque::runtime::fault_object_key;
using kwaque::runtime::fault_occurrence;
using kwaque::simulation::event_trace;
using kwaque::simulation::fake_file_state_digest;
using kwaque::simulation::fake_file_state_snapshot;
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
using kwaque::simulation::trace_action;
using kwaque::simulation::trace_artifact;
using kwaque::simulation::trace_digest;
using kwaque::simulation::trace_header;
using kwaque::simulation::trace_limit_values;
using kwaque::simulation::trace_limits;

constexpr std::uint64_t seed{UINT64_C(0x4b5146494c455250)};

scheduler_limits make_scheduler_limits() {
    auto limits = scheduler_limits::make(
      scheduler_limit_values{
        .pending_events = 128,
        .events_per_pump = 256,
        .total_events = 2'048,
        .maximum_deadline = kwaque::runtime::monotonic_time{1'000'000},
      });
    BOOST_REQUIRE(limits.has_value());
    return *limits;
}

trace_limits make_trace_limits() {
    auto limits = trace_limits::make(
      trace_limit_values{
        .entries = 8'192,
        .encoded_bytes = kwaque::simulation::canonical_header_encoded_size
                         + 8'192U
                             * kwaque::simulation::canonical_entry_encoded_size,
        .line_bytes = 1'024,
      });
    BOOST_REQUIRE(limits.has_value());
    return *limits;
}

trace_digest digest(std::string_view bytes) {
    std::array<std::uint64_t, 4> lanes{
      UINT64_C(0xcbf29ce484222325),
      UINT64_C(0x9e3779b97f4a7c15),
      UINT64_C(0x6a09e667f3bcc909),
      UINT64_C(0xbb67ae8584caa73b),
    };
    for (const auto byte : bytes) {
        for (std::size_t lane = 0; lane < lanes.size(); ++lane) {
            lanes[lane] ^= static_cast<unsigned char>(byte)
                           + lane * UINT64_C(0x100000001b3);
            lanes[lane] *= UINT64_C(0x100000001b3);
        }
    }
    trace_digest result{};
    for (std::size_t lane = 0; lane < lanes.size(); ++lane) {
        for (std::size_t byte = 0; byte < sizeof(std::uint64_t); ++byte) {
            result[lane * sizeof(std::uint64_t) + byte]
              = static_cast<std::uint8_t>(lanes[lane] >> (byte * 8U));
        }
    }
    return result;
}

constexpr std::string_view compound_configuration{
  "v1;root=/kwaque;capacity=268435456;objects=65536;pending=96;"
  "read-latency=0:0;write-latency=0:0;read-iops=64;write-iops=64;"
  "rules=101:file_write:4:2:delay40,102:file_write:5:2:torn,"
  "103:file_flush:4:2:delay50,104:file_flush:5:2:drop_completion"};

constexpr std::string_view compound_input_script{
  "v1;mkdir(a);mkdir(b);sync(root);open(a/file);open(b/file);sync(a);"
  "sync(b);write(a);write(b);flush(a);flush(b);parallel(delay-write-a,"
  "torn-write-b);parallel(delay-flush-a,drop-flush-b);crash;stale-ops;"
  "rename-before-sync;crash;rename;sync(b);crash;reopen-read-close"};

trace_header make_header(
  const scheduler_limits& scheduler_budget, trace_limits trace_budget) {
    return trace_header::current(
      seed,
      kwaque::simulation::deterministic_random_algorithm_version,
      kwaque::simulation::deterministic_random_coordinate_version,
      kwaque::simulation::trace_budget(scheduler_budget),
      trace_budget,
      digest(compound_configuration),
      digest(compound_input_script));
}

fault_rule rule(
  std::uint64_t id,
  builtin_fault_point point,
  std::uint64_t object,
  std::uint64_t occurrence,
  fault_decision decision) {
    auto rule_id = fault_rule_id::make(id);
    auto selected = fault_occurrence::make(occurrence);
    BOOST_REQUIRE(rule_id.has_value());
    BOOST_REQUIRE(selected.has_value());
    auto result = fault_rule::make(
      *rule_id,
      point,
      fault_object_key::from_u64(object),
      *selected,
      *selected,
      fault_selector::once(),
      decision);
    BOOST_REQUIRE(result.has_value());
    return *result;
}

fault_rule wildcard_rule(
  std::uint64_t id,
  builtin_fault_point point,
  std::uint64_t occurrence,
  fault_decision decision) {
    auto rule_id = fault_rule_id::make(id);
    auto selected = fault_occurrence::make(occurrence);
    BOOST_REQUIRE(rule_id.has_value());
    BOOST_REQUIRE(selected.has_value());
    auto result = fault_rule::make(
      *rule_id,
      point,
      std::nullopt,
      *selected,
      *selected,
      fault_selector::once(),
      decision);
    BOOST_REQUIRE(result.has_value());
    return *result;
}

seastar::chunked_vector<fault_rule> compound_rules() {
    seastar::chunked_vector<fault_rule> rules;
    rules.push_back(rule(
      101,
      builtin_fault_point::file_write,
      4,
      2,
      fault_decision::make_delay(kwaque::runtime::monotonic_duration{40})));
    rules.push_back(rule(
      102,
      builtin_fault_point::file_write,
      5,
      2,
      fault_decision::make_torn_write()));
    rules.push_back(rule(
      103,
      builtin_fault_point::file_flush,
      4,
      2,
      fault_decision::make_delay(kwaque::runtime::monotonic_duration{50})));
    rules.push_back(rule(
      104,
      builtin_fault_point::file_flush,
      5,
      2,
      fault_decision::make_drop_completion()));
    return rules;
}

enum class compound_fixture_id : std::uint8_t {
    compound_v1,
};

struct compound_reproduction final {
    fake_file_system_config file_config;
    seastar::chunked_vector<fault_rule> rules;
    compound_fixture_id fixture{compound_fixture_id::compound_v1};
};

std::optional<compound_reproduction> decode_reproduction(
  std::string_view configuration, std::string_view input_script) {
    if (
      configuration != compound_configuration
      || input_script != compound_input_script) {
        return std::nullopt;
    }
    return compound_reproduction{
      .file_config = fake_file_system_config{},
      .rules = compound_rules(),
      .fixture = compound_fixture_id::compound_v1,
    };
}

compound_reproduction reproduction() {
    auto decoded = decode_reproduction(
      compound_configuration, compound_input_script);
    BOOST_REQUIRE(decoded.has_value());
    return std::move(*decoded);
}

struct fixture final {
    scheduler events;
    std::unique_ptr<fault_schedule> faults;
    std::unique_ptr<fake_file_system> files;

    fixture(
      const scheduler_limits& scheduler_budget,
      event_trace& trace,
      seastar::chunked_vector<fault_rule> rules = compound_rules(),
      fake_file_system_config config = {})
      : events(scheduler_budget, &trace) {
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

kwaque::bytes::fragmented_buffer payload(char value) {
    const std::string bytes(4'096, value);
    auto result = kwaque::bytes::fragmented_buffer::copy_of(
      std::span{bytes.data(), bytes.size()});
    BOOST_REQUIRE(result.has_value());
    return std::move(*result);
}

template<typename Future>
seastar::future<> require_success(scheduler& events, Future& waiting) {
    co_await pump_until(events, waiting);
    co_await require_ready_success(waiting);
}

template<typename Future>
seastar::future<> require_aborted(scheduler& events, Future& waiting) {
    co_await pump_until(events, waiting);
    const auto result = co_await std::move(waiting);
    BOOST_REQUIRE(!result.has_value());
    BOOST_CHECK(result.error().code() == kwaque::errc::aborted);
}

struct compound_result final {
    fake_file_state_snapshot state;
    fake_file_state_digest digest;
    std::vector<kwaque::errc> terminal_errors;

    bool operator==(const compound_result&) const = default;
};

struct partial_resize_result final {
    fake_file_state_snapshot state;
    fake_file_state_digest digest;
    kwaque::errc terminal{kwaque::errc::success};
    std::uint64_t selected_target{0};

    bool operator==(const partial_resize_result&) const = default;
};

seastar::future<partial_resize_result> run_partial_resize(
  event_trace& trace, const scheduler_limits& scheduler_budget) {
    seastar::chunked_vector<fault_rule> rules;
    rules.push_back(wildcard_rule(
      401,
      builtin_fault_point::file_truncate,
      1,
      fault_decision::make_partial_resize()));
    fixture environment{scheduler_budget, trace, std::move(rules)};
    auto creating = environment.files->create_directories(path("/kwaque/data"));
    co_await require_success(environment.events, creating);
    auto opening = environment.files->open(
      path("/kwaque/data/file"),
      {.access = kwaque::runtime::file_access::read_write, .create = true});
    co_await pump_until(environment.events, opening);
    auto opened = co_await std::move(opening);
    BOOST_REQUIRE(opened.has_value());
    auto file = std::move(*opened);
    auto writing = file.write(kwaque::runtime::file_position{0}, payload('p'));
    co_await require_success(environment.events, writing);
    auto flushing = file.flush();
    co_await require_success(environment.events, flushing);

    auto truncating = file.truncate(10);
    co_await pump_until(environment.events, truncating);
    const auto truncated = co_await std::move(truncating);
    BOOST_REQUIRE(!truncated.has_value());
    BOOST_CHECK(truncated.error().code() == kwaque::errc::io_failure);
    const auto effect = std::ranges::find_if(
      trace.entries(), [](const auto& entry) {
          return entry.action == trace_action::partial_resize_applied;
      });
    BOOST_REQUIRE(effect != trace.entries().end());
    BOOST_CHECK(effect->coordinate_a > 10U);
    BOOST_CHECK(effect->coordinate_a < 4'096U);

    auto closing = file.close();
    co_await require_success(environment.events, closing);
    auto stopping = environment.files->stop();
    if (!stopping.available()) {
        co_await pump_until(environment.events, stopping);
    }
    co_await require_ready_success(stopping);
    auto state = fake_file_test_access::snapshot(*environment.files);
    auto state_digest = fake_file_test_access::state_digest(*environment.files);
    BOOST_REQUIRE(state.has_value());
    BOOST_REQUIRE(state_digest.has_value());
    co_return partial_resize_result{
      .state = std::move(*state),
      .digest = *state_digest,
      .terminal = truncated.error().code(),
      .selected_target = effect->coordinate_a,
    };
}

seastar::future<compound_result> run_compound(
  event_trace& trace,
  const scheduler_limits& scheduler_budget,
  compound_reproduction input) {
    BOOST_REQUIRE(input.fixture == compound_fixture_id::compound_v1);
    fixture environment{
      scheduler_budget,
      trace,
      std::move(input.rules),
      std::move(input.file_config)};
    auto create_a = environment.files->create_directories(path("/kwaque/a"));
    co_await require_success(environment.events, create_a);
    auto create_b = environment.files->create_directories(path("/kwaque/b"));
    co_await require_success(environment.events, create_b);
    auto sync_root = environment.files->sync_directory(path("/kwaque"));
    co_await require_success(environment.events, sync_root);

    auto open_a = environment.files->open(
      path("/kwaque/a/file"),
      {.access = kwaque::runtime::file_access::read_write, .create = true});
    co_await pump_until(environment.events, open_a);
    auto opened_a = co_await std::move(open_a);
    BOOST_REQUIRE(opened_a.has_value());
    auto file_a = std::move(*opened_a);
    auto open_b = environment.files->open(
      path("/kwaque/b/file"),
      {.access = kwaque::runtime::file_access::read_write, .create = true});
    co_await pump_until(environment.events, open_b);
    auto opened_b = co_await std::move(open_b);
    BOOST_REQUIRE(opened_b.has_value());
    auto file_b = std::move(*opened_b);
    const auto path_a = fake_file_test_access::resolve(
      *environment.files, "/kwaque/a/file");
    const auto path_b = fake_file_test_access::resolve(
      *environment.files, "/kwaque/b/file");
    BOOST_REQUIRE(path_a.has_value());
    BOOST_REQUIRE(path_b.has_value());
    const auto object_a = fake_file_test_access::lookup(
      *environment.files, *path_a);
    const auto object_b = fake_file_test_access::lookup(
      *environment.files, *path_b);
    BOOST_REQUIRE(object_a.has_value());
    BOOST_REQUIRE(object_b.has_value());
    BOOST_REQUIRE(object_a->value() == 4U);
    BOOST_REQUIRE(object_b->value() == 5U);
    auto sync_a = environment.files->sync_directory(path("/kwaque/a"));
    co_await require_success(environment.events, sync_a);
    auto sync_b = environment.files->sync_directory(path("/kwaque/b"));
    co_await require_success(environment.events, sync_b);

    auto base_a = file_a.write(kwaque::runtime::file_position{0}, payload('a'));
    co_await require_success(environment.events, base_a);
    auto base_b = file_b.write(kwaque::runtime::file_position{0}, payload('b'));
    co_await require_success(environment.events, base_b);
    auto durable_a = file_a.flush();
    co_await require_success(environment.events, durable_a);
    auto durable_b = file_b.flush();
    co_await require_success(environment.events, durable_b);

    const auto scheduled_fault = [&trace](
                                   builtin_fault_point point,
                                   kwaque::runtime::fault_action action) {
        return std::ranges::find_if(
          trace.entries(), [point, action](const auto& entry) {
              return entry.action == trace_action::scheduled
                     && entry.domain
                          == kwaque::runtime::descriptor_for(point)->id.value()
                     && (entry.result & 0xffU)
                          == static_cast<std::uint8_t>(action);
          });
    };
    const auto writes_before = fake_file_test_access::submitted(
      *environment.files, fake_submission_kind::write);
    auto delayed_write = file_a.write(
      kwaque::runtime::file_position{0}, payload('x'));
    auto torn_write = file_b.write(
      kwaque::runtime::file_position{0}, payload('t'));
    co_await fake_file_test_access::wait_submitted(
      *environment.files, fake_submission_kind::write, writes_before + 2U);
    const auto delayed_write_event = scheduled_fault(
      builtin_fault_point::file_write, kwaque::runtime::fault_action::delay);
    const auto torn_write_event = scheduled_fault(
      builtin_fault_point::file_write,
      kwaque::runtime::fault_action::torn_write);
    BOOST_REQUIRE(delayed_write_event != trace.entries().end());
    BOOST_REQUIRE(torn_write_event != trace.entries().end());
    const auto delayed_write_id = delayed_write_event->stable_id;
    const auto delayed_write_deadline
      = delayed_write_event->deadline.nanoseconds();
    const auto torn_write_deadline = torn_write_event->deadline.nanoseconds();
    const auto torn_write_bytes = torn_write_event->coordinate_a;
    BOOST_REQUIRE(torn_write_deadline < delayed_write_deadline);
    BOOST_REQUIRE(torn_write_bytes > 0U);
    BOOST_REQUIRE(torn_write_bytes < 4'096U);
    const auto advanced_to_torn = environment.events.advance_to_next();
    BOOST_REQUIRE(advanced_to_torn.has_value());
    BOOST_REQUIRE(advanced_to_torn->has_value());
    BOOST_REQUIRE(
      advanced_to_torn->value().nanoseconds() == torn_write_deadline);
    BOOST_REQUIRE(environment.events.run_ready().has_value());
    const auto torn_write_result = co_await std::move(torn_write);
    BOOST_REQUIRE(torn_write_result.has_value());
    const auto flushes_before = fake_file_test_access::submitted(
      *environment.files, fake_submission_kind::flush);
    auto delayed_flush = file_a.flush();
    auto dropped_flush = file_b.flush();
    co_await fake_file_test_access::wait_submitted(
      *environment.files, fake_submission_kind::flush, flushes_before + 2U);
    const auto dropped_flush_event = scheduled_fault(
      builtin_fault_point::file_flush,
      kwaque::runtime::fault_action::drop_completion);
    BOOST_REQUIRE(dropped_flush_event != trace.entries().end());
    const auto dropped_flush_deadline
      = dropped_flush_event->deadline.nanoseconds();
    BOOST_REQUIRE_MESSAGE(
      dropped_flush_deadline < delayed_write_deadline,
      "dropped flush was not ordered before delayed write: dropped="
        << dropped_flush_deadline << " delayed=" << delayed_write_deadline
        << " now=" << environment.events.now().nanoseconds()
        << " writes-before=" << writes_before
        << " flushes-before=" << flushes_before);
    auto parked_flush = fake_file_test_access::wait_parked(
      *environment.files, fake_submission_kind::flush, 1);
    co_await pump_until(environment.events, parked_flush);
    co_await std::move(parked_flush);
    BOOST_CHECK(!dropped_flush.available());

    auto crashing = environment.files->crash();
    co_await require_success(environment.events, crashing);
    std::vector<kwaque::errc> terminal_errors;
    BOOST_REQUIRE(delayed_write.available());
    const auto delayed_write_result = co_await std::move(delayed_write);
    const bool delayed_write_discarded = std::ranges::any_of(
      trace.entries(), [delayed_write_id](const auto& entry) {
          return entry.action == trace_action::operation_discarded
                 && entry.stable_id == delayed_write_id;
      });
    BOOST_REQUIRE_MESSAGE(
      !delayed_write_result.has_value(),
      "delayed write completed successfully: deadline="
        << delayed_write_deadline
        << " dropped-flush-deadline=" << dropped_flush_deadline
        << " crash-completed-at=" << environment.events.now().nanoseconds()
        << " discarded=" << delayed_write_discarded);
    terminal_errors.push_back(delayed_write_result.error().code());
    BOOST_REQUIRE(delayed_flush.available());
    const auto delayed_flush_result = co_await std::move(delayed_flush);
    BOOST_REQUIRE(!delayed_flush_result.has_value());
    terminal_errors.push_back(delayed_flush_result.error().code());
    BOOST_REQUIRE(dropped_flush.available());
    const auto dropped_flush_result = co_await std::move(dropped_flush);
    BOOST_REQUIRE(!dropped_flush_result.has_value());
    terminal_errors.push_back(dropped_flush_result.error().code());
    BOOST_CHECK(std::ranges::all_of(terminal_errors, [](kwaque::errc error) {
        return error == kwaque::errc::aborted;
    }));

    auto stale_a = file_a.size();
    co_await require_aborted(environment.events, stale_a);
    auto stale_b = file_b.size();
    co_await require_aborted(environment.events, stale_b);
    auto close_a = file_a.close();
    co_await require_success(environment.events, close_a);
    auto close_b = file_b.close();
    co_await require_success(environment.events, close_b);

    auto reopen_b = environment.files->open(
      path("/kwaque/b/file"),
      {.access = kwaque::runtime::file_access::read_write});
    co_await pump_until(environment.events, reopen_b);
    auto reopened_b = co_await std::move(reopen_b);
    BOOST_REQUIRE(reopened_b.has_value());
    auto current_b = std::move(*reopened_b);
    auto rename_once = environment.files->rename(
      path("/kwaque/b/file"), path("/kwaque/b/renamed"));
    co_await require_success(environment.events, rename_once);
    auto crash_before_sync = environment.files->crash();
    co_await require_success(environment.events, crash_before_sync);
    auto close_after_unsynced = current_b.close();
    co_await require_success(environment.events, close_after_unsynced);

    auto reopen_old = environment.files->open(
      path("/kwaque/b/file"),
      {.access = kwaque::runtime::file_access::read_write});
    co_await pump_until(environment.events, reopen_old);
    auto reopened_old = co_await std::move(reopen_old);
    BOOST_REQUIRE(reopened_old.has_value());
    auto renamed_b = std::move(*reopened_old);
    auto rename_durable = environment.files->rename(
      path("/kwaque/b/file"), path("/kwaque/b/renamed"));
    co_await require_success(environment.events, rename_durable);
    auto sync_rename = environment.files->sync_directory(path("/kwaque/b"));
    co_await require_success(environment.events, sync_rename);
    auto crash_after_sync = environment.files->crash();
    co_await require_success(environment.events, crash_after_sync);
    auto close_after_sync = renamed_b.close();
    co_await require_success(environment.events, close_after_sync);

    auto final_a = environment.files->open(
      path("/kwaque/a/file"),
      {.access = kwaque::runtime::file_access::read_only});
    co_await pump_until(environment.events, final_a);
    auto opened_final_a = co_await std::move(final_a);
    BOOST_REQUIRE(opened_final_a.has_value());
    auto read_a = opened_final_a->read(
      kwaque::runtime::file_position{0}, kwaque::byte_count{4'096});
    co_await pump_until(environment.events, read_a);
    auto bytes_a = co_await std::move(read_a);
    BOOST_REQUIRE(bytes_a.has_value());
    BOOST_CHECK(bytes_a->data().content_equals(std::string(4'096, 'a')));
    auto final_close_a = opened_final_a->close();
    co_await require_success(environment.events, final_close_a);

    auto final_b = environment.files->open(
      path("/kwaque/b/renamed"),
      {.access = kwaque::runtime::file_access::read_only});
    co_await pump_until(environment.events, final_b);
    auto opened_final_b = co_await std::move(final_b);
    BOOST_REQUIRE(opened_final_b.has_value());
    auto read_b = opened_final_b->read(
      kwaque::runtime::file_position{0}, kwaque::byte_count{4'096});
    co_await pump_until(environment.events, read_b);
    auto bytes_b = co_await std::move(read_b);
    BOOST_REQUIRE(bytes_b.has_value());
    std::string expected_b(4'096, 'b');
    std::fill_n(
      expected_b.begin(), static_cast<std::size_t>(torn_write_bytes), 't');
    BOOST_CHECK(bytes_b->data().content_equals(expected_b));
    auto final_close_b = opened_final_b->close();
    co_await require_success(environment.events, final_close_b);

    auto captured_state = fake_file_test_access::snapshot(*environment.files);
    BOOST_REQUIRE(captured_state.has_value());
    auto state = std::move(*captured_state);
    auto digest_value = fake_file_test_access::state_digest(*environment.files);
    BOOST_REQUIRE(digest_value.has_value());
    auto stopping = environment.files->stop();
    if (!stopping.available()) {
        co_await pump_until(environment.events, stopping);
    }
    co_await require_ready_success(stopping);
    co_return compound_result{
      .state = std::move(state),
      .digest = *digest_value,
      .terminal_errors = std::move(terminal_errors),
    };
}

struct crash_boundary_result final {
    fake_file_state_snapshot state;
    bool diverged{false};
};

seastar::future<crash_boundary_result> run_crash_boundary(
  event_trace& trace, const scheduler_limits& scheduler_budget) {
    seastar::chunked_vector<fault_rule> rules;
    rules.push_back(rule(
      201,
      builtin_fault_point::file_write,
      3,
      3,
      fault_decision::make_delay(kwaque::runtime::monotonic_duration{50})));
    fixture environment{scheduler_budget, trace, std::move(rules)};
    auto creating = environment.files->create_directories(path("/kwaque/data"));
    co_await require_success(environment.events, creating);
    auto sync_root = environment.files->sync_directory(path("/kwaque"));
    co_await require_success(environment.events, sync_root);
    auto opening = environment.files->open(
      path("/kwaque/data/file"),
      {.access = kwaque::runtime::file_access::read_write, .create = true});
    co_await pump_until(environment.events, opening);
    auto opened = co_await std::move(opening);
    BOOST_REQUIRE(opened.has_value());
    auto file = std::move(*opened);
    auto sync_directory = environment.files->sync_directory(
      path("/kwaque/data"));
    co_await require_success(environment.events, sync_directory);
    auto durable_write = file.write(
      kwaque::runtime::file_position{0}, payload('a'));
    co_await require_success(environment.events, durable_write);
    auto flushing = file.flush();
    co_await require_success(environment.events, flushing);
    auto volatile_write = file.write(
      kwaque::runtime::file_position{0}, payload('b'));
    co_await require_success(environment.events, volatile_write);

    const auto writes_before = fake_file_test_access::submitted(
      *environment.files, fake_submission_kind::write);
    auto delayed = file.write(kwaque::runtime::file_position{0}, payload('c'));
    co_await fake_file_test_access::wait_submitted(
      *environment.files, fake_submission_kind::write, writes_before + 1U);
    auto crashing = environment.files->crash();
    bool diverged = false;
    while (!crashing.available()) {
        if (!environment.events.has_ready_events()) {
            const auto advanced = environment.events.advance_to_next();
            BOOST_REQUIRE(advanced.has_value());
            BOOST_REQUIRE(advanced->has_value());
        }
        const auto ran = environment.events.run_ready();
        if (!ran) {
            BOOST_CHECK(ran.error().code() == kwaque::errc::replay_divergence);
            diverged = true;
            static_cast<void>(environment.events.discard_failed());
            break;
        }
        co_await seastar::yield();
    }

    if (!diverged) {
        co_await require_ready_success(crashing);
        const auto delayed_result = co_await std::move(delayed);
        BOOST_REQUIRE(!delayed_result.has_value());
        BOOST_CHECK(delayed_result.error().code() == kwaque::errc::aborted);
    } else {
        const auto crash_result = co_await std::move(crashing);
        BOOST_REQUIRE(!crash_result.has_value());
        BOOST_CHECK(
          crash_result.error().code() == kwaque::errc::replay_divergence);
        const auto delayed_result = co_await std::move(delayed);
        BOOST_REQUIRE(!delayed_result.has_value());
        // The private file_impl transports the poison through a native
        // std::error_code; runtime::file therefore exposes it as I/O failure.
        BOOST_CHECK(delayed_result.error().code() == kwaque::errc::io_failure);
    }

    auto stopping = environment.files->stop();
    if (!stopping.available()) {
        co_await pump_until(environment.events, stopping);
    }
    const auto stopped = co_await std::move(stopping);
    BOOST_CHECK(
      stopped.has_value()
      || stopped.error().code() == kwaque::errc::replay_divergence);
    auto captured = fake_file_test_access::snapshot(*environment.files);
    BOOST_REQUIRE(captured.has_value());
    auto state = std::move(*captured);
    auto closing = file.close();
    co_await require_ready_success(closing);
    co_return crash_boundary_result{
      .state = std::move(state),
      .diverged = diverged,
    };
}

bool regular_file_contains_only(
  const fake_file_state_snapshot& state, char expected) {
    for (const auto& object : state.objects) {
        if (object.kind != kwaque::simulation::fake_file_kind::regular) {
            continue;
        }
        return object.visible_bytes.size() == 4'096U
               && std::ranges::all_of(
                 object.visible_bytes, [expected](std::byte value) {
                     return value == static_cast<std::byte>(expected);
                 });
    }
    return false;
}

} // namespace

SEASTAR_TEST_CASE(fake_file_compound_capture_replays_byte_identically) {
    const auto scheduler_budget = make_scheduler_limits();
    const auto trace_budget = make_trace_limits();
    const auto header = make_header(scheduler_budget, trace_budget);
    event_trace capture{header, trace_budget};
    const auto captured = co_await run_compound(
      capture, scheduler_budget, reproduction());
    auto encoded = capture.encode();
    BOOST_REQUIRE(encoded.has_value());

    auto decoded = event_trace::decode(*encoded, trace_budget);
    BOOST_REQUIRE(decoded.has_value());
    auto replay = event_trace::replay(
      header, trace_budget, std::move(*decoded));
    BOOST_REQUIRE(replay.has_value());
    const auto replayed = co_await run_compound(
      **replay, scheduler_budget, reproduction());
    BOOST_REQUIRE((*replay)->finish_replay().has_value());
    auto replay_encoding = (*replay)->encode();
    BOOST_REQUIRE(replay_encoding.has_value());

    BOOST_CHECK(*replay_encoding == *encoded);
    BOOST_CHECK(replayed == captured);
    co_return;
}

SEASTAR_TEST_CASE(fake_file_partial_resize_capture_replays_exactly) {
    const auto scheduler_budget = make_scheduler_limits();
    const auto trace_budget = make_trace_limits();
    const auto header = make_header(scheduler_budget, trace_budget);
    event_trace capture{header, trace_budget};
    const auto captured = co_await run_partial_resize(
      capture, scheduler_budget);
    auto encoded = capture.encode();
    BOOST_REQUIRE(encoded.has_value());

    auto decoded = event_trace::decode(*encoded, trace_budget);
    BOOST_REQUIRE(decoded.has_value());
    auto replay = event_trace::replay(
      header, trace_budget, std::move(*decoded));
    BOOST_REQUIRE(replay.has_value());
    const auto replayed = co_await run_partial_resize(
      **replay, scheduler_budget);
    BOOST_REQUIRE((*replay)->finish_replay().has_value());
    auto replay_encoding = (*replay)->encode();
    BOOST_REQUIRE(replay_encoding.has_value());

    BOOST_CHECK(captured == replayed);
    BOOST_CHECK(*encoded == *replay_encoding);
    BOOST_CHECK(captured.terminal == kwaque::errc::io_failure);
    BOOST_CHECK(captured.selected_target > 10U);
    BOOST_CHECK(captured.selected_target < 4'096U);
    co_return;
}

SEASTAR_TEST_CASE(fake_file_partial_resize_replay_diverges_before_size_change) {
    const auto scheduler_budget = make_scheduler_limits();
    const auto trace_budget = make_trace_limits();
    const auto header = make_header(scheduler_budget, trace_budget);
    event_trace capture{header, trace_budget};
    static_cast<void>(co_await run_partial_resize(capture, scheduler_budget));
    auto encoded = capture.encode();
    BOOST_REQUIRE(encoded.has_value());
    auto decoded = event_trace::decode(*encoded, trace_budget);
    BOOST_REQUIRE(decoded.has_value());
    const auto boundary = std::ranges::find_if(
      decoded->entries, [](const auto& entry) {
          return entry.action == trace_action::partial_resize_applied;
      });
    BOOST_REQUIRE(boundary != decoded->entries.end());
    boundary->coordinate_a ^= UINT64_C(1);
    auto replay = event_trace::replay(
      header, trace_budget, std::move(*decoded));
    BOOST_REQUIRE(replay.has_value());

    seastar::chunked_vector<fault_rule> rules;
    rules.push_back(wildcard_rule(
      401,
      builtin_fault_point::file_truncate,
      1,
      fault_decision::make_partial_resize()));
    fixture environment{scheduler_budget, **replay, std::move(rules)};
    auto creating = environment.files->create_directories(path("/kwaque/data"));
    co_await require_success(environment.events, creating);
    auto opening = environment.files->open(
      path("/kwaque/data/file"),
      {.access = kwaque::runtime::file_access::read_write, .create = true});
    co_await pump_until(environment.events, opening);
    auto opened = co_await std::move(opening);
    BOOST_REQUIRE(opened.has_value());
    auto file = std::move(*opened);
    auto writing = file.write(kwaque::runtime::file_position{0}, payload('p'));
    co_await require_success(environment.events, writing);
    auto flushing = file.flush();
    co_await require_success(environment.events, flushing);

    const auto file_path = fake_file_test_access::resolve(
      *environment.files, "/kwaque/data/file");
    BOOST_REQUIRE(file_path.has_value());
    BOOST_CHECK(
      *fake_file_test_access::visible_size(*environment.files, *file_path)
      == 4'096U);
    auto truncating = file.truncate(10);
    BOOST_REQUIRE(!truncating.available());
    if (!environment.events.has_ready_events()) {
        const auto advanced = environment.events.advance_to_next();
        BOOST_REQUIRE(advanced.has_value());
        BOOST_REQUIRE(advanced->has_value());
    }
    const auto ran = environment.events.run_ready();
    BOOST_REQUIRE(!ran.has_value());
    BOOST_CHECK(ran.error().code() == kwaque::errc::replay_divergence);
    BOOST_REQUIRE(environment.events.discard_failed());
    const auto truncated = co_await std::move(truncating);
    BOOST_REQUIRE(!truncated.has_value());
    BOOST_CHECK(
      *fake_file_test_access::visible_size(*environment.files, *file_path)
      == 4'096U);
    BOOST_CHECK(
      *fake_file_test_access::durable_size(*environment.files, *file_path)
      == 4'096U);
    BOOST_CHECK(environment.files->pending_operations() == 0U);
    BOOST_CHECK(environment.files->pending_bytes().value() == 0U);

    auto stopping = environment.files->stop();
    const auto stopped = co_await std::move(stopping);
    BOOST_REQUIRE(!stopped.has_value());
    BOOST_CHECK(stopped.error().code() == kwaque::errc::replay_divergence);
    BOOST_REQUIRE((co_await file.close()).has_value());
    co_return;
}

SEASTAR_TEST_CASE(fake_file_compound_trace_contains_every_replay_boundary) {
    const auto scheduler_budget = make_scheduler_limits();
    const auto trace_budget = make_trace_limits();
    event_trace capture{
      make_header(scheduler_budget, trace_budget), trace_budget};
    static_cast<void>(
      co_await run_compound(capture, scheduler_budget, reproduction()));

    std::size_t fault_entries = 0;
    std::size_t crash_entries = 0;
    std::size_t directory_sync_events = 0;
    for (const auto& entry : capture.entries()) {
        fault_entries += entry.action == trace_action::fault_evaluated;
        crash_entries += entry.action == trace_action::crash_applied;
        directory_sync_events
          += entry.kind == kwaque::simulation::trace_event_kind::filesystem
             && entry.domain
                  == kwaque::runtime::descriptor_for(
                       builtin_fault_point::directory_sync)
                       ->id.value()
             && entry.action == trace_action::selected;
    }
    BOOST_CHECK(fault_entries == 4U);
    BOOST_CHECK(crash_entries == 3U);
    BOOST_CHECK(directory_sync_events == 4U);
    co_return;
}

SEASTAR_TEST_CASE(
  fake_file_compound_replay_rejects_every_boundary_mutation_first) {
    enum class mutation_kind : std::uint8_t {
        missing,
        extra,
        reordered,
        field,
    };
    constexpr std::array mutations{
      mutation_kind::missing,
      mutation_kind::extra,
      mutation_kind::reordered,
      mutation_kind::field,
    };

    const auto scheduler_budget = make_scheduler_limits();
    const auto trace_budget = make_trace_limits();
    const auto header = make_header(scheduler_budget, trace_budget);
    event_trace capture{header, trace_budget};
    static_cast<void>(
      co_await run_compound(capture, scheduler_budget, reproduction()));
    auto encoded = capture.encode();
    BOOST_REQUIRE(encoded.has_value());

    std::vector<std::size_t> boundaries;
    for (std::size_t index = 0; index < capture.entries().size(); ++index) {
        const auto& entry = capture.entries()[index];
        if (
          entry.action == trace_action::fault_evaluated
          || entry.action == trace_action::crash_applied
          || (entry.action == trace_action::selected
              && entry.kind
                   == kwaque::simulation::trace_event_kind::filesystem
              && entry.domain
                   == kwaque::runtime::descriptor_for(
                        builtin_fault_point::directory_sync)
                        ->id.value())) {
            boundaries.push_back(index);
        }
    }
    BOOST_REQUIRE(boundaries.size() == 11U);

    for (const auto boundary : boundaries) {
        BOOST_REQUIRE(boundary + 1U < capture.entries().size());
        for (const auto mutation : mutations) {
            auto decoded = event_trace::decode(*encoded, trace_budget);
            BOOST_REQUIRE(decoded.has_value());
            seastar::chunked_vector<kwaque::simulation::trace_entry> changed;
            changed.reserve(
              decoded->entries.size()
              + static_cast<std::size_t>(mutation == mutation_kind::extra)
              - static_cast<std::size_t>(mutation == mutation_kind::missing));
            for (std::size_t index = 0; index < decoded->entries.size();
                 ++index) {
                if (index == boundary && mutation == mutation_kind::missing) {
                    continue;
                }
                if (index == boundary && mutation == mutation_kind::extra) {
                    auto inserted = decoded->entries[index];
                    inserted.stable_id ^= UINT64_C(1);
                    changed.push_back(inserted);
                }
                if (index == boundary && mutation == mutation_kind::reordered) {
                    changed.push_back(decoded->entries[index + 1U]);
                    changed.push_back(decoded->entries[index]);
                    ++index;
                    continue;
                }
                auto entry = decoded->entries[index];
                if (index == boundary && mutation == mutation_kind::field) {
                    entry.stable_id ^= UINT64_C(1);
                }
                changed.push_back(entry);
            }
            for (std::size_t index = 0; index < changed.size(); ++index) {
                changed[index].sequence = index + 1U;
            }
            decoded->encoded_bytes
              = kwaque::simulation::canonical_header_encoded_size
                + changed.size()
                    * kwaque::simulation::canonical_entry_encoded_size;
            decoded->entries = std::move(changed);
            auto replay = event_trace::replay(
              header, trace_budget, std::move(*decoded));
            BOOST_REQUIRE(replay.has_value());

            for (std::size_t index = 0; index <= boundary; ++index) {
                const auto observed = (*replay)->observe(
                  capture.entries()[index]);
                if (index != boundary) {
                    BOOST_REQUIRE(observed.has_value());
                    continue;
                }
                BOOST_REQUIRE(!observed.has_value());
                BOOST_CHECK(
                  observed.error().code() == kwaque::errc::replay_divergence);
                BOOST_REQUIRE(observed.error().context_at(0).has_value());
                BOOST_CHECK(
                  observed.error().context_at(0)->value == boundary + 1U);
            }
            co_await seastar::maybe_yield();
        }
    }
    co_return;
}

SEASTAR_TEST_CASE(
  fake_file_replay_divergence_during_crash_leaves_disk_untouched_and_drained) {
    const auto scheduler_budget = make_scheduler_limits();
    const auto trace_budget = make_trace_limits();
    const auto header = make_header(scheduler_budget, trace_budget);
    event_trace capture{header, trace_budget};
    const auto captured = co_await run_crash_boundary(
      capture, scheduler_budget);
    BOOST_CHECK(!captured.diverged);
    BOOST_CHECK(regular_file_contains_only(captured.state, 'a'));
    auto encoded = capture.encode();
    BOOST_REQUIRE(encoded.has_value());

    std::vector<std::size_t> boundaries;
    for (std::size_t index = 0; index < capture.entries().size(); ++index) {
        const auto action = capture.entries()[index].action;
        if (
          action == trace_action::canceled
          || action == trace_action::crash_applied) {
            boundaries.push_back(index);
        }
    }
    BOOST_REQUIRE(boundaries.size() == 2U);
    for (const auto boundary : boundaries) {
        auto decoded = event_trace::decode(*encoded, trace_budget);
        BOOST_REQUIRE(decoded.has_value());
        decoded->entries[boundary].stable_id ^= UINT64_C(1);
        auto replay = event_trace::replay(
          header, trace_budget, std::move(*decoded));
        BOOST_REQUIRE(replay.has_value());
        const auto result = co_await run_crash_boundary(
          **replay, scheduler_budget);
        BOOST_CHECK(result.diverged);
        BOOST_CHECK(regular_file_contains_only(result.state, 'b'));
        BOOST_CHECK(result.state.pending_operations == 0U);
        BOOST_CHECK(result.state.pending_bytes == 0U);
        BOOST_CHECK(result.state.open_handles == 0U);
    }
    co_return;
}

SEASTAR_TEST_CASE(
  fake_file_fault_replay_diverges_before_submission_or_state_change) {
    const auto scheduler_budget = make_scheduler_limits();
    const auto trace_budget = make_trace_limits();
    const auto header = make_header(scheduler_budget, trace_budget);
    event_trace capture{header, trace_budget};
    {
        seastar::chunked_vector<fault_rule> rules;
        rules.push_back(wildcard_rule(
          301,
          builtin_fault_point::file_exists,
          1,
          fault_decision::make_error()));
        fixture environment{scheduler_budget, capture, std::move(rules)};
        auto checking = environment.files->exists(path("/kwaque/missing"));
        co_await pump_until(environment.events, checking);
        const auto result = co_await std::move(checking);
        BOOST_REQUIRE(!result.has_value());
        BOOST_CHECK(result.error().code() == kwaque::errc::fault_injected);
        auto stopping = environment.files->stop();
        co_await require_ready_success(stopping);
    }
    auto encoded = capture.encode();
    BOOST_REQUIRE(encoded.has_value());
    auto decoded = event_trace::decode(*encoded, trace_budget);
    BOOST_REQUIRE(decoded.has_value());
    const auto boundary = std::ranges::find_if(
      decoded->entries, [](const auto& entry) {
          return entry.action == trace_action::fault_evaluated;
      });
    BOOST_REQUIRE(boundary != decoded->entries.end());
    boundary->stable_id ^= UINT64_C(1);
    auto replay = event_trace::replay(
      header, trace_budget, std::move(*decoded));
    BOOST_REQUIRE(replay.has_value());

    seastar::chunked_vector<fault_rule> replay_rules;
    replay_rules.push_back(wildcard_rule(
      301, builtin_fault_point::file_exists, 1, fault_decision::make_error()));
    fixture replay_environment{
      scheduler_budget, **replay, std::move(replay_rules)};
    const auto before = fake_file_test_access::snapshot(
      *replay_environment.files);
    BOOST_REQUIRE(before.has_value());
    auto checking = replay_environment.files->exists(path("/kwaque/missing"));
    co_await pump_until(replay_environment.events, checking);
    const auto divergence = co_await std::move(checking);
    BOOST_REQUIRE(!divergence.has_value());
    BOOST_CHECK(divergence.error().code() == kwaque::errc::replay_divergence);
    const auto after = fake_file_test_access::snapshot(
      *replay_environment.files);
    BOOST_REQUIRE(after.has_value());
    const auto before_root = std::ranges::find(
      before->objects,
      UINT64_C(1),
      &kwaque::simulation::fake_inode_snapshot::id);
    const auto after_root = std::ranges::find(
      after->objects,
      UINT64_C(1),
      &kwaque::simulation::fake_inode_snapshot::id);
    BOOST_REQUIRE(before_root != before->objects.end());
    BOOST_REQUIRE(after_root != after->objects.end());
    BOOST_CHECK(before_root->visible_entries == after_root->visible_entries);
    BOOST_CHECK(before_root->durable_entries == after_root->durable_entries);
    BOOST_CHECK(before->objects.size() == after->objects.size());
    BOOST_CHECK(before->retained_capacity == after->retained_capacity);
    BOOST_CHECK(replay_environment.files->pending_operations() == 0U);
    auto stopping = replay_environment.files->stop();
    const auto stopped = co_await std::move(stopping);
    BOOST_REQUIRE(!stopped.has_value());
    BOOST_CHECK(stopped.error().code() == kwaque::errc::replay_divergence);
    co_return;
}

SEASTAR_TEST_CASE(
  fake_directory_sync_replay_diverges_before_durable_membership_change) {
    const auto scheduler_budget = make_scheduler_limits();
    const auto trace_budget = make_trace_limits();
    const auto header = make_header(scheduler_budget, trace_budget);
    event_trace capture{header, trace_budget};
    {
        fixture environment{scheduler_budget, capture, {}};
        auto creating = environment.files->create_directories(
          path("/kwaque/data"));
        co_await require_success(environment.events, creating);
        auto syncing = environment.files->sync_directory(path("/kwaque"));
        co_await require_success(environment.events, syncing);
        auto stopping = environment.files->stop();
        co_await require_ready_success(stopping);
    }
    auto encoded = capture.encode();
    BOOST_REQUIRE(encoded.has_value());
    auto decoded = event_trace::decode(*encoded, trace_budget);
    BOOST_REQUIRE(decoded.has_value());
    const auto boundary = std::ranges::find_if(
      decoded->entries, [](const auto& entry) {
          return entry.action == trace_action::selected
                 && entry.domain
                      == kwaque::runtime::descriptor_for(
                           builtin_fault_point::directory_sync)
                           ->id.value();
      });
    BOOST_REQUIRE(boundary != decoded->entries.end());
    boundary->stable_id ^= UINT64_C(1);
    auto replay = event_trace::replay(
      header, trace_budget, std::move(*decoded));
    BOOST_REQUIRE(replay.has_value());

    fixture replay_environment{scheduler_budget, **replay, {}};
    auto creating = replay_environment.files->create_directories(
      path("/kwaque/data"));
    co_await require_success(replay_environment.events, creating);
    const auto before = fake_file_test_access::snapshot(
      *replay_environment.files);
    BOOST_REQUIRE(before.has_value());
    auto syncing = replay_environment.files->sync_directory(path("/kwaque"));
    BOOST_REQUIRE(!syncing.available());
    const auto advanced = replay_environment.events.advance_to_next();
    BOOST_REQUIRE(advanced.has_value());
    BOOST_REQUIRE(advanced->has_value());
    const auto ran = replay_environment.events.run_ready();
    BOOST_REQUIRE(!ran.has_value());
    BOOST_CHECK(ran.error().code() == kwaque::errc::replay_divergence);
    static_cast<void>(replay_environment.events.discard_failed());
    // discard_failed() resolves the source promise; the result-mapping
    // continuation becomes visible when this derived future is awaited.
    const auto divergence = co_await std::move(syncing);
    BOOST_REQUIRE(!divergence.has_value());
    BOOST_CHECK(divergence.error().code() == kwaque::errc::replay_divergence);
    const auto after = fake_file_test_access::snapshot(
      *replay_environment.files);
    BOOST_REQUIRE(after.has_value());
    const auto before_root = std::ranges::find(
      before->objects,
      UINT64_C(1),
      &kwaque::simulation::fake_inode_snapshot::id);
    const auto after_root = std::ranges::find(
      after->objects,
      UINT64_C(1),
      &kwaque::simulation::fake_inode_snapshot::id);
    BOOST_REQUIRE(before_root != before->objects.end());
    BOOST_REQUIRE(after_root != after->objects.end());
    BOOST_CHECK(before_root->visible_entries == after_root->visible_entries);
    BOOST_CHECK(before_root->durable_entries == after_root->durable_entries);
    BOOST_CHECK(before->objects.size() == after->objects.size());
    BOOST_CHECK(before->retained_capacity == after->retained_capacity);
    auto stopping = replay_environment.files->stop();
    const auto stopped = co_await std::move(stopping);
    BOOST_REQUIRE(!stopped.has_value());
    BOOST_CHECK(stopped.error().code() == kwaque::errc::replay_divergence);
    co_return;
}
