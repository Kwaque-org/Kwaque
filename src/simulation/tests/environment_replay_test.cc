#include "src/base/units.h"
#include "src/observability/event.h"
#include "src/observability/event_codec.h"
#include "src/observability/event_log.h"
#include "src/runtime/dns.h"
#include "src/runtime/fault.h"
#include "src/runtime/file.h"
#include "src/runtime/network.h"
#include "src/runtime/testing/contracts/environment_contract.h"
#include "src/simulation/environment.h"
#include "src/simulation/environment_test_support.h"
#include "src/simulation/event_trace.h"
#include "src/simulation/fake_dns.h"
#include "src/simulation/fake_file_test_support.h"
#include "src/simulation/scheduler_driver.h"
#include "src/simulation/sha256.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <stdexcept>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>

namespace {

using kwaque::observability::event;
using kwaque::observability::event_log;
using kwaque::runtime::testing::environment_contract_observation;
using kwaque::simulation::decoded_event_trace;
using kwaque::simulation::environment;
using kwaque::simulation::environment_config;
using kwaque::simulation::environment_config_values;
using kwaque::simulation::environment_test_access;
using kwaque::simulation::event_trace;
using kwaque::simulation::sha256_digest;
using kwaque::simulation::sha256_hasher;
using kwaque::simulation::trace_artifact;
using kwaque::simulation::trace_digest;
using kwaque::simulation::testing::scheduler_driver;

constexpr std::string_view canonical_configuration{
  "environment-fixture-v1;seed=71;stream=19;epoch=23;events=32;"
  "trace=2048;scheduler=256,64,2000;file-root=/kwaque;network=loopback;"
  "dns-records=16;resource-memory=134217728"};
constexpr std::string_view canonical_input{
  "environment-input-v1;root=/kwaque/environment-contract;listen=127.0.0.1:0;"
  "dns=environment.test:33145/ipv4;memory=4096"};
constexpr std::string_view changed_configuration{"environment-fixture-v2"};
constexpr std::string_view changed_input{"environment-input-v2"};
constexpr std::uint64_t fixture_epoch{23};
constexpr std::uint64_t fixture_seed{71};
constexpr std::uint64_t fixture_stream{19};
constexpr std::uint64_t fixture_random_word{UINT64_C(0xea975e34f614487d)};

trace_digest digest(std::string_view value) {
    sha256_hasher hasher;
    hasher.update(value.data(), value.size());
    const auto hashed = std::move(hasher).final();
    trace_digest result{};
    std::copy(hashed.begin(), hashed.end(), result.begin());
    return result;
}

struct fixture_identity final {
    std::string_view configuration{canonical_configuration};
    std::string_view input{canonical_input};
    std::uint64_t epoch{fixture_epoch};
    std::uint64_t seed{fixture_seed};
};

environment_config config(fixture_identity identity = {}) {
    environment_config_values values;
    values.master_seed = identity.seed;
    values.runtime_stream_stable_id = fixture_stream;
    values.event_epoch = identity.epoch;
    values.configuration_digest = digest(identity.configuration);
    values.input_digest = digest(identity.input);
    values.scheduler.pending_events = 256;
    values.scheduler.events_per_pump = 64;
    values.scheduler.total_events = 2'000;
    values.trace.entries = 2'048;
    values.trace.encoded_bytes = 512U * 1'024U;
    values.event_log.entries = 32;
    values.event_log.encoded_bytes = 32U * 1'024U;
    values.maximum_fault_rules = 16;
    values.file.virtual_root = "/kwaque";
    values.file.maximum_objects = 64;
    values.file.maximum_open_handles = 8;
    values.file.maximum_pending_operations = 8;
    values.file.maximum_pending_reads = 8;
    values.file.maximum_pending_writes = 8;
    values.network.maximum_listeners = 2;
    values.network.maximum_connection_pairs = 2;
    values.network.maximum_pending_connects = 2;
    values.network.maximum_backlog_entries = 4;
    values.network.maximum_operations = 8;
    values.network.maximum_parked_operations = 4;
    values.network.maximum_packets = 16;
    values.network.maximum_direction_packets = 8;
    values.network.maximum_links = 8;
    values.network.maximum_address_entries = 8;
    values.network.maximum_active_flows = 4;
    values.network.maximum_controls = 8;
    values.network.stop_batch = 8;
    values.dns.maximum_records = 16;
    values.dns.maximum_answers = 32;
    values.dns.maximum_name_bytes = kwaque::byte_count{8U * 1'024U};
    values.dns.stop_batch = 8;
    values.dns.query_limits.maximum_waiters = 8;
    auto made = environment_config::make(std::move(values));
    if (!made) {
        throw std::system_error(make_error_code(made.error().code()));
    }
    return std::move(*made);
}

kwaque::runtime::dns_query dns_query() {
    auto name = kwaque::runtime::dns_name::make("environment.test");
    if (!name) {
        throw std::system_error(make_error_code(name.error().code()));
    }
    return kwaque::runtime::dns_query{
      .host = std::move(*name),
      .port = 33'145,
      .family = kwaque::runtime::dns_address_family::ipv4,
    };
}

constexpr auto loopback = kwaque::runtime::network_address::ipv4(
  {std::byte{127}, std::byte{0}, std::byte{0}, std::byte{1}});
constexpr auto dns_address = kwaque::runtime::network_address::ipv4(
  {std::byte{127}, std::byte{0}, std::byte{0}, std::byte{42}});

kwaque::runtime::dns_answer dns_answer() {
    return kwaque::runtime::dns_answer{
      .endpoint = kwaque::runtime::network_endpoint{dns_address, 33'145},
      .ttl = kwaque::runtime::monotonic_duration{7'000'000'000},
    };
}

kwaque::runtime::file_path root_path() {
    auto made = kwaque::runtime::file_path::make(
      "/kwaque/environment-contract");
    if (!made) {
        throw std::system_error(make_error_code(made.error().code()));
    }
    return std::move(*made);
}

kwaque::runtime::testing::environment_component_input input() {
    return kwaque::runtime::testing::environment_component_input{
      .root_path = root_path(),
      .listen_endpoint = kwaque::runtime::network_endpoint{loopback, 0},
      .dns = dns_query(),
      .memory = kwaque::byte_count{4'096},
    };
}

kwaque::runtime::testing::environment_contract_expectation expectation() {
    return kwaque::runtime::testing::environment_contract_expectation{
      .dns_answers = {dns_answer()},
      .random_word = fixture_random_word,
    };
}

void add_dns_record(environment& target) {
    auto added = environment_test_access::dns_owner(target).add_record(
      kwaque::simulation::fake_dns_record{
        .key = dns_query(),
        .answers = {dns_answer()},
        .latency = kwaque::runtime::monotonic_duration{3},
      });
    if (!added) {
        throw std::system_error(make_error_code(added.error().code()));
    }
}

template<typename Integer>
void append_integer(sha256_hasher& hasher, Integer value) {
    using unsigned_type = std::make_unsigned_t<Integer>;
    auto encoded = static_cast<std::uint64_t>(
      static_cast<unsigned_type>(value));
    std::array<std::uint8_t, sizeof(Integer)> bytes{};
    for (std::size_t index = 0; index < bytes.size(); ++index) {
        bytes[index] = static_cast<std::uint8_t>(encoded & 0xffU);
        encoded >>= 8U;
    }
    hasher.update(bytes.data(), bytes.size());
}

sha256_digest terminal_digest(environment& target) {
    const auto file = kwaque::simulation::fake_file_test_access::state_digest(
      environment_test_access::file_system_owner(target));
    if (!file) {
        throw std::system_error(make_error_code(file.error().code()));
    }
    const auto network
      = environment_test_access::network_owner(target).allocation_digest();
    const auto queue_occurrences = target.failure_probe().occurrences(
      kwaque::runtime::builtin_fault_point::queue_admission);
    if (!queue_occurrences) {
        throw std::system_error(
          make_error_code(queue_occurrences.error().code()));
    }

    sha256_hasher hasher;
    for (const auto word : *file) {
        append_integer(hasher, word);
    }
    for (const auto word : network.words) {
        append_integer(hasher, word);
    }
    append_integer(hasher, static_cast<std::uint8_t>(target.state()));
    append_integer(hasher, target.event_scheduler().now().nanoseconds());
    append_integer(hasher, target.event_scheduler().executed_events());
    append_integer(hasher, target.event_scheduler().pending_events());
    append_integer(hasher, target.time().offset().nanoseconds());
    append_integer(hasher, target.time().pending_adjustments());
    auto& random = environment_test_access::random_owner(target);
    auto& dns = environment_test_access::dns_owner(target);
    append_integer(hasher, random.occurrence());
    append_integer(hasher, random.draw_index());
    append_integer(hasher, static_cast<std::uint8_t>(random.exhausted()));
    append_integer(hasher, dns.record_count());
    append_integer(hasher, dns.answer_count());
    append_integer(hasher, dns.retained_name_bytes().value());
    append_integer(hasher, dns.pending_queries());
    append_integer(hasher, dns.waiting_queries());
    append_integer(hasher, static_cast<std::uint8_t>(dns.active()));
    append_integer(hasher, static_cast<std::uint8_t>(dns.state()));
    append_integer(hasher, target.event_sink().last_sequence());
    append_integer(hasher, target.event_sink().events().entries().size());
    append_integer(hasher, *queue_occurrences);
    return std::move(hasher).final();
}

struct reproduction_result final {
    trace_artifact trace;
    kwaque::observability::event_log_artifact events;
    environment_contract_observation observation;
    sha256_digest terminal;
};

seastar::future<reproduction_result> capture() {
    auto made = environment::make(config());
    if (!made) {
        throw std::system_error(make_error_code(made.error().code()));
    }
    auto target = std::move(*made);
    add_dns_record(*target);
    auto observation
      = co_await kwaque::runtime::testing::run_environment_contract(
        *target,
        input(),
        expectation(),
        scheduler_driver{target->event_scheduler()});
    const auto finished = target->finish_replay();
    if (!finished) {
        throw std::system_error(make_error_code(finished.error().code()));
    }
    auto terminal = terminal_digest(*target);
    auto encoded_trace = co_await target->trace().encode_cooperatively(64);
    if (!encoded_trace) {
        throw std::system_error(make_error_code(encoded_trace.error().code()));
    }
    auto encoded_events
      = co_await target->event_sink().events().encode_cooperatively(64);
    if (!encoded_events) {
        throw std::system_error(make_error_code(encoded_events.error().code()));
    }
    co_return reproduction_result{
      .trace = std::move(*encoded_trace),
      .events = std::move(*encoded_events),
      .observation = std::move(observation),
      .terminal = terminal,
    };
}

std::unique_ptr<event_log>
decode_events(const kwaque::observability::event_log_artifact& encoded) {
    auto limits = config().event_budget();
    auto decoded = event_log::decode(encoded, limits);
    if (!decoded) {
        throw std::system_error(make_error_code(decoded.error().code()));
    }
    return std::move(*decoded);
}

decoded_event_trace decode_trace(const trace_artifact& encoded) {
    auto limits = config().trace_budget();
    auto decoded = event_trace::decode(encoded, limits);
    if (!decoded) {
        throw std::system_error(make_error_code(decoded.error().code()));
    }
    return std::move(*decoded);
}

seastar::future<reproduction_result>
replay(const reproduction_result& captured) {
    auto made = environment::replay(
      config(), decode_trace(captured.trace), decode_events(captured.events));
    if (!made) {
        throw std::system_error(make_error_code(made.error().code()));
    }
    auto target = std::move(*made);
    add_dns_record(*target);
    auto observation
      = co_await kwaque::runtime::testing::run_environment_contract(
        *target,
        input(),
        expectation(),
        scheduler_driver{target->event_scheduler()});
    const auto finished = target->finish_replay();
    if (!finished) {
        throw std::system_error(make_error_code(finished.error().code()));
    }
    auto terminal = terminal_digest(*target);
    auto encoded_trace = co_await target->trace().encode_cooperatively(64);
    if (!encoded_trace) {
        throw std::system_error(make_error_code(encoded_trace.error().code()));
    }
    auto encoded_events
      = co_await target->event_sink().events().encode_cooperatively(64);
    if (!encoded_events) {
        throw std::system_error(make_error_code(encoded_events.error().code()));
    }
    co_return reproduction_result{
      .trace = std::move(*encoded_trace),
      .events = std::move(*encoded_events),
      .observation = std::move(observation),
      .terminal = terminal,
    };
}

event rewrite_event(
  const event& source, std::uint64_t sequence, bool change_monotonic = false) {
    const auto encoded = kwaque::observability::encode_event(source);
    if (!encoded) {
        throw std::system_error(make_error_code(encoded.error().code()));
    }
    std::vector<std::uint8_t> bytes(
      encoded->bytes().begin(), encoded->bytes().end());
    const auto name_bytes = static_cast<std::size_t>(bytes[6]);
    if (change_monotonic) {
        bytes[8U + name_bytes] ^= 1U;
    }
    const auto sequence_offset = 29U + name_bytes;
    for (std::size_t index = 0; index < sizeof(sequence); ++index) {
        bytes[sequence_offset + index] = static_cast<std::uint8_t>(
          sequence >> (index * 8U));
    }
    auto decoded = kwaque::observability::decode_event(
      std::span<const std::uint8_t>{bytes});
    if (!decoded) {
        throw std::system_error(make_error_code(decoded.error().code()));
    }
    return std::move(*decoded);
}

enum class event_mutation : std::uint8_t {
    queue_value,
    queue_reordered,
    missing_terminal,
    extra_terminal,
};

std::unique_ptr<event_log>
mutate_events(const event_log& source, event_mutation mutation) {
    std::vector<event> selected;
    selected.reserve(
      source.entries().size()
      + static_cast<std::size_t>(mutation == event_mutation::extra_terminal));
    for (const auto& value : source.entries()) {
        selected.push_back(value);
    }
    switch (mutation) {
    case event_mutation::queue_value:
        selected[2] = rewrite_event(selected[2], 3, true);
        break;
    case event_mutation::queue_reordered:
        std::swap(selected[2], selected[3]);
        break;
    case event_mutation::missing_terminal:
        selected.pop_back();
        break;
    case event_mutation::extra_terminal:
        selected.push_back(selected.back());
        break;
    }

    auto result = std::make_unique<event_log>(
      source.identity(), source.limits());
    for (std::size_t index = 0; index < selected.size(); ++index) {
        auto value = rewrite_event(selected[index], index + 1U);
        const auto appended = result->append(value);
        if (!appended) {
            throw std::system_error(make_error_code(appended.error().code()));
        }
    }
    return result;
}

std::unique_ptr<environment> make_replay_environment(
  std::unique_ptr<event_log> expected_events,
  decoded_event_trace expected_trace) {
    auto made = environment::replay(
      config(), std::move(expected_trace), std::move(expected_events));
    if (!made) {
        throw std::system_error(make_error_code(made.error().code()));
    }
    add_dns_record(**made);
    return std::move(*made);
}

} // namespace

SEASTAR_TEST_CASE(environment_capture_replays_both_artifacts_byte_identically) {
    const auto captured = co_await capture();
    const auto replayed = co_await replay(captured);

    BOOST_CHECK(replayed.trace == captured.trace);
    BOOST_CHECK(replayed.events == captured.events);
    BOOST_CHECK(replayed.observation == captured.observation);
    BOOST_CHECK(replayed.terminal == captured.terminal);
}

SEASTAR_TEST_CASE(environment_replay_rejects_header_and_identity_mismatch) {
    const auto captured = co_await capture();
    const std::array identities{
      fixture_identity{.configuration = changed_configuration},
      fixture_identity{.input = changed_input},
      fixture_identity{.seed = fixture_seed + 1U},
      fixture_identity{.epoch = fixture_epoch + 1U},
    };
    for (const auto identity : identities) {
        auto replayed = environment::replay(
          config(identity),
          decode_trace(captured.trace),
          decode_events(captured.events));
        BOOST_REQUIRE(!replayed.has_value());
        BOOST_CHECK(replayed.error().code() == kwaque::errc::replay_divergence);
    }
}

SEASTAR_TEST_CASE(environment_scheduler_replay_diverges_before_file_effect) {
    const auto captured = co_await capture();
    auto expected_trace = decode_trace(captured.trace);
    const auto boundary = std::ranges::find_if(
      expected_trace.entries, [](const auto& entry) {
          return entry.kind == kwaque::simulation::trace_event_kind::filesystem
                 && entry.action == kwaque::simulation::trace_action::scheduled;
      });
    BOOST_REQUIRE(boundary != expected_trace.entries.end());
    ++boundary->stable_id;
    auto target = make_replay_environment(
      decode_events(captured.events), std::move(expected_trace));

    bool diverged = false;
    try {
        static_cast<void>(
          co_await kwaque::runtime::testing::run_environment_contract(
            *target,
            input(),
            expectation(),
            scheduler_driver{target->event_scheduler()}));
    } catch (const std::runtime_error&) {
        diverged = true;
    }
    BOOST_REQUIRE(diverged);
    BOOST_REQUIRE(target->trace().failure() != nullptr);
    BOOST_CHECK(
      target->trace().failure()->code() == kwaque::errc::replay_divergence);
    const auto state = kwaque::simulation::fake_file_test_access::snapshot(
      environment_test_access::file_system_owner(*target));
    BOOST_REQUIRE(state.has_value());
    BOOST_CHECK_EQUAL(state->objects.size(), 1U);
    BOOST_CHECK_EQUAL(state->pending_operations, 0U);
    BOOST_CHECK_EQUAL(state->pending_bytes, 0U);
    BOOST_CHECK_EQUAL(target->event_scheduler().pending_events(), 0U);
}

SEASTAR_TEST_CASE(environment_event_replay_rejects_before_following_effect) {
    const auto captured = co_await capture();
    const std::array mutations{
      event_mutation::queue_value,
      event_mutation::queue_reordered,
    };
    for (const auto mutation : mutations) {
        auto target = make_replay_environment(
          mutate_events(*decode_events(captured.events), mutation),
          decode_trace(captured.trace));
        bool diverged = false;
        try {
            static_cast<void>(
              co_await kwaque::runtime::testing::run_environment_contract(
                *target,
                input(),
                expectation(),
                scheduler_driver{target->event_scheduler()}));
        } catch (const std::runtime_error&) {
            diverged = true;
        }
        BOOST_REQUIRE(diverged);
        BOOST_REQUIRE(target->event_sink().replay_failure() != nullptr);
        BOOST_CHECK(
          target->event_sink().replay_failure()->code()
          == kwaque::errc::replay_divergence);
        BOOST_CHECK_EQUAL(target->event_sink().events().entries().size(), 2U);
        const auto state = kwaque::simulation::fake_file_test_access::snapshot(
          environment_test_access::file_system_owner(*target));
        BOOST_REQUIRE(state.has_value());
        BOOST_CHECK_EQUAL(state->objects.size(), 1U);
        BOOST_CHECK_EQUAL(state->next_operation_id, 1U);
        BOOST_CHECK_EQUAL(
          environment_test_access::random_owner(*target).draw_index(), 0U);
        BOOST_CHECK_EQUAL(
          environment_test_access::network_owner(*target).active_operations(),
          0U);
        BOOST_CHECK_EQUAL(
          environment_test_access::dns_owner(*target).pending_queries(), 0U);
    }
}

SEASTAR_TEST_CASE(environment_event_replay_detects_missing_and_extra_events) {
    const auto captured = co_await capture();
    {
        auto target = make_replay_environment(
          mutate_events(
            *decode_events(captured.events), event_mutation::missing_terminal),
          decode_trace(captured.trace));
        bool diverged = false;
        try {
            static_cast<void>(
              co_await kwaque::runtime::testing::run_environment_contract(
                *target,
                input(),
                expectation(),
                scheduler_driver{target->event_scheduler()}));
        } catch (const std::runtime_error&) {
            diverged = true;
        }
        BOOST_REQUIRE(diverged);
        BOOST_REQUIRE(target->event_sink().replay_failure() != nullptr);
        BOOST_CHECK_EQUAL(target->event_sink().events().entries().size(), 4U);
    }
    {
        auto target = make_replay_environment(
          mutate_events(
            *decode_events(captured.events), event_mutation::extra_terminal),
          decode_trace(captured.trace));
        static_cast<void>(
          co_await kwaque::runtime::testing::run_environment_contract(
            *target,
            input(),
            expectation(),
            scheduler_driver{target->event_scheduler()}));
        const auto finished = target->finish_replay();
        BOOST_REQUIRE(!finished.has_value());
        BOOST_CHECK(finished.error().code() == kwaque::errc::replay_divergence);
        BOOST_REQUIRE(target->event_sink().replay_failure() != nullptr);
        BOOST_CHECK_EQUAL(target->event_sink().events().entries().size(), 5U);
    }
}
