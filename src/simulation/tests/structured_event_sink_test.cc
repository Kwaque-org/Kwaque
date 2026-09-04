#include "src/observability/event.h"
#include "src/observability/event_identity.h"
#include "src/observability/event_log.h"
#include "src/simulation/event_sink.h"

#include <seastar/core/coroutine.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/alloc_failure_injector.hh>
#include <seastar/util/later.hh>

#include <boost/test/unit_test.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

namespace {

kwaque::observability::event_text
text(kwaque::observability::event_public_text value) {
    auto made = kwaque::observability::event_text::make(value);
    BOOST_REQUIRE(made.has_value());
    return *made;
}

kwaque::observability::event_request make_event(std::uint64_t monotonic = 17) {
    using kwaque::observability::event_field;
    using kwaque::observability::event_field_key;
    using kwaque::observability::event_field_value;
    using kwaque::observability::event_public_text;
    const std::array fields{
      event_field{
        .key = event_field_key::outcome,
        .value = event_field_value::from_text(
          text(event_public_text::outcome_completed))},
      event_field{
        .key = event_field_key::operation,
        .value = event_field_value::from_text(
          text(event_public_text::operation_dns_resolve))},
      event_field{
        .key = event_field_key::items,
        .value = event_field_value::from_unsigned(2)},
    };
    auto made = kwaque::observability::event_request::make(
      kwaque::observability::event_request_context{
        .kind = kwaque::observability::event_kind::dns_completion,
        .severity = kwaque::observability::event_severity::info,
        .monotonic = kwaque::runtime::monotonic_time{monotonic},
        .wall = kwaque::runtime::wall_time{23},
        .workload = kwaque::resource::workload_class::metadata,
      },
      fields);
    BOOST_REQUIRE(made.has_value());
    return *made;
}

kwaque::observability::event_sink_identity
identity(std::uint64_t epoch_value = 11) {
    auto epoch = kwaque::observability::event_sink_epoch::make(epoch_value);
    BOOST_REQUIRE(epoch.has_value());
    kwaque::observability::event_configuration_digest digest{};
    digest[0] = 0x51;
    return kwaque::observability::event_sink_identity{
      .epoch = *epoch,
      .configuration_digest = digest,
    };
}

kwaque::observability::event_log_limits limits(std::uint32_t entries = 2) {
    auto made = kwaque::observability::event_log_limits::make(
      kwaque::observability::event_log_limit_values{
        .entries = entries,
        .encoded_bytes = 4'096,
      });
    BOOST_REQUIRE(made.has_value());
    return *made;
}

seastar::future<std::vector<std::uint8_t>> capture_history(bool perturb_tasks) {
    kwaque::simulation::event_log_sink sink{identity(), limits(3)};
    const auto value = make_event();
    for (std::uint32_t index = 0; index < 3; ++index) {
        std::size_t attempts = 0;
        bool emitted = false;
        seastar::memory::with_allocation_failures([&] {
            ++attempts;
            emitted = sink.emit(value).has_value();
        });
        BOOST_REQUIRE(emitted);
        BOOST_REQUIRE(attempts == 1U);
        if (perturb_tasks) {
            co_await seastar::yield();
        }
    }
    const auto encoded = sink.events().encode();
    BOOST_REQUIRE(encoded.has_value());
    auto flattened = encoded->to_vector();
    BOOST_REQUIRE(flattened.has_value());
    BOOST_REQUIRE(sink.stop().has_value());
    co_return std::move(*flattened);
}

static_assert(
  kwaque::observability::event_sink<kwaque::simulation::event_log_sink>);

} // namespace

SEASTAR_TEST_CASE(simulation_event_sink_records_identical_canonical_events) {
    const auto sink_identity = identity();
    kwaque::simulation::event_log_sink sink{sink_identity, limits()};
    const auto value = make_event();
    std::size_t emit_attempts = 0;
    bool emitted = false;
    seastar::memory::with_allocation_failures([&] {
        ++emit_attempts;
        emitted = sink.emit(value).has_value();
    });
    BOOST_CHECK(emitted);
    BOOST_CHECK(emit_attempts == 1U);
    BOOST_CHECK(sink.events().entries().size() == 1U);
    BOOST_CHECK(sink.events().identity() == sink_identity);
    BOOST_CHECK(sink.events().entries()[0].sequence() == 1U);
    BOOST_CHECK(sink.events().entries()[0].kind() == value.kind());

    const auto encoded = sink.events().encode();
    BOOST_REQUIRE(encoded.has_value());
    const auto decoded = kwaque::observability::event_log::decode(
      *encoded, sink.events().limits());
    BOOST_REQUIRE(decoded.has_value());
    BOOST_CHECK((*decoded)->entries()[0] == sink.events().entries()[0]);

    BOOST_REQUIRE(sink.emit(value).has_value());
    const auto rejected = sink.emit(value);
    BOOST_REQUIRE(!rejected.has_value());
    BOOST_CHECK(rejected.error().code() == kwaque::errc::resource_exhausted);
    BOOST_CHECK(sink.last_sequence() == 2U);
    BOOST_REQUIRE(sink.stop().has_value());
    BOOST_REQUIRE(sink.stop().has_value());
    const auto after_stop = sink.emit(value);
    BOOST_REQUIRE(!after_stop.has_value());
    BOOST_CHECK(after_stop.error().code() == kwaque::errc::closed);
    co_return;
}

SEASTAR_TEST_CASE(simulation_event_reservation_protects_terminal_capacity) {
    kwaque::simulation::event_log_sink sink{identity(), limits()};
    const auto value = make_event();
    auto terminal = sink.reserve(
      1,
      kwaque::observability::canonical_event_log_record_prefix_size
        + value.encoded_size());
    BOOST_REQUIRE(terminal.has_value());

    BOOST_REQUIRE(sink.emit(value).has_value());
    const auto saturated = sink.emit(value);
    BOOST_REQUIRE(!saturated.has_value());
    BOOST_CHECK(saturated.error().code() == kwaque::errc::resource_exhausted);
    BOOST_REQUIRE(sink.emit_reserved(value, *terminal).has_value());
    BOOST_CHECK(!terminal->active());
    BOOST_CHECK_EQUAL(sink.events().entries().size(), 2U);
    BOOST_REQUIRE(sink.stop().has_value());
    co_return;
}

SEASTAR_TEST_CASE(simulation_event_reservation_releases_unused_capacity) {
    kwaque::simulation::event_log_sink sink{identity(), limits()};
    const auto value = make_event();
    {
        auto unused = sink.reserve(
          1,
          kwaque::observability::canonical_event_log_record_prefix_size
            + value.encoded_size());
        BOOST_REQUIRE(unused.has_value());
        BOOST_CHECK(unused->active());
    }
    BOOST_REQUIRE(sink.emit(value).has_value());
    BOOST_REQUIRE(sink.emit(value).has_value());
    BOOST_REQUIRE(sink.stop().has_value());
    const auto after_stop = sink.reserve(
      1,
      kwaque::observability::canonical_event_log_record_prefix_size
        + value.encoded_size());
    BOOST_REQUIRE(!after_stop.has_value());
    BOOST_CHECK(after_stop.error().code() == kwaque::errc::closed);
    co_return;
}

SEASTAR_TEST_CASE(simulation_event_sequences_ignore_task_and_allocator_noise) {
    const auto direct = co_await capture_history(false);
    const auto perturbed = co_await capture_history(true);
    BOOST_CHECK(direct == perturbed);
    co_return;
}

SEASTAR_TEST_CASE(simulation_event_replay_compares_before_publication) {
    kwaque::simulation::event_log_sink captured{identity(), limits()};
    BOOST_REQUIRE(captured.emit(make_event()).has_value());
    const auto encoded = captured.events().encode();
    BOOST_REQUIRE(encoded.has_value());
    auto expected = kwaque::observability::event_log::decode(
      *encoded, captured.events().limits());
    BOOST_REQUIRE(expected.has_value());
    auto replayed = kwaque::simulation::event_log_sink::replay(
      identity(), limits(), std::move(*expected));
    BOOST_REQUIRE(replayed.has_value());

    const auto mismatch = (*replayed)->emit(make_event(18));
    BOOST_REQUIRE(!mismatch.has_value());
    BOOST_CHECK(mismatch.error().code() == kwaque::errc::replay_divergence);
    BOOST_REQUIRE(mismatch.error().context_at(0).has_value());
    BOOST_CHECK_EQUAL(mismatch.error().context_at(0)->value, 1U);
    BOOST_REQUIRE(mismatch.error().context_at(1).has_value());
    BOOST_CHECK_EQUAL(
      mismatch.error().context_at(1)->value,
      static_cast<std::uint8_t>(
        kwaque::simulation::event_replay_difference::value));
    BOOST_CHECK_EQUAL((*replayed)->last_sequence(), 0U);
    BOOST_CHECK((*replayed)->events().entries().empty());
    BOOST_REQUIRE((*replayed)->replay_failure() != nullptr);
    BOOST_REQUIRE((*replayed)->stop().has_value());
    BOOST_REQUIRE(captured.stop().has_value());
    co_return;
}

SEASTAR_TEST_CASE(simulation_event_replay_detects_missing_and_extra_values) {
    kwaque::simulation::event_log_sink captured{identity(), limits()};
    BOOST_REQUIRE(captured.emit(make_event()).has_value());
    const auto encoded = captured.events().encode();
    BOOST_REQUIRE(encoded.has_value());

    {
        auto expected = kwaque::observability::event_log::decode(
          *encoded, captured.events().limits());
        BOOST_REQUIRE(expected.has_value());
        auto replayed = kwaque::simulation::event_log_sink::replay(
          identity(), limits(), std::move(*expected));
        BOOST_REQUIRE(replayed.has_value());
        const auto missing = (*replayed)->finish_replay();
        BOOST_REQUIRE(!missing.has_value());
        BOOST_CHECK(missing.error().code() == kwaque::errc::replay_divergence);
        BOOST_REQUIRE(missing.error().context_at(1).has_value());
        BOOST_CHECK_EQUAL(
          missing.error().context_at(1)->value,
          static_cast<std::uint8_t>(
            kwaque::simulation::event_replay_difference::actual_missing));
        BOOST_REQUIRE((*replayed)->stop().has_value());
    }
    {
        auto expected = kwaque::observability::event_log::decode(
          *encoded, captured.events().limits());
        BOOST_REQUIRE(expected.has_value());
        auto replayed = kwaque::simulation::event_log_sink::replay(
          identity(), limits(), std::move(*expected));
        BOOST_REQUIRE(replayed.has_value());
        BOOST_REQUIRE((*replayed)->emit(make_event()).has_value());
        const auto extra = (*replayed)->emit(make_event());
        BOOST_REQUIRE(!extra.has_value());
        BOOST_CHECK(extra.error().code() == kwaque::errc::replay_divergence);
        BOOST_REQUIRE(extra.error().context_at(1).has_value());
        BOOST_CHECK_EQUAL(
          extra.error().context_at(1)->value,
          static_cast<std::uint8_t>(
            kwaque::simulation::event_replay_difference::expected_missing));
        BOOST_CHECK_EQUAL((*replayed)->last_sequence(), 1U);
        BOOST_CHECK_EQUAL((*replayed)->events().entries().size(), 1U);
        BOOST_REQUIRE((*replayed)->stop().has_value());
    }
    BOOST_REQUIRE(captured.stop().has_value());
    co_return;
}
