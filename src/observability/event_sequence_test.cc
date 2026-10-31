#include "src/observability/event.h"
#include "src/observability/event_log.h"
#include "src/observability/event_sequence.h"
#include "src/observability/testing/capture_event_sink.h"
#include "src/observability/testing/event_sequence_test_access.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/smp.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/alloc_failure_injector.hh>

#include <boost/test/unit_test.hpp>

#include <array>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <utility>

namespace {

using kwaque::observability::event_configuration_digest;
using kwaque::observability::event_kind;
using kwaque::observability::event_log_limit_values;
using kwaque::observability::event_log_limits;
using kwaque::observability::event_request;
using kwaque::observability::event_request_context;
using kwaque::observability::event_sequence;
using kwaque::observability::event_sequence_test_access;
using kwaque::observability::event_severity;
using kwaque::observability::event_shard;
using kwaque::observability::event_sink_epoch;
using kwaque::observability::event_sink_identity;
using kwaque::observability::testing::capture_event_sink;

template<typename Value>
concept has_caller_shard = requires(const Value& value) { value.shard(); };

template<typename Value>
concept has_caller_sequence = requires(const Value& value) {
    value.sequence();
};

template<typename Sequence>
concept accepts_caller_shard = requires(
  Sequence& sequence, const event_request& value, event_shard shard) {
    sequence.prepare(value, shard);
};

static_assert(!has_caller_shard<event_request>);
static_assert(!has_caller_sequence<event_request>);
static_assert(!std::constructible_from<event_shard, std::uint32_t>);
static_assert(!std::constructible_from<event_sequence, event_sink_identity>);
static_assert(!accepts_caller_shard<event_sequence>);

event_sink_identity
identity(std::uint64_t epoch_value, std::uint8_t digest_value = 0x11) {
    auto epoch = event_sink_epoch::make(epoch_value);
    BOOST_REQUIRE(epoch.has_value());
    event_configuration_digest digest{};
    digest.fill(digest_value);
    return event_sink_identity{
      .epoch = *epoch,
      .configuration_digest = digest,
    };
}

event_request request() {
    auto made = event_request::make(
      event_request_context{
        .kind = event_kind::runtime_state_changed,
        .severity = event_severity::info,
        .monotonic = kwaque::runtime::monotonic_time{7},
        .wall = kwaque::runtime::wall_time{9},
        .workload = kwaque::resource::workload_class::metadata,
      },
      {});
    BOOST_REQUIRE(made.has_value());
    return *made;
}

event_log_limits limits(std::uint32_t entries = 4) {
    auto made = event_log_limits::make(
      event_log_limit_values{
        .entries = entries,
        .encoded_bytes = 4'096,
      });
    BOOST_REQUIRE(made.has_value());
    return *made;
}

std::array<std::uint64_t, 4>
local_sequence_history(event_sink_identity sink_identity) {
    auto sequence = event_sequence_test_access::make(sink_identity);
    const auto value = request();
    const auto shard = kwaque::runtime::owner_shard{}.value();
    auto first = event_sequence_test_access::prepare(*sequence, value);
    BOOST_REQUIRE(first.has_value());
    const auto first_value = first->value().sequence();
    first->commit();
    auto second = event_sequence_test_access::prepare(*sequence, value);
    BOOST_REQUIRE(second.has_value());
    const auto second_value = second->value().sequence();
    second->commit();
    return {shard, first_value, second_value, sink_identity.epoch.value()};
}

} // namespace

SEASTAR_TEST_CASE(event_sequence_is_transactional_and_overflow_checked) {
    BOOST_CHECK(!event_sink_epoch::make(0).has_value());
    BOOST_CHECK(
      event_sink_epoch::make(std::numeric_limits<std::uint64_t>::max())
        .has_value());

    auto sequence = event_sequence_test_access::make(identity(5));
    const auto value = request();
    {
        auto uncommitted = event_sequence_test_access::prepare(
          *sequence, value);
        BOOST_REQUIRE(uncommitted.has_value());
        BOOST_CHECK(uncommitted->value().sequence() == 1U);
        BOOST_CHECK(sequence->last_sequence() == 0U);
        const auto nested = event_sequence_test_access::prepare(
          *sequence, value);
        BOOST_REQUIRE(!nested.has_value());
        BOOST_CHECK(nested.error().code() == kwaque::errc::unavailable);
    }
    BOOST_CHECK(sequence->last_sequence() == 0U);

    std::size_t attempts = 0;
    std::uint64_t prepared_sequence = 0;
    bool prepared_without_allocation = false;
    seastar::memory::with_allocation_failures([&] {
        ++attempts;
        auto prepared = event_sequence_test_access::prepare(*sequence, value);
        if (prepared) {
            prepared_sequence = prepared->value().sequence();
            prepared->commit();
            prepared_without_allocation = true;
        }
    });
    BOOST_CHECK(prepared_without_allocation);
    BOOST_CHECK(attempts == 1U);
    BOOST_CHECK(prepared_sequence == 1U);
    BOOST_CHECK(sequence->last_sequence() == 1U);

    event_sequence_test_access::set_last_sequence(
      *sequence, std::numeric_limits<std::uint64_t>::max() - 1U);
    auto maximum = event_sequence_test_access::prepare(*sequence, value);
    BOOST_REQUIRE(maximum.has_value());
    BOOST_CHECK(
      maximum->value().sequence() == std::numeric_limits<std::uint64_t>::max());
    maximum->commit();
    const auto overflow = event_sequence_test_access::prepare(*sequence, value);
    BOOST_REQUIRE(!overflow.has_value());
    BOOST_CHECK(overflow.error().code() == kwaque::errc::out_of_range);
    BOOST_CHECK(
      sequence->last_sequence() == std::numeric_limits<std::uint64_t>::max());
    co_return;
}

SEASTAR_TEST_CASE(event_sequences_are_per_shard_not_globally_ordered) {
    BOOST_REQUIRE_GE(seastar::this_smp_shard_count(), 2U);
    const auto sink_identity = identity(17);
    const auto local = local_sequence_history(sink_identity);
    const auto remote = co_await seastar::smp::submit_to(
      1, [sink_identity] { return local_sequence_history(sink_identity); });

    BOOST_CHECK(local[0] == 0U);
    BOOST_CHECK(remote[0] == 1U);
    BOOST_CHECK(local[1] == 1U);
    BOOST_CHECK(remote[1] == 1U);
    BOOST_CHECK(local[2] == 2U);
    BOOST_CHECK(remote[2] == 2U);
    BOOST_CHECK(local[3] == remote[3]);
    co_return;
}

SEASTAR_TEST_CASE(new_sink_epochs_restart_sequences_and_change_reproduction) {
    const auto value = request();
    capture_event_sink first{identity(21), limits(1)};
    capture_event_sink second{identity(22), limits(1)};
    BOOST_REQUIRE(first.emit(value).has_value());
    BOOST_REQUIRE(second.emit(value).has_value());
    BOOST_CHECK(first.events().entries()[0].sequence() == 1U);
    BOOST_CHECK(second.events().entries()[0].sequence() == 1U);
    BOOST_CHECK(first.events().identity().epoch.value() == 21U);
    BOOST_CHECK(second.events().identity().epoch.value() == 22U);

    const auto first_encoded = first.events().encode();
    const auto second_encoded = second.events().encode();
    BOOST_REQUIRE(first_encoded.has_value());
    BOOST_REQUIRE(second_encoded.has_value());
    const auto first_bytes = first_encoded->to_vector();
    const auto second_bytes = second_encoded->to_vector();
    BOOST_REQUIRE(first_bytes.has_value());
    BOOST_REQUIRE(second_bytes.has_value());
    BOOST_CHECK(*first_bytes != *second_bytes);
    BOOST_REQUIRE(first.stop().has_value());
    BOOST_REQUIRE(second.stop().has_value());
    co_return;
}

namespace {

using kwaque::observability::event_field;
using kwaque::observability::event_field_key;
using kwaque::observability::event_field_value;
using kwaque::observability::event_public_text;
using kwaque::observability::event_text;

event_text text(event_public_text value) {
    auto made = event_text::make(value);
    BOOST_REQUIRE(made.has_value());
    return *made;
}

event_request kind_at(event_kind kind, std::uint64_t monotonic) {
    std::array<event_field, 1> fields{};
    if (kind == event_kind::append_refused) {
        fields[0] = event_field{
          .key = event_field_key::outcome,
          .value = event_field_value::from_text(
            text(event_public_text::outcome_no_space))};
    } else {
        fields[0] = event_field{
          .key = event_field_key::state,
          .value = event_field_value::from_text(
            text(event_public_text::state_not_ready))};
    }
    auto made = event_request::make(
      event_request_context{
        .kind = kind,
        .severity = event_severity::warning,
        .monotonic = kwaque::runtime::monotonic_time{monotonic},
        .wall = kwaque::runtime::wall_time{0},
        .workload = kwaque::resource::workload_class::foreground_protocol,
      },
      fields);
    BOOST_REQUIRE(made.has_value());
    return *made;
}

} // namespace

SEASTAR_TEST_CASE(a_refusal_storm_keeps_one_event_a_second_and_every_change) {
    // 1,000 refusals a second for ten seconds, and a readiness change every
    // tenth of a second.
    constexpr std::uint64_t millisecond = 1'000'000;
    auto made = event_log_limits::make(
      event_log_limit_values{.entries = 1'024, .encoded_bytes = 1'048'576});
    BOOST_REQUIRE(made.has_value());
    capture_event_sink sink{identity(31), *made};
    for (std::uint64_t at = 0; at < 10'000; ++at) {
        BOOST_REQUIRE(
          sink.emit(kind_at(event_kind::append_refused, at * millisecond))
            .has_value());
        if (at % 100 == 0) {
            BOOST_REQUIRE(
              sink
                .emit(kind_at(event_kind::readiness_changed, at * millisecond))
                .has_value());
        }
    }
    std::uint64_t refusals = 0;
    std::uint64_t changes = 0;
    std::uint64_t carried = 0;
    std::uint64_t expected_sequence = 1;
    for (const auto& value : sink.events().entries()) {
        // No sequence number is skipped by a dropped event.
        BOOST_CHECK_EQUAL(value.sequence(), expected_sequence++);
        if (value.kind() == event_kind::readiness_changed) {
            ++changes;
            continue;
        }
        BOOST_REQUIRE(value.kind() == event_kind::append_refused);
        // One a second, from the first, each after the first carrying the
        // 999 dropped since.
        BOOST_CHECK_EQUAL(
          value.monotonic().nanoseconds(), refusals * 1'000U * millisecond);
        std::uint64_t suppressed = 0;
        for (const auto& field : value.fields()) {
            if (field.key == event_field_key::suppressed) {
                suppressed = *field.value.as_unsigned();
            }
        }
        BOOST_CHECK_EQUAL(suppressed, refusals == 0 ? 0U : 999U);
        carried += suppressed;
        ++refusals;
    }
    BOOST_CHECK_EQUAL(refusals, 10U);
    BOOST_CHECK_EQUAL(changes, 100U);
    // The counter counts every drop; the last second's 999 wait for the next
    // admitted refusal.
    BOOST_CHECK_EQUAL(sink.suppressed_total(), 9'990U);
    BOOST_CHECK_EQUAL(carried + 999U, 9'990U);
    BOOST_REQUIRE(sink.stop().has_value());
    co_return;
}

SEASTAR_TEST_CASE(an_event_never_emitted_spends_no_grant) {
    auto sequence = event_sequence_test_access::make(identity(33));
    {
        auto prepared = event_sequence_test_access::prepare_bounded(
          *sequence, kind_at(event_kind::append_refused, 0));
        BOOST_REQUIRE(prepared.has_value() && prepared->has_value());
        // Released without a commit, as a sink that failed to write does.
    }
    auto again = event_sequence_test_access::prepare_bounded(
      *sequence, kind_at(event_kind::append_refused, 0));
    BOOST_REQUIRE(again.has_value());
    BOOST_REQUIRE(again->has_value());
    BOOST_CHECK_EQUAL((*again)->value().sequence(), 1U);
    (*again)->commit();
    auto dropped = event_sequence_test_access::prepare_bounded(
      *sequence, kind_at(event_kind::append_refused, 0));
    BOOST_REQUIRE(dropped.has_value());
    BOOST_CHECK(!dropped->has_value());
    BOOST_CHECK_EQUAL(sequence->suppressed_total(), 1U);
    BOOST_CHECK_EQUAL(sequence->last_sequence(), 1U);
    co_return;
}
