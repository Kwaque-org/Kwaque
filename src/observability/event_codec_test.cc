#include "src/observability/event.h"
#include "src/observability/event_codec.h"
#include "src/observability/event_log.h"
#include "src/observability/event_sequence.h"

#include <seastar/core/coroutine.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/alloc_failure_injector.hh>

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

namespace {

using kwaque::observability::canonical_event_fixed_encoded_size;
using kwaque::observability::canonical_event_log_header_encoded_size;
using kwaque::observability::canonical_event_log_record_prefix_size;
using kwaque::observability::decode_event;
using kwaque::observability::encode_event;
using kwaque::observability::event;
using kwaque::observability::event_configuration_digest;
using kwaque::observability::event_field;
using kwaque::observability::event_field_key;
using kwaque::observability::event_field_type;
using kwaque::observability::event_field_value;
using kwaque::observability::event_kind;
using kwaque::observability::event_log;
using kwaque::observability::event_log_limit_values;
using kwaque::observability::event_log_limits;
using kwaque::observability::event_public_text;
using kwaque::observability::event_request;
using kwaque::observability::event_request_context;
using kwaque::observability::event_schema_version;
using kwaque::observability::event_sequence;
using kwaque::observability::event_severity;
using kwaque::observability::event_shard;
using kwaque::observability::event_sink_epoch;
using kwaque::observability::event_sink_identity;
using kwaque::observability::event_stable_id;
using kwaque::observability::event_text;

event_text text(event_public_text value) {
    auto made = event_text::make(value);
    BOOST_REQUIRE(made.has_value());
    return *made;
}

event_stable_id stable_id(std::uint64_t value = 17) {
    auto made = event_stable_id::make(value);
    BOOST_REQUIRE(made.has_value());
    return *made;
}

event_sink_identity
identity(std::uint64_t epoch_value = 1, std::uint8_t digest_seed = 0x10) {
    auto epoch = event_sink_epoch::make(epoch_value);
    BOOST_REQUIRE(epoch.has_value());
    event_configuration_digest digest{};
    for (std::size_t index = 0; index < digest.size(); ++index) {
        digest[index] = static_cast<std::uint8_t>(digest_seed + index);
    }
    return event_sink_identity{
      .epoch = *epoch,
      .configuration_digest = digest,
    };
}

event stamp(event_sequence& sequence, const event_request& request) {
    auto prepared = sequence.prepare(
      request, event_shard::from_owner(kwaque::runtime::owner_shard{}));
    BOOST_REQUIRE(prepared.has_value());
    auto value = prepared->value();
    prepared->commit();
    return value;
}

event_request make_file_request() {
    const std::array fields{
      event_field{
        .key = event_field_key::stable_id,
        .value = event_field_value::from_stable_id(stable_id())},
      event_field{
        .key = event_field_key::bytes,
        .value = event_field_value::from_unsigned(
          UINT64_C(0x0102030405060708))},
      event_field{
        .key = event_field_key::operation,
        .value = event_field_value::from_text(
          text(event_public_text::operation_file_write))},
      event_field{
        .key = event_field_key::outcome,
        .value = event_field_value::from_text(
          text(event_public_text::outcome_completed))},
    };
    auto made = event_request::make(
      event_request_context{
        .kind = event_kind::file_completion,
        .severity = event_severity::info,
        .monotonic = kwaque::runtime::monotonic_time{UINT64_C(
          0x1112131415161718)},
        .wall = kwaque::runtime::wall_time{-4},
        .workload = kwaque::resource::workload_class::metadata,
      },
      fields);
    BOOST_REQUIRE(made.has_value());
    return *made;
}

event make_file_event() {
    event_sequence sequence{identity()};
    return stamp(sequence, make_file_request());
}

event_request make_large_fault_request() {
    const auto maximum = std::numeric_limits<std::uint64_t>::max();
    const std::array fields{
      event_field{
        .key = event_field_key::outcome,
        .value = event_field_value::from_text(
          text(event_public_text::outcome_applied))},
      event_field{
        .key = event_field_key::operation,
        .value = event_field_value::from_text(
          text(event_public_text::operation_fault_evaluate))},
      event_field{
        .key = event_field_key::reason,
        .value = event_field_value::from_text(
          text(event_public_text::reason_fault_injected))},
      event_field{
        .key = event_field_key::duration_ns,
        .value = event_field_value::from_unsigned(maximum)},
      event_field{
        .key = event_field_key::stable_id,
        .value = event_field_value::from_stable_id(stable_id(maximum))},
      event_field{
        .key = event_field_key::occurrence,
        .value = event_field_value::from_unsigned(maximum)},
      event_field{
        .key = event_field_key::delta,
        .value = event_field_value::from_signed(
          std::numeric_limits<std::int64_t>::min())},
      event_field{
        .key = event_field_key::enabled,
        .value = event_field_value::from_boolean(true)},
    };
    auto made = event_request::make(
      event_request_context{
        .kind = event_kind::fault_decision,
        .severity = event_severity::warning,
        .monotonic = kwaque::runtime::monotonic_time{maximum},
        .wall
        = kwaque::runtime::wall_time{std::numeric_limits<std::int64_t>::min()},
        .workload = kwaque::resource::workload_class::maintenance,
      },
      fields);
    BOOST_REQUIRE(made.has_value());
    return *made;
}

event make_large_fault_event() {
    event_sequence sequence{identity()};
    return stamp(sequence, make_large_fault_request());
}

event_log_limits limits(std::uint32_t entries, std::uint64_t encoded_bytes) {
    auto made = event_log_limits::make(
      event_log_limit_values{
        .entries = entries,
        .encoded_bytes = encoded_bytes,
      });
    BOOST_REQUIRE(made.has_value());
    return *made;
}

template<typename Integer>
Integer
read_little_endian(std::span<const std::uint8_t> bytes, std::size_t offset) {
    Integer result = 0;
    for (std::size_t index = 0; index < sizeof(Integer); ++index) {
        result |= static_cast<Integer>(bytes[offset + index]) << (index * 8U);
    }
    return result;
}

} // namespace

SEASTAR_TEST_CASE(event_codec_is_exact_canonical_and_allocation_free) {
    const auto source = make_file_event();
    const auto encoded = encode_event(source);
    BOOST_REQUIRE(encoded.has_value());
    BOOST_CHECK(encoded->size() == source.encoded_size());
    BOOST_REQUIRE(encoded->size() > canonical_event_fixed_encoded_size);
    BOOST_CHECK(
      read_little_endian<std::uint32_t>(encoded->bytes(), 0)
      == event_schema_version);
    BOOST_CHECK(
      read_little_endian<std::uint16_t>(encoded->bytes(), 4)
      == static_cast<std::uint16_t>(event_kind::file_completion));
    BOOST_CHECK(
      encoded->bytes()[6] == static_cast<std::uint8_t>(source.name().size()));
    const auto name_size = encoded->bytes()[6];
    const auto severity_offset = 7U + name_size;
    const std::string_view encoded_name{
      reinterpret_cast<const char*>(encoded->bytes().data() + 7U), name_size};
    BOOST_CHECK(encoded_name == source.name());
    BOOST_CHECK(
      encoded->bytes()[severity_offset]
      == static_cast<std::uint8_t>(source.severity()));
    BOOST_CHECK(
      read_little_endian<std::uint64_t>(encoded->bytes(), severity_offset + 1U)
      == source.monotonic().nanoseconds());
    BOOST_CHECK(
      read_little_endian<std::uint64_t>(encoded->bytes(), severity_offset + 9U)
      == std::bit_cast<std::uint64_t>(source.wall().unix_nanoseconds()));
    BOOST_CHECK(
      read_little_endian<std::uint32_t>(encoded->bytes(), severity_offset + 17U)
      == source.shard().value());
    BOOST_CHECK(
      encoded->bytes()[severity_offset + 21U]
      == static_cast<std::uint8_t>(source.workload()));
    BOOST_CHECK(
      read_little_endian<std::uint64_t>(encoded->bytes(), severity_offset + 22U)
      == source.sequence());
    BOOST_CHECK(
      encoded->bytes()[severity_offset + 30U]
      == static_cast<std::uint8_t>(source.fields().size()));

    const auto decoded = decode_event(encoded->bytes());
    BOOST_REQUIRE(decoded.has_value());
    BOOST_CHECK(*decoded == source);
    const auto reencoded = encode_event(*decoded);
    BOOST_REQUIRE(reencoded.has_value());
    BOOST_CHECK(*reencoded == *encoded);

    const auto extreme_source = make_large_fault_event();
    const auto extreme_encoded = encode_event(extreme_source);
    BOOST_REQUIRE(extreme_encoded.has_value());
    const auto extreme_decoded = decode_event(extreme_encoded->bytes());
    BOOST_REQUIRE(extreme_decoded.has_value());
    BOOST_CHECK(*extreme_decoded == extreme_source);
    const auto extreme_reencoded = encode_event(*extreme_decoded);
    BOOST_REQUIRE(extreme_reencoded.has_value());
    BOOST_CHECK(*extreme_reencoded == *extreme_encoded);

    std::size_t encode_attempts = 0;
    bool encode_succeeded = false;
    seastar::memory::with_allocation_failures([&] {
        ++encode_attempts;
        encode_succeeded = encode_event(source).has_value();
    });
    BOOST_CHECK(encode_succeeded);
    BOOST_CHECK(encode_attempts == 1U);
    std::size_t decode_attempts = 0;
    bool decode_succeeded = false;
    seastar::memory::with_allocation_failures([&] {
        ++decode_attempts;
        decode_succeeded = decode_event(encoded->bytes()).has_value();
    });
    BOOST_CHECK(decode_succeeded);
    BOOST_CHECK(decode_attempts == 1U);
    co_return;
}

SEASTAR_TEST_CASE(event_codec_rejects_noncanonical_and_malformed_bytes) {
    const auto source = make_file_event();
    const auto encoded = encode_event(source);
    BOOST_REQUIRE(encoded.has_value());
    const std::vector<std::uint8_t> canonical(
      encoded->bytes().begin(), encoded->bytes().end());
    const auto event_name_size = canonical[6];
    const auto severity_offset = 7U + event_name_size;
    const auto workload_offset = severity_offset + 1U + 8U + 8U + 4U;
    const auto sequence_offset = workload_offset + 1U;
    const auto field_count_offset = sequence_offset + 8U;
    const auto first_field_offset = field_count_offset + 1U;

    const auto rejected = [&](std::vector<std::uint8_t> bytes) {
        const auto result = decode_event(std::span<const std::uint8_t>{bytes});
        BOOST_CHECK(!result.has_value());
    };

    rejected({});
    auto truncated = canonical;
    truncated.pop_back();
    rejected(std::move(truncated));
    auto mutation = canonical;
    mutation[0] = 2;
    rejected(mutation);
    mutation = canonical;
    mutation[4] = 0;
    rejected(mutation);
    mutation = canonical;
    mutation[7] = 'x';
    rejected(mutation);
    mutation = canonical;
    mutation[severity_offset] = 0;
    rejected(mutation);
    mutation = canonical;
    mutation[workload_offset] = 255;
    rejected(mutation);
    mutation = canonical;
    auto sequence_bytes = std::span{mutation}.subspan(sequence_offset, 8U);
    std::fill(sequence_bytes.begin(), sequence_bytes.end(), std::uint8_t{0});
    rejected(mutation);
    mutation = canonical;
    mutation[field_count_offset] = 9;
    rejected(mutation);
    mutation = canonical;
    mutation[first_field_offset] = static_cast<std::uint8_t>(
      event_field_key::state);
    rejected(mutation);
    mutation = canonical;
    mutation[first_field_offset + 2U] = 1;
    rejected(mutation);
    mutation = canonical;
    mutation[first_field_offset + 3U] = static_cast<std::uint8_t>(
      event_field_type::unsigned_integer);
    rejected(mutation);
    mutation = canonical;
    mutation[first_field_offset + 4U] = 0;
    mutation[first_field_offset + 5U] = 0;
    rejected(mutation);
    mutation = canonical;
    const auto first_field_name_size = mutation[first_field_offset + 2U];
    mutation[first_field_offset + 6U + first_field_name_size] = 'x';
    rejected(mutation);
    mutation = canonical;
    const auto first_field_size = 6U + first_field_name_size
                                  + read_little_endian<std::uint16_t>(
                                    mutation, first_field_offset + 4U);
    mutation[first_field_offset + first_field_size]
      = mutation[first_field_offset];
    mutation[first_field_offset + first_field_size + 1U]
      = mutation[first_field_offset + 1U];
    rejected(mutation);
    mutation = canonical;
    mutation.push_back(0);
    rejected(mutation);
    mutation.assign(kwaque::observability::event_encoded_bytes_max + 1U, 0);
    const auto oversized = decode_event(
      std::span<const std::uint8_t>{mutation});
    BOOST_REQUIRE(!oversized.has_value());
    BOOST_CHECK(oversized.error().code() == kwaque::errc::out_of_range);

    const auto boolean_encoding = encode_event(make_large_fault_event());
    BOOST_REQUIRE(boolean_encoding.has_value());
    mutation.assign(
      boolean_encoding->bytes().begin(), boolean_encoding->bytes().end());
    std::size_t field_offset = canonical_event_fixed_encoded_size + mutation[6];
    bool boolean_found = false;
    for (std::uint8_t index = 0; index < mutation[field_offset - 1U]; ++index) {
        const auto key = read_little_endian<std::uint16_t>(
          mutation, field_offset);
        const auto name_size = mutation[field_offset + 2U];
        const auto payload_size = read_little_endian<std::uint16_t>(
          mutation, field_offset + 4U);
        if (key == static_cast<std::uint16_t>(event_field_key::enabled)) {
            mutation[field_offset + 6U + name_size] = 2;
            boolean_found = true;
            break;
        }
        field_offset += 6U + name_size + payload_size;
    }
    BOOST_REQUIRE(boolean_found);
    rejected(mutation);
    co_return;
}

SEASTAR_TEST_CASE(event_log_is_reserved_bounded_and_canonical) {
    BOOST_CHECK(!event_log_limits::make(
                   event_log_limit_values{
                     .entries = 0,
                     .encoded_bytes = canonical_event_log_header_encoded_size})
                   .has_value());
    BOOST_CHECK(
      event_log_limits::make(
        event_log_limit_values{
          .entries = event_log_limits::entries_absolute,
          .encoded_bytes = event_log_limits::encoded_bytes_absolute})
        .has_value());
    BOOST_CHECK(!event_log_limits::make(
                   event_log_limit_values{
                     .entries = event_log_limits::entries_absolute + 1U,
                     .encoded_bytes = event_log_limits::encoded_bytes_absolute})
                   .has_value());
    BOOST_CHECK(
      !event_log_limits::make(
         event_log_limit_values{
           .entries = 1,
           .encoded_bytes = canonical_event_log_header_encoded_size - 1U})
         .has_value());

    const auto sink_identity = identity();
    event_log empty{
      sink_identity, limits(1, canonical_event_log_header_encoded_size)};
    const auto empty_encoding = empty.encode();
    BOOST_REQUIRE(empty_encoding.has_value());
    BOOST_CHECK(
      empty_encoding->size() == canonical_event_log_header_encoded_size);
    const auto empty_decoded = event_log::decode(
      *empty_encoding, empty.limits());
    BOOST_REQUIRE(empty_decoded.has_value());
    BOOST_CHECK((*empty_decoded)->identity() == sink_identity);
    BOOST_CHECK((*empty_decoded)->entries().empty());

    const auto request = make_file_request();
    event_log ordered{sink_identity, limits(2, 4'096)};
    const auto first_ordered = make_file_event();
    BOOST_REQUIRE(ordered.append(first_ordered).has_value());
    const auto duplicate_sequence = ordered.append(first_ordered);
    BOOST_REQUIRE(!duplicate_sequence.has_value());
    BOOST_CHECK(
      duplicate_sequence.error().code() == kwaque::errc::invalid_argument);

    event_sequence count_sequence{sink_identity};
    const auto first = stamp(count_sequence, request);
    const auto second = stamp(count_sequence, request);
    const auto third = stamp(count_sequence, request);
    const auto record_bytes = canonical_event_log_record_prefix_size
                              + first.encoded_size();
    event_log count_limited{
      sink_identity,
      limits(2, canonical_event_log_header_encoded_size + 2U * record_bytes)};
    BOOST_REQUIRE(count_limited.append(first).has_value());
    std::size_t append_attempts = 0;
    bool append_succeeded = false;
    seastar::memory::with_allocation_failures([&] {
        ++append_attempts;
        append_succeeded = count_limited.append(second).has_value();
    });
    BOOST_CHECK(append_succeeded);
    BOOST_CHECK(append_attempts == 1U);
    const auto saturated = count_limited.append(third);
    BOOST_REQUIRE(!saturated.has_value());
    BOOST_CHECK(saturated.error().code() == kwaque::errc::resource_exhausted);
    BOOST_CHECK(count_limited.entries().size() == 2U);

    event_sequence byte_sequence{sink_identity};
    const auto byte_first = stamp(byte_sequence, request);
    const auto byte_second = stamp(byte_sequence, request);
    event_log byte_limited{
      sink_identity,
      limits(2, canonical_event_log_header_encoded_size + record_bytes)};
    BOOST_REQUIRE(byte_limited.append(byte_first).has_value());
    const auto byte_saturated = byte_limited.append(byte_second);
    BOOST_REQUIRE(!byte_saturated.has_value());
    BOOST_CHECK(
      byte_saturated.error().code() == kwaque::errc::resource_exhausted);
    BOOST_CHECK(byte_limited.entries().size() == 1U);

    const auto encoded = count_limited.encode();
    BOOST_REQUIRE(encoded.has_value());
    BOOST_CHECK(encoded->size() == count_limited.encoded_bytes());
    const auto flat = encoded->to_vector();
    BOOST_REQUIRE(flat.has_value());
    BOOST_REQUIRE(flat->size() >= canonical_event_log_header_encoded_size);
    BOOST_CHECK((*flat)[0] == 'K');
    BOOST_CHECK((*flat)[1] == 'Q');
    BOOST_CHECK((*flat)[2] == 'E');
    BOOST_CHECK((*flat)[3] == 'L');
    BOOST_CHECK(
      read_little_endian<std::uint32_t>(*flat, 4) == event_schema_version);
    BOOST_CHECK(
      read_little_endian<std::uint64_t>(*flat, 8)
      == sink_identity.epoch.value());
    const auto encoded_digest = std::span{*flat}.subspan(
      16, sink_identity.configuration_digest.size());
    BOOST_CHECK(
      std::equal(
        sink_identity.configuration_digest.begin(),
        sink_identity.configuration_digest.end(),
        encoded_digest.begin()));
    BOOST_CHECK(read_little_endian<std::uint32_t>(*flat, 48) == 2U);
    BOOST_CHECK(
      read_little_endian<std::uint64_t>(*flat, 52)
      == encoded->size() - canonical_event_log_header_encoded_size);

    const auto decoded = event_log::decode(*encoded, count_limited.limits());
    BOOST_REQUIRE(decoded.has_value());
    BOOST_CHECK((*decoded)->identity() == sink_identity);
    BOOST_CHECK((*decoded)->entries().size() == 2U);
    BOOST_CHECK((*decoded)->entries()[0] == first);
    BOOST_CHECK((*decoded)->entries()[1] == second);
    const auto reencoded = (*decoded)->encode();
    BOOST_REQUIRE(reencoded.has_value());
    const auto reencoded_flat = reencoded->to_vector();
    BOOST_REQUIRE(reencoded_flat.has_value());
    BOOST_CHECK(*reencoded_flat == *flat);

    const auto invalid_batch = co_await count_limited.encode_cooperatively(
      kwaque::observability::cooperative_event_log_entries_per_yield_max + 1U);
    BOOST_REQUIRE(!invalid_batch.has_value());
    BOOST_CHECK(invalid_batch.error().code() == kwaque::errc::invalid_argument);

    auto cooperative = co_await count_limited.encode_cooperatively(1);
    BOOST_REQUIRE(cooperative.has_value());
    auto decoded_cooperatively = co_await event_log::decode_cooperatively(
      std::move(*cooperative), count_limited.limits(), 1);
    BOOST_REQUIRE(decoded_cooperatively.has_value());
    BOOST_CHECK((*decoded_cooperatively)->entries().size() == 2U);
    co_return;
}

SEASTAR_TEST_CASE(event_log_decode_rejects_malformed_artifacts) {
    const auto source = make_file_event();
    const auto sink_identity = identity();
    event_log log{sink_identity, limits(2, 4'096)};
    BOOST_REQUIRE(log.append(source).has_value());
    const auto encoded = log.encode();
    BOOST_REQUIRE(encoded.has_value());
    const auto flat = encoded->to_vector();
    BOOST_REQUIRE(flat.has_value());

    const auto rejected = [&](std::vector<std::uint8_t> bytes) {
        auto result = event_log::decode(
          std::span<const std::uint8_t>{bytes}, log.limits());
        BOOST_CHECK(!result.has_value());
    };
    rejected({});
    auto mutation = *flat;
    mutation[0] = 'X';
    rejected(mutation);
    mutation = *flat;
    mutation[4] = 2;
    rejected(mutation);
    mutation = *flat;
    auto epoch_bytes = std::span{mutation}.subspan(8, 8);
    std::fill(epoch_bytes.begin(), epoch_bytes.end(), std::uint8_t{0});
    rejected(mutation);
    mutation = *flat;
    mutation[48] = 2;
    rejected(mutation);
    mutation = *flat;
    mutation[52] = static_cast<std::uint8_t>(mutation[52] + 1U);
    rejected(mutation);
    mutation = *flat;
    mutation[canonical_event_log_header_encoded_size] = 0;
    mutation[canonical_event_log_header_encoded_size + 1U] = 0;
    rejected(mutation);
    mutation = *flat;
    mutation.pop_back();
    rejected(mutation);
    mutation = *flat;
    mutation.push_back(0);
    rejected(mutation);
    co_return;
}

SEASTAR_TEST_CASE(large_event_logs_require_cooperative_codec_paths) {
    constexpr std::uint32_t entries{1'025};
    const auto request = make_large_fault_request();
    event_sequence sizing_sequence{identity()};
    const auto source = stamp(sizing_sequence, request);
    const auto record_bytes = canonical_event_log_record_prefix_size
                              + source.encoded_size();
    const auto sink_identity = identity();
    event_log log{
      sink_identity,
      limits(
        entries,
        canonical_event_log_header_encoded_size
          + static_cast<std::uint64_t>(entries) * record_bytes)};
    event_sequence log_sequence{sink_identity};
    for (std::uint32_t index = 0; index < entries; ++index) {
        BOOST_REQUIRE(log.append(stamp(log_sequence, request)).has_value());
    }
    const auto synchronous = log.encode();
    BOOST_REQUIRE(!synchronous.has_value());
    BOOST_CHECK(synchronous.error().code() == kwaque::errc::resource_exhausted);

    auto encoded = co_await log.encode_cooperatively(64);
    BOOST_REQUIRE(encoded.has_value());
    BOOST_CHECK(encoded->chunks().size() > 1U);
    auto decoded = co_await event_log::decode_cooperatively(
      std::move(*encoded), log.limits(), 64);
    BOOST_REQUIRE(decoded.has_value());
    BOOST_CHECK((*decoded)->entries().size() == entries);
    co_return;
}
