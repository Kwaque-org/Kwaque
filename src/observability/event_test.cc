#include "src/observability/event.h"

#include <seastar/core/coroutine.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/alloc_failure_injector.hh>

#include <boost/test/unit_test.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>

namespace {

using kwaque::observability::canonical_event_field_fixed_encoded_size;
using kwaque::observability::canonical_event_fixed_encoded_size;
using kwaque::observability::event;
using kwaque::observability::event_encoded_bytes_max;
using kwaque::observability::event_field;
using kwaque::observability::event_field_descriptor;
using kwaque::observability::event_field_key;
using kwaque::observability::event_field_name_bytes_max;
using kwaque::observability::event_field_type;
using kwaque::observability::event_field_value;
using kwaque::observability::event_fields_max;
using kwaque::observability::event_kind;
using kwaque::observability::event_name_bytes_max;
using kwaque::observability::event_public_text;
using kwaque::observability::event_request;
using kwaque::observability::event_request_context;
using kwaque::observability::event_schema_version;
using kwaque::observability::event_severity;
using kwaque::observability::event_shard;
using kwaque::observability::event_stable_id;
using kwaque::observability::event_text;
using kwaque::observability::event_text_bytes_max;

static_assert(event_schema_version == 1);
static_assert(event_fields_max == 8);
static_assert(event_name_bytes_max == 48);
static_assert(event_field_name_bytes_max == 32);
static_assert(event_text_bytes_max == 96);
static_assert(event_encoded_bytes_max == 1'024);
static_assert(sizeof(event_text) <= 104);
static_assert(sizeof(event_field_value) <= 128);
static_assert(sizeof(event) <= 2'048);
static_assert(sizeof(event_request) <= 2'048);
static_assert(sizeof(event_shard) == sizeof(std::uint32_t));
static_assert(std::is_nothrow_copy_constructible_v<event>);
static_assert(std::is_nothrow_move_constructible_v<event>);
static_assert(std::is_nothrow_move_constructible_v<event_request>);
static_assert(!std::is_default_constructible_v<event>);
static_assert(!std::is_default_constructible_v<event_request>);
static_assert(!std::is_constructible_v<event_text, std::string_view>);
static_assert(!std::is_constructible_v<event_field_value, std::string>);
static_assert(kwaque::observability::event_field_is_allowed(
  event_kind::runtime_state_changed, event_field_key::state));
static_assert(!kwaque::observability::event_field_is_allowed(
  event_kind::runtime_state_changed, event_field_key::bytes));

template<event_kind Kind, event_field_key Key, typename Value>
concept static_event_field_available = requires(Value value) {
    kwaque::observability::make_event_field<Kind, Key>(value);
};

template<typename Value>
concept event_text_constructible = requires(Value value) {
    event_text::make(value);
};

static_assert(static_event_field_available<
              event_kind::runtime_state_changed,
              event_field_key::state,
              event_public_text>);
static_assert(!static_event_field_available<
              event_kind::runtime_state_changed,
              event_field_key::bytes,
              std::uint64_t>);
static_assert(!static_event_field_available<
              event_kind::runtime_state_changed,
              event_field_key::state,
              std::uint64_t>);
static_assert(static_event_field_available<
              event_kind::queue_admission,
              event_field_key::bytes,
              std::uint64_t>);
static_assert(event_text_constructible<event_public_text>);
static_assert(!event_text_constructible<std::string_view>);

event_request_context
context(event_kind kind = event_kind::runtime_state_changed) {
    return event_request_context{
      .kind = kind,
      .severity = event_severity::info,
      .monotonic = kwaque::runtime::monotonic_time{11},
      .wall = kwaque::runtime::wall_time{-4},
      .workload = kwaque::resource::workload_class::metadata,
    };
}

event_text text(event_public_text value) {
    auto made = event_text::make(value);
    BOOST_REQUIRE(made.has_value());
    return *made;
}

} // namespace

SEASTAR_TEST_CASE(event_descriptor_registries_are_stable_and_bounded) {
    constexpr std::array expected_event_names{
      std::string_view{"runtime_state_changed"},
      std::string_view{"resource_group_state_changed"},
      std::string_view{"queue_admission"},
      std::string_view{"timer_completion"},
      std::string_view{"file_completion"},
      std::string_view{"network_delivery"},
      std::string_view{"dns_completion"},
      std::string_view{"fault_decision"},
    };
    constexpr std::array expected_event_fields{
      kwaque::observability::event_field_mask_of(
        event_field_key::state,
        event_field_key::operation,
        event_field_key::reason),
      kwaque::observability::event_field_mask_of(
        event_field_key::state,
        event_field_key::operation,
        event_field_key::reason,
        event_field_key::items,
        event_field_key::limit),
      kwaque::observability::event_field_mask_of(
        event_field_key::outcome,
        event_field_key::operation,
        event_field_key::reason,
        event_field_key::bytes,
        event_field_key::items,
        event_field_key::duration_ns,
        event_field_key::limit),
      kwaque::observability::event_field_mask_of(
        event_field_key::outcome,
        event_field_key::operation,
        event_field_key::reason,
        event_field_key::duration_ns,
        event_field_key::stable_id),
      kwaque::observability::event_field_mask_of(
        event_field_key::outcome,
        event_field_key::operation,
        event_field_key::reason,
        event_field_key::bytes,
        event_field_key::duration_ns,
        event_field_key::stable_id),
      kwaque::observability::event_field_mask_of(
        event_field_key::outcome,
        event_field_key::operation,
        event_field_key::reason,
        event_field_key::bytes,
        event_field_key::duration_ns,
        event_field_key::stable_id),
      kwaque::observability::event_field_mask_of(
        event_field_key::outcome,
        event_field_key::operation,
        event_field_key::reason,
        event_field_key::items,
        event_field_key::duration_ns,
        event_field_key::stable_id),
      kwaque::observability::event_field_mask_of(
        event_field_key::outcome,
        event_field_key::operation,
        event_field_key::reason,
        event_field_key::duration_ns,
        event_field_key::stable_id,
        event_field_key::occurrence,
        event_field_key::limit,
        event_field_key::expected,
        event_field_key::actual,
        event_field_key::delta,
        event_field_key::enabled),
    };
    const auto events = kwaque::observability::event_descriptors();
    BOOST_REQUIRE(events.size() == expected_event_names.size());
    for (std::size_t index = 0; index < events.size(); ++index) {
        BOOST_CHECK(
          static_cast<std::uint16_t>(events[index].kind) == index + 1U);
        BOOST_CHECK(events[index].name == expected_event_names[index]);
        BOOST_CHECK(
          kwaque::observability::event_name_is_valid(
            events[index].name, event_name_bytes_max));
        BOOST_CHECK(
          kwaque::observability::descriptor_for(events[index].kind)
          == &events[index]);
        BOOST_CHECK(
          events[index].allowed_fields == expected_event_fields[index]);
    }
    BOOST_CHECK(
      kwaque::observability::descriptor_for(static_cast<event_kind>(0))
      == nullptr);
    BOOST_CHECK(
      kwaque::observability::descriptor_for(static_cast<event_kind>(255))
      == nullptr);

    constexpr std::array expected_field_names{
      std::string_view{"state"},
      std::string_view{"outcome"},
      std::string_view{"operation"},
      std::string_view{"reason"},
      std::string_view{"bytes"},
      std::string_view{"items"},
      std::string_view{"duration_ns"},
      std::string_view{"stable_id"},
      std::string_view{"occurrence"},
      std::string_view{"limit"},
      std::string_view{"expected"},
      std::string_view{"actual"},
      std::string_view{"delta"},
      std::string_view{"enabled"},
    };
    constexpr std::array expected_field_types{
      event_field_type::bounded_string,
      event_field_type::bounded_string,
      event_field_type::bounded_string,
      event_field_type::bounded_string,
      event_field_type::unsigned_integer,
      event_field_type::unsigned_integer,
      event_field_type::unsigned_integer,
      event_field_type::stable_id,
      event_field_type::unsigned_integer,
      event_field_type::unsigned_integer,
      event_field_type::unsigned_integer,
      event_field_type::unsigned_integer,
      event_field_type::signed_integer,
      event_field_type::boolean,
    };
    const auto fields = kwaque::observability::event_field_descriptors();
    BOOST_REQUIRE(fields.size() == expected_field_names.size());
    for (std::size_t index = 0; index < fields.size(); ++index) {
        BOOST_CHECK(
          static_cast<std::uint16_t>(fields[index].key) == index + 1U);
        BOOST_CHECK(fields[index].name == expected_field_names[index]);
        BOOST_CHECK(fields[index].type == expected_field_types[index]);
        BOOST_CHECK(
          kwaque::observability::event_name_is_valid(
            fields[index].name, event_field_name_bytes_max));
        BOOST_CHECK(
          kwaque::observability::descriptor_for(fields[index].key)
          == &fields[index]);
    }
    BOOST_CHECK(
      kwaque::observability::descriptor_for(static_cast<event_field_key>(0))
      == nullptr);
    BOOST_CHECK(
      kwaque::observability::descriptor_for(static_cast<event_field_key>(255))
      == nullptr);

    const std::string exact_event_name(event_name_bytes_max, 'a');
    const std::string long_event_name(event_name_bytes_max + 1U, 'a');
    const std::string exact_field_name(event_field_name_bytes_max, 'a');
    const std::string long_field_name(event_field_name_bytes_max + 1U, 'a');
    BOOST_CHECK(
      kwaque::observability::event_name_is_valid(
        exact_event_name, event_name_bytes_max));
    BOOST_CHECK(!kwaque::observability::event_name_is_valid(
      long_event_name, event_name_bytes_max));
    BOOST_CHECK(!kwaque::observability::event_name_is_valid(
      "Uppercase", event_name_bytes_max));
    BOOST_CHECK(!kwaque::observability::event_name_is_valid(
      "1starts_with_digit", event_name_bytes_max));
    BOOST_CHECK(!kwaque::observability::event_name_is_valid(
      "contains-control\n", event_name_bytes_max));
    BOOST_CHECK(
      kwaque::observability::event_name_is_valid(
        exact_field_name, event_field_name_bytes_max));
    BOOST_CHECK(!kwaque::observability::event_name_is_valid(
      long_field_name, event_field_name_bytes_max));

    const auto texts = kwaque::observability::event_text_descriptors();
    BOOST_REQUIRE(texts.size() == 57U);
    for (std::size_t index = 0; index < texts.size(); ++index) {
        BOOST_CHECK(static_cast<std::uint16_t>(texts[index].id) == index + 1U);
        BOOST_CHECK(
          texts[index].role >= event_field_key::state
          && texts[index].role <= event_field_key::reason);
        BOOST_CHECK(
          kwaque::observability::event_name_is_valid(
            texts[index].value, event_text_bytes_max));
        BOOST_CHECK(
          kwaque::observability::descriptor_for(texts[index].id)
          == &texts[index]);
        BOOST_CHECK(texts[index].value.find('/') == std::string_view::npos);
        BOOST_CHECK(texts[index].value.find('\\') == std::string_view::npos);
        BOOST_CHECK(texts[index].value.find("host") == std::string_view::npos);
        BOOST_CHECK(texts[index].value.find("seed") == std::string_view::npos);
        BOOST_CHECK(
          texts[index].value.find("credential") == std::string_view::npos);
        BOOST_CHECK(texts[index].value.find("token") == std::string_view::npos);
        BOOST_CHECK(
          texts[index].value.find("secret") == std::string_view::npos);
        BOOST_CHECK(
          texts[index].value.find("password") == std::string_view::npos);
    }
    BOOST_CHECK(
      kwaque::observability::descriptor_for(static_cast<event_public_text>(0))
      == nullptr);
    BOOST_CHECK(
      !kwaque::observability::event_reason_for(kwaque::errc::success));
    for (std::uint8_t value = 1; value <= 23U; ++value) {
        BOOST_CHECK(
          kwaque::observability::event_reason_for(
            static_cast<kwaque::errc>(value)));
    }
    co_return;
}

SEASTAR_TEST_CASE(event_construction_is_canonical_and_preserves_typed_values) {
    const auto stable = event_stable_id::make(42);
    BOOST_REQUIRE(stable.has_value());
    const std::array input{
      event_field{
        .key = event_field_key::enabled,
        .value = event_field_value::from_boolean(true)},
      event_field{
        .key = event_field_key::delta,
        .value = event_field_value::from_signed(-7)},
      event_field{
        .key = event_field_key::stable_id,
        .value = event_field_value::from_stable_id(*stable)},
      event_field{
        .key = event_field_key::occurrence,
        .value = event_field_value::from_unsigned(4'096)},
      event_field{
        .key = event_field_key::outcome,
        .value = event_field_value::from_text(
          text(event_public_text::outcome_applied))},
    };
    const auto event_context = context(event_kind::fault_decision);
    const auto made = event_request::make(event_context, input);
    BOOST_REQUIRE(made.has_value());
    BOOST_CHECK(made->schema_version() == event_schema_version);
    BOOST_CHECK(made->kind() == event_context.kind);
    BOOST_CHECK(made->name() == "fault_decision");
    BOOST_CHECK(made->severity() == event_context.severity);
    BOOST_CHECK(made->monotonic() == event_context.monotonic);
    BOOST_CHECK(made->wall() == event_context.wall);
    BOOST_CHECK(made->workload() == event_context.workload);

    const auto fields = made->fields();
    BOOST_REQUIRE(fields.size() == input.size());
    const std::array expected_keys{
      event_field_key::outcome,
      event_field_key::stable_id,
      event_field_key::occurrence,
      event_field_key::delta,
      event_field_key::enabled,
    };
    for (std::size_t index = 0; index < fields.size(); ++index) {
        BOOST_CHECK(fields[index].key == expected_keys[index]);
    }
    BOOST_REQUIRE(fields[0].value.as_text().has_value());
    BOOST_CHECK(*fields[0].value.as_text() == "applied");
    BOOST_REQUIRE(fields[1].value.as_stable_id().has_value());
    BOOST_CHECK(fields[1].value.as_stable_id()->value() == 42U);
    BOOST_REQUIRE(fields[2].value.as_unsigned().has_value());
    BOOST_CHECK(*fields[2].value.as_unsigned() == 4'096U);
    BOOST_REQUIRE(fields[3].value.as_signed().has_value());
    BOOST_CHECK(*fields[3].value.as_signed() == -7);
    BOOST_REQUIRE(fields[4].value.as_boolean().has_value());
    BOOST_CHECK(*fields[4].value.as_boolean());
    BOOST_CHECK(!fields[4].value.as_unsigned().has_value());

    std::size_t expected_size = canonical_event_fixed_encoded_size
                                + made->name().size();
    for (const auto& field : fields) {
        const event_field_descriptor* descriptor
          = kwaque::observability::descriptor_for(field.key);
        BOOST_REQUIRE(descriptor != nullptr);
        expected_size += canonical_event_field_fixed_encoded_size
                         + descriptor->name.size()
                         + field.value.encoded_payload_size();
    }
    BOOST_CHECK(made->encoded_size() == expected_size);
    BOOST_CHECK(made->encoded_size() <= event_encoded_bytes_max);

    const auto empty = event_request::make(
      context(event_kind::fault_decision), {});
    BOOST_REQUIRE(empty.has_value());
    BOOST_CHECK(empty->fields().empty());
    BOOST_CHECK(
      empty->encoded_size()
      == canonical_event_fixed_encoded_size + empty->name().size());
    co_return;
}

SEASTAR_TEST_CASE(event_construction_rejects_every_bounded_boundary) {
    const auto ready = event_text::make(event_public_text::state_ready);
    BOOST_REQUIRE(ready.has_value());
    BOOST_CHECK(ready->value() == "ready");
    const auto invalid_text = event_text::make(
      static_cast<event_public_text>(0));
    BOOST_REQUIRE(!invalid_text.has_value());
    BOOST_CHECK(invalid_text.error().code() == kwaque::errc::invalid_argument);
    BOOST_CHECK(
      invalid_text.error().operation()
      == kwaque::runtime::operation_kind::observability);
    BOOST_CHECK((kwaque::observability::make_event_field<
                   event_kind::runtime_state_changed,
                   event_field_key::state>(event_public_text::state_ready)
                   .has_value()));
    BOOST_CHECK((!kwaque::observability::make_event_field<
                    event_kind::runtime_state_changed,
                    event_field_key::state>(event_public_text::outcome_accepted)
                    .has_value()));

    BOOST_CHECK(!event_stable_id::make(0).has_value());
    const auto max_stable = event_stable_id::make(
      std::numeric_limits<std::uint64_t>::max());
    BOOST_REQUIRE(max_stable.has_value());
    BOOST_CHECK(
      kwaque::observability::validate_event_encoded_size(
        event_encoded_bytes_max)
        .has_value());
    BOOST_CHECK(
      !kwaque::observability::validate_event_encoded_size(0).has_value());
    const auto excessive_size
      = kwaque::observability::validate_event_encoded_size(
        event_encoded_bytes_max + 1U);
    BOOST_REQUIRE(!excessive_size.has_value());
    BOOST_CHECK(excessive_size.error().code() == kwaque::errc::out_of_range);

    auto invalid_context = context();
    invalid_context.kind = static_cast<event_kind>(0);
    BOOST_CHECK(!event_request::make(invalid_context, {}).has_value());
    invalid_context = context();
    invalid_context.severity = static_cast<event_severity>(0);
    BOOST_CHECK(!event_request::make(invalid_context, {}).has_value());
    invalid_context = context();
    invalid_context.workload = static_cast<kwaque::resource::workload_class>(
      255);
    BOOST_CHECK(!event_request::make(invalid_context, {}).has_value());
    const std::array exact_field_count{
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
        .value = event_field_value::from_unsigned(
          std::numeric_limits<std::uint64_t>::max())},
      event_field{
        .key = event_field_key::stable_id,
        .value = event_field_value::from_stable_id(*max_stable)},
      event_field{
        .key = event_field_key::occurrence,
        .value = event_field_value::from_unsigned(
          std::numeric_limits<std::uint64_t>::max())},
      event_field{
        .key = event_field_key::delta,
        .value = event_field_value::from_signed(
          std::numeric_limits<std::int64_t>::min())},
      event_field{
        .key = event_field_key::enabled,
        .value = event_field_value::from_boolean(true)},
    };
    const auto exact_count = event_request::make(
      context(event_kind::fault_decision), exact_field_count);
    BOOST_REQUIRE(exact_count.has_value());
    BOOST_CHECK(exact_count->fields().size() == event_fields_max);

    const std::array too_many_fields{
      event_field{},
      event_field{},
      event_field{},
      event_field{},
      event_field{},
      event_field{},
      event_field{},
      event_field{},
      event_field{},
    };
    const auto too_many = event_request::make(context(), too_many_fields);
    BOOST_REQUIRE(!too_many.has_value());
    BOOST_CHECK(too_many.error().code() == kwaque::errc::out_of_range);

    const std::array invalid_key{
      event_field{
        .key = static_cast<event_field_key>(0),
        .value = event_field_value::from_unsigned(1)},
    };
    BOOST_CHECK(!event_request::make(context(), invalid_key).has_value());
    const std::array wrong_type{
      event_field{
        .key = event_field_key::state,
        .value = event_field_value::from_unsigned(1)},
    };
    BOOST_CHECK(!event_request::make(context(), wrong_type).has_value());
    const std::array unsupported_key{
      event_field{
        .key = event_field_key::bytes,
        .value = event_field_value::from_unsigned(1)},
    };
    BOOST_CHECK(!event_request::make(context(), unsupported_key).has_value());
    const std::array wrong_text_role{
      event_field{
        .key = event_field_key::state,
        .value = event_field_value::from_text(
          text(event_public_text::outcome_accepted))},
    };
    BOOST_CHECK(!event_request::make(context(), wrong_text_role).has_value());
    const std::array empty_text{
      event_field{
        .key = event_field_key::state,
        .value = event_field_value::from_text(event_text{})},
    };
    BOOST_CHECK(!event_request::make(context(), empty_text).has_value());
    const std::array duplicate{
      event_field{
        .key = event_field_key::state,
        .value = event_field_value::from_text(
          text(event_public_text::state_ready))},
      event_field{
        .key = event_field_key::state,
        .value = event_field_value::from_text(
          text(event_public_text::state_stopped))},
    };
    BOOST_CHECK(!event_request::make(context(), duplicate).has_value());
    co_return;
}

SEASTAR_TEST_CASE(event_construction_allocates_nothing) {
    const auto event_context = context();
    bool every_attempt_succeeded = true;
    std::size_t attempts = 0;
    seastar::memory::with_allocation_failures([&] {
        ++attempts;
        const auto state = event_text::make(event_public_text::state_ready);
        const auto operation = event_text::make(
          event_public_text::operation_environment_start);
        if (!state || !operation) {
            every_attempt_succeeded = false;
            return;
        }
        const std::array fields{
          event_field{
            .key = event_field_key::state,
            .value = event_field_value::from_text(*state)},
          event_field{
            .key = event_field_key::operation,
            .value = event_field_value::from_text(*operation)},
        };
        every_attempt_succeeded
          = every_attempt_succeeded
            && event_request::make(event_context, fields).has_value();
    });
    BOOST_CHECK(attempts == 1U);
    BOOST_CHECK(every_attempt_succeeded);
    co_return;
}
