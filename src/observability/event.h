#ifndef KWAQUE_SRC_OBSERVABILITY_EVENT_H_
#define KWAQUE_SRC_OBSERVABILITY_EVENT_H_

#include "src/resource/workload_class.h"
#include "src/runtime/error.h"
#include "src/runtime/shard_affinity.h"
#include "src/runtime/time.h"

#include <array>
#include <bit>
#include <compare>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string_view>
#include <utility>

namespace kwaque::observability {

inline constexpr std::uint32_t event_schema_version{1};
inline constexpr std::size_t event_fields_max{8};
inline constexpr std::size_t event_name_bytes_max{48};
inline constexpr std::size_t event_field_name_bytes_max{32};
inline constexpr std::size_t event_text_bytes_max{96};
inline constexpr std::size_t event_encoded_bytes_max{1'024};
inline constexpr std::size_t canonical_event_fixed_encoded_size{38};
inline constexpr std::size_t canonical_event_field_fixed_encoded_size{6};

enum class event_kind : std::uint16_t {
    runtime_state_changed = 1,
    resource_group_state_changed = 2,
    queue_admission = 3,
    timer_completion = 4,
    file_completion = 5,
    network_delivery = 6,
    dns_completion = 7,
    fault_decision = 8,
};

enum class event_severity : std::uint8_t {
    trace = 1,
    debug = 2,
    info = 3,
    warning = 4,
    error = 5,
};

enum class event_field_type : std::uint8_t {
    signed_integer = 1,
    unsigned_integer = 2,
    boolean = 3,
    bounded_string = 4,
    stable_id = 5,
};

enum class event_field_key : std::uint16_t {
    state = 1,
    outcome = 2,
    operation = 3,
    reason = 4,
    bytes = 5,
    items = 6,
    duration_ns = 7,
    stable_id = 8,
    occurrence = 9,
    limit = 10,
    expected = 11,
    actual = 12,
    delta = 13,
    enabled = 14,
};

[[nodiscard]] constexpr std::optional<event_field_type>
event_field_type_for(event_field_key key) noexcept {
    switch (key) {
    case event_field_key::state:
    case event_field_key::outcome:
    case event_field_key::operation:
    case event_field_key::reason:
        return event_field_type::bounded_string;
    case event_field_key::bytes:
    case event_field_key::items:
    case event_field_key::duration_ns:
    case event_field_key::occurrence:
    case event_field_key::limit:
    case event_field_key::expected:
    case event_field_key::actual:
        return event_field_type::unsigned_integer;
    case event_field_key::stable_id:
        return event_field_type::stable_id;
    case event_field_key::delta:
        return event_field_type::signed_integer;
    case event_field_key::enabled:
        return event_field_type::boolean;
    }
    return std::nullopt;
}

using event_field_mask = std::uint16_t;

[[nodiscard]] constexpr event_field_mask
event_field_bit(event_field_key key) noexcept {
    const auto value = static_cast<std::uint16_t>(key);
    return value == 0 || value > 16U ? event_field_mask{0}
                                     : static_cast<event_field_mask>(
                                         event_field_mask{1} << (value - 1U));
}

template<typename... Keys>
[[nodiscard]] consteval event_field_mask event_field_mask_of(Keys... keys) {
    return static_cast<event_field_mask>(
      (event_field_mask{0} | ... | event_field_bit(keys)));
}

[[nodiscard]] constexpr event_field_mask
allowed_event_fields(event_kind kind) noexcept {
    switch (kind) {
    case event_kind::runtime_state_changed:
        return event_field_mask_of(
          event_field_key::state,
          event_field_key::operation,
          event_field_key::reason);
    case event_kind::resource_group_state_changed:
        return event_field_mask_of(
          event_field_key::state,
          event_field_key::operation,
          event_field_key::reason,
          event_field_key::items,
          event_field_key::limit);
    case event_kind::queue_admission:
        return event_field_mask_of(
          event_field_key::outcome,
          event_field_key::operation,
          event_field_key::reason,
          event_field_key::bytes,
          event_field_key::items,
          event_field_key::duration_ns,
          event_field_key::limit);
    case event_kind::timer_completion:
        return event_field_mask_of(
          event_field_key::outcome,
          event_field_key::operation,
          event_field_key::reason,
          event_field_key::duration_ns,
          event_field_key::stable_id);
    case event_kind::file_completion:
    case event_kind::network_delivery:
        return event_field_mask_of(
          event_field_key::outcome,
          event_field_key::operation,
          event_field_key::reason,
          event_field_key::bytes,
          event_field_key::duration_ns,
          event_field_key::stable_id);
    case event_kind::dns_completion:
        return event_field_mask_of(
          event_field_key::outcome,
          event_field_key::operation,
          event_field_key::reason,
          event_field_key::items,
          event_field_key::duration_ns,
          event_field_key::stable_id);
    case event_kind::fault_decision:
        return event_field_mask_of(
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
          event_field_key::enabled);
    }
    return 0;
}

[[nodiscard]] constexpr bool
event_field_is_allowed(event_kind kind, event_field_key key) noexcept {
    const auto bit = event_field_bit(key);
    return bit != 0 && (allowed_event_fields(kind) & bit) != 0;
}

enum class event_public_text : std::uint16_t {
    state_starting = 1,
    state_ready = 2,
    state_stopping = 3,
    state_stopped = 4,
    state_failed = 5,
    outcome_accepted = 6,
    outcome_completed = 7,
    outcome_rejected = 8,
    outcome_failed = 9,
    outcome_aborted = 10,
    outcome_dropped = 11,
    outcome_delayed = 12,
    outcome_applied = 13,
    outcome_skipped = 14,
    operation_environment_start = 15,
    operation_environment_stop = 16,
    operation_resource_group_create = 17,
    operation_queue_admission = 18,
    operation_timer_wait = 19,
    operation_file_open = 20,
    operation_file_read = 21,
    operation_file_write = 22,
    operation_file_flush = 23,
    operation_file_truncate = 24,
    operation_file_close = 25,
    operation_network_bind = 26,
    operation_network_connect = 27,
    operation_network_accept = 28,
    operation_network_read = 29,
    operation_network_write = 30,
    operation_network_close = 31,
    operation_network_delivery = 32,
    operation_dns_resolve = 33,
    operation_fault_evaluate = 34,
    reason_invalid_argument = 35,
    reason_out_of_range = 36,
    reason_malformed_data = 37,
    reason_unavailable = 38,
    reason_aborted = 39,
    reason_closed = 40,
    reason_timed_out = 41,
    reason_resource_exhausted = 42,
    reason_queue_full = 43,
    reason_wrong_shard = 44,
    reason_io_failure = 45,
    reason_network_failure = 46,
    reason_dns_failure = 47,
    reason_fault_injected = 48,
    reason_replay_divergence = 49,
    reason_invariant_violation = 50,
    reason_truncated_data = 51,
    reason_not_found = 52,
    reason_already_exists = 53,
    reason_permission_denied = 54,
    reason_directory_not_empty = 55,
    reason_is_a_directory = 56,
    reason_not_a_directory = 57,
};

struct event_text_descriptor final {
    event_public_text id;
    event_field_key role;
    std::string_view value;

    bool operator==(const event_text_descriptor&) const = default;
};

struct event_descriptor final {
    event_kind kind;
    std::string_view name;
    event_field_mask allowed_fields;

    bool operator==(const event_descriptor&) const = default;
};

struct event_field_descriptor final {
    event_field_key key;
    std::string_view name;
    event_field_type type;

    bool operator==(const event_field_descriptor&) const = default;
};

[[nodiscard]] constexpr bool
event_name_is_valid(std::string_view name, std::size_t maximum_bytes) noexcept {
    if (name.empty() || name.size() > maximum_bytes) {
        return false;
    }
    for (std::size_t index = 0; index < name.size(); ++index) {
        const char character = name[index];
        const bool lower = character >= 'a' && character <= 'z';
        const bool digit = character >= '0' && character <= '9';
        if ((!lower && !digit && character != '_') || (index == 0 && !lower)) {
            return false;
        }
    }
    return true;
}

[[nodiscard]] const event_descriptor* descriptor_for(event_kind kind) noexcept;
[[nodiscard]] const event_field_descriptor*
descriptor_for(event_field_key key) noexcept;
[[nodiscard]] std::span<const event_descriptor> event_descriptors() noexcept;
[[nodiscard]] std::span<const event_field_descriptor>
event_field_descriptors() noexcept;
[[nodiscard]] const event_text_descriptor*
descriptor_for(event_public_text text) noexcept;
[[nodiscard]] std::span<const event_text_descriptor>
event_text_descriptors() noexcept;
[[nodiscard]] std::optional<event_public_text>
event_reason_for(errc reason) noexcept;

struct event_codec_access;

class event_text final {
public:
    constexpr event_text() noexcept = default;

    [[nodiscard]] static runtime::result<event_text>
    make(event_public_text value) noexcept;

    [[nodiscard]] constexpr std::string_view value() const noexcept {
        return {storage_.data(), size_};
    }
    [[nodiscard]] constexpr bool valid() const noexcept { return size_ != 0; }

    bool operator==(const event_text&) const = default;

private:
    friend class event_field_value;
    friend struct event_codec_access;

    [[nodiscard]] static runtime::result<event_text>
    decode(event_field_key role, std::string_view value) noexcept;

    std::array<char, event_text_bytes_max> storage_{};
    event_field_key role_{static_cast<event_field_key>(0)};
    std::uint8_t size_{0};
};

class event_stable_id final {
public:
    [[nodiscard]] static runtime::result<event_stable_id>
    make(std::uint64_t value) noexcept;

    [[nodiscard]] constexpr std::uint64_t value() const noexcept {
        return value_;
    }

    auto operator<=>(const event_stable_id&) const = default;

private:
    friend class event_field_value;

    constexpr explicit event_stable_id(std::uint64_t value) noexcept
      : value_(value) {}

    std::uint64_t value_;
};

class event_field_value final {
public:
    constexpr event_field_value() noexcept = default;

    [[nodiscard]] static constexpr event_field_value
    from_signed(std::int64_t value) noexcept {
        return event_field_value{
          event_field_type::signed_integer,
          std::bit_cast<std::uint64_t>(value)};
    }
    [[nodiscard]] static constexpr event_field_value
    from_unsigned(std::uint64_t value) noexcept {
        return event_field_value{event_field_type::unsigned_integer, value};
    }
    [[nodiscard]] static constexpr event_field_value
    from_boolean(bool value) noexcept {
        return event_field_value{
          event_field_type::boolean, static_cast<std::uint64_t>(value)};
    }
    [[nodiscard]] static constexpr event_field_value
    from_text(event_text value) noexcept {
        return event_field_value{std::move(value)};
    }
    [[nodiscard]] static constexpr event_field_value
    from_stable_id(event_stable_id value) noexcept {
        return event_field_value{event_field_type::stable_id, value.value()};
    }

    [[nodiscard]] constexpr event_field_type type() const noexcept {
        return type_;
    }
    [[nodiscard]] constexpr std::size_t encoded_payload_size() const noexcept {
        switch (type_) {
        case event_field_type::boolean:
            return 1;
        case event_field_type::bounded_string:
            return text_.value().size();
        case event_field_type::signed_integer:
        case event_field_type::unsigned_integer:
        case event_field_type::stable_id:
            return sizeof(std::uint64_t);
        }
        return 0;
    }
    [[nodiscard]] constexpr std::optional<std::int64_t>
    as_signed() const noexcept {
        return type_ == event_field_type::signed_integer
                 ? std::optional{std::bit_cast<std::int64_t>(numeric_)}
                 : std::nullopt;
    }
    [[nodiscard]] constexpr std::optional<std::uint64_t>
    as_unsigned() const noexcept {
        return type_ == event_field_type::unsigned_integer
                 ? std::optional{numeric_}
                 : std::nullopt;
    }
    [[nodiscard]] constexpr std::optional<bool> as_boolean() const noexcept {
        return type_ == event_field_type::boolean ? std::optional{numeric_ != 0}
                                                  : std::nullopt;
    }
    [[nodiscard]] constexpr std::optional<std::string_view>
    as_text() const noexcept {
        return type_ == event_field_type::bounded_string
                 ? std::optional{text_.value()}
                 : std::nullopt;
    }
    [[nodiscard]] constexpr std::optional<event_field_key>
    text_role() const noexcept {
        return type_ == event_field_type::bounded_string
                 ? std::optional{text_.role_}
                 : std::nullopt;
    }
    [[nodiscard]] constexpr std::optional<event_stable_id>
    as_stable_id() const noexcept {
        return type_ == event_field_type::stable_id
                 ? std::optional{event_stable_id{numeric_}}
                 : std::nullopt;
    }

    bool operator==(const event_field_value&) const = default;

private:
    constexpr event_field_value(
      event_field_type type, std::uint64_t numeric) noexcept
      : type_(type)
      , numeric_(numeric) {}
    constexpr explicit event_field_value(event_text text) noexcept
      : type_(event_field_type::bounded_string)
      , text_(std::move(text)) {}

    event_field_type type_{event_field_type::signed_integer};
    std::uint64_t numeric_{0};
    event_text text_{};
};

struct event_field final {
    event_field_key key{static_cast<event_field_key>(0)};
    event_field_value value{};

    bool operator==(const event_field&) const = default;
};

template<event_kind Kind, event_field_key Key>
requires(
  event_field_is_allowed(Kind, Key)
  && event_field_type_for(Key) == event_field_type::signed_integer)
[[nodiscard]] constexpr event_field
make_event_field(std::int64_t value) noexcept {
    return event_field{
      .key = Key, .value = event_field_value::from_signed(value)};
}

template<event_kind Kind, event_field_key Key>
requires(
  event_field_is_allowed(Kind, Key)
  && event_field_type_for(Key) == event_field_type::unsigned_integer)
[[nodiscard]] constexpr event_field
make_event_field(std::uint64_t value) noexcept {
    return event_field{
      .key = Key, .value = event_field_value::from_unsigned(value)};
}

template<event_kind Kind, event_field_key Key>
requires(
  event_field_is_allowed(Kind, Key)
  && event_field_type_for(Key) == event_field_type::boolean)
[[nodiscard]] constexpr event_field make_event_field(bool value) noexcept {
    return event_field{
      .key = Key, .value = event_field_value::from_boolean(value)};
}

template<event_kind Kind, event_field_key Key>
requires(
  event_field_is_allowed(Kind, Key)
  && event_field_type_for(Key) == event_field_type::stable_id)
[[nodiscard]] constexpr event_field
make_event_field(event_stable_id value) noexcept {
    return event_field{
      .key = Key, .value = event_field_value::from_stable_id(value)};
}

template<event_kind Kind, event_field_key Key>
requires(
  event_field_is_allowed(Kind, Key)
  && event_field_type_for(Key) == event_field_type::bounded_string)
[[nodiscard]] runtime::result<event_field>
make_event_field(event_public_text value) noexcept {
    const auto* descriptor = descriptor_for(value);
    if (descriptor == nullptr || descriptor->role != Key) {
        return runtime::failure(
          runtime::operation_error{
            errc::invalid_argument, runtime::operation_kind::observability});
    }
    auto text = event_text::make(value);
    if (!text) {
        return runtime::failure(text.error());
    }
    return event_field{
      .key = Key, .value = event_field_value::from_text(std::move(*text))};
}

class event_shard final {
public:
    [[nodiscard]] constexpr std::uint32_t value() const noexcept {
        return value_;
    }

    auto operator<=>(const event_shard&) const = default;

private:
    friend struct event_codec_access;
    friend class event_sequence;

    constexpr explicit event_shard(std::uint32_t value) noexcept
      : value_(value) {}
    [[nodiscard]] static constexpr event_shard
    from_owner(runtime::owner_shard owner) noexcept {
        return event_shard{static_cast<std::uint32_t>(owner.value())};
    }

    std::uint32_t value_;
};

struct event_request_context final {
    event_kind kind;
    event_severity severity;
    runtime::monotonic_time monotonic;
    runtime::wall_time wall;
    resource::workload_class workload;

    bool operator==(const event_request_context&) const = default;
};

[[nodiscard]] runtime::result<void>
validate_event_encoded_size(std::size_t bytes) noexcept;

class event_request;
class event_sequence;

class event final {
public:
    event(const event&) = default;
    event& operator=(const event&) = default;
    event(event&&) noexcept = default;
    event& operator=(event&&) noexcept = default;

    [[nodiscard]] constexpr std::uint32_t schema_version() const noexcept {
        return event_schema_version;
    }
    [[nodiscard]] constexpr event_kind kind() const noexcept { return kind_; }
    [[nodiscard]] std::string_view name() const noexcept;
    [[nodiscard]] constexpr event_severity severity() const noexcept {
        return severity_;
    }
    [[nodiscard]] constexpr runtime::monotonic_time monotonic() const noexcept {
        return monotonic_;
    }
    [[nodiscard]] constexpr runtime::wall_time wall() const noexcept {
        return wall_;
    }
    [[nodiscard]] constexpr event_shard shard() const noexcept {
        return shard_;
    }
    [[nodiscard]] constexpr resource::workload_class workload() const noexcept {
        return workload_;
    }
    [[nodiscard]] constexpr std::uint64_t sequence() const noexcept {
        return sequence_;
    }
    [[nodiscard]] constexpr std::span<const event_field>
    fields() const noexcept {
        return {fields_.data(), field_count_};
    }
    [[nodiscard]] constexpr std::size_t encoded_size() const noexcept {
        return encoded_size_;
    }

    bool operator==(const event&) const = default;

private:
    friend struct event_codec_access;
    friend class event_sequence;

    [[nodiscard]] static runtime::result<event> make(
      const event_request_context& context,
      event_shard shard,
      std::uint64_t sequence,
      std::span<const event_field> fields) noexcept;
    [[nodiscard]] static event from_request(
      const event_request& request,
      event_shard shard,
      std::uint64_t sequence) noexcept;

    event(
      const event_request_context& context,
      event_shard shard,
      std::uint64_t sequence) noexcept;

    event_kind kind_;
    event_severity severity_;
    runtime::monotonic_time monotonic_;
    runtime::wall_time wall_;
    event_shard shard_;
    resource::workload_class workload_;
    std::uint64_t sequence_;
    std::array<event_field, event_fields_max> fields_{};
    std::uint16_t encoded_size_{0};
    std::uint8_t field_count_{0};
};

class event_request final {
public:
    [[nodiscard]] static runtime::result<event_request> make(
      const event_request_context& context,
      std::span<const event_field> fields) noexcept;

    event_request(const event_request&) = default;
    event_request& operator=(const event_request&) = default;
    event_request(event_request&&) noexcept = default;
    event_request& operator=(event_request&&) noexcept = default;

    [[nodiscard]] constexpr std::uint32_t schema_version() const noexcept {
        return event_schema_version;
    }
    [[nodiscard]] constexpr event_kind kind() const noexcept {
        return context_.kind;
    }
    [[nodiscard]] std::string_view name() const noexcept;
    [[nodiscard]] constexpr event_severity severity() const noexcept {
        return context_.severity;
    }
    [[nodiscard]] constexpr runtime::monotonic_time monotonic() const noexcept {
        return context_.monotonic;
    }
    [[nodiscard]] constexpr runtime::wall_time wall() const noexcept {
        return context_.wall;
    }
    [[nodiscard]] constexpr resource::workload_class workload() const noexcept {
        return context_.workload;
    }
    [[nodiscard]] constexpr std::span<const event_field>
    fields() const noexcept {
        return {fields_.data(), field_count_};
    }
    [[nodiscard]] constexpr std::size_t encoded_size() const noexcept {
        return encoded_size_;
    }

    bool operator==(const event_request&) const = default;

private:
    friend class event;

    explicit event_request(const event_request_context& context) noexcept
      : context_(context) {}

    event_request_context context_;
    std::array<event_field, event_fields_max> fields_{};
    std::uint16_t encoded_size_{0};
    std::uint8_t field_count_{0};
};

} // namespace kwaque::observability

#endif // KWAQUE_SRC_OBSERVABILITY_EVENT_H_
