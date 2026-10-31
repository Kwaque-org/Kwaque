#include "src/observability/event.h"

#include "src/base/invariant.h"

#include <algorithm>
#include <array>

namespace kwaque::observability {

namespace {

constexpr std::array descriptors{
  event_descriptor{
    .kind = event_kind::runtime_state_changed,
    .name = "runtime_state_changed",
    .allowed_fields = allowed_event_fields(event_kind::runtime_state_changed),
  },
  event_descriptor{
    .kind = event_kind::resource_group_state_changed,
    .name = "resource_group_state_changed",
    .allowed_fields = allowed_event_fields(
      event_kind::resource_group_state_changed),
  },
  event_descriptor{
    .kind = event_kind::queue_admission,
    .name = "queue_admission",
    .allowed_fields = allowed_event_fields(event_kind::queue_admission),
  },
  event_descriptor{
    .kind = event_kind::timer_completion,
    .name = "timer_completion",
    .allowed_fields = allowed_event_fields(event_kind::timer_completion),
  },
  event_descriptor{
    .kind = event_kind::file_completion,
    .name = "file_completion",
    .allowed_fields = allowed_event_fields(event_kind::file_completion),
  },
  event_descriptor{
    .kind = event_kind::network_delivery,
    .name = "network_delivery",
    .allowed_fields = allowed_event_fields(event_kind::network_delivery),
  },
  event_descriptor{
    .kind = event_kind::dns_completion,
    .name = "dns_completion",
    .allowed_fields = allowed_event_fields(event_kind::dns_completion),
  },
  event_descriptor{
    .kind = event_kind::fault_decision,
    .name = "fault_decision",
    .allowed_fields = allowed_event_fields(event_kind::fault_decision),
  },
  event_descriptor{
    .kind = event_kind::readiness_changed,
    .name = "readiness_changed",
    .allowed_fields = allowed_event_fields(event_kind::readiness_changed),
  },
  event_descriptor{
    .kind = event_kind::append_refused,
    .name = "append_refused",
    .allowed_fields = allowed_event_fields(event_kind::append_refused),
  },
};

constexpr std::array field_descriptors{
  event_field_descriptor{
    .key = event_field_key::state,
    .name = "state",
    .type = event_field_type::bounded_string,
  },
  event_field_descriptor{
    .key = event_field_key::outcome,
    .name = "outcome",
    .type = event_field_type::bounded_string,
  },
  event_field_descriptor{
    .key = event_field_key::operation,
    .name = "operation",
    .type = event_field_type::bounded_string,
  },
  event_field_descriptor{
    .key = event_field_key::reason,
    .name = "reason",
    .type = event_field_type::bounded_string,
  },
  event_field_descriptor{
    .key = event_field_key::bytes,
    .name = "bytes",
    .type = event_field_type::unsigned_integer,
  },
  event_field_descriptor{
    .key = event_field_key::items,
    .name = "items",
    .type = event_field_type::unsigned_integer,
  },
  event_field_descriptor{
    .key = event_field_key::duration_ns,
    .name = "duration_ns",
    .type = event_field_type::unsigned_integer,
  },
  event_field_descriptor{
    .key = event_field_key::stable_id,
    .name = "stable_id",
    .type = event_field_type::stable_id,
  },
  event_field_descriptor{
    .key = event_field_key::occurrence,
    .name = "occurrence",
    .type = event_field_type::unsigned_integer,
  },
  event_field_descriptor{
    .key = event_field_key::limit,
    .name = "limit",
    .type = event_field_type::unsigned_integer,
  },
  event_field_descriptor{
    .key = event_field_key::expected,
    .name = "expected",
    .type = event_field_type::unsigned_integer,
  },
  event_field_descriptor{
    .key = event_field_key::actual,
    .name = "actual",
    .type = event_field_type::unsigned_integer,
  },
  event_field_descriptor{
    .key = event_field_key::delta,
    .name = "delta",
    .type = event_field_type::signed_integer,
  },
  event_field_descriptor{
    .key = event_field_key::enabled,
    .name = "enabled",
    .type = event_field_type::boolean,
  },
  event_field_descriptor{
    .key = event_field_key::suppressed,
    .name = "suppressed",
    .type = event_field_type::unsigned_integer,
  },
  event_field_descriptor{
    .key = event_field_key::topic_id,
    .name = "topic_id",
    .type = event_field_type::object_id,
  },
  event_field_descriptor{
    .key = event_field_key::range_id,
    .name = "range_id",
    .type = event_field_type::object_id,
  },
  event_field_descriptor{
    .key = event_field_key::reasons,
    .name = "reasons",
    .type = event_field_type::unsigned_integer,
  },
};

constexpr std::array text_descriptors{
  event_text_descriptor{
    .id = event_public_text::state_starting,
    .role = event_field_key::state,
    .value = "starting"},
  event_text_descriptor{
    .id = event_public_text::state_ready,
    .role = event_field_key::state,
    .value = "ready"},
  event_text_descriptor{
    .id = event_public_text::state_stopping,
    .role = event_field_key::state,
    .value = "stopping"},
  event_text_descriptor{
    .id = event_public_text::state_stopped,
    .role = event_field_key::state,
    .value = "stopped"},
  event_text_descriptor{
    .id = event_public_text::state_failed,
    .role = event_field_key::state,
    .value = "failed"},
  event_text_descriptor{
    .id = event_public_text::outcome_accepted,
    .role = event_field_key::outcome,
    .value = "accepted"},
  event_text_descriptor{
    .id = event_public_text::outcome_completed,
    .role = event_field_key::outcome,
    .value = "completed"},
  event_text_descriptor{
    .id = event_public_text::outcome_rejected,
    .role = event_field_key::outcome,
    .value = "rejected"},
  event_text_descriptor{
    .id = event_public_text::outcome_failed,
    .role = event_field_key::outcome,
    .value = "failed"},
  event_text_descriptor{
    .id = event_public_text::outcome_aborted,
    .role = event_field_key::outcome,
    .value = "aborted"},
  event_text_descriptor{
    .id = event_public_text::outcome_dropped,
    .role = event_field_key::outcome,
    .value = "dropped"},
  event_text_descriptor{
    .id = event_public_text::outcome_delayed,
    .role = event_field_key::outcome,
    .value = "delayed"},
  event_text_descriptor{
    .id = event_public_text::outcome_applied,
    .role = event_field_key::outcome,
    .value = "applied"},
  event_text_descriptor{
    .id = event_public_text::outcome_skipped,
    .role = event_field_key::outcome,
    .value = "skipped"},
  event_text_descriptor{
    .id = event_public_text::operation_environment_start,
    .role = event_field_key::operation,
    .value = "environment_start"},
  event_text_descriptor{
    .id = event_public_text::operation_environment_stop,
    .role = event_field_key::operation,
    .value = "environment_stop"},
  event_text_descriptor{
    .id = event_public_text::operation_resource_group_create,
    .role = event_field_key::operation,
    .value = "resource_group_create"},
  event_text_descriptor{
    .id = event_public_text::operation_queue_admission,
    .role = event_field_key::operation,
    .value = "queue_admission"},
  event_text_descriptor{
    .id = event_public_text::operation_timer_wait,
    .role = event_field_key::operation,
    .value = "timer_wait"},
  event_text_descriptor{
    .id = event_public_text::operation_file_open,
    .role = event_field_key::operation,
    .value = "file_open"},
  event_text_descriptor{
    .id = event_public_text::operation_file_read,
    .role = event_field_key::operation,
    .value = "file_read"},
  event_text_descriptor{
    .id = event_public_text::operation_file_write,
    .role = event_field_key::operation,
    .value = "file_write"},
  event_text_descriptor{
    .id = event_public_text::operation_file_flush,
    .role = event_field_key::operation,
    .value = "file_flush"},
  event_text_descriptor{
    .id = event_public_text::operation_file_truncate,
    .role = event_field_key::operation,
    .value = "file_truncate"},
  event_text_descriptor{
    .id = event_public_text::operation_file_close,
    .role = event_field_key::operation,
    .value = "file_close"},
  event_text_descriptor{
    .id = event_public_text::operation_network_bind,
    .role = event_field_key::operation,
    .value = "network_bind"},
  event_text_descriptor{
    .id = event_public_text::operation_network_connect,
    .role = event_field_key::operation,
    .value = "network_connect"},
  event_text_descriptor{
    .id = event_public_text::operation_network_accept,
    .role = event_field_key::operation,
    .value = "network_accept"},
  event_text_descriptor{
    .id = event_public_text::operation_network_read,
    .role = event_field_key::operation,
    .value = "network_read"},
  event_text_descriptor{
    .id = event_public_text::operation_network_write,
    .role = event_field_key::operation,
    .value = "network_write"},
  event_text_descriptor{
    .id = event_public_text::operation_network_close,
    .role = event_field_key::operation,
    .value = "network_close"},
  event_text_descriptor{
    .id = event_public_text::operation_network_delivery,
    .role = event_field_key::operation,
    .value = "network_delivery"},
  event_text_descriptor{
    .id = event_public_text::operation_dns_resolve,
    .role = event_field_key::operation,
    .value = "dns_resolve"},
  event_text_descriptor{
    .id = event_public_text::operation_fault_evaluate,
    .role = event_field_key::operation,
    .value = "fault_evaluate"},
  event_text_descriptor{
    .id = event_public_text::reason_invalid_argument,
    .role = event_field_key::reason,
    .value = "invalid_argument"},
  event_text_descriptor{
    .id = event_public_text::reason_out_of_range,
    .role = event_field_key::reason,
    .value = "out_of_range"},
  event_text_descriptor{
    .id = event_public_text::reason_malformed_data,
    .role = event_field_key::reason,
    .value = "malformed_data"},
  event_text_descriptor{
    .id = event_public_text::reason_unavailable,
    .role = event_field_key::reason,
    .value = "unavailable"},
  event_text_descriptor{
    .id = event_public_text::reason_aborted,
    .role = event_field_key::reason,
    .value = "aborted"},
  event_text_descriptor{
    .id = event_public_text::reason_closed,
    .role = event_field_key::reason,
    .value = "closed"},
  event_text_descriptor{
    .id = event_public_text::reason_timed_out,
    .role = event_field_key::reason,
    .value = "timed_out"},
  event_text_descriptor{
    .id = event_public_text::reason_resource_exhausted,
    .role = event_field_key::reason,
    .value = "resource_exhausted"},
  event_text_descriptor{
    .id = event_public_text::reason_queue_full,
    .role = event_field_key::reason,
    .value = "queue_full"},
  event_text_descriptor{
    .id = event_public_text::reason_wrong_shard,
    .role = event_field_key::reason,
    .value = "wrong_shard"},
  event_text_descriptor{
    .id = event_public_text::reason_io_failure,
    .role = event_field_key::reason,
    .value = "io_failure"},
  event_text_descriptor{
    .id = event_public_text::reason_network_failure,
    .role = event_field_key::reason,
    .value = "network_failure"},
  event_text_descriptor{
    .id = event_public_text::reason_dns_failure,
    .role = event_field_key::reason,
    .value = "dns_failure"},
  event_text_descriptor{
    .id = event_public_text::reason_fault_injected,
    .role = event_field_key::reason,
    .value = "fault_injected"},
  event_text_descriptor{
    .id = event_public_text::reason_replay_divergence,
    .role = event_field_key::reason,
    .value = "replay_divergence"},
  event_text_descriptor{
    .id = event_public_text::reason_invariant_violation,
    .role = event_field_key::reason,
    .value = "invariant_violation"},
  event_text_descriptor{
    .id = event_public_text::reason_truncated_data,
    .role = event_field_key::reason,
    .value = "truncated_data"},
  event_text_descriptor{
    .id = event_public_text::reason_not_found,
    .role = event_field_key::reason,
    .value = "not_found"},
  event_text_descriptor{
    .id = event_public_text::reason_already_exists,
    .role = event_field_key::reason,
    .value = "already_exists"},
  event_text_descriptor{
    .id = event_public_text::reason_permission_denied,
    .role = event_field_key::reason,
    .value = "permission_denied"},
  event_text_descriptor{
    .id = event_public_text::reason_directory_not_empty,
    .role = event_field_key::reason,
    .value = "directory_not_empty"},
  event_text_descriptor{
    .id = event_public_text::reason_is_a_directory,
    .role = event_field_key::reason,
    .value = "is_a_directory"},
  event_text_descriptor{
    .id = event_public_text::reason_not_a_directory,
    .role = event_field_key::reason,
    .value = "not_a_directory"},
  event_text_descriptor{
    .id = event_public_text::reason_corrupt_data,
    .role = event_field_key::reason,
    .value = "corrupt_data"},
  event_text_descriptor{
    .id = event_public_text::reason_wrong_context,
    .role = event_field_key::reason,
    .value = "wrong_context"},
  event_text_descriptor{
    .id = event_public_text::reason_unsupported_format,
    .role = event_field_key::reason,
    .value = "unsupported_format"},
  event_text_descriptor{
    .id = event_public_text::state_not_ready,
    .role = event_field_key::state,
    .value = "not_ready"},
  event_text_descriptor{
    .id = event_public_text::operation_envelope_size,
    .role = event_field_key::operation,
    .value = "envelope_size"},
  event_text_descriptor{
    .id = event_public_text::operation_ingress_memory,
    .role = event_field_key::operation,
    .value = "ingress_memory"},
  event_text_descriptor{
    .id = event_public_text::operation_stream_window,
    .role = event_field_key::operation,
    .value = "stream_window"},
  event_text_descriptor{
    .id = event_public_text::operation_range_queue,
    .role = event_field_key::operation,
    .value = "range_queue"},
  event_text_descriptor{
    .id = event_public_text::operation_shard_capacity,
    .role = event_field_key::operation,
    .value = "shard_capacity"},
  event_text_descriptor{
    .id = event_public_text::operation_forwarded_traffic,
    .role = event_field_key::operation,
    .value = "forwarded_traffic"},
  event_text_descriptor{
    .id = event_public_text::operation_storage_pressure,
    .role = event_field_key::operation,
    .value = "storage_pressure"},
  event_text_descriptor{
    .id = event_public_text::outcome_batch_id_conflict,
    .role = event_field_key::outcome,
    .value = "batch_id_conflict"},
  event_text_descriptor{
    .id = event_public_text::outcome_stale,
    .role = event_field_key::outcome,
    .value = "stale"},
  event_text_descriptor{
    .id = event_public_text::outcome_sequence_gap,
    .role = event_field_key::outcome,
    .value = "sequence_gap"},
  event_text_descriptor{
    .id = event_public_text::outcome_window_exceeded,
    .role = event_field_key::outcome,
    .value = "window_exceeded"},
  event_text_descriptor{
    .id = event_public_text::outcome_unknown_stream,
    .role = event_field_key::outcome,
    .value = "unknown_stream"},
  event_text_descriptor{
    .id = event_public_text::outcome_stream_sealed,
    .role = event_field_key::outcome,
    .value = "stream_sealed"},
  event_text_descriptor{
    .id = event_public_text::outcome_stream_failed,
    .role = event_field_key::outcome,
    .value = "stream_failed"},
  event_text_descriptor{
    .id = event_public_text::outcome_producer_fenced,
    .role = event_field_key::outcome,
    .value = "producer_fenced"},
  event_text_descriptor{
    .id = event_public_text::outcome_invalid_ack_floor,
    .role = event_field_key::outcome,
    .value = "invalid_ack_floor"},
  event_text_descriptor{
    .id = event_public_text::outcome_stream_limit,
    .role = event_field_key::outcome,
    .value = "stream_limit"},
  event_text_descriptor{
    .id = event_public_text::outcome_topic_sealed,
    .role = event_field_key::outcome,
    .value = "topic_sealed"},
  event_text_descriptor{
    .id = event_public_text::outcome_not_found,
    .role = event_field_key::outcome,
    .value = "not_found"},
  event_text_descriptor{
    .id = event_public_text::outcome_misrouted_key,
    .role = event_field_key::outcome,
    .value = "misrouted_key"},
  event_text_descriptor{
    .id = event_public_text::outcome_stale_route,
    .role = event_field_key::outcome,
    .value = "stale_route"},
  event_text_descriptor{
    .id = event_public_text::outcome_no_space,
    .role = event_field_key::outcome,
    .value = "no_space"},
  event_text_descriptor{
    .id = event_public_text::outcome_too_large,
    .role = event_field_key::outcome,
    .value = "too_large"},
  event_text_descriptor{
    .id = event_public_text::outcome_invalid_batch,
    .role = event_field_key::outcome,
    .value = "invalid_batch"},
  event_text_descriptor{
    .id = event_public_text::outcome_timed_out_before_acceptance,
    .role = event_field_key::outcome,
    .value = "timed_out_before_acceptance"},
  event_text_descriptor{
    .id = event_public_text::outcome_aborted_before_acceptance,
    .role = event_field_key::outcome,
    .value = "aborted_before_acceptance"},
  event_text_descriptor{
    .id = event_public_text::outcome_closed,
    .role = event_field_key::outcome,
    .value = "closed"},
  event_text_descriptor{
    .id = event_public_text::outcome_unknown_producer,
    .role = event_field_key::outcome,
    .value = "unknown_producer"},
};

constexpr std::array forbidden_text_fragments{
  std::string_view{"path"},
  std::string_view{"host"},
  std::string_view{"seed"},
  std::string_view{"credential"},
  std::string_view{"token"},
  std::string_view{"secret"},
  std::string_view{"password"},
};

[[nodiscard]] constexpr bool
field_type_is_valid(event_field_type type) noexcept {
    return type >= event_field_type::signed_integer
           && type <= event_field_type::object_id;
}

consteval bool descriptors_are_valid() {
    for (std::size_t index = 0; index < descriptors.size(); ++index) {
        const auto& descriptor = descriptors[index];
        if (
          static_cast<std::size_t>(descriptor.kind) != index + 1U
          || !event_name_is_valid(descriptor.name, event_name_bytes_max)
          || descriptor.allowed_fields == 0
          || descriptor.allowed_fields != allowed_event_fields(descriptor.kind)
          // A rate-bounded kind alone carries the count its bound dropped.
          || event_kind_is_rate_bounded(descriptor.kind)
               != event_field_is_allowed(
                 descriptor.kind, event_field_key::suppressed)) {
            return false;
        }
        for (std::size_t other = index + 1U; other < descriptors.size();
             ++other) {
            if (descriptor.name == descriptors[other].name) {
                return false;
            }
        }
    }
    return true;
}

[[nodiscard]] constexpr bool text_role_is_valid(event_field_key role) noexcept {
    return role == event_field_key::state || role == event_field_key::outcome
           || role == event_field_key::operation
           || role == event_field_key::reason;
}

consteval bool text_descriptors_are_valid() {
    for (std::size_t index = 0; index < text_descriptors.size(); ++index) {
        const auto& descriptor = text_descriptors[index];
        if (
          static_cast<std::size_t>(descriptor.id) != index + 1U
          || !text_role_is_valid(descriptor.role)
          || !event_name_is_valid(descriptor.value, event_text_bytes_max)) {
            return false;
        }
        for (const auto forbidden : forbidden_text_fragments) {
            if (descriptor.value.find(forbidden) != std::string_view::npos) {
                return false;
            }
        }
        for (std::size_t other = index + 1U; other < text_descriptors.size();
             ++other) {
            if (
              descriptor.role == text_descriptors[other].role
              && descriptor.value == text_descriptors[other].value) {
                return false;
            }
        }
    }
    return true;
}

consteval bool field_descriptors_are_valid() {
    for (std::size_t index = 0; index < field_descriptors.size(); ++index) {
        const auto& descriptor = field_descriptors[index];
        if (
          static_cast<std::size_t>(descriptor.key) != index + 1U
          || !event_name_is_valid(descriptor.name, event_field_name_bytes_max)
          || !field_type_is_valid(descriptor.type)
          || event_field_type_for(descriptor.key) != descriptor.type) {
            return false;
        }
        for (std::size_t other = index + 1U; other < field_descriptors.size();
             ++other) {
            if (descriptor.name == field_descriptors[other].name) {
                return false;
            }
        }
    }
    return true;
}

static_assert(descriptors_are_valid());
static_assert(descriptors.size() == event_kind_count);
static_assert(field_descriptors_are_valid());
static_assert(text_descriptors_are_valid());
static_assert(
  canonical_event_fixed_encoded_size + event_name_bytes_max
    + event_fields_max
        * (canonical_event_field_fixed_encoded_size + event_field_name_bytes_max + event_text_bytes_max)
  > event_encoded_bytes_max);

// What the sequence adds to a rate-bounded event: the key, name length, type,
// payload size, name and an unsigned payload.
constexpr std::size_t suppressed_field_encoded_size
  = canonical_event_field_fixed_encoded_size
    + std::string_view{"suppressed"}.size() + sizeof(std::uint64_t);

[[nodiscard]] runtime::operation_error event_error(errc code) noexcept {
    return runtime::operation_error{
      code, runtime::operation_kind::observability};
}

[[nodiscard]] runtime::operation_error event_limit_error(
  errc code,
  runtime::operation_context_key key,
  std::uint64_t actual,
  std::uint64_t limit) noexcept {
    auto error = event_error(code);
    static_cast<void>(error.add_context(key, actual));
    static_cast<void>(
      error.add_context(runtime::operation_context_key::limit, limit));
    return error;
}

[[nodiscard]] constexpr bool
severity_is_valid(event_severity severity) noexcept {
    return severity >= event_severity::trace
           && severity <= event_severity::error;
}

[[nodiscard]] bool
add_encoded_size(std::size_t& current, std::size_t added) noexcept {
    if (
      current > event_encoded_bytes_max
      || added > event_encoded_bytes_max - current) {
        return false;
    }
    current += added;
    return true;
}

} // namespace

const event_descriptor* descriptor_for(event_kind kind) noexcept {
    const auto value = static_cast<std::size_t>(kind);
    return value != 0 && value <= descriptors.size() ? &descriptors[value - 1U]
                                                     : nullptr;
}

const event_field_descriptor* descriptor_for(event_field_key key) noexcept {
    const auto value = static_cast<std::size_t>(key);
    return value != 0 && value <= field_descriptors.size()
             ? &field_descriptors[value - 1U]
             : nullptr;
}

std::span<const event_descriptor> event_descriptors() noexcept {
    return descriptors;
}

std::span<const event_field_descriptor> event_field_descriptors() noexcept {
    return field_descriptors;
}

const event_text_descriptor* descriptor_for(event_public_text text) noexcept {
    const auto value = static_cast<std::size_t>(text);
    return value != 0 && value <= text_descriptors.size()
             ? &text_descriptors[value - 1U]
             : nullptr;
}

std::span<const event_text_descriptor> event_text_descriptors() noexcept {
    return text_descriptors;
}

std::optional<event_public_text> event_reason_for(errc reason) noexcept {
    switch (reason) {
    case errc::success:
        return std::nullopt;
    case errc::invalid_argument:
        return event_public_text::reason_invalid_argument;
    case errc::out_of_range:
        return event_public_text::reason_out_of_range;
    case errc::malformed_data:
        return event_public_text::reason_malformed_data;
    case errc::unavailable:
        return event_public_text::reason_unavailable;
    case errc::aborted:
        return event_public_text::reason_aborted;
    case errc::closed:
        return event_public_text::reason_closed;
    case errc::timed_out:
        return event_public_text::reason_timed_out;
    case errc::resource_exhausted:
        return event_public_text::reason_resource_exhausted;
    case errc::queue_full:
        return event_public_text::reason_queue_full;
    case errc::wrong_shard:
        return event_public_text::reason_wrong_shard;
    case errc::io_failure:
        return event_public_text::reason_io_failure;
    case errc::network_failure:
        return event_public_text::reason_network_failure;
    case errc::dns_failure:
        return event_public_text::reason_dns_failure;
    case errc::fault_injected:
        return event_public_text::reason_fault_injected;
    case errc::replay_divergence:
        return event_public_text::reason_replay_divergence;
    case errc::invariant_violation:
        return event_public_text::reason_invariant_violation;
    case errc::truncated_data:
        return event_public_text::reason_truncated_data;
    case errc::not_found:
        return event_public_text::reason_not_found;
    case errc::already_exists:
        return event_public_text::reason_already_exists;
    case errc::permission_denied:
        return event_public_text::reason_permission_denied;
    case errc::directory_not_empty:
        return event_public_text::reason_directory_not_empty;
    case errc::is_a_directory:
        return event_public_text::reason_is_a_directory;
    case errc::not_a_directory:
        return event_public_text::reason_not_a_directory;
    case errc::corrupt_data:
        return event_public_text::reason_corrupt_data;
    case errc::wrong_context:
        return event_public_text::reason_wrong_context;
    case errc::unsupported_format:
        return event_public_text::reason_unsupported_format;
    }
    return std::nullopt;
}

runtime::result<event_text> event_text::make(event_public_text value) noexcept {
    const auto* descriptor = descriptor_for(value);
    if (descriptor == nullptr) {
        return runtime::failure(event_error(errc::invalid_argument));
    }
    event_text result;
    result.role_ = descriptor->role;
    for (const char character : descriptor->value) {
        result.storage_[result.size_++] = character;
    }
    return runtime::result<event_text>{std::move(result)};
}

runtime::result<event_text>
event_text::decode(event_field_key role, std::string_view value) noexcept {
    for (const auto& descriptor : text_descriptors) {
        if (descriptor.role == role && descriptor.value == value) {
            return make(descriptor.id);
        }
    }
    return runtime::failure(event_error(errc::malformed_data));
}

runtime::result<event_stable_id>
event_stable_id::make(std::uint64_t value) noexcept {
    if (value == 0) {
        return runtime::failure(event_error(errc::invalid_argument));
    }
    return event_stable_id{value};
}

runtime::result<event_object_id> event_object_id::make(
  std::span<const std::uint8_t, event_object_id_bytes> octets) noexcept {
    for (const auto octet : octets) {
        if (octet != 0) {
            octets_type copied{};
            std::ranges::copy(octets, copied.begin());
            return event_object_id{copied};
        }
    }
    return runtime::failure(event_error(errc::invalid_argument));
}

runtime::result<void> validate_event_encoded_size(std::size_t bytes) noexcept {
    if (bytes == 0) {
        return runtime::failure(event_error(errc::invalid_argument));
    }
    if (bytes > event_encoded_bytes_max) {
        return runtime::failure(event_limit_error(
          errc::out_of_range,
          runtime::operation_context_key::bytes,
          bytes,
          event_encoded_bytes_max));
    }
    return {};
}

event::event(
  const event_request_context& context,
  event_shard shard,
  std::uint64_t sequence) noexcept
  : kind_(context.kind)
  , severity_(context.severity)
  , monotonic_(context.monotonic)
  , wall_(context.wall)
  , shard_(shard)
  , workload_(context.workload)
  , sequence_(sequence) {}

runtime::result<event> event::make(
  const event_request_context& context,
  event_shard shard,
  std::uint64_t sequence,
  std::span<const event_field> fields) noexcept {
    if (sequence == 0) {
        return runtime::failure(event_error(errc::invalid_argument));
    }
    auto request = event_request::build(context, fields, true);
    if (!request) {
        return runtime::failure(request.error());
    }
    return runtime::result<event>{from_request(*request, shard, sequence)};
}

event event::from_request(
  const event_request& request,
  event_shard shard,
  std::uint64_t sequence,
  std::uint64_t suppressed) noexcept {
    event result{request.context_, shard, sequence};
    result.fields_ = request.fields_;
    result.encoded_size_ = request.encoded_size_;
    result.field_count_ = request.field_count_;
    if (suppressed == 0) {
        return result;
    }
    // A rate-bounded request left room for this field (event_request::make),
    // so the event stays inside its bounds. Fields stay ordered by key.
    KWAQUE_INVARIANT(
      invariant_id{"KQ-EVENT-SUPPRESSED"},
      event_kind_is_rate_bounded(request.kind())
        && result.field_count_ < event_fields_max,
      "a suppressed count was added to an event with no room for it");
    auto position = std::size_t{result.field_count_};
    while (position != 0
           && static_cast<std::uint16_t>(result.fields_[position - 1U].key)
                > static_cast<std::uint16_t>(event_field_key::suppressed)) {
        result.fields_[position] = result.fields_[position - 1U];
        --position;
    }
    result.fields_[position] = event_field{
      .key = event_field_key::suppressed,
      .value = event_field_value::from_unsigned(suppressed)};
    ++result.field_count_;
    result.encoded_size_ = static_cast<std::uint16_t>(
      result.encoded_size_ + suppressed_field_encoded_size);
    return result;
}

runtime::result<event_request> event_request::make(
  const event_request_context& context,
  std::span<const event_field> fields) noexcept {
    return build(context, fields, false);
}

runtime::result<event_request> event_request::build(
  const event_request_context& context,
  std::span<const event_field> fields,
  bool decoded) noexcept {
    const auto* event_descriptor = descriptor_for(context.kind);
    if (
      event_descriptor == nullptr || !severity_is_valid(context.severity)
      || resource::workload_index(context.workload)
           >= resource::workload_class_count) {
        return runtime::failure(event_error(errc::invalid_argument));
    }
    // A rate-bounded event keeps room for the suppressed count the sequence
    // may add.
    const bool bounded = event_kind_is_rate_bounded(context.kind) && !decoded;
    const std::size_t fields_max = bounded ? event_fields_max - 1U
                                           : event_fields_max;
    if (fields.size() > fields_max) {
        return runtime::failure(event_limit_error(
          errc::out_of_range,
          runtime::operation_context_key::items,
          fields.size(),
          fields_max));
    }

    event_request result{context};
    for (std::size_t index = 0; index < fields.size(); ++index) {
        const auto* field_descriptor = descriptor_for(fields[index].key);
        const auto text = fields[index].value.as_text();
        const auto text_role = fields[index].value.text_role();
        if (
          field_descriptor == nullptr
          || !event_field_is_allowed(context.kind, fields[index].key)
          // Only a sequence writes the count, and only when it is not zero.
          || (fields[index].key == event_field_key::suppressed
              && (!decoded || fields[index].value.as_unsigned().value_or(0) == 0))
          || field_descriptor->type != fields[index].value.type()
          || (fields[index].value.type() == event_field_type::bounded_string
              && (!text || text->empty() || !text_role
                  || *text_role != fields[index].key))) {
            return runtime::failure(event_error(errc::invalid_argument));
        }
        result.fields_[index] = fields[index];
    }

    for (std::size_t index = 1; index < fields.size(); ++index) {
        auto selected = result.fields_[index];
        auto position = index;
        while (position != 0
               && static_cast<std::uint16_t>(selected.key)
                    < static_cast<std::uint16_t>(
                      result.fields_[position - 1U].key)) {
            result.fields_[position] = result.fields_[position - 1U];
            --position;
        }
        result.fields_[position] = selected;
    }
    for (std::size_t index = 1; index < fields.size(); ++index) {
        if (result.fields_[index - 1U].key == result.fields_[index].key) {
            return runtime::failure(event_error(errc::invalid_argument));
        }
    }

    std::size_t encoded_size = canonical_event_fixed_encoded_size;
    if (!add_encoded_size(encoded_size, event_descriptor->name.size())) {
        return runtime::failure(event_limit_error(
          errc::out_of_range,
          runtime::operation_context_key::bytes,
          event_encoded_bytes_max + 1U,
          event_encoded_bytes_max));
    }
    for (std::size_t index = 0; index < fields.size(); ++index) {
        const auto* field_descriptor = descriptor_for(
          result.fields_[index].key);
        const auto added = canonical_event_field_fixed_encoded_size
                           + field_descriptor->name.size()
                           + result.fields_[index].value.encoded_payload_size();
        if (!add_encoded_size(encoded_size, added)) {
            return runtime::failure(event_limit_error(
              errc::out_of_range,
              runtime::operation_context_key::bytes,
              event_encoded_bytes_max + 1U,
              event_encoded_bytes_max));
        }
    }
    if (
      auto valid = validate_event_encoded_size(
        bounded ? encoded_size + suppressed_field_encoded_size : encoded_size);
      !valid) {
        return runtime::failure(valid.error());
    }
    result.encoded_size_ = static_cast<std::uint16_t>(encoded_size);
    result.field_count_ = static_cast<std::uint8_t>(fields.size());
    return runtime::result<event_request>{std::move(result)};
}

std::string_view event_request::name() const noexcept {
    const auto* descriptor = descriptor_for(context_.kind);
    return descriptor != nullptr ? descriptor->name : std::string_view{};
}

std::string_view event::name() const noexcept {
    const auto* descriptor = descriptor_for(kind_);
    return descriptor != nullptr ? descriptor->name : std::string_view{};
}

} // namespace kwaque::observability
