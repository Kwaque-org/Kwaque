#pragma once

#include "src/base/names.h"

#include <array>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

namespace kwaque::model {

// The outcome registry: every answer of a metadata command, a proposal, an
// append, a resolve and a read stop, each with a frozen u16 value. The
// hundreds of a value name its domain, so one value never means two
// outcomes, and zero is never an outcome. The values are serialized (a
// metadata group's result window keeps its result as a u16, and the wire
// will carry the rest): append a code after its domain's last one and move
// the domain's marker; never renumber, rename or remove a code.

enum class outcome_domain : std::uint8_t {
    // 1 to 99.
    metadata = 0,
    // 100 to 199.
    proposer = 1,
    // 200 to 299.
    append = 2,
    // 300 to 399.
    resolve = 3,
    // 400 to 499.
    read_stop = 4,
};

// The answer to a metadata command, kept in the group's result window.
enum class metadata_result : std::uint16_t {
    applied = 1,
    stale_epoch = 2,
    illegal_transition = 3,
    name_taken = 4,
    name_being_released = 5,
    not_found = 6,
    active_ranges_exhausted = 7,
    descriptors_exhausted = 8,
    manifest_full = 9,
    out_of_range = 10,
    invalid_policy = 11,
    invalid_name = 12,
    unknown_producer = 13,
    producer_fenced = 14,
    epoch_exhausted = 15,
    same_request = 16,
};
inline constexpr metadata_result metadata_result_last
  = metadata_result::same_request;

// The proposer's own answers, never persisted. Acceptance is the hand-off of
// the command to the journal's writer: a proposal that ends before it was
// refused, and one that ends after it, even while the write is in flight,
// may still be applied, so its caller resolves it by the request's identity
// (a retry with the same request ID answers from it).
enum class proposer_outcome : std::uint16_t {
    concurrent_init = 100,
    timed_out_before_acceptance = 101,
    closed_before_acceptance = 102,
    aborted_before_acceptance = 103,
    timed_out_after_acceptance = 104,
    closed_after_acceptance = 105,
    aborted_after_acceptance = 106,
};
inline constexpr proposer_outcome proposer_outcome_last
  = proposer_outcome::aborted_after_acceptance;

// The answer to an append. Only the three codes from 230 leave its effect
// open; every other one is definite.
enum class append_outcome : std::uint16_t {
    committed = 200,
    duplicate = 201,
    duplicate_outside_window = 202,
    batch_id_conflict = 203,
    stale = 204,
    sequence_gap = 205,
    window_exceeded = 206,
    unknown_stream = 207,
    stream_sealed = 208,
    stream_failed = 209,
    producer_fenced = 210,
    invalid_ack_floor = 211,
    stream_limit = 212,
    topic_sealed = 213,
    not_found = 214,
    misrouted_key = 215,
    stale_route = 216,
    no_space = 217,
    too_large = 218,
    invalid_batch = 219,
    timed_out_before_acceptance = 220,
    aborted_before_acceptance = 221,
    closed = 222,
    unknown_producer = 223,
    timed_out_after_acceptance = 230,
    aborted_after_acceptance = 231,
    storage_uncertain = 232,
};
inline constexpr append_outcome append_outcome_last
  = append_outcome::storage_uncertain;

// The answer to a stream resolve.
enum class resolve_code : std::uint16_t {
    active = 300,
    sealed = 301,
    committed = 302,
    not_committed = 303,
    committed_position_unavailable = 304,
    batch_id_conflict = 305,
    retry_window_expired = 306,
    unavailable = 307,
    generation_lost = 308,
};
inline constexpr resolve_code resolve_code_last = resolve_code::generation_lost;

// Why a consume stream stopped pushing.
enum class read_stop_code : std::uint16_t {
    window = 400,
    caught_up = 401,
    range_sealed = 402,
    below_start = 403,
    cancelled = 404,
    lost = 405,
    failed_damage = 410,
    failed_device = 411,
    failed_resource = 412,
    failed_closed = 413,
    failed_deleted = 414,
    failed_internal = 415,
};
inline constexpr read_stop_code read_stop_code_last
  = read_stop_code::failed_internal;

// What an outcome says about the effect of the request it answers.
enum class outcome_effect : std::uint8_t {
    // Definite: the effect happened.
    happened,
    // Definite: the effect did not happen and never will.
    refused,
    // The effect may still happen; the request's identity resolves it.
    indefinite,
    // A resolve answer or a read stop, which answer no request with an
    // effect.
    not_applicable,
};

// What the caller may do next.
enum class outcome_retry : std::uint8_t {
    // Nothing to retry: a success, the caller's own cancellation, or a stop
    // the read continues past.
    none,
    // The same request cannot succeed.
    never,
    // The same request may succeed later.
    later,
    // After reading the current epoch or version.
    after_reread,
    // After a range is sealed or deleted.
    after_seal_or_delete,
    // After retention removes segments.
    after_retention,
    // After resending from the expected sequence.
    after_resend,
    // Continue on the successor: an append resends on the successor stream,
    // a read opens the successor ranges.
    on_successor,
    // Resolve the stream, then continue on its successor.
    resolve_then_successor,
    // Resend the records to the ranges their keys route to.
    reroute,
    // Initialize the producer again.
    reinitialize,
    // Resolve by the request's identity.
    resolve,
    // Resolve once the segment is sealed.
    resolve_after_seal,
    // Reopen the read at the range's start.
    reopen_at_start,
    // Reopen the read past the lost span.
    reopen_past_lost,
    // Reopen the read.
    reopen,
};

// What an outcome carries besides its code; the owning operation defines
// the type.
enum class outcome_payload : std::uint8_t {
    none,
    // The command's value: an ID, a version or a start.
    command_value,
    // The object's current epoch or version.
    current_version,
    current_state,
    limit,
    bound,
    // The ordinal of the refused policy field.
    policy_field,
    current_epoch,
    // The ID of the object the original request made.
    original_object,
    // The span and its ack generation.
    span,
    // The largest floor seen.
    floor,
    // The next sequence the stream admits.
    expected_sequence,
    // The largest floor seen and the window.
    floor_and_window,
    // The committed point, the successors and the granted window.
    seal_notice,
    committed_point,
    // Which stream bound was reached.
    stream_bound,
    record_index,
    current_leaf,
    // When the next space sample is due.
    next_sample,
    // The refused field and its offset.
    codec_error,
    // The stream binding and the sequence to resolve.
    binding_and_sequence,
    // The committed point and the next sequence the stream admits.
    committed_and_next,
    committed_and_successors,
    // The range's end and its successors.
    end_and_successors,
    range_start,
    // The lost span [from, to); the read reopens at `to`.
    lost_span,
};

struct outcome_entry final {
    std::uint16_t code;
    std::string_view name;
    outcome_effect effect;
    // A resolve answer no later resolve changes, or a read stop that ends the
    // read. Every metadata, proposer and append outcome is its request's one
    // answer, so it is final.
    bool final_answer;
    outcome_retry retry;
    outcome_payload payload;

    bool operator==(const outcome_entry&) const = default;
};

namespace detail {

// One constructor per class, so each entry reads as its specification row.
[[nodiscard]] consteval outcome_entry
happened(std::uint16_t code, std::string_view name, outcome_payload payload) {
    return {
      code, name, outcome_effect::happened, true, outcome_retry::none, payload};
}

[[nodiscard]] consteval outcome_entry refused(
  std::uint16_t code,
  std::string_view name,
  outcome_retry retry,
  outcome_payload payload = outcome_payload::none) {
    return {code, name, outcome_effect::refused, true, retry, payload};
}

[[nodiscard]] consteval outcome_entry indefinite(
  std::uint16_t code,
  std::string_view name,
  outcome_retry retry,
  outcome_payload payload) {
    return {code, name, outcome_effect::indefinite, true, retry, payload};
}

// A resolve answer or a read stop; `final_answer` is the specification's
// Final or Terminal column.
[[nodiscard]] consteval outcome_entry answer(
  std::uint16_t code,
  std::string_view name,
  bool final_answer,
  outcome_retry retry = outcome_retry::none,
  outcome_payload payload = outcome_payload::none) {
    return {
      code, name, outcome_effect::not_applicable, final_answer, retry, payload};
}

[[nodiscard]] consteval auto make_outcome_registry() {
    using retry = outcome_retry;
    using carries = outcome_payload;
    constexpr bool final_answer = true;
    constexpr bool continues = false;
    return std::to_array<outcome_entry>({
      // Metadata results.
      happened(1, "applied", carries::command_value),
      refused(2, "stale_epoch", retry::after_reread, carries::current_version),
      refused(3, "illegal_transition", retry::never, carries::current_state),
      refused(4, "name_taken", retry::never),
      refused(5, "name_being_released", retry::later),
      refused(6, "not_found", retry::never),
      refused(
        7,
        "active_ranges_exhausted",
        retry::after_seal_or_delete,
        carries::limit),
      refused(8, "descriptors_exhausted", retry::later, carries::limit),
      refused(9, "manifest_full", retry::after_retention),
      refused(10, "out_of_range", retry::never, carries::bound),
      refused(11, "invalid_policy", retry::never, carries::policy_field),
      refused(12, "invalid_name", retry::never),
      refused(13, "unknown_producer", retry::never),
      refused(14, "producer_fenced", retry::never, carries::current_epoch),
      refused(15, "epoch_exhausted", retry::never),
      happened(16, "same_request", carries::original_object),
      // Proposer outcomes.
      refused(100, "concurrent_init", retry::later),
      refused(101, "timed_out_before_acceptance", retry::later),
      refused(102, "closed_before_acceptance", retry::later),
      refused(103, "aborted_before_acceptance", retry::later),
      indefinite(
        104, "timed_out_after_acceptance", retry::resolve, carries::none),
      indefinite(105, "closed_after_acceptance", retry::resolve, carries::none),
      indefinite(
        106, "aborted_after_acceptance", retry::resolve, carries::none),
      // Append outcomes.
      happened(200, "committed", carries::span),
      happened(201, "duplicate", carries::span),
      happened(202, "duplicate_outside_window", carries::none),
      refused(203, "batch_id_conflict", retry::never),
      refused(204, "stale", retry::never, carries::floor),
      refused(
        205, "sequence_gap", retry::after_resend, carries::expected_sequence),
      refused(206, "window_exceeded", retry::later, carries::floor_and_window),
      refused(207, "unknown_stream", retry::never),
      refused(208, "stream_sealed", retry::on_successor, carries::seal_notice),
      refused(
        209,
        "stream_failed",
        retry::resolve_then_successor,
        carries::committed_point),
      refused(210, "producer_fenced", retry::never, carries::current_epoch),
      refused(211, "invalid_ack_floor", retry::never, carries::committed_point),
      refused(212, "stream_limit", retry::later, carries::stream_bound),
      refused(213, "topic_sealed", retry::never),
      refused(214, "not_found", retry::never),
      refused(215, "misrouted_key", retry::reroute, carries::record_index),
      refused(216, "stale_route", retry::later, carries::current_leaf),
      refused(217, "no_space", retry::later, carries::next_sample),
      refused(218, "too_large", retry::never, carries::bound),
      refused(219, "invalid_batch", retry::never, carries::codec_error),
      refused(220, "timed_out_before_acceptance", retry::later),
      refused(221, "aborted_before_acceptance", retry::later),
      refused(222, "closed", retry::later),
      refused(223, "unknown_producer", retry::reinitialize),
      indefinite(
        230,
        "timed_out_after_acceptance",
        retry::resolve,
        carries::binding_and_sequence),
      indefinite(
        231,
        "aborted_after_acceptance",
        retry::resolve,
        carries::binding_and_sequence),
      indefinite(
        232,
        "storage_uncertain",
        retry::resolve_after_seal,
        carries::binding_and_sequence),
      // Resolve answers.
      answer(
        300, "active", continues, retry::none, carries::committed_and_next),
      answer(
        301,
        "sealed",
        continues,
        retry::none,
        carries::committed_and_successors),
      answer(302, "committed", final_answer, retry::none, carries::span),
      answer(303, "not_committed", final_answer),
      answer(304, "committed_position_unavailable", final_answer),
      answer(305, "batch_id_conflict", final_answer),
      answer(306, "retry_window_expired", final_answer),
      answer(307, "unavailable", continues, retry::later),
      answer(308, "generation_lost", final_answer),
      // Read stops.
      answer(400, "window", continues),
      answer(401, "caught_up", continues),
      answer(
        402,
        "range_sealed",
        final_answer,
        retry::on_successor,
        carries::end_and_successors),
      answer(
        403,
        "below_start",
        continues,
        retry::reopen_at_start,
        carries::range_start),
      answer(404, "cancelled", final_answer),
      answer(
        405, "lost", continues, retry::reopen_past_lost, carries::lost_span),
      answer(410, "failed_damage", final_answer, retry::never),
      answer(411, "failed_device", final_answer, retry::never),
      answer(412, "failed_resource", final_answer, retry::reopen),
      answer(413, "failed_closed", final_answer, retry::never),
      answer(414, "failed_deleted", final_answer, retry::never),
      answer(415, "failed_internal", final_answer, retry::never),
    });
}

} // namespace detail

// Every outcome, in value order.
inline constexpr auto outcome_registry = detail::make_outcome_registry();

template<typename Code>
concept outcome_code = std::same_as<Code, metadata_result>
                       || std::same_as<Code, proposer_outcome>
                       || std::same_as<Code, append_outcome>
                       || std::same_as<Code, resolve_code>
                       || std::same_as<Code, read_stop_code>;

template<outcome_code Code>
inline constexpr outcome_domain outcome_domain_of_code = [] {
    if constexpr (std::same_as<Code, metadata_result>) {
        return outcome_domain::metadata;
    } else if constexpr (std::same_as<Code, proposer_outcome>) {
        return outcome_domain::proposer;
    } else if constexpr (std::same_as<Code, append_outcome>) {
        return outcome_domain::append;
    } else if constexpr (std::same_as<Code, resolve_code>) {
        return outcome_domain::resolve;
    } else {
        return outcome_domain::read_stop;
    }
}();

[[nodiscard]] constexpr outcome_domain
domain_of(const outcome_entry& entry) noexcept {
    return static_cast<outcome_domain>(entry.code / 100U);
}

namespace detail {

inline constexpr std::uint8_t unlisted_outcome = 0xFF;

// The registry position of each value, so a lookup is one load.
inline constexpr auto outcome_index = [] {
    std::array<std::uint8_t, outcome_registry.back().code + 1U> index{};
    index.fill(unlisted_outcome);
    for (std::size_t at = 0; at < outcome_registry.size(); ++at) {
        index[outcome_registry[at].code] = static_cast<std::uint8_t>(at);
    }
    return index;
}();

template<outcome_code Code>
[[nodiscard]] consteval bool ends_domain(Code last) {
    const auto value = static_cast<std::uint16_t>(last);
    const outcome_entry* final_entry = nullptr;
    for (const auto& entry : outcome_registry) {
        if (domain_of(entry) == outcome_domain_of_code<Code>) {
            final_entry = &entry;
        }
    }
    return final_entry != nullptr && final_entry->code == value;
}

[[nodiscard]] consteval bool outcome_registry_is_valid() {
    if (outcome_registry.size() >= unlisted_outcome) {
        return false;
    }
    std::uint16_t previous = 0;
    for (const auto& entry : outcome_registry) {
        if (entry.code <= previous || entry.code >= 500U) {
            return false;
        }
        previous = entry.code;
        if (!is_lowercase_name(entry.name, 64)) {
            return false;
        }
        const auto domain = domain_of(entry);
        const bool answers_request = domain == outcome_domain::metadata
                                     || domain == outcome_domain::proposer
                                     || domain == outcome_domain::append;
        if (
          answers_request != (entry.effect != outcome_effect::not_applicable)) {
            return false;
        }
        if (answers_request && !entry.final_answer) {
            return false;
        }
        for (const auto& other : outcome_registry) {
            if (
              &other != &entry && domain_of(other) == domain
              && other.name == entry.name) {
                return false;
            }
        }
    }
    return ends_domain(metadata_result_last)
           && ends_domain(proposer_outcome_last)
           && ends_domain(append_outcome_last) && ends_domain(resolve_code_last)
           && ends_domain(read_stop_code_last);
}

static_assert(outcome_registry_is_valid());

} // namespace detail

// The entry of a listed value, or null.
[[nodiscard]] constexpr const outcome_entry*
find_outcome(std::uint16_t value) noexcept {
    if (value >= detail::outcome_index.size()) {
        return nullptr;
    }
    const auto at = detail::outcome_index[value];
    return at == detail::unlisted_outcome ? nullptr : &outcome_registry[at];
}

// The code of `value` in Code's domain; nothing for a value that domain does
// not list.
template<outcome_code Code>
[[nodiscard]] constexpr std::optional<Code>
decode(std::uint16_t value) noexcept {
    const auto* entry = find_outcome(value);
    if (entry == nullptr || domain_of(*entry) != outcome_domain_of_code<Code>) {
        return std::nullopt;
    }
    return static_cast<Code>(value);
}

// The entry of a code, or null for a value its domain does not list.
template<outcome_code Code>
[[nodiscard]] constexpr const outcome_entry* find_outcome(Code code) noexcept {
    const auto value = static_cast<std::uint16_t>(code);
    return decode<Code>(value) ? find_outcome(value) : nullptr;
}

// The code's snake-case name, or nothing for an unlisted value.
template<outcome_code Code>
[[nodiscard]] constexpr std::string_view name(Code code) noexcept {
    const auto* entry = find_outcome(code);
    return entry == nullptr ? std::string_view{} : entry->name;
}

// The codes that answer a request with an effect; a resolve answer or a read
// stop answers none.
template<typename Code>
concept request_outcome_code = std::same_as<Code, metadata_result>
                               || std::same_as<Code, proposer_outcome>
                               || std::same_as<Code, append_outcome>;

// Whether the outcome settles its request's effect.
template<request_outcome_code Code>
[[nodiscard]] constexpr bool is_definite(Code code) noexcept {
    const auto* entry = find_outcome(code);
    return entry != nullptr
           && (entry->effect == outcome_effect::happened || entry->effect == outcome_effect::refused);
}

} // namespace kwaque::model
