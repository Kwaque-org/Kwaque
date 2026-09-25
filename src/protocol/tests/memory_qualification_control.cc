#include "src/protocol/tests/memory_qualification_control.h"

#include "src/codec/tests/memory_qualification_support.h"
#include "src/codec/tests/prepared_abort_source.h"
#include "src/protocol/control_frame_codec.h"
#include "src/protocol/control_internal.h"
#include "src/protocol/control_memory.h"
#include "src/protocol/control_schema.h"
#include "src/protocol/tests/control_fuzz_oracle.h"
#include "src/protocol/tests/control_test_payload.h"
#include "src/protocol/tests/control_test_support.h"
#include "src/protocol/tests/frame_test_support.h"

#include <seastar/core/abort_source.hh>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <variant>

namespace kwaque::protocol::testing {
namespace {
namespace observation = codec::testing;
namespace fixture = control_fixture;
namespace frame = frame_fixture;
using bytes::fragmented_buffer_parser;
using bytes::testing::charge;
using observation::measure;
using observation::require;
using observation::retained_cost;

thread_local seastar::abort_source* admission_abort = nullptr;
thread_local std::size_t admission_count = 0;
thread_local std::size_t abort_at = 0;
byte_count observed_charge(byte_count request) noexcept {
    if (++admission_count == abort_at && admission_abort)
        admission_abort->request_abort();
    return charge(request);
}
class admissions final {
public:
    explicit admissions(
      seastar::abort_source* abort = nullptr,
      std::size_t ordinal = 0) noexcept {
        admission_count = 0;
        abort_at = ordinal;
        admission_abort = abort;
    }
    ~admissions() {
        admission_abort = nullptr;
        abort_at = 0;
    }
    admissions(const admissions&) = delete;
    admissions& operator=(const admissions&) = delete;
};

byte_count string_cost(const std::string& value) {
    return charge(byte_count{value.capacity() + 1U});
}
codec::decode_budget memory(byte_count other) {
    return {
      observation::residual.checked_sub(other).value(),
      byte_count{1U << 20U},
      observed_charge};
}

// Literal fields are prepared without parsing a generated message. Ingress
// observations therefore include the first native parse in a fresh process.
std::string capabilities(unsigned count, bool distributed = false) {
    std::string versions, formats, codecs;
    for (unsigned i = 1; i <= std::min(count, 16U); ++i) {
        versions += fixture::varint(i);
        codecs += fixture::varint(i - 1U);
    }
    for (unsigned i = 1; i <= count; ++i) {
        auto format = fixture::format(static_cast<std::uint16_t>(i));
        if (distributed) format += fixture::blob(99, std::string(100, 'u'));
        formats += fixture::blob(2, format);
    }
    auto result = fixture::blob(1, versions) + formats
                  + fixture::blob(3, codecs) + fixture::scalar(4, 16777216)
                  + fixture::scalar(5, 8388608) + fixture::scalar(6, 4096)
                  + fixture::scalar(7, 1048576) + fixture::scalar(8, 4096);
    if (distributed) result += fixture::blob(99, std::string(100, 'u'));
    return result;
}
std::string payload(frame_kind kind, std::string_view shape) {
    if (shape == "unknowns") {
        auto wire = fixture::scalar(1, 1);
        for (unsigned i = 0; i < 255; ++i)
            wire += fixture::blob(99, "");
        return wire;
    }
    if (shape == "distributed")
        return fixture::request(
                 fixture::blob(1, "v")
                   + fixture::blob(99, std::string(100, 'u')),
                 capabilities(32, true))
               + fixture::blob(99, std::string(100, 'u'));
    if (shape == "churn") {
        std::string build;
        for (unsigned n :
             {23U,
              24U,
              31U,
              32U,
              63U,
              64U,
              127U,
              128U,
              255U,
              256U,
              511U,
              512U,
              1000U})
            build += fixture::blob(1, std::string(n, 'x'));
        require(build.size() <= 4096, "control overwrite fixture is too large");
        return fixture::request(build);
    }
    if (shape.starts_with("repeated_")) {
        unsigned count = 0;
        for (const unsigned n : {1U, 2U, 3U, 15U, 16U, 17U, 31U, 32U})
            if (shape == "repeated_" + std::to_string(n)) count = n;
        require(count != 0, "unknown repeated control shape");
        return fixture::request({}, capabilities(count));
    }
    return control_payload(
      kind,
      shape == "tiny"          ? 0U
      : shape == "build_limit" ? 1U
      : shape == "wire_max"    ? 3U
                               : 2U);
}

template<typename Id>
Id identity(char octet) {
    std::array<std::uint8_t, 16> bytes{};
    bytes.fill(static_cast<std::uint8_t>(octet));
    return Id::make(bytes).value();
}
routing_context routing() {
    return {
      identity<model::topic_id>('t'),
      identity<model::range_id>('r'),
      model::range_routing_epoch::make(UINT64_MAX).value(),
      identity<model::segment_id>('s'),
      model::segment_generation::make(1).value()};
}
control_capabilities owning_capabilities(bool maximum) {
    control_capabilities result{
      {},
      {},
      {},
      byte_count{16777216},
      byte_count{8388608},
      byte_count{4096},
      byte_count{1048576},
      item_count{4096}};
    if (maximum) {
        for (std::uint16_t i = 1; i <= 16; ++i)
            result.protocol_versions.push_back(i);
        for (std::uint16_t i = 1; i <= 32; ++i)
            result.formats.push_back({i, 1, 9, std::uint64_t{1} << 63U});
        for (std::uint8_t i = 0; i < 16; ++i)
            result.compression_codecs.push_back(i);
    } else {
        result.protocol_versions = {1, 2};
        result.formats = {{65000, 1, 9, std::uint64_t{1} << 63U}};
        result.compression_codecs = {0, 255};
    }
    return result;
}
control_data draft(frame_kind kind, std::string_view shape) {
    const bool maximum = shape != "tiny" && shape != "build_limit";
    build_information build;
    if (shape == "build_limit")
        build.version.assign(4093, 'v');
    else if (
      maximum
      && (kind == frame_kind::handshake_request || kind == frame_kind::handshake_response))
        build = {
          std::string(1360, 'v'),
          std::string(1360, 'r'),
          std::string(1340, 'b')};
    switch (kind) {
    case frame_kind::handshake_request:
        return handshake_request{
          maximum ? peer_kind::broker : peer_kind::client,
          maximum ? std::optional{identity<model::cluster_id>('c')}
                  : std::nullopt,
          maximum ? std::optional{identity<model::broker_id>('b')}
                  : std::nullopt,
          std::move(build),
          owning_capabilities(maximum)};
    case frame_kind::handshake_response:
        return handshake_response{
          identity<model::cluster_id>('c'),
          identity<model::broker_id>('b'),
          std::move(build),
          owning_capabilities(maximum)};
    case frame_kind::redirect:
        return redirect_control{
          routing(),
          identity<model::broker_id>('b'),
          {maximum ? std::string(253, 'h') : "host", 65535},
          control_error_code::stale_routing};
    case frame_kind::error:
        if (maximum)
            return error_control{
              control_error_code::stale_routing,
              std::string(1024, 'e'),
              routing()};
        return error_control{control_error_code::invalid_request, {}, {}};
    default:
        throw std::invalid_argument("unknown control root");
    }
}
void check_value(const control& value, std::string_view wire, frame_kind kind) {
    // Native oracle construction is deliberately after the observation.
    const auto expected = probe_control(wire, kind);
    require(
      expected && matches_control(*expected, value.data()),
      "control changed fields or presence");
}
frame_metadata metadata(frame_kind kind) {
    auto result = frame::metadata;
    result.kind = kind;
    result.stream = model::transport_stream_id{};
    return result;
}
std::string wrap(std::string_view body, frame_kind kind) {
    auto result = frame::header(
      static_cast<std::uint32_t>(body.size()),
      frame::crc32c(body),
      {},
      metadata(kind));
    result.reserve(result.size() + body.size());
    result.append(body);
    return result;
}

void decode_operation(
  std::string_view name, frame_kind kind, std::string_view shape, bool framed) {
    auto body = payload(kind, shape);
    if (shape == "wire_max")
        require(body.size() == 65536, "maximum control fixture is short");
    if (shape == "malformed") body.push_back('\0');
    auto wire = framed ? wrap(body, kind) : body;
    wire.insert(0, "pre");
    std::optional<observation::prepared_abort_source> abort;
    const auto setup = observation::observe_setup([&] { abort.emplace(); });
    codec::cooperative_work work{codec::limits::defaults(), *abort};
    auto bytes = fixture::bounded_fragmented(wire, 7);
    const auto other = setup.checked_add(string_cost(body))
                         .value()
                         .checked_add(string_cost(wire))
                         .value();
    const auto held = other.checked_add(retained_cost(bytes)).value();
    std::optional<fragmented_buffer_parser> input{
      std::in_place, std::move(bytes)};
    input->skip(byte_count{3}).value();
    input->push_checkpoint().value();
    auto budget = codec::reserve_decode_input(
                    *input, work.policy(), memory(other))
                    .value();
    control_expectation expected;
    if (shape == "context") {
        if (
          kind == frame_kind::handshake_request
          || kind == frame_kind::handshake_response)
            expected.cluster = identity<model::cluster_id>('x');
        else
            expected.topic = identity<model::topic_id>('x');
    }
    std::size_t cancel_at = shape == "abort" ? 1U : 0U;
    const bool exact = shape == "exact" || shape == "short";
    if (exact || shape == "late_abort") {
        // Only boundary/late-abort scenarios warm the path to discover its
        // returned residual or final admission. Cold scenarios never do this.
        byte_count conversion;
        {
            fragmented_buffer_parser baseline_input{
              fixture::bounded_fragmented(wire, 7)};
            baseline_input.skip(byte_count{3}).value();
            baseline_input.push_checkpoint().value();
            seastar::abort_source baseline_abort;
            codec::cooperative_work baseline_work{
              codec::limits::defaults(), baseline_abort};
            const auto baseline_memory = codec::reserve_decode_input(
                                           baseline_input,
                                           baseline_work.policy(),
                                           memory(held))
                                           .value();
            admissions baseline;
            if (framed) {
                auto result = decode_control_frame(
                                baseline_input,
                                kind,
                                {},
                                frame::bounds,
                                baseline_memory,
                                baseline_work)
                                .get();
                require(
                  result
                    && std::holds_alternative<decoded_control_frame>(*result),
                  "control baseline frame failed");
            } else {
                auto result
                  = decode_control(
                      baseline_input, kind, {}, baseline_memory, baseline_work)
                      .get();
                require(result.has_value(), "control baseline decode failed");
                conversion = baseline_memory.operation_remaining
                               .checked_sub(
                                 result->remaining.operation_remaining)
                               .value();
            }
            if (shape == "late_abort") cancel_at = admission_count;
        }
        if (exact) {
            const auto cost = detail::bound_control_parse(
                                detail::schema_for(kind)->message,
                                byte_count{body.size()},
                                work.policy(),
                                charge)
                                .value();
            budget.operation_remaining = cost.generated_peak
                                           .checked_add(cost.input_copy)
                                           .value()
                                           .checked_add(conversion)
                                           .value();
            if (shape == "short")
                budget.operation_remaining = budget.operation_remaining
                                               .checked_sub(byte_count{1})
                                               .value();
        }
    }
    if (shape == "pressure") budget.metadata_remaining = {};
    const bool canceled = shape == "abort" || shape == "late_abort";
    require(
      !canceled || cancel_at != 0, "control cancellation has no admission");
    const auto wanted = canceled               ? errc::aborted
                        : shape == "malformed" ? errc::malformed_data
                        : shape == "context"   ? errc::wrong_context
                        : shape == "pressure" || shape == "short"
                          ? errc::resource_exhausted
                          : errc::success;
    const auto verify = [&](const auto& result, const auto* value) {
        require(input->checkpoint_depth() == 1, "control lost caller mark");
        require(
          !canceled || abort->abort_requested(),
          "control abort was not entered");
        if (wanted != errc::success) {
            require(
              !result && result.error().code() == wanted,
              "control rejection changed error");
            require(
              input->bytes_consumed() == byte_count{3},
              "control rejection consumed input");
            return;
        }
        require(
          value && input->at_end(),
          "control did not consume the complete input");
        require(
          budget.operation_remaining >= value->remaining.operation_remaining
            && budget.metadata_remaining >= value->remaining.metadata_remaining,
          "control widened a residual");
        require(
          budget.operation_remaining.value()
              - value->remaining.operation_remaining.value()
            == budget.metadata_remaining.value()
                 - value->remaining.metadata_remaining.value(),
          "control retained temporary input or generated charges");
        input.reset();
        check_value(value->value, body, kind);
    };
    if (framed) {
        auto result = measure(name, body.size(), held, [&] {
            admissions arm{canceled ? &*abort : nullptr, cancel_at};
            return decode_control_frame(
                     *input, kind, expected, frame::bounds, budget, work)
              .get();
        });
        const auto* value = result
                              ? std::get_if<decoded_control_frame>(&*result)
                              : nullptr;
        if (value)
            require(
              value->header.metadata == metadata(kind),
              "control frame changed transport identity");
        verify(result, value);
    } else {
        auto result = measure(name, body.size(), held, [&] {
            admissions arm{canceled ? &*abort : nullptr, cancel_at};
            return decode_control(*input, kind, expected, budget, work).get();
        });
        verify(result, result ? &*result : nullptr);
    }
}

void output_operation(
  std::string_view name,
  frame_kind kind,
  std::string_view op,
  std::string_view shape) {
    auto wire = payload(kind, shape);
    auto data = draft(kind, shape);
    std::optional<observation::prepared_abort_source> abort;
    const auto setup = observation::observe_setup([&] { abort.emplace(); });
    codec::cooperative_work work{codec::limits::defaults(), *abort};
    const auto layout
      = detail::inspect_control_value(data, work, charge, {}).get().value();
    const auto other = setup.checked_add(string_cost(wire)).value();
    const auto held = other.checked_add(layout.storage).value();
    auto budget = memory(other);
    const bool canceled = shape == "abort" || shape == "late_abort";
    const bool pressure = shape == "pressure";
    if (pressure) budget.operation_remaining = {};
    const auto rejected = [&](const auto& result) {
        require(
          !canceled || abort->abort_requested(),
          "control writer abort was not entered");
        require(
          !result
            && result.error().code()
                 == (canceled ? errc::aborted : errc::resource_exhausted),
          "control writer rejection changed error");
    };
    if (op == "make") {
        auto result = measure(name, wire.size(), held, [&] {
            admissions arm{canceled ? &*abort : nullptr, canceled ? 1U : 0U};
            return make_control(std::move(data), budget, work).get();
        });
        if (canceled || pressure) {
            rejected(result);
            return;
        }
        require(result.has_value(), "control construction failed");
        require(
          result->remaining.operation_remaining
              == budget.operation_remaining.checked_sub(layout.storage).value()
            && result->remaining.metadata_remaining
                 == budget.metadata_remaining.checked_sub(layout.storage)
                      .value(),
          "control constructor lost its donor charge");
        check_value(result->value, wire, kind);
        return;
    }
    // Construct an accepted owner without warming generated/native parsing.
    auto value
      = make_control(std::move(data), memory(other), work).get().value();
    std::size_t cancel_at = canceled ? 1U : 0U;
    if (shape == "late_abort") {
        seastar::abort_source baseline_abort;
        codec::cooperative_work baseline_work{
          codec::limits::defaults(), baseline_abort};
        admissions baseline;
        auto result = op == "encode" ? encode_control(
                                         value.value,
                                         baseline_work,
                                         {},
                                         budget.operation_remaining,
                                         observed_charge)
                                         .get()
                                     : encode_control_frame(
                                         value.value,
                                         frame::metadata.correlation,
                                         frame::metadata.sequence,
                                         baseline_work,
                                         frame::bounds,
                                         {},
                                         budget.operation_remaining,
                                         observed_charge)
                                         .get();
        require(result.has_value(), "control writer baseline failed");
        cancel_at = admission_count;
        require(cancel_at != 0, "control writer has no final admission");
    }
    auto result = measure(name, wire.size(), held, [&] {
        admissions arm{canceled ? &*abort : nullptr, cancel_at};
        return op == "encode" ? encode_control(
                                  value.value,
                                  work,
                                  {},
                                  budget.operation_remaining,
                                  observed_charge)
                                  .get()
                              : encode_control_frame(
                                  value.value,
                                  frame::metadata.correlation,
                                  frame::metadata.sequence,
                                  work,
                                  frame::bounds,
                                  {},
                                  budget.operation_remaining,
                                  observed_charge)
                                  .get();
    });
    if (canceled || pressure) {
        rejected(result);
        check_value(value.value, wire, kind);
        return;
    }
    require(result.has_value(), "control serialization failed");
    auto encoded = fixture::flatten(*result);
    std::string_view body = encoded;
    if (op == "encode_frame") {
        fragmented_buffer_parser header{result->share()};
        const auto prefix
          = peek_frame_prefix(header, work.policy(), frame::bounds).value();
        require(
          prefix.header_bytes == 48
            && prefix.kind == static_cast<std::uint16_t>(kind)
            && prefix.stream == 0
            && prefix.correlation == frame::metadata.correlation.value()
            && prefix.sequence == frame::metadata.sequence.value()
            && prefix.payload_bytes == encoded.size() - 48U,
          "serialized control frame changed extent or identity");
        body.remove_prefix(48);
        require(
          prefix.payload_crc32c == frame::crc32c(body),
          "serialized control frame changed payload integrity");
        auto header_bytes = encoded.substr(0, 48);
        frame::put_u32(header_bytes, 40, 0);
        require(
          prefix.header_crc32c == frame::crc32c(header_bytes),
          "serialized control frame changed header integrity");
    }
    const auto parsed = probe_control(body, kind);
    require(
      parsed && control_unknowns_empty(*parsed)
        && matches_control(*parsed, value.value.data()),
      "serialized control changed fields or retained unknowns");
    check_value(value.value, wire, kind);
}
} // namespace

void qualify_control_memory(std::string_view name) {
    require(name.starts_with("control-"), "unknown control scenario");
    auto rest = name;
    rest.remove_prefix(std::string_view{"control-"}.size());
    const auto root_end = rest.find('-');
    require(root_end != std::string_view::npos, "missing control operation");
    const auto root = rest.substr(0, root_end);
    const auto kind = root == "request"    ? frame_kind::handshake_request
                      : root == "response" ? frame_kind::handshake_response
                      : root == "redirect" ? frame_kind::redirect
                                           : frame_kind::error;
    require(
      root == "request" || root == "response" || root == "redirect"
        || root == "error",
      "unknown control root");
    rest.remove_prefix(root_end + 1U);
    const auto op_end = rest.find('-');
    require(op_end != std::string_view::npos, "missing control shape");
    const auto op = rest.substr(0, op_end), shape = rest.substr(op_end + 1U);
    const bool input = op == "decode" || op == "decode_frame";
    const bool ordinary
      = shape == "tiny" || shape == "max" || shape == "abort"
        || shape == "pressure"
        || (shape == "build_limit" && (root == "request" || root == "response"))
        || (shape == "late_abort" && (op == "encode" || op == "encode_frame"));
    const bool input_shape = input
      && (shape == "wire_max" || shape == "malformed" || shape == "context"
          || shape == "late_abort"
          || (op == "decode" && (shape == "exact" || shape == "short"))
          || (root == "error" && shape == "unknowns")
          || (root == "request"
              && (shape == "distributed" || shape == "churn"
                  || shape.starts_with("repeated_"))));
    require(ordinary || input_shape, "unknown control shape");
    if (input)
        decode_operation(name, kind, shape, op == "decode_frame");
    else {
        require(
          op == "make" || op == "encode" || op == "encode_frame",
          "unknown control operation");
        output_operation(name, kind, op, shape);
    }
}
} // namespace kwaque::protocol::testing
