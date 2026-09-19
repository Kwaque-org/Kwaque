#include "src/protocol/control_frame_codec.h"

#include "src/codec/framing_internal.h"
#include "src/protocol/control_internal.h"
#include "src/protocol/frame_decode_internal.h"

#include <seastar/core/coroutine.hh>

#include <exception>
#include <optional>
#include <utility>

namespace kwaque::protocol {
namespace {
template<frame_kind Kind>
struct control_decoder final {
    control_expectation expected;
    seastar::future<codec::result<decoded_control_frame>> operator()(
      bytes::fragmented_buffer_parser& input,
      const frame_header& header,
      codec::field_context context,
      codec::decode_budget memory,
      codec::cooperative_work& work) {
        auto decoded = co_await decode_control(
          input, Kind, expected, memory, work, context);
        if (!decoded) co_return codec::failure(decoded.error());
        co_return decoded_control_frame{
          header, std::move(decoded->value), decoded->remaining};
    }
};
template<frame_kind Kind>
auto decode(
  bytes::fragmented_buffer_parser& input,
  control_expectation expected,
  frame_extent_limits bounds,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context,
  codec::input_boundary boundary) {
    return detail::decode_frame_payload<decoded_control_frame>(
      input,
      Kind,
      bounds,
      memory,
      work,
      control_decoder<Kind>{expected},
      context,
      boundary);
}
} // namespace

seastar::future<frame_read_result<decoded_control_frame>> decode_control_frame(
  bytes::fragmented_buffer_parser& input,
  frame_kind kind,
  control_expectation expected,
  frame_extent_limits bounds,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context,
  codec::input_boundary boundary) {
    const auto fail = [](codec::error error) {
        return seastar::make_ready_future<
          frame_read_result<decoded_control_frame>>(codec::failure(error));
    };
    const auto start = codec::detail::integer_read_start(
      input, context, boundary);
    if (!start) return fail(start.error());
    const codec::error anchor{
      errc::success, context.family, context.field, *start};
    if (auto ready = work.poll(anchor); !ready) return fail(ready.error());
    if (!detail::valid_control_expectation(kind, expected))
        return fail(
          codec::error{
            errc::invalid_argument, context.family, context.field, *start});
    switch (kind) {
    case frame_kind::handshake_request:
        return decode<frame_kind::handshake_request>(
          input, expected, bounds, memory, work, context, boundary);
    case frame_kind::handshake_response:
        return decode<frame_kind::handshake_response>(
          input, expected, bounds, memory, work, context, boundary);
    case frame_kind::redirect:
        return decode<frame_kind::redirect>(
          input, expected, bounds, memory, work, context, boundary);
    case frame_kind::error:
        return decode<frame_kind::error>(
          input, expected, bounds, memory, work, context, boundary);
    case frame_kind::submitted_batch:
    case frame_kind::assigned_batch:
        break;
    }
    return fail(
      codec::error{
        errc::invalid_argument, context.family, context.field, *start});
}

seastar::future<codec::result<bytes::fragmented_buffer>> encode_control_frame(
  const control& value,
  model::correlation_id correlation,
  model::frame_sequence sequence,
  codec::cooperative_work& work,
  frame_extent_limits bounds,
  codec::operation_usage other_live,
  byte_count parent_remaining,
  bytes::allocation_charge_fn charge,
  codec::field_context context) {
    using codec::detail::framing::encode_error;
    const auto anchor = encode_error(errc::success, context);
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    if (context.origin > UINT64_MAX - frame_prefix_bytes)
        co_return codec::failure(encode_error(errc::invalid_argument, context));
    auto payload_context = context;
    payload_context.origin += frame_prefix_bytes;
    const auto layout = co_await detail::inspect_control_value(
      value.data(), work, charge, payload_context);
    if (!layout) co_return codec::failure(layout.error());
    if (
      auto ready = co_await work.admit(
        frame_prefix_work_bytes, frame_prefix_work_items, anchor);
      !ready)
        co_return codec::failure(ready.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    const frame_metadata metadata{
      layout->kind, model::transport_stream_id{}, correlation, sequence};
    auto prefix = encode_frame_prefix(
      {metadata, layout->wire_bytes, 0, 0}, work.policy(), bounds, context);
    if (!prefix) co_return codec::failure(prefix.error());
    auto frame_live = other_live;
    if (
      auto added = codec::detail::framing::add_charge(
        frame_live.decoded_metadata, layout->storage, context);
      !added)
        co_return codec::failure(added.error());
    std::optional<bytes::fragmented_buffer> payload;
    std::optional<bytes::fragmented_buffer> output;
    std::optional<codec::error> failed;
    std::exception_ptr exception;
    try {
        do {
            auto encoded = co_await detail::encode_control_checked(
              value.data(),
              *layout,
              work,
              other_live,
              parent_remaining,
              charge,
              payload_context);
            if (!encoded) {
                failed = encoded.error();
                break;
            }
            payload.emplace(std::move(*encoded));
            if (auto ready = work.poll(anchor); !ready) {
                failed = ready.error();
                break;
            }
            auto framed = co_await encode_frame(
              std::move(*payload),
              metadata,
              work,
              bounds,
              frame_live,
              parent_remaining,
              charge,
              context);
            if (!framed) {
                failed = framed.error();
                break;
            }
            output.emplace(std::move(*framed));
        } while (false);
    } catch (...) {
        exception = std::current_exception();
    }
    if (payload) {
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        payload.reset();
    }
    if (!failed && !exception) {
        if (auto ready = work.poll(anchor); !ready) failed = ready.error();
    }
    if (failed || exception) {
        if (output) {
            co_await work.drain_inline(
              work.byte_quantum(), work.item_quantum());
            output.reset();
        }
        if (exception) std::rethrow_exception(exception);
        co_return codec::failure(*failed);
    }
    co_return std::move(*output);
}
} // namespace kwaque::protocol
