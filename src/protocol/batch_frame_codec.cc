#include "src/protocol/batch_frame_codec.h"

#include "src/protocol/frame_decode_internal.h"

#include <seastar/core/coroutine.hh>

#include <type_traits>
#include <utility>

namespace kwaque::protocol {
namespace {

template<bool Assigned>
using decoded_type = std::
  conditional_t<Assigned, decoded_assigned_frame, decoded_submitted_frame>;

template<bool Assigned>
struct batch_decoder final {
    model::batch_decode_expectation expected;

    seastar::future<codec::result<decoded_type<Assigned>>> operator()(
      bytes::fragmented_buffer_parser& input,
      const frame_header& header,
      codec::field_context context,
      codec::decode_budget memory,
      codec::cooperative_work& work) {
        if constexpr (Assigned) {
            auto decoded = co_await model::decode_assigned_batch(
              input,
              expected,
              memory,
              work,
              context,
              codec::input_boundary::complete);
            if (!decoded) co_return codec::failure(decoded.error());
            co_return decoded_assigned_frame{
              header,
              std::move(decoded->value),
              decoded->remaining,
              decoded->fingerprint_verification};
        } else {
            auto decoded = co_await model::decode_submitted_batch(
              input,
              expected,
              memory,
              work,
              context,
              codec::input_boundary::complete);
            if (!decoded) co_return codec::failure(decoded.error());
            co_return decoded_submitted_frame{
              header, std::move(decoded->value), decoded->remaining};
        }
    }
};

template<bool Assigned>
seastar::future<frame_read_result<decoded_type<Assigned>>> decode_batch_frame(
  bytes::fragmented_buffer_parser& input,
  model::batch_decode_expectation expected,
  frame_extent_limits owner_limits,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context,
  codec::input_boundary boundary) {
    const auto start = codec::detail::integer_read_start(
      input, context, boundary);
    const auto fail = [](codec::error error) {
        return seastar::make_ready_future<
          frame_read_result<decoded_type<Assigned>>>(codec::failure(error));
    };
    if (!start) return fail(start.error());
    const auto anchor = detail::frame_error(
      errc::success, context, frame_field::payload_bytes, *start);
    if (auto ready = work.poll(anchor); !ready) return fail(ready.error());
    // Preserve the model boundary's independent-expectation preflight before
    // framing. No decoded identity or transport stream supplies expectations.
    if (expected.topic.is_nil() || expected.range.is_nil()
        || (expected.original_binding && (expected.original_binding->topic() != expected.topic
            || expected.original_binding->range() != expected.range)))
        return fail(
          detail::frame_error(
            errc::invalid_argument,
            context,
            frame_field::payload_bytes,
            *start));
    constexpr auto kind = Assigned ? frame_kind::assigned_batch
                                   : frame_kind::submitted_batch;
    return detail::decode_frame_payload<decoded_type<Assigned>>(
      input,
      kind,
      owner_limits,
      memory,
      work,
      batch_decoder<Assigned>{std::move(expected)},
      context,
      boundary);
}

} // namespace

seastar::future<frame_read_result<decoded_submitted_frame>>
decode_submitted_frame(
  bytes::fragmented_buffer_parser& input,
  model::batch_decode_expectation expected,
  frame_extent_limits owner_limits,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context,
  codec::input_boundary boundary) {
    return decode_batch_frame<false>(
      input,
      std::move(expected),
      owner_limits,
      memory,
      work,
      context,
      boundary);
}

seastar::future<frame_read_result<decoded_assigned_frame>>
decode_assigned_frame(
  bytes::fragmented_buffer_parser& input,
  model::batch_decode_expectation expected,
  frame_extent_limits owner_limits,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context,
  codec::input_boundary boundary) {
    return decode_batch_frame<true>(
      input,
      std::move(expected),
      owner_limits,
      memory,
      work,
      context,
      boundary);
}

} // namespace kwaque::protocol
