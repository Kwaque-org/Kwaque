#include "src/model/tests/model_bench_reference.h"

#include "src/codec/tests/envelope_bench_native.h"
#include "src/model/batch_decode_body.h"

#include <type_traits>

namespace kwaque::model::bench {
namespace {
template<bool Assigned>
using decoded = std::
  conditional_t<Assigned, decoded_assigned_batch, decoded_submitted_batch>;
template<bool Assigned>
struct body_reader {
    batch_decode_expectation expected;
    codec::decode_budget original;
    seastar::future<codec::result<decoded<Assigned>>> operator()(
      bytes::fragmented_buffer_parser& input,
      codec::field_context context,
      codec::input_boundary,
      codec::decode_budget remaining,
      codec::cooperative_work& work) const {
        if constexpr (Assigned)
            return detail::decode_assigned_body(
              input, expected, original, remaining, work, context);
        else
            return detail::decode_submitted_body(
              input, expected, original, remaining, work, context);
    }
};
template<bool Assigned>
seastar::future<codec::result<decoded<Assigned>>> checked_decode(
  bytes::fragmented_buffer_parser& input,
  batch_decode_expectation expected,
  codec::decode_budget memory,
  codec::cooperative_work& work) {
    constexpr auto family = Assigned ? codec::format_family::assigned_batch
                                     : codec::format_family::submitted_batch;
    const codec::field_context context{
      .family = static_cast<std::uint16_t>(family)};
    const auto start = codec::detail::integer_read_start(
      input, context, codec::input_boundary::open);
    const auto fail = [](codec::error error) {
        return seastar::make_ready_future<codec::result<decoded<Assigned>>>(
          codec::failure(error));
    };
    if (!start) return fail(start.error());
    const codec::error anchor{
      errc::success,
      context.family,
      static_cast<std::uint16_t>(batch_field::fixed_body),
      *start};
    if (auto ready = work.poll(anchor); !ready) return fail(ready.error());
    if (expected.topic.is_nil() || expected.range.is_nil() || (expected.original_binding &&
        (expected.original_binding->topic()!=expected.topic || expected.original_binding->range()!=expected.range)))
        return fail(
          codec::error{
            errc::invalid_argument,
            anchor.family(),
            anchor.field(),
            anchor.byte_offset()});
    const byte_count body{
      (Assigned ? assigned_batch_fixed_bytes : submitted_batch_fixed_bytes)
        .value()
      + work.policy().config().max_expanded_batch_bytes.value()};
    return codec::bench::checked::native_decode<decoded<Assigned>>(
      input,
      family,
      {body,
       byte_count{
         body.value() + work.policy().config().max_header_bytes.value()}},
      memory,
      work,
      body_reader<Assigned>{expected, memory},
      context);
}
} // namespace
seastar::future<codec::result<decoded_submitted_batch>>
checked_decode_submitted(
  bytes::fragmented_buffer_parser& input,
  batch_decode_expectation expected,
  codec::decode_budget memory,
  codec::cooperative_work& work) {
    return checked_decode<false>(input, expected, memory, work);
}
seastar::future<codec::result<decoded_submitted_batch>> codec_decode_submitted(
  bytes::fragmented_buffer_parser& input,
  batch_decode_expectation expected,
  codec::decode_budget memory,
  codec::cooperative_work& work) {
    return decode_submitted_batch(input, expected, memory, work);
}
seastar::future<codec::result<decoded_assigned_batch>> checked_decode_assigned(
  bytes::fragmented_buffer_parser& input,
  batch_decode_expectation expected,
  codec::decode_budget memory,
  codec::cooperative_work& work) {
    return checked_decode<true>(input, expected, memory, work);
}
seastar::future<codec::result<decoded_assigned_batch>> codec_decode_assigned(
  bytes::fragmented_buffer_parser& input,
  batch_decode_expectation expected,
  codec::decode_budget memory,
  codec::cooperative_work& work) {
    return decode_assigned_batch(input, expected, memory, work);
}
seastar::future<codec::result<bytes::fragmented_buffer>> codec_encode_submitted(
  submitted_batch&& value,
  codec::cooperative_work& work,
  byte_count remaining,
  bytes::allocation_charge_fn charge,
  codec::field_context context) {
    return encode_submitted_batch(
      std::move(value), work, remaining, charge, context);
}
seastar::future<codec::result<bytes::fragmented_buffer>> codec_encode_assigned(
  assigned_batch&& value,
  codec::cooperative_work& work,
  byte_count remaining,
  bytes::allocation_charge_fn charge,
  codec::field_context context) {
    return encode_assigned_batch(
      std::move(value), work, remaining, charge, context);
}
} // namespace kwaque::model::bench
