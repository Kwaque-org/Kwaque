#include "src/codec/tests/envelope_bench_fixture.h"
#include "src/codec/tests/envelope_bench_native.h"

namespace kwaque::codec::bench {

seastar::future<result<envelope_decoded_body>> envelope_body_reader::operator()(
  bytes::fragmented_buffer_parser& input,
  field_context context,
  input_boundary boundary,
  decode_budget memory,
  cooperative_work& work) const {
    const error anchor{
      errc::success, context.family, context.field, context.origin};
    if (
      auto admitted = co_await work.admit(
        byte_count{80}, item_count{40}, anchor);
      !admitted) {
        co_return codec::failure(admitted.error());
    }
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    const auto object = read_le<std::uint64_t>(input, context, boundary);
    if (!object) {
        co_return codec::failure(object.error());
    }
    const auto generation = read_le<std::uint64_t>(input, context, boundary);
    if (!generation) {
        co_return codec::failure(generation.error());
    }
    const auto length = read_le<std::uint32_t>(input, context, boundary);
    if (!length) {
        co_return codec::failure(length.error());
    }
    if (*object != expected.object || *generation != expected.generation) {
        co_return codec::failure(
          error{
            errc::wrong_context,
            context.family,
            context.field,
            context.origin});
    }
    if (byte_count{*length} != input.bytes_remaining()) {
        co_return codec::failure(
          error{
            errc::malformed_data,
            context.family,
            context.field,
            context.origin + input.bytes_consumed().value()});
    }
    if (auto ready = co_await work.checkpoint(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    const auto remaining = codec::detail::admit_envelope_child(
      input,
      byte_count{*length},
      byte_count{*length},
      memory,
      work.policy(),
      context);
    if (!remaining) {
        co_return codec::failure(remaining.error());
    }
    auto payload = input.read_buffer(byte_count{*length});
    if (!payload) {
        co_return codec::failure(
          codec::detail::allocation_cost_error(
            payload.error(),
            context,
            context.origin + input.bytes_consumed().value()));
    }
    if (auto ready = work.poll(anchor); !ready) {
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        payload = bytes::fragmented_buffer{};
        co_return codec::failure(ready.error());
    }
    co_return envelope_decoded_body{*object, *generation, std::move(*payload)};
}

result<unverified_envelope_prefix> checked_prefix_read(
  const bytes::fragmented_buffer_parser& input,
  const limits& policy,
  envelope_extent_limits bounds,
  field_context context,
  input_boundary boundary) {
    return checked::native_prefix_read(
      input, policy, bounds, context, boundary);
}
result<unverified_envelope_prefix> codec_prefix_read(
  const bytes::fragmented_buffer_parser& input,
  const limits& policy,
  envelope_extent_limits bounds,
  field_context context,
  input_boundary boundary) {
    return peek_envelope_prefix(input, policy, bounds, context, boundary);
}
result<encoded_envelope_prefix> checked_prefix_write(
  envelope_prefix_fields fields,
  const limits& policy,
  envelope_extent_limits bounds,
  field_context context) {
    return checked::native_prefix_write(fields, policy, bounds, context);
}
result<encoded_envelope_prefix> codec_prefix_write(
  envelope_prefix_fields fields,
  const limits& policy,
  envelope_extent_limits bounds,
  field_context context) {
    return encode_envelope_prefix(fields, policy, bounds, context);
}
seastar::future<result<item_count>> checked_extensions(
  bytes::fragmented_buffer_parser& input,
  byte_count extensions,
  byte_count fixed,
  cooperative_work& work,
  field_context context) {
    return checked::native_extensions(input, extensions, fixed, work, context);
}
seastar::future<result<item_count>> codec_extensions(
  bytes::fragmented_buffer_parser& input,
  byte_count extensions,
  byte_count fixed,
  cooperative_work& work,
  field_context context) {
    return scan_header_extensions_in_transaction(
      input, extensions, fixed, work, context);
}
seastar::future<result<envelope_decoded_body>> checked_owned_decode(
  bytes::fragmented_buffer_parser& input,
  format_family family,
  envelope_extent_limits bounds,
  decode_budget memory,
  cooperative_work& work,
  envelope_expected_body expected,
  field_context context,
  input_boundary boundary) {
    return checked::native_decode<envelope_decoded_body>(
      input,
      family,
      bounds,
      memory,
      work,
      envelope_body_reader{expected},
      context,
      boundary);
}
seastar::future<result<envelope_decoded_body>> codec_owned_decode(
  bytes::fragmented_buffer_parser& input,
  format_family family,
  envelope_extent_limits bounds,
  decode_budget memory,
  cooperative_work& work,
  envelope_expected_body expected,
  field_context context,
  input_boundary boundary) {
    return decode_envelope<envelope_decoded_body>(
      input,
      family,
      bounds,
      memory,
      work,
      envelope_body_reader{expected},
      context,
      boundary);
}
seastar::future<result<bytes::fragmented_buffer>> checked_owned_encode(
  bytes::fragmented_buffer&& body,
  format_family family,
  cooperative_work& work,
  envelope_extent_limits bounds,
  operation_usage live,
  byte_count remaining,
  bytes::allocation_charge_fn charge,
  field_context context) {
    return checked::native_encode(
      std::move(body), family, work, bounds, live, remaining, charge, context);
}
seastar::future<result<bytes::fragmented_buffer>> codec_owned_encode(
  bytes::fragmented_buffer&& body,
  format_family family,
  cooperative_work& work,
  envelope_extent_limits bounds,
  operation_usage live,
  byte_count remaining,
  bytes::allocation_charge_fn charge,
  field_context context) {
    return encode_envelope(
      std::move(body), family, work, bounds, live, remaining, charge, context);
}

} // namespace kwaque::codec::bench
