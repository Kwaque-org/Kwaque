#include "src/codec/crc32c_cooperative.h"
#include "src/protocol/frame_decode_internal.h"

#include <seastar/core/coroutine.hh>

namespace kwaque::protocol {
namespace detail {

codec::decode_budget release_payload_parser_charge(
  codec::decode_budget original,
  codec::decode_budget child,
  codec::decode_budget result) noexcept {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-FRAME-RESULT-BUDGET"},
      original.charge == child.charge && child.charge == result.charge
        && child.operation_remaining <= original.operation_remaining
        && child.metadata_remaining <= original.metadata_remaining
        && result.operation_remaining <= child.operation_remaining
        && result.metadata_remaining <= child.metadata_remaining,
      "payload result widened its admitted allowance");
    // Differences are bounded above by original, so neither sum can overflow.
    result.operation_remaining = byte_count{
      original.operation_remaining.value() - child.operation_remaining.value()
      + result.operation_remaining.value()};
    result.metadata_remaining = byte_count{
      original.metadata_remaining.value() - child.metadata_remaining.value()
      + result.metadata_remaining.value()};
    return result;
}

seastar::future<frame_read_result<frame_header>> inspect_frame_in_transaction(
  bytes::fragmented_buffer_parser& input,
  std::optional<frame_kind> expected_kind,
  frame_extent_limits owner_limits,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context,
  codec::input_boundary boundary,
  std::uint64_t start) {
    auto inspected = co_await inspect_frame_header_in_transaction(
      input, owner_limits, memory, work, context, boundary, start);
    if (!inspected) co_return codec::failure(inspected.error());
    if (std::holds_alternative<need_more>(*inspected)) co_return inspected;
    const auto anchor = frame_error(
      errc::success, context, frame_field::payload_crc32c, start + 44);
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    const auto header = std::get<frame_header>(*inspected);
    if (header.payload_bytes > input.bytes_remaining()) {
        if (boundary == codec::input_boundary::open)
            co_return need_more{byte_count{
              header.payload_bytes.value() - input.bytes_remaining().value()}};
        co_return codec::failure(frame_error(
          errc::malformed_data,
          context,
          frame_field::payload_bytes,
          context.origin + input.total_bytes().value()));
    }
    if (auto ready = co_await work.checkpoint(anchor); !ready)
        co_return codec::failure(ready.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    const auto remaining = codec::detail::admit_envelope_child(
      input,
      header.payload_bytes,
      header.payload_bytes,
      memory,
      work.policy(),
      context);
    if (!remaining) co_return codec::failure(remaining.error());
    auto alias = input.peek_buffer(header.payload_bytes);
    if (!alias)
        co_return codec::failure(
          codec::detail::allocation_cost_error(alias.error(), context, start));
    const auto checksum = co_await codec::crc32c_cooperatively(
      std::move(*alias), work, 0, anchor);
    if (!checksum) co_return codec::failure(checksum.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    if (*checksum != header.payload_crc32c)
        co_return codec::failure(frame_error(
          errc::corrupt_data,
          context,
          frame_field::payload_crc32c,
          start + 44));
    if (expected_kind && header.metadata.kind != *expected_kind)
        co_return codec::failure(frame_error(
          errc::wrong_context, context, frame_field::kind, start + 6));
    co_return header;
}

} // namespace detail
namespace {

struct payload_decoder final {
    seastar::future<codec::result<framed_payload>> operator()(
      bytes::fragmented_buffer_parser& input,
      const frame_header& header,
      codec::field_context context,
      codec::decode_budget memory,
      codec::cooperative_work& work) {
        const auto anchor = detail::frame_error(
          errc::success, context, frame_field::payload_bytes, context.origin);
        if (auto ready = co_await work.checkpoint(anchor); !ready)
            co_return codec::failure(ready.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        const auto remaining = codec::detail::admit_envelope_child(
          input,
          header.payload_bytes,
          header.payload_bytes,
          memory,
          work.policy(),
          context);
        if (!remaining) co_return codec::failure(remaining.error());
        auto payload = input.read_buffer(header.payload_bytes);
        if (!payload)
            co_return codec::failure(
              codec::detail::allocation_cost_error(
                payload.error(), context, context.origin));
        // The zero-length cost range counts actual retained descriptors without
        // revisiting backing. The larger construction peak was admitted above.
        const auto cost = payload->allocation_cost(0, 0, memory.charge);
        KWAQUE_INVARIANT(
          invariant_id{"KQ-FRAME-PAYLOAD-COST"},
          cost.has_value(),
          "admitted payload descriptor cost is not representable");
        const auto retained = codec::detail::consume_decode_budget(
          work.policy(),
          memory,
          byte_count{},
          cost->descriptors,
          context,
          context.origin);
        KWAQUE_INVARIANT(
          invariant_id{"KQ-FRAME-PAYLOAD-RESIDUAL"},
          retained.has_value(),
          "retained payload descriptors exceeded their construction admission");
        // The outer driver owns final cleanup, polling and publication. This
        // private return cannot escape before its temporary parser is released.
        co_return framed_payload{header, std::move(*payload), *retained};
    }
};

} // namespace

seastar::future<frame_read_result<framed_payload>> decode_frame(
  bytes::fragmented_buffer_parser& input,
  frame_extent_limits owner_limits,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context,
  codec::input_boundary boundary) {
    return detail::decode_frame_payload<framed_payload>(
      input,
      std::nullopt,
      owner_limits,
      memory,
      work,
      payload_decoder{},
      context,
      boundary);
}

} // namespace kwaque::protocol
