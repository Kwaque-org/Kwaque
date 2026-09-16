#include "src/base/invariant.h"
#include "src/codec/envelope_decode.h"
#include "src/codec/header_extensions.h"
#include "src/codec/header_integrity.h"
#include "src/protocol/frame_codec.h"
#include "src/protocol/frame_decode_internal.h"

#include <seastar/core/coroutine.hh>

#include <cstdint>
#include <exception>
#include <optional>
#include <utility>

namespace kwaque::protocol {
using codec::cooperative_work;
using codec::decode_budget;
using codec::error;
using codec::field_context;
using codec::input_boundary;
using codec::result;
namespace {

error framing_error(
  errc code,
  field_context context,
  frame_field field,
  std::uint64_t offset) noexcept {
    return error{
      code, context.family, static_cast<std::uint16_t>(field), offset};
}

// The only owner here is the header alias. Keep its cleanup outside the try
// block so native failures cannot bypass joined teardown. No body is shared.
seastar::future<result<void>> check_header(
  bytes::fragmented_buffer_parser& input,
  byte_count header_bytes,
  decode_budget memory,
  cooperative_work& work,
  field_context context,
  std::uint64_t start) {
    const auto anchor = framing_error(
      errc::success,
      context,
      frame_field::header_crc32c,
      start + frame_header_crc_offset);
    if (auto ready = co_await work.checkpoint(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    const auto remaining = codec::detail::admit_envelope_child(
      input, header_bytes, header_bytes, memory, work.policy(), context);
    if (!remaining) {
        co_return codec::failure(remaining.error());
    }
    std::optional<bytes::fragmented_buffer> header;
    std::optional<result<void>> verified;
    std::exception_ptr exception;
    try {
        auto shared = input.peek_buffer(header_bytes);
        if (!shared) {
            co_return codec::failure(
              codec::detail::allocation_cost_error(
                shared.error(), context, start));
        }
        header.emplace(std::move(*shared));
        verified.emplace(
          co_await verify_frame_header_crc(
            *header,
            work,
            field_context{
              .origin = start,
              .family = context.family,
              .field = context.field}));
    } catch (...) {
        exception = std::current_exception();
    }
    if (header) {
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        header.reset();
    }
    if (exception) {
        std::rethrow_exception(exception);
    }
    KWAQUE_INVARIANT(
      invariant_id{"KQ-FRAME-HEADER-OUTCOME"},
      verified.has_value(),
      "header verification completed without an outcome");
    if (!verified->has_value()) {
        co_return codec::failure(verified->error());
    }
    co_return work.poll(anchor);
}

} // namespace

namespace detail {

seastar::future<frame_read_result<frame_header>>
inspect_frame_header_in_transaction(
  bytes::fragmented_buffer_parser& input,
  frame_extent_limits owner_limits,
  decode_budget memory,
  cooperative_work& work,
  field_context context,
  input_boundary boundary,
  std::uint64_t start) {
    const auto anchor = framing_error(
      errc::success, context, frame_field::magic, start);
    if (
      auto admitted = co_await work.admit(
        frame_prefix_work_bytes, frame_prefix_work_items, anchor);
      !admitted)
        co_return codec::failure(admitted.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    const auto prefix = peek_frame_prefix(
      input, work.policy(), owner_limits, context, boundary);
    if (!prefix) {
        if (prefix.error().code() == errc::truncated_data)
            co_return need_more{
              byte_count{frame_prefix_bytes - input.bytes_remaining().value()}};
        co_return codec::failure(prefix.error());
    }
    const byte_count header_bytes{prefix->header_bytes};
    if (header_bytes > input.bytes_remaining()) {
        if (boundary == input_boundary::open)
            co_return need_more{byte_count{
              header_bytes.value() - input.bytes_remaining().value()}};
        co_return codec::failure(
          codec::detail::integer_shortage(
            field_context{
              .origin = context.origin,
              .family = context.family,
              .field = static_cast<std::uint16_t>(frame_field::header_bytes)},
            boundary,
            context.origin + input.total_bytes().value()));
    }
    if (
      auto checked = co_await check_header(
        input, header_bytes, memory, work, context, start);
      !checked)
        co_return codec::failure(checked.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    if (prefix->protocol_version != frame_protocol_version)
        co_return codec::failure(framing_error(
          errc::unsupported_format,
          context,
          frame_field::protocol_version,
          start + 4));
    const auto descriptor = lookup_frame_kind(
      prefix->kind,
      framing_error(errc::success, context, frame_field::kind, start + 6));
    if (!descriptor) co_return codec::failure(descriptor.error());
    if (prefix->flags != 0)
        co_return codec::failure(framing_error(
          errc::unsupported_format, context, frame_field::flags, start + 10));
    if (
      descriptor->connection_control()
      && prefix->payload_bytes
           > work.policy().config().max_control_bytes.value())
        co_return codec::failure(framing_error(
          errc::resource_exhausted,
          context,
          frame_field::payload_bytes,
          start + 12));
    if (descriptor->connection_control() != (prefix->stream == 0))
        co_return codec::failure(framing_error(
          errc::malformed_data, context, frame_field::stream, start + 16));
    const auto advanced = input.skip(byte_count{frame_prefix_bytes});
    KWAQUE_INVARIANT(
      invariant_id{"KQ-FRAME-HEADER-ADVANCE"},
      advanced.has_value(),
      "validated fixed header could not be advanced");
    if (
      auto extensions = co_await codec::scan_header_extensions_in_transaction(
        input,
        byte_count{prefix->header_bytes - frame_prefix_bytes},
        byte_count{frame_prefix_bytes},
        work,
        context);
      !extensions)
        co_return codec::failure(extensions.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    co_return frame_header{
      .metadata = {descriptor->kind, model::transport_stream_id{prefix->stream},
        model::correlation_id{prefix->correlation}, model::frame_sequence{prefix->sequence}},
      .header_bytes = header_bytes,
      .payload_bytes = byte_count{prefix->payload_bytes},
      .header_crc32c = prefix->header_crc32c,
      .payload_crc32c = prefix->payload_crc32c};
}

} // namespace detail

seastar::future<result<void>> verify_frame_header_crc(
  const bytes::fragmented_buffer& header,
  cooperative_work& work,
  field_context context) {
    return codec::detail::verify_header_crc(
      header,
      work,
      context,
      {frame_prefix_bytes,
       8,
       frame_header_crc_offset,
       static_cast<std::uint16_t>(frame_field::header_bytes),
       static_cast<std::uint16_t>(frame_field::header_crc32c)});
}

seastar::future<result<frame_header>> inspect_frame_header(
  bytes::fragmented_buffer_parser& input,
  frame_extent_limits owner_limits,
  decode_budget memory,
  cooperative_work& work,
  field_context context,
  input_boundary boundary) {
    const auto start = codec::detail::integer_read_start(
      input, context, boundary);
    if (!start) co_return codec::failure(start.error());
    const auto anchor = framing_error(
      errc::success, context, frame_field::magic, *start);
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    if (memory.charge == nullptr)
        co_return codec::failure(framing_error(
          errc::invalid_argument, context, frame_field::magic, *start));
    const auto depth = input.checkpoint_depth();
    if (auto marked = input.push_checkpoint(); !marked)
        co_return codec::failure(
          codec::detail::allocation_cost_error(
            marked.error(), context, *start));
    codec::detail::parser_transaction_guard transaction{input, depth};
    auto inspected = co_await detail::inspect_frame_header_in_transaction(
      input, owner_limits, memory, work, context, boundary, *start);
    if (!inspected) co_return codec::failure(inspected.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    if (std::holds_alternative<need_more>(*inspected)) {
        co_return codec::failure(framing_error(
          errc::truncated_data,
          context,
          input.bytes_remaining().value() < frame_prefix_bytes
            ? frame_field::magic
            : frame_field::header_bytes,
          context.origin + input.total_bytes().value()));
    }
    // This inspection never commits. The guard restores even successful reads.
    co_return std::get<frame_header>(*inspected);
}

} // namespace kwaque::protocol
