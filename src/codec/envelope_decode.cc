#include "src/codec/envelope_decode.h"

#include "src/base/error.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/codec/crc32c_cooperative.h"
#include "src/codec/envelope_integrity.h"
#include "src/codec/header_extensions.h"

#include <seastar/core/coroutine.hh>

#include <cstdint>
#include <exception>
#include <optional>
#include <utility>

namespace kwaque::codec::detail {
namespace {

error framing_error(
  errc reason,
  field_context context,
  envelope_field field,
  std::uint64_t position) noexcept {
    return error{
      reason, context.family, static_cast<std::uint16_t>(field), position};
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
      errc::success, context, envelope_field::header_crc32c, start + 28);
    if (auto ready = co_await work.checkpoint(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    const auto remaining = admit_envelope_child(
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
              allocation_cost_error(shared.error(), context, start));
        }
        header.emplace(std::move(*shared));
        verified.emplace(
          co_await verify_envelope_header_crc(
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
      invariant_id{"KQ-ENVELOPE-HEADER-OUTCOME"},
      verified.has_value(),
      "header verification completed without an outcome");
    if (!verified->has_value()) {
        co_return codec::failure(verified->error());
    }
    co_return work.poll(anchor);
}

} // namespace

result<decode_budget> admit_envelope_child(
  const bytes::fragmented_buffer_parser& input,
  byte_count length,
  byte_count logical_limit,
  decode_budget memory,
  const limits& policy,
  field_context context) {
    const auto start = integer_read_start(
      input, context, input_boundary::complete);
    if (!start) {
        return codec::failure(start.error());
    }
    if (memory.charge == nullptr) {
        return codec::failure(
          error{errc::invalid_argument, context.family, context.field, *start});
    }
    if (length > logical_limit) {
        return codec::failure(
          error{
            errc::resource_exhausted, context.family, context.field, *start});
    }
    if (length > input.bytes_remaining()) {
        return codec::failure(integer_shortage(
          context,
          input_boundary::complete,
          context.origin + input.total_bytes().value()));
    }
    const auto cost = input.next_buffer_allocation_cost(length, memory.charge);
    if (!cost) {
        return codec::failure(
          allocation_cost_error(cost.error(), context, *start));
    }
    if (
      auto valid = validate_decode_cost(
        length, logical_limit, *cost, policy, context, *start);
      !valid) {
        return codec::failure(valid.error());
    }
    return consume_decode_budget(
      policy, memory, byte_count{}, cost->descriptors, context, *start);
}

seastar::future<result<envelope_body_extent>> inspect_envelope_in_transaction(
  bytes::fragmented_buffer_parser& input,
  format_family expected_family,
  envelope_extent_limits owner_limits,
  decode_budget memory,
  cooperative_work& work,
  field_context context,
  input_boundary boundary) {
    const auto start = integer_read_start(input, context, boundary);
    if (!start) {
        co_return codec::failure(start.error());
    }
    const auto anchor = framing_error(
      errc::success, context, envelope_field::magic, *start);
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    if (input.checkpoint_depth() == 0 || memory.charge == nullptr) {
        co_return codec::failure(framing_error(
          errc::invalid_argument, context, envelope_field::magic, *start));
    }
    if (
      auto admitted = co_await work.admit(
        envelope_prefix_work_bytes, envelope_prefix_work_items, anchor);
      !admitted) {
        co_return codec::failure(admitted.error());
    }
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    const auto prefix = peek_envelope_prefix(
      input, work.policy(), owner_limits, context, boundary);
    if (!prefix) {
        co_return codec::failure(prefix.error());
    }
    const byte_count header_bytes{prefix->header_bytes};
    if (header_bytes > input.bytes_remaining()) {
        co_return codec::failure(integer_shortage(
          field_context{
            .origin = context.origin,
            .family = context.family,
            .field = static_cast<std::uint16_t>(envelope_field::header_bytes)},
          boundary,
          context.origin + input.total_bytes().value()));
    }
    if (
      auto verified = co_await check_header(
        input, header_bytes, memory, work, context, *start);
      !verified) {
        co_return codec::failure(verified.error());
    }
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    // Header integrity now permits interpreting the family and compatibility
    // fields. None of these checks allocates a body or invokes its decoder.
    const auto writer_anchor = framing_error(
      errc::success, context, envelope_field::writer_version, *start + 6);
    const auto minimum_reader_anchor = framing_error(
      errc::success,
      context,
      envelope_field::minimum_reader_version,
      *start + 8);
    if (
      auto values = validate_sender_version_values(
        prefix->writer_version,
        prefix->minimum_reader_version,
        writer_anchor,
        minimum_reader_anchor);
      !values) {
        co_return codec::failure(values.error());
    }
    const auto descriptor = lookup_format(
      prefix->family,
      framing_error(
        errc::success, context, envelope_field::family, *start + 4));
    if (!descriptor) {
        co_return codec::failure(descriptor.error());
    }
    if (
      auto versions = validate_sender_versions(
        prefix->writer_version,
        prefix->minimum_reader_version,
        *descriptor,
        writer_anchor,
        minimum_reader_anchor);
      !versions) {
        co_return codec::failure(versions.error());
    }
    if (
      auto features = validate_required_features(
        prefix->required_features,
        *descriptor,
        framing_error(
          errc::success,
          context,
          envelope_field::required_features,
          *start + 16));
      !features) {
        co_return codec::failure(features.error());
    }
    const auto advanced = input.skip(byte_count{envelope_prefix_bytes});
    KWAQUE_INVARIANT(
      invariant_id{"KQ-ENVELOPE-HEADER-ADVANCE"},
      advanced.has_value(),
      "validated fixed header could not be advanced");
    if (
      auto extensions = co_await scan_header_extensions_in_transaction(
        input,
        byte_count{prefix->header_bytes - envelope_prefix_bytes},
        byte_count{envelope_prefix_bytes},
        work,
        context);
      !extensions) {
        co_return codec::failure(extensions.error());
    }
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    const byte_count body_bytes{prefix->body_bytes};
    const auto body_start = *start + header_bytes.value();
    const field_context body_context{
      .origin = body_start,
      .family = context.family,
      .field = static_cast<std::uint16_t>(envelope_field::body_bytes)};
    if (body_bytes > input.bytes_remaining()) {
        co_return codec::failure(integer_shortage(
          body_context,
          boundary,
          context.origin + input.total_bytes().value()));
    }
    const auto body_anchor = framing_error(
      errc::success, context, envelope_field::body_crc32c, *start + 24);
    if (auto ready = co_await work.checkpoint(body_anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    if (auto ready = work.poll(body_anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    const auto remaining = admit_envelope_child(
      input,
      body_bytes,
      owner_limits.max_body_bytes,
      memory,
      work.policy(),
      context);
    if (!remaining) {
        co_return codec::failure(remaining.error());
    }
    auto crc_input = input.peek_buffer(body_bytes);
    if (!crc_input) {
        co_return codec::failure(
          allocation_cost_error(crc_input.error(), body_context, body_start));
    }
    // This existing driver consumes and drains its alias before completion.
    // Its descriptor reservation can be reused only after the awaited call.
    const auto checksum = co_await crc32c_cooperatively(
      std::move(*crc_input), work, 0, body_anchor);
    if (!checksum) {
        co_return codec::failure(checksum.error());
    }
    if (auto ready = work.poll(body_anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    if (*checksum != prefix->body_crc32c) {
        co_return codec::failure(framing_error(
          errc::corrupt_data,
          context,
          envelope_field::body_crc32c,
          *start + 24));
    }
    if (prefix->family != static_cast<std::uint16_t>(expected_family)) {
        co_return codec::failure(framing_error(
          errc::wrong_context, context, envelope_field::family, *start + 4));
    }
    co_return envelope_body_extent{
      .bytes = body_bytes,
      .encoded_bytes = prefix->encoded_bytes(),
      .context = body_context};
}

} // namespace kwaque::codec::detail
