#include "src/storage/segment_immutable.h"

#include "src/codec/envelope.h"
#include "src/storage/local_paths.h"

namespace kwaque::storage {
seastar::future<runtime::result<verified_extent>> verify_segment_immutable(
  runtime::file& file,
  const sealed_footer& root,
  segment_immutable_expectation expected,
  local_layout_kind layout,
  codec::decode_budget limits,
  codec::cooperative_work& work) {
    if (
      !limits.charge
      || limits.operation_remaining > work.policy().config().max_operation_bytes
      || limits.metadata_remaining > limits.operation_remaining)
        co_return runtime::failure(detail::path_error(errc::invalid_argument));
    if (
      root.coverage() != expected.coverage
      || root.extent_digest() != expected.digest
      || root.location().position != expected.coverage.bytes().end()
      || root.encoded_extent().end() != expected.file_end
      || (layout != local_layout_kind::initial && layout != local_layout_kind::rewrite))
        co_return runtime::failure(detail::path_error(errc::wrong_context));
    auto size = co_await file.size();
    if (!size) co_return runtime::failure(size.error());
    if (*size != expected.file_end.value())
        co_return runtime::failure(detail::path_error(errc::wrong_context));
    const auto history = root.location().history;
    auto made = extent_verifier::make(
      history,
      expected.coverage,
      work.policy(),
      layout == local_layout_kind::initial ? extent_layout_kind::initial_append
                                           : extent_layout_kind::rewrite,
      {},
      extent_integrity::crc32c_and_digest);
    if (!made)
        co_return runtime::failure(detail::path_error(made.error().code()));
    auto verifier = std::move(*made);
    auto at = expected.coverage.bytes().begin();
    while (at < expected.coverage.bytes().end()) {
        if (auto ready = co_await detail::path_checkpoint(work); !ready)
            co_return runtime::failure(ready.error());
        const byte_count remaining{
          expected.coverage.bytes().end().value() - at.value()};
        if (remaining < byte_count{codec::envelope_prefix_bytes})
            co_return runtime::failure(
              detail::path_error(errc::truncated_data));
        std::optional<codec::unverified_envelope_prefix> prefix;
        {
            auto read = co_await file.read(
              at, byte_count{codec::envelope_prefix_bytes});
            if (!read) co_return runtime::failure(read.error());
            bytes::fragmented_buffer_parser input{std::move(*read).take_data()};
            auto parsed = codec::peek_envelope_prefix(
              input,
              work.policy(),
              {remaining, remaining},
              {},
              codec::input_boundary::complete);
            if (!parsed)
                co_return runtime::failure(
                  detail::path_error(parsed.error().code()));
            prefix = *parsed;
        }
        const auto length = prefix->encoded_bytes();
        // Leave a separate decode budget; no allocation follows an unbounded
        // untrusted length. Runtime read buffers are themselves chunked.
        if (length > byte_count{limits.operation_remaining.value() / 2})
            co_return runtime::failure(
              detail::path_error(errc::resource_exhausted));
        auto read = co_await file.read(at, length);
        if (!read) co_return runtime::failure(read.error());
        if (read->data().size() != length)
            co_return runtime::failure(
              detail::path_error(errc::truncated_data));
        auto raw = std::move(*read).take_data();
        const auto cost = raw.allocation_cost(limits.charge);
        if (!cost)
            co_return runtime::failure(
              detail::path_error(errc::resource_exhausted));
        const auto metadata = cost->descriptors.checked_add(
          cost->share_controls);
        const auto total = metadata ? metadata->checked_add(cost->backing)
                                    : std::nullopt;
        if (
          !total || *total > limits.operation_remaining
          || *metadata > limits.metadata_remaining
          || cost->largest_allocation.value()
               > maximum_contiguous_allocation_bytes)
            co_return runtime::failure(
              detail::path_error(errc::resource_exhausted));
        const codec::decode_budget memory{
          limits.operation_remaining.checked_sub(*total).value(),
          limits.metadata_remaining.checked_sub(*metadata).value(),
          limits.charge};
        codec::result<void> verified;
        if (
          prefix->family
          == static_cast<std::uint16_t>(
            codec::format_family::segment_batch_block))
            verified = co_await verifier.add_block(
              std::move(raw),
              {history.segment.topic(), history.segment.range()},
              memory,
              work);
        else if (
          prefix->family
          == static_cast<std::uint16_t>(
            codec::format_family::durable_boundary_footer))
            verified = co_await verifier.add_footer(
              std::move(raw), memory, work);
        else
            co_return runtime::failure(
              detail::path_error(errc::unsupported_format));
        if (!verified)
            co_return runtime::failure(
              detail::path_error(verified.error().code()));
        at = at.checked_add(length).value();
    }
    auto verified = verifier.finish(work);
    if (!verified)
        co_return runtime::failure(detail::path_error(verified.error().code()));
    auto valid = validate_sealed_footer(root, *verified);
    if (!valid)
        co_return runtime::failure(detail::path_error(valid.error().code()));
    size = co_await file.size();
    if (!size) co_return runtime::failure(size.error());
    if (*size != expected.file_end.value())
        co_return runtime::failure(detail::path_error(errc::wrong_context));
    co_return *verified;
}
} // namespace kwaque::storage
