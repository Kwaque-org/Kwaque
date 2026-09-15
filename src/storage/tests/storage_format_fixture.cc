#include "src/storage/tests/storage_format_fixture.h"

#include <stdexcept>

namespace kwaque::storage::testing {
namespace {
void require(bool valid) {
    if (!valid) __builtin_trap();
}
} // namespace
storage_fixture::storage_fixture(
  storage_case selected,
  bool compressed,
  bool sparse,
  std::size_t outer_header,
  std::size_t inner_header,
  std::uint64_t a,
  std::uint64_t wal_a,
  std::uint32_t retry_count)
  : kind(selected)
  , header(
      segment_header::make(sc(), model::range_logical_end{100}, alignment(a))
        .value())
  , block_context(block_expected(0x30, 1, a, 0, a))
  , wal_context(wal_expected(wal_a, a))
  , history(testing::history(0x30, 1, a))
  , durable_context{history, runtime::file_position{a}}
  , sealed_context{history, runtime::file_position{a}}
  , coverage(scope(100, sparse ? 105U : 101U, 0, sparse ? 2U : 1U, a, a)) {
    if (
      retry_count == 0 || retry_count > 408
      || (outer_header != 32 && outer_header != 40 && outer_header != 4096))
        throw std::invalid_argument("invalid fixture size");
    child = assigned_wire(compressed, inner_header, sparse);
    block = block_wire(child, block_context, outer_header);
    coverage = scope(
      100, sparse ? 105U : 101U, 0, sparse ? 2U : 1U, a, a + block.size());
    durable_context.position = runtime::file_position{a + block.size()};
    const boundary_fields fields{coverage, 1, coverage, crc(block)};
    const auto durable = footer_wire(fields, durable_context, outer_header);
    sealed_context.position = runtime::file_position{
      a + block.size() + durable.size()};
    entries.reserve(retry_count);
    for (std::uint32_t i = 0; i < retry_count; ++i)
        entries.push_back(testing::retry(i));
    if (sparse) {
        codec::sha256_digest original_digest{};
        for (std::size_t i = 0; i < original_digest.size(); ++i)
            original_digest[i] = static_cast<unsigned char>(
              child[inner_header + 104 + i]);
        entries[0] = completed_retry::make(
                       entries[0].id(),
                       codec::semantic_batch_digest{original_digest},
                       entries[0].original_binding(),
                       coverage.logical(),
                       entries[0].ack_generation())
                       .value();
    }
    page = retry_page_wire(entries, sealed_context, 0, 0, outer_header);
    const std::array refs{reference(page, 0, 0, retry_count)};
    root = sealed_wire(
      fields, exact_sha(block), refs, sealed_context, outer_header);
    switch (kind) {
    case storage_case::header:
        wire = header_wire(header, outer_header);
        break;
    case storage_case::block:
        wire = block;
        break;
    case storage_case::wal:
        wire = wal_wire(child, wal_context, outer_header);
        break;
    case storage_case::durable:
        wire = durable;
        break;
    case storage_case::sealed:
        wire = root;
        break;
    case storage_case::retry:
        wire = page;
        break;
    }
}

storage_observation observe_storage(
  const storage_fixture& fixture,
  std::string_view candidate,
  std::size_t width,
  codec::limits policy,
  std::uint8_t restrictions,
  codec::input_boundary boundary) {
    // Keep 32 MiB outside the operation budget for fixture strings, expected
    // root metadata, native contexts, oracle state and test/coroutine frames.
    seastar::abort_source abort;
    codec::cooperative_work work{policy, abort};
    seastar::abort_source root_abort;
    codec::cooperative_work root_work{codec::limits::defaults(), root_abort};
    const auto root = pin_root(fixture.root, root_work, fixture.sealed_context);
    std::string framed = "p";
    framed.append(candidate);
    width = std::max(width, (framed.size() + 999U) / 1000U);
    bytes::fragmented_buffer_parser input{buffer(framed, width)};
    input.skip(byte_count{1}).value();
    input.push_checkpoint().value();
    const codec::field_context c{
      .origin = (restrictions & 8U) != 0 ? UINT64_MAX - framed.size() : 71};
    auto memory = reserve(input, work, c);
    if ((restrictions & 1U) != 0) memory.operation_remaining = {};
    if ((restrictions & 2U) != 0) memory.metadata_remaining = {};
    if ((restrictions & 4U) != 0) abort.request_abort();
    if ((restrictions & 16U) != 0) {
        while (input.checkpoint_depth() < bytes::max_parser_checkpoints)
            input.push_checkpoint().value();
    }
    const auto depth = input.checkpoint_depth();
    storage_observation seen;
    const auto record = [&](const auto& result) {
        if (!result) seen.error = result.error();
        return result.has_value();
    };
    const auto covered = [&](storage::coverage coverage) {
        seen.facts = {
          coverage.logical().begin().value(),
          coverage.logical().end().value(),
          coverage.physical().begin().value(),
          coverage.physical().end().value(),
          coverage.bytes().begin().value(),
          coverage.bytes().end().value()};
    };
    switch (fixture.kind) {
    case storage_case::header: {
        const auto result
          = decode_segment_header(
              input, fixture.header, {}, memory, work, c, boundary)
              .get();
        if (record(result)) {
            require(result->value == fixture.header);
            seen.facts[0] = result->bytes.end().value();
        }
        break;
    }
    case storage_case::block: {
        const auto result
          = decode_segment_block(
              input, fixture.block_context, memory, work, c, boundary)
              .get();
        if (record(result)) {
            const auto length = input.bytes_consumed().value() - 1U;
            require(length <= candidate.size());
            require(result->value.bytes().content_equals(
              candidate.substr(0, length)));
            covered(result->value.descriptor().coverage());
            seen.digest
              = result->value.descriptor().batch().fingerprint.bytes();
        }
        break;
    }
    case storage_case::wal: {
        const auto result
          = decode_wal_prepare(
              input, fixture.wal_context, memory, work, c, boundary)
              .get();
        if (record(result)) {
            const auto header = static_cast<std::size_t>(get(candidate, 10, 2));
            const auto size = result->value.batch().bytes().size().value();
            require(
              header + 136 <= candidate.size()
              && size <= candidate.size() - header - 136);
            require(result->value.batch().bytes().content_equals(
              candidate.substr(header + 136, size)));
            seen.digest = result->value.batch().info().fingerprint.bytes();
            seen.facts[0] = result->value.wal_extent().end().value();
            seen.facts[1] = result->value.target().position().value();
        }
        break;
    }
    case storage_case::durable: {
        const auto result
          = decode_durable_footer(
              input, fixture.durable_context, memory, work, c, boundary)
              .get();
        if (record(result)) {
            covered(result->boundary().coverage);
            seen.blocks = result->boundary().block_count;
            seen.checksum = result->boundary().data_crc32c;
        }
        break;
    }
    case storage_case::sealed: {
        const auto result = decode_sealed_footer(
                              input,
                              fixture.sealed_context,
                              codec::immutable_object_digest{
                                exact_sha(fixture.root)},
                              memory,
                              work,
                              c,
                              boundary)
                              .get();
        if (record(result)) {
            covered(result->value.coverage());
            seen.digest = result->value.digest().bytes();
            seen.blocks = result->value.block_count();
        }
        break;
    }
    case storage_case::retry: {
        const auto result = decode_retry_page(
                              input,
                              root,
                              page_ordinal::make(0).value(),
                              memory,
                              work,
                              c,
                              boundary)
                              .get();
        if (record(result)) {
            require(
              std::ranges::equal(result->value.entries(), fixture.entries));
            seen.digest = result->value.reference().digest().bytes();
            seen.facts[0] = result->value.entries().size();
        }
        break;
    }
    }
    require(input.checkpoint_depth() == depth);
    if (seen.error) {
        require(input.bytes_consumed().value() == 1);
        require(seen.error->byte_offset() >= c.origin);
        require(seen.error->byte_offset() - c.origin <= framed.size());
    } else {
        require(input.bytes_consumed().value() > 1);
        require(input.bytes_consumed().value() - 1U <= candidate.size());
    }
    seen.consumed = byte_count{input.bytes_consumed().value() - 1U};
    return seen;
}
} // namespace kwaque::storage::testing
