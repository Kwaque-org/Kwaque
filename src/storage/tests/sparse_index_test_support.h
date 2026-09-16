#pragma once

#include "src/storage/sparse_index_format.h"
#include "src/storage/tests/footer_test_support.h"

namespace kwaque::storage::testing::index {
using bytes::fragmented_buffer_parser;
inline codec::sha256_digest sha(std::string_view wire) {
    codec::sha256_hasher hash;
    hash.update(wire.data(), wire.size());
    return std::move(hash).final();
}
inline sparse_index_context
target(std::uint32_t records = 3, std::uint64_t a = 512) {
    // This is a metadata fixture. Actual data evidence is supplied separately
    // in the tests that check the reference against stored block bytes.
    return sparse_index_context::make(
             sc(),
             scope(
               100,
               100U + records,
               0,
               records,
               a,
               (static_cast<std::uint64_t>(records) + 1U) * a),
             codec::extent_digest{sha(records == 0 ? "" : "extent")},
             alignment(a))
      .value();
}
inline sparse_index_entry
entry(std::uint64_t logical = 100, std::uint64_t position = 512) {
    return {
      model::range_logical_offset::make(logical).value(),
      runtime::file_position{position}};
}
inline std::string fixed_scope(
  std::size_t size, sparse_index_context context, std::uint16_t kind) {
    std::string body(size, '\0'), segment(72, '\0');
    put(body, 0, kind, 2);
    put_sc(segment, context.segment());
    body.replace(4, 72, segment);
    put_coverage(body, 76, context.coverage());
    const auto hash = context.digest().bytes();
    for (std::size_t i = 0; i < hash.size(); ++i)
        body[124 + i] = std::bit_cast<char>(hash[i]);
    return body;
}
inline std::string finish_wire(
  std::string body,
  std::size_t padding_at,
  storage_alignment alignment,
  std::size_t header) {
    const auto a = alignment.bytes().value();
    const auto pad = (a - (header + body.size()) % a) % a;
    put(body, padding_at, pad, 4);
    body.append(pad, '\0');
    return frame(std::move(body), 8, header);
}
inline std::string page_wire(
  std::span<const sparse_index_entry> entries,
  sparse_index_context context = target(),
  std::uint32_t ordinal = 0,
  std::uint32_t first = 0,
  std::size_t header = 32) {
    auto body = fixed_scope(172, context, 2);
    put(body, 156, ordinal, 4);
    put(body, 160, first, 4);
    put(body, 164, entries.size(), 4);
    for (const auto& value : entries) {
        std::string item(16, '\0');
        put(item, 0, value.logical_anchor().value(), 8);
        put(item, 8, value.block_position().value(), 8);
        body += item;
    }
    return finish_wire(std::move(body), 168, context.alignment(), header);
}
inline page_ref page_reference(
  std::string_view wire,
  std::uint32_t count = 1,
  std::uint32_t ordinal = 0,
  std::uint32_t first = 0) {
    return page_ref::make(
             page_ordinal::make(ordinal).value(),
             first,
             count,
             byte_count{wire.size()},
             codec::immutable_object_digest{sha(wire)})
      .value();
}
inline std::string root_wire(
  std::span<const page_ref> refs,
  sparse_index_context context = target(),
  std::size_t header = 32) {
    auto body = fixed_scope(168, context, 1);
    std::uint32_t count = 0;
    for (const auto& r : refs) {
        std::string item(48, '\0');
        put(item, 0, r.ordinal().value(), 4);
        put(item, 4, r.first_entry(), 4);
        put(item, 8, r.entry_count(), 4);
        put(item, 12, r.encoded_bytes().value(), 4);
        const auto digest = r.digest().bytes();
        for (std::size_t i = 0; i < digest.size(); ++i)
            item[16 + i] = std::bit_cast<char>(digest[i]);
        body += item;
        count += r.entry_count();
    }
    put(body, 156, count, 4);
    put(body, 160, refs.size(), 4);
    return finish_wire(std::move(body), 164, context.alignment(), header);
}
inline sparse_index_root pin(
  std::string_view wire,
  sparse_index_context context,
  codec::cooperative_work& work) {
    fragmented_buffer_parser input{buffer(wire)};
    auto decoded = decode_sparse_index_root(
                     input,
                     context,
                     codec::immutable_object_digest{sha(wire)},
                     reserve(input, work),
                     work)
                     .get()
                     .value();
    return std::move(decoded.value);
}
inline codec::decode_budget page_memory(
  const fragmented_buffer_parser& input,
  const sparse_index_root& root,
  codec::cooperative_work& work) {
    auto memory = reserve(input, work);
    const auto cost = charge(
      byte_count{root.page_capacity() * sizeof(page_ref)});
    memory.operation_remaining
      = memory.operation_remaining.checked_sub(cost).value();
    memory.metadata_remaining
      = memory.metadata_remaining.checked_sub(cost).value();
    return memory;
}
} // namespace kwaque::storage::testing::index
