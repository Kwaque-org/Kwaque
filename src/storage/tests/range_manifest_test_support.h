#pragma once

#include "src/storage/range_manifest_format.h"
#include "src/storage/tests/footer_test_support.h"

namespace kwaque::storage::testing::manifest {
using bytes::fragmented_buffer_parser;
inline codec::sha256_digest manifest_sha(std::string_view bytes) {
    codec::sha256_hasher hash;
    hash.update(bytes.data(), bytes.size());
    return std::move(hash).final();
}
inline manifest_context
mc(std::uint8_t manifest = 0x60, std::uint64_t generation = 3) {
    return manifest_context::make(
             id<model::topic_id>(0x10),
             id<model::range_id>(0x20),
             id<model::manifest_id>(manifest),
             model::range_manifest_generation::make(generation).value())
      .value();
}
inline model::range_logical_span
logical(std::uint64_t begin, std::uint64_t end) {
    return model::range_logical_span::make(
             model::range_logical_end{begin}, model::range_logical_end{end})
      .value();
}
inline storage::coverage extent_scope(
  std::uint64_t l0,
  std::uint64_t l1,
  std::uint64_t p0,
  std::uint64_t p1,
  std::uint64_t b0,
  std::uint64_t b1) {
    return storage::coverage{
      logical(l0, l1),
      model::segment_relative_span::make(
        model::segment_relative_end{p0}, model::segment_relative_end{p1})
        .value(),
      model::file_byte_span::make(
        runtime::file_position{b0}, runtime::file_position{b1})
        .value()};
}
inline range_manifest_entry item(
  std::uint64_t begin = 100,
  std::uint64_t end = 102,
  bool empty = false,
  std::uint8_t segment = 0x30,
  std::uint64_t generation = 7) {
    return range_manifest_entry::make(
             id<model::segment_id>(segment),
             model::segment_generation::make(generation).value(),
             extent_scope(
               begin,
               end,
               empty ? 10U : 0U,
               empty ? 10U : end - begin,
               empty ? 4096U : 512U,
               empty ? 4096U : 1536U),
             codec::extent_digest{manifest_sha(empty ? "" : "data")})
      .value();
}
inline auto items() {
    return std::array{item(), item(102, 105, true, 0x80, 9)};
}
inline range_manifest_page_header page_header(
  std::uint32_t count = 2,
  manifest_context context = mc(),
  std::uint64_t begin = 100,
  std::uint64_t end = 105,
  std::uint32_t ordinal = 0,
  std::uint32_t first = 0) {
    return range_manifest_page_header::make(
             context,
             logical(begin, end),
             page_ordinal::make(ordinal).value(),
             first,
             count)
      .value();
}
inline range_manifest_root_header root_header(
  std::uint32_t count = 2,
  std::uint32_t pages = 1,
  manifest_context context = mc(),
  std::uint64_t begin = 100,
  std::uint64_t end = 105) {
    return range_manifest_root_header::make(
             context,
             logical(begin, end),
             count,
             page_count::make(pages).value())
      .value();
}
inline void
put_manifest_id(std::string& bytes, std::size_t at, const auto& value) {
    for (std::size_t i = 0; i < 16; ++i)
        bytes[at + i] = std::bit_cast<char>(value.bytes()[i]);
}
inline std::string manifest_fixed(
  std::size_t length,
  manifest_context context,
  model::range_logical_span span,
  std::uint16_t kind) {
    std::string out(length, '\0');
    put(out, 0, kind, 2);
    put_manifest_id(out, 4, context.topic());
    put_manifest_id(out, 20, context.range());
    put_manifest_id(out, 36, context.manifest());
    put(out, 52, context.generation().value(), 8);
    put(out, 60, span.begin().value(), 8);
    put(out, 68, span.end().value(), 8);
    return out;
}
inline std::string manifest_frame(
  std::string body, std::size_t pad_at, std::uint64_t a, std::size_t h) {
    const auto pad = (a - (h + body.size()) % a) % a;
    put(body, pad_at, pad, 4);
    body.append(pad, '\0');
    return frame(std::move(body), 9, h);
}
inline std::string expected_page(
  range_manifest_page_header header,
  std::span<const range_manifest_entry> entries,
  std::uint64_t a = 512,
  std::size_t h = 32) {
    auto body = manifest_fixed(92, header.context(), header.logical_span(), 2);
    put(body, 76, header.ordinal().value(), 4);
    put(body, 80, header.first_entry(), 4);
    put(body, 84, header.entry_count(), 4);
    for (const auto& entry : entries) {
        std::string bytes(104, '\0');
        const auto c = entry.coverage();
        put(bytes, 0, c.logical().begin().value(), 8);
        put(bytes, 8, c.logical().end().value(), 8);
        put_manifest_id(bytes, 16, entry.segment());
        put(bytes, 32, entry.generation().value(), 8);
        put(bytes, 40, c.physical().begin().value(), 8);
        put(bytes, 48, c.physical().end().value(), 8);
        put(bytes, 56, c.bytes().begin().value(), 8);
        put(bytes, 64, c.bytes().end().value(), 8);
        const auto digest = entry.digest().bytes();
        for (std::size_t i = 0; i < digest.size(); ++i)
            bytes[72 + i] = std::bit_cast<char>(digest[i]);
        body += bytes;
    }
    return manifest_frame(std::move(body), 88, a, h);
}
inline page_ref manifest_page_reference(
  std::string_view bytes, range_manifest_page_header header) {
    return page_ref::make(
             header.ordinal(),
             header.first_entry(),
             header.entry_count(),
             byte_count{bytes.size()},
             codec::immutable_object_digest{manifest_sha(bytes)})
      .value();
}
inline std::string expected_root(
  range_manifest_root_header header,
  std::span<const page_ref> refs,
  std::uint64_t a = 512,
  std::size_t h = 32) {
    auto body = manifest_fixed(88, header.context(), header.logical_span(), 1);
    put(body, 76, header.entry_count(), 4);
    put(body, 80, header.page_count().value(), 4);
    for (const auto& reference : refs) {
        std::string bytes(48, '\0');
        put(bytes, 0, reference.ordinal().value(), 4);
        put(bytes, 4, reference.first_entry(), 4);
        put(bytes, 8, reference.entry_count(), 4);
        put(bytes, 12, reference.encoded_bytes().value(), 4);
        const auto digest = reference.digest().bytes();
        for (std::size_t i = 0; i < digest.size(); ++i)
            bytes[16 + i] = std::bit_cast<char>(digest[i]);
        body += bytes;
    }
    return manifest_frame(std::move(body), 84, a, h);
}
inline range_manifest_root pin(
  std::string_view wire,
  range_manifest_root_header header,
  codec::cooperative_work& work,
  storage_alignment a = alignment()) {
    fragmented_buffer_parser input{buffer(wire)};
    auto decoded = decode_range_manifest_root(
                     input,
                     header.context(),
                     header.logical_span(),
                     a,
                     codec::immutable_object_digest{manifest_sha(wire)},
                     reserve(input, work),
                     work)
                     .get()
                     .value();
    return std::move(decoded.value);
}
inline codec::decode_budget page_memory(
  const fragmented_buffer_parser& input,
  const range_manifest_root& root,
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
} // namespace kwaque::storage::testing::manifest
