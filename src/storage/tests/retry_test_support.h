#pragma once

#include "src/storage/retry_format.h"
#include "src/storage/tests/footer_test_support.h"

namespace kwaque::storage::testing {
inline codec::sha256_digest exact_sha(std::string_view bytes) {
    codec::sha256_hasher hash;
    hash.update(bytes.data(), bytes.size());
    return std::move(hash).final();
}
inline footer_expectation root_location(
  std::uint8_t segment = 0x30,
  std::uint64_t generation = 1,
  std::uint64_t a = 512) {
    return {history(segment, generation, a), runtime::file_position{3U * a}};
}
inline completed_retry retry(
  std::uint64_t sequence = 0,
  std::uint64_t epoch = 1,
  std::uint64_t stream = 1,
  std::uint8_t producer = 0x40) {
    auto assigned = assigned_wire();
    assigned.replace(32, 16, 16, std::bit_cast<char>(producer));
    put(assigned, 32 + 16, epoch, 8);
    put(assigned, 32 + 24, stream, 8);
    put(assigned, 32 + 32, sequence, 8);
    // Independently assemble the original semantic projection, so distinct
    // requests in the large fixture also have consistent retained digests.
    codec::sha256_hasher hash;
    hash.update(
      codec::semantic_batch_domain.data(), codec::semantic_batch_domain.size());
    hash.update(assigned.data() + 32, 104);
    hash.update(assigned.data() + 32 + 136, 12);
    hash.update(assigned.data() + 32 + 184, 7);
    const auto digest = std::move(hash).final();
    return completed_retry::make(
             model::batch_id::make(
               id<model::producer_id>(producer),
               model::producer_epoch::make(epoch).value(),
               model::producer_stream_id::make(stream).value(),
               model::batch_sequence{sequence})
               .value(),
             codec::semantic_batch_digest{digest},
             model::producer_stream_binding::make(
               id<model::topic_id>(0x10),
               id<model::range_id>(0x20),
               model::range_routing_epoch::make(1).value(),
               id<model::segment_id>(0x30),
               model::segment_generation::make(1).value())
               .value(),
             model::range_logical_span::make(
               model::range_logical_end{100}, model::range_logical_end{101})
               .value(),
             model::segment_generation::make(1).value())
      .value();
}
inline std::string retry_wire(const completed_retry& entry) {
    std::string out(160, '\0');
    const auto copy_id = [&](auto id, std::size_t at) {
        for (std::size_t i = 0; i < id.bytes().size(); ++i)
            out[at + i] = std::bit_cast<char>(id.bytes()[i]);
    };
    const auto id = entry.id();
    const auto binding = entry.original_binding();
    copy_id(id.producer(), 0);
    put(out, 16, id.epoch().value(), 8);
    put(out, 24, id.stream().value(), 8);
    put(out, 32, id.sequence().value(), 8);
    const auto digest = entry.submitted_digest().bytes();
    for (std::size_t i = 0; i < digest.size(); ++i)
        out[40 + i] = std::bit_cast<char>(digest[i]);
    copy_id(binding.topic(), 72);
    copy_id(binding.range(), 88);
    put(out, 104, binding.routing_epoch().value(), 8);
    copy_id(binding.segment(), 112);
    put(out, 128, binding.generation().value(), 8);
    put(out, 136, entry.returned_span().begin().value(), 8);
    put(out, 144, entry.returned_span().end().value(), 8);
    put(out, 152, entry.ack_generation().value(), 8);
    return out;
}
inline std::string retry_page_wire(
  std::span<const completed_retry> entries,
  footer_expectation root = root_location(),
  std::uint32_t ordinal = 0,
  std::uint32_t first = 0,
  std::size_t h = 32) {
    std::string body(100, '\0');
    put(body, 0, 2, 2);
    std::string context(72, '\0');
    put_sc(context, root.history.segment);
    body.replace(4, 72, context);
    put(body, 76, root.position.value(), 8);
    put(body, 84, ordinal, 4);
    put(body, 88, first, 4);
    put(body, 92, entries.size(), 4);
    for (const auto& entry : entries)
        body += retry_wire(entry);
    const auto a = root.history.alignment.bytes().value();
    const auto pad = (a - (h + body.size()) % a) % a;
    put(body, 96, pad, 4);
    body.append(pad, '\0');
    return frame(std::move(body), 7, h);
}
inline page_ref reference(
  std::string_view page,
  std::uint32_t ordinal = 0,
  std::uint32_t first = 0,
  std::uint32_t count = 1) {
    return page_ref::make(
             page_ordinal::make(ordinal).value(),
             first,
             count,
             byte_count{page.size()},
             codec::immutable_object_digest{exact_sha(page)})
      .value();
}
inline std::string sealed_wire(
  boundary_fields fields,
  codec::sha256_digest digest,
  std::span<const page_ref> refs = {},
  footer_expectation root = root_location(),
  std::size_t h = 32) {
    std::string body(232, '\0');
    put(body, 0, 1, 2);
    std::string context(72, '\0');
    put_sc(context, root.history.segment);
    body.replace(4, 72, context);
    put(body, 76, root.position.value(), 8);
    put_coverage(body, 84, fields.coverage);
    put(body, 132, fields.block_count, 4);
    if (fields.last_block) {
        put(body, 136, 1, 1);
        put_coverage(body, 140, *fields.last_block);
    }
    for (std::size_t i = 0; i < digest.size(); ++i)
        body[188 + i] = std::bit_cast<char>(digest[i]);
    std::uint32_t total = 0;
    for (const auto& ref : refs) {
        std::string page(48, '\0');
        put(page, 0, ref.ordinal().value(), 4);
        put(page, 4, ref.first_entry(), 4);
        put(page, 8, ref.entry_count(), 4);
        put(page, 12, ref.encoded_bytes().value(), 4);
        const auto sha = ref.digest().bytes();
        for (std::size_t i = 0; i < sha.size(); ++i)
            page[16 + i] = std::bit_cast<char>(sha[i]);
        body += page;
        total += ref.entry_count();
    }
    put(body, 220, total, 4);
    put(body, 224, refs.size(), 4);
    const auto a = root.history.alignment.bytes().value();
    const auto pad = (a - (h + body.size()) % a) % a;
    put(body, 228, pad, 4);
    body.append(pad, '\0');
    return frame(std::move(body), 7, h);
}
inline std::string one_root(
  std::span<const page_ref> refs = {},
  footer_expectation root = root_location(),
  std::size_t h = 32) {
    const auto extent = scope(
      100,
      101,
      0,
      1,
      root.history.data_start.value(),
      2U * root.history.data_start.value());
    return sealed_wire(
      {extent, 1, extent, 0}, exact_sha(data_block()), refs, root, h);
}
inline sealed_footer pin_root(
  const std::string& wire,
  codec::cooperative_work& work,
  footer_expectation root = root_location()) {
    bytes::fragmented_buffer_parser parser{buffer(wire, 67)};
    auto value = decode_sealed_footer(
                   parser,
                   root,
                   codec::immutable_object_digest{exact_sha(wire)},
                   reserve(parser, work),
                   work)
                   .get()
                   .value();
    return std::move(value.value);
}
inline codec::decode_budget reserve_page(
  const bytes::fragmented_buffer_parser& input,
  const sealed_footer& root,
  codec::cooperative_work& work) {
    auto memory = reserve(input, work);
    const auto cost = charge(
      byte_count{root.page_capacity() * sizeof(page_ref)});
    memory.operation_remaining = byte_count{
      memory.operation_remaining.value() - cost.value()};
    memory.metadata_remaining = byte_count{
      memory.metadata_remaining.value() - cost.value()};
    return memory;
}
inline verified_extent hashed_evidence(codec::cooperative_work& work) {
    auto verifier = extent_verifier::make(
                      history(),
                      scope(100, 101, 0, 1, 512, 1024),
                      work.policy(),
                      extent_layout_kind::initial_append,
                      {},
                      extent_integrity::crc32c_and_sha256)
                      .value();
    feed_block(verifier, data_block(), work).value();
    return verifier.finish(work).value();
}
} // namespace kwaque::storage::testing
