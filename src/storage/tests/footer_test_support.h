#pragma once

#include "src/codec/xxh3.h"
#include "src/storage/extent_verifier.h"
#include "src/storage/tests/segment_test_support.h"

namespace kwaque::storage::testing {
inline segment_history_context history(
  std::uint8_t segment = 0x30,
  std::uint64_t generation = 1,
  std::uint64_t a = 512) {
    return {
      sc(segment, generation),
      alignment(a),
      runtime::file_position{a},
      model::range_logical_end{100},
      model::segment_relative_end{0}};
}
inline storage::coverage scope(
  std::uint64_t l0,
  std::uint64_t l1,
  std::uint64_t p0,
  std::uint64_t p1,
  std::uint64_t b0,
  std::uint64_t b1) {
    return storage::coverage{
      model::range_logical_span::make(
        model::range_logical_end{l0}, model::range_logical_end{l1})
        .value(),
      model::segment_relative_span::make(
        model::segment_relative_end{p0}, model::segment_relative_end{p1})
        .value(),
      model::file_byte_span::make(
        runtime::file_position{b0}, runtime::file_position{b1})
        .value()};
}
// The assigned child data_block frames: one original request per logical
// base.
inline std::string data_child(
  std::uint64_t logical = 100, bool sparse = false, bool compressed = false) {
    auto child = assigned_wire(compressed, 32, sparse);
    if (logical > 100) {
        // Distinct original requests, with an independently assembled semantic
        // projection. Placement itself remains outside that projection.
        put(child, 32 + 32, logical - 100U, 8);
        codec::xxh3_128_hasher hash;
        hash.update(
          codec::semantic_batch_domain.data(),
          codec::semantic_batch_domain.size());
        hash.update(child.data() + 32, 104);
        hash.update(child.data() + 32 + 136, 12);
        const auto original = hex(
          sparse ? "06000000010000060001010100000600140201000006000303010000060"
                   "00204010000"
                 : "06000000010000");
        hash.update(original.data(), original.size());
        const auto digest = std::move(hash).final();
        for (std::size_t i = 0; i < digest.size(); ++i)
            child[32 + 104 + i] = std::bit_cast<char>(digest[i]);
    }
    put(child, 32 + 168, logical, 8);
    put(child, 32 + 176, logical + (sparse ? 5U : 1U), 8);
    repair(child);
    return child;
}
// One record carrying a value of `value` bytes, with its length prefix. For
// values of 121 to 8,191 bytes both of its length varints take two bytes, so
// it is 9 + value bytes; below that it is one byte shorter.
inline std::string valued_record(std::size_t value) {
    std::string records;
    const auto varint = [&records](std::uint64_t number) {
        while (number >= 0x80U) {
            records.push_back(static_cast<char>((number & 0x7fU) | 0x80U));
            number >>= 7U;
        }
        records.push_back(static_cast<char>(number));
    };
    // Attributes, time and offset deltas, a null key, the value's ZigZag
    // length, the value and no header.
    std::string body = hex("00000001");
    {
        std::string length;
        records.swap(length);
        varint(2U * value);
        records.swap(length);
        body += length;
    }
    body.append(value, 'v');
    body.push_back('\0');
    varint(body.size());
    records += body;
    return records;
}
// child, whose header ends at h, with its one record replaced by records:
// lengths, semantic digest and checksum follow.
inline void replace_records(std::string& child, const std::string& records) {
    const auto h = static_cast<std::size_t>(get(child, 10, 2));
    child.resize(h + 184);
    child += records;
    put(child, h + 160, records.size(), 4);
    put(child, h + 164, records.size(), 4);
    codec::xxh3_128_hasher hash;
    hash.update(
      codec::semantic_batch_domain.data(), codec::semantic_batch_domain.size());
    hash.update(child.data() + h, 104);
    hash.update(child.data() + h + 136, 12);
    hash.update(records.data(), records.size());
    const auto digest = std::move(hash).final();
    for (std::size_t i = 0; i < digest.size(); ++i)
        child[h + 104 + i] = std::bit_cast<char>(digest[i]);
    put(child, 12, child.size() - h, 4);
    repair(child);
}
// data_child's request at `logical`, but its one record carries a value of
// `value` bytes, for whatever size a layout needs. For values of 121 to
// 8,191 bytes both of the record's length varints take two bytes, so the
// child is 225 + value bytes; below that it is one byte shorter.
inline std::string valued_child(std::uint64_t logical, std::size_t value) {
    auto child = data_child(logical);
    replace_records(child, valued_record(value));
    return child;
}
inline std::string data_block(
  std::uint64_t logical = 100,
  std::uint64_t physical = 0,
  std::uint64_t position = 512,
  bool sparse = false,
  std::uint8_t segment = 0x30,
  std::uint64_t generation = 1,
  bool compressed = false,
  std::uint64_t a = 512,
  std::int64_t append_time = 1) {
    const auto expected = block_expected(
      segment, generation, position, physical, a);
    return block_wire(
      data_child(logical, sparse, compressed),
      expected,
      32,
      segment_block_stamps{
        runtime::wall_time{append_time}, expected.data_start});
}
inline void
put_coverage(std::string& body, std::size_t at, const storage::coverage& c) {
    put(body, at, c.logical().begin().value(), 8);
    put(body, at + 8, c.logical().end().value(), 8);
    put(body, at + 16, c.physical().begin().value(), 8);
    put(body, at + 24, c.physical().end().value(), 8);
    put(body, at + 32, c.bytes().begin().value(), 8);
    put(body, at + 40, c.bytes().end().value(), 8);
}
// Pads from its position to the next boundary, wherever it starts.
inline std::string footer_wire(
  boundary_fields fields, footer_expectation expected, std::size_t h = 32) {
    std::string body(216, '\0');
    put_sc(body, expected.history.segment);
    put(body, 72, expected.position.value(), 8);
    put_coverage(body, 80, fields.coverage);
    put(body, 128, fields.block_count, 4);
    if (fields.last_block) {
        put(body, 132, 1, 1);
        put_coverage(body, 136, *fields.last_block);
    }
    const auto time = [](runtime::wall_time value) {
        return static_cast<std::uint64_t>(value.unix_nanoseconds());
    };
    put(body, 184, time(fields.times.first_append), 8);
    put(body, 192, time(fields.times.max_append), 8);
    put(body, 200, time(fields.times.max_record), 8);
    put(body, 208, fields.data_crc32c, 4);
    const auto a = expected.history.alignment.bytes().value();
    const auto padding = (a - (expected.position.value() + h + body.size()) % a)
                         % a;
    put(body, 212, padding, 4);
    body.append(padding, '\0');
    return frame(std::move(body), 6, h);
}
inline codec::result<void> feed_block(
  extent_verifier& verifier,
  const std::string& wire,
  codec::cooperative_work& work,
  std::size_t width = 67) {
    auto bytes = buffer(wire, width);
    const auto memory = reserve(bytes, work);
    return verifier.add_block(std::move(bytes), batch_expected(), memory, work)
      .get();
}
inline codec::result<void> feed_footer(
  extent_verifier& verifier,
  const std::string& wire,
  codec::cooperative_work& work,
  std::optional<verified_extent> reference = std::nullopt) {
    auto bytes = buffer(wire, 67);
    const auto memory = reserve(bytes, work);
    return verifier
      .add_footer(std::move(bytes), memory, work, std::move(reference))
      .get();
}
inline verified_extent single_evidence(codec::cooperative_work& work) {
    auto verifier = extent_verifier::make(
                      history(),
                      scope(100, 101, 0, 1, 512, 512 + data_block().size()),
                      work.policy())
                      .value();
    feed_block(verifier, data_block(), work).value();
    return verifier.finish(work).value();
}
} // namespace kwaque::storage::testing
