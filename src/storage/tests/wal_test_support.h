#pragma once

#include "src/storage/tests/segment_test_support.h"
#include "src/storage/wal_format.h"

namespace kwaque::storage::testing {
inline wal_prepare_expectation wal_expected(
  std::uint64_t wal_alignment = 512, std::uint64_t target_alignment = 512) {
    return {
      wal_write_context::make(
        id<model::wal_incarnation_id>(0x70), alignment(wal_alignment), {})
        .value(),
      segment_write_context::make(
        sc(),
        alignment(target_alignment),
        {},
        runtime::file_position{target_alignment})
        .value(),
      runtime::file_position{target_alignment},
      model::range_routing_epoch::make(1).value(),
      batch_expected()};
}

// Independent fixed offsets/CRC/padding; no production WAL writer is used.
inline std::string wal_wire(
  std::string child = assigned_wire(),
  wal_prepare_expectation context = wal_expected(),
  std::size_t h = 32) {
    std::string body(136, '\0');
    const auto incarnation = context.wal.incarnation();
    for (std::size_t i = 0; i < 16; ++i)
        body[i] = std::bit_cast<char>(incarnation.bytes()[i]);
    put(body, 16, context.wal.position().value(), 8);
    std::string target(72, '\0');
    put_sc(target, context.target.segment());
    body.replace(24, 72, target);
    put(body, 96, context.routing_epoch.value(), 8);
    put(body, 104, context.target.physical_begin().value(), 8);
    put(body, 112, context.target.position().value(), 8);
    const auto a = context.wal.alignment().bytes().value();
    put(body, 120, a, 4);
    put(body, 124, static_cast<std::uint16_t>(context.profile), 2);
    put(body, 128, child.size(), 4);
    const auto pad = (a - (h + body.size() + child.size()) % a) % a;
    put(body, 132, pad, 4);
    body += child;
    body.append(pad, '\0');
    return frame(std::move(body), 5, h);
}

} // namespace kwaque::storage::testing
