#pragma once

#include "src/storage/tests/retry_test_support.h"
#include "src/storage/tests/wal_test_support.h"

namespace kwaque::storage::testing {
enum class storage_case { header, block, wal, durable, sealed, retry };

// Independently assembled small wire fixtures and caller expectations. None
// of the storage writers are used to construct their expected bytes. Larger
// qualification uses bounded public builders rather than a contiguous string.
struct storage_fixture final {
    storage_fixture(
      storage_case kind,
      bool compressed = false,
      bool sparse = false,
      std::size_t outer_header = 32,
      std::size_t inner_header = 32,
      std::uint64_t segment_alignment = 512,
      std::uint64_t wal_alignment = 512,
      std::uint32_t retry_count = 1);
    storage_case kind;
    segment_header header;
    segment_block_expectation block_context;
    wal_prepare_expectation wal_context;
    segment_history_context history;
    footer_expectation durable_context;
    footer_expectation sealed_context;
    storage::coverage coverage;
    std::vector<completed_retry> entries;
    std::string child, block, page, root, wire;
};

struct storage_observation final {
    std::optional<codec::error> error;
    byte_count consumed;
    std::array<std::uint64_t, 6> facts{};
    std::uint32_t blocks{0};
    std::uint32_t checksum{0};
    codec::sha256_digest digest{};
    bool operator==(const storage_observation&) const noexcept = default;
};

// Test-thread only: joins each owning codec and releases its result before
// returning. Output bytes are checked against the actual supplied extent;
// root/page pins always come from the independent fixture, not the candidate.
storage_observation observe_storage(
  const storage_fixture&,
  std::string_view candidate,
  std::size_t width,
  codec::limits policy = codec::limits::defaults(),
  std::uint8_t restrictions = 0,
  codec::input_boundary boundary = codec::input_boundary::open);
} // namespace kwaque::storage::testing
