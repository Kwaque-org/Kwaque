#pragma once

#include "src/storage/tests/range_manifest_test_support.h"
#include "src/storage/tests/sparse_index_test_support.h"
#include "src/storage/tests/storage_format_fixture.h"

namespace kwaque::storage::testing {
// Independent fixed-offset fixtures shared by corruption cases and benchmarks.
// At most four encoded pages are retained by a caller; decoded walks retain
// only their root and current page. Entry arrays are generated one page at a
// time. No production writer constructs the expected bytes.
struct metadata_fixture final {
    metadata_fixture(
      bool manifest,
      std::uint32_t entries = 2,
      std::uint32_t pages = 1,
      std::size_t header = 32,
      std::uint64_t alignment = 512,
      bool sparse = false,
      bool wide = false);
    bool is_manifest;
    std::uint32_t entries, pages;
    std::size_t header;
    storage_alignment alignment;
    std::uint64_t logical_base, position_base, stride;
    sparse_index_context index_context;
    range_manifest_root_header manifest_header;
    [[nodiscard]] std::vector<sparse_index_entry>
    index_entries(std::uint32_t ordinal) const;
    [[nodiscard]] std::vector<range_manifest_entry>
    manifest_entries(std::uint32_t ordinal) const;
    [[nodiscard]] range_manifest_page_header
    page_header(std::uint32_t ordinal) const;
    [[nodiscard]] std::string page_wire(std::uint32_t ordinal) const;
    [[nodiscard]] page_ref
    reference(std::string_view, std::uint32_t ordinal) const;
    [[nodiscard]] std::string root_wire(std::span<const page_ref>) const;
};

// Independent pins are passed separately from the candidate bytes. Test-thread
// only, joins and destroys owners before returning. Error categories/fields
// agree across layouts, except that admission can differ with fragment count.
// Fragment-local malformed-data offsets are bounded independently. Decoded
// values must independently reproduce the supplied body bytes. Observations
// include their canonical digest, published cursor
// and scalar facts; optional envelope extensions retain their separate rules.
[[nodiscard]] storage_observation observe_metadata(
  const metadata_fixture&,
  bool root,
  std::string_view candidate,
  std::span<const page_ref> references,
  codec::immutable_object_digest root_digest,
  std::size_t width,
  std::uint8_t restrictions = 0,
  codec::input_boundary = codec::input_boundary::open);
} // namespace kwaque::storage::testing
