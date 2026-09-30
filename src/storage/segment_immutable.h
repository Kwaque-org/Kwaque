#pragma once

#include "src/storage/extent_verifier.h"
#include "src/storage/local_store_config.h"
#include "src/storage/sealed_format.h"

namespace kwaque::storage {
// Supplied independently of the bytes being opened. Original logical coverage
// can include removed records; file_end includes the pinned sealed root.
struct segment_immutable_expectation final {
    storage::coverage coverage;
    codec::extent_digest digest;
    runtime::file_position file_end;
    bool operator==(const segment_immutable_expectation&) const = default;
};

// The caller retains the checked read-only file and reserves the decode budget
// plus native/frame costs before entry. One complete bounded object is decoded
// at a time; fixed-prefix fields only bound the read, never prove its contents.
// Root and required retry pages must independently pass the pinned metadata
// reader. This walk supplies the missing data-extent proof, not append
// authority.
[[nodiscard]] seastar::future<runtime::result<verified_extent>>
verify_segment_immutable(
  runtime::file&,
  const sealed_footer&,
  segment_immutable_expectation,
  local_layout_kind,
  codec::decode_budget,
  codec::cooperative_work&);
} // namespace kwaque::storage
