#pragma once

#include "src/bytes/fragmented_buffer.h"
#include "src/codec/cooperative.h"
#include "src/codec/digest.h"
#include "src/model/batch_context.h"
#include "src/model/checkpoint.h"

namespace kwaque::model {

// Computes the original semantic projection, not a validation certificate.
// records must be the complete validated canonical original stream for context;
// sparse survivors cannot be substituted. The checked builder supplies these
// preconditions, or an input owner must finish full record-region validation.
// Borrowed input/work/abort stay alive, immutable and exclusive until
// completion. Caller reserves input backing/descriptors and verified SHA
// provider/context, frame and opaque-owner costs before entry. One SHA context
// hashes the fixed 127-byte prefix followed by the record bytes, including
// every size prefix.
[[nodiscard]] seastar::future<codec::result<codec::semantic_batch_digest>>
compute_submitted_fingerprint(
  submitted_batch_context context,
  const bytes::fragmented_buffer& records,
  codec::cooperative_work& work,
  codec::error anchor = codec::error{errc::success});

seastar::future<codec::result<codec::semantic_batch_digest>>
compute_submitted_fingerprint(
  submitted_batch_context,
  const bytes::fragmented_buffer&&,
  codec::cooperative_work&,
  codec::error = codec::error{errc::success}) = delete;

// Domain plus TopicID, fixed u32 count and canonical (RangeID, next-u64)
// entries. No envelope fields enter the projection. value/work/abort stay alive
// and unchanged through completion; caller reserves input, native SHA and frame
// costs. Native SHA state ends before the final abort poll and publication.
// Each fixed projection leaf needs at least 256 work bytes and 16 items.
[[nodiscard]] seastar::future<codec::result<codec::checkpoint_digest>>
compute_checkpoint_fingerprint(
  const read_checkpoint& value,
  codec::cooperative_work& work,
  codec::error anchor = codec::error{errc::success});

seastar::future<codec::result<codec::checkpoint_digest>>
compute_checkpoint_fingerprint(
  const read_checkpoint&&,
  codec::cooperative_work&,
  codec::error = codec::error{errc::success}) = delete;

} // namespace kwaque::model
