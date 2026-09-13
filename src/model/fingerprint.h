#pragma once

#include "src/bytes/fragmented_buffer.h"
#include "src/codec/cooperative.h"
#include "src/codec/digest.h"
#include "src/model/batch_context.h"

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

} // namespace kwaque::model
