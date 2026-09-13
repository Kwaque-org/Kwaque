#pragma once

#include "src/model/batch_codec.h"

namespace kwaque::model::detail {
// Internal composition under an owning envelope transaction. The caller has
// validated independent expectations, family/integrity and exact body bounds,
// admitted the parent and body alias, and owns final cleanup/poll/commit.
// original is the pre-body-alias residual; remaining excludes that alias.
// Returned residuals become usable only after the envelope's temporary owners
// are destroyed. These helpers validate the entire model body, including SHA
// when original content is present; they do not certify envelope bytes alone.
seastar::future<codec::result<decoded_submitted_batch>> decode_submitted_body(
  bytes::fragmented_buffer_parser& input,
  batch_decode_expectation expected,
  codec::decode_budget original,
  codec::decode_budget remaining,
  codec::cooperative_work& work,
  codec::field_context context);
seastar::future<codec::result<decoded_assigned_batch>> decode_assigned_body(
  bytes::fragmented_buffer_parser& input,
  batch_decode_expectation expected,
  codec::decode_budget original,
  codec::decode_budget remaining,
  codec::cooperative_work& work,
  codec::field_context context);
} // namespace kwaque::model::detail
