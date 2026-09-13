#pragma once

#include "src/bytes/fragmented_buffer_builder.h"
#include "src/model/record_codec.h"

namespace kwaque::model::detail {

// Internal composition for a batch's supplied relative coordinates. The owner
// admits input and staging, checks coordinates and keeps partial output
// private. size must be computed from these same fields/value; appending can
// suspend or fail after earlier fields, and never publishes or rolls back that
// staging.
seastar::future<codec::result<record_sizes>> record_size_with_fields(
  record_fields fields,
  const record& value,
  codec::cooperative_work& work,
  codec::error anchor);

seastar::future<codec::result<void>> append_record(
  record_fields fields,
  const record& value,
  record_sizes size,
  bytes::fragmented_buffer_builder& output,
  codec::cooperative_work& work,
  codec::field_context context);

} // namespace kwaque::model::detail
