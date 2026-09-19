#pragma once

#include "src/protocol/control_codec.h"

namespace kwaque::common::v1 {
class Error;
}
namespace kwaque::control::v1 {
class HandshakeRequest;
class HandshakeResponse;
class Redirect;
} // namespace kwaque::control::v1

namespace kwaque::protocol::detail {
// Private conversion leaves, shared by the owning decoder and stage-specific
// measurements. The SAME fresh native owner must already have passed exact
// profile preflight and parsing, and remain immutable/alive through completion.
// These functions neither validate arbitrary generated objects nor mint an
// accepted control. The owner admits returned storage against simultaneous
// native/staging costs, owns partial output, joins teardown and polls before
// publication. No generated type enters public control/model headers.
#define KWAQUE_CONTROL_CONVERSION(Type)                                        \
    codec::result<byte_count> control_conversion_storage(                      \
      const Type&,                                                             \
      control_expectation,                                                     \
      codec::decode_budget,                                                    \
      codec::limits,                                                           \
      codec::field_context);                                                   \
    seastar::future<codec::result<void>> copy_control_fields(                  \
      const Type&, control_data&, codec::cooperative_work&, codec::error);
KWAQUE_CONTROL_CONVERSION(kwaque::control::v1::HandshakeRequest)
KWAQUE_CONTROL_CONVERSION(kwaque::control::v1::HandshakeResponse)
KWAQUE_CONTROL_CONVERSION(kwaque::control::v1::Redirect)
KWAQUE_CONTROL_CONVERSION(kwaque::common::v1::Error)
#undef KWAQUE_CONTROL_CONVERSION
} // namespace kwaque::protocol::detail
