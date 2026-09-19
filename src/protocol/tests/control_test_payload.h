#pragma once
#include "src/protocol/frame.h"

#include <string>
namespace kwaque::protocol::testing {
// Independent bounded wire fixtures: minimal, text-heavy, full capabilities,
// maximum wire with opaque unknowns, and legacy informational overwrite.
std::string control_payload(frame_kind, unsigned shape);
} // namespace kwaque::protocol::testing
