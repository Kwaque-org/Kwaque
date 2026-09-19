#pragma once

#include "src/protocol/control_codec.h"

#include <google/protobuf/message.h>

#include <memory>
#include <string_view>

namespace kwaque::protocol::testing {
// Independent descriptor/native oracle. It never calls the production schema
// table, preflight, decoder, constructor or serializer. The bounded wire walk
// runs before native parsing. Success owns a fresh message; unknowns remain in
// that owner only, and comparisons concern values/presence rather than bytes.
std::unique_ptr<google::protobuf::Message>
  probe_control(std::string_view, frame_kind);
bool matches_control(const google::protobuf::Message&, const control_data&);
bool matches_expectation(
  const google::protobuf::Message&, frame_kind, control_expectation);
bool control_unknowns_empty(const google::protobuf::Message&);
} // namespace kwaque::protocol::testing
