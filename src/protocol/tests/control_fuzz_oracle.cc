#include "src/protocol/tests/control_fuzz_oracle.h"

#include "proto/kwaque/common/v1/build_info.pb.h"
#include "proto/kwaque/common/v1/capability.pb.h"
#include "proto/kwaque/common/v1/error.pb.h"
#include "proto/kwaque/common/v1/identity.pb.h"
#include "proto/kwaque/control/v1/handshake.pb.h"
#include "proto/kwaque/control/v1/redirect.pb.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/unknown_field_set.h>

#include <algorithm>
#include <array>
#include <concepts>
#include <cstdint>
#include <string>
#include <type_traits>
#include <utf8_range.h>
#include <utility>

namespace kwaque::protocol::testing {
namespace {
namespace common = kwaque::common::v1;
namespace native = kwaque::control::v1;
using google::protobuf::Descriptor;
using field = google::protobuf::FieldDescriptor;

// Public coded-stream scalar decoding is intentionally a different primitive
// from the production octet probe. Check its consumed extent/high bits before
// accepting its otherwise truncating tag/length representations.
bool number(
  std::string_view bytes,
  std::size_t& at,
  std::uint64_t& value,
  unsigned maximum = 10) {
    if (at == bytes.size()) return false;
    google::protobuf::io::CodedInputStream input{
      reinterpret_cast<const std::uint8_t*>(bytes.data() + at),
      static_cast<int>(bytes.size() - at)};
    if (!input.ReadVarint64(&value)) return false;
    const auto count = static_cast<std::size_t>(input.CurrentPosition());
    if (count == 0 || count > maximum || count > bytes.size() - at)
        return false;
    if (count == 10 && static_cast<unsigned char>(bytes[at + count - 1]) > 1)
        return false;
    if (
      maximum == 5
      && (value > UINT32_MAX || (count == 5 && static_cast<unsigned char>(bytes[at + 4]) > 15)))
        return false;
    at += count;
    return true;
}
unsigned wire_type(const field& f) {
    switch (f.type()) {
    case field::TYPE_FIXED64:
        return 1;
    case field::TYPE_STRING:
    case field::TYPE_BYTES:
    case field::TYPE_MESSAGE:
        return 2;
    default:
        return 0;
    }
}
bool scalar(const field* f, std::uint64_t value) {
    if (!f) return true;
    if (f->type() == field::TYPE_UINT32 && value > UINT32_MAX) return false;
    if (f->type() == field::TYPE_ENUM)
        return value > 0 && value <= INT32_MAX
               && f->enum_type()->FindValueByNumber(static_cast<int>(value))
                    != nullptr;
    return true;
}
bool walk(
  std::string_view bytes,
  const Descriptor& type,
  unsigned& units,
  unsigned depth = 1) {
    const bool legacy = type.name() == "BuildInfo";
    if (
      depth > 8 || bytes.size() > (legacy ? 4096U : 65536U)
      || type.field_count() > 8)
        return false;
    std::array<unsigned, 8> occurrences{};
    std::array<unsigned, 8> elements{};
    std::size_t at = 0;
    while (at < bytes.size()) {
        std::uint64_t tag = 0;
        if (!number(bytes, at, tag, 5) || tag < 8 || ++units > 256)
            return false;
        const auto wire = static_cast<unsigned>(tag & 7U);
        if (wire == 3 || wire == 4 || wire > 5) return false;
        const auto* f = type.FindFieldByNumber(static_cast<int>(tag >> 3U));
        const bool packed = f && f->is_repeated()
                            && f->type() == field::TYPE_UINT32 && wire == 2;
        if (f && wire != wire_type(*f) && !packed) {
            if (!legacy) return false;
            f = nullptr;
        }
        if (f) {
            const auto i = static_cast<std::size_t>(f->index());
            if (++occurrences[i] > 1 && !legacy && !f->is_repeated())
                return false;
            if (f->is_repeated() && !packed) ++elements[i];
        }
        if (wire == 0) {
            std::uint64_t value = 0;
            if (!number(bytes, at, value) || !scalar(f, value)) return false;
        } else if (wire == 1 || wire == 5) {
            const auto width = wire == 1 ? 8U : 4U;
            if (width > bytes.size() - at) return false;
            at += width;
        } else {
            std::uint64_t length = 0;
            if (
              !number(bytes, at, length, 5) || length > INT32_MAX
              || length > bytes.size() - at)
                return false;
            const auto body = bytes.substr(
              at, static_cast<std::size_t>(length));
            at += body.size();
            if (packed) {
                std::size_t offset = 0;
                while (offset < body.size()) {
                    std::uint64_t value = 0;
                    if (
                      ++units > 256 || !number(body, offset, value)
                      || !scalar(f, value))
                        return false;
                    ++elements[static_cast<std::size_t>(f->index())];
                }
            } else if (f && f->type() == field::TYPE_MESSAGE) {
                if (!walk(body, *f->message_type(), units, depth + 1))
                    return false;
            } else {
                const auto limit = f && f->name() == "host"     ? 253U
                                   : f && f->name() == "reason" ? 1024U
                                                                : 4096U;
                if (body.size() > limit) return false;
                if (
                  f && f->type() == field::TYPE_STRING
                  && utf8_range_IsValid(body.data(), body.size()) == 0)
                    return false;
                if (
                  f && f->type() == field::TYPE_BYTES
                  && (body.size() != 16 || std::ranges::all_of(body, [](char c) {
                          return c == 0;
                      })))
                    return false;
            }
        }
    }
    if (legacy) return true;
    for (int i = 0; i < type.field_count(); ++i) {
        const auto& f = *type.field(i);
        const auto index = static_cast<std::size_t>(i);
        if (f.is_repeated()) {
            const unsigned maximum = f.name() == "formats" ? 32 : 16;
            if (elements[index] == 0 || elements[index] > maximum) return false;
        } else {
            const bool optional
              = (type.name() == "HandshakeRequest"
                 && (f.number() == 2 || f.number() == 3))
                || (type.name() == "Error" && (f.number() == 2 || f.number() == 3));
            if (!optional && occurrences[index] == 0) return false;
        }
    }
    return true;
}
bool routing(const common::RoutingContext& value) {
    return value.routing_epoch() != 0 && value.segment_generation() != 0;
}
bool capabilities(const common::Capabilities& value) {
    std::uint32_t previous = 0;
    for (auto v : value.protocol_versions()) {
        if (v == 0 || v > 65535 || v <= previous) return false;
        previous = v;
    }
    previous = 0;
    for (const auto& f : value.formats()) {
        if (
          f.family() == 0 || f.family() > 65535 || f.family() <= previous
          || f.oldest_readable() == 0 || f.current() > 65535
          || f.oldest_readable() > f.current())
            return false;
        previous = f.family();
    }
    int last = -1;
    for (auto v : value.compression_codecs()) {
        if (v > 255 || static_cast<int>(v) <= last) return false;
        last = static_cast<int>(v);
    }
    return value.max_frame_body_bytes() > 0
           && value.max_frame_body_bytes() <= 16777216
           && value.max_expanded_batch_bytes() > 0
           && value.max_expanded_batch_bytes() <= 8388608
           && value.max_header_bytes() >= 48 && value.max_header_bytes() <= 4096
           && value.max_record_bytes() > 0
           && value.max_record_bytes() <= 1048576
           && value.max_original_records() > 0
           && value.max_original_records() <= 4096;
}
bool id_equal(const auto& id, const std::string& bytes) {
    const auto value = id.bytes();
    return value.size() == bytes.size()
           && std::equal(
             value.begin(),
             value.end(),
             bytes.begin(),
             [](std::uint8_t a, char b) {
                 return a == static_cast<std::uint8_t>(b);
             });
}
bool equal(const routing_context& a, const common::RoutingContext& b) {
    return id_equal(a.topic, b.topic_id()) && id_equal(a.range, b.range_id())
           && id_equal(a.segment, b.segment_id())
           && a.routing_epoch.value() == b.routing_epoch()
           && a.generation.value() == b.segment_generation();
}
bool equal(const build_information& a, const common::BuildInfo& b) {
    return a.version == b.version() && a.revision == b.revision()
           && a.build_mode == b.build_mode();
}
bool equal(const control_capabilities& a, const common::Capabilities& b) {
    if (
      a.protocol_versions.size()
        != static_cast<std::size_t>(b.protocol_versions_size())
      || a.formats.size() != static_cast<std::size_t>(b.formats_size())
      || a.compression_codecs.size()
           != static_cast<std::size_t>(b.compression_codecs_size()))
        return false;
    for (std::size_t i = 0; i < a.protocol_versions.size(); ++i)
        if (a.protocol_versions[i] != b.protocol_versions(static_cast<int>(i)))
            return false;
    for (std::size_t i = 0; i < a.compression_codecs.size(); ++i)
        if (
          a.compression_codecs[i] != b.compression_codecs(static_cast<int>(i)))
            return false;
    for (std::size_t i = 0; i < a.formats.size(); ++i) {
        const auto& x = a.formats[i];
        const auto& y = b.formats(static_cast<int>(i));
        if (
          x.family != y.family() || x.oldest_readable != y.oldest_readable()
          || x.current != y.current()
          || x.supported_features != y.supported_features())
            return false;
    }
    return a.max_frame_body_bytes.value() == b.max_frame_body_bytes()
           && a.max_expanded_batch_bytes.value() == b.max_expanded_batch_bytes()
           && a.max_header_bytes.value() == b.max_header_bytes()
           && a.max_record_bytes.value() == b.max_record_bytes()
           && a.max_original_records.value() == b.max_original_records();
}
} // namespace

std::unique_ptr<google::protobuf::Message>
probe_control(std::string_view bytes, frame_kind kind) {
    if (bytes.size() > 65536) return {};
    std::unique_ptr<google::protobuf::Message> value;
    switch (kind) {
    case frame_kind::handshake_request:
        value = std::make_unique<native::HandshakeRequest>();
        break;
    case frame_kind::handshake_response:
        value = std::make_unique<native::HandshakeResponse>();
        break;
    case frame_kind::redirect:
        value = std::make_unique<native::Redirect>();
        break;
    case frame_kind::error:
        value = std::make_unique<common::Error>();
        break;
    default:
        return {};
    }
    unsigned units = 0;
    if (!walk(bytes, *value->GetDescriptor(), units)) return {};
    google::protobuf::io::CodedInputStream stream{
      reinterpret_cast<const std::uint8_t*>(bytes.data()),
      static_cast<int>(bytes.size())};
    stream.SetTotalBytesLimit(static_cast<int>(bytes.size()));
    stream.SetRecursionLimit(7);
    if (
      !value->ParseFromCodedStream(&stream) || !stream.ConsumedEntireMessage()
      || stream.CurrentPosition() != static_cast<int>(bytes.size()))
        return {};
    bool valid = false;
    switch (kind) {
    case frame_kind::handshake_request: {
        const auto& v = static_cast<const native::HandshakeRequest&>(*value);
        valid = (v.peer_kind() == native::PEER_KIND_BROKER) == v.has_broker_id()
                && capabilities(v.capabilities());
        break;
    }
    case frame_kind::handshake_response:
        valid = capabilities(
          static_cast<const native::HandshakeResponse&>(*value).capabilities());
        break;
    case frame_kind::redirect: {
        const auto& v = static_cast<const native::Redirect&>(*value);
        valid
          = routing(v.destination()) && !v.endpoint().host().empty()
            && v.endpoint().port() > 0 && v.endpoint().port() <= 65535
            && std::ranges::none_of(
              v.endpoint().host(),
              [](char c) {
                  const auto octet = static_cast<unsigned char>(c);
                  return octet <= 32 || octet == 127;
              })
            && (v.reason() == common::ERROR_CODE_STALE_ROUTING || v.reason() == common::ERROR_CODE_STALE_SEGMENT || v.reason() == common::ERROR_CODE_NOT_LEADER);
        break;
    }
    case frame_kind::error: {
        const auto& v = static_cast<const common::Error&>(*value);
        valid
          = (!v.has_observed() || routing(v.observed()))
            && ((v.code() != common::ERROR_CODE_STALE_ROUTING && v.code() != common::ERROR_CODE_STALE_SEGMENT) || v.has_observed());
        break;
    }
    default:
        break;
    }
    if (!valid) return {};
    return value;
}

bool matches_control(
  const google::protobuf::Message& message, const control_data& data) {
    return std::visit(
      [&](const auto& value) {
          using T = std::remove_cvref_t<decltype(value)>;
          if constexpr (std::same_as<T, handshake_request>) {
              if (
                message.GetDescriptor()
                != native::HandshakeRequest::descriptor())
                  return false;
              const auto& v = static_cast<const native::HandshakeRequest&>(
                message);
              return static_cast<int>(value.peer)
                       == static_cast<int>(v.peer_kind())
                     && value.expected_cluster.has_value()
                          == v.has_expected_cluster_id()
                     && (!value.expected_cluster || id_equal(*value.expected_cluster, v.expected_cluster_id()))
                     && value.broker.has_value() == v.has_broker_id()
                     && (!value.broker || id_equal(*value.broker, v.broker_id()))
                     && equal(value.build, v.build())
                     && equal(value.capabilities, v.capabilities());
          } else if constexpr (std::same_as<T, handshake_response>) {
              if (
                message.GetDescriptor()
                != native::HandshakeResponse::descriptor())
                  return false;
              const auto& v = static_cast<const native::HandshakeResponse&>(
                message);
              return id_equal(value.cluster, v.cluster_id())
                     && id_equal(value.broker, v.broker_id())
                     && equal(value.build, v.build())
                     && equal(value.capabilities, v.capabilities());
          } else if constexpr (std::same_as<T, redirect_control>) {
              if (message.GetDescriptor() != native::Redirect::descriptor())
                  return false;
              const auto& v = static_cast<const native::Redirect&>(message);
              return equal(value.destination, v.destination())
                     && id_equal(value.broker, v.broker_id())
                     && value.endpoint.host == v.endpoint().host()
                     && value.endpoint.port == v.endpoint().port()
                     && static_cast<int>(value.reason)
                          == static_cast<int>(v.reason());
          } else {
              if (message.GetDescriptor() != common::Error::descriptor())
                  return false;
              const auto& v = static_cast<const common::Error&>(message);
              return static_cast<int>(value.code) == static_cast<int>(v.code())
                     && value.reason.has_value() == v.has_reason()
                     && (!value.reason || *value.reason == v.reason())
                     && value.observed.has_value() == v.has_observed()
                     && (!value.observed || equal(*value.observed, v.observed()));
          }
      },
      data);
}
bool matches_expectation(
  const google::protobuf::Message& message,
  frame_kind kind,
  control_expectation expected) {
    switch (kind) {
    case frame_kind::handshake_request: {
        const auto& v = static_cast<const native::HandshakeRequest&>(message);
        return (!expected.cluster || !v.has_expected_cluster_id()
                || id_equal(*expected.cluster, v.expected_cluster_id()))
               && (!expected.broker || (v.has_broker_id() && id_equal(*expected.broker, v.broker_id())));
    }
    case frame_kind::handshake_response: {
        const auto& v = static_cast<const native::HandshakeResponse&>(message);
        return (!expected.cluster
                || id_equal(*expected.cluster, v.cluster_id()))
               && (!expected.broker || id_equal(*expected.broker, v.broker_id()));
    }
    case frame_kind::redirect: {
        const auto& v = static_cast<const native::Redirect&>(message);
        return (!expected.topic
                || id_equal(*expected.topic, v.destination().topic_id()))
               && (!expected.broker || id_equal(*expected.broker, v.broker_id()));
    }
    case frame_kind::error: {
        const auto& v = static_cast<const common::Error&>(message);
        return !expected.topic
               || (v.has_observed() && id_equal(*expected.topic, v.observed().topic_id()));
    }
    default:
        return false;
    }
}
bool control_unknowns_empty(const google::protobuf::Message& message) {
    const auto* reflection = message.GetReflection();
    if (!reflection->GetUnknownFields(message).empty()) return false;
    const auto* descriptor = message.GetDescriptor();
    for (int i = 0; i < descriptor->field_count(); ++i) {
        const auto* f = descriptor->field(i);
        if (f->cpp_type() != field::CPPTYPE_MESSAGE) continue;
        if (f->is_repeated()) {
            for (int n = 0; n < reflection->FieldSize(message, f); ++n)
                if (!control_unknowns_empty(
                      reflection->GetRepeatedMessage(message, f, n)))
                    return false;
        } else if (
          reflection->HasField(message, f)
          && !control_unknowns_empty(reflection->GetMessage(message, f)))
            return false;
    }
    return true;
}
} // namespace kwaque::protocol::testing
