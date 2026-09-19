#include "proto/kwaque/common/v1/build_info.pb.h"
#include "proto/kwaque/common/v1/capability.pb.h"
#include "proto/kwaque/common/v1/error.pb.h"
#include "proto/kwaque/common/v1/identity.pb.h"
#include "proto/kwaque/control/v1/handshake.pb.h"
#include "proto/kwaque/control/v1/redirect.pb.h"
#include "src/protocol/control_schema.h"

#include <google/protobuf/descriptor.h>
#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <limits>
#include <string>
#include <string_view>
#include <utility>

namespace {
namespace common = kwaque::common::v1;
namespace control = kwaque::control::v1;
namespace schema = kwaque::protocol::detail;
using google::protobuf::Descriptor;
using google::protobuf::FieldDescriptor;
using namespace std::literals;

// These trusted literals pin schema bytes and generated presence semantics.
// Generated parsing does not establish the control boundary's domain validity.
constexpr auto topic_bytes = "abcdefghijklmnop"sv;
constexpr auto range_bytes = "ABCDEFGHIJKLMNOP"sv;
constexpr auto segment_bytes = "0123456789abcdef"sv;
static_assert(topic_bytes.size() == 16);
static_assert(range_bytes.size() == 16);
static_assert(segment_bytes.size() == 16);

constexpr auto capabilities_wire
  = "\x0a\x02\x01\x02"
    "\x12\x0f\x08\x01\x10\x01\x18\x01\x21\x00\x00\x00\x00\x00\x00\x00\x00"
    "\x1a\x02\x00\x01"
    "\x20\x80\x80\x80\x08"
    "\x28\x80\x80\x80\x04"
    "\x30\x80\x20"
    "\x38\x80\x80\x40"
    "\x40\x80\x20"sv;
static_assert(capabilities_wire.size() == 45);

common::Capabilities capabilities() {
    common::Capabilities result;
    result.add_protocol_versions(1);
    result.add_protocol_versions(2);
    auto* format = result.add_formats();
    format->set_family(1);
    format->set_oldest_readable(1);
    format->set_current(1);
    format->set_supported_features(0);
    result.add_compression_codecs(0);
    result.add_compression_codecs(1);
    result.set_max_frame_body_bytes(16'777'216);
    result.set_max_expanded_batch_bytes(8'388'608);
    result.set_max_header_bytes(4096);
    result.set_max_record_bytes(1'048'576);
    result.set_max_original_records(4096);
    return result;
}

struct field_shape {
    const char* name;
    int number;
    FieldDescriptor::Type type;
    bool repeated{false};
    const char* type_name{nullptr};
};

void expect_fields(
  const Descriptor* descriptor, std::initializer_list<field_shape> fields) {
    ASSERT_NE(descriptor, nullptr);
    SCOPED_TRACE(descriptor->full_name());
    EXPECT_EQ(descriptor->field_count(), static_cast<int>(fields.size()));
    EXPECT_EQ(descriptor->real_oneof_decl_count(), 0);
    for (const auto& shape : fields) {
        SCOPED_TRACE(shape.name);
        const auto* field = descriptor->FindFieldByNumber(shape.number);
        ASSERT_NE(field, nullptr);
        EXPECT_EQ(field->name(), shape.name);
        EXPECT_EQ(field->type(), shape.type);
        EXPECT_EQ(field->is_repeated(), shape.repeated);
        EXPECT_FALSE(field->is_required());
        EXPECT_FALSE(field->is_map());
        EXPECT_EQ(field->has_presence(), !shape.repeated);
        EXPECT_EQ(
          field->is_packed(),
          shape.repeated && shape.type == FieldDescriptor::TYPE_UINT32);
        if (shape.type == FieldDescriptor::TYPE_MESSAGE) {
            ASSERT_NE(field->message_type(), nullptr);
            ASSERT_NE(shape.type_name, nullptr);
            EXPECT_EQ(field->message_type()->full_name(), shape.type_name);
        } else if (shape.type == FieldDescriptor::TYPE_ENUM) {
            ASSERT_NE(field->enum_type(), nullptr);
            ASSERT_NE(shape.type_name, nullptr);
            EXPECT_EQ(field->enum_type()->full_name(), shape.type_name);
        }
    }
}

TEST(ControlSchemaTest, CommonFieldNumbersTypesAndPresenceAreStable) {
    expect_fields(
      common::RoutingContext::descriptor(),
      {{"topic_id", 1, FieldDescriptor::TYPE_BYTES},
       {"range_id", 2, FieldDescriptor::TYPE_BYTES},
       {"routing_epoch", 3, FieldDescriptor::TYPE_UINT64},
       {"segment_id", 4, FieldDescriptor::TYPE_BYTES},
       {"segment_generation", 5, FieldDescriptor::TYPE_UINT64}});
    expect_fields(
      common::FormatCapability::descriptor(),
      {{"family", 1, FieldDescriptor::TYPE_UINT32},
       {"oldest_readable", 2, FieldDescriptor::TYPE_UINT32},
       {"current", 3, FieldDescriptor::TYPE_UINT32},
       {"supported_features", 4, FieldDescriptor::TYPE_FIXED64}});
    expect_fields(
      common::Capabilities::descriptor(),
      {{"protocol_versions", 1, FieldDescriptor::TYPE_UINT32, true},
       {"formats",
        2,
        FieldDescriptor::TYPE_MESSAGE,
        true,
        "kwaque.common.v1.FormatCapability"},
       {"compression_codecs", 3, FieldDescriptor::TYPE_UINT32, true},
       {"max_frame_body_bytes", 4, FieldDescriptor::TYPE_UINT32},
       {"max_expanded_batch_bytes", 5, FieldDescriptor::TYPE_UINT32},
       {"max_header_bytes", 6, FieldDescriptor::TYPE_UINT32},
       {"max_record_bytes", 7, FieldDescriptor::TYPE_UINT32},
       {"max_original_records", 8, FieldDescriptor::TYPE_UINT32}});
    expect_fields(
      common::Error::descriptor(),
      {{"code",
        1,
        FieldDescriptor::TYPE_ENUM,
        false,
        "kwaque.common.v1.ErrorCode"},
       {"reason", 2, FieldDescriptor::TYPE_STRING},
       {"observed",
        3,
        FieldDescriptor::TYPE_MESSAGE,
        false,
        "kwaque.common.v1.RoutingContext"}});
}

TEST(ControlSchemaTest, HandshakeFieldNumbersTypesAndPresenceAreStable) {
    expect_fields(
      control::HandshakeRequest::descriptor(),
      {{"peer_kind",
        1,
        FieldDescriptor::TYPE_ENUM,
        false,
        "kwaque.control.v1.PeerKind"},
       {"expected_cluster_id", 2, FieldDescriptor::TYPE_BYTES},
       {"broker_id", 3, FieldDescriptor::TYPE_BYTES},
       {"build",
        4,
        FieldDescriptor::TYPE_MESSAGE,
        false,
        "kwaque.common.v1.BuildInfo"},
       {"capabilities",
        5,
        FieldDescriptor::TYPE_MESSAGE,
        false,
        "kwaque.common.v1.Capabilities"}});
    expect_fields(
      control::HandshakeResponse::descriptor(),
      {{"cluster_id", 1, FieldDescriptor::TYPE_BYTES},
       {"broker_id", 2, FieldDescriptor::TYPE_BYTES},
       {"build",
        3,
        FieldDescriptor::TYPE_MESSAGE,
        false,
        "kwaque.common.v1.BuildInfo"},
       {"capabilities",
        4,
        FieldDescriptor::TYPE_MESSAGE,
        false,
        "kwaque.common.v1.Capabilities"}});
}

TEST(ControlSchemaTest, RedirectFieldNumbersTypesAndPresenceAreStable) {
    expect_fields(
      control::Endpoint::descriptor(),
      {{"host", 1, FieldDescriptor::TYPE_STRING},
       {"port", 2, FieldDescriptor::TYPE_UINT32}});
    expect_fields(
      control::Redirect::descriptor(),
      {{"destination",
        1,
        FieldDescriptor::TYPE_MESSAGE,
        false,
        "kwaque.common.v1.RoutingContext"},
       {"broker_id", 2, FieldDescriptor::TYPE_BYTES},
       {"endpoint",
        3,
        FieldDescriptor::TYPE_MESSAGE,
        false,
        "kwaque.control.v1.Endpoint"},
       {"reason",
        4,
        FieldDescriptor::TYPE_ENUM,
        false,
        "kwaque.common.v1.ErrorCode"}});
}

TEST(ControlSchemaTest, FrozenFieldRulesMatchAllGeneratedDeclarations) {
    using message = schema::control_message;
    using kind = schema::control_field_kind;
    const std::array<std::pair<message, const Descriptor*>, 9> messages{
      {{message::build_info, common::BuildInfo::descriptor()},
       {message::routing_context, common::RoutingContext::descriptor()},
       {message::format_capability, common::FormatCapability::descriptor()},
       {message::capabilities, common::Capabilities::descriptor()},
       {message::error, common::Error::descriptor()},
       {message::handshake_request, control::HandshakeRequest::descriptor()},
       {message::handshake_response, control::HandshakeResponse::descriptor()},
       {message::endpoint, control::Endpoint::descriptor()},
       {message::redirect, control::Redirect::descriptor()}}};
    for (const auto& [id, descriptor] : messages) {
        const auto* rules = schema::schema_for(id);
        ASSERT_NE(rules, nullptr);
        EXPECT_EQ(descriptor->full_name(), rules->full_name);
        EXPECT_EQ(
          descriptor->field_count(), static_cast<int>(rules->fields.size()));
        EXPECT_EQ(rules->informational_merge, id == message::build_info);
        for (const auto& rule : rules->fields) {
            const auto* field = descriptor->FindFieldByNumber(
              static_cast<int>(rule.number));
            ASSERT_NE(field, nullptr);
            EXPECT_EQ(field->is_repeated(), rule.repeated);
            EXPECT_EQ(
              field->has_presence(),
              !rule.repeated && !rules->informational_merge);
            FieldDescriptor::Type expected = FieldDescriptor::TYPE_MESSAGE;
            switch (rule.kind) {
            case kind::uint32:
                expected = FieldDescriptor::TYPE_UINT32;
                break;
            case kind::uint64:
                expected = FieldDescriptor::TYPE_UINT64;
                break;
            case kind::fixed64:
                expected = FieldDescriptor::TYPE_FIXED64;
                break;
            case kind::enumeration:
                expected = FieldDescriptor::TYPE_ENUM;
                break;
            case kind::identity:
                expected = FieldDescriptor::TYPE_BYTES;
                break;
            case kind::text:
                expected = FieldDescriptor::TYPE_STRING;
                break;
            case kind::message:
                ASSERT_NE(field->message_type(), nullptr);
                ASSERT_NE(schema::schema_for(rule.child), nullptr);
                EXPECT_EQ(
                  field->message_type()->full_name(),
                  schema::schema_for(rule.child)->full_name);
                break;
            }
            EXPECT_EQ(field->type(), expected);
            if (rule.repeated) EXPECT_EQ(rule.minimum_elements.value(), 1U);
        }
    }
    EXPECT_EQ(
      schema::schema_for(kwaque::protocol::frame_kind::submitted_batch),
      nullptr);
    EXPECT_EQ(
      schema::schema_for(kwaque::protocol::frame_kind::assigned_batch),
      nullptr);
}

TEST(ControlSchemaTest, EndpointAndRedirectLimitsAreExplicit) {
    const auto* endpoint = schema::schema_for(
      schema::control_message::endpoint);
    ASSERT_NE(endpoint, nullptr);
    const auto* host = schema::field_for(*endpoint, 1);
    const auto* port = schema::field_for(*endpoint, 2);
    ASSERT_NE(host, nullptr);
    ASSERT_NE(port, nullptr);
    EXPECT_TRUE(host->required);
    EXPECT_EQ(host->minimum_bytes.value(), 1U);
    EXPECT_EQ(host->maximum_bytes.value(), 253U);
    EXPECT_TRUE(port->required);
    EXPECT_EQ(port->minimum_value, 1U);
    EXPECT_EQ(port->maximum_value, 65535U);
    const auto* redirect = schema::schema_for(
      schema::control_message::redirect);
    ASSERT_NE(redirect, nullptr);
    const auto* reason = schema::field_for(*redirect, 4);
    ASSERT_NE(reason, nullptr);
    EXPECT_EQ(
      reason->enum_values,
      (std::uint64_t{1} << common::ERROR_CODE_STALE_ROUTING)
        | (std::uint64_t{1} << common::ERROR_CODE_STALE_SEGMENT)
        | (std::uint64_t{1} << common::ERROR_CODE_NOT_LEADER));
    for (const auto& field : redirect->fields)
        EXPECT_TRUE(field.required);
    const auto* error = schema::schema_for(schema::control_message::error);
    ASSERT_NE(error, nullptr);
    ASSERT_NE(schema::field_for(*error, 2), nullptr);
    EXPECT_EQ(schema::field_for(*error, 2)->maximum_bytes.value(), 1024U);
}

TEST(ControlSchemaTest, EndpointGoldenPreservesHostAndMaximumPort) {
    constexpr auto wire = "\x0a\x09localhost\x10\xff\xff\x03"sv;
    control::Endpoint value;
    ASSERT_TRUE(
      value.ParseFromArray(wire.data(), static_cast<int>(wire.size())));
    EXPECT_EQ(value.host(), "localhost");
    EXPECT_EQ(value.port(), 65535U);
    EXPECT_TRUE(value.has_host());
    EXPECT_TRUE(value.has_port());
    EXPECT_EQ(value.SerializeAsString(), wire);
    value.clear_port();
    EXPECT_FALSE(value.has_port());
    value.set_port(0);
    EXPECT_TRUE(value.has_port());
}

TEST(ControlSchemaTest, RedirectAndStaleErrorPreserveObservedContext) {
    constexpr auto routing_wire = "\x0a\x10"
                                  "abcdefghijklmnop"
                                  "\x12\x10"
                                  "ABCDEFGHIJKLMNOP"
                                  "\x18\x07\x22\x10"
                                  "0123456789abcdef"
                                  "\x28\x09"sv;
    static_assert(routing_wire.size() == 58);
    const auto redirect_wire = std::string{"\x0a\x3a", 2}
                               + std::string{routing_wire}
                               + std::string{
                                 "\x12\x10"
                                 "ABCDEFGHIJKLMNOP"
                                 "\x1a\x0d\x0a\x09localhost\x10\x01\x20\x03"};
    control::Redirect redirect;
    ASSERT_TRUE(redirect.ParseFromString(redirect_wire));
    EXPECT_EQ(redirect.destination().SerializeAsString(), routing_wire);
    EXPECT_EQ(redirect.broker_id(), range_bytes);
    EXPECT_EQ(redirect.endpoint().host(), "localhost");
    EXPECT_EQ(redirect.endpoint().port(), 1U);
    EXPECT_EQ(redirect.reason(), common::ERROR_CODE_STALE_ROUTING);
    EXPECT_EQ(redirect.SerializeAsString(), redirect_wire);
    const auto error_wire = std::string{"\x08\x04\x1a\x3a", 4}
                            + std::string{routing_wire};
    common::Error error;
    ASSERT_TRUE(error.ParseFromString(error_wire));
    EXPECT_EQ(error.code(), common::ERROR_CODE_STALE_SEGMENT);
    EXPECT_TRUE(error.has_observed());
    EXPECT_EQ(error.observed().SerializeAsString(), routing_wire);
    EXPECT_EQ(error.SerializeAsString(), error_wire);
}

TEST(ControlSchemaTest, WireEnumsHaveExplicitStableNumbers) {
    constexpr std::array error_codes{
      common::ERROR_CODE_UNSPECIFIED,
      common::ERROR_CODE_INVALID_REQUEST,
      common::ERROR_CODE_UNSUPPORTED,
      common::ERROR_CODE_STALE_ROUTING,
      common::ERROR_CODE_STALE_SEGMENT,
      common::ERROR_CODE_UNAVAILABLE,
      common::ERROR_CODE_RESOURCE_LIMIT,
      common::ERROR_CODE_TIMED_OUT,
      common::ERROR_CODE_ABORTED,
      common::ERROR_CODE_NOT_LEADER};
    EXPECT_EQ(common::ErrorCode_descriptor()->value_count(), 10);
    for (std::size_t i = 0; i < error_codes.size(); ++i)
        EXPECT_EQ(static_cast<int>(error_codes[i]), static_cast<int>(i));
    EXPECT_FALSE(common::ErrorCode_IsValid(10));
    EXPECT_EQ(control::PeerKind_descriptor()->value_count(), 3);
    EXPECT_EQ(control::PEER_KIND_UNSPECIFIED, 0);
    EXPECT_EQ(control::PEER_KIND_BROKER, 1);
    EXPECT_EQ(control::PEER_KIND_CLIENT, 2);
    EXPECT_FALSE(control::PeerKind_IsValid(3));
}

TEST(ControlSchemaTest, RoutingContextPreservesIdentityBytesAndU64Epochs) {
    constexpr auto wire = "\x0a\x10"
                          "abcdefghijklmnop"
                          "\x12\x10"
                          "ABCDEFGHIJKLMNOP"
                          "\x18\x07\x22\x10"
                          "0123456789abcdef"
                          "\x28\x09"sv;
    common::RoutingContext context;
    ASSERT_TRUE(
      context.ParseFromArray(wire.data(), static_cast<int>(wire.size())));
    EXPECT_EQ(context.topic_id(), topic_bytes);
    EXPECT_EQ(context.range_id(), range_bytes);
    EXPECT_EQ(context.routing_epoch(), 7U);
    EXPECT_EQ(context.segment_id(), segment_bytes);
    EXPECT_EQ(context.segment_generation(), 9U);
    EXPECT_EQ(context.SerializeAsString(), wire);
    context.set_routing_epoch(std::numeric_limits<std::uint64_t>::max());
    context.set_segment_generation(std::numeric_limits<std::uint64_t>::max());
    common::RoutingContext restored;
    ASSERT_TRUE(restored.ParseFromString(context.SerializeAsString()));
    EXPECT_EQ(
      restored.routing_epoch(), std::numeric_limits<std::uint64_t>::max());
    EXPECT_EQ(
      restored.segment_generation(), std::numeric_limits<std::uint64_t>::max());
}

TEST(ControlSchemaTest, ZeroFeaturesRemainPresentAndUseFixed64Encoding) {
    common::FormatCapability format;
    EXPECT_FALSE(format.has_supported_features());
    format.set_supported_features(0);
    EXPECT_TRUE(format.has_supported_features());
    constexpr auto zero = "\x21\x00\x00\x00\x00\x00\x00\x00\x00"sv;
    EXPECT_EQ(format.SerializeAsString(), zero);
    common::FormatCapability restored;
    ASSERT_TRUE(
      restored.ParseFromArray(zero.data(), static_cast<int>(zero.size())));
    EXPECT_TRUE(restored.has_supported_features());
    EXPECT_EQ(restored.supported_features(), 0U);
    format.set_supported_features(0x0807060504030201ULL);
    EXPECT_EQ(
      format.SerializeAsString(), "\x21\x01\x02\x03\x04\x05\x06\x07\x08"sv);
    format.clear_supported_features();
    EXPECT_FALSE(format.has_supported_features());
    EXPECT_TRUE(format.SerializeAsString().empty());
}

TEST(ControlSchemaTest, ErrorPresenceDistinguishesAbsentZeroAndEmptyReason) {
    common::Error error;
    EXPECT_FALSE(error.has_code());
    EXPECT_FALSE(error.has_reason());
    EXPECT_FALSE(error.has_observed());
    error.set_code(common::ERROR_CODE_UNSPECIFIED);
    EXPECT_TRUE(error.has_code());
    EXPECT_EQ(error.SerializeAsString(), "\x08\x00"sv);
    error.set_code(common::ERROR_CODE_INVALID_REQUEST);
    EXPECT_EQ(error.SerializeAsString(), "\x08\x01"sv);
    error.set_reason("");
    EXPECT_TRUE(error.has_reason());
    EXPECT_EQ(error.SerializeAsString(), "\x08\x01\x12\x00"sv);
    common::Error restored;
    ASSERT_TRUE(restored.ParseFromString(error.SerializeAsString()));
    EXPECT_TRUE(restored.has_reason());
    EXPECT_TRUE(restored.reason().empty());
    error.mutable_observed()->set_routing_epoch(7);
    EXPECT_TRUE(error.has_observed());
    error.clear_observed();
    EXPECT_FALSE(error.has_observed());
}

TEST(ControlSchemaTest, CapabilitiesUsePackedSetsAndTheFrozenLimitFields) {
    const auto source = capabilities();
    EXPECT_EQ(source.SerializeAsString(), capabilities_wire);
    common::Capabilities restored;
    ASSERT_TRUE(restored.ParseFromArray(
      capabilities_wire.data(), static_cast<int>(capabilities_wire.size())));
    ASSERT_EQ(restored.protocol_versions_size(), 2);
    EXPECT_EQ(restored.protocol_versions(0), 1U);
    EXPECT_EQ(restored.protocol_versions(1), 2U);
    ASSERT_EQ(restored.formats_size(), 1);
    EXPECT_EQ(restored.formats(0).family(), 1U);
    EXPECT_TRUE(restored.formats(0).has_supported_features());
    EXPECT_EQ(restored.formats(0).supported_features(), 0U);
    ASSERT_EQ(restored.compression_codecs_size(), 2);
    EXPECT_EQ(restored.compression_codecs(0), 0U);
    EXPECT_EQ(restored.compression_codecs(1), 1U);
    EXPECT_EQ(restored.max_frame_body_bytes(), 16'777'216U);
    EXPECT_EQ(restored.max_expanded_batch_bytes(), 8'388'608U);
    EXPECT_EQ(restored.max_header_bytes(), 4096U);
    EXPECT_EQ(restored.max_record_bytes(), 1'048'576U);
    EXPECT_EQ(restored.max_original_records(), 4096U);
}

TEST(ControlSchemaTest, FutureAdvertisementsArePreservedByGeneratedMessages) {
    auto source = capabilities();
    source.add_protocol_versions(65535);
    source.add_compression_codecs(255);
    auto* future = source.add_formats();
    future->set_family(65535);
    future->set_oldest_readable(1);
    future->set_current(65535);
    future->set_supported_features(std::numeric_limits<std::uint64_t>::max());
    common::Capabilities restored;
    ASSERT_TRUE(restored.ParseFromString(source.SerializeAsString()));
    ASSERT_EQ(restored.protocol_versions_size(), 3);
    EXPECT_EQ(restored.protocol_versions(2), 65535U);
    ASSERT_EQ(restored.compression_codecs_size(), 3);
    EXPECT_EQ(restored.compression_codecs(2), 255U);
    ASSERT_EQ(restored.formats_size(), 2);
    EXPECT_EQ(restored.formats(1).family(), 65535U);
    EXPECT_EQ(restored.formats(1).current(), 65535U);
    EXPECT_EQ(
      restored.formats(1).supported_features(),
      std::numeric_limits<std::uint64_t>::max());
}

TEST(ControlSchemaTest, UnknownEnumNumbersRemainVisibleToBoundaryValidation) {
    constexpr auto wire = "\x08\x63"sv;
    common::Error error;
    ASSERT_TRUE(
      error.ParseFromArray(wire.data(), static_cast<int>(wire.size())));
    EXPECT_TRUE(error.has_code());
    EXPECT_EQ(static_cast<int>(error.code()), 99);
    EXPECT_FALSE(common::ErrorCode_IsValid(error.code()));
    control::HandshakeRequest request;
    ASSERT_TRUE(
      request.ParseFromArray(wire.data(), static_cast<int>(wire.size())));
    EXPECT_TRUE(request.has_peer_kind());
    EXPECT_EQ(static_cast<int>(request.peer_kind()), 99);
    EXPECT_FALSE(control::PeerKind_IsValid(request.peer_kind()));
}

TEST(ControlSchemaTest, BrokerHandshakePreservesIdentityBuildAndCapabilities) {
    control::HandshakeRequest source;
    source.set_peer_kind(control::PEER_KIND_BROKER);
    source.set_expected_cluster_id(std::string{topic_bytes});
    source.set_broker_id(std::string{range_bytes});
    source.mutable_build()->set_version("v");
    *source.mutable_capabilities() = capabilities();
    const auto wire = std::string{"\x08\x01\x12\x10"
                                  "abcdefghijklmnop"
                                  "\x1a\x10"
                                  "ABCDEFGHIJKLMNOP"
                                  "\x22\x03\x0a\x01v\x2a\x2d"}
                      + std::string{capabilities_wire};
    EXPECT_EQ(source.SerializeAsString(), wire);
    control::HandshakeRequest restored;
    ASSERT_TRUE(restored.ParseFromString(wire));
    EXPECT_EQ(restored.peer_kind(), control::PEER_KIND_BROKER);
    EXPECT_EQ(restored.expected_cluster_id(), topic_bytes);
    EXPECT_EQ(restored.broker_id(), range_bytes);
    EXPECT_TRUE(restored.has_build());
    EXPECT_EQ(restored.build().version(), "v");
    EXPECT_TRUE(restored.has_capabilities());
    EXPECT_EQ(restored.capabilities().SerializeAsString(), capabilities_wire);
}

TEST(ControlSchemaTest, DiscoveryPreservesAbsentIdsAndPresentEmptyBuildInfo) {
    control::HandshakeRequest source;
    source.set_peer_kind(control::PEER_KIND_CLIENT);
    static_cast<void>(source.mutable_build());
    *source.mutable_capabilities() = capabilities();
    const auto wire = std::string{"\x08\x02\x22\x00\x2a\x2d", 6}
                      + std::string{capabilities_wire};
    EXPECT_EQ(source.SerializeAsString(), wire);
    control::HandshakeRequest restored;
    ASSERT_TRUE(restored.ParseFromString(wire));
    EXPECT_EQ(restored.peer_kind(), control::PEER_KIND_CLIENT);
    EXPECT_FALSE(restored.has_expected_cluster_id());
    EXPECT_FALSE(restored.has_broker_id());
    EXPECT_TRUE(restored.has_build());
    EXPECT_TRUE(restored.build().SerializeAsString().empty());
    EXPECT_TRUE(restored.has_capabilities());
    source.set_expected_cluster_id("");
    ASSERT_TRUE(restored.ParseFromString(source.SerializeAsString()));
    EXPECT_TRUE(restored.has_expected_cluster_id());
    EXPECT_TRUE(restored.expected_cluster_id().empty());
}

TEST(ControlSchemaTest, HandshakeResponseCarriesIndependentServerIdentity) {
    control::HandshakeResponse source;
    source.set_cluster_id(std::string{topic_bytes});
    source.set_broker_id(std::string{range_bytes});
    static_cast<void>(source.mutable_build());
    *source.mutable_capabilities() = capabilities();
    const auto wire = std::string{
      "\x0a\x10"
      "abcdefghijklmnop"
      "\x12\x10"
      "ABCDEFGHIJKLMNOP"
      "\x1a\x00\x22\x2d", 40} + std::string{capabilities_wire};
    EXPECT_EQ(source.SerializeAsString(), wire);
    control::HandshakeResponse restored;
    ASSERT_TRUE(restored.ParseFromString(wire));
    EXPECT_EQ(restored.cluster_id(), topic_bytes);
    EXPECT_EQ(restored.broker_id(), range_bytes);
    EXPECT_TRUE(restored.has_build());
    EXPECT_TRUE(restored.has_capabilities());
    EXPECT_EQ(restored.capabilities().SerializeAsString(), capabilities_wire);
}

} // namespace
