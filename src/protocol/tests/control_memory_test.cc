#include "proto/kwaque/common/v1/build_info.pb.h"
#include "proto/kwaque/common/v1/capability.pb.h"
#include "proto/kwaque/common/v1/error.pb.h"
#include "proto/kwaque/common/v1/identity.pb.h"
#include "proto/kwaque/control/v1/handshake.pb.h"
#include "proto/kwaque/control/v1/redirect.pb.h"
#include "src/base/allocation.h"
#include "src/bytes/test_allocation_profile.h"
#include "src/codec/transaction.h"
#include "src/protocol/control_memory.h"

#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>

#include <google/protobuf/message.h>
#include <google/protobuf/repeated_field.h>
#include <google/protobuf/repeated_ptr_field.h>
#include <google/protobuf/unknown_field_set.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <malloc.h>
#include <memory>
#include <string>
#include <string_view>

namespace {
using namespace kwaque;
namespace common = kwaque::common::v1;
namespace control = kwaque::control::v1;
namespace detail = kwaque::protocol::detail;
using message = detail::control_message;
using bytes::testing::charge;
constexpr std::array roots{
  message::handshake_request,
  message::handshake_response,
  message::redirect,
  message::error};

void expect_error(const auto& result, errc code) {
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), code);
}

google::protobuf::UnknownFieldSet& unknown(google::protobuf::Message& value) {
    return *value.GetReflection()->MutableUnknownFields(&value);
}
void routing(common::RoutingContext& value) {
    value.set_topic_id(std::string(16, 't'));
    value.set_range_id(std::string(16, 'r'));
    value.set_routing_epoch(7);
    value.set_segment_id(std::string(16, 's'));
    value.set_segment_generation(11);
}
void capabilities(common::Capabilities& value) {
    for (std::uint32_t i = 1; i <= 16; ++i)
        value.add_protocol_versions(i);
    for (std::uint32_t i = 0; i < 16; ++i)
        value.add_compression_codecs(i);
    for (std::uint32_t i = 1; i <= 32; ++i) {
        auto* format = value.add_formats();
        format->set_family(i);
        format->set_oldest_readable(1);
        format->set_current(2);
        format->set_supported_features(0);
        unknown(*format).AddVarint(100, i);
    }
    value.set_max_frame_body_bytes(16'777'216);
    value.set_max_expanded_batch_bytes(8'388'608);
    value.set_max_header_bytes(4096);
    value.set_max_record_bytes(1'048'576);
    value.set_max_original_records(4096);
}
void build(common::BuildInfo& value) {
    value.set_version(std::string(1360, 'v'));
    value.set_revision(std::string(1360, 'r'));
    value.set_build_mode(std::string(1340, 'b'));
    ASSERT_LE(value.ByteSizeLong(), 4096U);
}

// These are constructed, bounded test fixtures, not a new ingress parser.
// Native size observations supplement the source-derived migration bound;
// SpaceUsedLong alone is not an admission or served-allocation proof.
template<typename T>
void qualify(message id, const std::string& wire, bool accepted = true) {
    ASSERT_LE(wire.size(), 65536U);
    const auto cost = detail::bound_control_parse(
      id, byte_count{wire.size()}, codec::limits::defaults(), charge);
    ASSERT_TRUE(cost.has_value());
    auto value = std::make_unique<T>();
    EXPECT_LE(
      ::malloc_usable_size(value.get()), charge(byte_count{sizeof(T)}).value());
    seastar::temporary_buffer<char> input{wire.size()};
    EXPECT_LE(
      ::malloc_usable_size(input.get_write()), cost->input_copy.value());
    std::memcpy(input.get_write(), wire.data(), wire.size());
    const auto parsed = value->ParseFromArray(
      input.get(), static_cast<int>(input.size()));
    EXPECT_EQ(parsed, accepted);
    EXPECT_LE(value->SpaceUsedLong(), cost->generated_peak.value());
    EXPECT_LE(
      cost->generated_peak.value() + cost->input_copy.value(), 1U << 20U);
    EXPECT_LE(
      cost->largest_allocation.value(), maximum_contiguous_allocation_bytes);
    seastar::thread::maybe_yield();
}

TEST(ControlMemoryTest, MaximumRootProofsFitTheAggregateAndContiguousCaps) {
    for (const auto id : roots) {
        const auto cost = detail::bound_control_parse(
          id, byte_count{65536}, codec::limits::defaults(), charge);
        ASSERT_TRUE(cost.has_value());
        EXPECT_GT(cost->generated_peak.value(), 0U);
        EXPECT_EQ(cost->input_copy, charge(byte_count{65536}));
        EXPECT_LE(
          cost->generated_peak.value() + cost->input_copy.value(), 1U << 20U);
        EXPECT_LE(
          cost->largest_allocation.value(),
          maximum_contiguous_allocation_bytes);
        const auto name = std::string{detail::schema_for(id)->full_name};
        ::testing::Test::RecordProperty(
          name + ".generated_peak",
          std::to_string(cost->generated_peak.value()));
        ::testing::Test::RecordProperty(
          name + ".input_copy", std::to_string(cost->input_copy.value()));
    }
}

TEST(ControlMemoryTest, FreshNativeMessagesAndUnknownsStayWithinEachRootProof) {
    // Full packed capabilities use 207 request units. One unknown in each of
    // 32 formats plus fourteen blobs and three scalars reaches exactly 256.
    control::HandshakeRequest request;
    request.set_peer_kind(control::PEER_KIND_BROKER);
    request.set_expected_cluster_id(std::string(16, 'c'));
    request.set_broker_id(std::string(16, 'b'));
    build(*request.mutable_build());
    capabilities(*request.mutable_capabilities());
    for (int i = 0; i < 14; ++i)
        unknown(request).AddLengthDelimited(100 + i, std::string(4096, 'u'));
    for (int i = 0; i < 3; ++i)
        unknown(request).AddVarint(200 + i, 1);
    qualify<control::HandshakeRequest>(
      message::handshake_request, request.SerializeAsString());

    control::HandshakeResponse response;
    response.set_cluster_id(request.expected_cluster_id());
    response.set_broker_id(request.broker_id());
    *response.mutable_build() = request.build();
    *response.mutable_capabilities() = request.capabilities();
    for (int i = 0; i < 14; ++i)
        unknown(response).AddLengthDelimited(100 + i, std::string(4096, 'u'));
    qualify<control::HandshakeResponse>(
      message::handshake_response, response.SerializeAsString());

    control::Redirect redirect;
    routing(*redirect.mutable_destination());
    redirect.set_broker_id(std::string(16, 'b'));
    redirect.mutable_endpoint()->set_host(std::string(253, 'h'));
    redirect.mutable_endpoint()->set_port(65535);
    redirect.set_reason(common::ERROR_CODE_STALE_ROUTING);
    for (int i = 0; i < 15; ++i)
        unknown(redirect).AddLengthDelimited(100 + i, std::string(4096, 'u'));
    qualify<control::Redirect>(message::redirect, redirect.SerializeAsString());

    common::Error error;
    error.set_code(common::ERROR_CODE_STALE_SEGMENT);
    error.set_reason(std::string(1024, 'e'));
    routing(*error.mutable_observed());
    for (int i = 0; i < 15; ++i)
        unknown(error).AddLengthDelimited(100 + i, std::string(4096, 'u'));
    qualify<common::Error>(message::error, error.SerializeAsString());
}

TEST(ControlMemoryTest, TinyUnknownFieldsExerciseObjectAndArrayAmplification) {
    for (const std::size_t size : {0U, 1U, 22U, 23U, 31U, 32U, 127U}) {
        common::Error error;
        error.set_code(common::ERROR_CODE_INVALID_REQUEST);
        for (int i = 0; i < 255; ++i)
            unknown(error).AddLengthDelimited(100 + i, std::string(size, 'x'));
        qualify<common::Error>(message::error, error.SerializeAsString());
    }
}

void append_varint(std::string& wire, std::uint64_t value) {
    while (value >= 128) {
        wire.push_back(static_cast<char>((value & 127U) | 128U));
        value >>= 7U;
    }
    wire.push_back(static_cast<char>(value));
}
void append_field(
  std::string& wire, std::uint32_t field, std::string_view value) {
    append_varint(wire, (std::uint64_t{field} << 3U) | 2U);
    append_varint(wire, value.size());
    wire.append(value);
}

TEST(ControlMemoryTest, LegacyOverwritesAndNativeFailurePrefixesAreBounded) {
    std::string wire;
    for (const std::size_t n :
         {23U,
          24U,
          31U,
          32U,
          63U,
          64U,
          127U,
          128U,
          255U,
          256U,
          511U,
          512U,
          1000U})
        append_field(wire, 1, std::string(n, 'x'));
    ASSERT_LE(wire.size(), 4096U);
    qualify<common::BuildInfo>(message::build_info, wire);
    wire.assign("\x08\x01", 2);
    for (std::uint32_t i = 0; i < 15; ++i)
        append_field(wire, 100 + i, std::string(4096, 'x'));
    // Structurally bounded, with the invalid known UTF-8 field last: the native
    // parser retains the preceding unknown allocations before reporting
    // failure. Observe generated storage only; production preflight rejects
    // this input before native diagnostic logging, whose cost is not sampled
    // here.
    append_field(wire, 2, std::string(1, static_cast<char>(0xff)));
    qualify<common::Error>(message::error, wire, false);
}

TEST(ControlMemoryTest, ActualStringGrowthAndNativeArraysFitTheRequestBounds) {
    std::string text;
    const auto inline_capacity = text.capacity();
    std::size_t largest = 0;
    for (const std::size_t n :
         {0U,
          1U,
          22U,
          23U,
          24U,
          31U,
          32U,
          63U,
          64U,
          255U,
          256U,
          1023U,
          1024U,
          2047U,
          2048U,
          4095U,
          4096U,
          1U,
          4096U}) {
        text.assign(n, 'x');
        largest = std::max(largest, n);
        EXPECT_LE(
          text.capacity() + 1U, 2U * largest + 2U * alignof(std::max_align_t));
        if (text.capacity() > inline_capacity)
            EXPECT_LE(
              ::malloc_usable_size(text.data()),
              charge(byte_count{text.capacity() + 1U}).value());
    }
    google::protobuf::RepeatedField<std::uint32_t> numbers;
    google::protobuf::RepeatedPtrField<common::FormatCapability> formats;
    google::protobuf::UnknownFieldSet fields;
    for (std::uint32_t n = 1; n <= 256; ++n) {
        if (n <= 16) {
            numbers.Add(n);
            EXPECT_LE(
              numbers.SpaceUsedExcludingSelfLong(),
              2U * n * sizeof(std::uint32_t) + 2U * sizeof(numbers));
        }
        if (n <= 32) {
            formats.Add()->set_family(n);
            EXPECT_LE(
              static_cast<std::uint64_t>(formats.Capacity()) * sizeof(void*),
              2U * n * sizeof(void*) + 2U * sizeof(formats));
        }
        fields.AddVarint(static_cast<int>(n), n);
        EXPECT_LE(
          fields.SpaceUsedExcludingSelfLong(),
          2U * n * sizeof(google::protobuf::UnknownField)
            + 2U
                * sizeof(google::protobuf::RepeatedField<
                         google::protobuf::UnknownField>));
        seastar::thread::maybe_yield();
    }
}

byte_count undercharge(byte_count request) noexcept {
    return byte_count{request.value() == 0 ? 0 : request.value() - 1U};
}

TEST(ControlMemoryTest, LimitsAndParentResidualsCannotBeWidened) {
    const auto policy = codec::limits::defaults();
    const auto cost
      = detail::bound_control_parse(
          message::handshake_request, byte_count{65536}, policy, charge)
          .value();
    auto fewer = policy.config();
    fewer.max_control_fields = item_count{32};
    fewer.max_control_repeated = item_count{2};
    fewer.max_nesting_depth = item_count{2};
    const auto smaller = detail::bound_control_parse(
      message::handshake_request,
      byte_count{65536},
      codec::limits::make(fewer).value(),
      charge);
    ASSERT_TRUE(smaller.has_value());
    EXPECT_LT(smaller->generated_peak, cost.generated_peak);
    EXPECT_EQ(smaller->input_copy, cost.input_copy);
    for (unsigned which = 0; which < 4; ++which) {
        auto config = policy.config();
        if (which == 0)
            config.max_metadata_bytes = byte_count{
              cost.generated_peak.value() - 1U};
        if (which == 1)
            config.max_allocation_bytes = byte_count{
              cost.largest_allocation.value() - 1U};
        if (which == 2)
            config.max_operation_bytes = byte_count{
              cost.generated_peak.value() + cost.input_copy.value() - 1U};
        if (which == 3)
            config.max_retained_bytes = byte_count{
              cost.input_copy.value() - 1U};
        const auto narrowed = codec::limits::make(config).value();
        expect_error(
          detail::bound_control_parse(
            message::handshake_request, byte_count{65536}, narrowed, charge),
          errc::resource_exhausted);
    }
    expect_error(
      detail::bound_control_parse(
        message::error, byte_count{65537}, policy, charge),
      errc::resource_exhausted);
    expect_error(
      detail::bound_control_parse(
        message::build_info, byte_count{4097}, policy, charge),
      errc::resource_exhausted);
    expect_error(
      detail::bound_control_parse(
        message::error, byte_count{2}, policy, nullptr),
      errc::invalid_argument);
    expect_error(
      detail::bound_control_parse(
        message::error, byte_count{2}, policy, undercharge),
      errc::invalid_argument);
    expect_error(
      detail::bound_control_parse(
        static_cast<message>(255), byte_count{2}, policy, charge),
      errc::invalid_argument);
    for (const bool zero_metadata : {false, true}) {
        codec::decode_budget parent{
          byte_count{1U << 20U}, byte_count{1U << 20U}, charge};
        if (zero_metadata)
            parent.metadata_remaining = {};
        else
            parent.operation_remaining = {};
        expect_error(
          codec::detail::consume_decode_budget(
            policy, parent, cost.input_copy, cost.generated_peak, {}, 0),
          errc::resource_exhausted);
    }
}
} // namespace
