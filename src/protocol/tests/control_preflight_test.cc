#include "proto/kwaque/common/v1/error.pb.h"
#include "proto/kwaque/control/v1/handshake.pb.h"
#include "src/protocol/control_preflight.h"
#include "src/protocol/tests/control_test_support.h"

#include <seastar/core/abort_source.hh>

#include <gtest/gtest.h>

#include <array>
#include <string>
#include <utility>

namespace {
using namespace kwaque;
namespace detail = protocol::detail;
namespace fixture = protocol::testing::control_fixture;
using message = detail::control_message;
using fixture::blob;
using fixture::scalar;

auto inspect(
  const std::string& wire,
  message schema = message::error,
  codec::limits policy = codec::limits::defaults(),
  codec::field_context context = fixture::context) {
    seastar::abort_source abort;
    codec::cooperative_work work{policy, abort};
    return detail::preflight_control(
             std::span<const char>{wire}, schema, work, context)
      .get();
}
void expect_error(const auto& result, errc code) {
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), code);
}

TEST(
  ControlPreflightTest, NonMinimalTagsLengthsAndScalarsAgreeWithNativeParser) {
    const std::array wires{
      std::string{"\x08\x81\x00", 3},
      std::string{"\x88\x80\x80\x80\x00\x01", 6},
      scalar(1, 1) + std::string{"\x12\x80\x80\x80\x80\x00", 6},
      scalar(1, 1) + scalar(536870911, UINT64_MAX),
      scalar(1, 1) + blob(55, std::string{"\xff", 1})};
    for (const auto& wire : wires) {
        ASSERT_TRUE(inspect(wire).has_value());
        kwaque::common::v1::Error native;
        ASSERT_TRUE(
          native.ParseFromArray(wire.data(), static_cast<int>(wire.size())));
        EXPECT_EQ(
          native.code(), kwaque::common::v1::ERROR_CODE_INVALID_REQUEST);
    }
}

TEST(
  ControlPreflightTest, MalformedTagsLengthsAndScalarHighBitsCannotTruncate) {
    const std::array wires{
      std::string{"\0", 1},
      std::string{"\x0e", 1},
      std::string{"\x88\x80\x80\x80\x80\x00\x01", 7},
      std::string{"\x88\x80\x80\x80\x10\x01", 6},
      scalar(1, 1) + std::string{"\x12\x80\x80\x80\x80\x80\x00", 7},
      scalar(1, 1) + std::string{"\x12\xff\xff\xff\xff\x07", 6},
      scalar(1, 1)
        + std::string{"\xa0\x01\xff\xff\xff\xff\xff\xff\xff\xff\xff\x02", 12},
      scalar(1, 1) + std::string{"\xa1\x01\x00", 3},
      scalar(1, 1) + std::string{"\xa5\x01\x00", 3}};
    for (const auto& wire : wires)
        expect_error(inspect(wire), errc::malformed_data);
    expect_error(inspect(scalar(1, 0)), errc::malformed_data);
    expect_error(inspect(scalar(1, UINT64_MAX)), errc::unsupported_format);
    expect_error(inspect(scalar(1, 10)), errc::unsupported_format);
    expect_error(inspect(std::string{"\x0b", 1}), errc::unsupported_format);
    expect_error(inspect(std::string{"\x0c", 1}), errc::unsupported_format);
    const auto bad_endpoint = blob(1, "h")
                              + scalar(2, (std::uint64_t{1} << 32U) + 1U);
    expect_error(
      inspect(bad_endpoint, message::endpoint), errc::malformed_data);
}

TEST(
  ControlPreflightTest, PresenceDuplicateAndKnownWrongTypeRulesPrecedeMerge) {
    expect_error(inspect(""), errc::malformed_data);
    expect_error(inspect(blob(1, "")), errc::malformed_data);
    expect_error(inspect(scalar(1, 1) + scalar(1, 2)), errc::malformed_data);
    expect_error(
      inspect(scalar(1, 1) + blob(2, "") + blob(2, "x")), errc::malformed_data);
    const auto valid = fixture::request();
    ASSERT_TRUE(inspect(valid, message::handshake_request).has_value());
    expect_error(
      inspect(valid + blob(4, ""), message::handshake_request),
      errc::malformed_data);
    const auto build = blob(1, "old") + scalar(1, 9) + blob(1, "new")
                       + blob(99, "unknown");
    const auto wire = fixture::request(build);
    ASSERT_TRUE(inspect(wire, message::handshake_request).has_value());
    kwaque::control::v1::HandshakeRequest native;
    ASSERT_TRUE(
      native.ParseFromArray(wire.data(), static_cast<int>(wire.size())));
    EXPECT_EQ(native.build().version(), "new");
    expect_error(
      inspect(
        fixture::request(blob(1, std::string{"\xff", 1})),
        message::handshake_request),
      errc::malformed_data);
}

TEST(ControlPreflightTest, PackedUnpackedInterleavingSharesCountsAndOrdering) {
    const auto mixed = scalar(1, 1) + blob(1, "") + blob(1, "\x02")
                       + scalar(1, 3);
    auto wire = fixture::request({}, fixture::capabilities(mixed));
    ASSERT_TRUE(inspect(wire, message::handshake_request).has_value());
    kwaque::control::v1::HandshakeRequest native;
    ASSERT_TRUE(
      native.ParseFromArray(wire.data(), static_cast<int>(wire.size())));
    ASSERT_EQ(native.capabilities().protocol_versions_size(), 3);
    EXPECT_EQ(native.capabilities().protocol_versions(2), 3);
    for (const auto& versions :
         {blob(1, ""),
          scalar(1, 0),
          scalar(1, 1) + blob(1, "\x01"),
          blob(1, std::string{"\x80", 1}),
          scalar(1, 65536)}) {
        expect_error(
          inspect(
            fixture::request({}, fixture::capabilities(versions)),
            message::handshake_request),
          errc::malformed_data);
    }
    std::string versions;
    for (std::uint32_t i = 1; i <= 16; ++i)
        versions += scalar(1, i);
    ASSERT_TRUE(inspect(fixture::capabilities(versions), message::capabilities)
                  .has_value());
    expect_error(
      inspect(
        fixture::capabilities(versions + scalar(1, 17)), message::capabilities),
      errc::resource_exhausted);
}

TEST(ControlPreflightTest, AggregateUnitsNestedDepthAndByteCapsIntersect) {
    auto wire = scalar(1, 1);
    for (unsigned i = 0; i < 255; ++i)
        wire += scalar(100, i);
    const auto accepted = inspect(wire);
    ASSERT_TRUE(accepted.has_value());
    EXPECT_EQ(*accepted, item_count{256});
    expect_error(inspect(wire + scalar(100, 0)), errc::resource_exhausted);
    for (std::uint64_t depth = 1; depth <= 3; ++depth) {
        auto config = codec::limits::defaults().config();
        config.max_nesting_depth = item_count{depth};
        auto result = inspect(
          fixture::request(),
          message::handshake_request,
          *codec::limits::make(config));
        if (depth == 3)
            EXPECT_TRUE(result.has_value());
        else
            expect_error(result, errc::resource_exhausted);
    }
    ASSERT_TRUE(
      inspect(scalar(1, 1) + blob(99, std::string(4096, 'x'))).has_value());
    expect_error(
      inspect(scalar(1, 1) + blob(99, std::string(4097, 'x'))),
      errc::resource_exhausted);
    expect_error(
      inspect(scalar(1, 1) + blob(2, std::string(1025, 'r'))),
      errc::resource_exhausted);
    expect_error(
      inspect(
        fixture::request(blob(1, std::string(4094, 'x'))),
        message::handshake_request),
      errc::resource_exhausted);
}

TEST(ControlPreflightTest, IdentityEndpointAndDiagnosticDomainsAreChecked) {
    for (std::size_t width : {15U, 17U})
        expect_error(
          inspect(
            blob(1, std::string(width, 't')) + blob(2, std::string(16, 'r'))
              + scalar(3, 1) + blob(4, std::string(16, 's')) + scalar(5, 1),
            message::routing_context),
          errc::malformed_data);
    expect_error(
      inspect(
        blob(1, std::string(16, '\0')) + blob(2, std::string(16, 'r'))
          + scalar(3, 1) + blob(4, std::string(16, 's')) + scalar(5, 1),
        message::routing_context),
      errc::malformed_data);
    ASSERT_TRUE(
      inspect(fixture::routing(), message::routing_context).has_value());
    for (const auto& host :
         {std::string{},
          std::string{"a b"},
          std::string{"a\0b", 3},
          std::string{"\xff", 1}})
        expect_error(
          inspect(fixture::redirect(host), message::redirect),
          errc::malformed_data);
    ASSERT_TRUE(
      inspect(fixture::redirect("h\xc3\xb6st"), message::redirect).has_value());
    expect_error(
      inspect(fixture::redirect("h", 65536), message::redirect),
      errc::malformed_data);
    const auto result = inspect(
      scalar(1, 1) + fixture::varint((std::uint64_t{536870911} << 3U) | 5U));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().field(), fixture::context.field);
    EXPECT_EQ(result.error().byte_offset(), fixture::context.origin + 7);
    auto context = fixture::context;
    context.origin = UINT64_MAX;
    expect_error(
      inspect(scalar(1, 1), message::error, codec::limits::defaults(), context),
      errc::invalid_argument);
}
} // namespace
