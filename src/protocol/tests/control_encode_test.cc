#include "proto/kwaque/common/v1/error.pb.h"
#include "proto/kwaque/control/v1/handshake.pb.h"
#include "src/protocol/tests/control_test_support.h"

#include <gtest/gtest.h>

#include <array>
#include <string>
#include <variant>

namespace {
using namespace kwaque;
namespace fixture = protocol::testing::control_fixture;
using fixture::blob;
using fixture::scalar;
using protocol::frame_kind;
void expect_error(const auto& result, errc code) {
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), code);
}
auto encode(
  const protocol::control& value,
  codec::cooperative_work& work,
  byte_count budget = fixture::budget().operation_remaining) {
    return protocol::encode_control(
             value, work, {}, budget, fixture::charge, fixture::context)
      .get();
}

TEST(ControlEncodeTest, FourConcreteControlsMatchIndependentWireVectors) {
    const std::array cases{
      std::pair{frame_kind::handshake_request, fixture::request()},
      std::pair{
        frame_kind::handshake_response, fixture::response(blob(1, "release"))},
      std::pair{frame_kind::redirect, fixture::redirect()},
      std::pair{
        frame_kind::error,
        scalar(1, 3) + blob(2, "stale") + blob(3, fixture::routing())}};
    for (const auto& [kind, wire] : cases) {
        auto value = fixture::read(wire, kind);
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto encoded = encode(value, work);
        ASSERT_TRUE(encoded.has_value());
        EXPECT_EQ(fixture::flatten(*encoded), wire);
        auto data = value.data();
        auto constructed = protocol::make_control(
                             std::move(data), fixture::budget(), work)
                             .get();
        ASSERT_TRUE(constructed.has_value());
        auto rebuilt = encode(constructed->value, work);
        ASSERT_TRUE(rebuilt.has_value());
        EXPECT_EQ(fixture::flatten(*rebuilt), wire);
    }
}

TEST(ControlEncodeTest, UnknownsAndNonMinimalEncodingAreNotForwarded) {
    const auto canonical = scalar(1, 1) + blob(2, "");
    auto value = fixture::read(
      std::string{"\x08\x81\x00", 3} + blob(99, "unknown") + blob(2, ""),
      frame_kind::error);
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto encoded = encode(value, work);
    ASSERT_TRUE(encoded.has_value());
    EXPECT_EQ(fixture::flatten(*encoded), canonical);
    kwaque::common::v1::Error native;
    ASSERT_TRUE(native.ParseFromString(fixture::flatten(*encoded)));
    EXPECT_TRUE(native.has_reason());
    EXPECT_TRUE(native.reason().empty());
    auto request = fixture::read(
      fixture::request(blob(1, "old") + blob(1, "new") + scalar(99, 42)),
      frame_kind::handshake_request);
    auto normalized = encode(request, work);
    ASSERT_TRUE(normalized.has_value());
    EXPECT_EQ(fixture::flatten(*normalized), fixture::request(blob(1, "new")));
}

TEST(ControlEncodeTest, ConstructorRejectsInvalidOwnedInputsAndExcessCapacity) {
    auto valid = fixture::read(
      fixture::request(), frame_kind::handshake_request);
    for (unsigned mode = 0; mode < 8; ++mode) {
        auto input = valid.data();
        auto& request = std::get<protocol::handshake_request>(input);
        switch (mode) {
        case 0:
            request.peer = protocol::peer_kind::broker;
            break;
        case 1:
            request.capabilities.protocol_versions[0] = 0;
            break;
        case 2:
            request.capabilities.protocol_versions[1] = 1;
            break;
        case 3:
            request.capabilities.formats[0].oldest_readable = 10;
            break;
        case 4:
            request.build.version = std::string{"\xff", 1};
            break;
        case 5:
            request.capabilities.max_header_bytes = byte_count{47};
            break;
        case 6:
            request.capabilities.formats.reserve(4096);
            break;
        case 7:
            request.build.version.assign(4094, 'v');
            break;
        }
        seastar::abort_source abort;
        auto config = codec::limits::defaults().config();
        if (mode == 6) config.max_allocation_bytes = byte_count{32768};
        codec::cooperative_work work{*codec::limits::make(config), abort};
        auto result = protocol::make_control(
                        std::move(input), fixture::budget(), work)
                        .get();
        expect_error(
          result,
          mode >= 6 ? errc::resource_exhausted : errc::invalid_argument);
    }
    protocol::control_data invalid = protocol::error_control{
      static_cast<protocol::control_error_code>(255), {}, {}};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    expect_error(
      protocol::make_control(std::move(invalid), fixture::budget(), work).get(),
      errc::unsupported_format);
}

TEST(
  ControlEncodeTest, NarrowLimitsAndRealZeroBudgetsRejectWithoutChangingOwner) {
    auto value = fixture::read(
      fixture::response(blob(1, std::string(100, 'v'))),
      frame_kind::handshake_response);
    for (unsigned mode = 0; mode < 6; ++mode) {
        auto config = codec::limits::defaults().config();
        switch (mode) {
        case 0:
            config.max_control_bytes = byte_count{16};
            break;
        case 1:
            config.max_control_field_bytes = byte_count{32};
            break;
        case 2:
            config.max_control_repeated = item_count{1};
            break;
        case 3:
            config.max_nesting_depth = item_count{2};
            break;
        case 4:
            config.max_metadata_bytes = byte_count{64};
            break;
        case 5:
            config.max_work_bytes = byte_count{64};
            break;
        }
        seastar::abort_source abort;
        codec::cooperative_work work{*codec::limits::make(config), abort};
        expect_error(encode(value, work), errc::resource_exhausted);
    }
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    expect_error(encode(value, work, byte_count{}), errc::resource_exhausted);
    auto success = encode(value, work);
    ASSERT_TRUE(success.has_value());
    EXPECT_EQ(
      fixture::flatten(*success),
      fixture::response(blob(1, std::string(100, 'v'))));
    abort.request_abort();
    expect_error(encode(value, work), errc::aborted);
}

TEST(
  ControlEncodeTest, PackedLimitsAndMaximumBuildInfoUseTheSameAdmittedLayout) {
    auto value = fixture::read(
      fixture::request(), frame_kind::handshake_request);
    auto data = value.data();
    auto& request = std::get<protocol::handshake_request>(data);
    request.build.version.assign(4093, 'v');
    auto& caps = request.capabilities;
    caps.protocol_versions.clear();
    caps.formats.clear();
    caps.compression_codecs.clear();
    for (std::uint16_t n = 1; n <= 16; ++n)
        caps.protocol_versions.push_back(n);
    for (std::uint16_t n = 1; n <= 32; ++n)
        caps.formats.push_back({n, 1, 65535, UINT64_MAX});
    for (std::uint8_t n = 0; n < 16; ++n)
        caps.compression_codecs.push_back(n);
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto constructed
      = protocol::make_control(std::move(data), fixture::budget(), work).get();
    ASSERT_TRUE(constructed.has_value());
    auto encoded = encode(constructed->value, work);
    ASSERT_TRUE(encoded.has_value());
    kwaque::control::v1::HandshakeRequest native;
    ASSERT_TRUE(native.ParseFromString(fixture::flatten(*encoded)));
    EXPECT_EQ(native.build().ByteSizeLong(), 4096U);
    EXPECT_EQ(native.capabilities().formats_size(), 32);
    EXPECT_EQ(
      native.capabilities().formats(31).supported_features(), UINT64_MAX);
    auto redecoded = fixture::read(
      fixture::flatten(*encoded), frame_kind::handshake_request);
    auto second = encode(redecoded, work);
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(fixture::flatten(*encoded), fixture::flatten(*second));
}
} // namespace
