#include "src/protocol/control_frame_codec.h"
#include "src/protocol/tests/control_test_support.h"
#include "src/protocol/tests/frame_test_support.h"

#include <gtest/gtest.h>

#include <array>
#include <optional>
#include <string>
#include <variant>

namespace {
using namespace kwaque;
namespace fixture = protocol::testing::control_fixture;
namespace frame = protocol::testing::frame_fixture;
using bytes::fragmented_buffer_parser;
using fixture::blob;
using fixture::scalar;
using protocol::frame_kind;
auto cases() {
    return std::array{
      std::pair{frame_kind::handshake_request, fixture::request()},
      std::pair{frame_kind::handshake_response, fixture::response()},
      std::pair{frame_kind::redirect, fixture::redirect()},
      std::pair{frame_kind::error, scalar(1, 1) + blob(2, "reason")}};
}

std::string wrap(
  frame_kind kind, std::string_view payload, std::string_view extensions = {}) {
    auto fields = frame::metadata;
    fields.kind = kind;
    fields.stream = model::transport_stream_id{};
    return frame::header(
             static_cast<std::uint32_t>(payload.size()),
             frame::crc32c(payload),
             extensions,
             fields)
           + std::string{payload};
}
auto decode(
  fragmented_buffer_parser& input,
  frame_kind kind,
  codec::cooperative_work& work,
  protocol::control_expectation expected = {},
  codec::input_boundary boundary = codec::input_boundary::open) {
    return protocol::decode_control_frame(
             input,
             kind,
             expected,
             frame::bounds,
             fixture::reserve(input, work),
             work,
             fixture::context,
             boundary)
      .get();
}
void expect_error(const auto& result, errc code) {
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), code);
}

TEST(
  ControlFrameCodecTest,
  AllKindsCommitOneFrameAcrossEverySplitAndOutliveInput) {
    for (const auto& [kind, payload] : cases()) {
        const auto wire = wrap(kind, payload, frame::extension());
        for (std::size_t cut = 0; cut <= wire.size(); ++cut) {
            std::optional<protocol::decoded_control_frame> owned;
            {
                fragmented_buffer_parser input{
                  fixture::split_at(wire + wire, cut)};
                ASSERT_TRUE(input.push_checkpoint().has_value());
                seastar::abort_source abort;
                codec::cooperative_work work{codec::limits::defaults(), abort};
                auto result = decode(input, kind, work);
                ASSERT_TRUE(result.has_value());
                auto* control = std::get_if<protocol::decoded_control_frame>(
                  &*result);
                ASSERT_NE(control, nullptr);
                EXPECT_EQ(input.bytes_consumed(), byte_count{wire.size()});
                EXPECT_EQ(input.checkpoint_depth(), 1U);
                EXPECT_EQ(control->header.metadata.kind, kind);
                EXPECT_EQ(control->header.metadata.stream.value(), 0U);
                EXPECT_EQ(
                  control->header.payload_bytes, byte_count{payload.size()});
                owned.emplace(std::move(*control));
                ASSERT_TRUE(input.commit().has_value());
            }
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            auto encoded = protocol::encode_control(
                             owned->value,
                             work,
                             {},
                             fixture::budget().operation_remaining,
                             fixture::charge)
                             .get();
            ASSERT_TRUE(encoded.has_value());
            EXPECT_EQ(fixture::flatten(*encoded), payload);
        }
    }
}

TEST(ControlFrameCodecTest, AllShortSnapshotsRemainUnreadAndReportExactNeed) {
    for (const auto& [kind, payload] : cases()) {
        const auto wire = wrap(kind, payload);
        for (std::size_t cut = 0; cut < wire.size(); ++cut) {
            fragmented_buffer_parser input{
              fixture::fragmented(std::string_view{wire}.substr(0, cut), 7)};
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            auto result = decode(input, kind, work);
            ASSERT_TRUE(result.has_value());
            auto* more = std::get_if<protocol::need_more>(&*result);
            ASSERT_NE(more, nullptr);
            EXPECT_EQ(
              more->additional_bytes,
              byte_count{cut < 48 ? 48 - cut : wire.size() - cut});
            EXPECT_EQ(input.bytes_consumed(), byte_count{});
            expect_error(
              decode(input, kind, work, {}, codec::input_boundary::complete),
              errc::malformed_data);
            EXPECT_EQ(input.bytes_consumed(), byte_count{});
        }
    }
}

TEST(
  ControlFrameCodecTest,
  RepairedOuterIntegrityStillRequiresExactControlGrammarAndContext) {
    for (const auto& [kind, payload] : cases()) {
        const auto wire = wrap(kind, payload + std::string{"\0", 1});
        fragmented_buffer_parser input{fixture::fragmented("pre" + wire, 1)};
        ASSERT_TRUE(input.skip(byte_count{3}).has_value());
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        expect_error(decode(input, kind, work), errc::malformed_data);
        EXPECT_EQ(input.bytes_consumed(), byte_count{3});
        EXPECT_EQ(input.checkpoint_depth(), 0U);
    }
    fragmented_buffer_parser wrong{
      fixture::fragmented(wrap(frame_kind::error, scalar(1, 1)), 1)};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    expect_error(
      decode(wrong, frame_kind::handshake_request, work), errc::wrong_context);
    expect_error(
      decode(wrong, frame_kind::assigned_batch, work), errc::invalid_argument);
    EXPECT_EQ(wrong.bytes_consumed(), byte_count{});
    std::array<std::uint8_t, 16> octets{};
    octets.fill('x');
    fragmented_buffer_parser redirect{
      fixture::fragmented(wrap(frame_kind::redirect, fixture::redirect()), 7)};
    expect_error(
      decode(
        redirect,
        frame_kind::redirect,
        work,
        {.topic = *model::topic_id::make(octets)}),
      errc::wrong_context);
    EXPECT_EQ(redirect.bytes_consumed(), byte_count{});
}

TEST(
  ControlFrameCodecTest,
  WriterDerivesKindAndControlStreamAndPreservesConnectionFields) {
    for (const auto& [kind, payload] : cases()) {
        auto value = fixture::read(payload, kind);
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto encoded = protocol::encode_control_frame(
                         value,
                         frame::metadata.correlation,
                         frame::metadata.sequence,
                         work,
                         frame::bounds,
                         {},
                         fixture::budget().operation_remaining,
                         fixture::charge)
                         .get();
        ASSERT_TRUE(encoded.has_value());
        EXPECT_EQ(fixture::flatten(*encoded), wrap(kind, payload));
        fragmented_buffer_parser input{std::move(*encoded)};
        auto decoded = decode(input, kind, work);
        ASSERT_TRUE(decoded.has_value());
        ASSERT_TRUE(
          std::holds_alternative<protocol::decoded_control_frame>(*decoded));
        EXPECT_TRUE(input.at_end());
        auto denied = protocol::encode_control_frame(
                        value,
                        {},
                        {},
                        work,
                        {byte_count{payload.size()},
                         byte_count{47 + payload.size()}},
                        {},
                        fixture::budget().operation_remaining,
                        fixture::charge)
                        .get();
        expect_error(denied, errc::resource_exhausted);
    }
}
} // namespace
