#include "src/protocol/tests/frame_test_support.h"

#include <seastar/util/alloc_failure_injector.hh>

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <limits>
#include <optional>
#include <type_traits>

namespace {
using namespace kwaque;
namespace protocol = kwaque::protocol;
namespace fixture = protocol::testing::frame_fixture;
using bytes::fragmented_buffer_parser;

static_assert(
  !std::is_convertible_v<model::transport_stream_id, model::correlation_id>);
static_assert(
  !std::is_convertible_v<model::frame_sequence, model::transport_stream_id>);
static_assert(std::is_nothrow_move_constructible_v<protocol::framed_payload>);
static_assert(!std::is_copy_constructible_v<protocol::framed_payload>);

void expect_error(const auto& result, errc code) {
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), code);
}

TEST(FramePrefixTest, SixKindsHaveExplicitPayloadAndStreamRoles) {
    constexpr std::array<std::uint16_t, 6> ids{1, 2, 3, 4, 16, 17};
    for (auto raw : ids) {
        const auto descriptor = protocol::lookup_frame_kind(raw);
        ASSERT_TRUE(descriptor.has_value());
        EXPECT_EQ(static_cast<std::uint16_t>(descriptor->kind), raw);
        EXPECT_EQ(descriptor->connection_control(), raw < 16);
        if (raw == 16)
            EXPECT_EQ(
              descriptor->payload,
              protocol::frame_payload_kind::submitted_batch);
        if (raw == 17)
            EXPECT_EQ(
              descriptor->payload,
              protocol::frame_payload_kind::assigned_batch);
    }
    expect_error(protocol::lookup_frame_kind(0), errc::malformed_data);
    for (const auto raw :
         {std::uint16_t{5},
          std::uint16_t{15},
          std::uint16_t{18},
          std::uint16_t{UINT16_MAX}}) {
        const auto value = protocol::lookup_frame_kind(
          raw, codec::error{errc::success, 7, 9, 11});
        ASSERT_FALSE(value.has_value());
        EXPECT_EQ(
          value.error(), (codec::error{errc::unsupported_format, 7, 9, 11}));
    }
}

TEST(FramePrefixTest, EverySplitExtractsAllFieldsWithoutMarksOrAdvancement) {
    const auto wire = fixture::wire();
    for (std::size_t cut = 0; cut <= wire.size(); ++cut) {
        fragmented_buffer_parser input{
          fixture::split_at("pre" + wire + "next", cut)};
        for (std::size_t mark = 0; mark < bytes::max_parser_checkpoints; ++mark)
            ASSERT_TRUE(input.push_checkpoint().has_value());
        ASSERT_TRUE(input.skip(byte_count{3}).has_value());
        const auto prefix = protocol::peek_frame_prefix(
          input, codec::limits::defaults(), fixture::bounds, fixture::context);
        ASSERT_TRUE(prefix.has_value());
        EXPECT_EQ(prefix->protocol_version, 1);
        EXPECT_EQ(prefix->kind, 16);
        EXPECT_EQ(prefix->header_bytes, 48);
        EXPECT_EQ(prefix->flags, 0);
        EXPECT_EQ(prefix->payload_bytes, 3U);
        EXPECT_EQ(prefix->stream, fixture::metadata.stream.value());
        EXPECT_EQ(prefix->correlation, fixture::metadata.correlation.value());
        EXPECT_EQ(prefix->sequence, fixture::metadata.sequence.value());
        EXPECT_EQ(prefix->payload_crc32c, fixture::crc32c("abc"));
        EXPECT_EQ(prefix->encoded_bytes(), byte_count{51});
        EXPECT_EQ(input.bytes_consumed(), byte_count{3});
        EXPECT_EQ(input.checkpoint_depth(), bytes::max_parser_checkpoints);
    }
}

TEST(FramePrefixTest, EveryShortPrefixDistinguishesOpenAndCompleteInput) {
    const auto wire = fixture::wire();
    for (std::size_t count = 0; count < 48; ++count) {
        for (const auto boundary :
             {codec::input_boundary::open, codec::input_boundary::complete}) {
            fragmented_buffer_parser input{
              fixture::fragmented(wire.substr(0, count), 1)};
            const auto prefix = protocol::peek_frame_prefix(
              input,
              codec::limits::defaults(),
              fixture::bounds,
              fixture::context,
              boundary);
            ASSERT_FALSE(prefix.has_value());
            EXPECT_EQ(
              prefix.error().code(),
              boundary == codec::input_boundary::open ? errc::truncated_data
                                                      : errc::malformed_data);
            EXPECT_EQ(
              prefix.error().byte_offset(), fixture::context.origin + count);
            EXPECT_EQ(input.bytes_consumed(), byte_count{});
        }
    }
}

TEST(FramePrefixTest, PrefixChecksOnlyFramingExtentsBeforeIntegrity) {
    auto wire = fixture::wire();
    fixture::put_u16(wire, 4, 999);
    fixture::put_u16(wire, 6, 0);
    fixture::put_u16(wire, 10, 0xffff);
    fragmented_buffer_parser raw{fixture::fragmented(wire.substr(0, 48), 1)};
    ASSERT_TRUE(
      protocol::peek_frame_prefix(
        raw, codec::limits::defaults(), fixture::bounds)
        .has_value());
    for (auto size : {std::uint16_t{47}, std::uint16_t{4097}}) {
        auto invalid = wire;
        fixture::put_u16(invalid, 8, size);
        fragmented_buffer_parser input{
          fixture::fragmented(invalid.substr(0, 48), 1)};
        const auto result = protocol::peek_frame_prefix(
          input, codec::limits::defaults(), fixture::bounds);
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(
          result.error().code(),
          size == 47 ? errc::malformed_data : errc::resource_exhausted);
    }
    fixture::put_u32(wire, 12, UINT32_MAX);
    fragmented_buffer_parser large{fixture::fragmented(wire.substr(0, 48), 1)};
    expect_error(
      protocol::peek_frame_prefix(
        large, codec::limits::defaults(), fixture::bounds),
      errc::resource_exhausted);
}

TEST(FramePrefixTest, WriterRejectsForgedKindsStreamsWidthsAndNarrowCeilings) {
    auto fields = protocol::frame_prefix_fields{
      fixture::metadata, byte_count{3}, 0, 0};
    auto write = [&] {
        return protocol::encode_frame_prefix(
          fields, codec::limits::defaults(), fixture::bounds);
    };
    fields.metadata.kind = static_cast<protocol::frame_kind>(5);
    expect_error(write(), errc::invalid_argument);
    fields.metadata.kind = protocol::frame_kind::handshake_request;
    expect_error(write(), errc::invalid_argument);
    fields.metadata.stream = model::transport_stream_id{};
    fields.payload_bytes = byte_count{65537};
    expect_error(write(), errc::resource_exhausted);
    fields.metadata = fixture::metadata;
    fields.payload_bytes = byte_count{std::uint64_t{UINT32_MAX} + 1};
    expect_error(write(), errc::invalid_argument);
    fields.payload_bytes = byte_count{3};
    expect_error(
      protocol::encode_frame_prefix(
        fields,
        codec::limits::defaults(),
        {byte_count{2}, fixture::bounds.max_encoded_bytes}),
      errc::resource_exhausted);
    expect_error(
      protocol::encode_frame_prefix(
        fields, codec::limits::defaults(), {byte_count{3}, byte_count{50}}),
      errc::resource_exhausted);
    fields.metadata.correlation = model::correlation_id{UINT64_MAX};
    fields.metadata.sequence = model::frame_sequence{UINT64_MAX};
    ASSERT_TRUE(write().has_value());
    auto config = codec::limits_config{};
    config.max_work_bytes = byte_count{1};
    expect_error(
      protocol::encode_frame_prefix(
        fields, codec::limits::make(config).value(), fixture::bounds),
      errc::resource_exhausted);
}

TEST(FramePrefixTest, TerminalCoordinateIncludesTheWholeDeclaredFrame) {
    const auto wire = fixture::wire();
    fragmented_buffer_parser input{fixture::fragmented(wire.substr(0, 48), 1)};
    auto context = fixture::context;
    context.origin = UINT64_MAX - 51;
    ASSERT_TRUE(
      protocol::peek_frame_prefix(
        input, codec::limits::defaults(), fixture::bounds, context)
        .has_value());
    ++context.origin;
    expect_error(
      protocol::peek_frame_prefix(
        input, codec::limits::defaults(), fixture::bounds, context),
      errc::malformed_data);
    context.origin = UINT64_MAX;
    expect_error(
      protocol::peek_frame_prefix(
        input, codec::limits::defaults(), fixture::bounds, context),
      errc::invalid_argument);
}

TEST(FramePrefixTest, FixedProbeHasNoAllocationEvenWithAllMarksOccupied) {
#if !defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    GTEST_SKIP() << "allocation failure injection is disabled";
#else
    fragmented_buffer_parser input{fixture::fragmented(fixture::wire(), 1)};
    for (std::size_t i = 0; i < bytes::max_parser_checkpoints; ++i)
        ASSERT_TRUE(input.push_checkpoint().has_value());
    auto& injector = seastar::memory::local_failure_injector();
    injector.fail_after(0);
    std::optional<codec::result<protocol::unverified_frame_prefix>> result;
    try {
        result.emplace(
          protocol::peek_frame_prefix(
            input, codec::limits::defaults(), fixture::bounds));
    } catch (...) {
        injector.cancel();
        throw;
    }
    const bool injected = injector.failed();
    injector.cancel();
    EXPECT_FALSE(injected);
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result->has_value());
    EXPECT_EQ(input.checkpoint_depth(), bytes::max_parser_checkpoints);
    EXPECT_EQ(input.bytes_consumed(), byte_count{});
#endif
}

} // namespace
