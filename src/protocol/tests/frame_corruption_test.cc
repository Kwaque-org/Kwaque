#include "src/protocol/batch_frame_codec.h"
#include "src/protocol/tests/batch_frame_test_support.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/temporary_buffer.hh>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace {
using namespace kwaque;
namespace protocol = kwaque::protocol;
namespace fixture = protocol::testing::frame_fixture;
namespace batch_fixture = protocol::testing::batch_frame_fixture;
using bytes::fragmented_buffer;
using bytes::fragmented_buffer_parser;

codec::decode_budget
reserve(const fragmented_buffer_parser& input, codec::cooperative_work& work) {
    return codec::reserve_decode_input(
             input,
             work.policy(),
             {fixture::parent_budget, byte_count{1U << 20U}, fixture::charge},
             fixture::context)
      .value();
}
auto decode(
  fragmented_buffer_parser& input,
  codec::cooperative_work& work,
  codec::input_boundary boundary = codec::input_boundary::open) {
    return protocol::decode_frame(
             input,
             fixture::bounds,
             reserve(input, work),
             work,
             fixture::context,
             boundary)
      .get();
}
void expect_error(const auto& value, errc code) {
    ASSERT_FALSE(value.has_value());
    EXPECT_EQ(value.error().code(), code);
}

TEST(
  FrameCorruptionTest,
  EveryFixedHeaderOctetIsProtectedWithEarlyExtentPrecedence) {
    const auto original = fixture::wire(std::string(4096, 'x'));
    for (std::size_t at = 0; at < 48; ++at) {
        auto wire = original;
        wire[at] ^= 1;
        fragmented_buffer_parser input{fixture::fragmented("pre" + wire, 67)};
        ASSERT_TRUE(input.skip(byte_count{3}).has_value());
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        const auto result = decode(input, work);
        const auto expected = at < 4     ? errc::malformed_data
                              : at == 15 ? errc::resource_exhausted
                                         : errc::corrupt_data;
        expect_error(result, expected);
        ASSERT_FALSE(result.has_value());
        if (expected == errc::corrupt_data)
            EXPECT_EQ(
              result.error().byte_offset(), fixture::context.origin + 3 + 40);
        EXPECT_EQ(input.bytes_consumed(), byte_count{3});
        EXPECT_EQ(input.checkpoint_depth(), 0U);
    }
}

TEST(
  FrameCorruptionTest,
  RepairedOuterAndInnerCrcsReachIndependentBatchContextChecks) {
    namespace oracle = model::testing;
    const auto original = batch_fixture::batch_wire(false);
    auto expected = batch_fixture::expected();
    expected.id = model::batch_id::make(
                    batch_fixture::id<model::producer_id>(1),
                    model::producer_epoch::make(2).value(),
                    model::producer_stream_id::make(3).value(),
                    model::batch_sequence{4})
                    .value();
    expected.original_binding = batch_fixture::binding();
    for (const std::size_t offset :
         {0U, 16U, 24U, 32U, 40U, 56U, 72U, 80U, 96U}) {
        auto batch = original;
        batch[32 + offset] ^= 1;
        oracle::repair_crc(batch);
        const auto wire = batch_fixture::frame(batch);
        fragmented_buffer_parser input{
          fixture::fragmented("pre" + wire + "next", 7)};
        ASSERT_TRUE(input.skip(byte_count{3}).has_value());
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto result = protocol::decode_submitted_frame(
                        input,
                        expected,
                        fixture::bounds,
                        reserve(input, work),
                        work,
                        fixture::context)
                        .get();
        expect_error(result, errc::wrong_context);
        EXPECT_EQ(input.bytes_consumed(), byte_count{3});
    }
    auto over = original;
    oracle::put(over, 32 + 144, 4097, 4);
    oracle::repair_crc(over);
    fragmented_buffer_parser input{
      fixture::fragmented(batch_fixture::frame(over), 7)};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    expect_error(
      protocol::decode_submitted_frame(
        input, expected, fixture::bounds, reserve(input, work), work)
        .get(),
      errc::resource_exhausted);
    EXPECT_EQ(input.bytes_consumed(), byte_count{});
}

TEST(
  FrameCorruptionTest,
  ExtensionValuesCannotBorrowPayloadOrFollowingFrameBytes) {
    const auto following = fixture::wire("good");
    for (const auto extension_bytes :
         {std::size_t{8}, std::size_t{9}, std::size_t{15}}) {
        auto extensions = fixture::extension(
          7, 0, std::string(extension_bytes - 8, 'x'));
        fixture::put_u32(
          extensions, 4, static_cast<std::uint32_t>(extension_bytes - 8 + 1));
        auto wire = fixture::wire("payload", extensions) + following;
        for (std::size_t cut = 0; cut <= wire.size(); ++cut) {
            fragmented_buffer_parser input{fixture::split_at(wire, cut)};
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            const auto result = decode(input, work);
            expect_error(result, errc::malformed_data);
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(
              result.error().byte_offset(),
              fixture::context.origin + 48 + extension_bytes);
            EXPECT_EQ(input.bytes_consumed(), byte_count{});
        }
    }
}

TEST(FrameCorruptionTest, ARejectedFrameNeverResynchronizesIntoFollowingMagic) {
    const auto first = fixture::wire("first");
    auto bad = fixture::wire("bad");
    bad[0] = 'z';
    const auto last = fixture::wire("last");
    for (const auto width : {1U, 7U, 67U}) {
        fragmented_buffer_parser input{
          fixture::fragmented(first + bad + last, width)};
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        const auto memory = reserve(input, work);
        {
            const auto result
              = protocol::decode_frame(
                  input, fixture::bounds, memory, work, fixture::context)
                  .get();
            ASSERT_TRUE(result.has_value());
            ASSERT_TRUE(
              std::holds_alternative<protocol::framed_payload>(*result));
        }
        for (unsigned attempt = 0; attempt < 2; ++attempt) {
            expect_error(
              protocol::decode_frame(
                input, fixture::bounds, memory, work, fixture::context)
                .get(),
              errc::malformed_data);
            EXPECT_EQ(input.bytes_consumed(), byte_count{first.size()});
            std::string tail(bad.size() + last.size(), '\0');
            ASSERT_TRUE(input.peek_to(tail).has_value());
            EXPECT_EQ(tail, bad + last);
        }
        // A fresh byte owner is the in-memory analogue of a new stream. The
        // codec itself neither skips the bad bytes nor reconnects anything.
        fragmented_buffer_parser fresh{fixture::fragmented(last, width)};
        const auto result = decode(fresh, work);
        ASSERT_TRUE(result.has_value());
        ASSERT_TRUE(std::holds_alternative<protocol::framed_payload>(*result));
        EXPECT_TRUE(fresh.at_end());
    }
}

template<typename F>
void for_each_composition(
  std::size_t remaining,
  std::size_t maximum,
  std::vector<std::size_t>& current,
  F&& function) {
    if (remaining == 0) {
        function(std::as_const(current));
        return;
    }
    for (std::size_t n = 1; n <= std::min(remaining, maximum); ++n) {
        current.push_back(n);
        for_each_composition(remaining - n, maximum, current, function);
        current.pop_back();
    }
}

fragmented_buffer
snapshot(std::string_view wire, std::span<const std::size_t> boundaries) {
    std::vector<seastar::temporary_buffer<char>> parts;
    std::size_t previous = 0;
    for (auto boundary : boundaries) {
        const auto end = std::min(boundary, wire.size());
        if (end > previous)
            parts.emplace_back(wire.data() + previous, end - previous);
        previous = end;
        if (end == wire.size()) break;
    }
    return fragmented_buffer::copy_from_fragments(parts).value();
}

TEST(
  FrameCorruptionTest,
  PartitionedFeedsPreserveFrameOrderAndEveryUnconsumedByte) {
    const std::array payloads{
      std::string{"a"}, std::string{"BC"}, std::string{}};
    std::string wire;
    for (std::size_t index = 0; index < payloads.size(); ++index) {
        auto fields = fixture::metadata;
        fields.sequence = model::frame_sequence{index};
        wire += fixture::header(
          static_cast<std::uint32_t>(payloads[index].size()),
          fixture::crc32c(payloads[index]),
          {},
          fields);
        wire += payloads[index];
    }
    std::vector<std::size_t> partition;
    unsigned histories = 0;
    for_each_composition(
      8, 3, partition, [&](const std::vector<std::size_t>& chunks) {
          std::vector<std::size_t> boundaries;
          std::size_t at = 0;
          for (auto n : chunks) {
              at += n;
              boundaries.push_back(at);
          }
          while (at < wire.size()) {
              at = std::min(at + 7, wire.size());
              boundaries.push_back(at);
          }
          for (const auto mark_depth : {0U, 7U}) {
              std::size_t consumed = 0, delivered = 0;
              seastar::abort_source abort;
              codec::cooperative_work work{codec::limits::defaults(), abort};
              for (auto available_bytes : boundaries) {
                  fragmented_buffer_parser input{snapshot(
                    std::string_view{wire}.substr(0, available_bytes),
                    boundaries)};
                  for (unsigned mark = 0; mark < mark_depth; ++mark)
                      ASSERT_TRUE(input.push_checkpoint().has_value());
                  ASSERT_TRUE(input.skip(byte_count{consumed}).has_value());
                  const auto memory = reserve(input, work);
                  while (!input.at_end()) {
                      const auto before = input.bytes_consumed();
                      const auto result = protocol::decode_frame(
                                            input,
                                            fixture::bounds,
                                            memory,
                                            work,
                                            fixture::context)
                                            .get();
                      ASSERT_TRUE(result.has_value());
                      if (
                        const auto* more = std::get_if<protocol::need_more>(
                          &*result)) {
                          const auto left = available_bytes - consumed;
                          const auto target
                            = left < 48 ? 48
                                        : 48 + payloads.at(delivered).size();
                          EXPECT_EQ(
                            more->additional_bytes, byte_count{target - left});
                          EXPECT_EQ(input.bytes_consumed(), before);
                          break;
                      }
                      const auto& frame = std::get<protocol::framed_payload>(
                        *result);
                      ASSERT_LT(delivered, payloads.size());
                      EXPECT_TRUE(
                        frame.payload.content_equals(payloads[delivered]));
                      EXPECT_EQ(
                        frame.header.metadata.sequence.value(), delivered);
                      consumed += 48 + payloads[delivered].size();
                      ++delivered;
                      EXPECT_EQ(input.bytes_consumed(), byte_count{consumed});
                  }
                  EXPECT_EQ(input.checkpoint_depth(), mark_depth);
                  std::string tail(available_bytes - consumed, '\0');
                  ASSERT_TRUE(input.peek_to(tail).has_value());
                  EXPECT_EQ(
                    tail, wire.substr(consumed, available_bytes - consumed));
              }
              EXPECT_EQ(delivered, payloads.size());
              EXPECT_EQ(consumed, wire.size());
              ++histories;
          }
      });
    EXPECT_GT(histories, 100U);
}

} // namespace
