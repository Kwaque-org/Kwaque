#include "src/protocol/control_frame_codec.h"
#include "src/protocol/tests/control_fuzz_cases.h"
#include "src/protocol/tests/control_fuzz_oracle.h"
#include "src/protocol/tests/control_test_support.h"
#include "src/protocol/tests/frame_test_support.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/preempt.hh>
#include <seastar/core/reactor.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/later.hh>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

namespace {
using namespace kwaque;
namespace test = protocol::testing;
namespace fixture = test::control_fixture;
namespace frame = test::frame_fixture;
using protocol::frame_kind;

TEST(ControlFuzzCasesTest, FixtureLayoutsKeepSmallSplitsAndBoundMaximumInputs) {
    for (const std::size_t size :
         {0U, 1U, 7U, 8U, 896U, 897U, 65536U, 65584U}) {
        SCOPED_TRACE(size);
        const std::string wire(size, 'x');
        const auto input = fixture::bounded_fragmented(wire);
        EXPECT_TRUE(input.content_equals(wire));
        EXPECT_LE(
          input.fragment_count(), fixture::control_fixture_fragment_limit);
        if (size <= 896) EXPECT_EQ(input.fragment_count(), (size + 6U) / 7U);
        if (size == 65536)
            EXPECT_EQ(
              input.fragment_count(), fixture::control_fixture_fragment_limit);
    }
    EXPECT_THROW(fixture::bounded_fragmented("x", 0), std::invalid_argument);
}

TEST(
  ControlFuzzCasesTest, OracleAndFixturesCoverAllRootsAndTheFullWireCeiling) {
    for (unsigned root = 1; root <= 4; ++root) {
        const auto kind = static_cast<frame_kind>(root);
        for (unsigned shape = 0; shape < 5; ++shape) {
            SCOPED_TRACE(
              ::testing::Message{} << "root=" << root << " shape=" << shape);
            const auto payload = test::control_payload(kind, shape);
            if (shape == 3) EXPECT_EQ(payload.size(), 65536U);
            auto expected = test::probe_control(payload, kind);
            ASSERT_NE(expected, nullptr);
            const auto value = fixture::read(payload, kind);
            EXPECT_TRUE(test::matches_control(*expected, value.data()));
            if (shape == 3)
                EXPECT_FALSE(test::control_unknowns_empty(*expected));
        }
    }
    const auto minimal = fixture::scalar(1, 1);
    EXPECT_NE(test::probe_control(minimal, frame_kind::error), nullptr);
    EXPECT_NE(
      test::probe_control(std::string{"\x08\x81\x00", 3}, frame_kind::error),
      nullptr);
    EXPECT_EQ(
      test::probe_control(minimal + minimal, frame_kind::error), nullptr);
    EXPECT_EQ(
      test::probe_control(minimal + std::string{"\0", 1}, frame_kind::error),
      nullptr);
    EXPECT_EQ(
      test::probe_control(
        std::string{"\x88\x80\x80\x80\x80\x00\x01", 7}, frame_kind::error),
      nullptr);
}

TEST(ControlFuzzCasesTest, StructuredPayloadAndFrameMutationsReachEveryShape) {
    for (unsigned root = 0; root < 4; ++root) {
        for (unsigned shape = 0; shape < 5; ++shape) {
            for (unsigned mutation = 0; mutation < 16; ++mutation) {
                for (const unsigned framed : {0U, 4U}) {
                    std::array<std::uint8_t, 12> data{};
                    data[0] = static_cast<std::uint8_t>(root | framed);
                    data[1] = static_cast<std::uint8_t>(shape);
                    data[2] = static_cast<std::uint8_t>(mutation);
                    data[3] = static_cast<std::uint8_t>(mutation & 1U);
                    data[6] = 31;
                    data[7] = 0x80;
                    data[8] = 0xff;
                    test::exercise_control_case(data);
                }
            }
        }
    }
}

TEST(ControlFuzzCasesTest, DenialsAndExistingMarksDoNotPublishPartialControls) {
    for (unsigned root = 0; root < 4; ++root) {
        for (unsigned flags = 0; flags < 256; ++flags) {
            for (const unsigned depth : {0U, 7U, 8U}) {
                std::array<std::uint8_t, 8> data{};
                data[0] = static_cast<std::uint8_t>(
                  root | ((flags & 1U) ? 4U : 0U));
                data[3] = static_cast<std::uint8_t>(flags);
                data[4] = 1;
                data[5] = static_cast<std::uint8_t>(depth);
                data[7] = 't';
                test::exercise_control_case(data);
            }
        }
    }
}

TEST(ControlFuzzCasesTest, RawSnapshotsAndOuterRepairsPreserveExactBoundaries) {
    for (unsigned root = 0; root < 4; ++root) {
        const auto kind = static_cast<frame_kind>(root + 1);
        const auto payload = test::control_payload(kind, 0);
        auto metadata = frame::metadata;
        metadata.kind = kind;
        metadata.stream = model::transport_stream_id{};
        const auto wire = frame::header(
                            static_cast<std::uint32_t>(payload.size()),
                            frame::crc32c(payload),
                            {},
                            metadata)
                          + payload;
        for (const bool framed : {false, true}) {
            const auto& source = framed ? wire : payload;
            for (std::size_t cut = 0; cut <= source.size(); ++cut) {
                std::vector<std::uint8_t> data(8);
                data[0] = static_cast<std::uint8_t>(
                  root | 8U | (framed ? 4U : 0U));
                data[2] = 4;
                data[4] = 1;
                for (char c : std::string_view{source}.substr(0, cut))
                    data.push_back(static_cast<std::uint8_t>(c));
                test::exercise_control_case(data);
            }
        }
    }
}

seastar::future<> observer(
  seastar::abort_source& stop,
  std::uint64_t& ticks,
  std::chrono::steady_clock::duration& gap) {
    auto previous = std::chrono::steady_clock::now();
    while (!stop.abort_requested()) {
        co_await seastar::yield();
        if (!stop.abort_requested()) {
            const auto now = std::chrono::steady_clock::now();
            gap = std::max(gap, now - previous);
            previous = now;
            ++ticks;
        }
    }
}
TEST(ControlFuzzCasesTest, MaximumFramesAndSerializationAllowControlProgress) {
    const auto kind = frame_kind::handshake_request;
    const auto payload = test::control_payload(kind, 3);
    auto value = fixture::read(payload, kind);
    for (const bool write : {false, true}) {
        seastar::abort_source abort, stop;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto metadata = frame::metadata;
        metadata.kind = kind;
        metadata.stream = model::transport_stream_id{};
        auto wire = frame::header(
          static_cast<std::uint32_t>(payload.size()),
          frame::crc32c(payload),
          {},
          metadata);
        wire.reserve(wire.size() + payload.size());
        wire += payload;
        bytes::fragmented_buffer_parser input{fixture::fragmented(wire, 512)};
        const auto memory = fixture::reserve(input, work);
        const auto deadline = std::chrono::steady_clock::now()
                              + std::chrono::seconds{2};
        while (!seastar::need_preempt()
               && std::chrono::steady_clock::now() < deadline) {
        }
        ASSERT_TRUE(seastar::need_preempt());
        std::uint64_t ticks = 0;
        std::chrono::steady_clock::duration gap{};
        auto progress = observer(stop, ticks, gap);
        auto joined = seastar::defer([&] {
            stop.request_abort();
            progress.get();
        });
        const auto tasks = seastar::engine().get_sched_stats().tasks_processed;
        bool pending = false;
        if (write) {
            auto future = protocol::encode_control(
              value,
              work,
              {},
              fixture::budget().operation_remaining,
              fixture::charge);
            pending = !future.available();
            auto encoded = future.get();
            ASSERT_TRUE(encoded.has_value());
        } else {
            auto future = protocol::decode_control_frame(
              input, kind, {}, frame::bounds, memory, work);
            pending = !future.available();
            auto decoded = future.get();
            ASSERT_TRUE(decoded.has_value());
            EXPECT_TRUE(input.at_end());
        }
        EXPECT_TRUE(pending);
        EXPECT_GT(ticks, 0U);
        const auto name = write ? "serialize" : "framed_decode";
        ::testing::Test::RecordProperty(
          std::string{name} + "_control_ticks", std::to_string(ticks));
        ::testing::Test::RecordProperty(
          std::string{name} + "_tasks",
          std::to_string(
            seastar::engine().get_sched_stats().tasks_processed - tasks));
        ::testing::Test::RecordProperty(
          std::string{name} + "_largest_gap_ns",
          std::to_string(
            std::chrono::duration_cast<std::chrono::nanoseconds>(gap).count()));
        // Observer-inclusive counters are progress evidence, not benchmark or
        // latency thresholds. Native perf cases measure without this observer.
    }
}
} // namespace
