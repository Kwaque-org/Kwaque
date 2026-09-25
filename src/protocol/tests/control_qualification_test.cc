#include "proto/kwaque/common/v1/build_info.pb.h"
#include "proto/kwaque/common/v1/capability.pb.h"
#include "proto/kwaque/common/v1/error.pb.h"
#include "proto/kwaque/common/v1/identity.pb.h"
#include "proto/kwaque/control/v1/handshake.pb.h"
#include "proto/kwaque/control/v1/redirect.pb.h"
#include "src/codec/tests/prepared_abort_source.h"
#include "src/protocol/control_frame_codec.h"
#include "src/protocol/control_preflight.h"
#include "src/protocol/tests/control_test_support.h"
#include "src/protocol/tests/frame_test_support.h"

#include <seastar/core/memory.hh>
#include <seastar/core/preempt.hh>
#include <seastar/util/alloc_failure_injector.hh>
#include <seastar/util/later.hh>

#include <google/protobuf/message.h>
#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <exception>
#include <memory>
#include <new>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace {
using namespace kwaque;
namespace fixture = protocol::testing::control_fixture;
namespace frame = protocol::testing::frame_fixture;
namespace detail = protocol::detail;
using message = detail::control_message;
using fixture::blob;
using fixture::scalar;
using protocol::frame_kind;

void expect_error(const auto& result, errc code) {
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), code);
}
auto inspect(
  message type,
  const std::string& wire,
  codec::limits policy = codec::limits::defaults()) {
    seastar::abort_source abort;
    codec::cooperative_work work{policy, abort};
    return detail::preflight_control(std::span<const char>{wire}, type, work)
      .get();
}
std::unique_ptr<google::protobuf::Message> native(message type) {
    namespace common = kwaque::common::v1;
    namespace control = kwaque::control::v1;
    switch (type) {
    case message::build_info:
        return std::make_unique<common::BuildInfo>();
    case message::routing_context:
        return std::make_unique<common::RoutingContext>();
    case message::format_capability:
        return std::make_unique<common::FormatCapability>();
    case message::capabilities:
        return std::make_unique<common::Capabilities>();
    case message::error:
        return std::make_unique<common::Error>();
    case message::handshake_request:
        return std::make_unique<control::HandshakeRequest>();
    case message::handshake_response:
        return std::make_unique<control::HandshakeResponse>();
    case message::endpoint:
        return std::make_unique<control::Endpoint>();
    case message::redirect:
        return std::make_unique<control::Redirect>();
    }
    throw std::runtime_error("unknown fixture message");
}

TEST(
  ControlQualificationTest,
  EveryRequiredFieldRejectsMissingDuplicateAndWrongType) {
    struct fields {
        message type;
        std::vector<std::string> values;
    };
    const std::array cases{
      fields{
        message::routing_context,
        {blob(1, std::string(16, 't')),
         blob(2, std::string(16, 'r')),
         scalar(3, UINT64_MAX),
         blob(4, std::string(16, 's')),
         scalar(5, UINT64_MAX)}},
      fields{
        message::format_capability,
        {scalar(1, 65535),
         scalar(2, 65535),
         scalar(3, 65535),
         std::string{"\x21\0\0\0\0\0\0\0\0", 9}}},
      fields{
        message::capabilities,
        {blob(1, fixture::varint(65535)),
         blob(2, fixture::format()),
         blob(3, fixture::varint(255)),
         scalar(4, 16'777'216),
         scalar(5, 8'388'608),
         scalar(6, 4096),
         scalar(7, 1'048'576),
         scalar(8, 4096)}},
      fields{message::error, {scalar(1, 9)}},
      fields{
        message::handshake_request,
        {scalar(1, 2), blob(4, ""), blob(5, fixture::capabilities())}},
      fields{
        message::handshake_response,
        {blob(1, std::string(16, 'c')),
         blob(2, std::string(16, 'b')),
         blob(3, ""),
         blob(4, fixture::capabilities())}},
      fields{
        message::endpoint, {blob(1, std::string(253, 'h')), scalar(2, 65535)}},
      fields{
        message::redirect,
        {blob(1, fixture::routing()),
         blob(2, std::string(16, 'b')),
         blob(3, blob(1, "h") + scalar(2, 1)),
         scalar(4, 9)}}};
    for (const auto& entry : cases) {
        std::string full;
        for (const auto& field : entry.values)
            full += field;
        ASSERT_TRUE(inspect(entry.type, full).has_value());
        auto generated = native(entry.type);
        ASSERT_TRUE(generated->ParseFromString(full));
        // An opaque unknown is legal in every concrete scope, including the
        // maximum 29-bit field number; unknown LEN bytes need not be UTF-8.
        const auto unknown = full + blob(536870911, std::string{"\xff", 1});
        ASSERT_TRUE(inspect(entry.type, unknown).has_value());
        ASSERT_TRUE(generated->ParseFromString(unknown));
        for (std::size_t index = 0; index < entry.values.size(); ++index) {
            std::string omitted;
            for (std::size_t i = 0; i < entry.values.size(); ++i)
                if (i != index) omitted += entry.values[i];
            expect_error(inspect(entry.type, omitted), errc::malformed_data);
            expect_error(
              inspect(entry.type, full + entry.values[index]),
              errc::malformed_data);
            const auto number = static_cast<std::uint32_t>(
                                  static_cast<unsigned char>(
                                    entry.values[index][0]))
                                >> 3U;
            const auto wrong = omitted
                               + fixture::varint(
                                 (static_cast<std::uint64_t>(number) << 3U)
                                 | 5U)
                               + std::string(4, '\0');
            expect_error(inspect(entry.type, wrong), errc::malformed_data);
            const auto type = static_cast<unsigned char>(entry.values[index][0])
                              & 7U;
            const auto zero = omitted + (type == 2 ? blob(number, "") : type == 1 ? entry.values[index] : scalar(number, 0));
            const bool empty_build
              = (entry.type == message::handshake_request && number == 4)
                || (entry.type == message::handshake_response && number == 3);
            if (type == 1 || empty_build)
                EXPECT_TRUE(inspect(entry.type, zero).has_value());
            else
                expect_error(inspect(entry.type, zero), errc::malformed_data);
        }
    }
}

TEST(
  ControlQualificationTest,
  EveryScopeHonorsFieldBytesDepthAndPackedUnitIntersections) {
    auto wire = scalar(1, 1);
    for (unsigned n = 0; n < 255; ++n)
        wire += blob(99, "");
    auto accepted = inspect(message::error, wire);
    ASSERT_TRUE(accepted.has_value());
    EXPECT_EQ(*accepted, item_count{256});
    auto value = fixture::read(wire, frame_kind::error);
    EXPECT_EQ(
      std::get<protocol::error_control>(value.data()).code,
      protocol::control_error_code::invalid_request);
    expect_error(
      inspect(message::error, wire + blob(99, "")), errc::resource_exhausted);
    auto config = codec::limits::defaults().config();
    config.max_control_fields = item_count{255};
    expect_error(
      inspect(message::error, wire, *codec::limits::make(config)),
      errc::resource_exhausted);
    // Zero-length packed chunks consume tags even though they add no element.
    auto caps = fixture::capabilities();
    auto initial = inspect(message::capabilities, caps);
    ASSERT_TRUE(initial.has_value());
    for (auto n = initial->value(); n < 256; ++n)
        caps += blob(1, "");
    accepted = inspect(message::capabilities, caps);
    ASSERT_TRUE(accepted.has_value());
    EXPECT_EQ(*accepted, item_count{256});
    expect_error(
      inspect(message::capabilities, caps + blob(1, "")),
      errc::resource_exhausted);
    config = codec::limits::defaults().config();
    config.max_control_field_bytes = byte_count{15};
    expect_error(
      inspect(
        message::routing_context,
        fixture::routing(),
        *codec::limits::make(config)),
      errc::resource_exhausted);
    for (std::uint64_t depth : {1U, 2U, 3U, 8U}) {
        config = codec::limits::defaults().config();
        config.max_nesting_depth = item_count{depth};
        const auto result = inspect(
          message::handshake_request,
          fixture::request(),
          *codec::limits::make(config));
        if (depth < 3)
            expect_error(result, errc::resource_exhausted);
        else
            EXPECT_TRUE(result.has_value());
    }
}

TEST(
  ControlQualificationTest,
  QueuedAbortDuringWritingAndRejectedOwnerCleanupIsJoined) {
    auto value = fixture::read(
      fixture::request(blob(1, std::string(4093, 'v'))),
      frame_kind::handshake_request);
    for (const bool constructor : {false, true}) {
        auto draft = value.data();
        if (constructor)
            std::get<protocol::handshake_request>(draft)
              .capabilities.formats.resize(1200);
        auto config = codec::limits::defaults().config();
        if (constructor) config.max_work_items = item_count{1};
        seastar::abort_source abort;
        codec::cooperative_work work{*codec::limits::make(config), abort};
        const auto deadline = std::chrono::steady_clock::now()
                              + std::chrono::seconds{2};
        while (!seastar::need_preempt()
               && std::chrono::steady_clock::now() < deadline) {
        }
        ASSERT_TRUE(seastar::need_preempt());
        bool observed = false;
        auto observer = seastar::yield().then([&] {
            observed = true;
            abort.request_abort();
        });
        std::optional<codec::error> failure;
        std::exception_ptr exception;
        bool pending = false;
        try {
            if (constructor) {
                auto future = protocol::make_control(
                  std::move(draft), fixture::budget(), work);
                pending = !future.available();
                auto result = future.get();
                if (!result) failure = result.error();
            } else {
                auto future = protocol::encode_control_frame(
                  value,
                  {},
                  {},
                  work,
                  frame::bounds,
                  {},
                  fixture::budget().operation_remaining,
                  fixture::charge);
                pending = !future.available();
                auto result = future.get();
                if (!result) failure = result.error();
            }
        } catch (...) {
            exception = std::current_exception();
        }
        const bool during_work = observed;
        observer.get();
        if (exception) std::rethrow_exception(exception);
        EXPECT_TRUE(pending);
        EXPECT_TRUE(during_work);
        ASSERT_TRUE(failure.has_value());
        EXPECT_EQ(
          failure->code(),
          constructor ? errc::resource_exhausted : errc::aborted);
    }
}

thread_local seastar::abort_source* admission_abort = nullptr;
thread_local std::size_t admission_ordinal = 0;
thread_local std::size_t abort_ordinal = 0;
thread_local bool undercharge = false;
byte_count observed_charge(byte_count request) noexcept {
    if (++admission_ordinal == abort_ordinal && admission_abort != nullptr) {
        admission_abort->request_abort();
        if (undercharge && request.value() != 0)
            return byte_count{request.value() - 1};
    }
    return fixture::charge(request);
}

TEST(
  ControlQualificationTest,
  AdmissionCancellationPreservesChargeWithoutAllocation) {
    codec::testing::prepared_abort_source abort;
    const byte_count request{4096};
    const auto expected = fixture::charge(request);
    admission_abort = &abort;
    admission_ordinal = 0;
    abort_ordinal = 1;
    undercharge = false;
    const auto before = seastar::memory::stats().mallocs();
    const auto actual = observed_charge(request);
    const auto allocations = seastar::memory::stats().mallocs() - before;
    admission_abort = nullptr;
    abort_ordinal = 0;
    EXPECT_TRUE(abort.abort_requested());
    EXPECT_EQ(admission_ordinal, 1U);
    EXPECT_EQ(actual, expected);
#if defined(SEASTAR_DEFAULT_ALLOCATOR)
    static_cast<void>(allocations);
    GTEST_SKIP() << "native allocation counters are unavailable";
#else
    EXPECT_EQ(allocations, 0U);
#endif
}

TEST(
  ControlQualificationTest,
  LatePublicationAbortDrainsAndPreservesAnEarlierError) {
    auto value = fixture::read(
      fixture::response(blob(1, std::string(100, 'v'))),
      frame_kind::handshake_response);
    seastar::abort_source baseline_abort;
    codec::cooperative_work baseline{codec::limits::defaults(), baseline_abort};
    admission_ordinal = 0;
    abort_ordinal = 0;
    auto result = protocol::encode_control(
                    value,
                    baseline,
                    {},
                    fixture::budget().operation_remaining,
                    observed_charge)
                    .get();
    ASSERT_TRUE(result.has_value());
    const auto final_admission = admission_ordinal;
    ASSERT_GT(final_admission, 0U);
    for (const bool earlier_error : {false, true}) {
        codec::testing::prepared_abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        admission_abort = &abort;
        admission_ordinal = 0;
        abort_ordinal = final_admission;
        undercharge = earlier_error;
        std::optional<codec::result<bytes::fragmented_buffer>> outcome;
        try {
            outcome.emplace(
              protocol::encode_control(
                value,
                work,
                {},
                fixture::budget().operation_remaining,
                observed_charge)
                .get());
        } catch (...) {
            admission_abort = nullptr;
            undercharge = false;
            throw;
        }
        admission_abort = nullptr;
        undercharge = false;
        ASSERT_TRUE(abort.abort_requested());
        ASSERT_TRUE(outcome.has_value());
        expect_error(
          *outcome, earlier_error ? errc::invalid_argument : errc::aborted);
    }
    EXPECT_EQ(
      std::get<protocol::handshake_response>(value.data()).build.version,
      std::string(100, 'v'));
}

TEST(
  ControlQualificationTest,
  EveryNativeWriterAndFrameAllocationFailureJoinsCleanup) {
#if !defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    GTEST_SKIP() << "allocation failure injection is disabled";
#else
    static_cast<void>(kwaque::error_category());
    const std::array cases{
      std::pair{
        frame_kind::handshake_request,
        fixture::request(blob(1, std::string(100, 'v')))},
      std::pair{
        frame_kind::handshake_response,
        fixture::response(blob(1, std::string(100, 'v')))},
      std::pair{frame_kind::redirect, fixture::redirect(std::string(100, 'h'))},
      std::pair{
        frame_kind::error,
        scalar(1, 3) + blob(2, std::string(100, 'r'))
          + blob(3, fixture::routing())}};
    for (const auto& [kind, wire] : cases) {
        auto value = fixture::read(wire, kind);
        auto draft = value.data();
        protocol::control_capabilities* caps = nullptr;
        if (auto* request = std::get_if<protocol::handshake_request>(&draft))
            caps = &request->capabilities;
        if (auto* response = std::get_if<protocol::handshake_response>(&draft))
            caps = &response->capabilities;
        if (caps) {
            for (std::uint16_t n = 65001; n <= 65007; ++n)
                caps->formats.push_back({n, 1, 9, UINT64_MAX});
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            auto prepared = protocol::make_control(
                              std::move(draft), fixture::budget(), work)
                              .get();
            ASSERT_TRUE(prepared.has_value());
            value = std::move(prepared->value);
        }
        for (const bool framed : {false, true}) {
            bool completed = false;
            std::uint64_t failures = 0;
            for (std::uint64_t ordinal = 0; ordinal < 512; ++ordinal) {
                seastar::abort_source abort;
                codec::cooperative_work work{codec::limits::defaults(), abort};
                std::optional<codec::result<bytes::fragmented_buffer>> outcome;
                bool allocation_failed = false;
                auto& injector = seastar::memory::local_failure_injector();
                injector.fail_after(ordinal);
                try {
                    if (framed)
                        outcome.emplace(
                          protocol::encode_control_frame(
                            value,
                            {},
                            {},
                            work,
                            frame::bounds,
                            {},
                            fixture::budget().operation_remaining,
                            fixture::charge)
                            .get());
                    else
                        outcome.emplace(
                          protocol::encode_control(
                            value,
                            work,
                            {},
                            fixture::budget().operation_remaining,
                            fixture::charge)
                            .get());
                } catch (const std::bad_alloc&) {
                    allocation_failed = true;
                } catch (...) {
                    injector.cancel();
                    throw;
                }
                const bool injected = injector.failed();
                injector.cancel();
                if (injected) {
                    ++failures;
                    EXPECT_TRUE(allocation_failed);
                    EXPECT_FALSE(outcome.has_value());
                } else {
                    ASSERT_TRUE(outcome.has_value());
                    ASSERT_TRUE(outcome->has_value());
                    completed = true;
                    break;
                }
            }
            EXPECT_GT(failures, 0U);
            EXPECT_TRUE(completed);
        }
    }
#endif
}

TEST(
  ControlQualificationTest,
  FramedNativeAllocationFailuresRestoreTheCompleteFrame) {
#if !defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    GTEST_SKIP() << "allocation failure injection is disabled";
#else
    static_cast<void>(kwaque::error_category());
    const std::array cases{
      std::pair{
        frame_kind::handshake_request,
        fixture::request(blob(1, std::string(100, 'v')))},
      std::pair{
        frame_kind::handshake_response,
        fixture::response(blob(1, std::string(100, 'v')))},
      std::pair{frame_kind::redirect, fixture::redirect(std::string(100, 'h'))},
      std::pair{
        frame_kind::error,
        scalar(1, 3) + blob(2, std::string(100, 'r'))
          + blob(3, fixture::routing())}};
    for (const auto& [kind, body] : cases) {
        auto fields = frame::metadata;
        fields.kind = kind;
        fields.stream = model::transport_stream_id{};
        const auto wire = frame::header(
                            static_cast<std::uint32_t>(body.size()),
                            frame::crc32c(body),
                            {},
                            fields)
                          + body;
        bool completed = false;
        std::uint64_t failures = 0;
        for (std::uint64_t ordinal = 0; ordinal < 512; ++ordinal) {
            bytes::fragmented_buffer_parser input{
              fixture::fragmented("pre" + wire, 7)};
            ASSERT_TRUE(input.skip(byte_count{3}).has_value());
            ASSERT_TRUE(input.push_checkpoint().has_value());
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            const auto memory = fixture::reserve(input, work);
            std::optional<
              protocol::frame_read_result<protocol::decoded_control_frame>>
              outcome;
            bool allocation_failed = false;
            auto& injector = seastar::memory::local_failure_injector();
            injector.fail_after(ordinal);
            try {
                outcome.emplace(
                  protocol::decode_control_frame(
                    input, kind, {}, frame::bounds, memory, work)
                    .get());
            } catch (const std::bad_alloc&) {
                allocation_failed = true;
            } catch (...) {
                injector.cancel();
                throw;
            }
            const bool injected = injector.failed();
            injector.cancel();
            EXPECT_EQ(input.checkpoint_depth(), 1U);
            if (injected) {
                ++failures;
                EXPECT_TRUE(allocation_failed);
                EXPECT_FALSE(outcome.has_value());
                EXPECT_EQ(input.bytes_consumed(), byte_count{3});
            } else {
                ASSERT_TRUE(outcome.has_value());
                ASSERT_TRUE(outcome->has_value());
                ASSERT_TRUE(
                  std::holds_alternative<protocol::decoded_control_frame>(
                    **outcome));
                EXPECT_TRUE(input.at_end());
                completed = true;
            }
            ASSERT_TRUE(input.rollback().has_value());
            if (completed) break;
        }
        EXPECT_GT(failures, 0U);
        EXPECT_TRUE(completed);
    }
#endif
}
} // namespace
