#include "src/protocol/control_memory.h"
#include "src/protocol/tests/control_test_support.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/preempt.hh>
#include <seastar/util/alloc_failure_injector.hh>
#include <seastar/util/later.hh>

#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <exception>
#include <malloc.h>
#include <new>
#include <optional>
#include <string>
#include <utility>
#include <variant>

namespace {
using namespace kwaque;
namespace fixture = protocol::testing::control_fixture;
using bytes::fragmented_buffer_parser;
using fixture::blob;
using fixture::scalar;
using protocol::frame_kind;

void expect_error(const auto& result, errc code) {
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), code);
}
auto decode(
  const std::string& wire,
  frame_kind kind = frame_kind::error,
  protocol::control_expectation expected = {}) {
    fragmented_buffer_parser input{fixture::fragmented(wire, 7)};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto result = protocol::decode_control(
                    input,
                    kind,
                    expected,
                    fixture::reserve(input, work),
                    work,
                    fixture::context)
                    .get();
    EXPECT_EQ(input.bytes_consumed(), byte_count{result ? wire.size() : 0});
    EXPECT_EQ(input.checkpoint_depth(), 0U);
    return result;
}
template<typename Id>
Id id(char value) {
    std::array<std::uint8_t, 16> bytes{};
    bytes.fill(static_cast<std::uint8_t>(value));
    return Id::make(bytes).value();
}

// Put scalar presence before allocations, grow repeated children beyond their
// inline slot, and exercise both native tag-dispatch paths. A non-minimal tag
// retains the same field number and is permitted by the wire profile.
std::string allocation_request(bool nonminimal, bool packed) {
    const auto tag = [nonminimal](std::uint32_t field, unsigned type) {
        auto wire = fixture::varint((std::uint64_t{field} << 3U) | type);
        if (nonminimal) {
            wire.back() = static_cast<char>(
              static_cast<unsigned char>(wire.back()) | 0x80U);
            wire.push_back('\0');
        }
        return wire;
    };
    const auto scalar_field = [&](std::uint32_t field, std::uint64_t value) {
        return tag(field, 0) + fixture::varint(value);
    };
    const auto bytes_field = [&](std::uint32_t field, std::string_view value) {
        return tag(field, 2) + fixture::varint(value.size())
               + std::string{value};
    };
    auto caps = scalar_field(4, 16'777'216) + scalar_field(5, 8'388'608)
                + scalar_field(6, 4096) + scalar_field(7, 1'048'576)
                + scalar_field(8, 4096);
    for (const std::uint32_t field : {1U, 3U}) {
        std::string values;
        for (std::uint32_t i = 0; i < 16; ++i) {
            const auto value = field == 1 ? i + 1 : i;
            if (packed)
                values += fixture::varint(value);
            else
                caps += scalar_field(field, value);
        }
        if (packed) caps += bytes_field(field, values);
    }
    for (std::uint32_t family = 1; family <= 8; ++family) {
        const auto format = scalar_field(1, family) + scalar_field(2, 1)
                            + scalar_field(3, 9) + tag(4, 1)
                            + std::string(8, '\0')
                            + bytes_field(99, std::string(100, 'u'));
        caps += bytes_field(2, format);
    }
    const auto build = bytes_field(1, std::string(100, 'v'))
                       + bytes_field(2, std::string(100, 'r'));
    return scalar_field(1, 1) + bytes_field(2, std::string(16, 'c'))
           + bytes_field(3, std::string(16, 'b')) + bytes_field(4, build)
           + bytes_field(5, caps);
}

TEST(ControlCodecTest, FieldOrderAndNonMinimalTagsPreserveOwningConversion) {
    for (const bool nonminimal : {false, true}) {
        for (const bool packed : {false, true}) {
            auto result = decode(
              allocation_request(nonminimal, packed),
              frame_kind::handshake_request);
            ASSERT_TRUE(result.has_value());
            const auto& value = std::get<protocol::handshake_request>(
              result->value.data());
            EXPECT_EQ(value.peer, protocol::peer_kind::broker);
            EXPECT_EQ(value.build.version, std::string(100, 'v'));
            ASSERT_EQ(value.capabilities.formats.size(), 8U);
            EXPECT_EQ(value.capabilities.formats.back().family, 8);
            ASSERT_EQ(value.capabilities.protocol_versions.size(), 16U);
            EXPECT_EQ(value.capabilities.protocol_versions.back(), 16);
            ASSERT_EQ(value.capabilities.compression_codecs.size(), 16U);
            EXPECT_EQ(value.capabilities.compression_codecs.back(), 15);
        }
    }
}

TEST(
  ControlCodecTest,
  FourKindsProduceOwningValuesAndPreserveAdvertisementDomains) {
    auto request = decode(fixture::request(), frame_kind::handshake_request);
    ASSERT_TRUE(request.has_value());
    const auto& client = std::get<protocol::handshake_request>(
      request->value.data());
    EXPECT_EQ(client.peer, protocol::peer_kind::client);
    EXPECT_FALSE(client.expected_cluster.has_value());
    EXPECT_FALSE(client.broker.has_value());
    EXPECT_TRUE(client.build.version.empty());
    ASSERT_EQ(client.capabilities.formats.size(), 1U);
    EXPECT_EQ(client.capabilities.formats[0].family, 65000);
    EXPECT_EQ(
      client.capabilities.formats[0].supported_features,
      std::uint64_t{1} << 63U);
    EXPECT_EQ(
      client.capabilities.compression_codecs,
      (std::vector<std::uint8_t>{0, 255}));
    EXPECT_EQ(
      client.capabilities.protocol_versions,
      (std::vector<std::uint16_t>{1, 2}));

    auto response = decode(
      fixture::response(blob(1, "release")),
      frame_kind::handshake_response,
      {.cluster = id<model::cluster_id>('c'),
       .broker = id<model::broker_id>('b')});
    ASSERT_TRUE(response.has_value());
    const auto& server = std::get<protocol::handshake_response>(
      response->value.data());
    EXPECT_EQ(server.cluster, id<model::cluster_id>('c'));
    EXPECT_EQ(server.build.version, "release");

    auto redirect = decode(
      fixture::redirect(),
      frame_kind::redirect,
      {.topic = id<model::topic_id>('t')});
    ASSERT_TRUE(redirect.has_value());
    const auto& destination = std::get<protocol::redirect_control>(
      redirect->value.data());
    EXPECT_EQ(destination.destination.routing_epoch.value(), UINT64_MAX);
    EXPECT_EQ(destination.endpoint.host, "localhost");
    EXPECT_EQ(destination.endpoint.port, 65535);

    auto failure = decode(
      scalar(1, 3) + blob(2, "stale") + blob(3, fixture::routing()),
      frame_kind::error,
      {.topic = id<model::topic_id>('t')});
    ASSERT_TRUE(failure.has_value());
    const auto& observed = std::get<protocol::error_control>(
      failure->value.data());
    EXPECT_EQ(observed.reason, std::optional<std::string>{"stale"});
    ASSERT_TRUE(observed.observed.has_value());
    EXPECT_EQ(observed.observed->range, id<model::range_id>('r'));
}

TEST(
  ControlCodecTest,
  PresenceBuildInfoMergeAndUnknownDiscardSurviveInputDestruction) {
    const auto build = blob(1, "old") + scalar(1, 4) + blob(1, "new")
                       + blob(99, "opaque");
    const auto wire = fixture::response(build) + blob(99, "unknown");
    for (std::size_t cut = 0; cut <= wire.size(); ++cut) {
        std::optional<protocol::decoded_control> owned;
        {
            fragmented_buffer_parser input{
              fixture::split_at("pre" + wire, cut)};
            ASSERT_TRUE(input.skip(byte_count{3}).has_value());
            ASSERT_TRUE(input.push_checkpoint().has_value());
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            auto result = protocol::decode_control(
                            input,
                            frame_kind::handshake_response,
                            {},
                            fixture::reserve(input, work),
                            work,
                            fixture::context)
                            .get();
            ASSERT_TRUE(result.has_value());
            EXPECT_TRUE(input.at_end());
            EXPECT_EQ(input.checkpoint_depth(), 1U);
            ASSERT_TRUE(input.commit().has_value());
            owned.emplace(std::move(*result));
        }
        EXPECT_EQ(
          std::get<protocol::handshake_response>(owned->value.data())
            .build.version,
          "new");
    }
    auto absent = decode(scalar(1, 1));
    auto empty = decode(scalar(1, 1) + blob(2, ""));
    ASSERT_TRUE(absent.has_value());
    ASSERT_TRUE(empty.has_value());
    EXPECT_FALSE(
      std::get<protocol::error_control>(absent->value.data())
        .reason.has_value());
    EXPECT_EQ(
      std::get<protocol::error_control>(empty->value.data()).reason,
      std::optional<std::string>{""});
}

TEST(ControlCodecTest, ConditionalIdentitiesAndIndependentExpectationsReject) {
    const auto broker = scalar(1, 1) + blob(2, std::string(16, 'c'))
                        + blob(3, std::string(16, 'b')) + blob(4, "")
                        + blob(5, fixture::capabilities());
    ASSERT_TRUE(decode(
                  broker,
                  frame_kind::handshake_request,
                  {.cluster = id<model::cluster_id>('c'),
                   .broker = id<model::broker_id>('b')})
                  .has_value());
    expect_error(
      decode(
        broker,
        frame_kind::handshake_request,
        {.cluster = id<model::cluster_id>('x')}),
      errc::wrong_context);
    expect_error(
      decode(
        scalar(1, 1) + blob(4, "") + blob(5, fixture::capabilities()),
        frame_kind::handshake_request),
      errc::malformed_data);
    expect_error(
      decode(
        fixture::request() + blob(3, std::string(16, 'b')),
        frame_kind::handshake_request),
      errc::malformed_data);
    ASSERT_TRUE(decode(
                  fixture::request(),
                  frame_kind::handshake_request,
                  {.cluster = id<model::cluster_id>('c')})
                  .has_value());
    expect_error(
      decode(
        fixture::response(),
        frame_kind::handshake_response,
        {.broker = id<model::broker_id>('x')}),
      errc::wrong_context);
    expect_error(
      decode(
        fixture::redirect(),
        frame_kind::redirect,
        {.topic = id<model::topic_id>('x')}),
      errc::wrong_context);
    expect_error(decode(scalar(1, 3)), errc::malformed_data);
    expect_error(decode(scalar(1, 4)), errc::malformed_data);
    expect_error(
      decode(
        scalar(1, 1), frame_kind::error, {.topic = id<model::topic_id>('t')}),
      errc::wrong_context);
    expect_error(
      decode(scalar(1, 1), frame_kind::error, {.topic = model::topic_id{}}),
      errc::invalid_argument);
    expect_error(
      decode(
        fixture::request(),
        frame_kind::handshake_request,
        {.topic = id<model::topic_id>('t')}),
      errc::invalid_argument);
    expect_error(
      decode(scalar(1, 1), frame_kind::submitted_batch),
      errc::invalid_argument);
}

TEST(
  ControlCodecTest, CompletePayloadRejectsTruncationAndPreservesParentMarks) {
    const auto wire = fixture::response();
    for (std::size_t size = 0; size < wire.size(); ++size) {
        fragmented_buffer_parser input{
          fixture::fragmented("pre" + wire.substr(0, size), 1)};
        ASSERT_TRUE(input.skip(byte_count{3}).has_value());
        ASSERT_TRUE(input.push_checkpoint().has_value());
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        expect_error(
          protocol::decode_control(
            input,
            frame_kind::handshake_response,
            {},
            fixture::reserve(input, work),
            work,
            fixture::context)
            .get(),
          errc::malformed_data);
        EXPECT_EQ(input.bytes_consumed(), byte_count{3});
        EXPECT_EQ(input.checkpoint_depth(), 1U);
        ASSERT_TRUE(input.rollback().has_value());
    }
    // Disjoint fields are one message; duplicate-sensitive concatenation is
    // not.
    ASSERT_TRUE(decode(scalar(1, 1) + blob(2, "explanation")).has_value());
    expect_error(decode(scalar(1, 1) + scalar(1, 1)), errc::malformed_data);
    expect_error(
      decode(wire + std::string{"\0", 1}, frame_kind::handshake_response),
      errc::malformed_data);
}

TEST(
  ControlCodecTest,
  ConversionPeakIsAdmittedBeforeCopyAndTemporaryCostsAreRefunded) {
    const auto wire = fixture::response(blob(1, std::string(300, 'v')));
    fragmented_buffer_parser input{fixture::fragmented(wire, 7)};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto memory = fixture::reserve(input, work);
    ASSERT_TRUE(input.push_checkpoint().has_value());
    auto success = protocol::decode_control(
                     input, frame_kind::handshake_response, {}, memory, work)
                     .get();
    ASSERT_TRUE(success.has_value());
    const auto output_cost = memory.metadata_remaining.value()
                             - success->remaining.metadata_remaining.value();
    EXPECT_GT(output_cost, 0U);
    EXPECT_EQ(
      memory.operation_remaining.value()
        - success->remaining.operation_remaining.value(),
      output_cost);
    ASSERT_TRUE(input.rollback().has_value());
    const auto parse = protocol::detail::bound_control_parse(
      protocol::detail::control_message::handshake_response,
      byte_count{wire.size()},
      work.policy(),
      fixture::charge);
    ASSERT_TRUE(parse.has_value());
    auto narrow = memory;
    narrow.operation_remaining = byte_count{
      parse->generated_peak.value() + parse->input_copy.value() + output_cost
      - 1};
    expect_error(
      protocol::decode_control(
        input, frame_kind::handshake_response, {}, narrow, work)
        .get(),
      errc::resource_exhausted);
    EXPECT_EQ(input.bytes_consumed(), byte_count{});
    narrow.operation_remaining = byte_count{
      narrow.operation_remaining.value() + 1};
    auto exact = protocol::decode_control(
                   input, frame_kind::handshake_response, {}, narrow, work)
                   .get();
    ASSERT_TRUE(exact.has_value());
    EXPECT_EQ(
      exact->remaining.operation_remaining.value(),
      parse->generated_peak.value() + parse->input_copy.value());
}

TEST(
  ControlCodecTest, NarrowNativeWorkAndExhaustedBudgetsRejectWithoutConsuming) {
    const auto wire = scalar(1, 1) + blob(99, std::string(1000, 'x'));
    fragmented_buffer_parser input{fixture::fragmented(wire, 7)};
    seastar::abort_source abort;
    auto config = codec::limits::defaults().config();
    config.max_work_bytes = byte_count{128};
    codec::cooperative_work work{*codec::limits::make(config), abort};
    auto memory = fixture::reserve(input, work);
    expect_error(
      protocol::decode_control(input, frame_kind::error, {}, memory, work)
        .get(),
      errc::resource_exhausted);
    memory.operation_remaining = byte_count{};
    expect_error(
      protocol::decode_control(input, frame_kind::error, {}, memory, work)
        .get(),
      errc::resource_exhausted);
    EXPECT_EQ(input.bytes_consumed(), byte_count{});
    EXPECT_EQ(input.checkpoint_depth(), 0U);
    abort.request_abort();
    expect_error(
      protocol::decode_control(input, frame_kind::error, {}, memory, work)
        .get(),
      errc::aborted);
}

TEST(ControlCodecTest, MaximumPayloadAndOwningCapacitiesFitTheCombinedProof) {
    std::string versions;
    std::string codecs;
    std::string formats;
    for (std::uint32_t n = 1; n <= 16; ++n)
        versions += fixture::varint(n);
    for (std::uint32_t n = 0; n < 16; ++n)
        codecs += fixture::varint(n);
    for (std::uint16_t n = 1; n <= 32; ++n)
        formats += blob(2, fixture::format(n));
    const auto caps = blob(1, versions) + formats + blob(3, codecs)
                      + scalar(4, 16'777'216) + scalar(5, 8'388'608)
                      + scalar(6, 4096) + scalar(7, 1'048'576)
                      + scalar(8, 4096);
    auto wire = fixture::request(blob(1, std::string(4093, 'v')), caps);
    while (wire.size() + 4100 <= 65536)
        wire += blob(99, std::string(4096, 'u'));
    const auto remaining = 65536 - wire.size();
    bool filled = remaining == 0;
    for (std::size_t n = 0; n <= 4096 && !filled; ++n) {
        auto tail = blob(99, std::string(n, 'u'));
        if (tail.size() == remaining) {
            wire += tail;
            filled = true;
        }
    }
    ASSERT_TRUE(filled);
    ASSERT_EQ(wire.size(), 65536U);
    fragmented_buffer_parser input{fixture::fragmented(wire, 67)};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto memory = fixture::reserve(input, work);
    auto decoded = protocol::decode_control(
                     input, frame_kind::handshake_request, {}, memory, work)
                     .get();
    ASSERT_TRUE(decoded.has_value());
    EXPECT_TRUE(input.at_end());
    const auto& value = std::get<protocol::handshake_request>(
      decoded->value.data());
    EXPECT_EQ(value.build.version.size(), 4093U);
    EXPECT_EQ(value.capabilities.formats.size(), 32U);
    // This fixture forces a heap string; the array pointers are also actual
    // allocation bases. No SSO or subview pointer is passed to the allocator.
    const auto served = ::malloc_usable_size(
                          const_cast<char*>(value.build.version.data()))
                        + ::malloc_usable_size(
                          const_cast<std::uint16_t*>(
                            value.capabilities.protocol_versions.data()))
                        + ::malloc_usable_size(
                          const_cast<protocol::format_capability*>(
                            value.capabilities.formats.data()))
                        + ::malloc_usable_size(
                          const_cast<std::uint8_t*>(
                            value.capabilities.compression_codecs.data()));
    const auto output_cost = memory.metadata_remaining.value()
                             - decoded->remaining.metadata_remaining.value();
    EXPECT_LE(served, output_cost);
    const auto native_cost = protocol::detail::bound_control_parse(
      protocol::detail::control_message::handshake_request,
      byte_count{wire.size()},
      work.policy(),
      fixture::charge);
    ASSERT_TRUE(native_cost.has_value());
    EXPECT_LE(
      native_cost->generated_peak.value() + native_cost->input_copy.value()
        + output_cost,
      1U << 20U);
    fragmented_buffer_parser oversized{
      fixture::fragmented(wire + scalar(99, 1), 67)};
    expect_error(
      protocol::decode_control(
        oversized,
        frame_kind::handshake_request,
        {},
        fixture::reserve(oversized, work),
        work)
        .get(),
      errc::resource_exhausted);
    EXPECT_EQ(oversized.bytes_consumed(), byte_count{});
}

TEST(ControlCodecTest, LocalPolicyDoesNotRewritePeerAdvertisements) {
    fragmented_buffer_parser input{fixture::fragmented(fixture::request(), 7)};
    seastar::abort_source abort;
    auto config = codec::limits::defaults().config();
    config.max_encoded_body_bytes = byte_count{1024};
    config.max_nesting_depth = item_count{3};
    codec::cooperative_work work{*codec::limits::make(config), abort};
    auto decoded = protocol::decode_control(
                     input,
                     frame_kind::handshake_request,
                     {},
                     fixture::reserve(input, work),
                     work)
                     .get();
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(
      std::get<protocol::handshake_request>(decoded->value.data())
        .capabilities.max_frame_body_bytes,
      byte_count{16'777'216});
}

TEST(ControlCodecTest, QueuedCancellationJoinsCleanupAndRollsBack) {
    const auto wire = scalar(1, 1) + blob(99, std::string(4096, 'x'));
    fragmented_buffer_parser input{fixture::fragmented(wire, 7)};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto memory = fixture::reserve(input, work);
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
    std::optional<codec::result<protocol::decoded_control>> outcome;
    std::exception_ptr exception;
    bool pending = false;
    try {
        auto future = protocol::decode_control(
          input, frame_kind::error, {}, memory, work);
        pending = !future.available();
        outcome.emplace(future.get());
    } catch (...) {
        exception = std::current_exception();
    }
    const bool during_work = observed;
    observer.get();
    if (exception) std::rethrow_exception(exception);
    EXPECT_TRUE(pending);
    EXPECT_TRUE(during_work);
    ASSERT_TRUE(outcome.has_value());
    expect_error(*outcome, errc::aborted);
    EXPECT_EQ(input.bytes_consumed(), byte_count{});
    EXPECT_EQ(input.checkpoint_depth(), 0U);
}

TEST(ControlCodecTest, NativeAndConversionAllocationFailuresRollbackEveryRoot) {
#if !defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    GTEST_SKIP() << "allocation failure injection is disabled";
#else
    const auto build = blob(1, std::string(100, 'v'))
                       + blob(2, std::string(100, 'r'));
    const std::array cases{
      std::pair{frame_kind::handshake_request, fixture::request(build)},
      std::pair{frame_kind::handshake_response, fixture::response(build)},
      std::pair{frame_kind::redirect, fixture::redirect(std::string(100, 'h'))},
      std::pair{
        frame_kind::handshake_request, allocation_request(false, false)},
      std::pair{frame_kind::handshake_request, allocation_request(false, true)},
      std::pair{frame_kind::handshake_request, allocation_request(true, false)},
      std::pair{frame_kind::handshake_request, allocation_request(true, true)},
      std::pair{
        frame_kind::error,
        scalar(1, 3) + blob(2, std::string(100, 'r'))
          + blob(3, fixture::routing())}};
    for (const auto& [kind, payload] : cases) {
        const auto wire = payload + blob(99, std::string(100, 'u'));
        ASSERT_TRUE(decode(wire, kind).has_value());
        std::size_t failures = 0;
        bool completed = false;
        for (std::uint64_t ordinal = 0; ordinal < 512; ++ordinal) {
            fragmented_buffer_parser input{
              fixture::fragmented("pre" + wire, 7)};
            ASSERT_TRUE(input.skip(byte_count{3}).has_value());
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            const auto memory = fixture::reserve(input, work);
            std::optional<codec::result<protocol::decoded_control>> result;
            bool allocation_failed = false;
            auto& injector = seastar::memory::local_failure_injector();
            injector.fail_after(ordinal);
            try {
                result.emplace(
                  protocol::decode_control(input, kind, {}, memory, work)
                    .get());
            } catch (const std::bad_alloc&) {
                allocation_failed = true;
            } catch (...) {
                injector.cancel();
                throw;
            }
            const bool injected = injector.failed();
            injector.cancel();
            EXPECT_EQ(input.checkpoint_depth(), 0U);
            if (injected) {
                ++failures;
                EXPECT_TRUE(allocation_failed);
                EXPECT_FALSE(result.has_value());
                EXPECT_EQ(input.bytes_consumed(), byte_count{3});
            } else {
                ASSERT_TRUE(result.has_value());
                ASSERT_TRUE(result->has_value());
                EXPECT_TRUE(input.at_end());
                completed = true;
                break;
            }
        }
        EXPECT_GT(failures, 0U);
        EXPECT_TRUE(completed);
    }
#endif
}
} // namespace
