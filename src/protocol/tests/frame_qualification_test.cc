#include "src/base/allocation.h"
#include "src/bytes/fragmented_buffer_builder.h"
#include "src/codec/staging_cooperative.h"
#include "src/codec/tests/prepared_abort_source.h"
#include "src/codec/tests/qualification_profile.h"
#include "src/model/tests/model_bench_fixture.h"
#include "src/protocol/batch_frame_codec.h"
#include "src/protocol/frame_decode_internal.h"
#include "src/protocol/tests/batch_frame_test_support.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/preempt.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/later.hh>

#include <crc32c/crc32c.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <malloc.h>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>

namespace {
using namespace kwaque;
namespace protocol = kwaque::protocol;
namespace fixture = protocol::testing::frame_fixture;
namespace batch_fixture = protocol::testing::batch_frame_fixture;
using bytes::fragmented_buffer;
using bytes::fragmented_buffer_parser;
using fixture::charge;
// One MiB stays unavailable for native/frame/fixture costs. No retained large
// source cache is kept alongside these operation-local inputs.
constexpr byte_count available{codec::testing::residual};
constexpr std::size_t maximum_payload = 16U << 20U;

codec::decode_budget
reserve(const fragmented_buffer_parser& input, codec::cooperative_work& work) {
    return codec::reserve_decode_input(
             input,
             work.policy(),
             {available, byte_count{1U << 20U}, charge},
             fixture::context)
      .value();
}
void expect_error(const auto& result, errc code) {
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), code);
}

using fixture::make_payload;
using fixture::patterned_payload;
using fixture::wrap;

std::uint64_t observed_backing(const fragmented_buffer& input) {
    std::uint64_t total = 0;
    // Only complete fresh builder/copy_of fragments are supplied here. Never
    // pass an interior pointer from a decoded slice to malloc_usable_size.
    for (const auto fragment : input) {
        const auto served = ::malloc_usable_size(
          const_cast<char*>(fragment.data()));
        EXPECT_GE(served, fragment.size());
        EXPECT_LE(served, maximum_contiguous_allocation_bytes);
        total += served;
        seastar::thread::maybe_yield();
    }
    return total;
}

seastar::future<> control_progress(
  seastar::abort_source& stop,
  std::uint64_t& ticks,
  std::chrono::steady_clock::duration& largest_gap) {
    auto previous = std::chrono::steady_clock::now();
    while (!stop.abort_requested()) {
        co_await seastar::yield();
        if (!stop.abort_requested()) {
            const auto now = std::chrono::steady_clock::now();
            largest_gap = std::max(largest_gap, now - previous);
            previous = now;
            ++ticks;
        }
    }
}

template<typename Start>
auto with_control_progress(const std::string& name, Start start) {
    const auto deadline = std::chrono::steady_clock::now()
                          + std::chrono::seconds{2};
    while (!seastar::need_preempt()
           && std::chrono::steady_clock::now() < deadline) {
    }
    if (!seastar::need_preempt())
        throw std::runtime_error("native preemption was not observed");
    seastar::abort_source stop;
    std::uint64_t ticks = 0;
    std::chrono::steady_clock::duration gap{};
    auto observer = control_progress(stop, ticks, gap);
    auto joined = seastar::defer([&] {
        stop.request_abort();
        observer.get();
    });
#if !defined(SEASTAR_DEFAULT_ALLOCATOR)
    const auto allocations = seastar::memory::stats().mallocs();
#endif
    const auto tasks = seastar::engine().get_sched_stats().tasks_processed;
    auto pending = start();
    const bool suspended = !pending.available();
    auto result = pending.get();
#if !defined(SEASTAR_DEFAULT_ALLOCATOR)
    const auto allocated = seastar::memory::stats().mallocs() - allocations;
#endif
    const auto processed = seastar::engine().get_sched_stats().tasks_processed
                           - tasks;
    EXPECT_TRUE(suspended);
    EXPECT_GT(ticks, 0U);
#if !defined(SEASTAR_DEFAULT_ALLOCATOR)
    ::testing::Test::RecordProperty("allocation_profile", "native");
    ::testing::Test::RecordProperty(
      name + "_allocations", std::to_string(allocated));
#else
    ::testing::Test::RecordProperty("allocation_profile", "system");
#endif
    ::testing::Test::RecordProperty(name + "_tasks", std::to_string(processed));
    ::testing::Test::RecordProperty(
      name + "_control_ticks", std::to_string(ticks));
    ::testing::Test::RecordProperty(
      name + "_largest_gap_ns",
      std::to_string(
        std::chrono::duration_cast<std::chrono::nanoseconds>(gap).count()));
    // Counts include the observer. They do not measure transient allocator
    // page use, opaque frame sizes, or establish a latency threshold.
    return result;
}

TEST(FrameQualificationTest, SmallPayloadFixturesPreserveBytesAndLayout) {
    for (const auto& [size, width] :
         std::array<std::pair<std::size_t, std::size_t>, 5>{
           {{0, 64}, {3, 64}, {64, 64}, {65, 64}, {71, 7}}}) {
        SCOPED_TRACE(
          ::testing::Message{} << "size=" << size << " width=" << width);
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto payload = make_payload(size, width, work).get();
        const std::string expected(size, 'x');
        EXPECT_TRUE(payload.bytes.content_equals(expected));
        EXPECT_EQ(payload.checksum, fixture::crc32c(expected));
        std::size_t offset = 0;
        for (const auto fragment : payload.bytes) {
            EXPECT_EQ(fragment.size(), std::min(width, size - offset));
            offset += fragment.size();
        }
        EXPECT_EQ(offset, size);
        auto encoded = wrap(std::move(payload), work).get();
        EXPECT_TRUE(encoded.content_equals(fixture::wire(expected)));
    }
}

TEST(
  FrameQualificationTest,
  MaximumPayloadAndFragmentedLayoutPermitControlProgress) {
    for (const std::size_t width : {65472U, 16401U}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto payload = make_payload(maximum_payload, width, work).get();
        const auto checksum = payload.checksum;
        const auto observed = observed_backing(payload.bytes);
        const auto cost = payload.bytes.allocation_cost(charge).value();
        EXPECT_LE(observed, cost.backing.value());
        EXPECT_LE(
          cost.largest_allocation.value(), maximum_contiguous_allocation_bytes);
        const auto name = "width_" + std::to_string(width);
        auto encoded = with_control_progress(name + "_encode", [&] {
            return protocol::encode_frame(
              std::move(payload.bytes),
              fixture::metadata,
              work,
              fixture::bounds,
              {},
              available,
              charge);
        });
        ASSERT_TRUE(encoded.has_value());
        EXPECT_EQ(encoded->size(), byte_count{48 + maximum_payload});
        EXPECT_EQ(encoded->fragment_count(), width == 16401 ? 1024U : 258U);
        fragmented_buffer_parser emitted{encoded->share()};
        std::array<char, 48> prefix{};
        ASSERT_TRUE(emitted.peek_to(prefix).has_value());
        EXPECT_EQ(
          (std::string_view{prefix.data(), prefix.size()}),
          fixture::header(maximum_payload, checksum));
        ASSERT_TRUE(emitted.skip(byte_count{48}).has_value());
        while (!emitted.at_end()) {
            const auto fragment = emitted.peek_current_fragment();
            EXPECT_TRUE(
              std::all_of(
                fragment.bytes().begin(), fragment.bytes().end(), [](char c) {
                    return c == 'x';
                }));
            ASSERT_TRUE(emitted.skip(byte_count{fragment.size()}).has_value());
            seastar::thread::maybe_yield();
        }
        // Drop the validation alias before the independent decode operation.
        emitted = fragmented_buffer_parser{};
        *encoded = fragmented_buffer{};

        auto independent
          = wrap(make_payload(maximum_payload, width, work).get(), work).get();
        EXPECT_LE(
          observed_backing(independent),
          independent.allocation_cost(charge)->backing.value());
        fragmented_buffer_parser input{std::move(independent)};
        const auto memory = reserve(input, work);
        auto decoded = with_control_progress(name + "_decode", [&] {
            return protocol::decode_frame(
              input, fixture::bounds, memory, work, fixture::context);
        });
        ASSERT_TRUE(decoded.has_value());
        auto* frame = std::get_if<protocol::framed_payload>(&*decoded);
        ASSERT_NE(frame, nullptr);
        EXPECT_TRUE(input.at_end());
        EXPECT_EQ(frame->payload.size(), byte_count{maximum_payload});
        EXPECT_EQ(frame->header.payload_crc32c, checksum);
        for (const auto fragment : frame->payload) {
            EXPECT_TRUE(
              std::all_of(
                fragment.bytes().begin(), fragment.bytes().end(), [](char c) {
                    return c == 'x';
                }));
            seastar::thread::maybe_yield();
        }
        const auto retained = frame->payload.allocation_cost(charge).value();
        EXPECT_EQ(
          memory.metadata_remaining.value()
            - frame->remaining.metadata_remaining.value(),
          retained.descriptors.value());
        EXPECT_EQ(
          memory.operation_remaining.value()
            - frame->remaining.operation_remaining.value(),
          retained.descriptors.value());
        EXPECT_LE(retained.backing, work.policy().config().max_retained_bytes);
        EXPECT_LE(
          retained.largest_allocation.value(),
          maximum_contiguous_allocation_bytes);
    }
}

TEST(
  FrameQualificationTest, MaximumValidAssignedRegionRetainsItsModelContract) {
    for (const bool compressed : {false, true}) {
        fragmented_buffer payload;
        {
            model::bench::model_fixture model_source{8, 1048565, 65472};
            model_source.initialize().get();
            payload = std::move(model_source.assigned_wire);
        }
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        if (compressed) {
            fragmented_buffer_parser source{std::move(payload)};
            const auto memory = reserve(source, work);
            auto decoded = model::decode_assigned_batch(
                             source,
                             model::bench::expected_context(),
                             memory,
                             work,
                             fixture::context)
                             .get();
            ASSERT_TRUE(decoded.has_value());
            // The source parser remains reserved while the consuming encoder
            // owns the record alias. Its input charges are conservative over
            // that alias.
            auto encoded = model::encode_assigned_batch(
                             std::move(decoded->value),
                             compression::codec_id::lz4,
                             work,
                             memory.operation_remaining,
                             charge,
                             fixture::context)
                             .get();
            ASSERT_TRUE(encoded.has_value());
            payload = std::move(*encoded);
        }
        std::uint32_t checksum = 0;
        for (auto fragment : payload) {
            checksum = ::crc32c::Extend(
              checksum,
              reinterpret_cast<const std::uint8_t*>(fragment.data()),
              fragment.size());
            seastar::thread::maybe_yield();
        }
        auto fields = fixture::metadata;
        fields.kind = protocol::frame_kind::assigned_batch;
        fragmented_buffer_parser input{
          wrap({std::move(payload), checksum}, work, {}, fields).get()};
        const auto memory = reserve(input, work);
        auto decoded = with_control_progress(
          compressed ? "assigned_max_lz4" : "assigned_max_none", [&] {
              return protocol::decode_assigned_frame(
                input,
                model::bench::expected_context(),
                fixture::bounds,
                memory,
                work,
                fixture::context);
          });
        ASSERT_TRUE(decoded.has_value());
        auto* frame = std::get_if<protocol::decoded_assigned_frame>(&*decoded);
        ASSERT_NE(frame, nullptr);
        EXPECT_TRUE(input.at_end());
        EXPECT_EQ(frame->batch.records().size(), byte_count{8U << 20U});
        EXPECT_EQ(
          frame->batch.context().submitted().binding(),
          model::bench::fixture_binding());
        EXPECT_EQ(
          frame->fingerprint_verification,
          model::batch_fingerprint_verification::recomputed);
        EXPECT_LE(
          memory.metadata_remaining.value()
            - frame->remaining.metadata_remaining.value(),
          1U << 20U);
    }
}

TEST(FrameQualificationTest, CompleteFrameAndControlCapsIncludeActualHeaders) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto fields = fixture::metadata;
    fields.kind = protocol::frame_kind::handshake_request;
    fields.stream = model::transport_stream_id{};
    const auto extensions = fixture::extension(7, 0, std::string(4040, 'x'));
    fragmented_buffer_parser input{
      wrap(make_payload(65536, 32704, work).get(), work, extensions, fields)
        .get()};
    const auto memory = reserve(input, work);
    auto result = protocol::decode_frame(
                    input,
                    {byte_count{65536}, byte_count{69632}},
                    memory,
                    work,
                    fixture::context)
                    .get();
    ASSERT_TRUE(result.has_value());
    auto* frame = std::get_if<protocol::framed_payload>(&*result);
    ASSERT_NE(frame, nullptr);
    EXPECT_EQ(frame->header.header_bytes, byte_count{4096});
    EXPECT_TRUE(input.at_end());
    auto too_large = fixture::header(65537, 0, {}, fields);
    fragmented_buffer_parser denied{fixture::fragmented(too_large, 7)};
    expect_error(
      protocol::decode_frame(
        denied, fixture::bounds, reserve(denied, work), work)
        .get(),
      errc::resource_exhausted);
    auto maximum = fixture::header(maximum_payload, 0, extensions);
    fragmented_buffer_parser narrow{fixture::fragmented(maximum, 7)};
    expect_error(
      protocol::decode_frame(
        narrow,
        {byte_count{maximum_payload}, byte_count{maximum_payload + 4095}},
        reserve(narrow, work),
        work)
        .get(),
      errc::resource_exhausted);
    fixture::put_u32(
      maximum, 12, static_cast<std::uint32_t>(maximum_payload + 1));
    fragmented_buffer_parser over{
      fixture::fragmented(maximum.substr(0, 48), 1)};
    expect_error(
      protocol::decode_frame(over, fixture::bounds, reserve(over, work), work)
        .get(),
      errc::resource_exhausted);
}

thread_local seastar::abort_source* admission_abort = nullptr;
thread_local std::size_t admission_ordinal = 0;
thread_local std::size_t admission_calls = 0;
thread_local std::uint64_t largest_served = 0;
byte_count aborting_charge(byte_count request) noexcept {
    const auto served = charge(request);
    largest_served = std::max(largest_served, served.value());
    if (admission_abort != nullptr && admission_calls++ == admission_ordinal)
        admission_abort->request_abort();
    return served;
}

TEST(
  FrameQualificationTest,
  AdmissionCancellationPreservesChargeWithoutAllocation) {
    codec::testing::prepared_abort_source abort;
    admission_abort = &abort;
    admission_ordinal = 0;
    admission_calls = 0;
    auto reset = seastar::defer([] { admission_abort = nullptr; });
    const byte_count request{4096};
    const auto expected = charge(request);
#if !defined(SEASTAR_DEFAULT_ALLOCATOR)
    const auto before = seastar::memory::stats().mallocs();
#endif
    const auto served = aborting_charge(request);
#if !defined(SEASTAR_DEFAULT_ALLOCATOR)
    const auto allocations = seastar::memory::stats().mallocs() - before;
    EXPECT_EQ(allocations, 0U);
#endif
    EXPECT_TRUE(abort.abort_requested());
    EXPECT_EQ(served, expected);
    EXPECT_EQ(admission_calls, 1U);
#if defined(SEASTAR_DEFAULT_ALLOCATOR)
    GTEST_SKIP() << "semantic cancellation passed; native allocation counters "
                    "are unavailable";
#endif
}

TEST(FrameQualificationTest, CancellationAtEveryAdmissionDrainsAllPublicPaths) {
    for (unsigned operation = 0; operation < 6; ++operation) {
        const bool assigned = operation >= 4;
        const bool compressed = operation == 3 || operation == 5;
        const auto wire
          = operation < 2
              ? fixture::wire(std::string(71, 'x'), fixture::extension())
              : batch_fixture::frame(
                  batch_fixture::batch_wire(assigned, compressed, assigned),
                  assigned);
        bool completed = false;
        unsigned cancellations = 0;
        for (std::size_t ordinal = 0; ordinal < 4096 && !completed; ++ordinal) {
            codec::testing::prepared_abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            fragmented_buffer_parser input{fixture::fragmented(wire, 67)};
            auto memory = reserve(input, work);
            memory.charge = aborting_charge;
            auto donor = fixture::fragmented("abc", 1);
            admission_ordinal = ordinal;
            admission_calls = 0;
            largest_served = 0;
            admission_abort = &abort;
            auto reset = seastar::defer([] { admission_abort = nullptr; });
            const auto run = [&] -> codec::result<void> {
                if (operation == 0) {
                    auto result = protocol::encode_frame(
                      std::move(donor),
                      fixture::metadata,
                      work,
                      fixture::bounds,
                      {},
                      available,
                      aborting_charge);
                    auto value = result.get();
                    if (!value) return codec::failure(value.error());
                } else if (operation == 1) {
                    auto value = protocol::decode_frame(
                                   input, fixture::bounds, memory, work)
                                   .get();
                    if (!value) return codec::failure(value.error());
                    if (!std::holds_alternative<protocol::framed_payload>(
                          *value))
                        throw std::runtime_error(
                          "complete fixture needs more input");
                } else if (assigned) {
                    auto value = protocol::decode_assigned_frame(
                                   input,
                                   batch_fixture::expected(),
                                   fixture::bounds,
                                   memory,
                                   work)
                                   .get();
                    if (!value) return codec::failure(value.error());
                    if (!std::holds_alternative<
                          protocol::decoded_assigned_frame>(*value))
                        throw std::runtime_error(
                          "complete fixture needs more input");
                } else {
                    auto value = protocol::decode_submitted_frame(
                                   input,
                                   batch_fixture::expected(),
                                   fixture::bounds,
                                   memory,
                                   work)
                                   .get();
                    if (!value) return codec::failure(value.error());
                    if (!std::holds_alternative<
                          protocol::decoded_submitted_frame>(*value))
                        throw std::runtime_error(
                          "complete fixture needs more input");
                }
                return {};
            };
            const auto result = run();
            admission_abort = nullptr;
            EXPECT_LE(largest_served, maximum_contiguous_allocation_bytes);
            if (abort.abort_requested()) {
                expect_error(result, errc::aborted);
                EXPECT_EQ(input.bytes_consumed(), byte_count{});
                ++cancellations;
            } else {
                ASSERT_TRUE(result.has_value());
                EXPECT_EQ(
                  input.bytes_consumed(),
                  byte_count{operation == 0 ? 0U : wire.size()});
                completed = true;
            }
            if (operation == 0) {
                // NOLINTNEXTLINE(bugprone-use-after-move)
                EXPECT_TRUE(donor.empty());
            }
            EXPECT_EQ(input.checkpoint_depth(), 0U);
        }
        EXPECT_TRUE(completed);
        EXPECT_GT(cancellations, 0U);
    }
}

struct decoder_state {
    seastar::abort_source* abort_on_destroy{nullptr};
    bool fail{false};
    bool throw_error{false};
    unsigned calls{0};
    unsigned destroyed{0};
    unsigned results_alive{0};
};
// The pointer observes fixture lifetime only; the state outlives the joined
// call and every result. No borrowed payload is returned by this probe.
struct tracked_result {
    codec::decode_budget remaining;
    decoder_state* state;
    tracked_result(codec::decode_budget memory, decoder_state& owner) noexcept
      : remaining(memory)
      , state(&owner) {
        ++state->results_alive;
    }
    tracked_result(tracked_result&& other) noexcept
      : remaining(other.remaining)
      , state(std::exchange(other.state, nullptr)) {}
    tracked_result(const tracked_result&) = delete;
    ~tracked_result() noexcept {
        if (state != nullptr) --state->results_alive;
    }
};
class cleanup_decoder {
public:
    explicit cleanup_decoder(decoder_state& state) noexcept
      : state_(&state) {}
    cleanup_decoder(cleanup_decoder&& other) noexcept
      : state_(std::exchange(other.state_, nullptr)) {}
    cleanup_decoder(const cleanup_decoder&) = delete;
    ~cleanup_decoder() noexcept {
        if (state_ != nullptr) {
            ++state_->destroyed;
            if (state_->abort_on_destroy != nullptr)
                state_->abort_on_destroy->request_abort();
        }
    }
    seastar::future<codec::result<tracked_result>> operator()(
      fragmented_buffer_parser& input,
      const protocol::frame_header& header,
      codec::field_context context,
      codec::decode_budget memory,
      codec::cooperative_work&) {
        ++state_->calls;
        input.skip(header.payload_bytes).value();
        if (state_->throw_error)
            throw std::runtime_error("test payload failure");
        if (state_->fail)
            co_return codec::failure(
              codec::error{
                errc::wrong_context,
                context.family,
                context.field,
                context.origin});
        co_return tracked_result{memory, *state_};
    }

private:
    decoder_state* state_;
};

TEST(
  FrameQualificationTest,
  CallbackCleanupAbortCannotCommitOrHideEarlierFailure) {
    for (unsigned mode = 0; mode < 4; ++mode) {
        fragmented_buffer_parser input{
          fixture::fragmented("pre" + fixture::wire(), 1)};
        ASSERT_TRUE(input.push_checkpoint().has_value());
        ASSERT_TRUE(input.skip(byte_count{3}).has_value());
        codec::testing::prepared_abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto memory = reserve(input, work);
        decoder_state state{
          .abort_on_destroy = mode == 0 ? nullptr : &abort,
          .fail = mode == 2,
          .throw_error = mode == 3};
        auto pending = protocol::detail::decode_frame_payload<tracked_result>(
          input,
          std::nullopt,
          fixture::bounds,
          memory,
          work,
          cleanup_decoder{state},
          fixture::context,
          codec::input_boundary::open);
        if (mode == 3) {
            EXPECT_THROW(static_cast<void>(pending.get()), std::runtime_error);
        } else {
            auto result = pending.get();
            if (mode == 0) {
                ASSERT_TRUE(result.has_value());
                ASSERT_TRUE(std::holds_alternative<tracked_result>(*result));
                EXPECT_EQ(state.results_alive, 1U);
            } else
                expect_error(
                  result, mode == 1 ? errc::aborted : errc::wrong_context);
        }
        EXPECT_EQ(state.results_alive, 0U);
        EXPECT_EQ(state.calls, 1U);
        EXPECT_EQ(state.destroyed, 1U);
        EXPECT_EQ(input.bytes_consumed(), byte_count{mode == 0 ? 54U : 3U});
        EXPECT_EQ(input.checkpoint_depth(), 1U);
    }
}

} // namespace
