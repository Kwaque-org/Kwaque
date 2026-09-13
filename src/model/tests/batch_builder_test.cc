#include "src/model/batch_builder.h"
#include "src/model/batch_codec.h"
#include "src/model/fingerprint.h"
#include "src/model/record_scan.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/preempt.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/util/alloc_failure_injector.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/later.hh>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <bit>
#include <chrono>
#include <exception>
#include <limits>
#include <new>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

namespace {
using namespace std::literals;
namespace model = kwaque::model;
namespace codec = kwaque::codec;
using kwaque::byte_count;
using kwaque::errc;
using kwaque::item_count;
using kwaque::bytes::fragmented_buffer;
using kwaque::runtime::wall_time;
using nullable = std::optional<fragmented_buffer>;

constexpr auto null_record = "\x06\x00\x00\x00\x01\x01\x00"sv;
constexpr auto golden_sha
  = "a094509035402abad319ae9b75972a8c3eb583f3c4829c4353c21f92bad9d9a7"sv;
// Independent complete envelope: opaque IDs, little-endian fixed fields,
// SHA-256 of the semantic stream, and both Castagnoli checksums.
constexpr auto golden_envelope
  = "4b5142460100010001002000af00000000000000000000009cc9e480c3709e26"
    "0102030405060708090a0b0c0d0e0f1002000000000000000300000000000000"
    "04000000000000002122232425262728292a2b2c2d2e2f304142434445464748"
    "494a4b4c4d4e4f5007000000000000006162636465666768696a6b6c6d6e6f70"
    "0900000000000000a094509035402abad319ae9b75972a8c3eb583f3c4829c435"
    "3c21f92bad9d9a7640000000000000001000000010000000000000000000100"
    "070000000700000006000000010100"sv;

static_assert(std::is_nothrow_move_constructible_v<model::submitted_batch>);
static_assert(!std::is_default_constructible_v<model::submitted_batch>);
static_assert(!std::is_copy_constructible_v<model::submitted_batch>);
static_assert(sizeof(model::batch_builder) < 4096);

byte_count charge(byte_count request) noexcept {
    if (request.value() == 0) return {};
    if (request.value() > (std::uint64_t{1} << 62U))
        return byte_count{UINT64_MAX};
    return byte_count{
      2U * std::bit_ceil(std::max(request.value(), std::uint64_t{16}))};
}
codec::decode_budget memory() {
    // Other fixture owners and verified native/frame reservations occupy the
    // unclaimed half. Allocator upper bounds are distinct from process RSS.
    return {byte_count{32U << 20U}, byte_count{1U << 20U}, charge};
}
template<typename Id>
Id object(std::uint8_t first) {
    std::array<std::uint8_t, 16> raw{};
    for (std::size_t i = 0; i < raw.size(); ++i)
        raw[i] = static_cast<std::uint8_t>(first + i);
    return Id::make(raw).value();
}
model::batch_id identity(
  std::uint64_t epoch = 2,
  std::uint64_t stream = 3,
  std::uint64_t sequence = 4,
  std::uint8_t producer = 1) {
    return model::batch_id::make(
             object<model::producer_id>(producer),
             model::producer_epoch::make(epoch).value(),
             model::producer_stream_id::make(stream).value(),
             model::batch_sequence{sequence})
      .value();
}
model::producer_stream_binding binding(
  std::uint8_t topic = 33,
  std::uint8_t range = 65,
  std::uint64_t routing = 7,
  std::uint8_t segment = 97,
  std::uint64_t generation = 9) {
    return model::producer_stream_binding::make(
             object<model::topic_id>(topic),
             object<model::range_id>(range),
             model::range_routing_epoch::make(routing).value(),
             object<model::segment_id>(segment),
             model::segment_generation::make(generation).value())
      .value();
}
fragmented_buffer bytes(std::string_view raw, std::size_t width = 65536) {
    std::vector<seastar::temporary_buffer<char>> parts;
    for (std::size_t at = 0; at < raw.size(); at += width) {
        const auto piece = raw.substr(at, width);
        seastar::temporary_buffer<char> part{piece.size()};
        std::copy(piece.begin(), piece.end(), part.get_write());
        parts.push_back(std::move(part));
    }
    return fragmented_buffer::copy_from_fragments(parts).value();
}
fragmented_buffer payload(std::size_t count) {
    std::vector<seastar::temporary_buffer<char>> parts;
    for (std::size_t at = 0; at < count; at += 65536) {
        seastar::temporary_buffer<char> part{
          std::min<std::size_t>(65536, count - at)};
        std::fill_n(part.get_write(), part.size(), 'x');
        parts.push_back(std::move(part));
    }
    return fragmented_buffer::copy_from_fragments(parts).value();
}
model::record null_value() {
    return model::make_record({}, std::nullopt, std::nullopt, {}).value();
}
model::record header_value(std::size_t width = 65536, bool reverse = false) {
    std::vector<model::record_header> headers;
    headers.push_back(
      model::make_record_header(bytes("a", width), nullable{bytes("1", width)})
        .value());
    headers.push_back(
      model::make_record_header(bytes("a", width), std::nullopt).value());
    if (reverse) std::reverse(headers.begin(), headers.end());
    return model::make_record(
             {.timestamp_delta = 123,
              .logical_delta = model::range_logical_count{999}},
             nullable{bytes("key", width)},
             nullable{bytes("value", width)},
             std::move(headers))
      .value();
}
model::batch_builder builder(codec::limits policy = codec::limits::defaults()) {
    return model::batch_builder::make(identity(), binding(), policy, charge)
      .value();
}
std::string hex_bytes(std::string_view hex) {
    const auto digit = [](char c) { return c >= 'a' ? c - 'a' + 10 : c - '0'; };
    std::string out;
    for (std::size_t i = 0; i < hex.size(); i += 2)
        out.push_back(
          static_cast<char>(16 * digit(hex[i]) + digit(hex[i + 1])));
    return out;
}
std::string digest_hex(codec::semantic_batch_digest digest) {
    std::string out;
    for (auto b : digest.bytes()) {
        out += "0123456789abcdef"[b >> 4U];
        out += "0123456789abcdef"[b & 15U];
    }
    return out;
}
template<typename T>
void error(const codec::result<T>& value, errc code) {
    ASSERT_FALSE(value.has_value());
    EXPECT_EQ(value.error().code(), code);
}
void add(
  model::batch_builder& output,
  const model::record& value,
  wall_time timestamp,
  codec::cooperative_work& work) {
    const auto reserved
      = model::reserve_record_input(value, work, memory()).get().value();
    ASSERT_TRUE(output.add(value, timestamp, work, reserved.operation_remaining)
                  .get()
                  .has_value());
}
model::submitted_batch
one_batch(const model::record& value, codec::cooperative_work& work) {
    auto output = builder(work.policy());
    add(output, value, wall_time{100}, work);
    return output.finalize(work, memory().operation_remaining).get().value();
}

TEST(BatchBuilderTest, IndependentSemanticDigestAndCompleteEnvelopeBytes) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto record = null_value();
    auto batch = one_batch(record, work);
    EXPECT_EQ(batch.context().id(), identity());
    EXPECT_EQ(batch.context().binding(), binding());
    EXPECT_EQ(batch.context().original_count().value(), 1U);
    EXPECT_EQ(batch.context().original_timestamp_base(), wall_time{100});
    EXPECT_TRUE(batch.records().content_equals(null_record));
    EXPECT_EQ(digest_hex(batch.fingerprint()), golden_sha);
    auto encoded
      = model::encode_submitted_batch(
          std::move(batch), work, memory().operation_remaining, charge)
          .get();
    ASSERT_TRUE(encoded.has_value());
    // NOLINTNEXTLINE(bugprone-use-after-move) -- empty donor contract.
    EXPECT_TRUE(batch.records().empty());
    EXPECT_EQ(encoded->size(), byte_count{207});
    EXPECT_TRUE(encoded->content_equals(hex_bytes(golden_envelope)));
}

TEST(
  BatchBuilderTest,
  ExplicitTimestampsDenseOrdinalsAndFullRecordsPreserveDonors) {
    auto source = header_value();
    auto output = builder();
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    constexpr std::array<std::int64_t, 5> times{100, 99, 110, 98, 101};
    for (const auto time : times)
        add(output, source, wall_time{time}, work);
    auto batch
      = output.finalize(work, memory().operation_remaining).get().value();
    EXPECT_EQ(source.timestamp_delta(), 123);
    EXPECT_EQ(source.logical_delta().value(), 999U);
    EXPECT_TRUE(source.key()->content_equals("key"sv));
    EXPECT_TRUE(source.value()->content_equals("value"sv));
    EXPECT_EQ(batch.header_count(), item_count{10});
    auto scanner = model::record_region_scanner::make(
                     std::move(batch).release_records(),
                     {wall_time{100},
                      model::range_logical_count{5},
                      item_count{5},
                      item_count{10}},
                     memory(),
                     work)
                     .get()
                     .value();
    for (std::size_t i = 0; i < times.size(); ++i) {
        ASSERT_TRUE(scanner.next(work).get().value());
        EXPECT_EQ(scanner.current()->fields.logical_delta.value(), i);
        EXPECT_EQ(scanner.current()->fields.timestamp_delta, times[i] - 100);
        EXPECT_EQ(scanner.current()->headers().size(), 2U);
    }
    EXPECT_TRUE(scanner.complete());
    EXPECT_TRUE(output.closed());
    error(
      output.finalize(work, memory().operation_remaining).get(), errc::closed);
    error(
      output.add(source, wall_time{102}, work, memory().operation_remaining)
        .get(),
      errc::closed);
}

TEST(
  BatchBuilderTest,
  EveryIdentityBindingTimestampAndRecordPresenceAffectsDigest) {
    std::optional<codec::semantic_batch_digest> baseline;
    for (int mode = 0; mode != 13; ++mode) {
        auto id = identity(
          mode == 1 ? 8 : 2,
          mode == 2 ? 8 : 3,
          mode == 3 ? 8 : 4,
          mode == 4 ? 2 : 1);
        auto bound = binding(
          mode == 5 ? 34 : 33,
          mode == 6 ? 66 : 65,
          mode == 7 ? 8 : 7,
          mode == 8 ? 98 : 97,
          mode == 9 ? 10 : 9);
        auto out = model::batch_builder::make(
                     id, bound, codec::limits::defaults(), charge)
                     .value();
        auto value
          = model::make_record(
              {},
              mode == 11 ? nullable{fragmented_buffer{}} : std::nullopt,
              mode == 12 ? nullable{fragmented_buffer{}} : std::nullopt,
              {})
              .value();
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        add(out, value, wall_time{mode == 10 ? 101 : 100}, work);
        auto batch
          = out.finalize(work, memory().operation_remaining).get().value();
        if (mode == 0)
            baseline = batch.fingerprint();
        else
            EXPECT_NE(batch.fingerprint(), *baseline);
    }
}

TEST(BatchBuilderTest, FragmentationDoesNotAffectDigestButHeaderOrderDoes) {
    std::optional<codec::semantic_batch_digest> baseline;
    for (int mode = 0; mode != 3; ++mode) {
        auto source = header_value(mode == 1 ? 1 : 65536, mode == 2);
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto batch = one_batch(source, work);
        if (mode == 0)
            baseline = batch.fingerprint();
        else if (mode == 1)
            EXPECT_EQ(batch.fingerprint(), *baseline);
        else
            EXPECT_NE(batch.fingerprint(), *baseline);
    }
}

TEST(BatchBuilderTest, FingerprintUsesOnePrefixThenRawRecordStream) {
    auto context
      = model::submitted_batch_context::make(
          identity(), binding(), model::range_logical_count{1}, wall_time{100})
          .value();
    for (std::size_t width = 1; width <= null_record.size(); ++width) {
        auto record_bytes = bytes(null_record, width);
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        const auto digest = model::compute_submitted_fingerprint(
                              context, record_bytes, work)
                              .get();
        ASSERT_TRUE(digest.has_value());
        EXPECT_EQ(digest_hex(*digest), golden_sha);
        EXPECT_TRUE(record_bytes.content_equals(null_record));
    }
}

TEST(
  BatchBuilderTest,
  EmptySubmissionAndTimestampOverflowCloseWithoutPublication) {
    auto source = null_value();
    for (const bool positive : {false, true}) {
        auto out = builder();
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        add(out, source, wall_time{positive ? INT64_MIN : INT64_MAX}, work);
        error(
          out
            .add(
              source,
              wall_time{positive ? INT64_MAX : INT64_MIN},
              work,
              memory().operation_remaining)
            .get(),
          errc::out_of_range);
        EXPECT_TRUE(out.closed());
        EXPECT_EQ(out.record_count(), item_count{1});
        error(
          out.finalize(work, memory().operation_remaining).get(), errc::closed);
    }
    auto empty = builder();
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    error(
      empty.finalize(work, memory().operation_remaining).get(),
      errc::invalid_argument);
    EXPECT_TRUE(empty.closed());
}

TEST(
  BatchBuilderTest, SignedTimestampEndpointsRemainRepresentableWhenDeltaFits) {
    for (const auto time : {INT64_MIN, INT64_MAX}) {
        auto source = null_value();
        auto out = builder();
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        add(out, source, wall_time{0}, work);
        add(out, source, wall_time{time}, work);
        auto batch
          = out.finalize(work, memory().operation_remaining).get().value();
        auto scanner = model::record_region_scanner::make(
                         std::move(batch).release_records(),
                         {wall_time{0},
                          model::range_logical_count{2},
                          item_count{2},
                          item_count{}},
                         memory(),
                         work)
                         .get()
                         .value();
        ASSERT_TRUE(scanner.next(work).get().value());
        ASSERT_TRUE(scanner.next(work).get().value());
        EXPECT_EQ(scanner.current()->fields.timestamp_delta, time);
    }
}

TEST(BatchBuilderTest, CountHeaderByteAndOperationCapsAreIntersecting) {
    auto source = header_value();
    for (int mode = 0; mode != 4; ++mode) {
        codec::limits_config config;
        if (mode == 0) config.max_original_records = item_count{1};
        if (mode == 1) config.max_batch_headers = item_count{2};
        if (mode == 2) config.max_expanded_batch_bytes = byte_count{30};
        auto policy = codec::limits::make(config).value();
        auto out = builder(policy);
        seastar::abort_source abort;
        codec::cooperative_work work{policy, abort};
        add(out, source, wall_time{100}, work);
        const auto allowance = mode == 3 ? byte_count{}
                                         : memory().operation_remaining;
        error(
          out.add(source, wall_time{101}, work, allowance).get(),
          errc::resource_exhausted);
        EXPECT_TRUE(out.closed());
        error(
          out.finalize(work, memory().operation_remaining).get(), errc::closed);
        EXPECT_TRUE(source.key()->content_equals("key"sv));
    }
}

TEST(BatchBuilderTest, ExactStagingBudgetAndOneByteShortAreDistinct) {
    auto source = null_value();
    // Uniform 64-KiB tails and one reserved array of 128 descriptors, which
    // transfers into publication. The bound uses actual compiled sizes.
    const auto desc = charge(
      byte_count{128U * fragmented_buffer::fragment_descriptor_size()});
    const byte_count exact{charge(byte_count{65536}).value() + desc.value()};
    for (const bool short_budget : {false, true}) {
        auto out = builder();
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        const auto result = out
                              .add(
                                source,
                                wall_time{100},
                                work,
                                byte_count{
                                  exact.value() - (short_budget ? 1U : 0U)})
                              .get();
        if (short_budget) {
            error(result, errc::resource_exhausted);
            EXPECT_TRUE(out.closed());
        } else {
            ASSERT_TRUE(result.has_value());
            auto batch = out.finalize(work, exact).get();
            ASSERT_TRUE(batch.has_value());
            EXPECT_EQ(digest_hex(batch->fingerprint()), golden_sha);
        }
    }
}

TEST(BatchBuilderTest, ThousandsOfTinyRecordsUseBoundedRawStorage) {
    auto source = model::make_record(
                    {},
                    std::nullopt,
                    std::nullopt,
                    [&] {
                        std::vector<model::record_header> h;
                        h.push_back(
                          model::make_record_header(
                            fragmented_buffer{}, std::nullopt)
                            .value());
                        return h;
                    }())
                    .value();
    auto out = builder();
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    for (std::uint64_t i = 0; i < 4096; ++i)
        add(out, source, wall_time{100}, work);
    auto batch = out.finalize(work, memory().operation_remaining).get().value();
    EXPECT_EQ(batch.context().original_count().value(), 4096U);
    EXPECT_EQ(batch.header_count(), item_count{4096});
    EXPECT_LE(batch.records().fragment_count(), 2U);
    const auto cost = batch.records().allocation_cost(charge).value();
    EXPECT_LT(cost.descriptors.value() + cost.share_controls.value(), 65536U);
    auto scanner = model::record_region_scanner::make(
                     std::move(batch).release_records(),
                     {wall_time{100},
                      model::range_logical_count{4096},
                      item_count{4096},
                      item_count{4096}},
                     memory(),
                     work)
                     .get()
                     .value();
    for (std::uint64_t i = 0; i < 4096; ++i)
        ASSERT_TRUE(scanner.next(work).get().value());
    EXPECT_TRUE(scanner.complete());
}

TEST(
  BatchBuilderTest,
  LargeRecordSurvivesBuildFingerprintAndEnvelopeUnderNarrowAllocationCap) {
    auto source = model::make_record(
                    {}, std::nullopt, nullable{payload(1048565)}, {})
                    .value();
    codec::limits_config config;
    config.max_allocation_bytes = byte_count{65536};
    const auto policy = codec::limits::make(config).value();
    auto out = builder(policy);
    seastar::abort_source abort;
    codec::cooperative_work work{policy, abort};
    // Already admitted caller-owned input may have larger old allocations; the
    // narrowed ceiling governs all new builder/encoding allocations here.
    ASSERT_TRUE(
      out.add(source, wall_time{100}, work, memory().operation_remaining)
        .get()
        .has_value());
    auto batch = out.finalize(work, memory().operation_remaining).get().value();
    EXPECT_EQ(batch.records().size(), byte_count{1048576});
    auto cost = batch.records().allocation_cost(charge).value();
    EXPECT_LE(cost.largest_allocation, config.max_allocation_bytes);
    auto encoded
      = model::encode_submitted_batch(
          std::move(batch), work, memory().operation_remaining, charge)
          .get();
    ASSERT_TRUE(encoded.has_value());
    EXPECT_EQ(encoded->size(), byte_count{1048776});
    EXPECT_LE(
      encoded->allocation_cost(charge)->largest_allocation,
      config.max_allocation_bytes);
    EXPECT_EQ(source.value()->size(), byte_count{1048565});
}

TEST(BatchBuilderTest, ExactExpandedBatchMaximumAndOneMoreRecord) {
    auto source = model::make_record(
                    {}, std::nullopt, nullable{payload(1048565)}, {})
                    .value();
    for (const bool over : {false, true}) {
        auto out = builder();
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        for (int i = 0; i < 8; ++i)
            add(out, source, wall_time{100}, work);
        if (over) {
            error(
              out
                .add(source, wall_time{100}, work, memory().operation_remaining)
                .get(),
              errc::resource_exhausted);
            EXPECT_TRUE(out.closed());
            continue;
        }
        auto batch
          = out.finalize(work, memory().operation_remaining).get().value();
        EXPECT_EQ(batch.records().size(), byte_count{8U << 20U});
        EXPECT_EQ(batch.context().original_count().value(), 8U);
        auto scanner = model::record_region_scanner::make(
                         std::move(batch).release_records(),
                         {wall_time{100},
                          model::range_logical_count{8},
                          item_count{8},
                          item_count{}},
                         memory(),
                         work)
                         .get()
                         .value();
        for (int i = 0; i < 8; ++i)
            ASSERT_TRUE(scanner.next(work).get().value());
        EXPECT_TRUE(scanner.complete());
        scanner.close(work).get();
    }
}

TEST(BatchBuilderTest, NarrowDescriptorCeilingStillAllowsSmallBatches) {
    codec::limits_config config;
    config.max_allocation_bytes = byte_count{4096};
    const auto policy = codec::limits::make(config).value();
    auto out = builder(policy);
    auto source = null_value();
    seastar::abort_source abort;
    codec::cooperative_work work{policy, abort};
    add(out, source, wall_time{100}, work);
    auto batch = out.finalize(work, memory().operation_remaining).get().value();
    EXPECT_TRUE(batch.records().content_equals(null_record));
    auto encoded
      = model::encode_submitted_batch(
          std::move(batch), work, memory().operation_remaining, charge)
          .get();
    ASSERT_TRUE(encoded.has_value());
    EXPECT_TRUE(encoded->content_equals(hex_bytes(golden_envelope)));
}

TEST(
  BatchBuilderTest,
  SubmittedWriterRejectsNarrowBodyBudgetOriginsAndConsumesDonor) {
    for (int mode = 0; mode != 4; ++mode) {
        auto source = null_value();
        seastar::abort_source abort;
        codec::cooperative_work build_work{codec::limits::defaults(), abort};
        auto batch = one_batch(source, build_work);
        codec::limits_config config;
        if (mode == 0) config.max_encoded_body_bytes = byte_count{174};
        codec::cooperative_work work{
          codec::limits::make(config).value(), abort};
        codec::field_context context;
        if (mode == 2) context.origin = UINT64_MAX - 206U;
        auto result = model::encode_submitted_batch(
                        std::move(batch),
                        work,
                        mode == 1 ? byte_count{} : memory().operation_remaining,
                        mode == 3 ? nullptr : charge,
                        context)
                        .get();
        error(
          result,
          mode >= 2 ? errc::invalid_argument : errc::resource_exhausted);
        // NOLINTNEXTLINE(bugprone-use-after-move) -- empty donor contract.
        EXPECT_TRUE(batch.records().empty());
    }
}

thread_local seastar::abort_source* cancel_source = nullptr;
thread_local unsigned calls = 0, cancel_at = 0;
byte_count observed_charge(byte_count amount) noexcept {
    if (++calls == cancel_at && cancel_source != nullptr)
        cancel_source->request_abort();
    return charge(amount);
}

TEST(
  BatchBuilderTest,
  AbortDuringSubmissionEncodingAndFinalizationNeverPublishes) {
    unsigned last = 0;
    for (const bool cancel : {false, true}) {
        auto source = null_value();
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto batch = one_batch(source, work);
        calls = 0;
        cancel_at = cancel ? last : 0;
        cancel_source = &abort;
        auto reset = seastar::defer([] noexcept { cancel_source = nullptr; });
        auto encoded = model::encode_submitted_batch(
                         std::move(batch),
                         work,
                         memory().operation_remaining,
                         observed_charge)
                         .get();
        if (!cancel) {
            ASSERT_TRUE(encoded.has_value());
            last = calls;
            EXPECT_GT(last, 0U);
        } else
            error(encoded, errc::aborted);
        // NOLINTNEXTLINE(bugprone-use-after-move) -- empty donor contract.
        EXPECT_TRUE(batch.records().empty());
    }
    auto source = null_value();
    auto out = builder();
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    add(out, source, wall_time{100}, work);
    abort.request_abort();
    error(
      out.finalize(work, memory().operation_remaining).get(), errc::aborted);
    EXPECT_TRUE(out.closed());
    error(
      out.add(source, wall_time{100}, work, memory().operation_remaining).get(),
      errc::closed);
}

TEST(
  BatchBuilderTest, QueuedAbortInterruptsPendingRecordAppendAndClosesBuilder) {
    auto source = model::make_record(
                    {}, std::nullopt, nullable{payload(1048565)}, {})
                    .value();
    codec::limits_config config;
    config.max_work_bytes = byte_count{128};
    config.max_work_items = item_count{64};
    const auto policy = codec::limits::make(config).value();
    auto out = builder(policy);
    seastar::abort_source abort;
    codec::cooperative_work work{policy, abort};
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
    bool pending = false;
    std::optional<codec::result<void>> result;
    std::exception_ptr exception;
    try {
        auto added = out.add(
          source, wall_time{100}, work, memory().operation_remaining);
        pending = !added.available();
        result.emplace(added.get());
    } catch (...) {
        exception = std::current_exception();
    }
    const auto during = observed;
    observer.get();
    if (exception) std::rethrow_exception(exception);
    EXPECT_TRUE(pending);
    EXPECT_TRUE(during);
    ASSERT_TRUE(result);
    error(*result, errc::aborted);
    EXPECT_TRUE(out.closed());
    error(out.finalize(work, memory().operation_remaining).get(), errc::closed);
    EXPECT_EQ(source.value()->size(), byte_count{1048565});
}

TEST(BatchBuilderTest, ReachedAllocationFailuresDoNotPublishPartialBatches) {
#if !defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    GTEST_SKIP() << "ordinary allocation injection is disabled";
#else
    static_cast<void>(kwaque::error_category());
    {
        auto source = null_value();
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        // Initialize the native SHA provider before sweeping operation-local
        // allocations, including when this test runs alone.
        static_cast<void>(one_batch(source, work));
    }
    for (int mode = 0; mode != 3; ++mode) {
        bool failed_once = false, succeeded = false;
        for (std::uint64_t ordinal = 0; ordinal < 256 && !succeeded;
             ++ordinal) {
            auto source = header_value();
            auto out = builder();
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            if (mode != 0) add(out, source, wall_time{100}, work);
            std::optional<model::submitted_batch> batch;
            if (mode == 2)
                batch.emplace(out.finalize(work, memory().operation_remaining)
                                .get()
                                .value());
            auto& injector = seastar::memory::local_failure_injector();
            bool threw = false, reached = false;
            injector.fail_after(ordinal);
            try {
                if (mode == 0) {
                    auto result = out
                                    .add(
                                      source,
                                      wall_time{100},
                                      work,
                                      memory().operation_remaining)
                                    .get();
                    reached = injector.failed();
                    injector.cancel();
                    ASSERT_TRUE(result.has_value());
                } else if (mode == 1) {
                    auto result
                      = out.finalize(work, memory().operation_remaining).get();
                    reached = injector.failed();
                    injector.cancel();
                    ASSERT_TRUE(result.has_value());
                } else {
                    auto result = model::encode_submitted_batch(
                                    std::move(*batch),
                                    work,
                                    memory().operation_remaining,
                                    charge)
                                    .get();
                    reached = injector.failed();
                    injector.cancel();
                    ASSERT_TRUE(result.has_value());
                }
                succeeded = !reached;
            } catch (const std::bad_alloc&) {
                reached = injector.failed();
                threw = true;
            } catch (const std::runtime_error&) {
                // A failed C allocator call returns null. The existing SHA
                // owner reports native context/setup failure by exception.
                reached = injector.failed();
                if (!reached || mode != 1) {
                    injector.cancel();
                    throw;
                }
                threw = true;
            } catch (...) {
                injector.cancel();
                throw;
            }
            injector.cancel();
            if (threw) {
                EXPECT_TRUE(reached);
                failed_once = true;
            } else if (mode != 1) {
                EXPECT_FALSE(reached);
            }
            // A provider may recover an optional failed allocation. Keep
            // sweeping until one complete call reaches no injection point.
            EXPECT_TRUE(source.key()->content_equals("key"sv));
            EXPECT_EQ(source.logical_delta().value(), 999U);
            out.close(work).get();
        }
        EXPECT_TRUE(failed_once);
        EXPECT_TRUE(succeeded);
    }
#endif
}
} // namespace
