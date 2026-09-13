#include "src/bytes/fragmented_buffer_parser.h"
#include "src/bytes/fragmented_buffer_test_support.h"
#include "src/model/batch_builder.h"
#include "src/model/batch_codec.h"
#include "src/model/batch_rewrite.h"
#include "src/model/fingerprint.h"
#include "src/model/record_scan.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/deleter.hh>
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
#include <cstddef>
#include <cstdint>
#include <exception>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
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
using delta = model::range_logical_count;
using nullable = std::optional<fragmented_buffer>;

constexpr std::array times{100, 99, 110, 98, 101};
constexpr std::array survivors{delta{1}, delta{3}};
constexpr auto dense_records = "\x06\x00\x00\x00\x01\x01\x00"
                               "\x06\x00\x01\x01\x01\x01\x00"
                               "\x06\x00\x14\x02\x01\x01\x00"
                               "\x06\x00\x03\x03\x01\x01\x00"
                               "\x06\x00\x02\x04\x01\x01\x00"sv;
constexpr auto sparse_records = "\x06\x00\x01\x01\x01\x01\x00"
                                "\x06\x00\x03\x03\x01\x01\x00"sv;
constexpr auto original_digest
  = "629cb7e9505f05b8ae49fe6ba38ea0ad5146c22f973fc1195b2da4fe00869ce3"sv;
// Independent family-2 bytes: full original identity/digest, original N=5,
// actual retained count/length, and logical [100,105) inside body integrity.
constexpr auto dense_envelope
  = "4b5142460200010001002000db0000000000000000000000c5342bd5689c9ed7"
    "0102030405060708090a0b0c0d0e0f1002000000000000000300000000000000"
    "04000000000000002122232425262728292a2b2c2d2e2f304142434445464748"
    "494a4b4c4d4e4f5007000000000000006162636465666768696a6b6c6d6e6f70"
    "0900000000000000629cb7e9505f05b8ae49fe6ba38ea0ad5146c22f973fc119"
    "5b2da4fe00869ce3640000000000000005000000050000000000000000000100"
    "23000000230000006400000000000000690000000000000006000000010100"
    "06000101010100060014020101000600030301010006000204010100"sv;
constexpr auto sparse_envelope
  = "4b5142460200010001002000c6000000000000000000000051a3849d4be31590"
    "0102030405060708090a0b0c0d0e0f1002000000000000000300000000000000"
    "04000000000000002122232425262728292a2b2c2d2e2f304142434445464748"
    "494a4b4c4d4e4f5007000000000000006162636465666768696a6b6c6d6e6f70"
    "0900000000000000629cb7e9505f05b8ae49fe6ba38ea0ad5146c22f973fc119"
    "5b2da4fe00869ce3640000000000000005000000020000000000000000000100"
    "0e0000000e0000006400000000000000690000000000000006000101010100"
    "06000303010100"sv;

static_assert(std::is_nothrow_move_constructible_v<model::assigned_batch>);
static_assert(std::is_nothrow_destructible_v<model::assigned_batch>);
static_assert(!std::is_default_constructible_v<model::assigned_batch>);
static_assert(!std::is_copy_constructible_v<model::assigned_batch>);
static_assert(!std::is_aggregate_v<model::assigned_batch>);
template<typename T>
concept temporary_records = requires(T&& value) {
    std::forward<T>(value).records();
};
static_assert(!temporary_records<model::assigned_batch>);
static_assert(!temporary_records<model::removed_batch_coverage>);
static_assert(
  std::is_nothrow_copy_constructible_v<model::removed_batch_coverage>);
static_assert(!std::is_default_constructible_v<model::removed_batch_coverage>);
static_assert(!std::is_aggregate_v<model::removed_batch_coverage>);
static_assert(
  !std::is_convertible_v<model::removed_batch_coverage, model::assigned_batch>);

template<typename T>
concept assigned_encoding = requires(T&& value, codec::cooperative_work& work) {
    model::encode_assigned_batch(
      std::forward<T>(value), work, byte_count{}, nullptr);
};
static_assert(!assigned_encoding<model::removed_batch_coverage>);

byte_count charge(byte_count request) noexcept {
    if (request.value() == 0) return {};
    if (request.value() > (std::uint64_t{1} << 62U))
        return byte_count{UINT64_MAX};
    return byte_count{
      2U * std::bit_ceil(std::max(request.value(), std::uint64_t{16}))};
}
codec::decode_budget memory() {
    // Other fixture/selection/native/frame reservations occupy the unclaimed
    // half. These profile bounds do not measure process RSS or hidden owners.
    return {byte_count{32U << 20U}, byte_count{1U << 20U}, charge};
}
template<typename Id>
Id object(std::uint8_t first) {
    std::array<std::uint8_t, 16> raw{};
    for (std::size_t i = 0; i < raw.size(); ++i)
        raw[i] = static_cast<std::uint8_t>(first + i);
    return Id::make(raw).value();
}
model::batch_id identity() {
    return model::batch_id::make(
             object<model::producer_id>(1),
             model::producer_epoch::make(2).value(),
             model::producer_stream_id::make(3).value(),
             model::batch_sequence{4})
      .value();
}
model::producer_stream_binding binding(int changed = -1) {
    return model::producer_stream_binding::make(
             object<model::topic_id>(changed == 0 ? 34 : 33),
             object<model::range_id>(changed == 1 ? 66 : 65),
             model::range_routing_epoch::make(changed == 2 ? 8 : 7).value(),
             object<model::segment_id>(changed == 3 ? 98 : 97),
             model::segment_generation::make(changed == 4 ? 10 : 9).value())
      .value();
}
fragmented_buffer bytes(std::string_view raw) {
    return fragmented_buffer::copy_of(raw).value();
}
fragmented_buffer payload(std::size_t count) {
    std::vector<seastar::temporary_buffer<char>> parts;
    for (std::size_t i = 0; i < count; i += 65536) {
        seastar::temporary_buffer<char> part{
          std::min<std::size_t>(65536, count - i)};
        std::fill_n(part.get_write(), part.size(), 'x');
        parts.push_back(std::move(part));
    }
    return fragmented_buffer::copy_from_fragments(parts).value();
}
model::record record(std::size_t index, bool rich) {
    nullable key, value;
    std::vector<model::record_header> headers;
    if (rich) {
        key.emplace(bytes(std::string(1, static_cast<char>('a' + index))));
        if (index != 1) value.emplace();
        for (std::size_t i = 0; i < index; ++i) {
            headers.push_back(
              model::make_record_header(
                bytes("\xff"sv),
                i == 0 ? std::nullopt : nullable{fragmented_buffer{}})
                .value());
        }
    }
    return model::make_record(
             {}, std::move(key), std::move(value), std::move(headers))
      .value();
}
void append(
  model::batch_builder& builder,
  const model::record& value,
  wall_time time,
  codec::cooperative_work& work) {
    const auto reserved
      = model::reserve_record_input(value, work, memory()).get().value();
    ASSERT_TRUE(builder.add(value, time, work, reserved.operation_remaining)
                  .get()
                  .has_value());
}
model::submitted_batch submission(
  codec::cooperative_work& work, bool rich = false, std::size_t count = 5) {
    auto builder = model::batch_builder::make(
                     identity(), binding(), work.policy(), charge)
                     .value();
    for (std::size_t i = 0; i < count; ++i) {
        auto value = record(i, rich);
        append(
          builder, value, wall_time{i < times.size() ? times[i] : 100}, work);
    }
    return builder.finalize(work, memory().operation_remaining).get().value();
}
model::assigned_batch assigned(
  codec::cooperative_work& work,
  bool rich = false,
  std::size_t count = 5,
  std::uint64_t base = 100) {
    return model::assigned_batch::assign(
             submission(work, rich, count),
             model::range_logical_end{base},
             binding())
      .value();
}
model::record_region_scanner
scan(model::assigned_batch&& batch, codec::cooperative_work& work) {
    const auto context = batch.context();
    const auto headers = batch.header_count();
    return model::record_region_scanner::make(
             std::move(batch).release_records(),
             {context.submitted().original_timestamp_base(),
              context.submitted().original_count(),
              context.retained_count(),
              headers,
              context.retained_count().value()
                  == context.submitted().original_count().value()
                ? model::record_region_kind::dense_original
                : model::record_region_kind::sparse},
             memory(),
             work)
      .get()
      .value();
}
std::string hex_bytes(std::string_view hex) {
    const auto digit = [](char c) { return c >= 'a' ? c - 'a' + 10 : c - '0'; };
    std::string out;
    for (std::size_t i = 0; i < hex.size(); i += 2)
        out.push_back(
          static_cast<char>(digit(hex[i]) * 16 + digit(hex[i + 1])));
    return out;
}
std::string digest_hex(codec::semantic_batch_digest digest) {
    std::string out;
    for (const auto b : digest.bytes()) {
        out += "0123456789abcdef"[b >> 4U];
        out += "0123456789abcdef"[b & 15U];
    }
    return out;
}
std::string wire_bytes(const fragmented_buffer& input) {
    std::string out;
    for (const auto fragment : input)
        out.append(fragment.data(), fragment.size());
    return out;
}
template<typename T>
void error(const codec::result<T>& result, errc code) {
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), code);
}

struct removal_probe {
    bool released{false};
    seastar::abort_source* abort_on_release{nullptr};
};

model::assigned_batch
observed_assignment(removal_probe& probe, codec::cooperative_work& work) {
    auto raw = std::make_unique<std::string>(hex_bytes(sparse_envelope));
    auto* data = raw->data();
    const auto size = raw->size();
    auto deleter = seastar::make_deleter(
      [raw = std::move(raw), &probe] mutable noexcept {
          raw.reset();
          probe.released = true;
          if (probe.abort_on_release) probe.abort_on_release->request_abort();
      });
    auto storage = seastar::temporary_buffer<char>::maybe_unsafe_from_deleter(
      data, size, std::move(deleter));
    kwaque::bytes::fragmented_buffer_parser input{
      kwaque::bytes::fragmented_buffer_test_access::adopt_fragment(
        std::move(storage), byte_count{size})
        .value()};
    auto decoded
      = model::decode_assigned_batch(
          input,
          {binding().topic(), binding().range()},
          codec::reserve_decode_input(input, work.policy(), memory()).value(),
          work)
          .get()
          .value();
    return std::move(decoded.value);
}

TEST(
  AssignedBatchTest,
  AssignmentTransfersExistingBytesAndPreservesOriginalFacts) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto source = submission(work, true);
    const auto context = source.context();
    const auto digest = source.fingerprint();
    const auto count = source.header_count();
    const auto first = (*source.records().begin()).data();
    const auto cost = source.records().allocation_cost(charge).value();
    auto result = model::assigned_batch::assign(
      std::move(source), model::range_logical_end{100}, binding());
    ASSERT_TRUE(result.has_value());
    // NOLINTNEXTLINE(bugprone-use-after-move) -- empty donor contract.
    EXPECT_TRUE(source.records().empty());
    EXPECT_EQ((*result->records().begin()).data(), first);
    EXPECT_EQ(result->records().allocation_cost(charge).value(), cost);
    EXPECT_EQ(result->context().submitted(), context);
    EXPECT_EQ(result->fingerprint(), digest);
    EXPECT_EQ(result->context().retained_count(), item_count{5});
    EXPECT_EQ(result->header_count(), count);
    EXPECT_EQ(result->context().logical_span().begin().value(), 100U);
    EXPECT_EQ(result->context().logical_span().end().value(), 105U);
    EXPECT_EQ(result->context().logical_span().count(), delta{5});
}

TEST(AssignedBatchTest, EveryOriginalBindingMismatchLeavesSubmissionIntact) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto source = submission(work);
    const auto first = (*source.records().begin()).data();
    // Rejected assignment preserves the donor for inspection and another try.
    // NOLINTBEGIN(bugprone-use-after-move)
    for (int changed = 0; changed != 5; ++changed) {
        const auto result = model::assigned_batch::assign(
          std::move(source), model::range_logical_end{100}, binding(changed));
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), errc::invalid_argument);
        EXPECT_EQ((*source.records().begin()).data(), first);
        EXPECT_TRUE(source.records().content_equals(dense_records));
    }
    ASSERT_TRUE(
      model::assigned_batch::assign(
        std::move(source), model::range_logical_end{100}, binding())
        .has_value());
    // NOLINTEND(bugprone-use-after-move)
}

TEST(AssignedBatchTest, ExclusiveEndMaximumIsValidAndOverflowPreservesDonor) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto source = submission(work);
    auto invalid = model::assigned_batch::assign(
      std::move(source), model::range_logical_end{UINT64_MAX - 4U}, binding());
    ASSERT_FALSE(invalid.has_value());
    EXPECT_EQ(invalid.error(), errc::out_of_range);
    // Overflow rejects before transfer; reuse the unchanged donor below.
    // NOLINTBEGIN(bugprone-use-after-move)
    EXPECT_TRUE(source.records().content_equals(dense_records));
    auto result = model::assigned_batch::assign(
                    std::move(source),
                    model::range_logical_end{UINT64_MAX - 5U},
                    binding())
                    .value();
    // NOLINTEND(bugprone-use-after-move)
    EXPECT_EQ(result.context().logical_span().end().value(), UINT64_MAX);
    auto encoded = model::encode_assigned_batch(
                     std::move(result),
                     work,
                     memory().operation_remaining,
                     charge,
                     {.origin = UINT64_MAX - 251U})
                     .get();
    ASSERT_TRUE(encoded.has_value());
    const auto raw = wire_bytes(*encoded);
    EXPECT_EQ(raw.substr(32 + 176, 8), std::string(8, '\xff'));
}

TEST(AssignedBatchTest, AssignmentDoesNotReachAnAllocationPoint) {
#if !defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    GTEST_SKIP() << "ordinary allocation injection is disabled";
#else
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto source = submission(work);
    auto& injector = seastar::memory::local_failure_injector();
    const auto before = injector.alloc_count();
    injector.fail_after(0);
    auto result = model::assigned_batch::assign(
      std::move(source), model::range_logical_end{100}, binding());
    const auto reached = injector.failed();
    const auto after = injector.alloc_count();
    injector.cancel();
    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(reached);
    EXPECT_EQ(after, before);
#endif
}

TEST(AssignedBatchTest, DenseAssignedEnvelopeMatchesIndependentBytes) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto source = assigned(work);
    EXPECT_EQ(digest_hex(source.fingerprint()), original_digest);
    auto encoded
      = model::encode_assigned_batch(
          std::move(source), work, memory().operation_remaining, charge)
          .get();
    ASSERT_TRUE(encoded.has_value());
    // NOLINTNEXTLINE(bugprone-use-after-move) -- empty donor contract.
    EXPECT_TRUE(source.records().empty());
    EXPECT_TRUE(encoded->content_equals(hex_bytes(dense_envelope)));
}

TEST(
  AssignedBatchTest,
  SparseAssignedEnvelopeMatchesIndependentBytesAndOriginalDigest) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto source = assigned(work);
    const auto original = source.context();
    auto result = model::rewrite_assigned_batch(
                    std::move(source), survivors, memory(), work)
                    .get();
    ASSERT_TRUE(result.has_value());
    // NOLINTNEXTLINE(bugprone-use-after-move) -- empty donor contract.
    EXPECT_TRUE(source.records().empty());
    EXPECT_TRUE(result->records().content_equals(sparse_records));
    EXPECT_EQ(result->context().submitted(), original.submitted());
    EXPECT_EQ(result->context().logical_span(), original.logical_span());
    EXPECT_EQ(result->context().retained_count(), item_count{2});
    EXPECT_EQ(digest_hex(result->fingerprint()), original_digest);
    auto encoded
      = model::encode_assigned_batch(
          std::move(*result), work, memory().operation_remaining, charge)
          .get();
    ASSERT_TRUE(encoded.has_value());
    EXPECT_TRUE(encoded->content_equals(hex_bytes(sparse_envelope)));
}

TEST(
  AssignedBatchTest, OriginalCoverageDoesNotDependOnEitherSurvivingEndpoint) {
    const std::array selections{
      std::array{delta{0}, delta{1}},
      std::array{delta{1}, delta{3}},
      std::array{delta{3}, delta{4}}};
    for (const auto& selected : selections) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto source = assigned(work);
        auto result = model::rewrite_assigned_batch(
                        std::move(source), selected, memory(), work)
                        .get()
                        .value();
        const auto context = result.context();
        EXPECT_EQ(context.logical_span().begin().value(), 100U);
        EXPECT_EQ(context.logical_span().end().value(), 105U);
        auto scanner = scan(std::move(result), work);
        for (auto delta : selected) {
            ASSERT_TRUE(scanner.next(work).get().value());
            EXPECT_EQ(scanner.current()->fields.logical_delta, delta);
            EXPECT_EQ(
              model::checked_timestamp_from_delta(
                context.submitted().original_timestamp_base(),
                scanner.current()->fields.timestamp_delta),
              wall_time{times[delta.value()]});
            const auto offset = model::range_logical_offset::make(
                                  context.logical_span().begin().value()
                                  + delta.value())
                                  .value();
            auto id = model::record_id::make(
                        binding().topic(), binding().range(), offset)
                        .value();
            EXPECT_EQ(id.offset().value(), 100U + delta.value());
        }
        EXPECT_TRUE(scanner.complete());
    }
}

TEST(
  AssignedBatchTest, CompleteCanonicalFieldsAndDuplicateHeadersSurviveRewrite) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto source = assigned(work, true);
    const auto digest = source.fingerprint();
    auto result = model::rewrite_assigned_batch(
                    std::move(source), survivors, memory(), work)
                    .get()
                    .value();
    EXPECT_EQ(result.header_count(), item_count{4});
    EXPECT_EQ(result.fingerprint(), digest);
    auto scanner = scan(std::move(result), work);
    for (auto delta : survivors) {
        ASSERT_TRUE(scanner.next(work).get().value());
        auto decoded = scanner.materialize_current(scanner.remaining(), work)
                         .get()
                         .value();
        EXPECT_TRUE(decoded.value.key()->content_equals(
          std::string(1, static_cast<char>('a' + delta.value()))));
        EXPECT_EQ(decoded.value.value().has_value(), delta.value() != 1);
        EXPECT_EQ(decoded.value.headers().size(), delta.value());
        for (std::size_t i = 0; i < decoded.value.headers().size(); ++i) {
            const auto& header = decoded.value.headers()[i];
            EXPECT_TRUE(header.name().content_equals("\xff"sv));
            EXPECT_EQ(header.value().has_value(), i != 0);
            if (header.value()) EXPECT_TRUE(header.value()->empty());
        }
    }
    EXPECT_TRUE(scanner.complete());
}

TEST(AssignedBatchTest, RepeatedRewriteCannotResurrectMissingOriginalSlots) {
    for (const auto selected :
         {delta{0}, delta{1}, delta{2}, delta{3}, delta{4}}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto source = assigned(work);
        auto sparse = model::rewrite_assigned_batch(
                        std::move(source), survivors, memory(), work)
                        .get()
                        .value();
        const auto context = sparse.context();
        const auto digest = sparse.fingerprint();
        const std::array keep{selected};
        auto result = model::rewrite_assigned_batch(
                        std::move(sparse), keep, memory(), work)
                        .get();
        // NOLINTNEXTLINE(bugprone-use-after-move) -- empty donor contract.
        EXPECT_TRUE(sparse.records().empty());
        if (selected == delta{1} || selected == delta{3}) {
            ASSERT_TRUE(result.has_value());
            EXPECT_EQ(result->context().retained_count(), item_count{1});
            EXPECT_EQ(result->context().logical_span(), context.logical_span());
            EXPECT_EQ(result->fingerprint(), digest);
            const auto expected = sparse_records.substr(
              selected == delta{1} ? 0 : 7, 7);
            EXPECT_TRUE(result->records().content_equals(expected));
        } else
            error(result, errc::invalid_argument);
    }
}

TEST(
  AssignedBatchTest,
  InvalidSelectionsConsumeInputWithoutPublishingPrefixOrEmptyData) {
    const std::array<std::vector<delta>, 6> selections{
      {{},
       {delta{1}, delta{1}},
       {delta{3}, delta{1}},
       {delta{5}},
       {delta{UINT64_MAX}},
       {delta{0}, delta{1}, delta{2}, delta{3}, delta{4}, delta{5}}}};
    for (const auto& selected : selections) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto source = assigned(work);
        error(
          model::rewrite_assigned_batch(
            std::move(source), selected, memory(), work)
            .get(),
          errc::invalid_argument);
        // NOLINTNEXTLINE(bugprone-use-after-move) -- empty donor contract.
        EXPECT_TRUE(source.records().empty());
    }
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto source = assigned(work);
    const std::array three{delta{1}, delta{2}, delta{3}};
    auto sparse = model::rewrite_assigned_batch(
                    std::move(source), three, memory(), work)
                    .get()
                    .value();
    const std::array missing_after_match{delta{1}, delta{4}};
    error(
      model::rewrite_assigned_batch(
        std::move(sparse), missing_after_match, memory(), work)
        .get(),
      errc::invalid_argument);
}

TEST(AssignedBatchTest, RemovingAllRecordsPreservesOriginalCoverageAndDigest) {
    for (const bool sparse : {false, true}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto source = assigned(work, false, 5, UINT64_MAX - 5U);
        const auto original = source.context();
        if (sparse) {
            source = model::rewrite_assigned_batch(
                       std::move(source), survivors, memory(), work)
                       .get()
                       .value();
        }
        auto removed = model::remove_all_records(std::move(source), work).get();
        ASSERT_TRUE(removed.has_value());
        EXPECT_EQ(removed->submitted(), original.submitted());
        EXPECT_EQ(removed->logical_span(), original.logical_span());
        EXPECT_EQ(removed->logical_span().end().value(), UINT64_MAX);
        EXPECT_EQ(removed->submitted().original_count(), delta{5});
        EXPECT_EQ(digest_hex(removed->fingerprint()), original_digest);
    }
}

TEST(AssignedBatchTest, RemovingAllRecordsRejectsAnAlreadyConsumedOwner) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto source = assigned(work);
    auto extracted = std::move(source).release_records();
    // Deliberately pass the emptied donor to verify its rejection contract.
    // NOLINTBEGIN(bugprone-use-after-move)
    error(
      model::remove_all_records(std::move(source), work).get(),
      errc::invalid_argument);
    // NOLINTEND(bugprone-use-after-move)
    EXPECT_TRUE(extracted.content_equals(dense_records));
}

TEST(AssignedBatchTest, RemovalJoinsBackingReleaseBeforePublishingCoverage) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    removal_probe probe;
    auto source = observed_assignment(probe, work);
    ASSERT_FALSE(probe.released);
    auto removed = model::remove_all_records(std::move(source), work).get();
    ASSERT_TRUE(removed.has_value());
    EXPECT_TRUE(probe.released);
    EXPECT_EQ(digest_hex(removed->fingerprint()), original_digest);
}

TEST(AssignedBatchTest, InitialAndCleanupAbortSuppressRemovedCoverage) {
    for (const bool late : {false, true}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        removal_probe probe;
        auto source = observed_assignment(probe, work);
        if (late)
            probe.abort_on_release = &abort;
        else
            abort.request_abort();
        error(
          model::remove_all_records(std::move(source), work).get(),
          errc::aborted);
        EXPECT_TRUE(probe.released);
    }
}

TEST(AssignedBatchTest, PendingRemovalOwnsBytesAndJoinsQueuedCancellation) {
    for (const bool cancel : {false, true}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        removal_probe probe;
        std::optional<model::assigned_batch> source;
        source.emplace(observed_assignment(probe, work));
        work.admit(work.byte_quantum(), work.item_quantum()).get().value();
        const auto deadline = std::chrono::steady_clock::now()
                              + std::chrono::seconds{2};
        while (!seastar::need_preempt()
               && std::chrono::steady_clock::now() < deadline) {
        }
        ASSERT_TRUE(seastar::need_preempt());
        auto observer = seastar::yield().then([&] {
            if (cancel) abort.request_abort();
        });
        bool pending = false, retained = false;
        std::exception_ptr exception;
        std::optional<codec::result<model::removed_batch_coverage>> result;
        try {
            auto removed = model::remove_all_records(std::move(*source), work);
            pending = !removed.available();
            source.reset();
            retained = !probe.released;
            result.emplace(removed.get());
        } catch (...) {
            exception = std::current_exception();
        }
        observer.get();
        if (exception) std::rethrow_exception(exception);
        EXPECT_TRUE(pending);
        EXPECT_TRUE(retained);
        EXPECT_TRUE(probe.released);
        ASSERT_TRUE(result.has_value());
        if (cancel)
            error(*result, errc::aborted);
        else {
            ASSERT_TRUE(result->has_value());
            EXPECT_EQ(digest_hex((*result)->fingerprint()), original_digest);
        }
    }
}

TEST(AssignedBatchTest, SelectionOfAllCurrentSurvivorsPreservesExactBytes) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const std::array all{delta{0}, delta{1}, delta{2}, delta{3}, delta{4}};
    auto source = assigned(work);
    const auto first = (*source.records().begin()).data();
    const auto context = source.context();
    const auto digest = source.fingerprint();
    auto dense = model::rewrite_assigned_batch(
                   std::move(source), all, memory(), work)
                   .get()
                   .value();
    EXPECT_EQ((*dense.records().begin()).data(), first);
    EXPECT_EQ(dense.context(), context);
    EXPECT_EQ(dense.fingerprint(), digest);
    EXPECT_TRUE(dense.records().content_equals(dense_records));
    auto sparse = model::rewrite_assigned_batch(
                    std::move(dense), survivors, memory(), work)
                    .get()
                    .value();
    const auto sparse_first = (*sparse.records().begin()).data();
    auto same = model::rewrite_assigned_batch(
                  std::move(sparse), survivors, memory(), work)
                  .get()
                  .value();
    EXPECT_EQ((*same.records().begin()).data(), sparse_first);
    EXPECT_TRUE(same.records().content_equals(sparse_records));
    EXPECT_EQ(same.fingerprint(), digest);
    const std::array same_count_missing_member{delta{1}, delta{2}};
    error(
      model::rewrite_assigned_batch(
        std::move(same), same_count_missing_member, memory(), work)
        .get(),
      errc::invalid_argument);
}

TEST(AssignedBatchTest, CompletedOutputDoesNotRetainDiscardedLargeBacking) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto builder = model::batch_builder::make(
                     identity(), binding(), work.policy(), charge)
                     .value();
    auto large = model::make_record(
                   {}, std::nullopt, nullable{payload(1048565)}, {})
                   .value();
    auto small = record(0, false);
    append(builder, large, wall_time{100}, work);
    append(builder, small, wall_time{99}, work);
    auto source
      = model::assigned_batch::assign(
          builder.finalize(work, memory().operation_remaining).get().value(),
          model::range_logical_end{100},
          binding())
          .value();
    const auto old_retained = source.records().retained_bytes();
    const std::array keep{delta{1}};
    auto result = model::rewrite_assigned_batch(
                    std::move(source), keep, memory(), work)
                    .get()
                    .value();
    EXPECT_EQ(result.records().size(), byte_count{7});
    EXPECT_LT(result.records().retained_bytes(), old_retained);
    EXPECT_EQ(result.context().logical_span().end().value(), 102U);
    EXPECT_TRUE(result.records().content_equals(sparse_records.substr(0, 7)));
}

TEST(
  AssignedBatchTest,
  MaximumExpandedRegionCanRetainEveryRecordWithinOneOperationBudget) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto builder = model::batch_builder::make(
                     identity(), binding(), work.policy(), charge)
                     .value();
    auto large = model::make_record(
                   {}, std::nullopt, nullable{payload(1048565)}, {})
                   .value();
    for (int i = 0; i < 8; ++i)
        append(builder, large, wall_time{100}, work);
    auto source
      = model::assigned_batch::assign(
          builder.finalize(work, memory().operation_remaining).get().value(),
          model::range_logical_end{100},
          binding())
          .value();
    const auto first = (*source.records().begin()).data();
    const auto context = source.context();
    const auto digest = source.fingerprint();
    const std::array all{
      delta{0},
      delta{1},
      delta{2},
      delta{3},
      delta{4},
      delta{5},
      delta{6},
      delta{7}};
    auto budget = memory();
    // Keeping all survivors reserves one maximum backing plus bounded scan
    // descriptors, without reserving a second maximum output region.
    auto result = model::rewrite_assigned_batch(
                    std::move(source), all, budget, work)
                    .get();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ((*result->records().begin()).data(), first);
    EXPECT_EQ(result->records().size(), byte_count{8U << 20U});
    EXPECT_EQ(result->context(), context);
    EXPECT_EQ(result->fingerprint(), digest);
    EXPECT_LE(
      result->records().allocation_cost(charge)->largest_allocation,
      work.policy().config().max_allocation_bytes);
    auto scanner = scan(std::move(*result), work);
    for (int i = 0; i < 8; ++i)
        ASSERT_TRUE(scanner.next(work).get().value());
    EXPECT_TRUE(scanner.complete());
    scanner.close(work).get();
}

TEST(AssignedBatchTest, MaximumAssignedEncodingIncludesItsWholeFixedBody) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto builder = model::batch_builder::make(
                     identity(), binding(), work.policy(), charge)
                     .value();
    auto large = model::make_record(
                   {}, std::nullopt, nullable{payload(1048565)}, {})
                   .value();
    append(builder, large, wall_time{100}, work);
    auto source
      = model::assigned_batch::assign(
          builder.finalize(work, memory().operation_remaining).get().value(),
          model::range_logical_end{100},
          binding())
          .value();
    auto encoded
      = model::encode_assigned_batch(
          std::move(source), work, memory().operation_remaining, charge)
          .get();
    ASSERT_TRUE(encoded.has_value());
    EXPECT_EQ(encoded->size(), byte_count{1048792});
    EXPECT_LE(
      encoded->allocation_cost(charge)->largest_allocation,
      work.policy().config().max_allocation_bytes);
    kwaque::bytes::fragmented_buffer_parser parser{std::move(*encoded)};
    parser.skip(byte_count{12}).value();
    EXPECT_EQ(parser.read_le<std::uint32_t>().value(), 1048760U);
    parser.skip(byte_count{32U + 168U - 16U}).value();
    EXPECT_EQ(parser.read_le<std::uint64_t>().value(), 100U);
    EXPECT_EQ(parser.read_le<std::uint64_t>().value(), 101U);
    EXPECT_EQ(parser.bytes_remaining(), byte_count{1048576});
}

TEST(AssignedBatchTest, LiveOutputReservationIsCarriedIntoLaterScanChildren) {
    for (const bool short_budget : {false, true}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto source = assigned(work);
        const auto input = source.records().allocation_cost(charge).value();
        const auto alias = source.records()
                             .slice_allocation_cost(
                               byte_count{}, source.records().size(), charge)
                             .value()
                             .descriptors;
        const auto child = source.records()
                             .slice_allocation_cost(
                               byte_count{8}, byte_count{6}, charge)
                             .value()
                             .descriptors;
        // One output tail and one reserved descriptor array remain live
        // during later scans. That same array transfers into publication.
        const auto staging
          = charge(byte_count{65536}).value()
            + charge(
                byte_count{
                  128U * fragmented_buffer::fragment_descriptor_size()})
                .value();
        auto budget = memory();
        budget.operation_remaining = byte_count{
          input.backing.value() + input.descriptors.value()
          + input.share_controls.value() + alias.value() + staging
          + child.value() - (short_budget ? 1U : 0U)};
        const std::array keep{delta{0}};
        auto result = model::rewrite_assigned_batch(
                        std::move(source), keep, budget, work, {.origin = 1000})
                        .get();
        if (short_budget) {
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(result.error().code(), errc::resource_exhausted);
            EXPECT_EQ(result.error().byte_offset(), 1008U);
        } else {
            ASSERT_TRUE(result.has_value());
            EXPECT_TRUE(
              result->records().content_equals(dense_records.substr(0, 7)));
        }
    }
}

TEST(
  AssignedBatchTest, MetadataAndCallerCoordinateFailuresConsumeOnlyTheDonor) {
    for (int mode = 0; mode < 5; ++mode) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto source = assigned(work);
        auto budget = memory();
        codec::field_context context;
        if (mode == 0) budget.metadata_remaining = byte_count{};
        if (mode == 1) budget.operation_remaining = byte_count{};
        if (mode == 2) budget.charge = nullptr;
        if (mode == 3) context.origin = UINT64_MAX - 34U;
        if (mode == 4) static_cast<void>(std::move(source).release_records());
        // Mode 4 deliberately checks rejection after prior extraction.
        // NOLINTBEGIN(bugprone-use-after-move)
        error(
          model::rewrite_assigned_batch(
            std::move(source), survivors, budget, work, context)
            .get(),
          mode < 2 ? errc::resource_exhausted : errc::invalid_argument);
        // NOLINTEND(bugprone-use-after-move)
        // NOLINTNEXTLINE(bugprone-use-after-move) -- empty donor contract.
        EXPECT_TRUE(source.records().empty());
    }
}

TEST(
  AssignedBatchTest,
  EncodedLogicalSpanChangesBothChecksumsAndKeepsSemanticDigest) {
    std::optional<std::string> original;
    for (const std::uint64_t base : {100U, 101U}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto source = assigned(work, false, 5, base);
        EXPECT_EQ(digest_hex(source.fingerprint()), original_digest);
        const auto encoded
          = model::encode_assigned_batch(
              std::move(source), work, memory().operation_remaining, charge)
              .get()
              .value();
        const auto raw = wire_bytes(encoded);
        if (!original)
            original = raw;
        else {
            EXPECT_NE(raw.substr(24, 4), original->substr(24, 4));
            EXPECT_NE(raw.substr(28, 4), original->substr(28, 4));
            EXPECT_EQ(raw.substr(32, 168), original->substr(32, 168));
            EXPECT_EQ(raw.substr(216), original->substr(216));
        }
    }
}

TEST(AssignedBatchTest, AssignedEncoderRejectsBodyBudgetAndOriginOverflow) {
    for (int mode = 0; mode != 4; ++mode) {
        seastar::abort_source abort;
        codec::cooperative_work build_work{codec::limits::defaults(), abort};
        auto source = assigned(build_work);
        codec::limits_config config;
        if (mode == 0) config.max_encoded_body_bytes = byte_count{218};
        codec::cooperative_work work{
          codec::limits::make(config).value(), abort};
        auto result = model::encode_assigned_batch(
                        std::move(source),
                        work,
                        mode == 1 ? byte_count{} : memory().operation_remaining,
                        mode == 2 ? nullptr : charge,
                        {.origin = mode == 3 ? UINT64_MAX - 250U : 0U})
                        .get();
        error(
          result, mode < 2 ? errc::resource_exhausted : errc::invalid_argument);
        // NOLINTNEXTLINE(bugprone-use-after-move) -- empty donor contract.
        EXPECT_TRUE(source.records().empty());
    }
}

TEST(
  AssignedBatchTest, MaximumRecordCopiesAcrossManyFragmentsWithoutReencoding) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto builder = model::batch_builder::make(
                     identity(), binding(), work.policy(), charge)
                     .value();
    auto large = model::make_record(
                   {}, std::nullopt, nullable{payload(1048565)}, {})
                   .value();
    append(builder, large, wall_time{100}, work);
    append(builder, large, wall_time{99}, work);
    auto source
      = model::assigned_batch::assign(
          builder.finalize(work, memory().operation_remaining).get().value(),
          model::range_logical_end{100},
          binding())
          .value();
    const auto original = source.fingerprint();
    const std::array keep{delta{1}};
    auto result = model::rewrite_assigned_batch(
                    std::move(source), keep, memory(), work)
                    .get()
                    .value();
    EXPECT_EQ(result.records().size(), byte_count{1048576});
    EXPECT_EQ(result.fingerprint(), original);
    auto scanner = scan(std::move(result), work);
    ASSERT_TRUE(scanner.next(work).get().value());
    EXPECT_EQ(scanner.current()->fields.logical_delta, delta{1});
    EXPECT_EQ(scanner.current()->fields.timestamp_delta, -1);
    EXPECT_EQ(scanner.current()->value->size(), byte_count{1048565});
    EXPECT_TRUE(scanner.complete());
    scanner.close(work).get();
}

TEST(
  AssignedBatchTest, ThousandsOfSurvivorsUseBoundedRawStorageAndRetainHoles) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto source = assigned(work, false, 4096);
    const auto original = source.context();
    std::vector<delta> selected;
    selected.reserve(2048);
    for (std::uint64_t i = 1; i < 4096; i += 2)
        selected.push_back(delta{i});
    auto result = model::rewrite_assigned_batch(
                    std::move(source), selected, memory(), work)
                    .get()
                    .value();
    EXPECT_EQ(result.context().logical_span(), original.logical_span());
    EXPECT_EQ(result.context().retained_count(), item_count{2048});
    EXPECT_LE(result.records().fragment_count(), 2U);
    const auto cost = result.records().allocation_cost(charge).value();
    EXPECT_LT(cost.descriptors.value() + cost.share_controls.value(), 65536U);
    auto scanner = scan(std::move(result), work);
    for (auto delta : selected) {
        ASSERT_TRUE(scanner.next(work).get().value());
        EXPECT_EQ(scanner.current()->fields.logical_delta, delta);
    }
    EXPECT_TRUE(scanner.complete());
    scanner.close(work).get();
}

thread_local seastar::abort_source* cancel_source = nullptr;
thread_local std::uint64_t calls = 0, cancel_at = 0;
byte_count observed_charge(byte_count amount) noexcept {
    if (++calls == cancel_at && cancel_source != nullptr)
        cancel_source->request_abort();
    return charge(amount);
}

TEST(
  AssignedBatchTest,
  InitialAndLateAdmissionAbortNeverPublishPartialRewriteOrEncoding) {
    for (const bool encoding : {false, true}) {
        std::uint64_t last = 0;
        for (const bool cancel : {false, true}) {
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            auto source = assigned(work, true);
            calls = 0;
            cancel_at = cancel ? last : 0;
            cancel_source = &abort;
            auto reset = seastar::defer(
              [] noexcept { cancel_source = nullptr; });
            if (encoding) {
                auto result = model::encode_assigned_batch(
                                std::move(source),
                                work,
                                memory().operation_remaining,
                                observed_charge)
                                .get();
                if (cancel)
                    error(result, errc::aborted);
                else
                    ASSERT_TRUE(result.has_value());
            } else {
                auto budget = memory();
                budget.charge = observed_charge;
                auto result = model::rewrite_assigned_batch(
                                std::move(source), survivors, budget, work)
                                .get();
                if (cancel)
                    error(result, errc::aborted);
                else
                    ASSERT_TRUE(result.has_value());
            }
            if (!cancel) {
                last = calls;
                EXPECT_GT(last, 0U);
            }
            // NOLINTNEXTLINE(bugprone-use-after-move) -- empty donor contract.
            EXPECT_TRUE(source.records().empty());
        }
    }
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto source = assigned(work);
    abort.request_abort();
    error(
      model::rewrite_assigned_batch(
        std::move(source), survivors, memory(), work)
        .get(),
      errc::aborted);
    // NOLINTNEXTLINE(bugprone-use-after-move) -- empty donor contract.
    EXPECT_TRUE(source.records().empty());
}

TEST(AssignedBatchTest, PendingRewriteOwnsBytesAfterDonorObjectDestruction) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto source = assigned(work, false, 4096);
    const std::array keep{delta{4095}};
    const auto deadline = std::chrono::steady_clock::now()
                          + std::chrono::seconds{2};
    while (!seastar::need_preempt()
           && std::chrono::steady_clock::now() < deadline) {
    }
    ASSERT_TRUE(seastar::need_preempt());
    auto result = [&] {
        auto donor = std::move(source);
        return model::rewrite_assigned_batch(
          std::move(donor), keep, memory(), work);
    }();
    const bool pending = !result.available();
    auto completed = result.get();
    EXPECT_TRUE(pending);
    ASSERT_TRUE(completed.has_value());
    EXPECT_EQ(completed->context().retained_count(), item_count{1});
    auto scanner = scan(std::move(*completed), work);
    ASSERT_TRUE(scanner.next(work).get().value());
    EXPECT_EQ(scanner.current()->fields.logical_delta, delta{4095});
    EXPECT_TRUE(scanner.complete());
    scanner.close(work).get();
}

TEST(AssignedBatchTest, QueuedAbortInterruptsPendingRewriteAndJoinsCleanup) {
    seastar::abort_source build_abort;
    codec::cooperative_work build_work{codec::limits::defaults(), build_abort};
    auto source = assigned(build_work, false, 4096);
    const std::array keep{delta{4095}};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
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
    std::exception_ptr exception;
    std::optional<codec::result<model::assigned_batch>> result;
    try {
        auto rewritten = model::rewrite_assigned_batch(
          std::move(source), keep, memory(), work);
        pending = !rewritten.available();
        result.emplace(rewritten.get());
    } catch (...) {
        exception = std::current_exception();
    }
    const auto during = observed;
    observer.get();
    if (exception) std::rethrow_exception(exception);
    EXPECT_TRUE(pending);
    EXPECT_TRUE(during);
    ASSERT_TRUE(result.has_value());
    error(*result, errc::aborted);
    // NOLINTNEXTLINE(bugprone-use-after-move) -- empty donor contract.
    EXPECT_TRUE(source.records().empty());
}

TEST(
  AssignedBatchTest, ReachedAllocationFailuresDiscardRewriteAndEncodingOutput) {
#if !defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    GTEST_SKIP() << "ordinary allocation injection is disabled";
#else
    static_cast<void>(kwaque::error_category());
    for (const bool encoding : {false, true}) {
        bool failed_once = false, succeeded = false;
        for (std::uint64_t ordinal = 0; ordinal < 256 && !succeeded;
             ++ordinal) {
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            auto source = assigned(work, true);
            auto& injector = seastar::memory::local_failure_injector();
            bool threw = false, reached = false;
            injector.fail_after(ordinal);
            try {
                if (encoding) {
                    auto result = model::encode_assigned_batch(
                                    std::move(source),
                                    work,
                                    memory().operation_remaining,
                                    charge)
                                    .get();
                    reached = injector.failed();
                    injector.cancel();
                    ASSERT_TRUE(result.has_value());
                } else {
                    auto result
                      = model::rewrite_assigned_batch(
                          std::move(source), survivors, memory(), work)
                          .get();
                    reached = injector.failed();
                    injector.cancel();
                    ASSERT_TRUE(result.has_value());
                    EXPECT_EQ(
                      result->context().retained_count(), item_count{2});
                    EXPECT_EQ(result->header_count(), item_count{4});
                }
                succeeded = true;
            } catch (const std::bad_alloc&) {
                reached = injector.failed();
                threw = true;
            } catch (...) {
                injector.cancel();
                throw;
            }
            injector.cancel();
            if (threw) {
                EXPECT_TRUE(reached);
                failed_once = true;
            } else
                EXPECT_FALSE(reached);
            // NOLINTNEXTLINE(bugprone-use-after-move) -- empty donor contract.
            EXPECT_TRUE(source.records().empty());
        }
        EXPECT_TRUE(failed_once);
        EXPECT_TRUE(succeeded);
    }
#endif
}
} // namespace
