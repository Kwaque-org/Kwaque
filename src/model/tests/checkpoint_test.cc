#include "src/bytes/test_allocation_profile.h"
#include "src/model/checkpoint_codec.h"
#include "src/model/fingerprint.h"
#include "src/model/tests/checkpoint_test_support.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/preempt.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/util/alloc_failure_injector.hh>
#include <seastar/util/later.hh>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <limits>
#include <malloc.h>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace {
namespace model = kwaque::model;
namespace codec = kwaque::codec;
using kwaque::byte_count;
using kwaque::errc;
using kwaque::item_count;
using kwaque::bytes::fragmented_buffer;
using kwaque::bytes::fragmented_buffer_parser;
using kwaque::bytes::testing::charge;
using namespace kwaque::model::testing::checkpoint_fixture;
using cursor_list = seastar::chunked_fifo<model::range_cursor, 16>;
constexpr codec::field_context context{.origin = 71, .family = 10};

constexpr std::string_view golden_hex
  = "4b5142460a0001000100200044000000000000000000000078ce1b7198351fbd"
    "0102030405060708090a0b0c0d0e0f10020000002122232425262728292a2b2c2d2e2f30"
    "08070605040302018182838485868788898a8b8c8d8e8f90ffffffffffffffff";
constexpr std::string_view fingerprint_hex
  = "89085e78bbc8c7f8c80a9ea8ff68c3d74ab2e44d4b7955fdc943866ae8348075";

static_assert(!std::is_default_constructible_v<model::range_cursor>);
static_assert(!std::is_default_constructible_v<model::read_checkpoint>);
static_assert(!std::is_copy_constructible_v<model::read_checkpoint>);
static_assert(std::is_nothrow_move_constructible_v<model::read_checkpoint>);
static_assert(std::is_trivially_destructible_v<model::range_cursor>);
template<typename T>
concept rvalue_cursor_view = requires(T&& value) {
    std::move(value).cursors();
};
static_assert(!rvalue_cursor_view<model::read_checkpoint>);
static_assert(!rvalue_cursor_view<const model::read_checkpoint>);

codec::decode_budget memory() {
    // The unclaimed half reserves native/frame/crypto costs and live fixtures.
    return {byte_count{32U << 20U}, byte_count{1U << 20U}, charge};
}
model::read_checkpoint build(
  std::span<const model::range_cursor> cursors, codec::cooperative_work& work) {
    auto built = model::make_read_checkpoint(
                   topic(), cursors, memory(), work, context)
                   .get();
    return std::move(built.value().value);
}
codec::result<model::decoded_read_checkpoint> decode(
  std::string_view wire,
  codec::cooperative_work& work,
  codec::input_boundary boundary = codec::input_boundary::open,
  std::size_t width = 7,
  model::topic_id expected = topic(),
  codec::decode_budget budget = memory(),
  std::size_t depth = 0) {
    std::string prefixed{"p"};
    prefixed += wire;
    fragmented_buffer_parser input{fragmented(prefixed, width)};
    EXPECT_TRUE(input.skip(byte_count{1}));
    for (std::size_t i = 0; i < depth; ++i)
        EXPECT_TRUE(input.push_checkpoint());
    const auto reserved = codec::reserve_decode_input(
      input, work.policy(), budget, context);
    if (!reserved) return codec::failure(reserved.error());
    auto result = model::decode_read_checkpoint(
                    input, expected, *reserved, work, context, boundary)
                    .get();
    EXPECT_EQ(input.checkpoint_depth(), depth);
    if (result) {
        const auto retained = charge(
          byte_count{
            result->value.cursor_capacity().value()
            * sizeof(model::range_cursor)});
        EXPECT_EQ(
          result->remaining.operation_remaining,
          *reserved->operation_remaining.checked_sub(retained));
        EXPECT_EQ(
          result->remaining.metadata_remaining,
          *reserved->metadata_remaining.checked_sub(retained));
    } else {
        EXPECT_EQ(input.bytes_consumed(), byte_count{1});
        EXPECT_GE(result.error().byte_offset(), context.origin);
        EXPECT_LE(
          result.error().byte_offset(),
          context.origin + input.total_bytes().value());
    }
    return result;
}

TEST(CheckpointTest, CursorUsesCheckedIdentityAndFullLogicalBoundaryDomain) {
    const auto nil = model::range_cursor::make({}, model::range_logical_end{});
    ASSERT_FALSE(nil);
    EXPECT_EQ(nil.error(), errc::invalid_argument);
    for (const auto next : {std::uint64_t{0}, maximum}) {
        const auto cursor = model::range_cursor::make(
          object<model::range_id>(33), model::range_logical_end{next});
        ASSERT_TRUE(cursor);
        EXPECT_EQ(cursor->next().value(), next);
    }
}

TEST(CheckpointTest, SortedOwnerIsIndependentAndRetainsOnlyItsActualCapacity) {
    auto cursors = golden_cursors();
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto result
      = model::make_read_checkpoint(topic(), cursors, memory(), work).get();
    ASSERT_TRUE(result);
    const auto retained = charge(
      byte_count{
        result->value.cursor_capacity().value() * sizeof(model::range_cursor)});
    EXPECT_EQ(
      result->remaining.operation_remaining,
      *memory().operation_remaining.checked_sub(retained));
    EXPECT_EQ(
      result->remaining.metadata_remaining,
      *memory().metadata_remaining.checked_sub(retained));
    cursors[0] = numbered(1);
    EXPECT_EQ(result->value.cursors()[0], golden_cursors()[0]);
    auto second = build(golden_cursors(), work);
    EXPECT_EQ(result->value, second);
    auto moved = std::move(result->value);
    EXPECT_EQ(moved, second);
}

TEST(CheckpointTest, EveryPermutationHasTheSameCanonicalBytesAndFingerprint) {
    const std::array entries{
      numbered(1), golden_cursors()[0], golden_cursors()[1]};
    std::array order{0U, 1U, 2U};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto expected = build(entries, work);
    auto baseline = model::encode_read_checkpoint(
                      expected, work, memory().operation_remaining, charge)
                      .get();
    ASSERT_TRUE(baseline);
    do {
        cursor_list source;
        for (auto i : order)
            source.push_back(entries[i]);
        auto result = model::make_read_checkpoint_from_unordered(
                        topic(), std::move(source), memory(), work)
                        .get();
        ASSERT_TRUE(result);
        EXPECT_EQ(result->value, expected);
        const auto retained = charge(
          byte_count{
            result->value.cursor_capacity().value()
            * sizeof(model::range_cursor)});
        EXPECT_EQ(
          result->remaining.metadata_remaining,
          *memory().metadata_remaining.checked_sub(retained));
        EXPECT_EQ(
          result->remaining.operation_remaining,
          *memory().operation_remaining.checked_sub(retained));
        auto encoded
          = model::encode_read_checkpoint(
              result->value, work, memory().operation_remaining, charge)
              .get();
        ASSERT_TRUE(encoded);
        EXPECT_EQ(flatten(encoded->bytes), flatten(baseline->bytes));
        EXPECT_EQ(encoded->fingerprint, baseline->fingerprint);
    } while (std::next_permutation(order.begin(), order.end()));
}

TEST(CheckpointTest, EmptyNilAndDuplicateInputsRejectWithoutDroppingEntries) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto empty = model::make_read_checkpoint(topic(), {}, memory(), work).get();
    ASSERT_FALSE(empty);
    EXPECT_EQ(empty.error().code(), errc::invalid_argument);
    auto nil
      = model::make_read_checkpoint({}, golden_cursors(), memory(), work).get();
    ASSERT_FALSE(nil);
    EXPECT_EQ(nil.error().code(), errc::invalid_argument);
    for (const auto next : {std::uint64_t{0x0102030405060708}, maximum}) {
        std::array duplicates{
          golden_cursors()[0],
          model::range_cursor::make(
            golden_cursors()[0].range(), model::range_logical_end{next})
            .value()};
        auto sorted = model::make_read_checkpoint(
                        topic(), duplicates, memory(), work)
                        .get();
        ASSERT_FALSE(sorted);
        EXPECT_EQ(sorted.error().code(), errc::malformed_data);
        cursor_list source;
        for (const auto& cursor : duplicates)
            source.push_back(cursor);
        auto unordered = model::make_read_checkpoint_from_unordered(
                           topic(), std::move(source), memory(), work)
                           .get();
        ASSERT_FALSE(unordered);
        EXPECT_EQ(unordered.error().code(), errc::malformed_data);
        // Entered failures consume the donor, including duplicate rejection.
        // NOLINTNEXTLINE(bugprone-use-after-move)
        EXPECT_TRUE(source.empty());
    }
    auto descending = golden_cursors();
    std::reverse(descending.begin(), descending.end());
    auto result
      = model::make_read_checkpoint(topic(), descending, memory(), work).get();
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code(), errc::malformed_data);
}

TEST(CheckpointTest, UnorderedPreflightAndEnteredFailureHaveDistinctOwnership) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    cursor_list reserved;
    reserved.reserve(33);
    reserved.push_back(numbered(1));
    auto rejected = model::make_read_checkpoint_from_unordered(
                      topic(), std::move(reserved), memory(), work)
                      .get();
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error().code(), errc::invalid_argument);
    // Free-chunk rejection precedes ownership transfer.
    // NOLINTNEXTLINE(bugprone-use-after-move)
    EXPECT_EQ(reserved.size(), 1);
    cursor_list source;
    source.push_back(numbered(1));
    abort.request_abort();
    auto canceled = model::make_read_checkpoint_from_unordered(
                      topic(), std::move(source), memory(), work)
                      .get();
    ASSERT_FALSE(canceled);
    EXPECT_EQ(canceled.error().code(), errc::aborted);
    // Abort is observed after the supported donor's ownership transfers.
    // NOLINTNEXTLINE(bugprone-use-after-move)
    EXPECT_TRUE(source.empty());
}

TEST(CheckpointTest, FrozenBodyEnvelopeAndSemanticShaMatchIndependentGoldens) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto checkpoint = build(golden_cursors(), work);
    auto encoded = model::encode_read_checkpoint(
                     checkpoint, work, memory().operation_remaining, charge)
                     .get();
    ASSERT_TRUE(encoded);
    EXPECT_EQ(flatten(encoded->bytes), unhex(golden_hex));
    EXPECT_EQ(digest_bytes(encoded->fingerprint), unhex(fingerprint_hex));
    auto fingerprint
      = model::compute_checkpoint_fingerprint(checkpoint, work).get();
    ASSERT_TRUE(fingerprint);
    EXPECT_EQ(*fingerprint, encoded->fingerprint);
    for (const auto width : {1U, 7U, 67U, 4096U}) {
        auto decoded = decode(
          unhex(golden_hex), work, codec::input_boundary::open, width);
        ASSERT_TRUE(decoded);
        EXPECT_EQ(decoded->value, checkpoint);
        EXPECT_EQ(decoded->fingerprint, encoded->fingerprint);
    }
}

TEST(CheckpointTest, EveryCursorFieldAndTopicParticipateInTheFingerprint) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    for (unsigned mutation = 0; mutation < 5; ++mutation) {
        auto entries = golden_cursors();
        auto scope = topic();
        if (mutation == 0)
            scope = object<model::topic_id>(2);
        else {
            const auto index = (mutation - 1U) / 2U;
            const auto original = entries[index];
            entries[index] = model::range_cursor::make(
                               mutation % 2U == 1U ? object<model::range_id>(
                                                       index == 0 ? 34 : 130)
                                                   : original.range(),
                               mutation % 2U == 0U ? model::range_logical_end{0}
                                                   : original.next())
                               .value();
        }
        auto value
          = model::make_read_checkpoint(scope, entries, memory(), work).get();
        ASSERT_TRUE(value);
        auto digest
          = model::compute_checkpoint_fingerprint(value->value, work).get();
        ASSERT_TRUE(digest);
        EXPECT_NE(digest_bytes(*digest), unhex(fingerprint_hex));
    }
    const std::array initial{numbered(1)};
    auto value = build(initial, work);
    auto encoded = model::encode_read_checkpoint(
                     value, work, memory().operation_remaining, charge)
                     .get();
    ASSERT_TRUE(encoded);
    EXPECT_EQ(encoded->bytes.size(), byte_count{76});
    auto restored = decode(flatten(encoded->bytes), work);
    ASSERT_TRUE(restored);
    EXPECT_EQ(
      restored->value.cursors().front().next(), model::range_logical_end{0});
}

TEST(
  CheckpointTest,
  OptionalHeadersDoNotChangeSemanticFingerprintOrRequireAlignment) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    for (const auto header : {40U, 41U, 4096U}) {
        auto wire = extended(unhex(golden_hex), header);
        auto decoded = decode(wire, work, codec::input_boundary::open, 67);
        ASSERT_TRUE(decoded);
        EXPECT_EQ(digest_bytes(decoded->fingerprint), unhex(fingerprint_hex));
        auto encoded
          = model::encode_read_checkpoint(
              decoded->value, work, memory().operation_remaining, charge)
              .get();
        ASSERT_TRUE(encoded);
        EXPECT_EQ(flatten(encoded->bytes), unhex(golden_hex));
        auto config = codec::limits::defaults().config();
        config.max_checkpoint_bytes = byte_count{wire.size() - 1U};
        codec::cooperative_work narrow{
          codec::limits::make(config).value(), abort};
        auto rejected = decode(wire, narrow, codec::input_boundary::open, 67);
        ASSERT_FALSE(rejected);
        EXPECT_EQ(rejected.error().code(), errc::resource_exhausted);
    }
}

TEST(CheckpointTest, StructuralFailuresAreTransactionalAndNeverCanonicalized) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    for (unsigned mutation = 0; mutation < 7; ++mutation) {
        SCOPED_TRACE(mutation);
        auto wire = unhex(golden_hex);
        switch (mutation) {
        case 0:
            std::fill_n(wire.begin() + 32, 16, '\0');
            break;
        case 1:
            std::fill_n(wire.begin() + 52, 16, '\0');
            break;
        case 2:
            wire.replace(76, 16, wire.substr(52, 16));
            break;
        case 3:
            std::swap_ranges(
              wire.begin() + 52, wire.begin() + 76, wire.begin() + 76);
            break;
        case 4:
            put(wire, 48, 0, 4);
            break;
        case 5:
            put(wire, 48, 1, 4);
            break;
        case 6:
            put(wire, 48, 4097, 4);
            break;
        default:
            break;
        }
        repair(wire);
        auto result = decode(wire, work);
        ASSERT_FALSE(result);
        EXPECT_EQ(
          result.error().code(),
          mutation == 6 ? errc::resource_exhausted : errc::malformed_data);
    }
    auto wrong = decode(
      unhex(golden_hex),
      work,
      codec::input_boundary::open,
      7,
      object<model::topic_id>(2));
    ASSERT_FALSE(wrong);
    EXPECT_EQ(wrong.error().code(), errc::wrong_context);
    auto nil = decode(
      unhex(golden_hex), work, codec::input_boundary::open, 7, {});
    ASSERT_FALSE(nil);
    EXPECT_EQ(nil.error().code(), errc::invalid_argument);
}

TEST(
  CheckpointTest,
  CompleteAndOpenTruncationVersionsAndIntegrityKeepEnvelopeRules) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto wire = unhex(golden_hex);
    for (std::size_t length = 0; length < wire.size(); ++length) {
        SCOPED_TRACE(length);
        for (const auto boundary :
             {codec::input_boundary::open, codec::input_boundary::complete}) {
            auto result = decode(
              std::string_view{wire}.substr(0, length), work, boundary);
            ASSERT_FALSE(result);
            EXPECT_EQ(
              result.error().code(),
              boundary == codec::input_boundary::open ? errc::truncated_data
                                                      : errc::malformed_data);
        }
    }
    for (unsigned mutation = 0; mutation < 4; ++mutation) {
        auto altered = wire;
        if (mutation == 0) {
            put(altered, 6, 2, 2);
            put(altered, 8, 2, 2);
        }
        if (mutation == 1) put(altered, 16, 1, 8);
        if (mutation == 2) {
            put(altered, 6, 1, 2);
            put(altered, 8, 2, 2);
        }
        repair(altered);
        if (mutation == 3) altered.back() ^= 1;
        auto result = decode(altered, work);
        ASSERT_FALSE(result);
        EXPECT_EQ(
          result.error().code(),
          mutation < 2    ? errc::unsupported_format
          : mutation == 2 ? errc::malformed_data
                          : errc::corrupt_data);
    }
}

TEST(
  CheckpointTest, ConcatenatedEnvelopesConsumeExactlyOneAndRespectParentMarks) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto wire = unhex(golden_hex);
    for (std::size_t depth = 0; depth <= 8; ++depth) {
        fragmented_buffer_parser input{fragmented(wire + wire)};
        for (std::size_t i = 0; i < depth; ++i)
            ASSERT_TRUE(input.push_checkpoint());
        auto reserved = codec::reserve_decode_input(
          input, work.policy(), memory(), context);
        ASSERT_TRUE(reserved);
        auto result = model::decode_read_checkpoint(
                        input, topic(), *reserved, work, context)
                        .get();
        EXPECT_EQ(input.checkpoint_depth(), depth);
        if (depth == 8) {
            ASSERT_FALSE(result);
            EXPECT_EQ(result.error().code(), errc::resource_exhausted);
            EXPECT_EQ(input.bytes_consumed(), byte_count{});
        } else {
            ASSERT_TRUE(result);
            EXPECT_EQ(input.bytes_consumed(), byte_count{wire.size()});
        }
    }
}

TEST(
  CheckpointTest,
  CountByteAndMemoryCeilingsApplyToBothConstructionPathsAndWire) {
    for (unsigned denied = 0; denied < 6; ++denied) {
        auto config = codec::limits::defaults().config();
        auto budget = memory();
        if (denied == 0) config.max_checkpoint_cursors = item_count{1};
        if (denied == 1) config.max_object_entries = item_count{1};
        if (denied == 2) config.max_checkpoint_bytes = byte_count{99};
        if (denied == 3) budget.operation_remaining = byte_count{};
        if (denied == 4) budget.metadata_remaining = byte_count{};
        if (denied == 5) config.max_allocation_bytes = byte_count{1};
        seastar::abort_source abort;
        codec::cooperative_work work{
          codec::limits::make(config).value(), abort};
        auto result = model::make_read_checkpoint(
                        topic(), golden_cursors(), budget, work)
                        .get();
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error().code(), errc::resource_exhausted);
        cursor_list source;
        for (const auto& cursor : golden_cursors())
            source.push_back(cursor);
        auto unordered = model::make_read_checkpoint_from_unordered(
                           topic(), std::move(source), budget, work)
                           .get();
        ASSERT_FALSE(unordered);
        EXPECT_EQ(unordered.error().code(), errc::resource_exhausted);
        auto decoded = decode(
          unhex(golden_hex),
          work,
          codec::input_boundary::open,
          7,
          topic(),
          budget);
        ASSERT_FALSE(decoded);
        EXPECT_EQ(decoded.error().code(), errc::resource_exhausted);
    }
}

TEST(
  CheckpointTest, MaximumCursorVectorFitsActualServedAllocationAndRoundTrips) {
    std::vector<model::range_cursor> cursors;
    cursors.reserve(4096);
    for (std::uint32_t i = 1; i <= 4096; ++i)
        cursors.push_back(numbered(i));
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto built
      = model::make_read_checkpoint(topic(), cursors, memory(), work).get();
    ASSERT_TRUE(built);
    EXPECT_EQ(built->value.cursor_capacity(), item_count{4096});
    const auto reserved = charge(
      byte_count{
        built->value.cursor_capacity().value() * sizeof(model::range_cursor)});
    const auto served = malloc_usable_size(
      const_cast<model::range_cursor*>(built->value.cursors().data()));
    EXPECT_LE(served, reserved.value());
    EXPECT_LE(reserved.value(), 128U * 1024U);
    auto encoded = model::encode_read_checkpoint(
                     built->value, work, memory().operation_remaining, charge)
                     .get();
    ASSERT_TRUE(encoded);
    EXPECT_EQ(encoded->bytes.size(), byte_count{98356});
    auto decoded = decode(
      flatten(encoded->bytes), work, codec::input_boundary::open, 4096);
    ASSERT_TRUE(decoded);
    EXPECT_EQ(decoded->value, built->value);
    EXPECT_EQ(decoded->fingerprint, encoded->fingerprint);
    cursor_list source;
    for (std::uint32_t i = 4096; i != 0; --i)
        source.push_back(numbered(i));
    auto unordered = model::make_read_checkpoint_from_unordered(
                       topic(), std::move(source), memory(), work)
                       .get();
    ASSERT_TRUE(unordered);
    EXPECT_EQ(unordered->value, built->value);
    // Avoid growing a 4096-element vector past the allocation ceiling.
    cursor_list excessive;
    for (std::uint32_t i = 1; i <= 4097; ++i)
        excessive.push_back(numbered(i));
    auto rejected = model::make_read_checkpoint_from_unordered(
                      topic(), std::move(excessive), memory(), work)
                      .get();
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error().code(), errc::resource_exhausted);
}

TEST(
  CheckpointTest,
  AllocationFailuresPreserveOrConsumeDonorAtDocumentedBoundary) {
#if !defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    GTEST_SKIP() << "allocation injection is disabled in this verified profile";
#else
    bool succeeded = false;
    bool observed_owned_failure = false;
    for (std::uint64_t ordinal = 0; ordinal < 256 && !succeeded; ++ordinal) {
        SCOPED_TRACE(ordinal);
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        cursor_list source;
        for (std::uint32_t i = 65; i != 0; --i)
            source.push_back(numbered(i));
        std::optional<codec::result<model::constructed_read_checkpoint>> result;
        std::exception_ptr exception;
        auto& injector = seastar::memory::local_failure_injector();
        injector.fail_after(ordinal);
        try {
            result.emplace(
              model::make_read_checkpoint_from_unordered(
                topic(), std::move(source), memory(), work)
                .get());
        } catch (...) {
            exception = std::current_exception();
        }
        const bool injected = injector.failed();
        injector.cancel();
        if (exception) {
            ASSERT_TRUE(injected);
            // Outer frame failure retains all entries; entered failures drain.
            // NOLINTBEGIN(bugprone-use-after-move)
            EXPECT_TRUE(source.empty() || source.size() == 65);
            observed_owned_failure |= source.empty();
            // NOLINTEND(bugprone-use-after-move)
        } else {
            ASSERT_TRUE(result);
            ASSERT_TRUE(*result);
            EXPECT_EQ(result->value().value.cursors().size(), 65);
            succeeded = true;
        }
    }
    EXPECT_TRUE(succeeded);
    EXPECT_TRUE(observed_owned_failure);
#endif
}

TEST(CheckpointTest, DecodeAllocationFailuresNeverCommitOrPublishPartialState) {
#if !defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    GTEST_SKIP() << "allocation injection is disabled in this verified profile";
#else
    const auto wire = unhex(golden_hex);
    seastar::abort_source abort;
    codec::cooperative_work warm{codec::limits::defaults(), abort};
    ASSERT_TRUE(decode(wire, warm));
    bool succeeded = false;
    std::uint64_t failures = 0;
    for (std::uint64_t ordinal = 0; ordinal < 256 && !succeeded; ++ordinal) {
        SCOPED_TRACE(ordinal);
        codec::cooperative_work work{codec::limits::defaults(), abort};
        fragmented_buffer_parser input{fragmented(wire, 67)};
        ASSERT_TRUE(input.push_checkpoint());
        const auto reserved = codec::reserve_decode_input(
          input, work.policy(), memory(), context);
        ASSERT_TRUE(reserved);
        std::optional<codec::result<model::decoded_read_checkpoint>> result;
        std::exception_ptr exception;
        auto& injector = seastar::memory::local_failure_injector();
        injector.fail_after(ordinal);
        try {
            result.emplace(
              model::decode_read_checkpoint(
                input, topic(), *reserved, work, context)
                .get());
        } catch (...) {
            exception = std::current_exception();
        }
        const bool injected = injector.failed();
        injector.cancel();
        EXPECT_EQ(input.checkpoint_depth(), 1);
        if (exception) {
            EXPECT_TRUE(injected);
            EXPECT_FALSE(result);
            EXPECT_EQ(input.bytes_consumed(), byte_count{});
            ++failures;
        } else {
            ASSERT_TRUE(result);
            ASSERT_TRUE(*result);
            EXPECT_EQ(input.bytes_consumed(), byte_count{wire.size()});
            EXPECT_EQ(
              digest_bytes(result->value().fingerprint),
              unhex(fingerprint_hex));
            succeeded = true;
        }
    }
    EXPECT_TRUE(succeeded);
    EXPECT_GT(failures, 0);
#endif
}

TEST(CheckpointTest, QueuedCancellationIsJoinedBeforeValueOrCursorPublication) {
    std::vector<model::range_cursor> entries;
    entries.reserve(4096);
    for (std::uint32_t i = 1; i <= 4096; ++i)
        entries.push_back(numbered(i));
    seastar::abort_source setup_abort;
    codec::cooperative_work setup{codec::limits::defaults(), setup_abort};
    auto checkpoint = build(entries, setup);
    auto wire = model::encode_read_checkpoint(
                  checkpoint, setup, memory().operation_remaining, charge)
                  .get();
    ASSERT_TRUE(wire);
    fragmented_buffer_parser input{fragmented(flatten(wire->bytes), 4096)};
    ASSERT_TRUE(input.push_checkpoint());
    const auto reserved = codec::reserve_decode_input(
      input, setup.policy(), memory(), context);
    ASSERT_TRUE(reserved);
    const auto exercise = [&](auto start) {
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
        std::optional<codec::error> failed;
        std::exception_ptr exception;
        try {
            auto result = start(work).get();
            if (!result) failed = result.error();
        } catch (...) {
            exception = std::current_exception();
        }
        const bool during = observed;
        observer.get();
        if (exception) std::rethrow_exception(exception);
        EXPECT_TRUE(during);
        ASSERT_TRUE(failed);
        EXPECT_EQ(failed->code(), errc::aborted);
    };
    exercise([&](codec::cooperative_work& work) {
        return model::make_read_checkpoint(topic(), entries, memory(), work);
    });
    exercise([&](codec::cooperative_work& work) {
        return model::compute_checkpoint_fingerprint(checkpoint, work);
    });
    exercise([&](codec::cooperative_work& work) {
        return model::encode_read_checkpoint(
          checkpoint, work, memory().operation_remaining, charge);
    });
    exercise([&](codec::cooperative_work& work) {
        return model::decode_read_checkpoint(
          input, topic(), *reserved, work, context);
    });
    EXPECT_EQ(input.bytes_consumed(), byte_count{});
    EXPECT_EQ(input.checkpoint_depth(), 1);
    EXPECT_EQ(checkpoint.cursors().size(), entries.size());
}

} // namespace
