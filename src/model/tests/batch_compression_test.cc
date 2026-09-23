#include "src/bytes/test_allocation_profile.h"
#include "src/model/batch_builder.h"
#include "src/model/batch_codec.h"
#include "src/model/batch_rewrite.h"
#include "src/model/record_codec.h"
#include "src/model/tests/record_fuzz_oracle.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/alloc_failure_injector.hh>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <bit>
#include <limits>
#include <new>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace {
namespace model = kwaque::model;
namespace codec = kwaque::codec;
namespace compression = kwaque::compression;
namespace oracle = kwaque::model::testing;
using kwaque::byte_count;
using kwaque::errc;
using kwaque::item_count;
using kwaque::bytes::fragmented_buffer;
using kwaque::bytes::fragmented_buffer_parser;
constexpr codec::field_context coordinates{.origin = 1234};

using kwaque::bytes::testing::charge;
codec::decode_budget memory() {
    return {byte_count{32U << 20U}, byte_count{1U << 20U}, charge};
}
template<typename Id>
Id id(std::uint8_t first) {
    std::array<std::uint8_t, 16> bytes{};
    for (std::size_t i = 0; i < bytes.size(); ++i)
        bytes[i] = static_cast<std::uint8_t>(first + i);
    return Id::make(bytes).value();
}
model::batch_decode_expectation expected() {
    return {id<model::topic_id>(33), id<model::range_id>(65)};
}
std::string records(std::size_t payload = 0) {
    std::string all;
    all.reserve(3U * (payload + 32U));
    for (std::uint64_t delta = 0; delta < 3; ++delta) {
        std::string body(1, '\0');
        body += oracle::varuint(2U * delta);
        body += oracle::varuint(delta);
        body += oracle::varuint(1);
        body += oracle::varuint(payload == 0 ? 1U : 2U * payload);
        body.append(payload, 'x');
        body += oracle::varuint(0);
        all += oracle::varuint(body.size()) + body;
    }
    return all;
}
std::string wire(bool assigned, bool compressed, std::size_t payload = 0) {
    const auto raw = records(payload);
    auto body = oracle::identity_bytes();
    body.resize(assigned ? 184 : 168, '\0');
    oracle::put(body, 136, 100, 8);
    oracle::put(body, 144, 3, 4);
    oracle::put(body, 148, 3, 4);
    oracle::put(body, 156, compressed ? 1U : 0U, 1);
    oracle::put(body, 158, 1, 2);
    const auto encoded = compressed ? oracle::lz4_records(raw) : raw;
    oracle::put(body, 160, encoded.size(), 4);
    oracle::put(body, 164, raw.size(), 4);
    if (assigned) {
        oracle::put(body, 168, 10, 8);
        oracle::put(body, 176, 13, 8);
    }
    const auto digest = oracle::fingerprint(body, raw);
    for (std::size_t i = 0; i < digest.size(); ++i)
        body[104 + i] = static_cast<char>(digest[i]);
    return oracle::frame(body + encoded, assigned, false);
}
std::string replace_records(
  std::string_view original, std::string_view raw, bool assigned) {
    auto body = std::string{original.substr(32, assigned ? 184 : 168)};
    const auto compressed = oracle::lz4_records(raw);
    oracle::put(body, 156, 1, 1);
    oracle::put(body, 160, compressed.size(), 4);
    oracle::put(body, 164, raw.size(), 4);
    return oracle::frame(body + compressed, assigned, false);
}
fragmented_buffer bytes(std::string_view raw, std::size_t width = 67) {
    std::vector<seastar::temporary_buffer<char>> pieces;
    pieces.reserve((raw.size() + width - 1U) / width);
    for (std::size_t at = 0; at < raw.size(); at += width) {
        const auto part = raw.substr(at, width);
        pieces.emplace_back(part.data(), part.size());
    }
    return fragmented_buffer::copy_from_fragments(pieces).value();
}
std::string flat(const fragmented_buffer& value) {
    std::string result;
    result.reserve(value.size().value());
    for (const auto fragment : value)
        result.append(fragment.data(), fragment.size());
    return result;
}
codec::decode_budget reserve(
  const fragmented_buffer_parser& input,
  codec::cooperative_work& work,
  codec::field_context context = coordinates) {
    return codec::reserve_decode_input(input, work.policy(), memory(), context)
      .value();
}
codec::result<void> decode(
  fragmented_buffer_parser& input,
  bool assigned,
  codec::decode_budget budget,
  codec::cooperative_work& work,
  codec::field_context context = coordinates) {
    if (assigned) {
        const auto result = model::decode_assigned_batch(
                              input, expected(), budget, work, context)
                              .get();
        if (!result) return codec::failure(result.error());
    } else {
        const auto result = model::decode_submitted_batch(
                              input, expected(), budget, work, context)
                              .get();
        if (!result) return codec::failure(result.error());
    }
    return {};
}

TEST(BatchCompressionTest, ExplicitChoicePreservesNoneAndNormalizesRawOwners) {
    for (const bool assigned : {false, true}) {
        const auto none = wire(assigned, false);
        for (const auto encoding :
             {compression::codec_id::none, compression::codec_id::lz4}) {
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            fragmented_buffer_parser input{bytes(none)};
            auto encoded = [&] {
                if (assigned) {
                    auto value = model::decode_assigned_batch(
                                   input,
                                   expected(),
                                   reserve(input, work),
                                   work,
                                   coordinates)
                                   .get()
                                   .value();
                    return model::encode_assigned_batch(
                             std::move(value.value),
                             encoding,
                             work,
                             memory().operation_remaining,
                             charge)
                      .get();
                }
                auto value = model::decode_submitted_batch(
                               input,
                               expected(),
                               reserve(input, work),
                               work,
                               coordinates)
                               .get()
                               .value();
                return model::encode_submitted_batch(
                         std::move(value.value),
                         encoding,
                         work,
                         memory().operation_remaining,
                         charge)
                  .get();
            }();
            ASSERT_TRUE(encoded.has_value());
            const auto serialized = flat(*encoded);
            EXPECT_EQ(
              oracle::little(serialized, 32 + 156, 1),
              static_cast<std::uint8_t>(encoding));
            if (encoding == compression::codec_id::none)
                EXPECT_EQ(serialized, none);
            EXPECT_EQ(
              oracle::probe_batch(serialized, assigned, true, false).error,
              errc::success);
            fragmented_buffer_parser read{std::move(*encoded)};
            if (assigned) {
                auto value
                  = model::decode_assigned_batch(
                      read, expected(), reserve(read, work), work, coordinates)
                      .get()
                      .value();
                EXPECT_EQ(flat(value.value.records()), records());
                EXPECT_EQ(
                  value.fingerprint_verification,
                  model::batch_fingerprint_verification::recomputed);
            } else {
                auto value
                  = model::decode_submitted_batch(
                      read, expected(), reserve(read, work), work, coordinates)
                      .get()
                      .value();
                EXPECT_EQ(flat(value.value.records()), records());
            }
            EXPECT_TRUE(read.at_end());
        }
    }
}

TEST(
  BatchCompressionTest, SparseRewritePreservesOriginalSpanDigestAndSurvivors) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    fragmented_buffer_parser input{bytes(wire(true, true))};
    auto dense = model::decode_assigned_batch(
                   input, expected(), reserve(input, work), work, coordinates)
                   .get()
                   .value();
    const auto original = dense.value.context();
    const auto digest = dense.value.fingerprint();
    const std::array selected{
      model::range_logical_count{0}, model::range_logical_count{2}};
    auto sparse = model::rewrite_assigned_batch(
                    std::move(dense.value), selected, memory(), work)
                    .get()
                    .value();
    const auto survivors = flat(sparse.records());
    auto encoded = model::encode_assigned_batch(
                     std::move(sparse),
                     compression::codec_id::lz4,
                     work,
                     memory().operation_remaining,
                     charge)
                     .get()
                     .value();
    EXPECT_EQ(
      oracle::probe_batch(flat(encoded), true, true, false).error,
      errc::success);
    fragmented_buffer_parser read{std::move(encoded)};
    auto decoded = model::decode_assigned_batch(
                     read, expected(), reserve(read, work), work, coordinates)
                     .get()
                     .value();
    EXPECT_EQ(decoded.value.context().submitted(), original.submitted());
    EXPECT_EQ(decoded.value.context().logical_span(), original.logical_span());
    EXPECT_EQ(decoded.value.fingerprint(), digest);
    EXPECT_EQ(flat(decoded.value.records()), survivors);
    EXPECT_EQ(
      decoded.fingerprint_verification,
      model::batch_fingerprint_verification::carried);
}

TEST(
  BatchCompressionTest,
  IntegrityPrecedesNativeDecodeAndGrammarErrorsUseWireAnchors) {
    for (const bool assigned : {false, true}) {
        const auto original = wire(assigned, true);
        const auto fixed = assigned ? 184U : 168U;
        for (int mode = 0; mode < 5; ++mode) {
            auto changed = original;
            if (mode <= 1) {
                changed[32 + fixed] = 'x';
                if (mode == 1) oracle::repair_crc(changed);
            } else if (mode == 2) {
                changed.back() = static_cast<char>(changed.back() ^ 1);
                oracle::repair_crc(changed);
            } else if (mode == 3) {
                auto malformed = records();
                malformed[1]
                  = 1; // record attributes, with both integrity layers repaired
                changed = replace_records(original, malformed, assigned);
            } else {
                oracle::put(
                  changed,
                  32 + 104,
                  oracle::little(changed, 32 + 104, 1) ^ 1U,
                  1);
                oracle::repair_crc(changed);
            }
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            fragmented_buffer_parser input{bytes("p" + changed, 7)};
            input.skip(byte_count{1}).value();
            input.push_checkpoint().value();
            const auto result = decode(
              input, assigned, reserve(input, work), work);
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(
              result.error().code(),
              mode == 1   ? errc::malformed_data
              : mode == 3 ? errc::unsupported_format
                          : errc::corrupt_data);
            if (mode == 1 || mode == 3) {
                EXPECT_EQ(
                  result.error().field(),
                  static_cast<std::uint16_t>(model::batch_field::records));
                EXPECT_EQ(
                  result.error().byte_offset(),
                  coordinates.origin + 1U + 32U + fixed);
            }
            if (mode == 4)
                EXPECT_EQ(
                  result.error().byte_offset(),
                  coordinates.origin + 1U + 32U + 104U);
            EXPECT_EQ(input.bytes_consumed(), byte_count{1});
            EXPECT_EQ(input.checkpoint_depth(), 1U);
        }
    }
}

TEST(
  BatchCompressionTest,
  ExpandedBackingSurvivesEncodedInputAndConsumesParentBudget) {
    const auto encoded = wire(false, true, 12000);
    const codec::field_context context{.origin = UINT64_MAX - encoded.size()};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto decoded = [&] {
        fragmented_buffer_parser input{bytes(encoded, 7)};
        const auto original = reserve(input, work, context);
        auto result = model::decode_submitted_batch(
                        input, expected(), original, work, context)
                        .get()
                        .value();
        const auto cost
          = result.value.records().allocation_cost(charge).value();
        EXPECT_EQ(
          original.operation_remaining.value()
            - result.remaining.operation_remaining.value(),
          cost.backing.value() + cost.descriptors.value()
            + cost.share_controls.value());
        EXPECT_TRUE(input.at_end());
        return result;
    }();
    EXPECT_EQ(flat(decoded.value.records()), records(12000));
    EXPECT_GT(decoded.value.records().size().value(), encoded.size());
    auto malformed = records(12000);
    malformed.back()
      = 1; // final header count has no corresponding header bytes
    const auto broken = replace_records(encoded, malformed, false);
    fragmented_buffer_parser input{bytes(broken, 7)};
    const codec::field_context near_end{.origin = UINT64_MAX - broken.size()};
    const auto failed = decode(
      input, false, reserve(input, work, near_end), work, near_end);
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(
      failed.error().field(),
      static_cast<std::uint16_t>(model::batch_field::records));
    EXPECT_EQ(failed.error().byte_offset(), near_end.origin + 32U + 168U);
    EXPECT_EQ(input.bytes_consumed(), byte_count{});
}

TEST(
  BatchCompressionTest, EncodingChecksTheCompressedWireExtentAtLargeOrigins) {
    const auto original = wire(false, false, 12000);
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto encode = [&](codec::field_context context) {
        fragmented_buffer_parser input{bytes(original)};
        auto value
          = model::decode_submitted_batch(
              input, expected(), reserve(input, work), work, coordinates)
              .get()
              .value();
        return model::encode_submitted_batch(
                 std::move(value.value),
                 compression::codec_id::lz4,
                 work,
                 memory().operation_remaining,
                 charge,
                 context)
          .get();
    };
    const auto first = encode({});
    ASSERT_TRUE(first.has_value());
    ASSERT_LT(first->size().value(), records(12000).size());
    const auto last = encode({.origin = UINT64_MAX - first->size().value()});
    ASSERT_TRUE(last.has_value());
    EXPECT_EQ(flat(*last), flat(*first));
}

TEST(BatchCompressionTest, FramingUsesCommonEncodedCapBeforeCodecIsTrusted) {
    for (const auto claimed : {(8U << 20U) + 200U, (16U << 20U) + 1U}) {
        auto header = wire(false, true).substr(0, 32);
        oracle::put(header, 12, claimed, 4);
        oracle::repair_crc(header);
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        fragmented_buffer_parser input{bytes(header)};
        const auto result = decode(input, false, reserve(input, work), work);
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(
          result.error().code(),
          claimed > (16U << 20U) ? errc::resource_exhausted
                                 : errc::truncated_data);
        EXPECT_EQ(input.bytes_consumed(), byte_count{});
    }
}

TEST(BatchCompressionTest, AdjacentExpandedOwnersShareTheParentRemainder) {
    const auto encoded = wire(false, true, 12000);
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    fragmented_buffer_parser input{bytes(encoded + encoded, 7)};
    const auto budget = reserve(input, work);
    auto first = model::decode_submitted_batch(
                   input, expected(), budget, work, coordinates)
                   .get()
                   .value();
    const auto first_end = input.bytes_consumed();
    EXPECT_EQ(first_end.value(), encoded.size());
    auto second = model::decode_submitted_batch(
                    input, expected(), first.remaining, work, coordinates)
                    .get()
                    .value();
    const auto cost = second.value.records().allocation_cost(charge).value();
    EXPECT_EQ(
      first.remaining.operation_remaining.value()
        - second.remaining.operation_remaining.value(),
      cost.backing.value() + cost.descriptors.value()
        + cost.share_controls.value());
    EXPECT_TRUE(input.at_end());
    EXPECT_EQ(flat(first.value.records()), records(12000));
    EXPECT_EQ(flat(second.value.records()), records(12000));
}

TEST(BatchCompressionTest, IncompressibleMaximumRegionMayGrowOnTheWire) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const codec::decode_budget budget{
      byte_count{63U << 20U}, byte_count{1U << 20U}, charge};
    const auto identity = model::batch_id::make(
                            id<model::producer_id>(1),
                            model::producer_epoch::make(2).value(),
                            model::producer_stream_id::make(3).value(),
                            model::batch_sequence{4})
                            .value();
    const auto binding = model::producer_stream_binding::make(
                           id<model::topic_id>(33),
                           id<model::range_id>(65),
                           model::range_routing_epoch::make(7).value(),
                           id<model::segment_id>(97),
                           model::segment_generation::make(9).value())
                           .value();
    kwaque::bytes::fragmented_buffer_builder payload{
      {.initial_fragment_bytes = byte_count{65536},
       .max_fragment_bytes = byte_count{65536},
       .max_total_bytes = byte_count{1048565},
       .max_retained_bytes = byte_count{1U << 20U},
       .max_fragments = 16}};
    payload.reserve_fragments(item_count{16}).value();
    std::array<char, 16384> chunk;
    std::uint32_t state = 0x6a09e667;
    while (payload.size().value() < 1048565) {
        const auto count = std::min<std::size_t>(
          chunk.size(), 1048565U - payload.size().value());
        for (std::size_t i = 0; i < count; ++i) {
            state ^= state << 13U;
            state ^= state >> 17U;
            state ^= state << 5U;
            chunk[i] = std::bit_cast<char>(static_cast<std::uint8_t>(state));
        }
        payload.append(std::span<const char>{chunk}.first(count)).value();
        seastar::thread::maybe_yield();
    }
    auto record
      = model::make_record(
          {}, std::nullopt, std::optional{payload.finish().value()}, {})
          .value();
    const auto admitted
      = model::reserve_record_input(record, work, budget).get().value();
    auto builder = model::batch_builder::make(
                     identity, binding, work.policy(), charge)
                     .value();
    for (unsigned i = 0; i < 8; ++i)
        builder
          .add(
            record,
            kwaque::runtime::wall_time{100},
            work,
            admitted.operation_remaining)
          .get()
          .value();
    auto batch
      = builder.finalize(work, budget.operation_remaining).get().value();
    ASSERT_EQ(batch.records().size(), byte_count{8U << 20U});
    const auto digest = batch.fingerprint();
    auto encoded = model::encode_submitted_batch(
                     std::move(batch),
                     compression::codec_id::lz4,
                     work,
                     budget.operation_remaining,
                     charge)
                     .get();
    ASSERT_TRUE(encoded.has_value());
    {
        fragmented_buffer_parser prefix{encoded->share()};
        std::array<char, 200> fixed;
        prefix.read_to(fixed).value();
        EXPECT_GT(
          oracle::little(
            std::string_view{fixed.data(), fixed.size()}, 32 + 160, 4),
          8U << 20U);
        EXPECT_EQ(
          oracle::little(
            std::string_view{fixed.data(), fixed.size()}, 32 + 164, 4),
          8U << 20U);
    }
    fragmented_buffer_parser input{std::move(*encoded)};
    const auto remaining
      = codec::reserve_decode_input(input, work.policy(), budget).value();
    auto decoded
      = model::decode_submitted_batch(input, expected(), remaining, work).get();
    ASSERT_TRUE(decoded.has_value());
    EXPECT_TRUE(input.at_end());
    EXPECT_EQ(decoded->value.records().size(), byte_count{8U << 20U});
    EXPECT_EQ(decoded->value.fingerprint(), digest);
}

TEST(
  BatchCompressionTest,
  InsufficientExpandedReservationRejectsWithoutAdvancing) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    fragmented_buffer_parser input{bytes(wire(false, true, 12000))};
    auto budget = reserve(input, work);
    budget.operation_remaining = byte_count{4096};
    const auto result = decode(input, false, budget, work);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), errc::resource_exhausted);
    EXPECT_EQ(input.bytes_consumed(), byte_count{});
}

TEST(BatchCompressionTest, FinalHeaderAssemblyHonorsExactFragmentLimit) {
    for (const auto encoding :
         {compression::codec_id::none, compression::codec_id::lz4}) {
        for (const std::uint64_t limit : {2U, 3U}) {
            seastar::abort_source abort;
            codec::cooperative_work prepare{codec::limits::defaults(), abort};
            fragmented_buffer_parser input{bytes(wire(false, false), 65536)};
            auto decoded
              = model::decode_submitted_batch(
                  input, expected(), reserve(input, prepare), prepare)
                  .get()
                  .value();
            auto config = codec::limits_config{};
            config.max_buffer_fragments = kwaque::item_count{limit};
            codec::cooperative_work work{
              codec::limits::make(config).value(), abort};
            const auto result = model::encode_submitted_batch(
                                  std::move(decoded.value),
                                  encoding,
                                  work,
                                  memory().operation_remaining,
                                  charge)
                                  .get();
            if (limit == 2) {
                ASSERT_FALSE(result.has_value());
                EXPECT_EQ(result.error().code(), errc::resource_exhausted);
            } else {
                ASSERT_TRUE(result.has_value());
                EXPECT_EQ(result->fragment_count(), 3U);
                EXPECT_EQ(
                  oracle::probe_batch(flat(*result), false, true, false).error,
                  errc::success);
            }
        }
    }
}

TEST(BatchCompressionTest, AllocationFailuresRollBackTheWholeEnvelope) {
#if !defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    GTEST_SKIP() << "allocation failure injection is unavailable";
#else
    for (const bool assigned : {false, true}) {
        const auto serialized = wire(assigned, true);
        bool completed = false;
        std::size_t failures = 0;
        for (std::uint64_t ordinal = 0; ordinal < 256 && !completed;
             ++ordinal) {
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            fragmented_buffer_parser input{bytes("p" + serialized)};
            input.skip(byte_count{1}).value();
            input.push_checkpoint().value();
            const auto budget = reserve(input, work);
            auto& injector = seastar::memory::local_failure_injector();
            std::optional<codec::result<void>> result;
            bool threw = false;
            injector.fail_after(ordinal);
            try {
                result.emplace(decode(input, assigned, budget, work));
            } catch (const std::bad_alloc&) {
                threw = true;
            } catch (const std::runtime_error&) {
                // Native SHA setup reports a failed C allocation by exception.
                if (!injector.failed()) {
                    injector.cancel();
                    throw;
                }
                threw = true;
            } catch (...) {
                injector.cancel();
                throw;
            }
            const bool reached = injector.failed();
            injector.cancel();
            if (threw) {
                EXPECT_TRUE(reached);
                EXPECT_FALSE(result.has_value());
                EXPECT_EQ(input.bytes_consumed(), byte_count{1});
                ++failures;
            } else {
                ASSERT_TRUE(result.has_value());
                ASSERT_TRUE(result->has_value());
                EXPECT_EQ(
                  input.bytes_consumed().value(), 1U + serialized.size());
                completed = !reached;
            }
            EXPECT_EQ(input.checkpoint_depth(), 1U);
        }
        EXPECT_TRUE(completed);
        EXPECT_GE(failures, 6U);
    }
#endif
}

} // namespace
