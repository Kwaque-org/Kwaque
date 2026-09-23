#include "src/storage/local_metadata.h"
#include "src/storage/tests/retry_test_support.h"

#include <seastar/core/preempt.hh>
#include <seastar/util/alloc_failure_injector.hh>

#include <gtest/gtest.h>

#include <array>
#include <optional>
#include <type_traits>

namespace kwaque::storage {
namespace {
using namespace testing;
using bytes::fragmented_buffer_parser;
codec::immutable_object_digest digest(std::uint8_t byte = 0x61) {
    codec::sha256_digest bytes{};
    bytes.fill(byte);
    return codec::immutable_object_digest{bytes};
}
model::wal_incarnation_id wal_id(std::uint64_t n = 1) {
    return local_wal_high{}.checked_advance(n)->incarnation().value();
}
local_wal_cursor cursor(std::uint64_t position, std::uint64_t id = 1) {
    return local_wal_cursor::make(wal_id(id), runtime::file_position{position})
      .value();
}
local_store_context
store_owner(bool store = false, std::uint8_t device = 0x33) {
    return local_store_context::make(
             id<model::cluster_id>(0x50),
             id<model::broker_id>(0x22),
             id<device_store_id>(device),
             store ? local_store_shard : 0)
      .value();
}
local_metadata_header
record_header(local_metadata_kind kind, std::uint64_t generation = 1) {
    return local_metadata_header::make(
             kind,
             store_owner(kind == local_metadata_kind::store_identity),
             local_publication_generation::make(generation).value())
      .value();
}
local_footer_reference footer(std::uint16_t family = 6) {
    return local_footer_reference::make(
             runtime::file_position{8192}, byte_count{4096}, family, digest())
      .value();
}
local_root_reference root(local_root_kind kind, std::uint64_t sequence = 40) {
    return local_root_reference::make(
             kind,
             local_object_sequence::make(sequence).value(),
             runtime::file_position{
               kind == local_root_kind::sealed_retry ? 8192U : 0U},
             byte_count{4096},
             page_count::make(0).value(),
             digest())
      .value();
}
std::string independent_record(
  local_metadata_kind kind,
  std::string payload,
  std::uint64_t generation = 1,
  std::size_t h = 32,
  std::size_t a = 4096) {
    std::string body(72, '\0');
    put(body, 0, static_cast<std::uint16_t>(kind), 2);
    body.replace(4, 16, 16, '\x50');
    body.replace(20, 16, 16, '\x22');
    body.replace(36, 16, 16, '\x33');
    put(
      body,
      52,
      kind == local_metadata_kind::store_identity ? UINT32_MAX : 0,
      4);
    put(body, 56, generation, 8);
    put(body, 64, payload.size(), 4);
    const auto padding = (a - (h + body.size() + payload.size()) % a) % a;
    put(body, 68, padding, 4);
    body += payload;
    body.append(padding, '\0');
    return frame(std::move(body), 11, h);
}
std::string
independent_wal(std::size_t a, bool predecessor = false, std::size_t h = 32) {
    std::string payload(predecessor ? 68 : 44, '\0');
    payload[15] = '\x09';
    payload[16] = predecessor ? '\x01' : '\0';
    if (predecessor) {
        payload[35] = '\x01';
        put(payload, 36, 65536, 8);
    }
    const auto tail = predecessor ? 44U : 20U;
    put(payload, tail, a, 4);
    put(payload, tail + 4, 1, 2);
    put(payload, tail + 8, ((h + 72 + payload.size() + a - 1) / a) * a, 8);
    put(payload, tail + 16, 67108864, 8);
    return independent_record(
      local_metadata_kind::wal_descriptor, std::move(payload), 1, h, a);
}
std::string store_wire(std::size_t h = 32) {
    std::string payload(16, '\0');
    put(payload, 0, 1, 2);
    put(payload, 2, 4096, 4);
    put(payload, 6, 2, 4);
    put(payload, 10, 7, 2);
    return independent_record(
      local_metadata_kind::store_identity, std::move(payload), 1, h);
}
local_metadata_expectation expected(
  local_metadata_header header,
  const local_metadata_payload& payload,
  codec::immutable_object_digest hash,
  byte_count bytes) {
    local_metadata_expectation value{header, alignment(4096)};
    std::visit(
      [&](const auto& body) {
          using T = std::remove_cvref_t<decltype(body)>;
          if constexpr (requires { body.segment; }) {
              value.segment = sc();
              value.segment_alignment = alignment(4096);
          }
          if constexpr (std::is_same_v<T, local_wal_descriptor>)
              value.wal_incarnation = wal_id();
          if constexpr (std::is_same_v<T, local_boundary_evidence>) {
              value.data_device = id<device_store_id>(0x44);
              value.data_metadata_alignment = alignment(4096);
          }
          if constexpr (
            std::is_same_v<T, local_checkpoint_page>
            || std::is_same_v<T, local_completed_retry_page>)
              value.page = page_ref::make(
                             body.ordinal,
                             body.first_entry,
                             static_cast<std::uint32_t>(body.entries.size()),
                             bytes,
                             hash)
                             .value();
      },
      payload);
    value.digest = hash;
    value.encoded_bytes = bytes;
    return value;
}
struct record_case final {
    local_metadata_kind kind;
    local_metadata_payload payload;
    std::uint64_t generation{1};
};
std::vector<record_case> cases() {
    return {
      {local_metadata_kind::store_identity,
       local_store_identity{1, alignment(4096), 2, local_device_role::all}},
      {local_metadata_kind::shard_control,
       local_shard_control{
         local_wal_high{}.checked_advance(16).value(),
         local_object_high{128},
         local_decision_high{},
         local_deletion_high{},
         root(local_root_kind::checkpoint, 70),
         local_wal_head{wal_id(), digest()}}},
      {local_metadata_kind::wal_descriptor,
       local_wal_descriptor{
         wal_id(),
         {},
         alignment(4096),
         replay_profile::v1,
         runtime::file_position{4096},
         byte_count{65536}}},
      {local_metadata_kind::segment_descriptor,
       local_segment_descriptor{
         sc(),
         model::range_logical_end{100},
         model::segment_relative_end{0},
         alignment(4096),
         storage_profile::v1,
         1,
         local_layout_kind::initial,
         byte_count{65536},
         runtime::monotonic_duration{3600000000000ULL}}},
      {local_metadata_kind::object_publication,
       local_object_publication{
         sc(),
         local_object_state::sealed,
         footer(7),
         {root(local_root_kind::sealed_retry)}}},
      {local_metadata_kind::checkpoint_root,
       local_checkpoint_root{cursor(4096), cursor(4096), 0, {}}},
      {local_metadata_kind::checkpoint_page,
       local_checkpoint_page{
         local_object_sequence::make(70).value(),
         page_ordinal::make(0).value(),
         0,
         {{cursor(4096),
           cursor(8192),
           sc(),
           local_checkpoint_disposition::segment_boundary,
           51,
           digest()}}},
       70},
      {local_metadata_kind::deletion_intent,
       local_deletion_intent{
         sc(),
         7,
         local_publication_generation::make(1).value(),
         local_deletion_reason::retention,
         {{local_deletion_kind::data, 0},
          {local_deletion_kind::immutable_bundle, 40}}},
       90},
      {local_metadata_kind::recovery_decision,
       local_recovery_decision{
         cursor(8192),
         digest(),
         sc(),
         runtime::file_position{4096},
         local_recovery_action::discard,
         7},
       63},
      {local_metadata_kind::boundary_evidence,
       local_boundary_evidence{
         sc(),
         id<device_store_id>(0x44),
         footer(),
         scope(100, 101, 0, 1, 4096, 8192),
         root(local_root_kind::completed_retry_snapshot)},
       51},
      {local_metadata_kind::completed_retry_root,
       local_completed_retry_root{sc(), footer(), 0, {}},
       40},
      {local_metadata_kind::completed_retry_page,
       local_completed_retry_page{
         sc(),
         local_object_sequence::make(40).value(),
         page_ordinal::make(0).value(),
         0,
         {retry()}},
       40},
    };
}
TEST(LocalMetadataTest, EveryTypedBodyRoundTripsAndKeepsOwningResiduals) {
    for (const auto& test : cases()) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        const auto header = record_header(test.kind, test.generation);
        auto encoding = local_metadata_encoding{header, alignment(4096), {}};
        std::visit(
          [&](const auto& body) {
              if constexpr (requires { body.segment; })
                  encoding.segment_alignment = alignment(4096);
              if constexpr (
                std::is_same_v<
                  std::remove_cvref_t<decltype(body)>,
                  local_boundary_evidence>)
                  encoding.data_metadata_alignment = alignment(4096);
          },
          test.payload);
        auto encoded = encode_local_metadata(
                         encoding,
                         test.payload,
                         work,
                         budget().operation_remaining,
                         charge)
                         .get();
        ASSERT_TRUE(encoded) << static_cast<unsigned>(test.kind);
        const auto wire = flat(encoded->bytes);
        EXPECT_EQ(get(wire, 4, 2), 11U);
        EXPECT_EQ(get(wire, 32, 2), static_cast<unsigned>(test.kind));
        EXPECT_EQ(encoded->digest.bytes(), exact_sha(wire));
        const auto expectation = expected(
          header, test.payload, encoded->digest, encoded->bytes.size());
        fragmented_buffer_parser input{buffer(wire, 67)};
        const auto memory = reserve(input, work);
        auto decoded = decode_local_metadata(
                         input,
                         expectation,
                         memory,
                         work,
                         {},
                         codec::input_boundary::complete)
                         .get();
        ASSERT_TRUE(decoded) << static_cast<unsigned>(test.kind);
        EXPECT_TRUE(input.at_end());
        EXPECT_EQ(input.checkpoint_depth(), 0U);
        EXPECT_EQ(decoded->value.header(), header);
        EXPECT_EQ(decoded->value.payload(), test.payload);
        EXPECT_LE(
          decoded->remaining.operation_remaining, memory.operation_remaining);
        EXPECT_LE(
          decoded->remaining.metadata_remaining, memory.metadata_remaining);
        fragmented_buffer_parser discovery{buffer(wire, 67)};
        auto probe = probe_local_metadata(
                       discovery,
                       alignment(4096),
                       reserve(discovery, work),
                       work,
                       {},
                       codec::input_boundary::complete)
                       .get();
        ASSERT_TRUE(probe);
        EXPECT_EQ(probe->header, header);
        EXPECT_TRUE(discovery.at_end());
    }
}
TEST(LocalMetadataTest, WalDiscoveryUsesStoredGeometryWithIndependentPins) {
    for (const auto a : {512U, 8192U, 65536U}) {
        for (const auto predecessor : {false, true}) {
            for (const auto h : {32U, 4096U}) {
                const auto wire = independent_wal(a, predecessor, h);
                const codec::immutable_object_digest pin{exact_sha(wire)};
                for (const auto pinned : {false, true}) {
                    seastar::abort_source abort;
                    codec::cooperative_work work{
                      codec::limits::defaults(), abort};
                    fragmented_buffer_parser input{buffer(wire, 67)};
                    const auto memory = reserve(input, work);
                    auto decoded = decode_local_wal_descriptor(
                                     input,
                                     store_owner(),
                                     wal_id(9),
                                     pinned ? std::optional{pin} : std::nullopt,
                                     memory,
                                     work,
                                     {},
                                     codec::input_boundary::complete)
                                     .get();
                    ASSERT_TRUE(decoded) << a << '/' << h << '/' << predecessor;
                    const auto& wal = std::get<local_wal_descriptor>(
                      decoded->value.payload());
                    EXPECT_EQ(wal.alignment, alignment(a));
                    EXPECT_EQ(wal.data_start.value(), wire.size());
                    EXPECT_EQ(wal.predecessor.has_value(), predecessor);
                    EXPECT_EQ(
                      decoded->value.encoded_bytes().value(), wire.size());
                    EXPECT_EQ(
                      decoded->remaining.operation_remaining,
                      memory.operation_remaining);
                    EXPECT_EQ(
                      decoded->remaining.metadata_remaining,
                      memory.metadata_remaining);
                    EXPECT_TRUE(input.at_end());
                    EXPECT_EQ(input.checkpoint_depth(), 0U);
                }
                seastar::abort_source abort;
                codec::cooperative_work work{codec::limits::defaults(), abort};
                fragmented_buffer_parser input{buffer(wire, 67)};
                auto claims = probe_local_metadata(
                                input,
                                alignment(4096),
                                reserve(input, work),
                                work,
                                {},
                                codec::input_boundary::complete)
                                .get();
                ASSERT_TRUE(claims);
                EXPECT_EQ(claims->alignment, alignment(a));
                EXPECT_EQ(claims->wal_incarnation, wal_id(9));
                EXPECT_TRUE(input.at_end());

                // An independently supplied explicit layout is still strict.
                fragmented_buffer_parser strict{buffer(wire, 67)};
                local_metadata_expectation expected{
                  record_header(local_metadata_kind::wal_descriptor),
                  alignment(4096)};
                expected.wal_incarnation = wal_id(9);
                expected.digest = pin;
                expected.encoded_bytes = byte_count{wire.size()};
                auto rejected = decode_local_metadata(
                                  strict,
                                  expected,
                                  reserve(strict, work),
                                  work,
                                  {},
                                  codec::input_boundary::complete)
                                  .get();
                EXPECT_FALSE(rejected);
                EXPECT_EQ(strict.bytes_consumed().value(), 0U);
            }
        }
    }
}
TEST(LocalMetadataTest, StoredWalGeometryRejectsDamageAndKeepsParentMarks) {
    struct mutation final {
        std::size_t offset;
        std::uint64_t value;
        std::size_t width;
        errc error;
    };
    const std::array changes{
      mutation{124, 0, 4, errc::malformed_data},
      mutation{124, 513, 4, errc::malformed_data},
      mutation{124, 131072, 4, errc::malformed_data},
      mutation{124, 4096, 4, errc::malformed_data},
      mutation{132, 4096, 8, errc::malformed_data},
      mutation{140, 4096, 8, errc::malformed_data},
      mutation{120, 2, 1, errc::malformed_data},
      mutation{121, 1, 1, errc::malformed_data},
      mutation{96, 43, 4, errc::malformed_data},
      mutation{100, 0, 4, errc::malformed_data},
      mutation{8191, 1, 1, errc::malformed_data},
      mutation{119, 8, 1, errc::wrong_context},
      mutation{36, 0x51, 1, errc::wrong_context},
      mutation{84, 1, 4, errc::wrong_context},
      mutation{128, 2, 2, errc::unsupported_format}};
    for (const auto change : changes) {
        auto wire = independent_wal(8192);
        put(wire, change.offset, change.value, change.width);
        repair(wire);
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        fragmented_buffer_parser input{buffer(std::string{"x"} + wire, 67)};
        input.skip(byte_count{1}).value();
        input.push_checkpoint().value();
        const auto depth = input.checkpoint_depth();
        auto rejected = decode_local_wal_descriptor(
                          input,
                          store_owner(),
                          wal_id(9),
                          {},
                          reserve(input, work),
                          work,
                          {},
                          codec::input_boundary::complete)
                          .get();
        ASSERT_FALSE(rejected) << change.offset;
        EXPECT_EQ(rejected.error().code(), change.error) << change.offset;
        EXPECT_EQ(input.bytes_consumed().value(), 1U);
        EXPECT_EQ(input.checkpoint_depth(), depth);
        input.rollback().value();
    }
    const auto original = independent_wal(8192);
    const codec::immutable_object_digest pin{exact_sha(original)};
    auto changed = original;
    put(changed, 140, 33554432, 8); // Valid different capacity, repaired CRC.
    repair(changed);
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    fragmented_buffer_parser input{buffer(changed, 67)};
    auto rejected = decode_local_wal_descriptor(
                      input,
                      store_owner(),
                      wal_id(9),
                      pin,
                      reserve(input, work),
                      work,
                      {},
                      codec::input_boundary::complete)
                      .get();
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error().code(), errc::corrupt_data);
    EXPECT_EQ(input.bytes_consumed().value(), 0U);
    EXPECT_EQ(input.checkpoint_depth(), 0U);
    for (const auto length : {0U, 31U, 32U, 103U, 123U, 147U, 8191U}) {
        fragmented_buffer_parser truncated{
          buffer(original.substr(0, length), 67)};
        auto incomplete = decode_local_wal_descriptor(
                            truncated,
                            store_owner(),
                            wal_id(9),
                            pin,
                            reserve(truncated, work),
                            work,
                            {},
                            codec::input_boundary::complete)
                            .get();
        EXPECT_FALSE(incomplete);
        EXPECT_EQ(truncated.bytes_consumed().value(), 0U);
        EXPECT_EQ(truncated.checkpoint_depth(), 0U);
    }
    fragmented_buffer_parser cancelled{buffer(original, 67)};
    const auto memory = reserve(cancelled, work);
    abort.request_abort();
    auto stopped = decode_local_wal_descriptor(
                     cancelled,
                     store_owner(),
                     wal_id(9),
                     pin,
                     memory,
                     work,
                     {},
                     codec::input_boundary::complete)
                     .get();
    ASSERT_FALSE(stopped);
    EXPECT_EQ(stopped.error().code(), errc::aborted);
    EXPECT_EQ(cancelled.bytes_consumed().value(), 0U);
}
TEST(
  LocalMetadataTest, SelectedControlLearnsGenerationButKeepsIndependentOwner) {
    const local_metadata_payload payload{local_shard_control{
      local_wal_high{},
      local_object_high{},
      local_decision_high{},
      local_deletion_high{},
      {},
      {}}};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto header = record_header(local_metadata_kind::shard_control, 9);
    auto encoded = encode_local_metadata(
                     {header, alignment(4096)},
                     payload,
                     work,
                     budget().operation_remaining,
                     charge)
                     .get();
    ASSERT_TRUE(encoded);
    const auto wire = flat(encoded->bytes);
    bytes::fragmented_buffer_parser input{buffer(wire, 67)};
    auto loaded
      = decode_selected_shard_control(
          input, header.owner(), alignment(4096), reserve(input, work), work)
          .get();
    ASSERT_TRUE(loaded);
    EXPECT_EQ(loaded->value.header().generation().value(), 9U);
    EXPECT_EQ(loaded->value.payload(), payload);
    EXPECT_TRUE(input.at_end());
    bytes::fragmented_buffer_parser exact{buffer(wire, 67)};
    auto rejected = decode_local_metadata(
                      exact,
                      {record_header(local_metadata_kind::shard_control, 1),
                       alignment(4096)},
                      reserve(exact, work),
                      work)
                      .get();
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error().code(), errc::wrong_context);
    EXPECT_EQ(exact.bytes_consumed().value(), 0U);
    bytes::fragmented_buffer_parser foreign{buffer(wire, 67)};
    auto wrong = decode_selected_shard_control(
                   foreign,
                   store_owner(false, 0x88),
                   alignment(4096),
                   reserve(foreign, work),
                   work)
                   .get();
    ASSERT_FALSE(wrong);
    EXPECT_EQ(wrong.error().code(), errc::wrong_context);
    EXPECT_EQ(foreign.bytes_consumed().value(), 0U);
}
TEST(LocalMetadataTest, StoreBytesMatchIndependentFieldsAndCompatibleHeaders) {
    for (const auto h : {32U, 64U}) {
        const auto wire = store_wire(h);
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        fragmented_buffer_parser input{buffer(wire, 7)};
        local_metadata_expectation e{
          record_header(local_metadata_kind::store_identity), alignment(4096)};
        auto decoded = decode_local_metadata(
                         input,
                         e,
                         reserve(input, work),
                         work,
                         {},
                         codec::input_boundary::complete)
                         .get();
        ASSERT_TRUE(decoded);
        EXPECT_TRUE(input.at_end());
        EXPECT_EQ(
          std::get<local_store_identity>(decoded->value.payload()).shard_count,
          2U);
        auto encoded = encode_local_metadata(
                         {e.header, e.alignment, {}},
                         decoded->value.payload(),
                         work,
                         budget().operation_remaining,
                         charge)
                         .get();
        ASSERT_TRUE(encoded);
        EXPECT_TRUE(encoded->bytes.content_equals(store_wire()));
    }
}
TEST(LocalMetadataTest, RepairedCorruptionAndWrongContextRollbackParentMarks) {
    struct mutation {
        std::size_t offset;
        std::uint64_t value;
        std::size_t width;
        errc error;
    };
    const std::array changes{
      mutation{32, 0, 2, errc::malformed_data},
      mutation{32, 13, 2, errc::unsupported_format},
      mutation{34, 1, 2, errc::malformed_data},
      mutation{36, 0, 1, errc::wrong_context},
      mutation{84, 0, 4, errc::malformed_data},
      mutation{88, 0, 8, errc::malformed_data},
      mutation{96, 15, 4, errc::malformed_data},
      mutation{100, 0, 4, errc::malformed_data},
      mutation{104, 2, 2, errc::unsupported_format},
      mutation{116, 1, 4, errc::malformed_data},
      mutation{4095, 1, 1, errc::malformed_data}};
    for (const auto& change : changes) {
        auto wire = store_wire();
        put(wire, change.offset, change.value, change.width);
        repair(wire);
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        fragmented_buffer_parser input{buffer(std::string{"x"} + wire, 67)};
        input.skip(byte_count{1}).value();
        input.push_checkpoint().value();
        const auto position = input.bytes_consumed();
        const auto depth = input.checkpoint_depth();
        auto decoded = decode_local_metadata(
                         input,
                         {record_header(local_metadata_kind::store_identity),
                          alignment(4096)},
                         reserve(input, work),
                         work,
                         {},
                         codec::input_boundary::complete)
                         .get();
        ASSERT_FALSE(decoded);
        EXPECT_EQ(decoded.error().code(), change.error) << change.offset;
        EXPECT_EQ(input.bytes_consumed(), position);
        EXPECT_EQ(input.checkpoint_depth(), depth);
        input.rollback().value();
    }
}
TEST(
  LocalMetadataTest,
  ExternalPinsAreRequiredAndCheckedBeforePublishingDecodedState) {
    std::string payload(128, '\0');
    put_sc(payload, sc());
    put(payload, 72, 8192, 8);
    put(payload, 80, 4096, 4);
    put(payload, 84, 6, 2);
    const auto wire = independent_record(
      local_metadata_kind::completed_retry_root, payload, 40);
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    local_metadata_expectation e{
      record_header(local_metadata_kind::completed_retry_root, 40),
      alignment(4096),
      sc(),
      alignment(4096)};
    fragmented_buffer_parser input{buffer(wire, 67)};
    auto result
      = decode_local_metadata(input, e, reserve(input, work), work).get();
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code(), errc::invalid_argument);
    e.digest = digest();
    e.encoded_bytes = byte_count{wire.size()};
    result = decode_local_metadata(input, e, reserve(input, work), work).get();
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code(), errc::corrupt_data);
    EXPECT_EQ(input.bytes_consumed().value(), 0U);
    EXPECT_EQ(input.checkpoint_depth(), 0U);
    e.digest = codec::immutable_object_digest{exact_sha(wire)};
    result = decode_local_metadata(input, e, reserve(input, work), work).get();
    EXPECT_TRUE(result);
    EXPECT_TRUE(input.at_end());
}
TEST(LocalMetadataTest, RetryLeafBytesAndPreviousFullKeyAreSharedAcrossPages) {
    for (const unsigned variant : {0U, 1U, 2U}) {
        const bool duplicate = variant == 1;
        std::string payload(92, '\0');
        put_sc(payload, sc());
        put(payload, 72, variant == 2 ? 41U : 40U, 8);
        put(payload, 80, 1, 4);
        put(payload, 84, 1, 4);
        put(payload, 88, 1, 4);
        payload += retry_wire(retry(duplicate ? 0 : 1));
        const auto wire = independent_record(
          local_metadata_kind::completed_retry_page, payload, 40);
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto hash = codec::immutable_object_digest{exact_sha(wire)};
        local_metadata_expectation e{
          record_header(local_metadata_kind::completed_retry_page, 40),
          alignment(4096),
          sc(),
          alignment(4096)};
        e.digest = hash;
        e.encoded_bytes = byte_count{wire.size()};
        e.page = page_ref::make(
                   page_ordinal::make(1).value(), 1, 1, *e.encoded_bytes, hash)
                   .value();
        e.previous_retry = retry(0).id();
        fragmented_buffer_parser input{buffer(wire, 67)};
        auto result
          = decode_local_metadata(input, e, reserve(input, work), work).get();
        if (variant != 0) {
            ASSERT_FALSE(result);
            EXPECT_EQ(
              result.error().code(),
              duplicate ? errc::malformed_data : errc::wrong_context);
            EXPECT_EQ(input.bytes_consumed().value(), 0U);
        } else {
            ASSERT_TRUE(result);
            EXPECT_EQ(
              std::get<local_completed_retry_page>(result->value.payload())
                .entries[0],
              retry(1));
            auto encoded = encode_local_metadata(
                             {e.header, e.alignment, e.segment_alignment},
                             result->value.payload(),
                             work,
                             budget().operation_remaining,
                             charge)
                             .get();
            ASSERT_TRUE(encoded);
            EXPECT_TRUE(encoded->bytes.content_equals(wire));
        }
    }
}
TEST(
  LocalMetadataTest,
  TruncationAbortAndMetadataPressureDoNotPublishPartialOwners) {
    const auto wire = store_wire();
    for (const auto end : {0U, 1U, 31U, 32U, 103U, 119U, 4095U}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        fragmented_buffer_parser input{
          buffer(std::string_view{wire}.substr(0, end), 67)};
        auto result = probe_local_metadata(
                        input,
                        alignment(4096),
                        reserve(input, work),
                        work,
                        {},
                        codec::input_boundary::complete)
                        .get();
        ASSERT_FALSE(result);
        EXPECT_EQ(input.bytes_consumed().value(), 0U);
        EXPECT_EQ(input.checkpoint_depth(), 0U);
    }
    std::string payload(92, '\0');
    put_sc(payload, sc());
    put(payload, 72, 40, 8);
    put(payload, 88, 1, 4);
    payload += retry_wire(retry());
    const auto page = independent_record(
      local_metadata_kind::completed_retry_page, payload, 40);
    for (const bool cancel : {false, true}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        fragmented_buffer_parser input{buffer(page, 67)};
        auto memory = reserve(input, work);
        if (cancel)
            abort.request_abort();
        else
            memory.metadata_remaining = byte_count{};
        auto result
          = probe_local_metadata(input, alignment(4096), memory, work).get();
        ASSERT_FALSE(result);
        EXPECT_EQ(
          result.error().code(),
          cancel ? errc::aborted : errc::resource_exhausted);
        EXPECT_EQ(input.bytes_consumed().value(), 0U);
        EXPECT_EQ(input.checkpoint_depth(), 0U);
    }
}
TEST(LocalMetadataTest, BoundaryEvidenceMatchesIndependentFields) {
    for (const bool with_retry : {false, true}) {
        const auto covered = scope(100, 101, 0, 1, 4096, 8192);
        const local_metadata_payload payload{local_boundary_evidence{
          sc(),
          id<device_store_id>(0x44),
          footer(),
          covered,
          with_retry
            ? std::optional{root(local_root_kind::completed_retry_snapshot)}
            : std::nullopt}};
        std::string fields(with_retry ? 248U : 188U, '\0');
        put_sc(fields, sc());
        fields.replace(72, 16, 16, '\x44');
        put(fields, 88, 8192, 8);
        put(fields, 96, 4096, 4);
        put(fields, 100, 6, 2);
        fields.replace(104, 32, 32, '\x61');
        put_coverage(fields, 136, covered);
        put(fields, 184, with_retry ? 1U : 0U, 1);
        if (with_retry) {
            put(fields, 188, 5, 2);
            put(fields, 192, 40, 8);
            put(fields, 208, 4096, 4);
            fields.replace(216, 32, 32, '\x61');
        }
        const auto wire = independent_record(
          local_metadata_kind::boundary_evidence, std::move(fields), 51);
        const auto header = record_header(
          local_metadata_kind::boundary_evidence, 51);
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto encoded
          = encode_local_metadata(
              {header, alignment(4096), alignment(4096), alignment(4096)},
              payload,
              work,
              budget().operation_remaining,
              charge)
              .get();
        ASSERT_TRUE(encoded);
        EXPECT_EQ(flat(encoded->bytes), wire);
        EXPECT_EQ(encoded->digest.bytes(), exact_sha(wire));
        fragmented_buffer_parser input{buffer(wire, 67)};
        auto decoded = decode_local_metadata(
                         input,
                         expected(
                           header,
                           payload,
                           codec::immutable_object_digest{exact_sha(wire)},
                           byte_count{wire.size()}),
                         reserve(input, work),
                         work)
                         .get();
        ASSERT_TRUE(decoded);
        EXPECT_EQ(decoded->value.payload(), payload);
        EXPECT_TRUE(input.at_end());
    }
}

TEST(LocalMetadataTest, EvidenceUsesTheReferencedDevicesMetadataAlignment) {
    const auto snapshot = local_root_reference::make(
                            local_root_kind::completed_retry_snapshot,
                            local_object_sequence::make(40).value(),
                            runtime::file_position{},
                            byte_count{512},
                            page_count::make(0).value(),
                            digest())
                            .value();
    const local_metadata_payload payload{local_boundary_evidence{
      sc(),
      id<device_store_id>(0x44),
      footer(),
      scope(100, 101, 0, 1, 4096, 8192),
      snapshot}};
    const auto header = record_header(
      local_metadata_kind::boundary_evidence, 51);
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto encoded = encode_local_metadata(
                     {header, alignment(4096), alignment(4096), alignment(512)},
                     payload,
                     work,
                     budget().operation_remaining,
                     charge)
                     .get();
    ASSERT_TRUE(encoded);
    auto e = expected(header, payload, encoded->digest, encoded->bytes.size());
    e.data_metadata_alignment = alignment(512);
    const auto wire = flat(encoded->bytes);
    fragmented_buffer_parser input{buffer(wire, 67)};
    auto decoded
      = decode_local_metadata(input, e, reserve(input, work), work).get();
    ASSERT_TRUE(decoded);
    EXPECT_EQ(decoded->value.payload(), payload);
    e.data_metadata_alignment.reset();
    fragmented_buffer_parser missing_context{buffer(wire, 67)};
    auto missing = decode_local_metadata(
                     missing_context, e, reserve(missing_context, work), work)
                     .get();
    ASSERT_FALSE(missing);
    EXPECT_EQ(missing.error().code(), errc::invalid_argument);
    EXPECT_EQ(missing_context.bytes_consumed().value(), 0U);
    fragmented_buffer_parser probe_input{buffer(wire, 67)};
    EXPECT_TRUE(
      probe_local_metadata(
        probe_input, alignment(4096), reserve(probe_input, work), work)
        .get());
}

TEST(LocalMetadataTest, MaximumRetryPageIsBoundedAndImpossibleCountRejects) {
    local_completed_retry_page page{
      sc(),
      local_object_sequence::make(40).value(),
      page_ordinal::make(0).value(),
      0,
      {}};
    page.entries.reserve(409);
    for (std::uint32_t i = 0; i < 408; ++i)
        page.entries.push_back(retry(i));
    local_metadata_payload payload{std::move(page)};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto header = record_header(
      local_metadata_kind::completed_retry_page, 40);
    const local_metadata_encoding context{
      header, alignment(4096), alignment(4096)};
    auto encoded
      = encode_local_metadata(
          context, payload, work, budget().operation_remaining, charge)
          .get();
    ASSERT_TRUE(encoded);
    EXPECT_EQ(encoded->bytes.size().value(), 65536U);
    auto e = expected(header, payload, encoded->digest, encoded->bytes.size());
    const auto wire = flat(encoded->bytes);
    fragmented_buffer_parser input{buffer(wire, 4096)};
    const auto memory = reserve(input, work);
    auto decoded = decode_local_metadata(input, e, memory, work).get();
    ASSERT_TRUE(decoded);
    const auto& entries
      = std::get<local_completed_retry_page>(decoded->value.payload()).entries;
    EXPECT_EQ(entries.size(), 408U);
    EXPECT_LE(
      entries.capacity() * sizeof(completed_retry),
      maximum_contiguous_allocation_bytes);
    EXPECT_LT(decoded->remaining.metadata_remaining, memory.metadata_remaining);
    auto bad = wire;
    put(bad, 192, 409, 4);
    repair(bad);
    e.digest = codec::immutable_object_digest{exact_sha(bad)};
    e.page
      = page_ref::make(
          page_ordinal::make(0).value(), 0, 409, *e.encoded_bytes, *e.digest)
          .value();
    fragmented_buffer_parser rejected_input{buffer(bad, 4096)};
    auto rejected = decode_local_metadata(
                      rejected_input, e, reserve(rejected_input, work), work)
                      .get();
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error().code(), errc::malformed_data);
    EXPECT_EQ(rejected_input.bytes_consumed().value(), 0U);
    std::get<local_completed_retry_page>(payload).entries.push_back(retry(408));
    auto overflow
      = encode_local_metadata(
          context, payload, work, budget().operation_remaining, charge)
          .get();
    ASSERT_FALSE(overflow);
    EXPECT_EQ(overflow.error().code(), errc::resource_exhausted);
}
TEST(
  LocalMetadataTest,
  WriterRejectsUnsupportedRelocationAndUnsafePublicationState) {
    auto examples = cases();
    auto& checkpoint = std::get<local_checkpoint_page>(examples[6].payload);
    checkpoint.entries[0].disposition
      = local_checkpoint_disposition::preserved_candidate;
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto unsupported = encode_local_metadata(
                         {record_header(
                            local_metadata_kind::checkpoint_page, 70),
                          alignment(4096),
                          {}},
                         examples[6].payload,
                         work,
                         budget().operation_remaining,
                         charge)
                         .get();
    ASSERT_FALSE(unsupported);
    EXPECT_EQ(unsupported.error().code(), errc::unsupported_format);
    auto& publication = std::get<local_object_publication>(examples[4].payload);
    publication.roots.clear();
    auto missing = encode_local_metadata(
                     {record_header(local_metadata_kind::object_publication),
                      alignment(4096),
                      alignment(4096)},
                     examples[4].payload,
                     work,
                     budget().operation_remaining,
                     charge)
                     .get();
    ASSERT_FALSE(missing);
    EXPECT_EQ(missing.error().code(), errc::malformed_data);
    EXPECT_FALSE(
      local_metadata_header::make(
        local_metadata_kind::store_identity,
        store_owner(true),
        local_publication_generation::make(2).value()));
}

TEST(
  LocalMetadataTest,
  AllocationFailureKeepsParserTransactionAndPermitsCleanRetry) {
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    std::string payload(92, '\0');
    put_sc(payload, sc());
    put(payload, 72, 40, 8);
    put(payload, 88, 1, 4);
    payload += retry_wire(retry());
    const auto wire = independent_record(
      local_metadata_kind::completed_retry_page, payload, 40);
    bool completed = false;
    unsigned failures = 0;
    for (unsigned allocation = 0; allocation < 128 && !completed;
         ++allocation) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        fragmented_buffer_parser input{buffer(wire, 4096)};
        auto memory = reserve(input, work);
        auto& injector = seastar::memory::local_failure_injector();
        injector.fail_after(allocation);
        std::optional<codec::result<local_metadata_claims>> outcome;
        std::exception_ptr exception;
        try {
            outcome.emplace(
              probe_local_metadata(input, alignment(4096), memory, work).get());
        } catch (...) {
            exception = std::current_exception();
        }
        const bool injected = injector.failed();
        injector.cancel();
        if (exception) {
            ++failures;
            EXPECT_TRUE(injected);
            EXPECT_EQ(input.bytes_consumed().value(), 0U);
            EXPECT_EQ(input.checkpoint_depth(), 0U);
            try {
                std::rethrow_exception(exception);
            } catch (const std::bad_alloc&) {
            } catch (...) {
                FAIL() << "unexpected allocation-failure category";
            }
        } else {
            ASSERT_TRUE(outcome && *outcome);
            completed = !injected;
        }
    }
    EXPECT_TRUE(completed);
    EXPECT_GT(failures, 0U);
#endif
}
} // namespace
} // namespace kwaque::storage
