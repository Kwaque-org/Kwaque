#include "src/model/batch_builder.h"
#include "src/model/batch_codec.h"
#include "src/model/batch_rewrite.h"
#include "src/model/record_scan.h"
#include "src/storage/tests/range_manifest_test_support.h"
#include "src/storage/tests/retry_test_support.h"
#include "src/storage/tests/sparse_index_test_support.h"
#include "src/storage/tests/wal_test_support.h"

#include <gtest/gtest.h>

#include <array>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace kwaque::storage {
namespace {
using namespace testing;
using bytes::fragmented_buffer_parser;
constexpr std::array<std::int64_t, 5> times{100, 99, 110, 98, 101};
constexpr std::array selected{
  model::range_logical_count{1}, model::range_logical_count{3}};
constexpr codec::field_context diagnostic{
  .origin = 71, .family = 5, .field = 192};

model::producer_stream_binding original_binding() {
    return model::producer_stream_binding::make(
             sc().topic(),
             sc().range(),
             model::range_routing_epoch::make(7).value(),
             sc().segment(),
             model::segment_generation::make(9).value())
      .value();
}
model::batch_id batch_identity(std::uint64_t sequence) {
    return model::batch_id::make(
             id<model::producer_id>(0x40),
             model::producer_epoch::make(2).value(),
             model::producer_stream_id::make(3).value(),
             model::batch_sequence{sequence})
      .value();
}
std::string original_records() {
    return hex(
      "070000000102610007000101010262000700140201026300070003030102640007000204"
      "01026500");
}
codec::semantic_batch_digest original_digest(std::uint64_t sequence) {
    // Independently specified projection: original identity, timestamp/count,
    // then all five ordered records. Placement and compression are excluded.
    std::string prefix(127, '\0');
    prefix.replace(0, 11, std::string{"KQ/BATCH/1\0", 11});
    prefix.replace(11, 16, 16, '\x40');
    put(prefix, 27, 2, 8);
    put(prefix, 35, 3, 8);
    put(prefix, 43, sequence, 8);
    prefix.replace(51, 16, 16, '\x10');
    prefix.replace(67, 16, 16, '\x20');
    put(prefix, 83, 7, 8);
    prefix.replace(91, 16, 16, '\x30');
    put(prefix, 107, 9, 8);
    put(prefix, 115, 100, 8);
    put(prefix, 123, 5, 4);
    return codec::semantic_batch_digest{exact_sha(prefix + original_records())};
}
model::batch_decode_expectation original_expected(std::uint64_t sequence) {
    return {
      sc().topic(),
      sc().range(),
      batch_identity(sequence),
      original_binding(),
      original_digest(sequence)};
}

codec::decode_budget operation_budget() {
    // Leave 32 MiB and half the metadata allowance for the fixed two-block
    // fixture, held aliases and native/test frames outside the current call.
    return {byte_count{32U << 20U}, byte_count{512U << 10U}, charge};
}

model::submitted_batch
submission(std::uint64_t sequence, codec::cooperative_work& work) {
    auto builder
      = model::batch_builder::make(
          batch_identity(sequence), original_binding(), work.policy(), charge)
          .value();
    for (std::size_t i = 0; i < times.size(); ++i) {
        const auto value = model::make_record(
                             {},
                             {},
                             buffer(std::string(1, static_cast<char>('a' + i))),
                             {},
                             work.policy())
                             .value();
        builder
          .add(
            value,
            runtime::wall_time{times[i]},
            work,
            operation_budget().operation_remaining)
          .get()
          .value();
    }
    return builder.finalize(work, operation_budget().operation_remaining)
      .get()
      .value();
}

segment_block_expectation location(
  const segment_history_context& h,
  std::uint64_t sequence,
  std::uint64_t physical,
  std::uint64_t position) {
    return {
      segment_write_context::make(
        h.segment,
        h.alignment,
        model::segment_relative_end{physical},
        runtime::file_position{position})
        .value(),
      h.data_start,
      original_expected(sequence),
      h.profile};
}

codec::decode_budget reserve_current(
  const fragmented_buffer_parser& input, codec::cooperative_work& work) {
    return codec::reserve_decode_input(input, work.policy(), operation_budget())
      .value();
}
codec::decode_budget reserve_current(
  const bytes::fragmented_buffer& input, codec::cooperative_work& work) {
    const auto cost = input.allocation_cost(charge).value();
    return codec::detail::consume_decode_budget(
             work.policy(),
             operation_budget(),
             cost.backing,
             cost.descriptors.checked_add(cost.share_controls).value(),
             {},
             0)
      .value();
}

// Retain only small encoded fixture bytes between steps. Returned/aliased data,
// native contexts and test frames fit the unclaimed 32-MiB operation half;
// each operation still reserves its current input and carries child residuals.
std::string make_block(
  std::uint64_t sequence,
  std::uint64_t logical,
  bool sparse,
  compression::codec_id encoding,
  segment_block_expectation expected,
  codec::cooperative_work& work,
  std::size_t width) {
    auto original = submission(sequence, work);
    EXPECT_EQ(original.fingerprint(), original_digest(sequence));
    EXPECT_TRUE(original.records().content_equals(original_records()));
    auto submitted_wire = model::encode_submitted_batch(
                            std::move(original),
                            work,
                            operation_budget().operation_remaining,
                            charge)
                            .get()
                            .value();
    auto assigned = [&] {
        fragmented_buffer_parser input{std::move(submitted_wire)};
        auto decoded = model::decode_submitted_batch(
                         input,
                         original_expected(sequence),
                         reserve_current(input, work),
                         work)
                         .get()
                         .value();
        EXPECT_TRUE(input.at_end());
        EXPECT_EQ(decoded.value.context().id(), batch_identity(sequence));
        return model::assigned_batch::assign(
                 std::move(decoded.value),
                 model::range_logical_end{logical},
                 original_binding())
          .value();
    }();
    if (sparse) {
        assigned = model::rewrite_assigned_batch(
                     std::move(assigned), selected, operation_budget(), work)
                     .get()
                     .value();
    }
    auto child = make_encoded_assigned_batch(
                   std::move(assigned),
                   encoding,
                   work,
                   operation_budget().operation_remaining,
                   charge)
                   .get()
                   .value();
    const auto child_wire = flat(child.bytes());
    const auto initial = validate_initial_append(child, expected.location);
    EXPECT_EQ(initial.has_value(), !sparse);
    const wal_prepare_expectation wal{
      wal_write_context::make(
        id<model::wal_incarnation_id>(0x70),
        alignment(1024),
        runtime::file_position{8192 + (sequence - 4) * 1024})
        .value(),
      expected.location,
      expected.data_start,
      original_binding().routing_epoch(),
      original_expected(sequence)};
    auto wire = encode_wal_prepare(
                  std::move(child),
                  wal,
                  work,
                  operation_budget().operation_remaining,
                  charge,
                  diagnostic)
                  .get()
                  .value();
    auto block = [&] {
        fragmented_buffer_parser input{buffer(flat(wire), width)};
        auto decoded
          = decode_wal_prepare(
              input, wal, reserve_current(input, work), work, diagnostic)
              .get()
              .value();
        EXPECT_TRUE(input.at_end());
        EXPECT_EQ(decoded.value.wal(), wal.wal);
        EXPECT_EQ(decoded.value.target(), expected.location);
        EXPECT_EQ(
          decoded.value.routing_epoch(), original_binding().routing_epoch());
        EXPECT_TRUE(decoded.value.batch().bytes().content_equals(child_wire));
        EXPECT_EQ(
          decoded.value.batch().info().fingerprint, original_digest(sequence));
        auto extracted = std::move(decoded.value).release_batch();
        return encode_segment_block(
                 std::move(extracted),
                 expected,
                 work,
                 operation_budget().operation_remaining,
                 charge)
          .get()
          .value();
    }();
    EXPECT_EQ(block.descriptor().context(), expected.location.segment());
    EXPECT_EQ(
      block.descriptor().batch().context.submitted().id(),
      batch_identity(sequence));
    EXPECT_EQ(
      block.descriptor().batch().context.submitted().binding(),
      original_binding());
    EXPECT_EQ(
      block.descriptor().coverage().logical(),
      manifest::logical(logical, logical + 5));
    EXPECT_EQ(
      block.descriptor().coverage().physical().begin(),
      expected.location.physical_begin());
    EXPECT_EQ(
      block.descriptor().coverage().physical().count().value(),
      sparse ? 2U : 5U);
    EXPECT_EQ(
      block.descriptor().coverage().bytes().begin(),
      expected.location.position());
    const auto stored = flat(block.bytes());
    const auto child_at = codec::envelope_prefix_bytes
                          + segment_block_fixed_bytes.value();
    EXPECT_EQ(stored.substr(child_at, child_wire.size()), child_wire);
    // Read back from the actual stored child, after the WAL parser was
    // destroyed.
    fragmented_buffer_parser input{
      buffer(stored.substr(child_at, child_wire.size()), width)};
    auto decoded = model::decode_assigned_batch(
                     input,
                     original_expected(sequence),
                     reserve_current(input, work),
                     work)
                     .get()
                     .value();
    EXPECT_TRUE(input.at_end());
    EXPECT_EQ(
      decoded.fingerprint_verification,
      sparse ? model::batch_fingerprint_verification::carried
             : model::batch_fingerprint_verification::recomputed);
    EXPECT_EQ(decoded.value.fingerprint(), original_digest(sequence));
    const auto batch = decoded.value.context();
    auto records = std::move(decoded.value).release_records();
    auto scanner = model::record_region_scanner::make(
                     std::move(records),
                     {batch.submitted().original_timestamp_base(),
                      batch.submitted().original_count(),
                      batch.retained_count(),
                      item_count{},
                      sparse ? model::record_region_kind::sparse
                             : model::record_region_kind::dense_original},
                     operation_budget(),
                     work)
                     .get()
                     .value();
    std::uint64_t retained = 0;
    while (scanner.next(work).get().value()) {
        if (retained >= (sparse ? selected.size() : times.size())) {
            ADD_FAILURE() << "scanner returned more records than the fixture";
            break;
        }
        const auto delta
          = sparse ? selected[static_cast<std::size_t>(retained)].value()
                   : retained;
        const auto* current = scanner.current();
        if (!current)
            throw std::runtime_error("scanner omitted its current record");
        EXPECT_EQ(current->fields.logical_delta.value(), delta);
        EXPECT_EQ(
          current->fields.timestamp_delta,
          times[static_cast<std::size_t>(delta)] - times[0]);
        const auto record_id = model::record_id::make(
                                 sc().topic(),
                                 sc().range(),
                                 model::range_logical_offset::make(
                                   batch.logical_span().begin().value()
                                   + current->fields.logical_delta.value())
                                   .value())
                                 .value();
        EXPECT_EQ(record_id.offset().value(), logical + delta);
        EXPECT_EQ(record_id.topic(), original_binding().topic());
        EXPECT_EQ(record_id.range(), original_binding().range());
        const auto physical = expected.location.physical_begin().value()
                              + retained;
        EXPECT_TRUE(block.descriptor().coverage().physical().contains(
          model::segment_relative_offset::make(physical).value()));
        auto record = scanner.materialize_current(scanner.remaining(), work)
                        .get()
                        .value();
        EXPECT_FALSE(record.value.has_key());
        EXPECT_TRUE(record.value.has_value());
        if (record.value.has_value())
            EXPECT_TRUE(record.value.value()->content_equals(
              std::string(1, static_cast<char>('a' + delta))));
        ++retained;
    }
    EXPECT_TRUE(scanner.complete());
    EXPECT_EQ(retained, sparse ? 2U : 5U);
    scanner.close(work).get();
    return stored;
}

void verify_manifest(
  const verified_extent& evidence, codec::cooperative_work& work) {
    const auto h = evidence.context();
    const auto mc = manifest::mc(0x60, 13);
    const std::array entries{range_manifest_entry::make(
                               h.segment.segment(),
                               h.segment.generation(),
                               evidence.boundary().coverage,
                               *evidence.digest())
                               .value()};
    const auto page_header = manifest::page_header(1, mc, 100, 110);
    auto page_wire = encode_range_manifest_page(
                       page_header,
                       entries,
                       alignment(1024),
                       work,
                       operation_budget().operation_remaining,
                       charge)
                       .get()
                       .value();
    const std::array manifest_refs{page_wire.reference};
    const auto root_header = manifest::root_header(1, 1, mc, 100, 110);
    auto root_wire = encode_range_manifest_root(
                       root_header,
                       manifest_refs,
                       alignment(1024),
                       work,
                       operation_budget().operation_remaining,
                       charge)
                       .get()
                       .value();
    const auto pin = codec::immutable_object_digest{
      exact_sha(flat(root_wire.bytes))};
    EXPECT_EQ(root_wire.digest, pin);
    fragmented_buffer_parser input{std::move(root_wire.bytes)};
    auto root = decode_range_manifest_root(
                  input,
                  mc,
                  manifest::logical(100, 110),
                  alignment(1024),
                  pin,
                  reserve_current(input, work),
                  work)
                  .get()
                  .value();
    EXPECT_TRUE(input.at_end());
    range_manifest_verifier walk{root.value, work.policy()};
    {
        fragmented_buffer_parser page_input{std::move(page_wire.bytes)};
        auto page = walk
                      .next(
                        page_input,
                        codec::reserve_decode_input(
                          page_input, work.policy(), root.remaining)
                          .value(),
                        work)
                      .get()
                      .value();
        EXPECT_TRUE(page_input.at_end());
        ASSERT_EQ(page.value.entries().size(), 1U);
        EXPECT_EQ(page.value.entries()[0], entries[0]);
        EXPECT_TRUE(validate_range_manifest_extent(
          mc, h.segment.cluster(), page.value.entries()[0], evidence));
    }
    EXPECT_EQ(walk.finish(work).value().root_digest(), pin);
}

void verify_metadata(
  const verified_extent& evidence,
  const std::array<std::string, 2>& blocks,
  const std::array<segment_block_expectation, 2>& locations,
  codec::cooperative_work& work) {
    const auto h = evidence.context();
    const auto index_context = sparse_index_context::make(
                                 h.segment,
                                 evidence.boundary().coverage,
                                 *evidence.digest(),
                                 h.alignment)
                                 .value();
    std::vector<page_ref> refs;
    refs.reserve(2);
    std::array<std::string, 2> pages;
    for (std::uint32_t i = 0; i < 2; ++i) {
        const std::array entries{sparse_index_entry{
          model::range_logical_offset::make(100U + 5U * i).value(),
          locations[i].location.position()}};
        auto encoded = encode_sparse_index_page(
                         entries,
                         index_context,
                         page_ordinal::make(i).value(),
                         i,
                         work,
                         operation_budget().operation_remaining,
                         charge)
                         .get()
                         .value();
        pages[i] = flat(encoded.bytes);
        refs.push_back(encoded.reference);
        EXPECT_EQ(refs[i].digest().bytes(), exact_sha(pages[i]));
    }
    {
        auto encoded = encode_sparse_index_root(
                         index_context,
                         2,
                         refs,
                         work,
                         operation_budget().operation_remaining,
                         charge)
                         .get()
                         .value();
        const auto root_pin = codec::immutable_object_digest{
          exact_sha(flat(encoded.bytes))};
        EXPECT_EQ(encoded.digest, root_pin);
        fragmented_buffer_parser input{std::move(encoded.bytes)};
        auto root = decode_sparse_index_root(
                      input,
                      index_context,
                      root_pin,
                      reserve_current(input, work),
                      work)
                      .get()
                      .value();
        EXPECT_TRUE(input.at_end());
        EXPECT_TRUE(validate_sparse_index_extent(root.value, evidence));
        sparse_index_verifier walk{root.value, work.policy()};
        for (std::size_t i = 0; i < 2; ++i) {
            fragmented_buffer_parser page_input{buffer(pages[i], 7)};
            auto page = walk
                          .next(
                            page_input,
                            codec::reserve_decode_input(
                              page_input, work.policy(), root.remaining)
                              .value(),
                            work)
                          .get()
                          .value();
            EXPECT_TRUE(page_input.at_end());
            ASSERT_EQ(page.value.entries().size(), 1U);
            fragmented_buffer_parser block_input{buffer(blocks[i], 7)};
            auto block = decode_segment_block(
                           block_input,
                           locations[i],
                           codec::reserve_decode_input(
                             block_input, work.policy(), page.remaining)
                             .value(),
                           work)
                           .get()
                           .value();
            EXPECT_TRUE(block_input.at_end());
            EXPECT_TRUE(validate_sparse_index_anchor(
              index_context,
              page.value.entries()[0],
              block.value.descriptor()));
        }
        auto complete = walk.finish(work).value();
        EXPECT_EQ(complete.entry_count(), 2U);
        EXPECT_EQ(complete.root_digest(), root_pin);
    }
    verify_manifest(evidence, work);
}

TEST(
  FormatIntegrationTest,
  ExactWalChildThroughExtentMetadataAndLogicalRecordIdentity) {
    for (const bool sparse : {false, true}) {
        for (const auto encoding :
             {compression::codec_id::none, compression::codec_id::lz4}) {
            for (const std::size_t width : {7U, 67U}) {
                SCOPED_TRACE(
                  ::testing::Message{} << "sparse=" << sparse << " codec="
                                       << static_cast<unsigned>(encoding)
                                       << " width=" << width);
                seastar::abort_source abort;
                codec::cooperative_work work{codec::limits::defaults(), abort};
                const auto context = sc(
                  static_cast<std::uint8_t>(sparse ? 0x80 : 0x30),
                  sparse ? 11U : 9U);
                const auto expected_header = segment_header::make(
                                               context,
                                               model::range_logical_end{100},
                                               alignment())
                                               .value();
                auto header_bytes = encode_segment_header(
                                      expected_header,
                                      work,
                                      operation_budget().operation_remaining,
                                      charge)
                                      .get()
                                      .value();
                fragmented_buffer_parser header_input{std::move(header_bytes)};
                const auto header = decode_segment_header(
                                      header_input,
                                      expected_header,
                                      {},
                                      reserve_current(header_input, work),
                                      work)
                                      .get()
                                      .value();
                ASSERT_TRUE(header_input.at_end());
                const auto first_position = header.bytes.end().value()
                                            + (sparse ? 1024U : 0U);
                const std::uint64_t physical = sparse ? 23 : 17;
                const std::uint64_t retained = sparse ? 2 : 5;
                const segment_history_context h{
                  context,
                  alignment(),
                  header.bytes.end(),
                  model::range_logical_end{100},
                  model::segment_relative_end{physical}};
                const auto first_location = location(
                  h, 4, physical, first_position);
                std::array<std::string, 2> blocks;
                blocks[0] = make_block(
                  4, 100, sparse, encoding, first_location, work, width);
                const auto prefix_coverage = scope(
                  100,
                  105,
                  physical,
                  physical + retained,
                  first_position,
                  first_position + blocks[0].size());
                auto prefix = extent_verifier::make(
                                h,
                                prefix_coverage,
                                work.policy(),
                                sparse ? extent_layout_kind::rewrite
                                       : extent_layout_kind::initial_append)
                                .value();
                {
                    auto bytes = buffer(blocks[0], width);
                    const auto memory = reserve_current(bytes, work);
                    prefix
                      .add_block(
                        std::move(bytes), original_expected(4), memory, work)
                      .get()
                      .value();
                }
                const auto prefix_proof = prefix.finish(work).value();
                const footer_expectation middle_location{
                  h, prefix_coverage.bytes().end()};
                auto middle = encode_durable_footer(
                                prefix_proof,
                                middle_location,
                                work,
                                operation_budget().operation_remaining,
                                charge)
                                .get()
                                .value();
                const auto middle_wire = flat(middle);
                const auto second_location = location(
                  h,
                  5,
                  physical + retained,
                  middle_location.position.value() + middle_wire.size());
                blocks[1] = make_block(
                  5, 105, sparse, encoding, second_location, work, width);
                const auto coverage = scope(
                  100,
                  110,
                  physical,
                  physical + 2 * retained,
                  first_position,
                  second_location.location.position().value()
                    + blocks[1].size());
                auto extent = extent_verifier::make(
                                h,
                                coverage,
                                work.policy(),
                                sparse ? extent_layout_kind::rewrite
                                       : extent_layout_kind::initial_append,
                                {},
                                extent_integrity::crc32c_and_sha256)
                                .value();
                const auto add_data = [&](std::size_t i) {
                    auto bytes = buffer(blocks[i], width);
                    const auto memory = reserve_current(bytes, work);
                    extent
                      .add_block(
                        std::move(bytes),
                        original_expected(4U + i),
                        memory,
                        work)
                      .get()
                      .value();
                };
                add_data(0);
                const auto footer_memory = reserve_current(middle, work);
                extent
                  .add_footer(
                    std::move(middle), footer_memory, work, prefix_proof)
                  .get()
                  .value();
                add_data(1);
                const auto evidence = extent.finish(work).value();
                EXPECT_EQ(evidence.boundary().coverage, coverage);
                EXPECT_EQ(evidence.boundary().block_count, 2U);
                EXPECT_EQ(
                  evidence.boundary().data_crc32c,
                  crc(blocks[0] + middle_wire + blocks[1]));
                ASSERT_TRUE(evidence.digest());
                EXPECT_EQ(
                  evidence.digest()->bytes(),
                  exact_sha(blocks[0] + middle_wire + blocks[1]));
                const footer_expectation footer_location{
                  h, coverage.bytes().end()};
                auto final = encode_durable_footer(
                               evidence,
                               footer_location,
                               work,
                               operation_budget().operation_remaining,
                               charge)
                               .get()
                               .value();
                fragmented_buffer_parser footer_input{std::move(final)};
                const auto footer = decode_durable_footer(
                                      footer_input,
                                      footer_location,
                                      reserve_current(footer_input, work),
                                      work)
                                      .get()
                                      .value();
                EXPECT_TRUE(footer_input.at_end());
                EXPECT_TRUE(validate_durable_footer(footer, evidence));
                const footer_expectation sealed_location{
                  h, footer.encoded_extent().end()};
                auto sealed_wire = encode_sealed_footer(
                                     evidence,
                                     sealed_location,
                                     0,
                                     {},
                                     work,
                                     operation_budget().operation_remaining,
                                     charge)
                                     .get()
                                     .value();
                const auto sealed_pin = codec::immutable_object_digest{
                  exact_sha(flat(sealed_wire.bytes))};
                EXPECT_EQ(sealed_wire.digest, sealed_pin);
                fragmented_buffer_parser sealed_input{
                  std::move(sealed_wire.bytes)};
                auto sealed = decode_sealed_footer(
                                sealed_input,
                                sealed_location,
                                sealed_pin,
                                reserve_current(sealed_input, work),
                                work)
                                .get()
                                .value();
                EXPECT_TRUE(sealed_input.at_end());
                EXPECT_TRUE(validate_sealed_footer(sealed.value, evidence));
                EXPECT_EQ(sealed.value.block_count(), 2U);
                const std::array locations{first_location, second_location};
                verify_metadata(evidence, blocks, locations, work);
            }
        }
    }
}

TEST(FormatIntegrationTest, FullRemovalRetainsOnlyCoverageAndCompletedResults) {
    for (const bool sparse : {false, true}) {
        SCOPED_TRACE(sparse);
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        // Supplied completed facts precede removal. Neither removing payload
        // nor finishing an extent is evidence that a request completed.
        const std::array completed{
          completed_retry::make(
            batch_identity(4),
            original_digest(4),
            original_binding(),
            manifest::logical(100, 105),
            model::segment_generation::make(9).value())
            .value(),
          completed_retry::make(
            batch_identity(5),
            original_digest(5),
            original_binding(),
            manifest::logical(105, 110),
            model::segment_generation::make(9).value())
            .value()};
        std::optional<model::range_logical_span> removed_span;
        for (const auto& prior : completed) {
            auto original = submission(prior.id().sequence().value(), work);
            auto assigned = model::assigned_batch::assign(
                              std::move(original),
                              prior.returned_span().begin(),
                              original_binding())
                              .value();
            if (sparse)
                assigned
                  = model::rewrite_assigned_batch(
                      std::move(assigned), selected, operation_budget(), work)
                      .get()
                      .value();
            const auto removed
              = model::remove_all_records(std::move(assigned), work).get();
            ASSERT_TRUE(removed.has_value());
            EXPECT_EQ(removed->submitted().id(), prior.id());
            EXPECT_EQ(removed->submitted().binding(), prior.original_binding());
            EXPECT_EQ(removed->submitted().original_count().value(), 5U);
            EXPECT_EQ(
              removed->submitted().original_timestamp_base().unix_nanoseconds(),
              100);
            EXPECT_EQ(removed->logical_span(), prior.returned_span());
            EXPECT_EQ(removed->fingerprint(), prior.submitted_digest());
            if (removed_span) {
                ASSERT_EQ(removed_span->end(), removed->logical_span().begin());
                removed_span = model::range_logical_span::make(
                                 removed_span->begin(),
                                 removed->logical_span().end())
                                 .value();
            } else {
                removed_span = removed->logical_span();
            }
        }
        const segment_history_context h{
          sc(0x80, 11),
          alignment(),
          runtime::file_position{512},
          model::range_logical_end{100},
          model::segment_relative_end{23}};
        const auto coverage = scope(
          removed_span->begin().value(),
          removed_span->end().value(),
          23,
          23,
          1536,
          1536);
        auto extent = extent_verifier::make(
                        h,
                        coverage,
                        work.policy(),
                        extent_layout_kind::rewrite,
                        {},
                        extent_integrity::crc32c_and_sha256)
                        .value();
        const auto evidence = extent.finish(work).value();
        EXPECT_EQ(evidence.boundary().coverage, coverage);
        EXPECT_FALSE(coverage.logical().empty());
        EXPECT_TRUE(coverage.physical().empty());
        EXPECT_TRUE(coverage.bytes().empty());
        EXPECT_EQ(evidence.boundary().block_count, 0U);
        EXPECT_FALSE(evidence.boundary().last_block);
        EXPECT_EQ(evidence.boundary().data_crc32c, 0U);
        ASSERT_TRUE(evidence.digest());
        EXPECT_EQ(evidence.digest()->bytes(), exact_sha(""));

        const footer_expectation root_location{h, coverage.bytes().end()};
        std::vector<page_ref> refs;
        refs.reserve(completed.size());
        std::array<std::string, 2> pages;
        for (std::uint32_t i = 0; i < completed.size(); ++i) {
            auto encoded = encode_retry_page(
                             std::span{completed}.subspan(i, 1),
                             root_location,
                             page_ordinal::make(i).value(),
                             i,
                             work,
                             operation_budget().operation_remaining,
                             charge)
                             .get()
                             .value();
            pages[i] = flat(encoded.bytes);
            EXPECT_EQ(encoded.reference.digest().bytes(), exact_sha(pages[i]));
            refs.push_back(encoded.reference);
        }
        {
            auto encoded = encode_sealed_footer(
                             evidence,
                             root_location,
                             2,
                             refs,
                             work,
                             operation_budget().operation_remaining,
                             charge)
                             .get()
                             .value();
            const codec::immutable_object_digest pin{
              exact_sha(flat(encoded.bytes))};
            EXPECT_EQ(encoded.digest, pin);
            fragmented_buffer_parser input{std::move(encoded.bytes)};
            const auto root
              = decode_sealed_footer(
                  input, root_location, pin, reserve_current(input, work), work)
                  .get()
                  .value();
            EXPECT_TRUE(input.at_end());
            EXPECT_TRUE(validate_sealed_footer(root.value, evidence));
            EXPECT_EQ(root.value.coverage(), coverage);
            EXPECT_EQ(root.value.block_count(), 0U);
            EXPECT_FALSE(root.value.last_block());
            EXPECT_EQ(root.value.retry_count(), 2U);
            retry_summary_verifier walk{root.value, work.policy()};
            for (std::size_t i = 0; i < pages.size(); ++i) {
                fragmented_buffer_parser page_input{buffer(pages[i], 7)};
                const auto page
                  = walk
                      .next(
                        page_input,
                        codec::reserve_decode_input(
                          page_input, work.policy(), root.remaining)
                          .value(),
                        work)
                      .get()
                      .value();
                EXPECT_TRUE(page_input.at_end());
                ASSERT_EQ(page.value.entries().size(), 1U);
                EXPECT_EQ(page.value.entries()[0], completed[i]);
                EXPECT_EQ(page.value.entries()[0].ack_generation().value(), 9U);
                EXPECT_EQ(
                  root.value.location().history.segment.generation().value(),
                  11U);
            }
            const auto complete = walk.finish(work).value();
            EXPECT_EQ(complete.entry_count(), 2U);
            EXPECT_EQ(complete.root_digest(), pin);
        }
        {
            const auto context
              = sparse_index_context::make(
                  h.segment, coverage, *evidence.digest(), h.alignment)
                  .value();
            auto encoded = encode_sparse_index_root(
                             context,
                             0,
                             {},
                             work,
                             operation_budget().operation_remaining,
                             charge)
                             .get()
                             .value();
            const codec::immutable_object_digest pin{
              exact_sha(flat(encoded.bytes))};
            EXPECT_EQ(encoded.digest, pin);
            fragmented_buffer_parser input{std::move(encoded.bytes)};
            const auto root
              = decode_sparse_index_root(
                  input, context, pin, reserve_current(input, work), work)
                  .get()
                  .value();
            EXPECT_TRUE(input.at_end());
            EXPECT_TRUE(root.value.pages().empty());
            EXPECT_TRUE(validate_sparse_index_extent(root.value, evidence));
            sparse_index_verifier walk{root.value, work.policy()};
            const auto complete = walk.finish(work).value();
            EXPECT_EQ(complete.entry_count(), 0U);
            EXPECT_EQ(complete.root_digest(), pin);
        }
        verify_manifest(evidence, work);
    }
}

TEST(
  FormatIntegrationTest,
  SemanticDigestExcludesCompressionAndPhysicalPlacement) {
    std::optional<codec::sha256_digest> baseline;
    for (const unsigned mode : {0U, 1U, 2U, 3U}) {
        for (const std::size_t width : {7U, 67U}) {
            SCOPED_TRACE(
              ::testing::Message{} << "mode=" << mode << " width=" << width);
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            const bool sparse = mode == 3;
            const segment_history_context h{
              sc(
                static_cast<std::uint8_t>(sparse ? 0x80 : 0x30),
                sparse ? 11U : 9U),
              alignment(),
              runtime::file_position{512},
              model::range_logical_end{100},
              model::segment_relative_end{17}};
            const auto expected = location(
              h, 4, mode == 2 ? 23 : 17, mode == 2 ? 1536 : 512);
            // make_block checks the independent original digest after each
            // owning decode, including the sparse carried-proof distinction.
            const auto wire = make_block(
              4,
              100,
              sparse,
              mode == 1 ? compression::codec_id::lz4
                        : compression::codec_id::none,
              expected,
              work,
              width);
            auto verifier = extent_verifier::make(
                              h,
                              scope(
                                100,
                                105,
                                expected.location.physical_begin().value(),
                                expected.location.physical_begin().value()
                                  + (sparse ? 2U : 5U),
                                expected.location.position().value(),
                                expected.location.position().value()
                                  + wire.size()),
                              work.policy(),
                              extent_layout_kind::rewrite,
                              {},
                              extent_integrity::crc32c_and_sha256)
                              .value();
            auto stored = buffer(wire, width);
            const auto memory = reserve_current(stored, work);
            verifier
              .add_block(std::move(stored), original_expected(4), memory, work)
              .get()
              .value();
            const auto evidence = verifier.finish(work).value();
            ASSERT_TRUE(evidence.digest());
            const auto exact = exact_sha(wire);
            EXPECT_EQ(evidence.digest()->bytes(), exact);
            if (!baseline) baseline = exact;
            if (mode == 0)
                EXPECT_EQ(exact, *baseline);
            else
                EXPECT_NE(exact, *baseline);
        }
    }
}
} // namespace
} // namespace kwaque::storage
