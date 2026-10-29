#include "src/base/units.h"
#include "src/storage/segment_writer_state.h"
#include "src/storage/tests/local_installation_contract.h"
#include "src/storage/tests/segment_test_support.h"

#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <array>
#include <cstdint>
#include <stdexcept>
#include <utility>

namespace {
using namespace kwaque;
using namespace kwaque::storage;
using namespace kwaque::storage::testing;

local_segment_descriptor descriptor() {
    return {
      sc(),
      {},
      alignment(),
      storage_profile::v1,
      1,
      local_layout_kind::initial,
      default_retry_depth,
      byte_count{1_MiB},
      runtime::monotonic_duration{1'000'000}};
}
seastar::future<encoded_assigned_batch> child(
  codec::cooperative_work& work,
  bool sparse = false,
  std::uint64_t logical = 100) {
    auto wire = assigned_wire(false, 4096, sparse);
    if (logical != 100) {
        put(wire, 4096 + 168, logical, 8);
        put(wire, 4096 + 176, logical + 1, 8);
        repair(wire);
    }
    auto raw = co_await installation_contract::buffer_async(std::move(wire));
    auto value = co_await validate_encoded_assigned_batch(
      std::move(raw), batch_expected(), budget(), work);
    if (!value)
        throw std::runtime_error("independent capacity child fixture rejected");
    co_return std::move(*value);
}
} // namespace

SEASTAR_TEST_CASE(
  segment_capacity_counts_header_group_footer_root_and_retry_pages) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const std::array input{co_await child(work)};
    auto description = descriptor();
    const segment_writer_position empty{
      model::range_logical_end{100}, {}, runtime::file_position{512}};
    auto plan = plan_segment_capacity(
                  description,
                  runtime::file_position{512},
                  empty,
                  input,
                  {},
                  work.policy(),
                  charge,
                  alignment(4096),
                  byte_count{256_KiB})
                  .value();
    BOOST_REQUIRE(plan.decision == segment_capacity_decision::fits);
    // The block packs (32 + 144 + a 4,287-byte child); the footer after it
    // (32 + 216) pads the span to the boundary.
    BOOST_CHECK_EQUAL(plan.blocks.value(), 4463U);
    BOOST_CHECK_EQUAL(plan.footer.value(), 657U);
    BOOST_CHECK_EQUAL(plan.sealed_root.value(), 512U);
    BOOST_CHECK_EQUAL(plan.disk.data.value(), 6144U);
    BOOST_CHECK_EQUAL(plan.disk.retry_bundle.value(), 512U);
    BOOST_CHECK_EQUAL(plan.disk.publication_overlap.value(), 8192U);
    BOOST_CHECK_EQUAL(plan.disk.descriptor.value(), 4096U);
    BOOST_REQUIRE(plan.end.has_value());
    BOOST_CHECK_EQUAL(plan.end->logical.value(), 101U);
    BOOST_CHECK_EQUAL(plan.end->physical.value(), 1U);
    BOOST_CHECK_EQUAL(plan.end->bytes.value(), 5632U);
    description.maximum_data_bytes = byte_count{6_KiB};
    const auto exact = plan_segment_capacity(
                         description,
                         runtime::file_position{512},
                         empty,
                         input,
                         {},
                         work.policy(),
                         charge,
                         alignment(4096),
                         byte_count{256_KiB})
                         .value();
    BOOST_CHECK(exact.decision == segment_capacity_decision::fits);
    BOOST_CHECK(exact.reasons.empty());
    description.maximum_data_bytes = byte_count{6143};
    const auto impossible = plan_segment_capacity(
                              description,
                              runtime::file_position{512},
                              empty,
                              input,
                              {},
                              work.policy(),
                              charge,
                              alignment(4096),
                              byte_count{256_KiB})
                              .value();
    BOOST_CHECK(impossible.decision == segment_capacity_decision::impossible);
    // Too large for an empty segment is no reason to roll.
    BOOST_CHECK(impossible.reasons.empty());
    description.maximum_data_bytes = byte_count{6_KiB};
    auto with_footer = empty;
    with_footer.bytes = runtime::file_position{1024};
    with_footer.footers = 1;
    const auto roll = plan_segment_capacity(
                        description,
                        runtime::file_position{512},
                        with_footer,
                        input,
                        {},
                        work.policy(),
                        charge,
                        alignment(4096),
                        byte_count{256_KiB})
                        .value();
    BOOST_CHECK(roll.decision == segment_capacity_decision::roll_required);
    BOOST_CHECK(
      roll.reasons == segment_roll_reasons{segment_roll_reason::size});
}

// k blocks of one group lie back to back from the span's aligned start and
// one footer pads them to the next boundary: the span is
// ceil((k * (c + 176) + 248) / A) units, at 512 and at 4,096.
SEASTAR_TEST_CASE(segment_capacity_packs_blocks_under_one_padded_footer) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    std::array<encoded_assigned_batch, 3> input{
      co_await child(work, false, 100),
      co_await child(work, false, 101),
      co_await child(work, false, 102)};
    for (const std::uint64_t unit : {512U, 4096U}) {
        auto description = descriptor();
        description.alignment = alignment(unit);
        const runtime::file_position start{unit};
        const segment_writer_position empty{
          model::range_logical_end{100}, {}, start};
        for (std::size_t k = 1; k <= input.size(); ++k) {
            const auto plan = plan_segment_capacity(
                                description,
                                start,
                                empty,
                                std::span{input}.first(k),
                                {},
                                work.policy(),
                                charge,
                                alignment(4096),
                                byte_count{256_KiB})
                                .value();
            BOOST_REQUIRE(plan.decision == segment_capacity_decision::fits);
            const auto blocks = k * 4463U;
            const auto span = (blocks + 248U + unit - 1U) / unit * unit;
            BOOST_CHECK_EQUAL(plan.blocks.value(), blocks);
            BOOST_CHECK_EQUAL(plan.footer.value(), span - blocks);
            BOOST_REQUIRE(plan.end.has_value());
            BOOST_CHECK_EQUAL(plan.end->bytes.value(), unit + span);
            BOOST_CHECK_EQUAL(plan.end->blocks, k);
            BOOST_CHECK_EQUAL(plan.end->footers, 1U);
        }
    }
}

// A block is at least 399 bytes: 32 + 144 around a child of one record with
// no key, value or header (32 + 184 + 7). Blocks pack, so a segment of D
// bytes holds at most D / 399 facts, and its disk bound keeps room for the
// retry bundle of that many, as the plan of the group that reaches that
// count projects it.
SEASTAR_TEST_CASE(segment_disk_bound_counts_minimal_packed_blocks) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto description = descriptor();
    segment_admission_limits limits;
    limits.metadata_bytes = byte_count{1_MiB};
    const auto bound
      = segment_disk_bound(
          description, limits, work.policy(), charge, byte_count{256_KiB})
          .value();
    const std::uint32_t facts = 1_MiB / 399;
    const std::array input{co_await child(work, false, 100U + facts - 1U)};
    const segment_writer_position current{
      model::range_logical_end{100U + facts - 1U},
      model::segment_relative_end{facts - 1U},
      runtime::file_position{512},
      facts - 1U,
      facts - 1U,
      facts - 1U};
    const auto plan = plan_segment_capacity(
                        description,
                        runtime::file_position{512},
                        current,
                        input,
                        limits,
                        work.policy(),
                        charge,
                        alignment(4096),
                        byte_count{256_KiB})
                        .value();
    BOOST_REQUIRE(plan.decision == segment_capacity_decision::fits);
    BOOST_CHECK_EQUAL(bound.value(), 1_MiB + plan.disk.retry_bundle.value());
}

SEASTAR_TEST_CASE(
  segment_capacity_rejects_relocation_and_intersects_counts_and_metadata) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const std::array input{co_await child(work)};
    const std::array sparse{co_await child(work, true)};
    auto description = descriptor();
    const segment_writer_position empty{
      model::range_logical_end{100}, {}, runtime::file_position{512}};
    const auto relocated = plan_segment_capacity(
      description,
      runtime::file_position{512},
      empty,
      sparse,
      {},
      work.policy(),
      charge,
      alignment(4096),
      byte_count{256_KiB});
    BOOST_REQUIRE(!relocated);
    BOOST_CHECK(relocated.error().code() == errc::invalid_argument);
    segment_admission_limits limits;
    limits.maximum_retry_entries = 1;
    auto full = empty;
    full.retry_entries = 1;
    auto plan = plan_segment_capacity(
                  description,
                  runtime::file_position{512},
                  full,
                  input,
                  limits,
                  work.policy(),
                  charge,
                  alignment(4096),
                  byte_count{256_KiB})
                  .value();
    BOOST_CHECK(plan.decision == segment_capacity_decision::roll_required);
    BOOST_CHECK(
      plan.reasons == segment_roll_reasons{segment_roll_reason::retry_state});
    full = empty;
    full.blocks = UINT32_MAX;
    plan = plan_segment_capacity(
             description,
             runtime::file_position{512},
             full,
             input,
             limits,
             work.policy(),
             charge,
             alignment(4096),
             byte_count{256_KiB})
             .value();
    BOOST_CHECK(plan.decision == segment_capacity_decision::roll_required);
    BOOST_CHECK(
      plan.reasons == segment_roll_reasons{segment_roll_reason::count});
    full = empty;
    full.footers = UINT32_MAX;
    plan = plan_segment_capacity(
             description,
             runtime::file_position{512},
             full,
             input,
             limits,
             work.policy(),
             charge,
             alignment(4096),
             byte_count{256_KiB})
             .value();
    BOOST_CHECK(plan.decision == segment_capacity_decision::roll_required);
    BOOST_CHECK(
      plan.reasons == segment_roll_reasons{segment_roll_reason::count});
    // Reasons that hold together are each given.
    full = empty;
    full.blocks = UINT32_MAX;
    full.retry_entries = 1;
    plan = plan_segment_capacity(
             description,
             runtime::file_position{512},
             full,
             input,
             limits,
             work.policy(),
             charge,
             alignment(4096),
             byte_count{256_KiB})
             .value();
    BOOST_CHECK(plan.decision == segment_capacity_decision::roll_required);
    BOOST_CHECK(
      plan.reasons
      == segment_roll_reasons{segment_roll_reason::count}.add(
        segment_roll_reason::retry_state));
    full = empty;
    full.blocks = UINT32_MAX;
    full.bytes = runtime::file_position{1_MiB - 512};
    plan = plan_segment_capacity(
             description,
             runtime::file_position{512},
             full,
             input,
             limits,
             work.policy(),
             charge,
             alignment(4096),
             byte_count{256_KiB})
             .value();
    BOOST_CHECK(plan.decision == segment_capacity_decision::roll_required);
    BOOST_CHECK(
      plan.reasons
      == segment_roll_reasons{segment_roll_reason::size}.add(
        segment_roll_reason::count));
    limits.metadata_bytes = byte_count{1};
    plan = plan_segment_capacity(
             description,
             runtime::file_position{512},
             empty,
             input,
             limits,
             work.policy(),
             charge,
             alignment(4096),
             byte_count{256_KiB})
             .value();
    BOOST_CHECK(plan.decision == segment_capacity_decision::impossible);
    auto config = work.policy().config();
    config.max_page_bytes = byte_count{128};
    const auto narrow = codec::limits::make(config).value();
    plan = plan_segment_capacity(
             description,
             runtime::file_position{512},
             empty,
             input,
             {},
             narrow,
             charge,
             alignment(4096),
             byte_count{256_KiB})
             .value();
    BOOST_CHECK(plan.decision == segment_capacity_decision::impossible);
}

SEASTAR_TEST_CASE(segment_capacity_honors_page_count_and_terminal_logical_end) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const std::array next{co_await child(work, false, 508)};
    const auto description = descriptor();
    segment_admission_limits limits;
    limits.maximum_retry_pages = 1;
    limits.metadata_bytes = byte_count{1_MiB};
    const segment_writer_position current{
      model::range_logical_end{508},
      model::segment_relative_end{408},
      runtime::file_position{212992},
      408,
      7,
      408};
    const auto full = plan_segment_capacity(
                        description,
                        runtime::file_position{512},
                        current,
                        next,
                        limits,
                        work.policy(),
                        charge,
                        alignment(4096),
                        byte_count{256_KiB})
                        .value();
    BOOST_CHECK(full.decision == segment_capacity_decision::roll_required);
    BOOST_CHECK(
      full.reasons == segment_roll_reasons{segment_roll_reason::retry_state});
    limits.maximum_retry_pages = 2;
    const auto fits = plan_segment_capacity(
                        description,
                        runtime::file_position{512},
                        current,
                        next,
                        limits,
                        work.policy(),
                        charge,
                        alignment(4096),
                        byte_count{256_KiB})
                        .value();
    BOOST_CHECK(fits.decision == segment_capacity_decision::fits);
    BOOST_CHECK(fits.reasons.empty());
    BOOST_CHECK_EQUAL(fits.retry_pages, 2U);
    const std::array terminal{co_await child(work, false, UINT64_MAX - 1)};
    const segment_writer_position empty{
      model::range_logical_end{UINT64_MAX - 1},
      {},
      runtime::file_position{512}};
    const auto end = plan_segment_capacity(
                       description,
                       runtime::file_position{512},
                       empty,
                       terminal,
                       limits,
                       work.policy(),
                       charge,
                       alignment(4096),
                       byte_count{256_KiB})
                       .value();
    BOOST_REQUIRE(end.decision == segment_capacity_decision::fits);
    BOOST_CHECK_EQUAL(end.end->logical.value(), UINT64_MAX);
    BOOST_CHECK_EQUAL(end.end->physical.value(), 1U);
}

SEASTAR_TEST_CASE(
  segment_capacity_intersects_finalization_workspace_before_admission) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const std::array input{co_await child(work, false, 508)};
    const auto description = descriptor();
    segment_admission_limits limits;
    limits.metadata_bytes = byte_count{1_MiB};
    const segment_writer_position current{
      model::range_logical_end{508},
      model::segment_relative_end{408},
      runtime::file_position{212992},
      408,
      7,
      408};
    const auto wide = plan_segment_capacity(
                        description,
                        runtime::file_position{512},
                        current,
                        input,
                        limits,
                        work.policy(),
                        charge,
                        alignment(4096),
                        byte_count{256_KiB})
                        .value();
    const auto narrow = plan_segment_capacity(
                          description,
                          runtime::file_position{512},
                          current,
                          input,
                          limits,
                          work.policy(),
                          charge,
                          alignment(4096),
                          byte_count{64_KiB})
                          .value();
    BOOST_REQUIRE(wide.decision == segment_capacity_decision::fits);
    BOOST_REQUIRE(narrow.decision == segment_capacity_decision::fits);
    BOOST_CHECK_LT(narrow.retry_page_entries, wide.retry_page_entries);
    BOOST_CHECK_GT(narrow.retry_pages, wide.retry_pages);
    const auto impossible = plan_segment_capacity(
                              description,
                              runtime::file_position{512},
                              current,
                              input,
                              limits,
                              work.policy(),
                              charge,
                              alignment(4096),
                              byte_count{4_KiB})
                              .value();
    BOOST_CHECK(impossible.decision == segment_capacity_decision::impossible);
}

SEASTAR_TEST_CASE(segment_capacity_full_summary_and_ref_memory_intersections) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto description = descriptor();
    description.maximum_data_bytes = byte_count{128_MiB};
    segment_admission_limits limits;
    limits.metadata_bytes = byte_count{1_MiB};
    const segment_writer_position current{
      model::range_logical_end{100 + maximum_object_entries - 1},
      model::segment_relative_end{maximum_object_entries - 1},
      runtime::file_position{512U + (maximum_object_entries - 1ULL) * 1024U},
      maximum_object_entries - 1,
      maximum_object_entries - 1,
      maximum_object_entries - 1};
    const std::array input{
      co_await child(work, false, current.logical.value())};
    const auto full = plan_segment_capacity(
      description,
      runtime::file_position{512},
      current,
      input,
      limits,
      work.policy(),
      charge,
      alignment(4096),
      byte_count{256_KiB});
    BOOST_REQUIRE(full.has_value());
    BOOST_REQUIRE(full->decision == segment_capacity_decision::fits);
    BOOST_REQUIRE(full->end.has_value());
    BOOST_CHECK_EQUAL(full->end->retry_entries, maximum_object_entries);
    BOOST_CHECK(
      full->retry_pages > 1 && full->retry_pages <= maximum_object_pages);
    BOOST_CHECK_GT(full->disk.retry_bundle.value(), 65536U);
    BOOST_CHECK_GT(full->sealed_root.value(), 512U);
    auto exhausted = current;
    exhausted.retry_entries = maximum_object_entries;
    const auto over = plan_segment_capacity(
      description,
      runtime::file_position{512},
      exhausted,
      input,
      limits,
      work.policy(),
      charge,
      alignment(4096),
      byte_count{256_KiB});
    BOOST_REQUIRE(over.has_value());
    BOOST_CHECK(over->decision == segment_capacity_decision::roll_required);
    BOOST_CHECK(
      over->reasons == segment_roll_reasons{segment_roll_reason::retry_state});
    // A batch whose stream already holds its depth of entries takes none, so
    // the segment still takes it.
    const auto held = plan_segment_capacity(
      description,
      runtime::file_position{512},
      exhausted,
      input,
      limits,
      work.policy(),
      charge,
      alignment(4096),
      byte_count{256_KiB},
      0);
    BOOST_REQUIRE(held.has_value());
    BOOST_REQUIRE(held->decision == segment_capacity_decision::fits);
    BOOST_CHECK_EQUAL(held->end->retry_entries, maximum_object_entries);
    limits.metadata_bytes = byte_count{8_KiB};
    const auto refs = plan_segment_capacity(
      description,
      runtime::file_position{512},
      current,
      input,
      limits,
      work.policy(),
      charge,
      alignment(4096),
      byte_count{256_KiB});
    BOOST_REQUIRE(refs.has_value());
    BOOST_CHECK(refs->decision == segment_capacity_decision::roll_required);
    BOOST_CHECK(
      refs->reasons == segment_roll_reasons{segment_roll_reason::retry_state});
    limits.metadata_bytes = byte_count{1_MiB};
    limits.maximum_retry_pages = 1;
    const auto pages = plan_segment_capacity(
      description,
      runtime::file_position{512},
      current,
      input,
      limits,
      work.policy(),
      charge,
      alignment(4096),
      byte_count{256_KiB});
    BOOST_REQUIRE(pages.has_value());
    BOOST_CHECK(pages->decision == segment_capacity_decision::roll_required);
    BOOST_CHECK(
      pages->reasons == segment_roll_reasons{segment_roll_reason::retry_state});
}
