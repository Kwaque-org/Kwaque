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
      model::range_logical_end{100},
      {},
      alignment(),
      storage_profile::v1,
      1,
      local_layout_kind::initial,
      byte_count{1U << 20U},
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
                  byte_count{262144})
                  .value();
    BOOST_REQUIRE(plan.decision == segment_capacity_decision::fits);
    BOOST_CHECK_EQUAL(plan.blocks.value(), 4608U);
    BOOST_CHECK_EQUAL(plan.footer.value(), 512U);
    BOOST_CHECK_EQUAL(plan.sealed_root.value(), 512U);
    BOOST_CHECK_EQUAL(plan.disk.data.value(), 6144U);
    BOOST_CHECK_EQUAL(plan.disk.retry_bundle.value(), 512U);
    BOOST_CHECK_EQUAL(plan.disk.publication_overlap.value(), 8192U);
    BOOST_CHECK_EQUAL(plan.disk.descriptor.value(), 4096U);
    BOOST_REQUIRE(plan.end.has_value());
    BOOST_CHECK_EQUAL(plan.end->logical.value(), 101U);
    BOOST_CHECK_EQUAL(plan.end->physical.value(), 1U);
    BOOST_CHECK_EQUAL(plan.end->bytes.value(), 5632U);
    description.maximum_data_bytes = byte_count{6144};
    const auto exact = plan_segment_capacity(
                         description,
                         runtime::file_position{512},
                         empty,
                         input,
                         {},
                         work.policy(),
                         charge,
                         alignment(4096),
                         byte_count{262144})
                         .value();
    BOOST_CHECK(exact.decision == segment_capacity_decision::fits);
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
                              byte_count{262144})
                              .value();
    BOOST_CHECK(impossible.decision == segment_capacity_decision::impossible);
    description.maximum_data_bytes = byte_count{6144};
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
                        byte_count{262144})
                        .value();
    BOOST_CHECK(roll.decision == segment_capacity_decision::roll_required);
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
      byte_count{262144});
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
                  byte_count{262144})
                  .value();
    BOOST_CHECK(plan.decision == segment_capacity_decision::roll_required);
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
             byte_count{262144})
             .value();
    BOOST_CHECK(plan.decision == segment_capacity_decision::roll_required);
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
             byte_count{262144})
             .value();
    BOOST_CHECK(plan.decision == segment_capacity_decision::roll_required);
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
             byte_count{262144})
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
             byte_count{262144})
             .value();
    BOOST_CHECK(plan.decision == segment_capacity_decision::impossible);
}

SEASTAR_TEST_CASE(segment_capacity_honors_page_count_and_terminal_logical_end) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const std::array next{co_await child(work, false, 508)};
    auto description = descriptor();
    segment_admission_limits limits;
    limits.maximum_retry_pages = 1;
    limits.metadata_bytes = byte_count{1U << 20U};
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
                        byte_count{262144})
                        .value();
    BOOST_CHECK(full.decision == segment_capacity_decision::roll_required);
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
                        byte_count{262144})
                        .value();
    BOOST_CHECK(fits.decision == segment_capacity_decision::fits);
    BOOST_CHECK_EQUAL(fits.retry_pages, 2U);
    const std::array terminal{co_await child(work, false, UINT64_MAX - 1)};
    description.logical_origin = model::range_logical_end{UINT64_MAX - 1};
    const segment_writer_position empty{
      description.logical_origin, {}, runtime::file_position{512}};
    const auto end = plan_segment_capacity(
                       description,
                       runtime::file_position{512},
                       empty,
                       terminal,
                       limits,
                       work.policy(),
                       charge,
                       alignment(4096),
                       byte_count{262144})
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
    limits.metadata_bytes = byte_count{1U << 20U};
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
                        byte_count{262144})
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
                          byte_count{65536})
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
                              byte_count{4096})
                              .value();
    BOOST_CHECK(impossible.decision == segment_capacity_decision::impossible);
}

SEASTAR_TEST_CASE(segment_capacity_full_summary_and_ref_memory_intersections) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto description = descriptor();
    description.maximum_data_bytes = byte_count{128U << 20U};
    segment_admission_limits limits;
    limits.metadata_bytes = byte_count{1U << 20U};
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
      byte_count{262144});
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
      byte_count{262144});
    BOOST_REQUIRE(over.has_value());
    BOOST_CHECK(over->decision == segment_capacity_decision::roll_required);
    limits.metadata_bytes = byte_count{8192};
    const auto refs = plan_segment_capacity(
      description,
      runtime::file_position{512},
      current,
      input,
      limits,
      work.policy(),
      charge,
      alignment(4096),
      byte_count{262144});
    BOOST_REQUIRE(refs.has_value());
    BOOST_CHECK(refs->decision == segment_capacity_decision::roll_required);
    limits.metadata_bytes = byte_count{1U << 20U};
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
      byte_count{262144});
    BOOST_REQUIRE(pages.has_value());
    BOOST_CHECK(pages->decision == segment_capacity_decision::roll_required);
}
