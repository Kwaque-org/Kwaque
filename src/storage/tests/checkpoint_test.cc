#include "src/base/units.h"
#include "src/bytes/test_allocation_profile.h"
#include "src/resource/resource_registry.h"
#include "src/storage/checkpoint.h"
#include "src/storage/tests/checkpoint_coverage_oracle.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/memory.hh>
#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <array>
#include <exception>
#include <optional>
#include <vector>

namespace {
using namespace kwaque;
using namespace kwaque::storage;

template<typename Func>
seastar::future<> with_budget(Func body) {
    auto config = resource::resource_config::from_total_memory(
      byte_count{seastar::memory::stats().total_memory()});
    BOOST_REQUIRE(config.has_value());
    resource::resource_registry registry;
    co_await registry.start(*config);
    resource::resource_manager manager{registry.handles()};
    std::exception_ptr failure;
    try {
        co_await manager.start();
        workload_budget budget{
          manager.acquire_workload(resource::workload_class::metadata),
          {.tasks = 8, .bytes = byte_count{4_MiB}, .handles = 2},
          bytes::testing::charge};
        co_await seastar::futurize_invoke(body, budget);
        BOOST_CHECK_EQUAL(budget.snapshot().bytes, 0U);
    } catch (...) {
        failure = std::current_exception();
    }
    co_await manager.stop();
    co_await registry.stop();
    if (failure) std::rethrow_exception(failure);
}

template<typename Id>
Id identity(std::uint8_t byte) {
    std::array<std::uint8_t, 16> bytes{};
    bytes.fill(byte);
    return Id::make(bytes).value();
}
local_store_context owner(std::uint32_t shard = 0) {
    return local_store_context::make(
             identity<model::cluster_id>(1),
             identity<model::broker_id>(2),
             identity<device_store_id>(3),
             shard)
      .value();
}
model::wal_incarnation_id wal(std::uint8_t file) {
    return identity<model::wal_incarnation_id>(file);
}
local_wal_cursor at(std::uint8_t file, std::uint64_t position) {
    return local_wal_cursor::make(wal(file), runtime::file_position{position})
      .value();
}
segment_context segment(std::uint64_t generation, std::uint8_t id = 6) {
    return segment_context::make(
             identity<model::cluster_id>(1),
             identity<model::topic_id>(4),
             identity<model::range_id>(5),
             identity<model::segment_id>(id),
             model::segment_generation::make(generation).value())
      .value();
}
device_store_id device(std::uint8_t byte = 9) {
    return identity<device_store_id>(byte);
}
codec::immutable_object_digest digest(unsigned char byte) {
    codec::content_digest bytes{};
    bytes.fill(byte);
    return codec::immutable_object_digest{bytes};
}
// A segment's durable footer at `footer`, covering everything before it.
local_durable_boundary boundary(
  std::uint64_t generation,
  std::uint64_t footer,
  device_store_id on = device()) {
    const model::range_logical_end logical{100};
    const model::segment_relative_end physical{0};
    const runtime::file_position start{4096};
    return local_durable_boundary{
      on,
      segment_history_context{
        segment(generation),
        storage_alignment::make(byte_count{4096}).value(),
        start,
        logical,
        physical},
      storage::coverage{
        model::range_logical_span::make(logical, logical).value(),
        model::segment_relative_span::make(physical, physical).value(),
        model::file_byte_span::make(start, runtime::file_position{footer})
          .value()},
      model::file_byte_span::from_size(
        runtime::file_position{footer}, byte_count{4096})
        .value()};
}
local_footer_reference
reference(const local_durable_boundary& value, unsigned char content) {
    return local_footer_reference::make(
             value.footer.begin(), value.footer.size(), 6, digest(content))
      .value();
}
local_checkpoint_entry
pinned(const local_durable_boundary& value, unsigned char c) {
    return local_checkpoint_entry{
      value.history.segment,
      local_checkpoint_disposition::segment_boundary,
      value.device,
      value.footer.begin().value(),
      value.footer.size(),
      6,
      digest(c)};
}
recovery_decision_record discard(
  std::uint64_t sequence, std::uint64_t generation, local_wal_cursor prepare) {
    return recovery_decision_record{
      {local_decision_sequence::make(sequence).value(),
       digest(static_cast<unsigned char>(sequence)),
       byte_count{4096}},
      {prepare,
       digest(0x70),
       segment(generation),
       runtime::file_position{8192},
       local_recovery_action::discard,
       7}};
}
local_checkpoint_entry discarded(const recovery_decision_record& record) {
    return local_checkpoint_entry{
      record.value.segment,
      local_checkpoint_disposition::authorized_discard,
      device_store_id{},
      record.pin.sequence.value(),
      record.pin.bytes,
      0,
      record.pin.digest};
}
local_object_publication published(
  std::uint64_t generation,
  local_object_state state,
  std::optional<local_footer_reference> pin = std::nullopt) {
    return local_object_publication{segment(generation), state, pin, {}};
}
template<typename Result>
void ok(const Result& done) {
    BOOST_REQUIRE(done.has_value());
}
template<typename Result>
errc code(const Result& failed) {
    BOOST_REQUIRE(!failed.has_value());
    return failed.error().code();
}
// Reads every footer of a cut as if the device held `content`.
void install_all(checkpoint_capture& cut, unsigned char content) {
    while (!cut.complete())
        ok(cut.install(reference(cut.refresh()[cut.installed()], content)));
}
} // namespace

// The cutoff is the lowest bound under the checked WAL order. With A/B/A and
// B unresolved it stops at B, whatever became durable later; a group that is
// segment-durable before its WAL barrier does not raise it.
SEASTAR_TEST_CASE(checkpoint_cutoff_is_the_lowest_bound) {
    const auto shard = owner();
    const auto cut =
      [&shard](
        local_wal_cursor durable,
        local_wal_cursor discharged,
        std::optional<local_wal_cursor> held = std::nullopt,
        std::optional<local_wal_cursor> previous = std::nullopt) {
          return checkpoint_cutoff(
            shard,
            local_obligation_snapshot{durable, discharged, 0},
            held,
            previous);
      };
    // B's first PREPARE holds the prefix below later durable groups.
    BOOST_CHECK(cut(at(1, 65536), at(1, 8192)).value() == at(1, 8192));
    // Nothing open: the discharged prefix is the reserved end, past what the
    // WAL made durable.
    BOOST_CHECK(cut(at(1, 16384), at(1, 32768)).value() == at(1, 16384));
    // A file's order comes before its positions.
    BOOST_CHECK(cut(at(2, 4096), at(1, 65536)).value() == at(1, 65536));
    BOOST_CHECK(cut(at(1, 65536), at(2, 4096)).value() == at(1, 65536));
    // A hold bounds it like an obligation, and only when it is lower.
    BOOST_CHECK(
      cut(at(2, 8192), at(2, 8192), at(1, 12288)).value() == at(1, 12288));
    BOOST_CHECK(
      cut(at(2, 8192), at(2, 8192), at(2, 16384)).value() == at(2, 8192));
    // It never falls below the durable checkpoint's end.
    BOOST_CHECK(
      cut(at(2, 8192), at(2, 8192), std::nullopt, at(2, 8192)).value()
      == at(2, 8192));
    BOOST_CHECK(
      code(cut(at(2, 8192), at(2, 4096), std::nullopt, at(2, 8192)))
      == errc::invariant_violation);
    BOOST_CHECK(
      code(cut(at(2, 8192), at(2, 8192), at(1, 4096), at(2, 4096)))
      == errc::invariant_violation);
    // Positions without one shard owner have no order.
    const auto store = owner(local_store_shard);
    BOOST_CHECK(
      code(checkpoint_cutoff(
        store,
        local_obligation_snapshot{at(1, 8192), at(1, 8192), 0},
        std::nullopt,
        std::nullopt))
      == errc::wrong_context);
    co_return;
}

// Rebuilt rows fold to one pin per segment: every pinned PREPARE it has
// across WAL files, at the oldest of them.
SEASTAR_TEST_CASE(checkpoint_folds_rebuilt_rows_per_segment) {
    const auto shard = owner();
    const auto row = [](
                       std::uint8_t file,
                       std::uint64_t generation,
                       std::uint32_t count,
                       std::optional<local_wal_cursor> first) {
        return recovery_obligation{
          wal(file), segment(generation), 3, count, first};
    };
    const std::array rows{
      row(2, 1, 1, at(2, 4096)),
      row(1, 1, 2, at(1, 8192)),
      // Satisfied PREPAREs alone pin nothing.
      row(1, 2, 0, std::nullopt),
      row(2, 2, 1, at(2, 8192))};
    const auto folded = fold_pinned_obligations(rows, shard).value();
    BOOST_REQUIRE_EQUAL(folded.size(), 2U);
    BOOST_CHECK(
      folded[0].wal == wal(1) && folded[0].segment == segment(1)
      && folded[0].pinned == 3 && folded[0].first == at(1, 8192));
    BOOST_CHECK(
      folded[1].wal == wal(2) && folded[1].segment == segment(2)
      && folded[1].pinned == 1 && folded[1].first == at(2, 8192));
    BOOST_CHECK(fold_pinned_obligations({}, shard).value().empty());

    // A pinned row names where its oldest PREPARE begins, in its own file.
    const std::array unplaced{row(1, 1, 1, std::nullopt)};
    BOOST_CHECK(
      code(fold_pinned_obligations(unplaced, shard)) == errc::invalid_argument);
    const std::array misfiled{row(1, 1, 1, at(2, 4096))};
    BOOST_CHECK(
      code(fold_pinned_obligations(misfiled, shard)) == errc::invalid_argument);
    const std::array overflow{
      row(1, 1, UINT32_MAX, at(1, 4096)), row(2, 1, 1, at(2, 4096))};
    BOOST_CHECK(
      code(fold_pinned_obligations(overflow, shard)) == errc::out_of_range);
    const std::array pair{row(1, 1, 1, at(1, 4096)), row(2, 1, 1, at(2, 4096))};
    BOOST_CHECK(
      code(fold_pinned_obligations(pair, owner(local_store_shard)))
      == errc::wrong_context);
    co_return;
}

// A segment's newest durable footer is pinned by the next cut from the bytes
// read for it, and an entry whose segment made no progress is carried into
// later cuts unchanged, with nothing to read.
SEASTAR_TEST_CASE(checkpoint_pins_refresh_and_carry_forward) {
    co_await with_budget([](workload_budget& budget) {
        const auto shard = owner();
        auto pins = checkpoint_pins::make(budget, {}).value();
        const auto first = boundary(1, 8192);
        ok(pins.observe(first));
        BOOST_CHECK(pins.entries().empty() && pins.stale() == 1);

        auto cut = pins.capture(shard, at(1, 8192)).value();
        BOOST_CHECK(
          cut.end() == at(1, 8192) && cut.entries().empty()
          && cut.refresh().size() == 1 && cut.refresh()[0] == first
          && !cut.complete());
        // Only the reference of that exact footer is its pin.
        const auto other = boundary(1, 12288);
        BOOST_CHECK(
          code(cut.install(reference(other, 1))) == errc::wrong_context);
        BOOST_CHECK(
          code(cut.install(
            local_footer_reference::make(
              first.footer.begin(), first.footer.size(), 7, digest(1))
              .value()))
          == errc::wrong_context);
        // An incomplete cut is never adopted.
        const auto incomplete = pins.adopt(std::move(cut));
        BOOST_CHECK(code(incomplete) == errc::wrong_context);

        cut = pins.capture(shard, at(1, 8192)).value();
        install_all(cut, 1);
        BOOST_CHECK(cut.complete() && cut.installed() == 1);
        BOOST_CHECK(
          code(cut.install(reference(first, 1))) == errc::invalid_argument);
        BOOST_REQUIRE_EQUAL(cut.entries().size(), 1U);
        BOOST_CHECK(cut.entries()[0] == pinned(first, 1));
        ok(pins.adopt(std::move(cut)));
        BOOST_REQUIRE_EQUAL(pins.entries().size(), 1U);
        BOOST_CHECK(pins.entries()[0] == pinned(first, 1) && pins.stale() == 0);

        // No progress: the same footer again, and a cut that reads nothing.
        ok(pins.observe(first));
        BOOST_CHECK(pins.stale() == 0);
        auto carried = pins.capture(shard, at(1, 16384)).value();
        BOOST_CHECK(
          carried.complete() && carried.refresh().empty()
          && carried.entries().size() == 1
          && carried.entries()[0] == pinned(first, 1));
        ok(pins.adopt(std::move(carried)));
        BOOST_CHECK(pins.entries()[0] == pinned(first, 1));

        // Progress replaces the segment's one entry. A footer observed while
        // the cut is in flight waits for the next cut.
        const auto second = boundary(1, 16384), third = boundary(1, 24576);
        ok(pins.observe(second));
        auto advanced = pins.capture(shard, at(1, 16384)).value();
        BOOST_CHECK(
          advanced.entries().size() == 1
          && advanced.entries()[0] == pinned(first, 1)
          && advanced.refresh().size() == 1);
        ok(pins.observe(third));
        install_all(advanced, 2);
        BOOST_CHECK(
          advanced.entries().size() == 1
          && advanced.entries()[0] == pinned(second, 2));
        ok(pins.adopt(std::move(advanced)));
        BOOST_CHECK(
          pins.entries().size() == 1 && pins.entries()[0] == pinned(second, 2)
          && pins.stale() == 1);

        // A segment's footers only advance, on one device.
        BOOST_CHECK(code(pins.observe(second)) == errc::wrong_context);
        BOOST_CHECK(code(pins.observe(first)) == errc::wrong_context);
        BOOST_CHECK(
          code(pins.observe(boundary(1, 32768, device(10))))
          == errc::wrong_context);
        BOOST_CHECK(
          code(pins.observe(boundary(1, 32768, device_store_id{})))
          == errc::invalid_argument);
    });
}

// Only a cut that is complete, and the latest issued, becomes the table.
SEASTAR_TEST_CASE(checkpoint_pins_adopt_only_the_latest_complete_cut) {
    co_await with_budget([](workload_budget& budget) {
        const auto shard = owner();
        auto pins = checkpoint_pins::make(budget, {}).value();
        ok(pins.observe(boundary(1, 8192)));
        auto stale = pins.capture(shard, at(1, 8192)).value();
        auto latest = pins.capture(shard, at(1, 8192)).value();
        install_all(stale, 1);
        const auto superseded = pins.adopt(std::move(stale));
        BOOST_CHECK(code(superseded) == errc::wrong_context);
        BOOST_CHECK(pins.entries().empty() && pins.stale() == 1);
        install_all(latest, 2);
        ok(pins.adopt(std::move(latest)));
        BOOST_CHECK(pins.entries()[0] == pinned(boundary(1, 8192), 2));
        // Adopted once.
        auto again = pins.capture(shard, at(1, 8192)).value();
        auto copy = pins.capture(shard, at(1, 8192)).value();
        ok(pins.adopt(std::move(copy)));
        const auto twice = pins.adopt(std::move(again));
        BOOST_CHECK(code(twice) == errc::wrong_context);
    });
}

// An entry leaves only when the segment's own publication covers it: a sealed
// or deleting one, or a recovering one that pins at least the segment's
// newest known footer. A publication that lands while a cut is in flight is
// not undone by adopting that cut.
SEASTAR_TEST_CASE(checkpoint_pins_leave_with_the_segments_publication) {
    co_await with_budget([](workload_budget& budget) {
        const auto shard = owner();
        auto pins = checkpoint_pins::make(budget, {}).value();
        const auto one = boundary(1, 8192), two = boundary(2, 8192),
                   three = boundary(3, 8192);
        for (const auto& value : {one, two, three})
            ok(pins.observe(value));
        auto cut = pins.capture(shard, at(1, 8192)).value();
        install_all(cut, 1);
        ok(pins.adopt(std::move(cut)));
        BOOST_REQUIRE_EQUAL(pins.entries().size(), 3U);
        // Canonical order: by segment.
        BOOST_CHECK(
          pins.entries()[0] == pinned(one, 1)
          && pins.entries()[1] == pinned(two, 1)
          && pins.entries()[2] == pinned(three, 1));

        // An active publication covers nothing.
        BOOST_CHECK(
          code(pins.cover(published(1, local_object_state::active)))
          == errc::invalid_argument);
        // A recovering one without a pin, or pinned below, leaves the entry.
        ok(pins.cover(published(1, local_object_state::recovering)));
        const auto below
          = local_footer_reference::make(
              runtime::file_position{4096}, byte_count{4096}, 6, digest(1))
              .value();
        ok(pins.cover(published(1, local_object_state::recovering, below)));
        BOOST_CHECK(pins.entries().size() == 3);
        // The newest known footer counts, pinned or not yet.
        const auto newer = boundary(1, 16384);
        ok(pins.observe(newer));
        ok(pins.cover(
          published(1, local_object_state::recovering, reference(one, 1))));
        BOOST_CHECK(pins.entries().size() == 3 && pins.stale() == 1);
        ok(pins.cover(
          published(1, local_object_state::recovering, reference(newer, 1))));
        BOOST_CHECK(
          pins.entries().size() == 2 && pins.entries()[0] == pinned(two, 1)
          && pins.stale() == 0);

        ok(pins.cover(published(2, local_object_state::sealed)));
        BOOST_CHECK(
          pins.entries().size() == 1 && pins.entries()[0] == pinned(three, 1));
        // Covering a segment that holds nothing changes nothing.
        ok(pins.cover(published(2, local_object_state::deleting)));
        BOOST_CHECK(pins.entries().size() == 1);

        // Sealed while its refreshed pin is being cut: it does not return.
        const auto later = boundary(3, 16384);
        ok(pins.observe(later));
        auto racing = pins.capture(shard, at(1, 8192)).value();
        ok(pins.cover(published(3, local_object_state::deleting)));
        install_all(racing, 2);
        BOOST_CHECK(
          racing.entries().size() == 1
          && racing.entries()[0] == pinned(later, 2));
        ok(pins.adopt(std::move(racing)));
        BOOST_CHECK(pins.entries().empty() && pins.stale() == 0);
    });
}

// A discard decision is carried once the cutoff has passed the PREPARE it
// names, and until its segment's sealed or deleting publication.
SEASTAR_TEST_CASE(checkpoint_pins_carry_passed_discards) {
    co_await with_budget([](workload_budget& budget) {
        const auto shard = owner();
        auto pins = checkpoint_pins::make(budget, {}).value();
        const auto live = boundary(2, 8192);
        ok(pins.observe(live));
        const auto early = discard(4, 1, at(1, 8192));
        const auto late = discard(3, 1, at(1, 16384));
        ok(pins.hold(late));
        ok(pins.hold(early));
        // Held again, nothing changes; another record under the same
        // sequence, or a decision that discards nothing, is refused.
        ok(pins.hold(early));
        auto forged = early;
        forged.pin.digest = digest(0x55);
        BOOST_CHECK(code(pins.hold(forged)) == errc::wrong_context);
        auto moved = early;
        moved.value.prepare = at(1, 12288);
        BOOST_CHECK(code(pins.hold(moved)) == errc::wrong_context);
        auto kept = early;
        kept.value.action = local_recovery_action::preserve;
        BOOST_CHECK(code(pins.hold(kept)) == errc::invalid_argument);

        // The cutoff stops at the first PREPARE: neither is passed.
        auto none = pins.capture(shard, at(1, 8192)).value();
        install_all(none, 1);
        BOOST_CHECK(
          none.entries().size() == 1 && none.entries()[0] == pinned(live, 1));
        ok(pins.adopt(std::move(none)));

        auto one = pins.capture(shard, at(1, 16384)).value();
        BOOST_REQUIRE_EQUAL(one.entries().size(), 2U);
        // The discarded segment sorts first; its decision is the locator.
        BOOST_CHECK(
          one.entries()[0] == discarded(early)
          && one.entries()[1] == pinned(live, 1) && one.complete());
        ok(pins.adopt(std::move(one)));
        BOOST_CHECK(pins.entries().size() == 2);
        ok(pins.hold(early));
        BOOST_CHECK(code(pins.hold(forged)) == errc::wrong_context);

        auto both = pins.capture(shard, at(2, 4096)).value();
        BOOST_REQUIRE_EQUAL(both.entries().size(), 3U);
        BOOST_CHECK(
          both.entries()[0] == discarded(late)
          && both.entries()[1] == discarded(early)
          && both.entries()[2] == pinned(live, 1));
        ok(pins.adopt(std::move(both)));
        BOOST_CHECK(pins.entries().size() == 3);

        // A recovering publication releases no discard; the seal does.
        ok(pins.cover(
          published(1, local_object_state::recovering, reference(live, 1))));
        BOOST_CHECK(pins.entries().size() == 3);
        ok(pins.cover(published(1, local_object_state::sealed)));
        BOOST_CHECK(
          pins.entries().size() == 1 && pins.entries()[0] == pinned(live, 1));

        // Covered before any cut carried it: it never appears.
        ok(pins.hold(discard(5, 3, at(1, 8192))));
        ok(pins.cover(published(3, local_object_state::deleting)));
        auto after = pins.capture(shard, at(2, 4096)).value();
        BOOST_CHECK(after.entries().size() == 1);
        ok(pins.adopt(std::move(after)));
    });
}

// No table is one large allocation. The largest limits are admitted, and a
// table that outgrows one fragment keeps its order through a cut, an adoption
// and a segment leaving.
SEASTAR_TEST_CASE(checkpoint_pins_span_fragments) {
    co_await with_budget([](workload_budget& budget) {
        const auto shard = owner();
        auto pins = checkpoint_pins::make(
                      budget,
                      {.segments = maximum_checkpoint_segments,
                       .decisions = maximum_checkpoint_decisions})
                      .value();
        constexpr std::uint64_t count = 700;
        for (std::uint64_t generation = 1; generation <= count; ++generation)
            ok(pins.observe(boundary(generation, 8192)));
        BOOST_CHECK(pins.stale() == count);
        auto cut = pins.capture(shard, at(1, 8192)).value();
        BOOST_CHECK(cut.refresh().size() == count && cut.entries().empty());
        install_all(cut, 1);
        BOOST_REQUIRE_EQUAL(cut.entries().size(), count);
        ok(pins.adopt(std::move(cut)));
        BOOST_REQUIRE_EQUAL(pins.entries().size(), count);
        bool ordered = true;
        for (std::uint64_t i = 0; i != count; ++i)
            ordered = ordered
                      && pins.entries()[i] == pinned(boundary(i + 1, 8192), 1);
        BOOST_CHECK(ordered && pins.stale() == 0);

        ok(pins.cover(published(100, local_object_state::sealed)));
        BOOST_REQUIRE_EQUAL(pins.entries().size(), count - 1);
        BOOST_CHECK(
          pins.entries()[98] == pinned(boundary(99, 8192), 1)
          && pins.entries()[99] == pinned(boundary(101, 8192), 1)
          && pins.entries()[count - 2] == pinned(boundary(count, 8192), 1));
        // The whole table is carried into the next cut without a read.
        auto carried = pins.capture(shard, at(1, 8192)).value();
        BOOST_CHECK(
          carried.complete() && carried.entries().size() == count - 1
          && carried.entries()[count - 2] == pinned(boundary(count, 8192), 1));
        ok(pins.adopt(std::move(carried)));
        BOOST_CHECK(pins.entries().size() == count - 1);
    });
}

// A page carries what a reader held to the same limits can decode, never
// just what the format can express: under the default budgets that is fewer
// entries than the encoded maximum, and the maximum once the budgets allow.
SEASTAR_TEST_CASE(checkpoint_pages_are_cut_by_the_readers_budget) {
    const auto policy = codec::limits::defaults();
    const auto alignment = storage_alignment::make(byte_count{4096}).value();
    const auto encoded = local_metadata_page_capacity(
                           local_metadata_kind::checkpoint_page,
                           byte_count{32},
                           alignment,
                           policy)
                           .value();
    BOOST_REQUIRE_EQUAL(encoded, 467U);
    local_store_io_limits limits{.charge = bytes::testing::charge};
    const auto usual = checkpoint_page_entries(limits, alignment, policy);
    BOOST_REQUIRE(usual.has_value());
    // The entries of one page, as an array, within half the metadata budget.
    BOOST_CHECK(
      *usual < encoded
      && limits.charge(byte_count{*usual * sizeof(local_checkpoint_entry)})
             .value()
           <= limits.metadata_bytes.value() / 2
      && limits
             .charge(byte_count{(*usual + 1) * sizeof(local_checkpoint_entry)})
             .value()
           > limits.metadata_bytes.value() / 2);
    limits.operation_bytes = byte_count{512_KiB};
    limits.metadata_bytes = byte_count{256_KiB};
    BOOST_CHECK(
      checkpoint_page_entries(limits, alignment, policy).value() == encoded);
    limits.metadata_bytes = byte_count{64};
    BOOST_CHECK(
      code(checkpoint_page_entries(limits, alignment, policy))
      == errc::resource_exhausted);
    co_return;
}

// A cut's table is encoded as pages in table order under one root. The first
// pass yields the references the root pins and the second the same pages, so
// the stream a bundle is published from verifies against its root under the
// limits the pages were cut for; a page out of its place does not. It holds
// for the default budgets and for ones that allow the largest page.
SEASTAR_TEST_CASE(checkpoint_bundle_pages_follow_the_table) {
    co_await with_budget([](workload_budget& budget) -> seastar::future<> {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        const auto alignment
          = storage_alignment::make(byte_count{4096}).value();
        constexpr std::uint32_t count = 1000;
        seastar::chunked_vector<local_checkpoint_entry> table;
        for (std::uint64_t generation = 1; generation <= count; ++generation)
            table.push_back(pinned(boundary(generation, 8192), 1));
        local_store_io_limits wide{.charge = bytes::testing::charge};
        wide.operation_bytes = byte_count{512_KiB};
        wide.metadata_bytes = byte_count{256_KiB};
        const std::array budgets{
          local_store_io_limits{.charge = bytes::testing::charge}, wide};
        for (const auto& limits : budgets) {
            const auto capacity = checkpoint_page_entries(
                                    limits, alignment, work.policy())
                                    .value();
            const auto expected = (count + capacity - 1) / capacity;
            storage::detail::checkpoint_pages pages{
              &table,
              owner(),
              alignment,
              local_object_sequence::make(70).value(),
              capacity,
              limits};
            auto root = (co_await storage::detail::encode_checkpoint_root(
                           pages, at(1, 4096), at(2, 8192), work))
                          .value();
            BOOST_CHECK(
              root.reference.kind() == local_root_kind::checkpoint
              && root.reference.sequence().value() == 70
              && root.reference.pages().value() == expected
              && root.reference.position().value() == 0 && pages.first == 0
              && pages.ordinal == 0);
            const auto bundle = (co_await local_bundle::make(
                                   root.reference,
                                   std::move(root.expected),
                                   std::move(root.bytes),
                                   budget,
                                   limits,
                                   work))
                                  .value();
            const auto& pinned_root = std::get<local_checkpoint_root>(
              std::get<local_metadata_record>(bundle.root()).payload());
            BOOST_CHECK(
              pinned_root.begin == at(1, 4096) && pinned_root.end == at(2, 8192)
              && pinned_root.entry_count == count);
            const auto references = bundle.pages();
            BOOST_REQUIRE_EQUAL(references.size(), expected);
            std::uint32_t first = 0;
            bool placed = true;
            for (const auto& reference : references) {
                placed = placed && reference.first_entry() == first
                         && reference.entry_count()
                              == std::min(capacity, count - first);
                first += reference.entry_count();
            }
            BOOST_CHECK(placed && first == count);
            {
                local_bundle_verifier verifier{bundle, work.policy()};
                for (;;) {
                    auto page = (co_await pages.next(work)).value();
                    if (!page) break;
                    ok(co_await verifier.next(*page, work));
                }
                ok(verifier.finish(work));
            }
            // The second page where the first belongs is not what the root
            // pins.
            pages.first = pages.ordinal = 0;
            static_cast<void>((co_await pages.next(work)).value());
            auto second = (co_await pages.next(work)).value();
            local_bundle_verifier verifier{bundle, work.policy()};
            const auto misplaced = co_await verifier.next(*second, work);
            BOOST_CHECK(!misplaced.has_value());
        }

        // An empty table is a root with no page.
        const local_store_io_limits limits{.charge = bytes::testing::charge};
        const seastar::chunked_vector<local_checkpoint_entry> none;
        storage::detail::checkpoint_pages empty{
          &none,
          owner(),
          alignment,
          local_object_sequence::make(71).value(),
          checkpoint_page_entries(limits, alignment, work.policy()).value(),
          limits};
        const auto bare = (co_await storage::detail::encode_checkpoint_root(
                             empty, at(1, 4096), at(1, 8192), work))
                            .value();
        BOOST_CHECK(bare.reference.pages().value() == 0);
        BOOST_CHECK(!(co_await empty.next(work)).value().has_value());
    });
}

// The table is bounded by its limits: past them a new segment or decision is
// refused, and room returns when a publication covers one.
SEASTAR_TEST_CASE(checkpoint_pins_are_bounded) {
    co_await with_budget([](workload_budget& budget) {
        BOOST_CHECK(
          code(checkpoint_pins::make(budget, {.segments = 0}))
          == errc::invalid_argument);
        BOOST_CHECK(
          code(
            checkpoint_pins::make(
              budget, {.segments = maximum_checkpoint_segments + 1}))
          == errc::invalid_argument);
        BOOST_CHECK(
          code(
            checkpoint_pins::make(
              budget, {.decisions = maximum_checkpoint_decisions + 1}))
          == errc::invalid_argument);

        const auto shard = owner();
        auto pins = checkpoint_pins::make(
                      budget, {.segments = 1, .decisions = 1})
                      .value();
        ok(pins.observe(boundary(1, 8192)));
        BOOST_CHECK(
          code(pins.observe(boundary(2, 8192))) == errc::resource_exhausted);
        // The tracked segment still advances.
        ok(pins.observe(boundary(1, 16384)));
        ok(pins.hold(discard(1, 3, at(1, 8192))));
        BOOST_CHECK(
          code(pins.hold(discard(2, 3, at(1, 12288))))
          == errc::resource_exhausted);
        auto cut = pins.capture(shard, at(1, 16384)).value();
        install_all(cut, 1);
        ok(pins.adopt(std::move(cut)));
        BOOST_CHECK(pins.entries().size() == 2);
        BOOST_CHECK(
          code(pins.observe(boundary(2, 8192))) == errc::resource_exhausted);
        BOOST_CHECK(
          code(pins.hold(discard(2, 3, at(1, 12288))))
          == errc::resource_exhausted);

        ok(pins.cover(published(1, local_object_state::sealed)));
        ok(pins.cover(published(3, local_object_state::sealed)));
        BOOST_CHECK(pins.entries().empty());
        ok(pins.observe(boundary(2, 8192)));
        ok(pins.hold(discard(2, 3, at(1, 12288))));
    });
}

namespace {
// A durable checkpoint as a reopen loads it, holding `entries`.
loaded_checkpoint loaded_table(
  workload_budget& budget, std::vector<local_checkpoint_entry> entries) {
    const auto reference = local_root_reference::make(
                             local_root_kind::checkpoint,
                             local_object_sequence::make(9).value(),
                             runtime::file_position{},
                             byte_count{4096},
                             page_count::make(1).value(),
                             digest(0x55))
                             .value();
    const local_checkpoint_root root{
      at(1, 4096), at(2, 4096), static_cast<std::uint32_t>(entries.size()), {}};
    auto loaded = storage::detail::admit_checkpoint_table(
                    budget, reference, root)
                    .value();
    for (const auto& entry : entries)
        loaded.entries.push_back(entry);
    return loaded;
}
// One catalog segment as a restart's inventory holds it.
recovery_entry found(
  std::uint64_t generation,
  recovery_entry_state state,
  local_object_state as,
  std::optional<local_footer_reference> pin = std::nullopt,
  device_store_id on = device()) {
    return recovery_entry{
      state,
      on,
      boundary(generation, 8192, on).history,
      local_publication_generation::make(1).value(),
      published(generation, as, pin),
      {},
      {}};
}
recovery_target reconciled(
  std::uint64_t generation,
  local_object_state state,
  std::optional<local_footer_reference> pin = std::nullopt) {
    const auto alignment = storage_alignment::make(byte_count{4096}).value();
    return recovery_target{
      local_segment_descriptor{
        segment(generation),
        model::range_logical_end{100},
        model::segment_relative_end{},
        alignment,
        storage_profile::v1,
        1,
        local_layout_kind::initial,
        byte_count{64_MiB},
        runtime::monotonic_duration{3600000000000ULL}},
      segment_header::make(
        segment(generation), model::range_logical_end{100}, alignment)
        .value(),
      state,
      local_publication_generation::make(1).value(),
      pin,
      {}};
}
seastar::chunked_vector<local_checkpoint_entry>
table_of(std::vector<local_checkpoint_entry> entries) {
    seastar::chunked_vector<local_checkpoint_entry> table;
    for (const auto& entry : entries)
        table.push_back(entry);
    return table;
}
} // namespace

// The names are a run of the chain, oldest first. Only files before the one
// the cutoff lies in are below it, the newest file named always stays, and
// the table is fixed.
SEASTAR_TEST_CASE(retained_wal_names_the_chain_oldest_first) {
    retained_wal chain;
    BOOST_CHECK(
      chain.empty() && !chain.oldest() && !chain.newest()
      && chain.below(at(9, 4096)) == 0);
    BOOST_CHECK(
      code(chain.extend(model::wal_incarnation_id{}))
      == errc::invalid_argument);
    for (const std::uint8_t file : std::array<std::uint8_t, 3>{2, 4, 5})
        ok(chain.extend(wal(file)));
    // A chain's incarnations only grow.
    BOOST_CHECK(code(chain.extend(wal(5))) == errc::wrong_context);
    BOOST_CHECK(code(chain.extend(wal(3))) == errc::wrong_context);
    BOOST_CHECK(
      chain.files() == 3 && chain.oldest() == wal(2) && chain.newest() == wal(5)
      && !chain.full());
    BOOST_CHECK(chain.below(at(2, 4096)) == 0);
    BOOST_CHECK(chain.below(at(3, 4096)) == 1);
    BOOST_CHECK(chain.below(at(4, 8192)) == 1);
    BOOST_CHECK(chain.below(at(5, 4096)) == 2);
    // A cutoff past every name still leaves the newest, which is the head.
    BOOST_CHECK(chain.below(at(9, 4096)) == 2);
    chain.drop_oldest();
    BOOST_CHECK(
      chain.files() == 2 && chain.oldest() == wal(4)
      && chain.below(at(5, 4096)) == 1);
    chain.drop_oldest();
    chain.drop_oldest();
    BOOST_CHECK(chain.files() == 1 && chain.oldest() == wal(5));

    retained_wal full;
    for (std::uint32_t i = 0; i != maximum_retained_wal_files; ++i)
        ok(full.extend(wal(static_cast<std::uint8_t>(i + 1))));
    BOOST_CHECK(
      full.full() && code(full.extend(wal(200))) == errc::resource_exhausted);
    full.drop_oldest();
    ok(full.extend(wal(200)));
    BOOST_CHECK(
      full.files() == maximum_retained_wal_files && full.oldest() == wal(2)
      && full.newest() == wal(200));
    return seastar::make_ready_future<>();
}

// After a restart the durable checkpoint's table is the table: it is carried
// forward unchanged and leaves entry by entry with each segment's own
// publication. It is restored once, in canonical order and within the
// owner's bounds.
SEASTAR_TEST_CASE(checkpoint_pins_continue_from_the_durable_table) {
    return with_budget([](workload_budget& budget) {
        const auto shard = owner();
        const auto one = boundary(1, 8192), two = boundary(2, 8192);
        const auto held = discard(1, 2, at(1, 8192));
        {
            auto pins = checkpoint_pins::make(budget, {}).value();
            ok(pins.restore(loaded_table(
              budget, {pinned(one, 1), pinned(two, 2), discarded(held)})));
            BOOST_REQUIRE(pins.entries().size() == 3);
            auto cut = pins.capture(shard, at(2, 8192)).value();
            BOOST_CHECK(
              cut.complete() && cut.refresh().empty()
              && cut.entries().size() == 3 && cut.entries()[0] == pinned(one, 1)
              && cut.entries()[1] == pinned(two, 2)
              && cut.entries()[2] == discarded(held));
            ok(pins.adopt(std::move(cut)));
            ok(pins.cover(
              published(1, local_object_state::recovering, reference(one, 1))));
            BOOST_CHECK(
              pins.entries().size() == 2
              && pins.entries()[0] == pinned(two, 2));
            ok(pins.cover(published(2, local_object_state::sealed)));
            BOOST_CHECK(pins.entries().empty());
            // A table is restored before anything else is recorded.
            const auto late = pins.restore(
              loaded_table(budget, {pinned(one, 1)}));
            BOOST_CHECK(code(late) == errc::wrong_context);
        }
        {
            auto pins = checkpoint_pins::make(budget, {}).value();
            const auto unordered = pins.restore(
              loaded_table(budget, {pinned(two, 2), pinned(one, 1)}));
            BOOST_CHECK(code(unordered) == errc::malformed_data);
            const auto twice = pins.restore(
              loaded_table(budget, {pinned(one, 1), pinned(one, 1)}));
            BOOST_CHECK(code(twice) == errc::malformed_data);
        }
        {
            auto pins = checkpoint_pins::make(
                          budget, {.segments = 1, .decisions = 1})
                          .value();
            const auto wide = pins.restore(
              loaded_table(budget, {pinned(one, 1), pinned(two, 2)}));
            BOOST_CHECK(code(wide) == errc::resource_exhausted);
            // A refused table leaves the owner as it was.
            ok(pins.observe(one));
            BOOST_CHECK(pins.entries().empty() && pins.stale() == 1);
        }
    });
}

// A restart resumes each active or recovering segment at the higher of its
// publication's pin and its checkpoint entry. A segment its own sealed
// publication or a pending seal decides is superseded. An entry whose
// segment, device or decision is not what it names fails the reopen.
SEASTAR_TEST_CASE(checkpoint_resume_takes_the_higher_pin) {
    return with_budget([](workload_budget& budget) {
        const auto pin = [](std::uint64_t generation, std::uint64_t footer) {
            return boundary(generation, footer);
        };
        const auto held = discard(1, 2, at(1, 8192));
        const auto table = table_of(
          {pinned(pin(1, 16384), 1),
           pinned(pin(2, 16384), 2),
           discarded(held),
           pinned(pin(3, 16384), 3),
           pinned(pin(4, 16384), 4),
           pinned(pin(5, 16384), 5)});
        const std::array decisions{held};
        const auto inventory = [&budget] {
            // In catalog order, which is not segment order.
            return recovery_inventory{
              budget.try_reserve(byte_count{1}).value(),
              {found(
                 3,
                 recovery_entry_state::target,
                 local_object_state::recovering,
                 reference(boundary(3, 24576), 9)),
               found(
                 4, recovery_entry_state::sealing, local_object_state::active),
               found(
                 1, recovery_entry_state::target, local_object_state::active),
               found(
                 5, recovery_entry_state::target, local_object_state::sealed),
               found(
                 2,
                 recovery_entry_state::target,
                 local_object_state::recovering,
                 reference(boundary(2, 8192), 7))},
              {reconciled(
                 3,
                 local_object_state::recovering,
                 reference(boundary(3, 24576), 9)),
               reconciled(1, local_object_state::active),
               reconciled(5, local_object_state::sealed),
               reconciled(
                 2,
                 local_object_state::recovering,
                 reference(boundary(2, 8192), 7))}};
        };
        {
            auto restart = inventory();
            const auto resumed
              = resume_from_checkpoint(table, restart, decisions).value();
            BOOST_CHECK(
              resumed.raised == 2 && resumed.superseded == 2
              && resumed.discards == 1);
            // Above its checkpoint entry already: the publication's pin stays.
            BOOST_CHECK(
              restart.targets[0].pin == reference(boundary(3, 24576), 9));
            BOOST_CHECK(restart.targets[1].pin == reference(pin(1, 16384), 1));
            BOOST_CHECK(!restart.targets[2].pin);
            BOOST_CHECK(restart.targets[3].pin == reference(pin(2, 16384), 2));
        }
        const auto refused =
          [&](
            std::vector<local_checkpoint_entry> entries,
            std::span<const recovery_decision_record> known) {
              auto restart = inventory();
              return code(resume_from_checkpoint(
                table_of(std::move(entries)), restart, known));
          };
        // A segment the catalog does not hold.
        BOOST_CHECK(refused({pinned(pin(8, 16384), 1)}, {}) == errc::not_found);
        // A segment found on another device than the one pinned.
        BOOST_CHECK(
          refused({pinned(boundary(1, 16384, device(0x21)), 1)}, {})
          == errc::wrong_context);
        // Two durable records pin one position as different bytes.
        BOOST_CHECK(
          refused({pinned(pin(2, 8192), 1)}, {}) == errc::corrupt_data);
        // A discard whose decision is absent, or is another record.
        BOOST_CHECK(refused({discarded(held)}, {}) == errc::not_found);
        auto other = held;
        other.pin.digest = digest(0x66);
        const std::array differing{other};
        BOOST_CHECK(
          refused({discarded(held)}, differing) == errc::wrong_context);
    });
}

// The relocation disposition is recognized and unsupported: no table that
// carries it is restored, and no restart resumes from one.
SEASTAR_TEST_CASE(checkpoint_refuses_the_relocation_disposition) {
    return with_budget([](workload_budget& budget) {
        auto relocated = pinned(boundary(2, 8192), 2);
        relocated.disposition
          = local_checkpoint_disposition::preserved_candidate;
        {
            auto pins = checkpoint_pins::make(budget, {}).value();
            const auto restored = pins.restore(
              loaded_table(budget, {pinned(boundary(1, 8192), 1), relocated}));
            BOOST_CHECK(code(restored) == errc::unsupported_format);
            // The owner is as it was: nothing of the table was taken.
            BOOST_CHECK(pins.entries().empty());
            ok(pins.observe(boundary(1, 8192)));
        }
        recovery_inventory restart{
          budget.try_reserve(byte_count{1}).value(),
          {found(1, recovery_entry_state::target, local_object_state::active),
           found(2, recovery_entry_state::target, local_object_state::active)},
          {reconciled(1, local_object_state::active),
           reconciled(2, local_object_state::active)}};
        const auto resumed = resume_from_checkpoint(
          table_of({pinned(boundary(1, 16384), 1), relocated}), restart, {});
        BOOST_CHECK(code(resumed) == errc::unsupported_format);
    });
}

// The oracle that checks a reopened store: every PREPARE below the cutoff
// needs a pin at or after its block, its segment's own publication, or the
// discard that names it. One above the cutoff needs nothing.
SEASTAR_TEST_CASE(checkpoint_coverage_oracle_names_what_nothing_covers) {
    using storage::testing::checkpoint_coverage_oracle;
    const auto shard = owner();
    const runtime::file_position first{8192}, second{16384};
    checkpoint_coverage_oracle seen;
    // Segment 1 has two PREPAREs, segment 2 one that a decision discards,
    // and segment 3 one above every cutoff used here.
    seen.appended(at(1, 8192), segment(1), first);
    seen.appended(at(1, 16384), segment(1), second);
    seen.appended(at(1, 24576), segment(2), first, at(1, 16384));
    seen.appended(at(2, 8192), segment(3), first);
    seen.discarded(at(1, 16384), 7);
    const auto cutoff = at(1, 24576);
    const auto check =
      [&](
        std::vector<local_checkpoint_entry> entries,
        std::vector<local_object_publication> publications = {}) {
          return seen.uncovered(shard, cutoff, entries, publications);
      };
    const auto held = recovery_decision_record{
      {local_decision_sequence::make(7).value(), digest(7), byte_count{4096}},
      {at(1, 16384),
       digest(0x70),
       segment(2),
       runtime::file_position{8192},
       local_recovery_action::discard,
       7}};
    // Nothing durable: the first PREPARE is the first uncovered.
    BOOST_CHECK(check({}) == 0U);
    // A pin that ends before the second block covers only the first.
    BOOST_CHECK(check({pinned(boundary(1, 8192), 1)}) == 1U);
    // A pin at the end of the second block covers both; segment 2 is next.
    BOOST_CHECK(check({pinned(boundary(1, 16384), 1)}) == 2U);
    // The discard that names the PREPARE covers it; another decision does
    // not.
    auto other = discarded(held);
    other.locator = 8;
    BOOST_CHECK(check({pinned(boundary(1, 16384), 1), other}) == 2U);
    BOOST_CHECK(!check({pinned(boundary(1, 16384), 1), discarded(held)}));
    // A segment's own publication covers it: sealed whatever it pins, or
    // recovering with a boundary at or after the block.
    BOOST_CHECK(
      !check({discarded(held)}, {published(1, local_object_state::sealed)}));
    BOOST_CHECK(
      check(
        {discarded(held)},
        {published(
          1, local_object_state::recovering, reference(boundary(1, 8192), 1))})
      == 1U);
    BOOST_CHECK(!check(
      {discarded(held)},
      {published(
        1, local_object_state::recovering, reference(boundary(1, 16384), 1))}));
    // A lower cutoff asks for less.
    BOOST_CHECK(!seen.uncovered(
      shard,
      at(1, 8192),
      std::vector{pinned(boundary(1, 8192), 1)},
      std::span<const local_object_publication>{}));
    co_return;
}
