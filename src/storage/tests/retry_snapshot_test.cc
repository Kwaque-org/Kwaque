#include "src/base/units.h"
#include "src/bytes/test_allocation_profile.h"
#include "src/resource/resource_registry.h"
#include "src/storage/retry_lookup.h"
#include "src/storage/retry_snapshot.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/memory.hh>
#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <array>
#include <exception>
#include <optional>

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
local_store_context owner() {
    return local_store_context::make(
             identity<model::cluster_id>(1),
             identity<model::broker_id>(2),
             identity<device_store_id>(3),
             0)
      .value();
}
segment_context segment(std::uint64_t generation = 1) {
    return segment_context::make(
             identity<model::cluster_id>(1),
             identity<model::topic_id>(4),
             identity<model::range_id>(5),
             identity<model::segment_id>(6),
             model::segment_generation::make(generation).value())
      .value();
}
// The completed fact of the batch with this sequence, bound to segment().
completed_retry fact(std::uint64_t sequence) {
    return completed_retry::make(
             model::batch_id::make(
               identity<model::producer_id>(0x40),
               model::producer_epoch::make(1).value(),
               model::producer_stream_id::make(1).value(),
               model::batch_sequence{sequence})
               .value(),
             codec::semantic_batch_digest{codec::content_digest{}},
             model::producer_stream_binding::make(
               identity<model::topic_id>(4),
               identity<model::range_id>(5),
               model::range_routing_epoch::make(1).value(),
               identity<model::segment_id>(6),
               model::segment_generation::make(1).value())
               .value(),
             model::range_logical_span::make(
               model::range_logical_end{100 + sequence},
               model::range_logical_end{101 + sequence})
               .value(),
             model::segment_generation::make(1).value())
      .value();
}
template<typename Result>
void ok(const Result& done) {
    BOOST_REQUIRE(done.has_value());
}
} // namespace

// A demand cuts only when something is unsaved, and unless forced only once
// the unsaved facts are at least as many as the saved. Demanding after every
// fact then writes each fact about twice in total.
SEASTAR_TEST_CASE(retry_cuts_are_due_at_each_doubling) {
    static_assert(!retry_cut_due(0, 0, true));
    static_assert(retry_cut_due(0, 1, false));
    static_assert(retry_cut_due(1, 2, false));
    static_assert(!retry_cut_due(2, 3, false));
    static_assert(retry_cut_due(2, 3, true));
    static_assert(retry_cut_due(2, 4, false));
    static_assert(!retry_cut_due(3, 3, true));
    static_assert(!retry_cut_due(5, 3, true));
    constexpr std::uint32_t facts = 1000;
    std::uint64_t written = 0;
    std::uint32_t saved = 0;
    for (std::uint32_t recorded = 1; recorded <= facts; ++recorded) {
        if (!retry_cut_due(saved, recorded, false)) continue;
        written += recorded;
        saved = recorded;
    }
    BOOST_CHECK(saved == 512 && written == 1023);
    BOOST_CHECK(written <= 2ULL * facts);
    return seastar::make_ready_future<>();
}

// A page holds no more facts than a reader held to the same limits can
// decode as one array.
SEASTAR_TEST_CASE(retry_snapshot_pages_are_cut_by_the_readers_budget) {
    const auto alignment = storage_alignment::make(byte_count{4096}).value();
    const auto policy = codec::limits::defaults();
    const local_store_io_limits limits{.charge = bytes::testing::charge};
    const auto capacity
      = retry_snapshot_page_entries(limits, alignment, policy).value();
    const auto room = std::min(
      limits.metadata_bytes.value() / 2, limits.operation_bytes.value() / 4);
    BOOST_CHECK(capacity != 0);
    BOOST_CHECK(
      limits.charge(byte_count{capacity * sizeof(completed_retry)}).value()
      <= room);
    BOOST_CHECK(
      limits.charge(byte_count{(capacity + 1) * sizeof(completed_retry)})
          .value()
        > room
      || capacity * 160U + 4096U > 65536U);
    local_store_io_limits wide{.charge = bytes::testing::charge};
    wide.operation_bytes = byte_count{512_KiB};
    wide.metadata_bytes = byte_count{256_KiB};
    BOOST_CHECK(
      retry_snapshot_page_entries(wide, alignment, policy).value() >= capacity);
    return seastar::make_ready_future<>();
}

// A cut copies the facts once, and its pages are a root over exactly those
// facts in batch identity order: every page decodes under the limits it was
// cut for, through the same verifier a publication uses.
SEASTAR_TEST_CASE(retry_snapshot_bundle_pages_follow_the_facts) {
    co_await with_budget([](workload_budget& budget) -> seastar::future<> {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        const auto alignment
          = storage_alignment::make(byte_count{4096}).value();
        constexpr std::uint32_t count = 700;
        seastar::chunked_vector<completed_retry> recorded;
        for (std::uint64_t sequence = 0; sequence != count; ++sequence)
            recorded.push_back(fact(sequence));
        const seastar::chunked_vector<completed_retry> none;
        BOOST_CHECK(
          !storage::detail::copy_retry_facts(budget, none).has_value());
        auto cut = storage::detail::copy_retry_facts(budget, recorded).value();
        BOOST_REQUIRE(cut.facts.size() == count);
        // A fact recorded after the cut is not part of it.
        recorded.push_back(fact(count));
        BOOST_CHECK(
          cut.facts.size() == count && cut.facts[count - 1] == fact(count - 1));

        const local_store_io_limits limits{.charge = bytes::testing::charge};
        const auto capacity = retry_snapshot_page_entries(
                                limits, alignment, work.policy())
                                .value();
        const auto expected = (count + capacity - 1) / capacity;
        BOOST_REQUIRE(expected > 1);
        const auto footer = local_footer_reference::make(
                              runtime::file_position{8192},
                              byte_count{4096},
                              6,
                              codec::immutable_object_digest{
                                codec::content_digest{}})
                              .value();
        storage::detail::retry_snapshot_pages pages{
          &cut.facts,
          owner(),
          segment(),
          alignment,
          alignment,
          local_object_sequence::make(70).value(),
          capacity,
          limits};
        auto root = (co_await storage::detail::encode_retry_snapshot_root(
                       pages, footer, work))
                      .value();
        BOOST_CHECK(
          root.reference.kind() == local_root_kind::completed_retry_snapshot
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
        const auto& pinned = std::get<local_completed_retry_root>(
          std::get<local_metadata_record>(bundle.root()).payload());
        BOOST_CHECK(
          pinned.segment == segment() && pinned.footer == footer
          && pinned.retry_count == count);
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
        local_bundle_verifier verifier{bundle, work.policy()};
        for (;;) {
            auto page = (co_await pages.next(work)).value();
            if (!page) break;
            ok(co_await verifier.next(*page, work));
        }
        ok(verifier.finish(work));
    });
}

// The directory names the one page that can hold an identity: the last page
// that begins at or before it, unless that page ends before it. Nothing lies
// before the first page, in the gap between two pages or after the last.
// Pages are added in order and must continue one another.
SEASTAR_TEST_CASE(retry_directory_finds_the_one_page) {
    return with_budget([](workload_budget& budget) {
        // Three pages: 10..19, 30..39 and the single identity 50.
        std::vector<std::vector<completed_retry>> pages(3);
        for (std::uint64_t sequence = 10; sequence != 20; ++sequence)
            pages[0].push_back(fact(sequence));
        for (std::uint64_t sequence = 30; sequence != 40; ++sequence)
            pages[1].push_back(fact(sequence));
        pages[2].push_back(fact(50));
        auto directory = retry_page_directory::make(budget, 3).value();
        BOOST_CHECK(!directory.complete() && !directory.find(fact(10).id()));
        for (const auto& page : pages)
            ok(directory.add(page));
        BOOST_CHECK(directory.complete() && directory.pages() == 3);
        const auto at = [&directory](std::uint64_t sequence) {
            return directory.find(fact(sequence).id());
        };
        BOOST_CHECK(!at(0) && !at(9));
        BOOST_CHECK(at(10) == 0U && at(15) == 0U && at(19) == 0U);
        BOOST_CHECK(!at(20) && !at(29));
        BOOST_CHECK(at(30) == 1U && at(39) == 1U);
        BOOST_CHECK(!at(40) && !at(49));
        BOOST_CHECK(at(50) == 2U);
        BOOST_CHECK(!at(51) && !at(UINT64_MAX));
        // An exact entry within a page, and its absence.
        const auto* held = find_completed_retry(pages[1], fact(33).id());
        BOOST_REQUIRE(held != nullptr);
        BOOST_CHECK(*held == fact(33));
        BOOST_CHECK(find_completed_retry(pages[1], fact(29).id()) == nullptr);
        BOOST_CHECK(find_completed_retry(pages[1], fact(40).id()) == nullptr);
        // No page is added past the root's count.
        BOOST_CHECK(!directory.add(pages[2]).has_value());

        auto unordered = retry_page_directory::make(budget, 2).value();
        ok(unordered.add(pages[1]));
        BOOST_CHECK(!unordered.add(pages[0]).has_value());
        BOOST_CHECK(!unordered.add(pages[1]).has_value());
        BOOST_CHECK(!unordered.add({}).has_value());
        auto empty = retry_page_directory::make(budget, 0).value();
        BOOST_CHECK(empty.complete() && !empty.find(fact(1).id()));
        BOOST_CHECK(
          !retry_page_directory::make(budget, maximum_object_pages + 1)
             .has_value());
    });
}

// A table kept in batch identity order: a fact is found by its identity,
// added at its place, and the facts of a cut that failed are put back.
SEASTAR_TEST_CASE(retry_tables_stay_in_batch_identity_order) {
    seastar::chunked_vector<completed_retry> kept, cut;
    for (const std::uint64_t sequence :
         std::array<std::uint64_t, 4>{7, 3, 9, 5})
        storage::detail::insert_retry(kept, fact(sequence));
    BOOST_REQUIRE(kept.size() == 4);
    BOOST_CHECK(
      kept[0] == fact(3) && kept[1] == fact(5) && kept[2] == fact(7)
      && kept[3] == fact(9));
    const auto* held = storage::detail::held_retry(kept, fact(7).id());
    BOOST_REQUIRE(held != nullptr);
    BOOST_CHECK(*held == fact(7));
    BOOST_CHECK(storage::detail::held_retry(kept, fact(6).id()) == nullptr);
    BOOST_CHECK(storage::detail::held_retry(cut, fact(6).id()) == nullptr);
    cut = std::move(kept);
    kept = {};
    storage::detail::insert_retry(kept, fact(6));
    storage::detail::restore_retries(kept, cut);
    BOOST_REQUIRE(kept.size() == 5 && cut.empty());
    BOOST_CHECK(
      kept[0] == fact(3) && kept[1] == fact(5) && kept[2] == fact(6)
      && kept[3] == fact(7) && kept[4] == fact(9));
    return seastar::make_ready_future<>();
}
