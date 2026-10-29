#include "src/base/units.h"
#include "src/bytes/test_allocation_profile.h"
#include "src/codec/envelope.h"
#include "src/codec/limits.h"
#include "src/resource/resource_registry.h"
#include "src/storage/page_ref.h"
#include "src/storage/retry_format.h"
#include "src/storage/retry_lookup.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/memory.hh>
#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <array>
#include <cstdint>
#include <exception>
#include <limits>
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
// The completed fact of the batch with this stream and sequence.
completed_retry fact(std::uint64_t sequence, std::uint64_t stream = 1) {
    return completed_retry::make(
             model::batch_id::make(
               identity<model::producer_id>(0x40),
               model::producer_epoch::make(1).value(),
               model::producer_stream_id::make(stream).value(),
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
retry_prefix stream(std::uint64_t value) {
    return retry_prefix::of(fact(0, value).id());
}
template<typename Result>
void ok(const Result& done) {
    BOOST_REQUIRE(done.has_value());
}
// What retry_root_reader::find_stream_last answers over these pages, by the
// same directory and page searches: the page the stream's largest identity
// floors to, and in it the floor entry when it is the stream's.
std::optional<completed_retry> stream_last(
  const retry_page_directory& directory,
  const std::vector<std::vector<completed_retry>>& pages,
  retry_prefix prefix) {
    const auto probe = model::batch_id::make(
                         prefix.producer,
                         prefix.epoch,
                         prefix.stream,
                         model::batch_sequence{
                           std::numeric_limits<std::uint64_t>::max()})
                         .value();
    const auto ordinal = directory.floor(probe);
    if (!ordinal) return std::nullopt;
    const auto* found = find_completed_retry_floor(pages[*ordinal], probe);
    if (!found || retry_prefix::of(found->id()) != prefix) return std::nullopt;
    return *found;
}
} // namespace

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
        BOOST_CHECK(!empty.floor(fact(1).id()));
        BOOST_CHECK(
          !retry_page_directory::make(budget, maximum_object_pages + 1)
             .has_value());
    });
}

// The floor names the page holding the greatest entry at or below an
// identity, gaps and the far end included; within a page, the floor entry is
// that greatest entry.
SEASTAR_TEST_CASE(retry_directory_floors_to_the_earlier_page) {
    return with_budget([](workload_budget& budget) {
        std::vector<std::vector<completed_retry>> pages(3);
        for (std::uint64_t sequence = 10; sequence != 20; ++sequence)
            pages[0].push_back(fact(sequence));
        for (std::uint64_t sequence = 30; sequence != 40; ++sequence)
            pages[1].push_back(fact(sequence));
        pages[2].push_back(fact(50));
        auto directory = retry_page_directory::make(budget, 3).value();
        for (const auto& page : pages)
            ok(directory.add(page));
        const auto at = [&directory](std::uint64_t sequence) {
            return directory.floor(fact(sequence).id());
        };
        BOOST_CHECK(!at(0) && !at(9));
        BOOST_CHECK(at(10) == 0U && at(19) == 0U);
        BOOST_CHECK(at(20) == 0U && at(29) == 0U);
        BOOST_CHECK(at(30) == 1U && at(49) == 1U);
        BOOST_CHECK(at(50) == 2U && at(UINT64_MAX) == 2U);

        BOOST_CHECK(
          find_completed_retry_floor(pages[1], fact(29).id()) == nullptr);
        const auto* first = find_completed_retry_floor(pages[1], fact(30).id());
        BOOST_REQUIRE(first != nullptr);
        BOOST_CHECK(*first == fact(30));
        const auto* last = find_completed_retry_floor(pages[1], fact(45).id());
        BOOST_REQUIRE(last != nullptr);
        BOOST_CHECK(*last == fact(39));
    });
}

// A stream's newest entry is found from one page: before the first page
// there is none; a stream spanning two pages ends in the later one; an
// absent stream between two present ones is absent, never its neighbour;
// and a stream may end at the largest sequence.
SEASTAR_TEST_CASE(retry_stream_last_reads_one_page) {
    return with_budget([](workload_budget& budget) {
        // Streams 2 and 4 of one producer and epoch; stream 4 spans the two
        // pages and its last batch has the largest sequence.
        std::vector<std::vector<completed_retry>> pages(2);
        for (std::uint64_t sequence = 0; sequence != 3; ++sequence)
            pages[0].push_back(fact(sequence, 2));
        pages[0].push_back(fact(7, 4));
        pages[1].push_back(fact(8, 4));
        pages[1].push_back(fact(UINT64_MAX, 4));
        auto directory = retry_page_directory::make(budget, 2).value();
        for (const auto& page : pages)
            ok(directory.add(page));
        BOOST_CHECK(!stream_last(directory, pages, stream(1)));
        BOOST_CHECK(stream_last(directory, pages, stream(2)) == fact(2, 2));
        BOOST_CHECK(!stream_last(directory, pages, stream(3)));
        BOOST_CHECK(
          stream_last(directory, pages, stream(4)) == fact(UINT64_MAX, 4));
        BOOST_CHECK(!stream_last(directory, pages, stream(5)));
    });
}

// At the largest summary a page holds as many entries as the wire allows,
// and streams of R = 5 cross page boundaries: every stream's newest entry is
// found from the one page its largest identity floors to, each page is the
// floor of its own first and last entries, and a stream past the last is
// absent.
SEASTAR_TEST_CASE(retry_stream_last_at_the_largest_summary) {
    return with_budget([](workload_budget& budget) {
        constexpr std::uint64_t depth = 5;
        const std::uint64_t streams = maximum_object_entries / depth;
        const auto capacity
          = retry_page_capacity(
              byte_count{codec::envelope_prefix_bytes},
              storage_alignment::make(byte_count{4096}).value(),
              codec::limits::defaults())
              .value();
        std::vector<std::vector<completed_retry>> pages;
        pages.reserve((streams * depth + capacity - 1) / capacity);
        for (std::uint64_t value = 1; value <= streams; ++value)
            for (std::uint64_t sequence = 0; sequence != depth; ++sequence) {
                if (pages.empty() || pages.back().size() == capacity) {
                    pages.emplace_back();
                    pages.back().reserve(capacity);
                }
                pages.back().push_back(fact(sequence, value));
            }
        BOOST_REQUIRE(pages.size() > 1 && capacity % depth != 0);
        auto directory = retry_page_directory::make(
                           budget, static_cast<std::uint32_t>(pages.size()))
                           .value();
        for (const auto& page : pages)
            ok(directory.add(page));
        for (std::uint32_t ordinal = 0; ordinal != pages.size(); ++ordinal) {
            const auto& page = pages[ordinal];
            BOOST_CHECK(
              directory.floor(page.front().id()) == ordinal
              && directory.floor(page.back().id()) == ordinal
              && directory.find(page.front().id()) == ordinal);
        }
        std::uint64_t wrong = 0;
        for (std::uint64_t value = 1; value <= streams; ++value)
            if (
              stream_last(directory, pages, stream(value))
              != fact(depth - 1, value))
                ++wrong;
        BOOST_CHECK_EQUAL(wrong, 0U);
        BOOST_CHECK(!stream_last(directory, pages, stream(streams + 1)));
    });
}
