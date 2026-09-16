#include "src/storage/tests/range_manifest_test_support.h"
#include "src/storage/tests/sparse_index_test_support.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/preempt.hh>
#include <seastar/core/reactor.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/later.hh>

#include <gtest/gtest.h>

#include <chrono>
#include <concepts>
#include <malloc.h>
#include <type_traits>

namespace kwaque::storage {
namespace {
using namespace testing;
using bytes::fragmented_buffer_parser;
namespace mi = testing::manifest;
namespace ix = testing::index;

// All sequential children spend the same operation reservation. Reserve one
// MiB for native SHA state, frames and fixed owners before admitting any input.
// Fixture strings are payload; fixture/decoded vectors are metadata. No input
// backing is charged again as an alias, and no full-object entry vector exists.
codec::decode_budget memory_for(
  const fragmented_buffer_parser& input,
  codec::cooperative_work& work,
  byte_count metadata,
  const std::string& wire) {
    const auto available
      = codec::detail::consume_decode_budget(
          work.policy(),
          {byte_count{63U << 20U}, byte_count{1U << 20U}, charge},
          charge(byte_count{wire.capacity() + 1U}),
          metadata,
          {},
          0)
          .value();
    return codec::reserve_decode_input(input, work.policy(), available).value();
}
template<typename T>
byte_count vector_cost(const std::vector<T>& values) {
    return charge(byte_count{values.capacity() * sizeof(T)});
}
byte_count writer_memory(
  codec::cooperative_work& work, byte_count metadata, const std::string& wire) {
    return work.policy()
      .remaining_operation_bytes(
        {.retained_input = charge(byte_count{wire.capacity() + 1U}),
         .decoded_metadata = metadata},
        byte_count{63U << 20U})
      .value();
}
template<typename Page>
void check_page(
  const Page& page, codec::decode_budget before, codec::decode_budget after) {
    using entry_type = decltype(page.entries())::value_type;
    const auto served = charge(
      byte_count{page.entry_capacity() * sizeof(entry_type)});
    EXPECT_LE(served.value(), 131072U);
    ASSERT_FALSE(page.entries().empty());
    EXPECT_LE(
      malloc_usable_size(const_cast<entry_type*>(page.entries().data())),
      served.value());
    EXPECT_EQ(
      before.operation_remaining.value() - after.operation_remaining.value(),
      served.value());
    EXPECT_EQ(
      before.metadata_remaining.value() - after.metadata_remaining.value(),
      served.value());
}
template<typename Root>
void check_root(
  const Root& root, codec::decode_budget before, codec::decode_budget after) {
    const auto served = charge(
      byte_count{root.page_capacity() * sizeof(page_ref)});
    EXPECT_LE(root.page_capacity(), 256U);
    EXPECT_LE(root.encoded_bytes().value(), 65536U);
    EXPECT_LE(served.value(), 131072U);
    ASSERT_FALSE(root.pages().empty());
    EXPECT_LE(
      malloc_usable_size(const_cast<page_ref*>(root.pages().data())),
      served.value());
    EXPECT_EQ(
      before.operation_remaining.value() - after.operation_remaining.value(),
      served.value());
    EXPECT_EQ(
      before.metadata_remaining.value() - after.metadata_remaining.value(),
      served.value());
}
std::vector<sparse_index_entry>
index_entries(std::uint32_t first, std::uint32_t count, std::uint64_t a) {
    std::vector<sparse_index_entry> entries;
    entries.reserve(count);
    for (std::uint32_t i = 0; i < count; ++i)
        entries.push_back(
          ix::entry(100U + first + i, (std::uint64_t{first} + i + 1U) * a));
    return entries;
}
std::vector<range_manifest_entry>
manifest_entries(std::uint32_t first, std::uint32_t count) {
    std::vector<range_manifest_entry> entries;
    entries.reserve(count);
    for (std::uint32_t i = 0; i < count; ++i)
        entries.push_back(mi::item(100U + first + i, 101U + first + i, true));
    return entries;
}

void qualify_index(std::size_t h, std::uint64_t a, bool underfilled) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto cap = sparse_index_page_capacity(
                       byte_count{h}, alignment(a), work.policy())
                       .value();
    EXPECT_EQ(cap, h == 32 ? 4083U : 3829U);
    const auto count_per_page = underfilled ? 256U : cap;
    const auto context = ix::target(65536, a);
    std::vector<page_ref> refs;
    refs.reserve((65536U + count_per_page - 1U) / count_per_page);
    for (std::uint32_t first = 0; first < 65536;) {
        const auto count = std::min(count_per_page, 65536U - first);
        const auto entries = index_entries(first, count, a);
        const auto wire = ix::page_wire(
          entries, context, static_cast<std::uint32_t>(refs.size()), first, h);
        if (h == 32 && first == 0) {
            const auto encoded
              = encode_sparse_index_page(
                  entries,
                  context,
                  page_ordinal::make(0).value(),
                  0,
                  work,
                  writer_memory(
                    work,
                    vector_cost(refs).checked_add(vector_cost(entries)).value(),
                    wire),
                  charge)
                  .get();
            ASSERT_TRUE(encoded.has_value());
            EXPECT_EQ(flat(encoded->bytes), wire);
        }
        refs.push_back(
          ix::page_reference(
            wire, count, static_cast<std::uint32_t>(refs.size()), first));
        first += count;
        seastar::thread::maybe_yield();
    }
    EXPECT_EQ(refs.size(), underfilled ? 256U : h == 32 ? 17U : 18U);
    auto root = [&] {
        const auto wire = ix::root_wire(refs, context, h);
        if (h == 32) {
            const auto encoded = encode_sparse_index_root(
                                   context,
                                   65536,
                                   refs,
                                   work,
                                   writer_memory(work, vector_cost(refs), wire),
                                   charge)
                                   .get();
            EXPECT_TRUE(encoded.has_value());
            if (encoded) EXPECT_EQ(flat(encoded->bytes), wire);
        }
        fragmented_buffer_parser input{buffer(wire, 1024)};
        const auto memory = memory_for(input, work, vector_cost(refs), wire);
        auto decoded = decode_sparse_index_root(
                         input,
                         context,
                         codec::immutable_object_digest{ix::sha(wire)},
                         memory,
                         work)
                         .get()
                         .value();
        check_root(decoded.value, memory, decoded.remaining);
        return std::move(decoded.value);
    }();
    sparse_index_verifier walk{root, work.policy()};
    std::uint32_t total = 0;
    const auto owners
      = vector_cost(refs)
          .checked_add(
            charge(byte_count{root.page_capacity() * sizeof(page_ref)}))
          .value();
    for (const auto& ref : root.pages()) {
        const auto entries = index_entries(
          ref.first_entry(), ref.entry_count(), a);
        EXPECT_LE(vector_cost(entries).value(), 131072U);
        const auto wire = ix::page_wire(
          entries, context, ref.ordinal().value(), ref.first_entry(), h);
        EXPECT_LE(wire.size(), 65536U);
        fragmented_buffer_parser input{buffer(wire, 1024)};
        const auto memory = memory_for(
          input, work, owners.checked_add(vector_cost(entries)).value(), wire);
        const auto decoded = walk.next(input, memory, work).get();
        ASSERT_TRUE(decoded.has_value());
        EXPECT_TRUE(std::ranges::equal(decoded->value.entries(), entries));
        EXPECT_LE(decoded->value.entry_capacity(), cap);
        check_page(decoded->value, memory, decoded->remaining);
        total += ref.entry_count();
        // The decoded page, its fixture and input all die before the next page.
    }
    EXPECT_EQ(total, 65536U);
    const auto finished = walk.finish(work);
    ASSERT_TRUE(finished.has_value());
    EXPECT_EQ(finished->entry_count(), total);
    EXPECT_EQ(finished->root_digest(), root.digest());
}

void qualify_manifest(std::size_t h, std::uint64_t a, bool underfilled) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto cap = range_manifest_page_capacity(
                       byte_count{h}, alignment(a), work.policy())
                       .value();
    EXPECT_EQ(cap, h == 32 ? 628U : 589U);
    const auto count_per_page = underfilled ? 256U : cap;
    std::vector<page_ref> refs;
    refs.reserve((65536U + count_per_page - 1U) / count_per_page);
    for (std::uint32_t first = 0; first < 65536;) {
        const auto count = std::min(count_per_page, 65536U - first);
        const auto entries = manifest_entries(first, count);
        const auto ph = mi::page_header(
          count,
          mi::mc(),
          100U + first,
          100U + first + count,
          static_cast<std::uint32_t>(refs.size()),
          first);
        const auto wire = mi::expected_page(ph, entries, a, h);
        if (h == 32 && first == 0) {
            const auto encoded
              = encode_range_manifest_page(
                  ph,
                  entries,
                  alignment(a),
                  work,
                  writer_memory(
                    work,
                    vector_cost(refs).checked_add(vector_cost(entries)).value(),
                    wire),
                  charge)
                  .get();
            ASSERT_TRUE(encoded.has_value());
            EXPECT_EQ(flat(encoded->bytes), wire);
        }
        refs.push_back(mi::manifest_page_reference(wire, ph));
        first += count;
        seastar::thread::maybe_yield();
    }
    EXPECT_EQ(refs.size(), underfilled ? 256U : h == 32 ? 105U : 112U);
    const auto rh = mi::root_header(
      65536, static_cast<std::uint32_t>(refs.size()), mi::mc(), 100, 65636);
    auto root = [&] {
        const auto wire = mi::expected_root(rh, refs, a, h);
        if (h == 32) {
            const auto encoded = encode_range_manifest_root(
                                   rh,
                                   refs,
                                   alignment(a),
                                   work,
                                   writer_memory(work, vector_cost(refs), wire),
                                   charge)
                                   .get();
            EXPECT_TRUE(encoded.has_value());
            if (encoded) EXPECT_EQ(flat(encoded->bytes), wire);
        }
        fragmented_buffer_parser input{buffer(wire, 1024)};
        const auto memory = memory_for(input, work, vector_cost(refs), wire);
        auto decoded = decode_range_manifest_root(
                         input,
                         mi::mc(),
                         rh.logical_span(),
                         alignment(a),
                         codec::immutable_object_digest{mi::manifest_sha(wire)},
                         memory,
                         work)
                         .get()
                         .value();
        check_root(decoded.value, memory, decoded.remaining);
        return std::move(decoded.value);
    }();
    range_manifest_verifier walk{root, work.policy()};
    std::uint32_t total = 0;
    const auto owners
      = vector_cost(refs)
          .checked_add(
            charge(byte_count{root.page_capacity() * sizeof(page_ref)}))
          .value();
    for (const auto& ref : root.pages()) {
        const auto entries = manifest_entries(
          ref.first_entry(), ref.entry_count());
        EXPECT_LE(vector_cost(entries).value(), 131072U);
        const auto ph = mi::page_header(
          ref.entry_count(),
          mi::mc(),
          100U + ref.first_entry(),
          100U + ref.first_entry() + ref.entry_count(),
          ref.ordinal().value(),
          ref.first_entry());
        const auto wire = mi::expected_page(ph, entries, a, h);
        EXPECT_LE(wire.size(), 65536U);
        fragmented_buffer_parser input{buffer(wire, 1024)};
        const auto memory = memory_for(
          input, work, owners.checked_add(vector_cost(entries)).value(), wire);
        const auto decoded = walk.next(input, memory, work).get();
        ASSERT_TRUE(decoded.has_value());
        EXPECT_TRUE(std::ranges::equal(decoded->value.entries(), entries));
        EXPECT_LE(decoded->value.entry_capacity(), cap);
        check_page(decoded->value, memory, decoded->remaining);
        total += ref.entry_count();
    }
    EXPECT_EQ(total, 65536U);
    const auto finished = walk.finish(work);
    ASSERT_TRUE(finished.has_value());
    EXPECT_EQ(finished->entry_count(), total);
    EXPECT_EQ(finished->root_digest(), root.digest());
}

TEST(
  IndexManifestQualificationTest,
  MaximumObjectsStreamAtBothHeaderAndAlignmentEndpoints) {
    for (const std::size_t h : {32U, 4096U}) {
        for (const std::uint64_t a : {512U, 65536U}) {
            SCOPED_TRACE(h);
            SCOPED_TRACE(a);
            qualify_index(h, a, false);
            qualify_manifest(h, a, false);
        }
    }
}
TEST(
  IndexManifestQualificationTest,
  MaximumCountAlsoFitsMaximumUnderfilledPageCount) {
    qualify_index(4096, 65536, true);
    qualify_manifest(4096, 65536, true);
    EXPECT_FALSE(page_ordinal::make(256));
    EXPECT_FALSE(page_count::make(257));
}

TEST(
  IndexManifestQualificationTest,
  NarrowerWireServedAllocationAndMetadataLimitsReject) {
    for (const std::size_t h : {32U, 4096U}) {
        seastar::abort_source abort;
        codec::cooperative_work setup{codec::limits::defaults(), abort};
        const auto cap = range_manifest_page_capacity(
                           byte_count{h}, alignment(), setup.policy())
                           .value();
        const auto entries = manifest_entries(0, cap);
        const auto ph = mi::page_header(cap, mi::mc(), 100, 100U + cap);
        const auto wire = mi::expected_page(ph, entries, 512, h);
        const std::array refs{mi::manifest_page_reference(wire, ph)};
        const auto rh = mi::root_header(cap, 1, mi::mc(), 100, 100U + cap);
        const auto root = mi::pin(mi::expected_root(rh, refs), rh, setup);
        for (const unsigned limit : {0U, 1U, 2U, 3U}) {
            auto config = setup.policy().config();
            if (limit == 0) config.max_page_bytes = byte_count{65535};
            if (limit == 1)
                config.max_allocation_bytes = byte_count{
                  charge(byte_count{cap * sizeof(range_manifest_entry)}).value()
                  - 1U};
            if (limit == 2) config.max_object_entries = item_count{cap - 1U};
            codec::cooperative_work work{
              codec::limits::make(config).value(), abort};
            fragmented_buffer_parser input{buffer(wire, 1024)};
            auto memory = mi::page_memory(input, root, work);
            if (limit == 3)
                memory.metadata_remaining = byte_count{
                  charge(byte_count{cap * sizeof(range_manifest_entry)}).value()
                  - 1U};
            const auto result = decode_range_manifest_page(
                                  input, root, refs[0].ordinal(), memory, work)
                                  .get();
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(result.error().code(), errc::resource_exhausted);
            EXPECT_EQ(input.bytes_consumed().value(), 0U);
        }
        // The fixed body fits, but one extra entry does not fit this actual H.
        const auto extra = manifest_entries(0, cap + 1U);
        const auto over_header = mi::page_header(
          cap + 1U, mi::mc(), 100, 101U + cap);
        const auto over = mi::expected_page(over_header, extra, 512, h);
        EXPECT_GT(over.size(), 65536U);
        // At H=32 the public writer must reject the same one-over boundary.
        if (h == 32) {
            const auto result = encode_range_manifest_page(
                                  over_header,
                                  extra,
                                  alignment(),
                                  setup,
                                  budget().operation_remaining,
                                  charge)
                                  .get();
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(result.error().code(), errc::resource_exhausted);
        }
    }
}
seastar::future<> control_progress(
  seastar::abort_source& stop,
  std::uint64_t& ticks,
  std::chrono::steady_clock::duration& largest_gap) {
    auto previous = std::chrono::steady_clock::now();
    while (!stop.abort_requested()) {
        co_await seastar::yield();
        if (!stop.abort_requested()) {
            const auto now = std::chrono::steady_clock::now();
            largest_gap = std::max(largest_gap, now - previous);
            previous = now;
            ++ticks;
        }
    }
}
TEST(
  IndexManifestQualificationTest,
  MaximumPageAllowsControlProgressAndReportsNativeWork) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto exercise = [&]<typename Root>(
                            const Root& root, const std::string& wire) {
        using walker_type = std::conditional_t<
          std::same_as<Root, range_manifest_root>,
          range_manifest_verifier,
          sparse_index_verifier>;
        fragmented_buffer_parser input{buffer(wire, 1024)};
        // The other 32 MiB covers fixture/root/native/coroutine owners.
        const auto memory = reserve(input, work);
        walker_type walker{root, work.policy()};
        const auto deadline = std::chrono::steady_clock::now()
                              + std::chrono::seconds{2};
        while (!seastar::need_preempt()
               && std::chrono::steady_clock::now() < deadline) {
        }
        ASSERT_TRUE(seastar::need_preempt());
        seastar::abort_source stop;
        std::uint64_t ticks = 0;
        std::chrono::steady_clock::duration largest_gap{};
        auto observer = control_progress(stop, ticks, largest_gap);
        auto joined = seastar::defer([&] {
            stop.request_abort();
            observer.get();
        });
        const auto allocations = seastar::memory::stats().mallocs();
        const auto tasks = seastar::engine().get_sched_stats().tasks_processed;
        auto pending = walker.next(input, memory, work);
        const bool suspended = !pending.available();
        const auto result = pending.get();
        const auto allocated = seastar::memory::stats().mallocs() - allocations;
        const auto processed
          = seastar::engine().get_sched_stats().tasks_processed - tasks;
        ASSERT_TRUE(result.has_value());
        EXPECT_TRUE(walker.finish(work));
        EXPECT_TRUE(suspended);
        EXPECT_GT(ticks, 0U);
        const std::string prefix = std::same_as<Root, range_manifest_root>
                                     ? "manifest_control_"
                                     : "index_control_";
        RecordProperty(prefix + "ticks", std::to_string(ticks));
        RecordProperty(
          prefix + "largest_gap_ns",
          std::to_string(
            std::chrono::duration_cast<std::chrono::nanoseconds>(largest_gap)
              .count()));
        // These counters include the control observer; compare like scopes.
        RecordProperty(prefix + "allocations", std::to_string(allocated));
        RecordProperty(prefix + "tasks", std::to_string(processed));
    };
    {
        const auto context = ix::target(4083);
        const auto entries = index_entries(0, 4083, 512);
        const auto wire = ix::page_wire(entries, context);
        const std::array refs{ix::page_reference(wire, 4083)};
        const auto root = ix::pin(ix::root_wire(refs, context), context, work);
        exercise(root, wire);
    }
    {
        const auto entries = manifest_entries(0, 628);
        const auto header = mi::page_header(628, mi::mc(), 100, 728);
        const auto wire = mi::expected_page(header, entries);
        const std::array refs{mi::manifest_page_reference(wire, header)};
        const auto rh = mi::root_header(628, 1, mi::mc(), 100, 728);
        const auto root = mi::pin(mi::expected_root(rh, refs), rh, work);
        exercise(root, wire);
    }
}
} // namespace
} // namespace kwaque::storage
