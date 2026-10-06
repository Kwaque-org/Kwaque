#include "src/base/allocation.h"
#include "src/base/units.h"
#include "src/bytes/test_allocation_profile.h"
#include "src/codec/limits.h"
#include "src/resource/resource_registry.h"
#include "src/storage/format_context.h"
#include "src/storage/local_store_config.h"
#include "src/storage/sparse_index.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/memory.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/alloc_failure_injector.hh>

#include <boost/test/unit_test.hpp>

#include <array>
#include <cstdint>
#include <iterator>
#include <limits>
#include <map>
#include <new>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

namespace {
using namespace kwaque;
using namespace kwaque::storage;
constexpr auto workload = resource::workload_class::metadata;
constexpr std::uint64_t stride_bytes = 32_KiB;
constexpr std::uint64_t chunk_entries = 4096;

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
          manager.acquire_workload(workload),
          {.tasks = 4, .bytes = byte_count{4_MiB}, .handles = 1},
          bytes::testing::charge};
        body(budget);
        BOOST_CHECK_EQUAL(budget.snapshot().tasks, 0U);
        BOOST_CHECK_EQUAL(budget.snapshot().bytes, 0U);
    } catch (...) {
        failure = std::current_exception();
    }
    co_await manager.stop();
    co_await registry.stop();
    if (failure) std::rethrow_exception(failure);
}

coverage block(
  std::uint64_t base,
  std::uint64_t ordinal,
  std::uint64_t records,
  std::uint64_t position,
  std::uint64_t size) {
    return coverage{
      model::range_logical_span::from_count(
        model::range_logical_end{base}, model::range_logical_count{records})
        .value(),
      model::segment_relative_span::from_count(
        model::segment_relative_end{ordinal},
        model::segment_record_count{records})
        .value(),
      model::file_byte_span::from_size(
        runtime::file_position{position}, byte_count{size})
        .value()};
}

// A block whose original span is wider than the records it kept.
coverage spanned(
  std::uint64_t base,
  std::uint64_t span,
  std::uint64_t ordinal,
  std::uint64_t records,
  std::uint64_t position,
  std::uint64_t size) {
    return coverage{
      model::range_logical_span::from_count(
        model::range_logical_end{base}, model::range_logical_count{span})
        .value(),
      model::segment_relative_span::from_count(
        model::segment_relative_end{ordinal},
        model::segment_record_count{records})
        .value(),
      model::file_byte_span::from_size(
        runtime::file_position{position}, byte_count{size})
        .value()};
}

using outcome = runtime::result<void>;
outcome failed(errc code) {
    return runtime::failure(
      runtime::operation_error{code, runtime::operation_kind::file});
}
seastar::future<outcome> done(outcome value = {}) {
    return seastar::make_ready_future<outcome>(std::move(value));
}
// Work for a request that must join a rebuild already running.
seastar::future<outcome> never_runs(seastar::abort_source&) {
    BOOST_ERROR("a joining request started its own rebuild");
    return done();
}
bool is(const outcome& value, errc code) {
    return !value && value.error().code() == code;
}

// Writes dense blocks one after another, as a segment does, and keeps where
// each began.
struct feeder final {
    active_sparse_index& index;
    std::uint64_t base, position, ordinal{0};
    std::map<std::uint64_t, std::uint64_t> begins{};
    void add(std::uint64_t records, std::uint64_t size) {
        index.written(block(base, ordinal, records, position, size));
        begins.emplace(base, position);
        base += records;
        ordinal += records;
        position += size;
    }
    void sync() { index.durable(runtime::file_position{position}); }
};

active_sparse_index make(
  workload_budget& budget, sparse_index_stride stride, std::uint32_t capacity) {
    auto made = active_sparse_index::make(stride, capacity, budget);
    BOOST_REQUIRE(made.has_value());
    return std::move(*made);
}

// A seeded stream of choices, so one seed replays one index exactly.
class seeded_choices final {
public:
    explicit seeded_choices(std::uint64_t seed) noexcept
      : state_(seed) {}
    // Uniform in [0, bound).
    std::uint64_t uniform(std::uint64_t bound) noexcept {
        state_ += 0x9e3779b97f4a7c15ULL;
        auto z = state_;
        z = (z ^ (z >> 30U)) * 0xbf58476d1ce4e5b9ULL;
        z = (z ^ (z >> 27U)) * 0x94d049bb133111ebULL;
        return (z ^ (z >> 31U)) % bound;
    }

private:
    std::uint64_t state_;
};

void check(
  const active_sparse_index& index,
  std::uint32_t anchor,
  std::uint64_t base,
  std::uint64_t position) {
    BOOST_REQUIRE_LT(anchor, index.size());
    BOOST_CHECK_EQUAL(index[anchor].logical_anchor().value(), base);
    BOOST_CHECK_EQUAL(index[anchor].block_position().value(), position);
}
} // namespace

SEASTAR_TEST_CASE(sparse_index_capacity_follows_the_stride_and_the_blocks) {
    constexpr byte_count data{1_GiB};
    BOOST_CHECK_EQUAL(
      sparse_index_capacity({}, data, maximum_object_entries), 32769U);
    BOOST_CHECK_EQUAL(
      sparse_index_capacity({byte_count{4_KiB}}, data, maximum_object_entries),
      maximum_object_entries);
    BOOST_CHECK_EQUAL(
      sparse_index_capacity({byte_count{16_KiB}}, data, maximum_object_entries),
      maximum_object_entries);
    BOOST_CHECK_EQUAL(
      sparse_index_capacity({}, byte_count{128_MiB}, maximum_object_entries),
      4097U);
    // No more anchors than blocks, whatever the stride.
    BOOST_CHECK_EQUAL(sparse_index_capacity({}, data, 100), 100U);
    BOOST_CHECK_EQUAL(sparse_index_capacity({}, data, UINT32_MAX), 32769U);
    // Every block may be an anchor.
    BOOST_CHECK_EQUAL(
      sparse_index_capacity({byte_count{0}}, data, maximum_object_entries),
      maximum_object_entries);
    BOOST_CHECK_EQUAL(
      sparse_index_capacity({byte_count{32_KiB}, 1000}, data, 4096), 4096U);
    // A stride longer than the file leaves the first block.
    BOOST_CHECK_EQUAL(
      sparse_index_capacity({byte_count{2_GiB}}, data, maximum_object_entries),
      1U);
    const byte_count most{std::numeric_limits<std::uint64_t>::max()};
    BOOST_CHECK_EQUAL(
      sparse_index_capacity({byte_count{1}}, most, maximum_object_entries),
      maximum_object_entries);
    BOOST_CHECK_EQUAL(
      sparse_index_capacity({most}, most, maximum_object_entries), 2U);
    const sparse_index_stride standard;
    BOOST_CHECK_EQUAL(standard.bytes.value(), stride_bytes);
    BOOST_CHECK_EQUAL(standard.records, 0U);
    co_return;
}

SEASTAR_TEST_CASE(sparse_index_admits_its_whole_table_when_made) {
    co_await with_budget([](workload_budget& budget) {
        const auto zero = active_sparse_index::make({}, 0, budget);
        BOOST_REQUIRE(!zero);
        BOOST_CHECK(zero.error().code() == errc::invalid_argument);
        const auto large = active_sparse_index::make(
          {}, maximum_object_entries + 1, budget);
        BOOST_REQUIRE(!large);
        BOOST_CHECK(large.error().code() == errc::invalid_argument);
        BOOST_CHECK_EQUAL(budget.snapshot().bytes, 0U);

        const auto charged = [&budget](std::uint64_t bytes) {
            return budget.allocation_charge(byte_count{bytes}).value().value();
        };
        const auto reserved = [&budget](std::uint64_t bytes) {
            return budget.reservation_charge(byte_count{bytes}).value().value();
        };
        const auto chunk = chunk_entries * sizeof(sparse_index_entry);
        BOOST_CHECK_LE(charged(chunk), maximum_contiguous_allocation_bytes);
        const auto list = sizeof(std::vector<sparse_index_entry>);
        {
            // One whole chunk and the one entry past it, each at its size.
            const auto small = make(budget, {}, 4097);
            BOOST_CHECK_EQUAL(small.capacity(), 4097U);
            BOOST_CHECK_EQUAL(
              budget.snapshot().bytes,
              reserved(
                charged(2 * list) + charged(chunk)
                + charged(sizeof(sparse_index_entry))));
            BOOST_CHECK_EQUAL(budget.snapshot().tasks, 1U);
        }
        BOOST_CHECK_EQUAL(budget.snapshot().bytes, 0U);
        {
            const auto full = make(budget, {}, maximum_object_entries);
            const auto used = budget.snapshot().bytes;
            BOOST_CHECK_EQUAL(
              used, reserved(charged(16 * list) + 16 * charged(chunk)));
            BOOST_CHECK_GE(
              used, maximum_object_entries * sizeof(sparse_index_entry));
            // Leave less than a second one needs.
            auto held = budget.try_reserve(byte_count{4_MiB - used - used / 2});
            BOOST_REQUIRE(held.has_value());
            const auto refused = active_sparse_index::make(
              {}, maximum_object_entries, budget);
            BOOST_REQUIRE(!refused);
            BOOST_CHECK(refused.error().code() == errc::queue_full);
        }
    });
}

SEASTAR_TEST_CASE(sparse_index_anchors_follow_durability_and_are_kept) {
    co_await with_budget([](workload_budget& budget) {
        auto index = make(budget, {byte_count{0}}, 8);
        feeder segment{index, 500, 8192};
        segment.add(10, 4096);
        segment.add(10, 4096);
        segment.add(10, 4096);
        BOOST_CHECK(index.empty());
        BOOST_CHECK_EQUAL(index.size(), 0U);
        // Only blocks that begin below the durable end.
        index.durable(runtime::file_position{8192});
        BOOST_CHECK_EQUAL(index.size(), 0U);
        index.durable(runtime::file_position{12288});
        BOOST_CHECK_EQUAL(index.size(), 1U);
        index.durable(runtime::file_position{16384});
        BOOST_CHECK_EQUAL(index.size(), 2U);
        // Never fewer.
        index.durable(runtime::file_position{8192});
        index.durable(runtime::file_position{});
        BOOST_CHECK_EQUAL(index.size(), 2U);
        segment.sync();
        segment.sync();
        BOOST_REQUIRE_EQUAL(index.size(), 3U);
        check(index, 0, 500, 8192);
        check(index, 1, 510, 12288);
        check(index, 2, 520, 16384);
        // A durable end past every written block admits nothing more.
        index.durable(
          runtime::file_position{std::numeric_limits<std::uint64_t>::max()});
        BOOST_CHECK_EQUAL(index.size(), 3U);
        segment.add(1, 4096);
        BOOST_CHECK_EQUAL(index.size(), 3U);
        segment.sync();
        BOOST_CHECK_EQUAL(index.size(), 4U);
        BOOST_CHECK_EQUAL(index.skipped(), 0U);
    });
}

SEASTAR_TEST_CASE(sparse_index_first_block_then_one_per_byte_stride) {
    co_await with_budget([](workload_budget& budget) {
        {
            // Blocks exactly one stride long are all anchors.
            auto index = make(budget, {}, 1024);
            feeder segment{index, 0, 4096};
            for (std::uint32_t i = 0; i < 1024; ++i)
                segment.add(1, stride_bytes);
            segment.sync();
            BOOST_REQUIRE_EQUAL(index.size(), 1024U);
            check(index, 0, 0, 4096);
            check(index, 1023, 1023, 4096 + 1023 * stride_bytes);
        }
        {
            // Variable sizes: a block is an anchor when it begins a stride
            // or more after the last anchor's block, however long that was.
            auto index = make(budget, {}, 16);
            feeder segment{index, 824, 0};
            segment.add(25, 155103);
            segment.add(30, 168865);
            segment.add(22, 134080);
            segment.add(25, 142073);
            segment.add(22, 126886);
            segment.add(2, 1667);
            // Within a stride of the last anchor.
            segment.add(3, 4096);
            segment.sync();
            BOOST_REQUIRE_EQUAL(index.size(), 6U);
            check(index, 0, 824, 0);
            check(index, 1, 849, 155103);
            check(index, 2, 879, 323968);
            check(index, 3, 901, 458048);
            check(index, 4, 926, 600121);
            check(index, 5, 948, 727007);
        }
        {
            // The boundary itself, one byte short of it, and a block many
            // strides long, which is one anchor.
            auto index = make(budget, {}, 16);
            feeder segment{index, 7, 4096};
            segment.add(1, stride_bytes - 1);
            segment.add(1, 1);
            segment.add(1, 10 * stride_bytes);
            segment.add(1, 4096);
            segment.add(1, stride_bytes - 4096 - 1);
            segment.add(1, 1);
            segment.add(1, 4096);
            segment.sync();
            BOOST_REQUIRE_EQUAL(index.size(), 4U);
            check(index, 0, 7, 4096);
            check(index, 1, 9, 4096 + stride_bytes);
            check(index, 2, 10, 4096 + 11 * stride_bytes);
            check(index, 3, 13, 4096 + 12 * stride_bytes);
        }
        {
            // A zero stride anchors every block.
            auto index = make(budget, {byte_count{0}}, 64);
            feeder segment{index, 0, 4096};
            for (std::uint32_t i = 0; i < 64; ++i)
                segment.add(3, 512);
            segment.sync();
            BOOST_REQUIRE_EQUAL(index.size(), 64U);
            check(index, 63, 189, 4096 + 63 * 512);
        }
    });
}

SEASTAR_TEST_CASE(sparse_index_record_stride_forces_anchors) {
    co_await with_budget([](workload_budget& budget) {
        // Blocks too small for the byte stride ever to choose one.
        const byte_count never{std::numeric_limits<std::uint64_t>::max()};
        {
            auto index = make(budget, {never, 100}, 1000);
            feeder segment{index, 0, 4096};
            for (std::uint32_t i = 0; i < 1000; ++i)
                segment.add(1, 64);
            segment.sync();
            BOOST_REQUIRE_EQUAL(index.size(), 10U);
            for (std::uint32_t i = 0; i < 10; ++i)
                check(index, i, 100ULL * i, 4096 + 6400ULL * i);
        }
        {
            // Evaluated where a block begins: a block is never split.
            auto index = make(budget, {never, 100}, 16);
            feeder segment{index, 0, 4096};
            segment.add(99, 64);
            segment.add(1, 64);
            segment.add(250, 64);
            segment.add(1, 64);
            segment.add(99, 64);
            segment.add(1, 64);
            segment.sync();
            BOOST_REQUIRE_EQUAL(index.size(), 4U);
            check(index, 0, 0, 4096);
            check(index, 1, 100, 4096 + 128);
            check(index, 2, 350, 4096 + 192);
            check(index, 3, 450, 4096 + 320);
        }
        {
            // Without one only the first block is chosen.
            auto index = make(budget, {never}, 16);
            feeder segment{index, 0, 4096};
            for (std::uint32_t i = 0; i < 1000; ++i)
                segment.add(1, 64);
            segment.sync();
            BOOST_REQUIRE_EQUAL(index.size(), 1U);
        }
        {
            // Either distance chooses a block.
            auto index = make(budget, {byte_count{stride_bytes}, 100}, 16);
            feeder segment{index, 0, 4096};
            segment.add(10, stride_bytes);
            segment.add(10, 64);
            segment.add(200, 64);
            segment.add(10, 64);
            segment.sync();
            BOOST_REQUIRE_EQUAL(index.size(), 3U);
            check(index, 1, 10, 4096 + stride_bytes);
            check(index, 2, 220, 4096 + stride_bytes + 128);
        }
    });
}

// The stride's rule and the capacity it is sized by, together: a table sized
// for a stride and a data file is never short of room for the blocks that
// stride chooses in that file, whatever their sizes, and the blocks it
// chooses are the ones the rule, restated here, chooses.
SEASTAR_TEST_CASE(sparse_index_sized_by_its_stride_never_fills) {
    co_await with_budget([](workload_budget& budget) {
        constexpr std::uint64_t cap = 1_MiB;
        seeded_choices choice{0x51ed};
        for (const std::uint64_t stride :
             std::array<std::uint64_t, 5>{1, 4096, 32768, 3000, cap}) {
            // Blocks of any size, and blocks exactly one stride long, which
            // put the most anchors in the file.
            for (const bool exact : {false, true}) {
                const sparse_index_stride rule{byte_count{stride}};
                const auto capacity = sparse_index_capacity(
                  rule, byte_count{cap}, maximum_object_entries);
                auto index = make(budget, rule, capacity);
                std::map<std::uint64_t, std::uint64_t> chosen;
                std::uint64_t position = 0, base = 100, ordinal = 0, last = 0;
                for (;;) {
                    const auto size = exact
                                        ? std::max<std::uint64_t>(stride, 64)
                                        : 64 * (1 + choice.uniform(192));
                    if (position + size > cap) break;
                    if (chosen.empty() || position - last >= stride) {
                        chosen.emplace(position, base);
                        last = position;
                    }
                    index.written(block(base, ordinal, 1, position, size));
                    position += size;
                    base += 1 + choice.uniform(3);
                    ++ordinal;
                }
                index.durable(runtime::file_position{position});
                BOOST_CHECK_EQUAL(index.skipped(), 0U);
                BOOST_CHECK_LE(index.size(), capacity);
                BOOST_REQUIRE_EQUAL(index.size(), chosen.size());
                std::uint32_t anchor = 0;
                for (const auto& [where, first] : chosen)
                    check(index, anchor++, first, where);
            }
        }
    });
}

// An index is moved with its table. What it was moved from holds no anchor
// and takes none: nothing can read an entry through it.
SEASTAR_TEST_CASE(sparse_index_moves_with_its_table) {
    co_await with_budget([](workload_budget& budget) {
        auto index = make(budget, {byte_count{0}}, 4);
        feeder segment{index, 100, 4096};
        segment.add(1, 4096);
        segment.add(1, 4096);
        segment.sync();
        index.freeze();
        const active_sparse_index moved{std::move(index)};
        BOOST_REQUIRE_EQUAL(moved.size(), 2U);
        BOOST_CHECK(moved.frozen());
        BOOST_CHECK_EQUAL(moved.pages(), 1U);
        check(moved, 0, 100, 4096);
        check(moved, 1, 101, 8192);
        // NOLINTBEGIN(bugprone-use-after-move,clang-analyzer-cplusplus.Move)
        BOOST_CHECK(index.empty());
        BOOST_CHECK_EQUAL(index.pages(), 0U);
        BOOST_CHECK_EQUAL(index.capacity(), 0U);
        // NOLINTEND(bugprone-use-after-move,clang-analyzer-cplusplus.Move)
    });
}

SEASTAR_TEST_CASE(sparse_index_keeps_whole_coordinates) {
    co_await with_budget([](workload_budget& budget) {
        constexpr std::uint64_t wide = std::uint64_t{1} << 32;
        {
            // The first block is an anchor however far from zero it lies,
            // and bases more than 32 bits apart resolve to themselves.
            auto index = make(budget, {byte_count{0}}, 8);
            index.written(block(wide + 1, 0, 1, 4096, 4096));
            index.written(block(wide + 2, 1, 98, 8192, 4096));
            index.written(block(2 * wide + 1, 99, 9, 12288, 4096));
            index.written(block(2 * wide + 10, 108, 1, wide + 4096, 4096));
            index.written(block(3 * wide, 109, 1, 3 * wide, 4096));
            index.durable(runtime::file_position{4 * wide});
            BOOST_REQUIRE_EQUAL(index.size(), 5U);
            check(index, 0, wide + 1, 4096);
            check(index, 1, wide + 2, 8192);
            check(index, 2, 2 * wide + 1, 12288);
            check(index, 3, 2 * wide + 10, wide + 4096);
            check(index, 4, 3 * wide, 3 * wide);
        }
        {
            const auto last = std::numeric_limits<std::uint64_t>::max();
            auto index = make(budget, {}, 8);
            index.written(block(0, 0, 1, 0, 4096));
            index.written(block(last - 2, last - 2, 1, last - 8192, 4096));
            index.written(block(last - 1, last - 1, 1, last - 4096, 4096));
            index.durable(runtime::file_position{last});
            BOOST_REQUIRE_EQUAL(index.size(), 2U);
            check(index, 1, last - 2, last - 8192);
        }
        {
            // A block that holds no record has no base to anchor.
            auto index = make(budget, {byte_count{0}}, 8);
            index.written(block(40, 0, 0, 4096, 4096));
            index.written(block(40, 0, 5, 8192, 4096));
            index.written(block(45, 5, 0, 12288, 4096));
            index.written(block(45, 5, 5, 16384, 4096));
            index.durable(runtime::file_position{20480});
            BOOST_REQUIRE_EQUAL(index.size(), 2U);
            check(index, 0, 40, 8192);
            check(index, 1, 45, 16384);
        }
    });
}

SEASTAR_TEST_CASE(sparse_index_grows_by_chunks_under_its_first_admission) {
    co_await with_budget([](workload_budget& budget) {
        auto index = make(budget, {byte_count{0}}, maximum_object_entries);
        const auto admitted = budget.snapshot();
        feeder segment{index, 1000, 4096};
        for (std::uint32_t i = 0; i < maximum_object_entries; ++i)
            segment.add(2, 512);
        segment.sync();
        BOOST_REQUIRE_EQUAL(index.size(), maximum_object_entries);
        for (const std::uint32_t i :
             {0U, 1U, 4095U, 4096U, 4097U, 8192U, 65535U})
            check(index, i, 1000 + 2ULL * i, 4096 + 512ULL * i);
        // Against an ordered map of every block written.
        std::uint32_t anchor = 0;
        bool same = true;
        for (const auto& [base, position] : segment.begins) {
            same = same && index[anchor].logical_anchor().value() == base
                   && index[anchor].block_position().value() == position;
            ++anchor;
        }
        BOOST_CHECK(same);
        BOOST_CHECK_EQUAL(index.skipped(), 0U);
        BOOST_CHECK_EQUAL(budget.snapshot().bytes, admitted.bytes);
        BOOST_CHECK_EQUAL(budget.snapshot().accepted, admitted.accepted);
        BOOST_CHECK_EQUAL(budget.snapshot().rejected, admitted.rejected);
        // A table that is full passes blocks over and keeps what it has.
        segment.add(2, 512);
        segment.sync();
        BOOST_CHECK_EQUAL(index.size(), maximum_object_entries);
        BOOST_CHECK_EQUAL(index.skipped(), 1U);
    });
}

SEASTAR_TEST_CASE(sparse_index_freezes_into_contiguous_pages) {
    co_await with_budget([](workload_budget& budget) {
        auto index = make(budget, {byte_count{0}}, 6000);
        feeder segment{index, 100, 4096};
        for (std::uint32_t i = 0; i < 5000; ++i)
            segment.add(1, 512);
        // Written and not yet durable: no anchor and so no page.
        BOOST_CHECK_EQUAL(index.pages(), 0U);
        segment.sync();
        BOOST_CHECK(!index.frozen());
        index.freeze();
        BOOST_CHECK(index.frozen());
        BOOST_REQUIRE_EQUAL(index.size(), 5000U);
        BOOST_REQUIRE_EQUAL(index.pages(), 5U);
        std::uint32_t anchor = 0;
        bool same = true;
        for (std::uint32_t ordinal = 0; ordinal < index.pages(); ++ordinal) {
            const auto page = index.page(ordinal);
            BOOST_CHECK_EQUAL(
              page.size(),
              ordinal < 4 ? std::size_t{sparse_index_page_entries} : 904U);
            // One run of the table: no page lies across two chunks.
            same = same && page.data() == &index[anchor];
            for (const auto& entry : page)
                same = same && entry == index[anchor++];
        }
        BOOST_CHECK(same);
        BOOST_CHECK_EQUAL(anchor, index.size());
        check(index, 1024, 1124, 4096 + 1024ULL * 512);
        check(index, 4096, 4196, 4096 + 4096ULL * 512);
    });
}

SEASTAR_TEST_CASE(sparse_index_page_fits_the_reader_budget) {
    const auto alignment = storage_alignment::make(byte_count{4096}).value();
    const auto policy = codec::limits::defaults();
    local_store_io_limits limits{bytes::testing::charge};
    const auto anchors = maximum_object_entries;
    BOOST_CHECK(
      validate_sparse_index_pages(limits, alignment, policy, anchors));
    BOOST_CHECK_EQUAL(
      sparse_index_page_entries * sizeof(sparse_index_entry), 16_KiB);
    // The decoded array is charged at most half the metadata budget.
    BOOST_CHECK_LE(
      bytes::testing::charge(byte_count{16_KiB}).value(),
      limits.metadata_bytes.value() / 2);
    auto small = limits;
    small.metadata_bytes = byte_count{32_KiB};
    const auto refused = validate_sparse_index_pages(
      small, alignment, policy, anchors);
    BOOST_REQUIRE(!refused);
    BOOST_CHECK(refused.error().code() == errc::resource_exhausted);
    // An index of fewer anchors than a page has one short page, and that is
    // the page its reader holds: limits too small for a full page still read
    // it. An index without an anchor has no page at all.
    BOOST_CHECK(validate_sparse_index_pages(small, alignment, policy, 16));
    BOOST_CHECK(validate_sparse_index_pages(small, alignment, policy, 0));
    auto narrow = limits;
    narrow.operation_bytes = byte_count{64_KiB};
    const auto slow = validate_sparse_index_pages(
      narrow, alignment, policy, anchors);
    BOOST_REQUIRE(!slow);
    BOOST_CHECK(slow.error().code() == errc::resource_exhausted);
    const auto unset = validate_sparse_index_pages(
      {}, alignment, policy, anchors);
    BOOST_REQUIRE(!unset);
    BOOST_CHECK(unset.error().code() == errc::invalid_argument);
    // A policy that lets one object hold fewer pages or entries than the
    // index has is refused before any index is fed, not when it is named.
    auto config = policy.config();
    config.max_object_pages = item_count{16};
    const auto paged = codec::limits::make(config).value();
    BOOST_CHECK(validate_sparse_index_pages(
      limits, alignment, paged, 16 * sparse_index_page_entries));
    const auto pages = validate_sparse_index_pages(
      limits, alignment, paged, 16 * sparse_index_page_entries + 1);
    BOOST_REQUIRE(!pages);
    BOOST_CHECK(pages.error().code() == errc::resource_exhausted);
    // Fewer entries an object than a page holds: every index the policy
    // admits is one short page, which a reader under it decodes.
    config = policy.config();
    config.max_object_entries = item_count{1000};
    const auto counted = codec::limits::make(config).value();
    BOOST_CHECK(validate_sparse_index_pages(limits, alignment, counted, 1000));
    const auto entries = validate_sparse_index_pages(
      limits, alignment, counted, 1001);
    BOOST_REQUIRE(!entries);
    BOOST_CHECK(entries.error().code() == errc::resource_exhausted);
    co_return;
}

SEASTAR_TEST_CASE(sparse_index_finds_the_nearest_anchor_at_or_before) {
    const auto offset = [](std::uint64_t value) {
        return model::range_logical_offset::make(value).value();
    };
    const auto at = [&offset](std::uint64_t base, std::uint64_t position) {
        return sparse_index_entry{
          offset(base), runtime::file_position{position}};
    };
    const runtime::file_position end{600};
    const auto last = std::numeric_limits<std::uint64_t>::max() - 1;
    {
        const std::array anchors{
          at(3, 0),
          at(5, 100),
          at(20, 200),
          at(30, 300),
          at(50, 400),
          at(60, 500)};
        // Nothing lies at or before an offset below the first anchor.
        BOOST_CHECK(!find_sparse_index_anchor(anchors, offset(2), end));
        BOOST_CHECK(!find_sparse_index_anchor(anchors, offset(0), end));
        BOOST_CHECK(!find_sparse_index_anchor({}, offset(2), end));
        // Offset, the position of its anchor's block, and the scan's end.
        struct probe {
            std::uint64_t target, position, end;
        };
        for (const auto expected : std::array{
               probe{3, 0, 100},
               probe{4, 0, 100},
               probe{5, 100, 200},
               probe{6, 100, 200},
               probe{19, 100, 200},
               probe{20, 200, 300},
               probe{21, 200, 300},
               probe{29, 200, 300},
               probe{30, 300, 400},
               probe{31, 300, 400},
               probe{49, 300, 400},
               probe{50, 400, 500},
               probe{51, 400, 500},
               probe{59, 400, 500},
               probe{60, 500, 600},
               probe{61, 500, 600},
               probe{65, 500, 600},
               probe{66, 500, 600},
               probe{last, 500, 600}}) {
            const auto found = find_sparse_index_anchor(
              anchors, offset(expected.target), end);
            BOOST_REQUIRE(found.has_value());
            BOOST_CHECK_EQUAL(
              found->anchor.block_position().value(), expected.position);
            BOOST_CHECK_LE(
              found->anchor.logical_anchor().value(), expected.target);
            BOOST_CHECK_EQUAL(found->end.value(), expected.end);
        }
    }
    {
        // Variable distances: each anchor's own base, and the offsets
        // between two anchors and after the last.
        const std::array anchors{
          at(824, 0),
          at(849, 155103),
          at(879, 323968),
          at(901, 458048),
          at(926, 600121)};
        const runtime::file_position extent{727007};
        for (std::size_t i = 0; i < anchors.size(); ++i) {
            const auto found = find_sparse_index_anchor(
              anchors, anchors[i].logical_anchor(), extent);
            BOOST_REQUIRE(found.has_value());
            BOOST_CHECK(found->anchor == anchors[i]);
        }
        const auto between = find_sparse_index_anchor(
          anchors, offset(947), extent);
        BOOST_REQUIRE(between.has_value());
        BOOST_CHECK(between->anchor == anchors[4]);
        BOOST_CHECK(between->end == extent);
    }
    {
        // Bases more than 32 bits apart resolve to their own anchor.
        constexpr std::uint64_t wide = std::uint64_t{1} << 32;
        const std::array anchors{
          at(0, 1), at(100, 2), at(wide + 1, 3), at(wide + 10, 4)};
        for (const auto [target, position] :
             std::array<std::pair<std::uint64_t, std::uint64_t>, 6>{
               {{100, 2},
                {wide, 2},
                {wide + 1, 3},
                {wide + 9, 3},
                {wide + 10, 4},
                {last, 4}}}) {
            const auto found = find_sparse_index_anchor(
              anchors, offset(target), end);
            BOOST_REQUIRE(found.has_value());
            BOOST_CHECK_EQUAL(found->anchor.block_position().value(), position);
        }
    }
    {
        // The page that can hold an offset's anchor: the last one that
        // begins at or below it.
        const std::array firsts{offset(3), offset(20), offset(50)};
        BOOST_CHECK(!find_sparse_index_page(firsts, offset(2)));
        BOOST_CHECK(!find_sparse_index_page({}, offset(2)));
        for (const auto [target, page] :
             std::array<std::pair<std::uint64_t, std::uint32_t>, 7>{
               {{3, 0},
                {19, 0},
                {20, 1},
                {21, 1},
                {49, 1},
                {50, 2},
                {last, 2}}}) {
            const auto found = find_sparse_index_page(firsts, offset(target));
            BOOST_REQUIRE(found.has_value());
            BOOST_CHECK_EQUAL(*found, page);
        }
    }
    co_return;
}

SEASTAR_TEST_CASE(sparse_index_lookup_follows_durable_anchors_across_chunks) {
    co_await with_budget([](workload_budget& budget) {
        const auto offset = [](std::uint64_t value) {
            return model::range_logical_offset::make(value).value();
        };
        auto index = make(budget, {byte_count{0}}, 6000);
        feeder segment{index, 100, 4096};
        for (std::uint32_t i = 0; i < 5000; ++i)
            segment.add(2, 512);
        const runtime::file_position written{segment.position};
        // Written blocks are no answer until they are durable.
        BOOST_CHECK(!index.find(offset(5000), written));
        const runtime::file_position durable{4096 + 3000ULL * 512};
        index.durable(durable);
        const auto bounded = index.find(offset(100 + 2 * 4000), durable);
        BOOST_REQUIRE(bounded.has_value());
        BOOST_CHECK_EQUAL(
          bounded->anchor.logical_anchor().value(), 100U + 2 * 2999);
        BOOST_CHECK(bounded->end == durable);
        segment.sync();
        BOOST_CHECK(!index.find(offset(99), written));
        // Against an ordered map of every block: the floor of the offset,
        // and where the block after it begins.
        bool same = true;
        for (const std::uint64_t target : std::array<std::uint64_t, 11>{
               100,
               101,
               102,
               8290,
               8291,
               8292,
               8293,
               10098,
               10099,
               10100,
               std::numeric_limits<std::uint64_t>::max() - 1}) {
            const auto found = index.find(offset(target), written);
            auto after = segment.begins.upper_bound(target);
            const auto next = after == segment.begins.end() ? written.value()
                                                            : after->second;
            const auto floor = std::prev(after);
            same = same && found
                   && found->anchor.logical_anchor().value() == floor->first
                   && found->anchor.block_position().value() == floor->second
                   && found->end.value() == next;
        }
        BOOST_CHECK(same);
        index.freeze();
        // A page searched alone answers as the whole table does, up to
        // where the page ends.
        const auto paged = find_sparse_index_anchor(
          index.page(4), offset(100 + 2 * 4500 + 1), written);
        BOOST_REQUIRE(paged.has_value());
        BOOST_CHECK(paged == index.find(offset(100 + 2 * 4500 + 1), written));
        const auto routed = find_sparse_index_page(
          std::array{
            index.page(0).front().logical_anchor(),
            index.page(1).front().logical_anchor(),
            index.page(2).front().logical_anchor(),
            index.page(3).front().logical_anchor(),
            index.page(4).front().logical_anchor()},
          offset(100 + 2 * 4500 + 1));
        BOOST_REQUIRE(routed.has_value());
        BOOST_CHECK_EQUAL(*routed, 4U);
    });
}

SEASTAR_TEST_CASE(sparse_index_keeps_original_bases_across_removed_slots) {
    co_await with_budget([](workload_budget& budget) {
        const auto offset = [](std::uint64_t value) {
            return model::range_logical_offset::make(value).value();
        };
        const runtime::file_position end{16384};
        // Three records, then a batch that kept two of its five after six
        // removed slots, then one record far on.
        const std::array blocks{
          spanned(100, 3, 0, 3, 4096, 4096),
          spanned(110, 5, 3, 2, 8192, 4096),
          spanned(200, 1, 5, 1, 12288, 4096)};
        auto index = make(budget, {byte_count{0}}, 8);
        for (const auto& kept : blocks)
            index.written(kept);
        index.durable(end);
        BOOST_REQUIRE_EQUAL(index.size(), 3U);
        check(index, 0, 100, 4096);
        check(index, 1, 110, 8192);
        check(index, 2, 200, 12288);
        struct probe {
            std::uint64_t target, base, end;
        };
        // An offset in removed slots answers with the anchor before the
        // next surviving block, and the scan ends where that block begins.
        for (const auto expected : std::array{
               probe{100, 100, 8192},
               probe{102, 100, 8192},
               probe{105, 100, 8192},
               probe{109, 100, 8192},
               probe{110, 110, 12288},
               probe{113, 110, 12288},
               probe{150, 110, 12288},
               probe{199, 110, 12288},
               probe{200, 200, 16384},
               probe{5000, 200, 16384}}) {
            const auto found = index.find(offset(expected.target), end);
            BOOST_REQUIRE(found.has_value());
            BOOST_CHECK_EQUAL(
              found->anchor.logical_anchor().value(), expected.base);
            BOOST_CHECK_EQUAL(found->end.value(), expected.end);
        }
        BOOST_CHECK(!index.find(offset(99), end));
        // A record stride counts the records a block kept.
        const byte_count never{std::numeric_limits<std::uint64_t>::max()};
        auto strided = make(budget, {never, 3}, 8);
        for (const auto& kept : blocks)
            strided.written(kept);
        strided.durable(end);
        BOOST_REQUIRE_EQUAL(strided.size(), 2U);
        check(strided, 1, 110, 8192);
    });
}

SEASTAR_TEST_CASE(sparse_index_rebuild_starts_once_and_is_shared) {
    sparse_index_rebuild_limit shard;
    sparse_index_rebuilder rebuilder{shard};
    BOOST_CHECK(!rebuilder.running());
    {
        // A request with nothing in flight starts the work.
        seastar::abort_source caller;
        bool ran = false;
        const auto cold = co_await rebuilder.run(
          [&](seastar::abort_source&) {
              ran = true;
              BOOST_CHECK(rebuilder.running());
              BOOST_CHECK_EQUAL(shard.running(), 1U);
              return done();
          },
          caller);
        BOOST_CHECK(ran && cold.has_value());
        BOOST_CHECK(!rebuilder.running());
        BOOST_CHECK_EQUAL(shard.running(), 0U);
    }
    {
        // A caller already released starts nothing.
        seastar::abort_source caller;
        caller.request_abort();
        bool ran = false;
        const auto refused = co_await rebuilder.run(
          [&](seastar::abort_source&) {
              ran = true;
              return done();
          },
          caller);
        BOOST_CHECK(!ran && is(refused, errc::aborted));
        BOOST_CHECK(!rebuilder.running());
    }
    for (const bool succeeds : {true, false}) {
        // Requests during a rebuild join it and receive its outcome.
        seastar::promise<outcome> work;
        seastar::abort_source first_caller, second_caller;
        auto first = rebuilder.run(
          [&](seastar::abort_source&) { return work.get_future(); },
          first_caller);
        BOOST_CHECK(rebuilder.running());
        auto second = rebuilder.run(never_runs, second_caller);
        BOOST_CHECK_EQUAL(rebuilder.waiters(), 2U);
        BOOST_CHECK_EQUAL(shard.running(), 1U);
        work.set_value(succeeds ? outcome{} : failed(errc::corrupt_data));
        const auto started = co_await std::move(first);
        const auto joined = co_await std::move(second);
        if (succeeds)
            BOOST_CHECK(started.has_value() && joined.has_value());
        else
            BOOST_CHECK(
              is(started, errc::corrupt_data)
              && is(joined, errc::corrupt_data));
        BOOST_CHECK(!rebuilder.running());
        BOOST_CHECK_EQUAL(shard.running(), 0U);
    }
    {
        // A failure is not kept: the next request builds again.
        seastar::abort_source caller;
        bool ran = false;
        const auto again = co_await rebuilder.run(
          [&](seastar::abort_source&) {
              ran = true;
              return done();
          },
          caller);
        BOOST_CHECK(ran && again.has_value());
    }
    co_await rebuilder.stop();
}

SEASTAR_TEST_CASE(sparse_index_rebuild_outlives_the_callers_that_leave) {
    sparse_index_rebuild_limit shard;
    sparse_index_rebuilder rebuilder{shard};
    seastar::promise<outcome> work;
    seastar::abort_source first_caller, second_caller, third_caller;
    auto first = rebuilder.run(
      [&](seastar::abort_source&) { return work.get_future(); }, first_caller);
    auto second = rebuilder.run(never_runs, second_caller);
    // A caller that joined leaves alone.
    second_caller.request_abort();
    {
        const auto settled = co_await std::move(second);
        BOOST_CHECK(is(settled, errc::aborted));
    }
    BOOST_CHECK(!first.available());
    BOOST_CHECK(rebuilder.running());
    // Its place still counts until the rebuild ends.
    BOOST_CHECK_EQUAL(rebuilder.waiters(), 2U);
    // So does the caller that started it: the owner runs the work.
    first_caller.request_abort();
    {
        const auto settled = co_await std::move(first);
        BOOST_CHECK(is(settled, errc::aborted));
    }
    BOOST_CHECK(rebuilder.running());
    BOOST_CHECK_EQUAL(shard.running(), 1U);
    auto third = rebuilder.run(never_runs, third_caller);
    BOOST_CHECK_EQUAL(rebuilder.waiters(), 3U);
    work.set_value(outcome{});
    {
        const auto settled = co_await std::move(third);
        BOOST_CHECK(settled.has_value());
    }
    BOOST_CHECK(!rebuilder.running());
    BOOST_CHECK_EQUAL(shard.running(), 0U);
    co_await rebuilder.stop();
}

SEASTAR_TEST_CASE(sparse_index_rebuild_that_throws_fails_like_any_other) {
    sparse_index_rebuild_limit shard;
    sparse_index_rebuilder rebuilder{shard};
    {
        seastar::promise<> gate;
        seastar::abort_source first_caller, second_caller;
        auto first = rebuilder.run(
          [&](seastar::abort_source&) -> seastar::future<outcome> {
              co_await gate.get_future();
              throw std::runtime_error("boom");
          },
          first_caller);
        auto second = rebuilder.run(never_runs, second_caller);
        gate.set_value();
        {
            const auto settled = co_await std::move(first);
            BOOST_CHECK(is(settled, errc::io_failure));
        }
        {
            const auto settled = co_await std::move(second);
            BOOST_CHECK(is(settled, errc::io_failure));
        }
        BOOST_CHECK(!rebuilder.running());
        BOOST_CHECK_EQUAL(shard.running(), 0U);
    }
    {
        // Thrown before any future exists.
        seastar::abort_source caller;
        const auto thrown = co_await rebuilder.run(
          [](seastar::abort_source&) -> seastar::future<outcome> {
              throw std::runtime_error("boom");
          },
          caller);
        BOOST_CHECK(is(thrown, errc::io_failure));
        BOOST_CHECK_EQUAL(shard.running(), 0U);
    }
    {
        // Memory that could not be had is reported as that, not as a
        // device's failure.
        seastar::abort_source caller;
        const auto starved = co_await rebuilder.run(
          [](seastar::abort_source&) -> seastar::future<outcome> {
              throw std::bad_alloc();
          },
          caller);
        BOOST_CHECK(is(starved, errc::resource_exhausted));
        BOOST_CHECK_EQUAL(shard.running(), 0U);
    }
    co_await rebuilder.stop();
}

SEASTAR_TEST_CASE(sparse_index_rebuild_is_refused_at_its_limits) {
    sparse_index_rebuild_limit shard{2};
    sparse_index_rebuilder a{shard}, b{shard}, c{shard};
    seastar::promise<outcome> work_a, work_b;
    seastar::abort_source caller;
    auto first = a.run(
      [&](seastar::abort_source&) { return work_a.get_future(); }, caller);
    auto second = b.run(
      [&](seastar::abort_source&) { return work_b.get_future(); }, caller);
    BOOST_CHECK_EQUAL(shard.running(), 2U);
    // A third segment's rebuild is refused, never run beside the limit.
    bool ran = false;
    const auto counted = [&](seastar::abort_source&) {
        ran = true;
        return done();
    };
    BOOST_CHECK(is(co_await c.run(counted, caller), errc::queue_full));
    BOOST_CHECK(!ran && !c.running());
    BOOST_CHECK_EQUAL(shard.running(), 2U);
    // A request for a segment already rebuilding still joins.
    auto joined = a.run(never_runs, caller);
    work_a.set_value(outcome{});
    {
        const auto settled = co_await std::move(first);
        BOOST_CHECK(settled.has_value());
    }
    {
        const auto settled = co_await std::move(joined);
        BOOST_CHECK(settled.has_value());
    }
    // The place a finished rebuild held is free again.
    BOOST_CHECK((co_await c.run(counted, caller)).has_value());
    BOOST_CHECK(ran);
    {
        // One rebuild takes a bounded number of callers, and a caller that
        // left still holds its place.
        std::vector<seastar::future<outcome>> waiting;
        waiting.reserve(sparse_index_rebuilder::maximum_waiters);
        seastar::abort_source leaving;
        waiting.push_back(b.run(never_runs, leaving));
        for (auto n = b.waiters(); n < sparse_index_rebuilder::maximum_waiters;
             ++n)
            waiting.push_back(b.run(never_runs, caller));
        BOOST_CHECK_EQUAL(b.waiters(), sparse_index_rebuilder::maximum_waiters);
        BOOST_CHECK(is(co_await b.run(never_runs, caller), errc::queue_full));
        leaving.request_abort();
        {
            const auto settled = co_await std::move(waiting.front());
            BOOST_CHECK(is(settled, errc::aborted));
        }
        BOOST_CHECK(is(co_await b.run(never_runs, caller), errc::queue_full));
        work_b.set_value(outcome{});
        {
            const auto settled = co_await std::move(second);
            BOOST_CHECK(settled.has_value());
        }
        for (std::size_t n = 1; n < waiting.size(); ++n) {
            const auto settled = co_await std::move(waiting[n]);
            BOOST_CHECK(settled.has_value());
        }
        BOOST_CHECK(!b.running());
        BOOST_CHECK((co_await b.run(counted, caller)).has_value());
    }
    co_await a.stop();
    co_await b.stop();
    co_await c.stop();
}

SEASTAR_TEST_CASE(sparse_index_rebuild_is_ended_by_its_owner_alone) {
    sparse_index_rebuild_limit shard;
    sparse_index_rebuilder rebuilder{shard};
    seastar::promise<outcome> work;
    seastar::optimized_optional<seastar::abort_source::subscription> asked;
    seastar::abort_source caller;
    auto waiting = rebuilder.run(
      [&](seastar::abort_source& stop) {
          // A code no failed wait is reported with: what a waiter gets is
          // what the stopped work returned.
          asked = stop.subscribe(
            [&work]() noexcept { work.set_value(failed(errc::unavailable)); });
          return work.get_future();
      },
      caller);
    BOOST_CHECK(rebuilder.running() && !waiting.available());
    co_await rebuilder.stop();
    BOOST_CHECK(!rebuilder.running());
    BOOST_CHECK_EQUAL(shard.running(), 0U);
    {
        const auto settled = co_await std::move(waiting);
        BOOST_CHECK(is(settled, errc::unavailable));
    }
    // Nothing starts after it.
    bool ran = false;
    const auto closed = co_await rebuilder.run(
      [&](seastar::abort_source&) {
          ran = true;
          return done();
      },
      caller);
    BOOST_CHECK(!ran && is(closed, errc::closed));
}

SEASTAR_TEST_CASE(sparse_index_budget_is_checked_before_any_segment) {
    co_await with_budget([](workload_budget& budget) {
        const auto limits = budget.limits();
        const auto each = budget
                            .reservation_charge(
                              active_sparse_index::admission(
                                maximum_object_entries, budget)
                                .value())
                            .value()
                            .value();
        {
            // What the check counts for one index is what making one admits.
            const auto index = make(budget, {}, maximum_object_entries);
            BOOST_CHECK_EQUAL(budget.snapshot().bytes, each);
            BOOST_CHECK_EQUAL(budget.snapshot().tasks, 1U);
        }
        const auto refused = [&budget](
                               sparse_index_demand demand,
                               std::uint64_t limit,
                               std::uint64_t expected) {
            const auto checked = validate_sparse_index_budget(budget, demand);
            if (
              checked || checked.error().code() != errc::resource_exhausted
              || checked.error().context_size() != 2)
                return false;
            const auto has = *checked.error().context_at(0);
            const auto wants = *checked.error().context_at(1);
            return has.key == runtime::operation_context_key::limit
                   && has.value == limit
                   && wants.key == runtime::operation_context_key::expected
                   && wants.value == expected;
        };
        const auto bytes = limits.bytes.value();
        const auto most = static_cast<std::uint32_t>(bytes / each);
        BOOST_REQUIRE_GE(most, 1U);
        BOOST_REQUIRE_LT(most, limits.tasks);
        const auto full = [](std::uint32_t segments) {
            return sparse_index_demand{segments, maximum_object_entries};
        };
        // As many whole indexes as the bytes hold, and not one more.
        BOOST_CHECK(validate_sparse_index_budget(budget, full(most)));
        BOOST_CHECK(
          refused(full(most + 1), bytes, std::uint64_t{most + 1} * each));
        // Whatever else draws on the budget is counted beside them.
        auto shared = full(most);
        shared.reserved_bytes = byte_count{bytes - most * each};
        BOOST_CHECK(validate_sparse_index_budget(budget, shared));
        shared.reserved_bytes = byte_count{bytes - most * each + 1};
        BOOST_CHECK(refused(shared, bytes, bytes + 1));
        shared.reserved_bytes = byte_count{
          std::numeric_limits<std::uint64_t>::max()};
        const auto wrapped = validate_sparse_index_budget(budget, shared);
        BOOST_REQUIRE(!wrapped);
        BOOST_CHECK(wrapped.error().code() == errc::out_of_range);
        // Every index holds one admission, however small it is.
        BOOST_CHECK(validate_sparse_index_budget(budget, {limits.tasks, 1}));
        BOOST_CHECK(
          refused({limits.tasks + 1, 1}, limits.tasks, limits.tasks + 1));
        BOOST_CHECK(refused(
          {1, 1, limits.tasks}, limits.tasks, std::uint64_t{limits.tasks} + 1));
        BOOST_CHECK(
          validate_sparse_index_budget(budget, {1, 1, limits.tasks - 1}));
        // No segment asks for nothing; a segment asks for an index it can
        // have.
        BOOST_CHECK(validate_sparse_index_budget(budget, {}));
        for (const std::uint32_t capacity : {0U, maximum_object_entries + 1}) {
            const auto invalid = validate_sparse_index_budget(
              budget, {1, capacity});
            BOOST_REQUIRE(!invalid);
            BOOST_CHECK(invalid.error().code() == errc::invalid_argument);
        }
        BOOST_CHECK_EQUAL(budget.snapshot().bytes, 0U);
    });
}

SEASTAR_TEST_CASE(sparse_index_resolves_bases_past_32_bits_to_themselves) {
    co_await with_budget([](workload_budget& budget) {
        const auto offset = [](std::uint64_t value) {
            return model::range_logical_offset::make(value).value();
        };
        // One past the largest 32-bit distance from the first base.
        constexpr std::uint64_t wide = std::uint64_t{1} << 32;
        {
            auto index = make(budget, {byte_count{0}}, 8);
            index.written(block(0, 0, 100, 1, 1));
            index.written(block(100, 100, 1, 2, 1));
            index.written(block(wide, 101, 9, 3, 1));
            index.written(block(wide + 9, 110, 1, 4, 1));
            const runtime::file_position end{5};
            index.durable(end);
            BOOST_REQUIRE_EQUAL(index.size(), 4U);
            // An offset below the wide ones finds its own anchor, not the
            // first entry, and so does the first wide one.
            for (const auto [target, base, position] :
                 std::array<std::array<std::uint64_t, 3>, 7>{
                   {{0, 0, 1},
                    {100, 100, 2},
                    {wide - 1, 100, 2},
                    {wide, wide, 3},
                    {wide + 5, wide, 3},
                    {wide + 9, wide + 9, 4},
                    {std::numeric_limits<std::uint64_t>::max() - 1,
                     wide + 9,
                     4}}}) {
                const auto found = index.find(offset(target), end);
                BOOST_REQUIRE(found.has_value());
                BOOST_CHECK_EQUAL(found->anchor.logical_anchor().value(), base);
                BOOST_CHECK_EQUAL(
                  found->anchor.block_position().value(), position);
            }
            // Nothing was dropped to get there.
            check(index, 0, 0, 1);
            check(index, 3, wide + 9, 4);
        }
        {
            // The first block is an anchor however far its base lies from
            // zero, and the block after it is found beside it.
            auto index = make(budget, {byte_count{1}}, 8);
            index.written(block(wide, 0, 1, 0, 1));
            index.written(block(wide + 1, 1, 1, 1, 1));
            const runtime::file_position end{2};
            index.durable(end);
            BOOST_REQUIRE_EQUAL(index.size(), 2U);
            check(index, 0, wide, 0);
            check(index, 1, wide + 1, 1);
            BOOST_CHECK(!index.find(offset(wide - 1), end));
            const auto found = index.find(offset(wide + 1), end);
            BOOST_REQUIRE(found.has_value());
            BOOST_CHECK_EQUAL(found->anchor.block_position().value(), 1U);
        }
    });
}

SEASTAR_TEST_CASE(sparse_index_lookup_agrees_with_a_sorted_map_on_any_offset) {
    co_await with_budget([](workload_budget& budget) {
        const auto offset = [](std::uint64_t value) {
            return model::range_logical_offset::make(value).value();
        };
        constexpr std::uint64_t first_offset = 45;
        constexpr std::uint32_t entries = 30;
        for (std::uint64_t seed = 1; seed <= 16; ++seed) {
            seeded_choices choices{seed};
            const auto step = [&choices] { return choices.uniform(15) + 1; };
            auto index = make(budget, {byte_count{0}}, entries);
            // An index that holds nothing finds nothing.
            BOOST_CHECK(!index.find(offset(92), runtime::file_position{}));
            // Offsets and positions that both grow by a step chosen anew
            // for each entry.
            std::map<std::uint64_t, std::uint64_t> placed;
            for (std::uint64_t at = first_offset + 1, position = 0;
                 placed.size() < entries;) {
                at += step();
                position += step();
                placed.emplace(at, position);
            }
            std::uint64_t ordinal = 0;
            for (auto entry = placed.begin(); entry != placed.end(); ++entry) {
                const auto next = std::next(entry);
                const auto records = next == placed.end()
                                       ? 1
                                       : next->first - entry->first;
                const auto size = next == placed.end()
                                    ? 1
                                    : next->second - entry->second;
                index.written(
                  block(entry->first, ordinal, records, entry->second, size));
                ordinal += records;
            }
            const runtime::file_position end{placed.rbegin()->second + 1};
            index.durable(end);
            BOOST_REQUIRE_EQUAL(index.size(), entries);
            // Every offset that was added is found where it was put.
            bool exact = true;
            for (const auto& [at, position] : placed) {
                const auto found = index.find(offset(at), end);
                exact = exact && found
                        && found->anchor.logical_anchor().value() == at
                        && found->anchor.block_position().value() == position;
            }
            BOOST_CHECK(exact);
            // Any other offset finds the largest one at or below it, as the
            // sorted map does; one below them all finds nothing.
            std::vector<std::uint64_t> targets;
            for (auto at = first_offset; at < placed.rbegin()->first; ++at)
                targets.push_back(at);
            for (auto left = targets.size(); left > 1; --left)
                std::swap(targets[left - 1], targets[choices.uniform(left)]);
            targets.resize(entries);
            index.freeze();
            bool same = true;
            for (const auto target : targets) {
                const auto found = index.find(offset(target), end);
                same = same
                       && found
                            == find_sparse_index_anchor(
                              index.page(0), offset(target), end);
                if (target < placed.begin()->first) {
                    same = same && !found;
                    continue;
                }
                const auto after = placed.upper_bound(target);
                const auto floor = std::prev(after);
                same
                  = same && found
                    && found->anchor.logical_anchor().value() == floor->first
                    && found->anchor.block_position().value() == floor->second
                    && found->end.value()
                         == (after == placed.end() ? end.value() : after->second);
            }
            BOOST_CHECK_MESSAGE(same, "seed " << seed);
        }
    });
}

SEASTAR_TEST_CASE(sparse_index_allocation_failure_only_skips_an_anchor) {
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    co_await with_budget([](workload_budget& budget) {
        // Four blocks to a stride, and enough of them to fill one chunk.
        auto index = make(budget, {byte_count{4096}}, maximum_object_entries);
        feeder segment{index, 0, 4096};
        for (std::uint64_t i = 0; i < 4 * chunk_entries; ++i)
            segment.add(1, 1024);
        segment.sync();
        BOOST_REQUIRE_EQUAL(index.size(), chunk_entries);
        const auto chosen = segment.position;
        auto& injector = seastar::memory::local_failure_injector();
        injector.fail_after(0);
        segment.add(1, 1024);
        const bool injected = injector.failed();
        injector.cancel();
        BOOST_CHECK(injected);
        segment.sync();
        BOOST_CHECK_EQUAL(index.size(), chunk_entries);
        BOOST_CHECK_EQUAL(index.skipped(), 1U);
        // The next block is chosen at once, and the stride resumes from it.
        for (std::uint32_t i = 0; i < 5; ++i)
            segment.add(1, 1024);
        segment.sync();
        BOOST_REQUIRE_EQUAL(index.size(), chunk_entries + 2);
        check(index, chunk_entries, 4 * chunk_entries + 1, chosen + 1024);
        check(index, chunk_entries + 1, 4 * chunk_entries + 5, chosen + 5120);
        BOOST_CHECK_EQUAL(index.skipped(), 1U);
    });
#endif
    co_return;
}
