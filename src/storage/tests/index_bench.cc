#include "src/base/units.h"
#include "src/bytes/test_allocation_profile.h"
#include "src/codec/tests/allocation_observer.h"
#include "src/resource/resource_manager.h"
#include "src/resource/resource_registry.h"
#include "src/resource/workload_class.h"
#include "src/runtime/first_failure.h"
#include "src/runtime/production/clocks.h"
#include "src/runtime/testing/test_directory.h"
#include "src/storage/local_bundle.h"
#include "src/storage/local_paths.h"
#include "src/storage/local_root.h"
#include "src/storage/segment_scan.h"
#include "src/storage/segment_writer.h"
#include "src/storage/sparse_index.h"
#include "src/storage/sparse_index_owner.h"
#include "src/storage/tests/index_bench_resident.h"
#include "src/storage/tests/local_installation_contract.h"
#include "src/storage/tests/local_storage_bench_file.h"
#include "src/storage/tests/local_store_contract.h"
#include "src/storage/tests/segment_bench_fixture.h"
#include "src/storage/tests/segment_bench_observer.h"
#include "src/storage/tests/segment_writer_contract.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/thread_cputime_clock.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/testing/perf_tests.hh>
#include <seastar/util/later.hh>
#include <seastar/util/tmp_file.hh>

#include <algorithm>
#include <array>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace kwaque::storage::testing::index_bench_support {
namespace {
using bytes::testing::charge;
using local_storage_bench_support::file_kind;
using local_storage_bench_support::file_system;
using local_storage_bench_support::io_sample;
using local_storage_bench_support::measurement_time;
using store_contract::require;
using store_contract::take;
namespace fixture = segment_bench_support;
namespace installation = installation_contract;
namespace segments = segment_writer_contract;
using clock = runtime::production::monotonic_clock;
using owner_type = store_contract::ownership_input;
using segment_type = segment_writer<file_system, owner_type, clock>;
using index_type = sealed_sparse_index<file_system, owner_type>;
#if defined(KWAQUE_INDEX_BENCH_TIMING_ONLY)
constexpr bool timing_only = true;
#else
constexpr bool timing_only = false;
#endif

// Lookups of one unit of work.
constexpr std::uint32_t lookups = 4096;
// One anchor for every block: the densest index a segment can have.
constexpr sparse_index_stride every_block{byte_count{0}, 0};
// The strides of the sweep, in bytes.
constexpr std::array<std::uint64_t, 9> swept_strides{
  4_KiB, 8_KiB, 16_KiB, 32_KiB, 64_KiB, 128_KiB, 256_KiB, 512_KiB, 1_MiB};

struct settings final {
    // Blocks of the measured segment, 4 KiB each. A segment holds at most
    // 65,536; the count written is reported with every row. The default
    // keeps a run of every case short and still fills more than one page.
    std::uint32_t blocks{2048};
    // Sealed segments of the many-segment cases, and the blocks of each. An
    // index of 64 anchors and its root are one device block each; the cost
    // of loading every index whole shows with more.
    std::uint32_t segments{32};
    std::uint32_t segment_blocks{64};
    // Observes the allocations of an open root and of a lookup.
    bool memory{false};
    // Runs a fiber beside a rebuild that counts its own turns.
    bool foreground{false};
    // Where the anchors and lookups of a case are written, for a comparison
    // made outside this binary.
    const char* export_directory{nullptr};
};
std::uint32_t chosen_count(
  const char* name,
  std::uint32_t otherwise,
  std::uint32_t least,
  std::uint32_t most) {
    const auto* text = std::getenv(name);
    if (!text) return otherwise;
    const auto value = std::strtoull(text, nullptr, 10);
    require(
      value >= least && value <= most,
      "an index benchmark count is outside its bound");
    return static_cast<std::uint32_t>(value);
}
settings read_settings() {
    settings out;
    out.blocks = chosen_count(
      "KWAQUE_INDEX_BENCH_BLOCKS", out.blocks, 64, maximum_object_entries);
    out.segments = chosen_count(
      "KWAQUE_INDEX_BENCH_SEGMENTS", out.segments, 2, 4096);
    out.segment_blocks = chosen_count(
      "KWAQUE_INDEX_BENCH_SEGMENT_BLOCKS", out.segment_blocks, 64, 8192);
    out.memory = std::getenv("KWAQUE_INDEX_BENCH_OBSERVE_ALLOCATIONS")
                 != nullptr;
    out.foreground = std::getenv("KWAQUE_INDEX_BENCH_FOREGROUND_PROBE")
                     != nullptr;
    require(
      !timing_only || (!out.memory && !out.foreground),
      "allocation observation and the foreground probe require index_bench");
    out.export_directory = std::getenv("KWAQUE_INDEX_BENCH_EXPORT");
    return out;
}

// The largest aligned read window this allocator profile serves within one
// contiguous allocation.
byte_count admissible_window() {
    auto window = maximum_contiguous_allocation_bytes;
    while (charge(byte_count{window}).value()
           > maximum_contiguous_allocation_bytes)
        window -= 4_KiB;
    return byte_count{window};
}
// A walk of a segment's data under the defaults a shard walks with.
segment_scan_limits scan_limits() {
    segment_scan_limits limits;
    limits.metadata = store_contract::limits();
    limits.reader.window_bytes = admissible_window();
    return limits;
}

// A scheduling group's I/O class is created, and its metrics registered, on
// the group's first read or write. That one-time registration is setup.
seastar::future<>
register_io_class(seastar::scheduling_group group, std::string path) {
    co_await seastar::with_scheduling_group(group, [path = std::move(path)] {
        return seastar::open_file_dma(path, seastar::open_flags::ro)
          .then([](seastar::file file) {
              return file.dma_read_bulk<char>(0, file.disk_read_dma_alignment())
                .discard_result()
                .finally([file]() mutable { return file.close(); });
          });
    });
}

// One store directory and what funds its owners. Segments are written as
// produce-path work. Index roots, lookups and rebuilds draw on a budget of
// their own, so that what an index holds is read off that budget alone.
struct environment final {
    file_system& files;
    owner_type& owner;
    const local_device_spec& spec;
    workload_budget& writing;
    workload_budget& indexing;
    const std::filesystem::path& scratch;
    settings chosen;
};

struct index_bench {
    // Runs `body` over a fresh store and returns the units of work it did.
    template<typename Body>
    seastar::future<std::size_t> run_store(Body body) const {
        const auto chosen = read_settings();
        const auto config = resource::resource_config::from_total_memory(
                              byte_count{
                                seastar::memory::stats().total_memory()})
                              .value();
        resource::resource_registry registry;
        co_await registry.start(config);
        resource::resource_manager manager{registry.handles()};
        runtime::first_failure failed;
        std::size_t done = 0;
        try {
            co_await manager.start();
            co_await seastar::tmp_dir::do_with(
              runtime::testing::test_directory_template(),
              seastar::coroutine::lambda(
                [&](seastar::tmp_dir& directory) -> seastar::future<> {
                    done = co_await run_in(
                      chosen, manager, directory.get_path(), body);
                }));
        } catch (...) {
            failed.observe(std::current_exception());
        }
        try {
            co_await manager.stop();
        } catch (...) {
            failed.observe(std::current_exception());
        }
        try {
            co_await registry.stop();
        } catch (...) {
            failed.observe(std::current_exception());
        }
        take(failed.outcome());
        co_return done;
    }

private:
    template<typename Body>
    static seastar::future<std::size_t> run_in(
      const settings& chosen,
      resource::resource_manager& manager,
      const std::filesystem::path& directory,
      Body& body) {
        file_system files{!timing_only, 0};
        const auto root = take(
          runtime::file_path::make((directory / "store").string()));
        take(co_await files.create_directories(root));
        const auto status = co_await seastar::file_stat(
          root.value(), seastar::follow_symlink::no);
        const auto spec = store_contract::specification(
          root, {status.device_id, status.inode_number}, 68);
        const std::array specs{spec};
        owner_type owner{specs};
        // One segment is written at a time. Its retry table and its index
        // are admitted in full before its first block, and every batch of a
        // group in flight holds an admission of its own.
        auto producing = manager.acquire_workload(
          resource::workload_class::foreground_protocol);
        const auto share = std::min<std::uint64_t>(
          96_MiB, producing.hard_budget().value());
        workload_budget writing{
          std::move(producing),
          {.tasks = 8192,
           .bytes = byte_count{share},
           .handles = static_cast<std::uint32_t>(
             32 + segment_creation_handles
             + segment_writer_handles(segments::configuration()))},
          charge};
        // Every open root, every page while it is searched, every index
        // held in memory and one walk of a segment's data. Each index held
        // in memory takes one admission.
        auto deriving = manager.acquire_workload(
          resource::workload_class::metadata);
        const auto held = std::min<std::uint64_t>(
          32_MiB, deriving.hard_budget().value());
        workload_budget indexing{
          std::move(deriving),
          {.tasks = 256 + chosen.segments,
           .bytes = byte_count{held},
           .handles = 64},
          charge};
        {
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            co_await installation::bootstrap(
              files, owner, spec, writing, work, native_driver{});
        }
        co_await register_io_class(
          indexing.scheduling_group(),
          take(take(local_paths::make(spec.root)).control(0)).value());
        co_return co_await body(
          environment{
            files, owner, spec, writing, indexing, directory, chosen});
    }
    struct native_driver final {
        template<typename T>
        seastar::future<T> lifecycle(seastar::future<T> value) const {
            return value;
        }
    };
};

// A 16-byte identity whose last four bytes count segments.
template<typename Id>
Id numbered(std::uint8_t fill, std::uint32_t ordinal) {
    std::array<std::uint8_t, 16> bytes{};
    bytes.fill(fill);
    for (std::size_t at = 0; at < 4; ++at)
        bytes[12 + at] = static_cast<std::uint8_t>(ordinal >> (8 * (3 - at)));
    return Id::make(bytes).value();
}
// Segment `ordinal` of the store. Its first offset lies just below 2^32, so
// that a segment of a few thousand records has offsets on both sides of it.
local_segment_descriptor
describe(std::uint32_t ordinal, byte_count maximum_data_bytes) {
    auto value = installation::descriptor();
    value.segment = segment_context::make(
                      value.segment.cluster(),
                      value.segment.topic(),
                      numbered<model::range_id>(0x20, ordinal),
                      numbered<model::segment_id>(0x30, ordinal),
                      value.segment.generation())
                      .value();
    value.logical_origin = model::range_logical_end{(1ULL << 32U) - 1024};
    value.maximum_data_bytes = maximum_data_bytes;
    return value;
}

// What a segment is asked to be.
struct segment_plan final {
    std::uint32_t ordinal{0};
    std::uint32_t blocks{0};
    sparse_index_stride stride{};
    // The most blocks and data bytes the segment may hold: what its index
    // is sized by before its first block.
    std::uint32_t maximum_blocks{maximum_object_entries};
    byte_count maximum_data_bytes{1_GiB};
    // Whether the seal names the index. A segment sealed without owes one.
    bool named{true};
    // Keeps the segment's owner open, for a later publication.
    bool keep_writer{false};
};
// A sealed segment written by its owner, and what its publication sealed.
struct sealed final {
    local_segment_descriptor descriptor;
    sparse_index_stride stride{};
    std::optional<segment_history_context> history;
    std::optional<sparse_index_context> context;
    std::optional<local_root_reference> index;
    // The index its writer fed, frozen by the seal.
    std::unique_ptr<active_sparse_index> memory;
    std::unique_ptr<segment_type> writer;
    std::uint32_t blocks{0};
    // What the index was sized and admitted for when the segment was made.
    std::uint32_t capacity{0};
    byte_count admitted{};
    // The segment refused a group before `blocks` were written.
    bool filled{false};
    [[nodiscard]] std::uint64_t end() const {
        return context->coverage().bytes().end().value();
    }
    [[nodiscard]] local_object_sequence spare_object(std::uint32_t nth) const {
        return local_object_sequence::make(
                 1'000'000 + 16 * std::uint64_t{ordinal} + nth)
          .value();
    }
    std::uint32_t ordinal{0};
};

// Writes a segment of 4-KiB blocks, 64 to a group and eight groups to a
// barrier, feeds an index from its owner and seals it. Every block is a
// batch of its own producer sequence, and nothing is copied between them.
seastar::future<sealed> write_segment(
  const environment& env, segment_plan plan, codec::cooperative_work& work) {
    sealed out{
      .descriptor = describe(plan.ordinal, plan.maximum_data_bytes),
      .stride = plan.stride};
    out.ordinal = plan.ordinal;
    auto config = segments::configuration();
    config.retry_object = local_object_sequence::make(
                            1000 + 2 * std::uint64_t{plan.ordinal})
                            .value();
    config.policy = work.policy();
    config.admission.working_bytes = byte_count{1_MiB};
    config.admission.maximum_blocks = plan.maximum_blocks;
    out.capacity = sparse_index_capacity(
      plan.stride, plan.maximum_data_bytes, plan.maximum_blocks);
    // As a shard does before it starts: the budget must hold the index of
    // every segment it writes at once.
    take(validate_sparse_index_budget(
      env.writing, {.segments = 1, .capacity = out.capacity}));
    out.admitted = take(
      active_sparse_index::admission(out.capacity, env.writing));
    out.memory = std::make_unique<active_sparse_index>(
      take(active_sparse_index::make(plan.stride, out.capacity, env.writing)));
    auto writer = take(
      segment_type::make_new(
        env.files,
        env.owner,
        env.spec,
        0,
        out.descriptor,
        env.writing,
        config));
    constexpr std::uint32_t window = 8;
    std::array<std::optional<segment_submission>, window> pending;
    runtime::first_failure failed;
    try {
        take(
          co_await seastar::with_scheduling_group(
            env.writing.scheduling_group(),
            [&writer, &work] { return writer->create_new(work); }));
        take(writer->observe_blocks(out.memory->observer()));
        const fixture::shape shape{.name = "index"};
        auto logical = out.descriptor.logical_origin;
        auto sequence = std::uint64_t{plan.ordinal} << 32U;
        while (out.blocks < plan.blocks && !out.filled) {
            std::optional<segment_captured_boundary> cut;
            for (std::uint32_t g = 0; g < window && out.blocks < plan.blocks;
                 ++g) {
                const auto count = std::min(
                  maximum_segment_group_blocks, plan.blocks - out.blocks);
                std::vector<encoded_assigned_batch> batches;
                batches.reserve(count);
                auto next = logical;
                for (std::uint32_t b = 0; b < count; ++b) {
                    auto child = co_await fixture::make_batch(
                      shape, out.descriptor.segment, next, sequence++, work);
                    next = child.info().context.logical_span().end();
                    batches.push_back(std::move(child));
                }
                std::optional<runtime::result<segment_group_preparation>>
                  preparation;
                preparation.emplace(
                  writer->prepare_group(std::span{batches}, work));
                // Ordinary pressure: a written group keeps its memory until
                // the deferred digest has hashed it.
                if (
                  !*preparation
                  && preparation->error().code() == errc::queue_full) {
                    take(co_await writer->digest_caught_up());
                    preparation.emplace(
                      writer->prepare_group(std::span{batches}, work));
                }
                auto prepared = take(std::move(*preparation));
                if (!prepared.prepared) {
                    // The segment is full: it rolls here, for its own
                    // reasons, and its index never had to.
                    out.filled = true;
                    break;
                }
                std::vector<admitted_wal_batch> children;
                children.reserve(batches.size());
                for (auto& batch : batches) {
                    auto backing = take(
                      env.writing.try_reserve_buffer(batch.bytes()));
                    children.push_back(take(
                      admitted_wal_batch::make(
                        std::move(batch), std::move(backing), charge)));
                }
                auto group = take(
                  co_await writer->freeze_group(
                    std::move(*prepared.prepared), std::move(children), work));
                take(co_await writer->encode_group(group, work));
                auto accepted = take(writer->submit(std::move(group), work));
                cut = accepted.boundary;
                pending[g].emplace(std::move(accepted));
                logical = next;
                out.blocks += count;
            }
            if (!cut) break;
            const auto barrier = co_await writer->barrier(*cut);
            take(barrier.failure.outcome());
            for (auto& submission : pending) {
                if (!submission) continue;
                auto waiting = std::move(submission->written);
                submission.reset();
                const auto done = co_await std::move(waiting);
                take(done.failure.outcome());
            }
        }
        require(out.blocks != 0, "the benchmark segment holds no block");
        std::uint32_t calls = 0;
        const auto outcome = co_await writer->seal(
          segments::completed_source{{}, {}, &calls},
          0,
          out.blocks,
          work,
          sparse_index_seal{
            out.memory.get(),
            plan.named ? local_object_sequence::make(
                           1001 + 2 * std::uint64_t{plan.ordinal})
                           .value()
                       : local_object_sequence{}});
        take(outcome.failure.outcome());
        require(
          outcome.boundary && outcome.retry && outcome.extent
            && out.memory->frozen() && out.memory->skipped() == 0
            && outcome.index.has_value() == plan.named,
          "the benchmark segment was not sealed as asked");
        out.history = writer->seal_progress().extent->context();
        out.context = take(
          sparse_index_context::make(
            out.descriptor.segment,
            outcome.extent->coverage,
            outcome.extent->digest,
            out.descriptor.alignment));
        out.index = outcome.index;
    } catch (...) {
        failed.observe(std::current_exception());
    }
    // A failed earlier receipt must not discard another entered write.
    for (auto& submission : pending) {
        if (!submission) continue;
        auto waiting = std::move(submission->written);
        submission.reset();
        try {
            failed.observe((co_await std::move(waiting)).failure.outcome());
        } catch (...) {
            failed.observe(std::current_exception());
        }
    }
    if (failed.failed() || !plan.keep_writer) {
        try {
            failed.observe(co_await writer->close());
        } catch (...) {
            failed.observe(std::current_exception());
        }
        writer.reset();
    }
    take(failed.outcome());
    out.writer = std::move(writer);
    co_return out;
}
seastar::future<> close_writer(sealed& segment) {
    if (!segment.writer) co_return;
    const auto closed = co_await segment.writer->close();
    segment.writer.reset();
    take(closed);
}

// One block of a segment as a walk of its data found it: the offsets it
// holds and where it lies. Nothing here comes from an index.
struct block_span final {
    std::uint64_t base, next, begin, end;
};
using block_list = std::deque<block_span>;
seastar::future<block_list> scan_blocks(
  const environment& env,
  const sealed& segment,
  codec::cooperative_work& work) {
    block_list out;
    const auto covered = segment.context->coverage();
    const auto extent = take(
      co_await verify_local_segment_extent(
        env.files,
        env.owner,
        env.spec,
        0,
        *segment.history,
        covered.bytes().end(),
        env.indexing,
        scan_limits(),
        work,
        [&out](const segment_scanned_object& object) {
            if (object.block) {
                const auto block = object.block->descriptor().coverage();
                out.push_back(
                  {block.logical().begin().value(),
                   block.logical().end().value(),
                   block.bytes().begin().value(),
                   block.bytes().end().value()});
            }
            return seastar::make_ready_future<runtime::result<bool>>(true);
        }));
    require(
      extent.boundary().coverage == covered && extent.digest()
        && *extent.digest() == segment.context->digest()
        && out.size() == segment.blocks,
      "a walk of the benchmark segment did not find what was sealed");
    co_return out;
}

// A seeded stream of choices: the same lookups in every run and on every
// side.
class choices final {
public:
    explicit choices(std::uint64_t seed) noexcept
      : state_(seed) {}
    [[nodiscard]] std::uint64_t next() noexcept {
        auto value = (state_ += 0x9e3779b97f4a7c15ULL);
        value = (value ^ (value >> 30U)) * 0xbf58476d1ce4e5b9ULL;
        value = (value ^ (value >> 27U)) * 0x94d049bb133111ebULL;
        return value ^ (value >> 31U);
    }
    [[nodiscard]] std::uint64_t uniform(std::uint64_t bound) noexcept {
        return next() % bound;
    }

private:
    std::uint64_t state_;
};
// Offsets to look up: below the first block, a block's first and last
// offset, anywhere inside the segment, past its end and at the far end of
// the offset space.
std::vector<model::range_logical_offset>
make_targets(const block_list& blocks, std::uint64_t seed) {
    choices drawn{seed};
    const auto first = blocks.front().base, end = blocks.back().next;
    std::vector<model::range_logical_offset> out;
    out.reserve(lookups);
    for (std::uint32_t q = 0; q < lookups; ++q) {
        const auto& block = blocks[drawn.uniform(blocks.size())];
        std::uint64_t target = 0;
        switch (q % 8) {
        case 0:
            target = first == 0 ? 0 : drawn.uniform(first);
            break;
        case 1:
        case 2:
            target = block.base;
            break;
        case 3:
            target = block.next - 1;
            break;
        case 4:
            target = end + drawn.uniform(1_MiB);
            break;
        case 5:
            target = UINT64_MAX - 1 - drawn.uniform(16);
            break;
        default:
            target = first + drawn.uniform(end - first);
            break;
        }
        out.push_back(model::range_logical_offset::make(target).value());
    }
    return out;
}

void mix(std::uint64_t& digest, std::uint64_t value) noexcept {
    for (unsigned shift = 0; shift < 64; shift += 8) {
        digest ^= (value >> shift) & 0xffU;
        digest *= 0x100000001b3ULL;
    }
}
constexpr std::uint64_t digest_seed = 0xcbf29ce484222325ULL;
std::uint64_t dataset_digest(const block_list& blocks) noexcept {
    auto digest = digest_seed;
    for (const auto& block : blocks) {
        mix(digest, block.base);
        mix(digest, block.next);
        mix(digest, block.begin);
        mix(digest, block.end);
    }
    return digest;
}

// What a unit's lookups came to, checked against the walk.
struct lookup_summary final {
    // Over every offset, whether an anchor was found and which. Equal on
    // every side that looked the same offsets up in the same data.
    std::uint64_t digest{digest_seed};
    // The same over the offsets no further than 32 bits past the segment's
    // first, which is as far as a 32-bit distance can tell offsets apart.
    std::uint64_t near_digest{digest_seed};
    std::uint32_t found{0};
    // Offsets further than that, and how many of them were answered with
    // the segment's first block.
    std::uint32_t far{0}, far_at_first_block{0};
    // From the anchor to the end of the block that holds the offset: what a
    // reader passes over. And from the anchor to where the lookup said the
    // scan need not pass.
    std::uint64_t scan_bytes{0}, longest_scan{0};
    std::uint64_t bound_bytes{0}, longest_bound{0};
};
// Checks one answer against the blocks the walk found. An anchor must name a
// block that the walk found where it points, at or before the block that
// holds the offset, and the scan's end must not fall short of that block.
void check_lookup(
  const block_list& blocks,
  std::uint64_t extent_end,
  model::range_logical_offset target,
  std::optional<sparse_index_entry> anchor,
  std::optional<runtime::file_position> end,
  lookup_summary& out) {
    const bool near = target.value() < blocks.front().base
                      || target.value() - blocks.front().base
                           <= std::numeric_limits<std::uint32_t>::max();
    const auto record = [&](std::uint64_t value) {
        mix(out.digest, value);
        if (near) mix(out.near_digest, value);
    };
    record(target.value());
    const auto after = std::ranges::upper_bound(
      blocks, target.value(), {}, &block_span::base);
    if (after == blocks.begin()) {
        require(!anchor, "a lookup found an anchor below the first block");
        record(0);
        return;
    }
    require(anchor.has_value(), "a lookup found no anchor at or below it");
    const auto holder = std::prev(after);
    const auto base = anchor->logical_anchor().value();
    const auto position = anchor->block_position().value();
    const auto named = std::ranges::lower_bound(
      blocks, base, {}, &block_span::base);
    require(
      named != blocks.end() && named->base == base && named->begin == position
        && named <= holder,
      "an anchor names no block at or before the one that holds the offset");
    const auto passed = holder->end - position;
    out.scan_bytes += passed;
    out.longest_scan = std::max(out.longest_scan, passed);
    if (end) {
        require(
          end->value() >= holder->end && end->value() <= extent_end,
          "a scan's end falls short of the block that holds the offset");
        const auto bound = end->value() - position;
        out.bound_bytes += bound;
        out.longest_bound = std::max(out.longest_bound, bound);
    }
    record(1);
    record(base);
    record(position);
    ++out.found;
    if (!near) {
        ++out.far;
        if (named == blocks.begin()) ++out.far_at_first_block;
    }
}
using answers = std::vector<std::optional<sparse_index_position>>;
lookup_summary check_answers(
  const block_list& blocks,
  std::uint64_t extent_end,
  std::span<const model::range_logical_offset> targets,
  const answers& found) {
    require(found.size() == targets.size(), "a lookup went unanswered");
    lookup_summary out;
    for (std::size_t q = 0; q < targets.size(); ++q)
        check_lookup(
          blocks,
          extent_end,
          targets[q],
          found[q] ? std::optional{found[q]->anchor} : std::nullopt,
          found[q] ? std::optional{found[q]->end} : std::nullopt,
          out);
    return out;
}

// Native work of one file kind in an interval.
struct io_count final {
    std::uint64_t reads{0}, read_bytes{0}, opens{0};
    std::uint64_t writes{0}, write_bytes{0}, flushes{0};
};
// Everything observed of one interval. In the timing build only the times
// and the reactor's own counters are.
struct measurement final {
    std::uint64_t elapsed{0}, cpu{0}, allocations{0}, tasks{0};
    // Index bundles, with every other metadata file, and segment data.
    io_count index, data;
    std::uint64_t stats{0}, directory_syncs{0};
    fixture::work_sample work;
};
io_count counted(const io_sample& sample, file_kind kind) {
    const auto& value = sample[kind];
    return {
      value.reads,
      value.read_bytes,
      value.opens,
      value.calls,
      value.bytes,
      value.flushes};
}
io_count operator-(const io_count& now, const io_count& then) {
    return {
      now.reads - then.reads,
      now.read_bytes - then.read_bytes,
      now.opens - then.opens,
      now.writes - then.writes,
      now.write_bytes - then.write_bytes,
      now.flushes - then.flushes};
}
// One measured interval. Native work, hashing and the benchmark's own time
// count only inside it. `timed` is false for an interval that is observed
// beside the one the case reports.
class measure_scope final {
public:
    measure_scope(file_system& files, measurement& output, bool timed = true)
      : files_(files)
      , output_(output)
      , timed_(timed)
      , index_(counted(files.sample, file_kind::other))
      , data_(counted(files.sample, file_kind::data))
      , stats_(files.sample.stats)
      , directory_syncs_(files.sample.directory_syncs)
      , allocations_(seastar::memory::stats().mallocs())
      , tasks_(seastar::engine().get_sched_stats().tasks_processed) {
        files_.sample.measuring = true;
#if !defined(KWAQUE_INDEX_BENCH_TIMING_ONLY)
        fixture::begin_work_observation(output_.work);
#endif
        cpu_ = seastar::thread_cputime_clock::now();
        epoch_ = measurement_time();
        if (timed_) perf_tests::start_measuring_time();
    }
    ~measure_scope() {
        if (timed_) perf_tests::stop_measuring_time();
        output_.elapsed += measurement_time() - epoch_;
        output_.cpu += static_cast<std::uint64_t>(
          (seastar::thread_cputime_clock::now() - cpu_).count());
        files_.sample.measuring = false;
#if !defined(KWAQUE_INDEX_BENCH_TIMING_ONLY)
        fixture::end_work_observation();
#endif
        output_.allocations += seastar::memory::stats().mallocs()
                               - allocations_;
        output_.tasks += seastar::engine().get_sched_stats().tasks_processed
                         - tasks_;
        const auto index = counted(files_.sample, file_kind::other) - index_;
        const auto data = counted(files_.sample, file_kind::data) - data_;
        output_.index = add(output_.index, index);
        output_.data = add(output_.data, data);
        output_.stats += files_.sample.stats - stats_;
        output_.directory_syncs += files_.sample.directory_syncs
                                   - directory_syncs_;
    }
    measure_scope(const measure_scope&) = delete;
    measure_scope& operator=(const measure_scope&) = delete;

private:
    static io_count add(const io_count& a, const io_count& b) {
        return {
          a.reads + b.reads,
          a.read_bytes + b.read_bytes,
          a.opens + b.opens,
          a.writes + b.writes,
          a.write_bytes + b.write_bytes,
          a.flushes + b.flushes};
    }
    file_system& files_;
    measurement& output_;
    bool timed_;
    io_count index_, data_;
    std::uint64_t stats_, directory_syncs_, allocations_, tasks_;
    std::uint64_t epoch_{0};
    seastar::thread_cputime_clock::time_point cpu_;
};

// One printed row. Every row carries its case, its side and the data it was
// measured over, so that no number is read against another dataset.
class row final {
public:
    row(const char* name, const char* side) {
        std::printf(
          "index_bench_v1 {\"case\":\"%s\",\"side\":\"%s\"", name, side);
        flag("timing_only", timing_only);
#if defined(NDEBUG)
        flag("ndebug", true);
#else
        flag("ndebug", false);
#endif
    }
    ~row() { std::puts("}"); }
    row(const row&) = delete;
    row& operator=(const row&) = delete;
    row& number(const char* key, std::uint64_t value) {
        std::printf(",\"%s\":%" PRIu64, key, value);
        return *this;
    }
    row& flag(const char* key, bool value) {
        std::printf(",\"%s\":%s", key, value ? "true" : "false");
        return *this;
    }
    row& data(const sealed& segment, const block_list& blocks) {
        number("blocks", segment.blocks);
        flag("segment_filled", segment.filled);
        number("data_bytes", segment.end());
        number("stride_bytes", segment.stride.bytes.value());
        number("stride_records", segment.stride.records);
        number("anchors", segment.memory ? segment.memory->size() : 0);
        number("dataset_digest", dataset_digest(blocks));
        return *this;
    }
    row& lookups_checked(const lookup_summary& checked, std::uint32_t asked) {
        number("lookups", asked);
        number("found", checked.found);
        number("result_digest", checked.digest);
        number("near_result_digest", checked.near_digest);
        number("far_lookups", checked.far);
        number("far_at_first_block", checked.far_at_first_block);
        number("scan_bytes", checked.scan_bytes);
        number("longest_scan_bytes", checked.longest_scan);
        number("scan_bound_bytes", checked.bound_bytes);
        number("longest_scan_bound_bytes", checked.longest_bound);
        return *this;
    }
    row& measured(const char* prefix, const measurement& value) {
        const auto field = [&](const char* name, std::uint64_t counted) {
            std::printf(",\"%s%s\":%" PRIu64, prefix, name, counted);
        };
        field("elapsed_ns", value.elapsed);
        field("cpu_ns", value.cpu);
        field("allocations", value.allocations);
        field("tasks", value.tasks);
        field("index_reads", value.index.reads);
        field("index_read_bytes", value.index.read_bytes);
        field("index_opens", value.index.opens);
        field("index_writes", value.index.writes);
        field("index_write_bytes", value.index.write_bytes);
        field("index_flushes", value.index.flushes);
        field("data_reads", value.data.reads);
        field("data_read_bytes", value.data.read_bytes);
        field("data_opens", value.data.opens);
        field("path_stats", value.stats);
        field("directory_syncs", value.directory_syncs);
        field("hashed_bytes", value.work.digest_bytes);
        field("crc_bulk_bytes", value.work.crc_bulk_bytes);
        return *this;
    }
    row& budget(const char* prefix, const workload_budget_snapshot& held) {
        std::printf(
          ",\"%sbudget_bytes\":%" PRIu64 ",\"%sbudget_handles\":%" PRIu64
          ",\"%sbudget_tasks\":%" PRIu64,
          prefix,
          held.bytes,
          prefix,
          held.handles,
          prefix,
          held.tasks);
        return *this;
    }
};
workload_budget_snapshot held_since(
  const workload_budget& budget, const workload_budget_snapshot& then) {
    const auto now = budget.snapshot();
    return {
      now.tasks - then.tasks,
      now.bytes - then.bytes,
      now.handles - then.handles,
      now.accepted - then.accepted,
      now.rejected - then.rejected};
}

// The owner of a sealed segment's index, as a shard would make it.
std::unique_ptr<index_type> own_index(
  const environment& env,
  const sealed& segment,
  sparse_index_residency& residency,
  sparse_index_rebuild_limit& rebuilds,
  const codec::cooperative_work& work) {
    return take(
      index_type::make(
        env.files,
        env.owner,
        env.spec,
        0,
        *segment.history,
        *segment.context,
        segment.stride,
        segment.index,
        std::nullopt,
        residency,
        rebuilds,
        env.indexing,
        env.indexing,
        store_contract::limits(),
        scan_limits(),
        work.policy()));
}
seastar::future<> close_index(std::unique_ptr<index_type>& index) {
    if (!index) co_return;
    const auto closed = co_await index->close();
    index.reset();
    take(closed);
}
seastar::future<> close_root(std::unique_ptr<local_root_owner>& root) {
    if (!root) co_return;
    const auto closed = co_await root->close();
    root.reset();
    take(closed);
}
seastar::future<std::unique_ptr<local_root_owner>> open_root(
  const environment& env,
  const sparse_index_context& context,
  local_root_reference reference,
  codec::cooperative_work& work) {
    co_return take(
      co_await local_root_owner::open(
        env.files,
        env.owner,
        env.spec,
        0,
        reference,
        local_bundle_context{context},
        env.indexing,
        store_contract::limits(),
        work));
}

// Lookups through the owner of the segment's index.
seastar::future<> owned_lookups(
  index_type& index,
  std::span<const model::range_logical_offset> targets,
  answers& out,
  codec::cooperative_work& work) {
    for (const auto target : targets)
        out.push_back(take(co_await index.find(target, work)));
}
// The same lookups with no owner between the caller and the root: a pin for
// each, the page the root routes to, read and verified, and its search.
seastar::future<> composed_lookups(
  local_root_owner& root,
  std::span<const model::range_logical_offset> targets,
  answers& out,
  codec::cooperative_work& work) {
    for (const auto target : targets) {
        const auto pinned = take(root.pin());
        out.push_back(
          take(co_await find_published_anchor(pinned, target, work)));
    }
}

// The resident index of a segment, holding the anchors its writer fed.
resident_index resident_of(const sealed& segment, workload_budget& budget) {
    const auto& fed = *segment.memory;
    auto out = take(
      resident_index::make(fed[0].logical_anchor(), fed.size(), budget));
    for (std::uint32_t at = 0; at < fed.size(); ++at)
        require(
          out.add(fed[at].logical_anchor(), fed[at].block_position()),
          "an anchor lies more than 32 bits past its segment's first");
    out.cover(
      model::range_logical_offset::make(
        segment.context->coverage().logical().end().value() - 1)
        .value());
    return out;
}
runtime::file_path
resident_path(const environment& env, std::uint32_t ordinal) {
    return take(
      runtime::file_path::make(
        (env.scratch / ("resident." + std::to_string(ordinal))).string()));
}
seastar::future<> write_resident(
  const environment& env,
  const runtime::file_path& path,
  const resident_index& index) {
    auto file = take(
      co_await env.files.open(
        path,
        {.access = runtime::file_access::read_write,
         .create = true,
         .truncate = true,
         .close_policy = runtime::file_close_policy::checked}));
    runtime::first_failure failed;
    try {
        auto image = take(index.encode());
        const auto size = image.size();
        const auto written = take(
          co_await file.write(runtime::file_position{}, std::move(image)));
        require(written == size, "the resident index was written short");
        failed.observe(co_await file.flush());
    } catch (...) {
        failed.observe(std::current_exception());
    }
    try {
        failed.observe(co_await file.close());
    } catch (...) {
        failed.observe(std::current_exception());
    }
    take(failed.outcome());
    take(
      co_await env.files.sync_directory(
        take(runtime::file_path::make(env.scratch.string())),
        runtime::file_close_policy::checked));
}
// Opens a resident index: its whole file and no more, in the largest reads
// the profile admits, checked and held in memory from then on.
seastar::future<resident_index>
open_resident(const environment& env, const runtime::file_path& path) {
    auto file = take(
      co_await env.files.open(
        path, {.close_policy = runtime::file_close_policy::checked}));
    runtime::first_failure failed;
    std::optional<resident_index> out;
    try {
        resident_index_decoder decoder{env.indexing};
        const auto window = admissible_window().value();
        const auto size = take(co_await file.size());
        for (std::uint64_t at = 0; at < size;) {
            const auto read = take(
              co_await file.read(
                runtime::file_position{at},
                byte_count{std::min(window, size - at)}));
            const auto before = at;
            for (const auto fragment : read.data()) {
                take(decoder.feed(fragment.data(), fragment.size()));
                at += fragment.size();
            }
            require(at != before, "a resident index ends before its size");
        }
        out.emplace(take(decoder.finish()));
    } catch (...) {
        failed.observe(std::current_exception());
    }
    try {
        failed.observe(co_await file.close());
    } catch (...) {
        failed.observe(std::current_exception());
    }
    take(failed.outcome());
    co_return std::move(*out);
}
using resident_answers = std::vector<std::optional<resident_index::entry>>;
lookup_summary check_resident(
  const block_list& blocks,
  std::uint64_t extent_end,
  std::span<const model::range_logical_offset> targets,
  const resident_answers& found) {
    require(found.size() == targets.size(), "a lookup went unanswered");
    lookup_summary out;
    for (std::size_t q = 0; q < targets.size(); ++q)
        check_lookup(
          blocks,
          extent_end,
          targets[q],
          found[q] ? std::optional{sparse_index_entry{
                       found[q]->offset, found[q]->position}}
                   : std::nullopt,
          std::nullopt,
          out);
    return out;
}

// Writes the anchors and the lookups of a case as text, one segment after
// another: `a segment base position` for an anchor, and `q segment offset
// found base position` for a lookup and its checked answer.
class exported final {
public:
    exported(const settings& chosen, const char* name) {
        if (!chosen.export_directory) return;
        out_.open(
          std::filesystem::path{chosen.export_directory} / name,
          std::ios::trunc);
        require(out_.is_open(), "the export file could not be made");
        out_ << "index_dataset_v1\n";
    }
    [[nodiscard]] bool wanted() const noexcept { return out_.is_open(); }
    void segment(const sealed& value) {
        if (!wanted()) return;
        const auto& fed = *value.memory;
        out_ << "s " << value.ordinal << ' ' << fed.size() << ' ' << value.end()
             << '\n';
        for (std::uint32_t at = 0; at < fed.size(); ++at)
            out_ << "a " << value.ordinal << ' '
                 << fed[at].logical_anchor().value() << ' '
                 << fed[at].block_position().value() << '\n';
    }
    void lookup(
      std::uint32_t ordinal,
      model::range_logical_offset target,
      const std::optional<sparse_index_position>& found) {
        if (!wanted()) return;
        out_ << "q " << ordinal << ' ' << target.value() << ' '
             << (found ? 1 : 0) << ' '
             << (found ? found->anchor.logical_anchor().value() : 0) << ' '
             << (found ? found->anchor.block_position().value() : 0) << '\n';
    }
    void finish() {
        if (!wanted()) return;
        out_ << "end\n";
        out_.close();
        require(!out_.fail(), "the export file could not be written");
    }

private:
    std::ofstream out_;
};

// Which side of a comparison a case measures.
enum class side : std::uint8_t {
    // The owner of a sealed segment's index.
    candidate,
    // The same root and pages with no owner between.
    baseline,
    // The whole index in memory.
    resident,
};
const char* side_name(side value) noexcept {
    switch (value) {
    case side::candidate:
        return "candidate";
    case side::baseline:
        return "baseline";
    case side::resident:
        return "resident";
    }
    std::abort();
}

// One segment whose every block is an anchor, and `lookups` offsets looked
// up in its index. Cold, nothing of the index is open when the interval
// starts: it holds the opening and every lookup. Warm, the root is open
// before it starts, or the resident index is loaded, and it holds the
// lookups alone. A lookup in a published index reads the one page its root
// routes to and verifies it, every time: nothing here keeps a page.
seastar::future<std::size_t>
lookup_case(environment env, side measured, bool cold) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto segment = co_await write_segment(
      env, {.blocks = env.chosen.blocks, .stride = every_block}, work);
    const auto blocks = co_await scan_blocks(env, segment, work);
    const auto targets = make_targets(blocks, 0x1dea);
    const auto path = resident_path(env, segment.ordinal);
    if (measured == side::resident) {
        const auto built = resident_of(segment, env.indexing);
        co_await write_resident(env, path, built);
    }
    sparse_index_residency residency;
    sparse_index_rebuild_limit rebuilds;
    std::unique_ptr<index_type> index;
    std::unique_ptr<local_root_owner> root;
    std::optional<resident_index> resident;
    answers found;
    resident_answers resident_found;
    found.reserve(targets.size());
    resident_found.reserve(targets.size());
    measurement timed;
    workload_budget_snapshot held;
    runtime::first_failure failed;
    try {
        const auto before = env.indexing.snapshot();
        const auto open = [&]() -> seastar::future<> {
            switch (measured) {
            case side::candidate:
                index = own_index(env, segment, residency, rebuilds, work);
                // The first pin opens the root.
                static_cast<void>(take(co_await index->pin(work)));
                break;
            case side::baseline:
                root = co_await open_root(
                  env, *segment.context, *segment.index, work);
                break;
            case side::resident:
                resident.emplace(co_await open_resident(env, path));
                break;
            }
        };
        const auto look = [&]() -> seastar::future<> {
            switch (measured) {
            case side::candidate:
                co_await owned_lookups(*index, targets, found, work);
                break;
            case side::baseline:
                co_await composed_lookups(*root, targets, found, work);
                break;
            case side::resident:
                for (const auto target : targets)
                    resident_found.push_back(resident->find_nearest(target));
                break;
            }
        };
        const auto unit = [&]() -> seastar::future<> {
            if (!cold) co_await open();
            measure_scope scope{env.files, timed};
            if (cold) co_await open();
            co_await look();
        };
        co_await seastar::with_scheduling_group(
          env.indexing.scheduling_group(), [&unit] { return unit(); });
        held = held_since(env.indexing, before);
    } catch (...) {
        failed.observe(std::current_exception());
    }
    try {
        co_await close_index(index);
    } catch (...) {
        failed.observe(std::current_exception());
    }
    try {
        co_await close_root(root);
    } catch (...) {
        failed.observe(std::current_exception());
    }
    take(failed.outcome());
    const auto checked
      = measured == side::resident
          ? check_resident(blocks, segment.end(), targets, resident_found)
          : check_answers(blocks, segment.end(), targets, found);
    {
        row out{cold ? "cold_open_lookup" : "warm_lookup", side_name(measured)};
        out.data(segment, blocks)
          .lookups_checked(checked, lookups)
          .measured("", timed)
          .budget("held_", held);
        if (resident)
            out
              .number("resident_column_bytes", resident->column_bytes().value())
              .number(
                "resident_file_bytes",
                resident_index::encoded_bytes(resident->size()).value());
    }
    if (measured == side::candidate && cold) {
        exported file{env.chosen, "lookup.txt"};
        file.segment(segment);
        for (std::size_t q = 0; q < targets.size(); ++q)
            file.lookup(segment.ordinal, targets[q], found[q]);
        file.finish();
    }
    // A cold unit is the opening and its lookups together.
    co_return cold ? 1 : lookups;
}

// Many sealed segments, each with a small index, found again at once: every
// index is asked for in turn. The owner opens one root for each and keeps no
// more of them open than the shard's bound. The resident index reads every
// index whole and keeps them all.
seastar::future<std::size_t>
many_segments_case(environment env, side measured) {
    const auto small_blocks = env.chosen.segment_blocks;
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto count = env.chosen.segments;
    std::deque<sealed> stored;
    std::uint64_t data_bytes = 0, anchors = 0;
    exported file{
      measured == side::candidate ? env.chosen : settings{}, "segments.txt"};
    for (std::uint32_t s = 0; s < count; ++s) {
        stored.push_back(
          co_await write_segment(
            env,
            {.ordinal = s,
             .blocks = small_blocks,
             .stride = every_block,
             .maximum_blocks = small_blocks,
             .maximum_data_bytes = byte_count{64_MiB}},
            work));
        auto& segment = stored.back();
        require(segment.blocks == small_blocks, "a small segment did not fit");
        data_bytes += segment.end();
        anchors += segment.memory->size();
        if (measured == side::resident) {
            const auto built = resident_of(segment, env.indexing);
            co_await write_resident(env, resident_path(env, s), built);
        }
        file.segment(segment);
        // Its writer's index is not needed again: the store is as a
        // restart finds it.
        segment.memory.reset();
    }
    sparse_index_residency residency;
    sparse_index_rebuild_limit rebuilds;
    std::deque<std::unique_ptr<index_type>> owners;
    std::deque<resident_index> residents;
    measurement timed;
    workload_budget_snapshot held, most;
    runtime::first_failure failed;
    std::uint64_t loads = 0, left_open = 0;
    try {
        if (measured == side::candidate)
            for (const auto& segment : stored)
                owners.push_back(
                  own_index(env, segment, residency, rebuilds, work));
        const auto before = env.indexing.snapshot();
        const auto unit = [&]() -> seastar::future<> {
            measure_scope scope{env.files, timed};
            for (std::uint32_t s = 0; s < count; ++s) {
                if (measured == side::candidate)
                    static_cast<void>(take(co_await owners[s]->pin(work)));
                else
                    residents.push_back(
                      co_await open_resident(env, resident_path(env, s)));
                const auto now = held_since(env.indexing, before);
                most.bytes = std::max(most.bytes, now.bytes);
                most.handles = std::max(most.handles, now.handles);
                most.tasks = std::max(most.tasks, now.tasks);
            }
        };
        co_await seastar::with_scheduling_group(
          env.indexing.scheduling_group(), [&unit] { return unit(); });
        held = held_since(env.indexing, before);
        left_open = residency.resident();
        for (const auto& index : owners)
            loads += index->loads();
        require(
          measured != side::candidate || loads == count,
          "a segment's index root was not opened exactly once");
        // Every index answers afterwards, whether its root stayed open.
        for (std::uint32_t s = 0; s < count; ++s) {
            const auto first = model::range_logical_offset::make(
                                 stored[s].descriptor.logical_origin.value())
                                 .value();
            std::optional<sparse_index_position> found;
            if (measured == side::candidate)
                found = take(co_await owners[s]->find(first, work));
            else if (const auto entry = residents[s].find_nearest(first))
                found = sparse_index_position{
                  {entry->offset, entry->position},
                  runtime::file_position{stored[s].end()}};
            require(
              found && found->anchor.logical_anchor() == first,
              "a segment's first offset did not find its first block");
            file.lookup(s, first, found);
        }
        if (measured == side::candidate)
            require(
              residency.resident() == std::min(count, residency.most())
                && most.handles <= residency.most(),
              "more index roots stayed open than the shard's bound");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    for (auto& index : owners) {
        try {
            co_await close_index(index);
        } catch (...) {
            failed.observe(std::current_exception());
        }
    }
    take(failed.outcome());
    file.finish();
    {
        row out{"open_many_segments", side_name(measured)};
        out.number("segments", count)
          .number("blocks_per_segment", small_blocks)
          .number("data_bytes", data_bytes)
          .number("anchors", anchors)
          .number("open_root_bound", residency.most())
          .number("roots_left_open", left_open)
          .number("root_loads", loads)
          .measured("", timed)
          .budget("held_", held)
          .budget("most_", most);
    }
    co_return count;
}

struct foreground final {
    bool stop{false};
    std::uint64_t turns{0}, longest{0};
};
// Counts its own turns and the longest wait between two of them: how long
// anything else kept the reactor.
seastar::future<> foreground_loop(foreground& probe) {
    auto last = measurement_time();
    while (!probe.stop) {
        co_await seastar::yield();
        const auto now = measurement_time();
        if (!probe.stop) {
            ++probe.turns;
            probe.longest = std::max(probe.longest, now - last);
        }
        last = now;
    }
}

// An index a reader reported is built again by its owner from the
// segment's data, and named in the publication. The interval the case
// reports is the walk. Making the result durable is observed beside it:
// for the owner, the bundle and the publication that names it; for the
// resident index, built from the same walk, its one file written whole.
seastar::future<std::size_t> rebuild_case(environment env, side measured) {
    seastar::abort_source abort, caller;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto segment = co_await write_segment(
      env,
      {.blocks = env.chosen.blocks, .keep_writer = measured == side::candidate},
      work);
    sparse_index_residency residency;
    sparse_index_rebuild_limit rebuilds;
    std::unique_ptr<index_type> index;
    measurement walked, persisted;
    foreground probe;
    std::optional<seastar::future<>> turning;
    lookup_summary checked;
    workload_budget_snapshot in_memory;
    block_list blocks;
    runtime::first_failure failed;
    try {
        blocks = co_await scan_blocks(env, segment, work);
        const auto targets = make_targets(blocks, 0x1dea);
        const auto before = env.indexing.snapshot();
        answers found;
        found.reserve(targets.size());
        if (measured == side::candidate) {
            index = own_index(env, segment, residency, rebuilds, work);
            // A reader followed an anchor to a block it does not name.
            index->owe(storage::detail::path_error(errc::corrupt_data));
            require(
              index->source() == sparse_index_source::owed,
              "a reported index went on answering");
        }
        std::optional<active_sparse_index> rebuilt;
        if (env.chosen.foreground) turning.emplace(foreground_loop(probe));
        const auto walk = [&]() -> seastar::future<> {
            measure_scope scope{env.files, walked};
            if (measured == side::candidate)
                take(co_await index->rebuild(caller));
            else
                rebuilt.emplace(take(
                  co_await rebuild_sparse_index(
                    env.files,
                    env.owner,
                    env.spec,
                    0,
                    *segment.history,
                    *segment.context,
                    segment.stride,
                    env.indexing,
                    scan_limits(),
                    work)));
        };
        co_await seastar::with_scheduling_group(
          env.indexing.scheduling_group(), [&walk] { return walk(); });
        probe.stop = true;
        if (turning) {
            auto turned = std::move(*turning);
            turning.reset();
            co_await std::move(turned);
        }
        in_memory = held_since(env.indexing, before);
        if (measured == side::candidate) {
            require(
              index->source() == sparse_index_source::memory,
              "a rebuilt index does not answer from memory");
            co_await owned_lookups(*index, targets, found, work);
            measure_scope scope{env.files, persisted, false};
            static_cast<void>(take(
              co_await index->publish(
                *segment.writer, segment.spare_object(0), work)));
        } else {
            const auto end = segment.context->coverage().bytes().end();
            for (const auto target : targets)
                found.push_back(rebuilt->find(target, end));
            measure_scope scope{env.files, persisted, false};
            auto columns = take(
              resident_index::make(
                (*rebuilt)[0].logical_anchor(), rebuilt->size(), env.indexing));
            for (std::uint32_t at = 0; at < rebuilt->size(); ++at)
                require(
                  columns.add(
                    (*rebuilt)[at].logical_anchor(),
                    (*rebuilt)[at].block_position()),
                  "an anchor lies more than 32 bits past its segment's first");
            co_await write_resident(
              env, resident_path(env, segment.ordinal), columns);
        }
        checked = check_answers(blocks, segment.end(), targets, found);
        require(
          measured != side::candidate
            || index->source() == sparse_index_source::published,
          "a rebuilt index was not named");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    probe.stop = true;
    if (turning) {
        try {
            co_await std::move(*turning);
        } catch (...) {
            failed.observe(std::current_exception());
        }
    }
    try {
        co_await close_index(index);
    } catch (...) {
        failed.observe(std::current_exception());
    }
    try {
        co_await close_writer(segment);
    } catch (...) {
        failed.observe(std::current_exception());
    }
    take(failed.outcome());
    {
        row out{"rebuild", side_name(measured)};
        out.data(segment, blocks)
          .lookups_checked(checked, lookups)
          .measured("", walked)
          .measured("persist_", persisted)
          .budget("memory_", in_memory)
          .flag("foreground_probe", env.chosen.foreground)
          .number("foreground_turns", probe.turns)
          .number("foreground_longest_wait_ns", probe.longest);
    }
    co_return 1;
}

// One segment's data indexed again at each stride of the sweep: what the
// index then holds in memory and on the device, what opening it and one
// cold lookup read, and how far a reader scans from the anchor it is given.
// Each stride is one walk of the data, which is what the case reports.
seastar::future<std::size_t> stride_case(environment env) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto segment = co_await write_segment(
      env, {.blocks = env.chosen.blocks}, work);
    const auto blocks = co_await scan_blocks(env, segment, work);
    const auto targets = make_targets(blocks, 0x1dea);
    const auto end = segment.context->coverage().bytes().end();
    std::uint32_t nth = 0;
    for (const auto bytes : swept_strides) {
        const sparse_index_stride stride{byte_count{bytes}, 0};
        measurement walked, opened, looked;
        std::optional<active_sparse_index> rebuilt;
        const auto walk = [&]() -> seastar::future<> {
            measure_scope scope{env.files, walked};
            rebuilt.emplace(take(
              co_await rebuild_sparse_index(
                env.files,
                env.owner,
                env.spec,
                0,
                *segment.history,
                *segment.context,
                stride,
                env.indexing,
                scan_limits(),
                work)));
        };
        co_await seastar::with_scheduling_group(
          env.indexing.scheduling_group(), [&walk] { return walk(); });
        require(rebuilt->skipped() == 0, "a rebuilt index passed a block over");
        // What a 1-GiB segment's index is sized and admitted for at this
        // stride, whatever this one holds.
        const auto capacity = sparse_index_capacity(
          stride, byte_count{1_GiB}, maximum_object_entries);
        const auto admitted = take(
          active_sparse_index::admission(capacity, env.indexing));
        // Every lookup from memory, checked against the walk.
        answers found;
        found.reserve(targets.size());
        for (const auto target : targets)
            found.push_back(rebuilt->find(target, end));
        const auto checked = check_answers(
          blocks, segment.end(), targets, found);
        // The same index as a bundle nothing names, opened by its root.
        const auto reference = take(
          co_await publish_sparse_index(
            env.files,
            env.owner,
            env.spec,
            0,
            *rebuilt,
            *segment.context,
            segment.spare_object(1 + nth),
            env.indexing,
            store_contract::limits(),
            work));
        const auto before = env.indexing.snapshot();
        std::unique_ptr<local_root_owner> root;
        codec::testing::allocation_observation open_memory, lookup_memory;
        workload_budget_snapshot held;
        std::uint64_t bundle_bytes = reference.bytes().value(), pages = 0;
        runtime::first_failure failed;
        try {
            {
#if !defined(KWAQUE_INDEX_BENCH_TIMING_ONLY)
                if (env.chosen.memory)
                    codec::testing::begin_allocation_observation();
#endif
                {
                    measure_scope scope{env.files, opened, false};
                    root = co_await open_root(
                      env, *segment.context, reference, work);
                }
#if !defined(KWAQUE_INDEX_BENCH_TIMING_ONLY)
                if (env.chosen.memory)
                    open_memory = codec::testing::end_allocation_observation();
#endif
            }
            held = held_since(env.indexing, before);
            {
                const auto pinned = take(root->pin());
                const auto& described = std::get<sparse_index_root>(
                  pinned.metadata());
                pages = described.pages().size();
                for (const auto& page : described.pages())
                    bundle_bytes += page.encoded_bytes().value();
            }
            // One lookup with nothing but the root in memory: the first of
            // the offsets that is a block's own. Then a share of them all,
            // each a page read.
            answers first, published;
            {
#if !defined(KWAQUE_INDEX_BENCH_TIMING_ONLY)
                if (env.chosen.memory)
                    codec::testing::begin_allocation_observation();
#endif
                {
                    measure_scope scope{env.files, looked, false};
                    co_await composed_lookups(
                      *root, std::span{targets}.subspan(1, 1), first, work);
                }
#if !defined(KWAQUE_INDEX_BENCH_TIMING_ONLY)
                if (env.chosen.memory)
                    lookup_memory
                      = codec::testing::end_allocation_observation();
#endif
            }
            constexpr std::size_t share = 256;
            published.reserve(share);
            co_await composed_lookups(
              *root, std::span{targets}.first(share), published, work);
            require(
              first.front() && published[1]
                && first.front()->anchor == published[1]->anchor,
              "a cold lookup did not find its block");
            for (std::size_t q = 0; q < share; ++q)
                require(
                  published[q].has_value() == found[q].has_value()
                    && (!found[q] || published[q]->anchor == found[q]->anchor),
                  "a published index and the one in memory disagree");
        } catch (...) {
            failed.observe(std::current_exception());
        }
        try {
            co_await close_root(root);
        } catch (...) {
            failed.observe(std::current_exception());
        }
        take(failed.outcome());
        {
            row out{"stride", "candidate"};
            out.number("blocks", segment.blocks)
              .flag("segment_filled", segment.filled)
              .number("data_bytes", segment.end())
              .number("stride_bytes", bytes)
              .number("stride_records", 0)
              .number("anchors", rebuilt->size())
              .number("dataset_digest", dataset_digest(blocks))
              .number("pages", pages)
              .number("bundle_bytes", bundle_bytes)
              .number("root_bytes", reference.bytes().value())
              .number("capacity_at_1gib", capacity)
              .number("admitted_at_1gib_bytes", admitted.value())
              .lookups_checked(checked, lookups)
              .measured("", walked)
              .measured("open_", opened)
              .measured("cold_lookup_", looked)
              .budget("root_", held)
              .flag("memory_observed", env.chosen.memory);
            if (env.chosen.memory)
                out.number("open_live_bytes", open_memory.live_upper_bound)
                  .number("open_peak_bytes", open_memory.peak_upper_bound)
                  .flag("open_memory_complete", open_memory.complete)
                  .number("lookup_peak_bytes", lookup_memory.peak_upper_bound)
                  .number(
                    "lookup_largest_allocation",
                    lookup_memory.largest_allocation)
                  .flag("lookup_memory_complete", lookup_memory.complete);
        }
        ++nth;
    }
    co_return swept_strides.size();
}

// A segment sealed without an index, answered from the index its writer
// fed: every lookup is a search of the table in memory.
seastar::future<std::size_t> memory_case(environment env) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto segment = co_await write_segment(
      env,
      {.blocks = env.chosen.blocks, .stride = every_block, .named = false},
      work);
    const auto blocks = co_await scan_blocks(env, segment, work);
    const auto targets = make_targets(blocks, 0x1dea);
    sparse_index_residency residency;
    sparse_index_rebuild_limit rebuilds;
    auto index = own_index(env, segment, residency, rebuilds, work);
    answers found;
    found.reserve(targets.size());
    measurement timed;
    const auto fed = segment.memory->size();
    runtime::first_failure failed;
    try {
        require(
          index->source() == sparse_index_source::owed,
          "a segment sealed without an index owes none");
        take(index->adopt(std::move(*segment.memory)));
        measure_scope scope{env.files, timed};
        co_await owned_lookups(*index, targets, found, work);
    } catch (...) {
        failed.observe(std::current_exception());
    }
    try {
        co_await close_index(index);
    } catch (...) {
        failed.observe(std::current_exception());
    }
    take(failed.outcome());
    const auto checked = check_answers(blocks, segment.end(), targets, found);
    {
        row out{"memory_lookup", "candidate"};
        out.number("blocks", segment.blocks)
          .flag("segment_filled", segment.filled)
          .number("data_bytes", segment.end())
          .number("stride_bytes", segment.stride.bytes.value())
          .number("stride_records", segment.stride.records)
          .number("anchors", fed)
          .number("dataset_digest", dataset_digest(blocks))
          .number("admitted_bytes", segment.admitted.value())
          .lookups_checked(checked, lookups)
          .measured("", timed);
    }
    co_return lookups;
}

struct candidate_index : index_bench {};
struct baseline_index : index_bench {};
struct qualification : index_bench {};

PERF_TEST_F(candidate_index, warm_lookup) {
    return run_store(
      [](environment env) { return lookup_case(env, side::candidate, false); });
}
PERF_TEST_F(baseline_index, warm_lookup) {
    return run_store(
      [](environment env) { return lookup_case(env, side::baseline, false); });
}
PERF_TEST_F(candidate_index, cold_open_lookup) {
    return run_store(
      [](environment env) { return lookup_case(env, side::candidate, true); });
}
PERF_TEST_F(baseline_index, cold_open_lookup) {
    return run_store(
      [](environment env) { return lookup_case(env, side::baseline, true); });
}
PERF_TEST_F(candidate_index, open_many_segments) {
    return run_store(
      [](environment env) { return many_segments_case(env, side::candidate); });
}
PERF_TEST_F(candidate_index, memory_lookup) {
    return run_store([](environment env) { return memory_case(env); });
}
PERF_TEST_F(qualification, stride_sweep) {
    return run_store([](environment env) { return stride_case(env); });
}
PERF_TEST_F(candidate_index, rebuild) {
    return run_store(
      [](environment env) { return rebuild_case(env, side::candidate); });
}

// The lookup over one page's anchors with no branch on a comparison: each
// step keeps the half that can hold the answer by a conditional move, and
// may ask for the two places the next step can read.
template<bool Prefetch, typename Value, typename Key>
[[nodiscard]] std::size_t last_at_or_below(
  std::span<const Value> values,
  model::range_logical_offset target,
  Key key) noexcept {
    if (values.empty()) return 0;
    const auto* first = values.data();
    std::size_t offset = 0, length = values.size();
    while (length > 1) {
        const auto half = length / 2;
        if constexpr (Prefetch) {
            const auto* quarter = first + offset + half / 2;
            __builtin_prefetch(quarter, 0, 0);
            __builtin_prefetch(quarter + half, 0, 0);
        }
        const auto mid = offset + half;
        if (__builtin_unpredictable(key(first[mid]) <= target)) offset = mid;
        length -= half;
    }
    // One past the last value when none is at or below the target.
    return target < key(first[offset]) ? values.size() : offset;
}
template<bool Prefetch>
[[nodiscard, gnu::noinline]] std::optional<sparse_index_position>
find_anchor_branch_free(
  std::span<const sparse_index_entry> anchors,
  model::range_logical_offset target,
  runtime::file_position end) noexcept {
    const auto at = last_at_or_below<Prefetch>(
      anchors, target, [](const sparse_index_entry& entry) noexcept {
          return entry.logical_anchor();
      });
    if (at == anchors.size()) return std::nullopt;
    return sparse_index_position{
      anchors[at],
      at + 1 == anchors.size() ? end : anchors[at + 1].block_position()};
}
template<bool Prefetch>
[[nodiscard, gnu::noinline]] std::optional<std::uint32_t> find_page_branch_free(
  std::span<const model::range_logical_offset> first_anchors,
  model::range_logical_offset target) noexcept {
    const auto at = last_at_or_below<Prefetch>(
      first_anchors, target, [](model::range_logical_offset value) noexcept {
          return value;
      });
    if (at == first_anchors.size()) return std::nullopt;
    return static_cast<std::uint32_t>(at);
}

// One full page of anchors, the first anchors of a full root's pages, and
// offsets to look up among them in no order. No device is involved. The
// offsets are drawn again before every measured pass, from one seeded stream
// that every side replays alike: a list searched over and over is one a
// branch predictor learns, which would hide what a search without branches
// saves.
struct search_bench {
    search_bench() {
        // Bases a few records apart that pass 2^32, blocks 4 KiB apart.
        auto base = (1ULL << 32U) - 2 * sparse_index_page_entries;
        page.reserve(sparse_index_page_entries);
        for (std::uint32_t at = 0; at < sparse_index_page_entries; ++at) {
            base += 1 + drawn.uniform(7);
            page.emplace_back(
              model::range_logical_offset::make(base).value(),
              runtime::file_position{4_KiB * (at + 1)});
        }
        end = runtime::file_position{4_KiB * (sparse_index_page_entries + 2)};
        constexpr std::uint32_t pages = maximum_object_entries
                                        / sparse_index_page_entries;
        separators.reserve(pages);
        for (std::uint32_t at = 0; at < pages; ++at)
            separators.push_back(
              model::range_logical_offset::make(
                page.front().logical_anchor().value()
                + std::uint64_t{at} * 4096)
                .value());
        low = page.front().logical_anchor().value() - 64;
        span = page.back().logical_anchor().value() + 64 - low;
        wide = separators.back().value() + 4096 - low;
        targets.resize(lookups, page.front().logical_anchor());
        routed.resize(lookups, page.front().logical_anchor());
        redraw();
        for (const auto target : targets)
            require(
              find_anchor_branch_free<false>(page, target, end)
                  == find_sparse_index_anchor(page, target, end)
                && find_anchor_branch_free<true>(page, target, end)
                     == find_sparse_index_anchor(page, target, end),
              "the two searches of a page disagree");
        for (const auto target : routed)
            require(
              find_page_branch_free<false>(separators, target)
                  == find_sparse_index_page(separators, target)
                && find_page_branch_free<true>(separators, target)
                     == find_sparse_index_page(separators, target),
              "the two searches of a root disagree");
    }
    void redraw() noexcept {
        for (std::uint32_t q = 0; q < lookups; ++q) {
            targets[q] = model::range_logical_offset::make(
                           low + drawn.uniform(span))
                           .value();
            routed[q] = model::range_logical_offset::make(
                          low + drawn.uniform(wide))
                          .value();
        }
    }
    template<typename Find>
    std::size_t in_page(Find find) {
        std::uint64_t sum = 0;
        redraw();
        perf_tests::start_measuring_time();
        for (const auto target : targets) {
            const auto found = find(page, target, end);
            sum += found ? found->end.value() : 1;
        }
        perf_tests::do_not_optimize(sum);
        perf_tests::stop_measuring_time();
        return targets.size();
    }
    template<typename Find>
    std::size_t in_root(Find find) {
        std::uint64_t sum = 0;
        redraw();
        perf_tests::start_measuring_time();
        for (const auto target : routed) {
            const auto found = find(separators, target);
            sum += found ? *found : 1;
        }
        perf_tests::do_not_optimize(sum);
        perf_tests::stop_measuring_time();
        return routed.size();
    }
    choices drawn{0x5eed};
    std::uint64_t low{0}, span{0}, wide{0};
    std::vector<sparse_index_entry> page;
    std::vector<model::range_logical_offset> separators, targets, routed;
    runtime::file_position end;
};
struct standard_search : search_bench {};
struct branch_free_search : search_bench {};
struct branch_free_prefetch_search : search_bench {};

PERF_TEST_F(standard_search, page) { return in_page(find_sparse_index_anchor); }
PERF_TEST_F(branch_free_search, page) {
    return in_page(find_anchor_branch_free<false>);
}
PERF_TEST_F(branch_free_prefetch_search, page) {
    return in_page(find_anchor_branch_free<true>);
}
PERF_TEST_F(standard_search, root) { return in_root(find_sparse_index_page); }
PERF_TEST_F(branch_free_search, root) {
    return in_root(find_page_branch_free<false>);
}
PERF_TEST_F(branch_free_prefetch_search, root) {
    return in_root(find_page_branch_free<true>);
}
} // namespace

// The whole index of a segment kept in memory, measured beside the paged
// one: its cases carry the same names in a group of their own.
namespace resident_cases {
struct resident_index : index_bench {};
PERF_TEST_F(resident_index, warm_lookup) {
    return run_store(
      [](environment env) { return lookup_case(env, side::resident, false); });
}
PERF_TEST_F(resident_index, cold_open_lookup) {
    return run_store(
      [](environment env) { return lookup_case(env, side::resident, true); });
}
PERF_TEST_F(resident_index, open_many_segments) {
    return run_store(
      [](environment env) { return many_segments_case(env, side::resident); });
}
PERF_TEST_F(resident_index, rebuild) {
    return run_store(
      [](environment env) { return rebuild_case(env, side::resident); });
}
} // namespace resident_cases
} // namespace kwaque::storage::testing::index_bench_support
