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
#include "src/storage/local_generation.h"
#include "src/storage/local_paths.h"
#include "src/storage/local_root.h"
#include "src/storage/page_cache.h"
#include "src/storage/segment_reader.h"
#include "src/storage/segment_run.h"
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
#include "src/storage/tests/wal_bench_file.h"

#include <seastar/core/condition-variable.hh>
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
#include <charconv>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <cstring>
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
using wal_bench_support::reactor_io;
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
    // Blocks of the measured segment, 4 KiB each, and of the extent with
    // holes. A segment holds at most 65,536; the count written is reported
    // with every row. The default keeps a run of every case short and still
    // fills more than one page.
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
// A count from the environment: a whole number within its bound, written
// as digits alone.
std::uint32_t chosen_count(
  const char* name,
  std::uint32_t otherwise,
  std::uint32_t least,
  std::uint32_t most) {
    const auto* text = std::getenv(name);
    if (!text) return otherwise;
    std::uint64_t value = 0;
    const auto* end = text + std::strlen(text);
    const auto parsed = std::from_chars(text, end, value);
    require(
      parsed.ec == std::errc{} && parsed.ptr == end && value >= least
        && value <= most,
      "an index benchmark count is not a whole number within its bound");
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
    // What its publication names beside the index, for whoever opens its
    // generation to read it.
    std::optional<local_publication_generation> generation;
    std::optional<local_footer_reference> boundary;
    std::optional<local_root_reference> retry;
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
        out.generation = writer->publication_generation();
        out.boundary = outcome.boundary;
        out.retry = outcome.retry;
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
    // What the shard itself issued, in both builds: every file together, a
    // read counted when it was submitted.
    reactor_io io;
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
      , tasks_(seastar::engine().get_sched_stats().tasks_processed)
      , io_(reactor_io::now()) {
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
        const auto issued = reactor_io::now().since(io_);
        output_.io.flushes += issued.flushes;
        output_.io.writes += issued.writes;
        output_.io.write_bytes += issued.write_bytes;
        output_.io.reads += issued.reads;
        output_.io.read_bytes += issued.read_bytes;
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
    reactor_io io_;
    std::uint64_t epoch_{0};
    seastar::thread_cputime_clock::time_point cpu_;
};

// One printed row. Every row carries its case, its side and the data it was
// measured over, so that no number is read against another dataset.
class row final {
public:
    // `family` names what reads the row: the index's report by default.
    row(
      const char* name,
      const char* side,
      const char* family = "index_bench_v1") {
        std::printf("%s {\"case\":\"%s\",\"side\":\"%s\"", family, name, side);
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
        field("reactor_flushes", value.io.flushes);
        field("reactor_writes", value.io.writes);
        field("reactor_write_bytes", value.io.write_bytes);
        field("reactor_reads", value.io.reads);
        field("reactor_read_bytes", value.io.read_bytes);
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

// The owner of a sealed segment's index, as a shard would make it: with a
// cache that keeps what a reader's lookups read, or with none.
std::unique_ptr<index_type> own_index(
  const environment& env,
  const sealed& segment,
  sparse_index_residency& residency,
  sparse_index_rebuild_limit& rebuilds,
  const codec::cooperative_work& work,
  page_cache* cache = nullptr) {
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
        work.policy(),
        cache));
}
// What the cache of a sealed segment's index or runs may hold: ten of the
// widest runs of the read sweep, with room to spare, or half of what funds
// it. Every page of the largest index a segment has fits many times over.
byte_count sealed_cache(const environment& env) {
    return byte_count{
      std::min<std::uint64_t>(16_MiB, env.indexing.limits().bytes.value() / 2)};
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
         .truncate = true}));
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
        take(runtime::file_path::make(env.scratch.string()))));
}
// Opens a resident index: its whole file and no more, in the largest reads
// the profile admits, checked and held in memory from then on.
seastar::future<resident_index>
open_resident(const environment& env, const runtime::file_path& path) {
    auto file = take(co_await env.files.open(path, {}));
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

// What one side's lookups of a unit came to, checked against the blocks.
struct looked_up final {
    measurement timed;
    workload_budget_snapshot held;
    lookup_summary checked;
    // Each lookup's answer, on a paged side.
    answers found;
    // What a resident index holds, and the bytes of its file.
    std::uint64_t resident_columns{0}, resident_file{0};
};
// `targets` looked up in the index of `segment` by one side. Cold, nothing
// of the index is open when the interval starts: it holds the opening and
// every lookup. Warm, the root is open before it starts, or the resident
// index is loaded, and it holds the lookups alone. A lookup in a published
// index reads the one page its root routes to and verifies it, every time:
// nothing here keeps a page.
seastar::future<looked_up> look_up(
  environment env,
  const sealed& segment,
  const block_list& blocks,
  std::span<const model::range_logical_offset> targets,
  side measured,
  bool cold,
  codec::cooperative_work& work) {
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
    looked_up out;
    resident_answers resident_found;
    out.found.reserve(targets.size());
    resident_found.reserve(targets.size());
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
                co_await owned_lookups(*index, targets, out.found, work);
                break;
            case side::baseline:
                co_await composed_lookups(*root, targets, out.found, work);
                break;
            case side::resident:
                for (const auto target : targets)
                    resident_found.push_back(resident->find_nearest(target));
                break;
            }
        };
        const auto unit = [&]() -> seastar::future<> {
            if (!cold) co_await open();
            measure_scope scope{env.files, out.timed};
            if (cold) co_await open();
            co_await look();
        };
        co_await seastar::with_scheduling_group(
          env.indexing.scheduling_group(), [&unit] { return unit(); });
        out.held = held_since(env.indexing, before);
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
    out.checked = measured == side::resident
                    ? check_resident(
                        blocks, segment.end(), targets, resident_found)
                    : check_answers(blocks, segment.end(), targets, out.found);
    if (resident) {
        out.resident_columns = resident->column_bytes().value();
        out.resident_file
          = resident_index::encoded_bytes(resident->size()).value();
    }
    co_return out;
}
void resident_fields(row& out, side measured, const looked_up& done) {
    if (measured != side::resident) return;
    out.number("resident_column_bytes", done.resident_columns)
      .number("resident_file_bytes", done.resident_file);
}

// One segment whose every block is an anchor, and `lookups` offsets looked
// up in its index, warm or cold.
seastar::future<std::size_t>
lookup_case(environment env, side measured, bool cold) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto segment = co_await write_segment(
      env, {.blocks = env.chosen.blocks, .stride = every_block}, work);
    const auto blocks = co_await scan_blocks(env, segment, work);
    const auto targets = make_targets(blocks, 0x1dea);
    const auto done = co_await look_up(
      env, segment, blocks, targets, measured, cold, work);
    {
        row out{cold ? "cold_open_lookup" : "warm_lookup", side_name(measured)};
        out.data(segment, blocks)
          .lookups_checked(done.checked, lookups)
          .measured("", done.timed)
          .budget("held_", done.held);
        resident_fields(out, measured, done);
    }
    if (measured == side::candidate && cold) {
        exported file{env.chosen, "lookup.txt"};
        file.segment(segment);
        for (std::size_t q = 0; q < targets.size(); ++q)
            file.lookup(segment.ordinal, targets[q], done.found[q]);
        file.finish();
    }
    // A cold unit is the opening and its lookups together.
    co_return cold ? 1 : lookups;
}

// An extent whose original slots have holes, as a rewrite of a segment
// leaves them: slots removed between batches, and batches that kept fewer
// records than their original span. File positions and record ordinals stay
// dense. No writer makes one, since an appended segment has no hole, so its
// index is fed from a list made here. An index bundle lives in the directory
// of the segment it indexes, so this one is published in the directory of a
// segment written for it, under a context of its own: the list's coverage,
// bound to a digest of the list. Its lookups read the index's pages alone and
// never the data, which nothing here writes.
struct hole_extent final {
    sealed extent;
    // The list's blocks: what every answer is checked against.
    block_list blocks;
    // Blocks followed by removed slots, and blocks that kept fewer records
    // than their span, in order.
    std::deque<std::size_t> gapped, sparse;
    // Slots removed between batches, slots of a sparse batch's span that it
    // kept no record of, and the records kept.
    std::uint64_t removed{0}, unkept{0}, records{0};
};
constexpr std::uint64_t hole_block_bytes = 4_KiB;
// `count` blocks of one to fifteen original slots each, after the segment's
// first offset, each fed to the index as it is made. Every odd block keeps
// fewer records than its span when it spans more than one, and two blocks in
// every four are followed by one to fifteen removed slots, the last block
// excepted: every pairing of the two.
seastar::future<hole_extent> make_holes(
  const environment& env,
  const sealed& carrier,
  std::uint32_t count,
  codec::cooperative_work& work) {
    hole_extent out{
      .extent = {.descriptor = carrier.descriptor, .stride = every_block}};
    const auto history = *carrier.history;
    auto& extent = out.extent;
    extent.ordinal = carrier.ordinal;
    extent.history = history;
    extent.blocks = count;
    extent.capacity = sparse_index_capacity(
      every_block, byte_count{1_GiB}, count);
    extent.memory = std::make_unique<active_sparse_index>(take(
      active_sparse_index::make(every_block, extent.capacity, env.writing)));
    choices drawn{0x401e};
    auto base = history.logical_origin.value();
    auto ordinal = history.physical_origin.value();
    auto position = history.data_start.value();
    for (std::uint32_t at = 0; at < count; ++at) {
        const auto span = 1 + drawn.uniform(15);
        const auto kept = at % 2 == 1 && span > 1 ? 1 + drawn.uniform(span - 1)
                                                  : span;
        if (kept < span) {
            out.sparse.push_back(at);
            out.unkept += span - kept;
        }
        require(
          extent.memory->written(
            coverage{
              model::range_logical_span::from_count(
                model::range_logical_end{base},
                model::range_logical_count{span})
                .value(),
              model::segment_relative_span::from_count(
                model::segment_relative_end{ordinal},
                model::segment_record_count{kept})
                .value(),
              model::file_byte_span::from_size(
                runtime::file_position{position}, byte_count{hole_block_bytes})
                .value()}),
          "a block of the extent with holes was not chosen");
        out.blocks.push_back(
          {base, base + span, position, position + hole_block_bytes});
        out.records += kept;
        base += span;
        ordinal += kept;
        position += hole_block_bytes;
        if (at % 4 < 2 && at + 1 < count) {
            const auto gap = 1 + drawn.uniform(15);
            out.gapped.push_back(at);
            out.removed += gap;
            base += gap;
        }
    }
    extent.memory->durable(runtime::file_position{position});
    extent.memory->freeze();
    require(
      !out.gapped.empty() && !out.sparse.empty()
        && extent.memory->size() == count && extent.memory->skipped() == 0,
      "the extent with holes has none, or a block that is not an anchor");
    const coverage covered{
      model::range_logical_span::from_count(
        history.logical_origin,
        model::range_logical_count{
          out.blocks.back().next - history.logical_origin.value()})
        .value(),
      model::segment_relative_span::from_count(
        history.physical_origin, model::segment_record_count{out.records})
        .value(),
      model::file_byte_span::from_size(
        history.data_start, byte_count{position - history.data_start.value()})
        .value()};
    // No data bears this digest out: nothing reads the extent's data.
    codec::content_digest listed{};
    const auto digest = dataset_digest(out.blocks);
    for (std::size_t at = 0; at < listed.size(); ++at)
        listed[at] = static_cast<unsigned char>(digest >> (8 * (at % 8)));
    extent.context = take(
      sparse_index_context::make(
        carrier.descriptor.segment,
        covered,
        codec::extent_digest{listed},
        carrier.descriptor.alignment));
    extent.index = take(
      co_await publish_sparse_index(
        env.files,
        env.owner,
        env.spec,
        0,
        *extent.memory,
        *extent.context,
        carrier.spare_object(0),
        env.indexing,
        store_contract::limits(),
        work));
    co_return out;
}
// Offsets to look up in an extent with holes: below its first batch, at a
// batch's base, inside a sparse batch's span past its base, inside slots
// removed after a batch, at the first base after them, past the end, at the
// far end of the offset space, and anywhere across the extent, holes and
// batches alike.
std::vector<model::range_logical_offset>
make_hole_targets(const hole_extent& holes, std::uint64_t seed) {
    choices drawn{seed};
    const auto& blocks = holes.blocks;
    const auto first = blocks.front().base, end = blocks.back().next;
    std::vector<model::range_logical_offset> out;
    out.reserve(lookups);
    for (std::uint32_t q = 0; q < lookups; ++q) {
        std::uint64_t target = 0;
        switch (q % 8) {
        case 0:
            target = first == 0 ? 0 : drawn.uniform(first);
            break;
        case 1:
            target = blocks[drawn.uniform(blocks.size())].base;
            break;
        case 2: {
            const auto& block
              = blocks[holes.sparse[drawn.uniform(holes.sparse.size())]];
            target = block.base + 1
                     + drawn.uniform(block.next - block.base - 1);
            break;
        }
        case 3: {
            const auto at = holes.gapped[drawn.uniform(holes.gapped.size())];
            target = blocks[at].next
                     + drawn.uniform(blocks[at + 1].base - blocks[at].next);
            break;
        }
        case 4:
            target
              = blocks[holes.gapped[drawn.uniform(holes.gapped.size())] + 1]
                  .base;
            break;
        case 5:
            target = end + drawn.uniform(1_MiB);
            break;
        case 6:
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
// How many offsets lie in slots removed between batches, and how many in a
// sparse batch's span past its base, by the list alone.
struct hole_hits final {
    std::uint32_t in_gap{0}, in_sparse{0};
};
hole_hits classify(
  const hole_extent& holes,
  std::span<const model::range_logical_offset> targets) {
    hole_hits out;
    const auto& blocks = holes.blocks;
    for (const auto target : targets) {
        const auto after = std::ranges::upper_bound(
          blocks, target.value(), {}, &block_span::base);
        if (after == blocks.begin()) continue;
        const auto holder = std::prev(after);
        const auto at = static_cast<std::size_t>(holder - blocks.begin());
        if (target.value() >= holder->next) {
            if (after != blocks.end()) ++out.in_gap;
        } else if (
          target.value() > holder->base
          && std::ranges::binary_search(holes.sparse, at)) {
            ++out.in_sparse;
        }
    }
    return out;
}

// The warm lookups of an extent with holes, by each side, each answer
// checked against the list: the anchor at or before the offset by original
// base, as for any other extent.
seastar::future<std::size_t> hole_case(environment env, side measured) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto carrier = co_await write_segment(
      env,
      {.blocks = 64,
       .stride = every_block,
       .maximum_blocks = 64,
       .maximum_data_bytes = byte_count{64_MiB}},
      work);
    const auto holes = co_await make_holes(
      env, carrier, env.chosen.blocks, work);
    const auto targets = make_hole_targets(holes, 0x401e);
    const auto hits = classify(holes, targets);
    const auto done = co_await look_up(
      env, holes.extent, holes.blocks, targets, measured, false, work);
    {
        row out{"hole_lookup", side_name(measured)};
        out.data(holes.extent, holes.blocks)
          .number("gaps", holes.gapped.size())
          .number("removed_slots", holes.removed)
          .number("sparse_batches", holes.sparse.size())
          .number("unkept_slots", holes.unkept)
          .number("records", holes.records)
          .number("gap_lookups", hits.in_gap)
          .number("sparse_lookups", hits.in_sparse)
          .lookups_checked(done.checked, lookups)
          .measured("", done.timed)
          .budget("held_", done.held);
        resident_fields(out, measured, done);
    }
    co_return lookups;
}

// The same lookups as a reader makes them: through locate(), with a cache
// that keeps what they read of the index's root and pages. Each lookup is a
// reader's first and stands in no page. Every one has been made once before
// the interval starts, so each is answered from what the cache holds, and
// reads nothing and waits for nothing.
seastar::future<std::size_t> cached_case(environment env) {
    seastar::abort_source abort, asking;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto segment = co_await write_segment(
      env, {.blocks = env.chosen.blocks, .stride = every_block}, work);
    const auto blocks = co_await scan_blocks(env, segment, work);
    const auto targets = make_targets(blocks, 0x1dea);
    auto cache = take(
      page_cache::make(env.indexing, {.capacity = sealed_cache(env)}));
    sparse_index_residency residency;
    sparse_index_rebuild_limit rebuilds;
    std::unique_ptr<index_type> index;
    answers found;
    found.reserve(targets.size());
    measurement timed;
    workload_budget_snapshot held;
    page_cache_snapshot then, now;
    runtime::first_failure failed;
    try {
        const auto before = env.indexing.snapshot();
        index = own_index(env, segment, residency, rebuilds, work, cache.get());
        const auto pass = [&]() -> seastar::future<> {
            found.clear();
            for (const auto target : targets)
                found.push_back(
                  take(co_await index->locate(target, asking)).position);
        };
        const auto unit = [&]() -> seastar::future<> {
            co_await pass();
            then = cache->snapshot();
            measure_scope scope{env.files, timed};
            co_await pass();
        };
        co_await seastar::with_scheduling_group(
          env.indexing.scheduling_group(), [&unit] { return unit(); });
        now = cache->snapshot();
        held = held_since(env.indexing, before);
        require(
          now.loads == then.loads && timed.index.reads == 0
            && timed.index.opens == 0,
          "a lookup whose root and page are kept read a file");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    try {
        co_await close_index(index);
    } catch (...) {
        failed.observe(std::current_exception());
    }
    try {
        co_await cache->stop();
    } catch (...) {
        failed.observe(std::current_exception());
    }
    take(failed.outcome());
    const auto checked = check_answers(blocks, segment.end(), targets, found);
    {
        row out{"cached_lookup", "candidate"};
        out.data(segment, blocks)
          .lookups_checked(checked, lookups)
          .measured("", timed)
          .budget("held_", held)
          .number("cache_capacity", now.capacity)
          .number("cache_bytes", now.bytes)
          .number("cache_loads", now.loads - then.loads);
    }
    co_return lookups;
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
PERF_TEST_F(candidate_index, hole_lookup) {
    return run_store(
      [](environment env) { return hole_case(env, side::candidate); });
}
PERF_TEST_F(baseline_index, hole_lookup) {
    return run_store(
      [](environment env) { return hole_case(env, side::baseline); });
}
PERF_TEST_F(candidate_index, cached_lookup) {
    return run_store([](environment env) { return cached_case(env); });
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
PERF_TEST_F(resident_index, hole_lookup) {
    return run_store(
      [](environment env) { return hole_case(env, side::resident); });
}
} // namespace resident_cases

// The read path over what the cases above write: a cache, a segment's runs
// and a reader's sessions. Its rows are a family of their own, with a report
// of their own.
namespace read_cases {
constexpr const char* family = "read_bench_v1";

struct read_settings final {
    // Blocks of a sealed segment that is read, 4 KiB each. The default
    // keeps a run of every case short and still holds a few runs at the
    // widest stride.
    std::uint32_t blocks{512};
    // Groups appended to the segment whose tail is read, eight blocks each.
    std::uint32_t groups{48};
    // What the cache of the tail cases may hold.
    std::uint64_t tail_cache_bytes{1_MiB};
};
read_settings chosen_reads() {
    read_settings out;
    out.blocks = chosen_count(
      "KWAQUE_READ_BENCH_BLOCKS", out.blocks, 64, maximum_object_entries);
    out.groups = chosen_count("KWAQUE_READ_BENCH_GROUPS", out.groups, 16, 4096);
    out.tail_cache_bytes = std::uint64_t{chosen_count(
                             "KWAQUE_READ_BENCH_TAIL_CACHE_KIB",
                             static_cast<std::uint32_t>(
                               out.tail_cache_bytes / 1_KiB),
                             256,
                             16 * 1024)}
                           * 1_KiB;
    return out;
}
// Seeks of one unit of work.
constexpr std::uint32_t seeks = 256;
// The strides of the sweep as a read unit, in bytes.
constexpr std::array<std::uint64_t, 6> read_strides{
  32_KiB, 64_KiB, 128_KiB, 256_KiB, 512_KiB, 1_MiB};

segment_run_limits run_limits() { return {.charge = charge}; }
// Whether this allocator profile can price a read as long as one whole
// contiguous allocation: a run that long is not readable where it cannot.
bool prices_whole_chunks() {
    return charge(byte_count{maximum_contiguous_allocation_bytes}).value()
           <= maximum_contiguous_allocation_bytes;
}

// A sealed segment opened to be read: its generation and its index over a
// cache, and its runs.
struct read_world final {
    sparse_index_residency residency;
    sparse_index_rebuild_limit rebuilds;
    // The cache, when this world made it.
    std::unique_ptr<page_cache> owned;
    page_cache* cache{nullptr};
    std::unique_ptr<local_generation_owner> generation;
    std::unique_ptr<index_type> index;
    std::optional<local_generation_pin> pin;
    std::unique_ptr<segment_runs> runs;
    runtime::file_position bound;
    model::range_logical_end last;
};
seastar::future<> close_world(read_world& world) {
    runtime::first_failure failed;
    if (world.runs) {
        try {
            failed.observe(co_await world.runs->close());
        } catch (...) {
            failed.observe(std::current_exception());
        }
        world.runs.reset();
    }
    try {
        co_await close_index(world.index);
    } catch (...) {
        failed.observe(std::current_exception());
    }
    world.pin.reset();
    if (world.generation) {
        try {
            failed.observe(co_await world.generation->close());
        } catch (...) {
            failed.observe(std::current_exception());
        }
        world.generation.reset();
    }
    if (world.owned) {
        try {
            co_await world.owned->stop();
        } catch (...) {
            failed.observe(std::current_exception());
        }
        world.owned.reset();
    }
    take(failed.outcome());
}
// `shared` is the cache to read through, or nothing for one of `capacity`
// that the world makes and stops.
seastar::future<std::unique_ptr<read_world>> open_world(
  const environment& env,
  const sealed& segment,
  page_cache* shared,
  byte_count capacity,
  codec::cooperative_work& work) {
    auto made = std::make_unique<read_world>();
    runtime::first_failure failed;
    try {
        if (shared != nullptr) {
            made->cache = shared;
        } else {
            made->owned = take(
              page_cache::make(env.indexing, {.capacity = capacity}));
            made->cache = made->owned.get();
        }
        const std::array<local_bundle_context, 2> contexts{
          *segment.context,
          footer_expectation{*segment.history, segment.boundary->position()}};
        const local_generation_expectation expected{
          *segment.generation,
          {segment.descriptor.segment,
           local_object_state::sealed,
           *segment.boundary,
           {*segment.index, *segment.retry}},
          segment.descriptor,
          contexts};
        made->generation = take(
          co_await local_generation_owner::open(
            env.files,
            env.owner,
            env.spec,
            0,
            expected,
            env.indexing,
            store_contract::limits(),
            work));
        made->pin.emplace(take(made->generation->pin()));
        made->index = take(
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
            made->residency,
            made->rebuilds,
            env.indexing,
            env.indexing,
            store_contract::limits(),
            scan_limits(),
            work.policy(),
            made->cache));
        made->runs = take(
          segment_runs::make(
            *made->cache,
            env.indexing,
            *segment.history,
            {.limits = run_limits()}));
        made->bound = made->pin->data_end().value();
        made->last = segment.context->coverage().logical().end();
    } catch (...) {
        failed.observe(std::current_exception());
    }
    if (failed.failed()) {
        try {
            co_await close_world(*made);
        } catch (...) {
        }
        take(failed.outcome());
    }
    co_return made;
}
// A session that may read all of a sealed segment.
segment_reader session(
  read_world& world,
  const environment& env,
  segment_reader_config config = {}) {
    return take(
      segment_reader::open(
        *world.runs,
        segment_data_source{take(world.pin->share())},
        {.durable = world.bound, .sealed = world.last, .visible = world.last},
        sealed_read_index(*world.index),
        env.indexing,
        config));
}

// What some reads came to.
struct reads_done final {
    std::uint64_t reads{0}, batches{0}, bytes{0};
};
// Reads from `from` to the end of a sealed segment, `limit` bytes a read.
seastar::future<reads_done> read_through(
  segment_reader& reader,
  model::range_logical_end from,
  byte_count limit,
  seastar::abort_source& asking) {
    reads_done done;
    segment_read_position at{from, std::nullopt};
    for (;;) {
        const auto got = co_await reader.read({at, limit, true}, asking);
        ++done.reads;
        done.batches += got.batches.size();
        done.bytes += got.bytes.value();
        require(
          got.stop == segment_read_stop::limit
            || got.stop == segment_read_stop::end_of_segment,
          "a read of a sealed segment stopped for no reason of its own");
        if (got.stop != segment_read_stop::limit) break;
        at = got.next;
    }
    co_return done;
}

// What a read row says of the cache between two looks at it.
void cache_fields(
  row& out, const page_cache_snapshot& then, const page_cache_snapshot& now) {
    out.number("cache_capacity", now.capacity)
      .number("cache_bytes", now.bytes)
      .number("cache_loads", now.loads - then.loads)
      .number("cache_waits", now.waits - then.waits)
      .number("cache_uncached", now.uncached - then.uncached)
      .number("cache_refused", now.refused - then.refused)
      .number("cache_evicted", now.evicted - then.evicted);
}

// Seeks to offsets blocks begin with, each answered with the one batch that
// holds it. Cold, nothing of the segment is in memory when the interval
// starts: it holds the index's root and page, and each run once. Warm, the
// same seeks were made before it starts: it reads no file.
seastar::future<std::size_t> seek_case(environment env, bool cold) {
    const auto chosen = chosen_reads();
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto segment = co_await write_segment(env, {.blocks = chosen.blocks}, work);
    const auto blocks = co_await scan_blocks(env, segment, work);
    choices drawn{0x5eed};
    std::vector<std::size_t> wanted;
    wanted.reserve(seeks);
    for (std::uint32_t q = 0; q < seeks; ++q)
        wanted.push_back(drawn.uniform(blocks.size()));
    auto world = co_await open_world(
      env, segment, nullptr, sealed_cache(env), work);
    measurement timed;
    reads_done done;
    page_cache_snapshot before, after;
    runtime::first_failure failed;
    try {
        seastar::abort_source asking;
        // Seeks go anywhere: nothing is asked for ahead of them.
        auto reader = session(*world, env, {.read_ahead = 0});
        const auto pass = [&]() -> seastar::future<> {
            for (const auto at : wanted) {
                const auto& block = blocks[at];
                const auto got = co_await reader.read(
                  model::range_logical_offset::make(block.base).value(),
                  byte_count{1},
                  asking);
                require(
                  got.batches.size() == 1
                    && got.batches[0].logical.begin().value() == block.base
                    && got.batches[0].logical.end().value() == block.next,
                  "a seek did not return the batch that holds its offset");
                ++done.reads;
                ++done.batches;
                done.bytes += got.bytes.value();
            }
        };
        const auto unit = [&]() -> seastar::future<> {
            if (!cold) co_await pass();
            done = {};
            before = world->cache->snapshot();
            measure_scope scope{env.files, timed};
            co_await pass();
        };
        co_await seastar::with_scheduling_group(
          env.indexing.scheduling_group(), [&unit] { return unit(); });
        after = world->cache->snapshot();
        require(
          cold || (timed.data.reads == 0 && timed.index.reads == 0),
          "a seek whose run and index page are kept read a file");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    try {
        co_await close_world(*world);
    } catch (...) {
        failed.observe(std::current_exception());
    }
    take(failed.outcome());
    {
        row out{"read_seek", cold ? "cold" : "warm", family};
        out.data(segment, blocks)
          .number("seeks", done.reads)
          .number("returned_bytes", done.bytes)
          .measured("", timed);
        cache_fields(out, before, after);
    }
    co_return seeks;
}

// One segment at each stride of the sweep, read from its first offset to
// its end by one session that reads ahead, and then sought into with
// nothing in memory. The stride is the read unit: what a seek reads to
// return one batch, and how many reads a scan makes. Each row says how far
// its session looked ahead, whether that grew, and what it asked for.
seastar::future<std::size_t> read_stride_case(environment env) {
    const auto chosen = chosen_reads();
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    std::uint32_t nth = 0;
    for (const auto bytes : read_strides) {
        const auto ordinal = nth++;
        // A run of this stride holds a whole contiguous allocation.
        if (
          bytes >= maximum_contiguous_allocation_bytes
          && !prices_whole_chunks()) {
            row out{"read_stride", "skipped", family};
            out.number("stride_bytes", bytes);
            continue;
        }
        auto segment = co_await write_segment(
          env,
          {.ordinal = ordinal,
           .blocks = chosen.blocks,
           .stride = sparse_index_stride{byte_count{bytes}, 0}},
          work);
        const auto blocks = co_await scan_blocks(env, segment, work);
        choices drawn{0x5eed + bytes};
        for (const bool scanned : {true, false}) {
            auto world = co_await open_world(
              env, segment, nullptr, sealed_cache(env), work);
            measurement timed;
            reads_done done;
            page_cache_snapshot before, after;
            std::uint64_t asked = 0;
            // A scan reads ahead as a session does when nothing is said:
            // the bytes it asks for ahead are its window's, at every
            // stride, past the first run. Seeks go anywhere and ask for
            // nothing. The row says which.
            const auto config = scanned
                                  ? segment_reader_config{}
                                  : segment_reader_config{.read_ahead = 0};
            runtime::first_failure failed;
            try {
                seastar::abort_source asking;
                auto reader = session(*world, env, config);
                const auto unit = [&]() -> seastar::future<> {
                    before = world->cache->snapshot();
                    measure_scope scope{env.files, timed};
                    if (scanned) {
                        done = co_await read_through(
                          reader,
                          segment.descriptor.logical_origin,
                          byte_count{1_MiB},
                          asking);
                        co_return;
                    }
                    for (std::uint32_t q = 0; q < seeks; ++q) {
                        const auto& block
                          = blocks[drawn.uniform(blocks.size())];
                        const auto got = co_await reader.read(
                          model::range_logical_offset::make(block.base).value(),
                          byte_count{1},
                          asking);
                        require(
                          got.batches.size() == 1
                            && got.batches[0].logical.begin().value()
                                 == block.base,
                          "a seek did not return the batch that holds its "
                          "offset");
                        ++done.reads;
                        ++done.batches;
                        done.bytes += got.bytes.value();
                    }
                };
                co_await seastar::with_scheduling_group(
                  env.indexing.scheduling_group(), [&unit] { return unit(); });
                after = world->cache->snapshot();
                asked = world->runs->prefetches();
                require(
                  !scanned || done.batches == segment.blocks,
                  "a scan did not return every batch of its segment");
            } catch (...) {
                failed.observe(std::current_exception());
            }
            try {
                co_await close_world(*world);
            } catch (...) {
                failed.observe(std::current_exception());
            }
            take(failed.outcome());
            row out{"read_stride", scanned ? "scan" : "seek", family};
            out.data(segment, blocks)
              .number("depth", config.read_ahead)
              .flag("grows", config.read_ahead_grows)
              .number("reads", done.reads)
              .number("batches", done.batches)
              .number("returned_bytes", done.bytes)
              .number("asked_ahead", asked)
              .measured("", timed);
            cache_fields(out, before, after);
        }
    }
    co_return read_strides.size();
}

// One segment read from its first offset to its end, with nothing of it in
// memory, by a session that looks from none to four runs ahead of itself,
// and by one that begins with one run and looks further each time it had to
// wait.
seastar::future<std::size_t> ahead_case(environment env) {
    const auto chosen = chosen_reads();
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto segment = co_await write_segment(env, {.blocks = chosen.blocks}, work);
    const auto blocks = co_await scan_blocks(env, segment, work);
    struct depth final {
        std::uint32_t runs;
        bool grows;
    };
    constexpr std::array<depth, 6> depths{
      {{0, false}, {1, false}, {2, false}, {3, false}, {4, false}, {4, true}}};
    for (const auto ahead : depths) {
        auto world = co_await open_world(
          env, segment, nullptr, sealed_cache(env), work);
        measurement timed;
        reads_done done;
        page_cache_snapshot before, after;
        std::uint64_t asked = 0;
        runtime::first_failure failed;
        try {
            seastar::abort_source asking;
            auto reader = session(
              *world,
              env,
              {.read_ahead = ahead.runs, .read_ahead_grows = ahead.grows});
            const auto unit = [&]() -> seastar::future<> {
                before = world->cache->snapshot();
                measure_scope scope{env.files, timed};
                // A consumer's fetch: a few batches at a time.
                done = co_await read_through(
                  reader,
                  segment.descriptor.logical_origin,
                  byte_count{16_KiB},
                  asking);
            };
            co_await seastar::with_scheduling_group(
              env.indexing.scheduling_group(), [&unit] { return unit(); });
            after = world->cache->snapshot();
            asked = world->runs->prefetches();
            require(
              done.batches == segment.blocks,
              "a read to the end did not return every batch");
        } catch (...) {
            failed.observe(std::current_exception());
        }
        try {
            co_await close_world(*world);
        } catch (...) {
            failed.observe(std::current_exception());
        }
        take(failed.outcome());
        row out{"read_ahead", ahead.grows ? "growing" : "fixed", family};
        out.data(segment, blocks)
          .number("depth", ahead.runs)
          .number("reads", done.reads)
          .number("batches", done.batches)
          .number("returned_bytes", done.bytes)
          .number("asked_ahead", asked)
          .measured("", timed);
        cache_fields(out, before, after);
    }
    co_return depths.size();
}

// One segment read from its first offset to its end through a cache every
// byte of which is taken and held: each run is refused and read for its
// reader alone, under its reader's budget, and each lookup reads its index
// page through the root. Beside it, the same read through a cache that has
// room: the batches a read returns keep their runs, so room is room for
// every run of one read's batches at once.
seastar::future<std::size_t> held_case(environment env, bool held) {
    const auto chosen = chosen_reads();
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto segment = co_await write_segment(env, {.blocks = chosen.blocks}, work);
    const auto blocks = co_await scan_blocks(env, segment, work);
    // Held: room for a few runs and no more, so that filling it is quick.
    auto world = co_await open_world(
      env,
      segment,
      nullptr,
      held ? byte_count{1_MiB} : sealed_cache(env),
      work);
    std::vector<std::unique_ptr<cache_slot<std::uint64_t>>> ballast;
    std::vector<cache_ref<std::uint64_t>> weights;
    measurement timed;
    reads_done done;
    page_cache_snapshot before, after;
    runtime::first_failure failed;
    try {
        seastar::abort_source asking;
        if (held) {
            for (const std::uint64_t backing : {64_KiB, 1_KiB})
                for (;;) {
                    auto slot = std::make_unique<cache_slot<std::uint64_t>>();
                    auto led = slot->lead(*world->cache, byte_count{backing});
                    if (!led) break;
                    std::move(*led).publish(0);
                    weights.push_back(slot->get());
                    ballast.push_back(std::move(slot));
                }
        }
        auto reader = session(*world, env, {.read_ahead = 0});
        const auto unit = [&]() -> seastar::future<> {
            before = world->cache->snapshot();
            measure_scope scope{env.files, timed};
            done = co_await read_through(
              reader,
              segment.descriptor.logical_origin,
              byte_count{1_MiB},
              asking);
        };
        co_await seastar::with_scheduling_group(
          env.indexing.scheduling_group(), [&unit] { return unit(); });
        after = world->cache->snapshot();
        require(
          done.batches == segment.blocks
            && (!held || after.uncached - before.uncached
                           == after.loads - before.loads),
          "a read through a cache with no room did not return every batch, "
          "or was kept");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    weights.clear();
    ballast.clear();
    try {
        co_await close_world(*world);
    } catch (...) {
        failed.observe(std::current_exception());
    }
    take(failed.outcome());
    {
        row out{"read_held", held ? "held" : "kept", family};
        out.data(segment, blocks)
          .number("reads", done.reads)
          .number("batches", done.batches)
          .number("returned_bytes", done.bytes)
          .measured("", timed);
        cache_fields(out, before, after);
    }
    co_return 1;
}

// How a segment that is written is fed to the cache: not at all, for as
// long as it was read lately, or for the readers its owner says read it.
enum class feeding : std::uint8_t { unfed, untold, told };
const char* feeding_name(feeding how) noexcept {
    return how == feeding::unfed    ? "unfed"
           : how == feeding::untold ? "untold"
                                    : "told";
}
// Blocks to a group, and how many groups behind the tail each reader is.
constexpr std::uint32_t group_blocks = 8;
constexpr std::array<std::uint32_t, 3> lags{0, 2, 8};

// What the scan beside a tail is owed and has done: the bytes of batches
// appended since it last read, whether the appends are over, and the time
// its reads took and the batches they returned.
struct scan_pace final {
    std::uint64_t owed{0};
    bool done{false};
    seastar::condition_variable more;
    std::uint64_t ns{0}, batches{0};
};
// A scan of a sealed segment, as far as the appends beside it have gone: as
// many bytes of batches as they appended, until they are over and nothing
// is owed, or the segment ends.
seastar::future<> scan_beside(
  segment_reader& reader,
  segment_read_position at,
  scan_pace& pace,
  seastar::abort_source& asking) {
    for (;;) {
        while (pace.owed == 0 && !pace.done)
            co_await pace.more.wait();
        if (pace.owed == 0) co_return;
        const auto started = measurement_time();
        const auto got = co_await reader.read(
          {at, byte_count{pace.owed}, true}, asking);
        pace.ns += measurement_time() - started;
        require(
          (got.stop == segment_read_stop::limit && !got.batches.empty())
            || got.stop == segment_read_stop::end_of_segment,
          "a scan beside a tail stopped for no reason of its own");
        pace.batches += got.batches.size();
        pace.owed -= std::min(pace.owed, got.bytes.value());
        at = got.next;
        if (got.stop == segment_read_stop::end_of_segment) co_return;
    }
}

// A segment that is appended to, a group and a barrier at a time, with a
// reader at each of three distances behind its tail that reads each group
// once it is that far behind. With `scanned`, a session that does not fill
// the cache reads a sealed segment nothing has in memory through the same
// cache, as a scan does, in a fiber of its own beside the appends and the
// readers: as many bytes of batches as were appended. It reads through a
// file system of its own, so that a reader's device reads are its own, and
// the shard's counts hold both. A read that no file was read for is a hit.
// The append's own time is reported apart from the reads'.
seastar::future<std::size_t>
tail_case(environment env, feeding how, bool scanned) {
    const auto chosen = chosen_reads();
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto blocks = chosen.groups * group_blocks;
    std::optional<sealed> cold;
    if (scanned)
        cold.emplace(
          co_await write_segment(env, {.ordinal = 1, .blocks = blocks}, work));
    auto cache = take(
      page_cache::make(
        env.indexing, {.capacity = byte_count{chosen.tail_cache_bytes}}));
    // The scan's own files, abort source and work account.
    file_system scan_files{!timing_only, 0};
    const environment scanner{
      scan_files,
      env.owner,
      env.spec,
      env.writing,
      env.indexing,
      env.scratch,
      env.chosen};
    seastar::abort_source scan_abort;
    codec::cooperative_work scan_work{codec::limits::defaults(), scan_abort};
    std::unique_ptr<read_world> beside;
    std::optional<segment_reader> scanning;
    scan_pace pace;
    std::optional<seastar::future<>> scanned_by;
    const auto descriptor = describe(0, byte_count{1_GiB});
    const sparse_index_stride stride{};
    auto config = segments::configuration();
    config.retry_object = local_object_sequence::make(1000).value();
    config.policy = work.policy();
    config.admission.working_bytes = byte_count{1_MiB};
    config.admission.maximum_blocks = blocks;
    std::unique_ptr<active_sparse_index> index;
    std::unique_ptr<segment_type> writer;
    std::unique_ptr<segment_runs> runs;
    struct lagging final {
        std::uint64_t reads{0}, hits{0}, device_reads{0}, batches{0};
    };
    std::array<lagging, lags.size()> readers{};
    measurement timed;
    std::uint64_t append_ns = 0, read_ns = 0;
    std::uint64_t dropped = 0;
    page_cache_snapshot after;
    foreground probe;
    std::optional<seastar::future<>> turning;
    runtime::first_failure failed;
    try {
        if (scanned)
            beside = co_await open_world(
              scanner, *cold, cache.get(), byte_count{}, scan_work);
        index = std::make_unique<active_sparse_index>(take(
          active_sparse_index::make(
            stride,
            sparse_index_capacity(stride, byte_count{1_GiB}, blocks),
            env.writing)));
        writer = take(
          segment_type::make_new(
            env.files,
            env.owner,
            env.spec,
            0,
            descriptor,
            env.writing,
            config));
        take(
          co_await seastar::with_scheduling_group(
            env.writing.scheduling_group(),
            [&writer, &work] { return writer->create_new(work); }));
        const auto history = take(co_await writer->pin_data(work)).history;
        segment_runs_config fed{.limits = run_limits()};
        if (how == feeding::untold)
            fed.now = []() noexcept {
                return runtime::monotonic_time{measurement_time()};
            };
        runs = take(
          segment_runs::make(*cache, env.indexing, history, std::move(fed)));
        take(writer->observe_blocks(runs->observer(*index)));
        if (how == feeding::told)
            runs->establish(static_cast<std::uint32_t>(lags.size()));
        seastar::abort_source asking;
        if (scanned) {
            scanning.emplace(session(*beside, scanner, {.fill = false}));
            scanned_by.emplace(scan_beside(
              *scanning,
              {cold->descriptor.logical_origin, std::nullopt},
              pace,
              scan_abort));
        }
        // Where each group begins and ends, in offsets.
        std::vector<std::uint64_t> begins, ends;
        begins.reserve(chosen.groups);
        ends.reserve(chosen.groups);
        const fixture::shape shape{.name = "index"};
        auto logical = descriptor.logical_origin;
        std::uint64_t sequence = 0;
        if (env.chosen.foreground) turning.emplace(foreground_loop(probe));
        {
            measure_scope scope{env.files, timed};
            for (std::uint32_t g = 0; g < chosen.groups; ++g) {
                // The append: one group, and its barrier.
                std::vector<encoded_assigned_batch> batches;
                batches.reserve(group_blocks);
                auto next = logical;
                std::uint64_t appended_bytes = 0;
                for (std::uint32_t b = 0; b < group_blocks; ++b) {
                    auto child = co_await fixture::make_batch(
                      shape, descriptor.segment, next, sequence++, work);
                    next = child.info().context.logical_span().end();
                    appended_bytes += child.bytes().size().value();
                    batches.push_back(std::move(child));
                }
                const auto appending = measurement_time();
                std::optional<runtime::result<segment_group_preparation>>
                  preparation;
                preparation.emplace(
                  writer->prepare_group(std::span{batches}, work));
                if (
                  !*preparation
                  && preparation->error().code() == errc::queue_full) {
                    take(co_await writer->digest_caught_up());
                    preparation.emplace(
                      writer->prepare_group(std::span{batches}, work));
                }
                auto prepared = take(std::move(*preparation));
                require(
                  prepared.prepared.has_value(),
                  "the segment whose tail is read filled");
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
                const auto written = co_await std::move(accepted.written);
                take(written.failure.outcome());
                const auto barrier = co_await writer->barrier(
                  accepted.boundary);
                take(barrier.failure.outcome());
                append_ns += measurement_time() - appending;
                begins.push_back(logical.value());
                ends.push_back(next.value());
                logical = next;
                if (scanned) {
                    // The scan beside may read as many bytes of batches as
                    // were just appended.
                    pace.owed += appended_bytes;
                    pace.more.signal();
                }
                // What the writer handed over is copied off its path: the
                // readers come once that is done.
                co_await runs->settled();
                // Each reader reads the group it is now behind by its lag.
                const auto reading = measurement_time();
                for (std::size_t r = 0; r < lags.size(); ++r) {
                    if (g < lags[r]) continue;
                    const auto wanted = g - lags[r];
                    auto view = take(co_await writer->pin_data(work));
                    auto reader = take(
                      segment_reader::open(
                        *runs,
                        segment_data_source{std::move(view.pin)},
                        {.durable = view.durable,
                         .sealed = std::nullopt,
                         .visible = model::range_logical_end{ends[wanted]}},
                        active_read_index(*index, view.durable),
                        env.indexing));
                    const auto then = counted(
                      env.files.sample, file_kind::data);
                    const auto got = co_await reader.read(
                      model::range_logical_offset::make(begins[wanted]).value(),
                      byte_count{1_MiB},
                      asking);
                    const auto read
                      = counted(env.files.sample, file_kind::data).reads
                        - then.reads;
                    require(
                      got.batches.size() == group_blocks
                        && got.stop != segment_read_stop::failed,
                      "a reader behind the tail was not given its group");
                    auto& mine = readers[r];
                    ++mine.reads;
                    mine.batches += got.batches.size();
                    mine.device_reads += read;
                    if (read == 0) ++mine.hits;
                }
                read_ns += measurement_time() - reading;
            }
            // The scan ends with what it is owed, inside the interval.
            if (scanned_by) {
                pace.done = true;
                pace.more.signal();
                auto joined = std::move(*scanned_by);
                scanned_by.reset();
                co_await std::move(joined);
            }
        }
        after = cache->snapshot();
        dropped = runs->dropped();
    } catch (...) {
        failed.observe(std::current_exception());
    }
    if (scanned_by) {
        pace.done = true;
        pace.more.broadcast();
        scan_abort.request_abort();
        try {
            co_await std::move(*scanned_by);
        } catch (...) {
            failed.observe(std::current_exception());
        }
        scanned_by.reset();
    }
    // A session leaves before its segment's runs are closed.
    scanning.reset();
    probe.stop = true;
    if (turning) {
        try {
            co_await std::move(*turning);
        } catch (...) {
            failed.observe(std::current_exception());
        }
    }
    if (runs) {
        try {
            failed.observe(co_await runs->close());
        } catch (...) {
            failed.observe(std::current_exception());
        }
    }
    if (writer) {
        try {
            failed.observe(co_await writer->close());
        } catch (...) {
            failed.observe(std::current_exception());
        }
    }
    // The writer's observer refers to the runs and the index until here.
    writer.reset();
    runs.reset();
    if (beside) {
        try {
            co_await close_world(*beside);
        } catch (...) {
            failed.observe(std::current_exception());
        }
    }
    try {
        co_await cache->stop();
    } catch (...) {
        failed.observe(std::current_exception());
    }
    take(failed.outcome());
    {
        row out{
          scanned ? "read_tail_scanned" : "read_tail",
          feeding_name(how),
          family};
        out.number("groups", chosen.groups)
          .number("group_blocks", group_blocks)
          .number("stride_bytes", stride.bytes.value())
          .number("append_ns", append_ns)
          .number("read_ns", read_ns)
          .number("scan_ns", pace.ns)
          .number("scanned_batches", pace.batches)
          .number("feed_dropped", dropped)
          .number("foreground_turns", probe.turns)
          .number("longest_wait_ns", probe.longest);
        for (std::size_t r = 0; r < lags.size(); ++r) {
            const auto label = [r](const char* what) {
                return std::string{"lag"} + std::to_string(lags[r]) + "_"
                       + what;
            };
            out.number(label("reads").c_str(), readers[r].reads)
              .number(label("hits").c_str(), readers[r].hits)
              .number(label("device_reads").c_str(), readers[r].device_reads);
        }
        out.measured("", timed);
        cache_fields(out, page_cache_snapshot{}, after);
    }
    co_return chosen.groups;
}

struct read_path : index_bench {};
PERF_TEST_F(read_path, cold_seek) {
    return run_store([](environment env) { return seek_case(env, true); });
}
PERF_TEST_F(read_path, warm_seek) {
    return run_store([](environment env) { return seek_case(env, false); });
}
PERF_TEST_F(read_path, stride_sweep) {
    return run_store([](environment env) { return read_stride_case(env); });
}
PERF_TEST_F(read_path, ahead_sweep) {
    return run_store([](environment env) { return ahead_case(env); });
}
PERF_TEST_F(read_path, every_entry_held) {
    return run_store([](environment env) { return held_case(env, true); });
}
PERF_TEST_F(read_path, every_entry_kept) {
    return run_store([](environment env) { return held_case(env, false); });
}
PERF_TEST_F(read_path, tail_unfed) {
    return run_store(
      [](environment env) { return tail_case(env, feeding::unfed, false); });
}
PERF_TEST_F(read_path, tail_untold) {
    return run_store(
      [](environment env) { return tail_case(env, feeding::untold, false); });
}
PERF_TEST_F(read_path, tail_told) {
    return run_store(
      [](environment env) { return tail_case(env, feeding::told, false); });
}
PERF_TEST_F(read_path, tail_untold_scanned) {
    return run_store(
      [](environment env) { return tail_case(env, feeding::untold, true); });
}
PERF_TEST_F(read_path, tail_told_scanned) {
    return run_store(
      [](environment env) { return tail_case(env, feeding::told, true); });
}
} // namespace read_cases
} // namespace kwaque::storage::testing::index_bench_support
