#include "src/base/units.h"
#include "src/bytes/test_allocation_profile.h"
#include "src/codec/cooperative.h"
#include "src/codec/tests/memory_qualification_support.h"
#include "src/resource/resource_manager.h"
#include "src/runtime/file.h"
#include "src/runtime/first_failure.h"
#include "src/runtime/production/timer.h"
#include "src/runtime/testing/test_directory.h"
#include "src/storage/local_append.h"
#include "src/storage/local_engine.h"
#include "src/storage/local_paths.h"
#include "src/storage/retention.h"
#include "src/storage/segment_lifecycle.h"
#include "src/storage/segment_writer.h"
#include "src/storage/sparse_index.h"
#include "src/storage/tests/engine_bench_fixture.h"
#include "src/storage/tests/local_append_contract.h"
#include "src/storage/tests/local_installation_contract.h"
#include "src/storage/tests/local_storage_bench_file.h"
#include "src/storage/tests/local_store_contract.h"
#include "src/storage/tests/segment_bench_fixture.h"
#include "src/storage/tests/segment_bench_observer.h"
#include "src/storage/tests/segment_lifecycle_contract.h"
#include "src/storage/tests/segment_writer_contract.h"
#include "src/storage/tests/wal_bench_file.h"
#include "src/storage/workload_budget.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/posix.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/thread_cputime_clock.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/testing/perf_tests.hh>
#include <seastar/util/tmp_file.hh>

#include <linux/fiemap.h>
#include <linux/fs.h>
#include <sys/stat.h>

#include <algorithm>
#include <cerrno>
#include <cinttypes>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <exception>
#include <memory>
#include <optional>
#include <set>
#include <span>
#include <system_error>
#include <utility>
#include <vector>

// One segment's lifecycle operations on the engine and on a segment's
// writer, each timed alone over native files: a seal with and without the
// index it fed, the gap a roll leaves between two segments' appends, one
// removal against the extents its data file holds, and one creation. Every
// case builds what it needs before its timed region and prints one row.
namespace kwaque::storage::testing::segment_lifecycle_bench_support {
namespace {
using bytes::testing::charge;
using local_storage_bench_support::file_kind;
using local_storage_bench_support::kind_sample;
using local_storage_bench_support::measurement_time;
using store_contract::require;
using store_contract::take;
using wal_bench_support::reactor_io;
namespace support = engine_bench_support;
namespace measured = local_storage_bench_support;
namespace appends = local_append_contract;
namespace lifecycle = segment_lifecycle_contract;
namespace installation = installation_contract;
namespace segments = segment_writer_contract;
namespace fixture = segment_bench_support;
using segment_type
  = segment_writer<measured::file_system, support::owner_type, support::clock>;

// What a run may change. Every case runs once at these sizes in the smoke
// tests of every build, so the defaults keep the cases short.
struct settings final {
    // Blocks of a sealed segment, each an anchor of the index it fed: more
    // than one index page of 1,024. About two pages is what a full 64-MiB
    // segment holds at the table's default stride of 32 KiB.
    std::uint32_t seal_blocks{2048};
    // Data bytes the segment a roll leaves may hold: room for a few dozen
    // requests of 8-KiB blocks.
    std::uint64_t roll_bytes{256_KiB};
    // Requests of the segment a removal takes away. Each is an 8-KiB block,
    // so its data region holds two 4-KiB file system blocks for each of
    // 1,024 extents, with room to spare.
    std::uint32_t removal_requests{1152};
};
std::uint64_t chosen_value(
  const char* name,
  std::uint64_t otherwise,
  std::uint64_t least,
  std::uint64_t most) {
    const auto* text = std::getenv(name);
    if (!text) return otherwise;
    char* end = nullptr;
    const auto value = std::strtoull(text, &end, 10);
    require(
      end != text && *end == '\0' && value >= least && value <= most,
      "a segment lifecycle setting is outside its bound");
    return value;
}
settings read_settings() {
    settings out;
    // The most stays under the segment's 64 MiB even when every request is
    // a group of its own, with a footer of its own.
    out.seal_blocks = static_cast<std::uint32_t>(chosen_value(
      "KWAQUE_SEGMENT_LIFECYCLE_SEAL_BLOCKS",
      out.seal_blocks,
      sparse_index_page_entries + 1,
      3072));
    out.roll_bytes = chosen_value(
      "KWAQUE_SEGMENT_LIFECYCLE_ROLL_BYTES", out.roll_bytes, 128_KiB, 8_MiB);
    require(
      out.roll_bytes % 8_KiB == 0,
      "a rolled segment's bound must be whole 8-KiB blocks");
    out.removal_requests = static_cast<std::uint32_t>(chosen_value(
      "KWAQUE_SEGMENT_LIFECYCLE_REMOVAL_REQUESTS",
      out.removal_requests,
      1024,
      3072));
    return out;
}

// The shard's and the observer's counts at one instant.
struct instant final {
    std::uint64_t at{0}, allocations{0}, tasks{0};
    seastar::thread_cputime_clock::time_point cpu;
    reactor_io reactor;
    measured::io_sample io;
    fixture::work_sample work;
};
// What one timed region took, and the native work it did by file kind.
struct region final {
    std::uint64_t elapsed{0}, cpu{0}, allocations{0}, tasks{0};
    reactor_io reactor;
    measured::io_sample io;
    fixture::work_sample work;
};
kind_sample between(const kind_sample& then, const kind_sample& now) {
    kind_sample out;
    out.calls = now.calls - then.calls;
    out.bytes = now.bytes - then.bytes;
    out.flushes = now.flushes - then.flushes;
    out.reads = now.reads - then.reads;
    out.read_bytes = now.read_bytes - then.read_bytes;
    out.opens = now.opens - then.opens;
    out.unlinks = now.unlinks - then.unlinks;
    out.unlink_ns = now.unlink_ns - then.unlink_ns;
    // The observer keeps the longest since observation began; no region
    // here follows one that removed a file.
    out.unlink_longest_ns = out.unlinks != 0 ? now.unlink_longest_ns : 0;
    return out;
}

// One case's timed region: the observer's counts, the hashed bytes and the
// harness's timer run from begin() to finish(). restart() moves the
// region's beginning to now, for a case that learns only later which of
// its instants began the region; whatever came before is not counted.
class timed_region final {
public:
    explicit timed_region(measured::file_system& files) noexcept
      : files_(files) {}
    timed_region(const timed_region&) = delete;
    timed_region& operator=(const timed_region&) = delete;
    ~timed_region() {
        if (!observing_) return;
        fixture::end_work_observation();
        files_.sample.measuring = false;
    }

    void begin() {
        files_.sample.measuring = true;
        fixture::begin_work_observation(work_);
        observing_ = true;
        restart();
    }
    void restart() {
        from_ = now();
        perf_tests::start_measuring_time();
    }
    [[nodiscard]] std::uint64_t began() const noexcept { return from_.at; }
    region finish() {
        perf_tests::stop_measuring_time();
        const auto to = now();
        fixture::end_work_observation();
        observing_ = false;
        files_.sample.measuring = false;
        region out;
        out.elapsed = to.at - from_.at;
        out.cpu = static_cast<std::uint64_t>((to.cpu - from_.cpu).count());
        out.allocations = to.allocations - from_.allocations;
        out.tasks = to.tasks - from_.tasks;
        out.reactor = to.reactor.since(from_.reactor);
        for (std::size_t k = 0; k != out.io.kinds.size(); ++k)
            out.io.kinds[k] = between(from_.io.kinds[k], to.io.kinds[k]);
        out.io.directory_opens = to.io.directory_opens
                                 - from_.io.directory_opens;
        out.io.directory_syncs = to.io.directory_syncs
                                 - from_.io.directory_syncs;
        out.io.listings = to.io.listings - from_.io.listings;
        out.io.stats = to.io.stats - from_.io.stats;
        out.io.renames = to.io.renames - from_.io.renames;
        out.io.directory_removes = to.io.directory_removes
                                   - from_.io.directory_removes;
        out.work.crc_bulk_calls = to.work.crc_bulk_calls
                                  - from_.work.crc_bulk_calls;
        out.work.crc_bulk_bytes = to.work.crc_bulk_bytes
                                  - from_.work.crc_bulk_bytes;
        out.work.digest_calls = to.work.digest_calls - from_.work.digest_calls;
        out.work.digest_bytes = to.work.digest_bytes - from_.work.digest_bytes;
        return out;
    }

private:
    [[nodiscard]] instant now() const {
        instant out;
        out.io = files_.sample;
        out.work = work_;
        out.reactor = reactor_io::now();
        out.allocations = seastar::memory::stats().mallocs();
        out.tasks = seastar::engine().get_sched_stats().tasks_processed;
        out.cpu = seastar::thread_cputime_clock::now();
        out.at = measurement_time();
        return out;
    }

    measured::file_system& files_;
    // Written by the hashing observer while observing: it stays in place.
    fixture::work_sample work_;
    instant from_;
    bool observing_{false};
};

void number(const char* name, std::uint64_t value) {
    std::printf(",\"%s\":%" PRIu64, name, value);
}
void flag(const char* name, bool value) {
    std::printf(",\"%s\":%s", name, value ? "true" : "false");
}
void print_kind(const char* name, const kind_sample& kind) {
    std::printf(
      ",\"%s\":{\"writes\":%" PRIu64 ",\"write_bytes\":%" PRIu64
      ",\"flushes\":%" PRIu64 ",\"reads\":%" PRIu64 ",\"read_bytes\":%" PRIu64
      ",\"opens\":%" PRIu64 ",\"unlinks\":%" PRIu64 ",\"unlink_ns\":%" PRIu64
      ",\"unlink_longest_ns\":%" PRIu64 "}",
      name,
      kind.calls,
      kind.bytes,
      kind.flushes,
      kind.reads,
      kind.read_bytes,
      kind.opens,
      kind.unlinks,
      kind.unlink_ns,
      kind.unlink_longest_ns);
}
// One row: what every case records, then `fields()` for its own. A
// lifecycle operation's durable steps are publications, data flushes and
// directory syncs, counted as they are; no barrier name describes them.
template<typename Fields>
void print_row(
  const char* name,
  const region& took,
  const std::set<unsigned>& affinity,
  Fields fields) {
    std::printf("segment_lifecycle_v1 {\"case\":\"%s\"", name);
    number("elapsed_ns", took.elapsed);
    number("reactor_cpu_ns", took.cpu);
    number("allocations", took.allocations);
    number("tasks", took.tasks);
    number("reactor_flushes", took.reactor.flushes);
    number("reactor_writes", took.reactor.writes);
    number("reactor_write_bytes", took.reactor.write_bytes);
    number("reactor_reads", took.reactor.reads);
    number("reactor_read_bytes", took.reactor.read_bytes);
    print_kind("wal", took.io[file_kind::wal]);
    print_kind("data", took.io[file_kind::data]);
    print_kind("other", took.io[file_kind::other]);
    number("directory_opens", took.io.directory_opens);
    number("directory_syncs", took.io.directory_syncs);
    number("listings", took.io.listings);
    number("stats", took.io.stats);
    number("renames", took.io.renames);
    number("directory_removes", took.io.directory_removes);
    number("crc_bulk_calls", took.work.crc_bulk_calls);
    number("crc_bulk_bytes", took.work.crc_bulk_bytes);
    number("digest_calls", took.work.digest_calls);
    number("digest_bytes", took.work.digest_bytes);
    fields();
    flag("timing_only", false);
#ifdef NDEBUG
    flag("ndebug", true);
#else
    flag("ndebug", false);
#endif
    std::printf(",\"affinity_cpus\":[");
    bool first = true;
    for (const auto cpu : affinity) {
        std::printf("%s%u", first ? "" : ",", cpu);
        first = false;
    }
    std::puts("]}");
}

// The engine under the tests' profile, for two segments written at once.
// The sampler's part is played once, after the start, and its share is
// trusted for an hour, so that no timed region waits for it again.
local_engine_config configured() {
    auto config = support::configuration(2);
    config.placement.stale_nanoseconds = 3'600'000'000'000;
    return config;
}
seastar::future<> started(support::bench_store& store) {
    const auto began = co_await support::start(store, {});
    take(began.failure.outcome());
    require(
      began.state == engine_state::ready && !began.restarted,
      "a fresh store did not start ready");
    support::share(store, byte_count{1_GiB});
}
support::engine_type::table_type& table_of(const support::bench_store& store) {
    auto* table = store.engine->table();
    require(table != nullptr, "the shard is not ready");
    return *table;
}

bool durable(const local_append_outcome& outcome) {
    return outcome.status == local_append_status::durable
           && outcome.receipt.has_value() && !outcome.failure.failed();
}
// A request's acceptance and its outcome.
struct offered final {
    local_append_acceptance decided;
    local_append_outcome outcome;
};
// The block of each request the roll and removal cases make, in bytes: their
// settings count blocks of this size, which no group or held run it shares
// changes.
constexpr std::size_t request_block = 8_KiB;
// One request of `generation` at `logical` whose block is `block` bytes
// long, or the smallest request when `block` is zero.
seastar::future<local_append_request> request_of(
  support::bench_store& store,
  std::uint64_t logical,
  std::uint64_t generation,
  std::size_t block,
  codec::cooperative_work& work) {
    if (block == 0)
        co_return co_await appends::request(
          appends::child_wire(logical, generation), store.appends, work);
    co_return co_await appends::request(
      lifecycle::sized_wire(logical, block, generation),
      store.appends,
      work,
      appends::large_width);
}
// One request of `generation` at `logical` with a block of request_block
// bytes, offered and joined, with a work account of its own: the shared
// fixture's append, keeping the acceptance.
seastar::future<offered> offer(
  support::bench_store& store,
  local_append_target target,
  std::uint64_t logical,
  std::uint64_t generation) {
    auto* owner = store.engine->append();
    require(owner != nullptr, "the shard is not ready");
    codec::cooperative_work work{store.config.policy, store.abort};
    auto stages = owner->append(
      target,
      co_await request_of(store, logical, generation, request_block, work),
      work);
    offered out;
    out.decided = co_await std::move(stages.accepted);
    out.outcome = co_await std::move(stages.result);
    co_return out;
}

// Joins both stages of one offered request, the result also after an
// acceptance that failed; true when it became durable. A failure is kept in
// `failed`.
seastar::future<bool>
settled(local_append_stages& stages, runtime::first_failure& failed) {
    bool accepted = false;
    try {
        accepted = (co_await std::move(stages.accepted)).accepted();
    } catch (...) {
        failed.observe(std::current_exception());
    }
    try {
        const auto outcome = co_await std::move(stages.result);
        const bool made = accepted && durable(outcome);
        co_return made;
    } catch (...) {
        failed.observe(std::current_exception());
    }
    co_return false;
}

// Requests in flight at once while a segment is filled: half of what the
// append owner accepts.
constexpr std::uint32_t fill_window = 32;

// Appends `count` one-record requests of `generation` to `target` from the
// segment's logical origin, before any timed region, each with a block of
// `block` bytes, or the smallest when it is zero. Each window's requests
// are made first and offered in one step, so they take that order, then
// joined; each has a work account of its own, kept until its result. Every
// one must be durable.
seastar::future<> fill(
  support::bench_store& store,
  local_append_target target,
  std::uint64_t generation,
  std::uint32_t count,
  std::size_t block = 0) {
    auto* owner = store.engine->append();
    require(owner != nullptr, "the shard is not ready");
    const auto origin = segments::logical_origin().value();
    runtime::first_failure failed;
    std::uint32_t done = 0, made_durable = 0;
    while (done != count && !failed.failed()) {
        const auto n = std::min(fill_window, count - done);
        std::deque<codec::cooperative_work> works;
        std::vector<local_append_request> inputs;
        inputs.reserve(n);
        for (std::uint32_t i = 0; i != n; ++i) {
            auto& work = works.emplace_back(store.config.policy, store.abort);
            inputs.push_back(
              co_await request_of(
                store, origin + done + i, generation, block, work));
        }
        std::vector<local_append_stages> stages;
        stages.reserve(n);
        try {
            for (std::uint32_t i = 0; i != n; ++i)
                stages.push_back(
                  owner->append(target, std::move(inputs[i]), works[i]));
        } catch (...) {
            failed.observe(std::current_exception());
        }
        for (auto& offered_stages : stages)
            if (co_await settled(offered_stages, failed)) ++made_durable;
        done += n;
    }
    take(failed.outcome());
    require(
      made_durable == count, "a request filling a segment was not durable");
}

// A segment of `generation` whose data may hold no more than `data_bytes`,
// created through the table as the shared fixture creates one.
seastar::future<runtime::result<local_append_target>> create_bounded(
  support::bench_store& store,
  std::uint64_t generation,
  std::uint64_t data_bytes) {
    auto& table = table_of(store);
    auto made = lifecycle::creation(store.devices.front(), generation);
    made.descriptor.maximum_data_bytes = byte_count{data_bytes};
    made.device = std::nullopt;
    codec::cooperative_work work{store.config.policy, store.abort};
    co_return co_await table.create(std::move(made), work);
}

// A seal's facts: its blocks, the index and retry bundles it published,
// each through the end of its root, and the data file it left.
void print_seal(
  std::uint32_t blocks,
  std::uint64_t alignment,
  const segment_seal_outcome& outcome) {
    number("blocks", blocks);
    number("alignment", alignment);
    flag("indexed", outcome.index.has_value());
    number("index_pages", outcome.index ? outcome.index->pages().value() : 0);
    number(
      "index_bundle_bytes",
      outcome.index
        ? outcome.index->position().value() + outcome.index->bytes().value()
        : 0);
    number("retry_pages", outcome.retry->pages().value());
    number(
      "retry_bundle_bytes",
      outcome.retry->position().value() + outcome.retry->bytes().value());
    number("data_file_bytes", outcome.extent->file_end.value());
}

// Every block an anchor: the densest index, so that a segment of a few
// thousand blocks has one of more than one page.
constexpr sparse_index_stride every_block{byte_count{0}, 0};

// A segment written through its own writer, and the index it fed.
struct written final {
    // Declared first: the writer refers to it, and goes first.
    std::unique_ptr<active_sparse_index> index;
    std::unique_ptr<segment_type> writer;
};

// Writes `blocks` 4-KiB blocks through a new segment's writer, 64 to a
// group and eight groups to a barrier, and feeds an index that takes every
// block as an anchor. Every block is a batch of its own producer sequence.
// Everything written is durable, and hashed into the extent's digest, when
// this returns.
seastar::future<> write_blocks(
  support::bench_store& store,
  written& out,
  std::uint32_t blocks,
  codec::cooperative_work& work) {
    const auto descriptor = installation::descriptor();
    auto config = segments::configuration();
    config.retry_object = local_object_sequence::make(1000).value();
    config.policy = work.policy();
    config.admission.working_bytes = byte_count{1_MiB};
    const auto capacity = sparse_index_capacity(
      every_block,
      descriptor.maximum_data_bytes,
      config.admission.maximum_blocks);
    // As a shard does before it starts: the budget must hold the index of
    // the segment it writes.
    take(validate_sparse_index_budget(
      store.appends, {.segments = 1, .capacity = capacity}));
    out.index = std::make_unique<active_sparse_index>(
      take(active_sparse_index::make(every_block, capacity, store.appends)));
    out.writer = take(
      segment_type::make_new(
        store.files,
        store.owner,
        store.devices.front(),
        0,
        descriptor,
        segments::logical_origin(),
        store.appends,
        config));
    auto& writer = *out.writer;
    constexpr std::uint32_t window = 8;
    std::vector<std::optional<segment_submission>> pending(window);
    runtime::first_failure failed;
    try {
        take(
          co_await seastar::with_scheduling_group(
            store.appends.scheduling_group(),
            [&writer, &work] { return writer.create_new(work); }));
        take(writer.observe_blocks(out.index->observer()));
        const fixture::shape shape{.name = "seal"};
        auto logical = segments::logical_origin();
        std::uint64_t sequence = 0;
        std::uint32_t done = 0;
        while (done != blocks) {
            std::optional<segment_captured_boundary> cut;
            for (std::uint32_t g = 0; g != window && done != blocks; ++g) {
                const auto count = std::min(
                  maximum_segment_group_blocks, blocks - done);
                std::vector<encoded_assigned_batch> batches;
                batches.reserve(count);
                auto next = logical;
                for (std::uint32_t b = 0; b != count; ++b) {
                    auto child = co_await fixture::make_batch(
                      shape, descriptor.segment, next, sequence++, work);
                    next = child.info().context.logical_span().end();
                    batches.push_back(std::move(child));
                }
                std::optional<runtime::result<segment_group_preparation>>
                  preparation;
                preparation.emplace(
                  writer.prepare_group(std::span{batches}, work));
                // Ordinary pressure: a written group keeps its memory until
                // the deferred digest has hashed it.
                if (
                  !*preparation
                  && preparation->error().code() == errc::queue_full) {
                    take(co_await writer.digest_caught_up());
                    preparation.emplace(
                      writer.prepare_group(std::span{batches}, work));
                }
                auto prepared = take(std::move(*preparation));
                require(
                  prepared.prepared.has_value(),
                  "the sealed segment's bound refused a group");
                std::vector<admitted_wal_batch> children;
                children.reserve(batches.size());
                for (auto& batch : batches) {
                    auto backing = take(
                      store.appends.try_reserve_buffer(batch.bytes()));
                    children.push_back(take(
                      admitted_wal_batch::make(
                        std::move(batch),
                        std::move(backing),
                        segments::child_append_time,
                        charge)));
                }
                auto group = take(
                  co_await writer.freeze_group(
                    std::move(*prepared.prepared), std::move(children), work));
                take(co_await writer.encode_group(group, work));
                auto accepted = take(writer.submit(std::move(group), work));
                cut = accepted.boundary;
                pending[g].emplace(std::move(accepted));
                logical = next;
                done += count;
            }
            require(cut.has_value(), "a window of groups took no barrier");
            const auto barrier = co_await writer.barrier(*cut);
            take(barrier.failure.outcome());
            for (auto& submission : pending) {
                if (!submission) continue;
                auto waiting = std::move(submission->written);
                submission.reset();
                const auto finished = co_await std::move(waiting);
                take(finished.failure.outcome());
            }
        }
        // The seal hashes nothing it was not given: the extent's deferred
        // digest has caught up before its timed region.
        take(co_await writer.digest_caught_up());
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
    take(failed.outcome());
}

// A seal through the segment's own writer, of a segment written the same
// way whichever it is: with the index it fed published in the same step, or
// with none. Only the seal is timed: the retry bundle, the sealed root and
// its flush, the index bundle when there is one, and the sealed
// publication. The writer is not asked what to retain: nothing completed,
// every block unresolved.
seastar::future<> writer_seal(
  support::bench_store& store,
  const settings& chosen,
  const std::set<unsigned>& affinity,
  bool indexed) {
    codec::cooperative_work work{store.config.policy, store.abort};
    written out;
    runtime::first_failure failed;
    try {
        co_await write_blocks(store, out, chosen.seal_blocks, work);
        auto& writer = *out.writer;
        const auto object = local_object_sequence::make(1001).value();
        std::uint32_t calls = 0;
        segment_seal_outcome outcome;
        timed_region timed{store.files};
        timed.begin();
        if (indexed)
            outcome = co_await writer.seal(
              segments::completed_source{{}, {}, &calls},
              0,
              work,
              sparse_index_seal{out.index.get(), object});
        else
            outcome = co_await writer.seal(
              segments::completed_source{{}, {}, &calls},
              0,
              work,
              segment_no_index{});
        const auto took = timed.finish();
        take(outcome.failure.outcome());
        require(
          outcome.boundary && outcome.retry && outcome.extent
            && outcome.index.has_value() == indexed
            && (!indexed
                || (out.index->frozen()
                    && out.index->size() == chosen.seal_blocks
                    && out.index->skipped() == 0
                    && outcome.index->pages().value() > 1)),
          "the segment was not sealed as asked");
        print_row(
          indexed ? "seal_with_index" : "seal_without_index",
          took,
          affinity,
          [&] {
              print_seal(
                chosen.seal_blocks,
                installation::descriptor().alignment.bytes().value(),
                outcome);
          });
    } catch (...) {
        failed.observe(std::current_exception());
    }
    if (out.writer) {
        try {
            failed.observe(co_await out.writer->close());
        } catch (...) {
            failed.observe(std::current_exception());
        }
    }
    take(failed.outcome());
}

// The product's whole seal: the table's, from the call to its answer. Its
// cuts end, the segment's admission closes and drains, it detaches, its
// writer seals with the index it fed, the publication covers the
// checkpoint's entries, and its writer closes. The table's stride takes
// every block as an anchor, so that the index has more than one page.
seastar::future<> table_seal(
  support::bench_store& store,
  const settings& chosen,
  const std::set<unsigned>& affinity) {
    co_await started(store);
    auto& table = table_of(store);
    const auto context = appends::context(1);
    const auto target = take(co_await support::create(store, 1));
    co_await fill(store, target, 1, chosen.seal_blocks);
    // What the WAL's rotations asked of the checkpoint owner is done before
    // the timed region, not inside it.
    static_cast<void>(take(co_await store.engine->checkpoint()->request()));
    timed_region timed{store.files};
    timed.begin();
    const auto sealed = co_await table.seal(context);
    const auto took = timed.finish();
    take(sealed.failure.outcome());
    require(
      sealed.boundary && sealed.retry && sealed.extent && sealed.expected
        && sealed.index && sealed.index->pages().value() > 1,
      "the table's seal did not publish an index of more than one page");
    print_row("seal_through_table", took, affinity, [&] {
        print_seal(
          chosen.seal_blocks,
          segments::descriptor().alignment.bytes().value(),
          sealed);
    });
}

// The time from the last durable result on a segment to the first on its
// successor, requests made one at a time. The first segment fills by its
// own bound. Its successor is either created through the table before the
// first takes its last request, and the next request goes to it once the
// first's room, read after each append as whoever owns the segments reads
// it, holds no more blocks of the size it took; or created only when the
// first refuses a request as needing a roll, and then offered the next one.
// The region begins anew at each durable result on the first segment, so
// it begins at its last; the time between the last two of them is the
// row's baseline. Making each request is the driver's work, in both.
seastar::future<> roll(
  support::bench_store& store,
  const settings& chosen,
  const std::set<unsigned>& affinity,
  bool ahead) {
    co_await started(store);
    auto& table = table_of(store);
    const auto first_context = appends::context(1);
    const auto origin = segments::logical_origin().value();
    const auto first = take(
      co_await create_bounded(store, 1, chosen.roll_bytes));
    std::optional<local_append_target> second;
    if (ahead) second = take(co_await support::create(store, 2));
    timed_region timed{store.files};
    std::uint64_t began = 0, baseline = 0, block = 0;
    std::uint32_t taken = 0;
    bool refused = false;
    for (;;) {
        require(
          taken <= chosen.roll_bytes / request_block,
          "the first segment took more requests than its bound holds");
        if (ahead && taken != 0) {
            const auto room = take(table.left(first_context));
            if (room.blocks == 0 || room.bytes.value() < block) break;
        }
        const auto sent = co_await offer(store, first, origin + taken, 1);
        if (
          !ahead && !sent.decided.failure.failed()
          && sent.decided.capacity
               == segment_capacity_decision::roll_required) {
            require(
              sent.outcome.status == local_append_status::not_written
                && !sent.outcome.receipt,
              "a request the first segment refused had an effect");
            refused = true;
            break;
        }
        require(
          sent.decided.accepted() && durable(sent.outcome),
          "the first segment did not take a request its room held");
        block = sent.outcome.receipt->block.records.bytes().size().value();
        ++taken;
        const auto previous = began;
        if (taken == 1)
            timed.begin();
        else
            timed.restart();
        began = timed.began();
        if (taken != 1) baseline = began - previous;
    }
    require(taken >= 2, "the first segment took fewer than two requests");
    if (!ahead) second = take(co_await support::create(store, 2));
    const auto last = co_await offer(store, *second, origin, 2);
    const auto took = timed.finish();
    require(
      last.decided.accepted() && durable(last.outcome),
      "the successor did not take its first request");
    print_row(ahead ? "roll_ahead" : "roll_on_refusal", took, affinity, [&] {
        number("gap_ns", took.elapsed);
        number("baseline_ns", baseline);
        number("first_requests", taken);
        flag("successor_ahead", ahead);
        flag("refused", refused);
        number("first_bytes", chosen.roll_bytes);
        number("block_bytes", block);
    });
}

// What a data file was made to hold: holes punched to separate its runs,
// the extents the file system then maps, its size and what it allocates.
struct shaped_file final {
    std::uint32_t target{0}, holes{0};
    // The file system refused to punch: the file is measured as written.
    bool punch_refused{false};
    // The file system mapped its extents.
    bool extents_known{false};
    std::uint64_t extents{0}, size{0}, allocated{0}, block{0};
};

// The extents FS_IOC_FIEMAP maps over the whole file, asked a page at a
// time until one carries FIEMAP_EXTENT_LAST, or until none is mapped past
// the last asked for. None when the file system maps no extents. The
// request is written into a byte buffer and the answer read out of it with
// memcpy; the ioctl fills it on the syscall thread, and it stays in place
// until the ioctl returns.
seastar::future<std::optional<std::uint64_t>>
count_extents(seastar::file& file) {
    constexpr std::uint32_t page = 64;
    std::vector<std::byte> buffer(
      sizeof(fiemap) + page * sizeof(fiemap_extent));
    std::uint64_t start = 0, counted = 0;
    for (;;) {
        fiemap asked{};
        asked.fm_start = start;
        asked.fm_length = FIEMAP_MAX_OFFSET - start;
        asked.fm_flags = FIEMAP_FLAG_SYNC;
        asked.fm_extent_count = page;
        std::fill(buffer.begin(), buffer.end(), std::byte{0});
        std::memcpy(buffer.data(), &asked, sizeof(asked));
        bool refused = false;
        try {
            static_cast<void>(
              co_await file.ioctl(FS_IOC_FIEMAP, buffer.data()));
        } catch (const std::system_error& error) {
            if (error.code().value() != EOPNOTSUPP || counted != 0) throw;
            refused = true;
        }
        if (refused) co_return std::nullopt;
        fiemap answered{};
        std::memcpy(&answered, buffer.data(), sizeof(answered));
        if (answered.fm_mapped_extents == 0) co_return counted;
        counted += answered.fm_mapped_extents;
        fiemap_extent last{};
        std::memcpy(
          &last,
          buffer.data() + sizeof(fiemap)
            + std::size_t{answered.fm_mapped_extents - 1}
                * sizeof(fiemap_extent),
          sizeof(last));
        if ((last.fe_flags & FIEMAP_EXTENT_LAST) != 0) co_return counted;
        start = last.fe_logical + last.fe_length;
    }
}

// Makes the data file at `path` hold at least `target` extents by punching
// `target - 1` holes of one file system block each, spread evenly through
// the data region `data` and never at its first or last block, so neither
// the header before it nor the sealed root after it is touched and every
// run between two holes keeps at least one block. With
// FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE (seastar::file::discard) the
// size stays and whole blocks are freed. Then the extents are counted.
seastar::future<shaped_file> shape_extents(
  const runtime::file_path& path,
  model::file_byte_span data,
  std::uint32_t target) {
    shaped_file out{.target = target};
    auto file = co_await seastar::open_file_dma(
      path.value(), seastar::open_flags::rw);
    std::exception_ptr failure;
    try {
        const auto before = co_await file.stat();
        out.block = static_cast<std::uint64_t>(before.st_blksize);
        require(out.block != 0, "the file system names no block size");
        const auto first = (data.begin().value() + out.block - 1) / out.block;
        const auto end = data.end().value() / out.block;
        const auto blocks = end > first ? end - first : 0;
        require(
          blocks >= 2 * std::uint64_t{target},
          "the data region cannot hold the extents asked of it; raise "
          "KWAQUE_SEGMENT_LIFECYCLE_REMOVAL_REQUESTS");
        for (std::uint32_t i = 1; i < target && !out.punch_refused; ++i) {
            const auto at = (first + std::uint64_t{i} * blocks / target)
                            * out.block;
            try {
                co_await file.discard(at, out.block);
                ++out.holes;
            } catch (const std::system_error& error) {
                if (error.code().value() != EOPNOTSUPP || out.holes != 0) throw;
                out.punch_refused = true;
            }
        }
        if (const auto counted = co_await count_extents(file)) {
            out.extents_known = true;
            out.extents = *counted;
        }
        const auto after = co_await file.stat();
        out.size = static_cast<std::uint64_t>(after.st_size);
        out.allocated = static_cast<std::uint64_t>(after.st_blocks) * 512U;
    } catch (...) {
        failure = std::current_exception();
    }
    co_await file.close();
    if (failure) std::rethrow_exception(failure);
    co_return out;
}

// One sealed segment's removal through the table, authorized with its
// sealed extent as its owner of segments authorizes it, timed whole: its
// deleting name and the sync above it, its files one call at a time, and
// its directory with one more sync. A checkpoint after the seal has let go
// of the segment, so the removal asks for none. Its data file was made to
// hold `target` extents first; the observer times that file's unlink alone.
seastar::future<> removal(
  support::bench_store& store,
  const settings& chosen,
  const std::set<unsigned>& affinity,
  const char* name,
  std::uint32_t target) {
    co_await started(store);
    auto& table = table_of(store);
    const auto context = appends::context(1);
    const auto made = take(co_await support::create(store, 1));
    co_await fill(store, made, 1, chosen.removal_requests, request_block);
    const auto sealed = co_await table.seal(context);
    take(sealed.failure.outcome());
    require(
      sealed.extent && sealed.expected, "the removed segment was not sealed");
    for (unsigned asked = 0; asked != 4 && table.restart_names(context);
         ++asked)
        static_cast<void>(take(co_await store.engine->checkpoint()->let_go()));
    require(
      !table.restart_names(context),
      "the checkpoints after a seal still name the segment");
    const auto path = take(take(local_paths::make(store.devices.front().root))
                             .segment_file(
                               0,
                               {context.segment(), context.generation()},
                               local_segment_file::data));
    const auto shaped = co_await shape_extents(
      path, sealed.extent->coverage.bytes(), target);
    timed_region timed{store.files};
    timed.begin();
    const auto removed = co_await table.remove(
      {context, sealed.extent}, sealed.expected.get());
    const auto took = timed.finish();
    take(removed.failure.outcome());
    require(
      removed.verdict == authorization_verdict::authorized && removed.gone,
      "the sealed segment was not removed");
    print_row(name, took, affinity, [&] {
        number("requests", chosen.removal_requests);
        number("extent_target", shaped.target);
        number("holes", shaped.holes);
        flag("punch_refused", shaped.punch_refused);
        flag("extents_known", shaped.extents_known);
        number("extents", shaped.extents);
        number("fs_block_bytes", shaped.block);
        number("data_file_bytes", shaped.size);
        number("data_allocated_bytes", shaped.allocated);
        number("removed_files", removed.removed);
    });
}

// One segment created through the table, from the call to its target: its
// directories, each with a sync of the one above, its descriptor, its data
// file's header and its first published pointer. The store's first creation
// came before it, untimed: it took the first identity, which refills the
// control record's block of them, and made the bucket every generation of
// the segment shares.
seastar::future<> creation(
  support::bench_store& store,
  const settings&,
  const std::set<unsigned>& affinity) {
    co_await started(store);
    static_cast<void>(take(co_await support::create(store, 1)));
    timed_region timed{store.files};
    timed.begin();
    auto made = co_await support::create(store, 2);
    const auto took = timed.finish();
    static_cast<void>(take(std::move(made)));
    print_row("creation", took, affinity, [] {});
}

// The group's cases. Each runs over a fresh store under the test directory,
// with the shard's resource owners started for it alone.
struct segment_lifecycle {
    segment_lifecycle() {
        static const bool reported = [] {
            codec::testing::report_profile();
            return true;
        }();
        static_cast<void>(reported);
    }

    template<typename Body>
    seastar::future<std::size_t>
    run_case(local_engine_config config, Body body) const {
        const auto chosen = read_settings();
        const auto affinity = seastar::get_current_cpuset();
        co_await support::with_bench_resources(
          [&](
            resource::resource_manager& manager,
            runtime::production::timer& timer) -> seastar::future<> {
              co_await seastar::tmp_dir::do_with(
                runtime::testing::test_directory_template(),
                seastar::coroutine::lambda(
                  [&](seastar::tmp_dir& directory) -> seastar::future<> {
                      co_await support::with_bench_store(
                        manager,
                        timer,
                        {directory.get_path() / "store", {}},
                        true,
                        config,
                        [&](support::bench_store& store) {
                            return body(store, chosen, affinity);
                        });
                  }));
          });
        require(
          seastar::get_current_cpuset() == affinity,
          "benchmark CPU affinity changed during execution");
        co_return 1;
    }
};

PERF_TEST_F(segment_lifecycle, seal_without_index) {
    return run_case(
      configured(),
      [](
        support::bench_store& store,
        const settings& chosen,
        const std::set<unsigned>& affinity) {
          return writer_seal(store, chosen, affinity, false);
      });
}
PERF_TEST_F(segment_lifecycle, seal_with_index) {
    return run_case(
      configured(),
      [](
        support::bench_store& store,
        const settings& chosen,
        const std::set<unsigned>& affinity) {
          return writer_seal(store, chosen, affinity, true);
      });
}
PERF_TEST_F(segment_lifecycle, seal_through_table) {
    auto config = configured();
    config.table.stride = every_block;
    return run_case(std::move(config), table_seal);
}

PERF_TEST_F(segment_lifecycle, roll_ahead) {
    return run_case(
      configured(),
      [](
        support::bench_store& store,
        const settings& chosen,
        const std::set<unsigned>& affinity) {
          return roll(store, chosen, affinity, true);
      });
}
PERF_TEST_F(segment_lifecycle, roll_on_refusal) {
    return run_case(
      configured(),
      [](
        support::bench_store& store,
        const settings& chosen,
        const std::set<unsigned>& affinity) {
          return roll(store, chosen, affinity, false);
      });
}

PERF_TEST_F(segment_lifecycle, removal_extents_1) {
    return run_case(
      configured(),
      [](
        support::bench_store& store,
        const settings& chosen,
        const std::set<unsigned>& affinity) {
          return removal(store, chosen, affinity, "removal_extents_1", 1);
      });
}
PERF_TEST_F(segment_lifecycle, removal_extents_64) {
    return run_case(
      configured(),
      [](
        support::bench_store& store,
        const settings& chosen,
        const std::set<unsigned>& affinity) {
          return removal(store, chosen, affinity, "removal_extents_64", 64);
      });
}
PERF_TEST_F(segment_lifecycle, removal_extents_1024) {
    return run_case(
      configured(),
      [](
        support::bench_store& store,
        const settings& chosen,
        const std::set<unsigned>& affinity) {
          return removal(store, chosen, affinity, "removal_extents_1024", 1024);
      });
}

PERF_TEST_F(segment_lifecycle, creation) {
    return run_case(configured(), creation);
}
} // namespace
} // namespace kwaque::storage::testing::segment_lifecycle_bench_support
