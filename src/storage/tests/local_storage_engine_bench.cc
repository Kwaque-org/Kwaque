#include "src/base/error.h"
#include "src/base/units.h"
#include "src/codec/cooperative.h"
#include "src/codec/tests/memory_qualification_support.h"
#include "src/model/position.h"
#include "src/resource/resource_manager.h"
#include "src/runtime/first_failure.h"
#include "src/runtime/production/timer.h"
#include "src/runtime/testing/test_directory.h"
#include "src/storage/local_append.h"
#include "src/storage/local_engine.h"
#include "src/storage/local_metadata.h"
#include "src/storage/local_store_config.h"
#include "src/storage/local_types.h"
#include "src/storage/placement_statistics.h"
#include "src/storage/retention.h"
#include "src/storage/segment_lifecycle.h"
#include "src/storage/segment_writer.h"
#include "src/storage/tests/engine_bench_fixture.h"
#include "src/storage/tests/latency_histogram.h"
#include "src/storage/tests/local_append_contract.h"
#include "src/storage/tests/local_storage_bench_file.h"
#include "src/storage/tests/local_store_contract.h"
#include "src/storage/tests/segment_bench_fixture.h"
#include "src/storage/tests/segment_test_support.h"
#include "src/storage/tests/segment_writer_contract.h"
#include "src/storage/tests/wal_bench_file.h"
#include "src/storage/wal_group.h"
#include "src/storage/wal_writer.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/posix.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread_cputime_clock.hh>
#include <seastar/testing/perf_tests.hh>
#include <seastar/util/tmp_file.hh>

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cinttypes>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <exception>
#include <filesystem>
#include <fstream>
#include <initializer_list>
#include <limits>
#include <memory>
#include <optional>
#include <set>
#include <span>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>
#include <variant>
#include <vector>

// Appends through the engine, the product's composition of one shard's
// storage, on native files: an open-loop stream of requests at a fixed
// spacing to two segments, alone or with a seal, a removal or a refused
// creation started beside it, on one data device or two, and at a fixed rate
// until enough WAL barriers are durable for a tail. Each request's latency is
// taken from the instant it was planned for. These cases are reported and
// never paired: what runs beside the stream changes the work.
namespace kwaque::storage::testing::engine_append_bench {
namespace {
using store_contract::require;
using store_contract::take;
using wal_bench_support::measurement_time;
namespace stores = engine_bench_support;
namespace appends = local_append_contract;
namespace fixture = segment_bench_support;
namespace measured = local_storage_bench_support;
using stores::bench_store;
using segment_type
  = segment_writer<measured::file_system, stores::owner_type, stores::clock>;
#if defined(KWAQUE_LOCAL_STORAGE_TIMING_ONLY)
constexpr bool timing_only = true;
#else
constexpr bool timing_only = false;
#endif

// The stream's segments by default, created first, and the most a run may
// set; the one after them is what a seal, a removal or a refused creation
// takes.
constexpr std::uint32_t stream_targets = 2;
constexpr std::uint32_t maximum_stream_targets = 1024;
// What runs beside the stream starts at the planned instant of this request.
constexpr std::size_t beside_at = 128;
// Every segment's records begin here, as its creation says.
constexpr std::uint64_t first_logical = 100;
// The histograms keep one-second intervals.
constexpr std::uint64_t interval_ns = 1'000'000'000;
// The stream's first request is planned this far ahead of the histograms'
// making, so that it is not late for it.
constexpr std::uint64_t lead_ns = 1'000'000;
// The sampler's part, each pool's share for this shard, as the engine's
// contract hands it.
constexpr std::uint64_t pool_share = 1_GiB;
// The third segment is filled to half of the 64 MiB its descriptor allows:
// a seal or removal of a near-empty segment interferes little. Its children
// are the composed cases' aligned ones, offered 16 at a time.
constexpr std::size_t fill_payload = 128_KiB;
constexpr std::uint32_t fill_requests = 256, fill_burst = 16;

enum class beside_kind : std::uint8_t { none, seal, removal, creation };
const char* beside_name(beside_kind kind) noexcept {
    switch (kind) {
    case beside_kind::none:
        return "none";
    case beside_kind::seal:
        return "seal";
    case beside_kind::removal:
        return "removal";
    case beside_kind::creation:
        return "creation";
    }
    std::abort();
}

struct case_shape final {
    const char* name;
    beside_kind beside{beside_kind::none};
    std::uint32_t data_devices{1};
    // Every pool's share is zero from before the interval to its end.
    bool exhausted{false};
    // Planned at its own spacing until enough WAL barriers are durable.
    bool fixed_rate{false};
    // The bench owns the stream's segments as their owner of segments
    // would: a request refused because its segment must roll is offered
    // again to a successor made afresh, and the full segment is sealed
    // beside the stream.
    bool rolling{false};
    // Segments written at once: the stream's and one more. A creation that
    // is refused for space needs the room for it, so that the share and
    // not the bound on writable segments refuses it. A rolling stream's
    // segments fill together, and each successor is made before its
    // predecessor's seal ends.
    [[nodiscard]] std::uint32_t writable(std::uint32_t targets) const noexcept {
        if (rolling) return 2 * targets;
        return beside == beside_kind::none ? targets : targets + 1;
    }
};

// The most requests one stream plans: what its histograms hold, and what its
// segments take. A request of the stream takes at most its child's units of
// its segment's alignment and two more, for its block's envelope and its
// group's footer; a sixteenth of the segment is kept for its header, the
// root its seal writes and the requests in flight. A rolling stream's
// segments are followed by others, so only its histograms bound it.
std::size_t request_bound(
  std::uint32_t targets,
  std::size_t payload,
  std::uint64_t unit,
  std::uint64_t data,
  bool rolling) {
    const auto each = (data - data / 16)
                      / ((payload + unit - 1) / unit * unit + 2 * unit);
    return rolling ? interval_histogram::maximum_values
                   : std::min<std::size_t>(
                       interval_histogram::maximum_values,
                       std::size_t{targets} * each);
}

// A whole number from the environment within its bound, or the default.
std::uint64_t setting(
  const char* name,
  std::uint64_t fallback,
  std::uint64_t least,
  std::uint64_t most) {
    const char* text = std::getenv(name);
    if (!text) return fallback;
    std::uint64_t value = 0;
    const char* end = text + std::strlen(text);
    const auto parsed = std::from_chars(text, end, value);
    if (
      parsed.ec != std::errc{} || parsed.ptr != end || value < least
      || value > most)
        throw std::runtime_error(
          std::string{name} + " must be a whole number from "
          + std::to_string(least) + " to " + std::to_string(most));
    return value;
}

// What one block device did, from the counters the kernel keeps for it:
// requests and bytes each way, and the cache flushes sent to it. They count
// every process's I/O and what the filesystem writes for itself, which no
// count of the shard's own sees, so a run reads them on a quiet machine.
struct block_io final {
    std::uint64_t reads{0}, read_bytes{0}, writes{0}, write_bytes{0},
      flushes{0};

    static block_io read(const char* device) {
        const std::string name{device};
        require(
          !name.empty()
            && std::all_of(
              name.begin(),
              name.end(),
              [](char c) {
                  return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
                         || c == '-' || c == '_';
              }),
          "KWAQUE_BENCH_BLOCK_DEVICE must name a block device");
        std::ifstream stat{"/sys/block/" + name + "/stat"};
        // Sectors are 512 bytes whatever the device's own size.
        std::array<std::uint64_t, 17> field{};
        for (auto& value : field)
            stat >> value;
        require(bool(stat), "the block device reports no flush counts");
        return {field[0], field[2] * 512, field[4], field[6] * 512, field[15]};
    }
    [[nodiscard]] block_io since(const block_io& before) const noexcept {
        return {
          reads - before.reads,
          read_bytes - before.read_bytes,
          writes - before.writes,
          write_bytes - before.write_bytes,
          flushes - before.flushes};
    }
};

// What a case plans. The smoke tests run each case once in three builds:
// the short streams' defaults keep them brief, and the fixed-rate case runs
// to the 3,000 independent groups a tail needs, about 12 s at its spacing. A
// measured run sets the sizes it wants.
struct plan final {
    std::size_t requests{0};
    std::uint64_t spacing_ns{0};
    // The segments the stream goes to, and the encoded bytes of each
    // request's one record, from 1 KiB to the 1 MiB a record may take: zero
    // for the small fixture child.
    std::uint32_t targets{stream_targets};
    std::size_t payload{0};
    // The append owner's placement threshold and hold. By default the
    // engine contract's: every portion in its segment.
    byte_count placement{}, hold{};
    // The fixed-rate case plans until this many WAL barriers are durable.
    std::optional<std::uint64_t> groups;
    // The WAL file and every segment's data file zero-written before the
    // timed region, as recycled files would be, so that writes inside them
    // overwrite and small ones go through a synchronized handle.
    bool synchronized{false};
    // Of a synchronized run: segments made as new files are, with the
    // writer's own zero-written window and extensions, rather than with a
    // window that holds the whole stream.
    bool fresh{false};
    // The alignment of the WAL and of every segment: the engine contract's,
    // or the device's.
    std::uint64_t alignment{0};
    // The data bytes a segment of the stream holds before it must roll.
    std::uint64_t segment_bytes{0};
    // The largest write the WAL and the segments make through their
    // synchronized handles; zero keeps the engine contract's.
    std::uint64_t synchronous_write{0};
    // How long the store rests between its setup and the stream, so that a
    // device still absorbing the files zero-written for the run does not
    // stall it.
    std::uint64_t settle_ms{0};
    // The block device whose counters the row gives for the interval.
    const char* block{nullptr};
    const char* data{nullptr};
    const char* second{nullptr};

    // The requests one of the stream's segments takes, its first included.
    [[nodiscard]] std::size_t per_target() const noexcept {
        return (requests + targets - 1) / targets + 1;
    }

    static plan of(const case_shape& chosen) {
        // The composed cases' knobs mean nothing here; a run that sets one
        // is refused rather than read as if it applied.
        for (const char* composed :
             {"KWAQUE_LOCAL_APPEND_COMPARISON_ID",
              "KWAQUE_LOCAL_APPEND_FLUSH_CAP",
              "KWAQUE_LOCAL_APPEND_FIXTURE",
              "KWAQUE_LOCAL_APPEND_ZERO_TIMESTAMPS",
              "KWAQUE_LOCAL_APPEND_OBSERVE_ALLOCATIONS",
              "KWAQUE_LOCAL_APPEND_FOREGROUND_PROBE"})
            if (std::getenv(composed))
                throw std::runtime_error(
                  std::string{composed}
                  + " applies to the composed append cases only");
        plan result;
        result.targets = static_cast<std::uint32_t>(setting(
          "KWAQUE_LOCAL_APPEND_STREAM_SEGMENTS",
          stream_targets,
          chosen.data_devices,
          maximum_stream_targets));
        result.payload = setting(
          "KWAQUE_LOCAL_APPEND_STREAM_PAYLOAD_BYTES", 0, 0, 1_MiB);
        require(
          result.payload == 0 || result.payload >= 1_KiB,
          "KWAQUE_LOCAL_APPEND_STREAM_PAYLOAD_BYTES must be zero or from 1024 "
          "to 1048576");
        // A rolling stream's segments are the product's: the WAL
        // zero-written whole, each segment a new file with the writer's own
        // window and extensions.
        result.synchronized = std::getenv("KWAQUE_LOCAL_APPEND_SYNCHRONIZED")
                                != nullptr
                              || chosen.rolling;
        require(
          !result.synchronized || !timing_only,
          "synchronized writes are counted only by local_storage_bench");
        result.fresh = std::getenv("KWAQUE_LOCAL_APPEND_FRESH_SEGMENTS")
                         != nullptr
                       || chosen.rolling;
        require(
          !result.fresh || result.synchronized,
          "fresh segments are zero-written: set "
          "KWAQUE_LOCAL_APPEND_SYNCHRONIZED too");
        result.alignment = setting(
          "KWAQUE_LOCAL_APPEND_ALIGNMENT",
          segment_writer_contract::descriptor().alignment.bytes().value(),
          4_KiB,
          64_KiB);
        require(
          storage_alignment::make(byte_count{result.alignment}).has_value(),
          "KWAQUE_LOCAL_APPEND_ALIGNMENT must be a power of two");
        const auto contract_data
          = segment_writer_contract::descriptor().maximum_data_bytes.value();
        result.segment_bytes = setting(
          "KWAQUE_LOCAL_APPEND_SEGMENT_BYTES", contract_data, 1_MiB, 1_GiB);
        require(
          result.segment_bytes % result.alignment == 0,
          "KWAQUE_LOCAL_APPEND_SEGMENT_BYTES must be a multiple of the "
          "alignment");
        result.synchronous_write = setting(
          "KWAQUE_LOCAL_APPEND_SYNCHRONOUS_WRITE_BYTES",
          0,
          4_KiB,
          runtime::maximum_file_io_bytes.value());
        result.settle_ms = setting(
          "KWAQUE_LOCAL_APPEND_SETTLE_MS", 0, 0, 600'000);
        result.block = std::getenv("KWAQUE_BENCH_BLOCK_DEVICE");
        if (result.block) block_io::read(result.block);
        const auto contract = stores::engines::configuration().append;
        result.placement = byte_count{setting(
          "KWAQUE_LOCAL_APPEND_PLACEMENT_BYTES",
          contract.segment_placement_bytes.value(),
          0,
          std::numeric_limits<std::uint64_t>::max())};
        result.hold = byte_count{setting(
          "KWAQUE_LOCAL_APPEND_HOLD_BYTES",
          contract.held_run_bytes.value(),
          0,
          maximum_segment_hold_bytes.value())};
        const auto bound = request_bound(
          result.targets,
          result.payload,
          result.alignment,
          result.segment_bytes,
          chosen.rolling);
        if (chosen.fixed_rate) {
            result.requests = bound;
            result.spacing_ns = 1'000
                                * setting(
                                  "KWAQUE_LOCAL_APPEND_FIXED_RATE_SPACING_US",
                                  4'000,
                                  1,
                                  1'000'000);
            // A tail's p99.9 is read over 3,000 independent groups.
            result.groups = setting(
              "KWAQUE_LOCAL_APPEND_FIXED_RATE_GROUPS", 3'000, 1, bound);
        } else {
            // Fewer when the stream's segments take fewer: one segment of
            // 1 MiB records takes 59.
            result.requests = setting(
              "KWAQUE_LOCAL_APPEND_STREAM_REQUESTS",
              std::min<std::size_t>(512, bound),
              beside_at + 1,
              bound);
            result.spacing_ns
              = 1'000
                * setting(
                  "KWAQUE_LOCAL_APPEND_STREAM_SPACING_US", 1'000, 1, 1'000'000);
        }
        result.data = std::getenv("KWAQUE_LOCAL_APPEND_DATA_DIRECTORY");
        result.second = std::getenv(
          "KWAQUE_LOCAL_APPEND_SECOND_DATA_DIRECTORY");
        return result;
    }
};

// The engine's limits for `chosen`. The WAL file holds every request the
// case makes, and is at least the composed cases' 128 MiB, so that no case
// rotates it: a test-sized one would rotate every 128 small groups, and the
// fixed-rate case at its bound would reach the files a shard may hold. A
// synchronized run has it zero-written whole when it is made, as the
// product makes each file. The append owner attaches every segment written
// at once. A share handed before the interval is not stale inside it,
// whatever the knobs make it last.
local_engine_config
configuration(const case_shape& chosen, const plan& planned) {
    const auto writable = chosen.writable(planned.targets);
    auto config = stores::configuration(writable);
    const std::uint64_t each = std::max<std::uint64_t>(planned.payload, 1_KiB)
                               + 16_KiB;
    // Only WAL-placed portions take room there: records at or above the
    // placement threshold go to their segments alone.
    const bool wal_placed = std::max<std::uint64_t>(planned.payload, 1_KiB)
                            < planned.placement.value();
    std::uint64_t needed
      = (wal_placed ? (planned.requests + planned.targets) * each : 0) + 1_MiB;
    if (
      chosen.beside == beside_kind::seal
      || chosen.beside == beside_kind::removal)
        needed += std::uint64_t{fill_requests} * (fill_payload + 16_KiB);
    config.wal.capacity_bytes = byte_count{
      std::max<std::uint64_t>(128_MiB, (needed + 1_MiB - 1) / 1_MiB * 1_MiB)};
    config.wal.alignment = alignment(planned.alignment);
    if (planned.synchronous_write != 0)
        config.wal.synchronous_write_bytes = byte_count{
          planned.synchronous_write};
    if (planned.synchronized) config.wal.preallocation_bytes = wal_whole_file;
    config.append.maximum_segments = std::max(
      config.append.maximum_segments, writable);
    config.placement.stale_nanoseconds = 86'400'000'000'000ULL;
    config.append.segment_placement_bytes = planned.placement;
    config.append.held_run_bytes = planned.hold;
    return config;
}

// One device of the store as the row names it.
struct device_row final {
    device_store_id store;
    std::uint64_t filesystem{0};
    std::optional<std::size_t> pool;
    local_device_role role{local_device_role::all};
};

// What ran beside the stream: when it began and ended, and what it answered.
struct beside_state final {
    beside_kind kind{beside_kind::none};
    // The segment it seals, removes or makes: the one after the stream's.
    std::uint64_t generation{0};
    bool began{false}, ended{false};
    std::uint64_t begin{0}, end{0};
    // The removal's authorization and what the segment was sealed as, kept
    // until the removal returns.
    std::optional<deletion_authorization> authorization;
    seastar::lw_shared_ptr<const sealed_segment_expectation> expected;
    std::optional<segment_sealed> sealed;
    std::optional<segment_removal> removed;
    std::optional<runtime::result<local_append_target>> made;
    // The table's owners before the refused creation.
    std::size_t table_size{0};
    // What the third segment was filled with, and what its seal answered.
    std::uint64_t fill_bytes{0}, sealed_end{0}, sealed_records{0};
};

struct measured_interval final {
    std::uint64_t elapsed{0}, cpu{0}, allocations{0}, tasks{0}, operations{0};
    wal_bench_support::reactor_io io;
    // The block device's counters across the interval, when the run names
    // one.
    std::optional<block_io> block;
};

// The timed region: opened where it is made and closed where it is
// destroyed. Its time is counted from the stream's first planned instant.
class interval_scope final {
public:
    interval_scope(
      measured::file_system& files,
      measured_interval& output,
      std::uint64_t epoch)
      : files_(files)
      , output_(output)
      , epoch_(epoch)
      , before_allocations_(seastar::memory::stats().mallocs())
      , before_tasks_(seastar::engine().get_sched_stats().tasks_processed)
      , before_operations_(files.statistics().accepted)
      , before_io_(wal_bench_support::reactor_io::now()) {
        files_.sample.measuring = true;
        cpu_ = seastar::thread_cputime_clock::now();
        perf_tests::start_measuring_time();
    }
    ~interval_scope() {
        perf_tests::stop_measuring_time();
        const auto now = measurement_time();
        output_.elapsed = now > epoch_ ? now - epoch_ : 0;
        output_.cpu = static_cast<std::uint64_t>(
          (seastar::thread_cputime_clock::now() - cpu_).count());
        files_.sample.measuring = false;
        output_.allocations = seastar::memory::stats().mallocs()
                              - before_allocations_;
        output_.tasks = seastar::engine().get_sched_stats().tasks_processed
                        - before_tasks_;
        output_.operations = files_.statistics().accepted - before_operations_;
        output_.io = wal_bench_support::reactor_io::now().since(before_io_);
    }
    interval_scope(const interval_scope&) = delete;
    interval_scope& operator=(const interval_scope&) = delete;

private:
    measured::file_system& files_;
    measured_interval& output_;
    std::uint64_t epoch_;
    std::uint64_t before_allocations_, before_tasks_, before_operations_;
    wal_bench_support::reactor_io before_io_;
    seastar::thread_cputime_clock::time_point cpu_;
};

// Everything a case's row says, kept until the store is stopped.
struct row final {
    std::vector<device_row> devices;
    std::vector<std::size_t> target_devices;
    std::uint64_t share{0}, wal_capacity{0};
    std::size_t requests{0}, completed{0};
    // Distinct barrier cuts that published the stream's requests, those of
    // them that a request overlapping what ran beside took, and those
    // requests.
    std::uint64_t groups{0}, overlap_groups{0}, overlapping{0};
    // The stream's first planned instant.
    std::uint64_t epoch{0};
    // A rolling stream's successors, and how long their creations held its
    // requests back, together and the longest.
    std::uint64_t rolls{0}, roll_creation_ns{0}, roll_creation_longest_ns{0};
    // The largest write a segment makes through its synchronized handle.
    std::uint64_t synchronous_write{0};
    measured_interval interval;
    // What the append owner placed, held and synced in the interval: its
    // counts as they grew there, and what stood at its end.
    local_append_statistics placed;
    measured::io_sample observed;
    std::optional<interval_histogram> latency, delay, overlap;
    beside_state beside;
};

// What one request of the stream left for the count of its barriers: where
// its WAL barrier's cut ended, and whether it overlapped what ran beside.
// The barrier that published a request: its WAL barrier, named by its cut,
// or its segment's barrier, named by the segment and where its cut ends,
// when that came first.
using publishing_cut = std::
  variant<local_wal_cursor, std::pair<segment_context, runtime::file_position>>;
struct settled_request final {
    std::optional<publishing_cut> cut;
    bool overlapped{false};
};

// The append owner's counts as they grew from `before` to `after`, and what
// stood at `after`.
local_append_statistics grown(
  const local_append_statistics& before, const local_append_statistics& after) {
    auto result = after;
    for (std::size_t i = 0; i != result.portions.size(); ++i) {
        result.portions[i] -= before.portions[i];
        result.portion_bytes[i] -= before.portion_bytes[i];
    }
    result.held_portions -= before.held_portions;
    result.held_portion_bytes -= before.held_portion_bytes;
    for (std::size_t i = 0; i != result.runs.size(); ++i)
        result.runs[i] -= before.runs[i];
    result.barriers -= before.barriers;
    result.syncs -= before.syncs;
    result.sync_rounds -= before.sync_rounds;
    result.round_syncs -= before.round_syncs;
    return result;
}

// A segment of `generation` as the engine contract makes it, at the run's
// alignment. A synchronized run has its data file zero-written when it is
// made, past every request of the stream it takes and within its data
// limit, as a recycled file would be; a fresh one takes the writer's own
// window and extensions, as a new file does.
segment_creation creation(
  const bench_store& store, const plan& planned, std::uint64_t generation) {
    auto made = stores::lifecycle::creation(store.devices.front(), generation);
    // Placed where the shard's device table chooses.
    made.device = std::nullopt;
    made.descriptor.alignment = alignment(planned.alignment);
    made.descriptor.maximum_data_bytes = byte_count{planned.segment_bytes};
    if (planned.synchronous_write != 0)
        made.writer.synchronous_write_bytes = byte_count{
          planned.synchronous_write};
    if (planned.fresh) {
        const auto defaults = segment_writer_contract::with_defaults();
        made.writer.preallocation_bytes = defaults.preallocation_bytes;
        made.writer.preallocation_extension_bytes
          = defaults.preallocation_extension_bytes;
    } else if (planned.synchronized) {
        const std::uint64_t each = std::max<std::uint64_t>(
                                     planned.payload, 4_KiB)
                                   + 16_KiB;
        const auto wanted = (planned.per_target() * each + 2_MiB - 1) / 1_MiB
                            * 1_MiB;
        made.writer.preallocation_bytes = byte_count{
          std::min(wanted, made.descriptor.maximum_data_bytes.value())};
    }
    return made;
}

// The segment of `generation`, created through the table on the device the
// shard's device table chooses.
seastar::future<runtime::result<local_append_target>>
create(bench_store& store, const plan& planned, std::uint64_t generation) {
    auto* table = store.engine->table();
    require(table != nullptr, "the shard is not ready");
    codec::cooperative_work work{store.config.policy, store.abort};
    co_return co_await table->create(
      creation(store, planned, generation), work);
}

// The request at `logical` to the stream's segment of `generation`: the
// small fixture child, or one record of the run's size. Each segment's
// requests are one producer stream whose sequence follows the offset.
seastar::future<local_append_request> stream_request(
  bench_store& store,
  const plan& planned,
  std::uint64_t generation,
  std::uint64_t logical,
  codec::cooperative_work& work) {
    if (planned.payload == 0)
        co_return co_await appends::request(
          appends::child_wire(logical, generation), store.appends, work);
    const fixture::shape sized{
      .name = "sized", .payload_bytes = planned.payload, .exact = true};
    auto batch = co_await fixture::make_batch(
      sized,
      appends::context(generation),
      model::range_logical_end{logical},
      logical - (first_logical - 1),
      work);
    auto backing = take(store.appends.try_reserve_buffer(batch.bytes()));
    co_return local_append_request{
      std::move(batch),
      std::move(backing),
      batch_expected(),
      appends::routing(),
      appends::request_time,
      std::nullopt};
}

// Joins one request's two stages, its result also after a refusal. It must
// have been accepted and have ended durable with a receipt.
seastar::future<> settle(
  seastar::future<local_append_acceptance> accepted,
  seastar::future<local_append_outcome> result) {
    std::exception_ptr refused;
    std::optional<local_append_acceptance> decided;
    try {
        decided.emplace(co_await std::move(accepted));
    } catch (...) {
        refused = std::current_exception();
    }
    const auto outcome = co_await std::move(result);
    if (refused) std::rethrow_exception(refused);
    take(decided->failure.outcome());
    require(decided->accepted(), "a filling request was refused");
    take(outcome.failure.outcome());
    require(
      outcome.status == local_append_status::durable
        && outcome.receipt.has_value(),
      "a filling request did not become locally durable");
}

// Fills a segment with `fill_requests` children of `fill_payload` bytes,
// `fill_burst` offered together, each with a work account of its own kept
// until its acceptance is ready. Answers the bytes of the children.
seastar::future<std::uint64_t>
fill(bench_store& store, local_append_target target, std::uint64_t generation) {
    auto* owner = store.engine->append();
    require(owner != nullptr, "the shard is not ready");
    const fixture::shape filled{
      .name = "filled", .payload_bytes = fill_payload};
    const auto context = appends::context(generation);
    codec::cooperative_work making{store.config.policy, store.abort};
    std::uint64_t logical = first_logical, bytes = 0;
    for (std::uint32_t done = 0; done != fill_requests; done += fill_burst) {
        std::vector<local_append_request> requests;
        requests.reserve(fill_burst);
        for (std::uint32_t k = 0; k != fill_burst; ++k) {
            // One producer stream: its sequence follows the offset, as the
            // small requests' does.
            auto batch = co_await fixture::make_batch(
              filled,
              context,
              model::range_logical_end{logical + k},
              logical + k - (first_logical - 1),
              making);
            bytes += batch.bytes().size().value();
            auto backing = take(
              store.appends.try_reserve_buffer(batch.bytes()));
            requests.push_back(
              local_append_request{
                std::move(batch),
                std::move(backing),
                batch_expected(),
                appends::routing(),
                appends::request_time,
                std::nullopt});
        }
        runtime::first_failure failed;
        std::vector<seastar::future<>> settling;
        settling.reserve(fill_burst);
        try {
            for (auto& request : requests) {
                auto work = std::make_unique<codec::cooperative_work>(
                  store.config.policy, store.abort);
                auto stages = owner->append(target, std::move(request), *work);
                settling.push_back(settle(
                  std::move(stages.accepted)
                    .finally([kept = std::move(work)] noexcept {}),
                  std::move(stages.result)));
            }
        } catch (...) {
            failed.observe(std::current_exception());
        }
        for (auto& one : settling) {
            try {
                co_await std::move(one);
            } catch (...) {
                failed.observe(std::current_exception());
            }
        }
        take(failed.outcome());
        logical += fill_burst;
    }
    co_return bytes;
}

// Starts what runs beside the stream, from the planned instant of request
// `beside_at`, and keeps its answer. Whether it succeeded is judged once the
// interval is over.
seastar::future<> run_beside(bench_store& store, beside_state& state) {
    auto* table = store.engine->table();
    require(table != nullptr, "the shard is not ready");
    state.began = true;
    state.begin = measurement_time();
    switch (state.kind) {
    case beside_kind::seal:
        state.sealed.emplace(
          co_await table->seal(appends::context(state.generation)));
        break;
    case beside_kind::removal:
        state.removed.emplace(
          co_await table->remove(*state.authorization, state.expected.get()));
        break;
    case beside_kind::creation:
        state.made.emplace(co_await stores::create(store, state.generation));
        break;
    case beside_kind::none:
        break;
    }
    state.end = measurement_time();
    state.ended = true;
}

// The timed stream. Request i is planned at epoch + i * spacing, for the
// target i % 2; the loop sleeps to that instant when it is early and offers
// at once when it is late, and holds the arrival back while as many
// requests as one WAL group holds await their result, as the composed paced
// arrival does. Each request is made from a child wire just before its
// planned instant, with a work account of its own that it keeps until its
// acceptance is ready, so the driver's own work is in the interval: its
// allocations and tasks are not the owners' alone. Its making counts in a
// request's delay only when the stream runs late.
//
// Latency is from the planned instant to the result, and delay from the
// planned instant to the offer. Requests are offered in order and a
// barrier covers a prefix of its file, so a request was published by a
// barrier not seen before exactly when its cut differs from the last one
// of its kind: the WAL's, or its segment's. Either barrier may publish a
// request first, so the two kinds interleave. A barrier is counted once the
// requests before it have their results.
//
// A rolling stream owns its segments: once a request is refused because its
// segment must roll, the successor is made while the requests in flight to
// the full segment are decided, those refused are offered to it again in
// order, keeping their planned instants, and the full segment is sealed
// beside the stream.
seastar::future<> stream(
  bench_store& store,
  const case_shape& chosen,
  const plan& planned,
  std::vector<local_append_target>& targets,
  row& out) {
    auto* owner = store.engine->append();
    require(owner != nullptr, "the shard is not ready");
    const auto bound = planned.requests;
    const auto spacing = planned.spacing_ns;
    const bool beside = chosen.beside != beside_kind::none;
    std::deque<settled_request> results(bound);
    std::vector<std::uint64_t> next(planned.targets, first_logical + 1);
    // Each target's segment generation, the requests its full segment
    // refused, its requests in flight, and the seals of full segments.
    std::vector<std::uint64_t> generation(planned.targets);
    for (std::uint32_t t = 0; t != planned.targets; ++t)
        generation[t] = t + 1;
    std::uint64_t successor = planned.targets + 2;
    std::vector<std::vector<std::size_t>> roll_refused(planned.targets);
    // Each target's first request its full segment refused: the segment's
    // generation and the request's offset.
    std::vector<std::optional<std::pair<std::uint64_t, std::uint64_t>>> full(
      planned.targets);
    std::vector<std::size_t> in_flight(planned.targets, 0);
    std::vector<seastar::future<>> sealing;
    std::deque<seastar::future<>> running;
    runtime::first_failure failed;
    std::size_t outstanding = 0, offered = 0, frontier = 0;
    bool overlap_open = false;
    seastar::condition_variable drained;
    std::optional<seastar::future<>> beside_done;

    const auto epoch = measurement_time() + lead_ns;
    out.epoch = epoch;
    out.latency.emplace(epoch, interval_ns, bound);
    out.delay.emplace(epoch, interval_ns, bound);
    if (beside) out.overlap.emplace(epoch, interval_ns, bound);

    std::optional<local_wal_cursor> last_wal;
    std::vector<std::pair<segment_context, runtime::file_position>>
      last_segment;
    const auto unseen = [&](const publishing_cut& cut) {
        if (const auto* wal = std::get_if<local_wal_cursor>(&cut)) {
            if (last_wal && *last_wal == *wal) return false;
            last_wal = *wal;
            return true;
        }
        const auto& segment = std::get<1>(cut);
        for (auto& last : last_segment)
            if (last.first == segment.first) {
                if (last.second == segment.second) return false;
                last.second = segment.second;
                return true;
            }
        last_segment.push_back(segment);
        return true;
    };
    const auto advance = [&] {
        while (frontier != offered && results[frontier].cut) {
            const auto& at = results[frontier];
            if (unseen(*at.cut)) {
                ++out.groups;
                if (overlap_open) ++out.overlap_groups;
                overlap_open = false;
            }
            overlap_open = overlap_open || at.overlapped;
            ++frontier;
        }
    };
    // Never throws: a failure is kept for the end of the stream.
    const auto observe =
      [&](
        std::size_t i,
        std::size_t t,
        std::uint64_t made_for,
        std::uint64_t logical,
        seastar::future<local_append_acceptance> accepted,
        seastar::future<local_append_outcome> result) -> seastar::future<> {
        try {
            std::exception_ptr refused;
            std::optional<local_append_acceptance> decided;
            try {
                decided.emplace(co_await std::move(accepted));
            } catch (...) {
                refused = std::current_exception();
            }
            // Acceptances are decided in order, so a refusal for a roll is
            // marked before any later request's acceptance is looked at.
            if (
              chosen.rolling && decided && !decided->failure.failed()
              && decided->capacity == segment_capacity_decision::roll_required
              && (!full[t] || full[t]->first != made_for))
                full[t].emplace(made_for, logical);
            const auto outcome = co_await std::move(result);
            const auto final = measurement_time();
            if (refused) std::rethrow_exception(refused);
            // Not accepted at or behind the full segment's first refusal:
            // that one for the roll, the later ones because their offsets no
            // longer continue the segment. Every one goes to the successor
            // in order, as a writer resends what a sealed segment left
            // unacknowledged.
            const auto& error = decided->failure.error();
            const bool unaccepted
              = error ? error->code() == errc::wrong_context
                      : !decided->failure.failed()
                          && decided->capacity
                               == segment_capacity_decision::roll_required;
            if (
              chosen.rolling && unaccepted && full[t]
              && full[t]->first == made_for && logical >= full[t]->second)
                roll_refused[t].push_back(i);
            else {
                take(decided->failure.outcome());
                require(
                  decided->accepted(), "a request of the stream was refused");
                take(outcome.failure.outcome());
                require(
                  outcome.status == local_append_status::durable
                    && outcome.receipt.has_value(),
                  "a request of the stream did not become locally durable");
                require(
                  outcome.receipt->block.records.logical().begin().value()
                    == logical,
                  "a durable result names another offset than its request's");
                const auto planned_at = epoch + i * spacing;
                const auto latency = final > planned_at ? final - planned_at
                                                        : 0;
                out.latency->record(final, latency);
                // What began before this result overlaps it unless it ended
                // before the request was planned.
                const bool overlapped
                  = beside && out.beside.began
                    && (!out.beside.ended || planned_at <= out.beside.end);
                if (overlapped) {
                    ++out.overlapping;
                    out.overlap->record(final, latency);
                }
                results[i].overlapped = overlapped;
                const auto& receipt = *outcome.receipt;
                if (receipt.wal)
                    results[i].cut.emplace(receipt.wal->boundary().cursor());
                else
                    results[i].cut.emplace(
                      std::pair{
                        receipt.segment->boundary().context(),
                        receipt.segment->boundary().end().bytes});
                ++out.completed;
                advance();
            }
        } catch (...) {
            failed.observe(std::current_exception());
        }
        --outstanding;
        --in_flight[t];
        drained.signal();
    };
    // Seals a full segment of the stream beside it, as its owner would.
    const auto seal_full = [&store](std::uint64_t full) -> seastar::future<> {
        auto* table = store.engine->table();
        require(table != nullptr, "the shard is not ready");
        auto sealed = co_await table->seal(appends::context(full));
        take(sealed.failure.outcome());
        require(
          sealed.extent && sealed.expected,
          "a full segment of the stream was not sealed");
    };
    // Rolls target `t` while requests are refused for it: the successor is
    // made while the requests in flight to the full segment are decided,
    // then those refused are offered to it in order.
    const auto roll = [&](std::size_t t) -> seastar::future<> {
        while (!roll_refused[t].empty()) {
            const auto began = measurement_time();
            const auto made = successor++;
            auto making = create(store, planned, made);
            while (in_flight[t] != 0)
                co_await drained.wait();
            auto target = take(co_await std::move(making));
            const auto took = measurement_time() - began;
            ++out.rolls;
            out.roll_creation_ns += took;
            out.roll_creation_longest_ns = std::max(
              out.roll_creation_longest_ns, took);
            sealing.push_back(seal_full(generation[t]));
            targets[t] = target;
            generation[t] = made;
            next[t] = first_logical;
            auto waiting = std::exchange(roll_refused[t], {});
            std::sort(waiting.begin(), waiting.end());
            for (const auto i : waiting) {
                const auto logical = next[t]++;
                auto work = std::make_unique<codec::cooperative_work>(
                  store.config.policy, store.abort);
                auto request = co_await stream_request(
                  store, planned, generation[t], logical, *work);
                while (outstanding >= maximum_wal_group_members)
                    co_await drained.wait();
                auto stages = owner->append(
                  targets[t], std::move(request), *work);
                ++outstanding;
                ++in_flight[t];
                running.push_back(observe(
                  i,
                  t,
                  generation[t],
                  logical,
                  std::move(stages.accepted)
                    .finally([kept = std::move(work)] noexcept {}),
                  std::move(stages.result)));
            }
        }
    };

    if (const auto now = measurement_time(); epoch > now)
        co_await seastar::sleep<std::chrono::steady_clock>(
          std::chrono::nanoseconds{static_cast<std::int64_t>(epoch - now)});
    const auto counted = owner->statistics();
    std::optional<block_io> device_before;
    if (planned.block) device_before = block_io::read(planned.block);
    {
        interval_scope interval{store.files, out.interval, epoch};
        try {
            for (std::size_t i = 0; i != bound && !failed.failed(); ++i) {
                if (planned.groups && out.groups >= *planned.groups) break;
                const auto t = i % planned.targets;
                if (chosen.rolling) co_await roll(t);
                const auto logical = next[t]++;
                auto work = std::make_unique<codec::cooperative_work>(
                  store.config.policy, store.abort);
                auto request = co_await stream_request(
                  store, planned, generation[t], logical, *work);
                const auto due = epoch + i * spacing;
                if (const auto now = measurement_time(); due > now)
                    co_await seastar::sleep<std::chrono::steady_clock>(
                      std::chrono::nanoseconds{
                        static_cast<std::int64_t>(due - now)});
                if (beside && i == beside_at)
                    beside_done.emplace(run_beside(store, out.beside));
                while (outstanding >= maximum_wal_group_members)
                    co_await drained.wait();
                const auto started = measurement_time();
                out.delay->record(started, started > due ? started - due : 0);
                auto stages = owner->append(
                  targets[t], std::move(request), *work);
                ++outstanding;
                ++in_flight[t];
                offered = i + 1;
                running.push_back(observe(
                  i,
                  t,
                  generation[t],
                  logical,
                  std::move(stages.accepted)
                    .finally([kept = std::move(work)] noexcept {}),
                  std::move(stages.result)));
            }
            // What full segments refused last is offered once every request
            // in flight is decided.
            while (chosen.rolling && !failed.failed()) {
                while (outstanding != 0)
                    co_await drained.wait();
                bool rolled = false;
                for (std::size_t t = 0; t != planned.targets; ++t)
                    if (!roll_refused[t].empty()) {
                        co_await roll(t);
                        rolled = true;
                    }
                if (!rolled) break;
            }
        } catch (...) {
            failed.observe(std::current_exception());
        }
        for (auto& waiting : running)
            co_await std::move(waiting);
        running.clear();
        for (auto& seal : sealing) {
            try {
                co_await std::move(seal);
            } catch (...) {
                failed.observe(std::current_exception());
            }
        }
        sealing.clear();
        if (beside_done) {
            try {
                co_await std::move(*beside_done);
            } catch (...) {
                failed.observe(std::current_exception());
            }
        }
    }
    if (planned.block)
        out.interval.block
          = block_io::read(planned.block).since(*device_before);
    take(failed.outcome());
    out.placed = grown(counted, owner->statistics());
    out.latency->finish();
    out.delay->finish();
    if (out.overlap) out.overlap->finish();
    if (overlap_open) ++out.overlap_groups;
    out.requests = offered;
    require(
      out.completed == offered && frontier == offered && offered != 0,
      "a request of the stream has no result");
    require(
      !beside || offered > beside_at,
      "the stream ended before what runs beside it began");
    // Each barrier claimed reached the device: the shard issued a flush, and
    // at least every one its owners made, or a zero-written file took a
    // synchronized write, which needs none. One flush covers every write
    // completed before it began, so a barrier whose writes an earlier flush
    // covered takes none: the barriers the requests took do not bound the
    // flushes from below. A synchronized run's writes larger than the
    // synchronized handle takes still flush.
    std::uint64_t issued = 0, synchronized = 0;
    if constexpr (!timing_only) {
        const auto& wal = store.files.sample[measured::file_kind::wal];
        const auto& data = store.files.sample[measured::file_kind::data];
        issued = wal.flushes + data.flushes;
        synchronized = wal.synchronized_calls + data.synchronized_calls;
    }
    require(
      (wal_bench_support::barrier_evident(
         wal_bench_support::barrier_kind::extending, out.interval.io, 0)
       || (planned.synchronized
           && wal_bench_support::barrier_evident(
             wal_bench_support::barrier_kind::synchronized,
             out.interval.io,
             synchronized)))
        && out.interval.io.flushes >= issued,
      "claimed barrier reached no device");
}

// One case on a store that was never used: the engine started, a share
// handed, the stream's segments created where the device table places
// them, the third made ready, one request to each stream segment so that
// its first group's one-time work is not timed, and the stream.
seastar::future<> run_in(
  bench_store& store, const case_shape& chosen, const plan& planned, row& out) {
    const auto began = co_await stores::start(store, {});
    take(began.failure.outcome());
    require(
      began.state == engine_state::ready && !began.restarted,
      "a store that was never used did not start ready");
    auto* placement = store.engine->placement();
    auto* table = store.engine->table();
    require(placement && table, "the shard is not ready");
    out.wal_capacity = store.config.wal.capacity_bytes.value();
    // Each pool's share holds the whole bound of every segment the case
    // makes, as a filesystem with room for them would, and is at least the
    // engine contract's.
    const auto sample = creation(store, planned, 1);
    out.synchronous_write = sample.writer.synchronous_write_bytes.value();
    const auto bound = take(
      segment_type::disk_bound(sample.descriptor, sample.writer));
    // A rolling stream leaves every full segment it makes on its device.
    std::uint64_t made = chosen.writable(planned.targets);
    if (chosen.rolling) {
        const auto unit = planned.alignment;
        const auto each = (std::max<std::uint64_t>(planned.payload, 1_KiB)
                           + unit - 1)
                            / unit * unit
                          + 2 * unit;
        const auto per = std::max<std::uint64_t>(
          (planned.segment_bytes - planned.segment_bytes / 16) / each, 1);
        made += planned.requests / per + planned.targets;
    }
    out.share = std::max(pool_share, made * bound.value());
    stores::share(store, byte_count{out.share});
    for (const auto& spec : store.devices)
        out.devices.push_back(
          {spec.owner.device(),
           spec.directory.device,
           placement->pool(spec.owner.device()),
           spec.identity.role});
    std::vector<local_append_target> targets;
    targets.reserve(planned.targets);
    for (std::uint32_t t = 0; t != planned.targets; ++t) {
        // The device it is placed on is the one whose count of writable
        // segments grew.
        std::vector<std::uint32_t> before;
        before.reserve(out.devices.size());
        for (const auto& device : out.devices)
            before.push_back(placement->writable(device.store));
        targets.push_back(take(co_await create(store, planned, t + 1)));
        std::optional<std::size_t> placed;
        for (std::size_t d = 0; d != out.devices.size(); ++d)
            if (placement->writable(out.devices[d].store) == before[d] + 1)
                placed = d;
        require(placed.has_value(), "a created segment is on no device");
        out.target_devices.push_back(*placed);
    }
    if (chosen.data_devices == 2)
        require(
          out.target_devices[0] != out.target_devices[1],
          "the device table put both segments on one device");
    out.beside.kind = chosen.beside;
    out.beside.generation = planned.targets + 1U;
    const auto beside = out.beside.generation;
    if (
      chosen.beside == beside_kind::seal
      || chosen.beside == beside_kind::removal) {
        const auto third = take(co_await create(store, planned, beside));
        out.beside.fill_bytes = co_await fill(store, third, beside);
    }
    if (chosen.beside == beside_kind::removal) {
        // Sealed before the interval, and authorized with its sealed
        // extent, as its owner of segments would.
        auto sealed = co_await table->seal(appends::context(beside));
        take(sealed.failure.outcome());
        require(
          sealed.extent && sealed.expected,
          "the segment to remove was not sealed");
        out.beside.expected = std::move(sealed.expected);
        out.beside.authorization = deletion_authorization{
          appends::context(beside), out.beside.expected->extent};
    }
    for (std::uint32_t t = 0; t != planned.targets; ++t) {
        codec::cooperative_work work{store.config.policy, store.abort};
        auto request = co_await stream_request(
          store, planned, t + 1, first_logical, work);
        auto stages = store.engine->append()->append(
          targets[t], std::move(request), work);
        static_cast<void>(co_await std::move(stages.accepted));
        const auto warmed = co_await std::move(stages.result);
        take(warmed.failure.outcome());
        require(
          warmed.status == local_append_status::durable
            && warmed.receipt.has_value(),
          "a first request did not become locally durable");
    }
    if (chosen.exhausted) {
        stores::share(store, byte_count{0});
        out.share = 0;
        out.beside.table_size = table->size();
    }
    if (planned.settle_ms != 0)
        co_await seastar::sleep(std::chrono::milliseconds{planned.settle_ms});
    co_await stream(store, chosen, planned, targets, out);
    if constexpr (!timing_only) out.observed = store.files.sample;

    switch (chosen.beside) {
    case beside_kind::seal:
    case beside_kind::removal: {
        if (chosen.beside == beside_kind::seal) {
            require(
              out.beside.sealed.has_value(), "the seal beside never ended");
            const auto& sealed = *out.beside.sealed;
            take(sealed.failure.outcome());
            require(
              sealed.extent && sealed.expected,
              "the seal beside the stream did not seal");
            out.beside.expected = sealed.expected;
        } else {
            require(
              out.beside.removed.has_value(), "the removal beside never ended");
            const auto& removed = *out.beside.removed;
            take(removed.failure.outcome());
            require(
              removed.verdict == authorization_verdict::authorized
                && (removed.gone || removed.settling),
              "the removal beside the stream did not remove");
        }
        const auto& extent = out.beside.expected->extent;
        out.beside.sealed_end = extent.file_end.value();
        out.beside.sealed_records = extent.coverage.logical().end().value()
                                    - extent.coverage.logical().begin().value();
        break;
    }
    case beside_kind::creation: {
        require(out.beside.made.has_value(), "the creation beside never ended");
        const auto& made = *out.beside.made;
        require(
          !made && made.error().code() == errc::resource_exhausted,
          "a creation was not refused for space");
        require(
          table->size() == out.beside.table_size
            && !table->life(appends::context(beside)),
          "a creation refused for space left something");
        break;
    }
    case beside_kind::none:
        break;
    }
    // Nothing of the store is held past its stop.
    out.beside.expected = nullptr;
    out.beside.sealed.reset();
    out.beside.authorization.reset();
}

// Runs `function(roots)` with a fresh directory made from each template, and
// removes them afterwards, whatever it did.
template<typename Func>
seastar::future<>
with_directories(std::vector<std::filesystem::path> templates, Func function) {
    std::vector<seastar::tmp_dir> made;
    made.reserve(templates.size());
    std::exception_ptr failure;
    try {
        for (auto& path : templates)
            made.push_back(co_await seastar::make_tmp_dir(std::move(path)));
        std::vector<std::filesystem::path> roots;
        roots.reserve(made.size());
        for (const auto& directory : made)
            roots.push_back(directory.get_path());
        co_await function(std::span<const std::filesystem::path>{roots});
    } catch (...) {
        failure = std::current_exception();
    }
    for (auto directory = made.rbegin(); directory != made.rend();
         ++directory) {
        try {
            co_await directory->remove();
        } catch (...) {
            if (!failure) failure = std::current_exception();
        }
    }
    if (failure) std::rethrow_exception(failure);
}

// The control directory under the test directory; each data device under its
// own directory knob, or the test directory without it.
std::vector<std::filesystem::path>
templates(const case_shape& chosen, const plan& planned) {
    const auto under = [](const char* directory) {
        return directory ? std::filesystem::path{directory}
                             / "kwaque-local-append-XXXXXX"
                         : runtime::testing::test_directory_template();
    };
    std::vector<std::filesystem::path> result;
    result.push_back(runtime::testing::test_directory_template());
    result.push_back(under(planned.data));
    if (chosen.data_devices == 2) result.push_back(under(planned.second));
    return result;
}

const char* role_name(local_device_role role) noexcept {
    switch (role) {
    case local_device_role::data:
        return "data";
    case local_device_role::wal_control:
        return "wal_control";
    case local_device_role::all:
        return "all";
    }
    return "invalid";
}
const char* verdict_name(authorization_verdict verdict) noexcept {
    switch (verdict) {
    case authorization_verdict::authorized:
        return "authorized";
    case authorization_verdict::repeated:
        return "repeated";
    case authorization_verdict::wrong_context:
        return "wrong_context";
    case authorization_verdict::wrong_identity:
        return "wrong_identity";
    case authorization_verdict::sealed_here:
        return "sealed_here";
    case authorization_verdict::attached:
        return "attached";
    case authorization_verdict::unknown:
        return "unknown";
    }
    return "invalid";
}
std::array<char, 33> hex(const device_store_id& id) {
    std::array<char, 33> text{};
    constexpr char digits[] = "0123456789abcdef";
    const auto bytes = id.bytes();
    for (std::size_t i = 0; i != bytes.size(); ++i) {
        text[2 * i] = digits[bytes[i] >> 4U];
        text[2 * i + 1] = digits[bytes[i] & 15U];
    }
    return text;
}

void number(const char* name, std::uint64_t value) {
    std::printf(",\"%s\":%" PRIu64, name, value);
}
void flag(const char* name, bool set) {
    std::printf(",\"%s\":%s", name, set ? "true" : "false");
}
// Only the observing build reports native work by file kind.
[[maybe_unused]] void
print_kind(const char* name, const measured::kind_sample& sample) {
    std::printf(
      ",\"%s\":{\"writes\":%" PRIu64 ",\"write_bytes\":%" PRIu64
      ",\"write_service_ns\":%" PRIu64 ",\"write_busy_ns\":%" PRIu64
      ",\"flushes\":%" PRIu64 ",\"flush_service_ns\":%" PRIu64
      ",\"opens\":%" PRIu64 ",\"reads\":%" PRIu64 ",\"read_bytes\":%" PRIu64
      ",\"unlinks\":%" PRIu64 ",\"unlink_ns\":%" PRIu64
      ",\"unlink_longest_ns\":%" PRIu64 ",\"synchronized_calls\":%" PRIu64 "}",
      name,
      sample.calls,
      sample.bytes,
      sample.write_service_ns,
      sample.write_busy_ns,
      sample.flushes,
      sample.flush_service_ns,
      sample.opens,
      sample.reads,
      sample.read_bytes,
      sample.unlinks,
      sample.unlink_ns,
      sample.unlink_longest_ns,
      sample.synchronized_calls);
}

void print_row(
  const case_shape& chosen,
  const plan& planned,
  const row& out,
  const std::set<unsigned>& affinity) {
    std::printf(
      "engine_append_v1 {\"case\":\"%s\",\"beside\":\"%s\","
      "\"fdatasync\":\"%s\"",
      chosen.name,
      beside_name(chosen.beside),
      seastar::engine().have_aio_fdatasync() ? "aio" : "thread");
    number("requests", out.requests);
    number("completed", out.completed);
    number("request_bound", planned.requests);
    if (planned.groups)
        number("group_target", *planned.groups);
    else
        std::printf(",\"group_target\":null");
    number("spacing_ns", planned.spacing_ns);
    number("targets", planned.targets);
    number("payload_bytes", planned.payload);
    number("placement_bytes", planned.placement.value());
    number("hold_bytes", planned.hold.value());
    number("groups", out.groups);
    // Unless the run is synchronized, neither the WAL nor a segment's data
    // file is preallocated: each barrier follows writes that grew its file.
    const auto barrier = planned.synchronized
                           ? wal_bench_support::barrier_kind::synchronized
                           : wal_bench_support::barrier_kind::extending;
    std::printf(
      ",\"wal_barrier\":\"%s\",\"data_barrier\":\"%s\"",
      wal_bench_support::barrier_name(barrier),
      wal_bench_support::barrier_name(barrier));
    number("wal_capacity_bytes", out.wal_capacity);
    std::printf(",\"devices\":[");
    std::optional<std::uint64_t> filesystem;
    bool same_filesystem = true;
    for (std::size_t d = 0; d != out.devices.size(); ++d) {
        const auto& device = out.devices[d];
        std::printf(
          "%s{\"store\":\"%s\",\"st_dev\":%" PRIu64 ",\"pool\":",
          d ? "," : "",
          hex(device.store).data(),
          device.filesystem);
        if (device.pool)
            std::printf("%zu", *device.pool);
        else
            std::printf("null");
        std::printf(",\"role\":\"%s\"}", role_name(device.role));
        if (device.role == local_device_role::wal_control) continue;
        if (filesystem && *filesystem != device.filesystem)
            same_filesystem = false;
        filesystem = device.filesystem;
    }
    std::printf("],\"target_devices\":[");
    for (std::size_t t = 0; t != out.target_devices.size(); ++t)
        std::printf("%s%zu", t ? "," : "", out.target_devices[t]);
    std::printf("]");
    flag("same_filesystem", same_filesystem);
    number("share_bytes", out.share);
    number("elapsed_ns", out.interval.elapsed);
    number("reactor_cpu_ns", out.interval.cpu);
    number("allocations", out.interval.allocations);
    number("tasks", out.interval.tasks);
    number("runtime_operations", out.interval.operations);
    number("reactor_flushes", out.interval.io.flushes);
    number("reactor_writes", out.interval.io.writes);
    number("reactor_write_bytes", out.interval.io.write_bytes);
    number("reactor_reads", out.interval.io.reads);
    number("reactor_read_bytes", out.interval.io.read_bytes);
    number("alignment_bytes", planned.alignment);
    flag("fresh_segments", planned.fresh);
    number("segment_bytes", planned.segment_bytes);
    number("synchronous_write_bytes", out.synchronous_write);
    number("settle_ms", planned.settle_ms);
    number("rolls", out.rolls);
    number("roll_creation_ns", out.roll_creation_ns);
    number("roll_creation_longest_ns", out.roll_creation_longest_ns);
    if (const auto& block = out.interval.block)
        std::printf(
          ",\"block\":{\"device\":\"%s\",\"reads\":%" PRIu64
          ",\"read_bytes\":%" PRIu64 ",\"writes\":%" PRIu64
          ",\"write_bytes\":%" PRIu64 ",\"flushes\":%" PRIu64 "}",
          planned.block,
          block->reads,
          block->read_bytes,
          block->writes,
          block->write_bytes,
          block->flushes);
    else
        std::printf(",\"block\":null");
    if constexpr (!timing_only) {
        using measured::file_kind;
        print_kind("wal", out.observed[file_kind::wal]);
        print_kind("data", out.observed[file_kind::data]);
        print_kind("other", out.observed[file_kind::other]);
        std::printf(
          ",\"namespace\":{\"directory_opens\":%" PRIu64
          ",\"directory_syncs\":%" PRIu64 ",\"listings\":%" PRIu64
          ",\"stats\":%" PRIu64 ",\"renames\":%" PRIu64
          ",\"directory_removes\":%" PRIu64 "}",
          out.observed.directory_opens,
          out.observed.directory_syncs,
          out.observed.listings,
          out.observed.stats,
          out.observed.renames,
          out.observed.directory_removes);
    }
    // What the append owner placed, held and synced in the interval, and
    // what stood at its end.
    const auto& placed = out.placed;
    constexpr auto wal = static_cast<std::size_t>(local_append_placement::wal);
    constexpr auto in_segment = static_cast<std::size_t>(
      local_append_placement::segment);
    const auto runs = [&placed](segment_run_close why) {
        return placed.runs[static_cast<std::size_t>(why)];
    };
    number("wal_placed_portions", placed.portions[wal]);
    number("segment_placed_portions", placed.portions[in_segment]);
    number("wal_placed_bytes", placed.portion_bytes[wal]);
    number("segment_placed_bytes", placed.portion_bytes[in_segment]);
    number("held_portions", placed.held_portions);
    number("held_portion_bytes", placed.held_portion_bytes);
    number("runs_filled", runs(segment_run_close::filled));
    number("runs_wanted", runs(segment_run_close::wanted));
    number("runs_limited", runs(segment_run_close::limited));
    number("runs_synced", runs(segment_run_close::synced));
    number("runs_pressed", runs(segment_run_close::pressed));
    number("segment_barriers", placed.barriers);
    number("segment_syncs", placed.syncs);
    number("sync_rounds", placed.sync_rounds);
    number("round_syncs", placed.round_syncs);
    number("held_bytes", placed.held_bytes);
    number("held_runs", placed.held_runs);
    number("unsynced_bytes", placed.unsynced_bytes);
    number("unsynced_wal_files", placed.unsynced_files);
    const auto& beside = out.beside;
    if (chosen.beside != beside_kind::none) {
        // Its instants are from the stream's first planned instant.
        const auto since = [&out](std::uint64_t at) {
            return at > out.epoch ? at - out.epoch : 0;
        };
        number("beside_at", beside_at);
        number("beside_begin_ns", since(beside.begin));
        number("beside_end_ns", since(beside.end));
        number("overlapping", out.overlapping);
        number("overlap_groups", out.overlap_groups);
    }
    switch (chosen.beside) {
    case beside_kind::seal:
        std::printf(",\"beside_outcome\":\"sealed\"");
        number("beside_fill_bytes", beside.fill_bytes);
        number("sealed_file_end", beside.sealed_end);
        number("sealed_records", beside.sealed_records);
        break;
    case beside_kind::removal: {
        const auto& removed = *beside.removed;
        std::printf(
          ",\"beside_outcome\":\"%s\",\"verdict\":\"%s\"",
          removed.gone ? "gone" : "settling",
          verdict_name(removed.verdict));
        number("beside_fill_bytes", beside.fill_bytes);
        number("sealed_file_end", beside.sealed_end);
        number("sealed_records", beside.sealed_records);
        flag("gone", removed.gone);
        flag("settling", removed.settling);
        number("removed", removed.removed);
        number("foreign", removed.foreign);
        break;
    }
    case beside_kind::creation:
        std::printf(",\"beside_outcome\":\"resource_exhausted\"");
        number("table_size", beside.table_size);
        break;
    case beside_kind::none:
        break;
    }
    if (out.overlap) out.overlap->print("overlap");
    out.latency->print("latency");
    out.delay->print("delay");
    flag("timing_only", timing_only);
#ifdef NDEBUG
    flag("ndebug", true);
#else
    flag("ndebug", false);
#endif
    flag(
      "libcxx_hardening_none",
      _LIBCPP_HARDENING_MODE == _LIBCPP_HARDENING_MODE_NONE);
    flag("complete", true);
    std::printf(",\"affinity_cpus\":[");
    bool first = true;
    for (auto cpu : affinity) {
        std::printf("%s%u", first ? "" : ",", cpu);
        first = false;
    }
    std::puts("]}");
}

struct engine_append {
    engine_append() {
        static const bool initialized = [] {
            codec::testing::report_profile();
            return true;
        }();
        static_cast<void>(initialized);
    }

    static seastar::future<std::size_t> run_case(case_shape chosen) {
        const auto planned = plan::of(chosen);
        const auto affinity = seastar::get_current_cpuset();
        row out;
        co_await stores::with_bench_resources(
          [&](
            resource::resource_manager& manager,
            runtime::production::timer& timer) -> seastar::future<> {
              co_await with_directories(
                templates(chosen, planned),
                [&](std::span<const std::filesystem::path> roots)
                  -> seastar::future<> {
                    stores::store_layout layout{roots[0] / "store", {}};
                    for (std::size_t i = 1; i != roots.size(); ++i)
                        layout.data.push_back(roots[i] / "store");
                    co_await stores::with_bench_store(
                      manager,
                      timer,
                      std::move(layout),
                      !timing_only,
                      configuration(chosen, planned),
                      [&](bench_store& store) -> seastar::future<> {
                          co_await run_in(store, chosen, planned, out);
                      });
                });
          });
        require(
          seastar::get_current_cpuset() == affinity,
          "benchmark CPU affinity changed during execution");
        print_row(chosen, planned, out, affinity);
        co_return out.requests;
    }
};

PERF_TEST_F(engine_append, quiet) { return run_case({.name = "quiet"}); }
// The product's segments, written synchronized inside their windows, which
// only the counting build counts.
#if !defined(KWAQUE_LOCAL_STORAGE_TIMING_ONLY)
PERF_TEST_F(engine_append, rolling) {
    return run_case({.name = "rolling", .rolling = true});
}
#endif
PERF_TEST_F(engine_append, during_seal) {
    return run_case({.name = "during_seal", .beside = beside_kind::seal});
}
PERF_TEST_F(engine_append, during_removal) {
    return run_case({.name = "during_removal", .beside = beside_kind::removal});
}
PERF_TEST_F(engine_append, exhausted_share) {
    return run_case(
      {.name = "exhausted_share",
       .beside = beside_kind::creation,
       .exhausted = true});
}
PERF_TEST_F(engine_append, two_data_devices) {
    return run_case({.name = "two_data_devices", .data_devices = 2});
}
PERF_TEST_F(engine_append, fixed_rate) {
    return run_case({.name = "fixed_rate", .fixed_rate = true});
}
} // namespace
} // namespace kwaque::storage::testing::engine_append_bench
