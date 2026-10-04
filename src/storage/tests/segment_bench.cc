#include "src/base/units.h"
#include "src/bytes/fragmented_buffer_builder.h"
#include "src/codec/tests/benchmark_buffer.h"
#include "src/codec/tests/memory_qualification_support.h"
#include "src/codec/xxh3.h"
#include "src/resource/resource_registry.h"
#include "src/runtime/production/clocks.h"
#include "src/runtime/testing/test_directory.h"
#include "src/storage/tests/segment_bench_file.h"
#include "src/storage/tests/segment_bench_fixture.h"
#include "src/storage/tests/segment_bench_observer.h"
#include "src/storage/tests/segment_writer_contract.h"

#include <seastar/core/posix.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/thread_cputime_clock.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/testing/perf_tests.hh>
#include <seastar/util/later.hh>
#include <seastar/util/tmp_file.hh>

#include <array>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <set>
#include <string>

namespace kwaque::storage::testing::segment_bench_support {
namespace {
using bytes::testing::charge;
using store_contract::require;
using store_contract::take;
using writer_type = segment_writer<
  file_system,
  store_contract::ownership_input,
  runtime::production::monotonic_clock>;
#if defined(KWAQUE_SEGMENT_TIMING_ONLY)
constexpr bool timing_only = true;
#else
constexpr bool timing_only = false;
#endif
enum class operation {
    layout,
    encode,
    typed,
    raw,
    preencoded,
    writer,
    lifecycle
};
const char* scope(operation selected) noexcept {
    switch (selected) {
    case operation::layout:
        return "layout";
    case operation::encode:
        return "encode_evidence";
    case operation::typed:
        return "typed_evidence";
    case operation::raw:
        return "raw_evidence";
    case operation::preencoded:
        return "preencoded_barrier";
    case operation::writer:
        return "writer_append_barrier";
    case operation::lifecycle:
        return "lifecycle";
    }
    std::abort();
}
bool disk(operation selected) noexcept {
    return selected == operation::preencoded || selected == operation::writer
           || selected == operation::lifecycle;
}
struct native_driver final {
    template<typename T>
    seastar::future<T> lifecycle(seastar::future<T> value) const {
        return value;
    }
};
template<std::size_t Octets>
std::array<char, 2 * Octets + 1>
hex_digest(std::array<unsigned char, Octets> digest) {
    std::array<char, 2 * Octets + 1> result{};
    constexpr char digits[] = "0123456789abcdef";
    for (std::size_t i = 0; i < digest.size(); ++i) {
        result[2 * i] = digits[digest[i] >> 4U];
        result[2 * i + 1] = digits[digest[i] & 15U];
    }
    return result;
}
struct measurement final {
    std::uint64_t elapsed{0}, cpu{0}, allocations{0}, tasks{0},
      runtime_operations{0};
    std::uint64_t epoch{0}, foreground_turns{0}, peak_admission{0},
      admission_after{0};
    std::uint64_t barriers{0}, submitted{0}, sealed{0}, retry_pages{0},
      page_reads{0};
    std::uint64_t retained_results{0}, reopen_count{0}, setup_extent{0},
      layout_calls{0};
    // Preparations that met ordinary pressure and waited for the deferred
    // digest to release earlier groups' memory.
    std::uint64_t pressure_waits{0};
    std::uint64_t setup_allocated_bytes{0};
    // After the interval: the wait for the deferred extent digest to hash
    // every written byte. Receipts never wait for it.
    std::uint64_t digest_drain{0};
    codec::testing::allocation_observation memory;
    work_sample work;
};

// The benchmark root and supplied fixtures predate this interval. Report new
// native allocation bounds separately from retained fixture/admission charges.
class measure_scope final {
public:
    measure_scope(file_system& files, measurement& output, bool allocations)
      : files_(files)
      , output_(output)
      , observe_allocations_(allocations)
      , before_allocations_(seastar::memory::stats().mallocs())
      , before_tasks_(seastar::engine().get_sched_stats().tasks_processed)
      , before_operations_(files.statistics().accepted) {
        files_.sample.measuring = true;
        files_.sample.measure_service_time = !timing_only;
#if !defined(KWAQUE_SEGMENT_TIMING_ONLY)
        if (observe_allocations_)
            codec::testing::begin_allocation_observation();
        begin_work_observation(output_.work);
#endif
        cpu_ = seastar::thread_cputime_clock::now();
        output_.epoch = measurement_time();
        perf_tests::start_measuring_time();
    }
    ~measure_scope() {
        perf_tests::stop_measuring_time();
        output_.elapsed = measurement_time() - output_.epoch;
        output_.cpu = static_cast<std::uint64_t>(
          (seastar::thread_cputime_clock::now() - cpu_).count());
        files_.sample.measuring = false;
#if !defined(KWAQUE_SEGMENT_TIMING_ONLY)
        end_work_observation();
        if (observe_allocations_)
            output_.memory = codec::testing::end_allocation_observation();
#endif
        output_.allocations = seastar::memory::stats().mallocs()
                              - before_allocations_;
        output_.tasks = seastar::engine().get_sched_stats().tasks_processed
                        - before_tasks_;
        output_.runtime_operations = files_.statistics().accepted
                                     - before_operations_;
    }
    measure_scope(const measure_scope&) = delete;
    measure_scope& operator=(const measure_scope&) = delete;

private:
    file_system& files_;
    measurement& output_;
    [[maybe_unused]] bool observe_allocations_;
    std::uint64_t before_allocations_, before_tasks_, before_operations_;
    seastar::thread_cputime_clock::time_point cpu_;
};

runtime::file_path
data_path(const local_device_spec& spec, const extent_input& input) {
    return take(
      local_paths::make(spec.root)->segment_file(
        0,
        {input.descriptor.segment.segment(),
         input.descriptor.segment.generation()},
        local_segment_file::data));
}
segment_writer_config
writer_config(const shape& selected, byte_count preallocation = {}) {
    auto result = segment_writer_contract::configuration();
    result.preallocation_bytes = preallocation;
    result.policy = policy(selected);
    result.maximum_groups = selected.window;
    result.admission.working_bytes = selected.payload_bytes >= 4_MiB
                                       ? working_bytes
                                       : byte_count{1_MiB};
    return result;
}
seastar::future<std::unique_ptr<writer_type>> create_writer(
  file_system& files,
  store_contract::ownership_input& owner,
  const local_device_spec& spec,
  extent_input& input,
  workload_budget& resources,
  const shape& selected,
  codec::cooperative_work& work,
  byte_count preallocation = {}) {
    auto writer = take(
      writer_type::make_new(
        files,
        owner,
        spec,
        0,
        input.descriptor,
        resources,
        writer_config(selected, preallocation)));
    runtime::first_failure failed;
    try {
        // Header creation must initialize the same device/workload I/O class
        // that the dispatcher will use. Lifecycle callers time this setup;
        // append-only callers complete it before starting their interval.
        failed.observe(
          co_await seastar::with_scheduling_group(
            resources.scheduling_group(),
            [&writer, &work] { return writer->create_new(work); }));
    } catch (...) {
        failed.observe(std::current_exception());
    }
    if (failed.failed()) {
        try {
            failed.observe(co_await writer->close());
        } catch (...) {
            failed.observe(std::current_exception());
        }
        writer.reset();
        take(failed.outcome());
    }
    co_return writer;
}

// Freeze one group from all of its supplied batches: the writer places every
// block and one durable footer follows them, as one group commit would.
seastar::future<segment_frozen_group> freeze_children(
  writer_type& writer,
  std::vector<encoded_assigned_batch> batches,
  workload_budget& resources,
  codec::cooperative_work& work,
  measurement& observed) {
    std::optional<runtime::result<segment_group_preparation>> preparation;
    preparation.emplace(writer.prepare_group(std::span{batches}, work));
    // Ordinary pressure: a written group keeps its memory until the deferred
    // digest has hashed it. Wait for that, as a caller under pressure must,
    // and prepare once more.
    if (!*preparation && preparation->error().code() == errc::queue_full) {
        ++observed.pressure_waits;
        take(co_await writer.digest_caught_up());
        preparation.emplace(writer.prepare_group(std::span{batches}, work));
    }
    auto prepared = take(std::move(*preparation));
    require(
      prepared.prepared.has_value(),
      "writer admission rejected the benchmark group; reduce its batches");
    std::vector<admitted_wal_batch> children;
    children.reserve(batches.size());
    for (auto& batch : batches) {
        auto held = take(resources.try_reserve_buffer(batch.bytes()));
        children.push_back(take(
          admitted_wal_batch::make(std::move(batch), std::move(held), charge)));
    }
    co_return take(
      co_await writer.freeze_group(
        std::move(*prepared.prepared), std::move(children), work));
}

// One work account is used sequentially at the admission/encoding entrances.
// Each writer retains its own dispatcher work while writes overlap.
seastar::future<> append_groups(
  std::span<std::unique_ptr<writer_type>> writers,
  std::span<extent_input> inputs,
  const shape& selected,
  workload_budget& resources,
  codec::cooperative_work& work,
  measurement& observed,
  std::vector<segment_write_completion>& retained) {
    std::
      array<std::array<std::optional<segment_submission>, 8>, maximum_segments>
        pending;
    std::array<std::optional<segment_captured_boundary>, maximum_segments> cuts;
    runtime::first_failure failed;
    try {
        for (std::uint32_t first = 0; first < selected.groups;
             first += selected.window) {
            const auto count = std::min(
              selected.window, selected.groups - first);
            for (std::size_t f = 0; f < writers.size(); ++f) {
                for (std::uint32_t i = 0; i < count; ++i) {
                    auto group = co_await freeze_children(
                      *writers[f],
                      std::move(inputs[f].groups[first + i].children),
                      resources,
                      work,
                      observed);
                    require(
                      group.layout().boundary().end().bytes
                        == inputs[f].groups[first + i].end,
                      "writer changed the independently encoded group "
                      "boundary");
                    take(co_await writers[f]->encode_group(group, work));
                    auto accepted = take(
                      writers[f]->submit(std::move(group), work));
                    cuts[f] = accepted.boundary;
                    pending[f][i].emplace(std::move(accepted));
                    ++observed.submitted;
                    observed.peak_admission = std::max(
                      observed.peak_admission, resources.snapshot().bytes);
                }
            }
            for (std::size_t f = 0; f < writers.size(); ++f) {
                const auto barrier = co_await writers[f]->barrier(*cuts[f]);
                take(barrier.failure.outcome());
                require(
                  barrier.receipt && barrier.receipt->boundary() == *cuts[f],
                  "segment benchmark barrier did not cover its complete "
                  "window");
                ++observed.barriers;
                for (std::uint32_t i = 0; i < count; ++i) {
                    auto waiting = std::move(pending[f][i]->written);
                    pending[f][i].reset();
                    auto done = co_await std::move(waiting);
                    take(done.failure.outcome());
                    if (selected.retain) retained.push_back(std::move(done));
                }
            }
        }
    } catch (...) {
        failed.observe(std::current_exception());
    }
    // A failed earlier receipt must not discard another entered write future.
    for (auto& file : pending)
        for (auto& submission : file)
            if (submission) {
                auto waiting = std::move(submission->written);
                submission.reset();
                try {
                    failed.observe(
                      (co_await std::move(waiting)).failure.outcome());
                } catch (...) {
                    failed.observe(std::current_exception());
                }
            }
    take(failed.outcome());
}

struct completed_snapshot final {
    workload_reservation held;
    std::vector<completed_retry> facts;
    std::uint32_t* calls;
    seastar::future<runtime::result<std::vector<completed_retry>>> read(
      std::uint32_t first, std::uint32_t count, codec::cooperative_work& work) {
        if (first > facts.size() || count > facts.size() - first)
            co_return runtime::failure(detail::path_error(errc::out_of_range));
        ++*calls;
        std::vector<completed_retry> output;
        output.reserve(count);
        for (std::uint32_t i = 0; i < count; ++i) {
            auto ready = co_await work.admit(
              byte_count{sizeof(completed_retry)},
              item_count{1},
              codec::error{errc::success});
            if (!ready)
                co_return runtime::failure(
                  detail::path_error(ready.error().code()));
            output.push_back(facts[first + i]);
        }
        co_return output;
    }
};

seastar::future<> seal_writer(
  writer_type& writer,
  extent_input& input,
  const shape& selected,
  workload_budget& resources,
  const workload_reservation& fixture,
  codec::cooperative_work& work,
  measurement& observed) {
    const auto before = resources.snapshot();
    if (selected.pressure) resources.close_admission();
    std::uint32_t calls = 0;
    const auto sealed = co_await writer.seal(
      completed_snapshot{fixture.share(), std::move(input.completed), &calls},
      selected.batches(),
      0,
      work);
    take(sealed.failure.outcome());
    require(
      sealed.extent && sealed.boundary && sealed.retry
        && sealed.extent->coverage == input.proof->boundary().coverage
        && sealed.extent->digest == *input.proof->digest(),
      "sealed segment disagrees with the separately encoded extent");
    require(
      calls == 2 * sealed.retry->pages().value(),
      "seal did not replay each retry page exactly twice");
    if (selected.pressure)
        require(
          resources.snapshot().accepted == before.accepted
            && resources.snapshot().rejected == before.rejected,
          "seal reacquired closed ordinary admission");
    ++observed.sealed;
    input.sealed_end = sealed.extent->file_end;
    observed.retry_pages += sealed.retry->pages().value();
    observed.page_reads += calls;
    if (selected.reopen) {
        take(co_await writer.reopen_read_handle(work));
        take(co_await writer.evict_read_handle());
        take(co_await writer.reopen_read_handle(work));
        observed.reopen_count += 2;
    }
}

// Exact disk verification is untimed and bounded; never flatten a maximum
// extent into a contiguous host string. Metadata/root reads remain separate.
seastar::future<> verify_file(
  file_system& files,
  const runtime::file_path& path,
  extent_input& input,
  bool sealed,
  codec::cooperative_work& work) {
    auto file = take(
      co_await files.open(
        path, {.close_policy = runtime::file_close_policy::checked}));
    runtime::first_failure failed;
    try {
        const auto header = take(
          co_await file.read(runtime::file_position{}, input.header.size()));
        require(
          co_await codec::bench::buffers_equal(
            header.data(), input.header, work),
          "benchmark changed the segment header");
        const auto expected_end = input.history.data_start.value()
                                  + input.encoded_bytes;
        const auto size = take(co_await file.size());
        require(
          sealed ? input.sealed_end && size == input.sealed_end->value()
                 : size == expected_end,
          "benchmark disk extent disagrees with the completed boundary");
        codec::xxh3_128_hasher actual;
        auto position = input.history.data_start.value();
        while (position != expected_end) {
            auto read = take(
              co_await file.read(
                runtime::file_position{position},
                byte_count{
                  std::min<std::uint64_t>(65536, expected_end - position)}));
            require(
              read.data().size().value() != 0,
              "benchmark disk verification reached early EOF");
            for (auto part : read.data()) {
                actual.update(part.data(), part.size());
                co_await work.drain_inline(
                  byte_count{part.size()}, item_count{1});
            }
            position += read.data().size().value();
        }
        require(
          std::move(actual).final() == input.proof->digest()->bytes(),
          "stored block/footer bytes differ from the exported extent");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    try {
        failed.observe(co_await file.close());
    } catch (...) {
        failed.observe(std::current_exception());
    }
    take(failed.outcome());
}

seastar::future<> preencoded_io(
  runtime::file& file,
  extent_input& input,
  const shape& selected,
  measurement& observed,
  codec::cooperative_work& work) {
    for (std::uint32_t first = 0; first < selected.groups;
         first += selected.window) {
        const auto count = std::min(selected.window, selected.groups - first);
        std::uint32_t consumed = 0;
        while (consumed != count) {
            const auto begin = first + consumed;
            std::size_t fragments = 0;
            byte_count size{}, retained{};
            std::uint32_t gathered = 0;
            for (; gathered < count - consumed; ++gathered) {
                const auto& wire = input.groups[begin + gathered].wire;
                const auto next_size = size.checked_add(wire.size());
                const auto next_retained = retained.checked_add(
                  wire.retained_bytes());
                if (
                  !next_size || !next_retained
                  || *next_size > runtime::maximum_file_io_bytes
                  || *next_retained > runtime::maximum_file_io_bytes
                  || fragments + wire.fragment_count()
                       > bytes::max_buffer_fragments)
                    break;
                size = *next_size;
                retained = *next_retained;
                fragments += wire.fragment_count();
            }
            require(
              gathered != 0, "preencoded group exceeds native gather bounds");
            bytes::fragmented_buffer payload;
            if (gathered == 1)
                payload = std::move(input.groups[begin].wire);
            else {
                bytes::fragmented_buffer_builder builder{
                  {.initial_fragment_bytes = byte_count{1},
                   .max_fragment_bytes = byte_count{1},
                   .max_total_bytes = size,
                   .max_retained_bytes = retained,
                   .max_fragments = fragments}};
                builder.reserve_fragments(item_count{fragments}).value();
                for (std::uint32_t i = 0; i < gathered; ++i) {
                    co_await work.drain_inline(
                      work.byte_quantum(), work.item_quantum());
                    builder
                      .append_buffer(std::move(input.groups[begin + i].wire))
                      .value();
                }
                payload = builder.finish().value();
            }
            observed.submitted += gathered;
            const auto written = take(
              co_await file.write(
                input.groups[begin].begin, std::move(payload)));
            require(written == size, "short preencoded cohort write");
            consumed += gathered;
        }
        take(co_await file.flush());
        ++observed.barriers;
    }
}

void print_measurement(
  const shape& selected,
  operation op,
  const measurement& value,
  const file_system& files,
  std::span<const extent_input> input,
  byte_count fixture_bytes,
  std::uint64_t device,
  const std::set<unsigned>& affinity,
  std::string_view comparison,
  bool preallocated,
  bool foreground,
  bool memory) {
    std::uint64_t encoded = 0, children = 0, fragments = 0, wire_fragments = 0;
    for (const auto& extent : input) {
        encoded += extent.encoded_bytes;
        children += extent.child_bytes;
        fragments += extent.child_fragments;
        for (const auto& group : extent.groups)
            wire_fragments += group.wire_fragments;
    }
    const auto digest = hex_digest(input.front().proof->digest()->bytes());
    std::printf(
      "segment_bench_v4 "
      "{\"scope\":\"%s\",\"case\":\"%s\",\"encoding\":\"%s\",\"digest\":\"%s\","
      "\"comparison_id\":\"%.*s\"",
      scope(op),
      selected.name,
      selected.encoding == compression::codec_id::lz4 ? "lz4" : "none",
      digest.data(),
      static_cast<int>(comparison.size()),
      comparison.data());
    auto number = [](const char* name, std::uint64_t n) {
        std::printf(",\"%s\":%" PRIu64, name, n);
    };
    auto flag = [](const char* name, bool set) {
        std::printf(",\"%s\":%s", name, set ? "true" : "false");
    };
    number("payload_bytes", selected.payload_bytes);
    number("groups", selected.groups);
    number("blocks", selected.blocks);
    number("batches", selected.batches());
    number("window", selected.window);
    number("segments", selected.segments);
    number("alignment", selected.alignment);
    number("data_start", input.front().history.data_start.value());
    number("encoded_bytes", encoded);
    number("child_bytes", children);
    number("child_fragments", fragments);
    number("encoded_fragments", wire_fragments);
    number("page_bytes", selected.page_bytes);
    number("elapsed_ns", value.elapsed);
    number("digest_drain_ns", value.digest_drain);
    number("reactor_cpu_ns", value.cpu);
    number("allocations", value.allocations);
    number("tasks", value.tasks);
    number("runtime_operations", value.runtime_operations);
    number("foreground_turns", value.foreground_turns);
    number("fixture_retained_bound", fixture_bytes.value());
    number("sampled_admission_peak", value.peak_admission);
    number("admission_after", value.admission_after);
    number("barriers", value.barriers);
    number("submitted_groups", value.submitted);
    number("sealed_segments", value.sealed);
    number("retry_pages", value.retry_pages);
    number("page_source_reads", value.page_reads);
    number("retained_results", value.retained_results);
    number("reopens", value.reopen_count);
    number("layout_calls", value.layout_calls);
    number("pressure_waits", value.pressure_waits);
    number("setup_extent", value.setup_extent);
    number("setup_allocated_bytes", value.setup_allocated_bytes);
    number("device", device);
    flag("timing_only", timing_only);
#ifdef NDEBUG
    flag("ndebug", true);
#else
    flag("ndebug", false);
#endif
    flag(
      "libcxx_hardening_none",
      _LIBCPP_HARDENING_MODE == _LIBCPP_HARDENING_MODE_NONE);
    flag("preallocated", preallocated);
    flag("foreground_probe", foreground);
    flag("memory_observed", memory);
    flag("fragmented", selected.fragmented);
    flag("zero_timestamps", selected.zero_timestamps);
    flag("replacement", selected.replace);
    flag("closed_admission", selected.pressure);
    flag("retain_results", selected.retain);
    flag("reopen_profile", selected.reopen);
    flag("disk", disk(op));
    flag("complete", true);
    if (memory) {
        number("memory_peak", value.memory.peak_upper_bound);
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)                            \
  && !defined(SEASTAR_DEFAULT_ALLOCATOR)
        flag("critical_observed", true);
        number("critical_peak", value.memory.critical_peak_upper_bound);
#else
        flag("critical_observed", false);
        std::printf(",\"critical_peak\":null");
#endif
        number("largest_allocation", value.memory.largest_allocation);
        flag("memory_complete", value.memory.complete);
    }
    if constexpr (!timing_only) {
        number("native_calls", files.sample.calls);
        number("native_bytes", files.sample.bytes);
        number(
          "native_write_min", files.sample.calls ? files.sample.minimum : 0);
        number("native_write_max", files.sample.maximum);
        number("native_depth", files.sample.depth);
        number("file_flushes", files.sample.flushes);
        number("directory_syncs", files.directory_syncs);
        number("directory_sync_ns", files.directory_sync_ns);
        number("write_service_ns", files.sample.write_service_ns);
        number("write_busy_ns", files.sample.write_busy_ns);
        number("write_max_service_ns", files.sample.write_max_service_ns);
        number("flush_service_ns", files.sample.flush_service_ns);
        number("staging_copy_upper_bound", files.sample.bytes);
        number("digest_update_calls", value.work.digest_calls);
        number("digest_update_bytes", value.work.digest_bytes);
        number("crc_bulk_calls", value.work.crc_bulk_calls);
        number("crc_bulk_bytes", value.work.crc_bulk_bytes);
        number("lz4_calls", value.work.lz4_calls);
        number("lz4_frames", value.work.lz4_frames);
        number("lz4_input_bytes", value.work.lz4_input_bytes);
        number("lz4_output_bytes", value.work.lz4_output_bytes);
        flag("crc_inline_observed", false);
    }
    std::printf(",\"native_geometry\":[");
    for (std::size_t i = 0; i < files.native_geometry.size(); ++i)
        std::printf("%s%" PRIu64, i ? "," : "", files.native_geometry[i]);
    std::printf("],\"segment_digest\":[");
    for (std::size_t i = 0; i < input.size(); ++i) {
        const auto identity = hex_digest(input[i].proof->digest()->bytes());
        std::printf("%s\"%s\"", i ? "," : "", identity.data());
    }
    std::printf("],\"header_digest\":[");
    for (std::size_t i = 0; i < input.size(); ++i) {
        const auto identity = hex_digest(input[i].header_digest);
        std::printf("%s\"%s\"", i ? "," : "", identity.data());
    }
    std::printf("],\"encoded_bytes_by_segment\":[");
    for (std::size_t i = 0; i < input.size(); ++i)
        std::printf("%s%" PRIu64, i ? "," : "", input[i].encoded_bytes);
    std::printf("],\"affinity_cpus\":[");
    bool first = true;
    for (auto cpu : affinity) {
        std::printf("%s%u", first ? "" : ",", cpu);
        first = false;
    }
    std::printf("],\"cuts\":[");
    for (std::size_t i = 0; i < input.front().groups.size(); ++i) {
        const auto covering = std::min(
          input.front().groups.size() - 1,
          ((i / selected.window) + 1) * selected.window - 1);
        std::printf(
          "%s[%" PRIu64 ",%" PRIu64 "]",
          i ? "," : "",
          input.front().groups[i].end.value(),
          input.front().groups[covering].end.value());
    }
    std::printf("],\"flush_times\":[");
    if constexpr (!timing_only)
        for (std::size_t i = 0; i < files.sample.flushes; ++i)
            std::printf(
              "%s[%" PRIu64 ",%" PRIu64 "]",
              i ? "," : "",
              files.sample.flush_times[i].begin - value.epoch,
              files.sample.flush_times[i].end - value.epoch);
    std::puts("]}");
}

struct segment_bench {
    segment_bench() {
        static const bool initialized = [] {
            codec::testing::report_profile();
            return true;
        }();
        static_cast<void>(initialized);
    }
    seastar::future<std::size_t> run_case(shape selected, operation op) {
        selected.zero_timestamps = std::getenv("KWAQUE_SEGMENT_ZERO_TIMESTAMPS")
                                   != nullptr;
        const bool memory = std::getenv("KWAQUE_SEGMENT_OBSERVE_ALLOCATIONS")
                            != nullptr;
        const bool foreground = std::getenv("KWAQUE_SEGMENT_FOREGROUND_PROBE")
                                != nullptr;
        const bool preallocate = std::getenv("KWAQUE_SEGMENT_PREALLOCATE")
                                 != nullptr;
        require(
          !memory || !timing_only, "native observation requires segment_bench");
        require(
          !preallocate || op == operation::writer
            || op == operation::preencoded,
          "preallocation is limited to a fresh single-file append experiment");
        require(
          selected.segments <= maximum_segments && selected.segments != 0,
          "segment benchmark owner count exceeds its bound");
        const auto affinity = seastar::get_current_cpuset();
        const auto* supplied = std::getenv("KWAQUE_SEGMENT_COMPARISON_ID");
        const std::string_view comparison = supplied ? supplied : "";
        require(
          comparison.empty()
            || (comparison.size() == 64 && comparison.find_first_not_of("0123456789abcdef") == std::string_view::npos),
          "comparison ID must be a SHA-256 context digest");
        const auto config = resource::resource_config::from_total_memory(
                              byte_count{
                                seastar::memory::stats().total_memory()})
                              .value();
        resource::resource_registry registry;
        co_await registry.start(config);
        resource::resource_manager manager{registry.handles()};
        runtime::first_failure failed;
        try {
            co_await manager.start();
            co_await seastar::tmp_dir::do_with(
              runtime::testing::test_directory_template(),
              seastar::coroutine::lambda(
                [&](seastar::tmp_dir& directory) -> seastar::future<> {
                    file_system files{!timing_only};
                    const auto root = take(
                      runtime::file_path::make(
                        (directory.get_path() / "store").string()));
                    take(co_await files.create_directories(root));
                    const auto status = co_await seastar::file_stat(
                      root.value(), seastar::follow_symlink::no);
                    const auto spec = store_contract::specification(
                      root, {status.device_id, status.inode_number}, 68);
                    const std::array specs{spec};
                    store_contract::ownership_input owner{specs};
                    workload_budget resources{
                      manager.acquire_workload(
                        resource::workload_class::metadata),
                      {.tasks = 512,
                       .bytes = byte_count{96_MiB},
                       .handles = 32},
                      charge};
                    seastar::abort_source abort;
                    codec::cooperative_work work{policy(selected), abort};
                    co_await installation_contract::bootstrap(
                      files, owner, spec, resources, work, native_driver{});
                    std::optional<workload_reservation> fixture_held;
                    std::optional<workload_reservation> gather_held;
                    std::vector<extent_input> inputs;
                    inputs.reserve(selected.segments);
                    byte_count fixture_bytes{};
                    for (std::uint32_t i = 0; i < selected.segments; ++i) {
                        inputs.push_back(
                          co_await make_extent(selected, i, work));
                        fixture_bytes = fixture_bytes
                                          .checked_add(
                                            retained_bound(inputs.back()))
                                          .value();
                    }
                    fixture_held.emplace(
                      take(resources.try_reserve(fixture_bytes)));
                    if (op == operation::preencoded && selected.window > 1)
                        gather_held.emplace(take(resources.try_reserve(charge(
                          byte_count{
                            bytes::max_buffer_fragments
                            * bytes::fragmented_buffer::
                              fragment_descriptor_size()}))));
                    if (
                      const auto* fixture = std::getenv(
                        "KWAQUE_SEGMENT_FIXTURE")) {
                        require(
                          selected.segments == 1 && selected.groups != 0,
                          "fixture export requires one nonempty segment");
                        export_extent(inputs.front(), selected, fixture);
                        if (std::getenv("KWAQUE_SEGMENT_EXPORT_RECORDS"))
                            co_await export_records(
                              inputs.front(), selected, fixture, work);
                    }
                    std::vector<std::unique_ptr<writer_type>> writers(
                      selected.segments);
                    std::vector<segment_write_completion> retained;
                    if (selected.retain)
                        retained.reserve(selected.groups * selected.segments);
                    std::optional<runtime::file> raw_file;
                    std::optional<runtime::file_path> raw_path;
                    measurement result;
                    std::optional<verified_extent> measured_proof;
                    const auto benchmark_config = writer_config(selected);
                    runtime::first_failure execution;
                    bool stop_foreground = false;
                    std::optional<seastar::future<>> foreground_work;
                    auto foreground_loop = [&] -> seastar::future<> {
                        while (!stop_foreground) {
                            co_await seastar::yield();
                            if (!stop_foreground) ++result.foreground_turns;
                        }
                    };
                    // Every side starts from the same prepared extent, set
                    // up before the interval: the writer zero-writes it at
                    // creation (its own mechanism), while the raw baseline
                    // and the reference reserve it with fallocate.
                    const auto setup_extent
                      = preallocate ? ((inputs[0].history.data_start.value()
                                        + inputs[0].encoded_bytes + 2_MiB - 1)
                                       / 2_MiB)
                                        * 2_MiB
                                    : std::uint64_t{0};
                    try {
                        if (op == operation::writer)
                            writers[0] = co_await create_writer(
                              files,
                              owner,
                              spec,
                              inputs[0],
                              resources,
                              selected,
                              work,
                              byte_count{
                                preallocate
                                  ? setup_extent
                                      - inputs[0].history.data_start.value()
                                  : 0});
                        if (op == operation::preencoded) {
                            raw_path = take(local_child_path(
                              root,
                              take(runtime::file_name::make("preencoded"))));
                            auto created = take(
                              co_await files.open(
                                *raw_path,
                                {.access = runtime::file_access::read_write,
                                 .create = true,
                                 .exclusive = true,
                                 .close_policy
                                 = runtime::file_close_policy::checked}));
                            runtime::first_failure setup;
                            try {
                                auto header = take(
                                  co_await created.write(
                                    runtime::file_position{},
                                    inputs[0].header.share()));
                                require(
                                  header == inputs[0].header.size(),
                                  "short preencoded header");
                                setup.observe(co_await created.flush());
                            } catch (...) {
                                setup.observe(std::current_exception());
                            }
                            try {
                                setup.observe(co_await created.close());
                            } catch (...) {
                                setup.observe(std::current_exception());
                            }
                            take(setup.outcome());
                            raw_file.emplace(take(
                              co_await files.open(
                                *raw_path,
                                {.access = runtime::file_access::read_write,
                                 .close_policy
                                 = runtime::file_close_policy::checked})));
                        }
                        if (preallocate) {
                            result.setup_extent = setup_extent;
                            if (op != operation::writer)
                                co_await files.prepare_extent(
                                  inputs[0].history.data_start.value(),
                                  result.setup_extent);
                        }
                        if (
                          op == operation::preencoded
                          || op == operation::writer) {
                            result.setup_allocated_bytes
                              = co_await files.allocated_bytes();
                            require(
                              !preallocate
                                || result.setup_allocated_bytes
                                     >= result.setup_extent,
                              "filesystem did not confirm the requested setup "
                              "allocation");
                        }
                        if (foreground)
                            foreground_work.emplace(foreground_loop());
                        result.peak_admission = resources.snapshot().bytes;
                        {
                            measure_scope interval{files, result, memory};
                            if (op == operation::layout) {
                                const auto& input = inputs[0];
                                require(
                                  !input.groups.empty(),
                                  "layout needs a supplied child");
                                for (std::size_t i = 0; i < 256; ++i) {
                                    const auto index
                                      = static_cast<std::uint32_t>(
                                        i % input.groups.size());
                                    const auto& group = input.groups[index];
                                    // Groups are uniform: each earlier group
                                    // stored `blocks` blocks/facts and one
                                    // footer.
                                    const auto earlier = index
                                                         * selected.blocks;
                                    const segment_writer_position current{
                                      group.children.front()
                                        .info()
                                        .context.logical_span()
                                        .begin(),
                                      group.blocks.front()
                                        .descriptor()
                                        .coverage()
                                        .physical()
                                        .begin(),
                                      group.begin,
                                      earlier,
                                      index,
                                      earlier};
                                    auto plan = take(plan_segment_capacity(
                                      input.descriptor,
                                      input.history.data_start,
                                      current,
                                      std::span{group.children},
                                      benchmark_config.admission,
                                      work.policy(),
                                      charge,
                                      spec.identity.metadata_alignment,
                                      benchmark_config.metadata
                                        .operation_bytes));
                                    require(
                                      plan.decision
                                          == segment_capacity_decision::fits
                                        && plan.end
                                        && plan.end->bytes == group.end,
                                      "layout benchmark changed complete "
                                      "block/footer geometry");
                                    perf_tests::do_not_optimize(plan);
                                    ++result.layout_calls;
                                }
                            } else if (
                              op == operation::typed || op == operation::raw
                              || op == operation::encode) {
                                auto proof = op == operation::encode
                                               ? co_await encode_extent(
                                                   inputs[0], work)
                                               : co_await verify_extent(
                                                   inputs[0],
                                                   op == operation::raw,
                                                   work);
                                measured_proof.emplace(proof);
                                perf_tests::do_not_optimize(proof);
                            } else if (op == operation::preencoded) {
                                co_await preencoded_io(
                                  *raw_file, inputs[0], selected, result, work);
                            } else {
                                if (selected.replace) {
                                    for (std::size_t i = 0; i < writers.size();
                                         ++i) {
                                        writers[i] = co_await create_writer(
                                          files,
                                          owner,
                                          spec,
                                          inputs[i],
                                          resources,
                                          selected,
                                          work);
                                        result.peak_admission = std::max(
                                          result.peak_admission,
                                          resources.snapshot().bytes);
                                        co_await append_groups(
                                          std::span{writers}.subspan(i, 1),
                                          std::span{inputs}.subspan(i, 1),
                                          selected,
                                          resources,
                                          work,
                                          result,
                                          retained);
                                        co_await seal_writer(
                                          *writers[i],
                                          inputs[i],
                                          selected,
                                          resources,
                                          *fixture_held,
                                          work,
                                          result);
                                        take(co_await writers[i]->close());
                                        writers[i].reset();
                                    }
                                } else {
                                    if (op == operation::lifecycle)
                                        for (std::size_t i = 0;
                                             i < writers.size();
                                             ++i) {
                                            writers[i] = co_await create_writer(
                                              files,
                                              owner,
                                              spec,
                                              inputs[i],
                                              resources,
                                              selected,
                                              work);
                                            result.peak_admission = std::max(
                                              result.peak_admission,
                                              resources.snapshot().bytes);
                                        }
                                    co_await append_groups(
                                      writers,
                                      inputs,
                                      selected,
                                      resources,
                                      work,
                                      result,
                                      retained);
                                    if (op == operation::lifecycle)
                                        for (std::size_t i = 0;
                                             i < writers.size();
                                             ++i) {
                                            co_await seal_writer(
                                              *writers[i],
                                              inputs[i],
                                              selected,
                                              resources,
                                              *fixture_held,
                                              work,
                                              result);
                                            take(co_await writers[i]->close());
                                            writers[i].reset();
                                        }
                                }
                            }
                            require(
                              files.sample.active == 0,
                              "timing stopped with outstanding native writes");
                            result.admission_after = resources.snapshot().bytes;
                            result.retained_results = retained.size();
                        }
                        if (op == operation::writer)
                            for (auto& writer : writers)
                                if (writer) {
                                    const auto started = measurement_time();
                                    take(co_await writer->digest_caught_up());
                                    result.digest_drain += measurement_time()
                                                           - started;
                                }
                    } catch (...) {
                        execution.observe(std::current_exception());
                    }
                    stop_foreground = true;
                    if (foreground_work) {
                        try {
                            co_await std::move(*foreground_work);
                        } catch (...) {
                            execution.observe(std::current_exception());
                        }
                    }
                    if (preallocate && !execution.failed()) {
                        try {
                            co_await files.finish_extent(
                              inputs[0].history.data_start.value()
                              + inputs[0].encoded_bytes);
                        } catch (...) {
                            execution.observe(std::current_exception());
                        }
                    }
                    for (auto& writer : writers)
                        if (writer) {
                            try {
                                execution.observe(co_await writer->close());
                            } catch (...) {
                                execution.observe(std::current_exception());
                            }
                            writer.reset();
                        }
                    if (raw_file) {
                        try {
                            execution.observe(co_await raw_file->close());
                        } catch (...) {
                            execution.observe(std::current_exception());
                        }
                        raw_file.reset();
                    }
                    retained.clear();
                    take(execution.outcome());
                    if (measured_proof)
                        require(
                          measured_proof->boundary()
                              == inputs[0].proof->boundary()
                            && measured_proof->digest()
                                 == inputs[0].proof->digest(),
                          "encoding/evidence benchmark changed the exact "
                          "extent proof");
                    require(
                      seastar::get_current_cpuset() == affinity,
                      "benchmark CPU affinity changed");
                    if (memory) {
                        require(
                          result.memory.observed && result.memory.complete,
                          "native segment allocation observation is "
                          "incomplete");
                        require(
                          result.memory.largest_allocation
                            <= maximum_contiguous_allocation_bytes,
                          "segment benchmark exceeded the contiguous "
                          "allocation ceiling");
                    }
                    if constexpr (!timing_only) {
                        if (
                          op == operation::typed || op == operation::encode
                          || op == operation::writer)
                            require(
                              result.work.lz4_frames == 0,
                              "qualified typed input was decompressed again");
                        if (op == operation::typed) {
                            require(
                              result.work.crc_bulk_calls != 0,
                              "bulk CRC observation did not intercept the "
                              "extent walk");
                            require(
                              result.work.digest_bytes
                                == inputs[0].encoded_bytes,
                              "extent evidence did not hash each stored byte "
                              "exactly once");
                        }
                        if (
                          op == operation::encode || op == operation::writer) {
                            const auto header_crc_bound
                              = std::uint64_t{selected.batches()}
                                  * (2 * work.policy().config().max_header_bytes.value()
                                     + segment_block_fixed_bytes.value()
                                     + 2 * codec::envelope_prefix_bytes)
                                + std::uint64_t{selected.groups}
                                    * (durable_footer_fixed_bytes.value()
                                       + 2 * codec::envelope_prefix_bytes);
                            // The writer's deferred extent digest takes one
                            // CRC pass over each stored byte, off the receipt
                            // path and partly inside the interval. Framing
                            // never rescans child or padding bytes.
                            std::uint64_t stored = 0;
                            if (op == operation::writer)
                                for (const auto& extent : inputs)
                                    stored += extent.encoded_bytes;
                            require(
                              result.work.crc_bulk_bytes
                                <= header_crc_bound + stored,
                              "typed framing rescanned child or padding bytes "
                              "for CRC");
                        }
                        if (op == operation::raw)
                            require(
                              result.work.lz4_frames == (selected.encoding == compression::codec_id::lz4 ? selected.batches() : 0),
                              "raw evidence did not validate every compressed "
                              "child");
                    }
                    if (disk(op))
                        for (std::size_t i = 0; i < inputs.size(); ++i)
                            co_await verify_file(
                              files,
                              raw_path ? *raw_path : data_path(spec, inputs[i]),
                              inputs[i],
                              op == operation::lifecycle,
                              work);
                    print_measurement(
                      selected,
                      op,
                      result,
                      files,
                      inputs,
                      fixture_bytes,
                      static_cast<std::uint64_t>(status.device_id),
                      affinity,
                      comparison,
                      preallocate,
                      foreground,
                      memory);
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
        co_return std::max<std::size_t>(1, selected.groups * selected.segments);
    }
};

// Every shape names its group commit explicitly: `blocks` batches share one
// durable footer and `window` groups share one segment barrier. Only the
// footer cases keep one footer per batch; alignment cases vary the geometry.
#define SEGMENT_SHAPE(Name, ...)                                               \
    shape { .name = #Name __VA_OPT__(, ) __VA_ARGS__ }
#define SEGMENT_CASE(Name, ...)                                                \
    PERF_TEST_F(segment_bench, layout_##Name) {                                \
        return run_case(SEGMENT_SHAPE(Name, __VA_ARGS__), operation::layout);  \
    }                                                                          \
    PERF_TEST_F(segment_bench, encode_##Name) {                                \
        return run_case(SEGMENT_SHAPE(Name, __VA_ARGS__), operation::encode);  \
    }                                                                          \
    PERF_TEST_F(segment_bench, typed_##Name) {                                 \
        return run_case(SEGMENT_SHAPE(Name, __VA_ARGS__), operation::typed);   \
    }                                                                          \
    PERF_TEST_F(segment_bench, raw_##Name) {                                   \
        return run_case(SEGMENT_SHAPE(Name, __VA_ARGS__), operation::raw);     \
    }                                                                          \
    PERF_TEST_F(segment_bench, preencoded_##Name) {                            \
        return run_case(                                                       \
          SEGMENT_SHAPE(Name, __VA_ARGS__), operation::preencoded);            \
    }                                                                          \
    PERF_TEST_F(segment_bench, writer_##Name) {                                \
        return run_case(SEGMENT_SHAPE(Name, __VA_ARGS__), operation::writer);  \
    }
SEGMENT_CASE(tiny)
SEGMENT_CASE(aligned, .payload_bytes = 128_KiB)
// A 257-byte-fragmented batch uses about half of the writer's per-group
// fragment bound, so group commit can seal at most two under one footer.
SEGMENT_CASE(
  fragmented,
  .payload_bytes = 128_KiB,
  .fragmented = true,
  .groups = 4,
  .blocks = 2,
  .window = 2)
SEGMENT_CASE(m4, .payload_bytes = 4_MiB, .groups = 2, .blocks = 1)
SEGMENT_CASE(maximum, .payload_bytes = 8_MiB, .groups = 1, .blocks = 1)
SEGMENT_CASE(
  lz4, .payload_bytes = 128_KiB, .encoding = compression::codec_id::lz4)
SEGMENT_CASE(
  lz4_maximum,
  .payload_bytes = 8_MiB,
  .encoding = compression::codec_id::lz4,
  .groups = 1,
  .blocks = 1)
SEGMENT_CASE(a8192, .payload_bytes = 128_KiB, .alignment = 8192)
SEGMENT_CASE(a65536, .payload_bytes = 128_KiB, .alignment = 65536)
SEGMENT_CASE(singleton, .payload_bytes = 128_KiB, .groups = 8, .blocks = 1)
SEGMENT_CASE(final_flush, .groups = 1, .blocks = 8)
SEGMENT_CASE(footers32, .groups = 32, .blocks = 1, .window = 4)
SEGMENT_CASE(footers128, .groups = 128, .blocks = 1, .window = 4)
#undef SEGMENT_CASE

#define SEGMENT_LIFECYCLE(Name, ...)                                           \
    PERF_TEST_F(segment_bench, lifecycle_##Name) {                             \
        return run_case(                                                       \
          SEGMENT_SHAPE(Name, __VA_ARGS__), operation::lifecycle);             \
    }
SEGMENT_LIFECYCLE(tiny)
SEGMENT_LIFECYCLE(aligned, .payload_bytes = 128_KiB)
SEGMENT_LIFECYCLE(
  fragmented,
  .payload_bytes = 128_KiB,
  .fragmented = true,
  .groups = 4,
  .blocks = 2,
  .window = 2)
SEGMENT_LIFECYCLE(maximum, .payload_bytes = 8_MiB, .groups = 1, .blocks = 1)
SEGMENT_LIFECYCLE(
  lz4, .payload_bytes = 128_KiB, .encoding = compression::codec_id::lz4)
SEGMENT_LIFECYCLE(empty, .groups = 0)
SEGMENT_LIFECYCLE(many_segments, .segments = 2)
SEGMENT_LIFECYCLE(replacement, .segments = 3, .replace = true)
SEGMENT_LIFECYCLE(retained, .groups = 8, .retain = true)
SEGMENT_LIFECYCLE(
  retry_pages, .groups = 32, .pressure = true, .page_bytes = 4096)
SEGMENT_LIFECYCLE(reopen, .reopen = true)
#undef SEGMENT_LIFECYCLE
#undef SEGMENT_SHAPE
} // namespace
} // namespace kwaque::storage::testing::segment_bench_support
