#include "src/base/allocation.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/codec/cooperative.h"
#include "src/codec/limits.h"
#include "src/codec/tests/memory_qualification_support.h"
#include "src/runtime/file.h"
#include "src/runtime/production/file.h"
#include "src/runtime/testing/test_directory.h"
#include "src/storage/tests/local_store_contract.h"
#include "src/storage/tests/wal_bench_file.h"
#include "src/storage/zero_fill.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>
#include <seastar/testing/perf_tests.hh>
#include <seastar/util/tmp_file.hh>

#include <algorithm>
#include <charconv>
#include <cinttypes>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <optional>
#include <set>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

// Synchronized writes in flight at once to one device: N files, each
// zero-written over every byte its writes reach and flushed before the timed
// region, then opened for synchronized writes, so each write is durable when
// it completes and needs no flush, as a zero-written segment's small writes
// are. Each round writes the same number of bytes to every file at once, at
// the next offset, and waits for all of them. Whether N such writes cost one
// device round trip or N decides whether a group across N segments is
// cheaper made durable in each segment or in one WAL write.
namespace kwaque::storage::testing::concurrent_dsync_bench {
namespace {
using store_contract::require;
using store_contract::take;
using wal_bench_support::measurement_time;

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

// What a run sets: the rounds, each write's bytes, a multiple of 4 KiB, and
// where the files lie; the test directory without it.
struct plan final {
    std::uint64_t rounds{0}, write_bytes{0};
    const char* directory{nullptr};
    static plan of() {
        plan result;
        result.rounds = setting("KWAQUE_DSYNC_ROUNDS", 16, 1, 4096);
        result.write_bytes = setting(
          "KWAQUE_DSYNC_WRITE_BYTES", 4_KiB, 4_KiB, 1_MiB);
        require(
          result.write_bytes % 4_KiB == 0,
          "a write's bytes must be a multiple of 4 KiB");
        result.directory = std::getenv("KWAQUE_DSYNC_DIRECTORY");
        return result;
    }
};

// What one case measured: each round's time, and each write's from the
// round's start to its completion, in nanoseconds.
struct measured final {
    std::vector<std::uint64_t> rounds, writes;
    std::uint64_t elapsed{0};
    wal_bench_support::reactor_io io;
};

seastar::future<> run_in(
  const std::filesystem::path& root,
  std::uint32_t files,
  const plan& planned,
  measured& out) {
    runtime::production::file_system native;
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto window = planned.rounds * planned.write_bytes;
    auto chunk = storage::detail::make_zero_chunk(
      byte_count{maximum_contiguous_allocation_bytes});
    std::vector<runtime::file_path> paths;
    paths.reserve(files);
    for (std::uint32_t i = 0; i != files; ++i)
        paths.push_back(take(
          runtime::file_path::make(
            (root / ("dsync-" + std::to_string(i))).string())));
    // Every file zero-written over the window and flushed: its writes are
    // then pure overwrites.
    for (const auto& path : paths) {
        auto file = take(
          co_await native.open(
            path,
            {.access = runtime::file_access::read_write,
             .create = true,
             .truncate = true}));
        runtime::first_failure failed;
        try {
            failed.observe(
              co_await storage::detail::zero_fill(
                file, chunk, 0, window, work));
            if (!failed.failed()) failed.observe(co_await file.flush());
        } catch (...) {
            failed.observe(std::current_exception());
        }
        failed.observe(co_await file.close());
        take(failed.outcome());
    }
    take(
      co_await native.sync_directory(
        take(runtime::file_path::make(root.string()))));
    std::vector<runtime::file> opened;
    opened.reserve(files);
    runtime::first_failure failed;
    try {
        for (const auto& path : paths)
            opened.push_back(take(
              co_await native.open(
                path,
                {.access = runtime::file_access::read_write,
                 .synchronous = true})));
        const std::string payload(planned.write_bytes, '\x5a');
        auto source = take(
          bytes::fragmented_buffer::copy_of(
            std::span<const char>{payload.data(), payload.size()}));
        out.rounds.reserve(planned.rounds);
        out.writes.reserve(planned.rounds * files);
        std::vector<seastar::future<>> flight;
        flight.reserve(files);
        const auto before = wal_bench_support::reactor_io::now();
        const auto began = measurement_time();
        perf_tests::start_measuring_time();
        for (std::uint64_t round = 0; round != planned.rounds; ++round) {
            const auto start = measurement_time();
            const runtime::file_position at{round * planned.write_bytes};
            for (auto& file : opened)
                flight.push_back(
                  file.write(at, source.share())
                    .then([&out, start, &planned](
                            runtime::result<byte_count> written) {
                        const auto bytes = take(std::move(written));
                        require(
                          bytes.value() == planned.write_bytes,
                          "a synchronized write was short");
                        out.writes.push_back(measurement_time() - start);
                    }));
            for (auto& one : flight) {
                try {
                    co_await std::move(one);
                } catch (...) {
                    failed.observe(std::current_exception());
                }
            }
            flight.clear();
            take(failed.outcome());
            out.rounds.push_back(measurement_time() - start);
        }
        perf_tests::stop_measuring_time();
        out.elapsed = measurement_time() - began;
        out.io = wal_bench_support::reactor_io::now().since(before);
    } catch (...) {
        failed.observe(std::current_exception());
    }
    for (auto& file : opened)
        failed.observe(co_await file.close());
    take(failed.outcome());
}

std::uint64_t quantile(std::vector<std::uint64_t> values, double at) {
    std::ranges::sort(values);
    const auto rank = static_cast<std::size_t>(
      at * static_cast<double>(values.size() - 1));
    return values[rank];
}

void print_row(
  std::uint32_t files,
  const plan& planned,
  const measured& out,
  const std::set<unsigned>& affinity) {
    std::printf(
      "concurrent_dsync_v1 {\"files\":%" PRIu32 ",\"rounds\":%" PRIu64
      ",\"write_bytes\":%" PRIu64 ",\"directory\":\"%s\"",
      files,
      planned.rounds,
      planned.write_bytes,
      planned.directory ? "set" : "test");
    const auto number = [](const char* name, std::uint64_t value) {
        std::printf(",\"%s\":%" PRIu64, name, value);
    };
    number("elapsed_ns", out.elapsed);
    number("reactor_flushes", out.io.flushes);
    number("reactor_writes", out.io.writes);
    number("reactor_write_bytes", out.io.write_bytes);
    number("write_p50_ns", quantile(out.writes, 0.5));
    number("write_p99_ns", quantile(out.writes, 0.99));
    number("write_max_ns", quantile(out.writes, 1.0));
#ifdef NDEBUG
    std::printf(",\"ndebug\":true");
#else
    std::printf(",\"ndebug\":false");
#endif
    std::printf(",\"rounds_ns\":[");
    for (std::size_t i = 0; i != out.rounds.size(); ++i)
        std::printf("%s%" PRIu64, i ? "," : "", out.rounds[i]);
    std::printf("],\"affinity_cpus\":[");
    bool first = true;
    for (auto cpu : affinity) {
        std::printf("%s%u", first ? "" : ",", cpu);
        first = false;
    }
    std::puts("]}");
}

struct concurrent_dsync {
    concurrent_dsync() {
        static const bool initialized = [] {
            codec::testing::report_profile();
            return true;
        }();
        static_cast<void>(initialized);
    }

    static seastar::future<std::size_t> run_case(std::uint32_t files) {
        const auto planned = plan::of();
        const auto affinity = seastar::get_current_cpuset();
        measured out;
        auto where = planned.directory
                       ? std::filesystem::path{planned.directory}
                           / "kwaque-dsync-XXXXXX"
                       : runtime::testing::test_directory_template();
        co_await seastar::tmp_dir::do_with(
          std::move(where),
          seastar::coroutine::lambda(
            [&](seastar::tmp_dir& directory) -> seastar::future<> {
                co_await run_in(directory.get_path(), files, planned, out);
            }));
        require(
          out.writes.size() == planned.rounds * files, "a round lost a write");
        print_row(files, planned, out, affinity);
        co_return out.writes.size();
    }
};

PERF_TEST_F(concurrent_dsync, files_1) { return run_case(1); }
PERF_TEST_F(concurrent_dsync, files_4) { return run_case(4); }
PERF_TEST_F(concurrent_dsync, files_16) { return run_case(16); }
PERF_TEST_F(concurrent_dsync, files_64) { return run_case(64); }
PERF_TEST_F(concurrent_dsync, files_256) { return run_case(256); }
PERF_TEST_F(concurrent_dsync, files_1024) { return run_case(1024); }
} // namespace
} // namespace kwaque::storage::testing::concurrent_dsync_bench
