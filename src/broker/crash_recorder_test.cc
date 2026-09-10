#include "src/broker/crash_recorder.h"
#include "src/broker/crash_recorder_writer.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/seastar.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/alloc_failure_injector.hh>
#include <seastar/util/backtrace.hh>

#include <boost/test/unit_test.hpp>
#include <crc32c/crc32c.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace kwaque::broker {

class crash_recorder_test_access final {
public:
    static void set_directory_sync(
      crash_recorder& recorder,
      seastar::future<> (*operation)(const std::filesystem::path&)) {
        recorder.sync_directory_ = operation;
    }
};

} // namespace kwaque::broker

namespace {

thread_local unsigned directory_sync_calls = 0;
thread_local std::optional<seastar::future<>> directory_sync_result;

seastar::future<> scripted_directory_sync(const std::filesystem::path& path) {
    ++directory_sync_calls;
    if (directory_sync_result) {
        auto result = std::move(*directory_sync_result);
        directory_sync_result.reset();
        return result;
    }
    return seastar::sync_directory(path.native());
}

void write_report(
  const std::filesystem::path& path, std::uint64_t timestamp, bool legacy) {
    constexpr auto architecture = kwaque::broker::detail::crash_architecture();
    const auto header = legacy ? kwaque::broker::detail::crash_v1_header_size
                               : kwaque::broker::detail::crash_header_size;
    std::vector<char> bytes(
      header + 1U + (legacy ? 0U : architecture.size()) + 4U);
    const std::string_view magic = legacy ? "KQCRSH1" : "KQCRSH2";
    std::copy(magic.begin(), magic.end(), bytes.begin());
    const auto store =
      [&](std::size_t offset, std::uint64_t value, std::size_t width) {
          for (std::size_t index = 0; index < width; ++index) {
              bytes[offset + index] = static_cast<char>(value & 0xffU);
              value >>= 8U;
          }
      };
    store(8, timestamp, 8);
    store(24, 1, 4);
    store(36, 1, 4);
    bytes[header] = 't';
    if (!legacy) {
        store(40, architecture.size(), 4);
        std::copy(
          architecture.begin(), architecture.end(), bytes.data() + header + 1U);
    }
    store(
      bytes.size() - 4U, crc32c::Crc32c(bytes.data(), bytes.size() - 4U), 4);
    std::ofstream output{path, std::ios::binary | std::ios::trunc};
    output.exceptions(std::ios::badbit | std::ios::failbit);
    output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    output.close();
}

class temporary_directory final {
public:
    temporary_directory()
      : path_(std::filesystem::temp_directory_path()
              / ("kwaque-crash-test-" + std::to_string(::getpid()) + "-"
                 + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()))) {}

    ~temporary_directory() {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }

    [[nodiscard]] const std::filesystem::path& path() const noexcept {
        return path_;
    }

private:
    std::filesystem::path path_;
};

std::size_t count_reports(const std::filesystem::path& directory) {
    std::size_t count = 0;
    for (const auto& entry :
         std::filesystem::directory_iterator(directory / "crash_reports")) {
        if (
          entry.symlink_status().type() == std::filesystem::file_type::regular
          && entry.path().extension() == ".crash") {
            ++count;
        }
    }
    return count;
}

} // namespace

SEASTAR_TEST_CASE(crash_recorder_removes_only_the_unused_placeholder) {
    temporary_directory directory;
    seastar::abort_source abort;
    kwaque::broker::crash_recorder recorder;
    co_await recorder.start(directory.path(), abort, false);
    BOOST_REQUIRE(std::filesystem::exists(recorder.current_report()));
    BOOST_CHECK_EQUAL(
      std::filesystem::file_size(recorder.current_report()), 0U);
    co_await recorder.stop();
    co_await recorder.stop();
    BOOST_CHECK_EQUAL(count_reports(directory.path()), 0U);
}

SEASTAR_TEST_CASE(
  crash_recorder_retries_directory_sync_after_completed_unlink) {
    temporary_directory directory;
    seastar::abort_source abort;
    kwaque::broker::crash_recorder recorder;
    co_await recorder.start(directory.path(), abort, false);
    directory_sync_calls = 0;
    directory_sync_result.emplace(
      seastar::make_exception_future<>(
        std::system_error(std::make_error_code(std::errc::io_error))));
    kwaque::broker::crash_recorder_test_access::set_directory_sync(
      recorder, scripted_directory_sync);
    bool failed = false;
    try {
        co_await recorder.stop();
    } catch (const std::system_error&) {
        failed = true;
    }
    BOOST_CHECK(failed);
    BOOST_CHECK_EQUAL(directory_sync_calls, 1U);
    BOOST_CHECK_EQUAL(count_reports(directory.path()), 0U);
    co_await recorder.stop();
    BOOST_CHECK_EQUAL(directory_sync_calls, 2U);
    co_await recorder.stop();
    BOOST_CHECK_EQUAL(directory_sync_calls, 2U);
}

SEASTAR_TEST_CASE(crash_recorder_serializes_concurrent_cleanup) {
    temporary_directory directory;
    seastar::abort_source abort;
    kwaque::broker::crash_recorder recorder;
    co_await recorder.start(directory.path(), abort, false);
    seastar::promise<> release;
    directory_sync_calls = 0;
    directory_sync_result.emplace(release.get_future());
    kwaque::broker::crash_recorder_test_access::set_directory_sync(
      recorder, scripted_directory_sync);
    auto first = recorder.stop();
    auto second = recorder.stop();
    BOOST_CHECK(!first.available());
    BOOST_CHECK(!second.available());
    release.set_value();
    co_await std::move(first);
    co_await std::move(second);
    BOOST_CHECK_EQUAL(directory_sync_calls, 1U);
    BOOST_CHECK_EQUAL(count_reports(directory.path()), 0U);
}

SEASTAR_TEST_CASE(crash_recorder_retains_recorded_failure_after_stop) {
    temporary_directory directory;
    seastar::abort_source abort;
    kwaque::broker::crash_recorder recorder;
    co_await recorder.start(directory.path(), abort, false);
    recorder.record_startup_failure();
    const auto report = recorder.current_report();
    const auto size = std::filesystem::file_size(report);
    BOOST_CHECK_GT(size, kwaque::broker::detail::crash_header_size);
    BOOST_CHECK_LE(size, kwaque::broker::detail::crash_serialization_capacity);
    recorder.record_startup_failure();
    BOOST_CHECK_EQUAL(std::filesystem::file_size(report), size);
    co_await recorder.stop();
    BOOST_CHECK(std::filesystem::exists(report));

    kwaque::broker::crash_recorder restarted;
    co_await restarted.start(directory.path(), abort, false);
    BOOST_CHECK_EQUAL(std::filesystem::file_size(report), size);
    co_await restarted.stop();
    BOOST_CHECK_EQUAL(count_reports(directory.path()), 1U);
}

SEASTAR_TEST_CASE(crash_recorder_keeps_fifty_existing_reports_and_current) {
    temporary_directory directory;
    const auto report_directory = directory.path() / "crash_reports";
    std::filesystem::create_directories(report_directory);
    for (std::size_t index = 0; index < 55; ++index) {
        std::ofstream(report_directory / (std::to_string(index) + ".crash"))
          << "torn";
    }
    std::ofstream(report_directory / "keep.txt") << "unrelated";
    std::filesystem::create_directory(report_directory / "directory.crash");
    std::filesystem::create_symlink(
      "keep.txt", report_directory / "link.crash");
    seastar::abort_source abort;
    kwaque::broker::crash_recorder recorder;
    co_await recorder.start(directory.path(), abort, false);
    BOOST_CHECK_EQUAL(count_reports(directory.path()), 51U);
    BOOST_CHECK(std::filesystem::exists(report_directory / "keep.txt"));
    BOOST_CHECK(std::filesystem::is_symlink(report_directory / "link.crash"));
    BOOST_CHECK(
      std::filesystem::is_directory(report_directory / "directory.crash"));
    co_await recorder.stop();
    BOOST_CHECK_EQUAL(count_reports(directory.path()), 50U);
}

SEASTAR_TEST_CASE(
  crash_recorder_retention_uses_record_time_fallback_and_name_ties) {
    temporary_directory directory;
    const auto reports = directory.path() / "crash_reports";
    std::filesystem::create_directories(reports);
    const auto torn = reports / "z_torn.crash";
    std::ofstream(torn) << "torn";
    const auto status = co_await seastar::file_stat(torn.native());
    const auto fallback = std::chrono::duration_cast<std::chrono::milliseconds>(
                            status.time_changed.time_since_epoch())
                            .count();
    BOOST_REQUIRE_GT(fallback, 1000);
    BOOST_REQUIRE_LT(fallback, std::numeric_limits<std::int64_t>::max() - 1000);
    const auto timestamp = static_cast<std::uint64_t>(fallback);
    for (std::size_t index = 0; index < 49; ++index) {
        write_report(
          reports / ("new_" + std::to_string(index) + ".crash"),
          timestamp + 1000,
          index % 2 == 0);
    }
    write_report(reports / "a_tie.crash", timestamp, true);
    write_report(reports / "b_tie.crash", timestamp, false);
    write_report(reports / "x_ancient_v1.crash", 1, true);
    write_report(reports / "y_ancient_v2.crash", 2, false);
    seastar::abort_source abort;
    kwaque::broker::crash_recorder recorder;
    co_await recorder.start(directory.path(), abort, false);
    BOOST_CHECK_EQUAL(count_reports(directory.path()), 51U);
    BOOST_CHECK(std::filesystem::exists(torn));
    for (std::size_t index = 0; index < 49; ++index) {
        BOOST_CHECK(
          std::filesystem::exists(
            reports / ("new_" + std::to_string(index) + ".crash")));
    }
    for (const auto name :
         {"a_tie.crash",
          "b_tie.crash",
          "x_ancient_v1.crash",
          "y_ancient_v2.crash"}) {
        BOOST_CHECK(!std::filesystem::exists(reports / name));
    }
    co_await recorder.stop();
    BOOST_CHECK_EQUAL(count_reports(directory.path()), 50U);
}

SEASTAR_TEST_CASE(crash_recorder_counts_unrelated_entries_toward_scan_bound) {
    temporary_directory directory;
    const auto report_directory = directory.path() / "crash_reports";
    std::filesystem::create_directories(report_directory);
    for (std::size_t index = 0;
         index <= kwaque::broker::crash_recorder::directory_entry_limit;
         ++index) {
        std::ofstream(report_directory / (std::to_string(index) + ".txt"))
          .close();
    }
    seastar::abort_source abort;
    kwaque::broker::crash_recorder recorder;
    bool rejected = false;
    try {
        co_await recorder.start(directory.path(), abort, false);
    } catch (const std::runtime_error& error) {
        rejected = std::string_view(error.what())
                   == "crash report directory entry limit exceeded";
    }
    BOOST_CHECK(rejected);
    co_await recorder.stop();
    BOOST_CHECK_EQUAL(count_reports(directory.path()), 0U);
}

SEASTAR_TEST_CASE(crash_recorder_skips_oversized_report_reads) {
    temporary_directory directory;
    const auto report_directory = directory.path() / "crash_reports";
    std::filesystem::create_directories(report_directory);
    const auto report = report_directory / "oversized.crash";
    std::ofstream(report).close();
    std::filesystem::resize_file(report, 1ULL << 40U);
    seastar::abort_source abort;
    kwaque::broker::crash_recorder recorder;
    co_await recorder.start(directory.path(), abort, false);
    BOOST_CHECK_EQUAL(std::filesystem::file_size(report), 1ULL << 40U);
    co_await recorder.stop();
}

SEASTAR_TEST_CASE(crash_recorder_cancel_before_preparation_creates_nothing) {
    temporary_directory directory;
    seastar::abort_source abort;
    abort.request_abort();
    kwaque::broker::crash_recorder recorder;
    recorder.record_startup_failure();
    bool canceled = false;
    try {
        co_await recorder.start(directory.path(), abort, false);
    } catch (const seastar::abort_requested_exception&) {
        canceled = true;
    }
    BOOST_CHECK(canceled);
    co_await recorder.stop();
    BOOST_CHECK(!std::filesystem::exists(directory.path()));
}

SEASTAR_TEST_CASE(crash_recorder_prepared_recording_does_not_allocate) {
    temporary_directory directory;
    seastar::abort_source abort;
    kwaque::broker::crash_recorder recorder;
    co_await recorder.start(directory.path(), abort, false);
    seastar::backtrace([](const seastar::frame&) noexcept {});
#ifndef SEASTAR_DEFAULT_ALLOCATOR
    const auto before = seastar::memory::stats().mallocs();
#endif
#ifdef SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION
    auto& injector = seastar::memory::local_failure_injector();
    injector.fail_after(0);
#endif
    recorder.record_startup_failure();
#ifdef SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION
    const bool injected = injector.failed();
    injector.cancel();
#endif
#ifndef SEASTAR_DEFAULT_ALLOCATOR
    const auto after = seastar::memory::stats().mallocs();
#endif
#ifdef SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION
    BOOST_CHECK(!injected);
#endif
#ifndef SEASTAR_DEFAULT_ALLOCATOR
    BOOST_CHECK_EQUAL(before, after);
#endif
    // The system allocator still exercises recording and available injection,
    // but its synthetic native statistics cannot prove an allocation count.
    BOOST_CHECK_GT(std::filesystem::file_size(recorder.current_report()), 40U);
    co_await recorder.stop();
}
