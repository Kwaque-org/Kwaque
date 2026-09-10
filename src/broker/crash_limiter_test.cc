#include "src/broker/crash_limiter.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/seastar.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/tmp_file.hh>

#include <boost/test/unit_test.hpp>
#include <crc32c/crc32c.h>

#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

namespace kwaque::broker {

class crash_limiter_test_access final {
public:
    static void set_directory_sync(
      crash_limiter& limiter,
      seastar::future<> (*operation)(const std::filesystem::path&)) {
        limiter.sync_directory_ = operation;
    }
};

} // namespace kwaque::broker

namespace {

using kwaque::broker::crash_limiter;
using kwaque::broker::crash_loop_limit_reached;
using kwaque::broker::detail::configuration_identity;
using kwaque::broker::detail::crash_loop_metadata;
using kwaque::broker::detail::next_crash_count;

constexpr std::string_view tracker_name{".kwaque-crash-loop"};
constexpr std::int64_t test_time = 10'000'000;

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

std::int64_t fixed_time() noexcept { return test_time; }

configuration_identity identity(char digit = 'a') {
    configuration_identity result;
    result.checksum.fill(digit);
    result.bytes = 12;
    return result;
}

crash_loop_metadata previous(std::uint64_t count) {
    return {
      .crash_count = count,
      .configuration_checksum = identity().checksum,
      .last_start_milliseconds = test_time,
    };
}

std::string metadata(
  std::uint64_t count,
  std::int64_t when = test_time,
  char checksum_digit = 'a') {
    auto contents = "kwaque-crash-loop-v1\n" + std::to_string(count) + "\n"
                    + std::to_string(when) + "\n"
                    + std::string(64, checksum_digit) + "\n";
    std::array<char, 8> checksum{};
    const auto result = std::to_chars(
      checksum.data(),
      checksum.data() + checksum.size(),
      crc32c::Crc32c(contents),
      16);
    contents.append(
      checksum.data(), static_cast<std::size_t>(result.ptr - checksum.data()));
    contents += "\n";
    return contents;
}

void write_contents(
  const std::filesystem::path& path, std::string_view contents) {
    std::ofstream stream{path, std::ios::binary | std::ios::trunc};
    stream.exceptions(std::ios::badbit | std::ios::failbit);
    stream.write(
      contents.data(), static_cast<std::streamsize>(contents.size()));
    stream.close();
}

std::string read_contents(const std::filesystem::path& path) {
    std::ifstream stream{path, std::ios::binary};
    stream.exceptions(std::ios::badbit);
    if (!stream.is_open()) {
        throw std::runtime_error("unable to read limiter fixture");
    }
    return {
      std::istreambuf_iterator<char>{stream}, std::istreambuf_iterator<char>{}};
}

template<typename Exception>
seastar::future<> expect_failure(seastar::future<> operation) {
    bool failed = false;
    try {
        co_await std::move(operation);
    } catch (const Exception&) {
        failed = true;
    }
    BOOST_CHECK(failed);
}

} // namespace

SEASTAR_TEST_CASE(crash_limiter_preserves_exact_prior_count_boundary) {
    const auto configuration = identity();
    BOOST_CHECK(
      next_crash_count(std::nullopt, configuration, test_time, 5) == 1);
    BOOST_CHECK(
      next_crash_count(previous(4), configuration, test_time, 5) == 5);
    BOOST_CHECK(
      next_crash_count(previous(5), configuration, test_time, 5) == 6);
    BOOST_CHECK(!next_crash_count(previous(6), configuration, test_time, 5));
    BOOST_CHECK(
      next_crash_count(std::nullopt, configuration, test_time, 0) == 1);
    BOOST_CHECK(
      next_crash_count(previous(0), configuration, test_time, 0) == 1);
    BOOST_CHECK(!next_crash_count(previous(1), configuration, test_time, 0));
    constexpr auto maximum = std::numeric_limits<std::uint32_t>::max();
    BOOST_CHECK(
      next_crash_count(previous(maximum), configuration, test_time, maximum)
      == static_cast<std::uint64_t>(maximum) + 1);
    BOOST_CHECK(!next_crash_count(
      previous(static_cast<std::uint64_t>(maximum) + 1),
      configuration,
      test_time,
      maximum));
    BOOST_CHECK(!next_crash_count(
      previous(std::numeric_limits<std::uint64_t>::max()),
      configuration,
      test_time,
      maximum));
    co_return;
}

SEASTAR_TEST_CASE(crash_limiter_reset_requires_more_than_one_hour) {
    const auto configuration = identity();
    const auto state = previous(6);
    BOOST_CHECK(
      !next_crash_count(state, configuration, test_time + 3'599'999, 5));
    BOOST_CHECK(
      !next_crash_count(state, configuration, test_time + 3'600'000, 5));
    BOOST_CHECK(
      next_crash_count(state, configuration, test_time + 3'600'001, 5) == 1);
    BOOST_CHECK(!next_crash_count(state, configuration, test_time - 1, 5));
    BOOST_CHECK(
      next_crash_count(
        state, configuration, std::numeric_limits<std::int64_t>::max(), 5)
      == 1);
    co_return;
}

SEASTAR_TEST_CASE(crash_limiter_reset_uses_loaded_configuration_checksum) {
    auto configuration = identity();
    configuration.bytes = 100;
    BOOST_CHECK(!next_crash_count(previous(6), configuration, test_time, 5));
    configuration.checksum = identity('b').checksum;
    BOOST_CHECK(
      next_crash_count(previous(6), configuration, test_time, 5) == 1);
    co_return;
}

SEASTAR_TEST_CASE(
  crash_limiter_counts_before_start_and_refuses_without_mutation) {
    seastar::tmp_dir directory;
    co_await directory.create();
    seastar::abort_source abort;
    const auto configuration = identity();
    const auto path = directory.get_path() / tracker_name;
    for (std::uint64_t count = 1; count <= 6; ++count) {
        // Destruction without record_clean_shutdown models a failed start,
        // failed mandatory cleanup, or a process that never finished drain.
        crash_limiter limiter{fixed_time};
        co_await limiter.start(
          directory.get_path(), configuration, false, abort);
        BOOST_CHECK_EQUAL(read_contents(path), metadata(count));
    }
    const auto before = read_contents(path);
    crash_limiter refused{fixed_time};
    co_await expect_failure<crash_loop_limit_reached>(
      refused.start(directory.get_path(), configuration, false, abort));
    co_await refused.record_clean_shutdown();
    BOOST_CHECK_EQUAL(read_contents(path), before);
    co_await directory.remove();
}

SEASTAR_TEST_CASE(crash_limiter_clean_shutdown_resets_and_is_idempotent) {
    seastar::tmp_dir directory;
    co_await directory.create();
    seastar::abort_source abort;
    const auto configuration = identity();
    const auto path = directory.get_path() / tracker_name;
    crash_limiter started{fixed_time};
    co_await started.start(
      directory.get_path(), configuration, false, abort, 0);
    BOOST_CHECK_EQUAL(read_contents(path), metadata(1));
    co_await started.record_clean_shutdown();
    co_await started.record_clean_shutdown();
    BOOST_CHECK(!std::filesystem::exists(path));
    crash_limiter restarted{fixed_time};
    co_await restarted.start(
      directory.get_path(), configuration, false, abort, 0);
    BOOST_CHECK_EQUAL(read_contents(path), metadata(1));
    co_await restarted.record_clean_shutdown();
    co_await directory.remove();
}

SEASTAR_TEST_CASE(crash_limiter_time_and_configuration_reset_persist_one) {
    seastar::tmp_dir directory;
    co_await directory.create();
    const auto path = directory.get_path() / tracker_name;
    const auto configuration = identity();
    seastar::abort_source abort;
    write_contents(path, metadata(6, test_time - 3'600'000));
    crash_limiter exact{fixed_time};
    co_await expect_failure<crash_loop_limit_reached>(
      exact.start(directory.get_path(), configuration, false, abort));
    write_contents(path, metadata(6, test_time - 3'600'001));
    crash_limiter expired{fixed_time};
    co_await expired.start(directory.get_path(), configuration, false, abort);
    BOOST_CHECK_EQUAL(read_contents(path), metadata(1));
    write_contents(path, metadata(6));
    crash_limiter changed{fixed_time};
    const auto changed_configuration = identity('b');
    co_await changed.start(
      directory.get_path(), changed_configuration, false, abort);
    BOOST_CHECK_EQUAL(read_contents(path), metadata(1, test_time, 'b'));
    co_await changed.record_clean_shutdown();
    co_await directory.remove();
}

SEASTAR_TEST_CASE(crash_limiter_malformed_and_oversized_metadata_reset) {
    seastar::tmp_dir directory;
    co_await directory.create();
    const auto path = directory.get_path() / tracker_name;
    const auto configuration = identity();
    seastar::abort_source abort;
    const std::array malformed{
      std::string{},
      std::string{"unknown version\n"},
      metadata(6).substr(0, 20),
      metadata(6) + "unexpected\n",
      metadata(6, -1),
      metadata(6, test_time, 'z'),
      std::string{"kwaque-crash-loop-v1\n18446744073709551616\n0\n"}
        + std::string(64, 'a') + "\n",
    };
    for (const auto& contents : malformed) {
        write_contents(path, contents);
        crash_limiter limiter{fixed_time};
        co_await limiter.start(
          directory.get_path(), configuration, false, abort);
        BOOST_CHECK_EQUAL(read_contents(path), metadata(1));
    }
    // A sparse operator-created file must never determine read allocation size.
    std::filesystem::resize_file(path, 1ULL << 40);
    crash_limiter oversized{fixed_time};
    co_await oversized.start(directory.get_path(), configuration, false, abort);
    BOOST_CHECK_EQUAL(read_contents(path), metadata(1));
    co_await oversized.record_clean_shutdown();
    co_await directory.remove();
}

SEASTAR_TEST_CASE(
  crash_limiter_developer_mode_bypasses_start_but_cleans_tracker) {
    seastar::tmp_dir directory;
    co_await directory.create();
    const auto path = directory.get_path() / tracker_name;
    const auto configuration = identity();
    seastar::abort_source abort;
    write_contents(path, metadata(100));
    const auto before = read_contents(path);
    crash_limiter development{fixed_time};
    co_await development.start(
      directory.get_path(), configuration, true, abort);
    BOOST_CHECK_EQUAL(read_contents(path), before);
    co_await development.record_clean_shutdown();
    BOOST_CHECK(!std::filesystem::exists(path));
    crash_limiter no_directory{fixed_time};
    co_await no_directory.start(
      directory.get_path() / "absent", configuration, true, abort);
    co_await no_directory.record_clean_shutdown();
    BOOST_CHECK(!std::filesystem::exists(directory.get_path() / "absent"));
    co_await directory.remove();
}

SEASTAR_TEST_CASE(
  crash_limiter_null_limit_tracks_and_saturates_without_refusal) {
    const auto maximum = std::numeric_limits<std::uint64_t>::max();
    const auto configuration = identity();
    BOOST_CHECK(
      next_crash_count(previous(6), configuration, test_time, std::nullopt)
      == 7);
    BOOST_CHECK(
      next_crash_count(
        previous(maximum), configuration, test_time, std::nullopt)
      == maximum);
    BOOST_CHECK(
      !next_crash_count(previous(maximum), configuration, test_time, 5));
    seastar::tmp_dir directory;
    co_await directory.create();
    const auto path = directory.get_path() / tracker_name;
    seastar::abort_source abort;
    for (const auto count :
         std::array<std::uint64_t, 3>{6, maximum - 1, maximum}) {
        write_contents(path, metadata(count));
        crash_limiter limiter{fixed_time};
        co_await limiter.start(
          directory.get_path(), configuration, false, abort, std::nullopt);
        BOOST_CHECK_EQUAL(
          read_contents(path),
          metadata(count == maximum ? maximum : count + 1));
        co_await limiter.record_clean_shutdown();
    }
    co_await directory.remove();
}

SEASTAR_TEST_CASE(crash_limiter_retries_directory_sync_after_completed_unlink) {
    seastar::tmp_dir directory;
    co_await directory.create();
    seastar::abort_source abort;
    const auto configuration = identity();
    crash_limiter limiter{fixed_time};
    co_await limiter.start(directory.get_path(), configuration, false, abort);
    directory_sync_calls = 0;
    directory_sync_result.emplace(
      seastar::make_exception_future<>(
        std::system_error(std::make_error_code(std::errc::io_error))));
    kwaque::broker::crash_limiter_test_access::set_directory_sync(
      limiter, scripted_directory_sync);
    co_await expect_failure<std::system_error>(limiter.record_clean_shutdown());
    BOOST_CHECK_EQUAL(directory_sync_calls, 1U);
    BOOST_CHECK(!std::filesystem::exists(directory.get_path() / tracker_name));
    co_await limiter.record_clean_shutdown();
    BOOST_CHECK_EQUAL(directory_sync_calls, 2U);
    co_await limiter.record_clean_shutdown();
    BOOST_CHECK_EQUAL(directory_sync_calls, 2U);
    co_await directory.remove();
}

SEASTAR_TEST_CASE(crash_limiter_serializes_concurrent_clean_bookkeeping) {
    seastar::tmp_dir directory;
    co_await directory.create();
    seastar::abort_source abort;
    const auto configuration = identity();
    crash_limiter limiter{fixed_time};
    co_await limiter.start(directory.get_path(), configuration, false, abort);
    seastar::promise<> release;
    directory_sync_calls = 0;
    directory_sync_result.emplace(release.get_future());
    kwaque::broker::crash_limiter_test_access::set_directory_sync(
      limiter, scripted_directory_sync);
    auto first = limiter.record_clean_shutdown();
    auto second = limiter.record_clean_shutdown();
    BOOST_CHECK(!first.available());
    BOOST_CHECK(!second.available());
    release.set_value();
    co_await std::move(first);
    co_await std::move(second);
    BOOST_CHECK_EQUAL(directory_sync_calls, 1U);
    co_await directory.remove();
}

SEASTAR_TEST_CASE(crash_limiter_abort_before_start_does_not_create_metadata) {
    seastar::tmp_dir directory;
    co_await directory.create();
    const auto configuration = identity();
    seastar::abort_source abort;
    abort.request_abort();
    crash_limiter canceled{fixed_time};
    co_await expect_failure<seastar::abort_requested_exception>(
      canceled.start(directory.get_path(), configuration, false, abort));
    co_await canceled.record_clean_shutdown();
    BOOST_CHECK(!std::filesystem::exists(directory.get_path() / tracker_name));
    co_await directory.remove();
}

SEASTAR_TEST_CASE(crash_limiter_rejects_unsafe_files_without_altering_targets) {
    seastar::tmp_dir directory;
    co_await directory.create();
    const auto path = directory.get_path() / tracker_name;
    const auto target = directory.get_path() / "preserve";
    const auto configuration = identity();
    seastar::abort_source abort;
    write_contents(target, "preserved");
    std::filesystem::create_symlink(target, path);
    crash_limiter linked{fixed_time};
    co_await expect_failure<std::system_error>(
      linked.start(directory.get_path(), configuration, false, abort));
    BOOST_CHECK_EQUAL(read_contents(target), "preserved");
    std::filesystem::remove(path);
    std::filesystem::create_hard_link(target, path);
    crash_limiter hard_linked{fixed_time};
    co_await expect_failure<std::runtime_error>(
      hard_linked.start(directory.get_path(), configuration, false, abort));
    BOOST_CHECK_EQUAL(read_contents(target), "preserved");
    std::filesystem::remove(path);
    std::filesystem::create_directory(path);
    crash_limiter non_regular{fixed_time};
    co_await expect_failure<std::system_error>(
      non_regular.start(directory.get_path(), configuration, false, abort));
    BOOST_CHECK(std::filesystem::is_directory(path));
    co_await directory.remove();
}

SEASTAR_TEST_CASE(
  crash_limiter_failed_clean_bookkeeping_propagates_and_can_retry) {
    seastar::tmp_dir directory;
    co_await directory.create();
    const auto path = directory.get_path() / tracker_name;
    const auto retained = directory.get_path() / "retained";
    const auto configuration = identity();
    seastar::abort_source abort;
    crash_limiter limiter{fixed_time};
    co_await limiter.start(directory.get_path(), configuration, false, abort);
    std::filesystem::rename(path, retained);
    std::filesystem::create_directory(path);
    co_await expect_failure<std::runtime_error>(
      limiter.record_clean_shutdown());
    BOOST_CHECK_EQUAL(read_contents(retained), metadata(1));
    std::filesystem::remove(path);
    std::filesystem::rename(retained, path);
    co_await limiter.record_clean_shutdown();
    BOOST_CHECK(!std::filesystem::exists(path));
    co_await directory.remove();
}

SEASTAR_TEST_CASE(crash_limiter_rejects_repeated_start_and_invalid_identity) {
    seastar::tmp_dir directory;
    co_await directory.create();
    seastar::abort_source abort;
    const auto configuration = identity();
    crash_limiter limiter{fixed_time};
    co_await limiter.start(directory.get_path(), configuration, false, abort);
    co_await expect_failure<std::logic_error>(
      limiter.start(directory.get_path(), configuration, false, abort));
    BOOST_CHECK_EQUAL(
      read_contents(directory.get_path() / tracker_name), metadata(1));
    co_await limiter.record_clean_shutdown();
    const configuration_identity invalid;
    crash_limiter rejected{fixed_time};
    co_await expect_failure<std::invalid_argument>(
      rejected.start(directory.get_path(), invalid, false, abort));
    BOOST_CHECK(!std::filesystem::exists(directory.get_path() / tracker_name));
    co_await directory.remove();
}

SEASTAR_TEST_CASE(
  crash_limiter_integrity_check_rejects_valid_looking_corruption) {
    seastar::tmp_dir directory;
    co_await directory.create();
    const auto path = directory.get_path() / tracker_name;
    const auto configuration = identity();
    seastar::abort_source abort;
    auto corrupted_count = metadata(6);
    corrupted_count[std::string_view{"kwaque-crash-loop-v1\n"}.size()] = '7';
    auto corrupted_timestamp = metadata(6);
    corrupted_timestamp[std::string_view{"kwaque-crash-loop-v1\n6\n"}.size()]
      = '2';
    auto corrupted_crc = metadata(6);
    corrupted_crc[corrupted_crc.size() - 2]
      = corrupted_crc[corrupted_crc.size() - 2] == '0' ? '1' : '0';
    for (const auto& contents :
         {corrupted_count, corrupted_timestamp, corrupted_crc}) {
        write_contents(path, contents);
        crash_limiter limiter{fixed_time};
        co_await limiter.start(
          directory.get_path(), configuration, false, abort);
        BOOST_CHECK_EQUAL(read_contents(path), metadata(1));
    }
    co_await directory.remove();
}
