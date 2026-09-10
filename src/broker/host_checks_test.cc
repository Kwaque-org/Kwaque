#include "src/broker/host_checks.h"

#include <seastar/core/coroutine.hh>
#include <seastar/testing/test_case.hh>

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <utility>

namespace {

using kwaque::broker::host_check_report;
using kwaque::broker::host_check_severity;
using kwaque::broker::detail::evaluate_host_checks;
using kwaque::broker::detail::host_snapshot;
using kwaque::broker::detail::parse_cpu_count;
using kwaque::broker::detail::parse_cpu_quota;

const kwaque::broker::host_check_result&
find(const host_check_report& report, std::string_view name) {
    const auto entry = std::ranges::find(
      report.checks, name, &kwaque::broker::host_check_result::name);
    if (entry == report.checks.end()) {
        throw std::logic_error("missing host check");
    }
    return *entry;
}

class kernel_fixture final {
public:
    kernel_fixture()
      : root_(std::filesystem::temp_directory_path() /
          ("kwaque-host-test-" + std::to_string(::getpid()) + "-" +
           std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()))) {
        std::filesystem::create_directories(root_);
    }
    ~kernel_fixture() {
        std::error_code ignored;
        std::filesystem::remove_all(root_, ignored);
    }
    const std::filesystem::path& root() const noexcept { return root_; }
    void write(std::string_view path, std::string_view contents) const {
        const auto target = root_ / std::filesystem::path(path).relative_path();
        std::filesystem::create_directories(target.parent_path());
        std::ofstream output(target);
        output << contents;
        output.close();
        BOOST_REQUIRE(output.good());
    }
    std::string read(std::string_view path) const {
        const auto target = root_ / std::filesystem::path(path).relative_path();
        std::ifstream input(target);
        return {std::istreambuf_iterator<char>(input), {}};
    }

private:
    std::filesystem::path root_;
};

} // namespace

SEASTAR_TEST_CASE(host_checks_classify_filesystems_without_blanket_rejection) {
    host_snapshot snapshot;
    const std::array cases{
      std::pair{seastar::fs_type::xfs, host_check_severity::info},
      std::pair{seastar::fs_type::ext2, host_check_severity::warning},
      std::pair{seastar::fs_type::ext4, host_check_severity::warning},
      std::pair{seastar::fs_type::tmpfs, host_check_severity::error},
      std::pair{seastar::fs_type::other, host_check_severity::error},
    };
    for (const auto& [type, expected] : cases) {
        snapshot.filesystem = type;
        const auto report = evaluate_host_checks(snapshot);
        BOOST_CHECK(find(report, "filesystem").severity == expected);
    }
    co_return;
}

SEASTAR_TEST_CASE(host_checks_parse_bounded_cpu_ranges_and_reject_bad_input) {
    BOOST_CHECK(parse_cpu_count("0-3,8,10-11\n") == 7U);
    BOOST_CHECK(parse_cpu_count("0") == 1U);
    for (const auto text :
         {"",
          "0,",
          "-1",
          "3-1",
          "0-3,2",
          "foo",
          "0--3",
          "0-1048576",
          "18446744073709551615"}) {
        BOOST_CHECK(!parse_cpu_count(text));
    }
    BOOST_CHECK(parse_cpu_quota("150000 100000\n") == 1500U);
    BOOST_CHECK(
      parse_cpu_quota("max 100000")
      == std::numeric_limits<std::uint64_t>::max());
    BOOST_CHECK(!parse_cpu_quota("100000 0"));
    BOOST_CHECK(!parse_cpu_quota("-1 100000"));
    BOOST_CHECK(!parse_cpu_quota("18446744073709551615 1"));
    co_return;
}

SEASTAR_TEST_CASE(host_checks_keep_host_memory_recommendation_separate) {
    host_snapshot snapshot;
    snapshot.physical_memory_bytes = 16ULL * 1024U * 1024U * 1024U;
    snapshot.cgroup_memory_bytes = 8ULL * 1024U * 1024U * 1024U;
    snapshot.cgroup_cpu_count = 4U;
    snapshot.cgroup_cpu_quota_millicores = 1000U;
    auto report = evaluate_host_checks(snapshot);
    BOOST_CHECK_EQUAL(
      find(report, "host_memory_mib_per_cgroup_cpu").observed, "2048");
    BOOST_CHECK(
      find(report, "host_memory_mib_per_cgroup_cpu").severity
      == host_check_severity::info);
    --*snapshot.cgroup_memory_bytes;
    report = evaluate_host_checks(snapshot);
    BOOST_CHECK(
      find(report, "host_memory_mib_per_cgroup_cpu").severity
      == host_check_severity::warning);
    snapshot.cgroup_memory_bytes.reset();
    report = evaluate_host_checks(snapshot);
    BOOST_CHECK_EQUAL(
      find(report, "host_memory_mib_per_cgroup_cpu").observed, "unobserved");
    co_return;
}

SEASTAR_TEST_CASE(
  host_checks_calibration_requires_matching_finite_device_rates) {
    host_snapshot snapshot;
    const std::array cases{
      std::array{false, false, false, false},
      std::array{true, false, false, false},
      std::array{true, true, false, false},
      std::array{true, true, true, true},
    };
    for (const auto& values : cases) {
        snapshot.io_calibration_configured = values[0];
        snapshot.io_device_match = values[1];
        snapshot.io_rates_finite = values[2];
        const auto report = evaluate_host_checks(snapshot);
        BOOST_CHECK(
          (find(report, "io_calibration_device").severity
           == host_check_severity::info)
          == values[3]);
    }
    co_return;
}

SEASTAR_TEST_CASE(
  host_checks_tuning_and_swap_reporting_have_explicit_severity) {
    host_snapshot snapshot;
    snapshot.swap_bytes = 0;
    snapshot.swappiness = 1;
    snapshot.aio_max_nr = 10'000'137;
    snapshot.hugepages = "always [madvise] never\n";
    auto report = evaluate_host_checks(snapshot);
    for (const auto name :
         {"swap_bytes", "swappiness", "aio_max_nr", "transparent_hugepages"}) {
        BOOST_CHECK(find(report, name).severity == host_check_severity::info);
    }
    snapshot.swap_bytes = 1024;
    snapshot.swappiness = 60;
    snapshot.aio_max_nr = 65536;
    snapshot.hugepages = "always madvise [never]";
    report = evaluate_host_checks(snapshot);
    BOOST_CHECK(
      find(report, "swap_bytes").severity == host_check_severity::info);
    for (const auto name :
         {"swappiness", "aio_max_nr", "transparent_hugepages"}) {
        BOOST_CHECK(
          find(report, name).severity == host_check_severity::warning);
    }
    co_return;
}

SEASTAR_TEST_CASE(host_checks_readonly_fixture_has_bounded_file_reads) {
    kernel_fixture fixture;
    fixture.write("/proc/value", "1024\n");
    BOOST_CHECK(
      kwaque::broker::detail::read_host_file(fixture.root(), "/proc/value")
      == "1024\n");
    BOOST_CHECK_EQUAL(fixture.read("/proc/value"), "1024\n");
    fixture.write("/proc/large", std::string(16385, 'x'));
    BOOST_CHECK(
      !kwaque::broker::detail::read_host_file(fixture.root(), "/proc/large"));
    BOOST_CHECK(
      !kwaque::broker::detail::read_host_file(fixture.root(), "/proc/missing"));
    BOOST_CHECK(!std::filesystem::exists(fixture.root() / "proc/missing"));
    BOOST_CHECK(
      !kwaque::broker::detail::read_host_file(fixture.root(), "/proc"));
    co_return;
}

SEASTAR_TEST_CASE(host_checks_respect_cgroup_ancestors_and_do_not_tune) {
    kernel_fixture fixture;
    fixture.write("/proc/self/cgroup", "0::/broker\n");
    fixture.write("/sys/fs/cgroup/broker/cpuset.cpus.effective", "0-3\n");
    fixture.write("/sys/fs/cgroup/broker/memory.max", "max\n");
    fixture.write("/sys/fs/cgroup/memory.max", "8589934592\n");
    fixture.write("/sys/fs/cgroup/broker/cpu.max", "max 100000\n");
    fixture.write("/sys/fs/cgroup/cpu.max", "150000 100000\n");
    fixture.write("/proc/sys/vm/swappiness", "60\n");
    seastar::abort_source abort;
    const auto report = co_await kwaque::broker::inspect_host(
      fixture.root(), true, abort, fixture.root());
    BOOST_CHECK_EQUAL(
      find(report, "cgroup_memory_limit_bytes").observed, "8589934592");
    BOOST_CHECK_EQUAL(
      find(report, "cgroup_effective_cpuset_cpus").observed, "4");
    BOOST_CHECK_EQUAL(
      find(report, "cgroup_cpu_quota_millicores").observed, "1500");
    BOOST_CHECK_EQUAL(fixture.read("/proc/sys/vm/swappiness"), "60\n");
    BOOST_CHECK(
      !std::filesystem::exists(fixture.root() / "proc/sys/fs/aio-max-nr"));
}

SEASTAR_TEST_CASE(host_checks_reject_unsafe_cgroup_membership) {
    kernel_fixture fixture;
    fixture.write("/proc/self/cgroup", "0::/../outside\n");
    seastar::abort_source abort;
    const auto report = co_await kwaque::broker::inspect_host(
      fixture.root(), false, abort, fixture.root());
    BOOST_CHECK_EQUAL(
      find(report, "cgroup_memory_limit_bytes").observed, "unobserved");
    BOOST_CHECK_EQUAL(
      find(report, "cgroup_effective_cpuset_cpus").observed, "unobserved");
}

SEASTAR_TEST_CASE(host_checks_missing_child_limits_do_not_inherit_a_pass) {
    kernel_fixture fixture;
    fixture.write("/proc/self/cgroup", "0::/broker\n");
    fixture.write("/sys/fs/cgroup/broker/cpuset.cpus.effective", "0\n");
    fixture.write("/sys/fs/cgroup/memory.max", "max\n");
    fixture.write("/sys/fs/cgroup/cpu.max", "max 100000\n");
    seastar::abort_source abort;
    const auto report = co_await kwaque::broker::inspect_host(
      fixture.root(), false, abort, fixture.root());
    for (const auto name :
         {"cgroup_memory_limit_bytes",
          "cgroup_cpu_quota_millicores",
          "host_memory_mib_per_cgroup_cpu"}) {
        BOOST_CHECK_EQUAL(find(report, name).observed, "unobserved");
        BOOST_CHECK(
          find(report, name).severity == host_check_severity::warning);
    }
}

SEASTAR_TEST_CASE(host_checks_v2_root_absence_differs_from_unreadable_root) {
    kernel_fixture fixture;
    fixture.write("/proc/self/cgroup", "0::/broker\n");
    fixture.write("/sys/fs/cgroup/broker/memory.max", "1073741824\n");
    fixture.write("/sys/fs/cgroup/broker/cpu.max", "50000 100000\n");
    seastar::abort_source abort;
    const auto complete = co_await kwaque::broker::inspect_host(
      fixture.root(), false, abort, fixture.root());
    BOOST_CHECK_EQUAL(
      find(complete, "cgroup_memory_limit_bytes").observed, "1073741824");
    BOOST_CHECK_EQUAL(
      find(complete, "cgroup_cpu_quota_millicores").observed, "500");
    fixture.write("/sys/fs/cgroup/memory.max", std::string(16385, '1'));
    fixture.write("/sys/fs/cgroup/cpu.max", "invalid\n");
    const auto incomplete = co_await kwaque::broker::inspect_host(
      fixture.root(), false, abort, fixture.root());
    BOOST_CHECK_EQUAL(
      find(incomplete, "cgroup_memory_limit_bytes").observed, "unobserved");
    BOOST_CHECK_EQUAL(
      find(incomplete, "cgroup_cpu_quota_millicores").observed, "unobserved");
}

SEASTAR_TEST_CASE(host_checks_read_v1_resource_controllers) {
    kernel_fixture fixture;
    fixture.write(
      "/sys/fs/cgroup/cpuset/broker/cpuset.effective_cpus", "0-1\n");
    fixture.write(
      "/sys/fs/cgroup/memory/broker/memory.limit_in_bytes", "1073741824\n");
    fixture.write(
      "/sys/fs/cgroup/memory/memory.limit_in_bytes", "2147483648\n");
    fixture.write("/sys/fs/cgroup/cpu/broker/cpu.cfs_quota_us", "-1\n");
    fixture.write("/sys/fs/cgroup/cpu/broker/cpu.cfs_period_us", "100000\n");
    fixture.write("/sys/fs/cgroup/cpu/cpu.cfs_quota_us", "50000\n");
    fixture.write("/sys/fs/cgroup/cpu/cpu.cfs_period_us", "100000\n");
    const std::string controllers{
      "4:cpuset:/broker\n3:memory:/broker\n2:cpu,cpuacct:/broker\n"};
    for (const auto& membership :
         {controllers, "0::/\n" + controllers, controllers + "0::/\n"}) {
        fixture.write("/proc/self/cgroup", membership);
        seastar::abort_source abort;
        const auto report = co_await kwaque::broker::inspect_host(
          fixture.root(), false, abort, fixture.root());
        BOOST_CHECK_EQUAL(
          find(report, "cgroup_version").observed, "v1-or-hybrid");
        BOOST_CHECK_EQUAL(
          find(report, "cgroup_memory_limit_bytes").observed, "1073741824");
        BOOST_CHECK_EQUAL(
          find(report, "cgroup_effective_cpuset_cpus").observed, "2");
        BOOST_CHECK_EQUAL(
          find(report, "cgroup_cpu_quota_millicores").observed, "500");
    }
}

SEASTAR_TEST_CASE(host_checks_select_hybrid_hierarchy_per_controller) {
    kernel_fixture fixture;
    fixture.write(
      "/proc/self/cgroup",
      "0::/unified\n3:memory:/broker\n2:cpu,cpuacct:/broker\n");
    fixture.write("/sys/fs/cgroup/unified/cpuset.cpus.effective", "0-3\n");
    fixture.write(
      "/sys/fs/cgroup/memory/broker/memory.limit_in_bytes", "1073741824\n");
    fixture.write(
      "/sys/fs/cgroup/memory/memory.limit_in_bytes", "2147483648\n");
    fixture.write("/sys/fs/cgroup/cpu/broker/cpu.cfs_quota_us", "-1\n");
    fixture.write("/sys/fs/cgroup/cpu/broker/cpu.cfs_period_us", "100000\n");
    fixture.write("/sys/fs/cgroup/cpu/cpu.cfs_quota_us", "-1\n");
    fixture.write("/sys/fs/cgroup/cpu/cpu.cfs_period_us", "100000\n");
    seastar::abort_source abort;
    const auto report = co_await kwaque::broker::inspect_host(
      fixture.root(), false, abort, fixture.root());
    BOOST_CHECK_EQUAL(find(report, "cgroup_version").observed, "v1-or-hybrid");
    BOOST_CHECK_EQUAL(
      find(report, "cgroup_effective_cpuset_cpus").observed, "4");
    BOOST_CHECK_EQUAL(
      find(report, "cgroup_memory_limit_bytes").observed, "1073741824");
    BOOST_CHECK_EQUAL(
      find(report, "cgroup_cpu_quota_millicores").observed, "unlimited");

    fixture.write("/proc/self/cgroup", "0::/unified\n4:cpuset:/broker\n");
    fixture.write(
      "/sys/fs/cgroup/cpuset/broker/cpuset.effective_cpus", "0-1\n");
    fixture.write("/sys/fs/cgroup/unified/memory.max", "max\n");
    fixture.write("/sys/fs/cgroup/memory.max", "max\n");
    fixture.write("/sys/fs/cgroup/unified/cpu.max", "max 100000\n");
    fixture.write("/sys/fs/cgroup/cpu.max", "max 100000\n");
    const auto unlimited = co_await kwaque::broker::inspect_host(
      fixture.root(), false, abort, fixture.root());
    BOOST_CHECK_EQUAL(
      find(unlimited, "cgroup_version").observed, "v1-or-hybrid");
    BOOST_CHECK_EQUAL(
      find(unlimited, "cgroup_effective_cpuset_cpus").observed, "2");
    for (const auto name :
         {"cgroup_memory_limit_bytes", "cgroup_cpu_quota_millicores"}) {
        BOOST_CHECK_EQUAL(find(unlimited, name).observed, "unlimited");
        BOOST_CHECK(
          find(unlimited, name).severity == host_check_severity::info);
    }
}

SEASTAR_TEST_CASE(host_checks_do_not_replace_missing_v1_observations_with_v2) {
    kernel_fixture fixture;
    fixture.write(
      "/proc/self/cgroup",
      "0::/\n4:cpuset:/broker\n3:memory:/broker\n2:cpu,cpuacct:/broker\n");
    // Readable files outside each controller's declared v1 hierarchy cannot
    // certify a controller whose own observation is missing or malformed.
    fixture.write("/sys/fs/cgroup/cpuset.cpus.effective", "0-7\n");
    fixture.write("/sys/fs/cgroup/memory.max", "max\n");
    fixture.write("/sys/fs/cgroup/cpu.max", "max 100000\n");
    for (const bool malformed : {false, true}) {
        if (malformed) {
            fixture.write(
              "/sys/fs/cgroup/cpuset/broker/cpuset.effective_cpus",
              "invalid\n");
            fixture.write(
              "/sys/fs/cgroup/memory/broker/memory.limit_in_bytes",
              "invalid\n");
            fixture.write(
              "/sys/fs/cgroup/cpu/broker/cpu.cfs_quota_us", "invalid\n");
            fixture.write(
              "/sys/fs/cgroup/cpu/broker/cpu.cfs_period_us", "100000\n");
        }
        seastar::abort_source abort;
        const auto report = co_await kwaque::broker::inspect_host(
          fixture.root(), false, abort, fixture.root());
        BOOST_CHECK_EQUAL(
          find(report, "cgroup_version").observed, "v1-or-hybrid");
        for (const auto name :
             {"cgroup_effective_cpuset_cpus",
              "cgroup_memory_limit_bytes",
              "cgroup_cpu_quota_millicores"}) {
            BOOST_CHECK_EQUAL(find(report, name).observed, "unobserved");
            BOOST_CHECK(
              find(report, name).severity == host_check_severity::warning);
        }
    }
}

SEASTAR_TEST_CASE(host_checks_do_not_certify_truncated_cgroup_ancestry) {
    kernel_fixture fixture;
    std::string group;
    for (unsigned depth = 0; depth < 33U; ++depth) {
        group += "/child";
        fixture.write("/sys/fs/cgroup" + group + "/memory.max", "1073741824\n");
    }
    fixture.write("/proc/self/cgroup", "0::" + group + "\n");
    seastar::abort_source abort;
    const auto report = co_await kwaque::broker::inspect_host(
      fixture.root(), false, abort, fixture.root());
    BOOST_CHECK_EQUAL(
      find(report, "cgroup_memory_limit_bytes").observed, "unobserved");
}

SEASTAR_TEST_CASE(host_checks_abort_before_observation) {
    kernel_fixture fixture;
    seastar::abort_source abort;
    abort.request_abort();
    bool canceled = false;
    try {
        static_cast<void>(co_await kwaque::broker::inspect_host(
          fixture.root(), false, abort, fixture.root()));
    } catch (const seastar::abort_requested_exception&) {
        canceled = true;
    }
    BOOST_CHECK(canceled);
    BOOST_CHECK(std::filesystem::is_empty(fixture.root()));
}
