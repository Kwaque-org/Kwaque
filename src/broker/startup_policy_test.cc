#include "src/base/build_info.h"
#include "src/base/units.h"
#include "src/broker/startup_policy.h"

#include <seastar/core/memory.hh>
#include <seastar/util/std-compat.hh>

#include <boost/program_options.hpp>
#include <gtest/gtest.h>

#include <cstddef>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>

namespace {

namespace bpo = boost::program_options;
using kwaque::broker::detail::admin_is_loopback;
using kwaque::broker::detail::configure_allocation_failure_policy;
using kwaque::broker::detail::identify_configuration;
using kwaque::broker::detail::render_startup_policy;
using kwaque::broker::detail::validate_broker_profile;
using kwaque::broker::detail::validate_runtime_configuration;

std::string_view policy_field(std::string_view policy, std::string_view name) {
    const std::string prefix = std::string(name) + "=";
    std::size_t offset = policy.starts_with(prefix) ? 0
                                                    : policy.find(" " + prefix);
    if (offset == std::string_view::npos) {
        return {};
    }
    if (offset != 0) {
        ++offset;
    }
    offset += prefix.size();
    const auto end = policy.find(' ', offset);
    return policy.substr(
      offset, end == std::string_view::npos ? end : end - offset);
}

kwaque::resource::resource_config test_resources() {
    return *kwaque::resource::resource_config::from_total_memory(
      kwaque::byte_count{128ULL * 1024ULL * 1024ULL});
}

TEST(StartupPolicyTest, PresetsOnlySupportedNativeAllocationFailureMode) {
    seastar::app_template::seastar_options options;
    configure_allocation_failure_policy(options);
#if defined(SEASTAR_DEFAULT_ALLOCATOR)
    EXPECT_FALSE(options.reactor_opts.abort_on_seastar_bad_alloc);
#else
    EXPECT_TRUE(options.reactor_opts.abort_on_seastar_bad_alloc);
#endif
    EXPECT_DOUBLE_EQ(options.reactor_opts.task_quota_ms.get_value(), 0.5);
    EXPECT_FALSE(options.reactor_opts.unsafe_bypass_fsync.get_value());
    EXPECT_FALSE(options.reactor_opts.kernel_page_cache.get_value());
}

TEST(StartupPolicyTest, SystemAllocatorRequiresExplicitDevelopmentProfile) {
    kwaque::config::bootstrap_config configuration;
#if defined(SEASTAR_DEFAULT_ALLOCATOR)
    EXPECT_THROW(validate_broker_profile(configuration), std::runtime_error);
#else
    EXPECT_NO_THROW(validate_broker_profile(configuration));
#endif
    configuration.developer_mode = true;
#if defined(SEASTAR_DEFAULT_ALLOCATOR)
    EXPECT_THROW(validate_broker_profile(configuration), std::runtime_error);
#endif
    configuration.diagnostic_memory_per_shard_bytes = 134217728U;
    EXPECT_NO_THROW(validate_broker_profile(configuration));
}

TEST(StartupPolicyTest, ExplicitNativeOomSwitchRequiresAllocatorSupport) {
    bpo::variables_map options;
    options.emplace("abort-on-seastar-bad-alloc", bpo::variable_value{});
#if defined(SEASTAR_DEFAULT_ALLOCATOR)
    EXPECT_THROW(validate_runtime_configuration(options), bpo::error);
#else
    EXPECT_NO_THROW(validate_runtime_configuration(options));
#endif
}

TEST(StartupPolicyTest, RejectsUnsafeBooleanValuesThroughNativeErrorType) {
    for (const char* name : {"unsafe-bypass-fsync", "kernel-page-cache"}) {
        SCOPED_TRACE(name);
        for (const bool defaulted : {false, true}) {
            bpo::variables_map options;
            options.emplace(name, bpo::variable_value(true, defaulted));
            try {
                validate_runtime_configuration(options);
                FAIL() << "unsafe runtime option was accepted";
            } catch (const bpo::error& error) {
                EXPECT_EQ(
                  std::string(error.what()),
                  std::string(name) + " is not supported by the broker");
            }
        }
    }
}

TEST(StartupPolicyTest, AcceptsAbsentAndFalseRuntimeBooleansWithoutMutation) {
    bpo::variables_map options;
    EXPECT_NO_THROW(validate_runtime_configuration(options));
    EXPECT_TRUE(options.empty());
    for (const bool defaulted : {false, true}) {
        options.clear();
        options.emplace(
          "unsafe-bypass-fsync", bpo::variable_value(false, defaulted));
        options.emplace(
          "kernel-page-cache", bpo::variable_value(false, defaulted));
        EXPECT_NO_THROW(validate_runtime_configuration(options));
        EXPECT_EQ(options.size(), 2U);
        EXPECT_FALSE(options["unsafe-bypass-fsync"].as<bool>());
        EXPECT_FALSE(options["kernel-page-cache"].as<bool>());
    }
}

TEST(StartupPolicyTest, RejectsRelaxedDmaByPresence) {
    bpo::variables_map options;
    options.emplace("relaxed-dma", bpo::variable_value{});
    EXPECT_THROW(validate_runtime_configuration(options), bpo::error);
    options.clear();
    options.emplace("relaxed-dma", bpo::variable_value(false, false));
    EXPECT_THROW(validate_runtime_configuration(options), bpo::error);
}

TEST(StartupPolicyTest, MalformedBooleanCannotEscapeNativeErrorBoundary) {
    bpo::variables_map options;
    options.emplace(
      "unsafe-bypass-fsync", bpo::variable_value(std::string("true"), false));
    EXPECT_THROW(validate_runtime_configuration(options), bpo::error);
}

TEST(StartupPolicyTest, PreservesExplicitIoSourcesAndRejectsConflicts) {
    bpo::variables_map options;
    options.emplace(
      "io-properties", bpo::variable_value(std::string("disks: []"), false));
    EXPECT_NO_THROW(validate_runtime_configuration(options));
    EXPECT_EQ(options["io-properties"].as<std::string>(), "disks: []");
    options.clear();
    options.emplace(
      "io-properties-file",
      bpo::variable_value(std::string("/explicit/io.yaml"), false));
    EXPECT_NO_THROW(validate_runtime_configuration(options));
    EXPECT_EQ(
      options["io-properties-file"].as<std::string>(), "/explicit/io.yaml");
    options.emplace(
      "io-properties", bpo::variable_value(std::string("disks: []"), false));
    try {
        validate_runtime_configuration(options);
        FAIL() << "conflicting I/O sources were accepted";
    } catch (const bpo::error& error) {
        EXPECT_STREQ(
          error.what(),
          "io-properties and io-properties-file cannot be used together");
    }
}

TEST(StartupPolicyTest, HelpDoesNotApplyStartupPolicy) {
    for (const char* help : {"help", "help-seastar", "help-loggers"}) {
        SCOPED_TRACE(help);
        bpo::variables_map options;
        options.emplace(help, bpo::variable_value{});
        options.emplace(
          "unsafe-bypass-fsync", bpo::variable_value(true, false));
        options.emplace("kernel-page-cache", bpo::variable_value(true, false));
        options.emplace("relaxed-dma", bpo::variable_value{});
        options.emplace("io-properties", bpo::variable_value{});
        options.emplace("io-properties-file", bpo::variable_value{});
        EXPECT_NO_THROW(validate_runtime_configuration(options));
        EXPECT_EQ(options.size(), 6U);
    }
}

TEST(StartupPolicyTest, IdentifiesExactConfigurationBytes) {
    const auto empty = identify_configuration({});
    EXPECT_EQ(
      empty.checksum_view(),
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    EXPECT_EQ(empty.bytes, 0U);
    const auto abc = identify_configuration("abc");
    EXPECT_EQ(
      abc.checksum_view(),
      "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    EXPECT_EQ(abc.bytes, 3U);
    const auto nul = identify_configuration(std::string_view{"abc\0def", 7});
    EXPECT_EQ(
      nul.checksum_view(),
      "516a5e926ce20c5f4d80f00e1a01abdf14986def6588d6abeed9fce090bc660c");
    EXPECT_EQ(nul.bytes, 7U);
    EXPECT_NE(abc, identify_configuration("abc\n"));
}

TEST(StartupPolicyTest, IdentityOwnsTheLoadedSnapshot) {
    std::string contents = "kwaque: {schema_version: 1}\n";
    const auto identity = identify_configuration(contents);
    const auto copy = identity;
    contents.assign("kwaque: {schema_version: 1, developer_mode: true}\n");
    EXPECT_EQ(identity, copy);
    EXPECT_NE(identity, identify_configuration(contents));
}

TEST(StartupPolicyTest, BoundsConfigurationHashInput) {
    std::string contents(kwaque::config::max_bootstrap_config_bytes, 'x');
    const auto identity = identify_configuration(contents);
    EXPECT_EQ(identity.bytes, kwaque::config::max_bootstrap_config_bytes);
    EXPECT_EQ(
      identity.checksum_view(),
      "1f8745f0d2d1387ec1af2211a3cf417b2e9e885e853472649c1d979d0e9370e3");
    contents.push_back('x');
    EXPECT_THROW(
      static_cast<void>(identify_configuration(contents)), std::length_error);
}

TEST(StartupPolicyTest, RecognizesNumericLoopbackAddressesWithoutAReactor) {
    for (const std::string_view address :
         {"127.0.0.1",
          "127.0.0.2",
          "127.255.255.255",
          "::1",
          "0:0:0:0:0:0:0:1"}) {
        SCOPED_TRACE(address);
        EXPECT_TRUE(admin_is_loopback(address));
    }
    for (const std::string_view address :
         {"0.0.0.0",
          "128.0.0.1",
          "192.0.2.1",
          "::",
          "2001:db8::1",
          "::ffff:127.0.0.1"}) {
        SCOPED_TRACE(address);
        EXPECT_FALSE(admin_is_loopback(address));
    }
    for (const std::string_view address :
         {"", "localhost", "::1%lo", "[::1]"}) {
        SCOPED_TRACE(address);
        EXPECT_THROW(
          static_cast<void>(admin_is_loopback(address)), std::invalid_argument);
    }
    EXPECT_THROW(
      static_cast<void>(
        admin_is_loopback(std::string_view{"127.0.0.1\0x", 11})),
      std::invalid_argument);
}

TEST(StartupPolicyTest, ReportsDisabledAndDeveloperCrashLoopLimiting) {
    const seastar::app_template::seastar_options runtime;
    kwaque::config::bootstrap_config configuration;
    const auto identity = identify_configuration("fixture");
    const auto resources = test_resources();
    configuration.crash_loop_limit.reset();
    auto policy = render_startup_policy(
      runtime, configuration, identity, resources, 2, "epoll");
    EXPECT_EQ(policy_field(policy, "crash_loop_limit"), "null");
    EXPECT_EQ(policy_field(policy, "crash_loop_limiting"), "false");
    configuration.crash_loop_limit = 5;
    configuration.developer_mode = true;
    policy = render_startup_policy(
      runtime, configuration, identity, resources, 2, "epoll");
    EXPECT_EQ(policy_field(policy, "crash_loop_limit"), "5");
    EXPECT_EQ(policy_field(policy, "crash_loop_limiting"), "false");
}

TEST(StartupPolicyTest, ReportsCapabilitiesAndResolvedProfile) {
    const seastar::app_template::seastar_options runtime;
    kwaque::config::bootstrap_config configuration;
    const auto identity = identify_configuration("fixture");
    const auto resources = test_resources();
    const auto policy = render_startup_policy(
      runtime, configuration, identity, resources, 2, "epoll");
    EXPECT_EQ(policy_field(policy, "profile"), "production");
    EXPECT_EQ(policy_field(policy, "developer_mode"), "false");
    EXPECT_EQ(policy_field(policy, "crash_loop_limit"), "5");
    EXPECT_EQ(policy_field(policy, "crash_loop_limiting"), "true");
    EXPECT_EQ(
      policy_field(policy, "build_mode"), kwaque::build_info::build_mode());
    EXPECT_EQ(policy_field(policy, "compiler"), kwaque::build_info::compiler());
    EXPECT_EQ(policy_field(policy, "shards_requested"), "automatic");
    EXPECT_EQ(policy_field(policy, "shards_observed"), "2");
    EXPECT_EQ(policy_field(policy, "task_quota_ms_requested"), "0.500000");
    EXPECT_EQ(policy_field(policy, "stall_threshold_ms_requested"), "25");
    EXPECT_EQ(policy_field(policy, "stall_reports_per_minute_requested"), "5");
    EXPECT_EQ(policy_field(policy, "memory_requested_bytes"), "unspecified");
    EXPECT_EQ(policy_field(policy, "linux_aio_nowait_requested"), "automatic");
    EXPECT_EQ(policy_field(policy, "io_properties_source"), "none");
    EXPECT_EQ(policy_field(policy, "reactor_backend_observed"), "epoll");
    EXPECT_EQ(
      policy_field(policy, "configuration_checksum_algorithm"), "sha256");
    EXPECT_EQ(
      policy_field(policy, "configuration_checksum"), identity.checksum_view());
    EXPECT_EQ(policy_field(policy, "configuration_bytes"), "7");
    EXPECT_EQ(policy_field(policy, "class_budget_input_bytes"), "134217728");
    EXPECT_EQ(policy_field(policy, "reactor_headroom_bytes"), "16777216");
    EXPECT_EQ(policy_field(policy, "admin_exposure"), "loopback");
    EXPECT_EQ(policy_field(policy, "admin_authentication"), "false");
    EXPECT_EQ(policy_field(policy, "admin_tls"), "false");
#if defined(SEASTAR_DEFAULT_ALLOCATOR)
    EXPECT_EQ(policy_field(policy, "oom_abort_capability"), "unavailable");
    EXPECT_EQ(policy_field(policy, "allocator"), "system");
    EXPECT_EQ(policy_field(policy, "allocator_stats"), "synthetic");
    EXPECT_EQ(
      policy_field(policy, "class_budget_source"),
      "explicit_diagnostic_budget");
    EXPECT_EQ(policy_field(policy, "memory_option_effect"), "unsupported");
    EXPECT_EQ(policy_field(policy, "oom_abort_effective"), "false");
#else
    EXPECT_EQ(policy_field(policy, "oom_abort_capability"), "native");
    EXPECT_EQ(policy_field(policy, "allocator"), "seastar");
    EXPECT_EQ(policy_field(policy, "allocator_stats"), "native");
    EXPECT_EQ(policy_field(policy, "class_budget_source"), "allocator_stats");
    EXPECT_EQ(policy_field(policy, "memory_option_effect"), "allocator_budget");
#endif
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    EXPECT_EQ(policy_field(policy, "allocation_injection"), "true");
#else
    EXPECT_EQ(policy_field(policy, "allocation_injection"), "false");
#endif
#if defined(SEASTAR_ASAN_ENABLED)
    EXPECT_EQ(policy_field(policy, "address_sanitizer"), "true");
#else
    EXPECT_EQ(policy_field(policy, "address_sanitizer"), "false");
#endif
    EXPECT_EQ(
      policy_field(policy, "oom_abort_effective"),
      seastar::memory::is_abort_on_allocation_failure() ? "true" : "false");
    configuration.developer_mode = true;
    configuration.admin_address = "0.0.0.0";
    const auto development = render_startup_policy(
      runtime, configuration, identity, resources, 2, "epoll");
    EXPECT_EQ(policy_field(development, "profile"), "development");
    EXPECT_EQ(policy_field(development, "admin_exposure"), "non_loopback");
    EXPECT_EQ(policy_field(development, "unsafe_bypass_fsync"), "false");
    EXPECT_EQ(policy_field(development, "kernel_page_cache"), "false");
    EXPECT_EQ(policy_field(development, "relaxed_dma"), "false");
}

TEST(StartupPolicyTest, DistinguishesRequestedValuesFromUnobservedOutcomes) {
    seastar::app_template::seastar_options runtime;
    runtime.smp_opts.smp.set_value(4);
    runtime.smp_opts.cpuset.set_value({1, 3, 5, 7});
    runtime.smp_opts.memory.set_value("128MiB");
    runtime.smp_opts.reserve_memory.set_value("1GiB");
    runtime.smp_opts.reserve_additional_memory_per_shard = 4096;
    runtime.smp_opts.thread_affinity.set_value(false);
    runtime.smp_opts.mbind.set_value(true);
    runtime.reactor_opts.overprovisioned.set_value();
    runtime.reactor_opts.poll_aio.set_value(false);
    runtime.reactor_opts.idle_poll_time_us.set_value(0);
    runtime.reactor_opts.linux_aio_nowait.set_value(true);
    const auto policy = render_startup_policy(
      runtime,
      {},
      identify_configuration("fixture"),
      test_resources(),
      4,
      "epoll");
    EXPECT_EQ(policy_field(policy, "shards_requested"), "4");
    EXPECT_EQ(policy_field(policy, "cpuset_requested"), "1,3,5,7");
    EXPECT_EQ(policy_field(policy, "cpuset_requested_count"), "4");
    EXPECT_EQ(policy_field(policy, "memory_requested_bytes"), "134217728");
    EXPECT_EQ(
      policy_field(policy, "reserve_memory_requested_bytes"), "1073741824");
    EXPECT_EQ(
      policy_field(policy, "additional_reserve_per_shard_bytes"), "4096");
    EXPECT_EQ(policy_field(policy, "thread_affinity_requested"), "false");
    EXPECT_EQ(
      policy_field(policy, "thread_affinity_option_source"), "explicit");
    EXPECT_EQ(policy_field(policy, "mbind_requested"), "true");
    EXPECT_EQ(policy_field(policy, "overprovisioned_requested"), "true");
    EXPECT_EQ(policy_field(policy, "poll_aio_requested"), "false");
    EXPECT_EQ(policy_field(policy, "idle_poll_time_us_requested"), "0");
    EXPECT_EQ(policy_field(policy, "linux_aio_nowait_requested"), "true");
    for (const std::string_view key :
         {"cpu_placement_observed",
          "thread_affinity_observed",
          "mbind_observed",
          "polling_observed",
          "device_nowait_observed"}) {
        EXPECT_EQ(policy_field(policy, key), "unobserved");
    }
}

TEST(StartupPolicyTest, BoundsDiagnosticsAndOmitsIoContentsAndPaths) {
    seastar::app_template::seastar_options runtime;
    runtime.smp_opts.io_properties.set_value("secret-inline-io-properties");
    std::set<unsigned> cpus;
    for (unsigned cpu = 0; cpu < 64; ++cpu) {
        cpus.insert(cpu);
    }
    runtime.smp_opts.cpuset.set_value(cpus);
    kwaque::config::bootstrap_config configuration;
    configuration.data_directory = "/secret-data-directory";
    const auto identity = identify_configuration("secret-yaml-snapshot");
    const auto resources = test_resources();
    const auto inline_policy = render_startup_policy(
      runtime, configuration, identity, resources, 2, std::string(8192, '\n'));
    EXPECT_EQ(policy_field(inline_policy, "io_properties_source"), "inline");
    EXPECT_EQ(policy_field(inline_policy, "cpuset_requested_count"), "64");
    EXPECT_NE(inline_policy.find("<truncated>"), std::string::npos);
    EXPECT_EQ(inline_policy.find('\n'), std::string::npos);
    EXPECT_EQ(inline_policy.find("secret-"), std::string::npos);
    EXPECT_LT(inline_policy.size(), 16U * 1024U);

    seastar::app_template::seastar_options file_runtime;
    file_runtime.smp_opts.io_properties_file.set_value(
      "/secret-io-properties-path");
    const auto file_policy = render_startup_policy(
      file_runtime, configuration, identity, resources, 2, "epoll");
    EXPECT_EQ(policy_field(file_policy, "io_properties_source"), "file");
    EXPECT_EQ(file_policy.find("secret-"), std::string::npos);
}

} // namespace
