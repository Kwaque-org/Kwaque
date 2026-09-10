#include "src/broker/startup_policy.h"

#include "src/admin/admin_limits.h"
#include "src/base/build_info.h"

#include <seastar/core/memory.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/util/conversions.hh>
#include <seastar/util/std-compat.hh>

#include <arpa/inet.h>
#include <boost/any.hpp>
#include <boost/program_options/errors.hpp>
#include <openssl/evp.h>

#include <array>
#include <cstddef>
#include <stdexcept>
#include <string>
#include <string_view>

namespace kwaque::broker::detail {

namespace {

namespace bpo = boost::program_options;

constexpr std::string_view bool_value(bool value) noexcept {
    return value ? "true" : "false";
}

void reject_enabled_bool(const bpo::variables_map& options, const char* name) {
    const auto found = options.find(name);
    if (found == options.end()) {
        return;
    }
    const auto* value = boost::any_cast<bool>(&found->second.value());
    if (value == nullptr) {
        throw bpo::error(std::string(name) + " requires a boolean value");
    }
    if (*value) {
        throw bpo::error(std::string(name) + " is not supported by the broker");
    }
}

std::string
requested_memory(const seastar::program_options::value<std::string>& option) {
    return option
             ? std::to_string(seastar::parse_memory_size(option.get_value()))
             : "unspecified";
}

std::string requested_cpuset(const seastar::smp_options& options) {
    if (!options.cpuset) {
        return "inherited";
    }
    constexpr std::size_t maximum_reported_cpus = 16;
    std::string output;
    auto current = options.cpuset.get_value().begin();
    const auto end = options.cpuset.get_value().end();
    for (std::size_t count = 0; current != end && count < maximum_reported_cpus;
         ++current, ++count) {
        if (!output.empty()) {
            output.push_back(',');
        }
        output.append(std::to_string(*current));
    }
    if (current != end) {
        output.append(",<truncated>");
    }
    return output.empty() ? "empty" : output;
}

} // namespace

configuration_identity identify_configuration(std::string_view contents) {
    if (contents.size() > config::max_bootstrap_config_bytes) {
        throw std::length_error(
          "configuration exceeds the maximum supported size");
    }
    std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
    unsigned digest_size = 0;
    const auto* data = contents.empty() ? "" : contents.data();
    if (
      EVP_Digest(
        data,
        contents.size(),
        digest.data(),
        &digest_size,
        EVP_sha256(),
        nullptr)
        != 1
      || digest_size != 32U) {
        throw std::runtime_error("unable to identify bootstrap configuration");
    }
    constexpr std::string_view hex = "0123456789abcdef";
    configuration_identity identity{
      .bytes = static_cast<std::uint64_t>(contents.size())};
    for (std::size_t index = 0; index < digest_size; ++index) {
        const auto byte = static_cast<std::size_t>(digest[index]);
        identity.checksum[2 * index] = hex[byte >> 4U];
        identity.checksum[2 * index + 1] = hex[byte & 0x0fU];
    }
    return identity;
}

void configure_allocation_failure_policy(
  seastar::app_template::seastar_options& options) {
#if defined(SEASTAR_DEFAULT_ALLOCATOR)
    options.reactor_opts.abort_on_seastar_bad_alloc.unset_value();
#else
    options.reactor_opts.abort_on_seastar_bad_alloc.set_value();
#endif
}

void validate_broker_profile(const config::bootstrap_config& configuration) {
#if defined(SEASTAR_DEFAULT_ALLOCATOR)
    if (!configuration.developer_mode) {
        throw std::runtime_error(
          "production broker requires the native Seastar allocator; "
          "set developer_mode=true for diagnostic use");
    }
    if (!configuration.diagnostic_memory_per_shard_bytes) {
        throw std::runtime_error(
          "system-allocator diagnostic broker requires "
          "diagnostic_memory_per_shard_bytes");
    }
#else
    static_cast<void>(configuration);
#endif
}

void validate_runtime_configuration(bpo::variables_map& options) {
    if (
      options.contains("help") || options.contains("help-seastar")
      || options.contains("help-loggers")) {
        return;
    }
#if defined(SEASTAR_DEFAULT_ALLOCATOR)
    if (options.contains("abort-on-seastar-bad-alloc")) {
        throw bpo::error(
          "abort-on-seastar-bad-alloc requires the native Seastar allocator");
    }
#endif
    reject_enabled_bool(options, "unsafe-bypass-fsync");
    reject_enabled_bool(options, "kernel-page-cache");
    if (options.contains("relaxed-dma")) {
        throw bpo::error("relaxed-dma is not supported by the broker");
    }
    if (
      options.contains("io-properties")
      && options.contains("io-properties-file")) {
        throw bpo::error(
          "io-properties and io-properties-file cannot be used together");
    }
}

bool admin_is_loopback(std::string_view address) {
    if (
      address.empty() || address.size() >= INET6_ADDRSTRLEN
      || address.find('\0') != std::string_view::npos
      || address.find('%') != std::string_view::npos) {
        throw std::invalid_argument(
          "admin address must be a numeric IPv4 or IPv6 address");
    }
    return seastar::net::inet_address(seastar::sstring(address)).is_loopback();
}

std::string render_startup_policy(
  const seastar::app_template::seastar_options& runtime,
  const config::bootstrap_config& configuration,
  const configuration_identity& identity,
  const resource::resource_config& resources,
  unsigned actual_shards,
  std::string_view actual_backend) {
    const auto& smp = runtime.smp_opts;
    const auto& reactor = runtime.reactor_opts;
#if defined(SEASTAR_DEFAULT_ALLOCATOR)
    constexpr std::string_view allocator = "system";
    constexpr std::string_view allocator_stats = "synthetic";
    constexpr std::string_view class_budget_source
      = "explicit_diagnostic_budget";
    constexpr std::string_view memory_option_effect = "unsupported";
#else
    constexpr std::string_view allocator = "seastar";
    constexpr std::string_view allocator_stats = "native";
    constexpr std::string_view class_budget_source = "allocator_stats";
    const std::string_view memory_option_effect
      = smp.memory_allocator == seastar::memory_allocator::seastar
          ? "allocator_budget"
          : "unsupported";
#endif
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    constexpr std::string_view allocation_injection = "true";
#else
    constexpr std::string_view allocation_injection = "false";
#endif
#if defined(SEASTAR_DEBUG)
    constexpr std::string_view runtime_debug = "true";
#else
    constexpr std::string_view runtime_debug = "false";
#endif
#if defined(SEASTAR_ASAN_ENABLED)
    constexpr std::string_view address_sanitizer = "true";
#else
    constexpr std::string_view address_sanitizer = "false";
#endif
#if defined(__clang__)
    constexpr std::string_view undefined_sanitizer
      = __has_feature(undefined_behavior_sanitizer) ? "true" : "false";
#else
    constexpr std::string_view undefined_sanitizer = "unobserved";
#endif
    const std::string shards = std::to_string(actual_shards);
    const std::string requested_shards = smp.smp
                                           ? std::to_string(smp.smp.get_value())
                                           : "automatic";
    const std::string cpuset = requested_cpuset(smp);
    const std::string cpuset_count = smp.cpuset
                                       ? std::to_string(
                                           smp.cpuset.get_value().size())
                                       : "unspecified";
    const std::string memory = requested_memory(smp.memory);
    const std::string reserved_memory = requested_memory(smp.reserve_memory);
    const std::string additional_memory = std::to_string(
      smp.reserve_additional_memory_per_shard);
    const std::string class_budget = std::to_string(
      resources.total_memory().value());
    const std::string headroom = std::to_string(
      resources.reactor_headroom().value());
    const std::string admin_memory = std::to_string(
      resources.admin_memory_reservation().value());
    const std::string production_floor = std::to_string(
      resource::resource_config::production_baseline_memory().value()
      + resources.admin_memory_reservation().value());
    const std::string idle_poll_time = std::to_string(
      reactor.idle_poll_time_us.get_value());
    const std::string task_quota = std::to_string(
      reactor.task_quota_ms.get_value());
    const std::string stall_threshold = std::to_string(
      reactor.blocked_reactor_notify_ms.get_value());
    const std::string stall_reports = std::to_string(
      reactor.blocked_reactor_reports_per_minute.get_value());
    const std::string configuration_bytes = std::to_string(identity.bytes);
    const std::string admin_port = std::to_string(configuration.admin_port);
    const std::string crash_loop_limit = configuration.crash_loop_limit
                                           ? std::to_string(
                                               *configuration.crash_loop_limit)
                                           : "null";
    const std::string admin_connections = std::to_string(
      admin::connections_per_shard);
    const std::string admin_request_line = std::to_string(
      admin::request_line_bytes);
    const std::string admin_headers = std::to_string(admin::header_bytes);
    const std::string admin_header_count = std::to_string(admin::header_count);
    const std::string admin_header_seconds = std::to_string(
      admin::header_timeout.count());
    const std::string admin_exchange_seconds = std::to_string(
      admin::exchange_timeout.count());
    const std::string scrape_response_bytes = std::to_string(
      admin::metrics_response_bytes);
    const std::string admin_shares = std::to_string(admin::scheduling_shares);
    const std::string_view io_properties_source = smp.io_properties_file
                                                    ? "file"
                                                  : smp.io_properties ? "inline"
                                                                      : "none";
    const std::string_view nowait = reactor.linux_aio_nowait.defaulted()
                                      ? "automatic"
                                      : bool_value(
                                          reactor.linux_aio_nowait.get_value());
    using config::config_value;
    using enum config::config_visibility;
    const std::array values{
      config_value{
        "profile",
        configuration.developer_mode ? "development" : "production",
        safe},
      config_value{"build_mode", build_info::build_mode(), safe},
      config_value{"compiler", build_info::compiler(), safe},
      config_value{"runtime_debug", runtime_debug, safe},
      config_value{"address_sanitizer", address_sanitizer, safe},
      config_value{"undefined_sanitizer", undefined_sanitizer, safe},
      config_value{"allocator", allocator, safe},
      config_value{"allocator_stats", allocator_stats, safe},
      config_value{"allocation_injection", allocation_injection, safe},
      config_value{
        "oom_abort_capability",
        allocator == "seastar" ? "native" : "unavailable",
        safe},
      config_value{
        "oom_abort_requested",
        bool_value(reactor.abort_on_seastar_bad_alloc),
        safe},
      config_value{
        "oom_abort_effective",
        bool_value(seastar::memory::is_abort_on_allocation_failure()),
        safe},
      config_value{"runtime_configuration_source", "command_line", safe},
      config_value{
        "developer_mode", bool_value(configuration.developer_mode), safe},
      config_value{"crash_loop_limit", crash_loop_limit, safe},
      config_value{
        "crash_loop_limiting",
        bool_value(
          !configuration.developer_mode
          && configuration.crash_loop_limit.has_value()),
        safe},
      config_value{"shards_requested", requested_shards, safe},
      config_value{"shards_observed", shards, safe},
      config_value{"cpuset_requested", cpuset, safe},
      config_value{"cpuset_requested_count", cpuset_count, safe},
      config_value{"cpu_placement_observed", "unobserved", safe},
      config_value{"memory_requested_bytes", memory, safe},
      config_value{"memory_option_effect", memory_option_effect, safe},
      config_value{"reserve_memory_requested_bytes", reserved_memory, safe},
      config_value{
        "additional_reserve_per_shard_bytes", additional_memory, safe},
      config_value{"class_budget_source", class_budget_source, safe},
      config_value{"class_budget_input_bytes", class_budget, safe},
      config_value{"reactor_headroom_bytes", headroom, safe},
      config_value{"admin_memory_reservation_bytes", admin_memory, safe},
      config_value{
        "production_suitability_floor_bytes", production_floor, safe},
      config_value{
        "native_reclaim_observation",
        allocator == "system" ? "unsupported" : "unobserved",
        safe},
      config_value{
        "reactor_backend_requested",
        reactor.reactor_backend.get_selected_candidate_name(),
        safe},
      config_value{"reactor_backend_observed", actual_backend, safe},
      config_value{"linux_aio_nowait_requested", nowait, safe},
      config_value{"device_nowait_observed", "unobserved", safe},
      config_value{
        "thread_affinity_requested",
        bool_value(smp.thread_affinity.get_value()),
        safe},
      config_value{
        "thread_affinity_option_source",
        smp.thread_affinity.defaulted() ? "default" : "explicit",
        safe},
      config_value{"thread_affinity_observed", "unobserved", safe},
      config_value{"mbind_requested", bool_value(smp.mbind.get_value()), safe},
      config_value{"mbind_observed", "unobserved", safe},
      config_value{
        "overprovisioned_requested", bool_value(reactor.overprovisioned), safe},
      config_value{"poll_mode_requested", bool_value(reactor.poll_mode), safe},
      config_value{"idle_poll_time_us_requested", idle_poll_time, safe},
      config_value{
        "idle_poll_option_source",
        reactor.idle_poll_time_us.defaulted() ? "default" : "explicit",
        safe},
      config_value{
        "poll_aio_requested", bool_value(reactor.poll_aio.get_value()), safe},
      config_value{
        "poll_aio_option_source",
        reactor.poll_aio.defaulted() ? "default" : "explicit",
        safe},
      config_value{"polling_observed", "unobserved", safe},
      config_value{"task_quota_ms_requested", task_quota, safe},
      config_value{"stall_threshold_ms_requested", stall_threshold, safe},
      config_value{"stall_reports_per_minute_requested", stall_reports, safe},
      config_value{
        "unsafe_bypass_fsync",
        bool_value(reactor.unsafe_bypass_fsync.get_value()),
        safe},
      config_value{
        "kernel_page_cache",
        bool_value(reactor.kernel_page_cache.get_value()),
        safe},
      config_value{"relaxed_dma", bool_value(reactor.relaxed_dma), safe},
      config_value{"io_properties_source", io_properties_source, safe},
      config_value{
        "storage_strict_data_init",
        bool_value(configuration.storage_strict_data_init),
        safe},
      config_value{"configuration_checksum_algorithm", "sha256", safe},
      config_value{"configuration_checksum", identity.checksum_view(), safe},
      config_value{"configuration_bytes", configuration_bytes, safe},
      config_value{"admin_address", configuration.admin_address, safe},
      config_value{"admin_port", admin_port, safe},
      config_value{"admin_connections_per_shard", admin_connections, safe},
      config_value{"admin_request_line_bytes", admin_request_line, safe},
      config_value{"admin_header_bytes", admin_headers, safe},
      config_value{"admin_header_count", admin_header_count, safe},
      config_value{"admin_header_timeout_seconds", admin_header_seconds, safe},
      config_value{
        "admin_exchange_timeout_seconds", admin_exchange_seconds, safe},
      config_value{"admin_metrics_response_bytes", scrape_response_bytes, safe},
      config_value{"admin_scheduling_shares", admin_shares, safe},
      config_value{
        "admin_exposure",
        admin_is_loopback(configuration.admin_address) ? "loopback"
                                                       : "non_loopback",
        safe},
      config_value{"admin_authentication", "false", safe},
      config_value{"admin_tls", "false", safe},
    };
    return config::render_config(values);
}

} // namespace kwaque::broker::detail
