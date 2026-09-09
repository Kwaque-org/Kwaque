#pragma once

#include "src/config/bootstrap_config.h"
#include "src/resource/resource_config.h"

#include <seastar/core/app-template.hh>

#include <boost/program_options/variables_map.hpp>

#include <array>
#include <cstdint>
#include <string>
#include <string_view>

namespace kwaque::broker::detail {

struct configuration_identity final {
    std::array<char, 64> checksum{};
    std::uint64_t bytes{0};

    [[nodiscard]] std::string_view checksum_view() const noexcept {
        return {checksum.data(), checksum.size()};
    }

    bool operator==(const configuration_identity&) const = default;
};

[[nodiscard]] configuration_identity
identify_configuration(std::string_view contents);

void configure_allocation_failure_policy(
  seastar::app_template::seastar_options& options);
void validate_broker_profile(const config::bootstrap_config& configuration);

void validate_runtime_configuration(
  boost::program_options::variables_map& options);

[[nodiscard]] bool admin_is_loopback(std::string_view address);

[[nodiscard]] std::string render_startup_policy(
  const seastar::app_template::seastar_options& runtime,
  const config::bootstrap_config& configuration,
  const configuration_identity& identity,
  const resource::resource_config& resources,
  unsigned actual_shards,
  std::string_view actual_backend);

} // namespace kwaque::broker::detail
