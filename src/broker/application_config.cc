#include "src/broker/application_internal.h"

#include "src/base/logging.h"

#include <seastar/util/log-level.hh>

#include <array>
#include <stdexcept>
#include <string>

namespace kwaque::broker::detail {

namespace {

seastar::log_level to_seastar_log_level(config::log_level level) noexcept {
  switch (level) {
  case config::log_level::trace:
    return seastar::log_level::trace;
  case config::log_level::debug:
    return seastar::log_level::debug;
  case config::log_level::info:
    return seastar::log_level::info;
  case config::log_level::warn:
    return seastar::log_level::warn;
  case config::log_level::error:
    return seastar::log_level::error;
  }
  return seastar::log_level::info;
}

} // namespace

void application_state::load_configuration(
    const boost::program_options::variables_map &options) {
  config_path_ = options["config"].as<std::string>();
  auto loaded = config::load_bootstrap_config(config_path_);
  if (!loaded) {
    const auto &error = loaded.error();
    throw std::runtime_error("configuration error at " + error.field + ": " +
                             error.message);
  }
  configuration_ = std::move(*loaded);

  const std::string config_path_string = config_path_.string();
  const std::array path_value{config::config_value{
      "path", config_path_string, config::config_visibility::safe}};
  log::broker().set_level(to_seastar_log_level(configuration_->level));
  log::broker().info("configuration loaded {} {}",
                     config::render_config(path_value),
                     config::render_config(*configuration_));
}

} // namespace kwaque::broker::detail
