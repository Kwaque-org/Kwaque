#include "src/base/logging.h"
#include "src/broker/application_internal.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/iostream.hh>
#include <seastar/util/file.hh>
#include <seastar/util/log-level.hh>

#include <array>
#include <new>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

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

seastar::future<config::bootstrap_config_result>
parse_configuration_stream(seastar::input_stream<char>& input) {
    auto contents = co_await input.read_exactly(
      config::max_bootstrap_config_bytes + 1);
    if (contents.size() > config::max_bootstrap_config_bytes) {
        co_return std::unexpected(
          config::config_error{
            .code = config::config_errc::input_too_large,
            .field = "config",
            .message = "configuration exceeds the maximum supported size",
          });
    }
    const std::string_view view = contents.empty()
                                    ? std::string_view{}
                                    : std::string_view{
                                        contents.get(), contents.size()};
    co_return config::parse_bootstrap_config(view);
}

seastar::future<config::bootstrap_config_result>
load_configuration_file(const std::filesystem::path& path) {
    try {
        co_return co_await seastar::util::with_file_input_stream(
          path, &parse_configuration_stream);
    } catch (const std::bad_alloc&) {
        throw;
    } catch (...) {
        co_return std::unexpected(
          config::config_error{
            .code = config::config_errc::file_unavailable,
            .field = "config",
            .message = "unable to read configuration file",
          });
    }
}

} // namespace

seastar::future<> application_state::load_configuration(
  const boost::program_options::variables_map& options) {
    capture_or_assert_owner();
    config_path_ = options["config"].as<std::string>();
    auto loaded = co_await load_configuration_file(config_path_);
    const std::string config_path_string = config_path_.string();
    const std::array path_value{config::config_value{
      "path", config_path_string, config::config_visibility::safe}};
    if (!loaded) {
        const auto& error = loaded.error();
        throw std::runtime_error(
          "configuration error " + config::render_config(path_value) + " "
          + config::render_config_error(error));
    }
    configuration_ = std::move(*loaded);

    log::broker().set_level(to_seastar_log_level(configuration_->level));
    log::broker().info(
      "configuration loaded {} {}",
      config::render_config(path_value),
      config::render_config(*configuration_));
}

} // namespace kwaque::broker::detail
