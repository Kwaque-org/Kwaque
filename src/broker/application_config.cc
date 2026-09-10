#include "src/base/logging.h"
#include "src/broker/application_internal.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/iostream.hh>
#include <seastar/util/file.hh>
#include <seastar/util/log-level.hh>

#include <array>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
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

} // namespace

seastar::future<seastar::temporary_buffer<char>> read_configuration_bytes(
  seastar::input_stream<char>& input, seastar::abort_source& abort_source) {
    abort_source.check();
    auto contents = co_await input.read_exactly(
      config::max_bootstrap_config_bytes + 1);
    abort_source.check();
    co_return contents;
}

loaded_configuration_result
decode_configuration_snapshot(std::string_view contents) {
    if (contents.size() > config::max_bootstrap_config_bytes) {
        return std::unexpected(
          config::config_error{
            .code = config::config_errc::input_too_large,
            .field = "config",
            .message = "configuration exceeds the maximum supported size",
          });
    }
    auto parsed = config::parse_bootstrap_config(contents);
    if (!parsed) {
        return std::unexpected(std::move(parsed.error()));
    }
    return loaded_bootstrap_configuration{
      .settings = std::move(*parsed),
      .identity = identify_configuration(contents),
    };
}

namespace {

seastar::future<loaded_configuration_result> load_configuration_file(
  const std::filesystem::path& path, seastar::abort_source& abort_source) {
    seastar::temporary_buffer<char> contents;
    try {
        contents = co_await seastar::util::with_file_input_stream(
          path, [&abort_source](seastar::input_stream<char>& input) {
              return read_configuration_bytes(input, abort_source);
          });
    } catch (const std::system_error&) {
        co_return std::unexpected(
          config::config_error{
            .code = config::config_errc::file_unavailable,
            .field = "config",
            .message = "unable to read configuration file",
          });
    }
    abort_source.check();
    const std::string_view view = contents.empty()
                                    ? std::string_view{}
                                    : std::string_view{
                                        contents.get(), contents.size()};
    co_return decode_configuration_snapshot(view);
}

} // namespace

seastar::future<> application_state::load_configuration(
  const boost::program_options::variables_map& options) {
    capture_or_assert_owner();
    if (stop_signal_ == nullptr || configuration_) {
        throw std::logic_error(
          "configuration requires a stop owner and may only be loaded once");
    }
    stop_signal_->abort_source().check();
    config_path_ = options["config"].as<std::string>();
    auto loaded = co_await load_configuration_file(
      config_path_, stop_signal_->abort_source());
    const std::string config_path_string = config_path_.string();
    const std::array path_value{config::config_value{
      "path", config_path_string, config::config_visibility::safe}};
    if (!loaded) {
        const auto& error = loaded.error();
        throw std::runtime_error(
          "configuration error " + config::render_config(path_value) + " "
          + config::render_config_error(error));
    }
    configuration_identity_ = loaded->identity;
    configuration_ = std::move(loaded->settings);

    log::broker().set_level(to_seastar_log_level(configuration_->level));
    log::broker().info(
      "configuration loaded {} {}",
      config::render_config(path_value),
      config::render_config(*configuration_));
}

} // namespace kwaque::broker::detail
