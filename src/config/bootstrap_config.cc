#include "src/config/bootstrap_config.h"

#include <yaml-cpp/yaml.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <fstream>
#include <iterator>
#include <limits>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace kwaque::config {

namespace {

using validation_result = std::expected<void, config_error>;

config_error make_error(config_errc code, std::string field,
                        std::string message) {
  return config_error{
      .code = code, .field = std::move(field), .message = std::move(message)};
}

template <std::size_t Size>
validation_result validate_keys(
    const YAML::Node &mapping, std::string_view field,
    const std::array<std::string_view, Size> &allowed_keys) {
  if (!mapping.IsMap()) {
    return std::unexpected(make_error(
        config_errc::invalid_type, std::string(field), "expected a mapping"));
  }

  std::vector<std::string> seen;
  seen.reserve(mapping.size());
  for (const auto &entry : mapping) {
    if (!entry.first.IsScalar()) {
      return std::unexpected(make_error(config_errc::invalid_type,
                                        std::string(field),
                                        "mapping keys must be strings"));
    }

    const std::string key = entry.first.as<std::string>();
    if (std::ranges::find(allowed_keys, key) == allowed_keys.end()) {
      return std::unexpected(make_error(
          config_errc::unknown_key, std::string(field) + "." + key,
          "unknown configuration key"));
    }
    if (std::ranges::find(seen, key) != seen.end()) {
      return std::unexpected(make_error(
          config_errc::duplicate_key, std::string(field) + "." + key,
          "duplicate configuration key"));
    }
    seen.push_back(key);
  }
  return {};
}

template <typename Value>
bool converts_to(const YAML::Node &node) noexcept {
  try {
    static_cast<void>(node.as<Value>());
    return true;
  } catch (const YAML::Exception &) {
    return false;
  }
}

bool is_string_scalar(const YAML::Node &node) {
  constexpr std::string_view yaml_string_tag = "tag:yaml.org,2002:str";
  const std::string &tag = node.Tag();
  if (tag == "!" || tag == yaml_string_tag) {
    return true;
  }
  if (tag != "?") {
    return false;
  }

  return !converts_to<bool>(node) && !converts_to<std::int64_t>(node) &&
         !converts_to<std::uint64_t>(node) && !converts_to<double>(node);
}

template <typename Value>
std::expected<Value, config_error>
required_scalar(const YAML::Node &mapping, std::string_view key,
                std::string field) {
  const YAML::Node value = mapping[std::string(key)];
  if (!value) {
    return std::unexpected(make_error(config_errc::missing_key,
                                      std::move(field),
                                      "required configuration key is missing"));
  }
  if (!value.IsScalar()) {
    return std::unexpected(make_error(config_errc::invalid_type,
                                      std::move(field),
                                      "expected a scalar value"));
  }
  if constexpr (std::is_same_v<Value, std::string>) {
    if (!is_string_scalar(value)) {
      return std::unexpected(make_error(config_errc::invalid_type,
                                        std::move(field),
                                        "expected a string value"));
    }
  }
  try {
    return value.as<Value>();
  } catch (const YAML::Exception &) {
    return std::unexpected(make_error(config_errc::invalid_type,
                                      std::move(field),
                                      "value has the wrong type"));
  }
}

bool is_blank(std::string_view value) {
  return value.empty() ||
         std::ranges::all_of(value, [](unsigned char character) {
           return std::isspace(character) != 0;
         });
}

std::optional<log_level> parse_log_level(std::string_view value) noexcept {
  if (value == "trace") {
    return log_level::trace;
  }
  if (value == "debug") {
    return log_level::debug;
  }
  if (value == "info") {
    return log_level::info;
  }
  if (value == "warn") {
    return log_level::warn;
  }
  if (value == "error") {
    return log_level::error;
  }
  return std::nullopt;
}

void append_escaped(std::string &output, std::string_view value) {
  constexpr std::string_view hex_digits = "0123456789abcdef";
  for (const char raw_character : value) {
    const auto character = static_cast<unsigned char>(raw_character);
    switch (character) {
    case '\\':
      output += "\\\\";
      break;
    case '\n':
      output += "\\n";
      break;
    case '\r':
      output += "\\r";
      break;
    case '\t':
      output += "\\t";
      break;
    default:
      if (character < 0x20 || character == 0x7f) {
        output += "\\x";
        output.push_back(hex_digits[character >> 4]);
        output.push_back(hex_digits[character & 0x0f]);
      } else {
        output.push_back(static_cast<char>(character));
      }
      break;
    }
  }
}

bootstrap_config_result decode_bootstrap_config(const YAML::Node &root) {
  constexpr std::array<std::string_view, 1> root_keys{"kwaque"};
  if (auto validated = validate_keys(root, "root", root_keys); !validated) {
    return std::unexpected(std::move(validated.error()));
  }

  const YAML::Node settings = root["kwaque"];
  if (!settings) {
    return std::unexpected(make_error(config_errc::missing_key, "kwaque",
                                      "required configuration root is missing"));
  }

  constexpr std::array<std::string_view, 6> settings_keys{
      "schema_version", "node_id",       "data_directory", "admin",
      "log_level",      "developer_mode"};
  if (auto validated = validate_keys(settings, "kwaque", settings_keys);
      !validated) {
    return std::unexpected(std::move(validated.error()));
  }

  const auto schema_version = required_scalar<std::int64_t>(
      settings, "schema_version", "kwaque.schema_version");
  if (!schema_version) {
    return std::unexpected(schema_version.error());
  }
  if (*schema_version != bootstrap_config_schema_version) {
    return std::unexpected(make_error(
        config_errc::unsupported_schema_version, "kwaque.schema_version",
        "unsupported configuration schema version " +
            std::to_string(*schema_version) + "; supported version is " +
            std::to_string(bootstrap_config_schema_version)));
  }

  bootstrap_config configuration;

  if (settings["node_id"]) {
    const auto node_id = required_scalar<std::int64_t>(
        settings, "node_id", "kwaque.node_id");
    if (!node_id) {
      return std::unexpected(node_id.error());
    }
    if (*node_id < 0 ||
        *node_id > std::numeric_limits<std::int32_t>::max()) {
      return std::unexpected(make_error(
          config_errc::invalid_node_id, "kwaque.node_id",
          "node ID must be between 0 and " +
              std::to_string(std::numeric_limits<std::int32_t>::max())));
    }
    configuration.node_id = static_cast<std::int32_t>(*node_id);
  }

  if (settings["data_directory"]) {
    const auto data_directory = required_scalar<std::string>(
        settings, "data_directory", "kwaque.data_directory");
    if (!data_directory) {
      return std::unexpected(data_directory.error());
    }
    if (is_blank(*data_directory) ||
        data_directory->find('\0') != std::string::npos) {
      return std::unexpected(make_error(
          config_errc::invalid_data_directory, "kwaque.data_directory",
          "data directory must be a non-empty path"));
    }
    configuration.data_directory = *data_directory;
  }

  if (settings["admin"]) {
    constexpr std::array<std::string_view, 2> admin_keys{"address", "port"};
    const YAML::Node admin = settings["admin"];
    if (auto validated = validate_keys(admin, "kwaque.admin", admin_keys);
        !validated) {
      return std::unexpected(std::move(validated.error()));
    }

    if (admin["address"]) {
      const auto address = required_scalar<std::string>(
          admin, "address", "kwaque.admin.address");
      if (!address) {
        return std::unexpected(address.error());
      }
      if (is_blank(*address) ||
          std::ranges::any_of(*address, [](unsigned char character) {
            return std::isspace(character) != 0 ||
                   std::iscntrl(character) != 0;
          })) {
        return std::unexpected(make_error(
            config_errc::invalid_admin_address, "kwaque.admin.address",
            "admin address must be non-empty and contain no whitespace or "
            "control characters"));
      }
      configuration.admin_address = *address;
    }

    if (admin["port"]) {
      const auto port =
          required_scalar<std::int64_t>(admin, "port", "kwaque.admin.port");
      if (!port) {
        return std::unexpected(port.error());
      }
      if (*port <= 0 || *port > std::numeric_limits<std::uint16_t>::max()) {
        return std::unexpected(make_error(
            config_errc::invalid_admin_port, "kwaque.admin.port",
            "admin port must be between 1 and 65535"));
      }
      configuration.admin_port = static_cast<std::uint16_t>(*port);
    }
  }

  if (settings["log_level"]) {
    const auto value = required_scalar<std::string>(
        settings, "log_level", "kwaque.log_level");
    if (!value) {
      return std::unexpected(value.error());
    }
    const auto level = parse_log_level(*value);
    if (!level) {
      return std::unexpected(make_error(
          config_errc::invalid_log_level, "kwaque.log_level",
          "log level must be one of trace, debug, info, warn, or error"));
    }
    configuration.level = *level;
  }

  if (settings["developer_mode"]) {
    const auto developer_mode = required_scalar<bool>(
        settings, "developer_mode", "kwaque.developer_mode");
    if (!developer_mode) {
      return std::unexpected(developer_mode.error());
    }
    configuration.developer_mode = *developer_mode;
  }

  return configuration;
}

} // namespace

bootstrap_config_result parse_bootstrap_config(std::string_view yaml) {
  try {
    return decode_bootstrap_config(YAML::Load(std::string(yaml)));
  } catch (const YAML::Exception &error) {
    return std::unexpected(make_error(config_errc::malformed_yaml, "root",
                                      "unable to parse YAML: " +
                                          std::string(error.what())));
  }
}

bootstrap_config_result
load_bootstrap_config(const std::filesystem::path &path) {
  std::ifstream input(path, std::ios::binary);
  if (!input) {
    return std::unexpected(make_error(
        config_errc::file_unavailable, "config",
        "unable to open configuration file: " + path.string()));
  }

  std::string contents{std::istreambuf_iterator<char>(input),
                       std::istreambuf_iterator<char>()};
  if (input.bad()) {
    return std::unexpected(make_error(
        config_errc::file_unavailable, "config",
        "unable to read configuration file: " + path.string()));
  }
  return parse_bootstrap_config(contents);
}

std::string_view to_string(log_level level) noexcept {
  switch (level) {
  case log_level::trace:
    return "trace";
  case log_level::debug:
    return "debug";
  case log_level::info:
    return "info";
  case log_level::warn:
    return "warn";
  case log_level::error:
    return "error";
  }
  return "unknown";
}

std::string render_config(std::span<const config_value> values) {
  std::string output;
  for (const auto &value : values) {
    if (!output.empty()) {
      output.push_back(' ');
    }
    output.append(value.name);
    output.push_back('=');
    if (value.visibility == config_visibility::safe) {
      append_escaped(output, value.value);
    } else {
      output += "<redacted>";
    }
  }
  return output;
}

std::string render_config(const bootstrap_config &configuration) {
  const std::string schema_version =
      std::to_string(configuration.schema_version);
  const std::string node_id = std::to_string(configuration.node_id);
  const std::string data_directory = configuration.data_directory.string();
  const std::string admin_port = std::to_string(configuration.admin_port);
  const std::string developer_mode =
      configuration.developer_mode ? "true" : "false";
  const std::array values{
      config_value{"schema_version", schema_version, config_visibility::safe},
      config_value{"node_id", node_id, config_visibility::safe},
      config_value{"data_directory", data_directory, config_visibility::safe},
      config_value{"admin_address", configuration.admin_address,
                   config_visibility::safe},
      config_value{"admin_port", admin_port, config_visibility::safe},
      config_value{"log_level", to_string(configuration.level),
                   config_visibility::safe},
      config_value{"developer_mode", developer_mode, config_visibility::safe},
  };
  return render_config(values);
}

} // namespace kwaque::config
