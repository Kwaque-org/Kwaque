#include "src/config/bootstrap_config.h"

#include <arpa/inet.h>
#include <yaml-cpp/eventhandler.h>
#include <yaml-cpp/parser.h>
#include <yaml-cpp/yaml.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <charconv>
#include <fstream>
#include <limits>
#include <optional>
#include <regex>
#include <sstream>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

namespace kwaque::config {

namespace {

using validation_result = std::expected<void, config_error>;

config_error
make_error(config_errc code, std::string field, std::string message) {
    return config_error{
      .code = code, .field = std::move(field), .message = std::move(message)};
}

// YAML 1.2 streams are Unicode text. Accept only well-formed UTF-8 whose code
// points are YAML printable characters, which excludes C0 controls other than
// tab, line feed and carriage return, DEL, C1 controls other than NEL, and
// surrogates. This also rejects other encodings the parser would transcode.
bool is_printable_utf8(std::string_view text) noexcept {
    std::size_t index = 0;
    while (index < text.size()) {
        const auto lead = static_cast<unsigned char>(text[index]);
        if (lead < 0x80) {
            if (
              lead != 0x09 && lead != 0x0a && lead != 0x0d
              && (lead < 0x20 || lead > 0x7e)) {
                return false;
            }
            ++index;
            continue;
        }
        std::size_t length = 0;
        std::uint32_t code_point = 0;
        std::uint32_t minimum = 0;
        if ((lead & 0xe0U) == 0xc0U) {
            length = 2;
            code_point = lead & 0x1fU;
            minimum = 0x80;
        } else if ((lead & 0xf0U) == 0xe0U) {
            length = 3;
            code_point = lead & 0x0fU;
            minimum = 0x800;
        } else if ((lead & 0xf8U) == 0xf0U) {
            length = 4;
            code_point = lead & 0x07U;
            minimum = 0x10000;
        } else {
            return false;
        }
        if (length > text.size() - index) {
            return false;
        }
        for (std::size_t offset = 1; offset < length; ++offset) {
            const auto next = static_cast<unsigned char>(text[index + offset]);
            if ((next & 0xc0U) != 0x80U) {
                return false;
            }
            code_point = (code_point << 6U) | (next & 0x3fU);
        }
        const bool printable
          = code_point == 0x85 || (code_point >= 0xa0 && code_point <= 0xd7ff)
            || (code_point >= 0xe000 && code_point <= 0xfffd)
            || (code_point >= 0x10000 && code_point <= 0x10ffff);
        if (code_point < minimum || !printable) {
            return false;
        }
        index += length;
    }
    return true;
}

struct nesting_exceeded final {
    std::size_t line;
};

// Counts collection nesting from parser events. The parser announces each
// collection before recursing into it, so throwing here bounds its recursion
// at the configured depth instead of the parser's much deeper guard.
class structure_limit final : public YAML::EventHandler {
public:
    void OnDocumentStart(const YAML::Mark&) override {}
    void OnDocumentEnd() override {}
    void OnNull(const YAML::Mark&, YAML::anchor_t) override {}
    void OnAlias(const YAML::Mark&, YAML::anchor_t) override {}
    void OnScalar(
      const YAML::Mark&,
      const std::string&,
      YAML::anchor_t,
      const std::string&) override {}
    void OnSequenceStart(
      const YAML::Mark& mark,
      const std::string&,
      YAML::anchor_t,
      YAML::EmitterStyle::value) override {
        enter(mark);
    }
    void OnSequenceEnd() override { --depth_; }
    void OnMapStart(
      const YAML::Mark& mark,
      const std::string&,
      YAML::anchor_t,
      YAML::EmitterStyle::value) override {
        enter(mark);
    }
    void OnMapEnd() override { --depth_; }

private:
    void enter(const YAML::Mark& mark) {
        if (++depth_ > max_bootstrap_config_depth) {
            throw nesting_exceeded{static_cast<std::size_t>(mark.line) + 1};
        }
    }

    std::size_t depth_{0};
};

// Checks the stream before nodes are built: one document at most, nested no
// deeper than the schema allows. Parser errors propagate to the caller.
validation_result check_structure(const std::string& yaml) {
    std::istringstream stream(yaml);
    YAML::Parser parser(stream);
    structure_limit limit;
    std::size_t documents = 0;
    try {
        while (parser.HandleNextDocument(limit)) {
            if (++documents > 1) {
                return std::unexpected(make_error(
                  config_errc::malformed_yaml,
                  "root",
                  "configuration must contain exactly one YAML document"));
            }
        }
    } catch (const nesting_exceeded& error) {
        return std::unexpected(make_error(
          config_errc::malformed_yaml,
          "root",
          "configuration nests deeper than "
            + std::to_string(max_bootstrap_config_depth)
            + " collections at line " + std::to_string(error.line)));
    }
    return {};
}

template<std::size_t Size>
validation_result validate_keys(
  const YAML::Node& mapping,
  std::string_view field,
  const std::array<std::string_view, Size>& allowed_keys) {
    if (!mapping.IsMap()) {
        return std::unexpected(make_error(
          config_errc::invalid_type, std::string(field), "expected a mapping"));
    }

    std::vector<std::string> seen;
    seen.reserve(mapping.size());
    for (const auto& entry : mapping) {
        if (!entry.first.IsScalar()) {
            return std::unexpected(make_error(
              config_errc::invalid_type,
              std::string(field),
              "mapping keys must be strings"));
        }

        const std::string key = entry.first.as<std::string>();
        if (std::ranges::find(allowed_keys, key) == allowed_keys.end()) {
            return std::unexpected(make_error(
              config_errc::unknown_key,
              std::string(field) + "." + key,
              "unknown configuration key"));
        }
        if (std::ranges::find(seen, key) != seen.end()) {
            return std::unexpected(make_error(
              config_errc::duplicate_key,
              std::string(field) + "." + key,
              "duplicate configuration key"));
        }
        seen.push_back(key);
    }
    return {};
}

// The YAML 1.2 core schema resolves these plain scalars to non-string types.
// A string setting must be quoted when its text has one of these forms.
bool resolves_to_non_string(const std::string& text) {
    // Anchored so ECMAScript backtracking tries every alternative.
    static const std::regex core_scalar{
      "^(?:null|Null|NULL|~|true|True|TRUE|false|False|FALSE"
      "|[-+]?[0-9]+|0o[0-7]+|0x[0-9a-fA-F]+"
      "|[-+]?(\\.[0-9]+|[0-9]+(\\.[0-9]*)?)([eE][-+]?[0-9]+)?"
      "|[-+]?\\.(inf|Inf|INF)|\\.(nan|NaN|NAN))$",
      std::regex::ECMAScript | std::regex::optimize};
    return text.empty() || std::regex_match(text, core_scalar);
}

bool is_string_scalar(const YAML::Node& node) {
    constexpr std::string_view yaml_string_tag = "tag:yaml.org,2002:str";
    const std::string& tag = node.Tag();
    if (tag == "!" || tag == yaml_string_tag) {
        return true;
    }
    return tag == "?" && !resolves_to_non_string(node.Scalar());
}

std::expected<YAML::Node, config_error> required_value(
  const YAML::Node& mapping, std::string_view key, const std::string& field) {
    YAML::Node value = mapping[std::string(key)];
    if (!value) {
        return std::unexpected(make_error(
          config_errc::missing_key,
          field,
          "required configuration key is missing"));
    }
    if (!value.IsScalar()) {
        return std::unexpected(make_error(
          config_errc::invalid_type, field, "expected a scalar value"));
    }
    return value;
}

std::expected<std::string, config_error> required_string(
  const YAML::Node& mapping, std::string_view key, const std::string& field) {
    auto value = required_value(mapping, key, field);
    if (!value) {
        return std::unexpected(std::move(value.error()));
    }
    if (!is_string_scalar(*value)) {
        return std::unexpected(make_error(
          config_errc::invalid_type,
          field,
          "expected a string value; quote text that reads as another type"));
    }
    return value->Scalar();
}

// Integers are untagged plain scalars in canonical decimal: no plus sign, no
// leading zeros, no octal or hexadecimal prefixes. YAML 1.1 and 1.2 disagree
// about those forms, so accepting them would make values ambiguous.
std::expected<std::int64_t, config_error> required_integer(
  const YAML::Node& mapping, std::string_view key, const std::string& field) {
    auto value = required_value(mapping, key, field);
    if (!value) {
        return std::unexpected(std::move(value.error()));
    }
    const std::string& text = value->Scalar();
    const std::string_view digits = std::string_view{text}.starts_with('-')
                                      ? std::string_view{text}.substr(1)
                                      : std::string_view{text};
    const bool canonical = value->Tag() == "?" && !digits.empty()
                           && std::ranges::all_of(
                             digits,
                             [](char character) {
                                 return character >= '0' && character <= '9';
                             })
                           && (digits.size() == 1 || digits.front() != '0');
    if (!canonical) {
        return std::unexpected(make_error(
          config_errc::invalid_type,
          field,
          "expected an unquoted decimal integer without leading zeros"));
    }
    std::int64_t result = 0;
    const auto [end, error] = std::from_chars(
      text.data(), text.data() + text.size(), result);
    if (error != std::errc{} || end != text.data() + text.size()) {
        return std::unexpected(make_error(
          config_errc::invalid_type,
          field,
          "integer is outside the signed 64-bit range"));
    }
    return result;
}

std::expected<bool, config_error> required_boolean(
  const YAML::Node& mapping, std::string_view key, const std::string& field) {
    auto value = required_value(mapping, key, field);
    if (!value) {
        return std::unexpected(std::move(value.error()));
    }
    const std::string& text = value->Scalar();
    if (value->Tag() == "?") {
        if (text == "true" || text == "True" || text == "TRUE") {
            return true;
        }
        if (text == "false" || text == "False" || text == "FALSE") {
            return false;
        }
    }
    return std::unexpected(make_error(
      config_errc::invalid_type, field, "expected an unquoted true or false"));
}

bool is_blank(std::string_view value) {
    return value.empty()
           || std::ranges::all_of(value, [](unsigned char character) {
                  return std::isspace(character) != 0;
              });
}

void append_escaped(
  std::string& output,
  std::string_view value,
  std::size_t maximum_input = max_rendered_config_value_bytes) {
    constexpr std::string_view hex_digits = "0123456789abcdef";
    const auto bounded_size = std::min(value.size(), maximum_input);
    for (const char raw_character : value.substr(0, bounded_size)) {
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
            if (character < 0x20 || character > 0x7e) {
                output += "\\x";
                output.push_back(hex_digits[character >> 4]);
                output.push_back(hex_digits[character & 0x0f]);
            } else {
                output.push_back(static_cast<char>(character));
            }
            break;
        }
    }
    if (value.size() > maximum_input) {
        output += "<truncated>";
    }
}

bool is_numeric_address(std::string_view value) {
    in_addr address_v4{};
    in6_addr address_v6{};
    const std::string text{value};
    return ::inet_pton(AF_INET, text.c_str(), &address_v4) == 1
           || ::inet_pton(AF_INET6, text.c_str(), &address_v6) == 1;
}

bootstrap_config_result decode_bootstrap_config(const YAML::Node& root) {
    if (!root.IsMap()) {
        return std::unexpected(
          make_error(config_errc::invalid_type, "root", "expected a mapping"));
    }
    const YAML::Node settings = root["kwaque"];
    if (!settings) {
        return std::unexpected(make_error(
          config_errc::missing_key,
          "kwaque",
          "required configuration root is missing"));
    }
    if (!settings.IsMap()) {
        return std::unexpected(make_error(
          config_errc::invalid_type, "kwaque", "expected a mapping"));
    }

    // The version decides which keys are valid, so check it before the key
    // sets: a newer configuration must report its version, not its new keys.
    const auto schema_version = required_integer(
      settings, "schema_version", "kwaque.schema_version");
    if (!schema_version) {
        return std::unexpected(schema_version.error());
    }
    if (
      *schema_version < minimum_bootstrap_config_schema_version
      || *schema_version > maximum_bootstrap_config_schema_version) {
        const std::string supported
          = minimum_bootstrap_config_schema_version
                == maximum_bootstrap_config_schema_version
              ? "supported version is "
                  + std::to_string(maximum_bootstrap_config_schema_version)
              : "supported versions are "
                  + std::to_string(minimum_bootstrap_config_schema_version)
                  + " to "
                  + std::to_string(maximum_bootstrap_config_schema_version);
        return std::unexpected(make_error(
          config_errc::unsupported_schema_version,
          "kwaque.schema_version",
          "unsupported configuration schema version "
            + std::to_string(*schema_version) + "; " + supported));
    }

    constexpr std::array<std::string_view, 1> root_keys{"kwaque"};
    if (auto validated = validate_keys(root, "root", root_keys); !validated) {
        return std::unexpected(std::move(validated.error()));
    }
    if (
      auto validated = validate_keys(settings, "kwaque", bootstrap_config_keys);
      !validated) {
        return std::unexpected(std::move(validated.error()));
    }

    bootstrap_config configuration;
    configuration.schema_version = static_cast<std::uint32_t>(*schema_version);

    if (settings["data_directory"]) {
        const auto data_directory = required_string(
          settings, "data_directory", "kwaque.data_directory");
        if (!data_directory) {
            return std::unexpected(data_directory.error());
        }
        if (
          is_blank(*data_directory)
          || data_directory->find('\0') != std::string::npos) {
            return std::unexpected(make_error(
              config_errc::invalid_data_directory,
              "kwaque.data_directory",
              "data directory must be a non-empty path"));
        }
        configuration.data_directory = *data_directory;
    }

    if (settings["admin"]) {
        const YAML::Node admin = settings["admin"];
        if (
          auto validated = validate_keys(
            admin, "kwaque.admin", bootstrap_admin_keys);
          !validated) {
            return std::unexpected(std::move(validated.error()));
        }

        if (admin["address"]) {
            const auto address = required_string(
              admin, "address", "kwaque.admin.address");
            if (!address) {
                return std::unexpected(address.error());
            }
            if (
              is_blank(*address)
              || std::ranges::any_of(
                *address,
                [](unsigned char character) {
                    return std::isspace(character) != 0
                           || std::iscntrl(character) != 0;
                })
              || !is_numeric_address(*address)) {
                return std::unexpected(make_error(
                  config_errc::invalid_admin_address,
                  "kwaque.admin.address",
                  "admin address must be a numeric IPv4 or IPv6 address"));
            }
            configuration.admin_address = *address;
        }

        if (admin["port"]) {
            const auto port = required_integer(
              admin, "port", "kwaque.admin.port");
            if (!port) {
                return std::unexpected(port.error());
            }
            if (
              *port <= 0 || *port > std::numeric_limits<std::uint16_t>::max()) {
                return std::unexpected(make_error(
                  config_errc::invalid_admin_port,
                  "kwaque.admin.port",
                  "admin port must be between 1 and 65535"));
            }
            configuration.admin_port = static_cast<std::uint16_t>(*port);
        }
    }

    if (settings["crash_loop_limit"] && settings["crash_loop_limit"].IsNull()) {
        configuration.crash_loop_limit.reset();
    } else if (settings["crash_loop_limit"]) {
        const auto limit = required_integer(
          settings, "crash_loop_limit", "kwaque.crash_loop_limit");
        if (!limit) {
            return std::unexpected(limit.error());
        }
        if (*limit < 0 || *limit > std::numeric_limits<std::uint32_t>::max()) {
            return std::unexpected(make_error(
              config_errc::invalid_crash_loop_limit,
              "kwaque.crash_loop_limit",
              "crash loop limit must be an unsigned 32-bit integer"));
        }
        configuration.crash_loop_limit = static_cast<std::uint32_t>(*limit);
    }

    if (settings["developer_mode"]) {
        const auto developer_mode = required_boolean(
          settings, "developer_mode", "kwaque.developer_mode");
        if (!developer_mode) {
            return std::unexpected(developer_mode.error());
        }
        configuration.developer_mode = *developer_mode;
    }

    if (settings["storage_strict_data_init"]) {
        const auto strict = required_boolean(
          settings,
          "storage_strict_data_init",
          "kwaque.storage_strict_data_init");
        if (!strict) {
            return std::unexpected(strict.error());
        }
        configuration.storage_strict_data_init = *strict;
    }
    if (settings["diagnostic_memory_per_shard_bytes"]) {
        const auto memory = required_integer(
          settings,
          "diagnostic_memory_per_shard_bytes",
          "kwaque.diagnostic_memory_per_shard_bytes");
        if (!memory) {
            return std::unexpected(memory.error());
        }
        if (*memory <= 0) {
            return std::unexpected(make_error(
              config_errc::invalid_memory_budget,
              "kwaque.diagnostic_memory_per_shard_bytes",
              "diagnostic memory budget must be a positive finite byte count"));
        }
        configuration.diagnostic_memory_per_shard_bytes
          = static_cast<std::uint64_t>(*memory);
    }

    // A service manager usually starts the broker from "/", where a relative
    // path would silently name a different directory. Configuration values
    // are not shell words, so a leading "~" is never a home directory.
    const std::string& path = configuration.data_directory.native();
    if (path.starts_with('~')) {
        return std::unexpected(make_error(
          config_errc::invalid_data_directory,
          "kwaque.data_directory",
          "data directory must not start with '~'; configuration values are "
          "not expanded by a shell"));
    }
    if (
      !configuration.developer_mode
      && !configuration.data_directory.is_absolute()) {
        return std::unexpected(make_error(
          config_errc::invalid_data_directory,
          "kwaque.data_directory",
          "data directory must be an absolute path unless developer_mode is "
          "true"));
    }

    return configuration;
}

} // namespace

bootstrap_config_result parse_bootstrap_config(std::string_view yaml) {
    if (yaml.size() > max_bootstrap_config_bytes) {
        return std::unexpected(make_error(
          config_errc::input_too_large,
          "root",
          "configuration exceeds the maximum supported size"));
    }
    if (!is_printable_utf8(yaml)) {
        return std::unexpected(make_error(
          config_errc::malformed_yaml,
          "root",
          "configuration must be UTF-8 text without control characters"));
    }
    const std::string text{yaml};
    try {
        if (auto checked = check_structure(text); !checked) {
            return std::unexpected(std::move(checked.error()));
        }
        return decode_bootstrap_config(YAML::Load(text));
    } catch (const YAML::Exception& error) {
        return std::unexpected(make_error(
          config_errc::malformed_yaml,
          "root",
          "unable to parse YAML: " + std::string(error.what())));
    }
}

bootstrap_config_result
load_bootstrap_config(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    if (!input) {
        return std::unexpected(make_error(
          config_errc::file_unavailable,
          "config",
          "unable to open configuration file: " + path.string()));
    }

    std::string contents(max_bootstrap_config_bytes + 1, '\0');
    input.read(contents.data(), static_cast<std::streamsize>(contents.size()));
    const auto bytes_read = static_cast<std::size_t>(input.gcount());
    if (input.bad()) {
        return std::unexpected(make_error(
          config_errc::file_unavailable,
          "config",
          "unable to read configuration file: " + path.string()));
    }
    if (bytes_read > max_bootstrap_config_bytes) {
        return std::unexpected(make_error(
          config_errc::input_too_large,
          "config",
          "configuration exceeds the maximum supported size"));
    }
    contents.resize(bytes_read);
    return parse_bootstrap_config(contents);
}

std::string render_config(std::span<const config_value> values) {
    std::string output;
    for (const auto& value : values) {
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

std::string render_config(const bootstrap_config& configuration) {
    const std::string schema_version = std::to_string(
      configuration.schema_version);
    const std::string data_directory = configuration.data_directory.string();
    const std::string admin_port = std::to_string(configuration.admin_port);
    const std::string crash_loop_limit = configuration.crash_loop_limit
                                           ? std::to_string(
                                               *configuration.crash_loop_limit)
                                           : "null";
    const std::string developer_mode = configuration.developer_mode ? "true"
                                                                    : "false";
    const std::string diagnostic_memory
      = configuration.diagnostic_memory_per_shard_bytes
          ? std::to_string(*configuration.diagnostic_memory_per_shard_bytes)
          : "unspecified";
    const std::array values{
      config_value{"schema_version", schema_version, config_visibility::safe},
      config_value{"data_directory", data_directory, config_visibility::safe},
      config_value{
        "admin_address", configuration.admin_address, config_visibility::safe},
      config_value{"admin_port", admin_port, config_visibility::safe},
      config_value{"developer_mode", developer_mode, config_visibility::safe},
      config_value{
        "crash_loop_limit", crash_loop_limit, config_visibility::safe},
      config_value{
        "storage_strict_data_init",
        configuration.storage_strict_data_init ? "true" : "false",
        config_visibility::safe},
      config_value{
        "diagnostic_memory_per_shard_bytes",
        diagnostic_memory,
        config_visibility::safe},
    };
    return render_config(values);
}

std::string render_config_error(const config_error& error) {
    std::string output{"field="};
    append_escaped(output, error.field, 128);
    output += " message=";
    append_escaped(output, error.message, 256);
    return output;
}

} // namespace kwaque::config
