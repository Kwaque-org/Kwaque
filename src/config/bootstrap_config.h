#pragma once

#include <cstdint>
#include <expected>
#include <filesystem>
#include <span>
#include <string>
#include <string_view>

namespace kwaque::config {

inline constexpr std::uint32_t bootstrap_config_schema_version = 1;

enum class log_level { trace, debug, info, warn, error };

struct bootstrap_config final {
  std::uint32_t schema_version{bootstrap_config_schema_version};
  std::int32_t node_id{0};
  std::filesystem::path data_directory{"./data"};
  std::string admin_address{"127.0.0.1"};
  std::uint16_t admin_port{9644};
  log_level level{log_level::info};
  bool developer_mode{false};

  bool operator==(const bootstrap_config &) const = default;
};

enum class config_errc {
  file_unavailable,
  malformed_yaml,
  missing_key,
  unknown_key,
  duplicate_key,
  invalid_type,
  invalid_node_id,
  invalid_data_directory,
  invalid_admin_address,
  invalid_admin_port,
  invalid_log_level,
  unsupported_schema_version,
};

struct config_error final {
  config_errc code;
  std::string field;
  std::string message;
};

using bootstrap_config_result =
    std::expected<bootstrap_config, config_error>;

enum class config_visibility { redacted, safe };

struct config_value final {
  std::string_view name;
  std::string_view value;
  config_visibility visibility{config_visibility::redacted};
};

[[nodiscard]] bootstrap_config_result
parse_bootstrap_config(std::string_view yaml);

[[nodiscard]] bootstrap_config_result
load_bootstrap_config(const std::filesystem::path &path);

[[nodiscard]] std::string_view to_string(log_level level) noexcept;

[[nodiscard]] std::string
render_config(std::span<const config_value> values);

[[nodiscard]] std::string
render_config(const bootstrap_config &configuration);

} // namespace kwaque::config
