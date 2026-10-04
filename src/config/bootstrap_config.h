#pragma once

#include "src/base/units.h"

#include <array>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <optional>
#include <span>
#include <string>
#include <string_view>

namespace kwaque::config {

// Any change to the key set or to the meaning of a key increments the schema
// version. A binary accepts the inclusive range below, so a new binary can run
// with the previous configuration before the configuration is upgraded.
inline constexpr std::uint32_t bootstrap_config_schema_version = 1;
inline constexpr std::uint32_t minimum_bootstrap_config_schema_version = 1;
inline constexpr std::uint32_t maximum_bootstrap_config_schema_version = 1;
inline constexpr std::size_t max_bootstrap_config_bytes = 64_KiB;
// The schema nests three collections deep; anything deeper is rejected before
// the parser recurses further, so parsing fits a small reactor thread stack.
inline constexpr std::size_t max_bootstrap_config_depth = 8;
inline constexpr std::size_t max_rendered_config_value_bytes = 256;

inline constexpr std::array<std::string_view, 7> bootstrap_config_keys{
  "schema_version",
  "data_directory",
  "admin",
  "developer_mode",
  "storage_strict_data_init",
  "crash_loop_limit",
  "diagnostic_memory_per_shard_bytes"};
inline constexpr std::array<std::string_view, 2> bootstrap_admin_keys{
  "address", "port"};

struct bootstrap_config final {
    std::uint32_t schema_version{bootstrap_config_schema_version};
    std::filesystem::path data_directory{"./data"};
    std::string admin_address{"127.0.0.1"};
    std::uint16_t admin_port{9644};
    bool developer_mode{false};
    bool storage_strict_data_init{false};
    std::optional<std::uint32_t> crash_loop_limit{5};
    std::optional<std::uint64_t> diagnostic_memory_per_shard_bytes;

    bool operator==(const bootstrap_config&) const = default;
};

enum class config_errc {
    file_unavailable,
    malformed_yaml,
    input_too_large,
    missing_key,
    unknown_key,
    duplicate_key,
    invalid_type,
    invalid_data_directory,
    invalid_admin_address,
    invalid_admin_port,
    unsupported_schema_version,
    invalid_memory_budget,
    invalid_crash_loop_limit,
};

struct config_error final {
    config_errc code;
    std::string field;
    std::string message;
};

using bootstrap_config_result = std::expected<bootstrap_config, config_error>;

enum class config_visibility { redacted, safe };

struct config_value final {
    std::string_view name;
    std::string_view value;
    config_visibility visibility{config_visibility::redacted};
};

// Parses one YAML 1.2 document of UTF-8 text. Integers are plain decimal
// without leading zeros, booleans are true or false in any of the three core
// spellings, and typed values must be unquoted and untagged.
[[nodiscard]] bootstrap_config_result
parse_bootstrap_config(std::string_view yaml);

[[nodiscard]] bootstrap_config_result
load_bootstrap_config(const std::filesystem::path& path);

[[nodiscard]] std::string render_config(std::span<const config_value> values);

[[nodiscard]] std::string render_config(const bootstrap_config& configuration);

[[nodiscard]] std::string render_config_error(const config_error& error);

} // namespace kwaque::config
