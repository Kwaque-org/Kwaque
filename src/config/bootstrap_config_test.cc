#include "src/config/bootstrap_config.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

using kwaque::config::bootstrap_config;
using kwaque::config::config_errc;
using kwaque::config::config_value;
using kwaque::config::config_visibility;
using kwaque::config::load_bootstrap_config;
using kwaque::config::log_level;
using kwaque::config::parse_bootstrap_config;
using kwaque::config::render_config;

std::filesystem::path example_config_path() {
    const char* test_srcdir = std::getenv("TEST_SRCDIR");
    const char* test_workspace = std::getenv("TEST_WORKSPACE");
    if (test_srcdir == nullptr || test_workspace == nullptr) {
        return {};
    }
    return std::filesystem::path(test_srcdir) / test_workspace / "conf"
           / "kwaque.yaml";
}

TEST(BootstrapConfigTest, HasSafeDefaults) {
    const bootstrap_config configuration;
    EXPECT_EQ(configuration.schema_version, 1U);
    EXPECT_EQ(configuration.node_id, 0);
    EXPECT_EQ(configuration.data_directory, "./data");
    EXPECT_EQ(configuration.admin_address, "127.0.0.1");
    EXPECT_EQ(configuration.admin_port, 9644);
    EXPECT_EQ(configuration.level, log_level::info);
    EXPECT_FALSE(configuration.developer_mode);
}

TEST(BootstrapConfigTest, LoadsCommittedExample) {
    const auto configuration = load_bootstrap_config(example_config_path());
    ASSERT_TRUE(configuration.has_value())
      << (configuration ? "" : configuration.error().message);
    EXPECT_EQ(configuration->schema_version, 1U);
    EXPECT_EQ(configuration->node_id, 0);
    EXPECT_EQ(configuration->data_directory, "./data");
    EXPECT_EQ(configuration->admin_address, "127.0.0.1");
    EXPECT_EQ(configuration->admin_port, 9644);
    EXPECT_EQ(configuration->level, log_level::info);
    EXPECT_TRUE(configuration->developer_mode);
}

TEST(BootstrapConfigTest, RejectsInvalidConfiguration) {
    const std::vector<std::pair<std::string_view, config_errc>> cases{
      {"kwaque: {schema_version: 1, unknown: true}", config_errc::unknown_key},
      {"kwaque: {schema_version: 1, node_id: -1}",
       config_errc::invalid_node_id},
      {"kwaque: {schema_version: 1, data_directory: ''}",
       config_errc::invalid_data_directory},
      {"kwaque: {schema_version: 1, data_directory: 123}",
       config_errc::invalid_type},
      {"kwaque: {schema_version: 1, data_directory: true}",
       config_errc::invalid_type},
      {"kwaque: {schema_version: 1, admin: {address: 'bad address'}}",
       config_errc::invalid_admin_address},
      {"kwaque: {schema_version: 1, admin: {address: 123}}",
       config_errc::invalid_type},
      {"kwaque: {schema_version: 1, admin: {address: false}}",
       config_errc::invalid_type},
      {"kwaque: {schema_version: 1, admin: {address: \"bad\\naddress\"}}",
       config_errc::invalid_admin_address},
      {"kwaque: {schema_version: 1, admin: {port: 0}}",
       config_errc::invalid_admin_port},
      {"kwaque: {schema_version: 1, admin: {port: 65536}}",
       config_errc::invalid_admin_port},
      {"kwaque: {schema_version: 1, admin: {unknown: true}}",
       config_errc::unknown_key},
      {"kwaque: {schema_version: 1, log_level: verbose}",
       config_errc::invalid_log_level},
      {"kwaque: {schema_version: 1, log_level: 123}",
       config_errc::invalid_type},
      {"kwaque: {schema_version: 1, log_level: true}",
       config_errc::invalid_type},
      {"kwaque: {node_id: 0}", config_errc::missing_key},
      {"kwaque: [schema_version, 1]", config_errc::invalid_type},
      {"kwaque: {schema_version: nope}", config_errc::invalid_type},
      {"kwaque: {schema_version: 1}\nunexpected: true",
       config_errc::unknown_key},
      {"kwaque: {schema_version: 1", config_errc::malformed_yaml},
    };

    for (const auto& [yaml, expected_error] : cases) {
        SCOPED_TRACE(yaml);
        const auto configuration = parse_bootstrap_config(yaml);
        ASSERT_FALSE(configuration.has_value());
        EXPECT_EQ(configuration.error().code, expected_error);
        EXPECT_FALSE(configuration.error().field.empty());
        EXPECT_FALSE(configuration.error().message.empty());
    }
}

TEST(BootstrapConfigTest, PreservesExplicitStringScalars) {
    const auto configuration = parse_bootstrap_config(R"yaml(
kwaque:
  schema_version: 1
  data_directory: "123"
  admin:
    address: 'true'
  log_level: !!str info
)yaml");

    ASSERT_TRUE(configuration.has_value())
      << (configuration ? "" : configuration.error().message);
    EXPECT_EQ(configuration->data_directory, "123");
    EXPECT_EQ(configuration->admin_address, "true");
    EXPECT_EQ(configuration->level, log_level::info);
}

TEST(BootstrapConfigTest, RejectsUnsupportedFutureSchemaVersion) {
    const auto configuration = parse_bootstrap_config(
      "kwaque: {schema_version: 2}");

    ASSERT_FALSE(configuration.has_value());
    EXPECT_EQ(
      configuration.error().code, config_errc::unsupported_schema_version);
    EXPECT_EQ(configuration.error().field, "kwaque.schema_version");
    EXPECT_EQ(
      configuration.error().message,
      "unsupported configuration schema version 2; supported version is 1");
}

TEST(BootstrapConfigTest, ReportsUnavailableConfigurationFile) {
    const auto configuration = load_bootstrap_config({});

    ASSERT_FALSE(configuration.has_value());
    EXPECT_EQ(configuration.error().code, config_errc::file_unavailable);
}

TEST(BootstrapConfigTest, RejectsDuplicateKeys) {
    const auto configuration = parse_bootstrap_config(R"yaml(
kwaque:
  schema_version: 1
  node_id: 0
  node_id: 1
)yaml");
    ASSERT_FALSE(configuration.has_value());
    EXPECT_EQ(configuration.error().code, config_errc::duplicate_key);
    EXPECT_EQ(configuration.error().field, "kwaque.node_id");
}

TEST(BootstrapConfigTest, RedactsValuesUnlessExplicitlySafe) {
    constexpr std::array values{
      config_value{"node_id", "7", config_visibility::safe},
      config_value{"future_secret", "do-not-log"},
    };
    const std::string rendered = render_config(values);

    EXPECT_EQ(rendered, "node_id=7 future_secret=<redacted>");
    EXPECT_EQ(rendered.find("do-not-log"), std::string::npos);
}

TEST(BootstrapConfigTest, EscapesSafeValuesForSingleLineLogs) {
    std::string path{"first\nsecond\r\t\\"};
    path.push_back('\0');
    path.push_back('\x07');
    path.push_back('\x1b');
    path.push_back('\x1f');
    path.push_back('\x7f');
    const std::array values{
      config_value{"path", path, config_visibility::safe},
    };
    EXPECT_EQ(
      render_config(values),
      "path=first\\nsecond\\r\\t\\\\\\x00\\x07\\x1b\\x1f\\x7f");
}

} // namespace
