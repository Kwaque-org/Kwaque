#include "src/config/bootstrap_config.h"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
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
using kwaque::config::parse_bootstrap_config;
using kwaque::config::render_config;
using kwaque::config::render_config_error;

std::filesystem::path example_config_path() {
    const char* test_srcdir = std::getenv("TEST_SRCDIR");
    const char* test_workspace = std::getenv("TEST_WORKSPACE");
    if (test_srcdir == nullptr || test_workspace == nullptr) {
        return {};
    }
    return std::filesystem::path(test_srcdir) / test_workspace / "conf"
           / "kwaque.yaml";
}

std::filesystem::path production_example_path() {
    auto path = example_config_path();
    return path.empty() ? path
                        : path.replace_filename("kwaque.production.yaml");
}

TEST(BootstrapConfigTest, HasSafeDefaults) {
    const bootstrap_config configuration;
    EXPECT_EQ(configuration.schema_version, 1U);
    EXPECT_EQ(configuration.data_directory, "./data");
    EXPECT_EQ(configuration.admin_address, "127.0.0.1");
    EXPECT_EQ(configuration.admin_port, 9644);
    EXPECT_FALSE(configuration.developer_mode);
    EXPECT_FALSE(configuration.storage_strict_data_init);
    EXPECT_EQ(configuration.crash_loop_limit, 5U);
    EXPECT_FALSE(configuration.diagnostic_memory_per_shard_bytes);
}

TEST(BootstrapConfigTest, LoadsCommittedDevelopmentExample) {
    const auto configuration = load_bootstrap_config(example_config_path());
    ASSERT_TRUE(configuration.has_value())
      << (configuration ? "" : configuration.error().message);
    EXPECT_EQ(configuration->schema_version, 1U);
    EXPECT_EQ(configuration->data_directory, "./data");
    EXPECT_EQ(configuration->admin_address, "127.0.0.1");
    EXPECT_EQ(configuration->admin_port, 9644);
    EXPECT_TRUE(configuration->developer_mode);
    EXPECT_FALSE(configuration->storage_strict_data_init);
    EXPECT_EQ(configuration->diagnostic_memory_per_shard_bytes, 134217728U);
}

TEST(BootstrapConfigTest, LoadsCommittedProductionExample) {
    const auto configuration = load_bootstrap_config(production_example_path());
    ASSERT_TRUE(configuration.has_value())
      << (configuration ? "" : configuration.error().message);
    EXPECT_FALSE(configuration->developer_mode);
    EXPECT_TRUE(configuration->data_directory.is_absolute());
    EXPECT_EQ(configuration->admin_address, "127.0.0.1");
    EXPECT_EQ(configuration->crash_loop_limit, 5U);
    EXPECT_FALSE(configuration->diagnostic_memory_per_shard_bytes);
}

TEST(BootstrapConfigTest, RejectsInvalidConfiguration) {
    const std::vector<std::pair<std::string_view, config_errc>> cases{
      {"kwaque: {schema_version: 1, unknown: true}", config_errc::unknown_key},
      {"kwaque: {schema_version: 1, storage_strict_data_init: []}",
       config_errc::invalid_type},
      {"kwaque: {schema_version: 1, crash_loop_limit: -1}",
       config_errc::invalid_crash_loop_limit},
      {"kwaque: {schema_version: 1, crash_loop_limit: 4294967296}",
       config_errc::invalid_crash_loop_limit},
      {"kwaque: {schema_version: 1, crash_loop_limit: []}",
       config_errc::invalid_type},
      {"kwaque: {schema_version: 1, diagnostic_memory_per_shard_bytes: -1}",
       config_errc::invalid_memory_budget},
      {"kwaque: {schema_version: 1, diagnostic_memory_per_shard_bytes: 0}",
       config_errc::invalid_memory_budget},
      {"kwaque: {schema_version: 1, diagnostic_memory_per_shard_bytes: "
       "18446744073709551615}",
       config_errc::invalid_type},
      {"kwaque: {schema_version: 1, runtime: {memory: 1}}",
       config_errc::unknown_key},
      {"kwaque: {schema_version: 1, simulation: {seed: 1}}",
       config_errc::unknown_key},
      {"kwaque: {schema_version: 1, resource_memory: 67108864}",
       config_errc::unknown_key},
      {"kwaque: {schema_version: 1, reactor_headroom: 16777216}",
       config_errc::unknown_key},
      // Broker identity comes from the cluster registry, and log levels from
      // the runtime's command-line options; neither is configuration.
      {"kwaque: {schema_version: 1, node_id: 0}", config_errc::unknown_key},
      {"kwaque: {schema_version: 1, log_level: info}",
       config_errc::unknown_key},
      {"kwaque: {schema_version: 1, data_directory: ''}",
       config_errc::invalid_data_directory},
      {"kwaque: {schema_version: 1, data_directory: 123}",
       config_errc::invalid_type},
      {"kwaque: {schema_version: 1, data_directory: true}",
       config_errc::invalid_type},
      {"kwaque: {schema_version: 1, data_directory: 1.5}",
       config_errc::invalid_type},
      {"kwaque: {schema_version: 1, admin: {address: 'bad address'}}",
       config_errc::invalid_admin_address},
      {"kwaque: {schema_version: 1, admin: {address: 123}}",
       config_errc::invalid_type},
      {"kwaque: {schema_version: 1, admin: {address: false}}",
       config_errc::invalid_type},
      {"kwaque: {schema_version: 1, admin: {address: not-an-address}}",
       config_errc::invalid_admin_address},
      {"kwaque: {schema_version: 1, admin: {address: \"bad\\naddress\"}}",
       config_errc::invalid_admin_address},
      {"kwaque: {schema_version: 1, admin: {port: 0}}",
       config_errc::invalid_admin_port},
      {"kwaque: {schema_version: 1, admin: {port: 65536}}",
       config_errc::invalid_admin_port},
      {"kwaque: {schema_version: 1, admin: {unknown: true}}",
       config_errc::unknown_key},
      {"kwaque: {developer_mode: true}", config_errc::missing_key},
      {"kwaque: [schema_version, 1]", config_errc::invalid_type},
      {"[kwaque]", config_errc::invalid_type},
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

TEST(BootstrapConfigTest, AcceptsOnlyCanonicalUntaggedIntegers) {
    // YAML 1.1 reads 010000 as octal 4096 while YAML 1.2 reads 10000; prefixes,
    // signs and quoting are rejected rather than resolved either way.
    for (const auto* port :
         {"010000",
          "0x1F",
          "0o17",
          "+9644",
          "\"9644\"",
          "'9644'",
          "!!int 9644",
          "9_644",
          "9644.0",
          "1e3"}) {
        SCOPED_TRACE(port);
        const auto configuration = parse_bootstrap_config(
          std::string{"kwaque: {schema_version: 1, developer_mode: true, "
                      "admin: {port: "}
          + port + "}}");
        ASSERT_FALSE(configuration.has_value());
        EXPECT_EQ(configuration.error().code, config_errc::invalid_type);
        EXPECT_EQ(configuration.error().field, "kwaque.admin.port");
    }
    for (const auto* version : {"01", "\"1\"", "!!int 1"}) {
        SCOPED_TRACE(version);
        const auto configuration = parse_bootstrap_config(
          std::string{"kwaque: {schema_version: "} + version + "}");
        ASSERT_FALSE(configuration.has_value());
        EXPECT_EQ(configuration.error().code, config_errc::invalid_type);
    }
    // A canonical integer reaches its range check.
    const auto range_checked = parse_bootstrap_config(
      "kwaque: {schema_version: 1, developer_mode: true, admin: {port: 0}}");
    ASSERT_FALSE(range_checked.has_value());
    EXPECT_EQ(range_checked.error().code, config_errc::invalid_admin_port);
}

TEST(BootstrapConfigTest, AcceptsOnlyCoreSchemaBooleans) {
    for (const auto* value : {"true", "True", "TRUE"}) {
        SCOPED_TRACE(value);
        const auto configuration = parse_bootstrap_config(
          std::string{"kwaque: {schema_version: 1, developer_mode: "} + value
          + "}");
        ASSERT_TRUE(configuration.has_value()) << configuration.error().message;
        EXPECT_TRUE(configuration->developer_mode);
    }
    for (const auto* value :
         {"yes",
          "on",
          "y",
          "Yes",
          "ON",
          "no",
          "off",
          "n",
          "1",
          "\"true\"",
          "!!bool true",
          "tRUE"}) {
        SCOPED_TRACE(value);
        const auto configuration = parse_bootstrap_config(
          std::string{"kwaque: {schema_version: 1, developer_mode: "} + value
          + "}");
        ASSERT_FALSE(configuration.has_value());
        EXPECT_EQ(configuration.error().code, config_errc::invalid_type);
        EXPECT_EQ(configuration.error().field, "kwaque.developer_mode");
    }
}

TEST(BootstrapConfigTest, ResolvesStringScalarsByTheYaml12CoreSchema) {
    // Plain text that the 1.2 core schema keeps as a string is accepted;
    // YAML 1.1 would have read these as booleans.
    for (const auto* path : {"yes", "off", "/srv/on"}) {
        SCOPED_TRACE(path);
        const auto configuration = parse_bootstrap_config(
          std::string{"kwaque: {schema_version: 1, developer_mode: true, "
                      "data_directory: "}
          + path + "}");
        ASSERT_TRUE(configuration.has_value()) << configuration.error().message;
        EXPECT_EQ(configuration->data_directory, path);
    }
}

TEST(BootstrapConfigTest, PreservesExplicitHostAndDiagnosticPolicies) {
    const auto configuration = parse_bootstrap_config(
      "kwaque: {schema_version: 1, data_directory: /srv/kwaque, "
      "storage_strict_data_init: true, "
      "diagnostic_memory_per_shard_bytes: 100663296}");
    ASSERT_TRUE(configuration) << configuration.error().message;
    EXPECT_TRUE(configuration->storage_strict_data_init);
    EXPECT_EQ(configuration->diagnostic_memory_per_shard_bytes, 100663296U);
    const auto rendered = render_config(*configuration);
    EXPECT_NE(
      rendered.find("storage_strict_data_init=true"), std::string::npos);
    EXPECT_NE(
      rendered.find("diagnostic_memory_per_shard_bytes=100663296"),
      std::string::npos);
}

TEST(BootstrapConfigTest, PreservesExplicitStringScalars) {
    const auto configuration = parse_bootstrap_config(R"yaml(
kwaque:
  schema_version: 1
  developer_mode: true
  data_directory: "123"
  admin:
    address: !!str 127.0.0.2
)yaml");

    ASSERT_TRUE(configuration.has_value())
      << (configuration ? "" : configuration.error().message);
    EXPECT_EQ(configuration->data_directory, "123");
    EXPECT_EQ(configuration->admin_address, "127.0.0.2");
}

TEST(BootstrapConfigTest, RequiresAnAbsoluteDataDirectoryOutsideDeveloperMode) {
    for (const auto* yaml :
         {"kwaque: {schema_version: 1}",
          "kwaque: {schema_version: 1, data_directory: data}",
          "kwaque: {schema_version: 1, data_directory: ./data}",
          "kwaque: {schema_version: 1, developer_mode: false, "
          "data_directory: ../data}"}) {
        SCOPED_TRACE(yaml);
        const auto configuration = parse_bootstrap_config(yaml);
        ASSERT_FALSE(configuration.has_value());
        EXPECT_EQ(
          configuration.error().code, config_errc::invalid_data_directory);
    }
    const auto production = parse_bootstrap_config(
      "kwaque: {schema_version: 1, data_directory: /var/lib/kwaque}");
    ASSERT_TRUE(production.has_value()) << production.error().message;
    const auto development = parse_bootstrap_config(
      "kwaque: {schema_version: 1, developer_mode: true, data_directory: "
      "data}");
    ASSERT_TRUE(development.has_value()) << development.error().message;
}

TEST(BootstrapConfigTest, RejectsAHomeDirectoryShorthand) {
    for (const auto* path : {"'~/data'", "'~'", "'~operator/data'"}) {
        SCOPED_TRACE(path);
        const auto configuration = parse_bootstrap_config(
          std::string{"kwaque: {schema_version: 1, developer_mode: true, "
                      "data_directory: "}
          + path + "}");
        ASSERT_FALSE(configuration.has_value());
        EXPECT_EQ(
          configuration.error().code, config_errc::invalid_data_directory);
    }
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

TEST(BootstrapConfigTest, ReportsAFutureVersionBeforeItsNewKeys) {
    // A newer configuration has keys this binary does not know; the version
    // is the diagnosis, not the first unknown key.
    for (const auto* yaml :
         {"kwaque: {schema_version: 2, future_setting: 1}",
          "kwaque: {schema_version: 2}\nfuture_root: 1",
          "kwaque: {schema_version: 0, admin: {future: 1}}"}) {
        SCOPED_TRACE(yaml);
        const auto configuration = parse_bootstrap_config(yaml);
        ASSERT_FALSE(configuration.has_value());
        EXPECT_EQ(
          configuration.error().code, config_errc::unsupported_schema_version);
    }
}

TEST(BootstrapConfigTest, RejectsMoreThanOneDocument) {
    for (const auto* yaml :
         {"kwaque: {schema_version: 1, developer_mode: true}\n---\n"
          "kwaque: {schema_version: 1}\n",
          "kwaque: {schema_version: 1, developer_mode: true}\n...\n---\n"
          "unknown: [\n"}) {
        SCOPED_TRACE(yaml);
        const auto configuration = parse_bootstrap_config(yaml);
        ASSERT_FALSE(configuration.has_value());
        EXPECT_EQ(configuration.error().code, config_errc::malformed_yaml);
    }
}

TEST(BootstrapConfigTest, BoundsNestingBeforeTheParserRecursesDeeply) {
    // The broker parses on a small reactor thread stack; nesting is refused
    // at the schema's depth bound, far below the parser's own recursion guard.
    for (const std::size_t depth :
         {kwaque::config::max_bootstrap_config_depth + 1,
          std::size_t{501},
          std::size_t{20000}}) {
        SCOPED_TRACE(depth);
        const auto yaml = "kwaque: " + std::string(depth, '[')
                          + std::string(depth, ']');
        const auto configuration = parse_bootstrap_config(yaml);
        ASSERT_FALSE(configuration.has_value());
        EXPECT_EQ(configuration.error().code, config_errc::malformed_yaml);
        EXPECT_NE(
          configuration.error().message.find("nests deeper"),
          std::string::npos);
    }
    std::string block{"kwaque:\n"};
    for (std::size_t level = 0; level < 12; ++level) {
        block += std::string(2 * (level + 1), ' ') + "k:\n";
    }
    const auto configuration = parse_bootstrap_config(block);
    ASSERT_FALSE(configuration.has_value());
    EXPECT_EQ(configuration.error().code, config_errc::malformed_yaml);
}

TEST(BootstrapConfigTest, RejectsInputThatIsNotPrintableUtf8) {
    const std::string valid{
      "kwaque: {schema_version: 1, developer_mode: true}"};
    for (const auto& invalid :
         {valid + '\x04',
          valid + std::string(1, '\0'),
          valid + '\x7f',
          valid + "\xff",
          valid + "\xc0\x80",
          valid + "\xed\xa0\x80",
          valid + "\xc2\x9b",
          std::string{"\xff\xfe"} + valid}) {
        const auto configuration = parse_bootstrap_config(invalid);
        ASSERT_FALSE(configuration.has_value());
        EXPECT_EQ(configuration.error().code, config_errc::malformed_yaml);
    }
    const auto unicode = parse_bootstrap_config(
      "kwaque: {schema_version: 1, developer_mode: true, data_directory: "
      "\"/srv/\xc3\xa9t\xc3\xa9\"}");
    ASSERT_TRUE(unicode.has_value()) << unicode.error().message;
    EXPECT_EQ(unicode->data_directory, "/srv/\xc3\xa9t\xc3\xa9");
}

TEST(BootstrapConfigTest, RendersEverySchemaKey) {
    // Rendering is an allowlist; a new key must be rendered explicitly or it
    // would silently vanish from the startup record.
    const auto rendered = render_config(bootstrap_config{});
    for (const auto key : kwaque::config::bootstrap_config_keys) {
        if (key == "admin") {
            for (const auto admin_key : kwaque::config::bootstrap_admin_keys) {
                EXPECT_NE(
                  rendered.find("admin_" + std::string{admin_key} + "="),
                  std::string::npos)
                  << admin_key;
            }
            continue;
        }
        EXPECT_NE(rendered.find(std::string{key} + "="), std::string::npos)
          << key;
    }
}

TEST(BootstrapConfigTest, RejectsInputAboveTheProductionLimit) {
    const std::string oversized(
      kwaque::config::max_bootstrap_config_bytes + 1, 'x');
    const auto configuration = parse_bootstrap_config(oversized);

    ASSERT_FALSE(configuration.has_value());
    EXPECT_EQ(configuration.error().code, config_errc::input_too_large);
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
  developer_mode: true
  developer_mode: false
)yaml");
    ASSERT_FALSE(configuration.has_value());
    EXPECT_EQ(configuration.error().code, config_errc::duplicate_key);
    EXPECT_EQ(configuration.error().field, "kwaque.developer_mode");
}

TEST(BootstrapConfigTest, RedactsValuesUnlessExplicitlySafe) {
    constexpr std::array values{
      config_value{"admin_port", "9644", config_visibility::safe},
      config_value{"future_secret", "do-not-log"},
    };
    const std::string rendered = render_config(values);

    EXPECT_EQ(rendered, "admin_port=9644 future_secret=<redacted>");
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

TEST(BootstrapConfigTest, BoundsAndEscapesErrorRendering) {
    std::string field(160, 'f');
    std::string message{"first\nsecond"};
    message.push_back(static_cast<char>(0x80));
    message.append(300, 'm');
    const std::string rendered = render_config_error(
      kwaque::config::config_error{
        .code = config_errc::malformed_yaml,
        .field = std::move(field),
        .message = std::move(message),
      });

    EXPECT_EQ(rendered.find('\n'), std::string::npos);
    EXPECT_NE(rendered.find("\\n"), std::string::npos);
    EXPECT_NE(rendered.find("\\x80"), std::string::npos);
    EXPECT_NE(rendered.find("<truncated>"), std::string::npos);
}

} // namespace

TEST(BootstrapConfigLimitTest, AcceptsZeroAndLargestCrashLoopLimit) {
    for (const auto& value : {std::string{"0"}, std::string{"4294967295"}}) {
        const auto parsed = kwaque::config::parse_bootstrap_config(
          "kwaque: {schema_version: 1, developer_mode: true, crash_loop_limit: "
          + value + "}");
        ASSERT_TRUE(parsed);
        ASSERT_TRUE(parsed->crash_loop_limit);
        EXPECT_EQ(std::to_string(*parsed->crash_loop_limit), value);
        EXPECT_NE(
          kwaque::config::render_config(*parsed).find(
            "crash_loop_limit=" + value),
          std::string::npos);
    }
}

TEST(BootstrapConfigLimitTest, NullDisablesTheFiniteCrashLoopLimit) {
    for (const auto value : {"null", "~", ""}) {
        const auto parsed = kwaque::config::parse_bootstrap_config(
          std::string{"kwaque: {schema_version: 1, developer_mode: true, "
                      "crash_loop_limit: "}
          + value + "}");
        ASSERT_TRUE(parsed);
        EXPECT_FALSE(parsed->crash_loop_limit);
        EXPECT_NE(
          kwaque::config::render_config(*parsed).find("crash_loop_limit=null"),
          std::string::npos);
    }
}
