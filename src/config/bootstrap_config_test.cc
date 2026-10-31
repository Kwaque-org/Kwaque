#include "src/config/bootstrap_config.h"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
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

// Every accepted configuration lists its devices.
std::string with_device(std::string_view yaml) {
    std::string text{yaml};
    if (text.starts_with("kwaque: {")) {
        const auto close = text.rfind('}');
        text.insert(
          close, ", storage: {devices: [{path: /var/lib/kwaque, role: all}]}");
        return text;
    }
    return text
           + "  storage:\n    devices:\n      - {path: /var/lib/kwaque, role: "
             "all}\n";
}

kwaque::config::bootstrap_config_result
parse_with_device(std::string_view yaml) {
    return parse_bootstrap_config(with_device(yaml));
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
        const auto configuration = parse_with_device(
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
        const auto configuration = parse_with_device(
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
        const auto configuration = parse_with_device(
          std::string{"kwaque: {schema_version: 1, developer_mode: true, "
                      "data_directory: "}
          + path + "}");
        ASSERT_TRUE(configuration.has_value()) << configuration.error().message;
        EXPECT_EQ(configuration->data_directory, path);
    }
}

TEST(BootstrapConfigTest, PreservesExplicitHostAndDiagnosticPolicies) {
    const auto configuration = parse_with_device(
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
    const auto configuration = parse_with_device(R"yaml(
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
        const auto configuration = parse_with_device(yaml);
        ASSERT_FALSE(configuration.has_value());
        EXPECT_EQ(
          configuration.error().code, config_errc::invalid_data_directory);
    }
    const auto production = parse_with_device(
      "kwaque: {schema_version: 1, data_directory: /var/lib/kwaque}");
    ASSERT_TRUE(production.has_value()) << production.error().message;
    const auto development = parse_with_device(
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
        const auto configuration = parse_with_device(invalid);
        ASSERT_FALSE(configuration.has_value());
        EXPECT_EQ(configuration.error().code, config_errc::malformed_yaml);
    }
    const auto unicode = parse_with_device(
      "kwaque: {schema_version: 1, developer_mode: true, data_directory: "
      "\"/srv/\xc3\xa9t\xc3\xa9\"}");
    ASSERT_TRUE(unicode.has_value()) << unicode.error().message;
    EXPECT_EQ(unicode->data_directory, "/srv/\xc3\xa9t\xc3\xa9");
}

TEST(BootstrapConfigTest, RendersEverySchemaKey) {
    // Rendering is an allowlist; a new key must be rendered explicitly or it
    // would silently vanish from the startup record.
    const auto parsed = parse_with_device(
      "kwaque: {schema_version: 1, developer_mode: true}");
    ASSERT_TRUE(parsed) << parsed.error().message;
    const auto rendered = render_config(*parsed);
    const auto nested = [&rendered](std::string_view group, auto keys) {
        for (const auto key : keys) {
            if (key == "devices") {
                continue;
            }
            EXPECT_NE(
              rendered.find(std::string{group} + "." + std::string{key} + "="),
              std::string::npos)
              << group << "." << key;
        }
    };
    for (const auto key : kwaque::config::bootstrap_config_keys) {
        if (key == "admin") {
            for (const auto admin_key : kwaque::config::bootstrap_admin_keys) {
                EXPECT_NE(
                  rendered.find("admin_" + std::string{admin_key} + "="),
                  std::string::npos)
                  << admin_key;
            }
        } else if (key == "storage") {
            nested(key, kwaque::config::bootstrap_storage_keys);
            for (const auto device_key :
                 kwaque::config::bootstrap_device_keys) {
                EXPECT_NE(
                  rendered.find(
                    "storage.devices[0]." + std::string{device_key} + "="),
                  std::string::npos)
                  << device_key;
            }
        } else if (key == "metadata") {
            nested(key, kwaque::config::bootstrap_metadata_keys);
        } else if (key == "topic_defaults") {
            nested(key, kwaque::config::bootstrap_topic_default_keys);
        } else if (key == "range") {
            nested(key, kwaque::config::bootstrap_range_keys);
        } else {
            EXPECT_NE(rendered.find(std::string{key} + "="), std::string::npos)
              << key;
        }
    }
    // Derived and unbounded values say so.
    EXPECT_NE(
      rendered.find("topic_defaults.retention_bytes=derived"),
      std::string::npos);
    EXPECT_NE(
      rendered.find("range.maximum_consume_streams=derived"),
      std::string::npos);
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
        const auto parsed = parse_with_device(
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
        const auto parsed = parse_with_device(
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

namespace {

struct key_bounds final {
    std::string_view group;
    std::string_view key;
    std::uint64_t value;
    std::uint64_t minimum;
    std::uint64_t maximum;
    bool power_of_two;
};

constexpr std::uint64_t mebi = 1'048'576;
constexpr std::uint64_t no_maximum = 9'223'372'036'854'775'807;

// Every integer key with its default and bounds, as the configuration
// contract lists them.
constexpr std::array frozen_bounds{
  key_bounds{"storage", "metadata_alignment_bytes", 4'096, 512, 65'536, true},
  key_bounds{"metadata", "ring_size", 256, 1, 4'096, true},
  key_bounds{
    "metadata", "journal_file_bytes", 16 * mebi, mebi, 256 * mebi, false},
  key_bounds{"metadata", "journal_retained_files", 64, 4, 1'024, false},
  key_bounds{
    "metadata", "snapshot_floor_bytes", 4 * mebi, mebi, 256 * mebi, false},
  key_bounds{"metadata", "pending_proposals", 1'024, 64, 65'536, false},
  key_bounds{
    "metadata", "pending_proposal_bytes", 4 * mebi, mebi, 64 * mebi, false},
  key_bounds{"metadata", "cohort_records", 128, 1, 1'024, false},
  key_bounds{"metadata", "cohort_bytes", mebi, 65'536, 16 * mebi, false},
  key_bounds{
    "metadata",
    "producer_expiration_ms",
    86'400'000,
    60'000,
    no_maximum,
    false},
  key_bounds{
    "topic_defaults", "maximum_record_bytes", 1'048'576, 1, no_maximum, false},
  key_bounds{
    "topic_defaults", "maximum_batch_bytes", 1'052'672, 1, no_maximum, false},
  key_bounds{
    "topic_defaults", "segment_bytes", 1'000'000'000, 1, no_maximum, false},
  key_bounds{
    "topic_defaults",
    "segment_lifetime_ms",
    3'600'000,
    60'000,
    31'536'000'000,
    false},
  key_bounds{"topic_defaults", "retry_depth", 8, 1, 32, false},
  key_bounds{"topic_defaults", "active_leaf_cap", 4'096, 1, 4'096, false},
  key_bounds{"range", "stop_deadline_ms", 20'000, 1'000, 840'000, false},
  key_bounds{
    "range",
    "unsealed_bytes",
    10'737'418'240,
    128 * mebi,
    1'099'511'627'776,
    false},
  key_bounds{
    "range", "retention_sweep_interval_ms", 300'000, 1'000, 3'600'000, false},
  key_bounds{"range", "maximum_expiring_per_range", 64, 1, 1'024, false},
  key_bounds{"range", "queue_requests", 64, 1, 4'096, false},
  key_bounds{"range", "queue_bytes", 4 * mebi, 65'536, 64 * mebi, false},
  key_bounds{"range", "roll_rate_per_range_per_minute", 1, 1, 60, false},
  key_bounds{"range", "roll_burst_per_range", 2, 1, 16, false},
  key_bounds{"range", "roll_rate_per_shard_per_second", 8, 1, 1'000, false},
  key_bounds{"range", "roll_burst_per_shard", 16, 1, 1'024, false},
  key_bounds{"range", "placement_threshold_percent", 1, 0, 100, false},
  key_bounds{"range", "placement_byte_cost_ps", 500, 1, no_maximum, false},
  key_bounds{"range", "placement_batch_cost_ns", 5'000, 1, no_maximum, false},
  key_bounds{"range", "placement_minimum_load", 100, 1, no_maximum, false},
  key_bounds{"range", "maximum_consume_streams", 100'000, 1, 1'000'000, false},
  key_bounds{"range", "wheel_touches_per_task", 256, 1, 65'536, false},
};

// A file setting one key of a group; the storage group always lists a device.
std::string
one_key(std::string_view group, std::string_view key, std::string_view value) {
    std::string yaml
      = "kwaque:\n  schema_version: 1\n  data_directory: /var/lib/kwaque\n";
    if (group == "storage") {
        return yaml
               + "  storage:\n    devices: [{path: /var/lib/kwaque, role: "
                 "all}]\n    "
               + std::string{key} + ": " + std::string{value} + "\n";
    }
    return with_device(
      yaml + "  " + std::string{group} + ":\n    " + std::string{key} + ": "
      + std::string{value} + "\n");
}

} // namespace

TEST(
  BootstrapConfigBoundsTest,
  EveryKeyAcceptsItsDefaultAndBoundsAndRefusesPastThem) {
    const auto defaults = parse_with_device(
      "kwaque: {schema_version: 1, data_directory: /var/lib/kwaque}");
    ASSERT_TRUE(defaults) << defaults.error().message;
    for (const auto& entry : frozen_bounds) {
        SCOPED_TRACE(std::string{entry.group} + "." + std::string{entry.key});
        const auto field = "kwaque." + std::string{entry.group} + "."
                           + std::string{entry.key};
        for (const auto value : {entry.value, entry.minimum, entry.maximum}) {
            const auto parsed = parse_bootstrap_config(
              one_key(entry.group, entry.key, std::to_string(value)));
            ASSERT_TRUE(parsed) << value << ": " << parsed.error().message;
        }
        if (entry.minimum > 0) {
            const auto below = parse_bootstrap_config(one_key(
              entry.group, entry.key, std::to_string(entry.minimum - 1U)));
            ASSERT_FALSE(below);
            EXPECT_EQ(below.error().code, config_errc::out_of_range);
            EXPECT_EQ(below.error().field, field);
            EXPECT_NE(
              below.error().message.find(std::to_string(entry.minimum)),
              std::string::npos)
              << below.error().message;
        }
        const auto negative = parse_bootstrap_config(
          one_key(entry.group, entry.key, "-1"));
        ASSERT_FALSE(negative);
        EXPECT_EQ(negative.error().code, config_errc::out_of_range);
        if (entry.maximum != no_maximum) {
            const auto above = parse_bootstrap_config(one_key(
              entry.group, entry.key, std::to_string(entry.maximum + 1U)));
            ASSERT_FALSE(above);
            EXPECT_EQ(above.error().code, config_errc::out_of_range);
            EXPECT_EQ(above.error().field, field);
            EXPECT_NE(
              above.error().message.find(std::to_string(entry.maximum)),
              std::string::npos)
              << above.error().message;
        }
        if (entry.power_of_two) {
            const auto uneven = parse_bootstrap_config(one_key(
              entry.group, entry.key, std::to_string(entry.minimum * 3U)));
            ASSERT_FALSE(uneven);
            EXPECT_EQ(uneven.error().code, config_errc::invalid_value);
            EXPECT_EQ(uneven.error().field, field);
        }
        // A quoted number, a float and a unit string are not integers.
        for (const auto* written : {"\"64\"", "64.0", "64ms", "1d"}) {
            const auto parsed = parse_bootstrap_config(
              one_key(entry.group, entry.key, written));
            ASSERT_FALSE(parsed) << written;
            EXPECT_EQ(parsed.error().code, config_errc::invalid_type)
              << written;
            EXPECT_EQ(parsed.error().field, field);
        }
    }
}

TEST(BootstrapConfigBoundsTest, NullMeansUnboundedAndAbsenceMeansDerived) {
    const auto defaults = parse_with_device(
      "kwaque: {schema_version: 1, data_directory: /var/lib/kwaque}");
    ASSERT_TRUE(defaults);
    EXPECT_EQ(defaults->topic_defaults.retention_ms, 604'800'000U);
    EXPECT_FALSE(defaults->topic_defaults.retention_bytes);
    EXPECT_FALSE(defaults->range.maximum_consume_streams);
    const auto unbounded = parse_bootstrap_config(
      one_key("topic_defaults", "retention_ms", "null"));
    ASSERT_TRUE(unbounded) << unbounded.error().message;
    EXPECT_FALSE(unbounded->topic_defaults.retention_ms);
    EXPECT_NE(
      render_config(*unbounded).find("topic_defaults.retention_ms=null"),
      std::string::npos);
    // Only a key that may be unbounded takes a null.
    const auto refused = parse_bootstrap_config(
      one_key("range", "stop_deadline_ms", "null"));
    ASSERT_FALSE(refused);
    EXPECT_EQ(refused.error().code, config_errc::invalid_type);
}

TEST(BootstrapConfigBoundsTest, ARetryWindowLongerThanRetentionIsRefused) {
    const auto refused = parse_with_device(
      "kwaque: {schema_version: 1, data_directory: /var/lib/kwaque, "
      "topic_defaults: {retention_ms: 60000, retry_window_ms: 120000}}");
    ASSERT_FALSE(refused);
    EXPECT_EQ(refused.error().code, config_errc::invalid_value);
    EXPECT_EQ(refused.error().field, "kwaque.topic_defaults.retry_window_ms");
    const auto equal = parse_with_device(
      "kwaque: {schema_version: 1, data_directory: /var/lib/kwaque, "
      "topic_defaults: {retention_ms: 120000, retry_window_ms: 120000}}");
    ASSERT_TRUE(equal) << equal.error().message;
}

TEST(BootstrapConfigBoundsTest, TheDeviceListKeepsItsRules) {
    const auto devices = [](std::string_view list, bool developer = false) {
        return parse_bootstrap_config(
          std::string{
            "kwaque: {schema_version: 1, data_directory: /var/lib/kwaque, "}
          + (developer ? "developer_mode: true, " : "")
          + "storage: {devices: " + std::string{list} + "}}");
    };
    const auto accepted = devices(
      "[{path: /d0, role: wal_control}, {path: /d1, role: data, "
      "mount_marker: .mounted}]");
    ASSERT_TRUE(accepted) << accepted.error().message;
    ASSERT_EQ(accepted->storage.devices.size(), 2U);
    EXPECT_EQ(
      accepted->storage.devices[0].role,
      kwaque::config::device_role::wal_control);
    EXPECT_EQ(accepted->storage.devices[1].mount_marker, ".mounted");

    std::string nine{"["};
    for (int index = 0; index < 9; ++index) {
        nine += (index == 0 ? "" : ", ") + std::string{"{path: /d"}
                + std::to_string(index)
                + (index == 0 ? ", role: all}" : ", role: data}");
    }
    nine += "]";
    struct refused_case final {
        std::string list;
        bool developer;
        std::string_view field;
    };
    const std::vector<refused_case> refused{
      {"[]", false, "kwaque.storage.devices"},
      {nine, false, "kwaque.storage.devices"},
      // No control device, and two.
      {"[{path: /d0, role: data}]", false, "kwaque.storage.devices"},
      {"[{path: /d0, role: all}, {path: /d1, role: wal_control}]",
       false,
       "kwaque.storage.devices"},
      {"[{path: d0, role: all}]", false, "kwaque.storage.devices[0].path"},
      {"[{path: '~/d0', role: all}]", true, "kwaque.storage.devices[0].path"},
      {"[{path: /d0, role: all}, {path: /d0/, role: data}]",
       false,
       "kwaque.storage.devices[1].path"},
      {"[{path: /d0, role: everything}]",
       false,
       "kwaque.storage.devices[0].role"},
      {"[{path: /d0, role: all, mount_marker: a/b}]",
       false,
       "kwaque.storage.devices[0].mount_marker"},
      {"[{path: /d0, role: all, mount_marker: ..}]",
       false,
       "kwaque.storage.devices[0].mount_marker"},
    };
    for (const auto& value : refused) {
        SCOPED_TRACE(value.list);
        const auto parsed = devices(value.list, value.developer);
        ASSERT_FALSE(parsed);
        EXPECT_EQ(parsed.error().code, config_errc::invalid_device_set);
        EXPECT_EQ(parsed.error().field, value.field);
    }
    // A relative path is a developer's.
    EXPECT_TRUE(devices("[{path: d0, role: all}]", true));
    // The list is required, and so is each device's path and role.
    const auto missing = parse_bootstrap_config(
      "kwaque: {schema_version: 1, data_directory: /var/lib/kwaque}");
    ASSERT_FALSE(missing);
    EXPECT_EQ(missing.error().code, config_errc::missing_key);
    EXPECT_EQ(missing.error().field, "kwaque.storage");
    const auto no_list = devices("{}");
    ASSERT_FALSE(no_list);
    EXPECT_EQ(no_list.error().code, config_errc::invalid_type);
    const auto no_role = devices("[{path: /d0}]");
    ASSERT_FALSE(no_role);
    EXPECT_EQ(no_role.error().code, config_errc::missing_key);
    EXPECT_EQ(no_role.error().field, "kwaque.storage.devices[0].role");
}

TEST(BootstrapConfigBoundsTest, UnknownAndDuplicateKeysAreRefusedAtEveryLevel) {
    for (const auto* yaml :
         {"kwaque: {schema_version: 1, data_directory: /x, storage: {devices: "
          "[{path: /x, role: all}], unknown: 1}}",
          "kwaque: {schema_version: 1, data_directory: /x, storage: {devices: "
          "[{path: /x, role: all, unknown: 1}]}}",
          "kwaque: {schema_version: 1, data_directory: /x, metadata: {unknown: "
          "1}}",
          "kwaque: {schema_version: 1, data_directory: /x, topic_defaults: "
          "{unknown: 1}}",
          "kwaque: {schema_version: 1, data_directory: /x, range: {unknown: "
          "1}}"}) {
        SCOPED_TRACE(yaml);
        const std::string_view text{yaml};
        const auto parsed = text.find("storage") != std::string_view::npos
                              ? parse_bootstrap_config(text)
                              : parse_with_device(text);
        ASSERT_FALSE(parsed);
        EXPECT_EQ(parsed.error().code, config_errc::unknown_key);
        EXPECT_TRUE(parsed.error().field.ends_with(".unknown"))
          << parsed.error().field;
    }
    for (const auto* yaml :
         {"kwaque:\n  schema_version: 1\n  data_directory: /x\n  storage:\n"
          "    devices: [{path: /x, role: all}]\n"
          "    metadata_alignment_bytes: 4096\n    metadata_alignment_bytes: "
          "512\n",
          "kwaque:\n  schema_version: 1\n  data_directory: /x\n  storage:\n"
          "    devices:\n      - path: /x\n        role: all\n        role: "
          "data\n",
          "kwaque:\n  schema_version: 1\n  data_directory: /x\n  metadata:\n"
          "    ring_size: 4\n    ring_size: 8\n",
          "kwaque:\n  schema_version: 1\n  data_directory: /x\n  range:\n"
          "    queue_requests: 4\n    queue_requests: 8\n"}) {
        SCOPED_TRACE(yaml);
        const std::string_view text{yaml};
        const auto parsed = text.find("storage") != std::string_view::npos
                              ? parse_bootstrap_config(text)
                              : parse_with_device(text);
        ASSERT_FALSE(parsed);
        EXPECT_EQ(parsed.error().code, config_errc::duplicate_key);
    }
}

// Retention and the retry window are bounded together, since a window may not
// exceed retention, so they are checked here rather than one key at a time.
TEST(
  BootstrapConfigBoundsTest, RetentionAndTheRetryWindowRefusePastTheirBounds) {
    const auto parse = [](std::string_view keys) {
        return parse_bootstrap_config(with_device(
          "kwaque:\n  schema_version: 1\n  data_directory: /var/lib/kwaque\n"
          "  topic_defaults:\n"
          + std::string{keys}));
    };
    for (const auto* accepted :
         {"    retention_ms: 60000\n    retry_window_ms: 60000\n",
          "    retention_ms: ~\n    retry_window_ms: 9000000000000\n",
          "    retention_bytes: 1\n"}) {
        SCOPED_TRACE(accepted);
        const auto parsed = parse(accepted);
        EXPECT_TRUE(parsed) << parsed.error().message;
    }
    struct refusal final {
        const char* keys;
        config_errc code;
        const char* field;
    };
    for (const auto& [keys, code, field] : std::array{
           refusal{
             "    retention_ms: 0\n",
             config_errc::out_of_range,
             "kwaque.topic_defaults.retention_ms"},
           refusal{
             "    retention_ms: \"86400000\"\n",
             config_errc::invalid_type,
             "kwaque.topic_defaults.retention_ms"},
           refusal{
             "    retention_bytes: 0\n",
             config_errc::out_of_range,
             "kwaque.topic_defaults.retention_bytes"},
           refusal{
             "    retention_bytes: 1.5\n",
             config_errc::invalid_type,
             "kwaque.topic_defaults.retention_bytes"},
           refusal{
             "    retry_window_ms: 59999\n",
             config_errc::out_of_range,
             "kwaque.topic_defaults.retry_window_ms"},
           refusal{
             "    retry_window_ms: 60s\n",
             config_errc::invalid_type,
             "kwaque.topic_defaults.retry_window_ms"},
           refusal{
             "    retention_ms: 60000\n    retry_window_ms: 60001\n",
             config_errc::invalid_value,
             "kwaque.topic_defaults.retry_window_ms"},
           refusal{
             "    retry_depth: 4\n    retry_depth: 8\n",
             config_errc::duplicate_key,
             "kwaque.topic_defaults.retry_depth"},
         }) {
        SCOPED_TRACE(keys);
        const auto parsed = parse(keys);
        ASSERT_FALSE(parsed);
        EXPECT_EQ(parsed.error().code, code);
        EXPECT_EQ(parsed.error().field, field);
    }
    // A null byte bound is refused: only the time bound may be unbounded.
    EXPECT_FALSE(parse("    retention_bytes: ~\n"));
}

TEST(BootstrapConfigBoundsTest, ANewerVersionIsRefusedBeforeItsKeys) {
    const auto parsed = parse_bootstrap_config(
      "kwaque: {schema_version: 2, storage: {not_a_key: 1}}");
    ASSERT_FALSE(parsed);
    EXPECT_EQ(parsed.error().code, config_errc::unsupported_schema_version);
}

TEST(BootstrapConfigBoundsTest, TheShippedFilesListADevice) {
    const auto development = load_bootstrap_config(example_config_path());
    ASSERT_TRUE(development) << development.error().message;
    ASSERT_EQ(development->storage.devices.size(), 1U);
    EXPECT_EQ(
      development->storage.devices[0].path, development->data_directory);
    const auto production = load_bootstrap_config(production_example_path());
    ASSERT_TRUE(production) << production.error().message;
    ASSERT_EQ(production->storage.devices.size(), 1U);
    EXPECT_EQ(production->storage.devices[0].path, production->data_directory);
    EXPECT_EQ(
      production->storage.devices[0].role, kwaque::config::device_role::all);
}

TEST(BootstrapConfigBoundsTest, TheFuzzDictionaryAndCorpusCoverEveryKey) {
    const char* srcdir = std::getenv("TEST_SRCDIR");
    const char* workspace = std::getenv("TEST_WORKSPACE");
    ASSERT_NE(srcdir, nullptr);
    ASSERT_NE(workspace, nullptr);
    const auto root = std::filesystem::path{srcdir} / workspace / "src"
                      / "config";
    const auto read = [](const std::filesystem::path& path) {
        std::ifstream input{path};
        return std::string{
          std::istreambuf_iterator<char>{input},
          std::istreambuf_iterator<char>{}};
    };
    const auto dictionary = read(root / "bootstrap_config_fuzz.dict");
    const auto complete = read(
      root / "testdata" / "bootstrap_config_fuzz_corpus" / "complete.yaml");
    ASSERT_FALSE(dictionary.empty());
    ASSERT_FALSE(complete.empty());
    const auto covered = [&](auto keys) {
        for (const auto key : keys) {
            EXPECT_NE(
              dictionary.find("\"" + std::string{key} + "\""),
              std::string::npos)
              << key;
            EXPECT_NE(complete.find(std::string{key} + ":"), std::string::npos)
              << key;
        }
    };
    covered(kwaque::config::bootstrap_config_keys);
    covered(kwaque::config::bootstrap_admin_keys);
    covered(kwaque::config::bootstrap_storage_keys);
    covered(kwaque::config::bootstrap_device_keys);
    covered(kwaque::config::bootstrap_metadata_keys);
    covered(kwaque::config::bootstrap_topic_default_keys);
    covered(kwaque::config::bootstrap_range_keys);
    for (const auto* word : {"data", "wal_control", "all"}) {
        EXPECT_NE(
          dictionary.find("\"" + std::string{word} + "\""), std::string::npos)
          << word;
    }
    // The complete file parses.
    EXPECT_TRUE(parse_bootstrap_config(complete));
}
