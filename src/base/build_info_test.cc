#include "src/base/build_info.h"

#include <gtest/gtest.h>

#include <array>
#include <regex>
#include <string>
#include <string_view>

namespace {

bool matches(std::string_view value, const char* pattern) {
    return std::regex_match(
      std::string{value}, std::regex{pattern, std::regex::ECMAScript});
}

TEST(BuildInfoTest, ExposesCompleteBuildIdentity) {
    EXPECT_FALSE(kwaque::build_info::version().empty());
    EXPECT_FALSE(kwaque::build_info::compiler().empty());
    EXPECT_FALSE(kwaque::build_info::protobuf_version().empty());
    EXPECT_FALSE(kwaque::build_info::seastar_version().empty());
}

TEST(BuildInfoTest, StampedIdentityIsRealAndUnstampedIdentityIsExplicit) {
    if (kwaque::build_info::stamped()) {
        // A renamed status key or a missing repository fails stamping rather
        // than falling back to these defaults.
        EXPECT_TRUE(
          matches(kwaque::build_info::git_revision(), "^[0-9a-f]{40}$"))
          << kwaque::build_info::git_revision();
        EXPECT_TRUE(matches(kwaque::build_info::build_timestamp(), "^[0-9]+$"))
          << kwaque::build_info::build_timestamp();
    } else {
        EXPECT_EQ(kwaque::build_info::git_revision(), "unknown");
        EXPECT_FALSE(kwaque::build_info::git_dirty());
        EXPECT_EQ(kwaque::build_info::build_timestamp(), "0");
    }
}

TEST(BuildInfoTest, VersionIsSemanticVersion) {
    // The version also names package files and fills tab-separated output,
    // so it must stay within the semantic-versioning grammar.
    EXPECT_TRUE(matches(
      kwaque::build_info::version(),
      R"(^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*))"
      R"((?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))"
      R"((?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?)"
      R"((?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$)"))
      << kwaque::build_info::version();
}

TEST(BuildInfoTest, BuildModeNamesTheCompilationMode) {
    const auto mode = kwaque::build_info::build_mode();
    EXPECT_TRUE(mode == "dbg" || mode == "opt" || mode == "fastbuild") << mode;
}

TEST(BuildInfoTest, EmitsOneMachineReadableLine) {
    const std::string line = kwaque::build_info::version_line();
    EXPECT_EQ(line.find('\n'), std::string::npos);
    EXPECT_EQ(line.find('\r'), std::string::npos);

    constexpr std::array<std::string_view, 8> fields = {
      "version=",
      "revision=",
      "dirty=",
      "build_timestamp=",
      "build_mode=",
      "compiler=",
      "protobuf=",
      "seastar=",
    };
    std::size_t start = 0;
    for (const std::string_view field : fields) {
        const auto end = line.find('\t', start);
        const std::string_view value = std::string_view{line}.substr(
          start, end == std::string::npos ? std::string::npos : end - start);
        EXPECT_TRUE(value.starts_with(field)) << value;
        start = end == std::string::npos ? line.size() : end + 1;
    }
    EXPECT_EQ(start, line.size());
}

} // namespace
