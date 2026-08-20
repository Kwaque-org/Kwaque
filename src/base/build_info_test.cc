#include "src/base/build_info.h"

#include <gtest/gtest.h>

#include <array>
#include <string>
#include <string_view>

namespace {

TEST(BuildInfoTest, ExposesCompleteBuildIdentity) {
    EXPECT_FALSE(kwaque::build_info::version().empty());
    EXPECT_FALSE(kwaque::build_info::git_revision().empty());
    EXPECT_FALSE(kwaque::build_info::build_timestamp().empty());
    EXPECT_FALSE(kwaque::build_info::build_mode().empty());
    EXPECT_FALSE(kwaque::build_info::compiler().empty());
    EXPECT_FALSE(kwaque::build_info::protobuf_version().empty());
    EXPECT_FALSE(kwaque::build_info::seastar_version().empty());
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
    for (const std::string_view field : fields) {
        EXPECT_NE(line.find(field), std::string::npos) << field;
    }
}

} // namespace
