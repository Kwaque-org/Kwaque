#include "proto/kwaque/common/v1/build_info_codec.h"

#include <gtest/gtest.h>

#include <cstdlib>
#include <fstream>
#include <iterator>
#include <optional>
#include <string>
#include <string_view>

namespace {

using kwaque::common::v1::BuildInfo;
using kwaque::common::v1::max_build_info_payload_size;
using kwaque::common::v1::parse_build_info;

BuildInfo expected_build_info() {
    BuildInfo info;
    info.set_version("0.1.0");
    info.set_revision("0123456789abcdef");
    info.set_build_mode("release");
    return info;
}

std::optional<unsigned char> decode_nibble(char value) {
    if (value >= '0' && value <= '9') {
        return static_cast<unsigned char>(value - '0');
    }
    if (value >= 'a' && value <= 'f') {
        return static_cast<unsigned char>(value - 'a' + 10);
    }
    if (value >= 'A' && value <= 'F') {
        return static_cast<unsigned char>(value - 'A' + 10);
    }
    return std::nullopt;
}

std::optional<std::string> decode_hex(std::string_view encoded) {
    std::string decoded;
    decoded.reserve(encoded.size() / 2);
    std::optional<unsigned char> high_nibble;

    for (const char value : encoded) {
        if (value == ' ' || value == '\n' || value == '\r' || value == '\t') {
            continue;
        }

        const auto nibble = decode_nibble(value);
        if (!nibble) {
            return std::nullopt;
        }
        if (!high_nibble) {
            high_nibble = nibble;
            continue;
        }

        decoded.push_back(static_cast<char>((*high_nibble << 4U) | *nibble));
        high_nibble.reset();
    }

    if (high_nibble) {
        return std::nullopt;
    }
    return decoded;
}

std::optional<std::string> read_golden_fixture() {
    const char* test_srcdir = std::getenv("TEST_SRCDIR");
    const char* test_workspace = std::getenv("TEST_WORKSPACE");
    if (test_srcdir == nullptr || test_workspace == nullptr) {
        return std::nullopt;
    }

    const std::string path
      = std::string(test_srcdir) + "/" + test_workspace
        + "/proto/kwaque/common/v1/testdata/build_info_v1.hex";
    std::ifstream fixture(path, std::ios::binary);
    if (!fixture) {
        return std::nullopt;
    }
    return std::string(
      std::istreambuf_iterator<char>(fixture),
      std::istreambuf_iterator<char>());
}

TEST(BuildInfoTest, RoundTripsEveryField) {
    const BuildInfo expected = expected_build_info();
    const std::string serialized = expected.SerializeAsString();

    ASSERT_LE(serialized.size(), max_build_info_payload_size);
    const auto actual = parse_build_info(serialized);
    ASSERT_TRUE(actual);
    EXPECT_EQ(actual->version(), expected.version());
    EXPECT_EQ(actual->revision(), expected.revision());
    EXPECT_EQ(actual->build_mode(), expected.build_mode());
}

TEST(BuildInfoTest, DecodesStableWireFixture) {
    const auto encoded = read_golden_fixture();
    ASSERT_TRUE(encoded.has_value())
      << "unable to read the build-info golden fixture from Bazel runfiles";
    const auto serialized = decode_hex(*encoded);
    ASSERT_TRUE(serialized.has_value())
      << "the build-info golden fixture contains invalid hex";

    const BuildInfo expected = expected_build_info();
    EXPECT_EQ(*serialized, expected.SerializeAsString());

    const auto actual = parse_build_info(*serialized);
    ASSERT_TRUE(actual);
    EXPECT_EQ(actual->version(), expected.version());
    EXPECT_EQ(actual->revision(), expected.revision());
    EXPECT_EQ(actual->build_mode(), expected.build_mode());
}

TEST(BuildInfoTest, RejectsTruncatedPayload) {
    std::string truncated = expected_build_info().SerializeAsString();
    ASSERT_FALSE(truncated.empty());
    truncated.pop_back();
    EXPECT_FALSE(parse_build_info(truncated));
}

TEST(BuildInfoTest, AcceptsValidPayloadAtMaximumSize) {
    // The field tag and two-byte length prefix occupy three bytes at this size.
    constexpr std::size_t string_field_overhead = 3;
    static_assert(max_build_info_payload_size > string_field_overhead);

    BuildInfo at_limit;
    at_limit.set_version(
      std::string(max_build_info_payload_size - string_field_overhead, 'x'));
    const std::string serialized = at_limit.SerializeAsString();

    ASSERT_EQ(serialized.size(), max_build_info_payload_size);
    const auto actual = parse_build_info(serialized);
    ASSERT_TRUE(actual.has_value());
    EXPECT_EQ(actual->version(), at_limit.version());
}

TEST(BuildInfoTest, RejectsValidPayloadAboveMaximumSize) {
    BuildInfo oversized;
    oversized.set_version(std::string(max_build_info_payload_size, 'x'));
    const std::string serialized = oversized.SerializeAsString();

    ASSERT_GT(serialized.size(), max_build_info_payload_size);
    BuildInfo validity_check;
    ASSERT_TRUE(validity_check.ParseFromString(serialized));
    EXPECT_FALSE(parse_build_info(serialized));
}

} // namespace
