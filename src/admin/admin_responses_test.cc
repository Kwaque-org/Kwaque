#include "src/admin/admin_responses.h"
#include "src/base/build_info.h"

#include <gtest/gtest.h>

#include <string>

namespace {

TEST(AdminResponsesTest, BuildInfoJsonHasStableShape) {
    kwaque::admin::build_identity build{
      .version = "1.2.3",
      .revision = "abc123",
      .dirty = true,
      .build_timestamp = "1700000000",
      .build_mode = "opt",
    };
    EXPECT_EQ(
      kwaque::admin::build_info_json(build),
      R"({"version":"1.2.3","revision":"abc123","dirty":true,"build_timestamp":"1700000000","build_mode":"opt"})");

    build.dirty = false;
    build.build_timestamp = "0";
    EXPECT_EQ(
      kwaque::admin::build_info_json(build),
      R"({"version":"1.2.3","revision":"abc123","dirty":false,"build_timestamp":"0","build_mode":"opt"})");
}

TEST(AdminResponsesTest, CurrentVersionJsonReportsTheRunningBuild) {
    EXPECT_EQ(
      kwaque::admin::current_version_json(),
      kwaque::admin::build_info_json({
        .version = kwaque::build_info::version(),
        .revision = kwaque::build_info::git_revision(),
        .dirty = kwaque::build_info::git_dirty(),
        .build_timestamp = kwaque::build_info::build_timestamp(),
        .build_mode = kwaque::build_info::build_mode(),
      }));
}

TEST(AdminResponsesTest, ProblemJsonMatchesGoldenResponse) {
    EXPECT_EQ(
      kwaque::admin::problem_json(
        503, "broker_not_ready", "broker is not ready"),
      R"({"type":"about:blank","title":"Service Unavailable","status":503,"detail":"broker is not ready","code":"broker_not_ready"})");
    EXPECT_EQ(
      kwaque::admin::problem_json(404, "bad\"code", "line\nbreak"),
      R"({"type":"about:blank","title":"Not Found","status":404,"detail":"line\nbreak","code":"bad\"code"})");
}

TEST(AdminResponsesTest, ProblemJsonBoundsAndEscapesNonAsciiInput) {
    std::string detail(kwaque::admin::max_json_detail_bytes + 20, 'm');
    detail[0] = static_cast<char>(0x80);
    const std::string response = kwaque::admin::problem_json(
      503, std::string(200, 'c'), detail);

    EXPECT_NE(response.find("\\u0080"), std::string::npos);
    EXPECT_NE(response.find("<truncated>"), std::string::npos);
    EXPECT_EQ(response.find(static_cast<char>(0x80)), std::string::npos);
}

TEST(AdminResponsesTest, HealthResponsesReflectLifecycleState) {
    const auto starting = kwaque::admin::readiness_response(false);
    EXPECT_EQ(starting.status, 503);
    EXPECT_EQ(starting.content_type, "application/problem+json");
    EXPECT_EQ(
      starting.body,
      R"({"type":"about:blank","title":"Service Unavailable","status":503,"detail":"broker is not ready","code":"broker_not_ready"})");

    const auto ready = kwaque::admin::readiness_response(true);
    EXPECT_EQ(ready.status, 200);
    EXPECT_EQ(ready.content_type, "application/json");
    EXPECT_EQ(ready.body, R"({"status":"ready"})");

    const auto stopped = kwaque::admin::liveness_response(false);
    EXPECT_EQ(stopped.status, 503);
    EXPECT_EQ(stopped.content_type, "application/problem+json");
    EXPECT_EQ(
      stopped.body,
      R"({"type":"about:blank","title":"Service Unavailable","status":503,"detail":"broker is not live","code":"broker_not_live"})");
}

TEST(AdminResponsesTest, RoutingProblemsUseTheirHttpStatusTitles) {
    const auto missing = kwaque::admin::not_found_response();
    EXPECT_EQ(missing.status, 404);
    EXPECT_EQ(
      missing.body,
      R"({"type":"about:blank","title":"Not Found","status":404,"detail":"no such resource","code":"not_found"})");

    const auto method = kwaque::admin::method_not_allowed_response();
    EXPECT_EQ(method.status, 405);
    EXPECT_EQ(
      method.body,
      R"({"type":"about:blank","title":"Method Not Allowed","status":405,"detail":"the resource supports only GET and HEAD","code":"method_not_allowed"})");
}

} // namespace
