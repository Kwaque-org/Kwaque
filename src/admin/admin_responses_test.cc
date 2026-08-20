#include "src/admin/admin_responses.h"
#include "src/base/build_info.h"

#include <gtest/gtest.h>

namespace {

TEST(AdminResponsesTest, CurrentBuildInfoUsesStampedValues) {
    const auto info = kwaque::admin::current_build_info();

    EXPECT_EQ(info.version(), kwaque::build_info::version());
    EXPECT_EQ(info.revision(), kwaque::build_info::git_revision());
    EXPECT_EQ(info.build_mode(), kwaque::build_info::build_mode());
}

TEST(AdminResponsesTest, BuildInfoJsonHasStableShape) {
    kwaque::common::v1::BuildInfo info;
    info.set_version("1.2.3");
    info.set_revision("abc123");
    info.set_build_mode("release");

    EXPECT_EQ(
      kwaque::admin::build_info_json(info),
      R"({"version":"1.2.3","revision":"abc123","build_mode":"release"})");
}

TEST(AdminResponsesTest, ErrorJsonMatchesGoldenResponse) {
    EXPECT_EQ(
      kwaque::admin::error_json("broker_not_ready", "broker is not ready"),
      R"({"code":"broker_not_ready","message":"broker is not ready","correlation_id":null})");
    EXPECT_EQ(
      kwaque::admin::error_json("bad\"code", "line\nbreak", "req-7"),
      R"({"code":"bad\"code","message":"line\nbreak","correlation_id":"req-7"})");
}

TEST(AdminResponsesTest, HealthResponsesReflectLifecycleState) {
    const auto starting = kwaque::admin::readiness_response(false);
    EXPECT_EQ(starting.status, 503);
    EXPECT_EQ(
      starting.body,
      R"({"code":"broker_not_ready","message":"broker is not ready","correlation_id":null})");

    const auto ready = kwaque::admin::readiness_response(true);
    EXPECT_EQ(ready.status, 200);
    EXPECT_EQ(ready.body, R"({"status":"ready"})");

    const auto draining = kwaque::admin::liveness_response(false);
    EXPECT_EQ(draining.status, 503);
    EXPECT_EQ(
      draining.body,
      R"({"code":"broker_not_live","message":"broker shutdown is in progress","correlation_id":null})");
}

} // namespace
