load("//bazel:build.bzl", "kwaque_cc_library")
load("//bazel:test.bzl", "kwaque_cc_seastar_gtest", "kwaque_cc_test")

package(default_visibility = ["//visibility:private"])

filegroup(
    name = "package_boundary",
    srcs = ["README.md"],
)

kwaque_cc_library(
    name = "admin_state",
    srcs = ["admin_state.cc"],
    hdrs = ["admin_state.h"],
    deps = [
        "//src/runtime:shard_affinity",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "admin_responses",
    srcs = ["admin_responses.cc"],
    hdrs = ["admin_responses.h"],
    deps = [
        "//proto/kwaque/common/v1:build_info_cc_proto",
        "//src/base:build_info",
    ],
)

kwaque_cc_library(
    name = "admin_limits",
    hdrs = ["admin_limits.h"],
    visibility = ["//src/broker:__pkg__"],
)

kwaque_cc_library(
    name = "admin_server",
    srcs = ["admin_server.cc"],
    hdrs = ["admin_server.h"],
    visibility = ["//src/broker:__pkg__"],
    deps = [
        ":admin_limits",
        ":admin_responses",
        ":admin_state",
        "//src/base:invariant",
        "//src/runtime:shard_affinity",
        "@seastar",
    ],
)

kwaque_cc_seastar_gtest(
    name = "admin_memory_test",
    timeout = "moderate",
    srcs = ["admin_memory_test.cc"],
    cpu = 2,
    memory = "256MiB",
    deps = [
        ":admin_limits",
        ":admin_server",
    ],
)

kwaque_cc_library(
    name = "admin_server_test_support",
    testonly = True,
    hdrs = ["admin_server_test_support.h"],
    deps = [
        ":admin_server",
        "@seastar",
    ],
)

kwaque_cc_seastar_gtest(
    name = "admin_resource_test",
    timeout = "moderate",
    srcs = ["admin_resource_test.cc"],
    cpu = 2,
    memory = "256MiB",
    deps = [
        ":admin_limits",
        ":admin_server",
        ":admin_server_test_support",
    ],
)

kwaque_cc_seastar_gtest(
    name = "admin_state_test",
    srcs = ["admin_state_test.cc"],
    cpu = 1,
    memory = "128MiB",
    deps = [":admin_state"],
)

kwaque_cc_test(
    name = "admin_responses_test",
    srcs = ["admin_responses_test.cc"],
    deps = [
        ":admin_responses",
        "//proto/kwaque/common/v1:build_info_cc_proto",
        "//src/base:build_info",
    ],
)
