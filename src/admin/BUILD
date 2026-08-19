load("//bazel:build.bzl", "kwaque_cc_library")
load("//bazel:test.bzl", "kwaque_cc_test")

package(default_visibility = ["//visibility:private"])

filegroup(name = "package_boundary", srcs = ["README.md"])

kwaque_cc_library(
    name = "admin_state",
    srcs = ["admin_state.cc"],
    hdrs = ["admin_state.h"],
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
    name = "admin_server",
    srcs = ["admin_server.cc"],
    hdrs = ["admin_server.h"],
    visibility = ["//src/broker:__pkg__"],
    deps = [
        ":admin_responses",
        ":admin_state",
        "@seastar",
    ],
)

kwaque_cc_test(
    name = "admin_state_test",
    srcs = ["admin_state_test.cc"],
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
