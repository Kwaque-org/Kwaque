load("//bazel:build.bzl", "kwaque_cc_library")

package(default_visibility = ["//visibility:private"])

kwaque_cc_library(
    name = "limits",
    srcs = ["limits.cc"],
    hdrs = ["limits.h"],
    visibility = [
        "//src/codec/tests:__pkg__",
        "//src/compression:__pkg__",
        "//src/compression/tests:__pkg__",
        "//src/model:__pkg__",
        "//src/model/tests:__pkg__",
        "//src/protocol:__pkg__",
        "//src/protocol/tests:__pkg__",
        "//src/storage:__pkg__",
        "//src/storage/tests:__pkg__",
    ],
    deps = [
        "//src/base",
        "//src/base:error",
    ],
)

filegroup(
    name = "package_boundary",
    srcs = ["README.md"],
)
