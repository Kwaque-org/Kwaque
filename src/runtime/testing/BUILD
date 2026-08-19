load("//bazel:build.bzl", "kwaque_cc_library")

package(default_visibility = ["//visibility:private"])

kwaque_cc_library(
    name = "seastar_gtest_main",
    testonly = True,
    srcs = ["seastar_gtest_main.cc"],
    visibility = ["//visibility:public"],
    deps = [
        "@googletest//:gtest",
        "@seastar",
    ],
)
