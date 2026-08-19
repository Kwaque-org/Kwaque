load("//bazel:build.bzl", "kwaque_cc_library")
load("//bazel:test.bzl", "kwaque_cc_test")

package(default_visibility = ["//visibility:private"])

kwaque_cc_library(
    name = "bootstrap_config",
    srcs = ["bootstrap_config.cc"],
    hdrs = ["bootstrap_config.h"],
    visibility = ["//visibility:public"],
    deps = ["@yaml-cpp"],
)

kwaque_cc_test(
    name = "bootstrap_config_test",
    srcs = ["bootstrap_config_test.cc"],
    data = ["//conf:kwaque_config"],
    deps = [":bootstrap_config"],
)
