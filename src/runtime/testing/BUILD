load("@rules_python//python:defs.bzl", "py_test")
load("//bazel:build.bzl", "kwaque_cc_binary", "kwaque_cc_library")
load("//bazel:test.bzl", "kwaque_cc_seastar_gtest")

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

kwaque_cc_seastar_gtest(
    name = "seastar_gtest_main_test",
    srcs = ["seastar_gtest_main_test.cc"],
)

kwaque_cc_library(
    name = "seastar_fuzz",
    testonly = True,
    srcs = ["seastar_fuzz.cc"],
    hdrs = ["seastar_fuzz.h"],
    visibility = [
        "//src/codec/tests:__pkg__",
        "//src/simulation/tests:__pkg__",
    ],
    deps = [
        "@seastar",
        "@seastar//:testing",
    ],
)

kwaque_cc_library(
    name = "test_directory",
    testonly = True,
    hdrs = ["test_directory.h"],
    visibility = ["//src/runtime/tests:__pkg__"],
)

kwaque_cc_binary(
    name = "seastar_fuzz_canary",
    testonly = True,
    srcs = ["seastar_fuzz_canary.cc"],
    deps = [
        ":seastar_fuzz",
        "@seastar",
        "@seastar//:testing",
    ],
)

py_test(
    name = "seastar_fuzz_test",
    size = "small",
    timeout = "moderate",
    srcs = ["seastar_fuzz_test.py"],
    args = ["$(rootpath :seastar_fuzz_canary)"],
    data = [":seastar_fuzz_canary"],
    tags = [
        "resources:cpu:1",
        "resources:memory:256",
    ],
)
