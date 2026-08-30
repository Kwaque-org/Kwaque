load("//bazel:build.bzl", "kwaque_cc_library")
load("//bazel:stamp_template.bzl", "stamp_template")
load("//bazel:test.bzl", "kwaque_cc_test")
load("//bazel:versions.bzl", "KWAQUE_VERSION", "PROTOBUF_VERSION", "SEASTAR_REVISION")

package(default_visibility = ["//visibility:private"])

kwaque_cc_library(
    name = "error",
    srcs = ["error.cc"],
    hdrs = ["error.h"],
    visibility = ["//visibility:public"],
)

kwaque_cc_library(
    name = "invariant",
    srcs = ["invariant.cc"],
    hdrs = ["invariant.h"],
    defines = select({
        "//bazel:optimized_build": ["KWAQUE_ENABLE_DEBUG_ASSERTIONS=0"],
        "//conditions:default": ["KWAQUE_ENABLE_DEBUG_ASSERTIONS=1"],
    }),
    local_defines = ["KWAQUE_INVARIANT_TEST_OBSERVER=0"],
    visibility = ["//visibility:public"],
)

kwaque_cc_library(
    name = "invariant_test_support",
    testonly = True,
    srcs = ["invariant.cc"],
    hdrs = [
        "invariant.h",
        "invariant_test_observer.h",
    ],
    defines = select({
        "//bazel:optimized_build": ["KWAQUE_ENABLE_DEBUG_ASSERTIONS=0"],
        "//conditions:default": ["KWAQUE_ENABLE_DEBUG_ASSERTIONS=1"],
    }),
    local_defines = ["KWAQUE_INVARIANT_TEST_OBSERVER=1"],
    visibility = [
        "//src/resource:__pkg__",
        "//src/resource/tests:__pkg__",
        "//src/runtime:__pkg__",
        "//src/runtime/tests:__pkg__",
        "//src/simulation:__pkg__",
        "//src/simulation/tests:__pkg__",
    ],
)

kwaque_cc_library(
    name = "base",
    hdrs = [
        "allocation.h",
        "compiler.h",
        "result.h",
        "units.h",
    ],
    visibility = ["//visibility:public"],
    deps = [":error"],
)

stamp_template(
    name = "build_stamp",
    out = "build_stamp.h",
    defaults = "build_info.defaults",
    template = "build_stamp.h.in",
)

kwaque_cc_library(
    name = "build_info",
    srcs = [
        "build_info.cc",
        ":build_stamp",
    ],
    hdrs = ["build_info.h"],
    local_defines = [
        "KWAQUE_PROTOBUF_VERSION=\\\"{}\\\"".format(PROTOBUF_VERSION),
        "KWAQUE_SEASTAR_VERSION=\\\"{}\\\"".format(SEASTAR_REVISION),
        "KWAQUE_VERSION=\\\"{}\\\"".format(KWAQUE_VERSION),
    ] + select({
        "//bazel:debug_build": ["KWAQUE_BUILD_MODE=\\\"debug\\\""],
        "//bazel:optimized_build": ["KWAQUE_BUILD_MODE=\\\"release\\\""],
        "//conditions:default": ["KWAQUE_BUILD_MODE=\\\"development\\\""],
    }),
    visibility = ["//visibility:public"],
)

kwaque_cc_library(
    name = "logging",
    srcs = ["logging.cc"],
    hdrs = ["logging.h"],
    visibility = ["//visibility:public"],
    deps = ["@seastar"],
)

kwaque_cc_test(
    name = "base_test",
    srcs = ["base_test.cc"],
    deps = [
        ":base",
        ":error",
    ],
)

kwaque_cc_test(
    name = "error_test",
    srcs = ["error_test.cc"],
    deps = [":error"],
)

kwaque_cc_test(
    name = "invariant_test",
    srcs = ["invariant_test.cc"],
    deps = [":invariant"],
)

kwaque_cc_test(
    name = "invariant_observer_test",
    srcs = ["invariant_observer_test.cc"],
    deps = [":invariant_test_support"],
)

kwaque_cc_test(
    name = "build_info_test",
    srcs = ["build_info_test.cc"],
    deps = [":build_info"],
)

kwaque_cc_library(
    name = "visibility_test_private_library",
    testonly = True,
    tags = ["manual"],
)
