load("//bazel:build.bzl", "kwaque_cc_library")

package(default_visibility = ["//visibility:private"])

kwaque_cc_library(
    name = "clocks",
    hdrs = ["clocks.h"],
    visibility = ["//src/runtime/tests:__pkg__"],
    deps = [
        "//src/runtime:time",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "timer",
    srcs = ["timer.cc"],
    hdrs = ["timer.h"],
    visibility = ["//src/runtime/tests:__pkg__"],
    deps = [
        ":clocks",
        "//src/base:invariant",
        "//src/runtime:error",
        "//src/runtime:shard_affinity",
        "//src/runtime:time",
        "//src/runtime:timer",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "random",
    srcs = ["random.cc"],
    hdrs = ["random.h"],
    visibility = ["//src/runtime/tests:__pkg__"],
    deps = [
        "//src/runtime:error",
        "//src/runtime:random",
    ],
)

kwaque_cc_library(
    name = "file",
    srcs = ["file.cc"],
    hdrs = ["file.h"],
    visibility = ["//src/runtime/tests:__pkg__"],
    deps = [
        "//src/runtime:error",
        "//src/runtime:file",
        "//src/runtime:shard_affinity",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "network",
    srcs = ["network.cc"],
    hdrs = [
        "network.h",
        "network_connect_internal.h",
    ],
    visibility = ["//src/runtime/tests:__pkg__"],
    deps = [
        "//src/base:invariant",
        "//src/runtime:error",
        "//src/runtime:fragmented_buffer_internal",
        "//src/runtime:network",
        "//src/runtime:shard_affinity",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "dns",
    srcs = ["dns.cc"],
    hdrs = ["dns.h"],
    visibility = ["//src/runtime/tests:__pkg__"],
    deps = [
        "//src/base:invariant",
        "//src/runtime:dns",
        "//src/runtime:error",
        "//src/runtime:network",
        "//src/runtime:shard_affinity",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "backend",
    srcs = ["backend.cc"],
    hdrs = ["backend.h"],
    visibility = [
        "//src/broker:__pkg__",
        "//src/runtime/tests:__pkg__",
    ],
    deps = [
        ":clocks",
        ":dns",
        ":file",
        ":network",
        ":random",
        ":timer",
        "//src/base:invariant",
        "//src/runtime:environment",
        "//src/runtime:error",
        "//src/runtime:runtime_service",
        "//src/runtime:shard_affinity",
        "//src/runtime:sharded_service",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "backend_test_support",
    testonly = True,
    hdrs = ["backend_test_support.h"],
    visibility = ["//src/runtime/tests:__pkg__"],
    deps = [
        ":backend",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "network_test_support",
    testonly = True,
    hdrs = ["network_test_support.h"],
    visibility = ["//src/runtime/tests:__pkg__"],
    deps = [
        ":network",
        "@seastar",
    ],
)
