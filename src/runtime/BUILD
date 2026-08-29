load("//bazel:build.bzl", "kwaque_cc_binary", "kwaque_cc_library")

package(default_visibility = ["//visibility:private"])

kwaque_cc_library(
    name = "error",
    srcs = ["error.cc"],
    hdrs = ["error.h"],
    visibility = ["//visibility:public"],
    deps = ["//src/base:error"],
)

kwaque_cc_library(
    name = "time",
    hdrs = ["time.h"],
    visibility = ["//visibility:public"],
)

kwaque_cc_library(
    name = "timer",
    hdrs = ["timer.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":error",
        ":time",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "random",
    hdrs = ["random.h"],
    visibility = ["//visibility:public"],
    deps = [":error"],
)

kwaque_cc_library(
    name = "file",
    srcs = ["file.cc"],
    hdrs = ["file.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":error",
        ":shard_affinity",
        "//src/base",
        "//src/base:invariant",
        "//src/bytes:fragmented_buffer",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "fragmented_buffer_internal",
    hdrs = ["fragmented_buffer_internal.h"],
    visibility = ["//src/runtime/tests:__pkg__"],
    deps = [
        "//src/base",
        "//src/bytes:fragmented_buffer",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "network",
    srcs = ["network.cc"],
    hdrs = ["network.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":error",
        ":shard_affinity",
        "//src/base",
        "//src/base:invariant",
        "//src/bytes:fragmented_buffer",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "dns",
    srcs = ["dns.cc"],
    hdrs = ["dns.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":error",
        ":network",
        ":shard_affinity",
        ":time",
        "//src/base:invariant",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "fault",
    srcs = ["fault.cc"],
    hdrs = ["fault.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":error",
        ":time",
        "//src/base",
    ],
)

kwaque_cc_library(
    name = "environment",
    srcs = ["environment.cc"],
    hdrs = ["environment.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":dns",
        ":error",
        ":fault",
        ":file",
        ":network",
        ":random",
        ":shard_affinity",
        ":time",
        ":timer",
        "//src/base:invariant",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "shard_affinity",
    srcs = ["shard_affinity.cc"],
    hdrs = ["shard_affinity.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/base:invariant",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "shard_affinity_test_support",
    testonly = True,
    srcs = ["shard_affinity.cc"],
    hdrs = ["shard_affinity.h"],
    visibility = [
        "//src/resource:__pkg__",
        "//src/runtime/tests:__pkg__",
    ],
    deps = [
        "//src/base:invariant_test_support",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "cross_shard",
    srcs = ["cross_shard.cc"],
    hdrs = ["cross_shard.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":error",
        ":shard_affinity",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "task_scope",
    srcs = ["task_scope.cc"],
    hdrs = ["task_scope.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":error",
        ":shard_affinity",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "sharded_service",
    hdrs = ["sharded_service.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":cross_shard",
        ":shard_affinity",
        "//src/base:invariant",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "runtime_service",
    srcs = ["runtime_service.cc"],
    hdrs = ["runtime_service.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":shard_affinity",
        ":task_scope",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "stop_signal",
    srcs = ["stop_signal.cc"],
    hdrs = ["stop_signal.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":shard_affinity",
        "//src/base:invariant",
        "@seastar",
    ],
)

kwaque_cc_binary(
    name = "reactor_probe",
    srcs = ["reactor_probe.cc"],
    visibility = ["//visibility:public"],
    deps = ["@seastar"],
)
