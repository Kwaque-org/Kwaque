load("//bazel:build.bzl", "kwaque_cc_library")

package(default_visibility = ["//visibility:private"])

filegroup(
    name = "package_boundary",
    srcs = ["README.md"],
)

kwaque_cc_library(
    name = "workload_class",
    srcs = ["workload_class.cc"],
    hdrs = ["workload_class.h"],
    visibility = ["//visibility:public"],
)

kwaque_cc_library(
    name = "resource_config",
    srcs = ["resource_config.cc"],
    hdrs = ["resource_config.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":workload_class",
        "//src/base",
        "//src/base:error",
    ],
)

kwaque_cc_library(
    name = "resource_registry",
    srcs = ["resource_registry.cc"],
    hdrs = ["resource_registry.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":resource_config",
        ":workload_class",
        "//src/base:invariant",
        "//src/runtime:shard_affinity",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "resource_manager",
    srcs = ["resource_manager.cc"],
    hdrs = ["resource_manager.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":resource_registry",
        ":workload_class",
        "//src/base",
        "//src/base:error",
        "//src/base:invariant",
        "//src/runtime:error",
        "//src/runtime:shard_affinity",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "memory_budget",
    srcs = ["memory_budget.cc"],
    hdrs = ["memory_budget.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/base:invariant",
        "//src/runtime:error",
        "//src/runtime:shard_affinity",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "reclaimer_registry",
    srcs = ["reclaimer_registry.cc"],
    hdrs = ["reclaimer_registry.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/base:invariant",
        "//src/runtime:shard_affinity",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "bounded_work_queue",
    hdrs = ["bounded_work_queue.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":memory_budget",
        ":resource_manager",
        ":workload_class",
        "//src/base",
        "//src/base:error",
        "//src/base:invariant",
        "//src/runtime:error",
        "//src/runtime:shard_affinity",
        "//src/runtime:task_scope",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "resource_test_support",
    testonly = True,
    hdrs = ["resource_test_support.h"],
    visibility = ["//src/resource/tests:__pkg__"],
    deps = [
        ":resource_manager",
        ":resource_registry",
    ],
)

kwaque_cc_library(
    name = "resource_affinity_test_support",
    testonly = True,
    srcs = [
        "resource_manager.cc",
        "resource_registry.cc",
    ],
    hdrs = [
        "resource_manager.h",
        "resource_registry.h",
    ],
    visibility = ["//src/resource/tests:__pkg__"],
    deps = [
        ":resource_config",
        ":workload_class",
        "//src/base",
        "//src/base:error",
        "//src/base:invariant_test_support",
        "//src/runtime:error",
        "//src/runtime:shard_affinity_test_support",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "memory_affinity_test_support",
    testonly = True,
    srcs = [
        "memory_budget.cc",
        "reclaimer_registry.cc",
    ],
    hdrs = [
        "memory_budget.h",
        "reclaimer_registry.h",
    ],
    visibility = ["//src/resource/tests:__pkg__"],
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/base:invariant_test_support",
        "//src/runtime:error",
        "//src/runtime:shard_affinity_test_support",
        "@seastar",
    ],
)
