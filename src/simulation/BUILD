load("//bazel:build.bzl", "kwaque_cc_library")

package(default_visibility = ["//visibility:private"])

filegroup(
    name = "package_boundary",
    srcs = ["README.md"],
)

kwaque_cc_library(
    name = "event_trace",
    srcs = ["event_trace.cc"],
    hdrs = [
        "determinism_version.h",
        "event_trace.h",
    ],
    visibility = ["//src/simulation:__subpackages__"],
    deps = [
        "//src/base",
        "//src/base:invariant",
        "//src/runtime:error",
        "//src/runtime:fault",
        "//src/runtime:fault_api",
        "//src/runtime:time",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "event_trace_invariant_test_support",
    testonly = True,
    srcs = ["event_trace.cc"],
    hdrs = [
        "determinism_version.h",
        "event_trace.h",
    ],
    visibility = [
        "//src/simulation:__pkg__",
        "//src/simulation/tests:__pkg__",
    ],
    deps = [
        "//src/base",
        "//src/base:invariant_test_support",
        "//src/runtime:error",
        "//src/runtime:fault",
        "//src/runtime:fault_api",
        "//src/runtime:time",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "scheduler",
    srcs = ["scheduler.cc"],
    hdrs = ["scheduler.h"],
    visibility = ["//src/simulation:__subpackages__"],
    deps = [
        ":event_trace",
        "//src/base:invariant",
        "//src/runtime:error",
        "//src/runtime:shard_affinity",
        "//src/runtime:time",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "scheduler_test_support",
    testonly = True,
    hdrs = ["scheduler_test_support.h"],
    visibility = ["//src/simulation/tests:__pkg__"],
    deps = [":scheduler"],
)

kwaque_cc_library(
    name = "scheduler_affinity_test_support",
    testonly = True,
    srcs = ["scheduler.cc"],
    hdrs = ["scheduler.h"],
    visibility = ["//src/simulation/tests:__pkg__"],
    deps = [
        ":event_trace_invariant_test_support",
        "//src/base:invariant_test_support",
        "//src/runtime:error",
        "//src/runtime:shard_affinity_test_support",
        "//src/runtime:time",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "virtual_time",
    srcs = ["virtual_time.cc"],
    hdrs = ["virtual_time.h"],
    visibility = ["//src/simulation:__subpackages__"],
    deps = [
        ":scheduler",
        "//src/base:invariant",
        "//src/runtime:error",
        "//src/runtime:shard_affinity",
        "//src/runtime:time",
    ],
)

kwaque_cc_library(
    name = "virtual_time_test_support",
    testonly = True,
    hdrs = ["virtual_time_test_support.h"],
    visibility = ["//src/simulation/tests:__pkg__"],
    deps = [":virtual_time"],
)

kwaque_cc_library(
    name = "virtual_time_invariant_test_support",
    testonly = True,
    srcs = ["virtual_time.cc"],
    hdrs = [
        "virtual_time.h",
        "virtual_time_test_support.h",
    ],
    visibility = ["//src/simulation/tests:__pkg__"],
    deps = [
        ":scheduler_affinity_test_support",
        "//src/base:invariant_test_support",
        "//src/runtime:error",
        "//src/runtime:shard_affinity_test_support",
        "//src/runtime:time",
    ],
)

kwaque_cc_library(
    name = "timer",
    srcs = ["timer.cc"],
    hdrs = ["timer.h"],
    visibility = ["//src/simulation:__subpackages__"],
    deps = [
        ":scheduler",
        "//src/base:invariant",
        "//src/runtime:error",
        "//src/runtime:shard_affinity",
        "//src/runtime:time",
        "//src/runtime:timer",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "philox",
    hdrs = ["philox.h"],
    visibility = ["//src/simulation:__subpackages__"],
)

kwaque_cc_library(
    name = "deterministic_random",
    srcs = ["deterministic_random.cc"],
    hdrs = ["deterministic_random.h"],
    visibility = ["//src/simulation:__subpackages__"],
    deps = [
        ":event_trace",
        ":philox",
        "//src/base:invariant",
        "//src/runtime:error",
        "//src/runtime:random",
    ],
)

kwaque_cc_library(
    name = "deterministic_random_test_support",
    testonly = True,
    hdrs = ["deterministic_random_test_support.h"],
    visibility = ["//src/simulation/tests:__pkg__"],
    deps = [":deterministic_random"],
)

kwaque_cc_library(
    name = "fault_schedule",
    srcs = ["fault_schedule.cc"],
    hdrs = ["fault_schedule.h"],
    visibility = ["//src/simulation:__subpackages__"],
    deps = [
        ":deterministic_random",
        ":event_trace",
        ":scheduler",
        "//src/base:invariant",
        "//src/runtime:fault",
        "//src/runtime:fault_api",
        "//src/runtime:random",
        "//src/runtime:shard_affinity",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "fake_file",
    srcs = ["fake_file.cc"],
    hdrs = ["fake_file.h"],
    visibility = ["//src/simulation:__subpackages__"],
    deps = [
        ":fault_schedule",
        ":scheduler",
        "//src/base",
        "//src/base:invariant",
        "//src/runtime:error",
        "//src/runtime:fault_api",
        "//src/runtime:file",
        "//src/runtime:shard_affinity",
        "@abseil-cpp//absl/container:btree",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "fake_file_test_support",
    testonly = True,
    hdrs = ["fake_file_test_support.h"],
    visibility = ["//src/simulation/tests:__pkg__"],
    deps = [":fake_file"],
)
