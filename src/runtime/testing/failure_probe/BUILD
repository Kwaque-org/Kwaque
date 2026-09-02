load("//bazel:build.bzl", "kwaque_cc_library")

package(default_visibility = ["//visibility:private"])

kwaque_cc_library(
    name = "failure_probe",
    testonly = True,
    srcs = ["failure_probe.cc"],
    hdrs = ["failure_probe.h"],
    visibility = [
        "//src/runtime/tests:__pkg__",
        "//src/simulation:__subpackages__",
    ],
    deps = [
        "//src/runtime:error",
        "//src/runtime:fault",
        "//src/runtime:fault_api",
        "//src/runtime:shard_affinity",
        "//src/runtime:time",
        "//src/runtime:timer",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "failure_probe_test_support",
    testonly = True,
    hdrs = ["failure_probe_test_support.h"],
    visibility = ["//src/simulation/tests:__pkg__"],
    deps = [":failure_probe"],
)
