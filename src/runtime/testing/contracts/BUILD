load("//bazel:build.bzl", "kwaque_cc_library")

package(default_visibility = ["//visibility:private"])

kwaque_cc_library(
    name = "contract_backends",
    testonly = True,
    hdrs = ["contract_backends.h"],
    visibility = ["//src/runtime/tests:__pkg__"],
    deps = [
        "//src/bytes:fragmented_buffer",
        "//src/runtime:environment",
        "@seastar",
    ],
)
