load("//bazel:test.bzl", "kwaque_cc_test")

package(default_visibility = ["//visibility:private"])

kwaque_cc_test(
    name = "limits_test",
    srcs = ["limits_test.cc"],
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/codec:limits",
    ],
)
