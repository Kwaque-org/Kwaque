load("//bazel:build.bzl", "kwaque_cc_binary")

package(default_visibility = ["//visibility:private"])

kwaque_cc_binary(
    name = "reactor_probe",
    srcs = ["reactor_probe.cc"],
    visibility = ["//visibility:public"],
    deps = ["@seastar"],
)
