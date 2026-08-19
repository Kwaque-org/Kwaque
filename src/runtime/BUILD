load("//bazel:build.bzl", "kwaque_cc_binary", "kwaque_cc_library")

package(default_visibility = ["//visibility:private"])

kwaque_cc_library(
    name = "runtime_service",
    srcs = ["runtime_service.cc"],
    hdrs = ["runtime_service.h"],
    visibility = ["//visibility:public"],
    deps = ["@seastar"],
)

kwaque_cc_library(
    name = "stop_signal",
    srcs = ["stop_signal.cc"],
    hdrs = ["stop_signal.h"],
    visibility = ["//visibility:public"],
    deps = ["@seastar"],
)

kwaque_cc_binary(
    name = "reactor_probe",
    srcs = ["reactor_probe.cc"],
    visibility = ["//visibility:public"],
    deps = ["@seastar"],
)
