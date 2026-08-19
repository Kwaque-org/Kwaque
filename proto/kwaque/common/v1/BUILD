load("@protobuf//bazel:cc_proto_library.bzl", "cc_proto_library")
load("@protobuf//bazel:proto_library.bzl", "proto_library")
load("//bazel:build.bzl", "kwaque_cc_library")
load("//bazel:test.bzl", "kwaque_cc_seastar_test", "kwaque_cc_test")

package(default_visibility = ["//visibility:private"])

proto_library(
    name = "build_info_proto",
    srcs = ["build_info.proto"],
    visibility = ["//visibility:public"],
)

cc_proto_library(
    name = "build_info_cc_proto",
    visibility = ["//visibility:public"],
    deps = [":build_info_proto"],
)

kwaque_cc_library(
    name = "build_info_codec",
    srcs = ["build_info_codec.cc"],
    hdrs = ["build_info_codec.h"],
    visibility = ["//visibility:public"],
    deps = [":build_info_cc_proto"],
)

kwaque_cc_library(
    name = "build_info_seastar_compat",
    testonly = True,
    srcs = ["build_info_seastar_compat.cc"],
    hdrs = ["build_info_seastar_compat.h"],
    deps = [
        ":build_info_cc_proto",
        "@protobuf",
        "@seastar",
    ],
)

kwaque_cc_test(
    name = "build_info_test",
    srcs = ["build_info_test.cc"],
    data = ["testdata/build_info_v1.hex"],
    deps = [":build_info_codec"],
)

kwaque_cc_seastar_test(
    name = "build_info_seastar_test",
    srcs = ["build_info_seastar_test.cc"],
    cpu = 1,
    memory = "128MiB",
    deps = [":build_info_seastar_compat"],
)
