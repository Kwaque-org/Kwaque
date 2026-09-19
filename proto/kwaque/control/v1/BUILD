load("@protobuf//bazel:cc_proto_library.bzl", "cc_proto_library")
load("@protobuf//bazel:proto_library.bzl", "proto_library")

package(default_visibility = ["//visibility:private"])

proto_library(
    name = "redirect_proto",
    srcs = ["redirect.proto"],
    deps = [
        "//proto/kwaque/common/v1:error_proto",
        "//proto/kwaque/common/v1:identity_proto",
    ],
)

cc_proto_library(
    name = "redirect_cc_proto",
    visibility = [
        "//src/protocol:__pkg__",
        "//src/protocol/tests:__pkg__",
    ],
    deps = [":redirect_proto"],
)

proto_library(
    name = "handshake_proto",
    srcs = ["handshake.proto"],
    deps = [
        "//proto/kwaque/common/v1:build_info_proto",
        "//proto/kwaque/common/v1:capability_proto",
    ],
)

cc_proto_library(
    name = "handshake_cc_proto",
    visibility = [
        "//src/protocol:__pkg__",
        "//src/protocol/tests:__pkg__",
    ],
    deps = [":handshake_proto"],
)
