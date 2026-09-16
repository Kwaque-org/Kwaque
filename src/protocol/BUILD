load("//bazel:build.bzl", "kwaque_cc_library")

package(default_visibility = ["//visibility:private"])

kwaque_cc_library(
    name = "frame",
    hdrs = ["frame.h"],
    visibility = ["//src/protocol/tests:__pkg__"],
    deps = [
        "//src/base:error",
        "//src/codec:error",
        "//src/model:transport_identity",
    ],
)

kwaque_cc_library(
    name = "frame_codec",
    srcs = [
        "frame_decode.cc",
        "frame_encode.cc",
        "frame_header.cc",
        "frame_prefix.cc",
    ],
    hdrs = [
        "frame_codec.h",
        "frame_decode_internal.h",
    ],
    visibility = ["//src/protocol/tests:__pkg__"],
    deps = [
        ":frame",
        "//src/base",
        "//src/base:error",
        "//src/base:invariant",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:cooperative",
        "//src/codec:crc32c",
        "//src/codec:crc32c_cooperative",
        "//src/codec:envelope",
        "//src/codec:error",
        "//src/codec:framing_internal",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/codec:staging_cooperative",
        "//src/model:transport_identity",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "batch_frame_codec",
    srcs = ["batch_frame_codec.cc"],
    hdrs = ["batch_frame_codec.h"],
    visibility = ["//src/protocol/tests:__pkg__"],
    deps = [
        ":frame",
        ":frame_codec",
        "//src/base",
        "//src/base:error",
        "//src/base:invariant",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:cooperative",
        "//src/codec:envelope",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/model:batch_codec",
        "@seastar",
    ],
)

filegroup(
    name = "package_boundary",
    srcs = ["README.md"],
)
