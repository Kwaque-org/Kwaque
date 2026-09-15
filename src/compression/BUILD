load("//bazel:build.bzl", "kwaque_cc_library")

package(default_visibility = ["//visibility:private"])

kwaque_cc_library(
    name = "compression",
    srcs = [
        "compression.cc",
        "compression_internal.h",
        "lz4_stream.cc",
    ],
    hdrs = ["compression.h"],
    visibility = [
        "//src/compression/tests:__pkg__",
        "//src/model:__pkg__",
        "//src/model/tests:__pkg__",
    ],
    deps = [
        ":lz4",
        "//src/base",
        "//src/base:error",
        "//src/base:invariant",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_builder",
        "//src/codec:cooperative",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "@lz4//:lz4_frame",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "lz4",
    srcs = ["lz4.cc"],
    hdrs = ["lz4.h"],
    visibility = ["//src/compression/tests:__pkg__"],
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/base:invariant",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_builder",
        "//src/codec:cooperative",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "@lz4",
        "@lz4//:lz4_frame",
        "@seastar",
    ],
)
