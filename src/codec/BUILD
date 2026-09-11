load("//bazel:build.bzl", "kwaque_cc_library")

package(default_visibility = ["//visibility:private"])

kwaque_cc_library(
    name = "crc32c",
    hdrs = ["crc32c.h"],
    visibility = [
        "//src/codec/tests:__pkg__",
        "//src/model:__pkg__",
        "//src/model/tests:__pkg__",
        "//src/protocol:__pkg__",
        "//src/protocol/tests:__pkg__",
        "//src/storage:__pkg__",
        "//src/storage/tests:__pkg__",
    ],
    deps = ["@abseil-cpp//absl/crc:crc32c"],
)

kwaque_cc_library(
    name = "digest",
    hdrs = ["digest.h"],
    visibility = [
        "//src/codec/tests:__pkg__",
        "//src/compression:__pkg__",
        "//src/compression/tests:__pkg__",
        "//src/model:__pkg__",
        "//src/model/tests:__pkg__",
        "//src/protocol:__pkg__",
        "//src/protocol/tests:__pkg__",
        "//src/simulation:__pkg__",
        "//src/simulation/tests:__pkg__",
        "//src/storage:__pkg__",
        "//src/storage/tests:__pkg__",
    ],
)

kwaque_cc_library(
    name = "error",
    hdrs = ["error.h"],
    visibility = [
        "//src/codec/tests:__pkg__",
        "//src/compression:__pkg__",
        "//src/compression/tests:__pkg__",
        "//src/model:__pkg__",
        "//src/model/tests:__pkg__",
        "//src/protocol:__pkg__",
        "//src/protocol/tests:__pkg__",
        "//src/storage:__pkg__",
        "//src/storage/tests:__pkg__",
    ],
    deps = ["//src/base:error"],
)

kwaque_cc_library(
    name = "integer",
    srcs = [
        "integer.cc",
        "staging.cc",
    ],
    hdrs = [
        "integer.h",
        "staging.h",
        "transaction.h",
    ],
    visibility = [
        "//src/codec/tests:__pkg__",
        "//src/compression:__pkg__",
        "//src/compression/tests:__pkg__",
        "//src/model:__pkg__",
        "//src/model/tests:__pkg__",
        "//src/protocol:__pkg__",
        "//src/protocol/tests:__pkg__",
        "//src/storage:__pkg__",
        "//src/storage/tests:__pkg__",
    ],
    deps = [
        ":error",
        ":limits",
        "//src/base",
        "//src/base:error",
        "//src/base:invariant",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_builder",
        "//src/bytes:fragmented_buffer_parser",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "limits",
    srcs = ["limits.cc"],
    hdrs = ["limits.h"],
    visibility = [
        "//src/codec/tests:__pkg__",
        "//src/compression:__pkg__",
        "//src/compression/tests:__pkg__",
        "//src/model:__pkg__",
        "//src/model/tests:__pkg__",
        "//src/protocol:__pkg__",
        "//src/protocol/tests:__pkg__",
        "//src/storage:__pkg__",
        "//src/storage/tests:__pkg__",
    ],
    deps = [
        "//src/base",
        "//src/base:error",
    ],
)

kwaque_cc_library(
    name = "sha256",
    srcs = ["sha256.cc"],
    hdrs = ["sha256.h"],
    visibility = [
        "//src/codec/tests:__pkg__",
        "//src/model:__pkg__",
        "//src/model/tests:__pkg__",
        "//src/simulation:__pkg__",
        "//src/simulation/tests:__pkg__",
        "//src/storage:__pkg__",
        "//src/storage/tests:__pkg__",
    ],
    deps = [
        ":digest",
        "@openssl",
    ],
)

kwaque_cc_library(
    name = "cooperative",
    srcs = ["cooperative.cc"],
    hdrs = ["cooperative.h"],
    visibility = [
        "//src/codec/tests:__pkg__",
        "//src/compression:__pkg__",
        "//src/compression/tests:__pkg__",
        "//src/model:__pkg__",
        "//src/model/tests:__pkg__",
        "//src/protocol:__pkg__",
        "//src/protocol/tests:__pkg__",
        "//src/storage:__pkg__",
        "//src/storage/tests:__pkg__",
    ],
    deps = [
        ":error",
        ":limits",
        "//src/base",
        "//src/base:invariant",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "crc32c_cooperative",
    srcs = ["crc32c_cooperative.cc"],
    hdrs = ["crc32c_cooperative.h"],
    visibility = [
        "//src/codec/tests:__pkg__",
        "//src/model:__pkg__",
        "//src/model/tests:__pkg__",
        "//src/protocol:__pkg__",
        "//src/protocol/tests:__pkg__",
        "//src/storage:__pkg__",
        "//src/storage/tests:__pkg__",
    ],
    deps = [
        ":cooperative",
        ":crc32c",
        "//src/base:invariant",
        "//src/bytes:fragmented_buffer",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "sha256_cooperative",
    srcs = ["sha256_cooperative.cc"],
    hdrs = ["sha256_cooperative.h"],
    visibility = [
        "//src/codec/tests:__pkg__",
        "//src/model:__pkg__",
        "//src/model/tests:__pkg__",
        "//src/protocol:__pkg__",
        "//src/protocol/tests:__pkg__",
        "//src/storage:__pkg__",
        "//src/storage/tests:__pkg__",
    ],
    deps = [
        ":cooperative",
        ":sha256",
        "//src/base:invariant",
        "//src/bytes:fragmented_buffer",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "staging_cooperative",
    srcs = ["staging_cooperative.cc"],
    hdrs = ["staging_cooperative.h"],
    visibility = [
        "//src/codec/tests:__pkg__",
        "//src/compression:__pkg__",
        "//src/compression/tests:__pkg__",
        "//src/model:__pkg__",
        "//src/model/tests:__pkg__",
        "//src/protocol:__pkg__",
        "//src/protocol/tests:__pkg__",
        "//src/storage:__pkg__",
        "//src/storage/tests:__pkg__",
    ],
    deps = [
        ":cooperative",
        ":error",
        ":integer",
        ":limits",
        "//src/base",
        "//src/base:invariant",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_builder",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "collection",
    hdrs = ["collection.h"],
    visibility = [
        "//src/codec/tests:__pkg__",
        "//src/compression:__pkg__",
        "//src/compression/tests:__pkg__",
        "//src/model:__pkg__",
        "//src/model/tests:__pkg__",
        "//src/protocol:__pkg__",
        "//src/protocol/tests:__pkg__",
        "//src/storage:__pkg__",
        "//src/storage/tests:__pkg__",
    ],
    deps = [
        ":cooperative",
        ":error",
        ":integer",
        ":limits",
        "//src/base",
        "//src/base:error",
        "//src/base:invariant",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_builder",
        "//src/bytes:fragmented_buffer_parser",
        "@seastar",
    ],
)

filegroup(
    name = "package_boundary",
    srcs = ["README.md"],
)
