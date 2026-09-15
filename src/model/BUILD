load("//bazel:build.bzl", "kwaque_cc_library")

package(default_visibility = ["//visibility:private"])

kwaque_cc_library(
    name = "identity",
    hdrs = ["identity.h"],
    visibility = [
        "//src/model/tests:__pkg__",
        "//src/protocol:__pkg__",
        "//src/protocol/tests:__pkg__",
        "//src/storage:__pkg__",
        "//src/storage/tests:__pkg__",
    ],
    deps = [
        "//src/base",
        "//src/base:error",
        "@boost//:uuid_value",
    ],
)

kwaque_cc_library(
    name = "epoch",
    hdrs = ["epoch.h"],
    visibility = [
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
    name = "transport_identity",
    hdrs = ["transport_identity.h"],
    visibility = [
        "//src/model/tests:__pkg__",
        "//src/protocol:__pkg__",
        "//src/protocol/tests:__pkg__",
    ],
)

kwaque_cc_library(
    name = "batch_identity",
    hdrs = ["batch_identity.h"],
    visibility = [
        "//src/model/tests:__pkg__",
        "//src/protocol:__pkg__",
        "//src/protocol/tests:__pkg__",
        "//src/storage:__pkg__",
        "//src/storage/tests:__pkg__",
    ],
    deps = [
        ":epoch",
        ":identity",
        "//src/base",
        "//src/base:error",
    ],
)

kwaque_cc_library(
    name = "position",
    hdrs = ["position.h"],
    visibility = [
        "//src/model/tests:__pkg__",
        "//src/protocol:__pkg__",
        "//src/protocol/tests:__pkg__",
        "//src/storage:__pkg__",
        "//src/storage/tests:__pkg__",
    ],
    deps = [
        ":epoch",
        ":identity",
        "//src/base",
        "//src/base:error",
        "//src/runtime:file_position",
        "//src/runtime:time",
    ],
)

kwaque_cc_library(
    name = "keyspace",
    srcs = ["keyspace.cc"],
    hdrs = ["keyspace.h"],
    visibility = [
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
    name = "segment_state",
    hdrs = ["segment_state.h"],
    visibility = [
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
    name = "topic_policy",
    hdrs = ["topic_policy.h"],
    visibility = [
        "//src/model/tests:__pkg__",
        "//src/protocol:__pkg__",
        "//src/protocol/tests:__pkg__",
        "//src/storage:__pkg__",
        "//src/storage/tests:__pkg__",
    ],
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/runtime:time",
    ],
)

kwaque_cc_library(
    name = "batch_context",
    hdrs = ["batch_context.h"],
    visibility = [
        "//src/model/tests:__pkg__",
        "//src/protocol:__pkg__",
        "//src/protocol/tests:__pkg__",
        "//src/storage:__pkg__",
        "//src/storage/tests:__pkg__",
    ],
    deps = [
        ":batch_identity",
        ":position",
        "//src/base",
        "//src/base:error",
        "//src/codec:limits",
        "//src/runtime:time",
    ],
)

kwaque_cc_library(
    name = "record",
    hdrs = ["record.h"],
    visibility = [
        "//src/model/tests:__pkg__",
        "//src/protocol:__pkg__",
        "//src/protocol/tests:__pkg__",
        "//src/storage:__pkg__",
        "//src/storage/tests:__pkg__",
    ],
    deps = [
        ":position",
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
    ],
)

kwaque_cc_library(
    name = "record_codec",
    srcs = [
        "record_codec.cc",
        "record_decode.cc",
        "record_encode.cc",
        "record_scan.cc",
    ],
    hdrs = [
        "record_codec.h",
        "record_encode.h",
        "record_scan.h",
    ],
    implementation_deps = [
        "//src/base:invariant",
    ],
    visibility = [
        "//src/model/tests:__pkg__",
        "//src/protocol:__pkg__",
        "//src/protocol/tests:__pkg__",
        "//src/storage:__pkg__",
        "//src/storage/tests:__pkg__",
    ],
    deps = [
        ":position",
        ":record",
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_builder",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:cooperative",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/runtime:time",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "batch_wire",
    hdrs = ["batch_wire.h"],
    visibility = ["//src/model/tests:__pkg__"],
    deps = [
        ":batch_context",
        "//src/codec:integer",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "batch",
    hdrs = ["batch.h"],
    visibility = [
        "//src/model/tests:__pkg__",
        "//src/protocol:__pkg__",
        "//src/protocol/tests:__pkg__",
        "//src/storage:__pkg__",
        "//src/storage/tests:__pkg__",
    ],
    deps = [
        ":batch_context",
        ":batch_identity",
        ":position",
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/codec:digest",
    ],
)

kwaque_cc_library(
    name = "fingerprint",
    srcs = ["fingerprint.cc"],
    hdrs = ["fingerprint.h"],
    implementation_deps = [
        ":batch_wire",
        "//src/codec:sha256",
    ],
    visibility = [
        "//src/model/tests:__pkg__",
        "//src/protocol:__pkg__",
        "//src/protocol/tests:__pkg__",
        "//src/storage:__pkg__",
        "//src/storage/tests:__pkg__",
    ],
    deps = [
        ":batch_context",
        "//src/base",
        "//src/bytes:fragmented_buffer",
        "//src/codec:cooperative",
        "//src/codec:digest",
        "//src/codec:error",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "batch_staging",
    srcs = ["batch_staging.cc"],
    hdrs = ["batch_staging.h"],
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_builder",
        "//src/codec:error",
        "//src/codec:limits",
    ],
)

kwaque_cc_library(
    name = "batch_builder",
    srcs = [
        "batch_builder.cc",
        "batch_rewrite.cc",
    ],
    hdrs = [
        "batch_builder.h",
        "batch_rewrite.h",
    ],
    implementation_deps = [
        ":batch_context",
        ":batch_staging",
        ":fingerprint",
        ":position",
        ":record_codec",
        "//src/base:invariant",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_parser",
    ],
    visibility = [
        "//src/model/tests:__pkg__",
        "//src/protocol:__pkg__",
        "//src/protocol/tests:__pkg__",
        "//src/storage:__pkg__",
        "//src/storage/tests:__pkg__",
    ],
    deps = [
        ":batch",
        ":batch_identity",
        ":record",
        "//src/base",
        "//src/bytes:fragmented_buffer_builder",
        "//src/codec:cooperative",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/runtime:time",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "batch_codec",
    srcs = [
        "batch_codec.cc",
        "batch_decode.cc",
    ],
    hdrs = [
        "batch_codec.h",
        "batch_decode_body.h",
    ],
    implementation_deps = [
        ":batch_context",
        ":batch_identity",
        ":batch_wire",
        ":epoch",
        ":fingerprint",
        ":identity",
        ":position",
        ":record_codec",
        "//src/base:error",
        "//src/base:invariant",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:staging_cooperative",
        "//src/runtime:time",
    ],
    visibility = [
        "//src/model/tests:__pkg__",
        "//src/protocol:__pkg__",
        "//src/protocol/tests:__pkg__",
        "//src/storage:__pkg__",
        "//src/storage/tests:__pkg__",
    ],
    deps = [
        ":batch",
        "//src/base",
        "//src/bytes:fragmented_buffer",
        "//src/codec:cooperative",
        "//src/codec:envelope",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/compression",
        "@seastar",
    ],
)

filegroup(
    name = "package_boundary",
    srcs = ["README.md"],
)
