load("//bazel:build.bzl", "kwaque_cc_library")

package(default_visibility = ["//visibility:private"])

kwaque_cc_library(
    name = "control",
    hdrs = ["control.h"],
    visibility = ["//src/protocol/tests:__pkg__"],
    deps = [
        "//src/base",
        "//src/model:epoch",
        "//src/model:identity",
    ],
)

kwaque_cc_library(
    name = "control_preflight",
    srcs = ["control_preflight.cc"],
    hdrs = ["control_preflight.h"],
    implementation_deps = ["@protobuf//third_party/utf8_range"],
    visibility = ["//src/protocol/tests:__pkg__"],
    deps = [
        ":control_schema",
        "//src/base",
        "//src/base:error",
        "//src/codec:cooperative",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "control_codec",
    srcs = [
        "control_codec.cc",
        "control_encode.cc",
        "control_validation.cc",
    ],
    hdrs = [
        "control_codec.h",
        "control_internal.h",
        "control_native.h",
    ],
    implementation_deps = [
        ":control_memory",
        ":control_preflight",
        ":control_schema",
        "//proto/kwaque/common/v1:build_info_cc_proto",
        "//proto/kwaque/common/v1:capability_cc_proto",
        "//proto/kwaque/common/v1:error_cc_proto",
        "//proto/kwaque/common/v1:identity_cc_proto",
        "//proto/kwaque/control/v1:handshake_cc_proto",
        "//proto/kwaque/control/v1:redirect_cc_proto",
        "@protobuf//src/google/protobuf/io",
        "@protobuf//third_party/utf8_range",
    ],
    visibility = ["//src/protocol/tests:__pkg__"],
    deps = [
        ":control",
        ":frame",
        "//src/base",
        "//src/base:error",
        "//src/base:invariant",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:cooperative",
        "//src/codec:error",
        "//src/codec:framing_internal",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/model:epoch",
        "//src/model:identity",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "control_frame_codec",
    srcs = ["control_frame_codec.cc"],
    hdrs = ["control_frame_codec.h"],
    visibility = ["//src/protocol/tests:__pkg__"],
    deps = [
        ":control",
        ":control_codec",
        ":frame",
        ":frame_codec",
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:cooperative",
        "//src/codec:error",
        "//src/codec:framing_internal",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/model:transport_identity",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "control_schema",
    srcs = ["control_schema.cc"],
    hdrs = ["control_schema.h"],
    visibility = ["//src/protocol/tests:__pkg__"],
    deps = [
        ":frame",
        "//src/base",
        "//src/codec:limits",
    ],
)

kwaque_cc_library(
    name = "control_memory",
    srcs = ["control_memory.cc"],
    hdrs = ["control_memory.h"],
    implementation_deps = [
        "//proto/kwaque/common/v1:build_info_cc_proto",
        "//proto/kwaque/common/v1:capability_cc_proto",
        "//proto/kwaque/common/v1:error_cc_proto",
        "//proto/kwaque/common/v1:identity_cc_proto",
        "//proto/kwaque/control/v1:handshake_cc_proto",
        "//proto/kwaque/control/v1:redirect_cc_proto",
        "@protobuf",
        "@protobuf//:protobuf_lite",
    ],
    visibility = ["//src/protocol/tests:__pkg__"],
    deps = [
        ":control_schema",
        "//src/base",
        "//src/bytes:fragmented_buffer",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
    ],
)

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
