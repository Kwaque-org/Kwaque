load("//bazel:build.bzl", "kwaque_cc_library")
load("//bazel:test.bzl", "kwaque_cc_benchmark", "kwaque_cc_fuzz_test", "kwaque_cc_seastar_gtest")

package(default_visibility = ["//visibility:private"])

kwaque_cc_library(
    name = "batch_frame_test_support",
    testonly = True,
    hdrs = ["batch_frame_test_support.h"],
    deps = [
        ":frame_test_support",
        "//src/model:batch_codec",
        "//src/model:batch_identity",
        "//src/model:epoch",
        "//src/model:identity",
        "//src/model/tests:record_fuzz_cases",
        "//src/protocol:frame_codec",
    ],
)

kwaque_cc_library(
    name = "frame_test_support",
    testonly = True,
    srcs = ["frame_test_support.cc"],
    hdrs = ["frame_test_support.h"],
    deps = [
        "//src/base",
        "//src/bytes:allocation_test_profile",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_builder",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:cooperative",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/codec:staging_cooperative",
        "//src/codec/tests:envelope_test_fixture",
        "//src/model:transport_identity",
        "//src/protocol:frame",
        "//src/protocol:frame_codec",
        "@crc32c",
        "@seastar",
    ],
)

[
    kwaque_cc_seastar_gtest(
        name = name,
        srcs = [name + ".cc"],
        memory = "192MiB",
        deps = [
            ":frame_test_support",
            "//src/base",
            "//src/base:error",
            "//src/bytes:fragmented_buffer",
            "//src/bytes:fragmented_buffer_parser",
            "//src/bytes:fragmented_buffer_test_support",
            "//src/codec:cooperative",
            "//src/codec:crc32c",
            "//src/codec:error",
            "//src/codec:integer",
            "//src/codec:limits",
            "//src/model:transport_identity",
            "//src/protocol:frame",
            "//src/protocol:frame_codec",
        ],
    )
    for name in [
        "frame_prefix_test",
        "frame_header_test",
        "frame_encode_test",
        "frame_decode_test",
    ]
]

test_suite(
    name = "format_tests",
    tests = [
        ":batch_frame_codec_test",
        ":frame_corruption_test",
        ":frame_decode_test",
        ":frame_encode_test",
        ":frame_fuzz_cases_test",
        ":frame_header_test",
        ":frame_prefix_test",
        ":frame_qualification_test",
    ],
)

kwaque_cc_seastar_gtest(
    name = "batch_frame_codec_test",
    srcs = ["batch_frame_codec_test.cc"],
    memory = "256MiB",
    deps = [
        ":batch_frame_test_support",
        ":frame_test_support",
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:cooperative",
        "//src/codec:crc32c",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/model:batch",
        "//src/model:batch_codec",
        "//src/model:batch_context",
        "//src/model:batch_identity",
        "//src/model:epoch",
        "//src/model:identity",
        "//src/model:position",
        "//src/model/tests:record_fuzz_cases",
        "//src/protocol:batch_frame_codec",
        "//src/protocol:frame",
        "//src/protocol:frame_codec",
    ],
)

kwaque_cc_seastar_gtest(
    name = "frame_qualification_test",
    srcs = ["frame_qualification_test.cc"],
    memory = "256MiB",
    deps = [
        ":batch_frame_test_support",
        ":frame_test_support",
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_builder",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:cooperative",
        "//src/codec:envelope",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/codec:staging_cooperative",
        "//src/model:batch",
        "//src/model:batch_codec",
        "//src/model:batch_context",
        "//src/model:batch_identity",
        "//src/model:epoch",
        "//src/model:identity",
        "//src/model:transport_identity",
        "//src/model/tests:model_bench_support",
        "//src/model/tests:record_fuzz_cases",
        "//src/protocol:batch_frame_codec",
        "//src/protocol:frame",
        "//src/protocol:frame_codec",
        "@crc32c",
    ],
)

kwaque_cc_seastar_gtest(
    name = "frame_corruption_test",
    srcs = ["frame_corruption_test.cc"],
    memory = "256MiB",
    deps = [
        ":batch_frame_test_support",
        ":frame_test_support",
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:cooperative",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/model:batch_codec",
        "//src/model:batch_identity",
        "//src/model:epoch",
        "//src/model:identity",
        "//src/model:transport_identity",
        "//src/model/tests:record_fuzz_cases",
        "//src/protocol:batch_frame_codec",
        "//src/protocol:frame",
        "//src/protocol:frame_codec",
    ],
)

kwaque_cc_library(
    name = "frame_fuzz_cases",
    testonly = True,
    srcs = [
        "frame_fuzz_cases.cc",
        "frame_fuzz_oracle.cc",
    ],
    hdrs = [
        "frame_fuzz_cases.h",
        "frame_fuzz_oracle.h",
    ],
    deps = [
        ":batch_frame_test_support",
        ":frame_test_support",
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:cooperative",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/codec:sha256",
        "//src/model:batch",
        "//src/model:batch_codec",
        "//src/model:batch_context",
        "//src/model:identity",
        "//src/model/tests:record_fuzz_cases",
        "//src/protocol:batch_frame_codec",
        "//src/protocol:frame",
        "//src/protocol:frame_codec",
        "@seastar",
    ],
)

kwaque_cc_seastar_gtest(
    name = "frame_fuzz_cases_test",
    srcs = ["frame_fuzz_cases_test.cc"],
    memory = "256MiB",
    deps = [
        ":batch_frame_test_support",
        ":frame_fuzz_cases",
        ":frame_test_support",
        "//src/base:error",
    ],
)

kwaque_cc_fuzz_test(
    name = "frame_fuzz",
    srcs = ["frame_fuzz.cc"],
    args = [
        "-max_len=16384",
        "-rss_limit_mb=1024",
    ],
    corpus = glob(["testdata/frame_fuzz_corpus/*.bin"]),
    tags = [
        "resources:cpu:1",
        "resources:memory:1024",
    ],
    deps = [
        ":frame_fuzz_cases",
        "//src/runtime/testing:seastar_fuzz",
        "@openssl",
        "@seastar",
    ],
)

kwaque_cc_benchmark(
    name = "protocol_bench",
    srcs = ["frame_bench.cc"],
    memory = "512MiB",
    deps = [
        ":frame_test_support",
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:cooperative",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/codec:sha256",
        "//src/codec/tests:codec_bench_reference",
        "//src/model:batch",
        "//src/model:batch_codec",
        "//src/model:batch_context",
        "//src/model/tests:model_bench_support",
        "//src/protocol:batch_frame_codec",
        "//src/protocol:frame",
        "//src/protocol:frame_codec",
        "@crc32c",
        "@fmt",
    ],
)

test_suite(
    name = "golden_tests",
    tests = [
        ":batch_frame_codec_test",
        ":frame_corruption_test",
        ":frame_decode_test",
        ":frame_encode_test",
        ":frame_fuzz_cases_test",
        ":frame_prefix_test",
    ],
)
