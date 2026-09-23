load("//bazel:build.bzl", "kwaque_cc_library")
load("//bazel:test.bzl", "kwaque_cc_benchmark", "kwaque_cc_fuzz_test", "kwaque_cc_seastar_gtest")

package(default_visibility = ["//visibility:private"])

kwaque_cc_seastar_gtest(
    name = "format_fixture_test",
    srcs = ["format_fixture_test.cc"],
    data = ["//src/codec/tests:format_fixture_data"],
    memory = "256MiB",
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/bytes:allocation_test_profile",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:cooperative",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/codec/tests:envelope_test_fixture",
        "//src/codec/tests:format_fixture",
        "//src/compression",
    ],
)

kwaque_cc_seastar_gtest(
    name = "compression_test",
    srcs = [
        "compression_test.cc",
        "test_support.h",
    ],
    memory = "192MiB",
    deps = [
        ":test_support",
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_builder",
        "//src/bytes:fragmented_buffer_test_support",
        "//src/codec:cooperative",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/compression",
    ],
)

kwaque_cc_seastar_gtest(
    name = "lz4_context_test",
    srcs = [
        "lz4_context_test.cc",
        "test_support.h",
    ],
    memory = "192MiB",
    deps = [
        ":test_support",
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_builder",
        "//src/codec:cooperative",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/compression",
        "//src/compression:lz4",
        "@lz4//:lz4_frame",
    ],
)

test_suite(
    name = "format_tests",
    tests = [
        ":compression_cooperation_test",
        ":compression_fuzz_cases_test",
        ":compression_test",
        ":format_fixture_test",
        ":lz4_context_test",
        ":lz4_header_test",
        ":lz4_stream_test",
    ],
)

kwaque_cc_seastar_gtest(
    name = "compression_cooperation_test",
    srcs = [
        "compression_cooperation_test.cc",
        "test_support.h",
    ],
    memory = "256MiB",
    deps = [
        ":test_support",
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_builder",
        "//src/codec:cooperative",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/compression",
    ],
)

[
    kwaque_cc_seastar_gtest(
        name = name,
        srcs = [
            name + ".cc",
            "test_support.h",
        ],
        memory = "192MiB",
        deps = [
            ":test_support",
            "//src/base",
            "//src/base:error",
            "//src/bytes:fragmented_buffer",
            "//src/bytes:fragmented_buffer_builder",
            "//src/bytes:fragmented_buffer_test_support",
            "//src/codec:cooperative",
            "//src/codec:error",
            "//src/codec:integer",
            "//src/codec:limits",
            "//src/compression",
            "//src/compression:lz4",
            "@lz4//:lz4_frame",
        ],
    )
    for name in [
        "lz4_header_test",
        "lz4_stream_test",
    ]
]

kwaque_cc_library(
    name = "test_support",
    testonly = True,
    hdrs = [
        "allocation_profile.h",
        "lz4_test_support.h",
    ],
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/bytes:allocation_test_profile",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_builder",
        "//src/codec:cooperative",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/compression",
        "@lz4//:lz4_frame",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "compression_fuzz_cases",
    testonly = True,
    srcs = ["compression_fuzz_cases.cc"],
    hdrs = ["compression_fuzz_cases.h"],
    deps = [
        ":test_support",
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/codec:cooperative",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/compression",
        "@lz4//:lz4_frame",
        "@seastar",
    ],
)

kwaque_cc_seastar_gtest(
    name = "compression_fuzz_cases_test",
    srcs = ["compression_fuzz_cases_test.cc"],
    memory = "256MiB",
    deps = [
        ":compression_fuzz_cases",
        ":test_support",
    ],
)

kwaque_cc_fuzz_test(
    name = "compression_fuzz",
    srcs = ["compression_fuzz.cc"],
    args = [
        "-max_len=16384",
        "-rss_limit_mb=1024",
    ],
    corpus = glob(["testdata/compression_fuzz_corpus/*.bin"]),
    tags = [
        "resources:cpu:1",
        "resources:memory:1024",
    ],
    deps = [
        ":compression_fuzz_cases",
        "//src/runtime/testing:seastar_fuzz",
    ],
)

kwaque_cc_benchmark(
    name = "compression_bench",
    srcs = ["compression_bench.cc"],
    memory = "256MiB",
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/codec:cooperative",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/codec/tests:codec_bench_reference",
        "//src/codec/tests:qualification_profile",
        "//src/compression",
        "//src/compression:lz4",
        "@fmt",
        "@lz4//:lz4_frame",
    ],
)
