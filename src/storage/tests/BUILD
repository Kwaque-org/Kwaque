load("//bazel:build.bzl", "kwaque_cc_library")
load("//bazel:test.bzl", "kwaque_cc_benchmark", "kwaque_cc_fuzz_test", "kwaque_cc_seastar_gtest", "kwaque_cc_test")

package(default_visibility = ["//visibility:private"])

[
    kwaque_cc_seastar_gtest(
        name = name,
        srcs = [
            name + ".cc",
            "segment_test_support.h",
        ] + (["footer_test_support.h"] if name in [
            "footer_format_test",
            "extent_verifier_test",
            "retry_format_test",
            "sealed_format_test",
            "retry_qualification_test",
        ] else []) + (["retry_test_support.h"] if name in [
            "retry_format_test",
            "sealed_format_test",
            "retry_qualification_test",
        ] else []) + (["wal_test_support.h"] if name == "wal_format_test" else []),
        memory = "256MiB",
        deps = [
            "//src/base",
            "//src/base:error",
            "//src/bytes:allocation_test_profile",
            "//src/bytes:fragmented_buffer",
            "//src/bytes:fragmented_buffer_builder",
            "//src/bytes:fragmented_buffer_parser",
            "//src/codec:cooperative",
            "//src/codec:error",
            "//src/codec:integer",
            "//src/codec:limits",
            "//src/model:batch",
            "//src/model:batch_builder",
            "//src/model:batch_codec",
            "//src/model:batch_context",
            "//src/model:batch_identity",
            "//src/model:epoch",
            "//src/model:identity",
            "//src/model:position",
            "//src/model:record_codec",
            "//src/runtime:file_position",
            "//src/runtime:time",
            "//src/storage:encoded_batch",
            "//src/storage:format_context",
            "//src/storage:segment_format",
            "@lz4//:lz4_frame",
        ] + (["//src/storage:wal_format"] if name == "wal_format_test" else []) + ([
            "//src/storage:footer_format",
            "//src/storage:page_ref",
            "//src/storage:completed_retry",
            "//src/codec:sha256",
        ] if name in [
            "footer_format_test",
            "extent_verifier_test",
            "retry_format_test",
            "sealed_format_test",
            "retry_qualification_test",
        ] else []) + (["//src/bytes:fragmented_buffer_test_support"] if name == "extent_verifier_test" else []),
    )
    for name in [
        "encoded_batch_test",
        "extent_verifier_test",
        "retry_format_test",
        "sealed_format_test",
        "retry_qualification_test",
        "footer_format_test",
        "segment_format_test",
        "segment_relocation_test",
        "wal_format_test",
    ]
]

kwaque_cc_test(
    name = "format_context_test",
    srcs = ["format_context_test.cc"],
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/model:batch_identity",
        "//src/model:epoch",
        "//src/model:identity",
        "//src/model:position",
        "//src/runtime:file_position",
        "//src/storage:format_context",
    ],
)

kwaque_cc_test(
    name = "format_size_test",
    srcs = ["format_size_test.cc"],
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/codec:envelope",
        "//src/codec:limits",
        "//src/model:position",
        "//src/runtime:file_position",
        "//src/storage:format_context",
        "//src/storage:format_size",
    ],
)

test_suite(
    name = "format_tests",
    tests = [
        ":encoded_batch_test",
        ":extent_verifier_test",
        ":footer_format_test",
        ":format_context_test",
        ":format_size_test",
        ":retry_format_test",
        ":retry_qualification_test",
        ":sealed_format_test",
        ":segment_format_test",
        ":segment_relocation_test",
        ":storage_format_fuzz_cases_test",
        ":storage_format_qualification_test",
        ":wal_format_test",
    ],
)

kwaque_cc_library(
    name = "storage_test_support",
    testonly = True,
    srcs = ["storage_format_fixture.cc"],
    hdrs = [
        "footer_test_support.h",
        "retry_test_support.h",
        "segment_test_support.h",
        "storage_format_fixture.h",
        "wal_test_support.h",
    ],
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/bytes:allocation_test_profile",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_builder",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:cooperative",
        "//src/codec:digest",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/codec:sha256",
        "//src/model:batch_codec",
        "//src/model:batch_context",
        "//src/model:batch_identity",
        "//src/model:epoch",
        "//src/model:identity",
        "//src/model:position",
        "//src/runtime:file_position",
        "//src/runtime:time",
        "//src/storage:completed_retry",
        "//src/storage:encoded_batch",
        "//src/storage:footer_format",
        "//src/storage:format_context",
        "//src/storage:page_ref",
        "//src/storage:segment_format",
        "//src/storage:wal_format",
        "@lz4//:lz4_frame",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "storage_large_fixture",
    testonly = True,
    srcs = ["storage_large_fixture.cc"],
    hdrs = ["storage_large_fixture.h"],
    deps = [
        ":storage_test_support",
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/codec:cooperative",
        "//src/codec:error",
        "//src/codec:limits",
        "//src/codec/tests:codec_bench_reference",
        "//src/model:batch",
        "//src/model:batch_builder",
        "//src/model:batch_identity",
        "//src/model:epoch",
        "//src/model:identity",
        "//src/model:position",
        "//src/model:record_codec",
        "//src/runtime:time",
        "//src/storage:encoded_batch",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "storage_format_fuzz_cases",
    testonly = True,
    srcs = ["storage_format_fuzz_cases.cc"],
    hdrs = ["storage_format_fuzz_cases.h"],
    deps = [
        ":storage_test_support",
        "//src/base",
        "//src/base:error",
        "//src/codec:cooperative",
        "//src/codec:digest",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/runtime:file_position",
        "//src/storage:footer_format",
        "//src/storage:format_context",
        "@seastar",
    ],
)

kwaque_cc_seastar_gtest(
    name = "storage_format_fuzz_cases_test",
    srcs = ["storage_format_fuzz_cases_test.cc"],
    memory = "256MiB",
    deps = [
        ":storage_format_fuzz_cases",
        ":storage_test_support",
    ],
)

kwaque_cc_seastar_gtest(
    name = "storage_format_qualification_test",
    srcs = ["storage_format_qualification_test.cc"],
    memory = "384MiB",
    deps = [
        ":storage_large_fixture",
        ":storage_test_support",
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:cooperative",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/codec/tests:codec_bench_reference",
        "//src/model:identity",
        "//src/model:position",
        "//src/storage:encoded_batch",
        "//src/storage:format_context",
        "//src/storage:segment_format",
        "//src/storage:wal_format",
    ],
)

kwaque_cc_fuzz_test(
    name = "storage_format_fuzz",
    srcs = ["storage_format_fuzz.cc"],
    args = [
        "-max_len=16384",
        "-rss_limit_mb=1024",
    ],
    corpus = glob(["testdata/storage_format_fuzz_corpus/*.bin"]),
    tags = [
        "resources:cpu:1",
        "resources:memory:1024",
    ],
    deps = [
        ":storage_format_fuzz_cases",
        "//src/runtime/testing:seastar_fuzz",
        "@openssl",
        "@seastar",
    ],
)

kwaque_cc_benchmark(
    name = "storage_format_bench",
    srcs = ["storage_format_bench.cc"],
    memory = "384MiB",
    deps = [
        ":storage_large_fixture",
        ":storage_test_support",
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:cooperative",
        "//src/codec:digest",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/codec:sha256_cooperative",
        "//src/codec/tests:codec_bench_reference",
        "//src/runtime:file_position",
        "//src/storage:completed_retry",
        "//src/storage:encoded_batch",
        "//src/storage:footer_format",
        "//src/storage:format_context",
        "//src/storage:format_size",
        "//src/storage:page_ref",
        "//src/storage:segment_format",
        "//src/storage:wal_format",
        "@fmt",
    ],
)
