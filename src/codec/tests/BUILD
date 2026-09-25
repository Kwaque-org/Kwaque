load("@rules_python//python:defs.bzl", "py_library")
load("//bazel:build.bzl", "kwaque_cc_binary", "kwaque_cc_library")
load("//bazel:test.bzl", "kwaque_cc_benchmark", "kwaque_cc_fuzz_test", "kwaque_cc_seastar_gtest", "kwaque_cc_test", "kwaque_py_native_test")
load(":allocation_observer.bzl", "ALLOCATION_OBSERVER_LINKOPTS")

package(default_visibility = ["//visibility:private"])

kwaque_cc_library(
    name = "format_fixture",
    testonly = True,
    hdrs = ["format_fixture.h"],
    visibility = [
        "//src/compression/tests:__pkg__",
        "//src/model/tests:__pkg__",
        "//src/protocol/tests:__pkg__",
        "//src/storage/tests:__pkg__",
    ],
)

kwaque_cc_library(
    name = "format_test_support",
    testonly = True,
    hdrs = ["format_test_support.h"],
    visibility = [
        "//src/model/tests:__pkg__",
        "//src/protocol/tests:__pkg__",
        "//src/storage/tests:__pkg__",
    ],
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:error",
        "@googletest//:gtest",
    ],
)

filegroup(
    name = "format_fixture_data",
    srcs = glob(["testdata/formats/*.hex"]) + ["testdata/formats/manifest.json"],
    visibility = [
        "//src/compression/tests:__pkg__",
        "//src/model/tests:__pkg__",
        "//src/protocol/tests:__pkg__",
        "//src/storage/tests:__pkg__",
        "//tools:__pkg__",
    ],
)

kwaque_cc_library(
    name = "envelope_test_fixture",
    testonly = True,
    hdrs = ["envelope_decode_test_support.h"],
    visibility = [
        "//src/compression/tests:__pkg__",
        "//src/model/tests:__pkg__",
        "//src/protocol/tests:__pkg__",
    ],
    deps = [
        "//src/bytes:fragmented_buffer",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "envelope_fuzz_cases",
    testonly = True,
    srcs = ["envelope_fuzz_cases.cc"],
    hdrs = ["envelope_fuzz_cases.h"],
    deps = [
        ":envelope_test_fixture",
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:cooperative",
        "//src/codec:envelope",
        "//src/codec:error",
        "//src/codec:format_registry",
        "//src/codec:integer",
        "//src/codec:limits",
        "@seastar",
    ],
)

kwaque_cc_seastar_gtest(
    name = "envelope_fuzz_cases_test",
    srcs = ["envelope_fuzz_cases_test.cc"],
    memory = "256MiB",
    deps = [
        ":envelope_fuzz_cases",
        ":envelope_test_fixture",
        "//src/bytes:fragmented_buffer",
    ],
)

kwaque_cc_test(
    name = "format_registry_test",
    srcs = ["format_registry_test.cc"],
    deps = [
        "//src/base:error",
        "//src/codec:error",
        "//src/codec:format_registry",
    ],
)

kwaque_cc_seastar_gtest(
    name = "envelope_prefix_test",
    srcs = ["envelope_prefix_test.cc"],
    memory = "192MiB",
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:envelope",
        "//src/codec:error",
        "//src/codec:format_registry",
        "//src/codec:integer",
        "//src/codec:limits",
    ],
)

kwaque_cc_seastar_gtest(
    name = "header_extensions_test",
    srcs = [
        "header_extensions_test.cc",
        "header_extensions_test_support.cc",
        "header_extensions_test_support.h",
    ],
    memory = "192MiB",
    deps = [
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
    ],
)

kwaque_cc_seastar_gtest(
    name = "envelope_integrity_test",
    srcs = ["envelope_integrity_test.cc"],
    memory = "192MiB",
    deps = [
        ":envelope_test_fixture",
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/codec:cooperative",
        "//src/codec:crc32c",
        "//src/codec:envelope",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
    ],
)

kwaque_cc_seastar_gtest(
    name = "envelope_decode_test",
    srcs = ["envelope_decode_test.cc"],
    memory = "256MiB",
    deps = [
        ":envelope_test_fixture",
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:cooperative",
        "//src/codec:crc32c",
        "//src/codec:envelope",
        "//src/codec:error",
        "//src/codec:format_registry",
        "//src/codec:integer",
        "//src/codec:limits",
    ],
)

kwaque_cc_seastar_gtest(
    name = "envelope_encode_test",
    srcs = ["envelope_encode_test.cc"],
    memory = "256MiB",
    deps = [
        ":envelope_test_fixture",
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_parser",
        "//src/bytes:fragmented_buffer_test_support",
        "//src/codec:cooperative",
        "//src/codec:crc32c",
        "//src/codec:envelope",
        "//src/codec:error",
        "//src/codec:format_registry",
        "//src/codec:integer",
        "//src/codec:limits",
    ],
)

kwaque_cc_library(
    name = "codec_bench_reference",
    testonly = True,
    srcs = [
        "benchmark_buffer.cc",
        "codec_bench_reference.cc",
        "envelope_bench_decode.cc",
        "envelope_bench_encode.cc",
        "envelope_bench_extensions.cc",
        "envelope_bench_integrity.cc",
        "envelope_bench_prefix.cc",
    ],
    hdrs = [
        "benchmark_buffer.h",
        "codec_bench_reference.h",
        "envelope_bench_native.h",
    ],
    visibility = [
        "//src/compression/tests:__pkg__",
        "//src/model/tests:__pkg__",
        "//src/protocol/tests:__pkg__",
        "//src/storage/tests:__pkg__",
    ],
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/base:invariant",
        "//src/bytes:allocation_test_profile",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_builder",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:cooperative",
        "//src/codec:envelope",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/codec:staging_cooperative",
        "@crc32c",
        "@seastar",
    ],
)

kwaque_cc_benchmark(
    name = "codec_bench",
    srcs = [
        "codec_bench.cc",
        "crc32c_bench.cc",
        "envelope_bench.cc",
        "envelope_bench_fixture.h",
        "envelope_bench_reference.cc",
    ],
    memory = "512MiB",
    deps = [
        ":codec_bench_reference",
        ":qualification_profile",
        "//src/base",
        "//src/base:error",
        "//src/base:invariant",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_builder",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:collection",
        "//src/codec:cooperative",
        "//src/codec:crc32c",
        "//src/codec:crc32c_cooperative",
        "//src/codec:envelope",
        "//src/codec:error",
        "//src/codec:format_registry",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/codec:staging_cooperative",
        "@abseil-cpp//absl/crc:crc32c",
        "@crc32c",
    ],
)

kwaque_cc_fuzz_test(
    name = "codec_fuzz",
    srcs = ["codec_fuzz.cc"],
    args = [
        "-max_len=16384",
        "-rss_limit_mb=1024",
    ],
    corpus = glob(["testdata/codec_fuzz_corpus/*.bin"]),
    tags = [
        "resources:cpu:1",
        "resources:memory:1024",
    ],
    deps = [
        ":envelope_fuzz_cases",
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_builder",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:collection",
        "//src/codec:cooperative",
        "//src/codec:crc32c",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/runtime/testing:seastar_fuzz",
        "@crc32c",
        "@seastar",
    ],
)

kwaque_cc_fuzz_test(
    name = "codec_cooperative_fuzz",
    srcs = ["codec_cooperative_fuzz.cc"],
    args = [
        "-max_len=16384",
        "-rss_limit_mb=1024",
    ],
    corpus = glob(["testdata/codec_cooperative_fuzz_corpus/*.bin"]),
    tags = [
        "resources:cpu:1",
        "resources:memory:1024",
    ],
    deps = [
        ":envelope_fuzz_cases",
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:collection",
        "//src/codec:cooperative",
        "//src/codec:crc32c_cooperative",
        "//src/codec:digest",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/codec:sha256",
        "//src/codec:sha256_cooperative",
        "//src/codec:staging_cooperative",
        "//src/runtime/testing:seastar_fuzz",
        "@openssl",
        "@seastar",
    ],
)

kwaque_cc_seastar_gtest(
    name = "codec_qualification_test",
    srcs = ["codec_qualification_test.cc"],
    memory = "256MiB",
    deps = [
        ":envelope_test_fixture",
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_builder",
        "//src/bytes:fragmented_buffer_parser",
        "//src/bytes:fragmented_buffer_test_support",
        "//src/codec:collection",
        "//src/codec:cooperative",
        "//src/codec:crc32c_cooperative",
        "//src/codec:digest",
        "//src/codec:envelope",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/codec:sha256_cooperative",
        "//src/codec:staging_cooperative",
    ],
)

kwaque_cc_seastar_gtest(
    name = "cooperative_test",
    srcs = ["cooperative_test.cc"],
    memory = "256MiB",
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/codec:cooperative",
        "//src/codec:error",
        "//src/codec:limits",
    ],
)

kwaque_cc_seastar_gtest(
    name = "integrity_cooperative_test",
    srcs = ["integrity_cooperative_test.cc"],
    memory = "256MiB",
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_test_support",
        "//src/codec:cooperative",
        "//src/codec:crc32c",
        "//src/codec:crc32c_cooperative",
        "//src/codec:digest",
        "//src/codec:error",
        "//src/codec:limits",
        "//src/codec:sha256",
        "//src/codec:sha256_cooperative",
    ],
)

kwaque_cc_seastar_gtest(
    name = "staging_cooperative_test",
    srcs = ["staging_cooperative_test.cc"],
    memory = "256MiB",
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_test_support",
        "//src/codec:cooperative",
        "//src/codec:staging_cooperative",
    ],
)

kwaque_cc_seastar_gtest(
    name = "collection_test",
    srcs = ["collection_test.cc"],
    memory = "256MiB",
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_builder",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:collection",
        "//src/codec:cooperative",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
    ],
)

kwaque_cc_seastar_gtest(
    name = "crc32c_test",
    srcs = ["crc32c_test.cc"],
    memory = "192MiB",
    deps = [
        "//src/base",
        "//src/bytes:fragmented_buffer",
        "//src/codec:crc32c",
        "@crc32c",
    ],
)

kwaque_cc_test(
    name = "digest_test",
    srcs = ["digest_test.cc"],
    deps = ["//src/codec:digest"],
)

kwaque_cc_binary(
    name = "integrity_cold_probe",
    testonly = True,
    srcs = ["integrity_cold_probe.cc"],
    deps = [
        "//src/codec:crc32c",
        "//src/codec:digest",
        "//src/codec:sha256",
        "@boost//:program_options",
        "@crc32c",
        "@seastar",
    ],
)

kwaque_py_native_test(
    name = "integrity_cold_test",
    timeout = "moderate",
    srcs = ["integrity_cold_test.py"],
    data = [":integrity_cold_probe"],
)

kwaque_cc_test(
    name = "error_test",
    srcs = ["error_test.cc"],
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/codec:error",
        "//src/codec:limits",
    ],
)

kwaque_cc_seastar_gtest(
    name = "integer_test",
    srcs = ["integer_test.cc"],
    memory = "192MiB",
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_builder",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:error",
        "//src/codec:integer",
    ],
)

kwaque_cc_test(
    name = "limits_test",
    srcs = ["limits_test.cc"],
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/codec:error",
        "//src/codec:limits",
    ],
)

kwaque_cc_seastar_gtest(
    name = "nullable_integer_test",
    srcs = ["nullable_integer_test.cc"],
    memory = "192MiB",
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_builder",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:error",
        "//src/codec:integer",
    ],
)

kwaque_cc_seastar_gtest(
    name = "staging_test",
    srcs = ["staging_test.cc"],
    memory = "192MiB",
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_builder",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
    ],
)

kwaque_cc_test(
    name = "sha256_test",
    srcs = ["sha256_test.cc"],
    deps = [
        "//src/codec:digest",
        "//src/codec:sha256",
    ],
)

kwaque_cc_seastar_gtest(
    name = "transaction_test",
    srcs = ["transaction_test.cc"],
    memory = "192MiB",
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_builder",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
    ],
)

kwaque_cc_library(
    name = "qualification_profile",
    testonly = True,
    hdrs = ["qualification_profile.h"],
    visibility = [
        "//src/compression/tests:__pkg__",
        "//src/model/tests:__pkg__",
        "//src/protocol/tests:__pkg__",
        "//src/storage/tests:__pkg__",
    ],
    deps = ["//src/base"],
)

kwaque_cc_library(
    name = "allocation_observer",
    testonly = True,
    srcs = ["allocation_observer.cc"],
    hdrs = ["allocation_observer.h"],
    visibility = [
        "//src/broker:__pkg__",
        "//src/model/tests:__pkg__",
        "//src/protocol/tests:__pkg__",
        "//src/storage/tests:__pkg__",
    ],
    deps = [
        "@openssl",
        "@seastar",
    ],
)

kwaque_cc_binary(
    name = "memory_qualification_probe",
    testonly = True,
    srcs = [
        "envelope_bench_fixture.h",
        "envelope_bench_reference.cc",
        "memory_qualification_probe.cc",
    ],
    linkopts = ALLOCATION_OBSERVER_LINKOPTS,
    visibility = [
        "//src/model/tests:__pkg__",
        "//src/protocol/tests:__pkg__",
        "//src/storage/tests:__pkg__",
    ],
    deps = [
        ":allocation_observer",
        ":codec_bench_reference",
        ":memory_qualification_support",
        ":qualification_profile",
        "//src/base",
        "//src/base:error",
        "//src/bytes:allocation_test_profile",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:collection",
        "//src/codec:cooperative",
        "//src/codec:crc32c",
        "//src/codec:crc32c_cooperative",
        "//src/codec:digest",
        "//src/codec:envelope",
        "//src/codec:error",
        "//src/codec:format_registry",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/codec:sha256",
        "//src/codec:sha256_cooperative",
        "//src/codec:staging_cooperative",
        "@boost//:program_options",
        "@crc32c",
        "@seastar",
    ],
)

kwaque_py_native_test(
    name = "memory_qualification_test",
    timeout = "moderate",
    srcs = ["memory_qualification_test.py"],
    data = [":memory_qualification_probe"],
    tags = [
        "resources:cpu:1",
        "resources:memory:256",
    ],
    deps = [":memory_qualification_driver"],
)

kwaque_cc_library(
    name = "memory_qualification_support",
    testonly = True,
    hdrs = ["memory_qualification_support.h"],
    visibility = [
        "//src/model/tests:__pkg__",
        "//src/protocol/tests:__pkg__",
        "//src/storage/tests:__pkg__",
    ],
    deps = [
        ":allocation_observer",
        ":qualification_profile",
        "//src/base",
        "//src/bytes:allocation_test_profile",
        "//src/bytes:fragmented_buffer",
        "@seastar",
    ],
)

py_library(
    name = "memory_qualification_driver",
    testonly = True,
    srcs = ["memory_qualification_driver.py"],
    visibility = [
        "//src/model/tests:__pkg__",
        "//src/protocol/tests:__pkg__",
        "//src/storage/tests:__pkg__",
    ],
)

kwaque_cc_library(
    name = "prepared_abort_source",
    testonly = True,
    hdrs = ["prepared_abort_source.h"],
    visibility = [
        "//src/model/tests:__pkg__",
        "//src/protocol/tests:__pkg__",
        "//src/storage/tests:__pkg__",
    ],
    deps = ["@seastar"],
)
