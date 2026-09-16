load("//bazel:build.bzl", "kwaque_cc_binary", "kwaque_cc_library")
load("//bazel:test.bzl", "kwaque_cc_benchmark", "kwaque_cc_fuzz_test", "kwaque_cc_seastar_gtest", "kwaque_cc_test", "kwaque_py_native_test")

package(default_visibility = ["//visibility:private"])

kwaque_cc_library(
    name = "envelope_test_fixture",
    testonly = True,
    hdrs = ["envelope_decode_test_support.h"],
    visibility = ["//src/protocol/tests:__pkg__"],
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
