load("//bazel:build.bzl", "kwaque_cc_binary")
load("//bazel:test.bzl", "kwaque_cc_benchmark", "kwaque_cc_fuzz_test", "kwaque_cc_seastar_gtest", "kwaque_cc_test", "kwaque_py_native_test")

package(default_visibility = ["//visibility:private"])

kwaque_cc_benchmark(
    name = "codec_bench",
    srcs = [
        "codec_bench.cc",
        "codec_bench_fixture.h",
        "codec_bench_reference.cc",
        "crc32c_bench.cc",
    ],
    memory = "512MiB",
    deps = [
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
        "//src/codec:error",
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
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_builder",
        "//src/bytes:fragmented_buffer_test_support",
        "//src/codec:collection",
        "//src/codec:cooperative",
        "//src/codec:crc32c_cooperative",
        "//src/codec:digest",
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
