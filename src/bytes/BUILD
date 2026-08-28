load("//bazel:build.bzl", "kwaque_cc_library")
load(
    "//bazel:test.bzl",
    "kwaque_cc_benchmark",
    "kwaque_cc_fuzz_test",
    "kwaque_cc_seastar_gtest",
)

package(default_visibility = ["//visibility:private"])

filegroup(
    name = "package_boundary",
    srcs = ["README.md"],
)

kwaque_cc_library(
    name = "fragmented_buffer",
    srcs = ["fragmented_buffer.cc"],
    hdrs = ["fragmented_buffer.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/base",
        "//src/base:error",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "fragmented_buffer_builder",
    srcs = ["fragmented_buffer_builder.cc"],
    hdrs = ["fragmented_buffer_builder.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":fragmented_buffer",
        "//src/base",
        "//src/base:error",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "fragmented_buffer_parser",
    srcs = ["fragmented_buffer_parser.cc"],
    hdrs = ["fragmented_buffer_parser.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":fragmented_buffer",
        "//src/base",
        "//src/base:error",
    ],
)

kwaque_cc_seastar_gtest(
    name = "fragmented_buffer_test",
    srcs = ["fragmented_buffer_test.cc"],
    memory = "192MiB",
    deps = [
        ":fragmented_buffer",
        ":fragmented_buffer_builder",
        "//src/base",
        "//src/base:error",
    ],
)

kwaque_cc_seastar_gtest(
    name = "fragmented_buffer_parser_test",
    srcs = ["fragmented_buffer_parser_test.cc"],
    memory = "192MiB",
    deps = [
        ":fragmented_buffer",
        ":fragmented_buffer_parser",
        "//src/base",
        "//src/base:error",
    ],
)

kwaque_cc_fuzz_test(
    name = "fragmented_buffer_fuzz",
    srcs = ["fragmented_buffer_fuzz.cc"],
    corpus = [
        "testdata/fragmented_buffer_fuzz_corpus/append_parse.bin",
        "testdata/fragmented_buffer_fuzz_corpus/empty.bin",
        "testdata/fragmented_buffer_fuzz_corpus/trim_share.bin",
    ],
    deps = [
        ":fragmented_buffer",
        ":fragmented_buffer_builder",
        ":fragmented_buffer_parser",
        "//src/base",
        "@seastar",
    ],
)

kwaque_cc_benchmark(
    name = "fragmented_buffer_bench",
    srcs = ["fragmented_buffer_bench.cc"],
    memory = "256MiB",
    deps = [
        ":fragmented_buffer",
        ":fragmented_buffer_builder",
        ":fragmented_buffer_parser",
        "//src/base",
        "@seastar",
    ],
)
