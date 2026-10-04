load("//bazel:build.bzl", "kwaque_cc_library")
load("//bazel:test.bzl", "kwaque_cc_fuzz_test", "kwaque_cc_seastar_test", "kwaque_cc_test")

package(default_visibility = ["//visibility:private"])

kwaque_cc_library(
    name = "bootstrap_config",
    srcs = ["bootstrap_config.cc"],
    hdrs = ["bootstrap_config.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/base",
        "@yaml-cpp",
    ],
)

kwaque_cc_test(
    name = "bootstrap_config_test",
    srcs = ["bootstrap_config_test.cc"],
    data = [
        "//conf:kwaque_config",
        "//conf:kwaque_production_config",
    ],
    deps = [":bootstrap_config"],
)

kwaque_cc_seastar_test(
    name = "bootstrap_config_allocation_test",
    srcs = ["bootstrap_config_allocation_test.cc"],
    cpu = 1,
    memory = "128MiB",
    deps = [":bootstrap_config"],
)

kwaque_cc_fuzz_test(
    name = "bootstrap_config_fuzz",
    srcs = ["bootstrap_config_fuzz.cc"],
    # One byte past the production input cap, so the size check is reachable.
    # A single allocation never needs more than the cap's few copies.
    args = [
        "-dict=$(rootpath :bootstrap_config_fuzz.dict)",
        "-max_len=65537",
        "-malloc_limit_mb=8",
        "-rss_limit_mb=512",
    ],
    corpus = [
        "testdata/bootstrap_config_fuzz_corpus/aliases.yaml",
        "testdata/bootstrap_config_fuzz_corpus/complete.yaml",
        "testdata/bootstrap_config_fuzz_corpus/duplicate_key.yaml",
        "testdata/bootstrap_config_fuzz_corpus/minimal.yaml",
        "testdata/bootstrap_config_fuzz_corpus/nested.yaml",
        "testdata/bootstrap_config_fuzz_corpus/second_document.yaml",
        "testdata/bootstrap_config_fuzz_corpus/production.yaml",
    ],
    data = ["bootstrap_config_fuzz.dict"],
    tags = [
        "fuzz-campaign",
        "resources:cpu:1",
        "resources:memory:512",
    ],
    deps = [":bootstrap_config"],
)
