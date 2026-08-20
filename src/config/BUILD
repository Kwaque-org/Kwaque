load("//bazel:build.bzl", "kwaque_cc_library")
load("//bazel:test.bzl", "kwaque_cc_fuzz_test", "kwaque_cc_test")

package(default_visibility = ["//visibility:private"])

kwaque_cc_library(
    name = "bootstrap_config",
    srcs = ["bootstrap_config.cc"],
    hdrs = ["bootstrap_config.h"],
    visibility = ["//visibility:public"],
    deps = ["@yaml-cpp"],
)

kwaque_cc_test(
    name = "bootstrap_config_test",
    srcs = ["bootstrap_config_test.cc"],
    data = ["//conf:kwaque_config"],
    deps = [":bootstrap_config"],
)

kwaque_cc_fuzz_test(
    name = "bootstrap_config_fuzz",
    srcs = ["bootstrap_config_fuzz.cc"],
    args = [
        "-dict=$(rootpath :bootstrap_config_fuzz.dict)",
        "-max_len=4096",
        "-rss_limit_mb=512",
    ],
    corpus = [
        "testdata/bootstrap_config_fuzz_corpus/complete.yaml",
        "testdata/bootstrap_config_fuzz_corpus/duplicate_key.yaml",
        "testdata/bootstrap_config_fuzz_corpus/minimal.yaml",
    ],
    data = ["bootstrap_config_fuzz.dict"],
    tags = [
        "resources:cpu:1",
        "resources:memory:512",
    ],
    deps = [":bootstrap_config"],
)
