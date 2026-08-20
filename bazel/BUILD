load("@rules_python//python:defs.bzl", "py_binary", "py_test")
load("//bazel:build.bzl", "kwaque_cc_binary")
load("//bazel:test.bzl", "kwaque_cc_test")

exports_files(
    ["fuzz_test_wrapper.py"],
    visibility = ["//visibility:public"],
)

py_binary(
    name = "stamp_template",
    srcs = ["stamp_template.py"],
    main = "stamp_template.py",
)

py_test(
    name = "fuzz_test_wrapper_test",
    size = "small",
    srcs = [
        "fuzz_test_wrapper.py",
        "fuzz_test_wrapper_test.py",
    ],
    main = "fuzz_test_wrapper_test.py",
)

config_setting(
    name = "debug_build",
    values = {"compilation_mode": "dbg"},
    visibility = ["//visibility:public"],
)

config_setting(
    name = "optimized_build",
    values = {"compilation_mode": "opt"},
    visibility = ["//visibility:public"],
)

config_setting(
    name = "fuzz_build",
    define_values = {"kwaque_fuzz": "1"},
    visibility = ["//visibility:public"],
)

kwaque_cc_binary(
    name = "kwaque_bootstrap",
    srcs = ["toolchain_probe.cc"],
    visibility = ["//visibility:public"],
)

kwaque_cc_test(
    name = "toolchain_probe_test",
    size = "small",
    srcs = ["toolchain_probe.cc"],
)
