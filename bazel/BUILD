load("@bazel_skylib//rules:common_settings.bzl", "string_flag")
load("@rules_python//python:defs.bzl", "py_binary", "py_library", "py_test")
load("//bazel:build.bzl", "kwaque_cc_binary", "kwaque_cc_library")
load("//bazel:test.bzl", "kwaque_cc_test")

exports_files(
    [
        "benchmark_smoke.py",
        "fuzz_signal_canary_test.py",
        "fuzz_test_wrapper.py",
    ],
    visibility = ["//visibility:public"],
)

py_binary(
    name = "stamp_template",
    srcs = ["stamp_template.py"],
    main = "stamp_template.py",
)

py_test(
    name = "stamp_template_test",
    size = "small",
    srcs = [
        "stamp_template.py",
        "stamp_template_test.py",
    ],
    main = "stamp_template_test.py",
)

# Makes the sanitizer paths in a test's environment absolute, for harnesses
# that start a native process outside the test's working directory.
py_library(
    name = "native_test_environment",
    srcs = ["native_test_environment.py"],
    visibility = ["//visibility:public"],
)

py_test(
    name = "fuzz_test_wrapper_test",
    size = "small",
    srcs = [
        "fuzz_test_wrapper.py",
        "fuzz_test_wrapper_test.py",
    ],
    main = "fuzz_test_wrapper_test.py",
    deps = [":native_test_environment"],
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
    name = "system_allocator",
    flag_values = {"@seastar//:system_allocator": "true"},
    visibility = ["//visibility:public"],
)

config_setting(
    name = "stamped",
    values = {"stamp": "1"},
    visibility = ["//visibility:public"],
)

config_setting(
    name = "fuzz_build",
    define_values = {"kwaque_fuzz": "1"},
    visibility = ["//visibility:public"],
)

# Seastar reactor backend used by reactor tests and benchmarks.
string_flag(
    name = "reactor_backend",
    build_setting_default = "epoll",
    values = [
        "epoll",
        "io_uring",
        "linux-aio",
    ],
)

config_setting(
    name = "reactor_backend_io_uring",
    flag_values = {":reactor_backend": "io_uring"},
    visibility = ["//visibility:public"],
)

config_setting(
    name = "reactor_backend_linux_aio",
    flag_values = {":reactor_backend": "linux-aio"},
    visibility = ["//visibility:public"],
)

kwaque_cc_library(
    name = "fuzz_corpus_replay",
    testonly = True,
    srcs = ["fuzz_corpus_replay.cc"],
    visibility = ["//visibility:public"],
)

kwaque_cc_library(
    name = "benchmark_policy",
    testonly = True,
    srcs = ["benchmark_policy.cc"],
    visibility = ["//visibility:public"],
    deps = [
        "@seastar",
        "@seastar//:benchmark",
    ],
    alwayslink = True,
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
