load("//bazel:build.bzl", "kwaque_cc_binary")
load("//bazel:test.bzl", "kwaque_cc_benchmark", "kwaque_cc_fuzz_test", "kwaque_cc_seastar_gtest", "kwaque_cc_test", "kwaque_py_native_test")
load(":visibility_test.bzl", "visibility_policy_test")

package(default_visibility = ["//visibility:private"])

kwaque_cc_test(
    name = "pure_cpp_test",
    srcs = ["pure_cpp_test.cc"],
)

kwaque_cc_seastar_gtest(
    name = "seastar_gtest",
    srcs = ["seastar_gtest.cc"],
    cpu = 2,
    memory = "192MiB",
    deps = ["//src/base"],
)

kwaque_cc_benchmark(
    name = "empty_benchmark",
    srcs = ["empty_benchmark.cc"],
)

kwaque_py_native_test(
    name = "benchmark_list_test",
    timeout = "moderate",
    srcs = ["benchmark_list_test.py"],
    args = ["$(rootpath :empty_benchmark)"],
    data = [":empty_benchmark"],
    main = "benchmark_list_test.py",
    tags = [
        "benchmark",
        "resources:cpu:1",
        "resources:memory:128",
    ],
)

kwaque_cc_fuzz_test(
    name = "bounded_fuzz_test",
    srcs = ["bounded_fuzz_test.cc"],
    deps = ["//src/base"],
)

kwaque_cc_binary(
    name = "warning_probe",
    srcs = ["warning_probe.cc"],
    tags = ["manual"],
)

kwaque_cc_test(
    name = "sanitizer_failure_probe",
    srcs = ["sanitizer_failure_probe.cc"],
    tags = ["manual"],
)

visibility_policy_test(name = "visibility_policy_test")
