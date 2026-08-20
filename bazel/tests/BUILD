load("//bazel:build.bzl", "kwaque_cc_binary")
load("//bazel:test.bzl", "kwaque_cc_benchmark", "kwaque_cc_fuzz_test", "kwaque_cc_seastar_gtest", "kwaque_cc_test")
load(":visibility_test.bzl", "visibility_policy_test")

package(default_visibility = ["//visibility:private"])

kwaque_cc_test(
    name = "pure_cpp_test",
    srcs = ["pure_cpp_test.cc"],
)

kwaque_cc_test(
    name = "deterministic_seed_test",
    srcs = ["deterministic_seed_test.cc"],
)

kwaque_cc_seastar_gtest(
    name = "seastar_gtest",
    srcs = ["seastar_gtest.cc"],
    cpu = 2,
    memory = "192MiB",
)

kwaque_cc_benchmark(
    name = "empty_benchmark",
    srcs = ["empty_benchmark.cc"],
)

kwaque_cc_fuzz_test(
    name = "bounded_fuzz_test",
    srcs = ["bounded_fuzz_test.cc"],
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
