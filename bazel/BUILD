load("@rules_cc//cc:cc_binary.bzl", "cc_binary")
load("@rules_cc//cc:cc_test.bzl", "cc_test")

cc_binary(
    name = "kwaque_bootstrap",
    srcs = ["toolchain_probe.cc"],
    visibility = ["//visibility:public"],
)

cc_test(
    name = "toolchain_probe_test",
    size = "small",
    srcs = ["toolchain_probe.cc"],
)
