load("@rules_cc//cc:cc_library.bzl", "cc_library")

filegroup(
    name = "license",
    srcs = ["LICENSE"],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "unordered_dense",
    hdrs = ["include/ankerl/unordered_dense.h"],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
)
