load("//bazel:build.bzl", "kwaque_cc_binary")
load("@rules_python//python:defs.bzl", "py_test")

package(default_visibility = ["//visibility:private"])

kwaque_cc_binary(
    name = "kwaque",
    srcs = ["main.cc"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/base:build_info",
        "//src/base:logging",
        "@seastar",
    ],
)

py_test(
    name = "version_cli_test",
    size = "small",
    srcs = ["version_cli_test.py"],
    args = ["$(rootpath :kwaque)"],
    data = [":kwaque"],
    main = "version_cli_test.py",
)
