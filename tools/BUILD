load("@rules_python//python:defs.bzl", "py_binary", "py_test")

package(default_visibility = ["//visibility:private"])

py_binary(
    name = "check_bazel_package_cycles",
    srcs = ["check_bazel_package_cycles.py"],
    main = "check_bazel_package_cycles.py",
    visibility = ["//visibility:public"],
)

py_test(
    name = "check_bazel_package_cycles_test",
    size = "small",
    srcs = [
        "check_bazel_package_cycles.py",
        "check_bazel_package_cycles_test.py",
    ],
    main = "check_bazel_package_cycles_test.py",
)
