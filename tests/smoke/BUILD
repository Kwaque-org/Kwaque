load("@rules_python//python:defs.bzl", "py_library", "py_test")

package(default_visibility = ["//visibility:private"])

_COMMON_DATA = [
    "//conf:kwaque_config",
    "//src/broker:kwaque",
]

_COMMON_ARGS = [
    "$(rootpath //src/broker:kwaque)",
    "$(rootpath //conf:kwaque_config)",
]

_COMMON_TAGS = [
    "requires-network",
    "resources:cpu:1",
    "resources:memory:128",
    "smoke",
]

py_library(
    name = "broker_test_support",
    srcs = ["broker_test_support.py"],
)

[
    py_test(
        name = test_name,
        size = "small",
        timeout = "short",
        srcs = [test_name + ".py"],
        args = _COMMON_ARGS,
        data = _COMMON_DATA,
        main = test_name + ".py",
        tags = _COMMON_TAGS,
        deps = [":broker_test_support"],
    )
    for test_name in [
        "broker_smoke_test",
        "pid_file_smoke_test",
        "sigint_smoke_test",
        "startup_failure_smoke_test",
    ]
]
