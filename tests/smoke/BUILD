load("@rules_python//python:defs.bzl", "py_library")
load("//bazel:test.bzl", "kwaque_py_native_test")

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
    visibility = [
        "//bazel/packaging:__pkg__",
        "//src/broker:__pkg__",
    ],
    deps = ["//bazel:native_test_environment"],
)

kwaque_py_native_test(
    name = "resource_memory_smoke_test",
    timeout = "moderate",
    srcs = ["resource_memory_smoke_test.py"],
    args = _COMMON_ARGS,
    data = _COMMON_DATA,
    main = "resource_memory_smoke_test.py",
    tags = [
        "requires-network",
        "resources:cpu:2",
        "resources:memory:128",
        "smoke",
    ],
    deps = [":broker_test_support"],
)

# Each broker listens on its own leased loopback address, so these tests share
# the host network without port races and need not run exclusively.
[
    kwaque_py_native_test(
        name = test_name,
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
        "default_config_smoke_test",
        "pid_file_smoke_test",
        "sigint_smoke_test",
        "start_rollback_smoke_test",
        "startup_failure_smoke_test",
    ]
]
