load("@rules_python//python:defs.bzl", "py_test")
load("//bazel:build.bzl", "kwaque_cc_binary", "kwaque_cc_library")
load("//bazel:test.bzl", "kwaque_cc_seastar_test", "kwaque_cc_test")

package(default_visibility = ["//visibility:private"])

kwaque_cc_library(
    name = "service_lifecycle",
    srcs = ["service_lifecycle.cc"],
    hdrs = ["service_lifecycle.h"],
    deps = ["@seastar"],
)

kwaque_cc_library(
    name = "pid_file",
    srcs = ["pid_file.cc"],
    hdrs = ["pid_file.h"],
)

kwaque_cc_library(
    name = "data_directory",
    srcs = ["data_directory.cc"],
    hdrs = ["data_directory.h"],
    deps = ["@seastar"],
)

kwaque_cc_library(
    name = "application",
    srcs = [
        "application.cc",
        "application_config.cc",
        "application_services.cc",
        "application_start.cc",
    ],
    hdrs = [
        "application.h",
        "application_internal.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":data_directory",
        ":pid_file",
        ":service_lifecycle",
        "//src/admin:admin_server",
        "//src/base:build_info",
        "//src/base:logging",
        "//src/config:bootstrap_config",
        "//src/runtime:runtime_service",
        "//src/runtime:stop_signal",
        "@boost//:program_options",
        "@seastar",
    ],
)

kwaque_cc_binary(
    name = "kwaque",
    srcs = ["main.cc"],
    data = ["//conf:kwaque_config"],
    visibility = ["//visibility:public"],
    deps = [":application"],
)

kwaque_cc_test(
    name = "application_header_test",
    srcs = ["application_header_test.cc"],
    deps = [":application"],
)

kwaque_cc_test(
    name = "pid_file_test",
    srcs = ["pid_file_test.cc"],
    deps = [":pid_file"],
)

kwaque_cc_seastar_test(
    name = "application_lifecycle_test",
    srcs = ["application_lifecycle_test.cc"],
    cpu = 1,
    memory = "128MiB",
    deps = [
        ":application",
        ":service_lifecycle",
    ],
)

kwaque_cc_seastar_test(
    name = "data_directory_test",
    srcs = ["data_directory_test.cc"],
    cpu = 1,
    memory = "128MiB",
    deps = [":data_directory"],
)

py_test(
    name = "version_cli_test",
    size = "small",
    srcs = ["version_cli_test.py"],
    args = ["$(rootpath :kwaque)"],
    data = [":kwaque"],
    main = "version_cli_test.py",
)

py_test(
    name = "config_cli_test",
    size = "small",
    timeout = "moderate",
    srcs = ["config_cli_test.py"],
    args = [
        "$(rootpath :kwaque)",
        "$(rootpath //conf:kwaque_config)",
        "$(rootpath testdata/alternate.yaml)",
    ],
    data = [
        "testdata/alternate.yaml",
        ":kwaque",
        "//conf:kwaque_config",
    ],
    main = "config_cli_test.py",
    tags = [
        "requires-network",
        "resources:cpu:2",
        "resources:memory:128",
    ],
)
