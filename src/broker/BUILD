load("@rules_python//python:defs.bzl", "py_test")
load("//bazel:build.bzl", "kwaque_cc_binary", "kwaque_cc_library")
load("//bazel:launcher.bzl", "kwaque_launcher")
load("//bazel:test.bzl", "kwaque_cc_seastar_test", "kwaque_cc_test", "kwaque_py_native_test")

package(default_visibility = ["//visibility:private"])

kwaque_cc_library(
    name = "startup_policy",
    srcs = ["startup_policy.cc"],
    hdrs = ["startup_policy.h"],
    deps = [
        "//src/admin:admin_limits",
        "//src/base:build_info",
        "//src/config:bootstrap_config",
        "//src/resource:resource_config",
        "@boost//:any",
        "@boost//:program_options",
        "@openssl",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "shutdown_watchdog",
    srcs = ["shutdown_watchdog.cc"],
    hdrs = ["shutdown_watchdog.h"],
    deps = [
        "//src/base:logging",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "crash_limiter",
    srcs = ["crash_limiter.cc"],
    hdrs = ["crash_limiter.h"],
    deps = [
        ":startup_policy",
        "//src/base:logging",
        "@crc32c",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "crash_recorder_writer",
    srcs = ["crash_recorder_writer.cc"],
    hdrs = ["crash_recorder_writer.h"],
    deps = [
        "@crc32c",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "crash_recorder",
    srcs = ["crash_recorder.cc"],
    hdrs = ["crash_recorder.h"],
    deps = [
        ":crash_recorder_writer",
        "//src/base:build_info",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "service_lifecycle",
    srcs = ["service_lifecycle.cc"],
    hdrs = ["service_lifecycle.h"],
    deps = [
        ":shutdown_watchdog",
        "//src/base:invariant",
        "//src/runtime:shard_affinity",
        "@seastar",
    ],
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
    name = "host_checks",
    srcs = ["host_checks.cc"],
    hdrs = ["host_checks.h"],
    deps = ["@seastar"],
)

kwaque_cc_seastar_test(
    name = "host_checks_test",
    srcs = ["host_checks_test.cc"],
    deps = [":host_checks"],
)

kwaque_cc_library(
    name = "application_internal",
    srcs = [
        "application_config.cc",
        "application_services.cc",
        "application_start.cc",
    ],
    hdrs = ["application_internal.h"],
    deps = [
        ":crash_limiter",
        ":crash_recorder",
        ":data_directory",
        ":host_checks",
        ":pid_file",
        ":service_lifecycle",
        ":shutdown_watchdog",
        ":startup_policy",
        "//src/admin:admin_limits",
        "//src/admin:admin_server",
        "//src/base",
        "//src/base:build_info",
        "//src/base:invariant",
        "//src/base:logging",
        "//src/config:bootstrap_config",
        "//src/observability:event_identity",
        "//src/resource:resource_config",
        "//src/resource:resource_registry",
        "//src/runtime:stop_signal",
        "//src/runtime/production:environment",
        "@boost//:program_options",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "application",
    srcs = ["application.cc"],
    hdrs = ["application.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":application_internal",
        ":startup_policy",
        "//src/base:build_info",
        "@boost//:program_options",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "application_test_support",
    testonly = True,
    hdrs = ["application_test_support.h"],
    deps = [
        ":application_internal",
        ":startup_policy",
        "//src/config:bootstrap_config",
    ],
)

kwaque_cc_binary(
    name = "kwaque_native",
    srcs = ["main.cc"],
    data = ["//conf:kwaque_config"],
    visibility = ["//bazel/packaging:__pkg__"],
    deps = [":application"],
)

kwaque_launcher(
    name = "kwaque",
    srcs = [
        "cpu_features.c",
        "cpu_features.h",
        "launcher.c",
    ],
    data = [":kwaque_native"],
    visibility = ["//visibility:public"],
)

kwaque_launcher(
    name = "launcher_fixture",
    testonly = True,
    srcs = [
        "cpu_features.h",
        "cpu_features_fixture.c",
        "launcher.c",
    ],
)

py_test(
    name = "launcher_test",
    size = "small",
    srcs = ["launcher_test.py"],
    args = [
        "$(rootpath :launcher_fixture)",
        "$(rootpath :kwaque)",
    ],
    data = [
        ":kwaque",
        ":launcher_fixture",
    ],
    main = "launcher_test.py",
)

kwaque_cc_test(
    name = "startup_policy_test",
    srcs = ["startup_policy_test.cc"],
    deps = [
        ":startup_policy",
        "//src/base",
        "//src/base:build_info",
        "@boost//:program_options",
        "@seastar",
    ],
)

kwaque_cc_binary(
    name = "failure_policy_probe",
    testonly = True,
    srcs = ["failure_policy_test.cc"],
    deps = [
        ":startup_policy",
        "//src/base",
        "//src/base:error",
        "//src/base:invariant",
        "//src/resource:bounded_work_queue",
        "//src/runtime:shard_affinity",
        "@boost//:program_options",
        "@seastar",
    ],
)

kwaque_py_native_test(
    name = "failure_policy_test",
    timeout = "moderate",
    srcs = ["failure_policy_test.py"],
    data = [":failure_policy_probe"],
)

kwaque_cc_seastar_test(
    name = "startup_configuration_test",
    srcs = ["startup_configuration_test.cc"],
    cpu = 2,
    memory = "256MiB",
    deps = [
        ":application_internal",
        ":application_test_support",
        ":startup_policy",
        "//src/admin:admin_server",
        "@boost//:program_options",
    ],
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
    name = "broker_headroom_test",
    timeout = "moderate",
    srcs = ["broker_headroom_test.cc"],
    cpu = 1,
    memory = "192MiB",
    deps = [
        ":application_internal",
        ":application_test_support",
        "//src/admin:admin_limits",
        "//src/config:bootstrap_config",
        "//src/resource:resource_config",
        "//src/resource:resource_manager",
        "//src/resource:workload_class",
        "//src/runtime:cross_shard",
        "//src/runtime/production:environment",
    ],
)

kwaque_cc_seastar_test(
    name = "application_lifecycle_test",
    srcs = ["application_lifecycle_test.cc"],
    cpu = 1,
    memory = "128MiB",
    deps = [
        ":application",
        ":application_internal",
        ":application_test_support",
        ":service_lifecycle",
    ],
)

kwaque_cc_seastar_test(
    name = "metrics_lifecycle_test",
    srcs = ["metrics_lifecycle_test.cc"],
    cpu = 2,
    memory = "256MiB",
    deps = [
        "//src/admin:admin_server",
        "//src/base",
        "//src/base:metric_schema",
        "//src/observability:event_identity",
        "//src/resource:resource_config",
        "//src/resource:resource_registry",
        "//src/runtime/production:environment",
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
        "resources:cpu:4",
        "resources:memory:384",
    ],
)

kwaque_cc_seastar_test(
    name = "shutdown_watchdog_test",
    srcs = ["shutdown_watchdog_test.cc"],
    deps = [
        ":service_lifecycle",
        ":shutdown_watchdog",
    ],
)

kwaque_cc_seastar_test(
    name = "crash_limiter_test",
    srcs = ["crash_limiter_test.cc"],
    deps = [
        ":crash_limiter",
        ":startup_policy",
        "@crc32c",
    ],
)

kwaque_cc_test(
    name = "crash_recorder_writer_test",
    srcs = ["crash_recorder_writer_test.cc"],
    deps = [
        ":crash_recorder_writer",
        "@crc32c",
    ],
)

kwaque_cc_seastar_test(
    name = "crash_recorder_test",
    srcs = ["crash_recorder_test.cc"],
    deps = [
        ":crash_recorder",
        ":crash_recorder_writer",
        "@crc32c",
    ],
)

kwaque_cc_seastar_test(
    name = "application_shutdown_test",
    srcs = ["application_shutdown_test.cc"],
    cpu = 2,
    memory = "384MiB",
    deps = [
        ":application_internal",
        ":application_test_support",
        ":pid_file",
        "//src/runtime/production:environment",
    ],
)

kwaque_cc_binary(
    name = "crash_recorder_probe",
    testonly = True,
    srcs = ["crash_recorder_probe.cc"],
    deps = [
        ":crash_recorder",
        ":startup_policy",
        "//src/base:invariant",
        "@boost//:program_options",
        "@seastar",
    ],
)

kwaque_py_native_test(
    name = "crash_recorder_process_test",
    timeout = "moderate",
    srcs = ["crash_recorder_test.py"],
    data = [":crash_recorder_probe"],
    main = "crash_recorder_test.py",
)
