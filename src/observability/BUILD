load("//bazel:build.bzl", "kwaque_cc_library")
load("//bazel:test.bzl", "kwaque_cc_benchmark", "kwaque_cc_seastar_test")

package(default_visibility = ["//visibility:private"])

filegroup(
    name = "package_boundary",
    srcs = ["README.md"],
)

kwaque_cc_library(
    name = "event",
    srcs = ["event.cc"],
    hdrs = ["event.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/resource:workload_class",
        "//src/runtime:error",
        "//src/runtime:shard_affinity",
        "//src/runtime:time",
    ],
)

kwaque_cc_library(
    name = "event_codec",
    srcs = ["event_codec.cc"],
    hdrs = ["event_codec.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":event",
        "//src/runtime:error",
    ],
)

kwaque_cc_library(
    name = "event_identity",
    srcs = ["event_identity.cc"],
    hdrs = ["event_identity.h"],
    visibility = ["//visibility:public"],
    deps = ["//src/runtime:error"],
)

kwaque_cc_library(
    name = "event_sequence",
    srcs = ["event_sequence.cc"],
    hdrs = ["event_sequence.h"],
    visibility = [
        "//src/observability:__pkg__",
        "//src/simulation:__pkg__",
    ],
    deps = [
        ":event",
        ":event_identity",
        "//src/base:invariant",
        "//src/runtime:error",
        "//src/runtime:shard_affinity",
    ],
)

kwaque_cc_library(
    name = "event_sequence_test_access",
    testonly = True,
    hdrs = ["testing/event_sequence_test_access.h"],
    visibility = ["//src/observability:__pkg__"],
    deps = [":event_sequence"],
)

kwaque_cc_library(
    name = "event_log",
    srcs = ["event_log.cc"],
    hdrs = ["event_log.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":event",
        ":event_codec",
        ":event_identity",
        "//src/base",
        "//src/base:invariant",
        "//src/runtime:error",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "event_sink_concept",
    hdrs = ["event_sink_concept.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":event",
        "//src/runtime:error",
    ],
)

kwaque_cc_library(
    name = "event_sink",
    srcs = ["event_sink.cc"],
    hdrs = ["event_sink.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":event",
        ":event_sequence",
        ":event_sink_concept",
        "//src/runtime:error",
        "//src/runtime:shard_affinity",
        "@fmt",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "capture_event_sink",
    testonly = True,
    hdrs = ["testing/capture_event_sink.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":event_log",
        ":event_sequence",
        ":event_sink_concept",
        "//src/runtime:error",
        "//src/runtime:shard_affinity",
    ],
)

kwaque_cc_seastar_test(
    name = "event_test",
    srcs = ["event_test.cc"],
    cpu = 1,
    memory = "128MiB",
    deps = [":event"],
)

kwaque_cc_seastar_test(
    name = "event_codec_test",
    srcs = ["event_codec_test.cc"],
    cpu = 1,
    memory = "192MiB",
    deps = [
        ":event",
        ":event_codec",
        ":event_log",
        ":event_sequence",
        ":event_sequence_test_access",
        "//src/base",
    ],
)

kwaque_cc_seastar_test(
    name = "event_sink_test",
    srcs = ["event_sink_test.cc"],
    cpu = 1,
    memory = "192MiB",
    deps = [
        ":capture_event_sink",
        ":event",
        ":event_log",
        ":event_sequence",
        ":event_sink",
    ],
)

kwaque_cc_seastar_test(
    name = "event_sequence_test",
    srcs = ["event_sequence_test.cc"],
    cpu = 2,
    memory = "192MiB",
    deps = [
        ":capture_event_sink",
        ":event",
        ":event_log",
        ":event_sequence",
        ":event_sequence_test_access",
    ],
)

kwaque_cc_seastar_test(
    name = "metric_group_lifecycle_test",
    srcs = ["metric_group_lifecycle_test.cc"],
    cpu = 1,
    memory = "128MiB",
    deps = ["//src/runtime:shard_affinity"],
)

kwaque_cc_benchmark(
    name = "event_bench",
    srcs = ["event_bench.cc"],
    memory = "192MiB",
    deps = [
        ":capture_event_sink",
        ":event",
        ":event_identity",
        ":event_log",
        ":event_sequence",
        ":event_sink",
        "//src/resource:workload_class",
        "//src/runtime:time",
        "@seastar",
    ],
)
