load("//bazel:build.bzl", "kwaque_cc_library")

package(default_visibility = ["//visibility:private"])

kwaque_cc_library(
    name = "identity",
    hdrs = ["identity.h"],
    visibility = [
        "//src/model/tests:__pkg__",
        "//src/protocol:__pkg__",
        "//src/protocol/tests:__pkg__",
        "//src/storage:__pkg__",
        "//src/storage/tests:__pkg__",
    ],
    deps = [
        "//src/base",
        "//src/base:error",
        "@boost//:uuid_value",
    ],
)

kwaque_cc_library(
    name = "epoch",
    hdrs = ["epoch.h"],
    visibility = [
        "//src/model/tests:__pkg__",
        "//src/protocol:__pkg__",
        "//src/protocol/tests:__pkg__",
        "//src/storage:__pkg__",
        "//src/storage/tests:__pkg__",
    ],
    deps = [
        "//src/base",
        "//src/base:error",
    ],
)

kwaque_cc_library(
    name = "transport_identity",
    hdrs = ["transport_identity.h"],
    visibility = [
        "//src/model/tests:__pkg__",
        "//src/protocol:__pkg__",
        "//src/protocol/tests:__pkg__",
    ],
)

kwaque_cc_library(
    name = "batch_identity",
    hdrs = ["batch_identity.h"],
    visibility = [
        "//src/model/tests:__pkg__",
        "//src/protocol:__pkg__",
        "//src/protocol/tests:__pkg__",
        "//src/storage:__pkg__",
        "//src/storage/tests:__pkg__",
    ],
    deps = [
        ":epoch",
        ":identity",
        "//src/base",
        "//src/base:error",
    ],
)

kwaque_cc_library(
    name = "position",
    hdrs = ["position.h"],
    visibility = [
        "//src/model/tests:__pkg__",
        "//src/protocol:__pkg__",
        "//src/protocol/tests:__pkg__",
        "//src/storage:__pkg__",
        "//src/storage/tests:__pkg__",
    ],
    deps = [
        ":epoch",
        ":identity",
        "//src/base",
        "//src/base:error",
        "//src/runtime:file_position",
        "//src/runtime:time",
    ],
)

kwaque_cc_library(
    name = "keyspace",
    srcs = ["keyspace.cc"],
    hdrs = ["keyspace.h"],
    visibility = [
        "//src/model/tests:__pkg__",
        "//src/protocol:__pkg__",
        "//src/protocol/tests:__pkg__",
        "//src/storage:__pkg__",
        "//src/storage/tests:__pkg__",
    ],
    deps = [
        "//src/base",
        "//src/base:error",
    ],
)

kwaque_cc_library(
    name = "segment_state",
    hdrs = ["segment_state.h"],
    visibility = [
        "//src/model/tests:__pkg__",
        "//src/protocol:__pkg__",
        "//src/protocol/tests:__pkg__",
        "//src/storage:__pkg__",
        "//src/storage/tests:__pkg__",
    ],
    deps = [
        "//src/base",
        "//src/base:error",
    ],
)

kwaque_cc_library(
    name = "topic_policy",
    hdrs = ["topic_policy.h"],
    visibility = [
        "//src/model/tests:__pkg__",
        "//src/protocol:__pkg__",
        "//src/protocol/tests:__pkg__",
        "//src/storage:__pkg__",
        "//src/storage/tests:__pkg__",
    ],
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/runtime:time",
    ],
)

kwaque_cc_library(
    name = "batch_context",
    hdrs = ["batch_context.h"],
    visibility = [
        "//src/model/tests:__pkg__",
        "//src/protocol:__pkg__",
        "//src/protocol/tests:__pkg__",
        "//src/storage:__pkg__",
        "//src/storage/tests:__pkg__",
    ],
    deps = [
        ":batch_identity",
        ":position",
        "//src/base",
        "//src/base:error",
        "//src/codec:limits",
        "//src/runtime:time",
    ],
)

filegroup(
    name = "package_boundary",
    srcs = ["README.md"],
)
