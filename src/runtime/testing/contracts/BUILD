load("//bazel:build.bzl", "kwaque_cc_library")

package(default_visibility = ["//visibility:private"])

kwaque_cc_library(
    name = "environment_component",
    testonly = True,
    hdrs = ["environment_component.h"],
    visibility = [
        "//src/runtime/tests:__pkg__",
        "//src/simulation/tests:__pkg__",
    ],
    deps = [
        "//src/base:invariant",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_builder",
        "//src/observability:event",
        "//src/observability:event_sink_concept",
        "//src/resource:resource_manager",
        "//src/runtime:environment",
        "//src/runtime:task_scope",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "environment_contract",
    testonly = True,
    hdrs = ["environment_contract.h"],
    visibility = [
        "//src/runtime/tests:__pkg__",
        "//src/simulation/tests:__pkg__",
    ],
    deps = [
        ":environment_component",
        "//src/observability:event_sink_concept",
        "//src/resource:resource_manager",
        "//src/runtime:environment",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "production_environment_component",
    testonly = True,
    srcs = ["production_environment_component.cc"],
    visibility = ["//src/runtime/tests:__pkg__"],
    deps = [
        ":environment_contract",
        "//src/observability:event_sink",
        "//src/runtime/production:environment",
    ],
)

kwaque_cc_library(
    name = "simulation_environment_component",
    testonly = True,
    srcs = ["simulation_environment_component.cc"],
    visibility = ["//src/simulation/tests:__pkg__"],
    deps = [
        ":environment_contract",
        "//src/simulation:environment",
        "//src/simulation:structured_event_sink",
    ],
)

kwaque_cc_library(
    name = "environment_lifecycle_contract",
    testonly = True,
    hdrs = ["environment_lifecycle_contract.h"],
    visibility = [
        "//src/runtime/tests:__pkg__",
        "//src/simulation/tests:__pkg__",
    ],
    deps = [
        "//src/runtime:environment",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "network_contract",
    testonly = True,
    hdrs = ["network_contract.h"],
    visibility = [
        "//src/runtime/tests:__pkg__",
        "//src/simulation/tests:__pkg__",
    ],
    deps = [
        "//src/bytes:fragmented_buffer_builder",
        "//src/runtime:network",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "dns_contract",
    testonly = True,
    hdrs = ["dns_contract.h"],
    visibility = [
        "//src/runtime/tests:__pkg__",
        "//src/simulation/tests:__pkg__",
    ],
    deps = [
        "//src/runtime:dns",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "contract_backends",
    testonly = True,
    hdrs = ["contract_backends.h"],
    visibility = ["//src/runtime/tests:__pkg__"],
    deps = [
        "//src/bytes:fragmented_buffer",
        "//src/runtime:environment",
        "//src/runtime:fault",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "real_backend_conformance",
    testonly = True,
    hdrs = ["real_backend_conformance.h"],
    visibility = ["//src/runtime/tests:__pkg__"],
    deps = [
        "//src/bytes:fragmented_buffer",
        "//src/runtime:environment",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "dns_test_server",
    testonly = True,
    hdrs = ["dns_test_server.h"],
    visibility = ["//src/runtime/tests:__pkg__"],
    deps = [
        "//src/runtime:dns",
        "@seastar",
    ],
)
