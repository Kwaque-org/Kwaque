load("//bazel:build.bzl", "kwaque_cc_library")

package(default_visibility = ["//visibility:private"])

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
