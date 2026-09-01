load("//bazel:build.bzl", "kwaque_cc_library")
load(
    "//bazel:test.bzl",
    "kwaque_cc_seastar_test",
    "kwaque_cc_test",
)

package(default_visibility = ["//visibility:private"])

kwaque_cc_library(
    name = "fake_file_model",
    testonly = True,
    hdrs = ["fake_file_model.h"],
    deps = [
        "//src/runtime:random",
        "//src/simulation:deterministic_random",
    ],
)

kwaque_cc_library(
    name = "network_oracle",
    testonly = True,
    srcs = ["network_oracle.cc"],
    hdrs = ["network_oracle.h"],
    deps = [
        "//src/runtime:error",
        "//src/runtime:fault_api",
        "//src/simulation:sha256",
        "@boost//:multiprecision",
    ],
)

kwaque_cc_test(
    name = "network_oracle_test",
    srcs = ["network_oracle_test.cc"],
    deps = [
        ":network_oracle",
        "//src/simulation:bandwidth",
    ],
)

kwaque_cc_seastar_test(
    name = "scheduler_test",
    srcs = ["scheduler_test.cc"],
    cpu = 1,
    memory = "192MiB",
    deps = [
        "//src/runtime:error",
        "//src/runtime:fault_api",
        "//src/runtime:time",
        "//src/simulation:scheduler",
        "//src/simulation:scheduler_test_support",
    ],
)

kwaque_cc_seastar_test(
    name = "scheduler_affinity_test",
    srcs = ["scheduler_affinity_test.cc"],
    cpu = 2,
    memory = "192MiB",
    deps = [
        "//src/base:invariant_test_support",
        "//src/simulation:scheduler_affinity_test_support",
    ],
)

kwaque_cc_seastar_test(
    name = "virtual_time_test",
    srcs = ["virtual_time_test.cc"],
    cpu = 1,
    memory = "192MiB",
    deps = [
        "//src/runtime:time",
        "//src/simulation:scheduler",
        "//src/simulation:virtual_time",
        "//src/simulation:virtual_time_test_support",
    ],
)

kwaque_cc_seastar_test(
    name = "virtual_time_binding_test",
    srcs = ["virtual_time_binding_test.cc"],
    cpu = 1,
    memory = "192MiB",
    deps = [
        "//src/base:invariant_test_support",
        "//src/simulation:scheduler_affinity_test_support",
        "//src/simulation:virtual_time_invariant_test_support",
    ],
)

kwaque_cc_test(
    name = "virtual_time_death_test",
    srcs = ["virtual_time_death_test.cc"],
    deps = ["//src/simulation:virtual_time"],
)

kwaque_cc_seastar_test(
    name = "timer_test",
    srcs = ["timer_test.cc"],
    cpu = 1,
    memory = "192MiB",
    deps = [
        "//src/runtime:timer",
        "//src/simulation:deterministic_random",
        "//src/simulation:event_trace",
        "//src/simulation:scheduler",
        "//src/simulation:scheduler_test_support",
        "//src/simulation:timer",
        "//src/simulation:virtual_time",
    ],
)

kwaque_cc_test(
    name = "philox_kat_test",
    srcs = ["philox_kat_test.cc"],
    deps = ["//src/simulation:philox"],
)

kwaque_cc_test(
    name = "deterministic_random_test",
    srcs = ["deterministic_random_test.cc"],
    deps = [
        "//src/simulation:deterministic_random",
        "//src/simulation:deterministic_random_test_support",
    ],
)

kwaque_cc_test(
    name = "event_trace_test",
    srcs = ["event_trace_test.cc"],
    deps = [
        "//src/runtime:fault_api",
        "//src/simulation:event_trace",
    ],
)

kwaque_cc_test(
    name = "bandwidth_test",
    srcs = ["bandwidth_test.cc"],
    deps = [
        "//src/runtime:error",
        "//src/runtime:time",
        "//src/simulation:bandwidth",
    ],
)

kwaque_cc_seastar_test(
    name = "determinism_test",
    srcs = ["determinism_test.cc"],
    cpu = 1,
    memory = "256MiB",
    deps = [
        "//src/simulation:deterministic_random",
        "//src/simulation:event_trace",
        "//src/simulation:scheduler",
        "//src/simulation:sha256",
        "//src/simulation:virtual_time",
    ],
)

kwaque_cc_seastar_test(
    name = "fault_schedule_test",
    srcs = ["fault_schedule_test.cc"],
    cpu = 1,
    memory = "256MiB",
    deps = [
        "//src/runtime:fault",
        "//src/runtime:random",
        "//src/simulation:deterministic_random",
        "//src/simulation:event_trace",
        "//src/simulation:fault_schedule",
        "//src/simulation:scheduler",
    ],
)

kwaque_cc_seastar_test(
    name = "fake_file_test",
    srcs = ["fake_file_test.cc"],
    cpu = 1,
    memory = "256MiB",
    deps = [
        "//src/base",
        "//src/simulation:fake_file",
        "//src/simulation:fake_file_test_support",
    ],
)

kwaque_cc_seastar_test(
    name = "fake_file_runtime_test",
    srcs = ["fake_file_runtime_test.cc"],
    cpu = 2,
    memory = "256MiB",
    deps = [
        "//src/bytes:fragmented_buffer",
        "//src/runtime:file",
        "//src/runtime:random",
        "//src/simulation:deterministic_random",
        "//src/simulation:event_trace",
        "//src/simulation:fake_file",
        "//src/simulation:fake_file_test_support",
        "//src/simulation:fault_schedule",
        "//src/simulation:scheduler",
        "//src/simulation/tests:fake_file_model",
    ],
)

kwaque_cc_seastar_test(
    name = "fake_file_replay_test",
    srcs = ["fake_file_replay_test.cc"],
    cpu = 1,
    memory = "256MiB",
    deps = [
        "//src/bytes:fragmented_buffer",
        "//src/runtime:fault",
        "//src/runtime:file",
        "//src/simulation:event_trace",
        "//src/simulation:fake_file",
        "//src/simulation:fake_file_test_support",
        "//src/simulation:fault_schedule",
        "//src/simulation:scheduler",
    ],
)

kwaque_cc_seastar_test(
    name = "fake_network_test",
    srcs = ["fake_network_test.cc"],
    cpu = 1,
    memory = "256MiB",
    deps = [
        "//src/runtime:network",
        "//src/runtime/testing/contracts:network_contract",
        "//src/simulation:event_trace",
        "//src/simulation:fake_network",
        "//src/simulation:fault_schedule",
        "//src/simulation:scheduler",
        "//src/simulation/tests:network_oracle",
    ],
)

kwaque_cc_seastar_test(
    name = "fake_dns_test",
    srcs = ["fake_dns_test.cc"],
    cpu = 1,
    memory = "256MiB",
    deps = [
        "//src/runtime:dns",
        "//src/runtime/testing/contracts:dns_contract",
        "//src/simulation:event_trace",
        "//src/simulation:fake_dns",
        "//src/simulation:fault_schedule",
        "//src/simulation:scheduler",
    ],
)
