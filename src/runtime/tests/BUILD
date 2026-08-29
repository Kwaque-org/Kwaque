load(
    "//bazel:test.bzl",
    "kwaque_cc_benchmark",
    "kwaque_cc_seastar_gtest",
    "kwaque_cc_seastar_test",
    "kwaque_cc_test",
)

package(default_visibility = ["//visibility:private"])

kwaque_cc_test(
    name = "operation_error_test",
    srcs = ["operation_error_test.cc"],
    deps = ["//src/runtime:error"],
)

kwaque_cc_test(
    name = "time_random_contract_test",
    srcs = ["time_random_contract_test.cc"],
    deps = [
        "//src/runtime:random",
        "//src/runtime:time",
    ],
)

kwaque_cc_test(
    name = "file_contract_test",
    srcs = ["file_contract_test.cc"],
    deps = [
        "//src/runtime:file",
        "@seastar",
    ],
)

kwaque_cc_seastar_test(
    name = "timer_contract_test",
    srcs = ["timer_contract_test.cc"],
    cpu = 1,
    memory = "128MiB",
    deps = ["//src/runtime:timer"],
)

kwaque_cc_seastar_test(
    name = "file_test",
    srcs = ["file_test.cc"],
    cpu = 1,
    memory = "128MiB",
    deps = ["//src/runtime:file"],
)

kwaque_cc_test(
    name = "file_io_contract_test",
    srcs = ["file_io_contract_test.cc"],
    deps = [
        "//src/bytes:fragmented_buffer",
        "//src/runtime:file",
        "//src/runtime:fragmented_buffer_internal",
        "@seastar",
    ],
)

kwaque_cc_test(
    name = "network_dns_fault_contract_test",
    srcs = ["network_dns_fault_contract_test.cc"],
    deps = [
        "//src/bytes:fragmented_buffer",
        "//src/runtime:dns",
        "//src/runtime:fault",
        "//src/runtime:network",
        "@seastar",
    ],
)

kwaque_cc_seastar_test(
    name = "network_admission_test",
    srcs = ["network_admission_test.cc"],
    cpu = 1,
    memory = "128MiB",
    deps = [
        "//src/bytes:fragmented_buffer",
        "//src/runtime:dns",
        "//src/runtime:network",
    ],
)

kwaque_cc_seastar_test(
    name = "environment_contract_test",
    srcs = ["environment_contract_test.cc"],
    cpu = 1,
    memory = "128MiB",
    deps = [
        "//src/bytes:fragmented_buffer",
        "//src/runtime:environment",
        "//src/runtime/testing/contracts:contract_backends",
    ],
)

kwaque_cc_benchmark(
    name = "runtime_contract_bench",
    srcs = ["runtime_contract_bench.cc"],
    memory = "128MiB",
    deps = [
        "//src/runtime:environment",
        "//src/runtime/testing/contracts:contract_backends",
        "@seastar",
    ],
)

kwaque_cc_seastar_test(
    name = "shard_affinity_test",
    srcs = ["shard_affinity_test.cc"],
    cpu = 2,
    memory = "128MiB",
    deps = [
        "//src/base:invariant_test_support",
        "//src/runtime:shard_affinity_test_support",
    ],
)

kwaque_cc_seastar_test(
    name = "cross_shard_test",
    srcs = ["cross_shard_test.cc"],
    cpu = 2,
    memory = "128MiB",
    deps = [
        "//src/bytes:fragmented_buffer",
        "//src/runtime:cross_shard",
    ],
)

kwaque_cc_seastar_test(
    name = "task_scope_test",
    srcs = ["task_scope_test.cc"],
    cpu = 1,
    memory = "128MiB",
    deps = ["//src/runtime:task_scope"],
)

kwaque_cc_seastar_test(
    name = "sharded_service_test",
    srcs = ["sharded_service_test.cc"],
    cpu = 2,
    memory = "128MiB",
    deps = [
        "//src/runtime:sharded_service",
        "//src/runtime:task_scope",
    ],
)

kwaque_cc_seastar_test(
    name = "reactor_smoke_test",
    srcs = ["reactor_smoke_test.cc"],
    cpu = 1,
    memory = "128MiB",
)

kwaque_cc_seastar_test(
    name = "runtime_primitives_test",
    srcs = ["runtime_primitives_test.cc"],
    cpu = 2,
    memory = "128MiB",
)

kwaque_cc_seastar_test(
    name = "runtime_service_test",
    srcs = ["runtime_service_test.cc"],
    cpu = 2,
    memory = "128MiB",
    deps = [
        "//src/runtime:runtime_service",
        "//src/runtime:sharded_service",
        "//src/runtime:stop_signal",
    ],
)

kwaque_cc_seastar_gtest(
    name = "runtime_service_invariant_test",
    srcs = ["runtime_service_invariant_test.cc"],
    cpu = 1,
    memory = "128MiB",
    deps = ["//src/runtime:runtime_service"],
)
