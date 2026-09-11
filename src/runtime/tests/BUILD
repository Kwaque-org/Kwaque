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
    name = "operation_statistics_test",
    srcs = ["operation_statistics_test.cc"],
    deps = [
        "//src/runtime:operation_statistics",
        "//src/runtime:operation_statistics_test_support",
    ],
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
        "//src/base",
        "//src/runtime:file",
        "//src/runtime:file_position",
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
    name = "production_primitives_test",
    srcs = ["production_primitives_test.cc"],
    cpu = 1,
    memory = "128MiB",
    deps = [
        "//src/runtime:random",
        "//src/runtime:time",
        "//src/runtime:timer",
        "//src/runtime/production:clocks",
        "//src/runtime/production:random",
        "//src/runtime/production:timer",
    ],
)

kwaque_cc_seastar_test(
    name = "production_environment_test",
    srcs = ["production_environment_test.cc"],
    cpu = 2,
    memory = "256MiB",
    deps = [
        "//src/base",
        "//src/observability:event_identity",
        "//src/resource:resource_config",
        "//src/resource:resource_manager",
        "//src/resource:resource_registry",
        "//src/runtime:cross_shard",
        "//src/runtime:dns",
        "//src/runtime:environment",
        "//src/runtime:file",
        "//src/runtime:network",
        "//src/runtime:task_scope",
        "//src/runtime/production:environment",
        "//src/runtime/production:environment_test_support",
        "//src/runtime/testing:test_directory",
        "//src/runtime/testing/contracts:cleanup",
        "//src/runtime/testing/contracts:dns_test_server",
        "//src/runtime/testing/contracts:environment_contract",
        "//src/runtime/testing/contracts:environment_lifecycle_contract",
        "//src/runtime/testing/contracts:production_environment_component",
    ],
)

kwaque_cc_seastar_test(
    name = "file_test",
    srcs = ["file_test.cc"],
    cpu = 1,
    memory = "128MiB",
    deps = [
        "//src/runtime:file",
        "//src/runtime:file_test_support",
        "//src/runtime:fragmented_buffer_internal",
    ],
)

kwaque_cc_seastar_test(
    name = "production_file_test",
    srcs = ["production_file_test.cc"],
    cpu = 1,
    memory = "128MiB",
    deps = [
        "//src/runtime:file",
        "//src/runtime:fragmented_buffer_internal",
        "//src/runtime/testing:test_directory",
        "//src/runtime/testing/contracts:cleanup",
    ],
)

kwaque_cc_seastar_test(
    name = "production_file_system_test",
    srcs = ["production_file_system_test.cc"],
    cpu = 1,
    memory = "128MiB",
    deps = [
        "//src/runtime:file",
        "//src/runtime/production:file",
        "//src/runtime/testing:test_directory",
        "//src/runtime/testing/contracts:cleanup",
    ],
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
        "//src/runtime:fault_api",
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
    name = "production_network_test",
    srcs = ["production_network_test.cc"],
    cpu = 1,
    memory = "128MiB",
    deps = [
        "//src/bytes:fragmented_buffer",
        "//src/runtime:network",
        "//src/runtime/production:network",
        "//src/runtime/production:network_test_support",
        "//src/runtime/testing/contracts:network_contract",
    ],
)

kwaque_cc_seastar_test(
    name = "production_dns_test",
    srcs = ["production_dns_test.cc"],
    cpu = 1,
    memory = "128MiB",
    deps = [
        "//src/runtime:dns",
        "//src/runtime/production:dns",
        "//src/runtime/testing/contracts:cleanup",
        "//src/runtime/testing/contracts:dns_contract",
        "//src/runtime/testing/contracts:dns_test_server",
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

kwaque_cc_test(
    name = "environment_component_contract_test",
    srcs = ["environment_component_contract_test.cc"],
    deps = [
        "//src/base",
        "//src/observability:capture_event_sink",
        "//src/resource:resource_manager",
        "//src/runtime:environment",
        "//src/runtime/testing/contracts:contract_backends",
        "//src/runtime/testing/contracts:environment_component",
    ],
)

kwaque_cc_benchmark(
    name = "runtime_contract_bench",
    srcs = [
        "runtime_contract_bench.cc",
        "runtime_queue_bench.cc",
    ],
    memory = "128MiB",
    deps = [
        "//src/base",
        "//src/resource:bounded_work_queue",
        "//src/runtime:environment",
        "//src/runtime:operation_statistics",
        "//src/runtime:random",
        "//src/runtime:shard_affinity",
        "//src/runtime/production:clocks",
        "//src/runtime/production:dns",
        "//src/runtime/production:random",
        "//src/runtime/production:timer",
        "//src/runtime/testing/contracts:contract_backends",
        "@abseil-cpp//absl/random:random",
        "@seastar",
    ],
)

kwaque_cc_benchmark(
    name = "runtime_file_bench",
    srcs = ["runtime_file_bench.cc"],
    memory = "128MiB",
    deps = [
        "//src/bytes:fragmented_buffer",
        "//src/runtime:file",
        "//src/runtime:fragmented_buffer_internal",
        "@seastar",
    ],
)

kwaque_cc_benchmark(
    name = "runtime_network_bench",
    srcs = ["runtime_network_bench.cc"],
    memory = "128MiB",
    deps = [
        "//src/bytes:fragmented_buffer",
        "//src/runtime:fragmented_buffer_internal",
        "//src/runtime:network",
        "//src/runtime/production:network",
        "@seastar",
    ],
)

kwaque_cc_benchmark(
    name = "runtime_network_counter_bench",
    srcs = [
        "network_benchmark_transport.h",
        "runtime_network_bench.cc",
    ],
    args = ["--task-quota-ms=60000"],
    local_defines = ["KWAQUE_CONTROLLED_NETWORK_BENCH=1"],
    memory = "128MiB",
    deps = [
        "//src/base",
        "//src/bytes:fragmented_buffer",
        "//src/runtime:fragmented_buffer_internal",
        "//src/runtime:network",
        "//src/runtime/production:network",
        "//src/runtime/production:network_test_support",
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
    name = "runtime_metrics_test",
    srcs = ["runtime_metrics_test.cc"],
    cpu = 1,
    memory = "192MiB",
    deps = [
        "//src/base",
        "//src/base:metric_schema",
        "//src/observability:event_identity",
        "//src/resource:resource_config",
        "//src/resource:resource_registry",
        "//src/runtime/production:environment",
        "//src/runtime/production:environment_test_support",
    ],
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
    name = "stop_signal_test",
    srcs = ["stop_signal_test.cc"],
    cpu = 1,
    memory = "128MiB",
    deps = ["//src/runtime:stop_signal"],
)

kwaque_cc_seastar_gtest(
    name = "task_scope_invariant_test",
    srcs = ["task_scope_invariant_test.cc"],
    cpu = 1,
    memory = "128MiB",
    deps = ["//src/runtime:task_scope"],
)

test_suite(
    name = "hermetic_contracts",
    tests = [
        ":production_dns_test",
        ":production_environment_test",
        ":production_file_system_test",
        ":production_file_test",
        ":production_network_test",
        ":production_primitives_test",
        ":sharded_service_test",
        ":test_directory_test",
    ],
    visibility = ["//visibility:public"],
)

kwaque_cc_seastar_test(
    name = "test_directory_test",
    srcs = ["test_directory_test.cc"],
    deps = [
        "//src/runtime:error",
        "//src/runtime/testing:test_directory",
        "//src/runtime/testing/contracts:cleanup",
    ],
)
