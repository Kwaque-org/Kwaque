load(
    "//bazel:test.bzl",
    "kwaque_cc_seastar_test",
    "kwaque_cc_test",
)

package(default_visibility = ["//visibility:private"])

kwaque_cc_test(
    name = "workload_class_test",
    srcs = ["workload_class_test.cc"],
    deps = ["//src/resource:workload_class"],
)

kwaque_cc_test(
    name = "resource_config_test",
    srcs = ["resource_config_test.cc"],
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/resource:resource_config",
        "//src/resource:workload_class",
    ],
)

kwaque_cc_seastar_test(
    name = "resource_registry_test",
    srcs = ["resource_registry_test.cc"],
    cpu = 2,
    memory = "192MiB",
    deps = [
        "//src/base",
        "//src/resource:resource_config",
        "//src/resource:resource_test_support",
        "//src/resource:workload_class",
    ],
)

kwaque_cc_seastar_test(
    name = "resource_manager_test",
    srcs = ["resource_manager_test.cc"],
    cpu = 4,
    memory = "512MiB",
    deps = [
        "//src/base",
        "//src/resource:resource_manager",
        "//src/resource:resource_registry",
        "//src/resource:workload_class",
        "//src/runtime:cross_shard",
        "//src/runtime:sharded_service",
    ],
)

kwaque_cc_seastar_test(
    name = "resource_manager_affinity_test",
    srcs = ["resource_manager_affinity_test.cc"],
    cpu = 2,
    memory = "192MiB",
    deps = [
        "//src/base",
        "//src/base:invariant_test_support",
        "//src/resource:resource_affinity_test_support",
        "//src/resource:resource_config",
    ],
)

kwaque_cc_seastar_test(
    name = "bounded_work_queue_test",
    srcs = ["bounded_work_queue_test.cc"],
    cpu = 2,
    memory = "192MiB",
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/resource:bounded_work_queue",
        "//src/resource:resource_config",
        "//src/resource:resource_manager",
        "//src/resource:resource_registry",
        "//src/resource:workload_class",
    ],
)
