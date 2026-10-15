load("@rules_python//python:defs.bzl", "py_binary", "py_test")
load("//bazel:build.bzl", "kwaque_cc_binary", "kwaque_cc_library")
load("//bazel:test.bzl", "kwaque_cc_benchmark", "kwaque_cc_fuzz_test", "kwaque_cc_seastar_gtest", "kwaque_cc_seastar_test", "kwaque_cc_test", "kwaque_py_native_test")
load("//src/codec/tests:allocation_observer.bzl", "ALLOCATION_OBSERVER_LINKOPTS")

package(default_visibility = ["//visibility:private"])

kwaque_cc_library(
    name = "completion_contract",
    testonly = True,
    hdrs = ["completion_contract.h"],
    visibility = [
        "//src/broker:__pkg__",
        "//src/simulation/tests:__pkg__",
    ],
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/bytes:allocation_test_profile",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_builder",
        "//src/resource:resource_manager",
        "//src/resource:workload_class",
        "//src/runtime:environment",
        "//src/runtime:error",
        "//src/runtime:file",
        "//src/runtime:first_failure",
        "//src/runtime:timer",
        "//src/storage:completion_resources",
        "//src/storage:operation_scope",
        "//src/storage:statistics",
        "//src/storage:workload_budget",
        "@seastar",
    ],
)

kwaque_cc_seastar_gtest(
    name = "format_fixture_test",
    srcs = ["format_fixture_test.cc"],
    data = ["//src/codec/tests:format_fixture_data"],
    memory = "256MiB",
    deps = [
        ":storage_test_support",
        "//src/base",
        "//src/base:error",
        "//src/bytes:allocation_test_profile",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:cooperative",
        "//src/codec:digest",
        "//src/codec:envelope",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/codec/tests:format_fixture",
        "//src/codec/tests:format_test_support",
        "//src/model:batch",
        "//src/model:batch_codec",
        "//src/model:batch_context",
        "//src/model:batch_identity",
        "//src/model:epoch",
        "//src/model:identity",
        "//src/model:position",
        "//src/runtime:file_position",
        "//src/storage:encoded_batch",
        "//src/storage:footer_format",
        "//src/storage:format_context",
        "//src/storage:page_ref",
        "//src/storage:range_manifest_format",
        "//src/storage:segment_format",
        "//src/storage:sparse_index_format",
        "//src/storage:wal_format",
    ],
)

kwaque_cc_seastar_gtest(
    name = "format_integration_test",
    srcs = ["format_integration_test.cc"],
    memory = "256MiB",
    deps = [
        ":storage_test_support",
        "//src/base",
        "//src/base:error",
        "//src/bytes:allocation_test_profile",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:cooperative",
        "//src/codec:digest",
        "//src/codec:envelope",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/model:batch",
        "//src/model:batch_builder",
        "//src/model:batch_codec",
        "//src/model:batch_context",
        "//src/model:batch_identity",
        "//src/model:epoch",
        "//src/model:identity",
        "//src/model:position",
        "//src/model:record_codec",
        "//src/runtime:file_position",
        "//src/runtime:time",
        "//src/storage:encoded_batch",
        "//src/storage:footer_format",
        "//src/storage:format_context",
        "//src/storage:page_ref",
        "//src/storage:range_manifest_format",
        "//src/storage:segment_format",
        "//src/storage:sparse_index_format",
        "//src/storage:wal_format",
    ],
)

[
    kwaque_cc_seastar_gtest(
        name = name,
        srcs = [
            name + ".cc",
            "segment_test_support.h",
        ] + (["footer_test_support.h"] if name in [
            "footer_format_test",
            "extent_verifier_test",
            "recovery_test",
            "retry_format_test",
            "sealed_format_test",
            "retry_qualification_test",
        ] else []) + (["retry_test_support.h"] if name in [
            "retry_format_test",
            "sealed_format_test",
            "retry_qualification_test",
        ] else []) + (["wal_test_support.h"] if name in [
            "recovery_test",
            "wal_format_test",
        ] else []),
        memory = "256MiB",
        deps = [
            "//src/base",
            "//src/base:error",
            "//src/bytes:allocation_test_profile",
            "//src/bytes:fragmented_buffer",
            "//src/bytes:fragmented_buffer_builder",
            "//src/bytes:fragmented_buffer_parser",
            "//src/codec:cooperative",
            "//src/codec:error",
            "//src/codec:integer",
            "//src/codec:limits",
            "//src/model:batch",
            "//src/model:batch_builder",
            "//src/model:batch_codec",
            "//src/model:batch_context",
            "//src/model:batch_identity",
            "//src/model:epoch",
            "//src/model:identity",
            "//src/model:position",
            "//src/model:record_codec",
            "//src/runtime:file_position",
            "//src/runtime:time",
            "//src/storage:encoded_batch",
            "//src/storage:format_context",
            "//src/storage:segment_format",
            "@crc32c",
            "@lz4//:lz4_frame",
        ] + ([
            "//src/storage:wal_format",
            "//src/storage:wal_group",
        ] if name == "wal_format_test" else []) + ([
            "//src/storage:recovery",
            "//src/storage:wal_format",
        ] if name == "recovery_test" else []) + ([
            "//src/storage:footer_format",
            "//src/storage:page_ref",
            "//src/storage:completed_retry",
            "//src/codec:xxh3",
        ] if name in [
            "footer_format_test",
            "extent_verifier_test",
            "recovery_test",
            "retry_format_test",
            "sealed_format_test",
            "retry_qualification_test",
        ] else []) + (["//src/bytes:fragmented_buffer_test_support"] if name == "extent_verifier_test" else []),
    )
    for name in [
        "encoded_batch_test",
        "extent_verifier_test",
        "retry_format_test",
        "sealed_format_test",
        "retry_qualification_test",
        "footer_format_test",
        "recovery_test",
        "segment_format_test",
        "segment_relocation_test",
        "wal_format_test",
    ]
]

kwaque_cc_test(
    name = "format_context_test",
    srcs = ["format_context_test.cc"],
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/model:batch_identity",
        "//src/model:epoch",
        "//src/model:identity",
        "//src/model:position",
        "//src/runtime:file_position",
        "//src/storage:format_context",
    ],
)

kwaque_cc_test(
    name = "format_size_test",
    srcs = ["format_size_test.cc"],
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/codec:envelope",
        "//src/codec:limits",
        "//src/model:position",
        "//src/runtime:file_position",
        "//src/storage:format_context",
        "//src/storage:format_size",
    ],
)

test_suite(
    name = "format_tests",
    tests = [
        ":encoded_batch_test",
        ":extent_verifier_test",
        ":footer_format_test",
        ":format_context_test",
        ":format_fixture_test",
        ":format_integration_test",
        ":format_size_test",
        ":index_manifest_qualification_test",
        ":local_metadata_fixture_test",
        ":local_metadata_fuzz_cases_test",
        ":local_metadata_test",
        ":local_paths_test",
        ":local_store_config_test",
        ":local_types_test",
        ":memory_qualification_test",
        ":range_manifest_decode_test",
        ":range_manifest_format_test",
        ":retry_format_test",
        ":retry_qualification_test",
        ":sealed_format_test",
        ":segment_format_test",
        ":segment_relocation_test",
        ":sparse_index_format_test",
        ":storage_format_fuzz_cases_test",
        ":storage_format_qualification_test",
        ":wal_format_test",
    ],
)

# Parts of the storage suites, manual as the suites are (src/storage/suites).
# The storage unit tests outside the format suite: one owner or one rule at a time.
test_suite(
    name = "local_storage_tests",
    tags = ["manual"],
    tests = [
        ":cache_policy_test",
        ":checkpoint_test",
        ":device_placement_test",
        ":engine_history_test",
        ":engine_ledger_test",
        ":local_cleanup_test",
        ":local_store_admission_test",
        ":page_cache_test",
        ":recovery_test",
        ":retention_test",
        ":retry_snapshot_test",
        ":segment_writer_state_test",
        ":space_sampler_test",
        ":sparse_index_test",
        ":wal_writer_state_test",
        ":workload_budget_test",
    ],
    visibility = ["//src/storage/suites:__pkg__"],
)

# Contracts instantiated on the production file system in this package.
test_suite(
    name = "storage_backend_tests",
    tags = ["manual"],
    tests = [
        ":completion_resources_test",
        ":local_publication_test",
    ],
    visibility = ["//src/storage/suites:__pkg__"],
)

# The formats' independent goldens, as the golden storage suite holds them.
test_suite(
    name = "storage_golden_tests",
    tags = ["manual"],
    tests = [
        ":format_tests",
    ],
    visibility = ["//src/storage/suites:__pkg__"],
)

kwaque_cc_seastar_gtest(
    name = "sparse_index_format_test",
    srcs = [
        "footer_test_support.h",
        "segment_test_support.h",
        "sparse_index_format_test.cc",
        "sparse_index_test_support.h",
    ],
    memory = "256MiB",
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/bytes:allocation_test_profile",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_builder",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:cooperative",
        "//src/codec:digest",
        "//src/codec:envelope",
        "//src/codec:error",
        "//src/codec:limits",
        "//src/codec:xxh3",
        "//src/model:batch",
        "//src/model:batch_codec",
        "//src/model:batch_context",
        "//src/model:epoch",
        "//src/model:identity",
        "//src/model:position",
        "//src/runtime:file_position",
        "//src/storage:encoded_batch",
        "//src/storage:footer_format",
        "//src/storage:format_context",
        "//src/storage:page_ref",
        "//src/storage:segment_format",
        "//src/storage:sparse_index_format",
        "@crc32c",
        "@lz4//:lz4_frame",
    ],
)

kwaque_cc_seastar_test(
    name = "sparse_index_test",
    srcs = ["sparse_index_test.cc"],
    memory = "256MiB",
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/bytes:allocation_test_profile",
        "//src/codec:limits",
        "//src/model:position",
        "//src/resource:resource_config",
        "//src/resource:resource_manager",
        "//src/resource:resource_registry",
        "//src/resource:workload_class",
        "//src/runtime:error",
        "//src/runtime:file_position",
        "//src/storage:format_context",
        "//src/storage:local_store_config",
        "//src/storage:page_ref",
        "//src/storage:sparse_index",
        "//src/storage:sparse_index_format",
        "//src/storage:workload_budget",
    ],
)

kwaque_cc_seastar_gtest(
    name = "range_manifest_format_test",
    srcs = [
        "footer_test_support.h",
        "range_manifest_format_test.cc",
        "range_manifest_test_support.h",
        "segment_test_support.h",
    ],
    memory = "256MiB",
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/bytes:allocation_test_profile",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_builder",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:cooperative",
        "//src/codec:digest",
        "//src/codec:envelope",
        "//src/codec:error",
        "//src/codec:limits",
        "//src/codec:xxh3",
        "//src/model:batch",
        "//src/model:batch_codec",
        "//src/model:batch_context",
        "//src/model:epoch",
        "//src/model:identity",
        "//src/model:position",
        "//src/runtime:file_position",
        "//src/storage:encoded_batch",
        "//src/storage:footer_format",
        "//src/storage:format_context",
        "//src/storage:page_ref",
        "//src/storage:range_manifest_format",
        "//src/storage:segment_format",
        "@crc32c",
        "@lz4//:lz4_frame",
    ],
)

kwaque_cc_seastar_gtest(
    name = "range_manifest_decode_test",
    srcs = [
        "footer_test_support.h",
        "range_manifest_decode_test.cc",
        "range_manifest_test_support.h",
        "segment_test_support.h",
        "sparse_index_test_support.h",
    ],
    memory = "256MiB",
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/bytes:allocation_test_profile",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_builder",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:cooperative",
        "//src/codec:digest",
        "//src/codec:envelope",
        "//src/codec:error",
        "//src/codec:limits",
        "//src/codec:xxh3",
        "//src/model:batch",
        "//src/model:batch_codec",
        "//src/model:batch_context",
        "//src/model:epoch",
        "//src/model:identity",
        "//src/model:position",
        "//src/runtime:file_position",
        "//src/storage:encoded_batch",
        "//src/storage:footer_format",
        "//src/storage:format_context",
        "//src/storage:page_ref",
        "//src/storage:range_manifest_format",
        "//src/storage:segment_format",
        "//src/storage:sparse_index_format",
        "@crc32c",
        "@lz4//:lz4_frame",
    ],
)

kwaque_cc_seastar_gtest(
    name = "index_manifest_qualification_test",
    timeout = "long",
    srcs = [
        "footer_test_support.h",
        "index_manifest_qualification_test.cc",
        "range_manifest_test_support.h",
        "segment_test_support.h",
        "sparse_index_test_support.h",
    ],
    memory = "256MiB",
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/bytes:allocation_test_profile",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_builder",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:cooperative",
        "//src/codec:digest",
        "//src/codec:envelope",
        "//src/codec:error",
        "//src/codec:limits",
        "//src/codec:xxh3",
        "//src/codec/tests:qualification_profile",
        "//src/model:batch",
        "//src/model:batch_codec",
        "//src/model:batch_context",
        "//src/model:epoch",
        "//src/model:identity",
        "//src/model:position",
        "//src/runtime:file_position",
        "//src/storage:encoded_batch",
        "//src/storage:footer_format",
        "//src/storage:format_context",
        "//src/storage:page_ref",
        "//src/storage:range_manifest_format",
        "//src/storage:segment_format",
        "//src/storage:sparse_index_format",
        "@crc32c",
        "@lz4//:lz4_frame",
    ],
)

kwaque_cc_library(
    name = "storage_test_support",
    testonly = True,
    srcs = [
        "storage_format_fixture.cc",
        "storage_metadata_fixture.cc",
    ],
    hdrs = [
        "footer_test_support.h",
        "range_manifest_test_support.h",
        "retry_test_support.h",
        "segment_test_support.h",
        "sparse_index_test_support.h",
        "storage_format_fixture.h",
        "storage_metadata_fixture.h",
        "wal_test_support.h",
    ],
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/bytes:allocation_test_profile",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_builder",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:cooperative",
        "//src/codec:digest",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/codec:xxh3",
        "//src/model:batch",
        "//src/model:batch_codec",
        "//src/model:batch_context",
        "//src/model:batch_identity",
        "//src/model:epoch",
        "//src/model:identity",
        "//src/model:position",
        "//src/runtime:file_position",
        "//src/runtime:time",
        "//src/storage:completed_retry",
        "//src/storage:encoded_batch",
        "//src/storage:footer_format",
        "//src/storage:format_context",
        "//src/storage:page_ref",
        "//src/storage:range_manifest_format",
        "//src/storage:segment_format",
        "//src/storage:sparse_index_format",
        "//src/storage:wal_format",
        "@crc32c",
        "@lz4//:lz4_frame",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "storage_large_fixture",
    testonly = True,
    srcs = ["storage_large_fixture.cc"],
    hdrs = ["storage_large_fixture.h"],
    deps = [
        ":storage_test_support",
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/codec:cooperative",
        "//src/codec:error",
        "//src/codec:limits",
        "//src/codec/tests:codec_bench_reference",
        "//src/model:batch",
        "//src/model:batch_builder",
        "//src/model:batch_identity",
        "//src/model:epoch",
        "//src/model:identity",
        "//src/model:position",
        "//src/model:record_codec",
        "//src/runtime:time",
        "//src/storage:encoded_batch",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "storage_format_fuzz_cases",
    testonly = True,
    srcs = [
        "storage_format_fuzz_cases.cc",
        "storage_metadata_fuzz_cases.cc",
    ],
    hdrs = [
        "storage_format_fuzz_cases.h",
        "storage_metadata_fuzz_cases.h",
    ],
    deps = [
        ":storage_test_support",
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:cooperative",
        "//src/codec:digest",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/model:epoch",
        "//src/model:identity",
        "//src/runtime:file_position",
        "//src/storage:footer_format",
        "//src/storage:format_context",
        "//src/storage:page_ref",
        "//src/storage:range_manifest_format",
        "//src/storage:sparse_index_format",
        "@seastar",
    ],
)

kwaque_cc_seastar_gtest(
    name = "storage_format_fuzz_cases_test",
    srcs = ["storage_format_fuzz_cases_test.cc"],
    memory = "256MiB",
    deps = [
        ":storage_format_fuzz_cases",
        ":storage_test_support",
    ],
)

kwaque_cc_seastar_gtest(
    name = "storage_format_qualification_test",
    srcs = ["storage_format_qualification_test.cc"],
    memory = "384MiB",
    deps = [
        ":storage_large_fixture",
        ":storage_test_support",
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:cooperative",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/codec/tests:codec_bench_reference",
        "//src/codec/tests:qualification_profile",
        "//src/model:identity",
        "//src/model:position",
        "//src/storage:encoded_batch",
        "//src/storage:format_context",
        "//src/storage:segment_format",
        "//src/storage:wal_format",
    ],
)

kwaque_cc_fuzz_test(
    name = "storage_format_fuzz",
    srcs = ["storage_format_fuzz.cc"],
    args = [
        "-max_len=16384",
        "-rss_limit_mb=1024",
    ],
    corpus = glob(["testdata/storage_format_fuzz_corpus/*.bin"]),
    tags = [
        "fuzz-campaign",
        "resources:cpu:1",
        "resources:memory:1024",
    ],
    deps = [
        ":storage_format_fuzz_cases",
        "//src/runtime/testing:seastar_fuzz",
        "@seastar",
    ],
)

kwaque_cc_benchmark(
    name = "storage_format_bench",
    srcs = [
        "storage_format_bench.cc",
        "storage_metadata_bench.cc",
    ],
    memory = "384MiB",
    native_allocator_only = True,
    deps = [
        ":storage_large_fixture",
        ":storage_test_support",
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:cooperative",
        "//src/codec:digest",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/codec:xxh3_cooperative",
        "//src/codec/tests:codec_bench_reference",
        "//src/codec/tests:qualification_profile",
        "//src/model:identity",
        "//src/runtime:file_position",
        "//src/storage:completed_retry",
        "//src/storage:encoded_batch",
        "//src/storage:footer_format",
        "//src/storage:format_context",
        "//src/storage:format_size",
        "//src/storage:page_ref",
        "//src/storage:range_manifest_format",
        "//src/storage:segment_format",
        "//src/storage:sparse_index_format",
        "//src/storage:wal_format",
        "@fmt",
    ],
)

kwaque_cc_seastar_test(
    name = "workload_budget_test",
    srcs = ["workload_budget_test.cc"],
    memory = "256MiB",
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/base:metric_schema",
        "//src/bytes:allocation_test_profile",
        "//src/bytes:fragmented_buffer",
        "//src/resource:bounded_work_queue",
        "//src/resource:resource_config",
        "//src/resource:resource_manager",
        "//src/resource:resource_registry",
        "//src/resource:workload_class",
        "//src/runtime:cross_shard",
        "//src/runtime:error",
        "//src/storage:metrics",
        "//src/storage:operation_scope",
        "//src/storage:statistics",
        "//src/storage:workload_budget",
    ],
)

kwaque_cc_test(
    name = "retention_test",
    srcs = ["retention_test.cc"],
    deps = [
        "//src/base",
        "//src/codec:digest",
        "//src/model:identity",
        "//src/model:position",
        "//src/runtime:file_position",
        "//src/runtime:time",
        "//src/storage:format_context",
        "//src/storage:retention",
    ],
)

kwaque_cc_test(
    name = "local_store_admission_test",
    srcs = ["local_store_admission_test.cc"],
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/codec:digest",
        "//src/model:identity",
        "//src/runtime:file",
        "//src/storage:local_metadata",
        "//src/storage:local_store_admission",
        "//src/storage:local_store_config",
        "//src/storage:local_types",
        "//src/storage:recovery",
    ],
)

kwaque_cc_test(
    name = "device_placement_test",
    srcs = ["device_placement_test.cc"],
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/model:identity",
        "//src/runtime:file",
        "//src/runtime:file_error",
        "//src/runtime:time",
        "//src/storage:device_placement",
        "//src/storage:local_metadata",
        "//src/storage:local_store_config",
        "//src/storage:local_types",
        "//src/storage:space_sampler",
    ],
)

kwaque_cc_seastar_test(
    name = "space_sampler_test",
    srcs = ["space_sampler_test.cc"],
    memory = "256MiB",
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/model:identity",
        "//src/runtime:error",
        "//src/runtime:file",
        "//src/runtime:time",
        "//src/runtime/production:clocks",
        "//src/runtime/production:timer",
        "//src/storage:device_placement",
        "//src/storage:local_metadata",
        "//src/storage:local_paths",
        "//src/storage:local_store_config",
        "//src/storage:local_types",
        "//src/storage:space_sampler",
    ],
)

kwaque_cc_test(
    name = "cache_policy_test",
    srcs = ["cache_policy_test.cc"],
    deps = [
        "//src/storage:cache_policy",
    ],
)

kwaque_cc_seastar_test(
    name = "page_cache_test",
    srcs = ["page_cache_test.cc"],
    memory = "256MiB",
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/bytes:allocation_test_profile",
        "//src/resource:resource_config",
        "//src/resource:resource_manager",
        "//src/resource:resource_registry",
        "//src/resource:workload_class",
        "//src/runtime:error",
        "//src/storage:page_cache",
        "//src/storage:workload_budget",
    ],
)

kwaque_cc_seastar_test(
    name = "completion_resources_test",
    srcs = ["completion_resources_test.cc"],
    memory = "256MiB",
    deps = [
        ":completion_contract",
        "//src/base",
        "//src/base:error",
        "//src/bytes:allocation_test_profile",
        "//src/resource:resource_config",
        "//src/resource:resource_manager",
        "//src/resource:resource_registry",
        "//src/resource:workload_class",
        "//src/runtime:cross_shard",
        "//src/runtime:error",
        "//src/runtime:file",
        "//src/runtime/production:file",
        "//src/runtime/testing:test_directory",
        "//src/storage:completion_resources",
        "//src/storage:workload_budget",
    ],
)

kwaque_cc_test(
    name = "local_types_test",
    srcs = ["local_types_test.cc"],
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/codec:digest",
        "//src/codec:limits",
        "//src/model:epoch",
        "//src/model:identity",
        "//src/runtime:file_position",
        "//src/storage:format_context",
        "//src/storage:format_size",
        "//src/storage:local_metadata_layout",
        "//src/storage:local_types",
        "//src/storage:page_ref",
    ],
)

kwaque_cc_seastar_gtest(
    name = "local_metadata_test",
    srcs = ["local_metadata_test.cc"],
    deps = [
        ":storage_test_support",
        "//src/base",
        "//src/base:error",
        "//src/bytes:allocation_test_profile",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:cooperative",
        "//src/codec:digest",
        "//src/codec:envelope",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/model:batch_identity",
        "//src/model:epoch",
        "//src/model:identity",
        "//src/model:position",
        "//src/runtime:file_position",
        "//src/runtime:time",
        "//src/storage:completed_retry",
        "//src/storage:format_context",
        "//src/storage:local_metadata",
        "//src/storage:local_metadata_layout",
        "//src/storage:local_types",
        "//src/storage:page_ref",
    ],
)

filegroup(
    name = "local_metadata_fixture_data",
    srcs = glob([
        "testdata/local_metadata/*.hex",
        "testdata/local_metadata/*.json",
    ]),
    visibility = [
        "//src/broker:__pkg__",
        "//src/simulation/tests:__pkg__",
        "//tools:__pkg__",
    ],
)

kwaque_cc_library(
    name = "local_publication_contract",
    testonly = True,
    hdrs = ["local_publication_contract.h"],
    visibility = ["//src/simulation/tests:__pkg__"],
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/runtime:error",
        "//src/runtime:file",
        "//src/runtime:first_failure",
        "//src/runtime/testing/contracts:file_system_contract",
        "//src/storage:local_paths",
        "//src/storage:local_publication",
        "//src/storage:local_types",
        "//src/storage:workload_budget",
        "@seastar",
    ],
)

kwaque_cc_test(
    name = "local_paths_test",
    srcs = ["local_paths_test.cc"],
    deps = [
        "//src/model:epoch",
        "//src/model:identity",
        "//src/runtime:file",
        "//src/storage:local_paths",
        "//src/storage:local_types",
    ],
)

kwaque_cc_seastar_test(
    name = "local_publication_test",
    srcs = ["local_publication_test.cc"],
    deps = [
        ":local_publication_contract",
        "//src/base",
        "//src/base:error",
        "//src/bytes:allocation_test_profile",
        "//src/codec:cooperative",
        "//src/resource:resource_manager",
        "//src/resource:resource_registry",
        "//src/resource:workload_class",
        "//src/runtime:file",
        "//src/runtime/production:file",
        "//src/runtime/testing:test_directory",
        "//src/storage:local_paths",
        "//src/storage:workload_budget",
    ],
)

kwaque_cc_seastar_gtest(
    name = "local_metadata_fixture_test",
    srcs = [
        "local_metadata_fixture_test.cc",
    ],
    data = [":local_metadata_fixture_data"],
    deps = [
        ":local_metadata_fixture",
        ":local_metadata_fixture_expectation",
        ":storage_test_support",
        "//src/base",
        "//src/base:error",
        "//src/bytes:allocation_test_profile",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:cooperative",
        "//src/codec:digest",
        "//src/codec:envelope",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/model:batch_identity",
        "//src/model:epoch",
        "//src/model:identity",
        "//src/model:position",
        "//src/runtime:file_position",
        "//src/runtime:time",
        "//src/storage:completed_retry",
        "//src/storage:format_context",
        "//src/storage:local_metadata",
        "//src/storage:local_metadata_layout",
        "//src/storage:local_types",
        "//src/storage:page_ref",
    ],
)

kwaque_cc_library(
    name = "local_metadata_fixture",
    testonly = True,
    hdrs = [
        "local_metadata_fixture.h",
        "local_metadata_fixture_cases.h",
    ],
    deps = ["//src/base:error"],
)

kwaque_cc_library(
    name = "local_metadata_fixture_expectation",
    testonly = True,
    hdrs = ["local_metadata_fixture_expectation.h"],
    deps = [
        ":local_metadata_fixture",
        ":storage_test_support",
        "//src/codec:digest",
        "//src/storage:local_metadata",
    ],
)

kwaque_cc_library(
    name = "local_metadata_fuzz_cases",
    testonly = True,
    srcs = ["local_metadata_fuzz_cases.cc"],
    hdrs = ["local_metadata_fuzz_cases.h"],
    deps = [
        ":local_metadata_fixture",
        ":local_metadata_fixture_expectation",
        ":storage_test_support",
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:cooperative",
        "//src/codec:digest",
        "//src/codec:limits",
        "//src/runtime/testing:fuzz_verdict",
        "//src/storage:local_metadata",
        "//src/storage:page_ref",
        "@seastar",
    ],
)

kwaque_cc_seastar_gtest(
    name = "local_metadata_fuzz_cases_test",
    timeout = "moderate",
    srcs = ["local_metadata_fuzz_cases_test.cc"],
    data = [":local_metadata_fixture_data"] + glob(["testdata/local_metadata_fuzz_corpus/*.bin"]),
    memory = "256MiB",
    deps = [
        ":local_metadata_fixture",
        ":local_metadata_fuzz_cases",
        ":storage_test_support",
        "//src/base:error",
    ],
)

kwaque_cc_fuzz_test(
    name = "local_metadata_fuzz",
    srcs = ["local_metadata_fuzz.cc"],
    args = [
        "-max_len=16384",
        "-rss_limit_mb=1024",
    ],
    corpus = glob(["testdata/local_metadata_fuzz_corpus/*.bin"]),
    data = [":local_metadata_fixture_data"],
    tags = [
        "fuzz-campaign",
        "resources:cpu:1",
        "resources:memory:1024",
    ],
    deps = [
        ":local_metadata_fuzz_cases",
        "//src/runtime/testing:seastar_fuzz",
    ],
)

kwaque_cc_library(
    name = "local_store_contract",
    testonly = True,
    hdrs = ["local_store_contract.h"],
    visibility = [
        "//src/broker:__pkg__",
        "//src/simulation/tests:__pkg__",
    ],
    deps = [
        ":local_metadata_fixture",
        ":local_publication_contract",
        ":storage_test_support",
        "//src/base",
        "//src/base:error",
        "//src/bytes:allocation_test_profile",
        "//src/bytes:fragmented_buffer",
        "//src/codec:cooperative",
        "//src/runtime:file",
        "//src/runtime:first_failure",
        "//src/storage:local_control",
        "//src/storage:local_metadata",
        "//src/storage:local_paths",
        "//src/storage:local_publication",
        "//src/storage:local_store_bootstrap",
        "//src/storage:local_store_config",
        "//src/storage:local_store_inspection",
        "//src/storage:workload_budget",
        "@seastar",
    ],
)

kwaque_cc_test(
    name = "local_store_config_test",
    srcs = ["local_store_config_test.cc"],
    deps = [
        "//src/base:error",
        "//src/model:identity",
        "//src/runtime:file",
        "//src/storage:local_metadata",
        "//src/storage:local_store_config",
        "//src/storage:local_types",
    ],
)

kwaque_cc_library(
    name = "local_installation_contract",
    testonly = True,
    hdrs = [
        "local_installation_contract.h",
        "sparse_index_test_support.h",
    ],
    visibility = [
        "//src/broker:__pkg__",
        "//src/simulation/tests:__pkg__",
    ],
    deps = [
        ":local_metadata_fixture",
        ":local_store_contract",
        ":storage_test_support",
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_builder",
        "//src/codec:cooperative",
        "//src/codec:digest",
        "//src/runtime:file",
        "//src/runtime:first_failure",
        "//src/storage:format_context",
        "//src/storage:local_bundle",
        "//src/storage:local_control",
        "//src/storage:local_id_allocator",
        "//src/storage:local_metadata",
        "//src/storage:local_paths",
        "//src/storage:local_publication",
        "//src/storage:local_segment",
        "//src/storage:local_store_bootstrap",
        "//src/storage:local_store_config",
        "//src/storage:local_types",
        "//src/storage:page_ref",
        "//src/storage:segment_format",
        "//src/storage:sparse_index_format",
        "//src/storage:workload_budget",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "wal_scan_contract",
    testonly = True,
    hdrs = ["wal_scan_contract.h"],
    visibility = [
        "//src/broker:__pkg__",
        "//src/simulation/tests:__pkg__",
    ],
    deps = [
        ":local_reader_contract",
        "//src/base",
        "//src/storage:storage_scan",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "segment_scan_contract",
    testonly = True,
    hdrs = ["segment_scan_contract.h"],
    visibility = [
        "//src/broker:__pkg__",
        "//src/simulation/tests:__pkg__",
    ],
    deps = [
        ":storage_test_support",
        ":wal_scan_contract",
        "//src/base",
        "//src/storage:recovery",
        "//src/storage:storage_scan",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "recovery_contract",
    testonly = True,
    hdrs = ["recovery_contract.h"],
    visibility = [
        "//src/broker:__pkg__",
        "//src/simulation/tests:__pkg__",
    ],
    deps = [
        ":segment_scan_contract",
        ":storage_test_support",
        "//src/storage:recovery",
        "//src/storage:sparse_index",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "checkpoint_coverage_oracle",
    testonly = True,
    hdrs = ["checkpoint_coverage_oracle.h"],
    visibility = [
        "//src/broker:__pkg__",
        "//src/simulation/tests:__pkg__",
    ],
    deps = [
        "//src/runtime:file_position",
        "//src/storage:format_context",
        "//src/storage:local_metadata",
        "//src/storage:local_types",
    ],
)

kwaque_cc_library(
    name = "recovery_successor_contract",
    testonly = True,
    hdrs = ["recovery_successor_contract.h"],
    visibility = [
        "//src/broker:__pkg__",
        "//src/simulation/tests:__pkg__",
    ],
    deps = [
        ":checkpoint_coverage_oracle",
        ":local_append_contract",
        "//src/base",
        "//src/bytes:allocation_test_profile",
        "//src/storage:checkpoint",
        "//src/storage:recovery",
        "//src/storage:retry_lookup",
        "//src/storage:retry_snapshot",
        "//src/storage:wal_reclaimer",
        "@seastar",
    ],
)

kwaque_cc_seastar_test(
    name = "checkpoint_test",
    srcs = ["checkpoint_test.cc"],
    memory = "256MiB",
    deps = [
        ":checkpoint_coverage_oracle",
        "//src/base",
        "//src/base:error",
        "//src/bytes:allocation_test_profile",
        "//src/codec:cooperative",
        "//src/codec:digest",
        "//src/codec:limits",
        "//src/model:epoch",
        "//src/model:identity",
        "//src/model:position",
        "//src/resource:resource_config",
        "//src/resource:resource_manager",
        "//src/resource:resource_registry",
        "//src/resource:workload_class",
        "//src/runtime:error",
        "//src/runtime:file_position",
        "//src/storage:checkpoint",
        "//src/storage:footer_format",
        "//src/storage:format_context",
        "//src/storage:local_append",
        "//src/storage:local_bundle",
        "//src/storage:local_metadata",
        "//src/storage:local_metadata_layout",
        "//src/storage:local_store_config",
        "//src/storage:local_types",
        "//src/storage:page_ref",
        "//src/storage:recovery",
        "//src/storage:wal_reclaimer",
        "//src/storage:workload_budget",
    ],
)

kwaque_cc_seastar_test(
    name = "retry_snapshot_test",
    srcs = ["retry_snapshot_test.cc"],
    memory = "256MiB",
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/bytes:allocation_test_profile",
        "//src/codec:cooperative",
        "//src/codec:digest",
        "//src/codec:limits",
        "//src/model:batch_identity",
        "//src/model:epoch",
        "//src/model:identity",
        "//src/model:position",
        "//src/resource:resource_config",
        "//src/resource:resource_manager",
        "//src/resource:resource_registry",
        "//src/resource:workload_class",
        "//src/runtime:error",
        "//src/runtime:file_position",
        "//src/storage:completed_retry",
        "//src/storage:format_context",
        "//src/storage:local_bundle",
        "//src/storage:local_metadata",
        "//src/storage:local_metadata_layout",
        "//src/storage:local_store_config",
        "//src/storage:local_types",
        "//src/storage:page_ref",
        "//src/storage:retry_lookup",
        "//src/storage:retry_snapshot",
        "//src/storage:workload_budget",
    ],
)

kwaque_cc_library(
    name = "local_reader_contract",
    testonly = True,
    hdrs = ["local_reader_contract.h"],
    visibility = [
        "//src/broker:__pkg__",
        "//src/simulation/tests:__pkg__",
    ],
    deps = [
        ":local_installation_contract",
        ":local_metadata_fixture",
        ":local_store_contract",
        ":storage_test_support",
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/codec:cooperative",
        "//src/codec:digest",
        "//src/runtime:file",
        "//src/runtime:first_failure",
        "//src/storage:footer_format",
        "//src/storage:format_context",
        "//src/storage:local_bundle",
        "//src/storage:local_cleanup",
        "//src/storage:local_discovery",
        "//src/storage:local_generation",
        "//src/storage:local_metadata",
        "//src/storage:local_metadata_file",
        "//src/storage:local_namespace",
        "//src/storage:local_paths",
        "//src/storage:local_publication",
        "//src/storage:local_root",
        "//src/storage:local_segment",
        "//src/storage:local_store_bootstrap",
        "//src/storage:local_store_config",
        "//src/storage:local_types",
        "//src/storage:page_ref",
        "//src/storage:sparse_index_format",
        "//src/storage:workload_budget",
        "@seastar",
    ],
)

kwaque_cc_test(
    name = "local_cleanup_test",
    srcs = ["local_cleanup_test.cc"],
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/model:identity",
        "//src/runtime:file",
        "//src/storage:local_cleanup",
        "//src/storage:local_discovery",
        "//src/storage:local_metadata",
        "//src/storage:local_namespace",
        "//src/storage:local_paths",
        "//src/storage:local_types",
    ],
)

kwaque_cc_library(
    name = "local_qualification_contract",
    testonly = True,
    hdrs = ["local_qualification_contract.h"],
    visibility = [
        "//src/broker:__pkg__",
        "//src/simulation/tests:__pkg__",
    ],
    deps = [
        ":local_reader_contract",
        "//src/base",
        "@seastar",
    ],
)

kwaque_cc_binary(
    name = "memory_qualification_probe",
    testonly = True,
    srcs = [
        "memory_qualification_extent.cc",
        "memory_qualification_format.cc",
        "memory_qualification_metadata.cc",
        "memory_qualification_probe.cc",
        "memory_qualification_probe.h",
        "memory_qualification_segment.cc",
    ],
    linkopts = ALLOCATION_OBSERVER_LINKOPTS,
    deps = [
        ":local_store_contract",
        ":segment_qualification_contract",
        ":storage_large_fixture",
        ":storage_test_support",
        "//src/base",
        "//src/base:error",
        "//src/bytes:allocation_test_profile",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:cooperative",
        "//src/codec:digest",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/codec/tests:allocation_observer",
        "//src/codec/tests:codec_bench_reference",
        "//src/codec/tests:memory_qualification_support",
        "//src/codec/tests:prepared_abort_source",
        "//src/model:batch_codec",
        "//src/model:batch_context",
        "//src/model:batch_identity",
        "//src/model:position",
        "//src/resource:resource_manager",
        "//src/resource:resource_registry",
        "//src/resource:workload_class",
        "//src/runtime:file",
        "//src/runtime:file_position",
        "//src/runtime/production:clocks",
        "//src/runtime/production:file",
        "//src/runtime/testing:test_directory",
        "//src/storage:completed_retry",
        "//src/storage:encoded_batch",
        "//src/storage:footer_format",
        "//src/storage:format_context",
        "//src/storage:page_ref",
        "//src/storage:range_manifest_format",
        "//src/storage:segment_format",
        "//src/storage:sparse_index_format",
        "//src/storage:wal_format",
        "//src/storage:workload_budget",
        "@boost//:program_options",
        "@seastar",
    ],
)

kwaque_py_native_test(
    name = "memory_qualification_test",
    timeout = "long",
    srcs = ["memory_qualification_test.py"],
    data = [
        ":memory_qualification_probe",
        "//src/codec/tests:memory_qualification_probe",
    ],
    tags = [
        "resources:cpu:1",
        "resources:memory:256",
    ],
    deps = ["//src/codec/tests:memory_qualification_driver"],
)

kwaque_cc_library(
    name = "wal_writer_contract",
    testonly = True,
    hdrs = ["wal_writer_contract.h"],
    visibility = [
        "//src/broker:__pkg__",
        "//src/simulation/tests:__pkg__",
    ],
    deps = [
        ":local_installation_contract",
        ":local_store_contract",
        ":storage_test_support",
        "//src/base",
        "//src/base:error",
        "//src/bytes:allocation_test_profile",
        "//src/bytes:fragmented_buffer",
        "//src/codec:cooperative",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/model:batch_context",
        "//src/model:batch_identity",
        "//src/model:epoch",
        "//src/model:identity",
        "//src/runtime:file",
        "//src/runtime:first_failure",
        "//src/storage:encoded_batch",
        "//src/storage:local_control",
        "//src/storage:local_id_allocator",
        "//src/storage:local_paths",
        "//src/storage:local_types",
        "//src/storage:wal_child",
        "//src/storage:wal_writer",
        "//src/storage:workload_budget",
        "@seastar",
    ],
)

kwaque_cc_seastar_test(
    name = "wal_writer_state_test",
    srcs = ["wal_writer_state_test.cc"],
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/bytes:allocation_test_profile",
        "//src/bytes:fragmented_buffer",
        "//src/codec:limits",
        "//src/model:position",
        "//src/resource:resource_manager",
        "//src/resource:resource_registry",
        "//src/resource:workload_class",
        "//src/runtime:error",
        "//src/runtime:first_failure",
        "//src/storage:wal_writer_state",
        "//src/storage:workload_budget",
    ],
)

kwaque_cc_library(
    name = "wal_append_contract",
    testonly = True,
    hdrs = ["wal_append_contract.h"],
    visibility = [
        "//src/broker:__pkg__",
        "//src/simulation/tests:__pkg__",
    ],
    deps = [
        ":local_installation_contract",
        ":local_store_contract",
        ":storage_test_support",
        ":wal_writer_contract",
        "//src/base",
        "//src/base:error",
        "//src/codec:cooperative",
        "//src/codec:limits",
        "//src/model:epoch",
        "//src/runtime:file",
        "//src/runtime:first_failure",
        "//src/storage:local_control",
        "//src/storage:local_id_allocator",
        "//src/storage:local_paths",
        "//src/storage:wal_group",
        "//src/storage:wal_writer",
        "//src/storage:workload_budget",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "wal_group_commit_contract",
    testonly = True,
    hdrs = ["wal_group_commit_contract.h"],
    visibility = [
        "//src/broker:__pkg__",
        "//src/simulation/tests:__pkg__",
    ],
    deps = [
        ":local_store_contract",
        ":storage_test_support",
        ":wal_append_contract",
        ":wal_writer_contract",
        "//src/base",
        "//src/base:error",
        "//src/codec:cooperative",
        "//src/runtime:error",
        "//src/runtime:first_failure",
        "//src/runtime:time",
        "//src/runtime/testing:reactor_tasks",
        "//src/storage:local_store_config",
        "//src/storage:wal_group",
        "//src/storage:wal_group_commit",
        "//src/storage:wal_writer",
        "//src/storage:workload_budget",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "wal_durability_contract",
    testonly = True,
    hdrs = ["wal_durability_contract.h"],
    visibility = [
        "//src/broker:__pkg__",
        "//src/simulation/tests:__pkg__",
    ],
    deps = [
        ":local_store_contract",
        ":storage_test_support",
        ":wal_append_contract",
        ":wal_group_commit_contract",
        ":wal_writer_contract",
        "//src/base",
        "//src/base:error",
        "//src/runtime:first_failure",
        "//src/runtime:time",
        "//src/storage:wal_group_commit",
        "//src/storage:workload_budget",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "wal_commit_lifecycle_contract",
    testonly = True,
    hdrs = ["wal_commit_lifecycle_contract.h"],
    visibility = [
        "//src/broker:__pkg__",
        "//src/simulation/tests:__pkg__",
    ],
    deps = [
        ":local_store_contract",
        ":storage_test_support",
        ":wal_append_contract",
        ":wal_group_commit_contract",
        ":wal_writer_contract",
        "//src/base",
        "//src/base:error",
        "//src/runtime:error",
        "//src/runtime:first_failure",
        "//src/runtime:time",
        "//src/storage:wal_group_commit",
        "//src/storage:workload_budget",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "wal_commit_qualification_contract",
    testonly = True,
    hdrs = ["wal_commit_qualification_contract.h"],
    visibility = [
        "//src/broker:__pkg__",
        "//src/simulation/tests:__pkg__",
    ],
    deps = [
        ":local_store_contract",
        ":storage_test_support",
        ":wal_append_contract",
        ":wal_group_commit_contract",
        ":wal_writer_contract",
        "//src/base",
        "//src/base:error",
        "//src/runtime:error",
        "//src/runtime:first_failure",
        "//src/runtime:time",
        "//src/storage:wal_group_commit",
        "//src/storage:workload_budget",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "wal_rotation_contract",
    testonly = True,
    hdrs = ["wal_rotation_contract.h"],
    visibility = [
        "//src/broker:__pkg__",
        "//src/simulation/tests:__pkg__",
    ],
    deps = [
        ":local_installation_contract",
        ":local_store_contract",
        ":storage_test_support",
        ":wal_append_contract",
        ":wal_writer_contract",
        "//src/base",
        "//src/base:error",
        "//src/codec:cooperative",
        "//src/codec:limits",
        "//src/model:epoch",
        "//src/runtime:file",
        "//src/runtime:first_failure",
        "//src/storage:local_control",
        "//src/storage:local_id_allocator",
        "//src/storage:local_metadata",
        "//src/storage:local_paths",
        "//src/storage:local_types",
        "//src/storage:wal_group",
        "//src/storage:wal_writer",
        "//src/storage:workload_budget",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "wal_lifecycle_contract",
    testonly = True,
    hdrs = ["wal_lifecycle_contract.h"],
    visibility = [
        "//src/broker:__pkg__",
        "//src/simulation/tests:__pkg__",
    ],
    deps = [
        ":local_installation_contract",
        ":local_store_contract",
        ":storage_test_support",
        ":wal_append_contract",
        ":wal_rotation_contract",
        ":wal_writer_contract",
        "//src/base",
        "//src/base:error",
        "//src/codec:cooperative",
        "//src/codec:limits",
        "//src/model:epoch",
        "//src/runtime:file",
        "//src/runtime:first_failure",
        "//src/storage:local_cleanup",
        "//src/storage:local_control",
        "//src/storage:local_discovery",
        "//src/storage:local_id_allocator",
        "//src/storage:local_metadata",
        "//src/storage:local_namespace",
        "//src/storage:local_paths",
        "//src/storage:local_types",
        "//src/storage:wal_group",
        "//src/storage:wal_inventory",
        "//src/storage:wal_writer",
        "//src/storage:workload_budget",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "wal_survival_oracle",
    testonly = True,
    hdrs = ["wal_survival_oracle.h"],
    visibility = ["//src/simulation/tests:__pkg__"],
    deps = [":wal_append_contract"],
)

kwaque_cc_library(
    name = "wal_qualification_contract",
    testonly = True,
    hdrs = ["wal_qualification_contract.h"],
    visibility = [
        "//src/broker:__pkg__",
        "//src/simulation/tests:__pkg__",
    ],
    deps = [
        ":wal_lifecycle_contract",
        "//src/base",
        "//src/storage:wal_writer_state",
        "@seastar",
    ],
)

_WAL_BENCH_DEPS = [
    "//src/codec:xxh3",
    ":local_installation_contract",
    ":local_store_contract",
    ":storage_large_fixture",
    ":storage_test_support",
    ":wal_append_contract",
    ":wal_bench_file",
    ":wal_writer_contract",
    "//src/base",
    "//src/base:error",
    "//src/bytes:allocation_test_profile",
    "//src/bytes:fragmented_buffer",
    "//src/codec:cooperative",
    "//src/codec:limits",
    "//src/codec/tests:allocation_observer",
    "//src/codec/tests:codec_bench_reference",
    "//src/codec/tests:memory_qualification_support",
    "//src/model:batch_builder",
    "//src/model:record_codec",
    "//src/resource:resource_manager",
    "//src/resource:resource_registry",
    "//src/resource:workload_class",
    "//src/runtime:file",
    "//src/runtime:operation_statistics",
    "//src/runtime/production:file",
    "//src/runtime/production:clocks",
    "//src/runtime/production:timer",
    "//src/runtime/testing:test_directory",
    "//src/storage:encoded_batch",
    "//src/storage:local_paths",
    "//src/storage:wal_child",
    "//src/storage:wal_format",
    "//src/storage:wal_group",
    "//src/storage:wal_group_commit",
    "//src/storage:wal_writer",
    "//src/storage:workload_budget",
    "@seastar",
]

kwaque_cc_benchmark(
    name = "wal_bench",
    srcs = [
        "wal_bench.cc",
        "wal_bench_child.h",
        "wal_commit_bench.cc",
    ],
    linkopts = ALLOCATION_OBSERVER_LINKOPTS,
    memory = "1GiB",
    deps = _WAL_BENCH_DEPS,
)

kwaque_cc_benchmark(
    name = "wal_timing_bench",
    srcs = [
        "wal_bench.cc",
        "wal_bench_child.h",
        "wal_commit_bench.cc",
    ],
    local_defines = ["KWAQUE_WAL_TIMING_ONLY"],
    memory = "1GiB",
    deps = _WAL_BENCH_DEPS,
)

py_binary(
    name = "wal_cohort_report",
    srcs = ["wal_cohort_report.py"],
    main = "wal_cohort_report.py",
)

py_test(
    name = "wal_cohort_report_test",
    srcs = [
        "wal_cohort_report.py",
        "wal_cohort_report_test.py",
    ],
    main = "wal_cohort_report_test.py",
)

py_binary(
    name = "wal_saturation_report",
    srcs = [
        "wal_cohort_report.py",
        "wal_saturation_report.py",
    ],
    main = "wal_saturation_report.py",
)

py_test(
    name = "wal_saturation_report_test",
    srcs = [
        "wal_cohort_report.py",
        "wal_saturation_report.py",
        "wal_saturation_report_test.py",
    ],
    main = "wal_saturation_report_test.py",
)

kwaque_py_native_test(
    name = "wal_bench_pairs_test",
    timeout = "moderate",
    srcs = ["bench_pairs_test.py"],
    args = [
        "$(rootpath :wal_timing_bench)",
        "$(rootpath wal_bench_pairs.txt)",
    ],
    data = [
        "wal_bench_pairs.txt",
        ":wal_timing_bench",
    ],
    main = "bench_pairs_test.py",
    tags = [
        "benchmark",
        "resources:cpu:1",
        "resources:memory:128",
    ],
)

kwaque_cc_library(
    name = "segment_writer_contract",
    testonly = True,
    hdrs = ["segment_writer_contract.h"],
    visibility = [
        "//src/broker:__pkg__",
        "//src/simulation/tests:__pkg__",
    ],
    deps = [
        ":local_installation_contract",
        ":local_metadata_fixture",
        ":local_store_contract",
        ":storage_test_support",
        "//src/base",
        "//src/base:error",
        "//src/codec:cooperative",
        "//src/codec:crc32c",
        "//src/model:segment_state",
        "//src/runtime:file",
        "//src/runtime:first_failure",
        "//src/storage:encoded_batch",
        "//src/storage:footer_format",
        "//src/storage:local_bundle",
        "//src/storage:local_generation",
        "//src/storage:local_metadata",
        "//src/storage:local_paths",
        "//src/storage:local_publication",
        "//src/storage:local_segment",
        "//src/storage:segment_group",
        "//src/storage:segment_writer",
        "//src/storage:segment_writer_state",
        "//src/storage:wal_child",
        "//src/storage:workload_budget",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "segment_reader_contract",
    testonly = True,
    hdrs = ["segment_reader_contract.h"],
    visibility = [
        "//src/broker:__pkg__",
        "//src/simulation/tests:__pkg__",
    ],
    deps = [
        ":segment_writer_contract",
        ":sparse_index_contract",
        "//src/base",
        "//src/bytes:fragmented_buffer",
        "//src/model:batch",
        "//src/model:batch_builder",
        "//src/model:batch_identity",
        "//src/model:record_codec",
        "//src/runtime:time",
        "//src/storage:encoded_batch",
        "//src/storage:local_generation",
        "//src/storage:page_cache",
        "//src/storage:segment_reader",
        "//src/storage:segment_run",
        "//src/storage:sparse_index",
        "//src/storage:sparse_index_owner",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "sparse_index_contract",
    testonly = True,
    hdrs = ["sparse_index_contract.h"],
    visibility = [
        "//src/broker:__pkg__",
        "//src/simulation/tests:__pkg__",
    ],
    deps = [
        ":local_installation_contract",
        ":local_store_contract",
        ":segment_writer_contract",
        "//src/base",
        "//src/base:error",
        "//src/codec:cooperative",
        "//src/codec:digest",
        "//src/runtime:file",
        "//src/runtime:file_position",
        "//src/runtime:first_failure",
        "//src/storage:footer_format",
        "//src/storage:local_bundle",
        "//src/storage:local_generation",
        "//src/storage:local_metadata",
        "//src/storage:local_paths",
        "//src/storage:local_root",
        "//src/storage:local_types",
        "//src/storage:page_cache",
        "//src/storage:page_ref",
        "//src/storage:recovery",
        "//src/storage:retry_snapshot",
        "//src/storage:segment_format",
        "//src/storage:segment_group",
        "//src/storage:segment_writer",
        "//src/storage:segment_writer_state",
        "//src/storage:sparse_index",
        "//src/storage:sparse_index_format",
        "//src/storage:sparse_index_owner",
        "//src/storage:storage_scan",
        "//src/storage:workload_budget",
        "@seastar",
    ],
)

kwaque_cc_seastar_test(
    name = "segment_writer_state_test",
    srcs = ["segment_writer_state_test.cc"],
    deps = [
        ":local_installation_contract",
        ":storage_test_support",
        "//src/base",
        "//src/base:error",
        "//src/codec:cooperative",
        "//src/codec:limits",
        "//src/storage:encoded_batch",
        "//src/storage:segment_writer_state",
    ],
)

kwaque_cc_library(
    name = "local_append_contract",
    testonly = True,
    hdrs = ["local_append_contract.h"],
    visibility = [
        "//src/broker:__pkg__",
        "//src/simulation/tests:__pkg__",
    ],
    deps = [
        ":local_installation_contract",
        ":local_store_contract",
        ":segment_writer_contract",
        ":storage_test_support",
        ":wal_writer_contract",
        "//src/base",
        "//src/base:error",
        "//src/codec:cooperative",
        "//src/codec:digest",
        "//src/codec:xxh3",
        "//src/model:epoch",
        "//src/runtime:file",
        "//src/runtime:first_failure",
        "//src/runtime:time",
        "//src/storage:encoded_batch",
        "//src/storage:format_context",
        "//src/storage:local_append",
        "//src/storage:local_control",
        "//src/storage:local_id_allocator",
        "//src/storage:local_paths",
        "//src/storage:segment_writer",
        "//src/storage:segment_writer_state",
        "//src/storage:wal_group_commit",
        "//src/storage:wal_writer",
        "//src/storage:workload_budget",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "segment_lifecycle_contract",
    testonly = True,
    hdrs = ["segment_lifecycle_contract.h"],
    visibility = [
        "//src/broker:__pkg__",
        "//src/simulation/tests:__pkg__",
    ],
    deps = [
        ":local_append_contract",
        ":local_store_contract",
        ":segment_reader_contract",
        ":segment_writer_contract",
        ":sparse_index_contract",
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/codec:cooperative",
        "//src/codec:limits",
        "//src/model:position",
        "//src/runtime:file",
        "//src/runtime:file_error",
        "//src/runtime:file_flight",
        "//src/runtime:file_position",
        "//src/runtime:first_failure",
        "//src/runtime:time",
        "//src/storage:device_placement",
        "//src/storage:format_context",
        "//src/storage:local_append",
        "//src/storage:local_paths",
        "//src/storage:local_store_config",
        "//src/storage:page_cache",
        "//src/storage:segment_lifecycle",
        "//src/storage:segment_reader",
        "//src/storage:segment_writer",
        "//src/storage:segment_writer_state",
        "//src/storage:sparse_index",
        "//src/storage:sparse_index_owner",
        "//src/storage:workload_budget",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "engine_ledger",
    testonly = True,
    hdrs = ["engine_ledger.h"],
    visibility = [
        "//src/broker:__pkg__",
        "//src/simulation/tests:__pkg__",
    ],
)

kwaque_cc_test(
    name = "engine_ledger_test",
    srcs = ["engine_ledger_test.cc"],
    deps = [":engine_ledger"],
)

kwaque_cc_library(
    name = "engine_history",
    testonly = True,
    hdrs = [
        "choice_stack.h",
        "engine_history.h",
    ],
    visibility = [
        "//src/broker:__pkg__",
        "//src/simulation/tests:__pkg__",
    ],
)

kwaque_cc_test(
    name = "engine_history_test",
    srcs = ["engine_history_test.cc"],
    deps = [":engine_history"],
)

kwaque_cc_library(
    name = "engine_history_contract",
    testonly = True,
    hdrs = ["engine_history_contract.h"],
    visibility = [
        "//src/broker:__pkg__",
        "//src/simulation/tests:__pkg__",
    ],
    deps = [
        ":engine_history",
        ":engine_ledger",
        ":local_append_contract",
        ":local_engine_contract",
        ":local_installation_contract",
        ":local_store_contract",
        ":segment_lifecycle_contract",
        ":storage_test_support",
        "//src/base",
        "//src/codec:cooperative",
        "//src/codec:digest",
        "//src/codec:limits",
        "//src/model:position",
        "//src/runtime:file",
        "//src/runtime:time",
        "//src/storage:device_placement",
        "//src/storage:format_context",
        "//src/storage:local_append",
        "//src/storage:local_engine",
        "//src/storage:local_paths",
        "//src/storage:local_store_config",
        "//src/storage:recovery",
        "//src/storage:retention",
        "//src/storage:segment_lifecycle",
        "//src/storage:segment_reader",
        "//src/storage:workload_budget",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "engine_fixture_contract",
    testonly = True,
    hdrs = ["engine_fixture_contract.h"],
    visibility = [
        "//src/broker:__pkg__",
        "//src/simulation/tests:__pkg__",
    ],
    deps = [
        ":local_engine_contract",
        ":local_installation_contract",
        ":local_metadata_fixture",
        ":local_reader_contract",
        ":local_store_contract",
        ":recovery_contract",
        ":segment_lifecycle_contract",
        ":segment_scan_contract",
        ":storage_test_support",
        ":wal_scan_contract",
        "//src/base",
        "//src/base:error",
        "//src/codec:cooperative",
        "//src/codec:digest",
        "//src/codec:limits",
        "//src/model:position",
        "//src/runtime:file",
        "//src/runtime:time",
        "//src/storage:local_engine",
        "//src/storage:local_paths",
        "//src/storage:local_store_config",
        "//src/storage:local_types",
        "//src/storage:recovery",
        "//src/storage:segment_lifecycle",
        "//src/storage:workload_budget",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "local_engine_contract",
    testonly = True,
    hdrs = ["local_engine_contract.h"],
    visibility = [
        "//src/broker:__pkg__",
        "//src/simulation/tests:__pkg__",
    ],
    deps = [
        ":local_append_contract",
        ":local_installation_contract",
        ":local_store_contract",
        ":recovery_successor_contract",
        ":segment_lifecycle_contract",
        ":segment_writer_contract",
        ":wal_writer_contract",
        "//src/base",
        "//src/codec:cooperative",
        "//src/codec:limits",
        "//src/runtime:time",
        "//src/storage:device_placement",
        "//src/storage:local_engine",
        "//src/storage:local_paths",
        "//src/storage:local_store_config",
        "//src/storage:recovery",
        "//src/storage:retention",
        "//src/storage:segment_format",
        "//src/storage:segment_lifecycle",
        "//src/storage:workload_budget",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "segment_qualification_contract",
    testonly = True,
    hdrs = ["segment_qualification_contract.h"],
    visibility = [
        "//src/broker:__pkg__",
        "//src/simulation/tests:__pkg__",
    ],
    deps = [
        ":local_installation_contract",
        ":local_store_contract",
        ":segment_writer_contract",
        ":storage_test_support",
        "//src/base",
        "//src/base:error",
        "//src/codec:cooperative",
        "//src/codec:crc32c",
        "//src/model:segment_state",
        "//src/runtime:file",
        "//src/runtime:first_failure",
        "//src/runtime:time",
        "//src/storage:local_generation",
        "//src/storage:segment_group",
        "//src/storage:segment_writer",
        "//src/storage:segment_writer_state",
        "//src/storage:wal_child",
        "//src/storage:workload_budget",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "wal_bench_file",
    testonly = True,
    hdrs = ["wal_bench_file.h"],
    deps = [
        "//src/runtime:file",
        "//src/runtime:operation_statistics",
        "//src/runtime/production:file",
        "//src/storage:workload_budget",
        "@seastar",
        "@seastar//:benchmark",
    ],
)

kwaque_cc_library(
    name = "segment_bench_fixture",
    testonly = True,
    srcs = ["segment_bench_fixture.cc"],
    hdrs = ["segment_bench_fixture.h"],
    deps = [
        ":local_installation_contract",
        ":storage_test_support",
        "//src/base",
        "//src/base:error",
        "//src/bytes:allocation_test_profile",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_builder",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:cooperative",
        "//src/codec:integer",
        "//src/codec:xxh3",
        "//src/codec:xxh3_cooperative",
        "//src/codec/tests:codec_bench_reference",
        "//src/model:batch_builder",
        "//src/model:batch_codec",
        "//src/model:record_codec",
        "//src/storage:encoded_batch",
        "//src/storage:footer_format",
        "//src/storage:format_size",
        "//src/storage:local_metadata",
        "//src/storage:segment_format",
        "//src/storage:workload_budget",
        "@crc32c",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "segment_bench_file",
    testonly = True,
    hdrs = ["segment_bench_file.h"],
    deps = [
        ":wal_bench_file",
        "//src/runtime:file",
        "//src/runtime:first_failure",
        "//src/runtime:operation_statistics",
        "//src/runtime/production:file",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "segment_bench_observer_api",
    testonly = True,
    hdrs = ["segment_bench_observer.h"],
)

kwaque_cc_library(
    name = "segment_bench_observer",
    testonly = True,
    srcs = ["segment_bench_observer.cc"],
    deps = [
        ":segment_bench_observer_api",
        "@abseil-cpp//absl/crc:crc32c",
        "@lz4//:lz4_frame",
        "@xxhash",
    ],
)

_SEGMENT_BENCH_DEPS = [
    ":local_installation_contract",
    ":local_store_contract",
    ":segment_bench_file",
    ":segment_bench_fixture",
    ":segment_bench_observer_api",
    ":segment_writer_contract",
    "//src/base",
    "//src/base:error",
    "//src/bytes:allocation_test_profile",
    "//src/bytes:fragmented_buffer_builder",
    "//src/codec:cooperative",
    "//src/codec:xxh3",
    "//src/codec/tests:allocation_observer",
    "//src/codec/tests:codec_bench_reference",
    "//src/codec/tests:memory_qualification_support",
    "//src/resource:resource_manager",
    "//src/resource:resource_registry",
    "//src/resource:workload_class",
    "//src/runtime:file",
    "//src/runtime:first_failure",
    "//src/runtime/production:clocks",
    "//src/runtime/testing:test_directory",
    "//src/storage:local_paths",
    "//src/storage:segment_group",
    "//src/storage:segment_writer",
    "//src/storage:segment_writer_state",
    "//src/storage:workload_budget",
    "@seastar",
]

kwaque_cc_benchmark(
    name = "segment_bench",
    srcs = ["segment_bench.cc"],
    linkopts = ALLOCATION_OBSERVER_LINKOPTS + [
        "-Wl,--wrap=LZ4F_decompress",
        "-Wl,--wrap=_ZN4absl12lts_2026052612crc_internal20ExtendCrc32cInternalENS0_8crc32c_tENSt3__117basic_string_viewIcNS3_11char_traitsIcEEEE",
    ] + select({
        "@platforms//cpu:x86_64": ["-Wl,--wrap=XXH3_128bits_update_dispatch"],
        "//conditions:default": ["-Wl,--wrap=XXH3_128bits_update"],
    }),
    memory = "1GiB",
    native_allocator_only = True,
    deps = _SEGMENT_BENCH_DEPS + [":segment_bench_observer"],
)

kwaque_cc_benchmark(
    name = "segment_timing_bench",
    srcs = ["segment_bench.cc"],
    local_defines = ["KWAQUE_SEGMENT_TIMING_ONLY"],
    memory = "1GiB",
    native_allocator_only = True,
    deps = _SEGMENT_BENCH_DEPS,
)

py_binary(
    name = "segment_bench_report",
    srcs = [
        "segment_bench_report.py",
        "wal_cohort_report.py",
    ],
    main = "segment_bench_report.py",
)

py_test(
    name = "segment_bench_report_test",
    srcs = [
        "segment_bench_report.py",
        "segment_bench_report_test.py",
        "wal_cohort_report.py",
    ],
    main = "segment_bench_report_test.py",
)

kwaque_cc_library(
    name = "local_storage_bench_file",
    testonly = True,
    hdrs = ["local_storage_bench_file.h"],
    deps = [
        ":wal_bench_file",
        "//src/runtime:file",
        "//src/runtime:operation_statistics",
        "//src/runtime/production:file",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "latency_histogram",
    testonly = True,
    hdrs = ["latency_histogram.h"],
    deps = ["@seastar"],
)

kwaque_cc_library(
    name = "engine_bench_fixture",
    testonly = True,
    hdrs = ["engine_bench_fixture.h"],
    deps = [
        ":local_append_contract",
        ":local_engine_contract",
        ":local_storage_bench_file",
        ":local_store_contract",
        ":segment_lifecycle_contract",
        "//src/base",
        "//src/bytes:allocation_test_profile",
        "//src/codec:cooperative",
        "//src/resource:resource_config",
        "//src/resource:resource_manager",
        "//src/resource:resource_registry",
        "//src/resource:workload_class",
        "//src/runtime:file",
        "//src/runtime:first_failure",
        "//src/runtime/production:clocks",
        "//src/runtime/production:timer",
        "//src/storage:device_placement",
        "//src/storage:local_append",
        "//src/storage:local_engine",
        "//src/storage:local_store_bootstrap",
        "//src/storage:local_store_config",
        "//src/storage:recovery",
        "//src/storage:segment_lifecycle",
        "//src/storage:workload_budget",
        "@seastar",
    ],
)

_LOCAL_STORAGE_BENCH_DEPS = [
    ":engine_bench_fixture",
    ":latency_histogram",
    ":local_append_contract",
    ":local_installation_contract",
    ":local_storage_bench_file",
    ":local_store_contract",
    ":segment_bench_fixture",
    ":segment_writer_contract",
    ":storage_test_support",
    ":wal_bench_file",
    ":wal_writer_contract",
    "//src/base",
    "//src/base:error",
    "//src/bytes:allocation_test_profile",
    "//src/codec:cooperative",
    "//src/codec:xxh3",
    "//src/codec/tests:allocation_observer",
    "//src/codec/tests:codec_bench_reference",
    "//src/codec/tests:memory_qualification_support",
    "//src/model:position",
    "//src/resource:resource_manager",
    "//src/resource:resource_registry",
    "//src/resource:workload_class",
    "//src/runtime:file",
    "//src/runtime:first_failure",
    "//src/runtime/production:clocks",
    "//src/runtime/production:timer",
    "//src/runtime/testing:test_directory",
    "//src/storage:local_append",
    "//src/storage:local_engine",
    "//src/storage:local_metadata",
    "//src/storage:local_paths",
    "//src/storage:local_store_config",
    "//src/storage:local_types",
    "//src/storage:retention",
    "//src/storage:segment_lifecycle",
    "//src/storage:segment_writer",
    "//src/storage:segment_writer_state",
    "//src/storage:wal_child",
    "//src/storage:wal_format",
    "//src/storage:wal_group",
    "//src/storage:wal_group_commit",
    "//src/storage:wal_writer",
    "//src/storage:workload_budget",
    "@seastar",
]

kwaque_cc_benchmark(
    name = "local_storage_bench",
    srcs = [
        "local_storage_bench.cc",
        "local_storage_engine_bench.cc",
    ],
    linkopts = ALLOCATION_OBSERVER_LINKOPTS,
    memory = "1GiB",
    deps = _LOCAL_STORAGE_BENCH_DEPS,
)

kwaque_cc_benchmark(
    name = "local_storage_timing_bench",
    srcs = [
        "local_storage_bench.cc",
        "local_storage_engine_bench.cc",
    ],
    local_defines = ["KWAQUE_LOCAL_STORAGE_TIMING_ONLY"],
    memory = "1GiB",
    deps = _LOCAL_STORAGE_BENCH_DEPS,
)

py_binary(
    name = "local_storage_bench_report",
    srcs = [
        "local_storage_bench_report.py",
        "wal_cohort_report.py",
    ],
    main = "local_storage_bench_report.py",
)

py_test(
    name = "local_storage_bench_report_test",
    srcs = [
        "local_storage_bench_report.py",
        "local_storage_bench_report_test.py",
        "wal_cohort_report.py",
    ],
    main = "local_storage_bench_report_test.py",
)

kwaque_py_native_test(
    name = "local_storage_bench_pairs_test",
    timeout = "moderate",
    srcs = ["bench_pairs_test.py"],
    args = [
        "$(rootpath :local_storage_timing_bench)",
        "$(rootpath local_storage_bench_pairs.txt)",
    ],
    data = [
        "local_storage_bench_pairs.txt",
        ":local_storage_timing_bench",
    ],
    main = "bench_pairs_test.py",
    tags = [
        "benchmark",
        "resources:cpu:1",
        "resources:memory:128",
    ],
)

py_binary(
    name = "engine_append_report",
    srcs = [
        "engine_append_report.py",
        "wal_cohort_report.py",
    ],
    main = "engine_append_report.py",
)

py_test(
    name = "engine_append_report_test",
    srcs = [
        "engine_append_report.py",
        "engine_append_report_test.py",
        "wal_cohort_report.py",
    ],
    main = "engine_append_report_test.py",
)

_STORAGE_RECOVERY_BENCH_DEPS = [
    ":local_installation_contract",
    ":local_storage_bench_file",
    ":local_store_contract",
    ":segment_bench_fixture",
    ":segment_bench_observer_api",
    ":segment_writer_contract",
    ":storage_test_support",
    ":wal_writer_contract",
    "//src/base",
    "//src/bytes:allocation_test_profile",
    "//src/codec:cooperative",
    "//src/codec/tests:allocation_observer",
    "//src/codec/tests:memory_qualification_support",
    "//src/resource:resource_manager",
    "//src/resource:resource_registry",
    "//src/resource:workload_class",
    "//src/runtime:file",
    "//src/runtime:first_failure",
    "//src/runtime/production:clocks",
    "//src/runtime/production:timer",
    "//src/runtime/testing:test_directory",
    "//src/storage:local_append",
    "//src/storage:local_paths",
    "//src/storage:recovery",
    "//src/storage:segment_format",
    "//src/storage:segment_writer",
    "//src/storage:wal_group_commit",
    "//src/storage:wal_writer",
    "//src/storage:workload_budget",
    "@seastar",
]

kwaque_cc_benchmark(
    name = "storage_recovery_bench",
    srcs = [
        "engine_start_bench.cc",
        "storage_recovery_bench.cc",
    ],
    linkopts = ALLOCATION_OBSERVER_LINKOPTS + [
        "-Wl,--wrap=LZ4F_decompress",
        "-Wl,--wrap=_ZN4absl12lts_2026052612crc_internal20ExtendCrc32cInternalENS0_8crc32c_tENSt3__117basic_string_viewIcNS3_11char_traitsIcEEEE",
    ] + select({
        "@platforms//cpu:x86_64": ["-Wl,--wrap=XXH3_128bits_update_dispatch"],
        "//conditions:default": ["-Wl,--wrap=XXH3_128bits_update"],
    }),
    memory = "1GiB",
    deps = _STORAGE_RECOVERY_BENCH_DEPS + [
        ":engine_bench_fixture",
        ":local_append_contract",
        ":local_engine_contract",
        ":segment_bench_observer",
        ":wal_bench_file",
        "//src/storage:local_engine",
        "//src/storage:segment_lifecycle",
    ],
)

kwaque_cc_benchmark(
    name = "storage_recovery_timing_bench",
    srcs = [
        "engine_start_bench.cc",
        "storage_recovery_bench.cc",
    ],
    local_defines = ["KWAQUE_STORAGE_RECOVERY_TIMING_ONLY"],
    memory = "1GiB",
    deps = _STORAGE_RECOVERY_BENCH_DEPS + [
        ":engine_bench_fixture",
        ":local_append_contract",
        ":local_engine_contract",
        ":wal_bench_file",
        "//src/storage:local_engine",
        "//src/storage:segment_lifecycle",
    ],
)

kwaque_cc_benchmark(
    name = "checkpoint_bench",
    srcs = [
        "checkpoint_bench.cc",
        "segment_lifecycle_bench.cc",
    ],
    # Counts the bytes hashed and checksummed in a timed region; outside one
    # each wrapped call only checks that no region is open.
    linkopts = [
        "-Wl,--wrap=LZ4F_decompress",
        "-Wl,--wrap=_ZN4absl12lts_2026052612crc_internal20ExtendCrc32cInternalENS0_8crc32c_tENSt3__117basic_string_viewIcNS3_11char_traitsIcEEEE",
    ] + select({
        "@platforms//cpu:x86_64": ["-Wl,--wrap=XXH3_128bits_update_dispatch"],
        "//conditions:default": ["-Wl,--wrap=XXH3_128bits_update"],
    }),
    # Its largest store keeps 32 segments open: the append benchmark's
    # sizing for that many needs more than the produce class has at 1 GiB.
    memory = "2GiB",
    deps = _STORAGE_RECOVERY_BENCH_DEPS + [
        ":engine_bench_fixture",
        ":local_append_contract",
        ":segment_bench_observer",
        ":segment_lifecycle_contract",
        ":wal_bench_file",
        "//src/storage:checkpoint",
        "//src/storage:local_engine",
        "//src/storage:retention",
        "//src/storage:segment_lifecycle",
        "//src/storage:sparse_index",
        "//src/storage:wal_writer_state",
    ],
)

py_binary(
    name = "segment_lifecycle_report",
    srcs = [
        "segment_lifecycle_report.py",
        "wal_cohort_report.py",
    ],
    main = "segment_lifecycle_report.py",
)

py_test(
    name = "segment_lifecycle_report_test",
    srcs = [
        "segment_lifecycle_report.py",
        "segment_lifecycle_report_test.py",
        "wal_cohort_report.py",
    ],
    main = "segment_lifecycle_report_test.py",
)

_INDEX_BENCH_DEPS = [
    ":local_installation_contract",
    ":local_storage_bench_file",
    ":local_store_contract",
    ":segment_bench_fixture",
    ":segment_bench_observer_api",
    ":segment_writer_contract",
    ":wal_bench_file",
    "//src/base",
    "//src/base:error",
    "//src/bytes:allocation_test_profile",
    "//src/bytes:fragmented_buffer",
    "//src/bytes:fragmented_buffer_builder",
    "//src/codec:cooperative",
    "//src/codec:limits",
    "//src/codec/tests:allocation_observer",
    "//src/model:position",
    "//src/resource:resource_manager",
    "//src/resource:resource_registry",
    "//src/resource:workload_class",
    "//src/runtime:error",
    "//src/runtime:file",
    "//src/runtime:file_position",
    "//src/runtime:first_failure",
    "//src/runtime/production:clocks",
    "//src/runtime/testing:test_directory",
    "//src/storage:local_bundle",
    "//src/storage:local_generation",
    "//src/storage:local_paths",
    "//src/storage:local_root",
    "//src/storage:page_cache",
    "//src/storage:segment_reader",
    "//src/storage:segment_run",
    "//src/storage:segment_writer",
    "//src/storage:sparse_index",
    "//src/storage:sparse_index_owner",
    "//src/storage:storage_scan",
    "//src/storage:workload_budget",
    "@crc32c",
]

_INDEX_BENCH_SRCS = [
    "index_bench.cc",
    "index_bench_resident.h",
]

kwaque_cc_benchmark(
    name = "index_bench",
    srcs = _INDEX_BENCH_SRCS,
    linkopts = ALLOCATION_OBSERVER_LINKOPTS + [
        "-Wl,--wrap=LZ4F_decompress",
        "-Wl,--wrap=_ZN4absl12lts_2026052612crc_internal20ExtendCrc32cInternalENS0_8crc32c_tENSt3__117basic_string_viewIcNS3_11char_traitsIcEEEE",
    ] + select({
        "@platforms//cpu:x86_64": ["-Wl,--wrap=XXH3_128bits_update_dispatch"],
        "//conditions:default": ["-Wl,--wrap=XXH3_128bits_update"],
    }),
    memory = "1GiB",
    deps = _INDEX_BENCH_DEPS + [":segment_bench_observer"],
)

kwaque_cc_benchmark(
    name = "index_timing_bench",
    srcs = _INDEX_BENCH_SRCS,
    local_defines = ["KWAQUE_INDEX_BENCH_TIMING_ONLY"],
    memory = "1GiB",
    deps = _INDEX_BENCH_DEPS,
)

py_binary(
    name = "index_bench_report",
    srcs = ["index_bench_report.py"],
    main = "index_bench_report.py",
)

py_test(
    name = "index_bench_report_test",
    srcs = [
        "index_bench_report.py",
        "index_bench_report_test.py",
    ],
    main = "index_bench_report_test.py",
)

kwaque_py_native_test(
    name = "index_bench_pairs_test",
    timeout = "moderate",
    srcs = ["bench_pairs_test.py"],
    args = [
        "$(rootpath :index_timing_bench)",
        "$(rootpath index_bench_pairs.txt)",
    ],
    data = [
        "index_bench_pairs.txt",
        ":index_timing_bench",
    ],
    main = "bench_pairs_test.py",
    tags = [
        "benchmark",
        "resources:cpu:1",
        "resources:memory:128",
    ],
)

py_binary(
    name = "read_bench_report",
    srcs = ["read_bench_report.py"],
    main = "read_bench_report.py",
)

py_test(
    name = "read_bench_report_test",
    srcs = [
        "read_bench_report.py",
        "read_bench_report_test.py",
    ],
    main = "read_bench_report_test.py",
)

py_binary(
    name = "storage_recovery_bench_report",
    srcs = [
        "storage_recovery_bench_report.py",
        "wal_cohort_report.py",
    ],
    main = "storage_recovery_bench_report.py",
)

py_test(
    name = "storage_recovery_bench_report_test",
    srcs = [
        "storage_recovery_bench_report.py",
        "storage_recovery_bench_report_test.py",
        "wal_cohort_report.py",
    ],
    main = "storage_recovery_bench_report_test.py",
)

py_binary(
    name = "engine_start_report",
    srcs = [
        "engine_start_report.py",
        "wal_cohort_report.py",
    ],
    main = "engine_start_report.py",
)

py_test(
    name = "engine_start_report_test",
    srcs = [
        "engine_start_report.py",
        "engine_start_report_test.py",
        "wal_cohort_report.py",
    ],
    main = "engine_start_report_test.py",
)
