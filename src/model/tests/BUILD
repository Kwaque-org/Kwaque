load("//bazel:build.bzl", "kwaque_cc_library")
load("//bazel:test.bzl", "kwaque_cc_benchmark", "kwaque_cc_fuzz_test", "kwaque_cc_seastar_gtest", "kwaque_cc_test")
load(":identity_dependency_test.bzl", "identity_dependency_test")

package(default_visibility = ["//visibility:private"])

kwaque_cc_library(
    name = "lineage_model",
    testonly = True,
    srcs = ["lineage_model.cc"],
    hdrs = ["lineage_model.h"],
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/model:checkpoint",
        "//src/model:identity",
        "//src/model:keyspace",
        "//src/model:position",
    ],
)

kwaque_cc_test(
    name = "lineage_model_test",
    srcs = [
        "lineage_model_test.cc",
        "lineage_test_support.h",
    ],
    deps = [
        ":lineage_model",
        "//src/base",
        "//src/base:error",
        "//src/model:checkpoint",
        "//src/model:identity",
        "//src/model:keyspace",
        "//src/model:position",
    ],
)

kwaque_cc_seastar_gtest(
    name = "lineage_checkpoint_test",
    srcs = [
        "lineage_checkpoint_test.cc",
        "lineage_test_support.h",
    ],
    memory = "256MiB",
    deps = [
        ":lineage_model",
        "//src/base",
        "//src/base:error",
        "//src/bytes:allocation_test_profile",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:cooperative",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/model:checkpoint",
        "//src/model:checkpoint_codec",
        "//src/model:identity",
        "//src/model:keyspace",
        "//src/model:position",
    ],
)

kwaque_cc_seastar_gtest(
    name = "record_codec_test",
    srcs = ["record_codec_test.cc"],
    memory = "256MiB",
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:cooperative",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/model:position",
        "//src/model:record",
        "//src/model:record_codec",
        "//src/runtime:time",
    ],
)

kwaque_cc_seastar_gtest(
    name = "record_test",
    srcs = ["record_test.cc"],
    memory = "256MiB",
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/codec:cooperative",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/model:position",
        "//src/model:record",
        "//src/model:record_codec",
    ],
)

kwaque_cc_seastar_gtest(
    name = "record_scan_test",
    srcs = ["record_scan_test.cc"],
    memory = "256MiB",
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:cooperative",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/model:position",
        "//src/model:record",
        "//src/model:record_codec",
        "//src/runtime:time",
    ],
)

kwaque_cc_seastar_gtest(
    name = "batch_builder_test",
    srcs = ["batch_builder_test.cc"],
    memory = "256MiB",
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:cooperative",
        "//src/codec:digest",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/model:batch",
        "//src/model:batch_builder",
        "//src/model:batch_codec",
        "//src/model:batch_context",
        "//src/model:batch_identity",
        "//src/model:epoch",
        "//src/model:fingerprint",
        "//src/model:identity",
        "//src/model:position",
        "//src/model:record",
        "//src/model:record_codec",
        "//src/runtime:time",
    ],
)

kwaque_cc_seastar_gtest(
    name = "assigned_batch_test",
    srcs = ["assigned_batch_test.cc"],
    memory = "256MiB",
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_parser",
        "//src/bytes:fragmented_buffer_test_support",
        "//src/codec:cooperative",
        "//src/codec:digest",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/model:batch",
        "//src/model:batch_builder",
        "//src/model:batch_codec",
        "//src/model:batch_context",
        "//src/model:batch_identity",
        "//src/model:epoch",
        "//src/model:fingerprint",
        "//src/model:identity",
        "//src/model:position",
        "//src/model:record",
        "//src/model:record_codec",
        "//src/runtime:time",
    ],
)

kwaque_cc_seastar_gtest(
    name = "batch_codec_test",
    srcs = ["batch_codec_test.cc"],
    memory = "256MiB",
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:cooperative",
        "//src/codec:digest",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/codec:sha256",
        "//src/model:batch",
        "//src/model:batch_builder",
        "//src/model:batch_codec",
        "//src/model:batch_context",
        "//src/model:batch_identity",
        "//src/model:epoch",
        "//src/model:fingerprint",
        "//src/model:identity",
        "//src/model:position",
        "//src/model:record",
        "//src/model:record_codec",
        "//src/runtime:time",
    ],
)

kwaque_cc_library(
    name = "record_fuzz_cases",
    testonly = True,
    srcs = [
        "record_fuzz_cases.cc",
        "record_fuzz_oracle.cc",
    ],
    hdrs = [
        "record_fuzz_cases.h",
        "record_fuzz_oracle.h",
    ],
    visibility = ["//src/protocol/tests:__pkg__"],
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:cooperative",
        "//src/codec:digest",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/codec:sha256",
        "//src/model:batch",
        "//src/model:batch_builder",
        "//src/model:batch_codec",
        "//src/model:batch_context",
        "//src/model:identity",
        "//src/model:position",
        "//src/model:record",
        "//src/model:record_codec",
        "//src/runtime:time",
        "@crc32c",
        "@lz4//:lz4_frame",
        "@seastar",
    ],
)

kwaque_cc_seastar_gtest(
    name = "record_fuzz_cases_test",
    srcs = ["record_fuzz_cases_test.cc"],
    memory = "256MiB",
    deps = [":record_fuzz_cases"],
)

kwaque_cc_seastar_gtest(
    name = "batch_compression_test",
    srcs = ["batch_compression_test.cc"],
    memory = "256MiB",
    deps = [
        ":record_fuzz_cases",
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_builder",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:cooperative",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/compression",
        "//src/model:batch_builder",
        "//src/model:batch_codec",
        "//src/model:record_codec",
    ],
)

kwaque_cc_fuzz_test(
    name = "record_fuzz",
    srcs = ["record_fuzz.cc"],
    args = [
        "-max_len=16384",
        "-rss_limit_mb=1024",
    ],
    corpus = glob(["testdata/record_fuzz_corpus/*.bin"]),
    tags = [
        "resources:cpu:1",
        "resources:memory:1024",
    ],
    deps = [
        ":record_fuzz_cases",
        "//src/runtime/testing:seastar_fuzz",
        "@openssl",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "model_bench_support",
    testonly = True,
    srcs = [
        "model_bench_encode.cc",
        "model_bench_fixture.cc",
        "model_bench_reference.cc",
        "model_bench_size.cc",
    ],
    hdrs = [
        "model_bench_fixture.h",
        "model_bench_reference.h",
    ],
    visibility = ["//src/protocol/tests:__pkg__"],
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/base:invariant",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_builder",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:cooperative",
        "//src/codec:envelope",
        "//src/codec:error",
        "//src/codec:format_registry",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/codec:staging_cooperative",
        "//src/codec/tests:codec_bench_reference",
        "//src/model:batch",
        "//src/model:batch_builder",
        "//src/model:batch_codec",
        "//src/model:batch_context",
        "//src/model:batch_identity",
        "//src/model:batch_wire",
        "//src/model:epoch",
        "//src/model:identity",
        "//src/model:position",
        "//src/model:record",
        "//src/model:record_codec",
        "//src/runtime:time",
        "@crc32c",
        "@fmt",
        "@seastar",
    ],
)

kwaque_cc_seastar_gtest(
    name = "model_qualification_test",
    srcs = ["model_qualification_test.cc"],
    memory = "512MiB",
    deps = [
        ":model_bench_support",
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:cooperative",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/model:batch",
        "//src/model:batch_codec",
        "//src/model:record_codec",
    ],
)

kwaque_cc_benchmark(
    name = "model_bench",
    srcs = [
        "batch_compression_bench.cc",
        "checkpoint_bench.cc",
        "model_bench.cc",
    ],
    memory = "512MiB",
    deps = [
        ":checkpoint_test_support",
        ":model_bench_support",
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:cooperative",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/codec:sha256",
        "//src/codec/tests:codec_bench_reference",
        "//src/compression",
        "//src/model:batch",
        "//src/model:batch_builder",
        "//src/model:batch_codec",
        "//src/model:batch_context",
        "//src/model:checkpoint",
        "//src/model:checkpoint_codec",
        "//src/model:fingerprint",
        "//src/model:record",
        "//src/model:record_codec",
        "@fmt",
    ],
)

identity_dependency_test(
    name = "identity_dependency_test",
    size = "small",
    target_under_test = "//src/model:identity",
)

kwaque_cc_test(
    name = "uuid_dependency_compatibility_test",
    srcs = ["uuid_dependency_compatibility_test.cc"],
    deps = [
        "//src/model:identity",
        "@boost//:uuid",
    ],
)

kwaque_cc_test(
    name = "identity_test",
    srcs = ["identity_test.cc"],
    deps = [
        "//src/base:error",
        "//src/model:identity",
        "@abseil-cpp//absl/hash:hash",
        "@abseil-cpp//absl/hash:hash_testing",
    ],
)

kwaque_cc_test(
    name = "epoch_test",
    srcs = ["epoch_test.cc"],
    deps = [
        "//src/base:error",
        "//src/model:epoch",
        "@abseil-cpp//absl/hash:hash",
        "@abseil-cpp//absl/hash:hash_testing",
    ],
)

kwaque_cc_test(
    name = "transport_identity_test",
    srcs = ["transport_identity_test.cc"],
    deps = [
        "//src/model:transport_identity",
        "@abseil-cpp//absl/hash:hash",
        "@abseil-cpp//absl/hash:hash_testing",
    ],
)

kwaque_cc_test(
    name = "batch_identity_test",
    srcs = ["batch_identity_test.cc"],
    deps = [
        "//src/base:error",
        "//src/model:batch_identity",
        "//src/model:epoch",
        "//src/model:identity",
        "//src/model:transport_identity",
        "@abseil-cpp//absl/hash:hash",
        "@abseil-cpp//absl/hash:hash_testing",
    ],
)

kwaque_cc_test(
    name = "producer_stream_binding_test",
    srcs = ["producer_stream_binding_test.cc"],
    deps = [
        "//src/base:error",
        "//src/model:batch_identity",
        "//src/model:epoch",
        "//src/model:identity",
    ],
)

kwaque_cc_test(
    name = "position_test",
    srcs = ["position_test.cc"],
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/model:position",
        "//src/runtime:file_position",
    ],
)

kwaque_cc_test(
    name = "record_identity_test",
    srcs = ["record_identity_test.cc"],
    deps = [
        "//src/base:error",
        "//src/model:epoch",
        "//src/model:identity",
        "//src/model:position",
        "@abseil-cpp//absl/hash:hash",
        "@abseil-cpp//absl/hash:hash_testing",
    ],
)

kwaque_cc_test(
    name = "timestamp_test",
    srcs = ["timestamp_test.cc"],
    deps = [
        "//src/model:position",
        "//src/runtime:time",
    ],
)

kwaque_cc_test(
    name = "keyspace_test",
    srcs = ["keyspace_test.cc"],
    deps = [
        "//src/base:error",
        "//src/model:keyspace",
    ],
)

kwaque_cc_test(
    name = "segment_state_test",
    srcs = ["segment_state_test.cc"],
    deps = [
        "//src/base:error",
        "//src/model:segment_state",
    ],
)

kwaque_cc_test(
    name = "segment_state_validation_test",
    srcs = ["segment_state_validation_test.cc"],
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/model:segment_state",
    ],
)

kwaque_cc_test(
    name = "topic_policy_test",
    srcs = ["topic_policy_test.cc"],
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/model:topic_policy",
        "//src/runtime:time",
    ],
)

kwaque_cc_test(
    name = "batch_context_test",
    srcs = ["batch_context_test.cc"],
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/codec:limits",
        "//src/model:batch_context",
        "//src/model:batch_identity",
        "//src/model:epoch",
        "//src/model:identity",
        "//src/model:position",
        "//src/runtime:file_position",
        "//src/runtime:time",
    ],
)

kwaque_cc_test(
    name = "range_context_test",
    srcs = ["range_context_test.cc"],
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/model:batch_context",
        "//src/model:batch_identity",
        "//src/model:epoch",
        "//src/model:identity",
        "//src/model:keyspace",
        "//src/model:position",
        "//src/runtime:time",
    ],
)

kwaque_cc_test(
    name = "value_boundary_test",
    srcs = ["value_boundary_test.cc"],
    deps = [
        "//src/base",
        "//src/bytes:fragmented_buffer",
        "//src/codec:limits",
        "//src/model:batch_context",
        "//src/model:batch_identity",
        "//src/model:epoch",
        "//src/model:identity",
        "//src/model:keyspace",
        "//src/model:position",
        "//src/model:segment_state",
        "//src/model:topic_policy",
        "//src/model:transport_identity",
        "//src/runtime:cross_shard",
        "//src/runtime:error",
        "//src/runtime:file_position",
        "//src/runtime:shard_affinity",
        "//src/runtime:time",
    ],
)

test_suite(
    name = "value_tests",
    tests = [
        ":batch_context_test",
        ":batch_identity_test",
        ":epoch_test",
        ":identity_dependency_test",
        ":identity_test",
        ":keyspace_test",
        ":position_test",
        ":producer_stream_binding_test",
        ":range_context_test",
        ":record_identity_test",
        ":segment_state_test",
        ":segment_state_validation_test",
        ":timestamp_test",
        ":topic_policy_test",
        ":transport_identity_test",
        ":uuid_dependency_compatibility_test",
        ":value_boundary_test",
    ],
)

kwaque_cc_seastar_gtest(
    name = "checkpoint_test",
    srcs = ["checkpoint_test.cc"],
    memory = "256MiB",
    deps = [
        ":checkpoint_test_support",
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
        "//src/model:checkpoint",
        "//src/model:checkpoint_codec",
        "//src/model:fingerprint",
        "//src/model:identity",
        "//src/model:position",
    ],
)

kwaque_cc_library(
    name = "checkpoint_test_support",
    testonly = True,
    srcs = ["checkpoint_test_support.cc"],
    hdrs = ["checkpoint_test_support.h"],
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/bytes:fragmented_buffer",
        "//src/codec:digest",
        "//src/codec:limits",
        "//src/codec:sha256",
        "//src/model:checkpoint",
        "//src/model:identity",
        "//src/model:position",
        "@crc32c",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "checkpoint_fuzz_cases",
    testonly = True,
    srcs = ["checkpoint_fuzz_cases.cc"],
    hdrs = ["checkpoint_fuzz_cases.h"],
    deps = [
        ":checkpoint_test_support",
        "//src/base",
        "//src/base:error",
        "//src/bytes:allocation_test_profile",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:cooperative",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/model:checkpoint",
        "//src/model:checkpoint_codec",
        "//src/model:identity",
        "//src/model:position",
        "@seastar",
    ],
)

kwaque_cc_fuzz_test(
    name = "checkpoint_fuzz",
    srcs = ["checkpoint_fuzz.cc"],
    args = [
        "-max_len=16384",
        "-rss_limit_mb=1024",
    ],
    corpus = glob(["testdata/checkpoint_fuzz_corpus/*.bin"]),
    tags = [
        "resources:cpu:1",
        "resources:memory:1024",
    ],
    deps = [
        ":checkpoint_fuzz_cases",
        "//src/runtime/testing:seastar_fuzz",
        "@openssl",
        "@seastar",
    ],
)

kwaque_cc_seastar_gtest(
    name = "checkpoint_qualification_test",
    srcs = ["checkpoint_qualification_test.cc"],
    memory = "256MiB",
    deps = [
        ":checkpoint_fuzz_cases",
        ":checkpoint_test_support",
        "//src/base",
        "//src/base:error",
        "//src/bytes:allocation_test_profile",
        "//src/bytes:fragmented_buffer",
        "//src/bytes:fragmented_buffer_parser",
        "//src/codec:collection",
        "//src/codec:cooperative",
        "//src/codec:error",
        "//src/codec:integer",
        "//src/codec:limits",
        "//src/model:checkpoint",
        "//src/model:checkpoint_codec",
        "//src/model:fingerprint",
        "//src/model:identity",
        "//src/model:position",
    ],
)

test_suite(
    name = "checkpoint_tests",
    tests = [
        ":checkpoint_qualification_test",
        ":checkpoint_test",
        ":lineage_checkpoint_test",
        ":lineage_model_test",
    ],
)
