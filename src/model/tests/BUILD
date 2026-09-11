load("//bazel:test.bzl", "kwaque_cc_test")
load(":identity_dependency_test.bzl", "identity_dependency_test")

package(default_visibility = ["//visibility:private"])

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
