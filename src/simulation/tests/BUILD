load("//bazel:build.bzl", "kwaque_cc_binary", "kwaque_cc_library")
load(
    "//bazel:test.bzl",
    "kwaque_cc_benchmark",
    "kwaque_cc_fuzz_test",
    "kwaque_cc_seastar_test",
    "kwaque_cc_test",
    "kwaque_fuzz_signal_canary_test",
    "kwaque_py_native_test",
)

package(default_visibility = ["//visibility:private"])

test_suite(
    name = "determinism_goldens",
    tests = [
        ":determinism_test",
        ":deterministic_random_test",
        ":event_trace_test",
        ":fault_schedule_test",
        ":fuzz_reproduction_test",
        ":philox_kat_test",
        "//src/observability:event_codec_goldens",
    ],
    visibility = ["//visibility:public"],
)

kwaque_cc_library(
    name = "fuzz_reproduction",
    testonly = True,
    srcs = ["fuzz_reproduction.cc"],
    hdrs = ["fuzz_reproduction.h"],
    deps = [
        "//src/observability:event",
        "//src/observability:event_log",
        "//src/runtime:error",
        "//src/simulation:event_trace",
        "//src/simulation:sha256",
        "@seastar",
    ],
)

kwaque_cc_library(
    name = "fuzz_cases",
    testonly = True,
    srcs = ["fuzz_cases.cc"],
    hdrs = [
        "fuzz_cases.h",
        "fuzz_network_cases.h",
    ],
    deps = [
        ":fake_file_model",
        ":fuzz_reproduction",
        ":network_oracle",
        "//src/base",
        "//src/bytes:fragmented_buffer",
        "//src/observability:event",
        "//src/observability:event_log",
        "//src/resource:workload_class",
        "//src/runtime:error",
        "//src/runtime:fault",
        "//src/runtime:fault_api",
        "//src/runtime:file",
        "//src/runtime:network",
        "//src/runtime:random",
        "//src/runtime/testing:seastar_fuzz",
        "//src/runtime/testing/contracts:cleanup",
        "//src/simulation:bandwidth",
        "//src/simulation:deterministic_random",
        "//src/simulation:event_trace",
        "//src/simulation:fake_file",
        "//src/simulation:fake_file_test_support",
        "//src/simulation:fake_network",
        "//src/simulation:fault_schedule",
        "//src/simulation:scheduler",
        "//src/simulation:scheduler_driver",
        "//src/simulation:sha256",
        "//src/simulation:structured_event_sink",
        "@openssl",
        "@seastar",
    ],
)

kwaque_cc_binary(
    name = "fuzz_replay",
    testonly = True,
    srcs = ["fuzz_replay_main.cc"],
    deps = [
        ":fuzz_cases",
        ":fuzz_reproduction",
        "//src/runtime:error",
        "//src/runtime/testing:seastar_fuzz",
        "@openssl",
        "@seastar",
    ],
)

kwaque_cc_binary(
    name = "fuzz_semantic_canary",
    testonly = True,
    srcs = ["fuzz_semantic_canary_main.cc"],
    deps = [
        ":fuzz_cases",
        "//src/runtime/testing:seastar_fuzz",
    ],
)

kwaque_py_native_test(
    name = "fuzz_reproduction_test",
    timeout = "moderate",
    srcs = [
        "fuzz_reproduction_test.py",
        "//bazel:fuzz_test_wrapper.py",
    ],
    data = [
        ":fuzz_replay",
        ":fuzz_semantic_canary",
    ],
    main = "fuzz_reproduction_test.py",
)

kwaque_cc_seastar_test(
    name = "fuzz_cases_test",
    srcs = ["fuzz_cases_test.cc"],
    cpu = 1,
    memory = "512MiB",
    deps = [
        ":fuzz_cases",
        ":fuzz_reproduction",
        "//src/observability:event",
        "//src/observability:event_codec",
        "//src/observability:event_log",
        "//src/runtime:error",
        "//src/simulation:event_trace",
        "//src/simulation:scheduler",
        "//src/simulation:structured_event_sink",
    ],
)

kwaque_cc_fuzz_test(
    name = "scheduler_fuzz",
    srcs = ["scheduler_fuzz.cc"],
    args = [
        "-max_len=16384",
        "-rss_limit_mb=1024",
    ],
    corpus = [
        "testdata/scheduler_fuzz_corpus/basic.script",
        "testdata/scheduler_fuzz_corpus/saturation.script",
    ],
    tags = [
        "resources:cpu:1",
        "resources:memory:1024",
    ],
    deps = [":fuzz_cases"],
)

kwaque_cc_seastar_test(
    name = "fuzz_file_cases_test",
    srcs = ["fuzz_file_cases_test.cc"],
    cpu = 1,
    memory = "512MiB",
    deps = [
        ":fuzz_cases",
        ":fuzz_reproduction",
        "//src/runtime:fault_api",
        "//src/simulation:event_trace",
    ],
)

kwaque_cc_seastar_test(
    name = "fuzz_network_cases_test",
    srcs = ["fuzz_network_cases_test.cc"],
    cpu = 1,
    memory = "512MiB",
    deps = [
        ":fuzz_cases",
        ":fuzz_reproduction",
        "//src/bytes:fragmented_buffer",
        "//src/runtime:error",
        "//src/runtime:network",
        "//src/simulation:event_trace",
        "//src/simulation:fake_network",
        "//src/simulation:scheduler",
    ],
)

kwaque_cc_fuzz_test(
    name = "fault_schedule_fuzz",
    srcs = ["fault_schedule_fuzz.cc"],
    args = [
        "-max_len=16384",
        "-rss_limit_mb=1024",
    ],
    corpus = [
        "testdata/fault_schedule_fuzz_corpus/saturation.script",
        "testdata/fault_schedule_fuzz_corpus/selectors.script",
    ],
    tags = [
        "resources:cpu:1",
        "resources:memory:1024",
    ],
    deps = [":fuzz_cases"],
)

kwaque_cc_fuzz_test(
    name = "fake_file_fuzz",
    srcs = ["fake_file_fuzz.cc"],
    args = [
        "-max_len=16384",
        "-rss_limit_mb=1024",
    ],
    corpus = [
        "testdata/fake_file_fuzz_corpus/bootstrap_crash.script",
        "testdata/fake_file_fuzz_corpus/history.script",
        "testdata/fake_file_fuzz_corpus/limits.script",
        "testdata/fake_file_fuzz_corpus/overlap.script",
        "testdata/fake_file_fuzz_corpus/read_corrupt.script",
        "testdata/fake_file_fuzz_corpus/read_error.script",
        "testdata/fake_file_fuzz_corpus/read_short.script",
        "testdata/fake_file_fuzz_corpus/resize_partial.script",
        "testdata/fake_file_fuzz_corpus/write_corrupt.script",
        "testdata/fake_file_fuzz_corpus/write_delay.script",
        "testdata/fake_file_fuzz_corpus/write_error.script",
        "testdata/fake_file_fuzz_corpus/write_parked.script",
        "testdata/fake_file_fuzz_corpus/write_redirect.script",
        "testdata/fake_file_fuzz_corpus/write_restart.script",
        "testdata/fake_file_fuzz_corpus/write_short.script",
        "testdata/fake_file_fuzz_corpus/write_torn.script",
    ],
    tags = [
        "resources:cpu:1",
        "resources:memory:1024",
    ],
    deps = [":fuzz_cases"],
)

kwaque_cc_fuzz_test(
    name = "fake_network_fuzz",
    srcs = ["fake_network_fuzz.cc"],
    args = [
        "-max_len=16384",
        "-rss_limit_mb=1024",
    ],
    corpus = [
        "testdata/fake_network_fuzz_corpus/clog_partition_heal.script",
        "testdata/fake_network_fuzz_corpus/fault_corrupt.script",
        "testdata/fake_network_fuzz_corpus/fault_delay.script",
        "testdata/fake_network_fuzz_corpus/fault_disconnect.script",
        "testdata/fake_network_fuzz_corpus/fault_drop.script",
        "testdata/fake_network_fuzz_corpus/fault_duplicate.script",
        "testdata/fake_network_fuzz_corpus/fault_error.script",
        "testdata/fake_network_fuzz_corpus/fault_parked.script",
        "testdata/fake_network_fuzz_corpus/fault_reorder.script",
        "testdata/fake_network_fuzz_corpus/fault_short.script",
        "testdata/fake_network_fuzz_corpus/flows_1.script",
        "testdata/fake_network_fuzz_corpus/flows_32.script",
        "testdata/fake_network_fuzz_corpus/flows_8.script",
        "testdata/fake_network_fuzz_corpus/flows_96.script",
        "testdata/fake_network_fuzz_corpus/flows_96_mixed.script",
        "testdata/fake_network_fuzz_corpus/flows_96_reorder.script",
        "testdata/fake_network_fuzz_corpus/partition_then_write.script",
    ],
    tags = [
        "resources:cpu:1",
        "resources:memory:1024",
    ],
    deps = [":fuzz_cases"],
)

kwaque_cc_fuzz_test(
    name = "signal_canary_fuzz",
    srcs = ["signal_canary_fuzz.cc"],
    args = [
        "-max_len=8",
        "-rss_limit_mb=256",
    ],
    corpus = ["testdata/signal_canary_fuzz_corpus/crash.script"],
    tags = [
        "manual",
        "resources:cpu:1",
        "resources:memory:256",
    ],
    deps = ["//src/runtime/testing:seastar_fuzz"],
)

kwaque_fuzz_signal_canary_test(
    name = "signal_canary_test",
    runner = ":signal_canary_fuzz_runner",
    seed = "testdata/signal_canary_fuzz_corpus/crash.script",
)

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
        "//src/simulation:deterministic_random",
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
        "//src/simulation:deterministic_random",
    ],
)

kwaque_cc_benchmark(
    name = "simulation_bench",
    srcs = [
        "fake_file_bench.cc",
        "simulation_bench.cc",
    ],
    memory = "256MiB",
    deps = [
        ":network_oracle",
        "//src/base",
        "//src/bytes:fragmented_buffer",
        "//src/runtime:file",
        "//src/runtime:network",
        "//src/runtime/testing/contracts:network_contract",
        "//src/simulation:bandwidth",
        "//src/simulation:event_trace",
        "//src/simulation:fake_file",
        "//src/simulation:fake_file_test_support",
        "//src/simulation:fake_network",
        "//src/simulation:fault_schedule",
        "//src/simulation:scheduler",
        "//src/simulation:scheduler_driver",
        "@seastar",
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
    name = "structured_event_sink_test",
    srcs = ["structured_event_sink_test.cc"],
    cpu = 1,
    memory = "192MiB",
    deps = [
        "//src/observability:event",
        "//src/observability:event_identity",
        "//src/observability:event_log",
        "//src/observability:event_sink_concept",
        "//src/simulation:structured_event_sink",
    ],
)

kwaque_cc_seastar_test(
    name = "simulation_metrics_test",
    srcs = ["simulation_metrics_test.cc"],
    cpu = 1,
    memory = "256MiB",
    deps = [
        "//src/base:metric_schema",
        "//src/runtime:fault",
        "//src/runtime:fault_api",
        "//src/simulation:deterministic_random",
        "//src/simulation:event_trace",
        "//src/simulation:fake_dns",
        "//src/simulation:fake_file",
        "//src/simulation:fake_network",
        "//src/simulation:fault_schedule",
        "//src/simulation:metrics",
        "//src/simulation:scheduler",
    ],
)

kwaque_cc_seastar_test(
    name = "environment_test",
    srcs = ["environment_test.cc"],
    cpu = 1,
    memory = "256MiB",
    deps = [
        "//src/base",
        "//src/observability:event",
        "//src/observability:event_log",
        "//src/resource:resource_manager",
        "//src/resource:workload_class",
        "//src/runtime:dns",
        "//src/runtime:environment",
        "//src/runtime:fault",
        "//src/runtime:fault_api",
        "//src/runtime:file",
        "//src/runtime:network",
        "//src/runtime/testing/contracts:environment_contract",
        "//src/runtime/testing/contracts:environment_lifecycle_contract",
        "//src/runtime/testing/contracts:simulation_environment_component",
        "//src/simulation:environment",
        "//src/simulation:environment_test_support",
        "//src/simulation:fake_dns",
        "//src/simulation:fault_schedule",
        "//src/simulation:scheduler",
        "//src/simulation:scheduler_driver",
        "//src/simulation:sha256",
    ],
)

kwaque_cc_seastar_test(
    name = "environment_replay_test",
    srcs = ["environment_replay_test.cc"],
    cpu = 1,
    memory = "256MiB",
    deps = [
        "//src/base",
        "//src/base:error",
        "//src/observability:event",
        "//src/observability:event_codec",
        "//src/observability:event_log",
        "//src/runtime:dns",
        "//src/runtime:fault_api",
        "//src/runtime:file",
        "//src/runtime:network",
        "//src/runtime/testing/contracts:environment_contract",
        "//src/runtime/testing/contracts:simulation_environment_component",
        "//src/simulation:environment",
        "//src/simulation:environment_test_support",
        "//src/simulation:event_trace",
        "//src/simulation:fake_dns",
        "//src/simulation:fake_file_test_support",
        "//src/simulation:scheduler_driver",
        "//src/simulation:sha256",
    ],
)

kwaque_cc_seastar_test(
    name = "failure_probe_test",
    srcs = ["failure_probe_test.cc"],
    cpu = 1,
    memory = "192MiB",
    deps = [
        "//src/runtime:error",
        "//src/runtime:fault",
        "//src/runtime:fault_api",
        "//src/runtime:time",
        "//src/runtime/testing/failure_probe",
        "//src/runtime/testing/failure_probe:failure_probe_test_support",
        "//src/simulation:deterministic_random",
        "//src/simulation:event_trace",
        "//src/simulation:fault_schedule",
        "//src/simulation:scheduler",
        "//src/simulation:timer",
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
        "//src/base",
        "//src/runtime:fault_api",
        "//src/simulation:event_trace",
        "//src/simulation:sha256",
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
        "//src/simulation:scheduler_driver",
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
        "//src/simulation:scheduler_driver",
    ],
)

kwaque_cc_seastar_test(
    name = "fake_network_test",
    srcs = ["fake_network_test.cc"],
    cpu = 1,
    memory = "256MiB",
    deps = [
        "//src/runtime:network",
        "//src/runtime:random",
        "//src/runtime/testing/contracts:network_contract",
        "//src/simulation:deterministic_random",
        "//src/simulation:event_trace",
        "//src/simulation:fake_network",
        "//src/simulation:fake_network_test_support",
        "//src/simulation:fault_schedule",
        "//src/simulation:scheduler",
        "//src/simulation:scheduler_driver",
        "//src/simulation/tests:network_oracle",
    ],
)

kwaque_cc_seastar_test(
    name = "fake_network_affinity_test",
    srcs = ["fake_network_affinity_test.cc"],
    cpu = 2,
    memory = "256MiB",
    deps = [
        "//src/simulation:fake_dns",
        "//src/simulation:fake_network",
        "//src/simulation:scheduler",
    ],
)

kwaque_cc_seastar_test(
    name = "fake_dns_test",
    srcs = ["fake_dns_test.cc"],
    cpu = 1,
    memory = "256MiB",
    deps = [
        "//src/runtime:dns",
        "//src/runtime:random",
        "//src/runtime/testing/contracts:dns_contract",
        "//src/simulation:deterministic_random",
        "//src/simulation:event_trace",
        "//src/simulation:fake_dns",
        "//src/simulation:fault_schedule",
        "//src/simulation:scheduler",
        "//src/simulation:scheduler_driver",
    ],
)
