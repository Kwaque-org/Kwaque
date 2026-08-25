"""Canonical C++ test, reactor-test, benchmark, and fuzzing rules."""

load("@rules_cc//cc:cc_binary.bzl", "cc_binary")
load("@rules_cc//cc:cc_test.bzl", "cc_test")
load("@rules_python//python:defs.bzl", "py_test")
load(":internal.bzl", "kwaque_copts")

_SANITIZER_DATA = [
    "//:lsan_suppressions",
    "//:ubsan_suppressions",
    "@current_llvm_toolchain//:llvm-symbolizer",
]

_TEST_ENV = {
    "ASAN_OPTIONS": "abort_on_error=1:disable_coredump=0:symbolize=1",
    "ASAN_SYMBOLIZER_PATH": "$(rootpath @current_llvm_toolchain//:llvm-symbolizer)",
    "KWAQUE_TEST_SEED": "1",
    "LSAN_OPTIONS": "suppressions=$(rootpath //:lsan_suppressions)",
    "UBSAN_OPTIONS": "abort_on_error=1:halt_on_error=1:print_stacktrace=1:report_error_type=1:suppressions=$(rootpath //:ubsan_suppressions):symbolize=1",
}

def _merged_env(extra):
    result = dict(_TEST_ENV)
    result.update(extra)
    return result

def _parse_memory_mib(value):
    suffixes = [
        ("GiB", 1024),
        ("GB", 1024),
        ("G", 1024),
        ("MiB", 1),
        ("MB", 1),
        ("M", 1),
    ]
    for suffix, factor in suffixes:
        if value.endswith(suffix):
            amount = value[:-len(suffix)]
            if not amount.isdigit() or int(amount) <= 0:
                fail("memory must be a positive whole number with an M/MB/MiB/G/GB/GiB suffix")
            return int(amount) * factor
    fail("memory must use an M/MB/MiB/G/GB/GiB suffix")

def _has_reactor_resource_arg(args):
    for arg in args:
        if arg in ["-c", "-m", "--memory", "--smp"]:
            return True
        for prefix in ["-c", "-m", "--memory=", "--smp="]:
            if arg.startswith(prefix):
                return True
    return False

def _reactor_args(cpu, memory, args, dash_dash):
    if type(cpu) != "int" or cpu <= 0:
        fail("cpu must be a positive integer")
    _parse_memory_mib(memory)
    if _has_reactor_resource_arg(args):
        fail("set reactor CPU and memory with the cpu and memory rule parameters")
    backend = ["--reactor-backend=epoll"]
    for arg in args:
        if arg == "--reactor-backend" or arg.startswith("--reactor-backend="):
            backend = []
            break
    result = backend + [
        "--memory={}".format(memory),
        "--overprovisioned",
        "--smp={}".format(cpu),
    ] + args
    return (["--"] + result) if dash_dash else result

def _resource_tags(cpu, memory):
    return [
        "resources:cpu:{}".format(cpu),
        "resources:memory:{}".format(_parse_memory_mib(memory)),
    ]

def kwaque_cc_test(
        name,
        srcs = [],
        deps = [],
        args = [],
        data = [],
        env = {},
        defines = [],
        local_defines = [],
        size = "small",
        timeout = None,
        tags = []):
    """Defines a GoogleTest that does not start a reactor."""
    cc_test(
        name = name,
        srcs = srcs,
        args = args,
        copts = kwaque_copts(),
        data = data + _SANITIZER_DATA,
        defines = defines,
        deps = deps + [
            "@googletest//:gtest",
            "@googletest//:gtest_main",
        ],
        env = _merged_env(env),
        features = ["layering_check"],
        local_defines = local_defines,
        size = size,
        tags = tags,
        timeout = timeout,
    )

def kwaque_cc_seastar_gtest(
        name,
        srcs = [],
        deps = [],
        args = [],
        data = [],
        env = {},
        defines = [],
        local_defines = [],
        cpu = 1,
        memory = "128MiB",
        size = "small",
        timeout = None,
        tags = []):
    """Defines a GoogleTest that executes on a configured Seastar reactor."""
    cc_test(
        name = name,
        srcs = srcs,
        args = _reactor_args(cpu, memory, args, False),
        copts = kwaque_copts(),
        data = data + _SANITIZER_DATA,
        defines = defines,
        deps = deps + [
            "//src/runtime/testing:seastar_gtest_main",
            "@googletest//:gtest",
            "@seastar",
        ],
        env = _merged_env(env),
        features = ["layering_check"],
        local_defines = local_defines,
        size = size,
        tags = _resource_tags(cpu, memory) + tags,
        timeout = timeout,
    )

def kwaque_cc_seastar_test(
        name,
        srcs = [],
        deps = [],
        args = [],
        data = [],
        env = {},
        defines = [],
        local_defines = [],
        cpu = 1,
        memory = "128MiB",
        size = "small",
        timeout = None,
        tags = []):
    """Defines a Seastar asynchronous test using Seastar's test runner."""
    cc_test(
        name = name,
        srcs = srcs,
        args = _reactor_args(cpu, memory, args, True),
        copts = kwaque_copts(),
        data = data + _SANITIZER_DATA,
        defines = defines,
        deps = deps + [
            "@boost//:test.so",
            "@seastar",
            "@seastar//:testing",
        ],
        env = _merged_env(env),
        features = ["layering_check"],
        local_defines = local_defines + ["SEASTAR_TESTING_MAIN"],
        size = size,
        tags = _resource_tags(cpu, memory) + tags,
        timeout = timeout,
    )

def kwaque_cc_benchmark(
        name,
        srcs = [],
        deps = [],
        args = [],
        cpu = 1,
        memory = "128MiB",
        tags = []):
    """Defines a Seastar benchmark executable."""
    cc_binary(
        name = name,
        srcs = srcs,
        args = _reactor_args(cpu, memory, args, False),
        copts = kwaque_copts(),
        deps = deps + ["@seastar//:benchmark"],
        features = ["layering_check"],
        tags = _resource_tags(cpu, memory) + ["benchmark"] + tags,
        testonly = True,
    )

def kwaque_cc_fuzz_test(
        name,
        srcs = [],
        deps = [],
        args = [],
        data = [],
        env = {},
        corpus = [],
        tags = []):
    """Defines a libFuzzer test enabled only by --config=fuzz.

    Args:
      name: Name of the generated test target.
      srcs: C++ source files compiled into the fuzzing binary.
      deps: Dependencies of the fuzzing binary.
      args: Arguments passed to the fuzzing binary after wrapper arguments.
      data: Runtime data dependencies in addition to the corpus and sanitizer data.
      env: Environment variables set for the test.
      corpus: Seed corpus files passed to libFuzzer.
      tags: Additional Bazel tags applied to the test.
    """
    runner_name = name + "_runner"
    compatibility = select({
        "//bazel:fuzz_build": [],
        "//conditions:default": ["@platforms//:incompatible"],
    })
    cc_binary(
        name = runner_name,
        srcs = srcs,
        copts = kwaque_copts(),
        deps = deps,
        features = ["layering_check"],
        linkopts = ["-fsanitize=fuzzer"] + select({
            "@platforms//cpu:x86_64": ["-stdlib=libc++"],
            "//conditions:default": [],
        }),
        target_compatible_with = compatibility,
        testonly = True,
    )
    py_test(
        name = name,
        srcs = ["//bazel:fuzz_test_wrapper.py"],
        args = [
            "--binary=$(rootpath :{})".format(runner_name),
        ] + [
            "--seed=$(rootpath {})".format(seed)
            for seed in corpus
        ] + ["--"] + args,
        data = [":" + runner_name] + corpus + data + _SANITIZER_DATA,
        env = _merged_env(env),
        main = "//bazel:fuzz_test_wrapper.py",
        size = "small",
        tags = ["fuzz"] + tags,
        timeout = "short",
        target_compatible_with = compatibility,
    )
