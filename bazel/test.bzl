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
    "ASAN_OPTIONS": "abort_on_error=1:check_initialization_order=1:disable_coredump=0:symbolize=1",
    "ASAN_SYMBOLIZER_PATH": "$(rootpath @current_llvm_toolchain//:llvm-symbolizer)",
    "BOOST_TEST_CATCH_SYSTEM_ERRORS": "no",
    "LSAN_OPTIONS": "suppressions=$(rootpath //:lsan_suppressions)",
    "UBSAN_OPTIONS": "abort_on_error=1:halt_on_error=1:print_stacktrace=1:report_error_type=1:suppressions=$(rootpath //:ubsan_suppressions):symbolize=1",
}

def _merged_env(extra):
    result = dict(_TEST_ENV)
    result.update(extra)
    return result

def _test_env(extra):
    """Returns the sanitizer environment plus the selected reactor backend.

    Harnesses that start native processes read KWAQUE_REACTOR_BACKEND, so the
    --//bazel:reactor_backend flag reaches them as it reaches C++ tests. They
    bound a reactor's networking control blocks as _reactor_args does.
    """
    return select({
        "//bazel:reactor_backend_io_uring": _merged_env(dict(extra, KWAQUE_REACTOR_BACKEND = "io_uring")),
        "//bazel:reactor_backend_linux_aio": _merged_env(dict(extra, KWAQUE_REACTOR_BACKEND = "linux-aio")),
        "//conditions:default": _merged_env(dict(extra, KWAQUE_REACTOR_BACKEND = "epoll")),
    })

def kwaque_py_native_test(
        name,
        srcs = [],
        args = [],
        data = [],
        main = None,
        size = "small",
        timeout = None,
        tags = [],
        deps = []):
    """Defines a Python test that runs native binaries.

    Every test that starts a native process uses this macro, so the process
    inherits the sanitizer options that make reports fatal and the selected
    reactor backend. The options name files relative to the test's working
    directory; a harness that starts a process elsewhere passes it
    bazel.native_test_environment.normalized_environment().

    Args:
      name: Name of the test.
      srcs: Python sources of the test.
      args: Test arguments.
      data: Runtime data, such as the native binaries the test starts.
      main: Main Python source, when it differs from the test name.
      size: Bazel test size.
      timeout: Bazel test timeout.
      tags: Test tags.
      deps: Python libraries the test imports.
    """
    py_test(
        name = name,
        srcs = srcs,
        args = args,
        data = data + _SANITIZER_DATA,
        deps = deps + ["//bazel:native_test_environment"],
        env = _test_env({}),
        main = main,
        size = size,
        tags = tags,
        timeout = timeout,
    )

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

# A linux-aio shard asks the kernel for 10,000 networking control blocks
# unless told otherwise, beside 1,026 of its own, out of one allowance the
# whole host shares (fs.aio-max-nr, 65,536 unless raised). Six such shards use
# it up: the reactor that finds too few left takes every one that remains, and
# the next process to start a reactor, on any backend, fails before its first
# task. No test holds a thousand sockets on a shard, so each asks for a
# thousand and a parallel run fits the allowance a host has by default.
# Whatever starts a reactor for a test passes the same bound.
_LINUX_AIO_NETWORKING_BOUND = "--max-networking-io-control-blocks=1000"

def _reactor_args(cpu, memory, args, dash_dash):
    if type(cpu) != "int" or cpu <= 0:
        fail("cpu must be a positive integer")
    _parse_memory_mib(memory)
    if _has_reactor_resource_arg(args):
        fail("set reactor CPU and memory with the cpu and memory rule parameters")
    for arg in args:
        if arg == "--reactor-backend" or arg.startswith("--reactor-backend="):
            fail("select the reactor backend with --//bazel:reactor_backend")
    result = select({
        "//bazel:reactor_backend_io_uring": ["--reactor-backend=io_uring"],
        "//bazel:reactor_backend_linux_aio": [
            "--reactor-backend=linux-aio",
            _LINUX_AIO_NETWORKING_BOUND,
        ],
        "//conditions:default": ["--reactor-backend=epoll"],
    }) + [
        "--memory={}".format(memory),
        "--overprovisioned",
        "--smp={}".format(cpu),
    ] + select({
        "//bazel:system_allocator": [],
        "//conditions:default": ["--abort-on-seastar-bad-alloc"],
    }) + args
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
        env = _test_env(env),
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
        tags = [],
        linkopts = [],
        linkstatic = None):
    """Defines a GoogleTest that executes in a configured Seastar thread."""
    linking = {} if linkstatic == None else {"linkstatic": linkstatic}
    cc_test(
        name = name,
        srcs = srcs,
        args = _reactor_args(cpu, memory, args, False),
        copts = kwaque_copts(),
        linkopts = linkopts,
        data = data + _SANITIZER_DATA,
        defines = defines,
        deps = deps + [
            "//src/runtime/testing:seastar_gtest_main",
            "@googletest//:gtest",
            "@seastar",
        ],
        env = _test_env(env),
        features = ["layering_check"],
        local_defines = local_defines,
        size = size,
        tags = _resource_tags(cpu, memory) + tags,
        timeout = timeout,
        **linking
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
        tags = [],
        linkopts = [],
        linkstatic = None):
    """Defines a Seastar asynchronous test using Seastar's test runner.

    SEASTAR_TESTING_MAIN applies to every source. Only one translation unit
    may include the Seastar test registration headers that define main.
    """
    linking = {} if linkstatic == None else {"linkstatic": linkstatic}
    cc_test(
        name = name,
        srcs = srcs,
        args = _reactor_args(cpu, memory, args, True),
        copts = kwaque_copts(),
        linkopts = linkopts,
        data = data + _SANITIZER_DATA,
        defines = defines,
        deps = deps + [
            "@boost//:test.so",
            "@seastar",
            "@seastar//:testing",
        ],
        env = _test_env(env),
        features = ["layering_check"],
        local_defines = local_defines + ["SEASTAR_TESTING_MAIN"],
        size = size,
        tags = _resource_tags(cpu, memory) + tags,
        timeout = timeout,
        **linking
    )

def kwaque_cc_benchmark(
        name,
        srcs = [],
        deps = [],
        args = [],
        local_defines = [],
        linkopts = [],
        cpu = 1,
        memory = "128MiB",
        native_allocator_only = False,
        tags = []):
    """Defines a Seastar benchmark executable and a test that runs it once.

    The `<name>_test` target runs every benchmark case for one iteration, so
    ordinary test runs keep benchmark code compiling and executing without
    taking measurements.

    Args:
      name: Name of the benchmark executable.
      srcs: C++ sources of the benchmark.
      deps: Dependencies of the benchmark.
      args: Benchmark arguments, before the reactor arguments.
      local_defines: Defines for the benchmark sources only.
      linkopts: Additional linker options.
      cpu: Reactor shards for the benchmark and its test.
      memory: Reactor memory for the benchmark and its test.
      native_allocator_only: Whether the fixtures budget memory against the
        Seastar allocator. The test is then skipped in system-allocator
        builds, such as sanitizer builds, where the same budgets are exceeded.
      tags: Additional tags for both targets.
    """
    benchmark_args = list(args)
    has_stall_threshold = False
    for arg in benchmark_args:
        if arg == "--blocked-reactor-notify-ms" or arg.startswith("--blocked-reactor-notify-ms="):
            has_stall_threshold = True
    if not has_stall_threshold:
        # Synchronous benchmark loops deliberately occupy the shard for the
        # duration of a run. Keep the stall detector from interrupting and
        # printing backtraces for that expected behavior.
        benchmark_args = ["--blocked-reactor-notify-ms=2000000"] + benchmark_args
    cc_binary(
        name = name,
        srcs = srcs,
        args = _reactor_args(cpu, memory, benchmark_args, False),
        copts = kwaque_copts(),
        deps = depset(deps + [
            "//bazel:benchmark_policy",
            "@seastar",
            "@seastar//:benchmark",
        ]).to_list(),
        features = ["layering_check"],
        local_defines = local_defines,
        linkopts = linkopts,
        tags = _resource_tags(cpu, memory) + ["benchmark"] + tags,
        testonly = True,
    )
    py_test(
        name = name + "_test",
        srcs = ["//bazel:benchmark_smoke.py"],
        args = ["$(rootpath :{})".format(name)] + _reactor_args(
            cpu,
            memory,
            benchmark_args + [
                "--duration=0",
                "--iterations=1",
                "--no-perf-counters",
                "--random-seed=1",
                "--runs=1",
            ],
            False,
        ),
        data = [":" + name] + _SANITIZER_DATA,
        env = _test_env({}),
        main = "//bazel:benchmark_smoke.py",
        size = "medium",
        tags = _resource_tags(cpu, memory) + ["benchmark"] + tags,
        target_compatible_with = select({
            "//bazel:system_allocator": ["@platforms//:incompatible"],
            "//conditions:default": [],
        }) if native_allocator_only else [],
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
        deps = ["//bazel:native_test_environment"],
        env = _test_env(env),
        main = "//bazel:fuzz_test_wrapper.py",
        size = "small",
        tags = ["fuzz"] + tags,
        timeout = "moderate",
        target_compatible_with = compatibility,
    )

    # Replays the empty input and the seed corpus in every ordinary build,
    # including optimized and other-architecture builds that never fuzz.
    cc_test(
        name = name + "_replay",
        srcs = srcs,
        args = ["$(rootpath {})".format(seed) for seed in corpus],
        copts = kwaque_copts(),
        data = corpus + _SANITIZER_DATA,
        deps = deps + ["//bazel:fuzz_corpus_replay"],
        env = _test_env(env),
        features = ["layering_check"],
        size = "small",
        # A manual target, such as a deliberate crash canary, stays manual.
        tags = ["fuzz_replay"] + [
            tag
            for tag in tags
            if tag == "manual" or tag.startswith("resources:")
        ],
        target_compatible_with = select({
            "//bazel:fuzz_build": ["@platforms//:incompatible"],
            "//conditions:default": [],
        }),
    )

def kwaque_fuzz_signal_canary_test(name, runner, seed):
    """Verify a deliberate reactor signal produces a replayable crash artifact."""
    py_test(
        name = name,
        srcs = [
            "//bazel:fuzz_signal_canary_test.py",
            "//bazel:fuzz_test_wrapper.py",
        ],
        args = ["$(rootpath {})".format(runner), "$(rootpath {})".format(seed)],
        data = [runner, seed] + _SANITIZER_DATA,
        deps = ["//bazel:native_test_environment"],
        env = _test_env({}),
        main = "//bazel:fuzz_signal_canary_test.py",
        size = "small",
        tags = ["fuzz", "resources:cpu:1", "resources:memory:256"],
        timeout = "moderate",
        target_compatible_with = select({
            "//bazel:fuzz_build": [],
            "//conditions:default": ["@platforms//:incompatible"],
        }),
    )
