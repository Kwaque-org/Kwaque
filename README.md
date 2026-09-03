# Kwaque

>You can't break Kwaque with an earthquake.

Kwaque is a high-performance, self-governing distributed log/data-streaming platform built for developers, built in C++ for highest scale. This is what your AI-infrastructure requires.

## Project status

Kwaque is currently under active development.

## Requirements

### Build hosts

The build fetches its own compiler, sysroot, and third-party sources, so no
project-specific system packages are needed. It does require:

| Tool | Why |
|---|---|
| A Bazel launcher honoring `.bazelversion` | Selects the pinned Bazel `9.1.0`. [Bazelisk](https://github.com/bazelbuild/bazelisk) is the supported way to get it. |
| `git` | Build stamping reads the revision and worktree state. |
| `make` | Several native dependencies build through their own configure/make scripts. |
| `perl` | OpenSSL's `Configure` script is Perl. |
| `python3` | Repository tooling and subprocess tests. |

64-bit Linux on x86-64 or AArch64. A first build compiles the whole dependency
graph, including Seastar and OpenSSL, and needs several gigabytes of disk in the
Bazel cache.

### Runtime hosts

Kwaque currently supports 64-bit Linux on Westmere-class x86-64 processors and
ARMv8-A AArch64 processors with CRC and cryptography extensions. Packaged
binaries target the Ubuntu 22.04 userspace baseline and require glibc 2.35 or
newer. A Linux 5.15 or newer kernel is the supported baseline for the Seastar
runtime and its io_uring backend.

Seastar's default reactor backend is `linux-aio`. The broker also accepts
`--reactor-backend=io_uring`, `epoll`, or `asymmetric_io_uring`; see
[Troubleshooting](#troubleshooting) for choosing between them.

Use XFS or ext4 on a local filesystem for the data directory. The directory
must be writable by the broker process. Running the committed development
configuration does not require root privileges, device access, or privileged
ports. Hosts must provide enough unlocked memory for the selected Seastar
`--memory` value; production CPU, memory-locking, and filesystem tuning is not
yet automated.

## Quick start

Build and run the broker with the committed development configuration:

```bash
bazel build --config=dev //:kwaque
bazel run --config=dev //:kwaque -- --config conf/kwaque.yaml --smp 1
```

The example configuration binds the administrative listener to
`127.0.0.1:9644` and uses `./data` as the data directory. In another shell:

```bash
curl -s http://127.0.0.1:9644/v1/health/live
curl -s http://127.0.0.1:9644/v1/health/ready
curl -s http://127.0.0.1:9644/v1/version
curl -s http://127.0.0.1:9644/metrics | head
```

Stop the broker with `Ctrl+C` or `SIGTERM`; it drains readiness, stops accepting
administrative connections, and exits zero.

Print build metadata without starting the reactor:

```bash
bazel run --config=dev //:kwaque -- --version
```

Configuration keys, defaults, and validation rules live in
[`conf/kwaque.yaml`](conf/kwaque.yaml). Pass a different file with `--config`;
the default is `conf/kwaque.yaml` relative to the working directory.

## Development

These are the canonical commands. Continuous integration runs the same targets
through the `ci`, `ci-debug`, `ci-release`, and `ci-sanitizer` configurations,
which add `-Werror` for first-party sources and serialize test execution.

Build configurations are defined in [`.bazelrc`](.bazelrc): `dev` for the normal
build-and-test cycle (light optimization plus AddressSanitizer), `debug` for
full sanitizers, `release` for optimized and hardened binaries, `debugger` for
unoptimized debugging, and `fuzz` for libFuzzer targets. Personal overrides
belong in an untracked `user.bazelrc`.

### Build

```bash
bazel build --config=dev //:kwaque         # fast developer build
bazel build --config=release //:kwaque     # optimized, hardened
```

### Test

```bash
bazel test --config=dev //...              # unit, reactor, smoke, packaging
bazel test --config=debug //...            # same suite under ASan and UBSan
```

Both commands include the subprocess smoke tests. Fuzz targets are skipped
unless the `fuzz` configuration is selected. Select a single class of work with
the tags carried by every test target:

```bash
bazel test --config=dev --test_tag_filters=smoke //...
bazel query 'attr(tags, benchmark, //...)'
```

### Bounded fuzzing

```bash
bazel test --config=fuzz \
  //src/config:bootstrap_config_fuzz \
  //proto/kwaque/common/v1:build_info_fuzz
```

Each fuzz test runs for a bounded duration with fixed input and memory limits
and seeds itself from the committed corpus. Pass a longer budget explicitly with
`--test_arg=-max_total_time=60`.

### Benchmarks

```bash
bazel run --config=dev //bazel/tests:empty_benchmark -- --list
bazel run --config=release //bazel/tests:empty_benchmark
```

Benchmark numbers are only meaningful from a `release` build; the sanitizer-based
`dev` and `debug` configurations are far slower.

### Formatting

```bash
bazel run //tools:format_cpp_changed          # format C++ touched by your branch
bazel run //tools:format_cpp_all              # format every tracked C++ file
bazel run //tools:format_cpp_changed -- --check   # report without writing
bazel run //tools:buildifier_check            # check BUILD and .bzl files
bazel run //tools:buildifier_fix              # rewrite BUILD and .bzl files
```

### Static analysis

clang-tidy reads the generated compilation database, so regenerate it after
changing build files or adding sources:

```bash
bazel run //tools:compile_commands
bazel run //tools:clang_tidy            # baseline checks, all sources
bazel run //tools:clang_tidy_strict     # stricter profile, production sources
```

### Repository checks

```bash
bazel run //tools:check_dependency_inventory   # MODULE.bazel vs THIRD_PARTY.md
bazel run //tools:check_generated_artifacts    # reject tracked build output
bazel run //tools:check_bazel_package_cycles   # package graph must stay acyclic
bazel run //tools:check_cross_shard_usage       # enforce shared-nothing transfers
bazel run //tools:check_runtime_boundaries      # keep real/test backends separated
bazel mod tidy                                 # must leave the lockfile unchanged
```

### Package

```bash
bazel build //:kwaque_tar //:kwaque_tar_sha256
bazel test //bazel/packaging:all
```

The archive contains the broker, the example configuration, project license and
notice files, the bundled shared libraries, and upstream license material for
the dependencies that ship in or are linked into the binary. Its tests assert the
exact file layout, that two builds of the same inputs produce identical
archives, and that the extracted broker starts and stops cleanly.

### Pre-commit hooks

```bash
pre-commit install        # run the hooks on every commit
pre-commit run --all-files
```

The hooks cover whitespace, end-of-file newlines, C++ formatting, Bazel
formatting, and the tracked-artifact guard. They require `pre-commit` on the
host; every hook is also enforced in continuous integration, so installing them
locally is a convenience rather than a requirement.

## Repository layout

| Path | Contents |
|---|---|
| `src/base` | Compiler attributes, strong byte and count types, typed errors, results, logging, build metadata. |
| `src/config` | Bootstrap configuration schema, YAML decoding, validation, redacted rendering. |
| `src/runtime` | Shard ownership and lifecycle, typed runtime failures, cross-shard value rules, statically dispatched runtime contracts, owner-local operation statistics, and production clock, timer, random, file, network, and DNS mechanisms. |
| `src/bytes` | Immutable fragmented buffers, bounded construction and scatter export, checked parsing, fuzzing, and benchmarks. |
| `src/resource` | Workload classes, process/shard resource ownership, native memory admission, and bounded work queues. |
| `src/admin` | Administrative HTTP service, health and version responses, metric registration. |
| `src/broker` | Broker assembly: entry point, application ownership, ordered startup, data directory, PID file. |
| `src/simulation` | Deterministic scheduler, virtual time and timers, counter-addressed randomness, replayable faults, fake files/network/DNS, structured-event capture, and owner-local metrics. |
| `src/observability` | Bounded typed structured events, canonical event logs, owner-stamped sinks, and the fixed metric descriptor inventory. |
| `src/model`, `src/storage`, `src/protocol`, `src/raft`, `src/metadata`, `src/cluster`, `src/replication`, `src/consumer`, `src/cloud`, `src/security` | Ownership boundaries reserved for future work. Each holds a `BUILD` file and a `README.md` describing what belongs there. |
| `proto/` | Versioned Protocol Buffers control schemas and their generated-code consumers. |
| `conf/` | Example broker configuration. |
| `bazel/` | Build rules, dependency declarations, third-party overlays, packaging, rule probes. |
| `tools/` | Repository scripts: formatting, static analysis, compilation database, integrity checks. |
| `tests/smoke/` | Subprocess tests that exercise the built broker as a process. |

Read a package's `README.md` before adding code to it; that file, not this table,
is the authoritative statement of what the package owns.

### Dependency direction

Every package is private by default. A package exposes a target to others only
with an explicit `visibility` attribute, and an undeclared cross-package
dependency fails at analysis time. `//bazel/tests:visibility_policy_test`
asserts that. Two rules follow:

- Dependencies flow toward `src/base`. Nothing depends on `src/broker`, which is
  the assembly point where concrete services are wired together.
- Prefer the narrowest visibility that works. Exposing a target to one consuming
  package is better than making it public.

The package graph must stay acyclic; `//tools:check_bazel_package_cycles`
enforces this and runs in continuous integration.

## Troubleshooting

| Symptom | Cause | Diagnose or fix |
|---|---|---|
| Build uses an unexpected Bazel version | The launcher ignores `.bazelversion` | `bazel --version` must print `9.1.0` |
| `no such package` for a native dependency, or a configure script fails | Missing host build tool | `command -v make perl git python3` |
| Hermetic toolchain fails to fetch or compile | Download failure or corrupted cache entry | `bazel test //bazel:toolchain_probe_test` |
| Every target rebuilds after switching configurations | Bazel discards the analysis cache when build options change | Expected; keep one configuration per working session, or check the active one with `bazel config` |
| `Lock file is no longer up-to-date` | `MODULE.bazel` changed without refreshing the lockfile | `bazel mod tidy && git diff --stat MODULE.bazel MODULE.bazel.lock` |
| clang-tidy reports missing headers | Stale compilation database | `bazel run //tools:compile_commands` |
| `Could not setup Async I/O ... /proc/sys/fs/aio-max-nr` at startup | The default `linux-aio` backend needs more request capacity than the host allows | `cat /proc/sys/fs/aio-max-nr`, then raise it or start with `--reactor-backend=io_uring` |
| Reactor fails to start with an io_uring error | Kernel older than the supported baseline, or a sandbox blocking `io_uring` syscalls | `uname -r` (5.15 or newer), then fall back with `--reactor-backend=epoll` |
| `--memory` appears to be ignored | Sanitizer configurations build Seastar against the system allocator, which does not honor the reactor memory budget | Confirm with the startup warning; use `--config=release` when memory limits matter |
| Startup fails while locking memory | `--lock-memory 1` exceeds the process limit | `ulimit -l` |
| Broker exits reporting the data directory is unusable | The configured path exists but is not a directory, or is not writable | Check the `data_directory` value and its permissions; missing directories are created automatically |
| Second broker exits immediately | Another process already owns the PID file in that data directory | Inspect `<data_directory>/kwaque.pid` and confirm the owning process |
| `Unable to set SCHED_FIFO ... try adding CAP_SYS_NICE` | Seastar cannot raise timer-thread priority as an unprivileged process | Harmless for development; grant `CAP_SYS_NICE` for latency-sensitive runs |
| `Perf-based stall detector creation failed (EACCESS)` | Kernel perf events are restricted; `EACCESS` is the runtime's own spelling of the `EACCES` errno | Harmless; set `/proc/sys/kernel/perf_event_paranoid` to 1 or less for kernel backtraces |
| `IO queue was unable to find a suitable maximum request length` | Seastar's I/O probe was cut off early on this device | Informational only |
| `ASan doesn't fully support makecontext/swapcontext` | Expected under the sanitizer configurations | Informational only |

## Protocol boundary

Protocol Buffers encode versioned, low-volume control schemas. They do not
define Kwaque's native TCP framing or its raw record-batch representation.

## Project documents

- [Contributing](CONTRIBUTING.md)
- [Security policy](SECURITY.md)
- [Dependency baseline](DEPENDENCIES.md)
- [Third-party software](THIRD_PARTY.md)
- [Code of conduct](CODE_OF_CONDUCT.md)

## Acknowledgements

Kwaque runs on [Seastar](https://github.com/scylladb/seastar), the
thread-per-core asynchronous runtime developed and maintained by ScyllaDB. The
broker builds against the [Seastar fork maintained by
Redpanda](https://github.com/redpanda-data/seastar), whose additional runtime
work Kwaque uses directly. Thank you to both projects, and to the maintainers of
every dependency listed in [THIRD_PARTY.md](THIRD_PARTY.md), for the work Kwaque
is built on.

## License

Kwaque's original work is licensed under the [Apache License 2.0](LICENSE).
Third-party dependencies remain subject to their respective licenses; see
[THIRD_PARTY.md](THIRD_PARTY.md) and [NOTICE](NOTICE).
