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

The `kwaque` entry point checks the required CPU instructions before loading the
native broker. Keep the packaged `bin/kwaque` and `bin/kwaque_native` together;
launch through `kwaque` so the prerequisite check runs first.

Seastar's default reactor backend is `linux-aio`. The broker also accepts
`--reactor-backend=io_uring`, `epoll`, or `asymmetric_io_uring`; see
[Troubleshooting](#troubleshooting) for choosing between them.

Use XFS or ext4 on a local filesystem for the data directory. The directory
must be writable by the broker process. Running the committed development
configuration does not require root privileges, device access, or privileged
ports. Hosts must provide enough unlocked memory for the selected Seastar
`--memory` value; production CPU, memory-locking, and filesystem tuning is not
yet automated. Native-allocator builds derive workload admission from the
smallest shard-local allocator after Seastar applies `--memory`. The broker
reserves 16 MiB of reactor headroom and a separate 4 MiB for admin state per
shard before dividing the eight workload budgets. Production startup requires
128 MiB plus that admin reservation per shard; 1 GiB per shard is recommended.
Explicit development fixtures retain the 64 MiB floor. System-allocator builds
use `diagnostic_memory_per_shard_bytes` as a cooperative workload budget;
`--memory` does not cap their process allocations.

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

The broker reads native runtime options from its command line and explicitly
selected `--io-properties` or `--io-properties-file` input. It does not load
personal `~/.config/seastar/seastar.conf` or `io.conf` files, and the two explicit
I/O sources cannot be combined. Enabled `--unsafe-bypass-fsync`,
`--kernel-page-cache`, and `--relaxed-dma` are rejected before reactor startup in
every broker profile.

Startup records the resolved profile, allocator/instrumentation capabilities,
requested runtime settings, and available observations before creating broker
data-directory state. The configuration checksum is SHA256 of the exact bytes
loaded for parsing; changing the file afterward affects a later launch, not the
current snapshot. Native settings such as device NOWAIT or successful NUMA binding
are marked unobserved where the option value cannot prove the result.
`developer_mode` does not implicitly change CPU placement, polling, allocator,
or durability. Non-loopback admin configuration emits an unauthenticated/TLS
exposure warning; the current admin API remains health, version, and metrics only.

Native-allocator broker builds abort when managed allocation cannot succeed after
reclaim. Configured admission limits still return typed errors, and injected
allocation failures remain exceptions for testing. System-allocator builds require
explicit `developer_mode: true` for diagnostic use and report native OOM abort as
unavailable. The presence-only `--abort-on-seastar-bad-alloc` option cannot disable
the broker default or enable native allocator behavior in a system-allocator build.

System-allocator diagnostic runs also require an explicit
`diagnostic_memory_per_shard_bytes`; the local example sets 128 MiB. Native builds
use observed allocator capacity even when that diagnostic setting is present.
With `storage_strict_data_init: true`, `.kwaque_data_dir` must already exist inside
the data directory before startup. The broker never creates this marker.
The operator must control the data directory and its parent paths; PID inode
checks protect cleanup identity, not arbitrary external directory replacement.

Production restart limiting defaults to `crash_loop_limit: 5`; developer mode
bypasses it. Prepared crash reports and restart state are created only after PID
ownership, which is held through shutdown bookkeeping. See the
[broker lifecycle policy](src/broker/README.md) for reset boundaries, drain health,
and the 15/120-second shutdown warning timers.

Startup reports read-only host checks for filesystem, free space, cgroup limits,
descriptors, swap, selected tuning state, and matching device I/O configuration.
It does not tune the host or treat configured I/O rates as measured throughput.
The admin listener uses bounded connections, headers, metrics work and absolute
request lifetimes; see [admin limits](src/admin/README.md).

## Development

Keep related commands in the same build configuration: switching configurations
can invalidate Bazel's analysis cache and rebuild dependencies. Personal overrides
belong in an untracked `user.bazelrc`.

The validation profiles enable first-party warnings as errors and select explicit
allocator and injection settings:

- `ci-debug`: native allocator with allocation-failure injection, without optimization.
- `ci-native`: native allocator without injection or optimization.
- `ci-release`: optimized, hardened native binaries with injection disabled.
- `ci-sanitizer`: system allocator with ASan and UBSan; injection is disabled.

Native reactor tests and benchmarks enable real-OOM abort. Synthetic injection
remains independently recoverable, and required fault sweeps verify that injection
actually occurred. Reactor and process-policy tests compare compiled capabilities
and effective OOM behavior with independent expectations supplied by each profile.
Unsupported process injection/OOM cases are reported as skipped.

`fuzz` combines libFuzzer with ASan and UBSan. The normal developer `dev`
configuration uses light optimization and ASan. All configurations are defined
in [`.bazelrc`](.bazelrc).

CI skips native builds, tests, formatting, and analysis for additions or edits
limited to README files, contributor/security documents, and prose under
`docs/`. Workflow syntax and CI selection checks still run. Source, tests,
build/tool configuration, dependency inventories, deletions, and unknown paths
receive the complete checks. Manual workflow dispatches always run the full CI
suite, as do changes whose complete Git comparison cannot be established.

CI disk caches are separated by architecture and build configuration. One job
per configuration writes a commit-specific snapshot; matching analysis and
golden jobs restore it, falling back to an earlier snapshot for a new commit.
Ordinary and fuzz clang-tidy jobs run independently of the release build.
The native policy job uses its own configuration and cache. Release jobs execute
focused runtime and process-policy tests after the ordinary build.

### Ordinary tests and builds

Run all ordinary tests, including reactor, smoke, and packaging tests:

```bash
bazel test --config=ci-debug \
  --build_tag_filters=-fuzz,-manual --test_tag_filters=-fuzz,-manual //...
```

Release coverage builds tests and benchmarks as well as libraries and the broker:

```bash
bazel build --config=ci-release --build_tag_filters=-fuzz,-manual //...
```

Run the ordinary suite with sanitizers:

```bash
bazel test --config=ci-sanitizer \
  --build_tag_filters=-fuzz,-manual --test_tag_filters=-fuzz,-manual //...
```

The real adapter and environment suite uses loopback networking, an in-process
DNS server, and directories below `TEST_TMPDIR`. Cleanup is awaited and failures
propagate. To repeat that focused suite in sandboxes:

```bash
bazel test --config=ci-debug --spawn_strategy=sandboxed \
  --runs_per_test=10 --cache_test_results=no //src/runtime/tests:hermetic_contracts
```

### Determinism goldens

The same fixed random, fault-decision, trace, terminal-digest, and structured-event
constants run on x86-64 and native AArch64 under both debug and release in CI.
Run the suite locally with either configuration:

```bash
bazel test --config=ci-debug //src/simulation/tests:determinism_goldens
bazel test --config=ci-release //src/simulation/tests:determinism_goldens
```

### Bounded fuzzing

The PR smoke exercises every configuration, control-message, fragmented-buffer,
scheduler, fault-schedule, fake-file, and fake-network fuzzer. Each target starts
with a checked-in corpus and a two-second fuzzing budget:

```bash
bazel test --config=ci --config=fuzz --keep_going --test_output=all \
  --test_env=KWAQUE_FUZZ_MINIMIZE_SECONDS=30 \
  --test_arg=-seed=1 --test_arg=-max_total_time=2 \
  //src/config:bootstrap_config_fuzz \
  //proto/kwaque/common/v1:build_info_fuzz \
  //src/bytes:fragmented_buffer_fuzz \
  //src/simulation/tests:scheduler_fuzz \
  //src/simulation/tests:fault_schedule_fuzz \
  //src/simulation/tests:fake_file_fuzz \
  //src/simulation/tests:fake_network_fuzz \
  //src/simulation/tests:signal_canary_test
```

The buffer/parser input cap is 4 KiB; stateful inputs are capped at 16 KiB and
have additional command, callback, object, and retained-byte limits. Every
stateful input owns a fresh fixture and drains it before returning.

The scheduled workflow gives each stateful fuzzer ten minutes. To run that
campaign locally:

```bash
bazel test --config=ci --config=fuzz --keep_going --test_output=all \
  --test_timeout=720 --test_env=KWAQUE_FUZZ_MINIMIZE_SECONDS=30 \
  --test_arg=-max_total_time=600 \
  //src/simulation/tests:scheduler_fuzz \
  //src/simulation/tests:fault_schedule_fuzz \
  //src/simulation/tests:fake_file_fuzz \
  //src/simulation/tests:fake_network_fuzz
```

The `ci` configuration runs local tests one at a time, so four healthy ten-minute
campaigns take about forty minutes plus build time. The native per-input timeout
and external watchdog also bound stuck inputs. Diagnostic minimization after a
failure has a separate budget; the original failure status is preserved.

The wrapper resolves runfiles before changing the child's working directory.
Writable corpora stay below `TEST_TMPDIR`; logs and original/minimized failure
inputs go into Bazel's test undeclared outputs, alongside `test.log` under
`bazel-testlogs`. CI uploads these outputs even after failure. Keep a fixed failure's
reproducer in the corresponding checked-in corpus when landing its fix.

`signal_canary_test` verifies an intentional reactor crash, minimization, and
fresh-process replay and should pass. Its underlying manual
`signal_canary_fuzz` target intentionally fails when run directly.

### Reproduction replay

A semantic mismatch prints a canonical block from `KQREPRO 01` through
`END KQREPRO`. It contains the input and configuration, schema identities, typed
outcome, terminal digest, scheduler trace, and structured events. Extract that
whole block into a file outside the checkout, set `reproduction` to that file's
path, and feed it to the replay runner:

```bash
bazel run --config=ci-debug //src/simulation/tests:fuzz_replay < "$reproduction"
```

Exit 0 means the recorded outcome and
artifacts were reproduced, 1 means replay differed, and 2 means the envelope was
invalid. A sanitizer signal may terminate before an envelope is emitted; retain
its native crash input and harness identity for replay through the same fuzzer.

The current harness version is 2; earlier harness versions are rejected before
scenario execution. Input and configuration are bounded at 16 KiB each, and the
structured-event log at 128 KiB. Scheduler traces are bounded at 128 KiB for
scheduler/rule cases, 1 MiB for file histories, and 4 MiB for concurrent network
histories. Large traces use the cooperative chunked codec; output lines remain
at most 4 KiB. A replay difference reports the artifact and first entry's context.

The subprocess reproduction test checks capture, replay, mutations, and portable
output and retains its canonical sample in test undeclared outputs:

```bash
bazel test --config=ci-debug //src/simulation/tests:fuzz_reproduction_test
```

### Benchmarks

Use release binaries for measurements. List the available benchmark binaries and
cases before selecting comparable work:

```bash
bazel query 'attr(tags, benchmark, //...)'
bazel run --config=ci-release //src/runtime/tests:runtime_contract_bench -- --list
```

The byte, runtime-contract, event, and simulation binaries include buffer, queue,
scheduler, trace, event, fake-file, network, and bandwidth cases. Simulation
absolute timings are informational. The paired comparison tool uses three
randomized rounds with at least seven samples per invocation, checks allocation
and task counts, and rejects a median paired timing regression above five percent.
A speed win requires all three rounds to be below the baseline.

Build the release binary, then write results to a fresh directory outside the
checkout. This example compares the paired queue-admission cases:

```bash
bazel build --config=ci-release //src/runtime/tests:runtime_contract_bench
mkdir -p "$HOME/.cache/kwaque"
kwaque_results="$(mktemp -d "$HOME/.cache/kwaque/bench.XXXXXXXX")"
python3 tools/compare_benchmarks.py \
  --binary bazel-bin/src/runtime/tests/runtime_contract_bench \
  --pair native_queue_admission.admit_pop_charge4096=kwaque_queue_admission.admit_pop_charge4096 \
  --output-dir "$kwaque_results/queue"
```

The tool retains native JSON, logs, invocation order, and a comparison manifest.
It requests OOM abort and records a native pre-run profile before accepting each
result. Comparisons require optimized native allocation with injection and
sanitizers disabled. The profile hook runs before timing and allocation snapshots;
the caller still supplies release-build evidence for the measured binary.
Equal work and fixture boundaries still require review; a passing time ratio
alone does not establish an equivalent workload.

### Formatting and repository checks

```bash
bazel run //tools:format_cpp_changed -- --check
bazel run //tools:buildifier_check
python3 tools/check_generated_artifacts.py
python3 tools/check_dependency_inventory.py
python3 tools/check_bazel_package_cycles.py
python3 tools/check_cross_shard_usage.py
python3 -m tools.check_runtime_boundaries
python3 tools/check_determinism.py
```

The determinism checker is a lexical tripwire; executable goldens and noise tests
remain necessary. Run the Python tooling tests directly without compiling C++:

```bash
python3 -B -m unittest discover -s tools -p '*_test.py'
python3 -B -m unittest bazel.fuzz_test_wrapper_test
```

### Static analysis

Generate a fresh compilation database in the same configuration as the inputs
being analyzed. Ordinary analysis includes tests, benchmarks, and simulation;
strict analysis selects production sources using Bazel target ownership and
package boundaries:

```bash
bazel build --config=ci-debug --build_tag_filters=-fuzz,-manual //...
bazel run --config=ci-debug //tools:compile_commands -- --config=ci-debug
bazel run --config=ci-debug //tools:clang_tidy
bazel run --config=ci-debug //tools:clang_tidy_strict
```

Fuzz-only translation units need the fuzz configuration. CI runs this in a
separate job, using ordinary checks for the fuzzers and their dependencies:

```bash
bazel build --config=ci --config=fuzz --build_tag_filters=fuzz //...
bazel run --config=ci --config=fuzz //tools:compile_commands -- --fuzz-only --config=ci --config=fuzz
bazel run --config=ci --config=fuzz //tools:clang_tidy
```

The databases remain ignored. Ordinary analysis retains every distinct compile
variant of a source file; strict analysis uses the production commands in
`.cache/clang-tidy-production/compile_commands.json`. Fuzz-only generation updates
the main database while preserving this production subset. Regenerate the debug
database before returning to strict analysis after switching build configurations.
The generator adjusts compiler flags for workspace analysis; normal builds
continue to enforce strict header layering.

Both clang-tidy commands use the parallel runner from the pinned LLVM toolchain,
with two processes by default. CI keeps this limit to control memory use. Each
completed file reports its elapsed time. Increase concurrency locally when
memory permits, or select individual source files for a quick iteration:

```bash
bazel run --config=ci-debug //tools:clang_tidy -- --jobs=4
bazel run --config=ci-debug //tools:clang_tidy_strict -- --jobs=4
bazel run --config=ci-debug //tools:clang_tidy -- src/runtime/file.cc
bazel run --config=ci-debug //tools:clang_tidy -- --profile src/runtime/file.cc
```

These commands reuse the prepared compilation database and generated inputs;
keep them in the same configuration. Use `--jobs=1` to minimize concurrent memory
use. `--profile` reports aggregated native check timings to identify expensive
checks. A source selection still checks all of that file's compile variants.
After changing a shared header, analyze its affected source files or run the
complete scope. CI retains full ordinary, strict-production, and fuzz coverage.
Bazel's disk cache speeds the preparation build; clang-tidy analysis still runs.

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
formatting, and generated-artifact checks. They require `pre-commit` on the
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
