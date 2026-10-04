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
| A Bazel launcher honoring `.bazelversion` | Selects the pinned Bazel `9.2.0`. [Bazelisk](https://github.com/bazelbuild/bazelisk) is the supported way to get it. |
| `git` | Release builds record the revision and worktree state; other builds report `unknown`. |
| `make` | Several native dependencies build through their own configure/make scripts. |
| `perl` | OpenSSL's `Configure` script is Perl. |
| `python3` | Repository tooling and subprocess tests. |

64-bit Linux on x86-64 or AArch64. A first build compiles the whole dependency
graph, including Seastar and OpenSSL, and needs several gigabytes of disk in the
Bazel cache.

### Runtime hosts

Kwaque supports 64-bit Linux on Westmere-class x86-64 processors and ARMv8-A
AArch64 processors with CRC and cryptography extensions. The `kwaque` entry point
checks the required CPU instructions before loading the native broker. Keep the
packaged `bin/kwaque` and `bin/kwaque_native` together; launch through `kwaque`
so the prerequisite check runs first.

Packaged binaries need glibc 2.34 or newer and the host's `libgcc_s.so.1`.
Everything else, including OpenSSL and the C++ runtime, is linked into them; the
package tests reject any other host library, a run path, or a newer glibc symbol.
A Linux 5.15 or newer kernel is the supported baseline for the Seastar runtime
and its io_uring backend.

Seastar's default reactor backend is `linux-aio`. The broker also accepts
`--reactor-backend=io_uring`, `epoll`, or `asymmetric_io_uring`; see
[Troubleshooting](#troubleshooting) for choosing between them.

Put the data directory on a local XFS filesystem; ext4 works but is reported as
a warning. Storage I/O bypasses the page cache, so tmpfs, overlayfs and network
filesystems are not supported. The directory must be writable by the broker's
user. The broker needs no root privileges, device access, or privileged ports.
Two capabilities help: `CAP_SYS_NICE` lets Seastar raise its timer threads to
real-time priority, and `CAP_IPC_LOCK` with a large enough `RLIMIT_MEMLOCK`
allows `--lock-memory`. The reference systemd unit grants both.

Hosts must provide enough unlocked memory for the selected Seastar
`--memory` value; production CPU, memory-locking, and filesystem tuning is not
yet automated. Native-allocator builds derive workload admission from the
smallest shard-local allocator after Seastar applies `--memory`. The broker
reserves 16 MiB of reactor headroom and a separate 4 MiB for admin state per
shard before dividing the eight workload budgets. Production startup requires
128 MiB plus that admin reservation per shard; 1 GiB per shard is recommended.
Explicit development fixtures retain the 64 MiB floor. System-allocator builds
use `diagnostic_memory_per_shard_bytes` as a cooperative workload budget;
`--memory` does not cap their process allocations.

In a container, the cgroup's CPU set and memory limit bound the broker; choose
`--smp` and `--memory` within them. Under Kubernetes on Linux 6.12 through 7.0,
except long-term releases that carry the scheduler fix, Seastar enables
`--overprovisioned` by itself to avoid a scheduler deadlock.

#### Host checks

At startup the broker grades the host and logs one `host check` line per item.
The grades are advice: none of them stops startup, and the broker changes no host
setting except raising its own open-file soft limit.

| Check | Recommended | When not met |
|---|---|---|
| `filesystem` | XFS for the data directory | Warning on ext4; error on any other filesystem |
| `disk_free_bytes` | At least 10 GiB free | Warning |
| `host_physical_memory_bytes` | Readable | Warning |
| `cgroup_memory_limit_bytes` | Readable | Warning |
| `host_memory_mib_per_cgroup_cpu` | At least 2048 MiB per available CPU | Warning |
| `cgroup_effective_cpuset_cpus` | Readable | Warning |
| `cgroup_cpu_quota_millicores` | Readable | Warning |
| `cgroup_version` | Readable | Warning |
| `descriptor_limits` | Open-file soft limit of at least 200,000, after it is raised to the hard limit | Warning; error below 10,000 |
| `swap_bytes` | Readable | Warning |
| `swappiness` | `vm.swappiness` = 1 | Warning |
| `aio_max_nr` | `fs.aio-max-nr` of at least 10,000,137 | Warning |
| `clocksource` | `tsc` on x86-64, `arch_sys_counter` on AArch64 | Warning |
| `transparent_hugepages` | `always` or `madvise` | Warning |
| `io_calibration_configured` | An I/O properties file (see below) | Warning |
| `io_calibration_device` | Properties for the data directory's device, with finite rates | Warning |
| `data_mount_device` | Readable | Warning |

With the `linux-aio` backend each shard asks for 11,026 kernel AIO control blocks:
1,024 for storage, 2 for preemption and 10,000 for networking. When
`fs.aio-max-nr` is lower, Seastar shrinks the networking share or, below the
minimum, refuses to start.

#### I/O calibration

Seastar schedules disk I/O from measured device rates. Without them it treats
the disk as unlimited, so foreground and background work compete without bounds
when the disk saturates. Measure the data directory's device once with the
packaged `iotune`, while the broker is stopped, and pass the result at every
start:

```bash
bin/iotune --evaluation-directory /var/lib/kwaque \
  --properties-file /etc/kwaque/io-properties.yaml
bin/kwaque --config /etc/kwaque/kwaque.yaml \
  --io-properties-file /etc/kwaque/io-properties.yaml
```

With the reference systemd unit, set
`KWAQUE_ARGS=--io-properties-file /etc/kwaque/io-properties.yaml` in
`/etc/default/kwaque`.

## Quick start

Build the broker and run it from the repository root with the committed
development configuration, which it reads from `conf/kwaque.yaml` by default:

```bash
bazel build --config=dev //:kwaque
bazel-bin/src/broker/kwaque --smp 1
```

The development example binds the administrative listener to
`127.0.0.1:9644` and keeps its data in `./data`, resolved against the directory
the broker starts in. In another shell:

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
bazel-bin/src/broker/kwaque --version
```

### Configuration

The broker reads one bootstrap file, `conf/kwaque.yaml` relative to the working
directory unless `--config` names another. [`conf/kwaque.yaml`](conf/kwaque.yaml)
is the development example; [`conf/kwaque.production.yaml`](conf/kwaque.production.yaml)
is the production example, installed as `etc/kwaque/kwaque.yaml` by the package.

| Key | Default | Meaning |
|---|---|---|
| `schema_version` | required | Configuration schema; this broker accepts `1` |
| `data_directory` | `./data` | Broker state; must be absolute unless `developer_mode` is true |
| `admin.address`, `admin.port` | `127.0.0.1`, `9644` | Numeric listener address and port |
| `developer_mode` | `false` | Relaxes resource suitability checks and crash-loop limiting |
| `storage_strict_data_init` | `false` | Require `.kwaque_data_dir` in the data directory |
| `crash_loop_limit` | `5` | Unclean exits before startup is refused; `null` disables |
| `diagnostic_memory_per_shard_bytes` | unset | Workload budget for system-allocator diagnostic builds |

The file is one YAML 1.2 document of at most 64 KiB of UTF-8 text, nested at
most eight collections deep. Unknown and duplicate keys are rejected. Integers
are plain decimal without leading zeros or prefixes, booleans are `true` or
`false` (any of the three core capitalizations), and numbers and booleans must
be unquoted and untagged. A string that reads as another type, such as `123`,
must be quoted. Values are not shell words: a leading `~` is rejected rather
than expanded. The broker logs the absolute configuration path and data
directory it used.

The schema version is checked before any other key, so a file written for a
newer broker reports its version rather than its first unfamiliar key. Any change
to the key set or to the meaning of a key increments the version.

Log levels are runtime options, not configuration: use
`--default-log-level` and `--logger-log-level kwaque-broker=debug`. List the
available loggers with `--help-loggers`.

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
ownership, which is held through shutdown bookkeeping.

Startup reports read-only host checks for filesystem, free space, cgroup limits,
descriptors, swap, selected tuning state, and matching device I/O configuration.
It does not tune the host or treat configured I/O rates as measured throughput.
The admin listener uses bounded connections, headers, metrics work and absolute
request lifetimes.

### Process contract

Signals:

| Signal | Effect |
|---|---|
| `SIGTERM`, `SIGINT` | Drain and stop; a repeated signal does not escalate |
| `SIGHUP` | Ignored; the configuration is static, so restart to apply changes |
| `SIGKILL` | Immediate termination; the next start sees an unclean exit |

Exit statuses are stable, so a service manager can decide whether to restart:

| Status | Meaning | Retry? |
|---|---|---|
| 0 | Clean stop, or a stop requested before startup completed | |
| 1 | Unexpected failure | Yes |
| 2 | Invalid command line | No |
| 6 | Invalid configuration or runtime options | No; fix the configuration |
| 10 | Another process owns the data directory (`kwaque.pid` is locked) | No |
| 11 | The crash-loop limit refused startup | No; inspect `crash_reports` |
| 12 | The CPU lacks instructions the broker was built for | No |

The broker sends `READY=1` to the service manager named by `NOTIFY_SOCKET` once
startup completes and `STOPPING=1` when it starts to drain, as `sd_notify(3)`
describes. The package includes a reference systemd unit,
`share/kwaque/systemd/kwaque.service`, for a broker installed under `/opt/kwaque`
and running as the `kwaque` user. It uses `Type=notify`, restarts the broker
after an unexpected failure (status 1), and keeps a broker that cannot start from
restarting in a loop with `RestartPreventExitStatus=2 6 10 11 12`. Its
`TimeoutStopSec` leaves room for a full drain: a broker killed while draining
counts toward its crash-loop limit.

Health endpoints:

- `/v1/health/live` returns 200 from the moment the admin listener starts
  until the broker stops, including while it drains. A liveness failure means the
  process should be restarted.
- `/v1/health/ready` returns 200 only after startup completes and 503 from the
  start of drain. Route traffic by readiness.
- The admin listener starts after the other startup stages. Before it starts,
  liveness requests are refused, so give a supervisor a startup allowance, such
  as a Kubernetes `startupProbe` on `/v1/health/live`, rather than a short
  liveness deadline.
- Every endpoint answers `HEAD`. Errors are RFC 9457 problem documents
  (`application/problem+json`) with a stable `code` member; other methods on a
  known path return 405 with `Allow: GET, HEAD`.

The data directory holds `kwaque.pid`, an exclusive lock for the broker's
lifetime. A directory the broker creates is private to its owner. At startup the
broker raises its open-file soft limit to the hard limit; a limit below 10,000
is reported as an error and below 200,000 as a warning, so set `LimitNOFILE` or
the equivalent high enough for the deployment.

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

Every configuration except `release` guards Seastar thread stacks and enables
libc++'s extensive hardening checks. Configurations that use the system
allocator (`dev`, `debugger`, `debug`, `ci-sanitizer`, and `fuzz`) also enable
Seastar's debug checks: cross-shard pointer and promise checks, forced
preemption, and task-queue shuffling. `fuzz` keeps task order unshuffled so a
crashing input replays.

Reactor tests and benchmarks run on the `epoll` backend unless
`--//bazel:reactor_backend=linux-aio` or `--//bazel:reactor_backend=io_uring` is
set. The CI debug suite runs on `linux-aio`, and a scheduled job runs it on
`io_uring`. Tests tagged `exclusive` assert bounds on real elapsed time, so Bazel
runs them alone.

CI skips native builds, tests, formatting, and analysis for additions or edits
limited to README files, contributor/security documents, and prose under
`docs/`. Workflow syntax and CI selection checks still run. Source, tests,
build/tool configuration, dependency inventories, deletions, and unknown paths
receive the complete checks. Manual workflow dispatches always run the full CI
suite, as do changes whose complete Git comparison cannot be established.

CI caches downloads only: Bazel itself and the repository cache, written by runs
on `main` and restored by pull requests and merge queue runs. Build outputs are
not cached, because per-configuration snapshots exceed the repository's cache
budget and evict one another; every job compiles from source. The analysis jobs
and the native policy job run independently of the release builds.

The x86-64 and AArch64 release jobs build every ordinary target, run the focused
runtime and process-policy tests and the determinism goldens without cached
results, and then test the shipped package, the broker processes and the fuzz
replays on `linux-aio`. A newer push cancels a superseded pull request run but
never a run on `main`.

A single `CI result` job depends on every other job; it is the status check to
require before merging. It fails when any job fails or is cancelled, including a
job skipped because an earlier one failed, which GitHub would otherwise count as
passing.

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

Tests that start the broker or another native binary use the same sanitizer
options as C++ tests, so a sanitizer report fails them, and they scan the
broker's log for unexpected errors. Each broker in a test listens on its own
loopback address. Broker logs are kept in Bazel's test undeclared outputs.

`--//bazel:reactor_backend=epoll|linux-aio|io_uring` selects the reactor
backend for every test, including the Python harnesses; CI runs linux-aio, and
a scheduled job runs io_uring.

Sanitizer and debug builds shuffle the reactor's task queue. Each shard logs
`task queue shuffle seed N`; rerun a failure in the same order with
`--test_env=SEASTAR_SHUFFLE_TASK_QUEUE_SEED=N`. Test randomness otherwise comes
from explicit fixture seeds, which a repository check enforces.

A scheduled job repeats every `smoke` and `stress` test twenty times with
GoogleTest shuffling, and another collects line coverage:

```bash
bazel test --config=ci-debug --runs_per_test=20 --cache_test_results=no \
  --test_env=GTEST_SHUFFLE=1 --build_tag_filters=smoke,stress \
  --test_tag_filters=smoke,stress,-manual //...
bazel coverage --config=ci-debug \
  --build_tag_filters=-fuzz,-manual,-benchmark \
  --test_tag_filters=-fuzz,-manual,-benchmark //...
```

The combined report is `bazel-out/_coverage/_coverage_report.dat`.

### Determinism goldens

The same fixed random, fault-decision, trace, terminal-digest, and structured-event
constants run on x86-64 and native AArch64 under both debug and release in CI:
inside the debug suite, inside both release jobs, and in a dedicated AArch64
debug job.
Run the suite locally with either configuration:

```bash
bazel test --config=ci-debug //src/simulation/tests:determinism_goldens
bazel test --config=ci-release //src/simulation/tests:determinism_goldens
```

### Bounded fuzzing

The PR smoke runs every test tagged `fuzz`, so a new fuzz target is included
without editing the workflow. Each target starts with its checked-in corpus and
a two-second fuzzing budget:

```bash
bazel test --config=ci --config=fuzz --keep_going --test_output=all \
  --test_env=KWAQUE_FUZZ_MINIMIZE_SECONDS=30 \
  --test_arg=-seed=1 --test_arg=-max_total_time=2 \
  --build_tag_filters=fuzz --test_tag_filters=fuzz,-manual //...
```

Most parser inputs are capped at 4 KiB; the configuration parser accepts one
byte past its 64 KiB production limit. Stateful inputs are capped at 16 KiB and
have additional command, callback, object, and retained-byte limits. Every
stateful input owns a fresh fixture and drains it before returning.

Each fuzz target also has a `<name>_replay` test that runs the empty input and
its corpus through the target in ordinary builds. CI runs the replays in the
debug and sanitizer suites and in the x86-64 and AArch64 release jobs, which
never fuzz.

The scheduled workflow gives every test tagged `fuzz-campaign` ten minutes, one
target per job. To run one campaign locally:

```bash
bazel test --config=ci --config=fuzz --test_output=all \
  --test_timeout=720 --test_env=KWAQUE_FUZZ_MINIMIZE_SECONDS=30 \
  --test_arg=-max_total_time=600 --test_arg=-timeout=15 \
  //src/simulation/tests:scheduler_fuzz
```

The native per-input timeout and an external watchdog bound stuck inputs.
Diagnostic minimization after a failure has a separate budget; the original
failure status is preserved.

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

Each benchmark also has a `<name>_test` target that runs every case once with
`--iterations=1 --duration=0 --runs=1`, so ordinary test runs execute benchmark
code without measuring it.

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
Invocations use the `epoll` reactor backend unless `--reactor-backend=linux-aio`
or `--reactor-backend=io_uring` selects another; the manifest records the choice.
Equal work and fixture boundaries still require review; a passing time ratio
alone does not establish an equivalent workload.

### Formatting and repository checks

```bash
bazel run //tools:format_cpp_changed -- --check
bazel run //tools:format_cpp_changed -- --check --base=main
bazel run //tools:buildifier_check
ruff format --check && ruff check
python3 tools/check_generated_artifacts.py
python3 tools/check_dependency_inventory.py
python3 tools/check_package_licenses.py
python3 tools/check_bazel_package_cycles.py
python3 tools/check_cross_shard_usage.py
python3 -m tools.check_runtime_boundaries
python3 tools/check_determinism.py
```

`--base=main` also checks files committed on a branch since it left `main`.
Buildifier lint warnings fail the check like formatting differences, and
`//tools:buildifier_fix` applies the fixable ones. Python is formatted and linted
with ruff 0.16.9 using `.ruff.toml`. The license check compares the package's
`licenses/` directories with the C and C++ dependencies of the packaged binaries.

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
bazel build --config=ci-debug --remote_download_outputs=all \
  --aspects=//bazel:analysis_inputs.bzl%analysis_inputs \
  --output_groups=clang_tidy_inputs --build_tag_filters=-fuzz,-manual //...
bazel run --config=ci-debug //tools:compile_commands -- --config=ci-debug
bazel run --config=ci-debug //tools:clang_tidy -- --production-config=.clang-tidy-strict
```

Fuzz-only translation units need the fuzz configuration. CI runs this in a
separate job, using ordinary checks for the fuzzers and their dependencies:

```bash
bazel build --config=ci --config=fuzz --remote_download_outputs=all \
  --aspects=//bazel:analysis_inputs.bzl%analysis_inputs \
  --output_groups=clang_tidy_inputs --build_tag_filters=fuzz //...
bazel run --config=ci --config=fuzz //tools:compile_commands -- --fuzz-only --config=ci --config=fuzz
bazel run --config=ci --config=fuzz //tools:clang_tidy
```

The preparation builds request C++ compilation prerequisites throughout the
selected dependency graph, including implementation dependencies and build-tool
configurations. Code generators and required foreign-library build actions may
still run; linked tests and benchmarks are not requested merely for analysis.
`--remote_download_outputs=all` ensures cached generated headers and other
intermediate inputs are present for standalone clang-tidy. Full compilation and
link validation remain in the build and test jobs.

The databases remain ignored. Ordinary analysis retains every distinct compile
variant of a source file; strict analysis uses the production commands in
`.cache/clang-tidy-production/compile_commands.json`. Fuzz-only generation updates
the main database while preserving this production subset. Regenerate the debug
database before returning to strict analysis after switching build configurations.
The generator adjusts compiler flags for workspace analysis; normal builds
continue to enforce strict header layering.

The combined command partitions by exact compiler arguments: production commands
receive strict checks once, and every remaining command receives baseline checks.
A test variant of a production source remains in the baseline partition. Both
passes run even if one reports findings, and either failure fails the command.
The original databases are preserved. Without `--production-config`,
`//tools:clang_tidy` runs the full baseline scope; `//tools:clang_tidy_strict`
remains available for a standalone production pass.

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

Every enabled check is an error. Both configurations report diagnostics in
first-party headers as well as source files; headers under `external/` and
generated headers under `bazel-out/` are excluded. Strict production checks add
the static analyzer's C++ checkers, `this auto` for capturing lambda coroutines
(their captures then live in the coroutine frame), unused Seastar futures, and
move, copy and redundancy checks to the baseline. Checks are listed one by one,
apart from static-analyzer families, so a toolchain upgrade cannot enable new
ones.

### Package

Build the shipped artifact with the release configuration:

```bash
bazel build --config=release //:kwaque_tar //:kwaque_tar_sha256
bazel test --config=release //bazel/packaging:all
```

The archive contains the broker and its CPU-checking launcher, `iotune`, the
production configuration as `etc/kwaque/kwaque.yaml` with the development
example beside it, a reference systemd unit, project license and notice files,
and upstream license material for every dependency compiled or linked into the
binaries. Run the extracted broker with its installed configuration:

```bash
bin/kwaque --config etc/kwaque/kwaque.yaml
```

The package tests assert the exact file layout, the libraries and glibc version
each binary needs, the checksum, and that the extracted broker starts and stops
cleanly; under the native allocator it starts the installed production
configuration. A scheduled job builds the release package twice, on separate
runners from separate checkout paths and output bases, and requires
byte-identical archives.

### Pre-commit hooks

```bash
pre-commit install        # run the hooks on every commit
pre-commit run --all-files
```

The hooks cover merge-conflict markers, large added files, line endings,
whitespace, end-of-file newlines, Python formatting and lint, C++ and Protobuf
formatting, Bazel formatting and lint, and generated-artifact checks. Hook
revisions are frozen to commits. They require `pre-commit` on the host.
Continuous integration enforces the formatting, lint and generated-artifact checks
but not the whitespace, line-ending and end-of-file hooks.

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
| `src/model`, `src/storage`, `src/protocol`, `src/raft`, `src/metadata`, `src/cluster`, `src/replication`, `src/consumer`, `src/cloud`, `src/security` | Additional core packages. |
| `proto/` | Versioned Protocol Buffers control schemas and their generated-code consumers. |
| `conf/` | Example broker configuration. |
| `bazel/` | Build rules, dependency declarations, third-party overlays, packaging, rule probes. |
| `tools/` | Repository scripts: formatting, static analysis, compilation database, integrity checks. |
| `tests/smoke/` | Subprocess tests that exercise the built broker as a process. |

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
| Build uses an unexpected Bazel version | The launcher ignores `.bazelversion` | `bazel --version` must print `9.2.0` |
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

The implemented controls are handshake requests and responses, redirects, and
errors. They carry identities, scoped epochs, capability advertisements and
bounded destination tokens. Accepting these values does not perform negotiation,
authenticate a peer, authorize a redirect or resolve an endpoint. Callers supply
independent expected identities to the [control codec](src/protocol/control_codec.h).

The default control profile permits a 64-KiB payload, eight nesting levels and
256 tags and packed scalar elements counted together. Capability sets are
ordered and unique, with at most 16 protocol versions, 32 format entries and
16 compression IDs.
Known singular duplicates reject before native parsing; embedded BuildInfo
retains its informational merge semantics. Explicit presence distinguishes absent
fields from present zero or empty values. Unknown fields consume the enclosing
limits and are discarded during owning conversion. Advertised future capabilities
remain data; unknown peer and error enum values reject. Serialized Protobuf bytes are
not canonical identities or fingerprints.

`//proto:schema_lint_test` applies the Buf lint rules in
[`proto/buf.yaml`](proto/buf.yaml), and `//proto:schema_breaking_test` rejects
wire-incompatible changes against the committed
[`proto/schema_baseline.binpb`](proto/schema_baseline.binpb).

Decoding reserves input once, copies the admitted control payload, checks its
wire profile, and admits fresh generated state and owning conversion storage.
The default aggregate generated/staging/conversion bound is 1 MiB, and each
served contiguous allocation must fit 128 KiB. Callers retain reservations for
other live owners and native execution state, and share one exclusive cooperative
work account through sequential children. The returned value owns its strings
and vectors. Temporary cleanup finishes before final cancellation polling and
publication; failure restores borrowed parser position and marks. The
[compound frame codec](src/protocol/control_frame_codec.h) commits the outer
frame only after those checks and cleanup succeed.

The format integration tests carry exact assigned-batch bytes through WAL and
segment representations, then verify extent, page and target relationships.
Semantic batch digests retain original identity and ordered record content;
sparse rewrites carry that digest without reconstructing removed records.
Exact-object and extent hashes cover stored bytes. Parsed metadata and completed
page walks do not by themselves establish target integrity, file durability or
request completion. Those responsibilities belong to their storage owners.

Run the focused control and format checks with the current build profile:

```bash
bazel test //src/protocol/tests:golden_tests \
  //src/protocol/tests:memory_qualification_test \
  //src/model/tests:format_fixture_test \
  //src/storage/tests:format_fixture_test \
  //src/storage/tests:format_integration_test \
  //src/compression/tests:format_fixture_test \
  //tools:verify_format_fixtures_test --test_output=errors
```

Memory qualification uses isolated processes and records the effective allocator
profile, complete observed peaks and retained owner bounds. System-allocator
runs check semantics but explicitly skip native allocation qualification.
Critical-allocation classification requires an injection-enabled native build;
injection need not be armed. Without it, native operation bounds and bounds based
on all new allocations are still checked. Owners that require the critical
subset report that bound as unavailable and explicitly skip full qualification.
Use `ci-debug` for those additional checks; release timing remains a separate
profile. The protocol benchmarks label preflight, preflight plus native parsing, conversion,
serialization, complete payload decoding and framed decoding separately; compare
the same responsibility and include returned-owner cleanup. These checks do not
measure a socket, a connection, shard RSS or arbitrary caller-owned state.

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
