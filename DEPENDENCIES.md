# Dependency baseline

This file records Kwaque's compatible build inputs. Only versions declared by
the build and lock files are dependency pins.

## Compatibility baseline

| Input | Selected version/revision | Status |
|---|---|---|
| Bazel | `9.2.0` | Pinned |
| C++ language mode | C++23 | Required |
| LLVM/Clang | `23.1.2` | Hermetic toolchain |
| Linux x86_64 sysroot | Ubuntu 22.04, `2026-05-05` snapshot | Hermetic headers and libraries |
| Linux aarch64 sysroot | Ubuntu 22.04, `2026-05-05` snapshot | Hermetic headers and libraries |
| Protobuf | `33.5` | Pinned |
| Seastar | `a6ac2ff6190a4a9dce5059991355703e1073d11f` | Compatibility pin |
| `unordered_dense` | `f30ed41b58af8c79788e8581fe57a6faf856258e` | Compatibility pin with exception-safety corrections |

The Abseil pin carries a Clang 23 compatibility patch that replaces deprecated
`lifetime_capture_by(this)` annotations with `lifetime_capture_by_this` when
available, retaining the existing fallback for older compilers. This preserves
lifetime diagnostics without suppressing warnings or changing runtime behavior.

The Protobuf module carries two exception-safety patches. One changes the
runtime parser, unknown-field set and `RepeatedPtrField` growth so an allocation
failure while parsing leaves the message consistent. The other changes the C++
code generator so generated string and message setters update presence only
after the field allocation succeeds. Because the second patch changes `protoc`,
the build sets `--@protobuf//bazel/toolchains:prefer_prebuilt_protoc=false`:
newer Protobuf releases default to a prebuilt, unpatched compiler. The patches
cover the generators used by proto3 and edition 2023 schemas;
`//tools:check_dependency_inventory` rejects other schema declarations, such as
edition 2024 `VIEW` strings, whose generator paths are not patched. A Protobuf
update must rebase both patches and keep their parser and accessor tests.

The xxHash module carries two build patches. The first caps its x86-64
run-time kernel selection at AVX2: 512-bit kernels can lower the clock of the
whole core, which a shard-per-core runtime shares. The second exports the
license file for the distribution. Neither changes any digest; the published
vectors in `//src/codec/tests:xxh3_test` fix the output.

The Seastar archive at this baseline has SHA-256
`5918f72ec59c159a8d2fe36870e7d30c6e61426fde766d7dd6853fa7f9871f7f`.
The archive is the Seastar fork maintained by Redpanda, selected as part of a
single compatibility family with the Protobuf and toolchain versions above.
The Bazel repository rule applies a narrow metrics exception-safety patch. It
allows metric-group and metric-definition construction plus aggregate-label
allocation failures to propagate, reserves native registration bookkeeping
before registry insertion, and removes a partially inserted series if
registration fails. It also stores each sanitized family name in that reserved
bookkeeping before publication and reuses it for allocation-free destruction
and replica removal instead of reconstructing the name during `noexcept`
teardown. A Seastar update must either retain that patch or prove the selected
revision provides equivalent behavior. Non-release, non-sanitizer builds enable
Seastar's native allocation-failure injection so the rollback boundary is
executable; release and system-allocator sanitizer builds disable it.

A scoped Seastar HTTP/metrics patch adds optional connection admission,
header/body bounds, absolute exchange deadlines, and bounded metrics collection
and formatting. The broker enables these controls for its administrative listener;
other native HTTP and metrics users retain the default behavior. Dependency updates
must preserve the early admission, parser-fragment and scrape-cleanup tests before
removing or replacing this patch.
Bounded snapshot accounting includes deque/map capacity and cached function storage;
dirty bounded refreshes release old cache capacity before replacement. Its trusted
callback-copy and temporary-allocation requirements are part of the native API
contract. Filtered and empty output paths retain preemption checks.
Snapshot bounds count only enabled series, which are the only ones a snapshot
copies, so the disabled per-peer queue metrics do not grow with the shard count.
Each source shard admits one bounded snapshot at a time. A scrape queues for each
source shard in shard order, up to its configured wait, so concurrent scrapes
serialize and each returns complete output instead of a truncated body. The server
counts connections refused at the connection limit and connections closed by a
header or exchange deadline.
The Seastar patches use Bazel's native patch application so changes to patch
contents invalidate the materialized repository.

A header compatibility patch includes `<new>` at global scope in the native
spinlock header, preserving its alignment and locking implementation with libc++.

A task-queue patch makes debug-build task shuffling replayable. Each shard seeds
its shuffle generator from a process-wide base seed and logs it; setting
`SEASTAR_SHUFFLE_TASK_QUEUE_SEED` to a logged value reuses that seed.

A scheduling-group patch rolls back local task queues and scheduling-specific
state when construction fails, and cleans initialized shards when a remote
initialization fails. Group creation and destruction share a native management
semaphore so a released group slot cannot be reused during cross-shard cleanup.
Cleanup also excludes concurrent scheduling-specific key registration using the
native key mutex; local construction rollback already holds that mutex.
Dependency updates must retain the throwing-constructor rollback tests and prove
equivalent cleanup before removing this patch.

A separate Seastar patch makes its custom `chunked_vector` compatible with
those failure guarantees: a new fragment is fully allocated before it is
published into the outer fragment vector, capacity changes commit afterward,
and the allocating copy surface is no longer declared `noexcept`. Together,
these changes ensure failed segmented growth leaves the custom bucket container
internally consistent.

The `unordered_dense` baseline remains pinned at
`f30ed41b58af8c79788e8581fe57a6faf856258e`. Its repository rule applies narrow
compatibility corrections for extracted, moved-from, reserve, rehash, replace,
and insertion-growth behavior. Empty and moved-from tables no longer allocate
replacement buckets, and bucket growth constructs its target storage before
publishing the corresponding shift and capacity state.
For Seastar's in-place chunked bucket container, a failed multi-fragment growth
also removes the newly appended suffix so the older pin's physical-size-based
probe wrap remains valid. A future dependency update must prove those
guarantees under Seastar allocation-failure injection before removing the
patch.

The sysroot archives are published under an `llvmorg-22.1.0` release path, but
that path identifies the sysroot artifact release rather than the selected
compiler version. Kwaque intentionally pairs those Ubuntu 22.04 snapshots with
the LLVM/Clang 23.1.2 toolchain pinned above.

OpenSSL is built as static libraries only (`no-shared`) and linked into the
binaries, so the package carries no shared libraries and no run path. It is
configured with `no-autoload-config` and `no-module`: the broker never reads a
host `openssl.cnf` or loads provider modules, and its cryptography does not
depend on the host's OpenSSL installation. `OPENSSLDIR` stays `/etc/ssl` so TLS
finds the system certificate store.

Foreign C/C++ dependency builds that contribute code to the distribution map
both their transient execroot and Bazel's canonical external-repository root.
The second mapping is required for Clang's resource include directory: Clang
resolves that directory through the real toolchain location outside the action
sandbox, and otherwise records the host-specific Bazel output base in DWARF.

Adopting a newer Seastar or Protobuf revision requires isolated compatibility
work followed by updates to this file, `THIRD_PARTY.md`, the Bazel pin and
checksum, the module lock, and the relevant compatibility tests.

## Updating dependencies

Seastar and Protobuf are updated independently, one per change, so that a
failure identifies its own cause. Never combine the two in one commit, and never
raise a pin without completing the verification steps below.

Both procedures record the same evidence: what moved, what was rebuilt, what was
tested, and the decision to adopt or retain.

### Updating Seastar

The Seastar revision is single-sourced in `bazel/versions.bzl` as
`SEASTAR_REVISION`. `bazel/repositories.bzl` builds the archive URL and strip
prefix from that constant and carries the archive checksum separately;
`src/base` compiles the same constant into the broker's reported build metadata.

1. **Compute the checksum** of the candidate archive before changing anything:

   ```bash
   curl -sL https://github.com/redpanda-data/seastar/archive/<revision>.tar.gz \
     | sha256sum
   ```

2. **Move the pin.** Set `SEASTAR_REVISION` in `bazel/versions.bzl` and the
   `seastar` archive `sha256` in `bazel/repositories.bzl`. These are the only two
   places a revision and its hash are declared.
3. **Reconcile the build overlay and patches.** Compare the upstream source and
   header lists against `bazel/thirdparty/seastar.BUILD`, update added, removed,
   or renamed compilation units, and verify every Seastar patch still applies
   and remains necessary. Do not import optional upstream features that the
   current graph does not need.
4. **Refresh the lockfile** and confirm it settles:

   ```bash
   bazel mod tidy
   bazel mod tidy   # second run must leave MODULE.bazel and the lockfile unchanged
   ```

5. **Rebuild and test** in every configuration that links Seastar:

   ```bash
   bazel build --config=release //:kwaque
   bazel test --config=dev //...
   bazel test --config=debug //...
   bazel build //:kwaque_tar //:kwaque_tar_sha256
   ```

6. **Update the records.** Revise the baseline table and checksum in this file
   and the Seastar row in `THIRD_PARTY.md`, then confirm they agree with the
   build:

   ```bash
   bazel run //tools:check_dependency_inventory
   ```

7. **Decide and record.** State explicitly whether the new revision is adopted or
   the previous pin is retained, and why.

### Updating Protobuf

Protobuf is a registry module. Its pin lives in `MODULE.bazel` as
`bazel_dep(name = "protobuf", version = ...)`. The broker's build metadata reads
the version from the resolved module's `protobuf_version.bzl`, so it always
reports the version it was built against.

1. **Move the pin** in `MODULE.bazel` and rebase the two Protobuf patches in
   `bazel/thirdparty`.
2. **Refresh the lockfile** with two `bazel mod tidy` runs, as above. A registry
   bump can raise shared transitive modules; if module resolution selects new
   versions of other direct dependencies, update their `MODULE.bazel` entries in
   the same change so the declared graph matches the resolved one.
3. **Re-run the schema compatibility surface**, which is what a Protobuf upgrade
   most plausibly breaks:

   ```bash
   bazel test --config=dev //proto/... //src/protocol/tests:control_fuzz_replay
   bazel test --config=fuzz //src/protocol/tests:control_fuzz
   ```

   This covers generated-code round-tripping, the committed golden byte fixture,
   malformed and oversized input handling, linking generated code into a
   Seastar-based library, and the Buf lint and wire-compatibility tests.
4. **Rebuild and test** the whole project and the package, as in the Seastar
   procedure.
5. **Update the records** in this file and `THIRD_PARTY.md`, then run
   `bazel run //tools:check_dependency_inventory`.
6. **Decide and record** adoption or retention, and why.

### Schema compatibility baseline

`//proto:schema_breaking_test` compares the control schemas with
`proto/schema_baseline.binpb` under Buf's `WIRE` rules, and
`//proto:schema_lint_test` applies `proto/buf.yaml`. A change that deliberately
accepts a new wire baseline regenerates the image from the repository root with
the Buf CLI version pinned in `MODULE.bazel`, and states why in its description:

```bash
buf build --config proto/buf.yaml --path proto --exclude-source-info \
  -o proto/schema_baseline.binpb .
```

### Automated update proposals

Dependabot proposes updates for registry modules in `MODULE.bazel` and for the
pinned GitHub Actions. It does not track the archives in
`bazel/repositories.bzl`, the LLVM toolchain, the sysroots, or `.bazelversion`;
check those by hand before each release.

### Rolling back

A dependency change is reverted as a unit. Restore the pin, the checksum where
one applies, any build-overlay edits, the lockfile, and the documentation rows in
the same revert, then confirm recovery:

```bash
bazel mod tidy   # must report no change against the restored lockfile
bazel test --config=dev //...
```

If a partial revert leaves the lockfile disagreeing with `MODULE.bazel`, builds
configured with `--lockfile_mode=error`, including every continuous-integration
job, fail until the two agree again.

### Adding or removing a dependency

Any change to the set of direct modules or pinned archives must add or remove its
`THIRD_PARTY.md` row in the same commit, recording version, source, license,
distribution scope, and purpose. `//tools:check_dependency_inventory` fails when
a declared dependency is missing from the inventory, when a recorded version
disagrees with the build, when a pinned archive lacks a checksum, or when the
declared and imported archive sets diverge in either direction.

A dependency whose code is linked into the broker or shipped in the package must
also have its upstream license material included in the distribution; see the
packaging rules under `bazel/packaging`.

## Source of truth

`MODULE.bazel` and `MODULE.bazel.lock` are the machine-readable dependency graph
when present. `THIRD_PARTY.md` must contain every direct dependency declared
there. This document must never silently disagree with those files.
