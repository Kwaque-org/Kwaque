# Dependency baseline

This file records Kwaque's compatible build inputs. Only versions declared by
the build and lock files are dependency pins.

## Compatibility baseline

| Input | Selected version/revision | Status |
|---|---|---|
| Bazel | `9.1.0` | Pinned |
| C++ language mode | C++23 | Required |
| LLVM/Clang | `23.1.0-rc2` | Hermetic toolchain |
| Linux x86_64 sysroot | Ubuntu 22.04, `2026-05-05` snapshot | Hermetic headers and libraries |
| Linux aarch64 sysroot | Ubuntu 22.04, `2026-05-05` snapshot | Hermetic headers and libraries |
| Protobuf | `33.5` | Pinned |
| Seastar | `a6ac2ff6190a4a9dce5059991355703e1073d11f` | Compatibility pin |

The Seastar archive at this baseline has SHA-256
`5918f72ec59c159a8d2fe36870e7d30c6e61426fde766d7dd6853fa7f9871f7f`.
The archive is the Seastar fork maintained by Redpanda, selected as part of a
single compatibility family with the Protobuf and toolchain versions above.

The sysroot archives are published under an `llvmorg-22.1.0` release path, but
that path identifies the sysroot artifact release rather than the selected
compiler version. Kwaque intentionally pairs those Ubuntu 22.04 snapshots with
the LLVM/Clang 23.1.0-rc2 toolchain pinned above.

Adopting a newer Seastar or Protobuf revision requires isolated compatibility
work followed by updates to this file, `THIRD_PARTY.md`, the Bazel pin and
checksum, the module lock, and the relevant compatibility tests.

## Baseline evaluations

Newer candidates for the two largest pins were evaluated in isolation. Both were
retained. These records exist so that a later reader knows the decision was
measured rather than assumed, and knows what would change it.

### Seastar, evaluated 2026-08-22, pin retained

Candidate: upstream `scylladb/seastar` at
`1c05d41f5271a1e9feb618f835776c34da734c19` (2026-08-13).
Decision: **retain** `a6ac2ff6190a4a9dce5059991355703e1073d11f`.

The pinned archive is a Seastar fork maintained by Redpanda, whose additional
runtime work Kwaque uses directly and is grateful for. Adopting the candidate
would give that work up: two compilation units and six headers (a CPU profiler, a
signal mutex, an internal timers header, and three chunked container headers),
along with the `handle` and `route` fields of `prometheus::config`,
`metrics::update_aggregate_labels()`, and `scheduling_group::get_stats()`.

The candidate was verified to work, so this is a preference and not a
compatibility limit. With the pin moved and the six headers and two sources
removed from the build overlay, and no other change: the runtime and its test
support library build; the full development suite passes; the same suite passes
under AddressSanitizer and UndefinedBehaviorSanitizer; and the optimized broker,
distribution archive, and checksum build. Every one of the 384 paths the build
overlay references resolves against the candidate: 381 sources plus three
generated parser headers whose inputs are present.

What the candidate would gain is small and currently unreachable. Its source
tree is eleven days and twelve commits ahead of the pin's base. Only three of
those commits touch code paths Kwaque uses at all: a null-pointer guard in
socket abort and address lookup, a shard-count fix during shutdown, and an
`iovec` lifetime fix in a reactor backend that is not the default. None changed
observable behavior here. The exported metric surface is identical across both
revisions, the same 67 series names including scheduler starvation time, and the
test results were indistinguishable.

Revisit this when Kwaque needs one of the upstream fixes, when the fork's base
lags upstream far enough that unfixed defects reach Kwaque's code paths, or when
Kwaque should control its own runtime revision directly rather than track another
project's fork. The last of those is a strategic decision, not a compatibility
one, and it should be made deliberately rather than by drifting.

### Protobuf, evaluated 2026-08-22, pin retained

Candidate: `protocolbuffers/protobuf` at
`720e5468cebbb124e23c72080b28f2a41c4134a2`, which declares version `37.0-dev`.
Decision: **retain** `33.5`.

The candidate is functionally compatible. Generated-code round-tripping, the
committed golden byte fixture, malformed and oversized input handling, linking
generated code into a Seastar-based library, the distribution package, the
subprocess broker tests, and the repository tooling tests all pass against it.
The objection is not compatibility.

It is rejected on release maturity and graph cost. The revision declares
`37.0-dev` and is explicitly not a long-term-support release, which makes it
unsuitable as the reproducible baseline other work builds on. Adopting it also
forces unrelated upgrades: module resolution fails outright until Kwaque raises
its own `rules_python` declaration from `1.7.0` to `2.3.0`, a major version
change affecting every Python target in the repository, and the resolved graph
then also admits `rules_rust`, `rules_kotlin`, `rules_java`, and
`rules_jvm_external`. Kwaque's schema surface is three proto3 string fields read
through a bounded parser; nothing in the candidate is needed to support it.

Revisit this when Protobuf publishes a long-term-support release to the registry.
Handle the `rules_python` major upgrade as its own change, with its own
verification, rather than as a side effect of a schema-library bump.

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
3. **Reconcile the build overlay.** Compare the upstream source and header lists
   against `bazel/thirdparty/seastar.BUILD` and update added, removed, or renamed
   compilation units. Do not import optional upstream features that the current
   graph does not need.
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
`bazel_dep(name = "protobuf", version = ...)`, and `PROTOBUF_VERSION` in
`bazel/versions.bzl` is the same version reported in the broker's build
metadata. **Both must move together**, or the binary will report a version it was
not built against.

1. **Move the pin** in `MODULE.bazel` and update `PROTOBUF_VERSION` in
   `bazel/versions.bzl` to match.
2. **Refresh the lockfile** with two `bazel mod tidy` runs, as above. A registry
   bump can raise shared transitive modules; if module resolution selects new
   versions of other direct dependencies, update their `MODULE.bazel` entries in
   the same change so the declared graph matches the resolved one.
3. **Re-run the schema compatibility surface**, which is what a Protobuf upgrade
   most plausibly breaks:

   ```bash
   bazel test --config=dev //proto/...
   bazel test --config=fuzz //proto/kwaque/common/v1:build_info_fuzz
   ```

   This covers generated-code round-tripping, the committed golden byte fixture,
   malformed and oversized input handling, and linking generated code into a
   Seastar-based library.
4. **Rebuild and test** the whole project and the package, as in the Seastar
   procedure.
5. **Update the records** in this file and `THIRD_PARTY.md`, then run
   `bazel run //tools:check_dependency_inventory`.
6. **Decide and record** adoption or retention, and why.

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
