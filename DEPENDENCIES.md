# Dependency baseline

This file records Kwaque's compatible build inputs. Only versions declared by
the build and lock files are dependency pins.

## Compatibility baseline

| Input | Selected version/revision | Status |
|---|---|---|
| Bazel | `9.1.0` | Pinned |
| C++ language mode | C++23 | Required |
| LLVM/Clang | `23.1.0-rc2` | Hermetic toolchain |
| Protobuf | `33.5` | Pinned |
| Seastar | `a6ac2ff6190a4a9dce5059991355703e1073d11f` | Compatibility pin |

The Seastar archive at this baseline has SHA-256
`5918f72ec59c159a8d2fe36870e7d30c6e61426fde766d7dd6853fa7f9871f7f`.
It is selected as part of a single compatibility family with the Protobuf and
toolchain versions above.

Adopting a newer Seastar or Protobuf revision requires isolated compatibility
work followed by updates to this file, `THIRD_PARTY.md`, the Bazel pin and
checksum, the module lock, and the relevant compatibility tests.

## Source of truth

`MODULE.bazel` and `MODULE.bazel.lock` are the machine-readable dependency graph
when present. `THIRD_PARTY.md` must contain every direct dependency declared
there. This document must never silently disagree with those files.
