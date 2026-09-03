# Third-party software

This file records Kwaque's direct build/runtime dependencies and embedded
third-party source material. Do not declare an additional direct module or
archive, or embed third-party source, without recording it here in the same
change. The license distributed with each resolved artifact or retained in each
embedded source file is authoritative.

| Dependency | Version or revision | Source | License | Distribution scope | Purpose |
|---|---|---|---|---|---|
| Bazel | 9.1.0 | https://github.com/bazelbuild/bazel | Apache-2.0 | Build only | Hermetic build driver |
| LLVM/Clang toolchain | 23.1.0-rc2 | Hermetic archive declared by the build | Apache-2.0 WITH LLVM-exception | Build only | C++23 compiler, linker, and analysis tools |
| `toolchains_llvm` | 1.7.0, archive override `98414f360d37e4fc9fb308b357d1bd8df9f92428` | https://github.com/bazel-contrib/toolchains_llvm | Apache-2.0 | Build only | Bazel LLVM toolchain registration |
| `x86_64_sysroot` | `sysroot-ubuntu-22.04-x86_64-2026-05-05` | Hermetic archive declared by the build | Multiple Ubuntu 22.04 system licenses, including LGPL-2.1-or-later (glibc) | Build/link environment; not bundled | Linux x86_64 system headers and libraries for hermetic compilation and linking |
| `aarch64_sysroot` | `sysroot-ubuntu-22.04-aarch64-2026-05-05` | Hermetic archive declared by the build | Multiple Ubuntu 22.04 system licenses, including LGPL-2.1-or-later (glibc) | Build/link environment; not bundled | Linux aarch64 system headers and libraries for hermetic compilation and linking |
| `platforms` | 1.1.0 | https://github.com/bazelbuild/platforms | Apache-2.0 | Build only | Bazel platform constraints |
| Abseil (`abseil-cpp`) | 20260526.0 | https://github.com/abseil/abseil-cpp | Apache-2.0 | Static link input; license bundled | Common C++ utilities required by the baseline graph |
| Bazel Skylib | 1.9.0 | https://github.com/bazelbuild/bazel-skylib | Apache-2.0 | Build only | Shared Starlark helpers |
| `buildifier_prebuilt` | 8.2.0.2 | https://github.com/keith/buildifier-prebuilt | Apache-2.0 | Build only | Hermetic Buildifier formatting and checks |
| `rhysd/actionlint` | 1.7.12 | https://github.com/rhysd/actionlint | MIT | CI only | GitHub Actions workflow validation |
| `actions/checkout` | 6.0.1 | https://github.com/actions/checkout | MIT | CI only | CI source checkout |
| `bazel-contrib/setup-bazel` | 0.19.0 | https://github.com/bazel-contrib/setup-bazel | Apache-2.0 | CI only | Bazel installation and CI caches |
| `rules_boost` | `f5b0f8c904f2487d8f5a9a956d4388724e627210` | https://github.com/nelhage/rules_boost | Apache-2.0 | Build only | Bazel rules for Boost |
| Boost | 1.84.0 | https://github.com/boostorg/boost | BSL-1.0 | Static link input; license bundled | Seastar runtime and test dependencies |
| CRC32C | 1.1.0 | https://github.com/google/crc32c | BSD-3-Clause | Baseline dependency; not currently linked; license bundled | Checksums |
| fmt | 12.1.0 | https://github.com/fmtlib/fmt | MIT | Static link input; license bundled | Type-safe formatting |
| GoogleTest | 1.17.0.bcr.2 | https://github.com/google/googletest | BSD-3-Clause | Test only | C++ unit tests |
| liburing | 2.14 | https://github.com/axboe/liburing | MIT (selected from LGPL-2.1-only OR MIT) | Static link input; license bundled | Linux io_uring support for Seastar |
| LZ4 | 1.9.4 | https://github.com/lz4/lz4 | BSD-2-Clause | Static link input; license bundled | Compression support required by the Seastar build |
| PatchELF | 0.18.0 | https://github.com/NixOS/patchelf | GPL-3.0-or-later | Build only; not bundled | Sets the packaged broker's relative runtime-library search path |
| Protobuf | 33.5 | https://github.com/protocolbuffers/protobuf | BSD-3-Clause | Static link input; license bundled | Control schemas and generated C++; not Kwaque transport framing |
| `rules_cc` | 0.2.18 | https://github.com/bazelbuild/rules_cc | Apache-2.0 | Build only | Bazel C/C++ rules |
| `rules_foreign_cc` | 0.15.1 | https://github.com/bazel-contrib/rules_foreign_cc | Apache-2.0 | Build only | Hermetic builds for native libraries without Bazel metadata |
| `rules_pkg` | 1.0.1 | https://github.com/bazelbuild/rules_pkg | Apache-2.0 | Build only | Distribution packages |
| `rules_python` | 1.7.0 | https://github.com/bazelbuild/rules_python | Apache-2.0 | Build only | Repository tooling and test scripts |
| yaml-cpp | 0.8.0 | https://github.com/jbeder/yaml-cpp | MIT | Static link input; license bundled | Broker configuration parsing |
| Seastar | `a6ac2ff6190a4a9dce5059991355703e1073d11f` | Pinned archive declared by the build; the Seastar fork maintained by Redpanda, upstream project https://github.com/scylladb/seastar | Apache-2.0 | Static link input; license and notice bundled | Sharded asynchronous runtime |
| c-ares | 1.34.7 | https://github.com/c-ares/c-ares | MIT | Static link input; license bundled | Asynchronous DNS for Seastar |
| hwloc | 2.11.2 | https://github.com/open-mpi/hwloc | BSD-3-Clause | Static link input; license bundled | CPU and NUMA topology for Seastar |
| lksctp-tools (`lksctp`) | 1.0.19 | https://github.com/sctp/lksctp-tools | LGPL-2.1-or-later | Compile-time header only; license bundled | SCTP declarations required by the selected Seastar BUILD graph; implementation objects are not linked |
| OpenSSL | 3.5.7 | https://github.com/openssl/openssl | Apache-2.0 | Bundled shared libraries; license bundled | TLS and cryptography for Seastar |
| Ragel | 26.04.0-20260414092900-8841e561489e | https://www.colm.net/open-source/ragel/ | MIT | Build only | Generate Seastar protocol parsers at build time |
| `unordered_dense` | `f30ed41b58af8c79788e8581fe57a6faf856258e` | https://github.com/martinus/unordered_dense | MIT | Compile-time header; license bundled | Hash containers required by Seastar; patched for move and growth exception safety |

## Embedded source material

These entries are copied or trimmed into Kwaque and are not build/runtime
dependencies.

| Material | Source | License | Distribution scope | Purpose |
|---|---|---|---|---|
| Random123 Philox4x32-10 core and selected known-answer material | https://github.com/DEShawResearch/random123 | BSD-3-Clause | Embedded source and compiled binary material; full notice retained in source and root `NOTICE` | Deterministic simulation randomness and compatibility vectors |

Transitive dependencies are resolved and locked by Bazel. The binary package
preserves upstream license material for every direct native baseline dependency
that is bundled, linked, or contributes compile-time code. Review the resolved
graph and package contents before each release.
