# Third-party software

This file records the Kwaque's direct build and runtime dependencies. Do not
declare an additional direct module or archive without adding a row here in the
same change. The license distributed with each resolved artifact is
authoritative.

| Dependency | Version or revision | Source | License | Purpose |
|---|---|---|---|---|
| Bazel | 9.1.0 | https://github.com/bazelbuild/bazel | Apache-2.0 | Hermetic build driver |
| LLVM/Clang toolchain | 23.1.0-rc2 | Hermetic archive declared by the build | Apache-2.0 WITH LLVM-exception | C++23 compiler, linker, and analysis tools |
| `toolchains_llvm` | 1.7.0, archive override `98414f360d37e4fc9fb308b357d1bd8df9f92428` | https://github.com/bazel-contrib/toolchains_llvm | Apache-2.0 | Bazel LLVM toolchain registration |
| `platforms` | 1.1.0 | https://github.com/bazelbuild/platforms | Apache-2.0 | Bazel platform constraints |
| Abseil (`abseil-cpp`) | 20260526.0 | https://github.com/abseil/abseil-cpp | Apache-2.0 | Common C++ utilities required by the baseline graph |
| Bazel Skylib | 1.9.0 | https://github.com/bazelbuild/bazel-skylib | Apache-2.0 | Shared Starlark helpers |
| `buildifier_prebuilt` | 8.2.0.2 | https://github.com/keith/buildifier-prebuilt | Apache-2.0 | Hermetic Buildifier formatting and checks |
| `rhysd/actionlint` | 1.7.12 | https://github.com/rhysd/actionlint | MIT | GitHub Actions workflow validation |
| `actions/checkout` | 6.0.1 | https://github.com/actions/checkout | MIT | CI source checkout |
| `bazel-contrib/setup-bazel` | 0.19.0 | https://github.com/bazel-contrib/setup-bazel | Apache-2.0 | Bazel installation and CI caches |
| `rules_boost` | `f5b0f8c904f2487d8f5a9a956d4388724e627210` | https://github.com/nelhage/rules_boost | Apache-2.0 | Bazel rules for Boost |
| Boost | 1.84.0 | https://github.com/boostorg/boost | BSL-1.0 | Seastar runtime and test dependencies |
| CRC32C | 1.1.0 | https://github.com/google/crc32c | BSD-3-Clause | Checksums |
| fmt | 12.1.0 | https://github.com/fmtlib/fmt | MIT | Type-safe formatting |
| GoogleTest | 1.17.0.bcr.2 | https://github.com/google/googletest | BSD-3-Clause | C++ unit tests |
| liburing | 2.14 | https://github.com/axboe/liburing | LGPL-2.1-only OR MIT | Linux io_uring support for Seastar |
| LZ4 | 1.9.4 | https://github.com/lz4/lz4 | BSD-2-Clause | Compression support required by the Seastar build |
| Protobuf | 33.5 | https://github.com/protocolbuffers/protobuf | BSD-3-Clause | Control schemas and generated C++; not Kwaque transport framing |
| `rules_cc` | 0.2.18 | https://github.com/bazelbuild/rules_cc | Apache-2.0 | Bazel C/C++ rules |
| `rules_foreign_cc` | 0.15.1 | https://github.com/bazel-contrib/rules_foreign_cc | Apache-2.0 | Hermetic builds for native libraries without Bazel metadata |
| `rules_pkg` | 1.0.1 | https://github.com/bazelbuild/rules_pkg | Apache-2.0 | Distribution packages |
| `rules_python` | 1.7.0 | https://github.com/bazelbuild/rules_python | Apache-2.0 | Repository tooling and test scripts |
| yaml-cpp | 0.8.0 | https://github.com/jbeder/yaml-cpp | MIT | Broker configuration parsing |
| Seastar | `a6ac2ff6190a4a9dce5059991355703e1073d11f` | Pinned archive declared by the build | Apache-2.0 | Sharded asynchronous runtime |
| c-ares | 1.34.7 | https://github.com/c-ares/c-ares | MIT | Asynchronous DNS for Seastar |
| hwloc | 2.11.2 | https://github.com/open-mpi/hwloc | BSD-3-Clause | CPU and NUMA topology for Seastar |
| lksctp-tools | 1.0.19 | https://github.com/sctp/lksctp-tools | LGPL-2.1-or-later | SCTP support required by the selected Seastar BUILD graph |
| OpenSSL | 3.5.7 | https://github.com/openssl/openssl | Apache-2.0 | TLS and cryptography for Seastar |
| Ragel | 26.04.0-20260414092900-8841e561489e | https://www.colm.net/open-source/ragel/ | MIT | Generate Seastar protocol parsers at build time |
| `unordered_dense` | `f30ed41b58af8c79788e8581fe57a6faf856258e` | https://github.com/martinus/unordered_dense | MIT | Hash containers required by Seastar |

Transitive dependencies are resolved and locked by Bazel. Before distributing
a binary, generate a complete transitive license manifest and preserve every
license or notice required by the resolved artifacts.
