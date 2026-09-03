"""Pinned native archives used by the Kwaque build."""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@toolchains_llvm//toolchain:sysroot.bzl", "sysroot")
load("//bazel:versions.bzl", "SEASTAR_REVISION")

def declare_native_dependencies():
    """Declares the pinned native repositories and sysroots used by the build."""
    http_archive(
        name = "c-ares",
        build_file = "//bazel/thirdparty:c-ares.BUILD",
        sha256 = "556f781dd188ad932dc8263fee0ad3aaba675b4cd8e54d86908681b43ce3e327",
        strip_prefix = "c-ares-1.34.7",
        url = "https://vectorized-public.s3.amazonaws.com/dependencies/c-ares-1.34.7.tar.gz",
    )

    http_archive(
        name = "hwloc",
        build_file = "//bazel/thirdparty:hwloc.BUILD",
        sha256 = "866ac8ef07b350a6a2ba0c6826c37d78e8994dcbcd443bdd2b436350de19d540",
        strip_prefix = "hwloc-2.11.2",
        url = "https://vectorized-public.s3.amazonaws.com/dependencies/hwloc-2.11.2.tar.gz",
    )

    http_archive(
        name = "lksctp",
        build_file = "//bazel/thirdparty:lksctp.BUILD",
        sha256 = "0c8fac0a5c66eea339dce6be857101b308ce1064c838b81125b0dde3901e8032",
        strip_prefix = "lksctp-tools-lksctp-tools-1.0.19",
        url = "https://vectorized-public.s3.amazonaws.com/dependencies/lksctp-tools-1.0.19.tar.gz",
    )

    http_archive(
        name = "openssl",
        build_file = "//bazel/thirdparty:openssl.BUILD",
        patch_args = ["-p1"],
        patches = ["//bazel/thirdparty:openssl-reproducible-buildinf.patch"],
        sha256 = "a8c0d28a529ca480f9f36cf5792e2cd21984552a3c8e4aa11a24aa31aeac98e8",
        strip_prefix = "openssl-3.5.7",
        url = "https://vectorized-public.s3.amazonaws.com/dependencies/openssl-3.5.7.tar.gz",
    )

    http_archive(
        name = "seastar",
        build_file = "//bazel/thirdparty:seastar.BUILD",
        patch_args = ["-p1"],
        patch_tool = "patch",
        patches = [
            "//bazel/thirdparty:seastar-chunked-vector-exception-safety.patch",
            "//bazel/thirdparty:seastar-metrics-registration-exception-safety.patch",
        ],
        sha256 = "5918f72ec59c159a8d2fe36870e7d30c6e61426fde766d7dd6853fa7f9871f7f",
        strip_prefix = "seastar-{}".format(SEASTAR_REVISION),
        url = "https://github.com/redpanda-data/seastar/archive/{}.tar.gz".format(SEASTAR_REVISION),
    )

    http_archive(
        name = "unordered_dense",
        build_file = "//bazel/thirdparty:unordered_dense.BUILD",
        patch_args = ["-p1"],
        patches = ["//bazel/thirdparty:unordered-dense-exception-safety.patch"],
        sha256 = "8393d08b2a41949c70345926515036df55643e80118b608bcec6f4202d4a3026",
        strip_prefix = "unordered_dense-f30ed41b58af8c79788e8581fe57a6faf856258e",
        url = "https://github.com/martinus/unordered_dense/archive/f30ed41b58af8c79788e8581fe57a6faf856258e.tar.gz",
    )

    sysroot(
        name = "x86_64_sysroot",
        sha256 = "0d85fc9e155e664403c1c3c40831d865796d36a91b78a2e6d8922aa6ad3f0375",
        urls = ["https://github.com/redpanda-data/llvm-project/releases/download/llvmorg-22.1.0/sysroot-ubuntu-22.04-x86_64-2026-05-05.tar.zst"],
    )

    sysroot(
        name = "aarch64_sysroot",
        sha256 = "1afc00adf978c90ad8ffd3b729180923c27d57a7702ea23ba35c714e11d0def2",
        urls = ["https://github.com/redpanda-data/llvm-project/releases/download/llvmorg-22.1.0/sysroot-ubuntu-22.04-aarch64-2026-05-05.tar.zst"],
    )
