load("@bazel_skylib//rules:common_settings.bzl", "int_flag", "string_flag")
load("@rules_cc//cc:cc_library.bzl", "cc_library")
load("@rules_foreign_cc//foreign_cc:defs.bzl", "configure_make")

int_flag(
    name = "build_jobs",
    # Nested Make jobs are invisible to Bazel's resource scheduler. Keep the
    # inner build serial by default. For an isolated dependency build, override
    # it with --@openssl//:build_jobs=N.
    build_setting_default = 1,
    make_variable = "BUILD_JOBS",
)

string_flag(
    name = "build_mode",
    build_setting_default = "default",
    values = [
        "debug",
        "default",
        "release",
    ],
)

config_setting(
    name = "debug_mode",
    flag_values = {":build_mode": "debug"},
)

config_setting(
    name = "release_mode",
    flag_values = {":build_mode": "release"},
)

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
)

configure_make(
    name = "openssl_foreign_cc",
    args = [
        "-j$OPENSSL_BUILD_JOBS",
        "DESTDIR=$BUILD_TMPDIR/openssl_foreign_cc",
    ],
    configure_command = "Configure",
    configure_options = select({
        "@platforms//cpu:aarch64": ["linux-aarch64"],
        "@platforms//cpu:x86_64": ["linux-x86_64"],
    }) + [
        "--libdir=lib",
        "--openssldir=/etc/ssl",
        "--prefix=/",
        "no-docs",
        "no-tests",
    ] + select({
        ":debug_mode": ["--debug"],
        ":release_mode": ["--release"],
        "//conditions:default": [],
    }),
    env = {
        "OPENSSL_BUILD_JOBS": "$(BUILD_JOBS)",
        "SOURCE_DATE_EPOCH": "0",
    },
    lib_source = ":srcs",
    out_shared_libs = [
        "libcrypto.so.3",
        "libssl.so.3",
    ],
    toolchains = [":build_jobs"],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "openssl",
    hdrs = [":openssl_foreign_cc"],
    includes = ["openssl_foreign_cc/include"],
    visibility = ["//visibility:public"],
    deps = [":openssl_foreign_cc"],
)
