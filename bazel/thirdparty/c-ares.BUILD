load("@rules_foreign_cc//foreign_cc:defs.bzl", "cmake")

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
)

filegroup(
    name = "license",
    srcs = ["LICENSE.md"],
    visibility = ["//visibility:public"],
)

cmake(
    name = "c-ares",
    cache_entries = {
        "BUILD_SHARED_LIBS": "OFF",
        "CARES_BUILD_TOOLS": "OFF",
        "CARES_INSTALL": "ON",
        "CARES_SHARED": "OFF",
        "CARES_STATIC": "ON",
        "CMAKE_INSTALL_LIBDIR": "lib",
    },
    env = {
        "CFLAGS": " ".join([
            "-ffile-prefix-map=$$EXT_BUILD_ROOT=.",
            "-ffile-prefix-map=$${EXT_BUILD_ROOT%%/sandbox/*}/external=external",
        ]),
        "CXXFLAGS": " ".join([
            "-ffile-prefix-map=$$EXT_BUILD_ROOT=.",
            "-ffile-prefix-map=$${EXT_BUILD_ROOT%%/sandbox/*}/external=external",
        ]),
    },
    generate_args = ["-GNinja"],
    lib_source = ":srcs",
    out_static_libs = ["libcares.a"],
    visibility = ["//visibility:public"],
)
