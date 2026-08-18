load("@bazel_skylib//rules:common_settings.bzl", "int_flag")
load("@rules_foreign_cc//foreign_cc:defs.bzl", "configure_make")

int_flag(
    name = "build_jobs",
    # Nested Make jobs are invisible to Bazel's resource scheduler. Keep the
    # inner build serial by default. For an isolated dependency build, override
    # it with --@hwloc//:build_jobs=N.
    build_setting_default = 1,
    make_variable = "BUILD_JOBS",
)

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
)

configure_make(
    name = "hwloc",
    args = ["-j$HWLOC_BUILD_JOBS"],
    configure_in_place = True,
    configure_options = [
        "--disable-cuda",
        "--disable-gl",
        "--disable-libudev",
        "--disable-nvml",
        "--disable-opencl",
        "--disable-pci",
        "--disable-rsmi",
        "--disable-shared",
        "--enable-static",
        "--runstatedir=/var/run/hwloc",
    ],
    env = {
        "CFLAGS": "-ffile-prefix-map=$$EXT_BUILD_ROOT=.",
        "CXXFLAGS": "-ffile-prefix-map=$$EXT_BUILD_ROOT=.",
        "HWLOC_BUILD_JOBS": "$(BUILD_JOBS)",
    },
    lib_source = ":srcs",
    out_binaries = [
        "hwloc-calc",
        "hwloc-distrib",
    ],
    out_static_libs = ["libhwloc.a"],
    toolchains = [":build_jobs"],
    visibility = ["//visibility:public"],
)

filegroup(
    name = "hwloc_calc",
    srcs = [":hwloc"],
    output_group = "hwloc-calc",
    visibility = ["//visibility:public"],
)

filegroup(
    name = "hwloc_distrib",
    srcs = [":hwloc"],
    output_group = "hwloc-distrib",
    visibility = ["//visibility:public"],
)
