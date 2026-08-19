"""Canonical C++ library and binary rules for Kwaque."""

load("@rules_cc//cc:cc_binary.bzl", "cc_binary")
load("@rules_cc//cc:cc_library.bzl", "cc_library")
load(":internal.bzl", "kwaque_copts")

def kwaque_cc_library(
        name,
        srcs = [],
        hdrs = [],
        deps = [],
        implementation_deps = [],
        defines = [],
        local_defines = [],
        copts = [],
        visibility = None,
        testonly = False,
        alwayslink = False,
        tags = []):
    """Defines a first-party C++ library with Kwaque's common policy."""
    cc_library(
        name = name,
        srcs = srcs,
        hdrs = hdrs,
        alwayslink = alwayslink,
        copts = kwaque_copts() + copts,
        defines = defines,
        deps = deps,
        features = ["layering_check"],
        implementation_deps = implementation_deps,
        local_defines = local_defines,
        tags = tags,
        testonly = testonly,
        visibility = visibility,
    )

def kwaque_cc_binary(
        name,
        srcs = [],
        deps = [],
        defines = [],
        local_defines = [],
        copts = [],
        linkopts = [],
        data = [],
        visibility = None,
        testonly = False,
        tags = []):
    """Defines a first-party C++ executable with Kwaque's common policy."""
    cc_binary(
        name = name,
        srcs = srcs,
        copts = kwaque_copts() + copts,
        data = data,
        defines = defines,
        deps = deps,
        features = ["layering_check"],
        linkopts = linkopts,
        local_defines = local_defines,
        tags = tags,
        testonly = testonly,
        visibility = visibility,
    )
