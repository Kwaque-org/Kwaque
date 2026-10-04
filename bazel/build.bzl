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
        linkstatic = False,
        tags = []):
    """Defines a first-party C++ library with Kwaque's common policy.

    Args:
      name: Name of the library.
      srcs: C++ sources.
      hdrs: Public headers.
      deps: Dependencies exposed through the headers.
      implementation_deps: Dependencies used only by the sources.
      defines: Defines for the library and its dependents.
      local_defines: Defines for the library's own sources.
      copts: Additional compiler options.
      visibility: Bazel visibility.
      testonly: Whether only test targets may depend on the library.
      alwayslink: Whether every object is linked even when unreferenced.
      linkstatic: Whether the library is always linked statically, even into
        dynamically linked tests. Required for a library whose statics must
        be destroyed in order with Seastar's, which also links statically.
      tags: Bazel tags.
    """
    cc_library(
        name = name,
        srcs = srcs,
        hdrs = hdrs,
        alwayslink = alwayslink,
        copts = kwaque_copts() + copts,
        defines = defines,
        deps = deps,
        features = [
            "layering_check",
            "parse_headers",
        ],
        implementation_deps = implementation_deps,
        linkstatic = linkstatic,
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
