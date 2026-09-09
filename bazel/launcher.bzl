"""Build the broker's C-only CPU prerequisite boundary."""

load("@rules_cc//cc:cc_binary.bzl", "cc_binary")
load(":internal.bzl", "kwaque_copts")

def kwaque_launcher(name, srcs, data = [], visibility = None, testonly = False):
    """Build an uninstrumented launcher for the platform's base instruction set."""
    cc_binary(
        name = name,
        srcs = srcs,
        copts = kwaque_copts() + select({
            "@platforms//cpu:aarch64": ["-march=armv8-a"],
            "@platforms//cpu:x86_64": ["-march=x86-64"],
        }) + [
            "-std=c11",
            "-fno-sanitize=all",
            "-fno-lto",
            "-fno-profile-instr-generate",
            "-fno-coverage-mapping",
        ],
        data = data,
        features = [
            "-coverage",
            "-default_link_libs",
            "-thin_lto",
        ],
        linkopts = [
            "-fno-sanitize=all",
            # Global --linkopt sanitizer selections follow rule linkopts.
            # Runtime linking has a separate control that they do not reset.
            "-fno-sanitize-link-runtime",
            "-fno-lto",
            "-fno-profile-instr-generate",
            "-nostdlib++",
            "-Wl,--as-needed",
        ],
        testonly = testonly,
        visibility = visibility,
    )
