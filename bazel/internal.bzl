"""Private helpers shared by Kwaque's Bazel rule wrappers."""

def kwaque_copts():
    """Returns the warning policy for first-party C++ targets."""
    return [
        "-Wall",
        "-Wextra",
        "-Wpedantic",
        "-Wconversion",
        "-Wformat=2",
        "-Wimplicit-fallthrough",
        "-Wno-missing-field-initializers",
        "-Wsign-conversion",
        "-Wundef",
    ]
