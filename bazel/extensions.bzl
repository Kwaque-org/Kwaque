"""Module extensions for native Kwaque dependencies."""

load("//bazel:repositories.bzl", "declare_native_dependencies")

def _native_dependencies_impl(_ctx):
    declare_native_dependencies()

native_dependencies = module_extension(
    implementation = _native_dependencies_impl,
)
