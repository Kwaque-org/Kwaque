load("//bazel:test.bzl", "kwaque_cc_seastar_test")

package(default_visibility = ["//visibility:private"])

kwaque_cc_seastar_test(
    name = "reactor_smoke_test",
    srcs = ["reactor_smoke_test.cc"],
    cpu = 1,
    memory = "128MiB",
)

kwaque_cc_seastar_test(
    name = "runtime_primitives_test",
    srcs = ["runtime_primitives_test.cc"],
    cpu = 2,
    memory = "128MiB",
)
