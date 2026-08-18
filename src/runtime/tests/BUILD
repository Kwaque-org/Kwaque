load("@rules_cc//cc:cc_test.bzl", "cc_test")

cc_test(
    name = "reactor_smoke_test",
    size = "small",
    srcs = ["reactor_smoke_test.cc"],
    args = [
        "--",
        "--memory=128M",
        "--overprovisioned",
        "--smp=1",
    ],
    local_defines = ["SEASTAR_TESTING_MAIN"],
    deps = ["@seastar//:testing"],
)

cc_test(
    name = "runtime_primitives_test",
    size = "small",
    srcs = ["runtime_primitives_test.cc"],
    args = [
        "--",
        "--memory=128M",
        "--overprovisioned",
        "--smp=2",
    ],
    local_defines = ["SEASTAR_TESTING_MAIN"],
    tags = ["cpu:2"],
    deps = ["@seastar//:testing"],
)
