package(default_visibility = ["//visibility:private"])

exports_files(
    [
        "kwaque.production.yaml",
        "kwaque.yaml",
    ],
    visibility = ["//bazel/packaging:__pkg__"],
)

exports_files(
    ["kwaque.service"],
    visibility = [
        "//bazel/packaging:__pkg__",
        "//src/broker:__pkg__",
    ],
)

filegroup(
    name = "kwaque_config",
    srcs = ["kwaque.yaml"],
    visibility = ["//visibility:public"],
)

filegroup(
    name = "kwaque_production_config",
    srcs = ["kwaque.production.yaml"],
    visibility = ["//visibility:public"],
)
