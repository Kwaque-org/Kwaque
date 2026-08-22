alias(
    name = "kwaque",
    actual = "//src/broker:kwaque",
    visibility = ["//visibility:public"],
)

filegroup(
    name = "test_suppressions",
    testonly = True,
    srcs = [
        "lsan_suppressions.txt",
        "ubsan_suppressions.txt",
    ],
    visibility = ["//visibility:public"],
)

filegroup(
    name = "lsan_suppressions",
    testonly = True,
    srcs = ["lsan_suppressions.txt"],
    visibility = ["//visibility:public"],
)

filegroup(
    name = "ubsan_suppressions",
    testonly = True,
    srcs = ["ubsan_suppressions.txt"],
    visibility = ["//visibility:public"],
)

filegroup(
    name = "package_files",
    srcs = [
        "LICENSE",
        "NOTICE",
        "README.md",
        "THIRD_PARTY.md",
    ],
    visibility = ["//visibility:public"],
)

alias(
    name = "kwaque_tar",
    actual = "//bazel/packaging:kwaque_tar",
    visibility = ["//visibility:public"],
)

alias(
    name = "kwaque_tar_sha256",
    actual = "//bazel/packaging:kwaque_tar_sha256",
    visibility = ["//visibility:public"],
)

exports_files(
    [
        ".clang-format",
        ".clang-tidy",
        ".clang-tidy-strict",
        ".clangd",
        "MODULE.bazel",
        "LICENSE",
        "NOTICE",
    ],
    visibility = ["//visibility:public"],
)
