alias(
    name = "kwaque",
    actual = "//src/broker:kwaque",
    visibility = ["//visibility:public"],
)

filegroup(
    name = "test_suppressions",
    srcs = [
        "lsan_suppressions.txt",
        "ubsan_suppressions.txt",
    ],
    testonly = True,
    visibility = ["//visibility:public"],
)

filegroup(
    name = "lsan_suppressions",
    srcs = ["lsan_suppressions.txt"],
    testonly = True,
    visibility = ["//visibility:public"],
)

filegroup(
    name = "ubsan_suppressions",
    srcs = ["ubsan_suppressions.txt"],
    testonly = True,
    visibility = ["//visibility:public"],
)

filegroup(
    name = "package_files",
    srcs = [
        "LICENSE",
        "NOTICE",
        "README.md",
    ],
    visibility = ["//visibility:public"],
)

exports_files(
    [
        "MODULE.bazel",
        "LICENSE",
        "NOTICE",
    ],
    visibility = ["//visibility:public"],
)
