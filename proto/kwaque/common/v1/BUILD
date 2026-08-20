load("@protobuf//bazel:cc_proto_library.bzl", "cc_proto_library")
load("@protobuf//bazel:proto_library.bzl", "proto_library")
load("//bazel:build.bzl", "kwaque_cc_library")
load("//bazel:test.bzl", "kwaque_cc_fuzz_test", "kwaque_cc_seastar_test", "kwaque_cc_test")

package(default_visibility = ["//visibility:private"])

proto_library(
    name = "build_info_proto",
    srcs = ["build_info.proto"],
    visibility = ["//visibility:public"],
)

cc_proto_library(
    name = "build_info_cc_proto",
    visibility = ["//visibility:public"],
    deps = [":build_info_proto"],
)

kwaque_cc_library(
    name = "build_info_codec",
    srcs = ["build_info_codec.cc"],
    hdrs = ["build_info_codec.h"],
    visibility = ["//visibility:public"],
    deps = [":build_info_cc_proto"],
)

genrule(
    name = "build_info_fuzz_seed_truncated",
    outs = ["testdata/build_info_fuzz_corpus/truncated.pb"],
    cmd = "printf '\\x0a\\x05" + "0.1.0" + "\\x12\\x10" + "0123456789abcdef" + "\\x1a\\x07" + "releas' > $@",
)

genrule(
    name = "build_info_fuzz_seed_unknown_field",
    outs = ["testdata/build_info_fuzz_corpus/unknown_field.pb"],
    cmd = "printf '\\x20\\x01' > $@",
)

genrule(
    name = "build_info_fuzz_seed_valid",
    outs = ["testdata/build_info_fuzz_corpus/valid.pb"],
    cmd = "printf '\\x0a\\x05" + "0.1.0" + "\\x12\\x10" + "0123456789abcdef" + "\\x1a\\x07" + "release' > $@",
)

kwaque_cc_library(
    name = "build_info_seastar_compat",
    testonly = True,
    srcs = ["build_info_seastar_compat.cc"],
    hdrs = ["build_info_seastar_compat.h"],
    deps = [
        ":build_info_cc_proto",
        "@protobuf",
        "@seastar",
    ],
)

kwaque_cc_test(
    name = "build_info_test",
    srcs = ["build_info_test.cc"],
    data = ["testdata/build_info_v1.hex"],
    deps = [":build_info_codec"],
)

kwaque_cc_fuzz_test(
    name = "build_info_fuzz",
    srcs = ["build_info_fuzz.cc"],
    args = [
        "-dict=$(rootpath :build_info_fuzz.dict)",
        "-max_len=4096",
        "-rss_limit_mb=512",
    ],
    corpus = [
        ":build_info_fuzz_seed_truncated",
        ":build_info_fuzz_seed_unknown_field",
        ":build_info_fuzz_seed_valid",
    ],
    data = ["build_info_fuzz.dict"],
    tags = [
        "resources:cpu:1",
        "resources:memory:512",
    ],
    deps = [":build_info_codec"],
)

kwaque_cc_seastar_test(
    name = "build_info_seastar_test",
    srcs = ["build_info_seastar_test.cc"],
    cpu = 1,
    memory = "128MiB",
    deps = [":build_info_seastar_compat"],
)
