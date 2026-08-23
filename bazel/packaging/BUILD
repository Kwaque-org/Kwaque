load("@rules_pkg//pkg:mappings.bzl", "pkg_attributes", "pkg_files", "strip_prefix")
load("@rules_pkg//pkg:tar.bzl", "pkg_tar")
load("@rules_python//python:defs.bzl", "py_binary", "py_test")
load("//bazel:versions.bzl", "KWAQUE_VERSION")
load(":packaging.bzl", "patched_binary", "runtime_libraries", "sha256_file")

package(default_visibility = ["//visibility:private"])

_PACKAGE_NAMES = {
    "@platforms//cpu:aarch64": "kwaque-{}-linux-aarch64".format(KWAQUE_VERSION),
    "@platforms//cpu:x86_64": "kwaque-{}-linux-x86_64".format(KWAQUE_VERSION),
}

patched_binary(
    name = "packaged_kwaque",
    binary = "//src/broker:kwaque",
)

runtime_libraries(
    name = "runtime_libraries",
    openssl = "@openssl//:openssl_foreign_cc",
)

pkg_files(
    name = "binary_files",
    srcs = [":packaged_kwaque"],
    attributes = pkg_attributes(
        gid = 0,
        mode = "0755",
        uid = 0,
    ),
    prefix = "bin",
    strip_prefix = strip_prefix.files_only(),
)

pkg_files(
    name = "runtime_library_files",
    srcs = [":runtime_libraries"],
    attributes = pkg_attributes(
        gid = 0,
        mode = "0644",
        uid = 0,
    ),
    prefix = "lib",
    strip_prefix = strip_prefix.files_only(),
)

pkg_files(
    name = "config_files",
    srcs = ["//conf:kwaque_config"],
    attributes = pkg_attributes(
        gid = 0,
        mode = "0644",
        uid = 0,
    ),
    prefix = "etc/kwaque",
    strip_prefix = strip_prefix.files_only(),
)

pkg_files(
    name = "documentation_files",
    srcs = ["//:package_files"],
    attributes = pkg_attributes(
        gid = 0,
        mode = "0644",
        uid = 0,
    ),
    strip_prefix = strip_prefix.files_only(),
)

pkg_files(
    name = "abseil_license",
    srcs = ["@abseil-cpp//:LICENSE"],
    attributes = pkg_attributes(
        gid = 0,
        mode = "0644",
        uid = 0,
    ),
    prefix = "licenses/abseil",
    strip_prefix = strip_prefix.files_only(),
)

pkg_files(
    name = "boost_license",
    srcs = ["@boost//:LICENSE_1_0.txt"],
    attributes = pkg_attributes(
        gid = 0,
        mode = "0644",
        uid = 0,
    ),
    prefix = "licenses/boost",
    strip_prefix = strip_prefix.files_only(),
)

pkg_files(
    name = "cares_license",
    srcs = ["@c-ares//:license"],
    attributes = pkg_attributes(
        gid = 0,
        mode = "0644",
        uid = 0,
    ),
    prefix = "licenses/c-ares",
    strip_prefix = strip_prefix.files_only(),
)

pkg_files(
    name = "crc32c_license",
    srcs = ["@crc32c//:LICENSE"],
    attributes = pkg_attributes(
        gid = 0,
        mode = "0644",
        uid = 0,
    ),
    prefix = "licenses/crc32c",
    strip_prefix = strip_prefix.files_only(),
)

pkg_files(
    name = "fmt_license",
    srcs = ["@fmt//:LICENSE"],
    attributes = pkg_attributes(
        gid = 0,
        mode = "0644",
        uid = 0,
    ),
    prefix = "licenses/fmt",
    strip_prefix = strip_prefix.files_only(),
)

pkg_files(
    name = "hwloc_license",
    srcs = ["@hwloc//:license"],
    attributes = pkg_attributes(
        gid = 0,
        mode = "0644",
        uid = 0,
    ),
    prefix = "licenses/hwloc",
    strip_prefix = strip_prefix.files_only(),
)

pkg_files(
    name = "liburing_license",
    srcs = ["@liburing//:LICENSE"],
    attributes = pkg_attributes(
        gid = 0,
        mode = "0644",
        uid = 0,
    ),
    prefix = "licenses/liburing",
    strip_prefix = strip_prefix.files_only(),
)

pkg_files(
    name = "lksctp_license",
    srcs = ["@lksctp//:license"],
    attributes = pkg_attributes(
        gid = 0,
        mode = "0644",
        uid = 0,
    ),
    prefix = "licenses/lksctp-tools",
    strip_prefix = strip_prefix.files_only(),
)

pkg_files(
    name = "lz4_license",
    srcs = ["@lz4//:LICENSE"],
    attributes = pkg_attributes(
        gid = 0,
        mode = "0644",
        uid = 0,
    ),
    prefix = "licenses/lz4",
    strip_prefix = strip_prefix.files_only(),
)

pkg_files(
    name = "openssl_license",
    srcs = ["@openssl//:license"],
    attributes = pkg_attributes(
        gid = 0,
        mode = "0644",
        uid = 0,
    ),
    prefix = "licenses/openssl",
    strip_prefix = strip_prefix.files_only(),
)

pkg_files(
    name = "protobuf_license",
    srcs = ["@protobuf//:LICENSE"],
    attributes = pkg_attributes(
        gid = 0,
        mode = "0644",
        uid = 0,
    ),
    prefix = "licenses/protobuf",
    strip_prefix = strip_prefix.files_only(),
)

pkg_files(
    name = "seastar_license",
    srcs = ["@seastar//:license_files"],
    attributes = pkg_attributes(
        gid = 0,
        mode = "0644",
        uid = 0,
    ),
    prefix = "licenses/seastar",
    strip_prefix = strip_prefix.files_only(),
)

pkg_files(
    name = "unordered_dense_license",
    srcs = ["@unordered_dense//:license"],
    attributes = pkg_attributes(
        gid = 0,
        mode = "0644",
        uid = 0,
    ),
    prefix = "licenses/unordered_dense",
    strip_prefix = strip_prefix.files_only(),
)

pkg_files(
    name = "yaml_cpp_license",
    srcs = ["@yaml-cpp//:LICENSE"],
    attributes = pkg_attributes(
        gid = 0,
        mode = "0644",
        uid = 0,
    ),
    prefix = "licenses/yaml-cpp",
    strip_prefix = strip_prefix.files_only(),
)

_PACKAGE_SRCS = [
    ":abseil_license",
    ":binary_files",
    ":boost_license",
    ":cares_license",
    ":config_files",
    ":crc32c_license",
    ":documentation_files",
    ":fmt_license",
    ":hwloc_license",
    ":liburing_license",
    ":lksctp_license",
    ":lz4_license",
    ":openssl_license",
    ":protobuf_license",
    ":runtime_library_files",
    ":seastar_license",
    ":unordered_dense_license",
    ":yaml_cpp_license",
]

pkg_tar(
    name = "kwaque_tar",
    srcs = _PACKAGE_SRCS,
    allow_duplicates_with_different_content = False,
    extension = "tar.gz",
    package_dir = select(_PACKAGE_NAMES),
    package_file_name = select({
        architecture: name + ".tar.gz"
        for architecture, name in _PACKAGE_NAMES.items()
    }),
    portable_mtime = True,
    stamp = 0,
    visibility = ["//visibility:public"],
)

py_binary(
    name = "sha256sum",
    srcs = ["sha256sum.py"],
    main = "sha256sum.py",
)

sha256_file(
    name = "kwaque_tar_sha256",
    src = ":kwaque_tar",
    out = select({
        architecture: name + ".tar.gz.sha256"
        for architecture, name in _PACKAGE_NAMES.items()
    }),
    visibility = ["//visibility:public"],
)

py_test(
    name = "package_contents_test",
    size = "small",
    srcs = ["package_contents_test.py"],
    args = [
        "$(rootpath :kwaque_tar)",
        "$(rootpath :kwaque_tar_sha256)",
    ],
    data = [
        ":kwaque_tar",
        ":kwaque_tar_sha256",
    ],
    main = "package_contents_test.py",
)

pkg_tar(
    name = "reproducibility_archive_a",
    testonly = True,
    srcs = _PACKAGE_SRCS,
    allow_duplicates_with_different_content = False,
    extension = "tar.gz",
    package_dir = select(_PACKAGE_NAMES),
    package_file_name = select({
        architecture: "reproducibility/a/" + name + ".tar.gz"
        for architecture, name in _PACKAGE_NAMES.items()
    }),
    portable_mtime = True,
    stamp = 0,
)

pkg_tar(
    name = "reproducibility_archive_b",
    testonly = True,
    srcs = _PACKAGE_SRCS,
    allow_duplicates_with_different_content = False,
    extension = "tar.gz",
    package_dir = select(_PACKAGE_NAMES),
    package_file_name = select({
        architecture: "reproducibility/b/" + name + ".tar.gz"
        for architecture, name in _PACKAGE_NAMES.items()
    }),
    portable_mtime = True,
    stamp = 0,
)

py_test(
    name = "package_reproducibility_test",
    size = "small",
    srcs = ["package_reproducibility_test.py"],
    args = [
        "$(rootpath :reproducibility_archive_a)",
        "$(rootpath :reproducibility_archive_b)",
    ],
    data = [
        ":reproducibility_archive_a",
        ":reproducibility_archive_b",
    ],
    main = "package_reproducibility_test.py",
)

py_test(
    name = "package_smoke_test",
    size = "small",
    timeout = "short",
    srcs = ["package_smoke_test.py"],
    args = ["$(rootpath :kwaque_tar)"],
    data = [":kwaque_tar"],
    main = "package_smoke_test.py",
    tags = [
        "requires-network",
        "resources:cpu:1",
        "resources:memory:128",
        "smoke",
    ],
    deps = ["//tests/smoke:broker_test_support"],
)
