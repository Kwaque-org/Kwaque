load("@rules_pkg//pkg:mappings.bzl", "pkg_attributes", "pkg_files", "strip_prefix")
load("@rules_pkg//pkg:tar.bzl", "pkg_tar")
load("@rules_python//python:defs.bzl", "py_binary")
load("//bazel:test.bzl", "kwaque_py_native_test")
load("//bazel:versions.bzl", "KWAQUE_VERSION")
load(":packaging.bzl", "sha256_file")

package(default_visibility = ["//visibility:private"])

_PACKAGE_NAMES = {
    "@platforms//cpu:aarch64": "kwaque-{}-linux-aarch64".format(KWAQUE_VERSION),
    "@platforms//cpu:x86_64": "kwaque-{}-linux-x86_64".format(KWAQUE_VERSION),
}

pkg_files(
    name = "binary_files",
    srcs = [
        "//src/broker:kwaque",
        "//src/broker:kwaque_native",
        "@seastar//:iotune",
    ],
    attributes = pkg_attributes(
        gid = 0,
        mode = "0755",
        uid = 0,
    ),
    prefix = "bin",
    strip_prefix = strip_prefix.files_only(),
)

# The installed configuration is the production example; the development
# example ships beside it under a name that cannot be mistaken for it.
pkg_files(
    name = "config_files",
    srcs = [
        "//conf:kwaque.production.yaml",
        "//conf:kwaque.yaml",
    ],
    attributes = pkg_attributes(
        gid = 0,
        mode = "0644",
        uid = 0,
    ),
    prefix = "etc/kwaque",
    renames = {
        "//conf:kwaque.production.yaml": "kwaque.yaml",
        "//conf:kwaque.yaml": "kwaque.development.yaml",
    },
)

# A reference unit, not installed: the operator copies it into place.
pkg_files(
    name = "systemd_files",
    srcs = ["//conf:kwaque.service"],
    attributes = pkg_attributes(
        gid = 0,
        mode = "0644",
        uid = 0,
    ),
    prefix = "share/kwaque/systemd",
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
    name = "utf8_range_license",
    srcs = ["@protobuf//third_party/utf8_range:LICENSE"],
    attributes = pkg_attributes(
        gid = 0,
        mode = "0644",
        uid = 0,
    ),
    prefix = "licenses/utf8_range",
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

pkg_files(
    name = "zlib_license",
    srcs = ["@zlib//:LICENSE"],
    attributes = pkg_attributes(
        gid = 0,
        mode = "0644",
        uid = 0,
    ),
    prefix = "licenses/zlib",
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
    ":seastar_license",
    ":systemd_files",
    ":unordered_dense_license",
    ":utf8_range_license",
    ":yaml_cpp_license",
    ":zlib_license",
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

# The archive holds the native broker, so this test uses the native macro.
kwaque_py_native_test(
    name = "package_contents_test",
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
    tags = ["package"],
)

kwaque_py_native_test(
    name = "package_smoke_test",
    timeout = "short",
    srcs = ["package_smoke_test.py"],
    args = ["$(rootpath :kwaque_tar)"],
    data = [":kwaque_tar"],
    main = "package_smoke_test.py",
    tags = [
        "package",
        "requires-network",
        "resources:cpu:2",
        "resources:memory:384",
        "smoke",
    ],
    deps = ["//tests/smoke:broker_test_support"],
)
