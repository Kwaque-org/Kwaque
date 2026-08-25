load("@rules_python//python:defs.bzl", "py_binary", "py_library", "py_test")

package(default_visibility = ["//visibility:private"])

py_binary(
    name = "buildifier_check",
    srcs = ["run_buildifier.py"],
    args = [
        "--tool=$(rlocationpath @buildifier_prebuilt//:buildifier)",
        "--mode=diff",
    ],
    data = ["@buildifier_prebuilt//:buildifier"],
    main = "run_buildifier.py",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

py_binary(
    name = "buildifier_fix",
    srcs = ["run_buildifier.py"],
    args = [
        "--tool=$(rlocationpath @buildifier_prebuilt//:buildifier)",
        "--mode=fix",
    ],
    data = ["@buildifier_prebuilt//:buildifier"],
    main = "run_buildifier.py",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

py_binary(
    name = "check_bazel_package_cycles",
    srcs = ["check_bazel_package_cycles.py"],
    main = "check_bazel_package_cycles.py",
    visibility = ["//visibility:public"],
)

py_binary(
    name = "check_generated_artifacts",
    srcs = ["check_generated_artifacts.py"],
    main = "check_generated_artifacts.py",
    visibility = ["//visibility:public"],
)

py_binary(
    name = "check_dependency_inventory",
    srcs = ["check_dependency_inventory.py"],
    main = "check_dependency_inventory.py",
    visibility = ["//visibility:public"],
)

py_binary(
    name = "check_cross_shard_usage",
    srcs = ["check_cross_shard_usage.py"],
    main = "check_cross_shard_usage.py",
    visibility = ["//visibility:public"],
)

py_library(
    name = "cross_shard_usage_lib",
    srcs = ["check_cross_shard_usage.py"],
)

py_library(
    name = "dependency_inventory_lib",
    srcs = ["check_dependency_inventory.py"],
)

py_binary(
    name = "compile_commands",
    srcs = ["compilation_database.py"],
    main = "compilation_database.py",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

py_library(
    name = "compilation_database_lib",
    srcs = ["compilation_database.py"],
)

py_binary(
    name = "format_cpp_all",
    srcs = ["cpp_format.py"],
    args = [
        "--tool=$(rlocationpath @current_llvm_toolchain_llvm//:clang-format)",
        "--scope=all",
    ],
    data = ["@current_llvm_toolchain_llvm//:clang-format"],
    main = "cpp_format.py",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

py_binary(
    name = "format_cpp_changed",
    srcs = ["cpp_format.py"],
    args = [
        "--tool=$(rlocationpath @current_llvm_toolchain_llvm//:clang-format)",
        "--scope=changed",
    ],
    data = ["@current_llvm_toolchain_llvm//:clang-format"],
    main = "cpp_format.py",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

py_binary(
    name = "clang_tidy",
    srcs = ["run_clang_tidy.py"],
    args = [
        "--tool=$(rlocationpath @current_llvm_toolchain_llvm//:clang-tidy)",
        "--config=.clang-tidy",
    ],
    data = [
        "//:.clang-tidy",
        "@current_llvm_toolchain_llvm//:clang-tidy",
    ],
    main = "run_clang_tidy.py",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

py_binary(
    name = "clang_tidy_strict",
    srcs = ["run_clang_tidy.py"],
    args = [
        "--tool=$(rlocationpath @current_llvm_toolchain_llvm//:clang-tidy)",
        "--config=.clang-tidy-strict",
        "--production-only",
    ],
    data = [
        "//:.clang-tidy-strict",
        "@current_llvm_toolchain_llvm//:clang-tidy",
    ],
    main = "run_clang_tidy.py",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

py_test(
    name = "check_bazel_package_cycles_test",
    size = "small",
    srcs = [
        "check_bazel_package_cycles.py",
        "check_bazel_package_cycles_test.py",
    ],
    main = "check_bazel_package_cycles_test.py",
)

py_test(
    name = "check_dependency_inventory_test",
    size = "small",
    srcs = ["check_dependency_inventory_test.py"],
    main = "check_dependency_inventory_test.py",
    deps = [":dependency_inventory_lib"],
)

py_test(
    name = "check_cross_shard_usage_test",
    size = "small",
    srcs = ["check_cross_shard_usage_test.py"],
    main = "check_cross_shard_usage_test.py",
    deps = [":cross_shard_usage_lib"],
)

py_test(
    name = "compilation_database_test",
    size = "small",
    srcs = ["compilation_database_test.py"],
    main = "compilation_database_test.py",
    deps = [":compilation_database_lib"],
)
