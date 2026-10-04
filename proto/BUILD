load("@rules_buf//buf:defs.bzl", "buf_breaking_test", "buf_lint_test")

package(default_visibility = ["//visibility:private"])

_SCHEMAS = [
    "//proto/kwaque/common/v1:build_info_proto",
    "//proto/kwaque/common/v1:capability_proto",
    "//proto/kwaque/common/v1:error_proto",
    "//proto/kwaque/common/v1:identity_proto",
    "//proto/kwaque/control/v1:handshake_proto",
    "//proto/kwaque/control/v1:redirect_proto",
]

buf_lint_test(
    name = "schema_lint_test",
    config = "buf.yaml",
    targets = _SCHEMAS,
)

# schema_baseline.binpb is the last accepted schema image. Regenerate it only
# when a reviewed change deliberately accepts a new wire baseline.
buf_breaking_test(
    name = "schema_breaking_test",
    against = "schema_baseline.binpb",
    config = "buf.yaml",
    targets = _SCHEMAS,
)
