"""Hermetic helpers for constructing the Kwaque binary distribution."""

def _sha256_file_impl(ctx):
    output = ctx.actions.declare_file(ctx.attr.out)
    ctx.actions.run(
        arguments = [ctx.file.src.path, output.path],
        executable = ctx.executable._tool,
        inputs = [ctx.file.src],
        mnemonic = "KwaquePackageSha256",
        outputs = [output],
    )
    return [DefaultInfo(files = depset([output]))]

sha256_file = rule(
    implementation = _sha256_file_impl,
    attrs = {
        "out": attr.string(mandatory = True),
        "src": attr.label(allow_single_file = True, mandatory = True),
        "_tool": attr.label(
            cfg = "exec",
            default = Label("//bazel/packaging:sha256sum"),
            executable = True,
        ),
    },
)
