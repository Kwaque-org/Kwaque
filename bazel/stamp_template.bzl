"""Rule for expanding source templates with stable workspace-status values."""

def _stamp_template_impl(ctx):
    # Unstamped builds use only the defaults, so a new commit does not
    # invalidate the expanded output.
    variables = [ctx.file.defaults] + ([ctx.info_file] if ctx.attr.stamp else [])
    arguments = ctx.actions.args()
    arguments.add("--template", ctx.file.template)
    arguments.add("--output", ctx.outputs.out)
    arguments.add("--defaults", ctx.file.defaults)
    if ctx.attr.stamp:
        arguments.add("--stamped", ctx.info_file)

    ctx.actions.run(
        executable = ctx.executable._tool,
        arguments = [arguments],
        inputs = [ctx.file.template] + variables,
        outputs = [ctx.outputs.out],
        tools = [ctx.executable._tool],
        mnemonic = "KwaqueStampTemplate",
        progress_message = "Expanding stamped metadata for %{label}",
    )
    return [DefaultInfo(files = depset([ctx.outputs.out]))]

stamp_template = rule(
    implementation = _stamp_template_impl,
    attrs = {
        "defaults": attr.label(allow_single_file = True, mandatory = True),
        "out": attr.output(mandatory = True),
        "stamp": attr.bool(
            default = False,
            doc = "Override the defaults with stable workspace-status values.",
        ),
        "template": attr.label(allow_single_file = True, mandatory = True),
        "_tool": attr.label(
            default = Label("//bazel:stamp_template"),
            cfg = "exec",
            executable = True,
        ),
    },
)
