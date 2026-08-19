"""Rule for expanding source templates with stable workspace-status values."""

def _stamp_template_impl(ctx):
    arguments = ctx.actions.args()
    arguments.add("--template", ctx.file.template)
    arguments.add("--output", ctx.outputs.out)
    arguments.add("--variables")
    arguments.add(ctx.file.defaults)
    arguments.add(ctx.info_file)

    ctx.actions.run(
        executable = ctx.executable._tool,
        arguments = [arguments],
        inputs = [
            ctx.file.defaults,
            ctx.file.template,
            ctx.info_file,
        ],
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
        "template": attr.label(allow_single_file = True, mandatory = True),
        "_tool": attr.label(
            default = Label("//bazel:stamp_template"),
            cfg = "exec",
            executable = True,
        ),
    },
)
