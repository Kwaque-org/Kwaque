"""Hermetic helpers for constructing the Kwaque binary distribution."""

def _patched_binary_impl(ctx):
    binary = ctx.file.binary
    output = ctx.actions.declare_file("kwaque")
    ctx.actions.run(
        arguments = [
            "--set-rpath",
            "$ORIGIN/../lib",
            binary.path,
            "--output",
            output.path,
        ],
        executable = ctx.executable._patchelf,
        inputs = [binary],
        mnemonic = "PatchKwaqueRpath",
        outputs = [output],
    )
    return [DefaultInfo(files = depset([output]))]

patched_binary = rule(
    implementation = _patched_binary_impl,
    attrs = {
        "binary": attr.label(allow_single_file = True, mandatory = True),
        "_patchelf": attr.label(
            allow_single_file = True,
            cfg = "exec",
            default = Label("@patchelf"),
            executable = True,
        ),
    },
)

def _runtime_libraries_impl(ctx):
    expected = {
        "libcrypto.so.3": True,
        "libssl.so.3": True,
    }
    libraries = [
        file
        for file in ctx.attr.openssl[DefaultInfo].files.to_list()
        if file.basename in expected
    ]
    found = {file.basename: True for file in libraries}
    missing = sorted([name for name in expected if name not in found])
    if missing:
        fail("OpenSSL package outputs are missing {}".format(", ".join(missing)))
    return [DefaultInfo(files = depset(libraries))]

runtime_libraries = rule(
    implementation = _runtime_libraries_impl,
    attrs = {
        "openssl": attr.label(mandatory = True),
    },
)

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
