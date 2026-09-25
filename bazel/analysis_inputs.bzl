"""Materialize C++ analysis inputs without requesting linked executables."""

_AnalysisInputsInfo = provider(
    "Compilation inputs collected across configured dependencies.",
    fields = {"files": "Transitive C++ compilation prerequisites."},
)

def _analysis_inputs_impl(target, ctx):
    transitive = []
    groups = target[OutputGroupInfo] if OutputGroupInfo in target else None
    prerequisites = getattr(groups, "compilation_prerequisites_INTERNAL_", None)
    if prerequisites != None:
        transitive.append(prerequisites)
    elif ctx.rule.kind in ("cc_library", "cc_binary", "cc_test"):
        fail("C++ rule no longer exposes compilation prerequisites: %s" % target.label)

    # Visit wrappers, tool transitions and implementation dependencies too.
    # A top-level rule's headers alone do not cover every compile command.
    for name in dir(ctx.rule.attr):
        value = getattr(ctx.rule.attr, name)
        if type(value) == "dict":
            dependencies = value.keys()
        elif type(value) == "list":
            dependencies = value
        else:
            dependencies = [value]
        for dependency in dependencies:
            if type(dependency) == "Target" and _AnalysisInputsInfo in dependency:
                transitive.append(dependency[_AnalysisInputsInfo].files)

    files = depset(transitive = transitive)
    return [
        _AnalysisInputsInfo(files = files),
        OutputGroupInfo(clang_tidy_inputs = files),
    ]

analysis_inputs = aspect(
    implementation = _analysis_inputs_impl,
    attr_aspects = ["*"],
)
