"""Analysis guard for the identity value's compiled dependency closure."""

load("@bazel_skylib//lib:unittest.bzl", "analysistest", "asserts")
load("@rules_cc//cc/common:cc_info.bzl", "CcInfo")

def _identity_dependency_test_impl(ctx):
    env = analysistest.begin(ctx)
    target = analysistest.target_under_test(env)
    asserts.equals(env, Label("//src/model:identity"), target.label)
    asserts.true(env, CcInfo in target, "Identity must provide CcInfo")
    if CcInfo not in target:
        return analysistest.end(env)

    for linker_input in target[CcInfo].linking_context.linker_inputs.to_list():
        if linker_input.owner != Label("//src/base:error"):
            asserts.false(
                env,
                bool(linker_input.libraries),
                "Identity has compiled libraries or objects from {}".format(linker_input.owner),
            )
        asserts.false(
            env,
            bool(linker_input.user_link_flags),
            "Identity has linker flags from {}".format(linker_input.owner),
        )
        asserts.false(
            env,
            bool(linker_input.linkstamps),
            "Identity has linkstamps from {}".format(linker_input.owner),
        )
        asserts.false(
            env,
            bool(linker_input.additional_inputs),
            "Identity has additional linker inputs from {}".format(linker_input.owner),
        )

    return analysistest.end(env)

identity_dependency_test = analysistest.make(_identity_dependency_test_impl)
