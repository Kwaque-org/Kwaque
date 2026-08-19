"""Analysis test for Kwaque's default-private package policy."""

load("@bazel_skylib//lib:unittest.bzl", "analysistest", "asserts")

def _invalid_consumer_impl(ctx):
    return [DefaultInfo(files = ctx.attr.dep[DefaultInfo].files)]

_invalid_consumer = rule(
    implementation = _invalid_consumer_impl,
    attrs = {
        "dep": attr.label(mandatory = True),
    },
)

def _visibility_policy_test_impl(ctx):
    env = analysistest.begin(ctx)
    asserts.expect_failure(env, "not visible")
    return analysistest.end(env)

_visibility_policy_test = analysistest.make(
    _visibility_policy_test_impl,
    expect_failure = True,
)

def visibility_policy_test(name):
    invalid_target = name + "_invalid_consumer"
    _invalid_consumer(
        name = invalid_target,
        dep = "//src/base:visibility_test_private_library",
        tags = ["manual"],
    )
    _visibility_policy_test(
        name = name,
        size = "small",
        target_under_test = ":" + invalid_target,
    )
