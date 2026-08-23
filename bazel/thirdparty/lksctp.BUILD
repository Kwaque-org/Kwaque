load("@bazel_skylib//rules:expand_template.bzl", "expand_template")
load("@rules_cc//cc:cc_library.bzl", "cc_library")

cc_library(
    name = "lksctp",
    hdrs = [":gen_header"],
    strip_include_prefix = "src/include",
    visibility = ["//visibility:public"],
)

filegroup(
    name = "license",
    srcs = ["COPYING.lib"],
    visibility = ["//visibility:public"],
)

expand_template(
    name = "gen_header",
    out = "src/include/netinet/sctp.h",
    substitutions = {
        "#undef HAVE_SCTP_ASSOC_RESET_EVENT": "/* #undef HAVE_SCTP_ASSOC_RESET_EVENT */",
        "#undef HAVE_SCTP_AUTH_NO_AUTH": "#define HAVE_SCTP_AUTH_NO_AUTH 1",
        "#undef HAVE_SCTP_PDAPI_EVENT_PDAPI_SEQ": "#define HAVE_SCTP_PDAPI_EVENT_PDAPI_SEQ 1",
        "#undef HAVE_SCTP_PDAPI_EVENT_PDAPI_STREAM": "#define HAVE_SCTP_PDAPI_EVENT_PDAPI_STREAM 1",
        "#undef HAVE_SCTP_PEELOFF_FLAGS": "#define HAVE_SCTP_PEELOFF_FLAGS 1",
        "#undef HAVE_SCTP_SENDV": "#define HAVE_SCTP_SENDV 1",
        "#undef HAVE_SCTP_SPP_DSCP": "#define HAVE_SCTP_SPP_DSCP 1",
        "#undef HAVE_SCTP_SPP_IPV6_FLOWLABEL": "#define HAVE_SCTP_SPP_IPV6_FLOWLABEL 1",
        "#undef HAVE_SCTP_STREAM_CHANGE_EVENT": "/* #undef HAVE_SCTP_STREAM_CHANGE_EVENT */",
        "#undef HAVE_SCTP_STREAM_RECONFIG": "#define HAVE_SCTP_STREAM_RECONFIG 1",
        "#undef HAVE_SCTP_STREAM_RESET_EVENT": "#define HAVE_SCTP_STREAM_RESET_EVENT 1",
    },
    template = "src/include/netinet/sctp.h.in",
)
