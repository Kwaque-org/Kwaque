import io
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from tools.check_package_edges import (
    check,
    default_visibilities,
    graph_from_query_xml,
    main,
)

PRIVATE = {
    "//src/broker": ("//visibility:private",),
    "//src/metadata": ("//visibility:private",),
    "//src/range": ("//visibility:private",),
    "//src/storage": ("//visibility:private",),
}


def rule(
    label: str,
    *inputs: str,
    kind: str = "cc_library",
    testonly: bool = False,
    visibility: tuple[str, ...] | None = None,
    sources: tuple[str, ...] = (),
) -> str:
    parts = [f'  <rule class="{kind}" name="{label}">']
    parts.append(
        f'    <boolean name="testonly" value="{"true" if testonly else "false"}"/>'
    )
    if visibility is not None:
        parts.append('    <list name="visibility">')
        parts.extend(f'      <label value="{entry}"/>' for entry in visibility)
        parts.append("    </list>")
    if sources:
        parts.append('    <list name="srcs">')
        parts.extend(f'      <label value="{source}"/>' for source in sources)
        parts.append("    </list>")
    parts.extend(f'    <rule-input name="{entry}"/>' for entry in inputs)
    parts.append("  </rule>")
    return "\n".join(parts)


# A small tree shaped like the real one, with every allowed edge.
CLEAN = (
    rule("//src/base:base"),
    rule("//src/runtime:time", "//src/base:base"),
    rule("//src/runtime:random", "//src/base:base"),
    rule("//src/runtime:buggify", "//src/base:base"),
    rule("//src/model:outcome"),
    rule("//src/model:identity", "//src/base:base", "//src/runtime:time"),
    rule("//src/codec:xxh3", "//src/base:base"),
    rule(
        "//src/storage:local_paths",
        "//src/model:identity",
        visibility=("//src/raft:__pkg__", "//src/range:__pkg__"),
    ),
    rule("//src/storage:local_engine", "//src/storage:local_paths"),
    rule("//src/config:bootstrap_config", "//src/base:base"),
    rule(
        "//src/raft:journal",
        "//src/storage:local_paths",
        "//src/codec:xxh3",
        "//src/runtime:random",
    ),
    rule(
        "//src/metadata:objects",
        "//src/model:identity",
        "//src/model:outcome",
        sources=("//src/metadata:objects.h", "//src/metadata:objects_topic.cc"),
    ),
    rule("//src/metadata:apply", "//src/metadata:objects"),
    rule(
        "//src/metadata:proposer",
        "//src/metadata:apply",
        "//src/raft:journal",
        "//src/runtime:random",
        "//src/config:bootstrap_config",
    ),
    rule(
        "//src/range:engine",
        "//src/metadata:proposer",
        "//src/raft:journal",
        "//src/storage:local_engine",
    ),
    rule("//src/broker:application_internal", "//src/range:engine"),
    rule("//src/simulation:environment", "//src/runtime:random", testonly=True),
    rule(
        "//src/storage/tests:engine_harness",
        "//src/range:engine",
        "//src/simulation:environment",
        testonly=True,
    ),
    rule("//specs/range:model", kind="filegroup"),
)


def graph(*extra: str, without: tuple[str, ...] = ()):
    rules = [
        entry
        for entry in CLEAN
        if not any(
            entry.splitlines()[0].endswith(f'name="{label}">') for label in without
        )
    ]
    return graph_from_query_xml(
        '<query version="2">\n' + "\n".join([*rules, *extra]) + "\n</query>\n"
    )


class PackageEdgesTest(unittest.TestCase):
    def assert_fails(self, expected: str, *extra: str, without=()) -> None:
        violations = check(graph(*extra, without=without), PRIVATE)
        self.assertTrue(violations, f"no violation for {expected}")
        self.assertTrue(
            all(violation.startswith(expected) for violation in violations),
            violations,
        )

    def test_accepts_the_allowed_edges(self) -> None:
        self.assertEqual(check(graph(), PRIVATE), [])
        self.assertTrue(graph()["//src/storage/tests:engine_harness"].testonly)

    def test_storage_may_not_reach_raft_metadata_or_range(self) -> None:
        for target in (
            "//src/raft:journal",
            "//src/metadata:objects",
            "//src/range:routing",
        ):
            with self.subTest(target=target):
                self.assert_fails(
                    "storage depends on raft, metadata or range",
                    rule("//src/storage:local_engine", target),
                    rule("//src/range:routing", "//src/model:identity"),
                    without=("//src/storage:local_engine",),
                )

    def test_raft_may_not_reach_metadata_or_range(self) -> None:
        self.assert_fails(
            "raft depends on metadata or range",
            rule("//src/raft:journal", "//src/metadata:objects"),
            without=("//src/raft:journal",),
        )

    def test_pure_metadata_may_not_reach_a_service(self) -> None:
        for target in (
            "//src/runtime:random",
            "//src/runtime:buggify",
            "//src/runtime:stop_signal",
            "//src/config:bootstrap_config",
            "//src/storage:local_paths",
            "//src/raft:journal",
            "//src/observability:event_sink",
            "//src/resource:resource_manager",
        ):
            with self.subTest(target=target):
                violations = check(
                    graph(
                        rule(target, "//src/base:base"),
                        rule("//src/metadata:apply", "//src/metadata:objects", target),
                        without=("//src/metadata:apply", target),
                    ),
                    PRIVATE,
                )
                self.assertIn(
                    "pure metadata depends on a runtime service or an owner "
                    "package: //src/metadata:apply -> " + target,
                    violations,
                )

    def test_range_may_not_reach_the_broker(self) -> None:
        self.assert_fails(
            "range depends on the broker",
            rule("//src/range:engine", "//src/broker:application_internal"),
            without=("//src/range:engine",),
        )

    def test_only_the_composition_root_starts_the_range_engine(self) -> None:
        self.assert_fails(
            "the range engine is started outside the composition root",
            rule("//src/broker:admin_routes", "//src/range:engine"),
        )
        self.assert_fails(
            "the range engine is started outside the composition root",
            rule("//src/range:engine_alias", "//src/range:engine", kind="alias"),
            rule("//src/broker:admin_routes", "//src/range:engine_alias"),
        )

    def test_production_code_may_not_reach_simulation(self) -> None:
        self.assert_fails(
            "production code depends on simulation",
            rule("//src/storage:local_engine", "//src/simulation:environment"),
            without=("//src/storage:local_engine",),
        )

    def test_value_packages_may_not_reach_services_or_owners(self) -> None:
        for source, target in (
            ("//src/model:identity", "//src/runtime:random"),
            ("//src/codec:xxh3", "//src/storage:local_paths"),
            ("//src/base:base", "//src/runtime:buggify"),
            ("//src/model:identity", "//src/runtime:stop_signal"),
        ):
            with self.subTest(source=source, target=target):
                violations = check(
                    graph(
                        rule(target, "//src/base:base"),
                        rule(source, target),
                        without=(source, target),
                    ),
                    PRIVATE,
                )
                self.assertIn(
                    "a value package depends on a runtime service, storage, "
                    f"metadata or range: {source} -> {target}",
                    violations,
                )

    def test_specification_packages_hold_files_only(self) -> None:
        self.assert_fails(
            "a specification package holds a cc_library",
            rule("//specs/range:checker"),
        )

    def test_pure_targets_hold_only_pure_sources(self) -> None:
        self.assert_fails(
            "a pure metadata target holds a source the determinism checker",
            rule(
                "//src/metadata:commands",
                "//src/model:identity",
                sources=("//src/metadata:command_log.cc",),
            ),
        )

    def test_no_storage_library_is_visible_to_metadata(self) -> None:
        for visibility in (
            ("//src/metadata:__pkg__",),
            ("//src/metadata/tests:__pkg__",),
            ("//src:__subpackages__",),
            ("//visibility:public",),
        ):
            with self.subTest(visibility=visibility):
                self.assert_fails(
                    "a storage library is visible to metadata",
                    rule(
                        "//src/storage:local_paths",
                        "//src/model:identity",
                        visibility=visibility,
                    ),
                    without=("//src/storage:local_paths",),
                )

    def test_a_package_default_grants_its_visibility(self) -> None:
        violations = check(
            graph(),
            {**PRIVATE, "//src/storage": ("//src/metadata:__pkg__",)},
        )
        self.assertEqual(
            violations,
            ["a storage library is visible to metadata: //src/storage:local_engine"],
        )

    def test_reads_package_defaults_from_build_files(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "src/storage").mkdir(parents=True)
            (root / "src/storage/BUILD").write_text(
                'package(default_visibility = ["//visibility:private"])\n',
                encoding="utf-8",
            )
            (root / "src/range").mkdir(parents=True)
            (root / "src/range/BUILD").write_text("", encoding="utf-8")
            # Bazel reads BUILD.bazel over BUILD, and the argument need not
            # come first; a commented call is not one.
            (root / "src/metadata").mkdir(parents=True)
            (root / "src/metadata/BUILD").write_text(
                'package(default_visibility = ["//visibility:public"])\n',
                encoding="utf-8",
            )
            (root / "src/metadata/BUILD.bazel").write_text(
                '# package(default_visibility = ["//visibility:public"])\n'
                "package(\n"
                '    default_applicable_licenses = ["//:license"],\n'
                '    default_visibility = ["//src/range:__pkg__"],\n'
                ")\n",
                encoding="utf-8",
            )
            defaults = default_visibilities(root, graph())
        self.assertEqual(defaults["//src/storage"], ("//visibility:private",))
        self.assertEqual(defaults["//src/range"], ())
        self.assertEqual(defaults["//src/metadata"], ("//src/range:__pkg__",))

    def test_a_visibility_it_cannot_read_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            check(
                graph(
                    rule(
                        "//src/storage:local_paths",
                        "//src/model:identity",
                        visibility=("//src/range:readers",),
                    ),
                    without=("//src/storage:local_paths",),
                ),
                PRIVATE,
            )

    def test_main_reports_each_violation_and_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            query = Path(directory) / "query.xml"
            query.write_text(
                '<query version="2">\n'
                + "\n".join(
                    [*CLEAN, rule("//src/codec:crc32c", "//src/runtime:random")]
                )
                + "\n</query>\n",
                encoding="utf-8",
            )
            errors = io.StringIO()
            with (
                patch(
                    "sys.argv",
                    ["check", "--query-xml", str(query), "--workspace", directory],
                ),
                redirect_stderr(errors),
                redirect_stdout(io.StringIO()),
            ):
                self.assertEqual(main(), 1)
        self.assertIn("//src/codec:crc32c -> //src/runtime:random", errors.getvalue())

    def test_main_refuses_unreadable_query_output(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            query = Path(directory) / "query.xml"
            query.write_text("<query", encoding="utf-8")
            with (
                patch("sys.argv", ["check", "--query-xml", str(query)]),
                redirect_stderr(io.StringIO()),
            ):
                self.assertEqual(main(), 2)


if __name__ == "__main__":
    unittest.main()
