from __future__ import annotations

import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from tools.compilation_database import (
    EXTERNAL_LINK_TARGET,
    PRODUCTION_DATABASE_DIRECTORY,
    active_output_base,
    compiler_arguments,
    ensure_external_link,
    generate,
    query_expression,
    select_commands,
)


def action(*arguments: str) -> dict[str, list[str]]:
    return {"arguments": list(arguments)}


class CompilerArgumentsTest(unittest.TestCase):
    execution_root = Path("/output_base/execroot/_main")

    def parse(self, *arguments: str) -> tuple[str, list[str]] | None:
        return compiler_arguments(action(*arguments), self.execution_root)

    def test_shared_source_keeps_all_variants_and_strict_subset_in_any_action_order(
        self,
    ) -> None:
        actions = [
            {
                "targetId": 1,
                "arguments": ["clang", "-DTEST=1", "-c", "src/model/record.cc"],
            },
            {
                "targetId": 2,
                "arguments": ["clang", "-DTEST=0", "-c", "src/model/record.cc"],
            },
        ]
        targets = [
            {"id": 1, "label": "//src/model:test_support"},
            {"id": 2, "label": "//src/model:record"},
        ]
        for ordered in (actions, list(reversed(actions))):
            entries = select_commands(
                ordered, targets, {targets[1]["label"]}, self.execution_root, Path(".")
            )
            self.assertEqual(len(entries), 2)
            strict = select_commands(
                ordered,
                targets,
                {targets[1]["label"]},
                self.execution_root,
                Path("."),
                production_only=True,
            )
            self.assertEqual(len(strict), 1)
            self.assertIn("-DTEST=0", strict[0]["arguments"])
        with self.assertRaisesRegex(RuntimeError, "target identity"):
            select_commands(actions, [], set(), self.execution_root, Path("."))

    def test_fuzz_analysis_uses_fuzz_roots_and_their_dependencies(self) -> None:
        self.assertEqual(
            query_expression(True),
            'mnemonic("CppCompile", deps(attr(tags, "fuzz", //...)))',
        )
        self.assertEqual(
            query_expression(False),
            'mnemonic("CppCompile", deps(//... except attr(tags, "manual|fuzz", //...)))',
        )

    def test_external_paths_stay_workspace_relative(self) -> None:
        """They must resolve through the workspace link, not a fixed output base."""
        parsed = self.parse(
            "clang",
            "-iquote",
            "external/seastar/include",
            "--sysroot=external/x86_64_sysroot/sysroot",
            "-c",
            "src/base/build_info.cc",
        )
        self.assertIsNotNone(parsed)
        assert parsed is not None
        _, arguments = parsed
        self.assertIn("external/seastar/include", arguments)
        self.assertIn("--sysroot=external/x86_64_sysroot/sysroot", arguments)
        self.assertFalse(
            any("/output_base/" in argument for argument in arguments[1:]),
            "no include argument may hard-code an output base",
        )

    def test_generated_output_paths_stay_workspace_relative(self) -> None:
        parsed = self.parse(
            "clang", "-iquote", "bazel-out/k8-fastbuild/bin", "-c", "src/base/error.cc"
        )
        assert parsed is not None
        self.assertIn("bazel-out/k8-fastbuild/bin", parsed[1])

    def test_path_remapping_flags_are_dropped(self) -> None:
        parsed = self.parse(
            "/toolchain/bin/clang",
            "-ffile-compilation-dir=.",
            "-fdebug-prefix-map=/somewhere=.",
            "-fmacro-prefix-map=/somewhere=.",
            "-ffile-prefix-map=/somewhere=.",
            "-O1",
            "-c",
            "src/base/error.cc",
        )
        assert parsed is not None
        _, arguments = parsed
        self.assertEqual(
            arguments,
            ["/toolchain/bin/clang", "-O1", "-c", "src/base/error.cc"],
        )

    def test_relative_compiler_is_anchored_to_the_execution_root(self) -> None:
        parsed = self.parse("bin/clang", "-c", "src/base/error.cc")
        assert parsed is not None
        self.assertEqual(parsed[1][0], "/output_base/execroot/_main/bin/clang")

    def test_non_cpp_and_external_sources_are_skipped(self) -> None:
        self.assertIsNone(self.parse("clang", "-c", "src/base/notes.txt"))
        self.assertIsNone(self.parse("clang", "-c", "external/seastar/src/core.cc"))
        self.assertIsNone(self.parse())


class DatabaseGenerationTest(unittest.TestCase):
    production = {
        "//src/base:error": {"src/base/error.cc"},
        "//src/broker:application": {"src/broker/application.cc"},
    }
    ordinary_sources = (
        ("//src/base:error", "src/base/error.cc"),
        ("//src/broker:application", "src/broker/application.cc"),
        ("//src/base:error_test", "src/base/error_test.cc"),
    )
    fuzz_source = (
        "//src/simulation/tests:scheduler_fuzz_runner",
        "src/simulation/tests/scheduler_fuzz.cc",
    )

    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        base = Path(directory.name) / "base"
        output = base / "execroot" / "_main" / "bazel-out"
        output.mkdir(parents=True)
        (base / "external").mkdir()
        self.root = Path(directory.name) / "workspace"
        self.root.mkdir()
        (self.root / "bazel-out").symlink_to(output)
        (self.root / "external").symlink_to(EXTERNAL_LINK_TARGET)
        self.strict_database = (
            self.root / PRODUCTION_DATABASE_DIRECTORY / "compile_commands.json"
        )

    def run_generator(
        self, sources: tuple[tuple[str, str], ...], *, fuzz_only: bool = False
    ) -> list[dict]:
        mode = "-DFUZZ=1" if fuzz_only else "-DORDINARY=1"
        response = {
            "targets": [
                {"id": index, "label": label}
                for index, (label, _) in enumerate(sources)
            ],
            "actions": [
                {"targetId": index, "arguments": ["clang", mode, "-c", source]}
                for index, (_, source) in enumerate(sources)
            ],
        }
        with (
            mock.patch(
                "tools.compilation_database.workspace_root", return_value=self.root
            ),
            mock.patch(
                "tools.compilation_database.production_targets",
                return_value=self.production,
            ),
            mock.patch(
                "tools.compilation_database.subprocess.run",
                return_value=mock.Mock(stdout=json.dumps(response)),
            ),
            redirect_stdout(io.StringIO()),
        ):
            self.assertEqual(generate([], fuzz_only=fuzz_only), 0)
        return json.loads((self.root / "compile_commands.json").read_text())

    def test_ordinary_generation_writes_the_complete_production_subset(self) -> None:
        entries = self.run_generator(self.ordinary_sources)
        self.assertEqual(
            {entry["file"] for entry in entries},
            {source for _, source in self.ordinary_sources},
        )
        strict = json.loads(self.strict_database.read_text())
        self.assertEqual(
            {entry["file"] for entry in strict}, set().union(*self.production.values())
        )
        self.assertTrue(all("-DORDINARY=1" in entry["arguments"] for entry in strict))

    def test_fuzz_generation_preserves_the_ordinary_production_database(self) -> None:
        for dependencies in ((), (self.ordinary_sources[0],)):
            with self.subTest(production_dependencies=dependencies):
                self.run_generator(self.ordinary_sources)
                original = self.strict_database.read_bytes()
                entries = self.run_generator(
                    (self.fuzz_source, *dependencies), fuzz_only=True
                )
                self.assertEqual(self.strict_database.read_bytes(), original)
                self.assertEqual(
                    {entry["file"] for entry in entries},
                    {source for _, source in (self.fuzz_source, *dependencies)},
                )
                self.assertTrue(
                    all("-DFUZZ=1" in entry["arguments"] for entry in entries)
                )

    def test_fuzz_generation_does_not_create_a_production_database(self) -> None:
        self.run_generator((self.fuzz_source,), fuzz_only=True)
        self.assertFalse(self.strict_database.parent.exists())


class ExternalLinkTest(unittest.TestCase):
    def test_link_is_created_and_verified(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            external = root / "base" / "external"
            external.mkdir(parents=True)
            (root / "base" / "execroot" / "_main").mkdir(parents=True)
            (root / "workspace").mkdir()
            (root / "workspace" / "bazel-out").symlink_to(
                root / "base" / "execroot" / "_main" / "bazel-out"
            )
            (root / "base" / "execroot" / "_main" / "bazel-out").mkdir()

            ensure_external_link(root / "workspace", external)

            link = root / "workspace" / "external"
            self.assertTrue(link.is_symlink())
            self.assertEqual(os.readlink(link), EXTERNAL_LINK_TARGET)
            self.assertEqual(link.resolve(), external.resolve())

    def test_a_stale_link_is_repaired(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            external = root / "base" / "external"
            external.mkdir(parents=True)
            main = root / "base" / "execroot" / "_main"
            main.mkdir(parents=True)
            (main / "bazel-out").mkdir()
            workspace = root / "workspace"
            workspace.mkdir()
            (workspace / "bazel-out").symlink_to(main / "bazel-out")
            (workspace / "external").symlink_to("/somewhere/stale")

            ensure_external_link(workspace, external)

            self.assertEqual(os.readlink(workspace / "external"), EXTERNAL_LINK_TARGET)

    def test_active_output_base_comes_from_the_workspace_symlink(self) -> None:
        """The base must follow the workspace, not a nested bazel's default."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = root / "some_output_base"
            main = base / "execroot" / "_main"
            (main / "bazel-out").mkdir(parents=True)
            workspace = root / "workspace"
            workspace.mkdir()
            (workspace / "bazel-out").symlink_to(main / "bazel-out")

            self.assertEqual(active_output_base(workspace), base)

    def test_active_output_base_requires_a_build(self) -> None:
        with (
            tempfile.TemporaryDirectory() as directory,
            self.assertRaisesRegex(RuntimeError, "bazel-out is missing"),
        ):
            active_output_base(Path(directory))

    def test_a_regular_bazel_out_directory_is_rejected(self) -> None:
        """A real directory carries no output base to derive."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "bazel-out").mkdir()
            with self.assertRaisesRegex(RuntimeError, "is not a symlink"):
                active_output_base(root)

    def test_a_dangling_bazel_out_link_is_rejected(self) -> None:
        """It satisfies lexists, so only resolving it catches the bad target."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "bazel-out").symlink_to(
                root / "gone" / "execroot" / "_main" / "bazel-out"
            )
            with self.assertRaisesRegex(RuntimeError, "unexpected bazel-out target"):
                active_output_base(root)

    def test_a_link_outside_an_execroot_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            elsewhere = root / "somewhere" / "else" / "bazel-out"
            elsewhere.mkdir(parents=True)
            (root / "bazel-out").symlink_to(elsewhere)
            with self.assertRaisesRegex(RuntimeError, "unexpected bazel-out target"):
                active_output_base(root)

    def test_a_link_to_a_differently_named_target_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "base" / "execroot" / "_main" / "other-out"
            target.mkdir(parents=True)
            (root / "bazel-out").symlink_to(target)
            with self.assertRaisesRegex(RuntimeError, "unexpected bazel-out target"):
                active_output_base(root)

    def test_missing_bazel_out_is_reported(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(RuntimeError, "bazel-out is missing"):
                ensure_external_link(root, root / "external")

    def test_a_real_directory_is_not_replaced(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "bazel-out").mkdir()
            (root / "external").mkdir()
            with self.assertRaisesRegex(RuntimeError, "not a symlink"):
                ensure_external_link(root, root / "elsewhere")


if __name__ == "__main__":
    unittest.main()
