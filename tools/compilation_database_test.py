from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from tools.compilation_database import (
    EXTERNAL_LINK_TARGET,
    active_output_base,
    compiler_arguments,
    ensure_external_link,
)


def action(*arguments: str) -> dict[str, list[str]]:
    return {"arguments": list(arguments)}


class CompilerArgumentsTest(unittest.TestCase):
    execution_root = Path("/output_base/execroot/_main")

    def parse(self, *arguments: str) -> tuple[str, list[str]] | None:
        return compiler_arguments(action(*arguments), self.execution_root)

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
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(RuntimeError, "bazel-out is missing"):
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
            (root / "bazel-out").symlink_to(root / "gone" / "execroot" / "_main" / "bazel-out")
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
