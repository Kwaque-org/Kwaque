from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from tools.run_buildifier import main, resolve_runfile


@unittest.skipUnless(
    os.environ.get("KWAQUE_TEST_BUILDIFIER"),
    "buildifier supplied by the Bazel test target",
)
class BuildifierTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tool = str(resolve_runfile(os.environ["KWAQUE_TEST_BUILDIFIER"]))
        self.git = shutil.which("git")
        self.assertIsNotNone(self.git, "Git is a required build-host tool")
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        subprocess.run(
            [self.git, "init", "--template=", "-q", str(self.root)],
            check=True,
            env={**os.environ, "GIT_CONFIG_GLOBAL": os.devnull},
        )

    def run_tool(self, mode: str) -> int:
        argv = ["buildifier", f"--tool={self.tool}", f"--mode={mode}"]
        with (
            mock.patch.dict(os.environ, {"BUILD_WORKSPACE_DIRECTORY": str(self.root)}),
            mock.patch("sys.argv", argv),
        ):
            return main()

    def test_lint_and_format_failures_are_reported_and_then_fixed(self) -> None:
        build = self.root / "pkg" / "BUILD"
        build.parent.mkdir()
        # A load after a rule is a lint warning; the call layout is a format
        # difference.
        build.write_text(
            'cc_library(name="a")\n\nload("@rules_cc//cc:defs.bzl", "cc_library")\n'
        )
        overlay = self.root / "third_party.BUILD"
        overlay.write_text('cc_library(name="b")\n')

        self.assertNotEqual(self.run_tool("diff"), 0)
        self.assertEqual(self.run_tool("fix"), 0)
        fixed = build.read_text()
        self.assertTrue(fixed.startswith('load("@rules_cc//cc:defs.bzl"'), fixed)
        # Overlay files are linted too: the rule gains its load statement.
        self.assertEqual(
            overlay.read_text(),
            'load("@rules_cc//cc:cc_library.bzl", "cc_library")\n\n'
            'cc_library(name = "b")\n',
        )
        self.assertEqual(self.run_tool("diff"), 0)
        self.assertEqual(self.run_tool("fix"), 0)
        self.assertEqual(build.read_text(), fixed)


if __name__ == "__main__":
    unittest.main()
