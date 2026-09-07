from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from tools.cpp_format import main, selected_files


class CppFormatTest(unittest.TestCase):
    def test_all_scope_covers_tracked_and_new_sources_but_not_ignored_files(
        self,
    ) -> None:
        git = shutil.which("git")
        self.assertIsNotNone(
            git, "Git is a required build-host tool for this integration test"
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            environment = {
                name: value
                for name, value in os.environ.items()
                if not name.startswith("GIT_")
            }
            environment.update(
                {
                    "GIT_CONFIG_GLOBAL": str(root / "missing-global-config"),
                    "GIT_CONFIG_SYSTEM": str(root / "missing-system-config"),
                    # Override the default global ignore file as well as configured
                    # ones. Repository-local .gitignore rules must still apply.
                    "GIT_CONFIG_COUNT": "1",
                    "GIT_CONFIG_KEY_0": "core.excludesFile",
                    "GIT_CONFIG_VALUE_0": str(root / "missing-global-excludes"),
                }
            )
            # selected_files() invokes Git too; scope every subprocess to the
            # isolated configuration and discard inherited repository overrides.
            with mock.patch.dict(os.environ, environment, clear=True):
                subprocess.run(
                    [git, "init", "--template=", "-q", str(root)], check=True
                )
                (root / ".gitignore").write_text("ignored.cc\n")
                for name in ("tracked.cc", "new.h", "ignored.cc", "notes.txt"):
                    (root / name).write_text("")
                subprocess.run(
                    [git, "add", "tracked.cc", ".gitignore"], cwd=root, check=True
                )
                self.assertEqual(
                    selected_files(root, "all", []), [Path("new.h"), Path("tracked.cc")]
                )

    def test_formatter_failure_is_returned_with_warning_as_error_check_mode(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "example.cc").write_text("int main( ){return 0;}")
            with mock.patch(
                "tools.cpp_format.workspace_root", return_value=root
            ), mock.patch(
                "sys.argv", ["format", "--tool=/bin/false", "--check", "example.cc"]
            ), mock.patch(
                "tools.cpp_format.subprocess.run"
            ) as run:
                run.return_value.returncode = 1
                self.assertEqual(main(), 1)
                self.assertIn("--dry-run", run.call_args.args[0])
                self.assertIn("--Werror", run.call_args.args[0])


if __name__ == "__main__":
    unittest.main()
