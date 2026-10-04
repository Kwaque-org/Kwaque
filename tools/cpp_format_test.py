from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from tools.cpp_format import main, resolve_runfile, selected_files


def isolated_git_environment(root: Path) -> dict[str, str]:
    environment = {
        name: value for name, value in os.environ.items() if not name.startswith("GIT_")
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
            "GIT_AUTHOR_NAME": "test",
            "GIT_AUTHOR_EMAIL": "test@example.invalid",
            "GIT_COMMITTER_NAME": "test",
            "GIT_COMMITTER_EMAIL": "test@example.invalid",
        }
    )
    return environment


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
            # selected_files() invokes Git too; scope every subprocess to the
            # isolated configuration and discard inherited repository overrides.
            with mock.patch.dict(
                os.environ, isolated_git_environment(root), clear=True
            ):
                subprocess.run(
                    [git, "init", "--template=", "-q", str(root)], check=True
                )
                (root / ".gitignore").write_text("ignored.cc\n")
                for name in (
                    "tracked.cc",
                    "new.h",
                    "schema.proto",
                    "ignored.cc",
                    "notes.txt",
                ):
                    (root / name).write_text("")
                subprocess.run(
                    [git, "add", "tracked.cc", ".gitignore"], cwd=root, check=True
                )
                self.assertEqual(
                    selected_files(root, "all", []),
                    [Path("new.h"), Path("schema.proto"), Path("tracked.cc")],
                )

    def test_changed_scope_includes_earlier_branch_commits_against_a_base(
        self,
    ) -> None:
        git = shutil.which("git")
        self.assertIsNotNone(git)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with mock.patch.dict(
                os.environ, isolated_git_environment(root), clear=True
            ):

                def run(*arguments: str) -> None:
                    subprocess.run([git, *arguments], cwd=root, check=True)

                run("init", "--template=", "-q", "-b", "main")
                (root / "base.cc").write_text("")
                run("add", "base.cc")
                run("commit", "-q", "-m", "base")
                run("checkout", "-q", "-b", "topic")
                (root / "committed.cc").write_text("")
                run("add", "committed.cc")
                run("commit", "-q", "-m", "topic")
                (root / "base.cc").write_text("int x;\n")
                (root / "untracked.h").write_text("")

                self.assertEqual(
                    selected_files(root, "changed", [], "main"),
                    [Path("base.cc"), Path("committed.cc"), Path("untracked.h")],
                )
                self.assertEqual(
                    selected_files(root, "changed", []),
                    [Path("base.cc"), Path("untracked.h")],
                )

    @unittest.skipUnless(
        os.environ.get("KWAQUE_TEST_CLANG_FORMAT"),
        "clang-format supplied by the Bazel test target",
    )
    def test_pinned_formatter_reports_fixes_and_then_leaves_a_fixture_unchanged(
        self,
    ) -> None:
        tool = str(resolve_runfile(os.environ["KWAQUE_TEST_CLANG_FORMAT"]))
        repository = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copyfile(repository / ".clang-format", root / ".clang-format")
            fixture = root / "fixture.cc"
            fixture.write_text(
                '#include <vector>\n#include "src/base/units.h"\n'
                "namespace kwaque{int const answer( ){return 42;}}\n"
            )

            def run(*arguments: str) -> int:
                argv = ["format", f"--tool={tool}", *arguments, "fixture.cc"]
                with (
                    mock.patch("tools.cpp_format.workspace_root", return_value=root),
                    mock.patch("sys.argv", argv),
                ):
                    return main()

            self.assertNotEqual(run("--check"), 0)
            self.assertEqual(run(), 0)
            formatted = fixture.read_bytes()
            self.assertNotEqual(formatted, b"")
            self.assertIn(b"} // namespace kwaque", formatted)
            self.assertLess(
                formatted.index(b'"src/base/units.h"'), formatted.index(b"<vector>")
            )
            self.assertEqual(run("--check"), 0)
            self.assertEqual(run(), 0)
            self.assertEqual(fixture.read_bytes(), formatted)

    def test_formatter_failure_is_returned_with_warning_as_error_check_mode(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "example.cc").write_text("int main( ){return 0;}")
            with (
                mock.patch("tools.cpp_format.workspace_root", return_value=root),
                mock.patch(
                    "sys.argv", ["format", "--tool=/bin/false", "--check", "example.cc"]
                ),
                mock.patch("tools.cpp_format.subprocess.run") as run,
            ):
                run.return_value.returncode = 1
                self.assertEqual(main(), 1)
                self.assertIn("--dry-run", run.call_args.args[0])
                self.assertIn("--Werror", run.call_args.args[0])


if __name__ == "__main__":
    unittest.main()
