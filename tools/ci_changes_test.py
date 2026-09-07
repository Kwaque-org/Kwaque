from __future__ import annotations

import contextlib
import io
import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from tools import ci_changes


class DocumentationSelectionTest(unittest.TestCase):
    def test_only_documentation_content_changes_skip_checks(self) -> None:
        for path in (
            "README.md",
            "src/runtime/README.md",
            "CONTRIBUTING.md",
            "CODE_OF_CONDUCT.md",
            "SECURITY.md",
            "docs/design.md",
            "docs/user guide.rst",
            "docs/guide.adoc",
        ):
            for status in ("A", "M"):
                with self.subTest(path=path, status=status):
                    self.assertTrue(
                        ci_changes.documentation_only(f"{status}\0{path}\0".encode())
                    )

    def test_build_inputs_inventories_and_unknown_paths_run_checks(self) -> None:
        for path in (
            "src/runtime/file.cc",
            "proto/message.proto",
            "tools/check.py",
            "bazel/test.bzl",
            "BUILD",
            "MODULE.bazel.lock",
            ".bazelrc",
            ".clang-tidy",
            ".github/workflows/ci.yml",
            "THIRD_PARTY.md",
            "DEPENDENCIES.md",
            "LICENSE",
            "NOTICE",
            "ubsan_suppressions.txt",
            "docs/generator.py",
            "new-component/input.md",
        ):
            with self.subTest(path=path):
                diff = b"M\0README.md\0M\0" + os.fsencode(path) + b"\0"
                self.assertFalse(ci_changes.documentation_only(diff))

    def test_deletions_and_type_changes_keep_validation(self) -> None:
        for status in ("D", "T", "U"):
            with self.subTest(status=status):
                self.assertFalse(
                    ci_changes.documentation_only(f"{status}\0README.md\0".encode())
                )

    def test_complete_diff_is_not_limited_to_first_300_files_or_lines(self) -> None:
        documents = b"".join(
            f"M\0docs/page-{index}.md\0".encode() for index in range(400)
        )
        self.assertTrue(ci_changes.documentation_only(documents))
        self.assertFalse(
            ci_changes.documentation_only(documents + b"M\0src/line\nbreak.cc\0")
        )
        self.assertTrue(ci_changes.documentation_only(b"M\0docs/line\nbreak.md\0"))

    def test_incomplete_diff_is_not_documentation_only(self) -> None:
        for diff in (b"M\0README.md", b"M\0", b"M\0README.md\0M\0", b"M\0\0"):
            with self.subTest(diff=diff), self.assertRaises(ValueError):
                ci_changes.documentation_only(diff)

    def test_manual_and_unknown_events_run_without_git(self) -> None:
        with mock.patch.object(ci_changes.subprocess, "run") as run:
            for event_name in ("workflow_dispatch", "unknown"):
                self.assertTrue(
                    ci_changes.requires_checks(event_name, {}, "", Path.cwd())
                )
            run.assert_not_called()

    def test_unreadable_event_still_selects_checks(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            event, output = root / "event.json", root / "output"
            event.write_text("invalid json")
            with mock.patch.dict(
                os.environ,
                {"GITHUB_EVENT_PATH": str(event), "GITHUB_OUTPUT": str(output)},
            ), contextlib.redirect_stdout(io.StringIO()):
                ci_changes.main()
            self.assertEqual(output.read_text(), "run_checks=true\n")


class GitChangeSelectionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.git_binary = shutil.which("git")
        self.assertIsNotNone(self.git_binary, "Git is required for CI change selection")
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        environment = {
            name: value
            for name, value in os.environ.items()
            if not name.startswith("GIT_")
        }
        environment.update(
            {
                "GIT_CONFIG_GLOBAL": str(self.root / "missing-global"),
                "GIT_CONFIG_SYSTEM": str(self.root / "missing-system"),
                "GIT_CONFIG_COUNT": "1",
                "GIT_CONFIG_KEY_0": "core.excludesFile",
                "GIT_CONFIG_VALUE_0": str(self.root / "missing-excludes"),
            }
        )
        patch = mock.patch.dict(os.environ, environment, clear=True)
        patch.start()
        self.addCleanup(patch.stop)
        self.git("init", "--template=", "--initial-branch=main", "-q")
        (self.root / "README.md").write_text("original documentation\n")
        (self.root / "source.cc").write_text("int original;\n")
        self.base = self.commit()

    def git(self, *arguments: str) -> str:
        return subprocess.run(
            [
                self.git_binary,
                "-c",
                "user.name=CI test",
                "-c",
                "user.email=ci@example.invalid",
                "-c",
                "commit.gpgSign=false",
                *arguments,
            ],
            cwd=self.root,
            check=True,
            text=True,
            capture_output=True,
        ).stdout.strip()

    def commit(self) -> str:
        self.git("add", "--all", "--force")
        self.git("commit", "-qm", "test change")
        return self.git("rev-parse", "HEAD")

    def test_documentation_change_skips_for_pr_push_and_merge_queue(self) -> None:
        (self.root / "README.md").write_text("updated documentation\n")
        head = self.commit()
        for name, event in (
            ("pull_request", {"pull_request": {"base": {"sha": self.base}}}),
            ("push", {"before": self.base}),
            ("merge_group", {"merge_group": {"base_sha": self.base}}),
        ):
            with self.subTest(event=name):
                self.assertFalse(
                    ci_changes.requires_checks(name, event, head, self.root)
                )

    def test_code_in_an_earlier_pushed_commit_is_included(self) -> None:
        (self.root / "source.cc").write_text("int changed;\n")
        self.commit()
        (self.root / "README.md").write_text("docs in final commit\n")
        head = self.commit()
        self.assertTrue(
            ci_changes.requires_checks("push", {"before": self.base}, head, self.root)
        )

    def test_pr_uses_tested_merge_tree_against_current_base(self) -> None:
        self.git("checkout", "-qb", "documentation")
        (self.root / "README.md").write_text("PR documentation\n")
        self.commit()
        self.git("checkout", "-q", "main")
        (self.root / "source.cc").write_text("int advanced_base;\n")
        base = self.commit()
        self.git("merge", "--no-ff", "-qm", "test merge", "documentation")
        head = self.git("rev-parse", "HEAD")
        self.assertFalse(
            ci_changes.requires_checks(
                "pull_request",
                {"pull_request": {"base": {"sha": base}}},
                head,
                self.root,
            )
        )

    def test_source_renamed_to_documentation_still_runs_checks(self) -> None:
        (self.root / "docs").mkdir()
        (self.root / "source.cc").rename(self.root / "docs/README.md")
        head = self.commit()
        self.assertTrue(
            ci_changes.requires_checks("push", {"before": self.base}, head, self.root)
        )

    def test_removing_packaged_readme_keeps_checks(self) -> None:
        (self.root / "README.md").unlink()
        head = self.commit()
        self.assertTrue(
            ci_changes.requires_checks("push", {"before": self.base}, head, self.root)
        )

    def test_missing_history_invalid_event_and_wrong_checkout_run_checks(self) -> None:
        for event, head in (
            ({}, self.base),
            ({"before": "0" * 40}, self.base),
            ({"before": "f" * 40}, self.base),
            ({"before": self.base}, "f" * 40),
            ({"before": "--bad-option"}, self.base),
        ):
            with self.subTest(event=event, head=head), contextlib.redirect_stdout(
                io.StringIO()
            ):
                self.assertTrue(
                    ci_changes.requires_checks("push", event, head, self.root)
                )

    def test_output_for_documentation_only_push(self) -> None:
        (self.root / "README.md").write_text("updated documentation\n")
        head = self.commit()
        event, output = self.root / "event.json", self.root / "output"
        event.write_text(json.dumps({"before": self.base}))
        with mock.patch.dict(
            os.environ,
            {
                "GITHUB_EVENT_NAME": "push",
                "GITHUB_EVENT_PATH": str(event),
                "GITHUB_SHA": head,
                "GITHUB_OUTPUT": str(output),
            },
        ), mock.patch.object(
            ci_changes.Path, "cwd", return_value=self.root
        ), contextlib.redirect_stdout(
            io.StringIO()
        ):
            ci_changes.main()
        self.assertEqual(output.read_text(), "run_checks=false\n")


if __name__ == "__main__":
    unittest.main()
