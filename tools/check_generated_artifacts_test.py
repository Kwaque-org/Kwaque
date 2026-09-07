from __future__ import annotations

import contextlib
import io
import unittest
from unittest import mock

from tools.check_generated_artifacts import is_forbidden, main


class GeneratedArtifactsTest(unittest.TestCase):
    def test_compiler_and_generated_outputs_are_rejected(self) -> None:
        for path in (
            "compile_commands.json",
            "bazel-out/k8-dbg/bin/item",
            "bazel-bin/program",
            "src/model/record.pb.cc",
            "src/model/record.pb.h",
            "src/model/config_generated.h",
            "tools/__pycache__/tool.pyc",
            "src/model/record.o",
        ):
            with self.subTest(path=path):
                self.assertTrue(is_forbidden(path))

    def test_source_and_corpus_inputs_are_allowed(self) -> None:
        for path in (
            "src/model/record.cc",
            "src/model/record.h",
            "bazel/thirdparty/library.patch",
            "testdata/seed.bin",
        ):
            self.assertFalse(is_forbidden(path))

    def test_command_reports_a_tracked_violation(self) -> None:
        with mock.patch("tools.check_generated_artifacts.subprocess.run") as run:
            run.return_value.stdout = (
                b"src/model/record.cc\x00compile_commands.json\x00"
            )
            with contextlib.redirect_stderr(io.StringIO()) as output:
                self.assertEqual(main(), 1)
            self.assertIn("compile_commands.json", output.getvalue())
            run.return_value.stdout = b"src/model/record.cc\x00"
            self.assertEqual(main(), 0)


if __name__ == "__main__":
    unittest.main()
