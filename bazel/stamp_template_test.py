"""Tests for stamped template expansion."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

try:
    from bazel import stamp_template
except ModuleNotFoundError:
    import stamp_template


class MergeVariablesTest(unittest.TestCase):
    def test_unstamped_builds_use_the_defaults(self) -> None:
        defaults = {"STABLE_KWAQUE_GIT_REVISION": "unknown"}
        self.assertEqual(stamp_template.merge_variables(defaults, None), defaults)

    def test_stamped_values_replace_every_default(self) -> None:
        merged = stamp_template.merge_variables(
            {"STABLE_A": "default", "STABLE_B": "0"},
            {"STABLE_A": "stamped", "STABLE_B": "1", "BUILD_HOST": "ignored"},
        )
        self.assertEqual(merged["STABLE_A"], "stamped")
        self.assertEqual(merged["STABLE_B"], "1")

    def test_a_default_missing_from_stamped_status_fails(self) -> None:
        with self.assertRaisesRegex(ValueError, "STABLE_RENAMED"):
            stamp_template.merge_variables(
                {"STABLE_A": "default", "STABLE_RENAMED": "0"},
                {"STABLE_A": "stamped", "STABLE_NEW_NAME": "1"},
            )


class ReadVariablesTest(unittest.TestCase):
    def test_reads_key_value_lines_and_rejects_bare_keys(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "status"
            path.write_text("KEY value with spaces\n\n", encoding="utf-8")
            self.assertEqual(
                stamp_template.read_variables(path), {"KEY": "value with spaces"}
            )
            path.write_text("KEY\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "expected KEY VALUE"):
                stamp_template.read_variables(path)


if __name__ == "__main__":
    unittest.main()
