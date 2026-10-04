"""Keep the README's host-check table in step with the checks the broker grades."""

from __future__ import annotations

import re
import sys
import unittest
from pathlib import Path


class HostCheckDocumentationTest(unittest.TestCase):
    def test_readme_lists_exactly_the_checks_the_broker_grades(self) -> None:
        source = Path(sys.argv[1]).read_text(encoding="utf-8")
        readme = Path(sys.argv[2]).read_text(encoding="utf-8")
        body = source.split("host_check_report evaluate_host_checks(", 1)[1]
        body = body.split("\n}\n", 1)[0]
        checks = set(re.findall(r'^\s*\{"([a-z_]+)",', body, re.MULTILINE))
        section = readme.split("#### Host checks\n", 1)[1].split("\n#### ", 1)[0]
        documented = set(re.findall(r"^\| `([a-z_]+)` \|", section, re.MULTILINE))
        self.assertGreater(len(checks), 10)
        self.assertEqual(documented, checks)


if __name__ == "__main__":
    unittest.main(argv=[sys.argv[0]])
