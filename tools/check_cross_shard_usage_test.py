from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tools.check_cross_shard_usage import scan_paths


class CrossShardUsageTest(unittest.TestCase):
    def write(self, root: Path, relative: str, content: str) -> None:
        destination = root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        with destination.open("w", encoding="utf-8", newline="") as source:
            source.write(content)

    def test_rejects_direct_primitives(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.write(
                root,
                "src/component/unsafe.cc",
                """
                star::sharded <service> services;
                auto work = star::smp :: submit_to(1, [] {});
                foreign_ptr<std::unique_ptr<int>> value;
                """,
            )
            violations = scan_paths(root, [Path("src")], allowed_rules={})
            self.assertEqual(len(violations), 3)

    def test_rejects_comments_between_tokens(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.write(
                root,
                "src/component/commented.cc",
                """
                auto first = star::smp::submit_to /* note */ (1, [] {});
                star::sharded /* note */ <service> first_service;
                auto second = star::smp // note
                    :: submit_to // note
                    (1, [] {});
                star::sharded // note
                    <service> second_service;
                """,
            )
            violations = scan_paths(root, [Path("src")], allowed_rules={})
            self.assertEqual(len(violations), 4)
            self.assertEqual(
                [violation.rsplit(": ", maxsplit=1)[1] for violation in violations],
                [
                    "direct cross-shard submission",
                    "direct cross-shard submission",
                    "direct sharded-service ownership",
                    "direct sharded-service ownership",
                ],
            )

    def test_ignores_forbidden_spellings_outside_code_tokens(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.write(
                root,
                "src/component/literals.cc",
                r"""
                // smp::submit_to(1, [] {});
                /* sharded<service> and foreign_ptr */
                constexpr auto text = "smp::submit_to(";
                constexpr auto raw = R"tag(sharded<service>)tag";
                constexpr auto character = '<';
                """,
            )
            self.assertEqual(scan_paths(root, [Path("src")], allowed_rules={}), [])

    def test_rejects_line_spliced_identifiers_at_physical_lines(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.write(
                root,
                "src/component/spliced.cc",
                "auto first = smp::submit_\\\nto(1, [] {});\n"
                "shar\\\r\nded<service> services;\r\n"
                "foreign_\\\rptr<int> pointer;\r"
                "auto second = make_\\\r\nforeign(pointer);\r\n",
            )
            self.assertEqual(
                scan_paths(root, [Path("src")], allowed_rules={}),
                [
                    "src/component/spliced.cc:1: direct cross-shard submission",
                    "src/component/spliced.cc:3: direct sharded-service ownership",
                    "src/component/spliced.cc:5: foreign pointer",
                    "src/component/spliced.cc:7: foreign pointer",
                ],
            )

    def test_ignores_tests_and_unrelated_source(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.write(
                root,
                "src/component/safe.cc",
                "int local_value = 1;\n",
            )
            self.write(
                root,
                "src/component/safe_test.cc",
                "auto work = seastar::smp::submit_to(1, [] {});\n",
            )
            self.assertEqual(scan_paths(root, [Path("src")], allowed_rules={}), [])


if __name__ == "__main__":
    unittest.main()
