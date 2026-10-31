from __future__ import annotations

import unittest

from tools.check_release_seams import (
    contains_name,
    parse_names,
    parse_tables,
    parse_symbols,
    seam_errors,
)

CONTROL_SYMBOLS = [
    "kwaque::simulation::buggify_evaluator::fires(kwaque::simulation::"
    "deterministic_random const&, kwaque::runtime::buggify_site_descriptor "
    "const&, kwaque::runtime::buggify_key)",
    "main",
]
RELEASE_SYMBOLS = ["main", "kwaque::runtime::current_shard()"]


class ReleaseSeamsTest(unittest.TestCase):
    def test_a_clean_release_binary_and_a_full_control_pass(self) -> None:
        self.assertEqual(
            seam_errors(
                RELEASE_SYMBOLS,
                b"\x00other\x00",
                CONTROL_SYMBOLS,
                b"\x00early_roll\x00idle_seal\x00",
                ["early_roll", "idle_seal"],
            ),
            [],
        )

    def test_a_simulation_symbol_in_the_release_binary_fails(self) -> None:
        for symbol in (
            "kwaque::simulation::mark_counters::hit(unsigned long)",
            "kwaque::runtime::buggify_sites",
            "kwaque::runtime::coverage_marks",
        ):
            with self.subTest(symbol=symbol):
                errors = seam_errors(
                    [*RELEASE_SYMBOLS, symbol], b"", CONTROL_SYMBOLS, b"", []
                )
                self.assertEqual(len(errors), 1)
                self.assertIn(symbol, errors[0])

    def test_a_site_or_mark_name_in_release_data_fails(self) -> None:
        errors = seam_errors(
            RELEASE_SYMBOLS,
            b"text\x00early_roll\x00",
            CONTROL_SYMBOLS,
            b"\x00early_roll\x00",
            ["early_roll"],
        )
        self.assertEqual(
            errors, ["the release binary's read-only data names: early_roll"]
        )

    def test_a_control_without_simulation_symbols_fails(self) -> None:
        self.assertEqual(
            seam_errors(RELEASE_SYMBOLS, b"", ["main"], b"", []),
            ["the positive control defines no simulation symbol"],
        )

    def test_a_control_without_a_name_fails(self) -> None:
        self.assertEqual(
            seam_errors(
                RELEASE_SYMBOLS,
                b"",
                CONTROL_SYMBOLS,
                b"\x00early_roll\x00",
                ["idle_seal"],
            ),
            ["the positive control's read-only data lacks: idle_seal"],
        )

    def test_a_control_without_a_named_table_fails(self) -> None:
        tables = parse_tables("buggify_site slow_ack\ncoverage_mark rare_path\n")
        self.assertEqual(
            tables,
            ["kwaque::runtime::buggify_sites", "kwaque::runtime::coverage_marks"],
        )
        errors = seam_errors(
            RELEASE_SYMBOLS,
            b"",
            ["kwaque::simulation::environment::make()"],
            b"slow_ack\x00rare_path\x00",
            ["slow_ack", "rare_path"],
            tables,
        )
        self.assertEqual(
            errors,
            [
                "the positive control defines no table: "
                "kwaque::runtime::buggify_sites kwaque::runtime::coverage_marks"
            ],
        )
        self.assertEqual(
            seam_errors(
                RELEASE_SYMBOLS,
                b"",
                ["kwaque::simulation::environment::make()", *tables],
                b"slow_ack\x00rare_path\x00",
                ["slow_ack", "rare_path"],
                tables,
            ),
            [],
        )

    def test_a_name_inside_a_longer_word_is_not_a_name(self) -> None:
        self.assertTrue(contains_name(b"\x00idle_seal\x00", "idle_seal"))
        self.assertTrue(contains_name(b"idle_seal\x00", "idle_seal"))
        self.assertFalse(contains_name(b"\x00not_idle_seal\x00", "idle_seal"))
        self.assertFalse(contains_name(b"\x00idle_seal_more\x00", "idle_seal"))
        self.assertFalse(contains_name(b"\x00idle_seal", "idle_seal"))

    def test_parsers_read_tool_output(self) -> None:
        self.assertEqual(
            parse_symbols(
                "0000000000001000 T main\n"
                "0000000000002000 W kwaque::simulation::x::y(int, long)\n"
                "\n"
            ),
            ["main", "kwaque::simulation::x::y(int, long)"],
        )
        self.assertEqual(
            parse_names(
                "buggify_site early_roll\ncoverage_mark idle_seal\n"
                "something else\nbuggify_site Not_A_Name\n"
            ),
            ["early_roll", "idle_seal"],
        )


if __name__ == "__main__":
    unittest.main()
