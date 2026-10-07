from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from src.storage.tests import index_bench_report as report


def measured(prefix: str = "", **changed) -> dict:
    values = {prefix + name: 0 for name in report.MEASURED}
    values[prefix + "elapsed_ns"] = 1000
    values.update({prefix + name: value for name, value in changed.items()})
    return values


def budget(prefix: str, **changed) -> dict:
    values = {prefix + name: 0 for name in report.BUDGET}
    values.update({prefix + name: value for name, value in changed.items()})
    return values


def data(**changed) -> dict:
    values = {
        "blocks": 4096,
        "segment_filled": False,
        "data_bytes": 17043456,
        "stride_bytes": 0,
        "stride_records": 0,
        "anchors": 4096,
        "dataset_digest": 77,
    }
    values.update(changed)
    return values


def lookups(**changed) -> dict:
    values = {
        "lookups": 4096,
        "found": 3584,
        "result_digest": 5,
        "near_result_digest": 9,
        "far_lookups": 512,
        "far_at_first_block": 0,
        "scan_bytes": 3584 * 4096,
        "longest_scan_bytes": 4096,
        "scan_bound_bytes": 3584 * 8192,
        "longest_scan_bound_bytes": 16384,
    }
    values.update(changed)
    return values


def lookup_row(case: str, side: str, **changed) -> dict:
    cold = case == "cold_open_lookup"
    row = {
        "case": case,
        "side": side,
        "timing_only": False,
        "ndebug": True,
        **data(),
        **lookups(),
        **measured(
            index_opens=1 if cold else 0,
            index_reads=(3585 if cold else 3584) if side != "resident" else 0,
        ),
        **budget("held_", budget_bytes=360448, budget_handles=1),
    }
    if side == "resident":
        row.update(
            resident_column_bytes=65536,
            resident_file_bytes=65564,
            far_at_first_block=512,
            scan_bound_bytes=0,
            longest_scan_bound_bytes=0,
        )
        row["result_digest"] = 6
        if cold:
            row["index_reads"] = 1
    row.update(changed)
    return row


def stride_row(stride: int, anchors: int, **changed) -> dict:
    row = {
        "case": "stride",
        "side": "candidate",
        "timing_only": False,
        "ndebug": True,
        "memory_observed": False,
        **data(stride_bytes=stride, anchors=anchors),
        "pages": 1,
        "bundle_bytes": 24576,
        "root_bytes": 4096,
        "capacity_at_1gib": 32769,
        "admitted_at_1gib_bytes": 524288,
        **lookups(),
        **measured(data_reads=140, data_read_bytes=17100000),
        **measured("open_", index_opens=1, index_reads=1, index_read_bytes=4096),
        **measured("cold_lookup_", index_reads=1, index_read_bytes=20480),
        **budget("root_", budget_bytes=360448, budget_handles=1),
    }
    row.update(changed)
    return row


def many_row(side: str, **changed) -> dict:
    row = {
        "case": "open_many_segments",
        "side": side,
        "timing_only": False,
        "ndebug": True,
        "segments": 32,
        "blocks_per_segment": 64,
        "data_bytes": 32 * 270336,
        "anchors": 32 * 64,
        "open_root_bound": 16,
        "roots_left_open": 16 if side == "candidate" else 0,
        "root_loads": 32 if side == "candidate" else 0,
        **measured(index_opens=32, index_reads=32, index_read_bytes=32 * 4096),
        **budget("held_", budget_bytes=16 * 360448, budget_handles=16),
        **budget("most_", budget_bytes=16 * 360448, budget_handles=16),
    }
    row.update(changed)
    return row


def rebuild_row(**changed) -> dict:
    row = {
        "case": "rebuild",
        "side": "candidate",
        "timing_only": False,
        "ndebug": True,
        "foreground_probe": False,
        "foreground_turns": 0,
        "foreground_longest_wait_ns": 0,
        **data(stride_bytes=32768, anchors=520),
        **lookups(),
        **measured(data_reads=140, data_read_bytes=17100000, hashed_bytes=17039360),
        **measured("persist_", index_flushes=2, index_write_bytes=36864),
        **budget("memory_", budget_bytes=16384, budget_tasks=1),
    }
    row.update(changed)
    return row


def write_log(directory: str, rows: list[dict]) -> Path:
    path = Path(directory) / "index.log"
    path.write_text(
        "unrelated output\n"
        + "".join(report.PREFIX + json.dumps(row) + "\n" for row in rows),
        encoding="utf-8",
    )
    return path


class IndexBenchReportTest(unittest.TestCase):
    def summarize(self, rows: list[dict], partial: bool = True) -> list[dict]:
        with tempfile.TemporaryDirectory() as directory:
            return report.summarize(
                report.read_measurements(write_log(directory, rows)), partial
            )

    def test_sides_of_a_lookup_are_summarized_apart(self):
        rows = [
            lookup_row("warm_lookup", side, elapsed_ns=elapsed)
            for side, elapsed in (
                ("candidate", 8192000),
                ("candidate", 4096000),
                ("baseline", 4096000),
                ("resident", 40960),
            )
        ]
        summaries = {item["side"]: item for item in self.summarize(rows)}
        self.assertEqual(summaries["candidate"]["runs"], 2)
        self.assertEqual(summaries["candidate"]["elapsed_ns_per_lookup"], 1500.0)
        self.assertEqual(summaries["candidate"]["index_reads_per_found"], 1.0)
        self.assertEqual(summaries["resident"]["index_reads"], 0)
        self.assertEqual(
            summaries["resident"]["resident_bytes_per_tib"],
            65536 * report.TIB / 17043456,
        )
        self.assertIn("warm_lookup [candidate", report.render(list(summaries.values())))

    def test_cold_lookup_opens_one_file_and_no_data(self):
        self.summarize([lookup_row("cold_open_lookup", "candidate")])
        for changed in (
            {"index_opens": 2},
            {"data_reads": 1},
            {"data_opens": 1},
            {"index_writes": 1},
        ):
            with self.assertRaises(ValueError):
                self.summarize([lookup_row("cold_open_lookup", "candidate", **changed)])
        with self.assertRaises(ValueError):
            self.summarize([lookup_row("warm_lookup", "baseline", index_opens=1)])
        # One page for every lookup that has an anchor, and the root once.
        for case, side, reads in (
            ("warm_lookup", "candidate", 3585),
            ("warm_lookup", "baseline", 3583),
            ("cold_open_lookup", "candidate", 3584),
            ("cold_open_lookup", "baseline", 3586),
        ):
            with self.assertRaises(ValueError):
                self.summarize([lookup_row(case, side, index_reads=reads)])
        with self.assertRaises(ValueError):
            self.summarize([lookup_row("warm_lookup", "resident", index_reads=1)])

    def test_sides_must_find_the_same_anchors(self):
        with self.assertRaises(ValueError):
            self.summarize(
                [
                    lookup_row("warm_lookup", "candidate"),
                    lookup_row("warm_lookup", "baseline", result_digest=8),
                ]
            )
        with self.assertRaises(ValueError):
            self.summarize(
                [
                    lookup_row("warm_lookup", "candidate"),
                    lookup_row("warm_lookup", "resident", near_result_digest=8),
                ]
            )
        # Past 32 bits the resident index answers with its first entry.
        self.summarize(
            [
                lookup_row("warm_lookup", "candidate"),
                lookup_row("warm_lookup", "resident"),
            ]
        )
        with self.assertRaises(ValueError):
            self.summarize(
                [lookup_row("warm_lookup", "candidate", far_at_first_block=1)]
            )

    def test_sides_are_compared_over_one_dataset_or_not_at_all(self):
        sides = [
            lookup_row("warm_lookup", side)
            for side in ("candidate", "baseline", "resident")
        ]
        self.assertEqual(len(self.summarize(sides, partial=False)), 3)
        # A side that is missing would leave the others agreeing with nobody.
        with self.assertRaises(ValueError):
            self.summarize(sides[:2], partial=False)
        self.assertEqual(len(self.summarize(sides[:2], partial=True)), 2)
        # Another dataset is another comparison, whatever was asked for.
        other = lookup_row(
            "warm_lookup", "baseline", dataset_digest=78, result_digest=8
        )
        for partial in (False, True):
            with self.assertRaises(ValueError):
                self.summarize([sides[0], other, sides[2]], partial)
        # One side at each stride is what a sweep is.
        self.summarize(
            [stride_row(32768, 520), stride_row(4096, 4096, bundle_bytes=86016)],
            partial=False,
        )

    def test_timing_rows_carry_no_native_counts(self):
        row = lookup_row("cold_open_lookup", "candidate", timing_only=True)
        row.update(index_opens=0, index_reads=0)
        summary = self.summarize([row])[0]
        self.assertTrue(summary["timing_only"])

    def test_stride_rows_scale_to_a_tebibyte(self):
        summaries = self.summarize(
            [stride_row(32768, 520), stride_row(4096, 4096, bundle_bytes=86016)]
        )
        by_stride = {item["stride_bytes"]: item for item in summaries}
        self.assertEqual(
            by_stride[32768]["resident_bytes_per_tib"],
            520 * 16 * report.TIB / 17043456,
        )
        self.assertEqual(
            by_stride[4096]["bundle_bytes_per_tib"], 86016 * report.TIB / 17043456
        )
        self.assertEqual(by_stride[4096]["cold_lookup_index_reads"], 1)
        for changed in (
            {"index_reads": 1},
            {"data_reads": 0},
            {"open_index_opens": 0},
            {"open_data_reads": 1},
            {"cold_lookup_index_opens": 1},
            {"cold_lookup_data_reads": 1},
        ):
            with self.assertRaises(ValueError):
                self.summarize([stride_row(32768, 520, **changed)])

    def test_observed_memory_is_part_of_a_stride_row(self):
        observed = stride_row(
            32768,
            520,
            memory_observed=True,
            open_live_bytes=2048,
            open_peak_bytes=12288,
            open_memory_complete=True,
            lookup_peak_bytes=40960,
            lookup_largest_allocation=20480,
            lookup_memory_complete=True,
        )
        summary = self.summarize([observed])[0]
        self.assertEqual(summary["open_live_bytes"], 2048)
        self.assertTrue(summary["memory_complete"])
        del observed["lookup_peak_bytes"]
        with self.assertRaises(ValueError):
            self.summarize([observed])

    def test_many_segments_stay_within_the_bound(self):
        summaries = {
            item["side"]: item
            for item in self.summarize([many_row("candidate"), many_row("resident")])
        }
        self.assertEqual(summaries["candidate"]["index_reads_per_segment"], 1.0)
        self.assertEqual(summaries["candidate"]["roots_left_open"], 16)
        for changed in (
            {"index_opens": 33},
            {"root_loads": 33},
            {"roots_left_open": 17},
            {"most_budget_handles": 17},
            {"data_reads": 1},
        ):
            with self.assertRaises(ValueError):
                self.summarize([many_row("candidate", **changed)])

    def test_rebuild_reads_the_data_alone(self):
        self.summarize([rebuild_row(), rebuild_row(side="resident")])
        summary = self.summarize([rebuild_row()])[0]
        self.assertAlmostEqual(summary["read_amplification"], 17100000 / 17043456)
        self.assertNotIn("foreground_turns", summary)
        probed = self.summarize(
            [rebuild_row(foreground_probe=True, foreground_turns=9)]
        )[0]
        self.assertEqual(probed["foreground_turns"], 9)
        for changed in (
            {"index_reads": 1},
            {"data_reads": 0},
            {"persist_index_flushes": 0},
        ):
            with self.assertRaises(ValueError):
                self.summarize([rebuild_row(**changed)])

    def test_malformed_rows_are_rejected(self):
        good = lookup_row("warm_lookup", "candidate")
        for changed in (
            {"case": "other"},
            {"side": "other"},
            {"blocks": -1},
            {"blocks": 1.5},
            {"timing_only": 1},
            {"found": 5000},
            {"anchors": 0},
            {"anchors": 5000},
            {"extra": 1},
        ):
            with self.assertRaises(ValueError):
                self.summarize([{**good, **changed}])
        missing = dict(good)
        del missing["tasks"]
        with self.assertRaises(ValueError):
            self.summarize([missing])
        with self.assertRaises(ValueError):
            self.summarize([lookup_row("memory_lookup", "baseline")])
        with self.assertRaises(ValueError):
            report.summarize([])
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "bad.log"
            path.write_text(report.PREFIX + "{not json\n", encoding="utf-8")
            with self.assertRaises(ValueError):
                report.read_measurements(path)


if __name__ == "__main__":
    unittest.main()
