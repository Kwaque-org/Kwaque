from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from src.storage.tests import read_bench_report as report


def measured(**changed) -> dict:
    values = {name: 0 for name in report.MEASURED}
    values["elapsed_ns"] = 1000
    values.update(changed)
    return values


def cache(**changed) -> dict:
    values = {name: 0 for name in report.CACHE}
    values["cache_capacity"] = 1 << 24
    values.update(changed)
    return values


def data(**changed) -> dict:
    values = {
        "blocks": 512,
        "segment_filled": False,
        "data_bytes": 2134016,
        "stride_bytes": 32768,
        "stride_records": 0,
        "anchors": 64,
        "dataset_digest": 77,
    }
    values.update(changed)
    return values


def common(case: str, side: str) -> dict:
    return {"case": case, "side": side, "timing_only": False, "ndebug": True}


def seek_row(side: str, **changed) -> dict:
    cold = side == "cold"
    row = {
        **common("read_seek", side),
        **data(),
        "seeks": 256,
        "returned_bytes": 256 * 300,
        **measured(
            data_reads=60 if cold else 0,
            data_read_bytes=60 * 32768 if cold else 0,
            index_reads=2 if cold else 0,
            crc_bulk_bytes=60 * 32768 if cold else 0,
            reactor_reads=62 if cold else 0,
            reactor_read_bytes=60 * 32768 + 2 * 20480 if cold else 0,
        ),
        **cache(cache_loads=62 if cold else 0),
    }
    row.update(changed)
    return row


def sealed_row(case: str, side: str, **changed) -> dict:
    row = {
        **common(case, side),
        **data(),
        "reads": 2,
        "batches": 512,
        "returned_bytes": 512 * 300,
        **measured(data_reads=64, data_read_bytes=64 * 32768),
        **cache(cache_loads=66),
    }
    if case == "read_ahead":
        row.update(depth=1, asked_ahead=62)
    if case == "read_stride":
        scan = side == "scan"
        row.update(depth=4 if scan else 0, grows=True, asked_ahead=62 if scan else 0)
    row.update(changed)
    return row


def tail_row(case: str, side: str, **changed) -> dict:
    row = {
        **common(case, side),
        "groups": 48,
        "group_blocks": 8,
        "stride_bytes": 32768,
        "append_ns": 48 * 8 * 1000,
        "read_ns": 5000,
        "scan_ns": 700 if case == "read_tail_scanned" else 0,
        "scanned_batches": 384 if case == "read_tail_scanned" else 0,
        "feed_dropped": 0,
        "foreground_turns": 0,
        "longest_wait_ns": 0,
        **measured(),
        **cache(cache_capacity=1 << 20),
    }
    fed = side != "unfed"
    for lag in report.LAGS:
        reads = 48 - lag
        row[f"lag{lag}_reads"] = reads
        row[f"lag{lag}_hits"] = reads if fed and side == "told" else 0
        row[f"lag{lag}_device_reads"] = 0 if side == "told" else reads
    row.update(changed)
    return row


class ReadBenchReportTest(unittest.TestCase):
    def test_accepts_every_case_and_sets_what_it_derives(self) -> None:
        rows = [
            seek_row("cold"),
            seek_row("warm"),
            sealed_row("read_stride", "scan"),
            sealed_row("read_stride", "seek", reads=256, batches=256),
            {**common("read_stride", "skipped"), "stride_bytes": 1 << 20},
            sealed_row("read_ahead", "fixed"),
            sealed_row("read_ahead", "growing", depth=4),
            sealed_row("read_held", "held", cache_loads=64, cache_uncached=64),
            sealed_row("read_held", "kept"),
            tail_row("read_tail", "unfed"),
            tail_row("read_tail", "untold"),
            tail_row("read_tail", "told"),
            tail_row("read_tail_scanned", "untold"),
            tail_row("read_tail_scanned", "told"),
        ]
        for row in rows:
            report.validate(row)
        summaries = {
            (entry["case"], entry["side"]): entry for entry in report.summarize(rows)
        }
        self.assertEqual(len(summaries), 14)
        cold = summaries[("read_seek", "cold")]
        self.assertEqual(cold["ns_per_seek"], 1000 / 256)
        self.assertEqual(cold["reads_per_seek"], 62 / 256)
        self.assertEqual(cold["read_bytes_per_seek"], 60 * 32768 / 256)
        # Sixty of the 256 seeks were cold: sixty runs loaded.
        self.assertEqual(cold["runs_loaded"], 60)
        self.assertEqual(cold["ns_per_run_loaded"], 1000 / 60)
        self.assertEqual(cold["read_bytes_per_run_loaded"], 32768)
        self.assertEqual(cold["data_reads_per_run_loaded"], 1)
        self.assertEqual(cold["elapsed_least_ns"], 1000)
        self.assertEqual(cold["elapsed_most_ns"], 1000)
        self.assertEqual(cold["checked_bytes_per_returned"], 60 * 32768 / (256 * 300))
        self.assertEqual(summaries[("read_seek", "warm")]["reads_per_seek"], 0)
        told = summaries[("read_tail", "told")]
        self.assertEqual(told["lag0_hit_rate"], 1.0)
        self.assertEqual(told["lag8_hit_rate"], 1.0)
        self.assertEqual(told["append_ns_per_block"], 1000.0)
        self.assertEqual(summaries[("read_tail", "unfed")]["lag0_hit_rate"], 0.0)
        self.assertEqual(
            summaries[("read_stride", "skipped")],
            {
                "case": "read_stride",
                "side": "skipped",
                "samples": 1,
                "stride_bytes": 1 << 20,
            },
        )
        self.assertEqual(cold["build"], "observed")
        self.assertIn("read_seek.cold:", report.render(list(summaries.values())))

    def test_a_timing_row_is_held_to_what_the_cache_counted(self) -> None:
        # The timing build counts no read at the device, and every read of
        # its tail is a hit for want of one.
        nothing = dict(data_reads=0, data_read_bytes=0, index_reads=0, crc_bulk_bytes=0)
        hits = {f"lag{lag}_hits": 48 - lag for lag in report.LAGS}
        untouched = {f"lag{lag}_device_reads": 0 for lag in report.LAGS}
        rows = [
            seek_row("cold", timing_only=True, **nothing),
            seek_row("warm", timing_only=True),
            sealed_row("read_stride", "seek", timing_only=True, reads=256, **nothing),
            tail_row("read_tail", "unfed", timing_only=True, **hits, **untouched),
            tail_row("read_tail", "told", timing_only=True),
        ]
        for row in rows:
            report.validate(row)
        summaries = {
            (entry["case"], entry["side"]): entry for entry in report.summarize(rows)
        }
        cold = summaries[("read_seek", "cold")]
        self.assertEqual(cold["build"], "timing")
        self.assertEqual(cold["ns_per_seek"], 1000 / 256)
        self.assertEqual(cold["cache_loads"], 62)
        # The shard's own counts are kept: the file layer's are not.
        for entry in summaries.values():
            for name in entry:
                self.assertFalse(
                    not name.startswith("reactor_")
                    and (
                        name.endswith(("_reads", "_per_returned", "_hit_rate"))
                        or name in ("reads_per_seek", "read_bytes_per_seek")
                    ),
                    msg=(entry["case"], name),
                )
        self.assertIn("reactor_reads_per_seek", cold)
        # What the cache counted still binds it.
        for row in (
            seek_row("cold", timing_only=True, cache_loads=0, **nothing),
            seek_row("warm", timing_only=True, cache_loads=1),
            seek_row("cold", timing_only=True, cache_uncached=1, **nothing),
            tail_row("read_tail", "told", timing_only=True, lag2_hits=47),
        ):
            with self.assertRaises(ValueError, msg=row):
                report.validate(row)
        # The two builds of one case are two measurements.
        both = report.summarize([seek_row("warm"), seek_row("warm", timing_only=True)])
        self.assertEqual(
            sorted(entry["build"] for entry in both), ["observed", "timing"]
        )

    def test_medians_are_over_samples_of_one_measurement(self) -> None:
        rows = [seek_row("warm", elapsed_ns=elapsed) for elapsed in (900, 1000, 5000)]
        # Another dataset is another measurement.
        rows.append(seek_row("warm", dataset_digest=78, elapsed_ns=1))
        summaries = report.summarize(rows)
        self.assertEqual(
            sorted((entry["samples"], entry["elapsed_ns"]) for entry in summaries),
            [(1, 1), (3, 1000)],
        )
        # A stalled sample moves no median and is still shown.
        stalled = max(summaries, key=lambda entry: entry["samples"])
        self.assertEqual(stalled["elapsed_least_ns"], 900)
        self.assertEqual(stalled["elapsed_most_ns"], 5000)
        # A tail measured beside the probe is not a sample of one that
        # was not.
        tails = report.summarize(
            [
                tail_row("read_tail", "told"),
                tail_row("read_tail", "told", foreground_turns=7),
            ]
        )
        self.assertEqual(sorted(entry["probed"] for entry in tails), [False, True])
        # So is another depth of read-ahead over the same data.
        depths = report.summarize(
            [
                sealed_row("read_ahead", "fixed", depth=0, asked_ahead=0),
                sealed_row("read_ahead", "fixed", depth=1),
                sealed_row("read_ahead", "fixed", depth=1, elapsed_ns=3000),
            ]
        )
        self.assertEqual(
            sorted((entry["depth"], entry["samples"]) for entry in depths),
            [(0, 1), (1, 2)],
        )

    def test_rejects_rows_that_contradict_their_case(self) -> None:
        contradictions = [
            seek_row("warm", data_reads=1),
            seek_row("warm", index_reads=1),
            seek_row("cold", data_reads=0),
            seek_row("cold", cache_loads=0),
            seek_row("warm", cache_loads=1),
            seek_row("cold", cache_uncached=1),
            sealed_row("read_stride", "scan", batches=511),
            sealed_row("read_ahead", "fixed", batches=511),
            sealed_row("read_ahead", "fixed", depth=5),
            sealed_row("read_ahead", "fixed", depth=0, asked_ahead=1),
            sealed_row("read_ahead", "growing", depth=4, asked_ahead=0),
            sealed_row("read_held", "held"),
            sealed_row("read_held", "held", cache_loads=65, cache_uncached=64),
            sealed_row("read_held", "kept", cache_uncached=1),
            tail_row("read_tail", "told", lag0_device_reads=1),
            tail_row("read_tail", "unfed", lag0_hits=1),
            tail_row("read_tail", "told", lag2_hits=47),
            tail_row("read_tail", "told", lag8_reads=41),
            tail_row("read_tail", "told", scan_ns=1),
        ]
        for row in contradictions:
            with self.assertRaises(ValueError, msg=row):
                report.validate(row)

    def test_stride_rows_say_their_read_ahead(self) -> None:
        scans = [
            sealed_row("read_stride", "scan", elapsed_ns=elapsed)
            for elapsed in (900, 1100)
        ]
        # Another read-ahead over the same data is another measurement.
        scans.append(
            sealed_row("read_stride", "scan", depth=1, grows=False, asked_ahead=63)
        )
        scans.append(sealed_row("read_stride", "scan", grows=False))
        summaries = report.summarize(scans)
        self.assertEqual(
            sorted(
                (entry["depth"], entry["grows"], entry["samples"])
                for entry in summaries
            ),
            [(1, False, 1), (4, False, 1), (4, True, 2)],
        )
        pooled = next(entry for entry in summaries if entry["samples"] == 2)
        self.assertEqual(pooled["elapsed_ns"], 1000)
        self.assertEqual(pooled["asked_ahead"], 62)
        seek = report.summarize([sealed_row("read_stride", "seek", reads=256)])[0]
        self.assertEqual((seek["depth"], seek["asked_ahead"]), (0, 0))
        for row in (
            sealed_row("read_stride", "scan", depth=5),
            sealed_row("read_stride", "scan", depth=0, asked_ahead=1),
            sealed_row("read_stride", "seek", depth=1),
            sealed_row("read_stride", "seek", asked_ahead=1),
            sealed_row("read_stride", "scan", grows=1),
        ):
            with self.assertRaises(ValueError, msg=row):
                report.validate(row)
        for name in ("depth", "grows", "asked_ahead"):
            missing = sealed_row("read_stride", "scan")
            del missing[name]
            with self.assertRaises(ValueError, msg=name):
                report.validate(missing)

    def test_the_reactor_counts_bind_both_builds(self) -> None:
        cold = report.summarize([seek_row("cold")])[0]
        self.assertEqual(cold["reactor_reads"], 62)
        self.assertEqual(cold["reactor_reads_per_seek"], 62 / 256)
        timing = report.summarize(
            [
                seek_row(
                    "cold",
                    timing_only=True,
                    data_reads=0,
                    data_read_bytes=0,
                    index_reads=0,
                    crc_bulk_bytes=0,
                )
            ]
        )[0]
        # The shard's counts are the timing build's only device counts.
        self.assertEqual(timing["reactor_read_bytes"], 60 * 32768 + 2 * 20480)
        for timing_only in (False, True):
            with self.assertRaises(ValueError):
                report.validate(
                    seek_row("warm", timing_only=timing_only, reactor_reads=1)
                )
        missing = seek_row("warm")
        del missing["reactor_flushes"]
        with self.assertRaises(ValueError):
            report.validate(missing)
        with self.assertRaises(ValueError):
            report.validate(tail_row("read_tail", "told", reactor_writes=-1))
        # A scan beside the tail that read nothing measured no scan.
        with self.assertRaises(ValueError):
            report.validate(tail_row("read_tail_scanned", "told", scanned_batches=0))

    def test_rejects_rows_that_are_not_of_a_case(self) -> None:
        missing = seek_row("cold")
        del missing["seeks"]
        extra = seek_row("cold", surprise=1)
        for row in (
            missing,
            extra,
            seek_row("lukewarm"),
            {**seek_row("cold"), "case": "read_everything"},
            seek_row("cold", seeks=-1),
            seek_row("cold", seeks=1.5),
            seek_row("cold", ndebug=1),
            tail_row("read_tail_scanned", "unfed"),
            {**common("read_stride", "skipped"), "stride_bytes": 1, "reads": 1},
        ):
            with self.assertRaises(ValueError, msg=row):
                report.validate(row)

    def test_reads_only_its_own_rows_from_a_log(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "bench.log"
            path.write_text(
                'index_bench_v1 {"case":"warm_lookup"}\n'
                "noise\n" + report.PREFIX + json.dumps(seek_row("warm")) + "\n",
                encoding="utf-8",
            )
            self.assertEqual(len(report.read_measurements(path)), 1)
            path.write_text("noise\n", encoding="utf-8")
            with self.assertRaises(ValueError):
                report.read_measurements(path)
            path.write_text(report.PREFIX + "{not json\n", encoding="utf-8")
            with self.assertRaises(ValueError):
                report.read_measurements(path)
            path.write_text(report.PREFIX + "[1]\n", encoding="utf-8")
            with self.assertRaises(ValueError):
                report.read_measurements(path)


if __name__ == "__main__":
    unittest.main()
