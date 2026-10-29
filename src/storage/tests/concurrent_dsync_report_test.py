from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from src.storage.tests import concurrent_dsync_report as report

PROFILE = {
    "allocator": "native",
    "injection": "false",
    "optimized": "true",
    "asan": "false",
    "ubsan": "false",
    "oom_abort": "true",
}


def measurement(files=1, round_ns=100_000):
    rounds = 16
    return {
        "files": files,
        "rounds": rounds,
        "write_bytes": 4096,
        "directory": "test",
        "elapsed_ns": rounds * round_ns + 1_000,
        "reactor_flushes": 0,
        "reactor_writes": files * rounds,
        "reactor_write_bytes": files * rounds * 4096,
        "write_p50_ns": round_ns // 2,
        "write_p99_ns": round_ns - 10,
        "write_max_ns": round_ns,
        "ndebug": True,
        "rounds_ns": [round_ns] * rounds,
        "affinity_cpus": [3],
        "build_profile": dict(PROFILE),
    }


class ConcurrentDsyncReportTest(unittest.TestCase):
    def assertRefused(self, mutate):
        row = measurement()
        mutate(row)
        with self.assertRaises((ValueError, KeyError, TypeError)):
            report.validate(row)

    def test_a_row_validates(self):
        report.validate(measurement())
        report.validate(measurement(files=1024))

    def test_the_field_set_is_exact(self):
        self.assertRefused(lambda r: r.pop("rounds_ns"))
        self.assertRefused(lambda r: r.update(extra=1))

    def test_what_a_row_says_holds(self):
        self.assertRefused(lambda r: r.update(reactor_flushes=1))
        self.assertRefused(lambda r: r.update(reactor_writes=15))
        self.assertRefused(lambda r: r.update(write_bytes=1000))
        self.assertRefused(lambda r: r.update(rounds_ns=[1] * 15))
        self.assertRefused(lambda r: r.update(write_p99_ns=1))
        self.assertRefused(lambda r: r.update(write_max_ns=200_000))
        self.assertRefused(lambda r: r.update(elapsed_ns=1))
        self.assertRefused(lambda r: r["build_profile"].pop("asan"))
        self.assertRefused(lambda r: r.update(affinity_cpus=[]))

    def test_reads_rows_with_their_profile(self):
        row = measurement()
        row.pop("build_profile")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "run.log"
            line = "allocator=native abort=true injection=false optimized=true "
            line += "sanitized=false asan=false ubsan=false"
            path.write_text(line + "\n" + report.PREFIX + json.dumps(row) + "\n")
            self.assertEqual(report.read_measurements(path)[0]["files"], 1)
            path.write_text(line + "\n")
            with self.assertRaises(ValueError):
                report.read_measurements(path)

    def test_summary_sets_each_width_beside_the_first(self):
        rows = [measurement(1, 100_000), measurement(16, 150_000)]
        summary = report.summaries(rows)
        self.assertEqual(len(summary), 1)
        widths = summary[0]["widths"]
        self.assertEqual([w["files"] for w in widths], [1, 16])
        self.assertEqual(widths[1]["round_p50_over_first"], 1.5)
        self.assertAlmostEqual(
            widths[1]["writes_per_second"], 16 * 16 * 1e9 / (16 * 150_000 + 1_000)
        )
        other = measurement()
        other["write_bytes"] = 8192
        other["reactor_write_bytes"] *= 2
        self.assertEqual(len(report.summaries(rows + [other])), 2)


if __name__ == "__main__":
    unittest.main()
