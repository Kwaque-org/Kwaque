import copy
import json
import tempfile
import unittest
from pathlib import Path

from src.storage.tests import storage_recovery_bench_report as report

PROFILE = {
    "allocator": "native",
    "injection": "false",
    "optimized": "true",
    "asan": "false",
    "ubsan": "false",
    "oom_abort": "true",
}


def kind(reads=4, read_bytes=4 * 131072, writes=0, flushes=0):
    return {
        "reads": reads,
        "read_bytes": read_bytes,
        "read_minimum": 4096 if reads else 0,
        "read_maximum": 131072 if reads else 0,
        "opens": 1,
        "writes": writes,
        "write_bytes": 4096 * writes,
        "flushes": flushes,
    }


def step(elapsed, timing=False):
    value = {"elapsed_ns": elapsed}
    if not timing:
        value.update({key: 0 for key in report.STEP_COUNTERS})
    return value


def measurement(timing=False):
    row = {key: 0 for key in report.COUNTERS}
    row.update({key: False for key in report.FLAGS})
    row.update(
        case="many_small",
        state="stopped",
        fdatasync="aio",
        window_bytes=131072,
        read_ahead=2,
        requests=16,
        segments=2,
        child_bytes=4096,
        wal_files=1,
        wal_file_bytes=1 << 20,
        data_file_bytes=1 << 19,
        elapsed_ns=7000,
        reactor_cpu_ns=5000,
        allocations=100,
        tasks=50,
        complete=True,
        timing_only=timing,
        steps={name: step(1000, timing) for name in report.STEPS},
        classification={
            "verdict": "ready",
            "wal_action": "activate_successor",
            "predecessor_position": 139264,
            "wal_scan_files": 1,
            "prepares": 16,
            "unresolved": 0,
            "nonzero_slack": 0,
            "items": 40,
            "reloads": 3,
            "obligations": 2,
            "segments_scanned": 2,
            "boundaries": 2,
            "candidates": 0,
            "suffix": 0,
            "published": 2,
            "segment_actions": {
                "stop": 0,
                "retain": 0,
                "publish_recovering": 2,
                "activate_successor": 0,
            },
            "slots": {c: 16 if c == "satisfied" else 0 for c in report.CLASSIFICATIONS},
            "regions": {c: 3 if c == "content" else 0 for c in report.CLASSIFICATIONS},
        },
        affinity_cpus=[4],
        build_profile=dict(PROFILE),
    )
    if not timing:
        row.update(
            wal=kind(flushes=2, writes=2),
            data=kind(flushes=2),
            other=kind(writes=6, flushes=6),
            directory_opens=1,
            directory_syncs=4,
            listings=3,
            stats=40,
            crc_bulk_calls=16,
            crc_bulk_bytes=8192,
            digest_calls=0,
            digest_bytes=0,
        )
        # The merge checksums the PREPAREs it scans.
        row["steps"]["merge"]["crc_bulk_bytes"] = 8192
    return row


class StorageRecoveryBenchReportTest(unittest.TestCase):
    def test_valid_rows(self):
        report.validate(measurement())
        report.validate(measurement(timing=True))

    def test_rejects_a_restart_that_was_not_ready(self):
        row = measurement()
        row["classification"]["verdict"] = "corrupt"
        with self.assertRaisesRegex(ValueError, "ready"):
            report.validate(row)
        row = measurement()
        row["classification"]["segment_actions"]["publish_recovering"] = 1
        row["classification"]["segment_actions"]["stop"] = 1
        with self.assertRaisesRegex(ValueError, "stopped"):
            report.validate(row)

    def test_rejects_inconsistent_classification(self):
        row = measurement()
        row["classification"]["candidates"] = 1
        with self.assertRaisesRegex(ValueError, "disagree"):
            report.validate(row)
        row = measurement()
        row["classification"]["published"] = 1
        with self.assertRaisesRegex(ValueError, "disagree"):
            report.validate(row)

    def test_rejects_missing_integrity_work(self):
        row = measurement()
        row["steps"]["merge"]["crc_bulk_bytes"] = 0
        with self.assertRaisesRegex(ValueError, "checksum"):
            report.validate(row)
        row = measurement()
        row["data"]["flushes"] = 1
        with self.assertRaisesRegex(ValueError, "fresh flush"):
            report.validate(row)
        row = measurement()
        row["wal"]["flushes"] = 0
        with self.assertRaisesRegex(ValueError, "fresh flush"):
            report.validate(row)

    def test_rejects_steps_beyond_the_interval(self):
        row = measurement()
        row["steps"]["merge"]["elapsed_ns"] = 7000
        with self.assertRaisesRegex(ValueError, "time to ready"):
            report.validate(row)
        row = measurement()
        row["steps"]["merge"]["reads"] = 100
        with self.assertRaisesRegex(ValueError, "reads"):
            report.validate(row)

    def test_rejects_an_inadmissible_window(self):
        for window, depth in ((131072 + 4096, 2), (126976 + 512, 2), (126976, 5)):
            row = measurement()
            row.update(window_bytes=window, read_ahead=depth)
            with self.assertRaisesRegex(ValueError, "window"):
                report.validate(row)
        row = measurement()
        row.update(window_bytes=126976)
        report.validate(row)

    def test_timing_only_cannot_claim_native_work(self):
        row = measurement(timing=True)
        row["wal"] = kind()
        with self.assertRaisesRegex(ValueError, "timing-only"):
            report.validate(row)

    def test_summary_requires_one_classification(self):
        first, second = measurement(), measurement()
        second["elapsed_ns"] = 9000
        summary = report.summarize([first, second])
        self.assertEqual(summary["runs"], 2)
        self.assertEqual(summary["time_to_ready_ns"]["median"], 8000)
        self.assertEqual(summary["read_amplification"]["wal"], 4 * 131072 / (1 << 20))
        self.assertEqual(summary["hashed_bytes"]["crc32c_bulk"], 8192)
        changed = copy.deepcopy(second)
        changed["classification"]["reloads"] = 4
        with self.assertRaisesRegex(ValueError, "classified"):
            report.summarize([first, changed])
        changed = copy.deepcopy(second)
        changed["data_file_bytes"] += 4096
        with self.assertRaisesRegex(ValueError, "different stores"):
            report.summarize([first, changed])

    def test_reads_rows_with_their_profile(self):
        row = measurement()
        del row["build_profile"]
        lines = [
            "allocator=native abort=true injection=false optimized=true "
            "sanitized=false asan=false ubsan=false",
            report.PREFIX + json.dumps(row),
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "restart.log"
            path.write_text("\n".join(lines) + "\n")
            rows = report.read_measurements(path)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["build_profile"], PROFILE)


if __name__ == "__main__":
    unittest.main()
