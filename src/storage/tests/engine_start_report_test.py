import copy
import json
import tempfile
import unittest
from pathlib import Path

from src.storage.tests import engine_start_report as report

PROFILE = {
    "allocator": "native",
    "injection": "false",
    "optimized": "true",
    "asan": "false",
    "ubsan": "false",
    "oom_abort": "true",
}


def kind(reads=0, opens=0, writes=0, flushes=0, read_size=4096):
    return {
        "reads": reads,
        "read_bytes": read_size * reads,
        "read_minimum": read_size if reads else 0,
        "read_maximum": read_size if reads else 0,
        "opens": opens,
        "writes": writes,
        "write_bytes": 4096 * writes,
        "flushes": flushes,
        "unlinks": 0,
    }


def measurement(sealed=8, unsealed=0, unlisted=0, timing=False, elapsed=7000):
    listed = sealed + unsealed
    parts = [
        f"{name}_{count}"
        for name, count in (
            ("sealed", sealed),
            ("unsealed", unsealed),
            ("unlisted", unlisted),
        )
        if count
    ]
    row = {
        "case": "_".join(parts) or "no_segments",
        "fdatasync": "aio",
        "sealed": sealed,
        "unsealed": unsealed,
        "unlisted": unlisted,
        "listed": listed,
        "requests_per_segment": 4,
        "request_bytes": 300,
        "one_bucket": True,
        "catalog_bound": 122,
        "writable_bound": 4,
        "deleting_bound": 16,
        "elapsed_ns": elapsed,
        "reactor_cpu_ns": 5000,
        "allocations": 100,
        "tasks": 50,
        "runtime_operations": 40,
        "reactor_flushes": 30,
        "reactor_writes": 20,
        "reactor_write_bytes": 20 * 4096,
        "reactor_reads": 4 + 3 * listed + 6,
        "reactor_read_bytes": (4 + 3 * listed + 6) * 4096,
        "start": {
            "state": "ready",
            "restarted": True,
            "recovered": unsealed,
            "unadopted": 0,
            "resumed": 0,
            "marked_retained": 0,
            "gone": 0,
            "owed": 0,
            "refused": 0,
            "sealing": 0,
            "unresolved": 0,
            "wal_files": 1,
        },
        "store": {
            "wal": {"files": 1, "bytes": 1 << 20},
            "data": {
                "files": sealed + unsealed + unlisted,
                "bytes": (sealed + unsealed + unlisted) << 16,
            },
            "other": {"files": 20, "bytes": 20 << 12},
        },
        "timing_only": timing,
        "ndebug": True,
        "libcxx_hardening_none": True,
        "complete": True,
        "affinity_cpus": [4],
        "build_profile": dict(PROFILE),
    }
    if not timing:
        row.update(
            wal=kind(reads=2, opens=2, writes=4, flushes=2),
            data=kind(reads=2 * listed, opens=listed, flushes=unsealed),
            other=kind(reads=6 + listed, opens=6 + listed, writes=10, flushes=6),
            directory_opens=3,
            directory_syncs=10,
            listings=4,
            stats=12,
            renames=6,
            directory_removes=0,
            crc_bulk_calls=4 + listed,
            crc_bulk_bytes=8192 + 1024 * listed,
            digest_calls=listed,
            digest_bytes=256 * listed,
        )
    return row


class EngineStartReportTest(unittest.TestCase):
    def rejected(self, row, message):
        with self.assertRaisesRegex(ValueError, message):
            report.validate(row)

    def test_valid_rows(self):
        for shape in ({}, {"sealed": 0}, {"sealed": 98, "unsealed": 4}):
            report.validate(measurement(**shape))
            report.validate(measurement(timing=True, **shape))
        report.validate(measurement(sealed=0, unlisted=102))

    def test_rejects_malformed_fields(self):
        row = measurement()
        row["tasks"] = -1
        self.rejected(row, "invalid tasks")
        row = measurement()
        row["one_bucket"] = 1
        self.rejected(row, "invalid one_bucket")
        row = measurement()
        row["complete"] = False
        self.rejected(row, "incomplete")
        row = measurement()
        row["elapsed_ns"] = 0
        self.rejected(row, "incomplete")
        row = measurement()
        del row["build_profile"]["oom_abort"]
        self.rejected(row, "build profile")
        row = measurement()
        row["fdatasync"] = "none"
        self.rejected(row, "flush mode")
        row = measurement()
        row["affinity_cpus"] = [4, 4]
        self.rejected(row, "affinity")

    def test_rejects_a_name_or_count_that_contradicts_the_dimensions(self):
        row = measurement()
        row["case"] = "sealed_9"
        self.rejected(row, "name")
        row = measurement(sealed=98, unsealed=4)
        row["case"] = "sealed_98"
        self.rejected(row, "name")
        row = measurement()
        row["listed"] = 9
        self.rejected(row, "listed segments")
        row = measurement()
        row["requests_per_segment"] = 0
        self.rejected(row, "requests_per_segment")
        row = measurement()
        row["request_bytes"] = 0
        self.rejected(row, "request_bytes")

    def test_rejects_a_catalog_beyond_one_restart(self):
        row = measurement(sealed=0, unsealed=5)
        self.rejected(row, "one restart")
        row = measurement()
        row["deleting_bound"] = 119
        self.rejected(row, "one restart")
        row = measurement(sealed=103)
        self.rejected(row, "one restart")
        # The writable segments a case lists take their own room.
        report.validate(measurement(sealed=102))
        report.validate(measurement(sealed=98, unsealed=4))

    def test_rejects_a_start_that_was_not_ready(self):
        row = measurement()
        row["start"]["state"] = "recovering"
        self.rejected(row, "ready")
        row = measurement()
        row["start"]["restarted"] = False
        self.rejected(row, "ready")

    def test_rejects_a_start_that_found_another_store(self):
        row = measurement(sealed=98, unsealed=4)
        row["start"]["recovered"] = 3
        self.rejected(row, "another store")
        for key in report.UNFOUND:
            row = measurement()
            row["start"][key] = 1
            self.rejected(row, "another store")
        row = measurement()
        row["start"]["wal_files"] = 0
        self.rejected(row, "WAL file")

    def test_rejects_a_store_that_disagrees_with_its_case(self):
        row = measurement()
        del row["store"]["other"]
        self.rejected(row, "store kinds")
        row = measurement(sealed=0, unlisted=8)
        row["store"]["data"]["files"] = 7
        self.rejected(row, "number of segments")
        row = measurement()
        row["store"]["wal"]["files"] = 0
        self.rejected(row, "WAL file")

    def test_rejects_a_start_that_made_nothing_durable(self):
        for key in ("reactor_flushes", "reactor_writes"):
            row = measurement(timing=True)
            row[key] = 0
            self.rejected(row, "nothing durable")

    def test_timing_only_cannot_claim_native_work(self):
        row = measurement(timing=True)
        row["data"] = kind()
        self.rejected(row, "timing-only")
        row = measurement(timing=True)
        row["crc_bulk_bytes"] = 0
        self.rejected(row, "timing-only")

    def test_rejects_inconsistent_native_counts(self):
        row = measurement()
        row["data"]["read_minimum"] = 8192
        self.rejected(row, "read sizes")
        row = measurement(sealed=0)
        row["wal"]["read_bytes"] = 0
        row["wal"]["reads"] = 0
        row["wal"]["read_maximum"] = 4096
        self.rejected(row, "without reads")
        row = measurement()
        row["other"]["read_bytes"] += 1
        self.rejected(row, "largest read")

    def test_reads_only_what_the_catalog_lists(self):
        for key in ("reads", "opens"):
            row = measurement(sealed=0, unlisted=102)
            row["data"][key] = 1
            if key == "reads":
                row["data"].update(
                    read_bytes=4096, read_minimum=4096, read_maximum=4096
                )
            self.rejected(row, "does not list")
        for key in ("reads", "opens"):
            row = measurement()
            row["data"][key] = 7
            if key == "reads":
                row["data"]["read_bytes"] = 7 * 4096
            self.rejected(row, "was not read")

    def test_rejects_missing_fresh_flushes(self):
        row = measurement(sealed=98, unsealed=4)
        row["data"]["flushes"] = 3
        self.rejected(row, "fresh flush")
        row = measurement()
        row["wal"]["flushes"] = 0
        self.rejected(row, "fresh flush")

    def test_the_shard_issued_what_the_observer_saw(self):
        # Eight file flushes and ten directory syncs, fourteen writes and
        # thirty-two reads were observed.
        for key, value in (
            ("reactor_flushes", 17),
            ("reactor_writes", 13),
            ("reactor_write_bytes", 14 * 4096 - 1),
            ("reactor_reads", 31),
        ):
            row = measurement()
            row[key] = value
            self.rejected(row, "did not issue")
        row = measurement()
        row.update(
            reactor_flushes=18,
            reactor_writes=14,
            reactor_write_bytes=14 * 4096,
            reactor_reads=32,
        )
        report.validate(row)

    def test_summary_takes_medians_and_requires_one_store(self):
        first, second = measurement(), measurement(elapsed=9000)
        second["data"]["reads"] = 18
        second["data"]["read_bytes"] = 18 * 4096
        summary = report.summarize([first, second])
        self.assertEqual(summary["runs"], 2)
        self.assertEqual(summary["time_to_ready_ns"]["median"], 8000)
        self.assertEqual(summary["native"]["data"]["reads"], 17)
        self.assertEqual(summary["read_amplification"]["data"], 17 * 4096 / (8 << 16))
        self.assertEqual(summary["hashed_bytes"], {"crc32c_bulk": 16384, "xxh3": 2048})
        changed = copy.deepcopy(second)
        changed["start"]["wal_files"] = 2
        with self.assertRaisesRegex(ValueError, "different stores"):
            report.summarize([first, changed])
        timing = report.summarize([measurement(timing=True)])
        self.assertNotIn("native", timing)

    def test_per_listed_segment_against_the_same_store_unlisted(self):
        rows = [
            measurement(sealed=0, elapsed=5000),
            measurement(sealed=0, unlisted=8, elapsed=6000),
            measurement(sealed=8, elapsed=8000),
            measurement(sealed=32, elapsed=13000),
            measurement(sealed=98, unsealed=4, elapsed=40000),
        ]
        cases = {s["case"]: s for s in report.report(rows)["cases"]}
        same = cases["sealed_8"]["per_listed_segment"]
        self.assertEqual(same["baseline"], "unlisted_8")
        self.assertEqual(same["elapsed_ns"], 250)
        self.assertEqual(same["reads"]["data"], 2)
        self.assertEqual(same["crc32c_bulk"], 1024)
        self.assertEqual(same["xxh3"], 256)
        fixed = cases["sealed_32"]["per_listed_segment"]
        self.assertEqual(fixed["baseline"], "no_segments")
        self.assertEqual(fixed["elapsed_ns"], 250)
        mixed = cases["sealed_98_unsealed_4"]["per_listed_segment"]
        self.assertEqual(mixed["baseline"], "no_segments")
        self.assertIsNone(cases["no_segments"]["per_listed_segment"])
        self.assertIsNone(cases["unlisted_8"]["per_listed_segment"])
        alone = report.report([measurement(sealed=32)])["cases"][0]
        self.assertIsNone(alone["per_listed_segment"])
        timing = report.report(
            [measurement(sealed=0, timing=True), measurement(sealed=8, timing=True)]
        )["cases"]
        self.assertEqual(
            timing[1]["per_listed_segment"],
            {"baseline": "no_segments", "elapsed_ns": 0},
        )
        # Another build's baseline is not this one's.
        other = measurement(sealed=0)
        other["build_profile"]["optimized"] = "false"
        separate = report.report([other, measurement(sealed=8)])["cases"]
        self.assertIsNone(separate[1]["per_listed_segment"])

    def test_retained_history_compares_the_unlisted_cases(self):
        rows = [
            measurement(sealed=0),
            measurement(sealed=0, unlisted=8),
            measurement(sealed=0, unlisted=102),
        ]
        rows[0]["other"].update(reads=4, read_bytes=4 * 4096, opens=4)
        (held,) = report.report(rows)["retained_history"]
        self.assertEqual(held["cases"], ["unlisted_8", "unlisted_102"])
        self.assertIs(held["grows"], False)
        self.assertEqual(held["beyond_no_segments"]["unlisted_102"]["other_reads"], 2)
        self.assertEqual(held["beyond_no_segments"]["unlisted_8"]["data_reads"], 0)
        rows[2]["listings"] = 5
        (grown,) = report.report(rows)["retained_history"]
        self.assertIs(grown["grows"], True)
        self.assertEqual(grown["counts"]["unlisted_102"]["listings"], 5)
        (alone,) = report.report(rows[1:2])["retained_history"]
        self.assertIsNone(alone["grows"])
        self.assertNotIn("beyond_no_segments", alone)
        self.assertEqual(
            report.report([measurement(sealed=0, unlisted=8, timing=True)])[
                "retained_history"
            ],
            [],
        )

    def test_reads_rows_with_their_profile(self):
        row = measurement()
        del row["build_profile"]
        lines = [
            "allocator=native abort=true injection=false optimized=true "
            "sanitized=false asan=false ubsan=false",
            report.PREFIX + json.dumps(row),
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "engine_start.log"
            path.write_text("\n".join(lines) + "\n")
            rows = report.read_measurements(path)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["build_profile"], PROFILE)
            path.write_text(
                "\n".join([*lines, "kwaque-benchmark-profile-v1 allocator=system"])
                + "\n"
            )
            with self.assertRaisesRegex(ValueError, "conflicting"):
                report.read_measurements(path)
            path.write_text(lines[0] + "\n")
            with self.assertRaisesRegex(ValueError, "no completed"):
                report.read_measurements(path)


if __name__ == "__main__":
    unittest.main()
